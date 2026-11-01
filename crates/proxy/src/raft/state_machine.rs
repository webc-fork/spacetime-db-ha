use std::{
    collections::HashMap,
    io::{Cursor, Error},
    sync::{Arc, Mutex},
};

use openraft::{
    BasicNode, Entry, ErrorSubject, ErrorVerb, LogId, RaftLogId, RaftSnapshotBuilder, Snapshot,
    SnapshotMeta, StorageError, StoredMembership, entry::EntryPayload, storage::RaftStateMachine,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::debug;

use crate::{
    metrics,
    raft::types::{ReducerCallResponse, TypeConfig},
    router::{self, SHARD_CONFIG_TAG, ShardConfigCommand, ShardId},
};

type C = TypeConfig;

/// Serializable snapshot of the state machine's metadata.
/// SpacetimeDB is the source of truth for actual data; we only snapshot
/// Raft bookkeeping (last_applied, membership, applied_count) and shard config.
#[derive(Serialize, Deserialize)]
struct SnapshotData {
    last_applied: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
    applied_count: u64,
    /// Shard routing table (module_name -> shard_id). Only populated on shard 0.
    #[serde(default)]
    shard_routes: HashMap<String, ShardId>,
    /// Active shard IDs (excluding shard 0 which always exists). Only populated on shard 0.
    #[serde(default)]
    active_shards: Vec<ShardId>,
}

/// The Raft state machine for RaftTimeDB.
///
/// When a reducer call is committed by Raft consensus, this state machine
/// forwards the raw WebSocket message to the local SpacetimeDB instance.
/// Since SpacetimeDB reducers are pure and deterministic, all replicas
/// executing the same ordered sequence of reducer calls produce identical state.
///
/// Shard 0's state machine additionally handles shard config commands (0xFF prefix).
#[derive(Clone)]
pub struct StateMachineStore {
    inner: Arc<Mutex<StateMachineInner>>,
    /// Channel sends (origin_node_id, raw_message). The forwarder task uses
    /// origin_node_id to skip writes that the local handler already forwarded.
    forwarder: Arc<Mutex<Option<mpsc::UnboundedSender<(u64, Vec<u8>)>>>>,
    /// Channel for shard config commands (shard 0 only).
    shard_config_tx: Arc<Mutex<Option<mpsc::UnboundedSender<ShardConfigCommand>>>>,
    /// Cached snapshot for fast retrieval by new nodes.
    cached_snapshot: Arc<Mutex<Option<Snapshot<C>>>>,
}

struct StateMachineInner {
    last_applied: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
    applied_count: u64,
    /// Shard routing table maintained by shard 0's state machine.
    shard_routes: HashMap<String, ShardId>,
    /// Active shard IDs (excluding shard 0).
    active_shards: Vec<ShardId>,
}

impl Default for StateMachineStore {
    fn default() -> Self {
        Self::new()
    }
}

impl StateMachineStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(StateMachineInner {
                last_applied: None,
                last_membership: StoredMembership::default(),
                applied_count: 0,
                shard_routes: HashMap::new(),
                active_shards: Vec::new(),
            })),
            forwarder: Arc::new(Mutex::new(None)),
            shard_config_tx: Arc::new(Mutex::new(None)),
            cached_snapshot: Arc::new(Mutex::new(None)),
        }
    }

    /// Set the channel used to forward committed reducer calls to SpacetimeDB.
    pub fn set_forwarder(&self, tx: mpsc::UnboundedSender<(u64, Vec<u8>)>) {
        *self.forwarder.lock().unwrap() = Some(tx);
    }

    /// Set the channel for dispatching shard config commands to the RaftPool.
    /// Only used by shard 0's state machine.
    pub fn set_shard_config_tx(&self, tx: mpsc::UnboundedSender<ShardConfigCommand>) {
        *self.shard_config_tx.lock().unwrap() = Some(tx);
    }

    /// Get the last applied log ID.
    pub fn last_applied(&self) -> Option<LogId<u64>> {
        self.inner.lock().unwrap().last_applied
    }

    /// Get the count of applied application entries.
    pub fn applied_count(&self) -> u64 {
        self.inner.lock().unwrap().applied_count
    }
}

impl RaftStateMachine<C> for StateMachineStore {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        let inner = self.inner.lock().unwrap();
        Ok((inner.last_applied, inner.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<ReducerCallResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<C>> + Send,
        I::IntoIter: Send,
    {
        let mut results = Vec::new();
        let forwarder = self.forwarder.lock().unwrap().clone();
        let shard_config_tx = self.shard_config_tx.lock().unwrap().clone();

        for entry in entries {
            let log_id = *entry.get_log_id();

            {
                let mut inner = self.inner.lock().unwrap();
                inner.last_applied = Some(log_id);

                if let EntryPayload::Membership(ref membership) = entry.payload {
                    inner.last_membership = StoredMembership::new(Some(log_id), membership.clone());
                }
            }

            if let EntryPayload::Normal(request) = &entry.payload {
                let is_shard_config = request.raw_message.first() == Some(&SHARD_CONFIG_TAG);

                if is_shard_config {
                    // Shard config command — decode and dispatch, don't forward to SpacetimeDB
                    if let Some(cmd) = router::decode_shard_config(&request.raw_message) {
                        // Update internal state for snapshots
                        {
                            let mut inner = self.inner.lock().unwrap();
                            match &cmd {
                                ShardConfigCommand::CreateShard { shard_id } => {
                                    if !inner.active_shards.contains(shard_id) {
                                        inner.active_shards.push(*shard_id);
                                    }
                                }
                                ShardConfigCommand::AddRoute {
                                    module_name,
                                    shard_id,
                                } => {
                                    inner.shard_routes.insert(module_name.clone(), *shard_id);
                                }
                                ShardConfigCommand::RemoveRoute { module_name } => {
                                    inner.shard_routes.remove(module_name);
                                }
                            }
                        }

                        // Dispatch to RaftPool
                        if let Some(ref tx) = shard_config_tx {
                            let _ = tx.send(cmd);
                        }
                    }
                } else {
                    // Normal write — forward to SpacetimeDB
                    debug!(
                        log_index = log_id.index,
                        msg_len = request.raw_message.len(),
                        "Forwarding committed reducer call to SpacetimeDB"
                    );

                    if let Some(ref tx) = forwarder {
                        let _ = tx.send((request.origin_node_id, request.raw_message.clone()));
                    }
                }

                {
                    let mut inner = self.inner.lock().unwrap();
                    inner.applied_count += 1;
                }

                metrics::ENTRIES_APPLIED.inc();
            }

            // openraft requires one response per entry (including Membership and Blank)
            results.push(ReducerCallResponse { success: true });
        }

        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let data: SnapshotData = serde_json::from_slice(snapshot.get_ref()).map_err(|e| {
            StorageError::from_io_error(
                ErrorSubject::StateMachine,
                ErrorVerb::Read,
                Error::other(e),
            )
        })?;

        {
            let mut inner = self.inner.lock().unwrap();
            inner.last_applied = data.last_applied;
            inner.last_membership = data.last_membership;
            inner.applied_count = data.applied_count;
            inner.shard_routes = data.shard_routes;
            inner.active_shards = data.active_shards;
        }

        // Replay shard config to RaftPool so it materializes shards and routes
        {
            let shard_config_tx = self.shard_config_tx.lock().unwrap().clone();
            if let Some(ref tx) = shard_config_tx {
                let inner = self.inner.lock().unwrap();
                for &shard_id in &inner.active_shards {
                    let _ = tx.send(ShardConfigCommand::CreateShard { shard_id });
                }
                for (module_name, &shard_id) in &inner.shard_routes {
                    let _ = tx.send(ShardConfigCommand::AddRoute {
                        module_name: module_name.clone(),
                        shard_id,
                    });
                }
            }
        }

        // Cache the installed snapshot for future retrieval
        let inner = self.inner.lock().unwrap();
        let json = serde_json::to_vec(&SnapshotData {
            last_applied: inner.last_applied,
            last_membership: inner.last_membership.clone(),
            applied_count: inner.applied_count,
            shard_routes: inner.shard_routes.clone(),
            active_shards: inner.active_shards.clone(),
        })
        .unwrap_or_default();
        drop(inner);

        *self.cached_snapshot.lock().unwrap() = Some(Snapshot {
            meta: meta.clone(),
            snapshot: Box::new(Cursor::new(json)),
        });

        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<C>>, StorageError<u64>> {
        // Return cached snapshot if available
        let cached = self.cached_snapshot.lock().unwrap();
        if let Some(ref snap) = *cached {
            let data = snap.snapshot.get_ref().clone();
            return Ok(Some(Snapshot {
                meta: snap.meta.clone(),
                snapshot: Box::new(Cursor::new(data)),
            }));
        }
        Ok(None)
    }
}

impl RaftSnapshotBuilder<C> for StateMachineStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<C>, StorageError<u64>> {
        let inner = self.inner.lock().unwrap();

        let data = SnapshotData {
            last_applied: inner.last_applied,
            last_membership: inner.last_membership.clone(),
            applied_count: inner.applied_count,
            shard_routes: inner.shard_routes.clone(),
            active_shards: inner.active_shards.clone(),
        };

        let json = serde_json::to_vec(&data).map_err(|e| {
            StorageError::from_io_error(
                ErrorSubject::StateMachine,
                ErrorVerb::Read,
                Error::other(e),
            )
        })?;

        let snapshot_id = inner
            .last_applied
            .map(|id| format!("{}-{}", id.leader_id, id.index))
            .unwrap_or_else(|| "0-0".to_string());

        let meta = SnapshotMeta {
            last_log_id: inner.last_applied,
            last_membership: inner.last_membership.clone(),
            snapshot_id,
        };

        let snapshot = Snapshot {
            meta: meta.clone(),
            snapshot: Box::new(Cursor::new(json.clone())),
        };

        // Cache the snapshot we just built
        drop(inner);
        *self.cached_snapshot.lock().unwrap() = Some(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(json)),
        });

        Ok(snapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_state_machine_has_no_applied_state() {
        let sm = StateMachineStore::new();
        assert!(sm.last_applied().is_none());
        assert_eq!(sm.applied_count(), 0);
    }

    #[tokio::test]
    async fn test_forwarder_receives_committed_writes() {
        let sm = StateMachineStore::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        sm.set_forwarder(tx);

        {
            let forwarder = sm.forwarder.lock().unwrap();
            forwarder
                .as_ref()
                .unwrap()
                .send((1, vec![3, 10, 20]))
                .unwrap();
        }

        let (origin, received) = rx.recv().await.unwrap();
        assert_eq!(origin, 1);
        assert_eq!(received, vec![3, 10, 20]);
    }

    #[test]
    fn test_forwarder_is_none_by_default() {
        let sm = StateMachineStore::new();
        let forwarder = sm.forwarder.lock().unwrap();
        assert!(forwarder.is_none());
    }

    #[test]
    fn test_shard_config_tx_is_none_by_default() {
        let sm = StateMachineStore::new();
        let tx = sm.shard_config_tx.lock().unwrap();
        assert!(tx.is_none());
    }

    #[test]
    fn test_snapshot_data_roundtrip() {
        use openraft::CommittedLeaderId;
        let data = SnapshotData {
            last_applied: Some(LogId::new(CommittedLeaderId::new(1, 0), 42)),
            last_membership: StoredMembership::default(),
            applied_count: 10,
            shard_routes: HashMap::new(),
            active_shards: Vec::new(),
        };
        let json = serde_json::to_vec(&data).unwrap();
        let restored: SnapshotData = serde_json::from_slice(&json).unwrap();
        assert_eq!(restored.last_applied, data.last_applied);
        assert_eq!(restored.applied_count, data.applied_count);
    }

    #[test]
    fn test_snapshot_data_backwards_compat() {
        // Old snapshot format without shard fields should deserialize with defaults
        let old_json = r#"{"last_applied":null,"last_membership":{"log_id":null,"membership":{"configs":[],"nodes":{}}},"applied_count":5}"#;
        let data: SnapshotData = serde_json::from_str(old_json).unwrap();
        assert_eq!(data.applied_count, 5);
        assert!(data.shard_routes.is_empty());
        assert!(data.active_shards.is_empty());
    }

    #[test]
    fn test_snapshot_data_with_shard_config() {
        use openraft::CommittedLeaderId;
        let mut routes = HashMap::new();
        routes.insert("game".to_string(), 1u64);
        routes.insert("chat".to_string(), 2u64);

        let data = SnapshotData {
            last_applied: Some(LogId::new(CommittedLeaderId::new(1, 0), 42)),
            last_membership: StoredMembership::default(),
            applied_count: 10,
            shard_routes: routes,
            active_shards: vec![1, 2],
        };
        let json = serde_json::to_vec(&data).unwrap();
        let restored: SnapshotData = serde_json::from_slice(&json).unwrap();
        assert_eq!(restored.shard_routes.len(), 2);
        assert_eq!(restored.shard_routes["game"], 1);
        assert_eq!(restored.active_shards, vec![1, 2]);
    }

    #[tokio::test]
    async fn test_get_current_snapshot_initially_none() {
        let mut sm = StateMachineStore::new();
        let snapshot = sm.get_current_snapshot().await.unwrap();
        assert!(snapshot.is_none());
    }
}
