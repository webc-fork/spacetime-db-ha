use std::{fs::read, path::Path, time::Duration};

use openraft::error::{ClientWriteError, RaftError};
use reqwest::Client;
use tokio::time::sleep;
mod log_store;
mod network;
pub mod persistent_log_store;
pub mod state_machine;
pub mod types;

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use openraft::{BasicNode, Config, Raft};
use state_machine::StateMachineStore;
use tokio::sync::{RwLock, mpsc, watch};
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};
pub use types::*;

use crate::{
    config::NodeConfig,
    metrics,
    router::{ShardConfigCommand, ShardId, ShardRouter},
};

/// Parse peer strings in the format "id=host:port" into a BTreeMap.
/// Example: ["2=node-2:4001", "3=node-3:4001"] -> {2: BasicNode{addr: "node-2:4001"}, ...}
pub fn parse_peers(peers: &[String]) -> BTreeMap<u64, BasicNode> {
    let mut map = BTreeMap::new();
    for peer in peers {
        if let Some((id_str, addr)) = peer.split_once('=') {
            if let Ok(id) = id_str.parse::<u64>() {
                map.insert(
                    id,
                    BasicNode {
                        addr: addr.to_string(),
                    },
                );
            } else {
                warn!(peer = %peer, "Failed to parse peer node ID");
            }
        } else {
            warn!(peer = %peer, "Invalid peer format, expected 'id=host:port'");
        }
    }
    map
}

/// Per-shard state: Raft instance, state machine, forwarder notification.
struct ShardState {
    raft: Raft<TypeConfig>,
    state_machine: StateMachineStore,
    /// Broadcasts the database path to this shard's forwarder task.
    db_path_tx: watch::Sender<String>,
}

/// Pool of Raft groups — one per shard.
///
/// Shard 0 always exists and is the default. Additional shards can be created
/// dynamically via shard config commands replicated through shard 0's Raft log.
///
/// All nodes participate in all shards (same membership).
pub struct RaftPool {
    pub node_config: Arc<NodeConfig>,
    pub peers: BTreeMap<u64, BasicNode>,
    shards: RwLock<HashMap<ShardId, ShardState>>,
    router: RwLock<ShardRouter>,
    shutdown_rx: watch::Receiver<bool>,
}

impl RaftPool {
    /// Create a new RaftPool with shard 0 initialized.
    /// Returns an Arc since background tasks reference the pool.
    pub async fn new(config: &NodeConfig, shutdown_rx: watch::Receiver<bool>) -> Result<Arc<Self>> {
        let peers = parse_peers(&config.peers);
        let node_config = Arc::new(config.clone());

        // Create shard 0's state machine with shard config callback
        let (shard_config_tx, shard_config_rx) = mpsc::unbounded_channel();
        let shard_state = Self::create_shard_state(config, 0).await?;
        shard_state
            .state_machine
            .set_shard_config_tx(shard_config_tx);

        // Set up forwarder for shard 0
        let (forwarder_tx, forwarder_rx) = mpsc::unbounded_channel::<(u64, Vec<u8>)>();
        shard_state.state_machine.set_forwarder(forwarder_tx);
        Self::spawn_forwarder(
            config.node_id,
            config.stdb_url.clone(),
            shard_state.db_path_tx.subscribe(),
            forwarder_rx,
            shutdown_rx.clone(),
        );

        let mut shards = HashMap::new();
        shards.insert(0, shard_state);

        info!(
            node_id = config.node_id,
            peer_count = peers.len(),
            tls = config.tls_enabled(),
            "RaftPool initialized with shard 0"
        );

        let pool = Arc::new(Self {
            node_config,
            peers,
            shards: RwLock::new(shards),
            router: RwLock::new(ShardRouter::new()),
            shutdown_rx,
        });

        // Spawn shard config listener — materializes shard changes from Raft
        let pool_ref = pool.clone();
        let mut listener_shutdown = pool.shutdown_rx.clone();
        tokio::spawn(async move {
            let mut rx = shard_config_rx;
            loop {
                tokio::select! {
                    cmd = rx.recv() => {
                        let Some(cmd) = cmd else { break; };
                        pool_ref.handle_shard_config(cmd).await;
                    }
                    _ = listener_shutdown.changed() => { break; }
                }
            }
        });

        Ok(pool)
    }

    /// Create the Raft instance, state machine, and db_path channel for a shard.
    async fn create_shard_state(config: &NodeConfig, shard_id: ShardId) -> Result<ShardState> {
        let raft_config = Config {
            cluster_name: "rafttimedb".to_string(),
            heartbeat_interval: 200,
            election_timeout_min: 1000,
            election_timeout_max: 2000,
            ..Default::default()
        };
        let raft_config = Arc::new(raft_config.validate()?);

        let state_machine = StateMachineStore::new();

        // Load CA cert for peer verification if configured
        let ca_cert_pem = if let Some(ref ca_path) = config.tls_ca_cert {
            Some(read(ca_path)?)
        } else {
            None
        };

        let network_factory =
            network::NetworkFactory::new(config.tls_enabled(), ca_cert_pem.as_deref(), shard_id);

        // Per-shard persistent log store
        let persistent_store =
            persistent_log_store::PersistentLogStore::new(Path::new(&config.data_dir), shard_id)?;

        let raft = Raft::new(
            config.node_id,
            raft_config,
            network_factory,
            persistent_store,
            state_machine.clone(),
        )
        .await?;

        let (db_path_tx, _) = watch::channel(String::new());

        info!(shard_id, "Raft shard initialized");

        Ok(ShardState {
            raft,
            state_machine,
            db_path_tx,
        })
    }

    /// Spawn a forwarder task for a shard.
    /// The forwarder waits for a database path, connects to SpacetimeDB,
    /// and forwards committed writes from other nodes.
    fn spawn_forwarder(
        node_id: u64,
        stdb_url: String,
        mut db_path_rx: watch::Receiver<String>,
        mut rx: mpsc::UnboundedReceiver<(u64, Vec<u8>)>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        tokio::spawn(async move {
            // Wait until a client connects and we learn the database path
            loop {
                tokio::select! {
                    result = db_path_rx.changed() => {
                        if result.is_err() {
                            return; // sender dropped
                        }
                        let path = db_path_rx.borrow().clone();
                        if !path.is_empty() {
                            info!(path = %path, "Forwarder: received database path from client");
                            break;
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        return;
                    }
                }
            }

            let db_path = db_path_rx.borrow().clone();
            let full_url = format!("{}{}", stdb_url, db_path);

            loop {
                if *shutdown_rx.borrow() {
                    info!("Forwarder shutting down");
                    return;
                }

                match tokio_tungstenite::connect_async(&full_url).await {
                    Ok((ws, _)) => {
                        info!(url = %full_url, "Forwarder connected to SpacetimeDB");
                        let (mut ws_tx, _ws_rx) = ws.split();

                        loop {
                            tokio::select! {
                                msg = rx.recv() => {
                                    let Some((origin_node_id, data)) = msg else {
                                        return;
                                    };

                                    // Skip writes from this node — the handler already
                                    // forwarded them through the client's connection
                                    if origin_node_id == node_id {
                                        continue;
                                    }

                                    if let Err(e) = ws_tx.send(Message::Binary(data.into())).await {
                                        warn!(error = %e, "Forwarder: SpacetimeDB send failed, reconnecting");
                                        break;
                                    }
                                }
                                _ = shutdown_rx.changed() => {
                                    info!("Forwarder shutting down");
                                    return;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, url = %full_url, "Forwarder: SpacetimeDB connect failed, retrying in 1s");
                        sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });
    }

    /// Handle a shard config command committed through shard 0's Raft.
    async fn handle_shard_config(&self, cmd: ShardConfigCommand) {
        match cmd {
            ShardConfigCommand::CreateShard { shard_id } => {
                // Check if shard already exists
                if self.shards.read().await.contains_key(&shard_id) {
                    return;
                }

                info!(shard_id, "Creating new shard from Raft config command");

                match Self::create_shard_state(&self.node_config, shard_id).await {
                    Ok(shard_state) => {
                        // Set up forwarder for the new shard
                        let (forwarder_tx, forwarder_rx) = mpsc::unbounded_channel();
                        shard_state.state_machine.set_forwarder(forwarder_tx);
                        Self::spawn_forwarder(
                            self.node_config.node_id,
                            self.node_config.stdb_url.clone(),
                            shard_state.db_path_tx.subscribe(),
                            forwarder_rx,
                            self.shutdown_rx.clone(),
                        );

                        self.shards.write().await.insert(shard_id, shard_state);

                        // If we're the shard 0 leader, initialize the new shard
                        // with the same membership as shard 0
                        if let Some(raft0) = self.get_raft(0).await
                            && raft0.ensure_linearizable().await.is_ok()
                        {
                            let members = self.get_current_members(0).await;
                            if let Some(new_raft) = self.get_raft(shard_id).await {
                                if let Err(e) = new_raft.initialize(members).await {
                                    warn!(shard_id, error = %e, "Failed to initialize new shard");
                                } else {
                                    info!(
                                        shard_id,
                                        "New shard initialized with existing membership"
                                    );
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!(shard_id, error = %e, "Failed to create shard state");
                    }
                }
            }
            ShardConfigCommand::AddRoute {
                module_name,
                shard_id,
            } => {
                info!(module = %module_name, shard_id, "Adding route");
                self.router.write().await.add_route(module_name, shard_id);
            }
            ShardConfigCommand::RemoveRoute { module_name } => {
                info!(module = %module_name, "Removing route");
                self.router.write().await.remove_route(&module_name);
            }
        }
    }

    /// Get current members from a shard's Raft metrics.
    async fn get_current_members(&self, shard_id: ShardId) -> BTreeMap<u64, BasicNode> {
        let shards = self.shards.read().await;
        if let Some(shard) = shards.get(&shard_id) {
            let metrics = shard.raft.metrics().borrow().clone();
            let voter_ids: BTreeSet<u64> =
                metrics.membership_config.membership().voter_ids().collect();

            let mut members = BTreeMap::new();
            for id in voter_ids {
                if id == self.node_config.node_id {
                    members.insert(
                        id,
                        BasicNode {
                            addr: self.node_config.raft_addr.clone(),
                        },
                    );
                } else if let Some(node) = self.peers.get(&id) {
                    members.insert(id, node.clone());
                }
            }
            members
        } else {
            BTreeMap::new()
        }
    }

    // ========================================================================
    // Public API
    // ========================================================================

    /// Get the Raft instance for a specific shard.
    pub async fn get_raft(&self, shard_id: ShardId) -> Option<Raft<TypeConfig>> {
        self.shards
            .read()
            .await
            .get(&shard_id)
            .map(|s| s.raft.clone())
    }

    /// Get the state machine for a specific shard.
    pub async fn get_state_machine(&self, shard_id: ShardId) -> Option<StateMachineStore> {
        self.shards
            .read()
            .await
            .get(&shard_id)
            .map(|s| s.state_machine.clone())
    }

    /// Route a module name to a shard ID.
    pub async fn route_module(&self, module_name: &str) -> ShardId {
        self.router.read().await.route(module_name)
    }

    /// Broadcast a database path to a shard's forwarder.
    pub async fn send_db_path(&self, shard_id: ShardId, path: String) {
        let shards = self.shards.read().await;
        if let Some(shard) = shards.get(&shard_id) {
            let _ = shard.db_path_tx.send(path);
        }
    }

    /// Propose a write (reducer call) through a shard's Raft consensus.
    /// If this node is not the leader, forwards the request to the leader via HTTP.
    pub async fn propose_write(&self, shard_id: ShardId, data: Vec<u8>) -> Result<()> {
        let timer = metrics::WRITE_LATENCY.start_timer();
        metrics::WRITES_TOTAL.inc();

        let raft = self
            .get_raft(shard_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("shard {} not found", shard_id))?;

        let request = ReducerCallRequest {
            raw_message: data.clone(),
            origin_node_id: self.node_config.node_id,
        };

        let result = match raft.client_write(request).await {
            Ok(_) => Ok(()),
            Err(err) => {
                // Check for ForwardToLeader — this node isn't the leader for this shard
                if let RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) = &err
                    && let Some(leader_node) = &forward.leader_node
                {
                    let scheme = self.node_config.http_scheme();
                    let url = format!(
                        "{}://{}/cluster/{}/write",
                        scheme, leader_node.addr, shard_id
                    );
                    info!(leader_addr = %leader_node.addr, shard_id, "Forwarding write to leader");

                    // Preserve origin_node_id so the forwarder on this node
                    // knows to skip (handler already forwards to local SpacetimeDB)
                    let forward_request = ReducerCallRequest {
                        raw_message: data,
                        origin_node_id: self.node_config.node_id,
                    };

                    let client = Client::new();
                    let resp = client
                        .post(&url)
                        .json(&forward_request)
                        .send()
                        .await
                        .map_err(|e| anyhow::anyhow!("Leader forward failed: {}", e))?;

                    if resp.status().is_success() {
                        return Ok(());
                    }
                    return Err(anyhow::anyhow!("Leader forward returned {}", resp.status()));
                }
                Err(err.into())
            }
        };

        timer.observe_duration();
        result
    }

    /// Check if this node is the current leader for a shard.
    pub async fn is_leader(&self, shard_id: ShardId) -> bool {
        if let Some(raft) = self.get_raft(shard_id).await {
            raft.ensure_linearizable().await.is_ok()
        } else {
            false
        }
    }

    /// Get the current leader's node ID and address for a shard.
    pub async fn current_leader(&self, shard_id: ShardId) -> Option<(u64, String)> {
        let shards = self.shards.read().await;
        let shard = shards.get(&shard_id)?;
        let metrics = shard.raft.metrics().borrow().clone();
        let leader_id = metrics.current_leader?;

        if leader_id == self.node_config.node_id {
            return Some((leader_id, self.node_config.raft_addr.clone()));
        }

        let addr = self.peers.get(&leader_id)?.addr.clone();
        Some((leader_id, addr))
    }

    /// Update Prometheus metrics from current Raft state (using shard 0).
    pub async fn update_metrics(&self) {
        let shards = self.shards.read().await;
        if let Some(shard) = shards.get(&0) {
            let raft_metrics = shard.raft.metrics().borrow().clone();
            metrics::RAFT_TERM.set(raft_metrics.vote.leader_id().term as i64);
            let is_leader = raft_metrics.current_leader == Some(self.node_config.node_id);
            metrics::RAFT_IS_LEADER.set(if is_leader { 1 } else { 0 });
        }
    }

    /// List all active shard IDs.
    pub async fn list_shards(&self) -> Vec<ShardId> {
        let shards = self.shards.read().await;
        let mut ids: Vec<_> = shards.keys().copied().collect();
        ids.sort();
        ids
    }

    /// Get all routes from the shard router.
    pub async fn get_routes(&self) -> HashMap<String, ShardId> {
        self.router.read().await.routes().clone()
    }

    /// Add a node to all active Raft shard groups.
    pub async fn add_node_all_shards(&self, node_id: u64, addr: String) -> Result<(), String> {
        let shard_ids = self.list_shards().await;
        for shard_id in shard_ids {
            let raft = self
                .get_raft(shard_id)
                .await
                .ok_or_else(|| format!("shard {} not found", shard_id))?;

            let node = BasicNode { addr: addr.clone() };
            raft.add_learner(node_id, node, true)
                .await
                .map_err(|e| format!("add_learner on shard {}: {}", shard_id, e))?;

            let metrics = raft.metrics().borrow().clone();
            let mut voter_ids: BTreeSet<u64> =
                metrics.membership_config.membership().voter_ids().collect();
            voter_ids.insert(node_id);

            raft.change_membership(voter_ids, false)
                .await
                .map_err(|e| format!("change_membership on shard {}: {}", shard_id, e))?;

            info!(shard_id, node_id, "Node added to shard");
        }
        Ok(())
    }

    /// Remove a node from all active Raft shard groups.
    pub async fn remove_node_all_shards(&self, node_id: u64) -> Result<(), String> {
        let shard_ids = self.list_shards().await;
        for shard_id in shard_ids {
            let raft = self
                .get_raft(shard_id)
                .await
                .ok_or_else(|| format!("shard {} not found", shard_id))?;

            let metrics = raft.metrics().borrow().clone();
            let voter_ids: BTreeSet<u64> = metrics
                .membership_config
                .membership()
                .voter_ids()
                .filter(|id| *id != node_id)
                .collect();

            raft.change_membership(voter_ids, false)
                .await
                .map_err(|e| format!("change_membership on shard {}: {}", shard_id, e))?;

            info!(shard_id, node_id, "Node removed from shard");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_peers_valid() {
        let peers = vec!["2=node-2:4001".to_string(), "3=node-3:4001".to_string()];
        let map = parse_peers(&peers);
        assert_eq!(map.len(), 2);
        assert_eq!(map[&2].addr, "node-2:4001");
        assert_eq!(map[&3].addr, "node-3:4001");
    }

    #[test]
    fn test_parse_peers_empty() {
        let peers: Vec<String> = vec![];
        let map = parse_peers(&peers);
        assert!(map.is_empty());
    }

    #[test]
    fn test_parse_peers_invalid_format() {
        let peers = vec!["bad-format".to_string()];
        let map = parse_peers(&peers);
        assert!(map.is_empty());
    }

    #[test]
    fn test_parse_peers_invalid_id() {
        let peers = vec!["abc=node:4001".to_string()];
        let map = parse_peers(&peers);
        assert!(map.is_empty());
    }
}
