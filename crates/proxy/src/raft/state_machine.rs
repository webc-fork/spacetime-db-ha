use std::{
    fs::create_dir_all,
    io::{self, Cursor},
    path::Path,
    sync::Arc,
};

use bitcode::{Decode, Encode};
use crossfire::{
    AsyncRx, MAsyncTx,
    mpsc::{Array, bounded_async},
};
use fjall::{Database, Keyspace, KeyspaceCreateOptions};
use futures::{Stream, StreamExt};
use gxhash::HashMap;
use openraft::{
    OptionalSend, RaftSnapshotBuilder, Snapshot, SnapshotMeta,
    entry::EntryPayload,
    storage::{EntryResponder, RaftStateMachine},
};
use parking_lot::Mutex;
use tracing::{debug, warn};

use crate::{
    metrics,
    raft::{
        CommittedWrite,
        codec::{WireLogId, WireStoredMembership},
        error::{StoreError, storage_io_err},
        persistent_log_store::{DB_DIR, shard_dir},
        types::{
            RaftEntry, RaftLogId, RaftSnapshot, RaftSnapshotMeta, RaftStoredMembership,
            ReducerCallResponse, TypeConfig,
        },
    },
    router::{self, DEFAULT_SHARD, SHARD_CONFIG_TAG, ShardConfigCommand, ShardId},
    transport::watch,
};

type C = TypeConfig;

/// 有界队列容量（修复 P2-12：消除无界通道内存膨胀）。
pub const FORWARDER_CHANNEL_CAPACITY: usize = 4096;
/// 分片配置命令队列容量。
pub const CONFIG_CHANNEL_CAPACITY: usize = 256;

/// 提交后待投递的写请求有界通道发送端（基于 `crossfire`）。
pub type WriteSender = MAsyncTx<Array<CommittedWrite>>;
/// 提交后待投递的写请求有界通道接收端（基于 `crossfire`）。
pub type WriteReceiver = AsyncRx<Array<CommittedWrite>>;
/// 分片配置命令有界通道发送端（基于 `crossfire`）。
pub type ConfigSender = MAsyncTx<Array<ShardConfigCommand>>;
/// 分片配置命令有界通道接收端（基于 `crossfire`）。
pub type ConfigReceiver = AsyncRx<Array<ShardConfigCommand>>;

/// 创建待投递写请求的有界异步通道。
pub fn write_channel(capacity: usize) -> (WriteSender, WriteReceiver) {
    bounded_async(capacity)
}

/// 创建分片配置命令的有界异步通道。
pub fn config_channel(capacity: usize) -> (ConfigSender, ConfigReceiver) {
    bounded_async(capacity)
}

/// 状态机元数据 Keyspace：键 -> `bitcode` 序列化元数据。
const STATE_KEYSPACE: &str = "state_machine";
/// 元数据键。
const META_KEY: &str = "meta";
/// 已投递到上游 SpacetimeDB 的最新日志索引键（修复 P0-2）。
const META_DELIVERED: &str = "last_delivered";

/// 状态机元数据的可序列化快照。
///
/// 真实数据以 SpacetimeDB 为准，这里只固化 Raft 记账信息与分片配置。
#[derive(Encode, Decode, Default)]
struct SnapshotData {
    last_applied: Option<WireLogId>,
    last_membership: WireStoredMembership,
    /// 分片路由表（模块名 -> 分片），仅分片 0 有内容。
    shard_routes: HashMap<String, ShardId>,
    /// 已激活的分片 ID（不含分片 0），仅分片 0 有内容。
    active_shards: Vec<ShardId>,
}

/// Raft 状态机（基于 fjall 持久化）。
///
/// reducer 调用被 Raft 提交后，这里先持久化水位，再把写请求交给本地转发器。
/// 同时持久化 `last_delivered` 水位，崩溃重启后自动重放 `(last_delivered, last_applied]`。
#[derive(Clone)]
pub struct StateMachineStore {
    shard_id: ShardId,
    inner: Arc<Mutex<StateMachineInner>>,
    /// 把已提交的写投递给转发任务。
    forwarder: Arc<Mutex<Option<WriteSender>>>,
    /// 分片配置命令通道，仅分片 0 使用。
    shard_config_tx: Arc<Mutex<Option<ConfigSender>>>,
    /// 缓存最近一次快照，供新节点快速拉取。
    cached_snapshot: Arc<Mutex<Option<RaftSnapshot>>>,
    /// 状态机元数据 Keyspace（与 `PersistentLogStore` 共享同一个 `fjall::Database`）。
    state_ks: Keyspace,
    /// 已应用到本节点的最新日志 index，供写入方等待「写已落地」。
    applied: Arc<watch::Sender<u64>>,
    /// 已投递到上游 SpacetimeDB 的最新日志 index（用于保序与崩溃重放）。
    delivered: Arc<watch::Sender<u64>>,
}

struct StateMachineInner {
    last_applied: Option<RaftLogId>,
    last_membership: RaftStoredMembership,
    shard_routes: HashMap<String, ShardId>,
    active_shards: Vec<ShardId>,
}

impl StateMachineInner {
    /// 把配置命令体现在状态机内部状态上（快照据此重建）。
    fn apply_command(&mut self, cmd: &ShardConfigCommand) {
        match cmd {
            ShardConfigCommand::CreateShard { shard_id } => {
                if !self.active_shards.contains(shard_id) {
                    self.active_shards.push(*shard_id);
                }
            }
            ShardConfigCommand::RemoveShard { shard_id } => {
                self.active_shards.retain(|id| id != shard_id);
                self.shard_routes.retain(|_, sid| sid != shard_id);
            }
            ShardConfigCommand::AddRoute {
                module_name,
                shard_id,
            } => {
                self.shard_routes.insert(module_name.clone(), *shard_id);
            }
            ShardConfigCommand::RemoveRoute { module_name } => {
                self.shard_routes.remove(module_name);
            }
        }
    }

    /// 生成待持久化的快照数据。
    fn snapshot_data(&self) -> SnapshotData {
        SnapshotData {
            last_applied: self.last_applied.map(WireLogId::from),
            last_membership: WireStoredMembership::from(&self.last_membership),
            shard_routes: self.shard_routes.clone(),
            active_shards: self.active_shards.clone(),
        }
    }
}

impl StateMachineStore {
    /// 独立打开（或创建）分片的持久化状态机，使用与日志库一致的 `fjall` 目录。
    pub fn open(data_dir: &Path, shard_id: ShardId) -> Result<Self, StoreError> {
        let dir = shard_dir(data_dir, shard_id);
        create_dir_all(&dir)?;
        let db = Database::builder(dir.join(DB_DIR)).open()?;
        Self::open_with_db(&db, shard_id)
    }

    /// 复用已打开的 `fjall::Database` 实例创建状态机（修复 P2-10：单库统一持久化）。
    pub fn open_with_db(db: &Database, shard_id: ShardId) -> Result<Self, StoreError> {
        let state_ks = db.keyspace(STATE_KEYSPACE, KeyspaceCreateOptions::default)?;

        let meta = match state_ks.get(META_KEY)? {
            Some(value) => bitcode::decode(&value)?,
            None => SnapshotData::default(),
        };
        let delivered = match state_ks.get(META_DELIVERED)? {
            Some(value) => bitcode::decode::<u64>(&value).unwrap_or(0),
            None => 0,
        };

        let last_applied = meta.last_applied.map(RaftLogId::from);
        let last_membership = RaftStoredMembership::from(meta.last_membership);
        let applied = last_applied.map_or(0, |log_id| log_id.index);
        debug!(
            shard_id,
            applied, delivered, "State machine restored from fjall"
        );

        Ok(Self {
            shard_id,
            inner: Arc::new(Mutex::new(StateMachineInner {
                last_applied,
                last_membership,
                shard_routes: meta.shard_routes,
                active_shards: meta.active_shards,
            })),
            forwarder: Arc::new(Mutex::new(None)),
            shard_config_tx: Arc::new(Mutex::new(None)),
            cached_snapshot: Arc::new(Mutex::new(None)),
            state_ks,
            applied: Arc::new(watch::channel(applied).0),
            delivered: Arc::new(watch::channel(delivered).0),
        })
    }

    /// 启动时校验 `last_applied >= last_purged`，防止状态机落后于已清理日志（修复 P2-10）。
    pub fn align_with_purged(&self, last_purged: Option<RaftLogId>) -> Result<(), StoreError> {
        let Some(purged) = last_purged else {
            return Ok(());
        };
        let needs_align = {
            let mut inner = self.inner.lock();
            if inner.last_applied.map_or(0, |id| id.index) < purged.index {
                inner.last_applied = Some(purged);
                true
            } else {
                false
            }
        };
        if needs_align {
            self.persist()?;
            self.applied.send_replace(purged.index);
            self.mark_delivered(purged.index);
        }
        Ok(())
    }

    /// 设置已提交写的投递通道。
    pub fn set_forwarder(&self, tx: WriteSender) {
        *self.forwarder.lock() = Some(tx);
    }

    /// 设置分片配置命令通道，仅分片 0 的状态机需要。
    pub fn set_shard_config_tx(&self, tx: ConfigSender) {
        *self.shard_config_tx.lock() = Some(tx);
    }

    /// 订阅「已应用的日志 index」，用于等待某条写真正落地。
    pub fn applied_rx(&self) -> watch::Receiver<u64> {
        self.applied.subscribe()
    }

    /// 当前已应用的最大日志 index。
    pub fn last_applied_index(&self) -> u64 {
        *self.applied.borrow()
    }

    /// 当前已投递到上游的最大日志 index。
    pub fn last_delivered_index(&self) -> u64 {
        *self.delivered.borrow()
    }

    /// 订阅「已投递到上游的日志 index」，用于跨连接全局保序（修复 P0-1）。
    pub fn delivered_rx(&self) -> watch::Receiver<u64> {
        self.delivered.subscribe()
    }

    /// 推进并持久化 `last_delivered` 水位（修复 P0-2）。
    pub fn mark_delivered(&self, index: u64) {
        let mut updated = false;
        self.delivered.send_if_modified(|cur| {
            if index > *cur {
                *cur = index;
                updated = true;
                true
            } else {
                false
            }
        });
        if updated {
            let bytes = bitcode::encode(&index);
            let _ = self.state_ks.insert(META_DELIVERED, bytes);
        }
    }

    /// 读取已持久化的分片配置（激活分片列表与路由表）。
    pub fn persisted_topology(&self) -> (Vec<ShardId>, HashMap<String, ShardId>) {
        let inner = self.inner.lock();
        (inner.active_shards.clone(), inner.shard_routes.clone())
    }

    /// 把当前记账状态落盘。先取快照再写盘，避免持锁做 IO。
    fn persist(&self) -> Result<(), StoreError> {
        let data = self.inner.lock().snapshot_data();
        let bytes = bitcode::encode(&data);
        self.state_ks.insert(META_KEY, bytes)?;
        Ok(())
    }

    /// 批量应用日志条目（供 `RaftStateMachine::apply` 与单测直接调用）。
    pub async fn apply_entries<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<ReducerCallResponse>, io::Error>
    where
        I: IntoIterator<Item = RaftEntry>,
    {
        let mut results = Vec::new();
        self.apply_entries_with_hook(entries, |resp| results.push(resp))
            .await?;
        Ok(results)
    }

    /// 内部共享的应用逻辑。
    async fn apply_entries_with_hook<I, F>(
        &mut self,
        entries: I,
        mut on_applied: F,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = RaftEntry>,
        F: FnMut(ReducerCallResponse),
    {
        let forwarder = self.forwarder.lock().clone();
        let shard_config_tx = self.shard_config_tx.lock().clone();
        let mut pending_commands = Vec::new();
        let mut pending_writes = Vec::new();
        let mut non_write_indices = Vec::new();
        let mut last_index = None;
        let mut count = 0usize;

        for entry in entries {
            let log_id = entry.log_id;
            let is_application = matches!(&entry.payload, EntryPayload::Normal(_));

            // 先分类，反序列化与分支判断都不持锁
            // 仅当分片 0 且 db_path 为空且首字节为 SHARD_CONFIG_TAG 时，才按分片配置命令解析（修复 P2-14）
            let (membership, command, write) = match entry.payload {
                EntryPayload::Membership(membership) => (
                    Some(RaftStoredMembership::new(Some(log_id), membership)),
                    None,
                    None,
                ),
                EntryPayload::Normal(request) => {
                    if self.shard_id == DEFAULT_SHARD
                        && request.db_path.is_empty()
                        && request.raw_message.first() == Some(&SHARD_CONFIG_TAG)
                    {
                        match router::decode_shard_config(&request.raw_message) {
                            Some(command) => (None, Some(command), None),
                            None => {
                                warn!(
                                    log_index = log_id.index,
                                    "Dropping malformed shard config entry"
                                );
                                (None, None, None)
                            }
                        }
                    } else {
                        (None, None, Some((log_id.index, request)))
                    }
                }
                EntryPayload::Blank => (None, None, None),
            };

            {
                let mut inner = self.inner.lock();
                inner.last_applied = Some(log_id);
                if let Some(membership) = membership {
                    inner.last_membership = membership;
                }
                if let Some(command) = &command {
                    inner.apply_command(command);
                }
            }

            if let Some(command) = command {
                pending_commands.push(command);
                non_write_indices.push(log_id.index);
            } else if let Some((idx, request)) = write {
                pending_writes.push(CommittedWrite {
                    log_index: idx,
                    origin_node_id: request.origin_node_id,
                    db_path: request.db_path,
                    raw_message: request.raw_message,
                });
            } else {
                non_write_indices.push(log_id.index);
            }

            if is_application {
                metrics::ENTRIES_APPLIED.inc();
            }

            last_index = Some(log_id.index);
            count += 1;
        }

        // 先落盘再投递（修复 P0-2：保证持久化先于外部副作用）
        self.persist().map_err(storage_io_err)?;

        if let Some(tx) = &shard_config_tx {
            for command in pending_commands {
                let _ = tx.send(command).await;
            }
        }

        // 非业务写条目（Blank / Membership / 配置命令）无需投递上游，若无前置未投递写则合并推进一次 delivered 水位
        let initial_delivered = self.last_delivered_index();
        let mut delivered_target = initial_delivered;
        for idx in non_write_indices {
            if delivered_target + 1 == idx {
                delivered_target = idx;
            }
        }
        if delivered_target > initial_delivered {
            self.mark_delivered(delivered_target);
        }

        if let Some(tx) = &forwarder {
            for write in pending_writes {
                debug!(
                    log_index = write.log_index,
                    msg_len = write.raw_message.len(),
                    db_path = %write.db_path,
                    "Forwarding committed reducer call to SpacetimeDB"
                );
                let _ = tx.send(write).await;
            }
        } else if let Some(last_write) = pending_writes.last() {
            // 未挂载转发器时（如纯状态机单测），直接推进 delivered 水位
            self.mark_delivered(last_write.log_index);
        }

        if let Some(index) = last_index {
            self.applied.send_replace(index);
        }

        for _ in 0..count {
            on_applied(ReducerCallResponse { success: true });
        }

        Ok(())
    }
}

impl RaftStateMachine<C> for StateMachineStore {
    type SnapshotBuilder = Self;
    type SnapshotData = Cursor<Vec<u8>>;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<RaftLogId>, RaftStoredMembership), io::Error> {
        let inner = self.inner.lock();
        Ok((inner.last_applied, inner.last_membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<C>, io::Error>> + Unpin + OptionalSend,
    {
        let mut batch_entries = Vec::new();
        let mut responders = Vec::new();
        while let Some(item) = entries.next().await {
            let (entry, responder) = item?;
            batch_entries.push(entry);
            responders.push(responder);
        }

        let mut resp_iter = responders.into_iter();
        self.apply_entries_with_hook(batch_entries, |resp| {
            if let Some(Some(tx)) = resp_iter.next() {
                tx.send(resp);
            }
        })
        .await
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn install_snapshot(
        &mut self,
        meta: &RaftSnapshotMeta,
        snapshot: Self::SnapshotData,
    ) -> Result<(), io::Error> {
        let bytes = snapshot.into_inner();
        let data: SnapshotData = bitcode::decode(&bytes).map_err(storage_io_err)?;

        {
            let mut inner = self.inner.lock();
            inner.last_applied = meta.last_log_id;
            inner.last_membership = meta.last_membership.clone();
            inner.shard_routes = data.shard_routes;
            inner.active_shards = data.active_shards;
        }

        let shard_config_tx = self.shard_config_tx.lock().clone();
        if let Some(tx) = shard_config_tx {
            let (active_shards, shard_routes) = self.persisted_topology();
            for shard_id in active_shards {
                let _ = tx.send(ShardConfigCommand::CreateShard { shard_id }).await;
            }
            for (module_name, shard_id) in shard_routes {
                let _ = tx
                    .send(ShardConfigCommand::AddRoute {
                        module_name,
                        shard_id,
                    })
                    .await;
            }
        }

        self.persist().map_err(storage_io_err)?;
        if let Some(log_id) = meta.last_log_id {
            self.applied.send_replace(log_id.index);
            self.mark_delivered(log_id.index);
        }

        *self.cached_snapshot.lock() = Some(Snapshot {
            meta: meta.clone(),
            snapshot: Cursor::new(bytes),
        });

        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<RaftSnapshot>, io::Error> {
        if let Some(snapshot) = self.cached_snapshot.lock().as_ref() {
            return Ok(Some(Snapshot {
                meta: snapshot.meta.clone(),
                snapshot: Cursor::new(snapshot.snapshot.get_ref().clone()),
            }));
        }

        if self.inner.lock().last_applied.is_none() {
            return Ok(None);
        }
        Ok(Some(self.build_snapshot().await?))
    }
}

impl RaftSnapshotBuilder<C> for StateMachineStore {
    type SnapshotData = Cursor<Vec<u8>>;

    async fn build_snapshot(&mut self) -> Result<RaftSnapshot, io::Error> {
        let (encoded, meta) = {
            let inner = self.inner.lock();
            let encoded = bitcode::encode(&inner.snapshot_data());
            let meta = SnapshotMeta {
                last_log_id: inner.last_applied,
                last_membership: inner.last_membership.clone(),
            };
            (encoded, meta)
        };

        *self.cached_snapshot.lock() = Some(Snapshot {
            meta: meta.clone(),
            snapshot: Cursor::new(encoded.clone()),
        });

        Ok(Snapshot {
            meta,
            snapshot: Cursor::new(encoded),
        })
    }
}
