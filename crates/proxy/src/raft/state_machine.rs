use std::{
    collections::BTreeMap,
    fs::create_dir_all,
    io::{self, Cursor},
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use bitcode::{Decode, Encode};
use compio::runtime::spawn;
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
use tracing::{debug, error, warn};

use crate::{
    metrics,
    raft::{
        APPLY_TIMEOUT, CommittedWrite,
        codec::{WireLogId, WireStoredMembership},
        error::{StoreError, storage_io_err},
        persistent_log_store::{DB_DIR, shard_dir},
        types::{
            RaftEntry, RaftLogId, RaftSnapshot, RaftSnapshotMeta, RaftStoredMembership,
            ReducerCallResponse, TypeConfig,
        },
        wait_watermark,
    },
    router::{self, ShardConfigCommand, ShardId, is_shard_config_entry},
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
/// 内部通过单层 `Arc<StateMachineShared>` 聚合全部共享状态，使 `Clone` 仅需单次原子引用计数递增。
#[derive(Clone)]
pub struct StateMachineStore {
    shared: Arc<StateMachineShared>,
}

struct StateMachineShared {
    shard_id: ShardId,
    inner: Mutex<StateMachineInner>,
    /// 把已提交的写投递给转发任务。
    forwarder: Mutex<Option<WriteSender>>,
    /// 分片配置命令通道，仅分片 0 使用。
    shard_config_tx: Mutex<Option<ConfigSender>>,
    /// 状态机元数据 Keyspace（与 `PersistentLogStore` 共享同一个 `fjall::Database`）。
    state_ks: Keyspace,
    /// 已应用到本节点的最新日志 index，供写入方等待「写已落地」。
    applied: watch::Sender<u64>,
    /// 已投递到上游 SpacetimeDB 的最新日志 index（用于保序与崩溃重放）。
    delivered: watch::Sender<u64>,
}

struct StateMachineInner {
    last_applied: Option<RaftLogId>,
    last_membership: RaftStoredMembership,
    shard_routes: HashMap<String, ShardId>,
    active_shards: Vec<ShardId>,
    /// 已应用（或启动重放已入队）但尚未完成上游投递的客户端写日志索引及其入队时间。
    /// 连续已投递水位由 `in_flight_writes.first_key_value().map_or(last_applied, |(&idx, _)| idx - 1)` 唯一确定，禁止跨空洞跳变。
    in_flight_writes: BTreeMap<u64, Instant>,
    /// 标记本地 SpacetimeDB 是否因安装了跨越未投递日志区间的快照而落后于集群状态（修复 R4/Point-3）。
    db_stale: bool,
    /// 缓存最近一次快照；当状态推进时自动失效，避免返回陈旧快照。
    cached_snapshot: Option<RaftSnapshot>,
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

    /// 计算当前连续已投递水位上界（受限于最早未完成写条目 `first_pending - 1` 与 `last_applied`，禁止跨空洞或超前跳变）。
    #[inline]
    fn contiguous_delivered(&self, fallback: u64) -> u64 {
        match self.in_flight_writes.first_key_value() {
            Some((&first_pending, _)) => first_pending.saturating_sub(1),
            None => self.last_applied.map_or(fallback, |id| id.index),
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
            Some(value) => match <[u8; 8]>::try_from(value.as_ref()) {
                Ok(bytes) => u64::from_le_bytes(bytes),
                Err(_) => bitcode::decode::<u64>(&value)?,
            },
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
            shared: Arc::new(StateMachineShared {
                shard_id,
                inner: Mutex::new(StateMachineInner {
                    last_applied,
                    last_membership,
                    shard_routes: meta.shard_routes,
                    active_shards: meta.active_shards,
                    in_flight_writes: BTreeMap::new(),
                    db_stale: false,
                    cached_snapshot: None,
                }),
                forwarder: Mutex::new(None),
                shard_config_tx: Mutex::new(None),
                state_ks,
                applied: watch::channel(applied).0,
                delivered: watch::channel(delivered).0,
            }),
        })
    }

    /// 启动时校验 `last_applied >= last_purged`，防止状态机落后于已清理日志（修复 P2-10）。
    pub fn align_with_purged(&self, last_purged: Option<RaftLogId>) -> Result<(), StoreError> {
        let Some(purged) = last_purged else {
            return Ok(());
        };
        let delivered_target = {
            let mut inner = self.shared.inner.lock();
            if inner.last_applied.map_or(0, |id| id.index) < purged.index {
                inner.last_applied = Some(purged);
                inner.in_flight_writes.retain(|&idx, _| idx > purged.index);
                inner.cached_snapshot = None;
                Some(inner.contiguous_delivered(purged.index))
            } else {
                None
            }
        };
        if let Some(target) = delivered_target {
            self.persist()?;
            self.shared.applied.send_replace(purged.index);
            self.commit_delivered_watermark(target);
        }
        Ok(())
    }

    /// 设置已提交写的投递通道。
    pub fn set_forwarder(&self, tx: WriteSender) {
        *self.shared.forwarder.lock() = Some(tx);
    }

    /// 设置分片配置命令通道，仅分片 0 的状态机需要。
    pub fn set_shard_config_tx(&self, tx: ConfigSender) {
        *self.shared.shard_config_tx.lock() = Some(tx);
    }

    /// 订阅「已应用的日志 index」，用于等待某条写真正落地。
    pub fn applied_rx(&self) -> watch::Receiver<u64> {
        self.shared.applied.subscribe()
    }

    /// 当前已应用的最大日志 index。
    pub fn last_applied_index(&self) -> u64 {
        *self.shared.applied.borrow()
    }

    /// 当前已投递到上游的最大日志 index。
    pub fn last_delivered_index(&self) -> u64 {
        *self.shared.delivered.borrow()
    }

    /// 订阅「已投递到上游的日志 index」，用于跨连接全局保序（修复 P0-1）。
    pub fn delivered_rx(&self) -> watch::Receiver<u64> {
        self.shared.delivered.subscribe()
    }

    /// 本地 SpacetimeDB 是否因快照跳过未投递日志而处于需重建状态。
    pub fn is_db_stale(&self) -> bool {
        self.shared.inner.lock().db_stale
    }

    /// 登记一条启动重放期间在途的写日志索引（在写投递完成前阻止后续非写条目跨洞推进 `delivered` 水位，修复 P0-1）。
    pub fn register_in_flight_write(&self, index: u64) {
        self.shared
            .inner
            .lock()
            .in_flight_writes
            .insert(index, Instant::now());
    }

    /// 提取超过 `max_age` 仍未完成投递的已应用写日志索引，并刷新其时间戳以防重复触发（供周期性自愈补投使用，修复 R2）。
    pub fn take_stalled_writes(&self, max_age: Duration) -> Vec<u64> {
        let now = Instant::now();
        let mut inner = self.shared.inner.lock();
        let applied = inner.last_applied.map_or(0, |id| id.index);
        let mut stalled = Vec::new();
        for (&idx, ts) in &mut inner.in_flight_writes {
            if idx > applied {
                break;
            }
            if now.duration_since(*ts) >= max_age {
                *ts = now;
                stalled.push(idx);
            }
        }
        stalled
    }

    /// 按当前最早未完成写条目推进连续 `last_delivered` 水位（不跨空洞）。
    pub fn advance_delivered(&self) {
        let target = self.shared.inner.lock().contiguous_delivered(0);
        self.commit_delivered_watermark(target);
    }

    /// 标记指定写条目已投递并推进连续 `last_delivered` 水位（禁止跨未投递空洞跳变，自动跨越后续已应用非写条目，修复 P0-1/P0-2）。
    pub fn mark_delivered(&self, index: u64) {
        let target = {
            let mut inner = self.shared.inner.lock();
            inner.in_flight_writes.remove(&index);
            inner.contiguous_delivered(index)
        };
        self.commit_delivered_watermark(target);
    }

    /// 单调递增持久化并发布 `last_delivered` 水位（在 `send_if_modified` 锁内先写入共享 `fjall` 日志再推进水位；写盘失败则中止推进，修复 R1b/Point-7）。
    fn commit_delivered_watermark(&self, target: u64) {
        self.shared.delivered.send_if_modified(|cur| {
            if target > *cur {
                if let Err(e) = self
                    .shared
                    .state_ks
                    .insert(META_DELIVERED, target.to_le_bytes())
                {
                    error!(
                        shard_id = self.shared.shard_id,
                        target,
                        error = %e,
                        "Failed to persist delivered watermark"
                    );
                    return false;
                }
                *cur = target;
                true
            } else {
                false
            }
        });
    }

    /// 当源节点客户端断连或直投失败时，等待该日志条目在本节点应用后将其（置 `origin_node_id = 0`）交还转发器兜底投递（修复 P0-2/R6）。
    pub fn requeue_write(&self, mut write: CommittedWrite) {
        write.origin_node_id = 0;
        let forwarder = self.shared.forwarder.lock().clone();
        if let Some(tx) = forwarder {
            let shard_id = self.shared.shard_id;
            let idx = write.log_index;
            let applied_rx = (self.last_applied_index() < idx).then(|| self.applied_rx());
            spawn(async move {
                if let Some(rx) = applied_rx
                    && !wait_watermark(rx, idx, APPLY_TIMEOUT).await
                {
                    return;
                }
                if tx.try_send(write).is_err() {
                    metrics::DELIVERY_BACKPRESSURE.inc();
                    warn!(
                        shard_id,
                        log_index = idx,
                        "Forwarder channel full during requeue; write will be reclaimed from log"
                    );
                }
            })
            .detach();
        }
    }

    /// 读取已持久化的分片配置（激活分片列表与路由表）。
    pub fn persisted_topology(&self) -> (Vec<ShardId>, HashMap<String, ShardId>) {
        let inner = self.shared.inner.lock();
        (inner.active_shards.clone(), inner.shard_routes.clone())
    }

    /// 把当前记账状态落盘。先取快照再写盘，避免持锁做 IO。
    fn persist(&self) -> Result<(), StoreError> {
        let data = self.shared.inner.lock().snapshot_data();
        let bytes = bitcode::encode(&data);
        self.shared.state_ks.insert(META_KEY, bytes)?;
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
        let forwarder = self.shared.forwarder.lock().clone();
        let shard_config_tx = self.shared.shard_config_tx.lock().clone();
        let shard_id = self.shared.shard_id;
        let mut pending_commands = Vec::new();
        let mut pending_writes = Vec::new();
        let mut has_non_write = false;
        let mut last_log_id = None;
        let mut last_membership = None;
        let mut app_entries = 0u64;
        let mut count = 0usize;

        for entry in entries {
            let log_id = entry.log_id;
            last_log_id = Some(log_id);
            count += 1;

            // 先分类，反序列化与分支判断都不持锁
            // 仅当分片 0 且 db_path 为空且首字节为 SHARD_CONFIG_TAG 时，才按分片配置命令解析（修复 P2-14）
            match entry.payload {
                EntryPayload::Membership(membership) => {
                    last_membership = Some(RaftStoredMembership::new(Some(log_id), membership));
                    has_non_write = true;
                }
                EntryPayload::Normal(request) => {
                    app_entries += 1;
                    if is_shard_config_entry(shard_id, &request.db_path, &request.raw_message) {
                        match router::decode_shard_config(&request.raw_message) {
                            Some(command) => {
                                pending_commands.push(command);
                            }
                            None => {
                                warn!(
                                    log_index = log_id.index,
                                    "Dropping malformed shard config entry"
                                );
                            }
                        }
                        has_non_write = true;
                    } else {
                        pending_writes.push(CommittedWrite {
                            log_index: log_id.index,
                            origin_node_id: request.origin_node_id,
                            db_path: request.db_path,
                            raw_message: request.raw_message,
                        });
                    }
                }
                EntryPayload::Blank => {
                    has_non_write = true;
                }
            }
        }

        // 单次加锁合并更新整批日志的 last_applied、last_membership、配置命令、在途写集合与连续 delivered 水位上界
        let (snapshot_data, delivered_target) = {
            let mut inner = self.shared.inner.lock();
            if let Some(log_id) = last_log_id {
                inner.last_applied = Some(log_id);
                inner.cached_snapshot = None;
            }
            if let Some(membership) = last_membership {
                inner.last_membership = membership;
            }
            for command in &pending_commands {
                inner.apply_command(command);
            }
            let target = if forwarder.is_some() {
                let now = Instant::now();
                for write in &pending_writes {
                    inner.in_flight_writes.insert(write.log_index, now);
                }
                has_non_write.then(|| inner.contiguous_delivered(0))
            } else {
                last_log_id.map(|id| id.index)
            };
            (inner.snapshot_data(), target)
        };

        if app_entries > 0 {
            metrics::ENTRIES_APPLIED.inc_by(app_entries);
        }

        // 先落盘再投递（修复 P0-2：保证持久化先于外部副作用）
        let bytes = bitcode::encode(&snapshot_data);
        self.shared
            .state_ks
            .insert(META_KEY, bytes)
            .map_err(storage_io_err)?;

        if let Some(tx) = &shard_config_tx {
            for command in pending_commands {
                let _ = tx.send(command).await;
            }
        }

        if let Some(target) = delivered_target {
            self.commit_delivered_watermark(target);
        }

        if let Some(tx) = &forwarder {
            for write in pending_writes {
                let log_index = write.log_index;
                debug!(
                    log_index,
                    msg_len = write.raw_message.len(),
                    db_path = %write.db_path,
                    "Forwarding committed reducer call to SpacetimeDB"
                );
                // 使用非阻塞 try_send 防止下游 SpacetimeDB 故障阻塞 Raft apply（满载溢出的写已记入 in_flight_writes 与 fjall 日志，由周期性 reclaimer 补投，修复 R6）
                if tx.try_send(write).is_err() {
                    metrics::DELIVERY_BACKPRESSURE.inc();
                    warn!(
                        shard_id = self.shared.shard_id,
                        log_index,
                        "Forwarder channel full during apply; write will be reclaimed from log"
                    );
                }
            }
        }

        if let Some(log_id) = last_log_id {
            self.shared.applied.send_replace(log_id.index);
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
        let inner = self.shared.inner.lock();
        Ok((inner.last_applied, inner.last_membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<C>, io::Error>> + Unpin + OptionalSend,
    {
        let cap = entries.size_hint().0;
        let mut batch_entries = Vec::with_capacity(cap);
        let mut responders = Vec::with_capacity(cap);
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

        let shard_config_tx = self.shared.shard_config_tx.lock().clone();
        let replay = shard_config_tx
            .as_ref()
            .map(|_| (data.active_shards.clone(), data.shard_routes.clone()));

        let prev_delivered = self.last_delivered_index();
        let snapshot_index = meta.last_log_id.map_or(0, |id| id.index);
        let delivered_target = {
            let mut inner = self.shared.inner.lock();
            if prev_delivered < snapshot_index {
                error!(
                    shard_id = self.shared.shard_id,
                    snapshot_index,
                    delivered = prev_delivered,
                    "Local SpacetimeDB is behind the installed snapshot and cannot be repaired from the log"
                );
                if !inner.db_stale {
                    inner.db_stale = true;
                    metrics::DB_STALE_SHARDS.inc();
                }
            }
            inner.last_applied = meta.last_log_id;
            inner.last_membership = meta.last_membership.clone();
            inner.shard_routes = data.shard_routes;
            inner.active_shards = data.active_shards;
            let target = if let Some(log_id) = meta.last_log_id {
                inner.in_flight_writes.retain(|&idx, _| idx > log_id.index);
                Some(inner.contiguous_delivered(log_id.index))
            } else {
                inner.in_flight_writes.clear();
                None
            };
            inner.cached_snapshot = Some(Snapshot {
                meta: meta.clone(),
                snapshot: Cursor::new(bytes),
            });
            target
        };

        if let (Some(tx), Some((active_shards, shard_routes))) = (shard_config_tx, replay) {
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
        if let (Some(log_id), Some(target)) = (meta.last_log_id, delivered_target) {
            self.shared.applied.send_replace(log_id.index);
            self.commit_delivered_watermark(target);
        }

        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<RaftSnapshot>, io::Error> {
        {
            let inner = self.shared.inner.lock();
            if let Some(snapshot) = inner.cached_snapshot.as_ref() {
                return Ok(Some(Snapshot {
                    meta: snapshot.meta.clone(),
                    snapshot: Cursor::new(snapshot.snapshot.get_ref().clone()),
                }));
            }
            if inner.last_applied.is_none() {
                return Ok(None);
            }
        }
        Ok(Some(self.build_snapshot().await?))
    }
}

impl RaftSnapshotBuilder<C> for StateMachineStore {
    type SnapshotData = Cursor<Vec<u8>>;

    async fn build_snapshot(&mut self) -> Result<RaftSnapshot, io::Error> {
        let mut inner = self.shared.inner.lock();
        let encoded = bitcode::encode(&inner.snapshot_data());
        let meta = SnapshotMeta {
            last_log_id: inner.last_applied,
            last_membership: inner.last_membership.clone(),
        };
        inner.cached_snapshot = Some(Snapshot {
            meta: meta.clone(),
            snapshot: Cursor::new(encoded.clone()),
        });

        Ok(Snapshot {
            meta,
            snapshot: Cursor::new(encoded),
        })
    }
}
