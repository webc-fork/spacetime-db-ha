use std::{
    collections::{BTreeMap, BTreeSet},
    fs::remove_dir_all,
    path::Path,
    rc::Rc,
    result,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use bitcode::{__private::Encoder, Encode};
use bytes::Bytes;
use compio::{
    runtime::spawn,
    time::{interval, timeout},
};
use crossfire::{
    AsyncRx, MAsyncTx,
    mpsc::{Array, bounded_async},
};
use futures::{
    FutureExt,
    channel::oneshot,
    future::{join_all, try_join_all},
    select,
};
use gxhash::{HashMap, HashMapExt};
use openraft::{
    BasicNode, Config, Membership, Raft, ReadPolicy,
    async_runtime::watch::WatchReceiver,
    entry::EntryPayload,
    error::{ClientWriteError, RaftError},
};
use openraft_zenoh::NetworkFactory;
use parking_lot::RwLock;
use tracing::{info, warn};

use crate::{
    config::NodeConfig,
    metrics,
    raft::{
        codec::ReducerCallRequestEncoder,
        persistent_log_store::{PersistentLogStore, shard_dir},
        state_machine::{
            CONFIG_CHANNEL_CAPACITY, ConfigSender, FORWARDER_CHANNEL_CAPACITY, StateMachineStore,
            config_channel, write_channel,
        },
        types::{RaftLogId, ReducerCallRequest, TypeConfig, WriteAck, leader_term},
    },
    router::{DEFAULT_SHARD, ShardConfigCommand, ShardId, ShardRouter, is_shard_config_entry},
    transport::{
        CLUSTER_PREFIX, SUFFIX_WRITE, SUFFIX_WRITE_BATCH, ZenohClientPool, mid_u64_key,
        normalize_addr, watch,
    },
};

pub mod codec;
pub mod error;
pub mod forwarder;
pub mod persistent_log_store;
pub mod state_machine;
pub mod types;

/// 单分片 Raft 实例类型别名（`openraft 0.10` 带状态机类型参数）。
pub type ShardRaft = Raft<TypeConfig, StateMachineStore>;

/// Raft 集群名。
const CLUSTER_NAME: &str = "spacetime-db-ha";
/// 节点间心跳间隔（毫秒）。
const HEARTBEAT_INTERVAL_MS: u64 = 200;
/// 选举超时下界（毫秒）。
const ELECTION_TIMEOUT_MIN_MS: u64 = 1_000;
/// 选举超时上界（毫秒）。
const ELECTION_TIMEOUT_MAX_MS: u64 = 2_000;
/// 等待本节点把写应用到状态机的超时。
pub(crate) const APPLY_TIMEOUT: Duration = Duration::from_secs(30);
/// 等待前置日志投递完成的超时上限（需覆盖自愈与上游重连窗口，超时返回错误严禁乱序放行，修复 Point-2）。
pub(crate) const DELIVER_ORDER_TIMEOUT: Duration = Duration::from_secs(30);
/// 读请求因果同步栅栏等待上限。
const READ_BARRIER_TIMEOUT: Duration = Duration::from_millis(200);
/// 线性一致 Leader 身份确认（`ReadIndex`）超时上限（防止网络分区时健康检查悬挂，修复 P1-3）。
const LINEARIZABLE_CHECK_TIMEOUT: Duration = Duration::from_secs(1);
/// 未初始化分片协调重试间隔（修复 P1-7）。
const SHARD_RECONCILE_INTERVAL: Duration = Duration::from_millis(500);
/// 在途写投递停滞阈值：超过该时长仍未完成投递的已应用写将由协调循环自动从 `fjall` 日志补投（修复 R2）。
const STALLED_DELIVERY_TIMEOUT: Duration = Duration::from_secs(2);
/// 单分片微批写聚合通道容量。
const WRITE_BATCH_CHANNEL_CAPACITY: usize = 4096;
/// 单次微批合并提案的最大写条目数（摊薄 Raft 共识落盘与跨节点 RPC 开销）。
pub const MAX_WRITE_BATCH_SIZE: usize = 64;

/// 解析单条 `id=host:port` 节点规格并规范化连接地址（一处定义，供代理与 CLI 共享）。
pub fn parse_peer_spec(spec: &str) -> Result<(u64, String)> {
    let (id_str, addr) = spec
        .split_once('=')
        .with_context(|| format!("Invalid node format '{spec}', expected 'id=host:port'"))?;
    let id = id_str
        .parse::<u64>()
        .with_context(|| format!("Invalid node ID '{id_str}'"))?;
    Ok((id, normalize_addr(addr, true)))
}

/// 解析 `id=host:port` 形式的对端列表，单趟遍历完成，非法项记日志后跳过。
pub fn parse_peers(peers: &[String]) -> BTreeMap<u64, BasicNode> {
    peers
        .iter()
        .filter_map(|peer| match parse_peer_spec(peer) {
            Ok((id, addr)) => Some((id, BasicNode { addr })),
            Err(e) => {
                warn!(peer = %peer, error = %e, "Skipping invalid peer specification");
                None
            }
        })
        .collect()
}

/// 等待单调递增水位通道达到 `target`（一处定义，供 `wait_applied`、`wait_delivered` 与 `requeue_write` 共用）。
pub(crate) async fn wait_watermark(
    mut rx: watch::Receiver<u64>,
    target: u64,
    timeout_dur: Duration,
) -> bool {
    if *rx.borrow_and_update() >= target {
        return true;
    }
    timeout(timeout_dur, async {
        while rx.changed().await.is_ok() {
            if *rx.borrow_and_update() >= target {
                return true;
            }
        }
        false
    })
    .await
    .unwrap_or(false)
}

/// 已提交、待投递给本地 SpacetimeDB 的写请求。
#[derive(Debug, Clone)]
pub struct CommittedWrite {
    /// 该写对应的 Raft 日志索引（用于投递保序与 `last_delivered` 水位更新）。
    pub log_index: u64,
    /// 提议该写的节点；本节点发起的写由连接自己直投，转发任务跳过。
    pub origin_node_id: u64,
    /// 目标数据库路径，决定投递到哪个上游连接。
    pub db_path: String,
    pub raw_message: Bytes,
}

/// 待合并提案的单条写作业。
struct BatchWriteJob {
    request: ReducerCallRequest,
    reply_tx: oneshot::Sender<Result<RaftLogId>>,
}

impl Encoder<BatchWriteJob> for ReducerCallRequestEncoder {
    #[inline(always)]
    fn encode(&mut self, job: &BatchWriteJob) {
        self.encode(&job.request);
    }
}

impl Encode for BatchWriteJob {
    type Encoder = ReducerCallRequestEncoder;
}

type BatchWriteSender = MAsyncTx<Array<BatchWriteJob>>;
type BatchWriteReceiver = AsyncRx<Array<BatchWriteJob>>;

/// 单个分片的运行时状态。
struct ShardState {
    raft: ShardRaft,
    log_store: PersistentLogStore,
    state_machine: StateMachineStore,
    write_batch_tx: BatchWriteSender,
    forwarder_shutdown: watch::Sender<bool>,
}

/// 分片池：一分片一 Raft 组。
///
/// 分片 0 始终存在且为默认分片；其余分片由分片 0 日志里的配置命令动态创建或删除。
/// 所有节点参与全部成员相同的分片。
pub struct RaftPool {
    pub node_config: Arc<NodeConfig>,
    pub peers: Arc<BTreeMap<u64, BasicNode>>,
    shards: RwLock<HashMap<ShardId, ShardState>>,
    router: RwLock<ShardRouter>,
    /// 节点间 Zenoh 明文 QUIC 连接池。
    zenoh_pool: ZenohClientPool,
}

impl RaftPool {
    /// 创建分片池并完成分片 0 的初始化及已持久化拓扑恢复。返回 `Rc` 供单线程 `compio` 后台任务共享。
    pub async fn new(config: &NodeConfig, shutdown_rx: watch::Receiver<bool>) -> Result<Rc<Self>> {
        let node_config = Arc::new(config.clone());
        let peers = Arc::new(parse_peers(&config.peers));
        let zenoh_pool = ZenohClientPool::new();

        let (shard_config_tx, shard_config_rx) = config_channel(CONFIG_CHANNEL_CAPACITY);
        let shard0 = Self::create_shard(
            Arc::clone(&node_config),
            Arc::clone(&peers),
            zenoh_pool.clone(),
            DEFAULT_SHARD,
            Some(shard_config_tx),
        )
        .await?;

        let (persisted_shards, persisted_routes) = shard0.state_machine.persisted_topology();

        let mut shards = HashMap::with_capacity(1 + persisted_shards.len());
        shards.insert(DEFAULT_SHARD, shard0);

        let mut initial_router = ShardRouter::new();
        for (module_name, sid) in persisted_routes {
            initial_router.add_route(module_name, sid);
        }

        info!(
            node_id = config.node_id,
            peer_count = peers.len(),
            "RaftPool initialized with shard 0 over Zenoh QUIC + Compio"
        );

        let pool = Rc::new(Self {
            node_config,
            peers,
            shards: RwLock::new(shards),
            router: RwLock::new(initial_router),
            zenoh_pool,
        });

        // 恢复重启前已激活的非 0 分片
        for sid in persisted_shards {
            if sid != DEFAULT_SHARD {
                pool.materialize_shard(sid).await;
            }
        }

        // 分片配置监听：把分片 0 提交的配置命令落地成实际分片与路由；收到全局停机信号时统一通知全部分片后台协程退出
        let pool_ref = Rc::clone(&pool);
        let mut listener_shutdown = shutdown_rx.clone();
        spawn(async move {
            loop {
                select! {
                    cmd = shard_config_rx.recv().fuse() => {
                        let Ok(cmd) = cmd else { return };
                        pool_ref.handle_shard_config(cmd).await;
                    }
                    _ = listener_shutdown.changed().fuse() => {
                        for shard in pool_ref.shards.read().values() {
                            let _ = shard.forwarder_shutdown.send(true);
                        }
                        return;
                    }
                }
            }
        })
        .detach();

        // 修复 P1-7 & R2：启动周期性幂等协调循环，确保新建分片最终完成初始化且停滞在途写自动自愈补投
        let reconcile_pool = Rc::clone(&pool);
        let mut reconcile_shutdown = shutdown_rx;
        spawn(async move {
            let mut ticker = interval(SHARD_RECONCILE_INTERVAL);
            loop {
                select! {
                    _ = ticker.tick().fuse() => {
                        reconcile_pool.reclaim_stalled_deliveries(STALLED_DELIVERY_TIMEOUT);
                        reconcile_pool.reconcile_uninitialized_shards().await;
                    }
                    _ = reconcile_shutdown.changed().fuse() => return,
                }
            }
        })
        .detach();

        Ok(pool)
    }

    /// 创建分片的 Raft 实例、状态机、微批提案队列与转发通道，并重放崩溃前未完成投递的已提交写 `(last_delivered, last_applied]`。
    async fn create_shard(
        config: Arc<NodeConfig>,
        peers: Arc<BTreeMap<u64, BasicNode>>,
        zenoh_pool: ZenohClientPool,
        shard_id: ShardId,
        shard_config_tx: Option<ConfigSender>,
    ) -> Result<ShardState> {
        let raft_config = Config {
            cluster_name: CLUSTER_NAME.to_owned(),
            heartbeat_interval: HEARTBEAT_INTERVAL_MS,
            election_timeout_min: ELECTION_TIMEOUT_MIN_MS,
            election_timeout_max: ELECTION_TIMEOUT_MAX_MS,
            ..Default::default()
        }
        .validate()
        .context("invalid raft config")?;

        // 修复 P2-10 & P1-9：日志与状态机共享同一个 fjall::Database 实例，校验 last_purged 水位并绑定 delivered_rx 防止提前清理未投递日志
        let mut log_store = PersistentLogStore::new(Path::new(&config.data_dir), shard_id)?;
        let state_machine = StateMachineStore::open_with_db(log_store.db(), shard_id)?;
        state_machine.align_with_purged(log_store.last_purged_log_id()?)?;
        log_store.set_delivered_rx(state_machine.delivered_rx());

        if let Some(cfg_tx) = shard_config_tx {
            state_machine.set_shard_config_tx(cfg_tx);
        }

        let (forwarder_shutdown_tx, forwarder_shutdown_rx) = watch::channel(false);

        let (write_tx, write_rx) = write_channel(FORWARDER_CHANNEL_CAPACITY);
        forwarder::spawn_pooled(
            config.node_id,
            config.stdb_url.clone(),
            zenoh_pool.clone(),
            write_rx,
            Some(state_machine.clone()),
            forwarder_shutdown_rx.clone(),
        );

        // 修复 P0-1 & P0-2：启动时重放 (last_delivered, last_applied] 区间内尚未投递到 SpacetimeDB 的日志条目；
        // 先将待补投写登记入 in_flight_writes，防止重放区间内的后续非写条目跨空洞跳变推进 delivered 水位
        let delivered = state_machine.last_delivered_index();
        let applied = state_machine.last_applied_index();
        if delivered < applied {
            let entries = log_store.read_entries_range(delivered + 1, applied)?;
            for entry in entries {
                let idx = entry.log_id.index;
                if let EntryPayload::Normal(req) = entry.payload
                    && !is_shard_config_entry(shard_id, &req.db_path, &req.raw_message)
                {
                    state_machine.register_in_flight_write(idx);
                    // 重放时设 origin_node_id = 0 并使用非阻塞 try_send，避免积压超过通道容量且上游不可达时卡死节点启动（溢出部分由 reclaimer 自动从日志补投，修复 Point-4）
                    if write_tx
                        .try_send(CommittedWrite {
                            log_index: idx,
                            origin_node_id: 0,
                            db_path: req.db_path,
                            raw_message: req.raw_message,
                        })
                        .is_err()
                    {
                        metrics::DELIVERY_BACKPRESSURE.inc();
                    }
                }
            }
            state_machine.advance_delivered();
        }
        state_machine.set_forwarder(write_tx);

        let network = NetworkFactory::new(zenoh_pool.clone(), shard_id);
        let raft = Raft::new(
            config.node_id,
            Arc::new(raft_config),
            network,
            log_store.clone(),
            state_machine.clone(),
        )
        .await?;

        let (write_batch_tx, write_batch_rx) = bounded_async(WRITE_BATCH_CHANNEL_CAPACITY);
        ShardBatcherCtx::new(
            raft.clone(),
            state_machine.clone(),
            zenoh_pool,
            config,
            peers,
            shard_id,
        )
        .spawn(write_batch_rx, forwarder_shutdown_rx);

        info!(shard_id, "Raft shard initialized");

        Ok(ShardState {
            raft,
            log_store,
            state_machine,
            write_batch_tx,
            forwarder_shutdown: forwarder_shutdown_tx,
        })
    }

    /// 处理分片 0 提交的分片配置命令。
    async fn handle_shard_config(&self, cmd: ShardConfigCommand) {
        match cmd {
            ShardConfigCommand::CreateShard { shard_id } => self.materialize_shard(shard_id).await,
            ShardConfigCommand::RemoveShard { shard_id } => {
                self.dematerialize_shard(shard_id).await;
            }
            ShardConfigCommand::AddRoute {
                module_name,
                shard_id,
            } => {
                info!(module = %module_name, shard_id, "Adding route");
                self.router.write().add_route(module_name, shard_id);
            }
            ShardConfigCommand::RemoveRoute { module_name } => {
                info!(module = %module_name, "Removing route");
                self.router.write().remove_route(&module_name);
            }
        }
    }

    /// 真正拉起一个新分片；若本节点是分片 0 的 leader，顺带用同成员表初始化它。
    async fn materialize_shard(&self, shard_id: ShardId) {
        if shard_id == DEFAULT_SHARD || self.has_shard(shard_id) {
            return;
        }

        info!(shard_id, "Creating new shard from Raft config command");
        let shard = match Self::create_shard(
            Arc::clone(&self.node_config),
            Arc::clone(&self.peers),
            self.zenoh_pool.clone(),
            shard_id,
            None,
        )
        .await
        {
            Ok(shard) => shard,
            Err(e) => {
                warn!(shard_id, error = %e, "Failed to create shard state");
                return;
            }
        };

        self.shards.write().insert(shard_id, shard);
        self.try_initialize_shard(shard_id).await;
    }

    /// 删除指定分片，级联清理路由表、停止转发任务与 Raft 实例并移除磁盘目录（修复 P1-6）。
    async fn dematerialize_shard(&self, shard_id: ShardId) {
        if shard_id == DEFAULT_SHARD {
            warn!("Cannot remove default shard 0");
            return;
        }

        self.router.write().remove_routes_for_shard(shard_id);
        let removed = self.shards.write().remove(&shard_id);
        if let Some(shard) = removed {
            if shard.state_machine.is_db_stale() {
                metrics::DB_STALE_SHARDS.dec();
            }
            let _ = shard.forwarder_shutdown.send(true);
            let _ = shard.raft.shutdown().await;
            // 先释放 ShardState 持有的 fjall 数据库与 Keyspace 句柄，再清理磁盘目录（修复 P1-7）
            drop(shard);
            let dir = shard_dir(Path::new(&self.node_config.data_dir), shard_id);
            let _ = remove_dir_all(&dir);
            info!(shard_id, "Shard removed and resources cleaned up");
        }
    }

    /// 检查本节点在本地指标中是否为分片 0 的当前 leader（无网络开销，供分片协调循环快速判定，修复 P1-3）。
    fn is_local_default_leader(&self) -> bool {
        let Some(raft) = self.get_raft(DEFAULT_SHARD) else {
            return false;
        };
        raft.metrics().borrow_watched().current_leader == Some(self.node_config.node_id)
    }

    /// 若目标分片尚未初始化且本节点是分片 0 的 leader，则使用分片 0 成员表初始化该分片（修复 P1-3/P1-7）。
    async fn try_initialize_shard(&self, shard_id: ShardId) {
        let Some(raft) = self.get_raft(shard_id) else {
            return;
        };
        let is_uninitialized = raft
            .metrics()
            .borrow_watched()
            .membership_config
            .membership()
            .voter_ids()
            .next()
            .is_none();
        if !is_uninitialized || !self.is_local_default_leader() {
            return;
        }

        let members = self.current_members(DEFAULT_SHARD);
        if members.is_empty() {
            return;
        }
        match timeout(LINEARIZABLE_CHECK_TIMEOUT * 3, raft.initialize(members)).await {
            Ok(Ok(())) => info!(shard_id, "New shard initialized with existing membership"),
            Ok(Err(e)) => warn!(shard_id, error = %e, "Failed to initialize new shard"),
            Err(_) => warn!(shard_id, "Timed out initializing new shard"),
        }
    }

    /// 扫描所有分片中超过 `max_age` 仍未完成投递的在途写，从 `fjall` 日志读取并交由转发器自愈补投（按分片聚合日志防刷屏，修复 R2/Point-8）。
    pub fn reclaim_stalled_deliveries(&self, max_age: Duration) {
        let shards = self.shards.read();
        for (&shard_id, shard) in shards.iter() {
            let mut reclaimed_count = 0usize;
            for idx in shard.state_machine.take_stalled_writes(max_age) {
                if let Ok(Some(entry)) = shard.log_store.get_entry(idx)
                    && let EntryPayload::Normal(req) = entry.payload
                    && !is_shard_config_entry(shard_id, &req.db_path, &req.raw_message)
                {
                    reclaimed_count += 1;
                    shard.state_machine.requeue_write(CommittedWrite {
                        log_index: idx,
                        origin_node_id: 0,
                        db_path: req.db_path,
                        raw_message: req.raw_message,
                    });
                }
            }
            if reclaimed_count > 0 {
                warn!(
                    shard_id,
                    reclaimed_count, "Reclaimed stalled in-flight writes from log"
                );
            }
        }
    }

    /// 检查是否存在因安装快照跳过未投递日志而处于 stale 状态的分片（修复 Point-3）。
    pub fn has_stale_shard(&self) -> bool {
        self.shards
            .read()
            .values()
            .any(|shard| shard.state_machine.is_db_stale())
    }

    /// 定期检查并初始化所有尚未完成成员初始化的活跃分片（先做本地无锁指标过滤，消除周期性 `ReadIndex` 网络开销，修复 P1-3/P1-7）。
    async fn reconcile_uninitialized_shards(&self) {
        if !self.is_local_default_leader() {
            return;
        }
        for shard_id in self.list_shards() {
            if shard_id != DEFAULT_SHARD {
                self.try_initialize_shard(shard_id).await;
            }
        }
    }

    /// 解析指定节点的可路由广播地址（一处定义，供成员表提取与 leader 查询共用，修复 P1-8）。
    fn resolve_node_addr(
        &self,
        membership: &Membership<u64, BasicNode>,
        node_id: u64,
    ) -> Option<String> {
        resolve_peer_addr(&self.node_config, &self.peers, membership, node_id)
    }

    /// 获取节点级共享的 [`ZenohClientPool`]（供客户端会话、上游转发与节点间 RPC 复用同一底层 QUIC 会话）。
    #[inline]
    pub fn zenoh_pool(&self) -> &ZenohClientPool {
        &self.zenoh_pool
    }

    /// 从某分片的 Raft 指标读出当前选民集合（使用可路由广播地址，修复 P1-8）。
    fn current_members(&self, shard_id: ShardId) -> BTreeMap<u64, BasicNode> {
        let shards = self.shards.read();
        let Some(shard) = shards.get(&shard_id) else {
            return BTreeMap::new();
        };

        let metrics_rx = shard.raft.metrics();
        let metrics = metrics_rx.borrow_watched();
        let membership = metrics.membership_config.membership();
        membership
            .voter_ids()
            .filter_map(|id| {
                self.resolve_node_addr(membership, id)
                    .map(|addr| (id, BasicNode { addr }))
            })
            .collect()
    }

    /// 取指定分片的 Raft 实例。
    pub fn get_raft(&self, shard_id: ShardId) -> Option<ShardRaft> {
        self.shards
            .read()
            .get(&shard_id)
            .map(|shard| shard.raft.clone())
    }

    /// 取指定分片的微批写提案通道。
    fn get_write_batch_tx(&self, shard_id: ShardId) -> Option<BatchWriteSender> {
        self.shards
            .read()
            .get(&shard_id)
            .map(|shard| shard.write_batch_tx.clone())
    }

    /// 按模块名查分片。
    pub fn route_module(&self, module_name: &str) -> ShardId {
        self.router.read().route(module_name)
    }

    /// 通过指定分片的微批提案队列提交一次写（reducer 调用）；
    /// 同一事件循环窗口内的并发写将自动合并为单次 Raft 批量提交或单次跨节点批量转发。
    pub async fn propose_write(
        &self,
        shard_id: ShardId,
        raw_message: Bytes,
        db_path: &str,
    ) -> Result<RaftLogId> {
        let _timer = metrics::WRITE_LATENCY.start_timer();
        metrics::WRITES_TOTAL.inc();

        let batch_tx = self
            .get_write_batch_tx(shard_id)
            .ok_or_else(|| anyhow!("shard {shard_id} not found"))?;
        let (reply_tx, reply_rx) = oneshot::channel();
        let job = BatchWriteJob {
            request: ReducerCallRequest {
                raw_message,
                db_path: db_path.to_owned(),
                origin_node_id: self.node_config.node_id,
            },
            reply_tx,
        };
        batch_tx
            .send(job)
            .await
            .map_err(|_| anyhow!("shard {shard_id} write batcher closed"))?;
        timeout(APPLY_TIMEOUT, reply_rx)
            .await
            .map_err(|_| anyhow!("timed out waiting for write proposal on shard {shard_id}"))?
            .map_err(|_| anyhow!("shard {shard_id} write batcher dropped reply"))?
    }

    /// 批量提交多条写请求（一次性全部推入微批提案队列后再统一等待结果，保证同批次合并落盘与复制）。
    pub async fn propose_write_batch(
        &self,
        shard_id: ShardId,
        writes: Vec<(Bytes, String)>,
    ) -> Result<Vec<RaftLogId>> {
        if writes.is_empty() {
            return Ok(Vec::new());
        }
        let _timer = metrics::WRITE_LATENCY.start_timer();
        metrics::WRITES_TOTAL.inc_by(writes.len() as u64);

        let batch_tx = self
            .get_write_batch_tx(shard_id)
            .ok_or_else(|| anyhow!("shard {shard_id} not found"))?;
        let origin_node_id = self.node_config.node_id;
        let mut receivers = Vec::with_capacity(writes.len());

        for (raw_message, db_path) in writes {
            let (reply_tx, reply_rx) = oneshot::channel();
            batch_tx
                .send(BatchWriteJob {
                    request: ReducerCallRequest {
                        raw_message,
                        db_path,
                        origin_node_id,
                    },
                    reply_tx,
                })
                .await
                .map_err(|_| anyhow!("shard {shard_id} write batcher closed"))?;
            receivers.push(reply_rx);
        }

        let results = timeout(APPLY_TIMEOUT, try_join_all(receivers))
            .await
            .map_err(|_| anyhow!("timed out waiting for write batch proposal on shard {shard_id}"))?
            .map_err(|_| anyhow!("shard {shard_id} write batcher dropped reply"))?;
        results.into_iter().collect()
    }

    /// 等待本节点把日志应用到指定 index。
    pub async fn wait_applied(&self, shard_id: ShardId, index: u64) -> Result<()> {
        let applied = {
            let shards = self.shards.read();
            let shard = shards
                .get(&shard_id)
                .ok_or_else(|| anyhow!("shard {shard_id} not found"))?;
            if shard.state_machine.last_applied_index() >= index {
                return Ok(());
            }
            shard.state_machine.applied_rx()
        };

        if wait_watermark(applied, index, APPLY_TIMEOUT).await {
            return Ok(());
        }
        bail!("timed out waiting for log index {index} to be applied")
    }

    /// 等待指定日志索引之前的写投递完毕，实现同节点跨连接写的全局保序（超时返回错误而非静默乱序放行，修复 P0-1/Point-2）。
    pub async fn wait_delivered(&self, shard_id: ShardId, target_index: u64) -> Result<()> {
        if target_index == 0 {
            return Ok(());
        }
        let delivered = {
            let shards = self.shards.read();
            let Some(shard) = shards.get(&shard_id) else {
                return Ok(());
            };
            if shard.state_machine.last_delivered_index() >= target_index {
                return Ok(());
            }
            shard.state_machine.delivered_rx()
        };

        if wait_watermark(delivered, target_index, DELIVER_ORDER_TIMEOUT).await {
            return Ok(());
        }
        bail!("timed out waiting for prior write delivery watermark at index {target_index}")
    }

    /// 标记指定日志索引的写已投递到上游 SpacetimeDB（修复 P0-2）。
    pub fn mark_delivered(&self, shard_id: ShardId, index: u64) {
        let shards = self.shards.read();
        if let Some(shard) = shards.get(&shard_id) {
            shard.state_machine.mark_delivered(index);
        }
    }

    /// 当源节点客户端在提交后断开连接或直投失败时，将写交还转发器兜底投递（修复 P0-2）。
    pub fn requeue_write(&self, shard_id: ShardId, write: CommittedWrite) {
        let shards = self.shards.read();
        if let Some(shard) = shards.get(&shard_id) {
            shard.state_machine.requeue_write(write);
        }
    }

    /// 读请求前的因果同步栅栏：等待本节点状态机追平已知最后日志索引（修复 P0-4）。
    pub async fn wait_read_ready(&self, shard_id: ShardId) {
        let (applied, target) = {
            let shards = self.shards.read();
            let Some(shard) = shards.get(&shard_id) else {
                return;
            };
            let target = shard
                .raft
                .metrics()
                .borrow_watched()
                .last_log_index
                .unwrap_or(0);
            if target == 0 || shard.state_machine.last_applied_index() >= target {
                return;
            }
            (shard.state_machine.applied_rx(), target)
        };
        let _ = wait_watermark(applied, target, READ_BARRIER_TIMEOUT).await;
    }

    /// 本节点是否为指定分片的线性一致 leader（带超时保护，防止网络分区时无限挂起，修复 P1-3）。
    pub async fn is_leader(&self, shard_id: ShardId) -> bool {
        let Some(raft) = self.get_raft(shard_id) else {
            return false;
        };
        matches!(
            timeout(
                LINEARIZABLE_CHECK_TIMEOUT,
                raft.ensure_linearizable(ReadPolicy::ReadIndex),
            )
            .await,
            Ok(Ok(_))
        )
    }

    /// 指定分片当前 leader 的 ID 与可路由广播地址（修复 P1-8）。
    pub fn current_leader(&self, shard_id: ShardId) -> Option<(u64, String)> {
        let shards = self.shards.read();
        let shard = shards.get(&shard_id)?;
        let metrics_rx = shard.raft.metrics();
        let metrics = metrics_rx.borrow_watched();
        let leader_id = metrics.current_leader?;
        let addr = self.resolve_node_addr(metrics.membership_config.membership(), leader_id)?;
        Some((leader_id, addr))
    }

    /// 汇总全部分片的 Raft 与投递状态刷新指标（修复 P3-17）。
    pub fn update_metrics(&self) {
        let shards = self.shards.read();
        metrics::SHARDS_ACTIVE.set(shards.len() as i64);

        let total_lag: i64 = shards
            .values()
            .map(|shard| {
                let applied = shard.state_machine.last_applied_index();
                let delivered = shard.state_machine.last_delivered_index();
                applied.saturating_sub(delivered) as i64
            })
            .sum();
        metrics::DELIVERY_LAG.set(total_lag);

        let Some(shard0) = shards.get(&DEFAULT_SHARD) else {
            return;
        };

        let metrics_rx = shard0.raft.metrics();
        let raft_metrics = metrics_rx.borrow_watched();
        metrics::RAFT_TERM.set(leader_term(&raft_metrics.vote) as i64);
        let is_leader = raft_metrics.current_leader == Some(self.node_config.node_id);
        metrics::RAFT_IS_LEADER.set(i64::from(is_leader));
    }

    /// 判断指定分片是否存在（O(1) 哈希查询，无内存分配）。
    pub fn has_shard(&self, shard_id: ShardId) -> bool {
        self.shards.read().contains_key(&shard_id)
    }

    /// 当前所有分片 ID（升序）。
    pub fn list_shards(&self) -> Vec<ShardId> {
        let shards = self.shards.read();
        let mut ids: Vec<_> = shards.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    /// 全部路由。
    pub fn get_routes(&self) -> HashMap<String, ShardId> {
        self.router.read().routes().clone()
    }

    /// 把节点加入所有分片的 Raft 组。
    pub async fn add_node_all_shards(&self, node_id: u64, addr: String) -> Result<()> {
        let clean_addr = normalize_addr(&addr, true);
        for shard_id in self.list_shards() {
            let raft = self
                .get_raft(shard_id)
                .ok_or_else(|| anyhow!("shard {shard_id} not found"))?;

            raft.add_learner(
                node_id,
                BasicNode {
                    addr: clean_addr.clone(),
                },
                true,
            )
            .await
            .with_context(|| format!("add_learner on shard {shard_id}"))?;

            let voters: BTreeSet<u64> = raft
                .metrics()
                .borrow_watched()
                .membership_config
                .membership()
                .voter_ids()
                .chain([node_id])
                .collect();

            raft.change_membership(voters, false)
                .await
                .with_context(|| format!("change_membership on shard {shard_id}"))?;

            info!(shard_id, node_id, "Node added to shard");
        }
        Ok(())
    }

    /// 把节点从所有分片的 Raft 组中移除，并驱逐其缓存的 QUIC 会话。
    pub async fn remove_node_all_shards(&self, node_id: u64) -> Result<()> {
        let mut evicted_addr = None;
        for shard_id in self.list_shards() {
            let raft = self
                .get_raft(shard_id)
                .ok_or_else(|| anyhow!("shard {shard_id} not found"))?;

            let voters: BTreeSet<u64> = {
                let metrics_rx = raft.metrics();
                let metrics = metrics_rx.borrow_watched();
                let membership = metrics.membership_config.membership();
                if evicted_addr.is_none() && node_id != self.node_config.node_id {
                    evicted_addr = self.resolve_node_addr(membership, node_id);
                }
                membership.voter_ids().filter(|id| *id != node_id).collect()
            };

            raft.change_membership(voters, false)
                .await
                .with_context(|| format!("change_membership on shard {shard_id}"))?;

            info!(shard_id, node_id, "Node removed from shard");
        }
        if let Some(addr) = evicted_addr {
            self.zenoh_pool.evict(&addr);
        }
        Ok(())
    }
}

/// 解析节点可路由广播地址的独立纯函数（供 `RaftPool` 与后台微批提案协程共用）。
fn resolve_peer_addr(
    node_config: &NodeConfig,
    peers: &BTreeMap<u64, BasicNode>,
    membership: &Membership<u64, BasicNode>,
    node_id: u64,
) -> Option<String> {
    if node_id == node_config.node_id {
        Some(node_config.advertise_raft_addr())
    } else if let Some(node) = membership.get_node(&node_id) {
        Some(node.addr.clone())
    } else {
        peers.get(&node_id).map(|node| node.addr.clone())
    }
}

/// 分片微批写提案器上下文（预计算分片级转发键表达式，消除热路径字符串分配）。
struct ShardBatcherCtx {
    raft: ShardRaft,
    state_machine: StateMachineStore,
    zenoh_pool: ZenohClientPool,
    node_config: Arc<NodeConfig>,
    peers: Arc<BTreeMap<u64, BasicNode>>,
    shard_id: ShardId,
    write_key: String,
    write_batch_key: String,
}

impl ShardBatcherCtx {
    /// 若当前分片已知远端 leader（非本节点），返回其可路由地址；用于 Follower 直投快路径，跳过本地 `ForwardToLeader` 报错往返。
    fn current_remote_leader(&self) -> Option<String> {
        let metrics_rx = self.raft.metrics();
        let metrics = metrics_rx.borrow_watched();
        let leader_id = metrics.current_leader?;
        if leader_id == self.node_config.node_id {
            return None;
        }
        resolve_peer_addr(
            &self.node_config,
            &self.peers,
            metrics.membership_config.membership(),
            leader_id,
        )
    }

    /// 向调用方回传已提交的 `log_id`；若调用方在等待提案期间已断开连接（`reply_tx` 接收端被丢弃），
    /// 则在状态机应用到该索引后自动交由转发器兜底投递，防止源节点漏写（修复 P0-2）。
    fn send_committed_reply(&self, job: BatchWriteJob, log_id: RaftLogId) {
        if job.reply_tx.send(Ok(log_id)).is_err() {
            self.state_machine.requeue_write(CommittedWrite {
                log_index: log_id.index,
                origin_node_id: 0,
                db_path: job.request.db_path,
                raw_message: job.request.raw_message,
            });
        }
    }

    /// 将一批写请求转发给远端 Leader（单条走 `/write`，多条直接按 `&[BatchWriteJob]` 零克隆序列化走 `/write-batch` 单次非幂等 `rpc_once`，修复 R3）。
    async fn forward_write_jobs(
        &self,
        leader_addr: &str,
        jobs: &[BatchWriteJob],
    ) -> Result<Vec<result::Result<WriteAck, String>>> {
        if let [single] = jobs {
            let ack: WriteAck = self
                .zenoh_pool
                .rpc_once(leader_addr, &self.write_key, &single.request)
                .await
                .with_context(|| format!("forward write to {leader_addr} on {}", self.write_key))?;
            return Ok(vec![Ok(ack)]);
        }

        self.zenoh_pool
            .rpc_once(leader_addr, &self.write_batch_key, jobs)
            .await
            .with_context(|| {
                format!(
                    "forward write batch to {leader_addr} on {}",
                    self.write_batch_key
                )
            })
    }

    /// 分发远端 Leader 批量写响应。
    fn dispatch_forwarded_results(
        &self,
        jobs: impl IntoIterator<Item = BatchWriteJob>,
        results: Result<Vec<result::Result<WriteAck, String>>>,
        expected_len: usize,
    ) {
        match results {
            Ok(acks) if acks.len() == expected_len => {
                for (job, item) in jobs.into_iter().zip(acks) {
                    match item {
                        Ok(ack) => self.send_committed_reply(job, ack.log_id),
                        Err(msg) => {
                            let _ = job.reply_tx.send(Err(anyhow!("{msg}")));
                        }
                    }
                }
            }
            Ok(_) => {
                for job in jobs {
                    let _ = job
                        .reply_tx
                        .send(Err(anyhow!("leader returned mismatched batch ack count")));
                }
            }
            Err(e) => {
                let err_msg = e.to_string();
                for job in jobs {
                    let _ = job.reply_tx.send(Err(anyhow!("{err_msg}")));
                }
            }
        }
    }

    /// 执行单批写作业：
    /// 1. 若本节点是已知 Follower，直接走 Zenoh 单次或批量 RPC 转发给远端 Leader（转发错误直接上报，禁止回落重提以防重复执行，修复 P0-5）；
    /// 2. 若本节点是 Leader（或未知 Leader），通过 `join_all` 在同一 tick 并发入队 `RaftCore`，触发底层单次 `fjall` 批量落盘与单次 `AppendEntries` 复制。
    async fn execute_write_batch(&self, batch: &mut Vec<BatchWriteJob>) {
        if let Some(leader_addr) = self.current_remote_leader() {
            let len = batch.len();
            let res = self.forward_write_jobs(&leader_addr, batch).await;
            self.dispatch_forwarded_results(batch.drain(..), res, len);
            return;
        }

        let results = join_all(
            batch
                .iter()
                .map(|job| self.raft.client_write(job.request.clone())),
        )
        .await;
        let mut to_forward = Vec::new();
        let mut forwarded_leader_addr: Option<String> = None;

        for (job, res) in batch.drain(..).zip(results) {
            match res {
                Ok(resp) => {
                    self.send_committed_reply(job, resp.log_id);
                }
                Err(RaftError::APIError(ClientWriteError::ForwardToLeader(forward))) => {
                    if forwarded_leader_addr.is_none()
                        && let Some(node) = forward.leader_node
                    {
                        forwarded_leader_addr = Some(node.addr);
                    }
                    to_forward.push(job);
                }
                Err(e) => {
                    let _ = job.reply_tx.send(Err(e.into()));
                }
            }
        }

        if to_forward.is_empty() {
            return;
        }

        let shard_id = self.shard_id;
        let Some(leader_addr) = forwarded_leader_addr.or_else(|| self.current_remote_leader())
        else {
            for job in to_forward {
                let _ = job
                    .reply_tx
                    .send(Err(anyhow!("shard {shard_id} has no leader yet")));
            }
            return;
        };

        let len = to_forward.len();
        info!(
            leader_addr = %leader_addr,
            shard_id,
            batch_size = len,
            "Forwarding write batch to leader over Zenoh"
        );
        let res = self.forward_write_jobs(&leader_addr, &to_forward).await;
        self.dispatch_forwarded_results(to_forward, res, len);
    }

    fn new(
        raft: ShardRaft,
        state_machine: StateMachineStore,
        zenoh_pool: ZenohClientPool,
        node_config: Arc<NodeConfig>,
        peers: Arc<BTreeMap<u64, BasicNode>>,
        shard_id: ShardId,
    ) -> Self {
        Self {
            raft,
            state_machine,
            zenoh_pool,
            node_config,
            peers,
            shard_id,
            write_key: mid_u64_key(CLUSTER_PREFIX, shard_id, SUFFIX_WRITE),
            write_batch_key: mid_u64_key(CLUSTER_PREFIX, shard_id, SUFFIX_WRITE_BATCH),
        }
    }

    /// 启动分片的微批写提案后台协程：零延迟接收首条写并立即排空当前就绪队列，合并提交。
    fn spawn(self, batch_rx: BatchWriteReceiver, mut shutdown_rx: watch::Receiver<bool>) {
        spawn(async move {
            let mut batch = Vec::with_capacity(MAX_WRITE_BATCH_SIZE);
            loop {
                let first = select! {
                    item = batch_rx.recv().fuse() => {
                        let Ok(job) = item else { return };
                        job
                    }
                    _ = shutdown_rx.changed().fuse() => return,
                };
                batch.push(first);
                while batch.len() < MAX_WRITE_BATCH_SIZE
                    && let Ok(job) = batch_rx.try_recv()
                {
                    batch.push(job);
                }
                self.execute_write_batch(&mut batch).await;
            }
        })
        .detach();
    }
}
