use std::{
    collections::{BTreeMap, BTreeSet},
    fs::remove_dir_all,
    path::Path,
    rc::Rc,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use compio::{
    runtime::spawn,
    time::{interval, timeout},
};
use futures::{FutureExt, select};
use gxhash::{HashMap, HashMapExt};
use openraft::{
    BasicNode, Config, Raft, ReadPolicy,
    async_runtime::watch::WatchReceiver,
    entry::EntryPayload,
    error::{ClientWriteError, RaftError},
};
use parking_lot::RwLock;
use tracing::{info, warn};

use crate::{
    config::NodeConfig,
    metrics,
    raft::{
        persistent_log_store::{PersistentLogStore, shard_dir},
        state_machine::{
            CONFIG_CHANNEL_CAPACITY, ConfigSender, FORWARDER_CHANNEL_CAPACITY, StateMachineStore,
            config_channel, write_channel,
        },
        types::{RaftLogId, ReducerCallRequest, TypeConfig, WriteAck, leader_term},
    },
    router::{DEFAULT_SHARD, SHARD_CONFIG_TAG, ShardConfigCommand, ShardId, ShardRouter},
    transport::{CLUSTER_PREFIX, SUFFIX_WRITE, ZenohClientPool, mid_u64_key, normalize_addr, watch},
};

pub mod codec;
pub mod error;
pub mod forwarder;
pub mod network;
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
const APPLY_TIMEOUT: Duration = Duration::from_secs(30);
/// 等待前置日志投递完成的超时上限。
const DELIVER_ORDER_TIMEOUT: Duration = Duration::from_millis(500);
/// 读请求因果同步栅栏等待上限。
const READ_BARRIER_TIMEOUT: Duration = Duration::from_millis(200);
/// 未初始化分片协调重试间隔（修复 P1-7）。
const SHARD_RECONCILE_INTERVAL: Duration = Duration::from_millis(500);

/// 解析 `id=host:port` 形式的对端列表，单趟遍历完成，非法项记日志后跳过。
pub fn parse_peers(peers: &[String]) -> BTreeMap<u64, BasicNode> {
    peers
        .iter()
        .filter_map(|peer| match peer.split_once('=') {
            Some((id_str, addr)) => match id_str.parse::<u64>() {
                Ok(id) => Some((
                    id,
                    BasicNode {
                        addr: normalize_addr(addr, true),
                    },
                )),
                Err(_) => {
                    warn!(peer = %peer, "Failed to parse peer node ID");
                    None
                }
            },
            None => {
                warn!(peer = %peer, "Invalid peer format, expected 'id=host:port'");
                None
            }
        })
        .collect()
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

/// 单个分片的运行时状态。
struct ShardState {
    raft: ShardRaft,
    state_machine: StateMachineStore,
    forwarder_shutdown: watch::Sender<bool>,
}

/// 分片池：一分片一 Raft 组。
///
/// 分片 0 始终存在且为默认分片；其余分片由分片 0 日志里的配置命令动态创建或删除。
/// 所有节点参与全部成员相同的分片。
pub struct RaftPool {
    pub node_config: Arc<NodeConfig>,
    pub peers: BTreeMap<u64, BasicNode>,
    shards: RwLock<HashMap<ShardId, ShardState>>,
    router: RwLock<ShardRouter>,
    shutdown_rx: watch::Receiver<bool>,
    /// 节点间 Zenoh 明文 QUIC 连接池。
    zenoh_pool: ZenohClientPool,
}

impl RaftPool {
    /// 创建分片池并完成分片 0 的初始化及已持久化拓扑恢复。返回 `Rc` 供单线程 `compio` 后台任务共享。
    pub async fn new(config: &NodeConfig, shutdown_rx: watch::Receiver<bool>) -> Result<Rc<Self>> {
        let node_config = Arc::new(config.clone());
        let peers = parse_peers(&config.peers);
        let zenoh_pool = ZenohClientPool::new();

        let (shard_config_tx, shard_config_rx) = config_channel(CONFIG_CHANNEL_CAPACITY);
        let shard0 = Self::create_shard(
            node_config.clone(),
            zenoh_pool.clone(),
            DEFAULT_SHARD,
            shutdown_rx.clone(),
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
            shutdown_rx,
            zenoh_pool,
        });

        // 恢复重启前已激活的非 0 分片
        for sid in persisted_shards {
            if sid != DEFAULT_SHARD {
                pool.materialize_shard(sid).await;
            }
        }

        // 分片配置监听：把分片 0 提交的配置命令落地成实际分片与路由
        let pool_ref = Rc::clone(&pool);
        let mut listener_shutdown = pool.shutdown_rx.clone();
        spawn(async move {
            loop {
                select! {
                    cmd = shard_config_rx.recv().fuse() => {
                        let Ok(cmd) = cmd else { return };
                        pool_ref.handle_shard_config(cmd).await;
                    }
                    _ = listener_shutdown.changed().fuse() => return,
                }
            }
        })
        .detach();

        // 修复 P1-7：启动周期性幂等协调循环，确保 leader 切换或网络抖动期间新建的分片最终完成初始化
        let reconcile_pool = Rc::clone(&pool);
        let mut reconcile_shutdown = pool.shutdown_rx.clone();
        spawn(async move {
            let mut ticker = interval(SHARD_RECONCILE_INTERVAL);
            loop {
                select! {
                    _ = ticker.tick().fuse() => {
                        reconcile_pool.reconcile_uninitialized_shards().await;
                    }
                    _ = reconcile_shutdown.changed().fuse() => return,
                }
            }
        })
        .detach();

        Ok(pool)
    }

    /// 创建分片的 Raft 实例、状态机、转发通道，并重放崩溃前未完成投递的已提交写 `(last_delivered, last_applied]`。
    async fn create_shard(
        config: Arc<NodeConfig>,
        zenoh_pool: ZenohClientPool,
        shard_id: ShardId,
        mut global_shutdown: watch::Receiver<bool>,
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

        // 修复 P2-10：日志与状态机共享同一个 fjall::Database 实例，并校验 last_purged 水位
        let log_store = PersistentLogStore::new(Path::new(&config.data_dir), shard_id)?;
        let state_machine = StateMachineStore::open_with_db(log_store.db(), shard_id)?;
        state_machine.align_with_purged(log_store.last_purged_log_id()?)?;

        if let Some(cfg_tx) = shard_config_tx {
            state_machine.set_shard_config_tx(cfg_tx);
        }

        let (forwarder_shutdown_tx, forwarder_shutdown_rx) = watch::channel(false);
        let fwd_tx = forwarder_shutdown_tx.clone();
        spawn(async move {
            let _ = global_shutdown.changed().await;
            let _ = fwd_tx.send(true);
        })
        .detach();

        let (write_tx, write_rx) = write_channel(FORWARDER_CHANNEL_CAPACITY);
        state_machine.set_forwarder(write_tx.clone());
        forwarder::spawn(
            config.node_id,
            config.stdb_url.clone(),
            write_rx,
            Some(state_machine.clone()),
            forwarder_shutdown_rx,
        );

        // 修复 P0-2：启动时重放 (last_delivered, last_applied] 区间内尚未投递到 SpacetimeDB 的日志条目
        let delivered = state_machine.last_delivered_index();
        let applied = state_machine.last_applied_index();
        if delivered < applied {
            let entries = log_store.read_entries_range(delivered + 1, applied)?;
            for entry in entries {
                let idx = entry.log_id.index;
                if let EntryPayload::Normal(req) = entry.payload {
                    let is_cfg = shard_id == DEFAULT_SHARD
                        && req.db_path.is_empty()
                        && req.raw_message.first() == Some(&SHARD_CONFIG_TAG);
                    if !is_cfg {
                        // 重放时设 origin_node_id = 0，确保无论当初由哪个节点提议，本节点恢复后都会补投
                        let _ = write_tx
                            .send(CommittedWrite {
                                log_index: idx,
                                origin_node_id: 0,
                                db_path: req.db_path,
                                raw_message: req.raw_message,
                            })
                            .await;
                        continue;
                    }
                }
                state_machine.mark_delivered(idx);
            }
        }

        let network = network::NetworkFactory::new(zenoh_pool, shard_id);
        let raft = Raft::new(
            config.node_id,
            Arc::new(raft_config),
            network,
            log_store,
            state_machine.clone(),
        )
        .await?;

        info!(shard_id, "Raft shard initialized");

        Ok(ShardState {
            raft,
            state_machine,
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
            self.node_config.clone(),
            self.zenoh_pool.clone(),
            shard_id,
            self.shutdown_rx.clone(),
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
            let _ = shard.forwarder_shutdown.send(true);
            let _ = shard.raft.shutdown().await;
            let dir = shard_dir(Path::new(&self.node_config.data_dir), shard_id);
            let _ = remove_dir_all(&dir);
            info!(shard_id, "Shard removed and resources cleaned up");
        }
    }

    /// 若本节点是分片 0 的 leader 且目标分片尚未初始化，则使用分片 0 成员表初始化该分片（修复 P1-7）。
    async fn try_initialize_shard(&self, shard_id: ShardId) {
        if !self.is_leader(DEFAULT_SHARD).await {
            return;
        }

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
        if !is_uninitialized {
            return;
        }

        let members = self.current_members(DEFAULT_SHARD);
        if members.is_empty() {
            return;
        }
        match raft.initialize(members).await {
            Ok(()) => info!(shard_id, "New shard initialized with existing membership"),
            Err(e) => warn!(shard_id, error = %e, "Failed to initialize new shard"),
        }
    }

    /// 定期检查并初始化所有尚未完成成员初始化的活跃分片（修复 P1-7）。
    async fn reconcile_uninitialized_shards(&self) {
        if !self.is_leader(DEFAULT_SHARD).await {
            return;
        }
        for shard_id in self.list_shards() {
            if shard_id != DEFAULT_SHARD {
                self.try_initialize_shard(shard_id).await;
            }
        }
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
                if id == self.node_config.node_id {
                    Some((
                        id,
                        BasicNode {
                            addr: self.node_config.advertise_raft_addr(),
                        },
                    ))
                } else if let Some(node) = membership.get_node(&id) {
                    Some((id, node.clone()))
                } else {
                    self.peers.get(&id).map(|node| (id, node.clone()))
                }
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

    /// 按模块名查分片。
    pub fn route_module(&self, module_name: &str) -> ShardId {
        self.router.read().route(module_name)
    }

    /// 通过指定分片提交一次写（reducer 调用）；本节点不是 leader 时转发给 leader。
    /// 返回提交后的日志位置，调用方可据此等待本地应用完成。
    pub async fn propose_write(
        &self,
        shard_id: ShardId,
        raw_message: Bytes,
        db_path: String,
    ) -> Result<RaftLogId> {
        let _timer = metrics::WRITE_LATENCY.start_timer();
        metrics::WRITES_TOTAL.inc();

        let raft = self
            .get_raft(shard_id)
            .ok_or_else(|| anyhow!("shard {shard_id} not found"))?;
        let origin_node_id = self.node_config.node_id;
        let request = ReducerCallRequest {
            raw_message: raw_message.clone(),
            db_path: db_path.clone(),
            origin_node_id,
        };

        match raft.client_write(request).await {
            Ok(resp) => Ok(resp.log_id),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(forward))) => {
                let leader_addr = if let Some(leader) = forward.leader_node {
                    leader.addr
                } else if let Some((_, addr)) = self.current_leader(shard_id) {
                    addr
                } else {
                    bail!("shard {shard_id} has no leader yet");
                };
                info!(leader_addr = %leader_addr, shard_id, "Forwarding write to leader over Zenoh");
                self.forward_write(shard_id, &leader_addr, raw_message, db_path)
                    .await
            }
            Err(e) => Err(e.into()),
        }
    }

    /// 等待本节点把日志应用到指定 index。
    pub async fn wait_applied(&self, shard_id: ShardId, index: u64) -> Result<()> {
        let mut applied = {
            let shards = self.shards.read();
            let shard = shards
                .get(&shard_id)
                .ok_or_else(|| anyhow!("shard {shard_id} not found"))?;
            shard.state_machine.applied_rx()
        };

        if *applied.borrow_and_update() >= index {
            return Ok(());
        }

        let applied_in_time = timeout(APPLY_TIMEOUT, async {
            while applied.changed().await.is_ok() {
                if *applied.borrow_and_update() >= index {
                    return true;
                }
            }
            false
        })
        .await
        .unwrap_or(false);

        if applied_in_time {
            return Ok(());
        }
        bail!("timed out waiting for log index {index} to be applied")
    }

    /// 等待指定日志索引之前的写投递完毕，实现同节点跨连接写的全局保序（修复 P0-1）。
    pub async fn wait_delivered(&self, shard_id: ShardId, target_index: u64) {
        if target_index == 0 {
            return;
        }
        let mut delivered = {
            let shards = self.shards.read();
            let Some(shard) = shards.get(&shard_id) else {
                return;
            };
            shard.state_machine.delivered_rx()
        };

        if *delivered.borrow_and_update() >= target_index {
            return;
        }

        let _ = timeout(DELIVER_ORDER_TIMEOUT, async {
            while delivered.changed().await.is_ok() {
                if *delivered.borrow_and_update() >= target_index {
                    break;
                }
            }
        })
        .await;
    }

    /// 标记指定日志索引的写已投递到上游 SpacetimeDB（修复 P0-2）。
    pub fn mark_delivered(&self, shard_id: ShardId, index: u64) {
        let shards = self.shards.read();
        if let Some(shard) = shards.get(&shard_id) {
            shard.state_machine.mark_delivered(index);
        }
    }

    /// 读请求前的因果同步栅栏：等待本节点状态机追平已知最后日志索引（修复 P0-4）。
    pub async fn wait_read_ready(&self, shard_id: ShardId) {
        let target = {
            let shards = self.shards.read();
            let Some(shard) = shards.get(&shard_id) else {
                return;
            };
            shard
                .raft
                .metrics()
                .borrow_watched()
                .last_log_index
                .unwrap_or(0)
        };
        if target > 0 {
            let _ = timeout(READ_BARRIER_TIMEOUT, self.wait_applied(shard_id, target)).await;
        }
    }

    /// 把写请求通过 Zenoh + `bitcode` 转发给 leader 节点，取回提交后的日志位置。
    async fn forward_write(
        &self,
        shard_id: ShardId,
        leader_addr: &str,
        raw_message: Bytes,
        db_path: String,
    ) -> Result<RaftLogId> {
        let key_expr = mid_u64_key(CLUSTER_PREFIX, shard_id, SUFFIX_WRITE);
        let request = ReducerCallRequest {
            raw_message,
            db_path,
            origin_node_id: self.node_config.node_id,
        };

        let ack: WriteAck = self
            .zenoh_pool
            .rpc(leader_addr, &key_expr, &request)
            .await
            .with_context(|| format!("forward write to {leader_addr} on {key_expr}"))?;
        Ok(ack.log_id)
    }

    /// 本节点是否为指定分片的 leader。
    pub async fn is_leader(&self, shard_id: ShardId) -> bool {
        match self.get_raft(shard_id) {
            Some(raft) => raft
                .ensure_linearizable(ReadPolicy::ReadIndex)
                .await
                .is_ok(),
            None => false,
        }
    }

    /// 指定分片当前 leader 的 ID 与可路由广播地址（修复 P1-8）。
    pub fn current_leader(&self, shard_id: ShardId) -> Option<(u64, String)> {
        let shards = self.shards.read();
        let shard = shards.get(&shard_id)?;
        let metrics_rx = shard.raft.metrics();
        let metrics = metrics_rx.borrow_watched();
        let leader_id = metrics.current_leader?;

        let addr = if leader_id == self.node_config.node_id {
            self.node_config.advertise_raft_addr()
        } else if let Some(node) = metrics.membership_config.membership().get_node(&leader_id) {
            node.addr.clone()
        } else {
            self.peers.get(&leader_id)?.addr.clone()
        };

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

    /// 把节点从所有分片的 Raft 组中移除。
    pub async fn remove_node_all_shards(&self, node_id: u64) -> Result<()> {
        for shard_id in self.list_shards() {
            let raft = self
                .get_raft(shard_id)
                .ok_or_else(|| anyhow!("shard {shard_id} not found"))?;

            let voters: BTreeSet<u64> = raft
                .metrics()
                .borrow_watched()
                .membership_config
                .membership()
                .voter_ids()
                .filter(|id| *id != node_id)
                .collect();

            raft.change_membership(voters, false)
                .await
                .with_context(|| format!("change_membership on shard {shard_id}"))?;

            info!(shard_id, node_id, "Node removed from shard");
        }
        Ok(())
    }
}
