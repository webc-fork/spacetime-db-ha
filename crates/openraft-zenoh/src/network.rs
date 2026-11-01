//! `openraft 0.10` 基于 Zenoh + 明文 QUIC + `bitcode` 的 `RaftNetworkFactory`、`RaftNetworkV2` 与 Multi-Raft 服务端路由分发。
//!
//! ### 核心特性
//! 1. **QoS 优先级隔离（防止假阳性选举）**：
//!    - `vote` 与空 `append_entries`（心跳）使用 `Priority::RealTime`，在底层 QUIC 传输上高优插队；
//!    - 含日志条目的 `append_entries` 使用 `Priority::InteractiveHigh`；
//!    - `full_snapshot` 使用 `Priority::Data`，避免大体积快照阻塞心跳。
//! 2. **透传 `RPCOption::hard_ttl()` 与快照取消信号**：
//!    - 将 OpenRaft 引擎计算的 `option.hard_ttl()` 透传至 Zenoh 查询超时（设最小下界保护以防极小 TTL 误杀连接握手）；
//!    - `full_snapshot` 同时监听 `cancel` Future，复制流关闭时立即中断。
//! 3. **天然支持 Multi-Raft**：
//!    - 分片级预计算 `raft/{shard_id}/{append|vote|snapshot}` 键表达式，全部分片共享同一个 [`ZenohClientPool`]。

use std::{future::Future, io::Cursor, marker::PhantomData, pin::pin, rc::Rc, time::Duration};

use bitcode::{DecodeOwned, Encode};
use futures::{FutureExt, select};
use openraft::{
    AppData, BasicNode, Entry, EntryPayload, OptionalSend, Raft, RaftTypeConfig, Snapshot,
    errors::{RPCError, ReplicationClosed, StreamingError, Unreachable},
    network::{RPCOption, RaftNetworkFactory, v2::RaftNetworkV2},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
    },
    storage::RaftStateMachine,
};
use zenoh::{qos::Priority, query::Query};

use crate::{
    codec::{
        LeaderId, RaftSnapshot, RaftSnapshotMeta, RaftVote, WireAppendEntriesRequest,
        WireAppendEntriesResponse, WireFatal, WireFullSnapshotRequest, WireSnapshotMeta,
        WireSnapshotResponse, WireVote, WireVoteRequest, WireVoteResponse,
    },
    transport::{
        KEY_SEP, RAFT_PREFIX, RPC_APPEND, RPC_SNAPSHOT, RPC_VOTE, RpcEnvelope, ZenohClientPool,
        decode_req, mid_u64_key, normalize_addr, reply_bitcode,
    },
};

/// Raft RPC 子命令带前导 `/` 的编译期常量后缀。
const SUFFIX_APPEND: &str = const_str::concat!(KEY_SEP, RPC_APPEND);
const SUFFIX_VOTE: &str = const_str::concat!(KEY_SEP, RPC_VOTE);
const SUFFIX_SNAPSHOT: &str = const_str::concat!(KEY_SEP, RPC_SNAPSHOT);

/// Raft RPC 超时下界（保证首次 QUIC 会话建立与 Queryable 退避重试有充足窗口）。
const MIN_RAFT_RPC_TIMEOUT: Duration = Duration::from_secs(1);
/// 快照传输 RPC 超时下界。
const MIN_SNAPSHOT_RPC_TIMEOUT: Duration = Duration::from_secs(10);

/// 适配 `openraft-zenoh` 零拷贝编解码层的 `RaftTypeConfig` 约束 Trait。
pub trait ZenohRaftConfig:
    RaftTypeConfig<
        D = Self::AppPayload,
        NodeId = u64,
        Node = BasicNode,
        LeaderId = LeaderId,
        Vote = RaftVote,
        Entry = Entry<LeaderId, EntryPayload<Self::AppPayload, u64, BasicNode>>,
    >
{
    type AppPayload: AppData + Encode + DecodeOwned;
}

impl<C, D> ZenohRaftConfig for C
where
    D: AppData + Encode + DecodeOwned,
    C: RaftTypeConfig<
            D = D,
            NodeId = u64,
            Node = BasicNode,
            LeaderId = LeaderId,
            Vote = RaftVote,
            Entry = Entry<LeaderId, EntryPayload<D, u64, BasicNode>>,
        >,
{
    type AppPayload = D;
}

/// 分片级预计算的 Raft RPC Zenoh 键表达式（在分片创建时一次性拼装，消除每次心跳/追加 RPC 的 `String` 分配）。
struct ShardRpcKeys {
    append: String,
    vote: String,
    snapshot: String,
}

impl ShardRpcKeys {
    fn new(shard_id: u64) -> Self {
        Self {
            append: mid_u64_key(RAFT_PREFIX, shard_id, SUFFIX_APPEND),
            vote: mid_u64_key(RAFT_PREFIX, shard_id, SUFFIX_VOTE),
            snapshot: mid_u64_key(RAFT_PREFIX, shard_id, SUFFIX_SNAPSHOT),
        }
    }
}

/// Raft 节点间网络工厂：每个分片一套工厂，RPC 落到 `raft/{shard_id}/{rpc}`，底层共享同一 [`ZenohClientPool`]。
pub struct NetworkFactory<C> {
    pool: ZenohClientPool,
    keys: Rc<ShardRpcKeys>,
    _marker: PhantomData<fn() -> C>,
}

impl<C> NetworkFactory<C> {
    pub fn new(pool: ZenohClientPool, shard_id: u64) -> Self {
        Self {
            pool,
            keys: Rc::new(ShardRpcKeys::new(shard_id)),
            _marker: PhantomData,
        }
    }
}

/// 指向某个对端节点的 Zenoh 明文 QUIC 连接。
pub struct NetworkConnection<C> {
    addr: String,
    pool: ZenohClientPool,
    keys: Rc<ShardRpcKeys>,
    _marker: PhantomData<fn() -> C>,
}

/// 将远端 `WireFatal` 转换为 `Unreachable<C>`（一处定义，供 append / vote / snapshot 共用）。
#[inline]
fn fatal_unreachable<C: RaftTypeConfig>(e: WireFatal) -> Unreachable<C> {
    Unreachable::from_string(format!("{e:?}"))
}

impl<C: RaftTypeConfig> NetworkConnection<C> {
    /// 通过 Zenoh 发送带超时与 QoS 优先级的 `bitcode` RPC 请求并反序列化响应体。
    async fn call_rpc<P, T>(
        &self,
        key_expr: &str,
        payload: &P,
        timeout: Duration,
        priority: Priority,
    ) -> Result<T, Unreachable<C>>
    where
        P: Encode,
        T: DecodeOwned,
    {
        self.pool
            .rpc_with_qos(&self.addr, key_expr, payload, timeout, priority)
            .await
            .map_err(|e| Unreachable::from_string(e.to_string()))
    }
}

impl<C: ZenohRaftConfig> RaftNetworkFactory<C> for NetworkFactory<C> {
    type Network = NetworkConnection<C>;

    async fn new_client(&mut self, _target: u64, node: &BasicNode) -> Self::Network {
        NetworkConnection {
            addr: normalize_addr(&node.addr, true),
            pool: self.pool.clone(),
            keys: Rc::clone(&self.keys),
            _marker: PhantomData,
        }
    }
}

impl<C: ZenohRaftConfig> RaftNetworkV2<C> for NetworkConnection<C> {
    type SnapshotData = Cursor<Vec<u8>>;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<C>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<C>, RPCError<C>> {
        // 空条目为心跳包，赋予 RealTime 最高优先级插队发送，防止日志复制打满带宽引发误选举
        let priority = if rpc.entries.is_empty() {
            Priority::RealTime
        } else {
            Priority::InteractiveHigh
        };
        let timeout = option.hard_ttl().max(MIN_RAFT_RPC_TIMEOUT);
        let req = WireAppendEntriesRequest::<C::AppPayload>::from(rpc);
        let result: Result<WireAppendEntriesResponse, WireFatal> = self
            .call_rpc(&self.keys.append, &req, timeout, priority)
            .await?;
        result
            .map(AppendEntriesResponse::from)
            .map_err(|e| RPCError::Unreachable(fatal_unreachable(e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<C>,
        option: RPCOption,
    ) -> Result<VoteResponse<C>, RPCError<C>> {
        let timeout = option.hard_ttl().max(MIN_RAFT_RPC_TIMEOUT);
        let req = WireVoteRequest::from(rpc);
        let result: Result<WireVoteResponse, WireFatal> = self
            .call_rpc(&self.keys.vote, &req, timeout, Priority::RealTime)
            .await?;
        result
            .map(VoteResponse::from)
            .map_err(|e| RPCError::Unreachable(fatal_unreachable(e)))
    }

    async fn full_snapshot(
        &mut self,
        vote: RaftVote,
        snapshot: RaftSnapshot,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<C>, StreamingError<C>> {
        let timeout = option.hard_ttl().max(MIN_SNAPSHOT_RPC_TIMEOUT);
        let req = WireFullSnapshotRequest {
            vote: WireVote::from(vote),
            meta: WireSnapshotMeta::from(snapshot.meta),
            data: snapshot.snapshot.into_inner(),
        };
        let rpc_fut = self
            .call_rpc(&self.keys.snapshot, &req, timeout, Priority::Data)
            .fuse();
        let cancel_fut = cancel.fuse();
        let mut rpc_pin = pin!(rpc_fut);
        let mut cancel_pin = pin!(cancel_fut);

        let result: Result<WireSnapshotResponse, WireFatal> = select! {
            res = rpc_pin => res.map_err(StreamingError::Unreachable)?,
            closed = cancel_pin => return Err(StreamingError::Closed(closed)),
        };

        result
            .map(SnapshotResponse::from)
            .map_err(|e| StreamingError::Unreachable(fatal_unreachable(e)))
    }
}

/// 分发 `raft/**` 命名空间下的 Multi-Raft 节点间共识查询（`append` / `vote` / `snapshot`）。
pub async fn dispatch_raft_query<C, SM, F>(query: Query, default_shard: u64, get_raft: F)
where
    C: ZenohRaftConfig,
    SM: RaftStateMachine<C, SnapshotData = Cursor<Vec<u8>>>,
    F: FnOnce(u64) -> Result<Raft<C, SM>, String>,
{
    let key = query.key_expr().as_str();
    let rest = key.strip_prefix(RAFT_PREFIX).unwrap_or("");

    let (shard_id, rpc) = match rest.split_once('/') {
        Some((sid_str, rpc_name)) => match sid_str.parse::<u64>() {
            Ok(sid) => (sid, rpc_name),
            Err(_) => {
                reply_bitcode::<()>(&query, Err(format!("invalid shard id in {key}"))).await;
                return;
            }
        },
        None => (default_shard, rest),
    };

    match rpc {
        RPC_APPEND => {
            let res: RpcEnvelope<Result<WireAppendEntriesResponse, WireFatal>> = async {
                let req: WireAppendEntriesRequest<C::AppPayload> = decode_req(&query)?;
                let raft = get_raft(shard_id)?;
                Ok(raft
                    .append_entries(AppendEntriesRequest::from(req))
                    .await
                    .map(WireAppendEntriesResponse::from)
                    .map_err(WireFatal::from))
            }
            .await;
            reply_bitcode(&query, res).await;
        }
        RPC_VOTE => {
            let res: RpcEnvelope<Result<WireVoteResponse, WireFatal>> = async {
                let req: WireVoteRequest = decode_req(&query)?;
                let raft = get_raft(shard_id)?;
                Ok(raft
                    .vote(VoteRequest::from(req))
                    .await
                    .map(WireVoteResponse::from)
                    .map_err(WireFatal::from))
            }
            .await;
            reply_bitcode(&query, res).await;
        }
        RPC_SNAPSHOT => {
            let res: RpcEnvelope<Result<WireSnapshotResponse, WireFatal>> = async {
                let req: WireFullSnapshotRequest = decode_req(&query)?;
                let raft = get_raft(shard_id)?;
                let vote = RaftVote::from(req.vote);
                let snapshot = Snapshot {
                    meta: RaftSnapshotMeta::from(req.meta),
                    snapshot: Cursor::new(req.data),
                };
                Ok(raft
                    .install_full_snapshot(vote, snapshot)
                    .await
                    .map(WireSnapshotResponse::from)
                    .map_err(WireFatal::from))
            }
            .await;
            reply_bitcode(&query, res).await;
        }
        _ => {
            reply_bitcode::<()>(&query, Err(format!("unknown raft rpc: {key}"))).await;
        }
    }
}
