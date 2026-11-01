use std::{
    future::Future,
    io::{self, Cursor},
    rc::Rc,
};

use bitcode::{DecodeOwned, Encode};
use openraft::{
    BasicNode, OptionalSend,
    errors::{RPCError, ReplicationClosed, StreamingError, Unreachable},
    network::{RPCOption, RaftNetworkFactory, v2::RaftNetworkV2},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
    },
};

use crate::{
    raft::{
        codec::{
            WireAppendEntriesRequest, WireAppendEntriesResponse, WireFatal,
            WireFullSnapshotRequest, WireSnapshotMeta, WireSnapshotResponse, WireVote,
            WireVoteRequest, WireVoteResponse,
        },
        types::{RaftSnapshot, RaftVote, TypeConfig},
    },
    transport::{
        KEY_SEP, RAFT_PREFIX, RPC_APPEND, RPC_SNAPSHOT, RPC_VOTE, ZenohClientPool, mid_u64_key,
        normalize_addr,
    },
};

type C = TypeConfig;

/// Raft RPC 子命令带前导 `/` 的编译期常量后缀。
const SUFFIX_APPEND: &str = const_str::concat!(KEY_SEP, RPC_APPEND);
const SUFFIX_VOTE: &str = const_str::concat!(KEY_SEP, RPC_VOTE);
const SUFFIX_SNAPSHOT: &str = const_str::concat!(KEY_SEP, RPC_SNAPSHOT);

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

/// Raft 节点间网络层：每个分片一套工厂，RPC 落到 `raft/{shard_id}/{rpc}`。
pub struct NetworkFactory {
    pool: ZenohClientPool,
    keys: Rc<ShardRpcKeys>,
}

impl NetworkFactory {
    pub fn new(pool: ZenohClientPool, shard_id: u64) -> Self {
        Self {
            pool,
            keys: Rc::new(ShardRpcKeys::new(shard_id)),
        }
    }
}

/// 指向某个对端节点的 Zenoh 明文 QUIC 连接。
pub struct NetworkConnection {
    addr: String,
    pool: ZenohClientPool,
    keys: Rc<ShardRpcKeys>,
}

impl NetworkConnection {
    /// 通过 Zenoh 发送 `bitcode` 编码的 RPC 请求并反序列化响应体。
    async fn call_rpc<P, T>(&self, key_expr: &str, payload: &P) -> Result<T, Unreachable<C>>
    where
        P: Encode,
        T: DecodeOwned,
    {
        self.pool
            .rpc(&self.addr, key_expr, payload)
            .await
            .map_err(|e| {
                let io_err = io::Error::other(e.to_string());
                Unreachable::new(&io_err)
            })
    }
}

impl RaftNetworkFactory<C> for NetworkFactory {
    type Network = NetworkConnection;

    async fn new_client(&mut self, _target: u64, node: &BasicNode) -> Self::Network {
        NetworkConnection {
            addr: normalize_addr(&node.addr, true),
            pool: self.pool.clone(),
            keys: Rc::clone(&self.keys),
        }
    }
}

impl RaftNetworkV2<C> for NetworkConnection {
    type SnapshotData = Cursor<Vec<u8>>;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<C>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<C>, RPCError<C>> {
        let req = WireAppendEntriesRequest::from(rpc);
        let result: Result<WireAppendEntriesResponse, WireFatal> =
            self.call_rpc(&self.keys.append, &req).await?;
        result
            .map(AppendEntriesResponse::from)
            .map_err(|e| RPCError::Unreachable(Unreachable::from_string(format!("{e:?}"))))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<C>,
        _option: RPCOption,
    ) -> Result<VoteResponse<C>, RPCError<C>> {
        let req = WireVoteRequest::from(rpc);
        let result: Result<WireVoteResponse, WireFatal> =
            self.call_rpc(&self.keys.vote, &req).await?;
        result
            .map(VoteResponse::from)
            .map_err(|e| RPCError::Unreachable(Unreachable::from_string(format!("{e:?}"))))
    }

    async fn full_snapshot(
        &mut self,
        vote: RaftVote,
        snapshot: RaftSnapshot,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<C>, StreamingError<C>> {
        let req = WireFullSnapshotRequest {
            vote: WireVote::from(vote),
            meta: WireSnapshotMeta::from(snapshot.meta),
            data: snapshot.snapshot.into_inner(),
        };
        let result: Result<WireSnapshotResponse, WireFatal> = self
            .call_rpc(&self.keys.snapshot, &req)
            .await
            .map_err(StreamingError::Unreachable)?;
        result
            .map(SnapshotResponse::from)
            .map_err(|e| StreamingError::Unreachable(Unreachable::from_string(format!("{e:?}"))))
    }
}
