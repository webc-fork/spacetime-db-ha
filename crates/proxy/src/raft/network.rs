use std::{
    future::Future,
    io::{self, Cursor},
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
    },
};

type C = TypeConfig;

/// Raft RPC 子命令带前导 `/` 的编译期常量后缀（消除运行时二次拼接）。
const SUFFIX_APPEND: &str = const_str::concat!(KEY_SEP, RPC_APPEND);
const SUFFIX_VOTE: &str = const_str::concat!(KEY_SEP, RPC_VOTE);
const SUFFIX_SNAPSHOT: &str = const_str::concat!(KEY_SEP, RPC_SNAPSHOT);

/// Raft 节点间网络层：每个分片一套工厂，RPC 落到 `raft/{shard_id}/{rpc}`。
pub struct NetworkFactory {
    pool: ZenohClientPool,
    shard_id: u64,
}

impl NetworkFactory {
    pub const fn new(pool: ZenohClientPool, shard_id: u64) -> Self {
        Self { pool, shard_id }
    }
}

/// 指向某个对端节点的 Zenoh 明文 QUIC 连接。
pub struct NetworkConnection {
    addr: String,
    pool: ZenohClientPool,
    shard_id: u64,
}

impl NetworkConnection {
    /// 拼出 Raft RPC 的 Zenoh 键表达式（基于编译期前缀/后缀 + `itoa` 栈缓冲，无 `format!` 开销）。
    #[inline]
    fn raft_key(&self, rpc_suffix: &str) -> String {
        mid_u64_key(RAFT_PREFIX, self.shard_id, rpc_suffix)
    }

    /// 通过 Zenoh 发送 `bitcode` 编码的 RPC 请求并反序列化响应体。
    async fn call_rpc<P, T>(&self, rpc_suffix: &str, payload: &P) -> Result<T, Unreachable<C>>
    where
        P: Encode,
        T: DecodeOwned,
    {
        self.pool
            .rpc(&self.addr, &self.raft_key(rpc_suffix), payload)
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
            addr: node.addr.clone(),
            pool: self.pool.clone(),
            shard_id: self.shard_id,
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
            self.call_rpc(SUFFIX_APPEND, &req).await?;
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
        let result: Result<WireVoteResponse, WireFatal> = self.call_rpc(SUFFIX_VOTE, &req).await?;
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
            .call_rpc(SUFFIX_SNAPSHOT, &req)
            .await
            .map_err(StreamingError::Unreachable)?;
        result
            .map(SnapshotResponse::from)
            .map_err(|e| StreamingError::Unreachable(Unreachable::from_string(format!("{e:?}"))))
    }
}
