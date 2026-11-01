use std::{fmt, io::Cursor};

use bitcode::{Decode, Encode};
use bytes::Bytes;
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, Snapshot, SnapshotMeta, StoredMembership, Vote,
    impls::leader_id_adv,
};
use openraft_rt_compio::CompioRuntime;

/// Raft 选主标识类型（支持同任期多选主对比）。
pub type LeaderId = leader_id_adv::LeaderId<u64, u64>;

/// Raft 已提交选主标识类型。
pub type CommittedLeaderId = LeaderId;

/// Raft 日志条目唯一标识类型。
pub type RaftLogId = LogId<LeaderId>;

/// Raft 选票类型。
pub type RaftVote = Vote<LeaderId>;

/// 提取选票的任期号。
pub fn leader_term(vote: &RaftVote) -> u64 {
    vote.leader_id().term
}

/// Raft 持久化成员配置类型。
pub type RaftStoredMembership = StoredMembership<LeaderId, u64, BasicNode>;

/// Raft 快照元数据类型。
pub type RaftSnapshotMeta = SnapshotMeta<LeaderId, u64, BasicNode>;

/// Raft 快照类型。
pub type RaftSnapshot = Snapshot<LeaderId, u64, BasicNode, Cursor<Vec<u8>>>;

/// Raft 日志条目类型。
pub type RaftEntry = Entry<LeaderId, EntryPayload<ReducerCallRequest, u64, BasicNode>>;

/// 待复制的 reducer 调用。
/// 消息整体当作不透明字节处理，无需解析 BSATN。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReducerCallRequest {
    /// 原始二进制消息；`Bytes` 克隆只加引用计数，避免逐层拷贝。
    pub raw_message: Bytes,
    /// 目标数据库路径（客户端握手路径）。
    /// 其它节点按此路径选择共享上游连接，避免不同数据库的写串到一起。
    pub db_path: String,
    /// 提议该写的节点。源节点的连接会自己投递（事务结果需回到发起方），
    /// 转发任务据此跳过，只投递其它节点提交的写。
    pub origin_node_id: u64,
}

impl fmt::Display for ReducerCallRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ReducerCallRequest(node={}, db={}, len={})",
            self.origin_node_id,
            self.db_path,
            self.raw_message.len()
        )
    }
}

/// reducer 调用提交后的响应。
#[derive(Debug, Clone, Encode, Decode)]
pub struct ReducerCallResponse {
    pub success: bool,
}

/// leader 对转发写的应答：回传提交后的日志位置，
/// follower 据此等待本地应用到该条目后再向自己的客户端回话。
#[derive(Debug, Clone)]
pub struct WriteAck {
    pub log_id: RaftLogId,
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = ReducerCallRequest,
        R = ReducerCallResponse,
        Node = BasicNode,
        AsyncRuntime = CompioRuntime,
);
