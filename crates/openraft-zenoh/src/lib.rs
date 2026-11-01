//! `openraft-zenoh`：面向 `openraft 0.10` 的零 `serde`、基于 Zenoh 明文 QUIC + `bitcode` + `compio` + `crossfire` 的 Multi-Raft 网络与传输库。

#![recursion_limit = "256"]

pub mod codec;
pub mod network;
pub mod transport;

pub use codec::{
    CommittedLeaderId, LeaderId, RaftLogId, RaftSnapshot, RaftSnapshotMeta, RaftStoredMembership,
    RaftVote, WireBytes, leader_term,
};
pub use network::{NetworkConnection, NetworkFactory, ZenohRaftConfig, dispatch_raft_query};
pub use transport::{
    CrossfireChannel, CrossfireHandler, RpcEnvelope, ZenohClientPool, close_session,
    decode_query_payload, mid_u64_key, normalize_addr, normalize_addr_cow, oneshot_rpc,
    open_client_session, open_server_session, prefix_u64_key, reply_bitcode, rpc_once,
    rpc_once_with_qos, strip_addr_parts, watch,
};
