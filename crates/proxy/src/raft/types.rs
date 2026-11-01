use std::io::Cursor;

use openraft::BasicNode;
use serde::{Deserialize, Serialize};

/// A reducer call to be replicated through Raft.
/// We treat the WebSocket message as an opaque blob — no BSATN deserialization needed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReducerCallRequest {
    pub raw_message: Vec<u8>,
    /// Node that proposed this write. Used by the forwarder to avoid
    /// double-sending: the originating node's handler already forwards
    /// the write to SpacetimeDB, so the forwarder skips it.
    pub origin_node_id: u64,
}

/// Response after a reducer call is committed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReducerCallResponse {
    pub success: bool,
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = ReducerCallRequest,
        R = ReducerCallResponse,
        Node = BasicNode,
        SnapshotData = Cursor<Vec<u8>>,
);
