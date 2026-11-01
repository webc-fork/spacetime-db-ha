//! 业务层 `ReducerCallRequest` / `WriteAck` 的 `bitcode` 零拷贝编解码适配，并重导出 `openraft_zenoh::codec`。

use std::num::NonZeroUsize;

use bitcode::{
    __private::{Buffer, Encoder},
    Decode, Encode,
};
use bytes::Bytes;
use openraft_zenoh::codec;
pub use openraft_zenoh::codec::{
    WireBytes, WireDecoder, WireEncoder, WireLogId, WireStoredMembership, WireVote,
    impl_bitcode_via,
};

use crate::raft::types::{RaftLogId, ReducerCallRequest, WriteAck};

/// `RaftEntry` 的 `bitcode` 原生编码表示。
pub type WireEntry = codec::WireEntry<ReducerCallRequest>;

/// `ReducerCallRequest` 的零拷贝直接编码器（直接按 `&[u8]` 与 `&str` 写入 `bitcode` 缓冲区，零 `.to_vec()`、零 `.clone()`）。
#[derive(Default)]
pub struct ReducerCallRequestEncoder {
    raw_message: <[u8] as Encode>::Encoder,
    db_path: <str as Encode>::Encoder,
    origin_node_id: <u64 as Encode>::Encoder,
}

impl Encoder<ReducerCallRequest> for ReducerCallRequestEncoder {
    #[inline(always)]
    fn encode(&mut self, t: &ReducerCallRequest) {
        Encoder::<[u8]>::encode(&mut self.raw_message, &t.raw_message);
        Encoder::<str>::encode(&mut self.db_path, &t.db_path);
        self.origin_node_id.encode(&t.origin_node_id);
    }
}

impl Buffer for ReducerCallRequestEncoder {
    #[inline(always)]
    fn collect_into(&mut self, out: &mut Vec<u8>) {
        self.raw_message.collect_into(out);
        self.db_path.collect_into(out);
        self.origin_node_id.collect_into(out);
    }

    #[inline(always)]
    fn reserve(&mut self, additional: NonZeroUsize) {
        self.raw_message.reserve(additional);
        self.db_path.reserve(additional);
        self.origin_node_id.reserve(additional);
    }
}

/// `ReducerCallRequest` 的 `bitcode` 原生解码表示。
#[derive(Debug, Clone, PartialEq, Eq, Decode)]
pub struct WireReducerCallRequest {
    pub raw_message: Vec<u8>,
    pub db_path: String,
    pub origin_node_id: u64,
}

impl From<WireReducerCallRequest> for ReducerCallRequest {
    #[inline(always)]
    fn from(w: WireReducerCallRequest) -> Self {
        Self {
            raw_message: Bytes::from(w.raw_message),
            db_path: w.db_path,
            origin_node_id: w.origin_node_id,
        }
    }
}

impl Encode for ReducerCallRequest {
    type Encoder = ReducerCallRequestEncoder;
}

impl<'a> Decode<'a> for ReducerCallRequest {
    type Decoder = WireDecoder<'a, WireReducerCallRequest>;
}

impl From<&WriteAck> for WireLogId {
    fn from(ack: &WriteAck) -> Self {
        Self::from(ack.log_id)
    }
}

impl From<WireLogId> for WriteAck {
    fn from(w: WireLogId) -> Self {
        Self {
            log_id: RaftLogId::from(w),
        }
    }
}

impl_bitcode_via!(WriteAck, WireLogId);
