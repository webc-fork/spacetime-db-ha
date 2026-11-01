//! `openraft` 与 `bytes::Bytes` 相关类型到 `bitcode` 原生编解码 (`#[derive(Encode, Decode)]`) 的零 `serde` 转换层。

use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    num::NonZeroUsize,
};

use bitcode::{
    __private::{Buffer, Decoder, Encoder, Result as BitcodeResult, View},
    Decode, Encode,
};
use bytes::Bytes;
use openraft::{
    BasicNode, ErrorSubject, ErrorVerb, Membership, StorageError,
    entry::EntryPayload,
    errors::{Fatal, RaftError},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
    },
    vote::RaftLeaderId,
};

use crate::raft::types::{
    LeaderId, RaftEntry, RaftLogId, RaftSnapshotMeta, RaftStoredMembership, RaftVote,
    ReducerCallRequest, TypeConfig, WriteAck,
};

type C = TypeConfig;
type Payload = EntryPayload<ReducerCallRequest, u64, BasicNode>;

/// 将本地类型 `T` 委托给派生了 `#[derive(Encode, Decode)]` 的线格式 `W` 的通用编码器。
pub struct WireEncoder<W: Encode>(W::Encoder);

impl<W: Encode> Default for WireEncoder<W> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<T, W> Encoder<T> for WireEncoder<W>
where
    W: Encode + for<'a> From<&'a T>,
{
    #[inline(always)]
    fn encode(&mut self, t: &T) {
        self.0.encode(&W::from(t));
    }
}

impl<W: Encode> Buffer for WireEncoder<W> {
    #[inline(always)]
    fn collect_into(&mut self, out: &mut Vec<u8>) {
        self.0.collect_into(out);
    }

    #[inline(always)]
    fn reserve(&mut self, additional: NonZeroUsize) {
        self.0.reserve(additional);
    }
}

/// 将线格式 `W` 解码并转换为本地类型 `T` 的通用解码器。
pub struct WireDecoder<'a, W: Decode<'a>>(W::Decoder);

impl<'a, W: Decode<'a>> Default for WireDecoder<'a, W> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<'a, W: Decode<'a>> View<'a> for WireDecoder<'a, W> {
    fn populate(&mut self, input: &mut &'a [u8], length: usize) -> BitcodeResult<()> {
        self.0.populate(input, length)
    }
}

impl<'a, T, W> Decoder<'a, T> for WireDecoder<'a, W>
where
    W: Decode<'a>,
    T: From<W>,
{
    #[inline(always)]
    fn decode(&mut self) -> T {
        T::from(self.0.decode())
    }
}

/// 为包含外部非 `bitcode` 字段（如 `bytes::Bytes` 或 `openraft::LogId`）的本 crate 类型实现原生 `Encode` / `Decode`。
macro_rules! impl_bitcode_via {
    ($target:ty, $wire:ty) => {
        impl bitcode::Encode for $target {
            type Encoder = $crate::raft::codec::WireEncoder<$wire>;
        }

        impl<'a> bitcode::Decode<'a> for $target {
            type Decoder = $crate::raft::codec::WireDecoder<'a, $wire>;
        }
    };
}

pub(crate) use impl_bitcode_via;

/// `bytes::Bytes` 的零拷贝 `bitcode` 包装器：
/// - 克隆仅递增 `Bytes` 引用计数（O(1) 零堆分配）；
/// - 编码直接委托给 `<[u8] as Encode>::Encoder` 写入切片（消除 `.to_vec()` 临时堆分配）；
/// - 解码将 `Vec<u8>` 零拷贝移交为 `Bytes`。
#[derive(Debug, Clone, PartialEq, Eq)]
#[repr(transparent)]
pub struct WireBytes(pub Bytes);

#[derive(Default)]
pub struct WireBytesEncoder(<[u8] as Encode>::Encoder);

impl Encoder<WireBytes> for WireBytesEncoder {
    #[inline(always)]
    fn encode(&mut self, t: &WireBytes) {
        Encoder::<[u8]>::encode(&mut self.0, &t.0);
    }
}

impl Buffer for WireBytesEncoder {
    #[inline(always)]
    fn collect_into(&mut self, out: &mut Vec<u8>) {
        self.0.collect_into(out);
    }

    #[inline(always)]
    fn reserve(&mut self, additional: NonZeroUsize) {
        self.0.reserve(additional);
    }
}

impl Encode for WireBytes {
    type Encoder = WireBytesEncoder;
}

impl From<Vec<u8>> for WireBytes {
    #[inline(always)]
    fn from(v: Vec<u8>) -> Self {
        Self(Bytes::from(v))
    }
}

impl<'a> Decode<'a> for WireBytes {
    type Decoder = WireDecoder<'a, Vec<u8>>;
}

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

/// `RaftLogId` 的 `bitcode` 原生编码表示。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub struct WireLogId {
    pub term: u64,
    pub node_id: u64,
    pub index: u64,
}

impl From<RaftLogId> for WireLogId {
    fn from(id: RaftLogId) -> Self {
        Self {
            term: id.leader_id.term,
            node_id: id.leader_id.node_id,
            index: id.index,
        }
    }
}

impl From<&RaftLogId> for WireLogId {
    fn from(id: &RaftLogId) -> Self {
        Self::from(*id)
    }
}

impl From<WireLogId> for RaftLogId {
    fn from(w: WireLogId) -> Self {
        Self::new(LeaderId::new(w.term, w.node_id), w.index)
    }
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

/// `RaftVote` 的 `bitcode` 原生编码表示。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub struct WireVote {
    pub term: u64,
    pub node_id: u64,
    pub committed: bool,
}

impl From<RaftVote> for WireVote {
    fn from(v: RaftVote) -> Self {
        Self {
            term: v.leader_id.term,
            node_id: v.leader_id.node_id,
            committed: v.committed,
        }
    }
}

impl From<&RaftVote> for WireVote {
    fn from(v: &RaftVote) -> Self {
        Self::from(*v)
    }
}

impl From<WireVote> for RaftVote {
    fn from(w: WireVote) -> Self {
        if w.committed {
            Self::new_committed(w.term, w.node_id)
        } else {
            Self::new(w.term, w.node_id)
        }
    }
}

/// `Membership<u64, BasicNode>` 的 `bitcode` 原生编码表示。
#[derive(Debug, Default, Clone, PartialEq, Eq, Encode, Decode)]
pub struct WireMembership {
    pub configs: Vec<BTreeSet<u64>>,
    pub nodes: BTreeMap<u64, String>,
}

impl From<&Membership<u64, BasicNode>> for WireMembership {
    fn from(m: &Membership<u64, BasicNode>) -> Self {
        Self {
            configs: m.get_joint_config().clone(),
            nodes: m.nodes().map(|(&id, n)| (id, n.addr.clone())).collect(),
        }
    }
}

impl From<Membership<u64, BasicNode>> for WireMembership {
    fn from(m: Membership<u64, BasicNode>) -> Self {
        Self::from(&m)
    }
}

impl From<WireMembership> for Membership<u64, BasicNode> {
    fn from(w: WireMembership) -> Self {
        let nodes: BTreeMap<u64, BasicNode> = w
            .nodes
            .into_iter()
            .map(|(id, addr)| (id, BasicNode { addr }))
            .collect();
        Self::new(w.configs, nodes).unwrap_or_default()
    }
}

/// `RaftStoredMembership` 的 `bitcode` 原生编码表示。
#[derive(Debug, Default, Clone, PartialEq, Eq, Encode, Decode)]
pub struct WireStoredMembership {
    pub log_id: Option<WireLogId>,
    pub membership: WireMembership,
}

impl From<&RaftStoredMembership> for WireStoredMembership {
    fn from(sm: &RaftStoredMembership) -> Self {
        Self {
            log_id: (*sm.log_id()).map(WireLogId::from),
            membership: WireMembership::from(sm.membership()),
        }
    }
}

impl From<RaftStoredMembership> for WireStoredMembership {
    fn from(sm: RaftStoredMembership) -> Self {
        Self::from(&sm)
    }
}

impl From<WireStoredMembership> for RaftStoredMembership {
    fn from(w: WireStoredMembership) -> Self {
        Self::new(
            w.log_id.map(RaftLogId::from),
            Membership::from(w.membership),
        )
    }
}

/// `EntryPayload` 的 `bitcode` 原生编码表示。
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum WireEntryPayload {
    Blank,
    Normal(ReducerCallRequest),
    Membership(WireMembership),
}

impl From<Payload> for WireEntryPayload {
    fn from(p: Payload) -> Self {
        match p {
            EntryPayload::Blank => Self::Blank,
            EntryPayload::Normal(req) => Self::Normal(req),
            EntryPayload::Membership(m) => Self::Membership(WireMembership::from(&m)),
        }
    }
}

impl From<WireEntryPayload> for Payload {
    fn from(w: WireEntryPayload) -> Self {
        match w {
            WireEntryPayload::Blank => Self::Blank,
            WireEntryPayload::Normal(req) => Self::Normal(req),
            WireEntryPayload::Membership(m) => Self::Membership(Membership::from(m)),
        }
    }
}

/// `RaftEntry` 的 `bitcode` 原生编码表示。
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct WireEntry {
    pub log_id: WireLogId,
    pub payload: WireEntryPayload,
}

impl From<RaftEntry> for WireEntry {
    fn from(e: RaftEntry) -> Self {
        Self {
            log_id: WireLogId::from(e.log_id),
            payload: WireEntryPayload::from(e.payload),
        }
    }
}

impl From<WireEntry> for RaftEntry {
    fn from(w: WireEntry) -> Self {
        Self {
            log_id: RaftLogId::from(w.log_id),
            payload: Payload::from(w.payload),
        }
    }
}

/// `AppendEntriesRequest<TypeConfig>` 的 `bitcode` 原生编码表示。
#[derive(Debug, Clone, Encode, Decode)]
pub struct WireAppendEntriesRequest {
    pub vote: WireVote,
    pub prev_log_id: Option<WireLogId>,
    pub entries: Vec<WireEntry>,
    pub leader_commit: Option<WireLogId>,
}

impl From<AppendEntriesRequest<C>> for WireAppendEntriesRequest {
    fn from(req: AppendEntriesRequest<C>) -> Self {
        Self {
            vote: WireVote::from(req.vote),
            prev_log_id: req.prev_log_id.map(WireLogId::from),
            entries: req.entries.into_iter().map(WireEntry::from).collect(),
            leader_commit: req.leader_commit.map(WireLogId::from),
        }
    }
}

impl From<WireAppendEntriesRequest> for AppendEntriesRequest<C> {
    fn from(w: WireAppendEntriesRequest) -> Self {
        Self {
            vote: RaftVote::from(w.vote),
            prev_log_id: w.prev_log_id.map(RaftLogId::from),
            entries: w.entries.into_iter().map(RaftEntry::from).collect(),
            leader_commit: w.leader_commit.map(RaftLogId::from),
        }
    }
}

/// `AppendEntriesResponse<TypeConfig>` 的 `bitcode` 原生编码表示。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum WireAppendEntriesResponse {
    Success,
    PartialSuccess(Option<WireLogId>),
    Conflict,
    HigherVote(WireVote),
}

impl From<AppendEntriesResponse<C>> for WireAppendEntriesResponse {
    fn from(resp: AppendEntriesResponse<C>) -> Self {
        match resp {
            AppendEntriesResponse::Success => Self::Success,
            AppendEntriesResponse::PartialSuccess(id) => {
                Self::PartialSuccess(id.map(WireLogId::from))
            }
            AppendEntriesResponse::Conflict => Self::Conflict,
            AppendEntriesResponse::HigherVote(v) => Self::HigherVote(WireVote::from(v)),
        }
    }
}

impl From<WireAppendEntriesResponse> for AppendEntriesResponse<C> {
    fn from(w: WireAppendEntriesResponse) -> Self {
        match w {
            WireAppendEntriesResponse::Success => Self::Success,
            WireAppendEntriesResponse::PartialSuccess(id) => {
                Self::PartialSuccess(id.map(RaftLogId::from))
            }
            WireAppendEntriesResponse::Conflict => Self::Conflict,
            WireAppendEntriesResponse::HigherVote(v) => Self::HigherVote(RaftVote::from(v)),
        }
    }
}

/// `VoteRequest<TypeConfig>` 的 `bitcode` 原生编码表示。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub struct WireVoteRequest {
    pub vote: WireVote,
    pub last_log_id: Option<WireLogId>,
    pub leadership_transfer: bool,
}

impl From<VoteRequest<C>> for WireVoteRequest {
    fn from(req: VoteRequest<C>) -> Self {
        Self {
            vote: WireVote::from(req.vote),
            last_log_id: req.last_log_id.map(WireLogId::from),
            leadership_transfer: req.leadership_transfer,
        }
    }
}

impl From<WireVoteRequest> for VoteRequest<C> {
    fn from(w: WireVoteRequest) -> Self {
        Self {
            vote: RaftVote::from(w.vote),
            last_log_id: w.last_log_id.map(RaftLogId::from),
            leadership_transfer: w.leadership_transfer,
        }
    }
}

/// `VoteResponse<TypeConfig>` 的 `bitcode` 原生编码表示。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub struct WireVoteResponse {
    pub vote: WireVote,
    pub vote_granted: bool,
    pub last_log_id: Option<WireLogId>,
}

impl From<VoteResponse<C>> for WireVoteResponse {
    fn from(resp: VoteResponse<C>) -> Self {
        Self {
            vote: WireVote::from(resp.vote),
            vote_granted: resp.vote_granted,
            last_log_id: resp.last_log_id.map(WireLogId::from),
        }
    }
}

impl From<WireVoteResponse> for VoteResponse<C> {
    fn from(w: WireVoteResponse) -> Self {
        Self {
            vote: RaftVote::from(w.vote),
            vote_granted: w.vote_granted,
            last_log_id: w.last_log_id.map(RaftLogId::from),
        }
    }
}

/// `RaftSnapshotMeta` 的 `bitcode` 原生编码表示。
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct WireSnapshotMeta {
    pub last_log_id: Option<WireLogId>,
    pub last_membership: WireStoredMembership,
}

impl From<RaftSnapshotMeta> for WireSnapshotMeta {
    fn from(m: RaftSnapshotMeta) -> Self {
        Self {
            last_log_id: m.last_log_id.map(WireLogId::from),
            last_membership: WireStoredMembership::from(m.last_membership),
        }
    }
}

impl From<WireSnapshotMeta> for RaftSnapshotMeta {
    fn from(w: WireSnapshotMeta) -> Self {
        Self {
            last_log_id: w.last_log_id.map(RaftLogId::from),
            last_membership: RaftStoredMembership::from(w.last_membership),
        }
    }
}

/// `RaftNetworkV2::full_snapshot` 请求的 `bitcode` 原生编码表示。
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct WireFullSnapshotRequest {
    pub vote: WireVote,
    pub meta: WireSnapshotMeta,
    pub data: Vec<u8>,
}

/// `SnapshotResponse<TypeConfig>` 的 `bitcode` 原生编码表示。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub struct WireSnapshotResponse {
    pub vote: WireVote,
}

impl From<SnapshotResponse<C>> for WireSnapshotResponse {
    fn from(resp: SnapshotResponse<C>) -> Self {
        Self {
            vote: WireVote::from(resp.vote),
        }
    }
}

impl From<WireSnapshotResponse> for SnapshotResponse<C> {
    fn from(w: WireSnapshotResponse) -> Self {
        Self {
            vote: RaftVote::from(w.vote),
        }
    }
}

/// `Fatal<TypeConfig>` / `RaftError<TypeConfig>` 的 `bitcode` 原生编码表示。
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum WireFatal {
    StorageError(String),
    Panicked,
    Stopped,
}

impl From<Fatal<C>> for WireFatal {
    fn from(f: Fatal<C>) -> Self {
        match f {
            Fatal::StorageError(e) => Self::StorageError(e.to_string()),
            Fatal::Panicked => Self::Panicked,
            Fatal::Stopped => Self::Stopped,
        }
    }
}

impl From<WireFatal> for Fatal<C> {
    fn from(w: WireFatal) -> Self {
        match w {
            WireFatal::StorageError(msg) => Self::StorageError(StorageError::from_io_error(
                ErrorSubject::Store,
                ErrorVerb::Read,
                io::Error::other(msg),
            )),
            WireFatal::Panicked => Self::Panicked,
            WireFatal::Stopped => Self::Stopped,
        }
    }
}

impl From<RaftError<C>> for WireFatal {
    fn from(err: RaftError<C>) -> Self {
        match err {
            RaftError::APIError(infallible) => match infallible {},
            RaftError::Fatal(f) => Self::from(f),
        }
    }
}

impl From<WireFatal> for RaftError<C> {
    fn from(w: WireFatal) -> Self {
        Self::Fatal(Fatal::from(w))
    }
}
