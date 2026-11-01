//! `openraft 0.10` 与 `bytes::Bytes` 核心类型到 `bitcode` 原生编解码 (`#[derive(Encode, Decode)]`) 的零 `serde` 转换层。

use std::{
    collections::{BTreeMap, BTreeSet},
    io::Cursor,
    num::NonZeroUsize,
};

use bitcode::{
    __private::{Buffer, Decoder, Encoder, Result as BitcodeResult, View},
    Decode, Encode,
};
use bytes::Bytes;
use openraft::{
    AppData, BasicNode, Entry, EntryPayload, LogId, Membership, RaftTypeConfig, Snapshot,
    SnapshotMeta, StoredMembership, Vote,
    errors::{Fatal, RaftError},
    impls::leader_id_adv,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
    },
    vote::RaftLeaderId,
};

/// Raft 选主标识类型（支持同任期多选主对比）。
pub type LeaderId = leader_id_adv::LeaderId<u64, u64>;

/// Raft 日志条目唯一标识类型。
pub type RaftLogId = LogId<LeaderId>;

/// Raft 选票类型。
pub type RaftVote = Vote<LeaderId>;

/// 提取选票的任期号（编译期与运行期通用 `const fn`）。
#[inline]
pub const fn leader_term(vote: &RaftVote) -> u64 {
    vote.leader_id.term
}

/// Raft 持久化成员配置类型。
pub type RaftStoredMembership = StoredMembership<LeaderId, u64, BasicNode>;

/// Raft 快照元数据类型。
pub type RaftSnapshotMeta = SnapshotMeta<LeaderId, u64, BasicNode>;

/// Raft 快照类型。
pub type RaftSnapshot = Snapshot<LeaderId, u64, BasicNode, Cursor<Vec<u8>>>;

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

/// 为包含外部非 `bitcode` 字段（如 `bytes::Bytes` 或 `openraft::LogId`）的类型实现原生 `Encode` / `Decode`。
#[macro_export]
macro_rules! impl_bitcode_via {
    ($target:ty, $wire:ty) => {
        impl bitcode::Encode for $target {
            type Encoder = $crate::codec::WireEncoder<$wire>;
        }

        impl<'a> bitcode::Decode<'a> for $target {
            type Decoder = $crate::codec::WireDecoder<'a, $wire>;
        }
    };
}

pub use impl_bitcode_via;

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

impl From<WireStoredMembership> for RaftStoredMembership {
    fn from(w: WireStoredMembership) -> Self {
        Self::new(
            w.log_id.map(RaftLogId::from),
            Membership::from(w.membership),
        )
    }
}

/// `EntryPayload<D, u64, BasicNode>` 的泛型 `bitcode` 原生编码表示。
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum WireEntryPayload<D> {
    Blank,
    Normal(D),
    Membership(WireMembership),
}

impl<D: AppData> From<EntryPayload<D, u64, BasicNode>> for WireEntryPayload<D> {
    fn from(p: EntryPayload<D, u64, BasicNode>) -> Self {
        match p {
            EntryPayload::Blank => Self::Blank,
            EntryPayload::Normal(req) => Self::Normal(req),
            EntryPayload::Membership(m) => Self::Membership(WireMembership::from(&m)),
        }
    }
}

impl<D: AppData> From<WireEntryPayload<D>> for EntryPayload<D, u64, BasicNode> {
    fn from(w: WireEntryPayload<D>) -> Self {
        match w {
            WireEntryPayload::Blank => Self::Blank,
            WireEntryPayload::Normal(req) => Self::Normal(req),
            WireEntryPayload::Membership(m) => Self::Membership(Membership::from(m)),
        }
    }
}

/// `Entry<LeaderId, EntryPayload<D, u64, BasicNode>>` 的泛型 `bitcode` 原生编码表示。
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct WireEntry<D> {
    pub log_id: WireLogId,
    pub payload: WireEntryPayload<D>,
}

impl<D: AppData> From<Entry<LeaderId, EntryPayload<D, u64, BasicNode>>> for WireEntry<D> {
    fn from(e: Entry<LeaderId, EntryPayload<D, u64, BasicNode>>) -> Self {
        Self {
            log_id: WireLogId::from(e.log_id),
            payload: WireEntryPayload::from(e.payload),
        }
    }
}

impl<D: AppData> From<WireEntry<D>> for Entry<LeaderId, EntryPayload<D, u64, BasicNode>> {
    fn from(w: WireEntry<D>) -> Self {
        Self {
            log_id: RaftLogId::from(w.log_id),
            payload: EntryPayload::from(w.payload),
        }
    }
}

/// `AppendEntriesRequest<C>` 的泛型 `bitcode` 原生编码表示。
#[derive(Debug, Clone, Encode, Decode)]
pub struct WireAppendEntriesRequest<D> {
    pub vote: WireVote,
    pub prev_log_id: Option<WireLogId>,
    pub entries: Vec<WireEntry<D>>,
    pub leader_commit: Option<WireLogId>,
}

impl<C, D> From<AppendEntriesRequest<C>> for WireAppendEntriesRequest<D>
where
    D: AppData,
    C: RaftTypeConfig<
            D = D,
            NodeId = u64,
            Node = BasicNode,
            LeaderId = LeaderId,
            Vote = RaftVote,
            Entry = Entry<LeaderId, EntryPayload<D, u64, BasicNode>>,
        >,
{
    fn from(req: AppendEntriesRequest<C>) -> Self {
        Self {
            vote: WireVote::from(req.vote),
            prev_log_id: req.prev_log_id.map(WireLogId::from),
            entries: req.entries.into_iter().map(WireEntry::from).collect(),
            leader_commit: req.leader_commit.map(WireLogId::from),
        }
    }
}

impl<C, D> From<WireAppendEntriesRequest<D>> for AppendEntriesRequest<C>
where
    D: AppData,
    C: RaftTypeConfig<
            D = D,
            NodeId = u64,
            Node = BasicNode,
            LeaderId = LeaderId,
            Vote = RaftVote,
            Entry = Entry<LeaderId, EntryPayload<D, u64, BasicNode>>,
        >,
{
    fn from(w: WireAppendEntriesRequest<D>) -> Self {
        Self {
            vote: RaftVote::from(w.vote),
            prev_log_id: w.prev_log_id.map(RaftLogId::from),
            entries: w.entries.into_iter().map(Entry::from).collect(),
            leader_commit: w.leader_commit.map(RaftLogId::from),
        }
    }
}

/// `AppendEntriesResponse<C>` 的 `bitcode` 原生编码表示。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum WireAppendEntriesResponse {
    Success,
    PartialSuccess(Option<WireLogId>),
    Conflict,
    HigherVote(WireVote),
}

impl<C> From<AppendEntriesResponse<C>> for WireAppendEntriesResponse
where
    C: RaftTypeConfig<NodeId = u64, LeaderId = LeaderId, Vote = RaftVote>,
{
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

impl<C> From<WireAppendEntriesResponse> for AppendEntriesResponse<C>
where
    C: RaftTypeConfig<NodeId = u64, LeaderId = LeaderId, Vote = RaftVote>,
{
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

/// `VoteRequest<C>` 的 `bitcode` 原生编码表示。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub struct WireVoteRequest {
    pub vote: WireVote,
    pub last_log_id: Option<WireLogId>,
    pub leadership_transfer: bool,
}

impl<C> From<VoteRequest<C>> for WireVoteRequest
where
    C: RaftTypeConfig<NodeId = u64, LeaderId = LeaderId, Vote = RaftVote>,
{
    fn from(req: VoteRequest<C>) -> Self {
        Self {
            vote: WireVote::from(req.vote),
            last_log_id: req.last_log_id.map(WireLogId::from),
            leadership_transfer: req.leadership_transfer,
        }
    }
}

impl<C> From<WireVoteRequest> for VoteRequest<C>
where
    C: RaftTypeConfig<NodeId = u64, LeaderId = LeaderId, Vote = RaftVote>,
{
    fn from(w: WireVoteRequest) -> Self {
        Self {
            vote: RaftVote::from(w.vote),
            last_log_id: w.last_log_id.map(RaftLogId::from),
            leadership_transfer: w.leadership_transfer,
        }
    }
}

/// `VoteResponse<C>` 的 `bitcode` 原生编码表示。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub struct WireVoteResponse {
    pub vote: WireVote,
    pub vote_granted: bool,
    pub last_log_id: Option<WireLogId>,
}

impl<C> From<VoteResponse<C>> for WireVoteResponse
where
    C: RaftTypeConfig<NodeId = u64, LeaderId = LeaderId, Vote = RaftVote>,
{
    fn from(resp: VoteResponse<C>) -> Self {
        Self {
            vote: WireVote::from(resp.vote),
            vote_granted: resp.vote_granted,
            last_log_id: resp.last_log_id.map(WireLogId::from),
        }
    }
}

impl<C> From<WireVoteResponse> for VoteResponse<C>
where
    C: RaftTypeConfig<NodeId = u64, LeaderId = LeaderId, Vote = RaftVote>,
{
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
            last_membership: WireStoredMembership::from(&m.last_membership),
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

/// `SnapshotResponse<C>` 的 `bitcode` 原生编码表示。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub struct WireSnapshotResponse {
    pub vote: WireVote,
}

impl<C> From<SnapshotResponse<C>> for WireSnapshotResponse
where
    C: RaftTypeConfig<NodeId = u64, LeaderId = LeaderId, Vote = RaftVote>,
{
    fn from(resp: SnapshotResponse<C>) -> Self {
        Self {
            vote: WireVote::from(resp.vote),
        }
    }
}

impl<C> From<WireSnapshotResponse> for SnapshotResponse<C>
where
    C: RaftTypeConfig<NodeId = u64, LeaderId = LeaderId, Vote = RaftVote>,
{
    fn from(w: WireSnapshotResponse) -> Self {
        Self {
            vote: RaftVote::from(w.vote),
        }
    }
}

/// `Fatal<C>` / `RaftError<C>` 的 `bitcode` 原生编码表示。
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum WireFatal {
    StorageError(String),
    Panicked,
    Stopped,
}

impl<C: RaftTypeConfig> From<Fatal<C>> for WireFatal {
    fn from(f: Fatal<C>) -> Self {
        match f {
            Fatal::StorageError(e) => Self::StorageError(e.to_string()),
            Fatal::Panicked => Self::Panicked,
            Fatal::Stopped => Self::Stopped,
        }
    }
}

impl<C: RaftTypeConfig> From<RaftError<C>> for WireFatal {
    fn from(err: RaftError<C>) -> Self {
        match err {
            RaftError::APIError(infallible) => match infallible {},
            RaftError::Fatal(f) => Self::from(f),
        }
    }
}
