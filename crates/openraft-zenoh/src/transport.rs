//! 基于 Zenoh + 明文 QUIC (`udp/<addr>#rel=1`) 与 `bitcode` 的低延迟 RPC 传输层。
//!
//! 针对 OpenRaft 共识与分布式状态机场景做了三项关键调优：
//! 1. 显式关闭查询合并（`ConsolidationMode::None`）并指定 `QueryTarget::BestMatching`，收到首个应答立即返回，消除聚合等待尾延迟；
//! 2. 支持 Zenoh 8 级 QoS 优先级（`Priority::RealTime` 心跳/选举插队、`Priority::InteractiveHigh` 日志复制、`Priority::Data` 快照传输）；
//! 3. 基于 `crossfire` 有界通道实现 `IntoHandler`，全链路无 `tokio`、无 `flume`。

use std::{
    borrow::Cow, marker::PhantomData, ops::Deref, result, sync::Arc, thread, time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use bitcode::{DecodeOwned, Encode};
use compio::time::sleep;
use crossfire::{
    MAsyncRx,
    mpmc::{Array, bounded_blocking_async},
};
use gxhash::{HashMap, HashMapExt};
use itoa::Buffer as ItoaBuffer;
use parking_lot::Mutex;
use zenoh::{
    Session, Wait,
    bytes::ZBytes,
    config::Config,
    handlers::{Callback, IntoHandler},
    qos::{CongestionControl, Priority},
    query::{ConsolidationMode, Query, QueryTarget},
};

/// 键表达式路径分隔符。
pub const KEY_SEP: &str = "/";
/// 键表达式多级通配符。
pub const KEY_WILDCARD: &str = "**";

/// 未指定 IPv4 监听地址前缀。
pub const UNSPECIFIED_IPV4_PREFIX: &str = "0.0.0.0:";
/// 本地回环 IPv4 地址前缀。
pub const LOOPBACK_IPV4_PREFIX: &str = "127.0.0.1:";

/// Zenoh 明文 QUIC 协议前缀。
pub const QUIC_SCHEME_PREFIX: &str = "udp/";
/// Zenoh 明文 QUIC 可靠流参数后缀 (`LinkUnicastQuicUnsecure`)。
pub const QUIC_REL_SUFFIX: &str = "#rel=1";

/// Zenoh 配置键与编译期静态 JSON5 字面量。
pub const ZENOH_MODE_KEY: &str = "mode";
pub const ZENOH_MODE_PEER: &str = "\"peer\"";
pub const ZENOH_MODE_CLIENT: &str = "\"client\"";
pub const ZENOH_MULTICAST_KEY: &str = "scouting/multicast/enabled";
pub const ZENOH_GOSSIP_KEY: &str = "scouting/gossip/enabled";
pub const ZENOH_LISTEN_KEY: &str = "listen/endpoints";
pub const ZENOH_CONNECT_KEY: &str = "connect/endpoints";
pub const ZENOH_FALSE: &str = "false";
pub const ZENOH_EMPTY_ARRAY: &str = "[]";
const JSON_ARRAY_PREFIX: &str = "[\"";
const JSON_ARRAY_SUFFIX: &str = "\"]";
const JSON_QUIC_PREFIX: &str = const_str::concat!(JSON_ARRAY_PREFIX, QUIC_SCHEME_PREFIX);
const JSON_QUIC_SUFFIX: &str = const_str::concat!(QUIC_REL_SUFFIX, JSON_ARRAY_SUFFIX);

/// Raft 共识 RPC 命名空间与子命令。
pub const RAFT_NS: &str = "raft";
pub const RAFT_PREFIX: &str = const_str::concat!(RAFT_NS, KEY_SEP);
pub const RAFT_QUERYABLE_ALL: &str = const_str::concat!(RAFT_PREFIX, KEY_WILDCARD);
pub const RPC_APPEND: &str = "append";
pub const RPC_VOTE: &str = "vote";
pub const RPC_SNAPSHOT: &str = "snapshot";

/// 用编译期静态前缀与 `itoa` 栈缓冲拼装 `{prefix}{id}`（单次预分配，零 `format!` 开销）。
#[inline]
pub fn prefix_u64_key(prefix: &str, id: u64) -> String {
    let mut ibuf = ItoaBuffer::new();
    let id_str = ibuf.format(id);
    let mut out = String::with_capacity(prefix.len() + id_str.len());
    out.push_str(prefix);
    out.push_str(id_str);
    out
}

/// 用编译期静态前后缀与 `itoa` 栈缓冲拼装 `{prefix}{id}{suffix}`（单次预分配，零 `format!` 开销）。
#[inline]
pub fn mid_u64_key(prefix: &str, id: u64, suffix: &str) -> String {
    let mut ibuf = ItoaBuffer::new();
    let id_str = ibuf.format(id);
    let mut out = String::with_capacity(prefix.len() + id_str.len() + suffix.len());
    out.push_str(prefix);
    out.push_str(id_str);
    out.push_str(suffix);
    out
}

/// 基于 `see::sync` 的无锁/轻量级状态监听通道（替代 `tokio::sync::watch`）。
pub mod watch {
    use see::{
        error::{RecvError, SendError},
        sync,
    };

    use super::Deref;

    /// 创建初始值为 `init` 的状态广播通道。
    pub fn channel<T>(init: T) -> (Sender<T>, Receiver<T>) {
        let (tx, rx) = sync::channel(init);
        (Sender(tx), Receiver(rx))
    }

    /// 状态广播发送端。
    #[derive(Clone)]
    pub struct Sender<T>(sync::Sender<T>);

    impl<T> Sender<T> {
        /// 发送新状态并唤醒所有订阅者。
        pub fn send(&self, value: T) -> Result<(), T> {
            self.0.send(value).map_err(|e| match e {
                SendError::ChannelClosed(v) => v,
            })
        }

        /// 无条件替换当前状态并唤醒所有订阅者。
        pub fn send_replace(&self, value: T) {
            let _ = self.0.send_replace(value);
        }

        /// 就地按条件修改状态，仅当返回 `true` 时唤醒订阅者。
        pub fn send_if_modified<F>(&self, modify: F) -> bool
        where
            F: FnOnce(&mut T) -> bool,
        {
            self.0.send_if_modified(modify)
        }

        /// 只读借用当前状态。
        pub fn borrow(&self) -> impl Deref<Target = T> + '_ {
            self.0.borrow()
        }

        /// 派生一个新的状态接收端。
        pub fn subscribe(&self) -> Receiver<T> {
            Receiver(self.0.subscribe())
        }
    }

    /// 状态广播接收端。
    #[derive(Clone)]
    pub struct Receiver<T>(sync::Receiver<T>);

    impl<T> Receiver<T> {
        /// 等待状态发生变更并自动标记已读（与 `openraft-rt-compio` 语义一致）。
        pub async fn changed(&mut self) -> Result<(), RecvError> {
            self.0.changed().await?;
            self.0.mark_unchanged();
            Ok(())
        }

        /// 借用当前状态并将其标记为已读。
        pub fn borrow_and_update(&mut self) -> impl Deref<Target = T> + '_ {
            self.0.borrow_and_update()
        }
    }
}

/// 标准 RPC 响应信封：`Ok(T)` 或错误消息。
pub type RpcEnvelope<T> = result::Result<T, String>;

/// Zenoh 回调有界通道默认容量。
pub const ZENOH_CHANNEL_CAPACITY: usize = 256;

/// 单次 RPC 查询的回复通道容量。
const RPC_REPLY_CAPACITY: usize = 4;

/// 基于 `crossfire` 的有界 Zenoh 接收端类型（`Send + Sync`，替代 Zenoh 默认基于 `flume` 的 `FifoChannelHandler`）。
pub type CrossfireHandler<T> = MAsyncRx<Array<T>>;

/// 基于 `crossfire` 的 Zenoh FIFO 回调构建器（替代 `zenoh::handlers::FifoChannel`）。
pub struct CrossfireChannel<T> {
    capacity: usize,
    _marker: PhantomData<T>,
}

impl<T> CrossfireChannel<T> {
    /// 使用默认容量 `ZENOH_CHANNEL_CAPACITY` 创建 `crossfire` 通道处理器。
    pub const fn new() -> Self {
        Self {
            capacity: ZENOH_CHANNEL_CAPACITY,
            _marker: PhantomData,
        }
    }

    /// 使用指定容量创建 `crossfire` 通道处理器。
    pub const fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity,
            _marker: PhantomData,
        }
    }
}

impl<T> Default for CrossfireChannel<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Send + 'static> IntoHandler<T> for CrossfireChannel<T> {
    type Handler = CrossfireHandler<T>;

    fn into_handler(self) -> (Callback<T>, Self::Handler) {
        let (tx, rx) = bounded_blocking_async(self.capacity);
        (
            Callback::from(move |t: T| {
                let _ = tx.send(t);
            }),
            rx,
        )
    }
}

/// 默认 RPC 请求超时。
const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(5);

/// 连接失效重试前的退避等待时间。
const RETRY_BACKOFF: Duration = Duration::from_millis(25);

/// 待剥离的协议前缀列表。
const URL_PREFIXES: [&str; 7] = [
    "http://", "https://", "ws://", "wss://", "quic/", "udp/", "tcp/",
];

/// 零分配剥离 URL 协议前缀、路径与 `#` 参数，返回切片 `host:port`。
#[inline]
pub fn strip_addr_parts(addr: &str) -> &str {
    let mut s = addr.trim();
    for prefix in URL_PREFIXES {
        if let Some(rest) = s.strip_prefix(prefix) {
            s = rest;
            break;
        }
    }
    if let Some((host_port, _)) = s.split_once('#') {
        s = host_port;
    }
    if let Some((host_port, _)) = s.split_once('/') {
        s = host_port;
    }
    s
}

/// 剥离 URL 协议前缀、路径与 `#` 参数，规范化为 `Cow<'_, str>`（非 `0.0.0.0` 地址零堆分配借用）。
#[inline]
pub fn normalize_addr_cow(addr: &str, for_connect: bool) -> Cow<'_, str> {
    let s = strip_addr_parts(addr);
    if for_connect && let Some(port) = s.strip_prefix(UNSPECIFIED_IPV4_PREFIX) {
        let mut out = String::with_capacity(LOOPBACK_IPV4_PREFIX.len() + port.len());
        out.push_str(LOOPBACK_IPV4_PREFIX);
        out.push_str(port);
        return Cow::Owned(out);
    }
    Cow::Borrowed(s)
}

/// 剥离 URL 协议前缀、路径与 `#` 参数，规范化为 `host:port`（单次预分配，无 `format!` 开销）。
///
/// 若 `for_connect` 为 `true`，将 `0.0.0.0:port` 替换为 `127.0.0.1:port`，
/// 防止节点向未指定监听地址发起连接。
pub fn normalize_addr(addr: &str, for_connect: bool) -> String {
    normalize_addr_cow(addr, for_connect).into_owned()
}

/// 单次预分配生成 Zenoh 明文 QUIC (`LinkUnicastQuicUnsecure`) JSON 端点数组：`["udp/<host:port>#rel=1"]`。
fn json_quic_endpoint(addr: &str, for_connect: bool) -> String {
    let s = strip_addr_parts(addr);
    let (host_prefix, host_rest) =
        if for_connect && let Some(port) = s.strip_prefix(UNSPECIFIED_IPV4_PREFIX) {
            (LOOPBACK_IPV4_PREFIX, port)
        } else {
            ("", s)
        };
    let mut out = String::with_capacity(
        JSON_QUIC_PREFIX.len() + host_prefix.len() + host_rest.len() + JSON_QUIC_SUFFIX.len(),
    );
    out.push_str(JSON_QUIC_PREFIX);
    out.push_str(host_prefix);
    out.push_str(host_rest);
    out.push_str(JSON_QUIC_SUFFIX);
    out
}

/// 创建基础 Zenoh 配置（关闭多播与 gossip 发现，仅走显式明文 QUIC 端点）。
fn base_zenoh_config(quoted_mode: &str) -> Result<Config> {
    let mut cfg = Config::default();
    cfg.insert_json5(ZENOH_MODE_KEY, quoted_mode)
        .map_err(|e| anyhow!("zenoh mode config: {e}"))?;
    cfg.insert_json5(ZENOH_MULTICAST_KEY, ZENOH_FALSE)
        .map_err(|e| anyhow!("zenoh multicast config: {e}"))?;
    cfg.insert_json5(ZENOH_GOSSIP_KEY, ZENOH_FALSE)
        .map_err(|e| anyhow!("zenoh gossip config: {e}"))?;
    Ok(cfg)
}

/// 打开监听指定明文 QUIC 地址的服务端 Zenoh 会话。
pub async fn open_server_session(listen_addr: &str) -> Result<Session> {
    let endpoint_json = json_quic_endpoint(listen_addr, false);
    let mut cfg = base_zenoh_config(ZENOH_MODE_PEER)?;
    cfg.insert_json5(ZENOH_LISTEN_KEY, &endpoint_json)
        .map_err(|e| anyhow!("zenoh listen config: {e}"))?;
    cfg.insert_json5(ZENOH_CONNECT_KEY, ZENOH_EMPTY_ARRAY)
        .map_err(|e| anyhow!("zenoh connect config: {e}"))?;
    zenoh::open(cfg)
        .await
        .map_err(|e| anyhow!("Failed to open Zenoh server session on {endpoint_json}: {e}"))
}

/// 打开连接到指定明文 QUIC 地址的客户端 Zenoh 会话。
pub async fn open_client_session(target_addr: &str) -> Result<Session> {
    let endpoint_json = json_quic_endpoint(target_addr, true);
    let mut cfg = base_zenoh_config(ZENOH_MODE_CLIENT)?;
    cfg.insert_json5(ZENOH_LISTEN_KEY, ZENOH_EMPTY_ARRAY)
        .map_err(|e| anyhow!("zenoh listen config: {e}"))?;
    cfg.insert_json5(ZENOH_CONNECT_KEY, &endpoint_json)
        .map_err(|e| anyhow!("zenoh connect config: {e}"))?;
    zenoh::open(cfg)
        .await
        .map_err(|e| anyhow!("Failed to connect Zenoh client session to {endpoint_json}: {e}"))
}

/// 非阻塞关闭 Zenoh 会话：
/// 注意 `zenoh::Session::close()` 的 `IntoFuture` 内部直接调用 `tokio::time::timeout`，在 `compio` 执行器上直接 `.await` 会因缺少 Tokio reactor 而 panic；
/// 因此必须在后台线程通过 `Wait::wait()` 切入 Zenoh 内部 `ZRuntime` 完成关闭，既不阻塞 `compio` 事件循环，也不依赖调用方具备 Tokio 上下文。
pub fn close_session(session: &Session) {
    let s = session.clone();
    drop(thread::spawn(move || {
        let _ = s.close().wait();
    }));
}

/// 建立短连接客户端会话，发送单次 `bitcode` RPC 请求后安全关闭会话（供 CLI 等单次命令调用）。
pub async fn oneshot_rpc<Req, Resp>(
    target_addr: &str,
    key_expr: &str,
    req: &Req,
    timeout: Duration,
) -> Result<Resp>
where
    Req: Encode + ?Sized,
    Resp: DecodeOwned,
{
    let session = open_client_session(target_addr).await?;
    let res = rpc_with_retry(&session, key_expr, req, timeout).await;
    close_session(&session);
    res
}

/// 序列化响应并回复 Zenoh 查询。
pub async fn reply_bitcode<T: Encode>(query: &Query, result: RpcEnvelope<T>) {
    let bytes = bitcode::encode(&result);
    let _ = query.reply(query.key_expr(), ZBytes::from(bytes)).await;
}

/// 解析 Zenoh 查询负载中的 `bitcode` 请求。
pub fn decode_query_payload<T: DecodeOwned>(query: &Query) -> Result<T> {
    let payload = query
        .payload()
        .ok_or_else(|| anyhow!("Missing payload on query {}", query.key_expr()))?;
    let bytes = payload.to_bytes();
    bitcode::decode(&bytes).context("Failed to decode bitcode payload")
}

/// 解析 Zenoh 查询负载并将错误转为 `String`（供 RPC 服务端处理器统一使用）。
#[inline]
pub fn decode_req<T: DecodeOwned>(query: &Query) -> result::Result<T, String> {
    decode_query_payload(query).map_err(|e| e.to_string())
}

/// 在已连接的客户端会话上发起单次 `bitcode` 编码的 Zenoh 查询（默认 `Priority::InteractiveHigh`，严格单发 `attempts = 1`，修复 R3/Point-1）。
pub async fn rpc_once<Req, Resp>(
    session: &Session,
    key_expr: &str,
    req: &Req,
    timeout: Duration,
) -> Result<Resp>
where
    Req: Encode + ?Sized,
    Resp: DecodeOwned,
{
    let payload = bitcode::encode(req);
    rpc_payload_with_retry(
        session,
        key_expr,
        &payload,
        timeout,
        Priority::InteractiveHigh,
        1,
    )
    .await
}

/// 在刚建立的客户端会话上发起幂等握手/查询（若对端 Queryable 尚未完成路由同步则自动退避重试最多 5 次）。
pub async fn rpc_with_retry<Req, Resp>(
    session: &Session,
    key_expr: &str,
    req: &Req,
    timeout: Duration,
) -> Result<Resp>
where
    Req: Encode + ?Sized,
    Resp: DecodeOwned,
{
    let payload = bitcode::encode(req);
    rpc_payload_with_retry(
        session,
        key_expr,
        &payload,
        timeout,
        Priority::InteractiveHigh,
        5,
    )
    .await
}

/// 使用已编码的 `bitcode` 字节切片在客户端会话上发起查询：
/// - 显式设置 `ConsolidationMode::None` 与 `QueryTarget::BestMatching`，消除结果合并等待延迟；
/// - 显式设置 `Priority` 与 `CongestionControl::Block`，保证心跳/投票高优插队且大数据块不丢包；
/// - `attempts = 1` 时严格单次发送（用于非幂等写）；`attempts > 1` 时允许在远端 Queryable 尚未就绪时退避重试（用于幂等 RPC）。
async fn rpc_payload_with_retry<Resp: DecodeOwned>(
    session: &Session,
    key_expr: &str,
    payload: &[u8],
    timeout: Duration,
    priority: Priority,
    attempts: usize,
) -> Result<Resp> {
    for attempt in 0..attempts.max(1) {
        if attempt > 0 {
            sleep(RETRY_BACKOFF).await;
        }
        let replies = session
            .get(key_expr)
            .payload(ZBytes::from(payload))
            .consolidation(ConsolidationMode::None)
            .target(QueryTarget::BestMatching)
            .priority(priority)
            .congestion_control(CongestionControl::Block)
            .timeout(timeout)
            .with(CrossfireChannel::with_capacity(RPC_REPLY_CAPACITY))
            .await
            .map_err(|e| anyhow!("Zenoh get({key_expr}) failed: {e}"))?;

        if let Ok(reply) = replies.recv().await {
            let sample = reply
                .into_result()
                .map_err(|e| anyhow!("Zenoh query error on {key_expr}: {e}"))?;

            let bytes = sample.payload().to_bytes();
            let envelope: RpcEnvelope<Resp> =
                bitcode::decode(&bytes).context("Failed to decode bitcode RPC response")?;
            return envelope.map_err(|msg| anyhow!("{msg}"));
        }
    }

    bail!("No reply received for {key_expr}")
}

/// 客户端 Zenoh 会话连接池（按对端 `host:port` 复用明文 QUIC 连接）。
#[derive(Clone, Default)]
pub struct ZenohClientPool {
    sessions: Arc<Mutex<HashMap<String, Session>>>,
}

impl ZenohClientPool {
    /// 创建空连接池。
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// 获取或建立到目标地址的客户端会话（命中连接池时借用 `&str` 查找，零堆分配）。
    pub async fn get_or_connect(&self, target_addr: &str) -> Result<Session> {
        let key = normalize_addr_cow(target_addr, true);
        if let Some(existing) = self.sessions.lock().get(key.as_ref())
            && !existing.is_closed()
        {
            return Ok(existing.clone());
        }

        let session = open_client_session(key.as_ref()).await?;
        let duplicate = {
            let mut guard = self.sessions.lock();
            if let Some(existing) = guard.get(key.as_ref())
                && !existing.is_closed()
            {
                Some(existing.clone())
            } else {
                guard.insert(key.into_owned(), session.clone());
                None
            }
        };
        if let Some(existing) = duplicate {
            close_session(&session);
            return Ok(existing);
        }
        Ok(session)
    }

    /// 驱逐失效的目标会话（零堆分配查找）。
    pub fn evict(&self, target_addr: &str) {
        let key = normalize_addr_cow(target_addr, true);
        let removed = self.sessions.lock().remove(key.as_ref());
        if let Some(s) = removed {
            close_session(&s);
        }
    }

    /// 向目标地址发送幂等 `bitcode` RPC 请求；若缓存连接断开则自动重连一次。
    pub async fn rpc<Req, Resp>(&self, target_addr: &str, key_expr: &str, req: &Req) -> Result<Resp>
    where
        Req: Encode + ?Sized,
        Resp: DecodeOwned,
    {
        self.rpc_with_qos(
            target_addr,
            key_expr,
            req,
            DEFAULT_RPC_TIMEOUT,
            Priority::InteractiveHigh,
        )
        .await
    }

    /// 向目标地址发送非幂等 `bitcode` RPC 请求（如 `/write`、`/write-batch`）：
    /// 失败时仅驱逐失效连接，绝不重发请求负载，防止应答丢失时重复执行（修复 R3）。
    pub async fn rpc_once<Req, Resp>(
        &self,
        target_addr: &str,
        key_expr: &str,
        req: &Req,
    ) -> Result<Resp>
    where
        Req: Encode + ?Sized,
        Resp: DecodeOwned,
    {
        let session = self.get_or_connect(target_addr).await?;
        let res = rpc_once(&session, key_expr, req, DEFAULT_RPC_TIMEOUT).await;
        if res.is_err() {
            self.evict(target_addr);
        }
        res
    }

    /// 带超时与 QoS 优先级的幂等 `bitcode` RPC 调用（单次编码，断线重连时直接复用同一份 `payload`）。
    pub async fn rpc_with_qos<Req, Resp>(
        &self,
        target_addr: &str,
        key_expr: &str,
        req: &Req,
        timeout: Duration,
        priority: Priority,
    ) -> Result<Resp>
    where
        Req: Encode + ?Sized,
        Resp: DecodeOwned,
    {
        let payload = bitcode::encode(req);
        let session = self.get_or_connect(target_addr).await?;
        match rpc_payload_with_retry(&session, key_expr, &payload, timeout, priority, 5).await {
            Ok(resp) => Ok(resp),
            Err(first_err) => {
                // 若因远端刚注册 queryable 或连接中断导致无响应，短暂等待后重试一次
                self.evict(target_addr);
                sleep(RETRY_BACKOFF).await;
                let session = match self.get_or_connect(target_addr).await {
                    Ok(s) => s,
                    Err(_) => bail!("{first_err}"),
                };
                rpc_payload_with_retry(&session, key_expr, &payload, timeout, priority, 5).await
            }
        }
    }
}
