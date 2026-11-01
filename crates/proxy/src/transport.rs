//! 基于 Zenoh + 明文 QUIC (`udp/<addr>#rel=1`) 与 `bitcode` 的传输层。

use std::{marker::PhantomData, ops::Deref, result, sync::Arc, time::Duration};

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
    query::Query,
};

// ============================================================================
// 编译期公共协议常量与 Zenoh 键表达式（基于 const_str 组合）
// ============================================================================

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

/// Raft 共识 RPC 命名空间与子命令。
pub const RAFT_NS: &str = "raft";
pub const RAFT_PREFIX: &str = const_str::concat!(RAFT_NS, KEY_SEP);
pub const RAFT_QUERYABLE_ALL: &str = const_str::concat!(RAFT_PREFIX, KEY_WILDCARD);
pub const RPC_APPEND: &str = "append";
pub const RPC_VOTE: &str = "vote";
pub const RPC_SNAPSHOT: &str = "snapshot";

/// 集群管理与跨节点写转发命名空间与子路径。
pub const CLUSTER_NS: &str = "cluster";
pub const CLUSTER_PREFIX: &str = const_str::concat!(CLUSTER_NS, KEY_SEP);
pub const CLUSTER_QUERYABLE_ALL: &str = const_str::concat!(CLUSTER_PREFIX, KEY_WILDCARD);

pub const SUB_STATUS: &str = "status";
pub const SUB_INIT: &str = "init";
pub const SUB_ADD_NODE: &str = "add-node";
pub const SUB_REMOVE_NODE: &str = "remove-node";
pub const SUB_LEADER: &str = "leader";
pub const SUB_HEALTH: &str = "health";
pub const SUB_WRITE: &str = "write";
pub const SUB_SHARDS: &str = "shards";
pub const SUB_SHARDS_PREFIX: &str = const_str::concat!(SUB_SHARDS, KEY_SEP);
pub const SUB_SHARDS_CREATE: &str = const_str::concat!(SUB_SHARDS_PREFIX, "create");
pub const SUB_SHARDS_REMOVE: &str = const_str::concat!(SUB_SHARDS_PREFIX, "remove");
pub const SUB_SHARDS_ROUTE: &str = const_str::concat!(SUB_SHARDS_PREFIX, "route");
pub const SUB_SHARDS_ROUTE_ADD: &str = const_str::concat!(SUB_SHARDS_ROUTE, "/add");
pub const SUB_SHARDS_ROUTE_REMOVE: &str = const_str::concat!(SUB_SHARDS_ROUTE, "/remove");

pub const SUFFIX_WRITE: &str = const_str::concat!(KEY_SEP, SUB_WRITE);
pub const SUFFIX_STATUS: &str = const_str::concat!(KEY_SEP, SUB_STATUS);

pub const KEY_CLUSTER_STATUS: &str = const_str::concat!(CLUSTER_PREFIX, SUB_STATUS);
pub const KEY_CLUSTER_INIT: &str = const_str::concat!(CLUSTER_PREFIX, SUB_INIT);
pub const KEY_CLUSTER_ADD_NODE: &str = const_str::concat!(CLUSTER_PREFIX, SUB_ADD_NODE);
pub const KEY_CLUSTER_REMOVE_NODE: &str = const_str::concat!(CLUSTER_PREFIX, SUB_REMOVE_NODE);
pub const KEY_CLUSTER_LEADER: &str = const_str::concat!(CLUSTER_PREFIX, SUB_LEADER);
pub const KEY_CLUSTER_HEALTH: &str = const_str::concat!(CLUSTER_PREFIX, SUB_HEALTH);
pub const KEY_CLUSTER_WRITE: &str = const_str::concat!(CLUSTER_PREFIX, SUB_WRITE);
pub const KEY_CLUSTER_SHARDS: &str = const_str::concat!(CLUSTER_PREFIX, SUB_SHARDS);
pub const KEY_CLUSTER_SHARDS_PREFIX: &str = const_str::concat!(CLUSTER_PREFIX, SUB_SHARDS_PREFIX);
pub const KEY_SHARDS_CREATE: &str = const_str::concat!(CLUSTER_PREFIX, SUB_SHARDS_CREATE);
pub const KEY_SHARDS_REMOVE: &str = const_str::concat!(CLUSTER_PREFIX, SUB_SHARDS_REMOVE);
pub const KEY_SHARDS_ROUTE_ADD: &str = const_str::concat!(CLUSTER_PREFIX, SUB_SHARDS_ROUTE_ADD);
pub const KEY_SHARDS_ROUTE_REMOVE: &str =
    const_str::concat!(CLUSTER_PREFIX, SUB_SHARDS_ROUTE_REMOVE);

/// 指标查询键表达式。
pub const KEY_METRICS: &str = "metrics";

/// SpacetimeDB 客户端与上游流命名空间与键前缀。
pub const STDB_NS: &str = "stdb";
pub const STDB_PREFIX: &str = const_str::concat!(STDB_NS, KEY_SEP);
pub const KEY_STDB_CONNECT: &str = const_str::concat!(STDB_PREFIX, "connect");
pub const STDB_C2S_PREFIX: &str = const_str::concat!(STDB_PREFIX, "c2s/");
pub const STDB_S2C_PREFIX: &str = const_str::concat!(STDB_PREFIX, "s2c/");

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

        /// 只读借用当前状态。
        pub fn borrow(&self) -> impl Deref<Target = T> + '_ {
            self.0.borrow()
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

/// 剥离 URL 协议前缀、路径与 `#` 参数，规范化为 `host:port`（单次预分配，无 `format!` 开销）。
///
/// 若 `for_connect` 为 `true`，将 `0.0.0.0:port` 替换为 `127.0.0.1:port`，
/// 防止节点向未指定监听地址发起连接。
pub fn normalize_addr(addr: &str, for_connect: bool) -> String {
    let s = strip_addr_parts(addr);
    if for_connect && let Some(port) = s.strip_prefix(UNSPECIFIED_IPV4_PREFIX) {
        let mut out = String::with_capacity(LOOPBACK_IPV4_PREFIX.len() + port.len());
        out.push_str(LOOPBACK_IPV4_PREFIX);
        out.push_str(port);
        return out;
    }
    s.to_owned()
}

/// 生成 Zenoh 明文 QUIC (`LinkUnicastQuicUnsecure`) 端点定位符：`udp/<host:port>#rel=1`（单次预分配，消除双重 `format!`）。
pub fn quic_locator(addr: &str, for_connect: bool) -> String {
    let s = strip_addr_parts(addr);
    let (host_prefix, host_rest) =
        if for_connect && let Some(port) = s.strip_prefix(UNSPECIFIED_IPV4_PREFIX) {
            (LOOPBACK_IPV4_PREFIX, port)
        } else {
            ("", s)
        };
    let mut out = String::with_capacity(
        QUIC_SCHEME_PREFIX.len() + host_prefix.len() + host_rest.len() + QUIC_REL_SUFFIX.len(),
    );
    out.push_str(QUIC_SCHEME_PREFIX);
    out.push_str(host_prefix);
    out.push_str(host_rest);
    out.push_str(QUIC_REL_SUFFIX);
    out
}

/// 拼装单元素 JSON 字符串数组 `["<locator>"]`（单次预分配，零 `format!` 开销）。
#[inline]
pub fn json_single_endpoint(locator: &str) -> String {
    let mut out =
        String::with_capacity(JSON_ARRAY_PREFIX.len() + locator.len() + JSON_ARRAY_SUFFIX.len());
    out.push_str(JSON_ARRAY_PREFIX);
    out.push_str(locator);
    out.push_str(JSON_ARRAY_SUFFIX);
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
    let locator = quic_locator(listen_addr, false);
    let mut cfg = base_zenoh_config(ZENOH_MODE_PEER)?;
    cfg.insert_json5(ZENOH_LISTEN_KEY, &json_single_endpoint(&locator))
        .map_err(|e| anyhow!("zenoh listen config: {e}"))?;
    cfg.insert_json5(ZENOH_CONNECT_KEY, ZENOH_EMPTY_ARRAY)
        .map_err(|e| anyhow!("zenoh connect config: {e}"))?;
    zenoh::open(cfg)
        .await
        .map_err(|e| anyhow!("Failed to open Zenoh server session on {locator}: {e}"))
}

/// 打开连接到指定明文 QUIC 地址的客户端 Zenoh 会话。
pub async fn open_client_session(target_addr: &str) -> Result<Session> {
    let locator = quic_locator(target_addr, true);
    let mut cfg = base_zenoh_config(ZENOH_MODE_CLIENT)?;
    cfg.insert_json5(ZENOH_LISTEN_KEY, ZENOH_EMPTY_ARRAY)
        .map_err(|e| anyhow!("zenoh listen config: {e}"))?;
    cfg.insert_json5(ZENOH_CONNECT_KEY, &json_single_endpoint(&locator))
        .map_err(|e| anyhow!("zenoh connect config: {e}"))?;
    zenoh::open(cfg)
        .await
        .map_err(|e| anyhow!("Failed to connect Zenoh client session to {locator}: {e}"))
}

/// 在非 Tokio 运行时（如 `compio`）上安全关闭 Zenoh 会话（委托给 Zenoh 内部 `ZRuntime::Net`）。
pub fn close_session(session: &Session) {
    let _ = session.close().wait();
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

/// 在已连接的客户端会话上发起一次 `bitcode` 编码的 Zenoh 查询。
/// 若远端 Queryable 尚未完成声明导致回复通道立即关闭，自动退避让出调度并重试。
pub async fn rpc_once<Req, Resp>(
    session: &Session,
    key_expr: &str,
    req: &Req,
    timeout: Duration,
) -> Result<Resp>
where
    Req: Encode,
    Resp: DecodeOwned,
{
    let payload = bitcode::encode(req);
    for attempt in 0..5 {
        if attempt > 0 {
            sleep(RETRY_BACKOFF).await;
        }
        let replies = session
            .get(key_expr)
            .payload(ZBytes::from(payload.as_slice()))
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

    /// 获取或建立到目标地址的客户端会话。
    pub async fn get_or_connect(&self, target_addr: &str) -> Result<Session> {
        let key = normalize_addr(target_addr, true);
        if let Some(existing) = self.sessions.lock().get(&key)
            && !existing.is_closed()
        {
            return Ok(existing.clone());
        }

        let session = open_client_session(&key).await?;
        let duplicate = {
            let mut guard = self.sessions.lock();
            if let Some(existing) = guard.get(&key)
                && !existing.is_closed()
            {
                Some(existing.clone())
            } else {
                guard.insert(key, session.clone());
                None
            }
        };
        if let Some(existing) = duplicate {
            close_session(&session);
            return Ok(existing);
        }
        Ok(session)
    }

    /// 驱逐失效的目标会话。
    pub fn evict(&self, target_addr: &str) {
        let key = normalize_addr(target_addr, true);
        let removed = self.sessions.lock().remove(&key);
        if let Some(s) = removed {
            close_session(&s);
        }
    }

    /// 向目标地址发送 `bitcode` RPC 请求；若缓存连接断开则自动重连一次。
    pub async fn rpc<Req, Resp>(&self, target_addr: &str, key_expr: &str, req: &Req) -> Result<Resp>
    where
        Req: Encode,
        Resp: DecodeOwned,
    {
        self.rpc_with_timeout(target_addr, key_expr, req, DEFAULT_RPC_TIMEOUT)
            .await
    }

    /// 带超时配置的 `bitcode` RPC 调用。
    pub async fn rpc_with_timeout<Req, Resp>(
        &self,
        target_addr: &str,
        key_expr: &str,
        req: &Req,
        timeout: Duration,
    ) -> Result<Resp>
    where
        Req: Encode,
        Resp: DecodeOwned,
    {
        let session = self.get_or_connect(target_addr).await?;
        match rpc_once(&session, key_expr, req, timeout).await {
            Ok(resp) => Ok(resp),
            Err(first_err) => {
                // 若因远端刚注册 queryable 或连接中断导致无响应，短暂等待后重试一次
                self.evict(target_addr);
                sleep(RETRY_BACKOFF).await;
                let session = match self.get_or_connect(target_addr).await {
                    Ok(s) => s,
                    Err(_) => bail!("{first_err}"),
                };
                rpc_once(&session, key_expr, req, timeout).await
            }
        }
    }
}
