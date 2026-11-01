use std::{
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use bitcode::{Decode, Encode};
use bytes::Bytes;
use compio::runtime::spawn;
use futures::{FutureExt, select};
use tracing::{debug, info, warn};
use zenoh::{Session, bytes::ZBytes, pubsub::Subscriber, sample::Sample};

use crate::{
    config::NodeConfig,
    metrics,
    raft::{RaftPool, codec::impl_bitcode_via, forwarder::next_stream_id},
    router::{DEFAULT_SHARD, ShardId, extract_module_name},
    transport::{
        CrossfireChannel, CrossfireHandler, close_session, decode_query_payload,
        open_client_session, reply_bitcode, rpc_once, watch::Receiver,
    },
    websocket::ConnectionGuard,
};

/// SpacetimeDB v2 协议中 ClientMessage 的 BSATN 标签字节（消息首字节）。
pub mod stdb_tags {
    pub const SUBSCRIBE: u8 = 0;
    pub const UNSUBSCRIBE: u8 = 1;
    pub const ONE_OFF_QUERY: u8 = 2;
    pub const CALL_REDUCER: u8 = 3;
    pub const CALL_PROCEDURE: u8 = 4;
}

/// 关闭码：节点离开/切换 Leader（对应 1001 Away）。
pub const CLOSE_CODE_AWAY: u16 = 1001;

/// 客户端流单次 RPC 超时。
const STREAM_RPC_TIMEOUT: Duration = Duration::from_secs(5);

/// 连接关闭帧（由 `bitcode` 序列化后通过 Zenoh 传输）。
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct CloseFrame {
    pub code: u16,
    pub reason: String,
}

/// 代理与 SpacetimeDB 之间传输的帧消息（由 `bitcode` 编码，承载于 Zenoh 明文 QUIC）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    Binary(Bytes),
    Text(String),
    Ping(Bytes),
    Pong(Bytes),
    Close(Option<CloseFrame>),
}

/// `Message` 的 `bitcode` 原生线格式表示。
#[derive(Encode, Decode)]
pub struct WireMessage {
    kind: WireMessageKind,
}

#[derive(Encode, Decode)]
enum WireMessageKind {
    Binary(Vec<u8>),
    Text(String),
    Ping(Vec<u8>),
    Pong(Vec<u8>),
    Close(Option<CloseFrame>),
}

impl From<&Message> for WireMessage {
    fn from(msg: &Message) -> Self {
        let kind = match msg {
            Message::Binary(b) => WireMessageKind::Binary(b.to_vec()),
            Message::Text(s) => WireMessageKind::Text(s.clone()),
            Message::Ping(b) => WireMessageKind::Ping(b.to_vec()),
            Message::Pong(b) => WireMessageKind::Pong(b.to_vec()),
            Message::Close(c) => WireMessageKind::Close(c.clone()),
        };
        Self { kind }
    }
}

impl From<WireMessage> for Message {
    fn from(w: WireMessage) -> Self {
        match w.kind {
            WireMessageKind::Binary(b) => Self::Binary(Bytes::from(b)),
            WireMessageKind::Text(s) => Self::Text(s),
            WireMessageKind::Ping(b) => Self::Ping(Bytes::from(b)),
            WireMessageKind::Pong(b) => Self::Pong(Bytes::from(b)),
            WireMessageKind::Close(c) => Self::Close(c),
        }
    }
}

impl_bitcode_via!(Message, WireMessage);

/// Zenoh 会话握手请求。
#[derive(Debug, Clone, Encode, Decode)]
pub struct StdbConnectRequest {
    /// 客户端预先订阅的 `stdb/s2c/{stream_id}` 流标识。
    pub stream_id: u64,
    /// 请求数据库路径，如 `/database/subscribe/testdb`。
    pub db_path: String,
}

/// Zenoh 会话握手响应。
#[derive(Debug, Clone, Encode, Decode)]
pub struct StdbConnectResponse {
    /// 服务端分配的会话连接 ID，客户端后续向 `stdb/c2s/{conn_id}` 发送消息。
    pub conn_id: u64,
}

/// 消息分类结果，决定后续走哪条路径。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageKind {
    /// 写操作：必须先过 Raft 共识。
    Write,
    /// 读操作：直接转发给本地 SpacetimeDB。
    Read,
    /// 非二进制或空消息：原样透传。
    PassThrough,
}

/// 标签字节的全部取值个数。
const TAG_COUNT: usize = u8::MAX as usize + 1;

/// 编译期生成的分类表：一次查表完成分流，无分支、无函数调用开销。
/// 未知标签按读处理直接转发，交给 SpacetimeDB 自行判定。
static TAG_KINDS: [MessageKind; TAG_COUNT] = {
    let mut table = [MessageKind::Read; TAG_COUNT];
    table[stdb_tags::SUBSCRIBE as usize] = MessageKind::Read;
    table[stdb_tags::UNSUBSCRIBE as usize] = MessageKind::Read;
    table[stdb_tags::ONE_OFF_QUERY as usize] = MessageKind::Read;
    table[stdb_tags::CALL_REDUCER as usize] = MessageKind::Write;
    table[stdb_tags::CALL_PROCEDURE as usize] = MessageKind::Write;
    table
};

/// 按 BSATN 标签字节分类一条消息。
pub fn classify_message(msg: &Message) -> MessageKind {
    match msg {
        Message::Binary(data) => match data.first() {
            Some(&tag) => TAG_KINDS[tag as usize],
            None => MessageKind::PassThrough,
        },
        _ => MessageKind::PassThrough,
    }
}

/// 客户端连接发送端（可跨任务轻量克隆，无锁）。
#[derive(Clone)]
pub struct ZenohStdbSender {
    session: Session,
    c2s_key: String,
}

impl ZenohStdbSender {
    /// 向服务端发送一条帧消息。
    pub async fn send(&self, msg: Message) -> Result<()> {
        rpc_once::<Message, ()>(&self.session, &self.c2s_key, &msg, STREAM_RPC_TIMEOUT).await
    }
}

/// 客户端连接封装（基于 Zenoh 明文 QUIC + `bitcode` + `crossfire`），供客户端与测试使用。
pub struct ZenohStdbStream {
    sender: ZenohStdbSender,
    s2c_sub: Subscriber<CrossfireHandler<Sample>>,
}

impl ZenohStdbStream {
    /// 连接到代理或 SpacetimeDB 的指定数据库路径。
    pub async fn connect(addr: &str, db_path: &str) -> Result<Self> {
        let stream_id = next_stream_id();
        let session = open_client_session(addr).await?;
        let s2c_key = format!("stdb/s2c/{stream_id}");
        let s2c_sub = session
            .declare_subscriber(&s2c_key)
            .with(CrossfireChannel::new())
            .await
            .map_err(|e| anyhow!("Failed to declare s2c subscriber: {e}"))?;

        let req = StdbConnectRequest {
            stream_id,
            db_path: db_path.to_owned(),
        };
        let resp: StdbConnectResponse =
            rpc_once(&session, "stdb/connect", &req, STREAM_RPC_TIMEOUT).await?;

        Ok(Self {
            sender: ZenohStdbSender {
                session,
                c2s_key: format!("stdb/c2s/{}", resp.conn_id),
            },
            s2c_sub,
        })
    }

    /// 拆分为发送端与订阅接收端，便于读写任务独立持有而无需 `Arc` 共享 `!Sync` 的 `AsyncRx`。
    pub fn split(self) -> (ZenohStdbSender, Subscriber<CrossfireHandler<Sample>>) {
        (self.sender, self.s2c_sub)
    }

    /// 向服务端发送一条帧消息。
    pub async fn send(&self, msg: Message) -> Result<()> {
        self.sender.send(msg).await
    }

    /// 接收服务端推送的下一条帧消息。
    pub async fn recv(&self) -> Result<Option<Message>> {
        recv_s2c_message(&self.s2c_sub).await
    }

    /// 关闭客户端会话。
    pub async fn close(self) {
        let _ = self.sender.send(Message::Close(None)).await;
        close_session(&self.sender.session);
    }
}

/// 从 `s2c` 订阅通道接收并解码下一条帧消息。
async fn recv_s2c_message(
    s2c_sub: &Subscriber<CrossfireHandler<Sample>>,
) -> Result<Option<Message>> {
    let Ok(sample) = s2c_sub.recv().await else {
        return Ok(None);
    };
    let bytes = sample.payload().to_bytes();
    let msg: Message =
        bitcode::decode(&bytes).context("Failed to decode s2c Message via bitcode")?;
    Ok(Some(msg))
}

/// 服务端单连接会话计数器。
static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

/// 处理单个客户端 Zenoh 会话连接。
///
/// 1. 解析请求路径对应的模块名与目标分片；
/// 2. 与本地 SpacetimeDB 建立独立上游 Zenoh 会话；
/// 3. 注册 `stdb/c2s/{conn_id}` 处理客户端上行消息：
///    - 写请求走 Raft 共识 -> 等待本地 apply -> 按 `log_index` 全局保序投递上游 -> 更新 `last_delivered`；
///    - 读请求等待本地因果应用水位 -> 转发上游；
/// 4. 将上游 `stdb/s2c` 消息实时转发回客户端 `stdb/s2c/{client_stream_id}`；
/// 5. 收到关闭信号时向客户端推送包含 leader 提示的 `CloseFrame`。
pub async fn spawn_client_session(
    server_session: Session,
    req: StdbConnectRequest,
    pool: Rc<RaftPool>,
    config: Arc<NodeConfig>,
    mut shutdown: Receiver<bool>,
) -> Result<StdbConnectResponse> {
    let conn_id = NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed);
    let request_path = if req.db_path.is_empty() {
        "/".to_owned()
    } else {
        req.db_path
    };

    let shard_id = match extract_module_name(&request_path) {
        Some(module_name) => pool.route_module(module_name),
        None => DEFAULT_SHARD,
    };

    // 先建立到上游 SpacetimeDB 的连接，确保握手返回后即可收发
    let upstream = ZenohStdbStream::connect(&config.stdb_url, &request_path)
        .await
        .with_context(|| {
            format!(
                "Failed to connect upstream SpacetimeDB at {}{}",
                config.stdb_url, request_path
            )
        })?;
    let (upstream_tx, upstream_rx) = upstream.split();

    let c2s_key = format!("stdb/c2s/{conn_id}");
    let c2s_queryable = server_session
        .declare_queryable(&c2s_key)
        .with(CrossfireChannel::new())
        .await
        .map_err(|e| anyhow!("Failed to declare c2s queryable {c2s_key}: {e}"))?;

    let client_s2c_key = format!("stdb/s2c/{}", req.stream_id);
    info!(
        conn_id,
        path = %request_path,
        shard_id,
        "Client session established over Zenoh QUIC"
    );

    spawn(async move {
        let _guard = ConnectionGuard::new();
        let pool_for_in = Rc::clone(&pool);
        let path_for_in = request_path;

        let mut inbound = spawn(async move {
            while let Ok(query) = c2s_queryable.recv().await {
                let msg: Message = match decode_query_payload(&query) {
                    Ok(m) => m,
                    Err(e) => {
                        reply_bitcode::<()>(&query, Err(e.to_string())).await;
                        continue;
                    }
                };

                if matches!(msg, Message::Close(_)) {
                    let _ = upstream_tx.send(msg).await;
                    reply_bitcode(&query, Ok(())).await;
                    return;
                }

                let res =
                    handle_client_message(msg, &upstream_tx, &pool_for_in, shard_id, &path_for_in)
                        .await;

                match res {
                    Ok(()) => reply_bitcode(&query, Ok(())).await,
                    Err(e) => {
                        warn!(error = %e, shard_id, "Client message processing failed");
                        reply_bitcode::<()>(&query, Err(e.to_string())).await;
                    }
                }
            }
        });

        let server_for_out = server_session.clone();
        let s2c_key_for_out = client_s2c_key.clone();
        let mut outbound = spawn(async move {
            while let Ok(Some(msg)) = recv_s2c_message(&upstream_rx).await {
                let is_close = matches!(msg, Message::Close(_));
                let bytes = bitcode::encode(&msg);
                if server_for_out
                    .put(&s2c_key_for_out, ZBytes::from(bytes))
                    .await
                    .is_err()
                {
                    return;
                }
                if is_close {
                    return;
                }
            }
        });

        select! {
            _ = (&mut inbound).fuse() => {},
            _ = (&mut outbound).fuse() => {},
            _ = shutdown.changed().fuse() => {
                info!(conn_id, "Shutdown signal received, closing client Zenoh session");
                if let Some(close_frame) = leader_close_frame(&pool, shard_id) {
                    let bytes = bitcode::encode(&Message::Close(Some(close_frame)));
                    let _ = server_session.put(&client_s2c_key, ZBytes::from(bytes)).await;
                }
            }
        }

        // compio::runtime::JoinHandle 在 Drop 时会自动取消未完成的任务
        drop(inbound);
        drop(outbound);
    })
    .detach();

    Ok(StdbConnectResponse { conn_id })
}

/// 单条客户端消息处理：写走 Raft 并按全局日志顺序投递，读经因果栅栏后直投。
async fn handle_client_message(
    msg: Message,
    upstream: &ZenohStdbSender,
    pool: &RaftPool,
    shard_id: ShardId,
    db_path: &str,
) -> Result<()> {
    match classify_message(&msg) {
        MessageKind::Write => {
            if let Message::Binary(data) = &msg {
                debug!(
                    tag = data[0],
                    len = data.len(),
                    shard_id,
                    "Write -> Raft consensus"
                );
                let log_id = pool
                    .propose_write(shard_id, data.clone(), db_path.to_owned())
                    .await?;
                pool.wait_applied(shard_id, log_id.index).await?;

                // 修复 P0-1：等待前置日志索引投递完毕，保证同节点跨连接写的上游投递顺序严格等于 Raft 日志顺序
                pool.wait_delivered(shard_id, log_id.index.saturating_sub(1))
                    .await;

                let send_res = upstream.send(msg).await;
                // 修复 P0-2：投递完成后推进并持久化 delivered 水位
                if send_res.is_ok() {
                    pool.mark_delivered(shard_id, log_id.index);
                }
                return send_res;
            }
        }
        MessageKind::Read => {
            // 修复 P0-4：等待本节点状态机追平已知提交水位
            pool.wait_read_ready(shard_id).await;
            metrics::READS_TOTAL.inc();
        }
        MessageKind::PassThrough => {}
    }

    upstream.send(msg).await
}

/// 构造带 leader 提示的关闭帧，方便客户端重连到新 leader。
fn leader_close_frame(pool: &RaftPool, shard_id: ShardId) -> Option<CloseFrame> {
    let (leader_id, leader_addr) = pool.current_leader(shard_id)?;
    Some(CloseFrame {
        code: CLOSE_CODE_AWAY,
        reason: format!("leader={leader_id}:{leader_addr}"),
    })
}
