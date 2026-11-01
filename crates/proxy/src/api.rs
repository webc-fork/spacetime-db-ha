//! 基于 Zenoh Queryable + `bitcode` 的 Raft RPC 与集群管理端点，
//! 以及基于 `compio::net::TcpListener` 与 `const &str` 的最小开销 HTTP JSON `/metrics` 端点。

use std::{collections::BTreeMap, fmt::Display, io::Cursor, rc::Rc};

use anyhow::{Result, anyhow};
use askama::Template;
use bitcode::{Decode, DecodeOwned, Encode};
use compio::{
    BufResult,
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    runtime::spawn,
};
use futures::{FutureExt, select};
use gxhash::HashMap;
use openraft::{
    BasicNode, ReadPolicy, ServerState, Snapshot,
    async_runtime::watch::WatchReceiver,
    raft::{AppendEntriesRequest, VoteRequest},
};
use zenoh::{Session, query::Query};

use crate::{
    metrics,
    raft::{
        RaftPool, ShardRaft,
        codec::{
            WireAppendEntriesRequest, WireAppendEntriesResponse, WireFatal,
            WireFullSnapshotRequest, WireSnapshotResponse, WireVoteRequest, WireVoteResponse,
        },
        types::{RaftSnapshotMeta, RaftVote, ReducerCallRequest, WriteAck, leader_term},
    },
    router::{DEFAULT_SHARD, ShardConfigCommand, ShardId, encode_shard_config},
    transport::{
        CLUSTER_PREFIX, CLUSTER_QUERYABLE_ALL, CrossfireChannel, KEY_CLUSTER_HEALTH, KEY_METRICS,
        RAFT_PREFIX, RAFT_QUERYABLE_ALL, RPC_APPEND, RPC_SNAPSHOT, RPC_VOTE, RpcEnvelope,
        SUB_ADD_NODE, SUB_HEALTH, SUB_INIT, SUB_LEADER, SUB_REMOVE_NODE, SUB_SHARDS,
        SUB_SHARDS_CREATE, SUB_SHARDS_PREFIX, SUB_SHARDS_REMOVE, SUB_SHARDS_ROUTE,
        SUB_SHARDS_ROUTE_ADD, SUB_SHARDS_ROUTE_REMOVE, SUB_STATUS, SUB_WRITE, SUFFIX_STATUS,
        SUFFIX_WRITE, close_session, decode_query_payload, mid_u64_key, reply_bitcode,
        watch::Receiver,
    },
};

/// 把错误格式化为字符串。
fn err_str(err: impl Display) -> String {
    err.to_string()
}

/// 原生 TCP `/metrics` 请求读取缓冲区大小。
const METRICS_REQ_BUF_SIZE: usize = 1024;

// ============================================================================
// 编译期公共 HTTP 与 JSON 静态常量片段（基于 const_str 组合，消除硬编码与重复字面量）
// ============================================================================

/// HTTP 协议版本前缀。
const HTTP_VER: &str = "HTTP/1.1 ";
/// HTTP 行分隔符。
const CRLF: &str = "\r\n";

/// HTTP 状态码短语。
const STATUS_200: &str = "200 OK";
const STATUS_404: &str = "404 Not Found";

/// 公共 HTTP 响应头片段。
const HDR_CONTENT_TYPE_JSON: &str =
    const_str::concat!(CRLF, "Content-Type: application/json; charset=utf-8");
const HDR_CONTENT_LEN: &str = const_str::concat!(CRLF, "Content-Length: ");
const HTTP_HEADER_END: &str = const_str::concat!(CRLF, "Connection: close", CRLF, CRLF);

/// 编译期静态 HTTP 200 JSON 响应头前缀。
const HTTP_200_JSON_PREFIX: &str =
    const_str::concat!(HTTP_VER, STATUS_200, HDR_CONTENT_TYPE_JSON, HDR_CONTENT_LEN);
/// 编译期静态 HTTP 404 JSON 响应头前缀。
const HTTP_404_JSON_PREFIX: &str =
    const_str::concat!(HTTP_VER, STATUS_404, HDR_CONTENT_TYPE_JSON, HDR_CONTENT_LEN);

/// 编译期静态健康检查 200 JSON 响应体与完整报文（自动在编译期计算 Content-Length，零堆分配）。
const HEALTH_OK_BODY: &str = r#"{"status":"healthy"}"#;
const HTTP_200_HEALTH_RESP: &[u8] = const_str::concat_bytes!(
    HTTP_200_JSON_PREFIX,
    const_str::to_str!(HEALTH_OK_BODY.len()),
    HTTP_HEADER_END,
    HEALTH_OK_BODY
);

/// 编译期静态健康检查 503 JSON 响应体固定包装长度（`{"status":"unhealthy","error":""}`）。
const HEALTH_ERR_WRAP_LEN: usize = r#"{"status":"unhealthy","error":""}"#.len();

/// 编译期静态 404 JSON 响应体与完整报文（自动在编译期计算 Content-Length，零堆分配）。
const NOT_FOUND_BODY: &str = r#"{"error":"not found"}"#;
const HTTP_404_JSON_RESP: &[u8] = const_str::concat_bytes!(
    HTTP_404_JSON_PREFIX,
    const_str::to_str!(NOT_FOUND_BODY.len()),
    HTTP_HEADER_END,
    NOT_FOUND_BODY
);

/// HTTP GET 请求行匹配前缀（编译期拼接）。
const REQ_GET_PREFIX: &str = "GET /";
const REQ_GET_METRICS: &[u8] = const_str::concat_bytes!(REQ_GET_PREFIX, KEY_METRICS);
const REQ_GET_HEALTH: &[u8] = const_str::concat_bytes!(REQ_GET_PREFIX, KEY_CLUSTER_HEALTH);

/// 校验 HTTP 请求行是否精确匹配指定路径前缀（后跟空格、查询参数 `?` 或尾部 `/`）。
#[inline]
fn matches_get_path(head: &[u8], prefix: &[u8]) -> bool {
    head.strip_prefix(prefix)
        .is_some_and(|rest| matches!(rest.first(), Some(b' ' | b'?' | b'/')))
}

/// 取指定分片的 Raft 实例。
fn get_shard_raft(pool: &RaftPool, shard_id: ShardId) -> Result<ShardRaft, String> {
    pool.get_raft(shard_id)
        .ok_or_else(|| mid_u64_key("shard ", shard_id, " not found"))
}

/// 将 `ServerState` 映射为编译期静态字符串字面量，消除 `format!("{:?}", ...)` 运行时格式化开销。
#[inline]
const fn server_state_str(state: ServerState) -> &'static str {
    match state {
        ServerState::Learner => "Learner",
        ServerState::Follower => "Follower",
        ServerState::Candidate => "Candidate",
        ServerState::Leader => "Leader",
        ServerState::Shutdown => "Shutdown",
    }
}

/// 取分片 0 的 Raft 实例。
fn get_default_raft(pool: &RaftPool) -> Result<ShardRaft, String> {
    get_shard_raft(pool, DEFAULT_SHARD)
}

// ============================================================================
// 公共管理协议数据结构（bitcode 序列化，供代理与 CLI 共享）
// ============================================================================

/// 集群整体状态响应。
#[derive(Debug, Clone, Encode, Decode)]
pub struct ClusterStatus {
    pub node_id: u64,
    pub state: String,
    pub current_leader: Option<u64>,
    pub current_term: u64,
    pub last_applied: Option<String>,
    pub membership: String,
    pub active_shards: Vec<ShardId>,
}

/// 扩容节点请求。
#[derive(Debug, Clone, Encode, Decode)]
pub struct AddNodeRequest {
    pub node_id: u64,
    pub addr: String,
}

/// 缩容节点请求。
#[derive(Debug, Clone, Encode, Decode)]
pub struct RemoveNodeRequest {
    pub node_id: u64,
}

/// 当前 Leader 信息。
#[derive(Debug, Clone, Encode, Decode)]
pub struct LeaderInfo {
    pub leader_id: Option<u64>,
    pub leader_addr: Option<String>,
    pub this_node_is_leader: bool,
}

/// 创建分片请求。
#[derive(Debug, Clone, Encode, Decode)]
pub struct CreateShardRequest {
    pub shard_id: ShardId,
}

/// 删除分片请求（修复 P1-6）。
#[derive(Debug, Clone, Encode, Decode)]
pub struct RemoveShardRequest {
    pub shard_id: ShardId,
}

/// 添加模块路由请求。
#[derive(Debug, Clone, Encode, Decode)]
pub struct AddRouteRequest {
    pub module_name: String,
    pub shard_id: ShardId,
}

/// 删除模块路由请求。
#[derive(Debug, Clone, Encode, Decode)]
pub struct RemoveRouteRequest {
    pub module_name: String,
}

/// 分片与路由列表响应。
#[derive(Debug, Clone, Encode, Decode)]
pub struct ShardListResponse {
    pub shards: Vec<ShardId>,
    pub routes: HashMap<String, ShardId>,
}

/// 单个分片状态响应。
#[derive(Debug, Clone, Encode, Decode)]
pub struct ShardStatusResponse {
    pub shard_id: ShardId,
    pub state: String,
    pub current_leader: Option<u64>,
    pub current_term: u64,
    pub last_applied: Option<String>,
    pub membership: String,
}

// ============================================================================
// Zenoh RPC 路由与分发
// ============================================================================

/// 在指定的 Zenoh 服务端会话上注册 `raft/**` 与 `cluster/**` 查询处理器并运行至收到关闭信号。
pub async fn serve_zenoh(
    session: Session,
    pool: Rc<RaftPool>,
    mut shutdown: Receiver<bool>,
) -> Result<()> {
    let raft_qabl = session
        .declare_queryable(RAFT_QUERYABLE_ALL)
        .with(CrossfireChannel::new())
        .await
        .map_err(|e| anyhow!("declare {RAFT_QUERYABLE_ALL} queryable: {e}"))?;
    let cluster_qabl = session
        .declare_queryable(CLUSTER_QUERYABLE_ALL)
        .with(CrossfireChannel::new())
        .await
        .map_err(|e| anyhow!("declare {CLUSTER_QUERYABLE_ALL} queryable: {e}"))?;
    let metrics_qabl = session
        .declare_queryable(KEY_METRICS)
        .with(CrossfireChannel::new())
        .await
        .map_err(|e| anyhow!("declare {KEY_METRICS} queryable: {e}"))?;

    loop {
        select! {
            q = raft_qabl.recv().fuse() => {
                let Ok(query) = q else { break };
                let pool = Rc::clone(&pool);
                spawn(async move {
                    dispatch_raft_query(&pool, query).await;
                })
                .detach();
            }
            q = cluster_qabl.recv().fuse() => {
                let Ok(query) = q else { break };
                let pool = Rc::clone(&pool);
                spawn(async move {
                    dispatch_cluster_query(&pool, query).await;
                })
                .detach();
            }
            q = metrics_qabl.recv().fuse() => {
                let Ok(query) = q else { break };
                let pool = Rc::clone(&pool);
                spawn(async move {
                    pool.update_metrics();
                    reply_bitcode(&query, Ok(metrics::encode_metrics())).await;
                })
                .detach();
            }
            _ = shutdown.changed().fuse() => {
                break;
            }
        }
    }

    drop(raft_qabl);
    drop(cluster_qabl);
    drop(metrics_qabl);
    close_session(&session);
    Ok(())
}

/// 解码查询负载的辅助函数。
fn decode_req<T: DecodeOwned>(query: &Query) -> Result<T, String> {
    decode_query_payload(query).map_err(err_str)
}

/// 分发 `raft/**` 命名空间下的 Raft 节点间共识 RPC。
async fn dispatch_raft_query(pool: &RaftPool, query: Query) {
    let key = query.key_expr().as_str();
    let rest = key.strip_prefix(RAFT_PREFIX).unwrap_or("");

    let (shard_id, rpc) = match rest.split_once('/') {
        Some((sid_str, rpc_name)) => match sid_str.parse::<ShardId>() {
            Ok(sid) => (sid, rpc_name),
            Err(_) => {
                reply_bitcode::<()>(&query, Err(format!("invalid shard id in {key}"))).await;
                return;
            }
        },
        None => (DEFAULT_SHARD, rest),
    };

    match rpc {
        RPC_APPEND => {
            let res: RpcEnvelope<Result<WireAppendEntriesResponse, WireFatal>> = async {
                let req: WireAppendEntriesRequest = decode_req(&query)?;
                let raft = get_shard_raft(pool, shard_id)?;
                Ok(raft
                    .append_entries(AppendEntriesRequest::from(req))
                    .await
                    .map(WireAppendEntriesResponse::from)
                    .map_err(WireFatal::from))
            }
            .await;
            reply_bitcode(&query, res).await;
        }
        RPC_VOTE => {
            let res: RpcEnvelope<Result<WireVoteResponse, WireFatal>> = async {
                let req: WireVoteRequest = decode_req(&query)?;
                let raft = get_shard_raft(pool, shard_id)?;
                Ok(raft
                    .vote(VoteRequest::from(req))
                    .await
                    .map(WireVoteResponse::from)
                    .map_err(WireFatal::from))
            }
            .await;
            reply_bitcode(&query, res).await;
        }
        RPC_SNAPSHOT => {
            let res: RpcEnvelope<Result<WireSnapshotResponse, WireFatal>> = async {
                let req: WireFullSnapshotRequest = decode_req(&query)?;
                let raft = get_shard_raft(pool, shard_id)?;
                let vote = RaftVote::from(req.vote);
                let snapshot = Snapshot {
                    meta: RaftSnapshotMeta::from(req.meta),
                    snapshot: Cursor::new(req.data),
                };
                Ok(raft
                    .install_full_snapshot(vote, snapshot)
                    .await
                    .map(WireSnapshotResponse::from)
                    .map_err(WireFatal::from))
            }
            .await;
            reply_bitcode(&query, res).await;
        }
        _ => {
            reply_bitcode::<()>(&query, Err(format!("unknown raft rpc: {key}"))).await;
        }
    }
}

/// 分发 `cluster/**` 命名空间下的管理与写转发请求。
async fn dispatch_cluster_query(pool: &RaftPool, query: Query) {
    let key = query.key_expr().as_str();
    let rest = key.strip_prefix(CLUSTER_PREFIX).unwrap_or("");

    match rest {
        SUB_STATUS => {
            reply_bitcode(&query, handle_cluster_status(pool)).await;
        }
        SUB_INIT => {
            let res = async {
                let nodes: BTreeMap<u64, String> = decode_req(&query)?;
                let members: BTreeMap<u64, BasicNode> = nodes
                    .into_iter()
                    .map(|(id, addr)| (id, BasicNode { addr }))
                    .collect();
                let raft = get_default_raft(pool)?;
                raft.initialize(members).await.map_err(err_str)?;
                Ok("Cluster initialized".to_owned())
            }
            .await;
            reply_bitcode(&query, res).await;
        }
        SUB_ADD_NODE => {
            let res = async {
                let req: AddNodeRequest = decode_req(&query)?;
                pool.add_node_all_shards(req.node_id, req.addr)
                    .await
                    .map_err(err_str)?;
                Ok(mid_u64_key("Node ", req.node_id, " added to all shards"))
            }
            .await;
            reply_bitcode(&query, res).await;
        }
        SUB_REMOVE_NODE => {
            let res = async {
                let req: RemoveNodeRequest = decode_req(&query)?;
                pool.remove_node_all_shards(req.node_id)
                    .await
                    .map_err(err_str)?;
                Ok(mid_u64_key(
                    "Node ",
                    req.node_id,
                    " removed from all shards",
                ))
            }
            .await;
            reply_bitcode(&query, res).await;
        }
        SUB_LEADER => {
            reply_bitcode(&query, handle_cluster_leader(pool)).await;
        }
        SUB_HEALTH => {
            reply_bitcode(&query, handle_cluster_health(pool).await).await;
        }
        SUB_WRITE => {
            let res = async {
                let req: ReducerCallRequest = decode_req(&query)?;
                write_to_shard(pool, DEFAULT_SHARD, req).await
            }
            .await;
            reply_bitcode(&query, res).await;
        }
        SUB_SHARDS => {
            let resp = ShardListResponse {
                shards: pool.list_shards(),
                routes: pool.get_routes(),
            };
            reply_bitcode(&query, Ok(resp)).await;
        }
        SUB_SHARDS_CREATE => {
            let res = async {
                let req: CreateShardRequest = decode_req(&query)?;
                if req.shard_id == DEFAULT_SHARD {
                    return Err("shard 0 always exists".to_owned());
                }
                propose_config(
                    pool,
                    ShardConfigCommand::CreateShard {
                        shard_id: req.shard_id,
                    },
                )
                .await?;
                Ok(mid_u64_key("Shard ", req.shard_id, " created"))
            }
            .await;
            reply_bitcode(&query, res).await;
        }
        SUB_SHARDS_REMOVE => {
            let res = async {
                let req: RemoveShardRequest = decode_req(&query)?;
                if req.shard_id == DEFAULT_SHARD {
                    return Err("cannot remove default shard 0".to_owned());
                }
                propose_config(
                    pool,
                    ShardConfigCommand::RemoveShard {
                        shard_id: req.shard_id,
                    },
                )
                .await?;
                Ok(mid_u64_key("Shard ", req.shard_id, " removed"))
            }
            .await;
            reply_bitcode(&query, res).await;
        }
        SUB_SHARDS_ROUTE | SUB_SHARDS_ROUTE_ADD => {
            let res = async {
                let req: AddRouteRequest = decode_req(&query)?;
                if !pool.has_shard(req.shard_id) {
                    return Err(mid_u64_key("shard ", req.shard_id, " does not exist"));
                }
                let msg = format!("Route {} -> shard {} added", req.module_name, req.shard_id);
                propose_config(
                    pool,
                    ShardConfigCommand::AddRoute {
                        module_name: req.module_name,
                        shard_id: req.shard_id,
                    },
                )
                .await?;
                Ok(msg)
            }
            .await;
            reply_bitcode(&query, res).await;
        }
        SUB_SHARDS_ROUTE_REMOVE => {
            let res = async {
                let req: RemoveRouteRequest = decode_req(&query)?;
                let msg = format!("Route for {} removed", req.module_name);
                propose_config(
                    pool,
                    ShardConfigCommand::RemoveRoute {
                        module_name: req.module_name,
                    },
                )
                .await?;
                Ok(msg)
            }
            .await;
            reply_bitcode(&query, res).await;
        }
        _ => {
            // 处理动态路径：cluster/{shard_id}/write 或 cluster/shards/{shard_id}/status
            if let Some(sid_str) = rest.strip_suffix(SUFFIX_WRITE)
                && let Ok(shard_id) = sid_str.parse::<ShardId>()
            {
                let res = async {
                    let req: ReducerCallRequest = decode_req(&query)?;
                    write_to_shard(pool, shard_id, req).await
                }
                .await;
                reply_bitcode(&query, res).await;
                return;
            }

            if let Some(mid) = rest.strip_prefix(SUB_SHARDS_PREFIX)
                && let Some(sid_str) = mid.strip_suffix(SUFFIX_STATUS)
                && let Ok(shard_id) = sid_str.parse::<ShardId>()
            {
                reply_bitcode(&query, handle_shard_status(pool, shard_id)).await;
                return;
            }

            reply_bitcode::<()>(&query, Err(format!("unknown cluster endpoint: {key}"))).await;
        }
    }
}

/// 提交一次转发来的写，并把日志位置回给 follower。
async fn write_to_shard(
    pool: &RaftPool,
    shard_id: ShardId,
    req: ReducerCallRequest,
) -> Result<WriteAck, String> {
    let raft = get_shard_raft(pool, shard_id)?;
    let resp = raft.client_write(req).await.map_err(err_str)?;
    Ok(WriteAck {
        log_id: resp.log_id,
    })
}

/// 本节点视角的集群状态（以分片 0 为准，复用 [`handle_shard_status`] 消除重复提取）。
pub fn handle_cluster_status(pool: &RaftPool) -> Result<ClusterStatus, String> {
    let status = handle_shard_status(pool, DEFAULT_SHARD)?;
    Ok(ClusterStatus {
        node_id: pool.node_config.node_id,
        state: status.state,
        current_leader: status.current_leader,
        current_term: status.current_term,
        last_applied: status.last_applied,
        membership: status.membership,
        active_shards: pool.list_shards(),
    })
}

/// 当前 leader（分片 0，复用 [`RaftPool::current_leader`] 消除地址解析重复）。
pub fn handle_cluster_leader(pool: &RaftPool) -> Result<LeaderInfo, String> {
    let _ = get_default_raft(pool)?;
    let node_id = pool.node_config.node_id;
    let current = pool.current_leader(DEFAULT_SHARD);
    let leader_id = current.as_ref().map(|&(id, _)| id);
    let leader_addr = current
        .filter(|&(id, _)| id != node_id)
        .map(|(_, addr)| addr);

    Ok(LeaderInfo {
        leader_id,
        leader_addr,
        this_node_is_leader: leader_id == Some(node_id),
    })
}

/// 健康检查（修复 P3-16：若本节点自称 leader，必须通过线性化多数派校验，防止少数派分区误报健康）。
pub async fn handle_cluster_health(pool: &RaftPool) -> Result<String, String> {
    let raft = get_default_raft(pool)?;
    let (state, current_leader) = {
        let metrics_rx = raft.metrics();
        let metrics = metrics_rx.borrow_watched();
        (metrics.state, metrics.current_leader)
    };
    if matches!(state, ServerState::Shutdown) {
        return Err("raft node is shut down".to_owned());
    }
    let Some(leader_id) = current_leader else {
        return Err("no leader elected".to_owned());
    };
    if leader_id == pool.node_config.node_id
        && raft
            .ensure_linearizable(ReadPolicy::ReadIndex)
            .await
            .is_err()
    {
        return Err("leader lost quorum".to_owned());
    }
    Ok("healthy".to_owned())
}

/// 单个分片状态查询。
pub fn handle_shard_status(
    pool: &RaftPool,
    shard_id: ShardId,
) -> Result<ShardStatusResponse, String> {
    let raft = get_shard_raft(pool, shard_id)?;
    let metrics_rx = raft.metrics();
    let status = metrics_rx.borrow_watched();

    Ok(ShardStatusResponse {
        shard_id,
        state: server_state_str(status.state).to_owned(),
        current_leader: status.current_leader,
        current_term: leader_term(&status.vote),
        last_applied: status.last_applied.map(|id| id.to_string()),
        membership: format!("{:?}", status.membership_config),
    })
}

/// 通过分片 0 的 Raft 提交一条分片配置命令并等待本节点应用。
async fn propose_config(pool: &RaftPool, cmd: ShardConfigCommand) -> Result<(), String> {
    let raw_message = encode_shard_config(&cmd);
    let log_id = pool
        .propose_write(DEFAULT_SHARD, raw_message.into(), "")
        .await
        .map_err(err_str)?;
    pool.wait_applied(DEFAULT_SHARD, log_id.index)
        .await
        .map_err(err_str)?;
    Ok(())
}

// ============================================================================
// 基于 compio + askama::Template + const_str 的最小开销 HTTP JSON `/metrics` 响应器
// ============================================================================

/// 动态 HTTP 200 JSON 响应编译期模板（用于 `/metrics` 导出）。
#[derive(Template)]
#[template(
    source = "HTTP/1.1 200 OK\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: {{ body.len() }}\r\nConnection: close\r\n\r\n{{ body }}",
    ext = "txt"
)]
struct Http200JsonTemplate<'a> {
    body: &'a str,
}

/// 动态 HTTP 503 健康检查错误 JSON 响应编译期模板（单次渲染完整 HTTP 报文 + JSON 体）。
#[derive(Template)]
#[template(
    source = "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: {{ body_len }}\r\nConnection: close\r\n\r\n{\"status\":\"unhealthy\",\"error\":\"{{ msg }}\"}",
    ext = "txt"
)]
struct Http503HealthTemplate<'a> {
    body_len: usize,
    msg: &'a str,
}

/// 在给定 `compio::net::TcpListener` 上响应 HTTP JSON `GET /metrics` 与 `GET /cluster/health`。
pub async fn serve_metrics_tcp(
    listener: TcpListener,
    pool: Rc<RaftPool>,
    mut shutdown: Receiver<bool>,
) {
    loop {
        select! {
            accepted = listener.accept().fuse() => {
                let Ok((mut stream, _)) = accepted else { continue };
                let pool = Rc::clone(&pool);
                spawn(async move {
                    let BufResult(Ok(n), buf) = stream.read([0u8; METRICS_REQ_BUF_SIZE]).await else {
                        return;
                    };
                    let head = &buf[..n];

                    if matches_get_path(head, REQ_GET_METRICS) {
                        pool.update_metrics();
                        let body = metrics::encode_metrics();
                        let resp = Http200JsonTemplate { body: &body }
                            .render()
                            .unwrap_or_default();
                        let _ = stream.write_all(resp.into_bytes()).await;
                    } else if matches_get_path(head, REQ_GET_HEALTH) {
                        match handle_cluster_health(&pool).await {
                            Ok(_) => {
                                let _ = stream.write_all(HTTP_200_HEALTH_RESP).await;
                            }
                            Err(msg) => {
                                let resp = Http503HealthTemplate {
                                    body_len: HEALTH_ERR_WRAP_LEN + msg.len(),
                                    msg: &msg,
                                }
                                .render()
                                .unwrap_or_default();
                                let _ = stream.write_all(resp.into_bytes()).await;
                            }
                        }
                    } else {
                        let _ = stream.write_all(HTTP_404_JSON_RESP).await;
                    }
                    let _ = stream.shutdown().await;
                })
                .detach();
            }
            _ = shutdown.changed().fuse() => return,
        }
    }
}
