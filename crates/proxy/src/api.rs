use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
};
use openraft::{
    BasicNode, Raft,
    error::{InstallSnapshotError, RaftError},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};
use serde::{Deserialize, Serialize};

use crate::{
    metrics,
    raft::{
        RaftPool,
        types::{ReducerCallRequest, TypeConfig},
    },
    router::{self, ShardId},
};

/// Shared application state for the HTTP management API.
pub struct AppState {
    pub pool: Arc<RaftPool>,
}

/// Build the axum router with all Raft RPC and cluster management endpoints.
pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        // Shard-aware Raft RPC endpoints: /raft/{shard_id}/{rpc}
        .route("/raft/{shard_id}/append", post(raft_shard_append))
        .route("/raft/{shard_id}/vote", post(raft_shard_vote))
        .route("/raft/{shard_id}/snapshot", post(raft_shard_snapshot))
        // Legacy Raft RPC endpoints — alias to shard 0 (backwards compat)
        .route("/raft/append", post(raft_append))
        .route("/raft/vote", post(raft_vote))
        .route("/raft/snapshot", post(raft_snapshot))
        // Shard-aware write forwarding
        .route("/cluster/{shard_id}/write", post(cluster_shard_write))
        // Legacy write forwarding — alias to shard 0
        .route("/cluster/write", post(cluster_write))
        // Cluster management endpoints
        .route("/cluster/status", get(cluster_status))
        .route("/cluster/init", post(cluster_init))
        .route("/cluster/add-node", post(cluster_add_node))
        .route("/cluster/remove-node", post(cluster_remove_node))
        // Discovery / monitoring endpoints
        .route("/cluster/leader", get(cluster_leader))
        .route("/cluster/health", get(cluster_health))
        .route("/metrics", get(prometheus_metrics))
        // Shard management endpoints
        .route("/cluster/shards/create", post(shard_create))
        .route("/cluster/shards/route", post(shard_add_route))
        .route("/cluster/shards/route", delete(shard_remove_route))
        .route("/cluster/shards", get(shard_list))
        .route("/cluster/shards/{shard_id}/status", get(shard_status))
        .with_state(state)
}

// ============================================================================
// Helper: get raft for a shard, returning 404 if missing
// ============================================================================

async fn get_shard_raft(
    pool: &RaftPool,
    shard_id: ShardId,
) -> Result<Raft<TypeConfig>, (StatusCode, String)> {
    pool.get_raft(shard_id).await.ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            format!("shard {} not found", shard_id),
        )
    })
}

// ============================================================================
// Shard-aware Raft RPC Endpoints
// ============================================================================

async fn raft_shard_append(
    State(state): State<Arc<AppState>>,
    Path(shard_id): Path<u64>,
    Json(req): Json<AppendEntriesRequest<TypeConfig>>,
) -> Result<Json<Result<AppendEntriesResponse<u64>, RaftError<u64>>>, (StatusCode, String)> {
    let raft = get_shard_raft(&state.pool, shard_id).await?;
    Ok(Json(raft.append_entries(req).await))
}

async fn raft_shard_vote(
    State(state): State<Arc<AppState>>,
    Path(shard_id): Path<u64>,
    Json(req): Json<VoteRequest<u64>>,
) -> Result<Json<Result<VoteResponse<u64>, RaftError<u64>>>, (StatusCode, String)> {
    let raft = get_shard_raft(&state.pool, shard_id).await?;
    Ok(Json(raft.vote(req).await))
}

async fn raft_shard_snapshot(
    State(state): State<Arc<AppState>>,
    Path(shard_id): Path<u64>,
    Json(req): Json<InstallSnapshotRequest<TypeConfig>>,
) -> Result<
    Json<Result<InstallSnapshotResponse<u64>, RaftError<u64, InstallSnapshotError>>>,
    (StatusCode, String),
> {
    let raft = get_shard_raft(&state.pool, shard_id).await?;
    Ok(Json(raft.install_snapshot(req).await))
}

// ============================================================================
// Legacy Raft RPC Endpoints (alias to shard 0)
// ============================================================================

async fn raft_append(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AppendEntriesRequest<TypeConfig>>,
) -> Json<Result<AppendEntriesResponse<u64>, RaftError<u64>>> {
    let raft = state.pool.get_raft(0).await.expect("shard 0 must exist");
    Json(raft.append_entries(req).await)
}

async fn raft_vote(
    State(state): State<Arc<AppState>>,
    Json(req): Json<VoteRequest<u64>>,
) -> Json<Result<VoteResponse<u64>, RaftError<u64>>> {
    let raft = state.pool.get_raft(0).await.expect("shard 0 must exist");
    Json(raft.vote(req).await)
}

async fn raft_snapshot(
    State(state): State<Arc<AppState>>,
    Json(req): Json<InstallSnapshotRequest<TypeConfig>>,
) -> Json<Result<InstallSnapshotResponse<u64>, RaftError<u64, InstallSnapshotError>>> {
    let raft = state.pool.get_raft(0).await.expect("shard 0 must exist");
    Json(raft.install_snapshot(req).await)
}

// ============================================================================
// Cluster Management Endpoints
// ============================================================================

#[derive(Serialize)]
struct ClusterStatus {
    node_id: u64,
    state: String,
    current_leader: Option<u64>,
    current_term: u64,
    last_applied: Option<String>,
    membership: String,
    active_shards: Vec<ShardId>,
}

/// Return cluster status for this node (based on shard 0).
async fn cluster_status(State(state): State<Arc<AppState>>) -> Json<ClusterStatus> {
    let raft = state.pool.get_raft(0).await.expect("shard 0 must exist");
    let metrics = raft.metrics().borrow().clone();
    let active_shards = state.pool.list_shards().await;

    Json(ClusterStatus {
        node_id: state.pool.node_config.node_id,
        state: format!("{:?}", metrics.state),
        current_leader: metrics.current_leader,
        current_term: metrics.vote.leader_id().term,
        last_applied: metrics.last_applied.map(|id| format!("{}", id)),
        membership: format!("{:?}", metrics.membership_config),
        active_shards,
    })
}

/// Initialize the Raft cluster with the given set of members.
/// This must be called exactly once on one node to bootstrap the cluster.
/// Only initializes shard 0 — new shards inherit membership from shard 0.
async fn cluster_init(
    State(state): State<Arc<AppState>>,
    Json(members): Json<BTreeMap<u64, BasicNode>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let raft = state.pool.get_raft(0).await.expect("shard 0 must exist");
    raft.initialize(members)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok((StatusCode::OK, "Cluster initialized".to_string()))
}

#[derive(Deserialize)]
struct AddNodeRequest {
    node_id: u64,
    addr: String,
}

/// Add a new node to the cluster (all shards).
async fn cluster_add_node(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AddNodeRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    state
        .pool
        .add_node_all_shards(req.node_id, req.addr)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    Ok((
        StatusCode::OK,
        format!("Node {} added to all shards", req.node_id),
    ))
}

#[derive(Deserialize)]
struct RemoveNodeRequest {
    node_id: u64,
}

/// Remove a node from the cluster (all shards).
async fn cluster_remove_node(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RemoveNodeRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    state
        .pool
        .remove_node_all_shards(req.node_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    Ok((
        StatusCode::OK,
        format!("Node {} removed from all shards", req.node_id),
    ))
}

// ============================================================================
// Write Forwarding (shard-aware + legacy)
// ============================================================================

/// Handle a write forwarded from a follower node (shard-specific).
async fn cluster_shard_write(
    State(state): State<Arc<AppState>>,
    Path(shard_id): Path<u64>,
    Json(req): Json<ReducerCallRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let raft = get_shard_raft(&state.pool, shard_id).await?;
    raft.client_write(req)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok((StatusCode::OK, "Write committed".to_string()))
}

/// Handle a write forwarded from a follower node (legacy, shard 0).
async fn cluster_write(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ReducerCallRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let raft = state.pool.get_raft(0).await.expect("shard 0 must exist");
    raft.client_write(req)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok((StatusCode::OK, "Write committed".to_string()))
}

// ============================================================================
// Discovery & Monitoring Endpoints
// ============================================================================

#[derive(Serialize)]
struct LeaderInfo {
    leader_id: Option<u64>,
    leader_addr: Option<String>,
    this_node_is_leader: bool,
}

/// Return the current Raft leader's address (shard 0).
async fn cluster_leader(State(state): State<Arc<AppState>>) -> Json<LeaderInfo> {
    let raft = state.pool.get_raft(0).await.expect("shard 0 must exist");
    let metrics = raft.metrics().borrow().clone();
    let leader_id = metrics.current_leader;
    let node_id = state.pool.node_config.node_id;
    let this_node_is_leader = leader_id == Some(node_id);

    let leader_addr = leader_id.and_then(|id| {
        if id == node_id {
            None // Return None for self — client is already connected here
        } else {
            state.pool.peers.get(&id).map(|n| n.addr.clone())
        }
    });

    Json(LeaderInfo {
        leader_id,
        leader_addr,
        this_node_is_leader,
    })
}

/// Health check endpoint. Returns 200 if the node is healthy, 503 if not.
async fn cluster_health(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let raft = state.pool.get_raft(0).await.expect("shard 0 must exist");
    let metrics = raft.metrics().borrow().clone();

    if metrics.current_leader.is_some() {
        Ok((StatusCode::OK, "healthy".to_string()))
    } else {
        Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "no leader elected".to_string(),
        ))
    }
}

/// Prometheus metrics endpoint.
async fn prometheus_metrics() -> impl IntoResponse {
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        metrics::encode_metrics(),
    )
}

// ============================================================================
// Shard Management Endpoints
// ============================================================================

#[derive(Deserialize)]
struct CreateShardRequest {
    shard_id: ShardId,
}

/// Create a new shard by proposing a ShardConfigCommand through shard 0's Raft.
async fn shard_create(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateShardRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if req.shard_id == 0 {
        return Err((StatusCode::BAD_REQUEST, "shard 0 always exists".to_string()));
    }

    let cmd = router::ShardConfigCommand::CreateShard {
        shard_id: req.shard_id,
    };
    let data = router::encode_shard_config(&cmd);

    let raft = state.pool.get_raft(0).await.expect("shard 0 must exist");
    let request = ReducerCallRequest {
        raw_message: data,
        origin_node_id: state.pool.node_config.node_id,
    };
    raft.client_write(request)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok((StatusCode::OK, format!("Shard {} created", req.shard_id)))
}

#[derive(Deserialize)]
struct AddRouteRequest {
    module_name: String,
    shard_id: ShardId,
}

/// Add a module -> shard route by proposing through shard 0's Raft.
async fn shard_add_route(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AddRouteRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let cmd = router::ShardConfigCommand::AddRoute {
        module_name: req.module_name.clone(),
        shard_id: req.shard_id,
    };
    let data = router::encode_shard_config(&cmd);

    let raft = state.pool.get_raft(0).await.expect("shard 0 must exist");
    let request = ReducerCallRequest {
        raw_message: data,
        origin_node_id: state.pool.node_config.node_id,
    };
    raft.client_write(request)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok((
        StatusCode::OK,
        format!("Route {} -> shard {} added", req.module_name, req.shard_id),
    ))
}

#[derive(Deserialize)]
struct RemoveRouteRequest {
    module_name: String,
}

/// Remove a module route by proposing through shard 0's Raft.
async fn shard_remove_route(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RemoveRouteRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let cmd = router::ShardConfigCommand::RemoveRoute {
        module_name: req.module_name.clone(),
    };
    let data = router::encode_shard_config(&cmd);

    let raft = state.pool.get_raft(0).await.expect("shard 0 must exist");
    let request = ReducerCallRequest {
        raw_message: data,
        origin_node_id: state.pool.node_config.node_id,
    };
    raft.client_write(request)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok((
        StatusCode::OK,
        format!("Route for {} removed", req.module_name),
    ))
}

#[derive(Serialize)]
struct ShardListResponse {
    shards: Vec<ShardId>,
    routes: HashMap<String, ShardId>,
}

/// List all active shards and routes.
async fn shard_list(State(state): State<Arc<AppState>>) -> Json<ShardListResponse> {
    let shards = state.pool.list_shards().await;
    let routes = state.pool.get_routes().await;

    Json(ShardListResponse { shards, routes })
}

#[derive(Serialize)]
struct ShardStatusResponse {
    shard_id: ShardId,
    state: String,
    current_leader: Option<u64>,
    current_term: u64,
    last_applied: Option<String>,
    membership: String,
}

/// Get status for a specific shard.
async fn shard_status(
    State(state): State<Arc<AppState>>,
    Path(shard_id): Path<u64>,
) -> Result<Json<ShardStatusResponse>, (StatusCode, String)> {
    let raft = get_shard_raft(&state.pool, shard_id).await?;
    let metrics = raft.metrics().borrow().clone();

    Ok(Json(ShardStatusResponse {
        shard_id,
        state: format!("{:?}", metrics.state),
        current_leader: metrics.current_leader,
        current_term: metrics.vote.leader_id().term,
        last_applied: metrics.last_applied.map(|id| format!("{}", id)),
        membership: format!("{:?}", metrics.membership_config),
    }))
}
