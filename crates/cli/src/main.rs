#![recursion_limit = "256"]

use std::{
    collections::BTreeMap, io::ErrorKind, path::Path, process::Command, result, time::Duration,
};

use anyhow::{Context, Result, anyhow};
use bitcode::{Decode, DecodeOwned, Encode};
use clap::{Parser, Subcommand};
use crossfire::mpsc::bounded_blocking_async;
use gxhash::HashMap;
use rimalloc::Rimalloc;
use zenoh::{
    Wait,
    bytes::ZBytes,
    config::Config,
    handlers::{Callback, IntoHandler},
    query::Reply,
};

#[global_allocator]
static ALLOC: Rimalloc = Rimalloc;

type RpcEnvelope<T> = result::Result<T, String>;

/// 默认集群节点 Raft 地址。
const DEFAULT_CLUSTER_ADDR: &str = "127.0.0.1:4001";

/// 单次 Zenoh RPC 查询超时。
const RPC_TIMEOUT: Duration = Duration::from_secs(10);

/// 单次 RPC 查询的回复通道容量。
const RPC_REPLY_CAPACITY: usize = 4;

/// 需要剥离的协议前缀列表。
const URL_PREFIXES: [&str; 7] = [
    "http://", "https://", "ws://", "wss://", "quic/", "udp/", "tcp/",
];

#[derive(Debug, Clone, Encode, Decode)]
struct ClusterStatus {
    node_id: u64,
    state: String,
    current_leader: Option<u64>,
    current_term: u64,
    last_applied: Option<String>,
    membership: String,
    active_shards: Vec<u64>,
}

#[derive(Debug, Clone, Encode, Decode)]
struct AddNodeRequest {
    node_id: u64,
    addr: String,
}

#[derive(Debug, Clone, Encode, Decode)]
struct RemoveNodeRequest {
    node_id: u64,
}

#[derive(Debug, Clone, Encode, Decode)]
struct CreateShardRequest {
    shard_id: u64,
}

#[derive(Debug, Clone, Encode, Decode)]
struct RemoveShardRequest {
    shard_id: u64,
}

#[derive(Debug, Clone, Encode, Decode)]
struct AddRouteRequest {
    module_name: String,
    shard_id: u64,
}

#[derive(Debug, Clone, Encode, Decode)]
struct RemoveRouteRequest {
    module_name: String,
}

#[derive(Debug, Clone, Encode, Decode)]
struct ShardListResponse {
    shards: Vec<u64>,
    routes: HashMap<String, u64>,
}

#[derive(Debug, Clone, Encode, Decode)]
struct ShardStatusResponse {
    shard_id: u64,
    state: String,
    current_leader: Option<u64>,
    current_term: u64,
    last_applied: Option<String>,
    membership: String,
}

/// 剥离 URL 协议前缀，规范化为 `host:port`。
fn normalize_addr(addr: &str) -> String {
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
    if let Some(port) = s.strip_prefix("0.0.0.0:") {
        return format!("127.0.0.1:{port}");
    }
    s.to_string()
}

/// 通过 Zenoh 明文 QUIC (`udp/<addr>#rel=1`) 发送 `bitcode` RPC 请求。
async fn zenoh_rpc<Req, Resp>(target_addr: &str, key_expr: &str, req: &Req) -> Result<Resp>
where
    Req: Encode,
    Resp: DecodeOwned,
{
    let host_port = normalize_addr(target_addr);
    let locator = format!("udp/{host_port}#rel=1");

    let mut cfg = Config::default();
    cfg.insert_json5("mode", "\"client\"")
        .map_err(|e| anyhow!("zenoh mode config: {e}"))?;
    cfg.insert_json5("scouting/multicast/enabled", "false")
        .map_err(|e| anyhow!("zenoh multicast config: {e}"))?;
    cfg.insert_json5("scouting/gossip/enabled", "false")
        .map_err(|e| anyhow!("zenoh gossip config: {e}"))?;
    cfg.insert_json5("listen/endpoints", "[]")
        .map_err(|e| anyhow!("zenoh listen config: {e}"))?;
    cfg.insert_json5("connect/endpoints", &format!("[\"{locator}\"]"))
        .map_err(|e| anyhow!("zenoh connect config: {e}"))?;

    let session = zenoh::open(cfg)
        .await
        .map_err(|e| anyhow!("Failed to connect to {locator}: {e}"))?;

    let payload = bitcode::encode(req);
    let (reply_tx, reply_rx) = bounded_blocking_async::<Reply>(RPC_REPLY_CAPACITY);
    let replies = session
        .get(key_expr)
        .payload(ZBytes::from(payload))
        .timeout(RPC_TIMEOUT)
        .with(
            (
                Callback::from(move |r: Reply| {
                    let _ = reply_tx.send(r);
                }),
                reply_rx,
            )
                .into_handler(),
        )
        .await
        .map_err(|e| anyhow!("Zenoh query {key_expr} failed: {e}"))?;

    let reply = replies
        .recv()
        .await
        .map_err(|_| anyhow!("No reply received from {locator} on {key_expr}"))?;
    let sample = reply
        .into_result()
        .map_err(|e| anyhow!("Zenoh query error on {key_expr}: {e}"))?;
    let bytes = sample.payload().to_bytes();
    let _ = session.close().wait();

    let envelope: RpcEnvelope<Resp> =
        bitcode::decode(&bytes).context("Failed to decode bitcode response")?;
    envelope.map_err(|msg| anyhow!("{msg}"))
}

#[derive(Parser)]
#[command(
    name = "sdha",
    about = "spacetime-db-ha cluster management CLI (Zenoh + Plaintext QUIC + bitcode)"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// 查看集群整体状态
    Status {
        /// 节点 Raft 地址 (host:port)
        #[arg(long, default_value = DEFAULT_CLUSTER_ADDR)]
        addr: String,
    },

    /// 向集群扩容新节点
    AddNode {
        /// 待加入的节点 ID
        #[arg(long)]
        node_id: u64,
        /// 新节点的 Raft 地址 (host:port)
        #[arg(long)]
        addr: String,
        /// 接收请求的现有集群节点地址
        #[arg(long, default_value = DEFAULT_CLUSTER_ADDR)]
        cluster: String,
    },

    /// 从集群移除节点
    RemoveNode {
        /// 待移除的节点 ID
        #[arg(long)]
        node_id: u64,
        /// 接收请求的现有集群节点地址
        #[arg(long, default_value = DEFAULT_CLUSTER_ADDR)]
        cluster: String,
    },

    /// 初始化新集群
    Init {
        /// 节点 ID 与地址列表（格式 `id=host:port`，如 `1=node1:4001`）
        #[arg(long, num_args = 1..)]
        nodes: Vec<String>,
    },

    /// 将 SpacetimeDB 模块发布到集群内全部节点
    Deploy {
        /// SpacetimeDB 模块目录路径
        #[arg(long)]
        module: String,
        /// 数据库名（缺省取模块目录名）
        #[arg(long)]
        name: Option<String>,
        /// SpacetimeDB 服务端地址列表
        #[arg(long, num_args = 1.., default_values_t = vec![
            "127.0.0.1:5001".to_string(),
            "127.0.0.1:5002".to_string(),
            "127.0.0.1:5003".to_string(),
        ])]
        servers: Vec<String>,
    },

    /// 创建新分片（Raft 组）
    CreateShard {
        /// 待创建的分片 ID
        #[arg(long)]
        shard_id: u64,
        /// 接收请求的现有集群节点地址
        #[arg(long, default_value = DEFAULT_CLUSTER_ADDR)]
        cluster: String,
    },

    /// 删除分片并级联清理其路由
    RemoveShard {
        /// 待删除的分片 ID
        #[arg(long)]
        shard_id: u64,
        /// 接收请求的现有集群节点地址
        #[arg(long, default_value = DEFAULT_CLUSTER_ADDR)]
        cluster: String,
    },

    /// 添加 模块 -> 分片 路由
    AddRoute {
        /// 模块/数据库名
        #[arg(long)]
        module: String,
        /// 目标分片 ID
        #[arg(long)]
        shard_id: u64,
        /// 接收请求的现有集群节点地址
        #[arg(long, default_value = DEFAULT_CLUSTER_ADDR)]
        cluster: String,
    },

    /// 删除 模块 -> 分片 路由
    RemoveRoute {
        /// 待解绑的模块/数据库名
        #[arg(long)]
        module: String,
        /// 接收请求的现有集群节点地址
        #[arg(long, default_value = DEFAULT_CLUSTER_ADDR)]
        cluster: String,
    },

    /// 列出所有分片与路由
    ListShards {
        /// 节点 Raft 地址
        #[arg(long, default_value = DEFAULT_CLUSTER_ADDR)]
        addr: String,
    },

    /// 查看指定分片的状态
    ShardStatus {
        /// 分片 ID
        #[arg(long)]
        shard_id: u64,
        /// 节点 Raft 地址
        #[arg(long, default_value = DEFAULT_CLUSTER_ADDR)]
        addr: String,
    },
}

#[compio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Status { addr } => {
            let status: ClusterStatus = zenoh_rpc(&addr, "cluster/status", &()).await?;

            println!("Cluster Status:");
            println!("  Node ID:    {}", status.node_id);
            println!("  State:      {}", status.state);
            println!("  Leader:     {:?}", status.current_leader);
            println!("  Term:       {}", status.current_term);
            println!("  Applied:    {:?}", status.last_applied);
            println!("  Membership: {}", status.membership);
            println!("  Shards:     {:?}", status.active_shards);
        }

        Commands::Init { nodes } => {
            let mut members: BTreeMap<u64, String> = BTreeMap::new();
            let mut first_addr: Option<String> = None;

            for node_spec in &nodes {
                let (id_str, addr) = node_spec.split_once('=').context(format!(
                    "Invalid node format '{node_spec}', expected 'id=host:port'"
                ))?;

                let id: u64 = id_str
                    .parse()
                    .context(format!("Invalid node ID '{id_str}'"))?;

                let clean_addr = normalize_addr(addr);
                if first_addr.is_none() {
                    first_addr = Some(clean_addr.clone());
                }

                members.insert(id, clean_addr);
            }

            let target = first_addr.context("No nodes specified")?;
            println!("Initializing cluster via {target}...");
            for (id, addr) in &members {
                println!("  Node {id}: {addr}");
            }

            let _: String = zenoh_rpc(&target, "cluster/init", &members).await?;
            println!("Cluster initialized successfully!");
        }

        Commands::AddNode {
            node_id,
            addr,
            cluster,
        } => {
            println!("Adding node {node_id} ({addr}) via {cluster}...");
            let req = AddNodeRequest {
                node_id,
                addr: normalize_addr(&addr),
            };
            let _: String = zenoh_rpc(&cluster, "cluster/add-node", &req).await?;
            println!("Node {node_id} added successfully!");
        }

        Commands::Deploy {
            module,
            name,
            servers,
        } => {
            let db_name = name.unwrap_or_else(|| {
                Path::new(&module)
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| "my_module".to_string())
            });

            println!(
                "Deploying module '{}' from {} to {} node(s)...",
                db_name,
                module,
                servers.len()
            );

            let mut failed = 0;
            for server in &servers {
                print!("  Publishing to {server}... ");
                let result = Command::new("spacetime")
                    .args(["publish", "--server", server, &module, &db_name])
                    .output();

                match result {
                    Ok(output) if output.status.success() => {
                        println!("OK");
                    }
                    Ok(output) => {
                        let stderr = String::from_utf8_lossy(&output.stderr);
                        println!("FAILED: {}", stderr.trim());
                        failed += 1;
                    }
                    Err(e) => {
                        if e.kind() == ErrorKind::NotFound {
                            anyhow::bail!(
                                "spacetime CLI not found. Install it: https://spacetimedb.com/install"
                            );
                        }
                        println!("FAILED: {e}");
                        failed += 1;
                    }
                }
            }

            if failed > 0 {
                anyhow::bail!("{failed} node(s) failed. All nodes must have the same module.");
            }
            println!("Module '{db_name}' deployed to all nodes!");
        }

        Commands::RemoveNode { node_id, cluster } => {
            println!("Removing node {node_id} via {cluster}...");
            let req = RemoveNodeRequest { node_id };
            let _: String = zenoh_rpc(&cluster, "cluster/remove-node", &req).await?;
            println!("Node {node_id} removed successfully!");
        }

        Commands::CreateShard { shard_id, cluster } => {
            println!("Creating shard {shard_id} via {cluster}...");
            let req = CreateShardRequest { shard_id };
            let _: String = zenoh_rpc(&cluster, "cluster/shards/create", &req).await?;
            println!("Shard {shard_id} created successfully!");
        }

        Commands::RemoveShard { shard_id, cluster } => {
            println!("Removing shard {shard_id} via {cluster}...");
            let req = RemoveShardRequest { shard_id };
            let _: String = zenoh_rpc(&cluster, "cluster/shards/remove", &req).await?;
            println!("Shard {shard_id} removed successfully!");
        }

        Commands::AddRoute {
            module,
            shard_id,
            cluster,
        } => {
            println!("Adding route {module} -> shard {shard_id} via {cluster}...");
            let req = AddRouteRequest {
                module_name: module,
                shard_id,
            };
            let _: String = zenoh_rpc(&cluster, "cluster/shards/route/add", &req).await?;
            println!("Route {} -> shard {shard_id} added!", req.module_name);
        }

        Commands::RemoveRoute { module, cluster } => {
            println!("Removing route for {module} via {cluster}...");
            let req = RemoveRouteRequest {
                module_name: module,
            };
            let _: String = zenoh_rpc(&cluster, "cluster/shards/route/remove", &req).await?;
            println!("Route for {} removed!", req.module_name);
        }

        Commands::ListShards { addr } => {
            let data: ShardListResponse = zenoh_rpc(&addr, "cluster/shards", &()).await?;

            println!("Active Shards:");
            for shard in &data.shards {
                println!("  Shard {shard}");
            }

            println!("\nRoutes:");
            if data.routes.is_empty() {
                println!("  (none — all modules go to shard 0)");
            }
            for (module, shard) in &data.routes {
                println!("  {module} -> shard {shard}");
            }
        }

        Commands::ShardStatus { shard_id, addr } => {
            let key = format!("cluster/shards/{shard_id}/status");
            let status: ShardStatusResponse = zenoh_rpc(&addr, &key, &()).await?;

            println!("Shard {shard_id} Status:");
            println!("  State:      {}", status.state);
            println!("  Leader:     {:?}", status.current_leader);
            println!("  Term:       {}", status.current_term);
            println!("  Applied:    {:?}", status.last_applied);
            println!("  Membership: {}", status.membership);
        }
    }

    Ok(())
}
