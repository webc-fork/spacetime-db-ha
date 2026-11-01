#![recursion_limit = "256"]

use std::{
    collections::BTreeMap, io::ErrorKind, path::Path, process::Command as ProcessCommand, result,
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use bitcode::{Decode, DecodeOwned, Encode};
use clap_args::{
    arg,
    clap::{Command, value_parser},
};
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

/// 未指定 IPv4 监听地址前缀。
const UNSPECIFIED_IPV4_PREFIX: &str = "0.0.0.0:";
/// 本地回环 IPv4 地址前缀。
const LOOPBACK_IPV4_PREFIX: &str = "127.0.0.1:";

/// 默认集群节点 Raft 地址（编译期组合）。
const DEFAULT_CLUSTER_ADDR: &str = const_str::concat!(LOOPBACK_IPV4_PREFIX, "4001");
/// 默认 SpacetimeDB 部署目标节点地址（编译期组合）。
const DEFAULT_STDB_SERVER_1: &str = const_str::concat!(LOOPBACK_IPV4_PREFIX, "5001");
const DEFAULT_STDB_SERVER_2: &str = const_str::concat!(LOOPBACK_IPV4_PREFIX, "5002");
const DEFAULT_STDB_SERVER_3: &str = const_str::concat!(LOOPBACK_IPV4_PREFIX, "5003");

/// Zenoh 明文 QUIC 协议前缀与后缀。
const QUIC_SCHEME_PREFIX: &str = "udp/";
const QUIC_REL_SUFFIX: &str = "#rel=1";

/// Zenoh 配置常量。
const ZENOH_MODE_KEY: &str = "mode";
const ZENOH_MODE_CLIENT: &str = "\"client\"";
const ZENOH_MULTICAST_KEY: &str = "scouting/multicast/enabled";
const ZENOH_GOSSIP_KEY: &str = "scouting/gossip/enabled";
const ZENOH_LISTEN_KEY: &str = "listen/endpoints";
const ZENOH_CONNECT_KEY: &str = "connect/endpoints";
const ZENOH_FALSE: &str = "false";
const ZENOH_EMPTY_ARRAY: &str = "[]";
const JSON_ARRAY_PREFIX: &str = "[\"";
const JSON_ARRAY_SUFFIX: &str = "\"]";

/// 集群管理 Zenoh 键表达式（编译期基于 `const_str::concat!` 组合）。
const CLUSTER_PREFIX: &str = "cluster/";
const SHARDS_PREFIX: &str = const_str::concat!(CLUSTER_PREFIX, "shards/");
const SHARDS_ROUTE_PREFIX: &str = const_str::concat!(SHARDS_PREFIX, "route/");
const SUFFIX_STATUS: &str = "/status";

const KEY_CLUSTER_STATUS: &str = const_str::concat!(CLUSTER_PREFIX, "status");
const KEY_CLUSTER_INIT: &str = const_str::concat!(CLUSTER_PREFIX, "init");
const KEY_CLUSTER_ADD_NODE: &str = const_str::concat!(CLUSTER_PREFIX, "add-node");
const KEY_CLUSTER_REMOVE_NODE: &str = const_str::concat!(CLUSTER_PREFIX, "remove-node");
const KEY_CLUSTER_SHARDS: &str = const_str::concat!(CLUSTER_PREFIX, "shards");
const KEY_SHARDS_CREATE: &str = const_str::concat!(SHARDS_PREFIX, "create");
const KEY_SHARDS_REMOVE: &str = const_str::concat!(SHARDS_PREFIX, "remove");
const KEY_SHARDS_ROUTE_ADD: &str = const_str::concat!(SHARDS_ROUTE_PREFIX, "add");
const KEY_SHARDS_ROUTE_REMOVE: &str = const_str::concat!(SHARDS_ROUTE_PREFIX, "remove");

/// 子命令名称与公共参数常量。
const CMD_STATUS: &str = "status";
const CMD_INIT: &str = "init";
const CMD_ADD_NODE: &str = "add-node";
const CMD_REMOVE_NODE: &str = "remove-node";
const CMD_DEPLOY: &str = "deploy";
const CMD_CREATE_SHARD: &str = "create-shard";
const CMD_REMOVE_SHARD: &str = "remove-shard";
const CMD_ADD_ROUTE: &str = "add-route";
const CMD_REMOVE_ROUTE: &str = "remove-route";
const CMD_LIST_SHARDS: &str = "list-shards";
const CMD_SHARD_STATUS: &str = "shard-status";

const ARG_ADDR: &str = "addr";
const ARG_CLUSTER: &str = "cluster";
const ARG_NODE_ID: &str = "node-id";
const ARG_NODES: &str = "nodes";
const ARG_MODULE: &str = "module";
const ARG_NAME: &str = "name";
const ARG_SERVERS: &str = "servers";
const ARG_SHARD_ID: &str = "shard-id";

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

/// 零分配剥离 URL 协议前缀，返回 `host:port` 切片。
fn strip_addr_parts(addr: &str) -> &str {
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

/// 剥离 URL 协议前缀，规范化为 `host:port`（单次预分配，无 `format!` 开销）。
fn normalize_addr(addr: &str) -> String {
    let s = strip_addr_parts(addr);
    if let Some(port) = s.strip_prefix(UNSPECIFIED_IPV4_PREFIX) {
        let mut out = String::with_capacity(LOOPBACK_IPV4_PREFIX.len() + port.len());
        out.push_str(LOOPBACK_IPV4_PREFIX);
        out.push_str(port);
        return out;
    }
    s.to_owned()
}

/// 通过 Zenoh 明文 QUIC (`udp/<addr>#rel=1`) 发送 `bitcode` RPC 请求。
async fn zenoh_rpc<Req, Resp>(target_addr: &str, key_expr: &str, req: &Req) -> Result<Resp>
where
    Req: Encode,
    Resp: DecodeOwned,
{
    let host_port = normalize_addr(target_addr);
    let mut locator =
        String::with_capacity(QUIC_SCHEME_PREFIX.len() + host_port.len() + QUIC_REL_SUFFIX.len());
    locator.push_str(QUIC_SCHEME_PREFIX);
    locator.push_str(&host_port);
    locator.push_str(QUIC_REL_SUFFIX);

    let mut connect_json =
        String::with_capacity(JSON_ARRAY_PREFIX.len() + locator.len() + JSON_ARRAY_SUFFIX.len());
    connect_json.push_str(JSON_ARRAY_PREFIX);
    connect_json.push_str(&locator);
    connect_json.push_str(JSON_ARRAY_SUFFIX);

    let mut cfg = Config::default();
    cfg.insert_json5(ZENOH_MODE_KEY, ZENOH_MODE_CLIENT)
        .map_err(|e| anyhow!("zenoh mode config: {e}"))?;
    cfg.insert_json5(ZENOH_MULTICAST_KEY, ZENOH_FALSE)
        .map_err(|e| anyhow!("zenoh multicast config: {e}"))?;
    cfg.insert_json5(ZENOH_GOSSIP_KEY, ZENOH_FALSE)
        .map_err(|e| anyhow!("zenoh gossip config: {e}"))?;
    cfg.insert_json5(ZENOH_LISTEN_KEY, ZENOH_EMPTY_ARRAY)
        .map_err(|e| anyhow!("zenoh listen config: {e}"))?;
    cfg.insert_json5(ZENOH_CONNECT_KEY, &connect_json)
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

/// 构建 `sdha` 子命令树（配合 `clap_args::parse!` 自动处理 `-h`/`--help`/`-v`/`--version`/`--vv`）。
fn build_cli(cmd: Command) -> Command {
    cmd.about("spacetime-db-ha cluster management CLI (Zenoh + Plaintext QUIC + bitcode)")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new(CMD_STATUS)
                .about("Show overall cluster status")
                .arg(
                    arg!(--addr [ADDR] "Cluster node Raft address (host:port)")
                        .default_value(DEFAULT_CLUSTER_ADDR),
                ),
        )
        .subcommand(
            Command::new(CMD_INIT)
                .about("Initialize a new cluster")
                .arg(arg!(--nodes <NODES> ... "Node ID and address pairs (id=host:port)")),
        )
        .subcommand(
            Command::new(CMD_ADD_NODE)
                .about("Add a new node to the cluster")
                .arg(
                    arg!(--"node-id" <NODE_ID> "Node ID to add")
                        .value_parser(value_parser!(u64)),
                )
                .arg(arg!(--addr <ADDR> "Raft address of the new node (host:port)"))
                .arg(
                    arg!(--cluster [CLUSTER] "Existing cluster node address")
                        .default_value(DEFAULT_CLUSTER_ADDR),
                ),
        )
        .subcommand(
            Command::new(CMD_REMOVE_NODE)
                .about("Remove a node from the cluster")
                .arg(
                    arg!(--"node-id" <NODE_ID> "Node ID to remove")
                        .value_parser(value_parser!(u64)),
                )
                .arg(
                    arg!(--cluster [CLUSTER] "Existing cluster node address")
                        .default_value(DEFAULT_CLUSTER_ADDR),
                ),
        )
        .subcommand(
            Command::new(CMD_DEPLOY)
                .about("Publish a SpacetimeDB module to all nodes in the cluster")
                .arg(arg!(--module <MODULE> "Path to SpacetimeDB module directory"))
                .arg(arg!(--name [NAME] "Database name (defaults to module directory name)"))
                .arg(
                    arg!(--servers [SERVERS] ... "SpacetimeDB server addresses")
                        .default_values([
                            DEFAULT_STDB_SERVER_1,
                            DEFAULT_STDB_SERVER_2,
                            DEFAULT_STDB_SERVER_3,
                        ]),
                ),
        )
        .subcommand(
            Command::new(CMD_CREATE_SHARD)
                .about("Create a new Raft shard")
                .arg(
                    arg!(--"shard-id" <SHARD_ID> "Shard ID to create")
                        .value_parser(value_parser!(u64)),
                )
                .arg(
                    arg!(--cluster [CLUSTER] "Existing cluster node address")
                        .default_value(DEFAULT_CLUSTER_ADDR),
                ),
        )
        .subcommand(
            Command::new(CMD_REMOVE_SHARD)
                .about("Remove a shard and cascade-delete its routes")
                .arg(
                    arg!(--"shard-id" <SHARD_ID> "Shard ID to remove")
                        .value_parser(value_parser!(u64)),
                )
                .arg(
                    arg!(--cluster [CLUSTER] "Existing cluster node address")
                        .default_value(DEFAULT_CLUSTER_ADDR),
                ),
        )
        .subcommand(
            Command::new(CMD_ADD_ROUTE)
                .about("Add a module -> shard route")
                .arg(arg!(--module <MODULE> "Module/database name"))
                .arg(
                    arg!(--"shard-id" <SHARD_ID> "Target shard ID")
                        .value_parser(value_parser!(u64)),
                )
                .arg(
                    arg!(--cluster [CLUSTER] "Existing cluster node address")
                        .default_value(DEFAULT_CLUSTER_ADDR),
                ),
        )
        .subcommand(
            Command::new(CMD_REMOVE_ROUTE)
                .about("Remove a module -> shard route")
                .arg(arg!(--module <MODULE> "Module/database name to unbind"))
                .arg(
                    arg!(--cluster [CLUSTER] "Existing cluster node address")
                        .default_value(DEFAULT_CLUSTER_ADDR),
                ),
        )
        .subcommand(
            Command::new(CMD_LIST_SHARDS)
                .about("List all active shards and routes")
                .arg(
                    arg!(--addr [ADDR] "Cluster node Raft address")
                        .default_value(DEFAULT_CLUSTER_ADDR),
                ),
        )
        .subcommand(
            Command::new(CMD_SHARD_STATUS)
                .about("Show status of a specific shard")
                .arg(
                    arg!(--"shard-id" <SHARD_ID> "Shard ID")
                        .value_parser(value_parser!(u64)),
                )
                .arg(
                    arg!(--addr [ADDR] "Cluster node Raft address")
                        .default_value(DEFAULT_CLUSTER_ADDR),
                ),
        )
}

#[compio::main]
async fn main() -> Result<()> {
    let Some(matches) = clap_args::parse!(build_cli) else {
        return Ok(());
    };

    match matches.subcommand() {
        Some((CMD_STATUS, sub)) => {
            let addr = sub
                .get_one::<String>(ARG_ADDR)
                .map_or(DEFAULT_CLUSTER_ADDR, String::as_str);
            let status: ClusterStatus = zenoh_rpc(addr, KEY_CLUSTER_STATUS, &()).await?;

            println!("Cluster Status:");
            println!("  Node ID:    {}", status.node_id);
            println!("  State:      {}", status.state);
            println!("  Leader:     {:?}", status.current_leader);
            println!("  Term:       {}", status.current_term);
            println!("  Applied:    {:?}", status.last_applied);
            println!("  Membership: {}", status.membership);
            println!("  Shards:     {:?}", status.active_shards);
        }

        Some((CMD_INIT, sub)) => {
            let nodes: Vec<&String> = sub
                .get_many::<String>(ARG_NODES)
                .context("No nodes specified")?
                .collect();
            let mut members: BTreeMap<u64, String> = BTreeMap::new();
            let mut first_addr: Option<String> = None;

            for node_spec in nodes {
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

            let _: String = zenoh_rpc(&target, KEY_CLUSTER_INIT, &members).await?;
            println!("Cluster initialized successfully!");
        }

        Some((CMD_ADD_NODE, sub)) => {
            let node_id = *sub.get_one::<u64>(ARG_NODE_ID).context("missing --node-id")?;
            let addr = sub.get_one::<String>(ARG_ADDR).context("missing --addr")?;
            let cluster = sub
                .get_one::<String>(ARG_CLUSTER)
                .map_or(DEFAULT_CLUSTER_ADDR, String::as_str);

            println!("Adding node {node_id} ({addr}) via {cluster}...");
            let req = AddNodeRequest {
                node_id,
                addr: normalize_addr(addr),
            };
            let _: String = zenoh_rpc(cluster, KEY_CLUSTER_ADD_NODE, &req).await?;
            println!("Node {node_id} added successfully!");
        }

        Some((CMD_DEPLOY, sub)) => {
            let module = sub.get_one::<String>(ARG_MODULE).context("missing --module")?;
            let db_name = sub.get_one::<String>(ARG_NAME).cloned().unwrap_or_else(|| {
                Path::new(module)
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| "my_module".to_owned())
            });
            let servers: Vec<&String> = sub
                .get_many::<String>(ARG_SERVERS)
                .into_iter()
                .flatten()
                .collect();

            println!(
                "Deploying module '{}' from {} to {} node(s)...",
                db_name,
                module,
                servers.len()
            );

            let mut failed = 0;
            for server in &servers {
                print!("  Publishing to {server}... ");
                let result = ProcessCommand::new("spacetime")
                    .args(["publish", "--server", server, module, &db_name])
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

        Some((CMD_REMOVE_NODE, sub)) => {
            let node_id = *sub.get_one::<u64>(ARG_NODE_ID).context("missing --node-id")?;
            let cluster = sub
                .get_one::<String>(ARG_CLUSTER)
                .map_or(DEFAULT_CLUSTER_ADDR, String::as_str);

            println!("Removing node {node_id} via {cluster}...");
            let req = RemoveNodeRequest { node_id };
            let _: String = zenoh_rpc(cluster, KEY_CLUSTER_REMOVE_NODE, &req).await?;
            println!("Node {node_id} removed successfully!");
        }

        Some((CMD_CREATE_SHARD, sub)) => {
            let shard_id = *sub.get_one::<u64>(ARG_SHARD_ID).context("missing --shard-id")?;
            let cluster = sub
                .get_one::<String>(ARG_CLUSTER)
                .map_or(DEFAULT_CLUSTER_ADDR, String::as_str);

            println!("Creating shard {shard_id} via {cluster}...");
            let req = CreateShardRequest { shard_id };
            let _: String = zenoh_rpc(cluster, KEY_SHARDS_CREATE, &req).await?;
            println!("Shard {shard_id} created successfully!");
        }

        Some((CMD_REMOVE_SHARD, sub)) => {
            let shard_id = *sub.get_one::<u64>(ARG_SHARD_ID).context("missing --shard-id")?;
            let cluster = sub
                .get_one::<String>(ARG_CLUSTER)
                .map_or(DEFAULT_CLUSTER_ADDR, String::as_str);

            println!("Removing shard {shard_id} via {cluster}...");
            let req = RemoveShardRequest { shard_id };
            let _: String = zenoh_rpc(cluster, KEY_SHARDS_REMOVE, &req).await?;
            println!("Shard {shard_id} removed successfully!");
        }

        Some((CMD_ADD_ROUTE, sub)) => {
            let module = sub.get_one::<String>(ARG_MODULE).context("missing --module")?;
            let shard_id = *sub.get_one::<u64>(ARG_SHARD_ID).context("missing --shard-id")?;
            let cluster = sub
                .get_one::<String>(ARG_CLUSTER)
                .map_or(DEFAULT_CLUSTER_ADDR, String::as_str);

            println!("Adding route {module} -> shard {shard_id} via {cluster}...");
            let req = AddRouteRequest {
                module_name: module.clone(),
                shard_id,
            };
            let _: String = zenoh_rpc(cluster, KEY_SHARDS_ROUTE_ADD, &req).await?;
            println!("Route {} -> shard {shard_id} added!", req.module_name);
        }

        Some((CMD_REMOVE_ROUTE, sub)) => {
            let module = sub.get_one::<String>(ARG_MODULE).context("missing --module")?;
            let cluster = sub
                .get_one::<String>(ARG_CLUSTER)
                .map_or(DEFAULT_CLUSTER_ADDR, String::as_str);

            println!("Removing route for {module} via {cluster}...");
            let req = RemoveRouteRequest {
                module_name: module.clone(),
            };
            let _: String = zenoh_rpc(cluster, KEY_SHARDS_ROUTE_REMOVE, &req).await?;
            println!("Route for {} removed!", req.module_name);
        }

        Some((CMD_LIST_SHARDS, sub)) => {
            let addr = sub
                .get_one::<String>(ARG_ADDR)
                .map_or(DEFAULT_CLUSTER_ADDR, String::as_str);
            let data: ShardListResponse = zenoh_rpc(addr, KEY_CLUSTER_SHARDS, &()).await?;

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

        Some((CMD_SHARD_STATUS, sub)) => {
            let shard_id = *sub.get_one::<u64>(ARG_SHARD_ID).context("missing --shard-id")?;
            let addr = sub
                .get_one::<String>(ARG_ADDR)
                .map_or(DEFAULT_CLUSTER_ADDR, String::as_str);
            let key = format!("{SHARDS_PREFIX}{shard_id}{SUFFIX_STATUS}");
            let status: ShardStatusResponse = zenoh_rpc(addr, &key, &()).await?;

            println!("Shard {shard_id} Status:");
            println!("  State:      {}", status.state);
            println!("  Leader:     {:?}", status.current_leader);
            println!("  Term:       {}", status.current_term);
            println!("  Applied:    {:?}", status.last_applied);
            println!("  Membership: {}", status.membership);
        }

        _ => {}
    }

    Ok(())
}
