#![recursion_limit = "256"]

use std::{
    collections::BTreeMap, io::ErrorKind, path::Path, process::Command as ProcessCommand,
    time::Duration,
};

use anyhow::{Context, Result};
use clap_args::{
    arg,
    clap::{Command, value_parser},
};
use spacetime_db_ha::{
    api::{
        AddNodeRequest, AddRouteRequest, ClusterStatus, CreateShardRequest, RemoveNodeRequest,
        RemoveRouteRequest, RemoveShardRequest, ShardListResponse, ShardStatusResponse,
    },
    raft::parse_peer_spec,
    transport::{
        KEY_CLUSTER_ADD_NODE, KEY_CLUSTER_INIT, KEY_CLUSTER_REMOVE_NODE, KEY_CLUSTER_SHARDS,
        KEY_CLUSTER_SHARDS_PREFIX, KEY_CLUSTER_STATUS, KEY_SHARDS_CREATE, KEY_SHARDS_REMOVE,
        KEY_SHARDS_ROUTE_ADD, KEY_SHARDS_ROUTE_REMOVE, LOOPBACK_IPV4_PREFIX, SUFFIX_STATUS,
        mid_u64_key, normalize_addr, oneshot_rpc,
    },
};

/// 默认集群节点 Raft 地址（编译期组合）。
const DEFAULT_CLUSTER_ADDR: &str = const_str::concat!(LOOPBACK_IPV4_PREFIX, "4001");
/// 默认 SpacetimeDB 部署目标节点地址（编译期组合）。
const DEFAULT_STDB_SERVER_1: &str = const_str::concat!(LOOPBACK_IPV4_PREFIX, "5001");
const DEFAULT_STDB_SERVER_2: &str = const_str::concat!(LOOPBACK_IPV4_PREFIX, "5002");
const DEFAULT_STDB_SERVER_3: &str = const_str::concat!(LOOPBACK_IPV4_PREFIX, "5003");

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
                .arg(arg!(--"node-id" <NODE_ID> "Node ID to add").value_parser(value_parser!(u64)))
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
                    arg!(--servers [SERVERS] ... "SpacetimeDB server addresses").default_values([
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
                .arg(arg!(--"shard-id" <SHARD_ID> "Shard ID").value_parser(value_parser!(u64)))
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
            let status: ClusterStatus =
                oneshot_rpc(addr, KEY_CLUSTER_STATUS, &(), RPC_TIMEOUT).await?;

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
            let nodes = sub
                .get_many::<String>(ARG_NODES)
                .context("No nodes specified")?;
            let mut members: BTreeMap<u64, String> = BTreeMap::new();
            let mut first_addr: Option<String> = None;

            for node_spec in nodes {
                let (id, clean_addr) = parse_peer_spec(node_spec)?;
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

            let _: String = oneshot_rpc(&target, KEY_CLUSTER_INIT, &members, RPC_TIMEOUT).await?;
            println!("Cluster initialized successfully!");
        }

        Some((CMD_ADD_NODE, sub)) => {
            let node_id = *sub
                .get_one::<u64>(ARG_NODE_ID)
                .context("missing --node-id")?;
            let addr = sub.get_one::<String>(ARG_ADDR).context("missing --addr")?;
            let cluster = sub
                .get_one::<String>(ARG_CLUSTER)
                .map_or(DEFAULT_CLUSTER_ADDR, String::as_str);

            println!("Adding node {node_id} ({addr}) via {cluster}...");
            let req = AddNodeRequest {
                node_id,
                addr: normalize_addr(addr, true),
            };
            let _: String = oneshot_rpc(cluster, KEY_CLUSTER_ADD_NODE, &req, RPC_TIMEOUT).await?;
            println!("Node {node_id} added successfully!");
        }

        Some((CMD_DEPLOY, sub)) => {
            let module = sub
                .get_one::<String>(ARG_MODULE)
                .context("missing --module")?;
            let db_name = sub.get_one::<String>(ARG_NAME).cloned().unwrap_or_else(|| {
                Path::new(module)
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_else(|| "my_module".to_owned())
            });
            let servers = sub
                .get_many::<String>(ARG_SERVERS)
                .context("missing --servers")?;

            println!(
                "Deploying module '{}' from {} to {} node(s)...",
                db_name,
                module,
                servers.len()
            );

            let mut failed = 0;
            for server in servers {
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
            let node_id = *sub
                .get_one::<u64>(ARG_NODE_ID)
                .context("missing --node-id")?;
            let cluster = sub
                .get_one::<String>(ARG_CLUSTER)
                .map_or(DEFAULT_CLUSTER_ADDR, String::as_str);

            println!("Removing node {node_id} via {cluster}...");
            let req = RemoveNodeRequest { node_id };
            let _: String =
                oneshot_rpc(cluster, KEY_CLUSTER_REMOVE_NODE, &req, RPC_TIMEOUT).await?;
            println!("Node {node_id} removed successfully!");
        }

        Some((CMD_CREATE_SHARD, sub)) => {
            let shard_id = *sub
                .get_one::<u64>(ARG_SHARD_ID)
                .context("missing --shard-id")?;
            let cluster = sub
                .get_one::<String>(ARG_CLUSTER)
                .map_or(DEFAULT_CLUSTER_ADDR, String::as_str);

            println!("Creating shard {shard_id} via {cluster}...");
            let req = CreateShardRequest { shard_id };
            let _: String = oneshot_rpc(cluster, KEY_SHARDS_CREATE, &req, RPC_TIMEOUT).await?;
            println!("Shard {shard_id} created successfully!");
        }

        Some((CMD_REMOVE_SHARD, sub)) => {
            let shard_id = *sub
                .get_one::<u64>(ARG_SHARD_ID)
                .context("missing --shard-id")?;
            let cluster = sub
                .get_one::<String>(ARG_CLUSTER)
                .map_or(DEFAULT_CLUSTER_ADDR, String::as_str);

            println!("Removing shard {shard_id} via {cluster}...");
            let req = RemoveShardRequest { shard_id };
            let _: String = oneshot_rpc(cluster, KEY_SHARDS_REMOVE, &req, RPC_TIMEOUT).await?;
            println!("Shard {shard_id} removed successfully!");
        }

        Some((CMD_ADD_ROUTE, sub)) => {
            let module = sub
                .get_one::<String>(ARG_MODULE)
                .context("missing --module")?;
            let shard_id = *sub
                .get_one::<u64>(ARG_SHARD_ID)
                .context("missing --shard-id")?;
            let cluster = sub
                .get_one::<String>(ARG_CLUSTER)
                .map_or(DEFAULT_CLUSTER_ADDR, String::as_str);

            println!("Adding route {module} -> shard {shard_id} via {cluster}...");
            let req = AddRouteRequest {
                module_name: module.clone(),
                shard_id,
            };
            let _: String = oneshot_rpc(cluster, KEY_SHARDS_ROUTE_ADD, &req, RPC_TIMEOUT).await?;
            println!("Route {module} -> shard {shard_id} added!");
        }

        Some((CMD_REMOVE_ROUTE, sub)) => {
            let module = sub
                .get_one::<String>(ARG_MODULE)
                .context("missing --module")?;
            let cluster = sub
                .get_one::<String>(ARG_CLUSTER)
                .map_or(DEFAULT_CLUSTER_ADDR, String::as_str);

            println!("Removing route for {module} via {cluster}...");
            let req = RemoveRouteRequest {
                module_name: module.clone(),
            };
            let _: String =
                oneshot_rpc(cluster, KEY_SHARDS_ROUTE_REMOVE, &req, RPC_TIMEOUT).await?;
            println!("Route for {module} removed!");
        }

        Some((CMD_LIST_SHARDS, sub)) => {
            let addr = sub
                .get_one::<String>(ARG_ADDR)
                .map_or(DEFAULT_CLUSTER_ADDR, String::as_str);
            let data: ShardListResponse =
                oneshot_rpc(addr, KEY_CLUSTER_SHARDS, &(), RPC_TIMEOUT).await?;

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
            let shard_id = *sub
                .get_one::<u64>(ARG_SHARD_ID)
                .context("missing --shard-id")?;
            let addr = sub
                .get_one::<String>(ARG_ADDR)
                .map_or(DEFAULT_CLUSTER_ADDR, String::as_str);
            let key = mid_u64_key(KEY_CLUSTER_SHARDS_PREFIX, shard_id, SUFFIX_STATUS);
            let status: ShardStatusResponse = oneshot_rpc(addr, &key, &(), RPC_TIMEOUT).await?;

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
