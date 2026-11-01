#![recursion_limit = "256"]

use std::env;

use anyhow::{Context, Result, anyhow};
use clap_args::{arg, clap::ArgMatches};
use spacetime_db_ha::{
    config::NodeConfig,
    server,
    transport::{LOOPBACK_IPV4_PREFIX, UNSPECIFIED_IPV4_PREFIX},
};
use tracing::info;
use tracing_subscriber::EnvFilter;

/// 默认日志过滤规则。
const DEFAULT_LOG_FILTER: &str = "spacetime_db_ha=info";
/// 默认持久化数据目录。
const DEFAULT_DATA_DIR: &str = "./data";

/// 默认客户端 Zenoh 明文 QUIC 监听地址（编译期组合）。
const DEFAULT_LISTEN_ADDR: &str = const_str::concat!(UNSPECIFIED_IPV4_PREFIX, "3001");
/// 默认 Raft 节点间通信监听地址（编译期组合）。
const DEFAULT_RAFT_ADDR: &str = const_str::concat!(UNSPECIFIED_IPV4_PREFIX, "4001");
/// 默认本地 SpacetimeDB Zenoh 明文 QUIC 地址（编译期组合）。
const DEFAULT_STDB_URL: &str = const_str::concat!(LOOPBACK_IPV4_PREFIX, "3000");

/// 环境变量公共前缀与各项配置键名（编译期基于 `const_str::concat!` 组合）。
const ENV_PREFIX: &str = "SPACETIME_DB_HA_";
const ENV_NODE_ID: &str = const_str::concat!(ENV_PREFIX, "NODE_ID");
const ENV_LISTEN_ADDR: &str = const_str::concat!(ENV_PREFIX, "LISTEN_ADDR");
const ENV_RAFT_ADDR: &str = const_str::concat!(ENV_PREFIX, "RAFT_ADDR");
const ENV_RAFT_ADVERTISE_ADDR: &str = const_str::concat!(ENV_PREFIX, "RAFT_ADVERTISE_ADDR");
const ENV_STDB_URL: &str = const_str::concat!(ENV_PREFIX, "STDB_URL");
const ENV_PEERS: &str = const_str::concat!(ENV_PREFIX, "PEERS");
const ENV_DATA_DIR: &str = const_str::concat!(ENV_PREFIX, "DATA_DIR");

/// CLI 参数名常量。
const ARG_NODE_ID: &str = "node-id";
const ARG_LISTEN_ADDR: &str = "listen-addr";
const ARG_RAFT_ADDR: &str = "raft-addr";
const ARG_RAFT_ADVERTISE_ADDR: &str = "raft-advertise-addr";
const ARG_STDB_URL: &str = "stdb-url";
const ARG_PEERS: &str = "peers";
const ARG_DATA_DIR: &str = "data-dir";

/// 优先读取命令行参数，未指定时回退环境变量，最后使用编译期默认值。
fn arg_or_env(matches: &ArgMatches, id: &str, env_key: &str, default: &str) -> String {
    matches
        .get_one::<String>(id)
        .cloned()
        .or_else(|| env::var(env_key).ok().filter(|v| !v.is_empty()))
        .unwrap_or_else(|| default.to_owned())
}

/// 从 `clap_args` 解析结果与环境变量构建节点配置。
fn build_node_config(matches: &ArgMatches) -> Result<NodeConfig> {
    let node_id_raw = matches
        .get_one::<String>(ARG_NODE_ID)
        .cloned()
        .or_else(|| env::var(ENV_NODE_ID).ok().filter(|v| !v.is_empty()))
        .ok_or_else(|| anyhow!("missing required --{ARG_NODE_ID} (or env {ENV_NODE_ID})"))?;
    let node_id: u64 = node_id_raw
        .parse()
        .with_context(|| format!("invalid node_id '{node_id_raw}'"))?;

    let listen_addr = arg_or_env(matches, ARG_LISTEN_ADDR, ENV_LISTEN_ADDR, DEFAULT_LISTEN_ADDR);
    let raft_addr = arg_or_env(matches, ARG_RAFT_ADDR, ENV_RAFT_ADDR, DEFAULT_RAFT_ADDR);
    let raft_advertise_addr = matches
        .get_one::<String>(ARG_RAFT_ADVERTISE_ADDR)
        .cloned()
        .or_else(|| env::var(ENV_RAFT_ADVERTISE_ADDR).ok().filter(|v| !v.is_empty()));
    let stdb_url = arg_or_env(matches, ARG_STDB_URL, ENV_STDB_URL, DEFAULT_STDB_URL);
    let data_dir = arg_or_env(matches, ARG_DATA_DIR, ENV_DATA_DIR, DEFAULT_DATA_DIR);

    let peers = if let Some(values) = matches.get_many::<String>(ARG_PEERS) {
        values.cloned().collect()
    } else if let Ok(raw) = env::var(ENV_PEERS) {
        raw.split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .collect()
    } else {
        Vec::new()
    };

    Ok(NodeConfig {
        node_id,
        listen_addr,
        raft_addr,
        raft_advertise_addr,
        stdb_url,
        peers,
        data_dir,
    })
}

#[compio::main]
async fn main() -> Result<()> {
    let Some(matches) = clap_args::parse!(|cmd| {
        cmd.about("Distributed clustering layer for SpacetimeDB over Zenoh plaintext QUIC")
            .arg(arg!(--"node-id" [NODE_ID] "Unique node ID in the cluster"))
            .arg(arg!(--"listen-addr" [LISTEN_ADDR] "Client Zenoh QUIC listen address"))
            .arg(arg!(--"raft-addr" [RAFT_ADDR] "Raft Zenoh QUIC listen address"))
            .arg(arg!(--"raft-advertise-addr" [RAFT_ADVERTISE_ADDR] "Routable Raft address advertised to peers"))
            .arg(arg!(--"stdb-url" [STDB_URL] "Local SpacetimeDB Zenoh QUIC endpoint"))
            .arg(arg!(--peers [PEERS] "Comma-separated peer Raft addresses (id=host:port)").value_delimiter(','))
            .arg(arg!(--"data-dir" [DATA_DIR] "Directory for persistent Raft log and state machine"))
    }) else {
        return Ok(());
    };

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::from(DEFAULT_LOG_FILTER)),
        )
        .init();

    let config = build_node_config(&matches)?;
    config.validate()?;

    info!(
        node_id = config.node_id,
        listen_addr = %config.listen_addr,
        raft_addr = %config.raft_addr,
        advertise_addr = %config.advertise_raft_addr(),
        stdb_url = %config.stdb_url,
        peers = ?config.peers,
        "Starting spacetime-db-ha (Compio + Rimalloc + Zenoh + Plaintext QUIC + bitcode)"
    );

    server::serve(config).await
}
