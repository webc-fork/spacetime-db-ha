#![recursion_limit = "256"]

use anyhow::Result;
use clap::Parser;
use spacetime_db_ha::{config::NodeConfig, server};
use tracing::info;
use tracing_subscriber::EnvFilter;

/// 默认日志过滤规则。
const DEFAULT_LOG_FILTER: &str = "spacetime_db_ha=info";

#[derive(Parser)]
#[command(
    name = "spacetime-db-ha",
    about = "Distributed clustering layer for SpacetimeDB over Zenoh plaintext QUIC"
)]
struct Cli {
    /// 集群内唯一节点 ID
    #[arg(long, env = "SPACETIME_DB_HA_NODE_ID")]
    node_id: u64,

    /// 客户端 Zenoh 明文 QUIC 监听地址
    #[arg(
        long,
        default_value = "0.0.0.0:3001",
        env = "SPACETIME_DB_HA_LISTEN_ADDR"
    )]
    listen_addr: String,

    /// Raft 节点间 Zenoh 明文 QUIC 通信监听地址
    #[arg(
        long,
        default_value = "0.0.0.0:4001",
        env = "SPACETIME_DB_HA_RAFT_ADDR"
    )]
    raft_addr: String,

    /// 对外广播的 Raft 地址（缺省回退至 raft_addr）
    #[arg(long, env = "SPACETIME_DB_HA_RAFT_ADVERTISE_ADDR")]
    raft_advertise_addr: Option<String>,

    /// 本地 SpacetimeDB Zenoh 明文 QUIC 端点地址
    #[arg(
        long,
        default_value = "127.0.0.1:3000",
        env = "SPACETIME_DB_HA_STDB_URL"
    )]
    stdb_url: String,

    /// 对端 Raft 地址列表，逗号分隔（格式：`id=host:port`）
    #[arg(long, env = "SPACETIME_DB_HA_PEERS", value_delimiter = ',')]
    peers: Vec<String>,

    /// Raft 日志与状态机持久化数据目录
    #[arg(long, default_value = "./data", env = "SPACETIME_DB_HA_DATA_DIR")]
    data_dir: String,
}

impl From<Cli> for NodeConfig {
    fn from(cli: Cli) -> Self {
        Self {
            node_id: cli.node_id,
            listen_addr: cli.listen_addr,
            raft_addr: cli.raft_addr,
            raft_advertise_addr: cli.raft_advertise_addr,
            stdb_url: cli.stdb_url,
            peers: cli.peers,
            data_dir: cli.data_dir,
        }
    }
}

#[compio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::from(DEFAULT_LOG_FILTER)),
        )
        .init();

    let config = NodeConfig::from(Cli::parse());
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
