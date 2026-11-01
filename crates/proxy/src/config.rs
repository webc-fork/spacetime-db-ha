use anyhow::{Result, bail};

use crate::transport::normalize_addr;

/// 单个 spacetime-db-ha 节点的配置。
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// 集群内唯一节点 ID。
    pub node_id: u64,

    /// 客户端 Zenoh 明文 QUIC 监听地址。
    pub listen_addr: String,

    /// Raft 节点间通信监听地址（UDP 明文 QUIC 承载 Zenoh RPC，TCP 承载 `/metrics`）。
    pub raft_addr: String,

    /// 对外广播的 Raft 地址（缺省回退至 `raft_addr`，并将 `0.0.0.0` 映射为 `127.0.0.1`）。
    pub raft_advertise_addr: Option<String>,

    /// 本地 SpacetimeDB Zenoh 地址。
    pub stdb_url: String,

    /// Raft 对端列表，格式 `id=host:port`。
    pub peers: Vec<String>,

    /// 持久化数据目录。
    pub data_dir: String,
}

impl NodeConfig {
    /// 校验配置合法性。
    pub fn validate(&self) -> Result<()> {
        if self.node_id == 0 {
            bail!("node_id must be greater than 0");
        }
        if self.listen_addr.trim().is_empty() {
            bail!("listen_addr must not be empty");
        }
        if self.raft_addr.trim().is_empty() {
            bail!("raft_addr must not be empty");
        }
        if self.stdb_url.trim().is_empty() {
            bail!("stdb_url must not be empty");
        }
        Ok(())
    }

    /// 返回写入 Raft 成员表的可路由节点广播地址（修复 `0.0.0.0` 广播缺陷）。
    pub fn advertise_raft_addr(&self) -> String {
        let raw = self
            .raft_advertise_addr
            .as_deref()
            .unwrap_or(&self.raft_addr);
        normalize_addr(raw, true)
    }
}
