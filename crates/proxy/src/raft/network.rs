//! Raft 节点间网络层（特化 `openraft_zenoh::network` 至本 crate 的 `TypeConfig`）。

use openraft_zenoh::network;

use crate::raft::types::TypeConfig;

pub type NetworkFactory = network::NetworkFactory<TypeConfig>;
pub type NetworkConnection = network::NetworkConnection<TypeConfig>;
