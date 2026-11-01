//! 传输层常量与 `openraft_zenoh::transport` 重导出。

pub use openraft_zenoh::transport::*;

/// 集群管理与跨节点写转发命名空间与子路径。
pub const CLUSTER_NS: &str = "cluster";
pub const CLUSTER_PREFIX: &str = const_str::concat!(CLUSTER_NS, KEY_SEP);
pub const CLUSTER_QUERYABLE_ALL: &str = const_str::concat!(CLUSTER_PREFIX, KEY_WILDCARD);

pub const SUB_STATUS: &str = "status";
pub const SUB_INIT: &str = "init";
pub const SUB_ADD_NODE: &str = "add-node";
pub const SUB_REMOVE_NODE: &str = "remove-node";
pub const SUB_LEADER: &str = "leader";
pub const SUB_HEALTH: &str = "health";
pub const SUB_WRITE: &str = "write";
pub const SUB_WRITE_BATCH: &str = "write-batch";
pub const SUB_SHARDS: &str = "shards";
pub const SUB_SHARDS_PREFIX: &str = const_str::concat!(SUB_SHARDS, KEY_SEP);
pub const SUB_SHARDS_CREATE: &str = const_str::concat!(SUB_SHARDS_PREFIX, "create");
pub const SUB_SHARDS_REMOVE: &str = const_str::concat!(SUB_SHARDS_PREFIX, "remove");
pub const SUB_SHARDS_ROUTE: &str = const_str::concat!(SUB_SHARDS_PREFIX, "route");
pub const SUB_SHARDS_ROUTE_ADD: &str = const_str::concat!(SUB_SHARDS_ROUTE, "/add");
pub const SUB_SHARDS_ROUTE_REMOVE: &str = const_str::concat!(SUB_SHARDS_ROUTE, "/remove");

pub const SUFFIX_WRITE: &str = const_str::concat!(KEY_SEP, SUB_WRITE);
pub const SUFFIX_WRITE_BATCH: &str = const_str::concat!(KEY_SEP, SUB_WRITE_BATCH);
pub const SUFFIX_STATUS: &str = const_str::concat!(KEY_SEP, SUB_STATUS);

pub const KEY_CLUSTER_STATUS: &str = const_str::concat!(CLUSTER_PREFIX, SUB_STATUS);
pub const KEY_CLUSTER_INIT: &str = const_str::concat!(CLUSTER_PREFIX, SUB_INIT);
pub const KEY_CLUSTER_ADD_NODE: &str = const_str::concat!(CLUSTER_PREFIX, SUB_ADD_NODE);
pub const KEY_CLUSTER_REMOVE_NODE: &str = const_str::concat!(CLUSTER_PREFIX, SUB_REMOVE_NODE);
pub const KEY_CLUSTER_HEALTH: &str = const_str::concat!(CLUSTER_PREFIX, SUB_HEALTH);
pub const KEY_CLUSTER_SHARDS: &str = const_str::concat!(CLUSTER_PREFIX, SUB_SHARDS);
pub const KEY_CLUSTER_SHARDS_PREFIX: &str = const_str::concat!(CLUSTER_PREFIX, SUB_SHARDS_PREFIX);
pub const KEY_SHARDS_CREATE: &str = const_str::concat!(CLUSTER_PREFIX, SUB_SHARDS_CREATE);
pub const KEY_SHARDS_REMOVE: &str = const_str::concat!(CLUSTER_PREFIX, SUB_SHARDS_REMOVE);
pub const KEY_SHARDS_ROUTE_ADD: &str = const_str::concat!(CLUSTER_PREFIX, SUB_SHARDS_ROUTE_ADD);
pub const KEY_SHARDS_ROUTE_REMOVE: &str =
    const_str::concat!(CLUSTER_PREFIX, SUB_SHARDS_ROUTE_REMOVE);

/// 指标查询键表达式。
pub const KEY_METRICS: &str = "metrics";

/// SpacetimeDB 客户端与上游流命名空间与键前缀。
pub const STDB_NS: &str = "stdb";
pub const STDB_PREFIX: &str = const_str::concat!(STDB_NS, KEY_SEP);
pub const KEY_STDB_CONNECT: &str = const_str::concat!(STDB_PREFIX, "connect");
pub const STDB_C2S_PREFIX: &str = const_str::concat!(STDB_PREFIX, "c2s/");
pub const STDB_S2C_PREFIX: &str = const_str::concat!(STDB_PREFIX, "s2c/");
