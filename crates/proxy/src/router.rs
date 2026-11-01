//! 分片路由：把客户端请求映射到对应的 Raft 组。
//!
//! 单分片阶段所有请求都落到分片 0；多分片阶段按模块名分流。
//! 路由表本身通过分片 0 的 Raft 日志复制，各节点状态机一致。

use bitcode::{Decode, Encode};
use gxhash::HashMap;

/// 分片标识。
pub type ShardId = u64;

/// 默认分片，始终存在。
pub const DEFAULT_SHARD: ShardId = 0;

/// 分片配置命令首字节标签。
pub const SHARD_CONFIG_TAG: u8 = 0xFF;

/// 分片配置命令魔数后缀。
const SHARD_CONFIG_MAGIC_SUFFIX: &str = "SFG";

/// 分片配置命令在 Raft 条目中的 4 字节魔数前缀：`[0xFF, 'S', 'F', 'G']`（编译期通过 `const_str::concat_bytes!` 合成）。
/// 配合 `db_path.is_empty()` 校验，彻底消除客户端消息 `0xFF` 首字节冲突（修复 P2-14）。
pub const SHARD_CONFIG_MAGIC: [u8; 4] =
    *const_str::concat_bytes!(SHARD_CONFIG_TAG, SHARD_CONFIG_MAGIC_SUFFIX);

/// 通过分片 0 的 Raft 日志复制的分片配置命令。
/// 提交后各节点自行落地：创建/删除分片、更新路由。
#[derive(Debug, Clone, Encode, Decode, PartialEq, Eq)]
pub enum ShardConfigCommand {
    CreateShard {
        shard_id: ShardId,
    },
    RemoveShard {
        shard_id: ShardId,
    },
    AddRoute {
        module_name: String,
        shard_id: ShardId,
    },
    RemoveRoute {
        module_name: String,
    },
}

/// 把分片配置命令编码成 Raft 条目负载：`[0xFF, 'S', 'F', 'G'][bitcode]`。
pub fn encode_shard_config(cmd: &ShardConfigCommand) -> Vec<u8> {
    let payload = bitcode::encode(cmd);
    let mut buf = Vec::with_capacity(SHARD_CONFIG_MAGIC.len() + payload.len());
    buf.extend_from_slice(&SHARD_CONFIG_MAGIC);
    buf.extend_from_slice(&payload);
    buf
}

/// 从原始字节解码分片配置命令；魔数前缀不匹配或 `bitcode` 非法时返回 `None`。
pub fn decode_shard_config(data: &[u8]) -> Option<ShardConfigCommand> {
    let payload = data.strip_prefix(&SHARD_CONFIG_MAGIC)?;
    bitcode::decode(payload).ok()
}

/// 从请求路径中提取模块名，形如 `/database/subscribe/MODULE` 或 `/v1/database/MODULE/...`。
pub fn extract_module_name(path: &str) -> Option<&str> {
    let mut parts = path.trim_start_matches('/').splitn(3, '/');
    let (Some("database"), Some(_), Some(rest)) = (parts.next(), parts.next(), parts.next()) else {
        return None;
    };

    // 去掉查询串与尾部斜杠
    let name = rest.split('?').next().unwrap_or(rest).trim_end_matches('/');
    (!name.is_empty()).then_some(name)
}

/// 模块名 -> 分片 ID 的路由表。
#[derive(Debug, Default)]
pub struct ShardRouter {
    routes: HashMap<String, ShardId>,
}

impl ShardRouter {
    /// 空路由表，未命中时回落到 [`DEFAULT_SHARD`]。
    pub fn new() -> Self {
        Self::default()
    }

    /// 查分片：命中显式路由则用之，否则用默认分片。
    pub fn route(&self, database_name: &str) -> ShardId {
        self.routes
            .get(database_name)
            .copied()
            .unwrap_or(DEFAULT_SHARD)
    }

    /// 注册 模块名 -> 分片 映射。
    pub fn add_route(&mut self, database_name: String, shard: ShardId) {
        self.routes.insert(database_name, shard);
    }

    /// 删除路由，返回原分片 ID。
    pub fn remove_route(&mut self, database_name: &str) -> Option<ShardId> {
        self.routes.remove(database_name)
    }

    /// 级联删除指向指定分片的全部路由。
    pub fn remove_routes_for_shard(&mut self, shard_id: ShardId) {
        self.routes.retain(|_, &mut sid| sid != shard_id);
    }

    /// 全部路由。
    pub fn routes(&self) -> &HashMap<String, ShardId> {
        &self.routes
    }
}
