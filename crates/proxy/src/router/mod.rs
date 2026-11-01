// Shard router — maps client requests to the correct Raft group.
//
// In Phase 1 (single shard), all requests go to shard 0.
// In Phase 2 (multi-shard), each module can be routed to its own Raft group.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Shard identifier.
pub type ShardId = u64;

/// Magic byte prefix for shard configuration commands in Raft entries.
/// SpacetimeDB BSATN tags are 0-4, so 0xFF is safe as an internal marker.
pub const SHARD_CONFIG_TAG: u8 = 0xFF;

/// Shard configuration commands replicated via shard 0's Raft log.
/// When committed, all nodes materialize the change (create shards, update routes).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ShardConfigCommand {
    CreateShard {
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

/// Encode a shard config command as a Raft entry payload.
/// Format: [0xFF] [JSON bytes]
pub fn encode_shard_config(cmd: &ShardConfigCommand) -> Vec<u8> {
    let mut buf = vec![SHARD_CONFIG_TAG];
    buf.extend(serde_json::to_vec(cmd).expect("shard config serialization"));
    buf
}

/// Try to decode a shard config command from raw bytes.
/// Returns None if the prefix isn't 0xFF or the JSON is invalid.
pub fn decode_shard_config(data: &[u8]) -> Option<ShardConfigCommand> {
    if data.first() != Some(&SHARD_CONFIG_TAG) {
        return None;
    }
    serde_json::from_slice(&data[1..]).ok()
}

/// Extract the module/database name from a WebSocket request path.
/// SpacetimeDB paths look like: `/database/subscribe/MODULE_NAME`
pub fn extract_module_name(path: &str) -> Option<&str> {
    let trimmed = path.trim_start_matches('/');
    let mut parts = trimmed.splitn(3, '/');
    let first = parts.next()?;
    let _second = parts.next()?;
    let third = parts.next()?;
    if first == "database" && !third.is_empty() {
        // Strip any query string or trailing slashes
        let name = third.split('?').next().unwrap_or(third);
        let name = name.trim_end_matches('/');
        if name.is_empty() { None } else { Some(name) }
    } else {
        None
    }
}

/// Maps database/module names to shard IDs.
pub struct ShardRouter {
    routes: HashMap<String, ShardId>,
    default_shard: ShardId,
}

impl ShardRouter {
    /// Create a new router. Everything goes to shard 0 by default.
    pub fn new() -> Self {
        Self {
            routes: HashMap::new(),
            default_shard: 0,
        }
    }

    /// Route a request to a shard.
    /// Returns the explicit route if one exists, otherwise the default shard.
    pub fn route(&self, database_name: &str) -> ShardId {
        self.routes
            .get(database_name)
            .copied()
            .unwrap_or(self.default_shard)
    }

    /// Register a database -> shard mapping.
    pub fn add_route(&mut self, database_name: String, shard: ShardId) {
        self.routes.insert(database_name, shard);
    }

    /// Remove a database -> shard mapping.
    pub fn remove_route(&mut self, database_name: &str) -> Option<ShardId> {
        self.routes.remove(database_name)
    }

    /// Get all registered routes.
    pub fn routes(&self) -> &HashMap<String, ShardId> {
        &self.routes
    }

    /// Number of registered routes.
    pub fn route_count(&self) -> usize {
        self.routes.len()
    }

    /// Set the default shard for unrouted requests.
    pub fn set_default_shard(&mut self, shard: ShardId) {
        self.default_shard = shard;
    }
}

impl Default for ShardRouter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- ShardRouter tests --

    #[test]
    fn test_default_shard_is_zero() {
        let router = ShardRouter::new();
        assert_eq!(router.route("anything"), 0);
    }

    #[test]
    fn test_explicit_route() {
        let mut router = ShardRouter::new();
        router.add_route("game-zone-forest".into(), 1);
        router.add_route("game-zone-desert".into(), 2);

        assert_eq!(router.route("game-zone-forest"), 1);
        assert_eq!(router.route("game-zone-desert"), 2);
        assert_eq!(router.route("unknown-db"), 0); // falls back to default
    }

    #[test]
    fn test_remove_route_falls_back() {
        let mut router = ShardRouter::new();
        router.add_route("my-db".into(), 5);
        assert_eq!(router.route("my-db"), 5);

        let removed = router.remove_route("my-db");
        assert_eq!(removed, Some(5));
        assert_eq!(router.route("my-db"), 0); // back to default
    }

    #[test]
    fn test_custom_default_shard() {
        let mut router = ShardRouter::new();
        router.set_default_shard(42);
        assert_eq!(router.route("anything"), 42);
    }

    #[test]
    fn test_route_count() {
        let mut router = ShardRouter::new();
        assert_eq!(router.route_count(), 0);

        router.add_route("a".into(), 1);
        router.add_route("b".into(), 2);
        assert_eq!(router.route_count(), 2);
    }

    #[test]
    fn test_overwrite_route() {
        let mut router = ShardRouter::new();
        router.add_route("db".into(), 1);
        router.add_route("db".into(), 2);
        assert_eq!(router.route("db"), 2);
        assert_eq!(router.route_count(), 1); // didn't create duplicate
    }

    #[test]
    fn test_remove_nonexistent_route() {
        let mut router = ShardRouter::new();
        assert_eq!(router.remove_route("nope"), None);
    }

    // -- extract_module_name tests --

    #[test]
    fn test_extract_module_name_standard_path() {
        assert_eq!(
            extract_module_name("/database/subscribe/mydb"),
            Some("mydb")
        );
    }

    #[test]
    fn test_extract_module_name_no_leading_slash() {
        assert_eq!(extract_module_name("database/subscribe/mydb"), Some("mydb"));
    }

    #[test]
    fn test_extract_module_name_with_query_string() {
        assert_eq!(
            extract_module_name("/database/subscribe/mydb?token=abc"),
            Some("mydb")
        );
    }

    #[test]
    fn test_extract_module_name_too_few_parts() {
        assert_eq!(extract_module_name("/database/subscribe"), None);
        assert_eq!(extract_module_name("/database"), None);
        assert_eq!(extract_module_name("/"), None);
        assert_eq!(extract_module_name(""), None);
    }

    #[test]
    fn test_extract_module_name_wrong_prefix() {
        assert_eq!(extract_module_name("/api/subscribe/mydb"), None);
    }

    #[test]
    fn test_extract_module_name_trailing_slash() {
        assert_eq!(
            extract_module_name("/database/subscribe/mydb/"),
            Some("mydb")
        );
    }

    // -- ShardConfigCommand encode/decode tests --

    #[test]
    fn test_shard_config_create_roundtrip() {
        let cmd = ShardConfigCommand::CreateShard { shard_id: 1 };
        let encoded = encode_shard_config(&cmd);
        assert_eq!(encoded[0], SHARD_CONFIG_TAG);
        let decoded = decode_shard_config(&encoded).unwrap();
        assert_eq!(decoded, cmd);
    }

    #[test]
    fn test_shard_config_add_route_roundtrip() {
        let cmd = ShardConfigCommand::AddRoute {
            module_name: "game".to_string(),
            shard_id: 2,
        };
        let encoded = encode_shard_config(&cmd);
        let decoded = decode_shard_config(&encoded).unwrap();
        assert_eq!(decoded, cmd);
    }

    #[test]
    fn test_shard_config_remove_route_roundtrip() {
        let cmd = ShardConfigCommand::RemoveRoute {
            module_name: "game".to_string(),
        };
        let encoded = encode_shard_config(&cmd);
        let decoded = decode_shard_config(&encoded).unwrap();
        assert_eq!(decoded, cmd);
    }

    #[test]
    fn test_decode_non_shard_config_returns_none() {
        // Normal SpacetimeDB message (tag 3 = CallReducer)
        let data = vec![3, 0, 1, 2];
        assert!(decode_shard_config(&data).is_none());
    }

    #[test]
    fn test_decode_empty_returns_none() {
        assert!(decode_shard_config(&[]).is_none());
    }

    #[test]
    fn test_decode_invalid_json_returns_none() {
        let data = vec![SHARD_CONFIG_TAG, 0, 0, 0];
        assert!(decode_shard_config(&data).is_none());
    }
}
