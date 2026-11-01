//! 分片路由与分片配置命令编解码。

use spacetime_db_ha::router::{
    DEFAULT_SHARD, SHARD_CONFIG_MAGIC, ShardConfigCommand, ShardRouter, decode_shard_config,
    encode_shard_config, extract_module_name,
};

#[test]
fn unrouted_module_falls_back_to_default_shard() {
    assert_eq!(ShardRouter::new().route("anything"), DEFAULT_SHARD);
}

#[test]
fn explicit_routes_win_over_default() {
    let mut router = ShardRouter::new();
    router.add_route("game-zone-forest".to_owned(), 1);
    router.add_route("game-zone-desert".to_owned(), 2);

    assert_eq!(router.route("game-zone-forest"), 1);
    assert_eq!(router.route("game-zone-desert"), 2);
    assert_eq!(router.route("unknown-db"), DEFAULT_SHARD);
}

#[test]
fn removing_a_route_restores_default() {
    let mut router = ShardRouter::new();
    router.add_route("my-db".to_owned(), 5);

    assert_eq!(router.remove_route("my-db"), Some(5));
    assert_eq!(router.route("my-db"), DEFAULT_SHARD);
    assert_eq!(router.remove_route("my-db"), None);
}

#[test]
fn remove_routes_for_shard_cascades() {
    let mut router = ShardRouter::new();
    router.add_route("db-a".to_owned(), 2);
    router.add_route("db-b".to_owned(), 2);
    router.add_route("db-c".to_owned(), 3);

    router.remove_routes_for_shard(2);
    assert_eq!(router.route("db-a"), DEFAULT_SHARD);
    assert_eq!(router.route("db-b"), DEFAULT_SHARD);
    assert_eq!(router.route("db-c"), 3);
}

#[test]
fn adding_same_route_twice_overwrites() {
    let mut router = ShardRouter::new();
    router.add_route("db".to_owned(), 1);
    router.add_route("db".to_owned(), 2);

    assert_eq!(router.route("db"), 2);
    assert_eq!(router.routes().len(), 1);
}

#[test]
fn module_name_is_extracted_from_database_paths() {
    assert_eq!(
        extract_module_name("/database/subscribe/mydb"),
        Some("mydb")
    );
    assert_eq!(extract_module_name("database/subscribe/mydb"), Some("mydb"));
    assert_eq!(
        extract_module_name("/database/subscribe/mydb?token=abc"),
        Some("mydb")
    );
    assert_eq!(
        extract_module_name("/database/subscribe/mydb/"),
        Some("mydb")
    );
}

#[test]
fn incomplete_or_foreign_paths_yield_no_module() {
    for path in [
        "/database/subscribe",
        "/database",
        "/",
        "",
        "/api/subscribe/mydb",
    ] {
        assert_eq!(extract_module_name(path), None, "path {path}");
    }
}

#[test]
fn every_config_command_survives_encode_decode() {
    let commands = [
        ShardConfigCommand::CreateShard { shard_id: 1 },
        ShardConfigCommand::RemoveShard { shard_id: 1 },
        ShardConfigCommand::AddRoute {
            module_name: "game".to_owned(),
            shard_id: 2,
        },
        ShardConfigCommand::RemoveRoute {
            module_name: "game".to_owned(),
        },
    ];

    for cmd in commands {
        let encoded = encode_shard_config(&cmd);
        assert!(encoded.starts_with(&SHARD_CONFIG_MAGIC));
        assert_eq!(decode_shard_config(&encoded), Some(cmd));
    }
}

#[test]
fn non_config_payloads_are_rejected() {
    assert!(decode_shard_config(&[]).is_none());
    assert!(decode_shard_config(&[3, 0, 1, 2]).is_none());
    // 仅单字节 0xFF 开头不再被误判为配置命令（修复 P2-14）
    assert!(decode_shard_config(&[0xFF, 0, 0, 0]).is_none());
    // 4 字节魔数正确但后续 bitcode 非法
    assert!(decode_shard_config(&[0xFF, b'S', b'F', b'G', 0xFF, 0xFF]).is_none());
}
