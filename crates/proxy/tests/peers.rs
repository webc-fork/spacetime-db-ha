//! 对端节点列表解析。

use spacetime_db_ha::raft::parse_peers;

#[test]
fn parses_id_address_pairs() {
    let peers = vec!["2=node-2:4001".to_owned(), "3=node-3:4001".to_owned()];

    let map = parse_peers(&peers);

    assert_eq!(map.len(), 2);
    assert_eq!(map[&2].addr, "node-2:4001");
    assert_eq!(map[&3].addr, "node-3:4001");
}

#[test]
fn skips_malformed_entries_but_keeps_valid_ones() {
    let peers = vec![
        "bad-format".to_owned(),
        "abc=node:4001".to_owned(),
        "1=node-1:4001".to_owned(),
    ];

    let map = parse_peers(&peers);

    assert_eq!(map.len(), 1);
    assert_eq!(map[&1].addr, "node-1:4001");
}

#[test]
fn empty_input_yields_empty_map() {
    assert!(parse_peers(&[]).is_empty());
}
