//! 消息分类：标签字节 -> 处理路径。

use spacetime_db_ha::websocket::handler::{Message, MessageKind, classify_message, stdb_tags};

/// 构造带指定标签字节的二进制消息。
fn tagged(tag: u8) -> Message {
    Message::Binary(vec![tag, 0, 0, 0].into())
}

#[test]
fn reducer_and_procedure_calls_are_writes() {
    assert_eq!(
        classify_message(&tagged(stdb_tags::CALL_REDUCER)),
        MessageKind::Write
    );
    assert_eq!(
        classify_message(&tagged(stdb_tags::CALL_PROCEDURE)),
        MessageKind::Write
    );
    // 单字节写消息也是写
    assert_eq!(
        classify_message(&Message::Binary(vec![stdb_tags::CALL_REDUCER].into())),
        MessageKind::Write
    );
}

#[test]
fn read_tags_are_forwarded_directly() {
    for tag in [
        stdb_tags::SUBSCRIBE,
        stdb_tags::UNSUBSCRIBE,
        stdb_tags::ONE_OFF_QUERY,
    ] {
        assert_eq!(
            classify_message(&tagged(tag)),
            MessageKind::Read,
            "tag {tag}"
        );
    }
}

#[test]
fn unknown_tags_are_forwarded_as_reads() {
    // 未知标签不进 Raft，交给 SpacetimeDB 自行拒绝
    for tag in (stdb_tags::CALL_PROCEDURE + 1)..=u8::MAX {
        assert_eq!(
            classify_message(&tagged(tag)),
            MessageKind::Read,
            "tag {tag}"
        );
    }
}

#[test]
fn empty_and_non_binary_messages_pass_through() {
    for msg in [
        Message::Binary(Vec::new().into()),
        Message::Text("hello".into()),
        Message::Ping(Vec::new().into()),
        Message::Pong(Vec::new().into()),
        Message::Close(None),
    ] {
        assert_eq!(classify_message(&msg), MessageKind::PassThrough);
    }
}

#[test]
fn non_empty_binary_never_passes_through() {
    for tag in 0..=u8::MAX {
        assert_ne!(
            classify_message(&tagged(tag)),
            MessageKind::PassThrough,
            "tag {tag}"
        );
    }
}
