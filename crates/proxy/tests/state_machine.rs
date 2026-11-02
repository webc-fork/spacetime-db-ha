//! Raft 状态机：提交写转发、配置命令分发、快照构建与安装、记账信息持久化。

use std::{
    collections::{BTreeMap, BTreeSet},
    io::Cursor,
    time::Duration,
};

use bytes::Bytes;
use openraft::{
    BasicNode, LogId, Membership, RaftSnapshotBuilder, entry::EntryPayload,
    storage::RaftStateMachine, vote::RaftLeaderId,
};
use spacetime_db_ha::{
    raft::{
        CommittedWrite,
        state_machine::{
            CONFIG_CHANNEL_CAPACITY, ConfigReceiver, FORWARDER_CHANNEL_CAPACITY, StateMachineStore,
            WriteReceiver, config_channel, write_channel,
        },
        types::{LeaderId, RaftEntry, RaftLogId, ReducerCallRequest},
    },
    router::{ShardConfigCommand, encode_shard_config},
};
use tempfile::TempDir;

/// 测试用的数据库路径。
const DB_PATH: &str = "/database/subscribe/testdb";

fn log_id(term: u64, index: u64) -> RaftLogId {
    LogId::new(LeaderId::new(term, 0), index)
}

/// 构造一条普通写条目。
fn write_entry(term: u64, index: u64, origin_node_id: u64, payload: &[u8]) -> RaftEntry {
    RaftEntry {
        log_id: log_id(term, index),
        payload: EntryPayload::Normal(ReducerCallRequest {
            raw_message: Bytes::copy_from_slice(payload),
            db_path: DB_PATH.to_owned(),
            origin_node_id,
        }),
    }
}

/// 构造一条分片配置条目。
fn config_entry(term: u64, index: u64, cmd: &ShardConfigCommand) -> RaftEntry {
    RaftEntry {
        log_id: log_id(term, index),
        payload: EntryPayload::Normal(ReducerCallRequest {
            raw_message: encode_shard_config(cmd).into(),
            db_path: String::new(),
            origin_node_id: 1,
        }),
    }
}

/// 状态机 + 转发/配置通道 + 数据目录（目录需存活，否则 fjall 数据库目录被删）。
struct Harness {
    sm: StateMachineStore,
    writes: WriteReceiver,
    configs: ConfigReceiver,
    /// 持有数据目录，drop 时连同 fjall 数据库目录一起清理。
    _dir: TempDir,
}

fn harness() -> Harness {
    let dir = TempDir::new().unwrap();
    let (sm, writes, configs) = open_state_machine(&dir);
    Harness {
        sm,
        writes,
        configs,
        _dir: dir,
    }
}

fn open_state_machine(dir: &TempDir) -> (StateMachineStore, WriteReceiver, ConfigReceiver) {
    let sm = StateMachineStore::open(dir.path(), 0).unwrap();
    let (write_tx, writes) = write_channel(FORWARDER_CHANNEL_CAPACITY);
    let (config_tx, configs) = config_channel(CONFIG_CHANNEL_CAPACITY);
    sm.set_forwarder(write_tx);
    sm.set_shard_config_tx(config_tx);
    (sm, writes, configs)
}

#[compio::test]
async fn committed_writes_are_forwarded_with_target_database() {
    let mut h = harness();

    let results =
        h.sm.apply_entries(vec![
            write_entry(1, 1, 7, b"\x03payload"),
            RaftEntry {
                log_id: log_id(1, 2),
                payload: EntryPayload::Blank,
            },
        ])
        .await
        .unwrap();

    assert_eq!(results.len(), 2);
    let write = h.writes.recv().await.unwrap();
    assert_eq!(write.log_index, 1);
    assert_eq!(write.origin_node_id, 7);
    assert_eq!(write.db_path, DB_PATH);
    assert_eq!(&write.raw_message[..], b"\x03payload");
    assert!(h.configs.try_recv().is_err(), "普通写不应触发分片配置");

    // 前置写 (index=1) 尚未投递完成时，后续非写条目 (index=2) 暂缓推进 delivered 水位
    assert_eq!(h.sm.last_delivered_index(), 0);
    // 前置写投递完成后，delivered 水位自动跨越后续已应用的 Blank 条目推进至 2
    h.sm.mark_delivered(1);
    assert_eq!(h.sm.last_delivered_index(), 2);
}

#[compio::test]
async fn client_write_starting_with_0xff_is_not_treated_as_config() {
    // 修复 P2-14：带非空 db_path 的客户端二进制消息即使以 0xFF 开头也按普通写投递，不会丢弃
    let mut h = harness();

    let results =
        h.sm.apply_entries(vec![write_entry(1, 1, 2, &[0xFF, 1, 2, 3])])
            .await
            .unwrap();

    assert_eq!(results.len(), 1);
    let write = h.writes.recv().await.unwrap();
    assert_eq!(&write.raw_message[..], &[0xFF, 1, 2, 3]);
    assert!(h.configs.try_recv().is_err());
}

#[compio::test]
async fn blank_and_membership_entries_are_not_forwarded() {
    let mut h = harness();
    let membership = Membership::new(
        vec![BTreeSet::from([1u64])],
        BTreeMap::from([(
            1u64,
            BasicNode {
                addr: "node-1:4001".to_owned(),
            },
        )]),
    )
    .unwrap();

    let results =
        h.sm.apply_entries(vec![
            RaftEntry {
                log_id: log_id(1, 1),
                payload: EntryPayload::Blank,
            },
            RaftEntry {
                log_id: log_id(1, 2),
                payload: EntryPayload::Membership(membership),
            },
        ])
        .await
        .unwrap();

    assert_eq!(results.len(), 2);
    assert!(h.writes.try_recv().is_err());
    assert!(h.configs.try_recv().is_err());

    let (last_applied, stored_membership) = h.sm.applied_state().await.unwrap();
    assert_eq!(last_applied, Some(log_id(1, 2)));
    assert_eq!(
        stored_membership
            .membership()
            .voter_ids()
            .collect::<Vec<_>>(),
        vec![1]
    );
    assert_eq!(stored_membership.log_id(), &Some(log_id(1, 2)));
}

#[compio::test]
async fn shard_config_entries_are_dispatched_and_remove_shard_cascades() {
    let mut h = harness();

    h.sm.apply_entries(vec![
        config_entry(1, 1, &ShardConfigCommand::CreateShard { shard_id: 1 }),
        config_entry(
            1,
            2,
            &ShardConfigCommand::AddRoute {
                module_name: "game".to_owned(),
                shard_id: 1,
            },
        ),
        config_entry(1, 3, &ShardConfigCommand::RemoveShard { shard_id: 1 }),
    ])
    .await
    .unwrap();

    assert_eq!(
        h.configs.recv().await.unwrap(),
        ShardConfigCommand::CreateShard { shard_id: 1 }
    );
    assert_eq!(
        h.configs.recv().await.unwrap(),
        ShardConfigCommand::AddRoute {
            module_name: "game".to_owned(),
            shard_id: 1
        }
    );
    assert_eq!(
        h.configs.recv().await.unwrap(),
        ShardConfigCommand::RemoveShard { shard_id: 1 }
    );
    assert!(
        h.writes.try_recv().is_err(),
        "配置命令不应投递给 SpacetimeDB"
    );

    let (active_shards, routes) = h.sm.persisted_topology();
    assert!(active_shards.is_empty());
    assert!(routes.is_empty());
}

#[compio::test]
async fn malformed_config_entry_is_skipped_without_stopping_apply() {
    let mut h = harness();
    let entry = RaftEntry {
        log_id: log_id(1, 1),
        payload: EntryPayload::Normal(ReducerCallRequest {
            raw_message: Bytes::from_static(&[0xFF, 0, 0, 0]),
            db_path: String::new(),
            origin_node_id: 1,
        }),
    };

    let results = h.sm.apply_entries(vec![entry]).await.unwrap();

    assert_eq!(results.len(), 1);
    assert!(h.writes.try_recv().is_err());
    assert!(h.configs.try_recv().is_err());
    assert_eq!(h.sm.applied_state().await.unwrap().0, Some(log_id(1, 1)));
}

#[compio::test]
async fn applied_state_survives_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let (mut sm, _writes, _configs) = open_state_machine(&dir);
        sm.apply_entries(vec![write_entry(2, 5, 1, b"\x03a")])
            .await
            .unwrap();
        sm.mark_delivered(5);
    }

    let (mut reopened, _writes, _configs) = open_state_machine(&dir);
    assert_eq!(
        reopened.applied_state().await.unwrap().0,
        Some(log_id(2, 5))
    );
    assert_eq!(reopened.last_delivered_index(), 5);

    let mut applied = reopened.applied_rx();
    assert!(*applied.borrow_and_update() >= 5);

    let snapshot = reopened.get_current_snapshot().await.unwrap().unwrap();
    assert_eq!(snapshot.meta.last_log_id, Some(log_id(2, 5)));
}

#[compio::test]
async fn snapshot_roundtrip_restores_state_and_replays_config() {
    let mut source = harness();
    source
        .sm
        .apply_entries(vec![
            config_entry(2, 1, &ShardConfigCommand::CreateShard { shard_id: 1 }),
            config_entry(
                2,
                2,
                &ShardConfigCommand::AddRoute {
                    module_name: "game".to_owned(),
                    shard_id: 1,
                },
            ),
            write_entry(2, 3, 1, b"\x03move"),
        ])
        .await
        .unwrap();

    let snapshot = source.sm.build_snapshot().await.unwrap();

    let mut restored = harness();
    // 模拟快照安装前存在未推进的非写条目（前置写 1 未完成，Blank 2 挂起），安装快照到 3 后必须清理并推进 delivered 至 3
    restored
        .sm
        .apply_entries(vec![
            write_entry(1, 1, 1, b"\x03old"),
            RaftEntry {
                log_id: log_id(1, 2),
                payload: EntryPayload::Blank,
            },
        ])
        .await
        .unwrap();
    let _ = restored.writes.recv().await.unwrap();
    assert_eq!(restored.sm.last_delivered_index(), 0);

    let bytes = snapshot.snapshot.into_inner();
    restored
        .sm
        .install_snapshot(&snapshot.meta, Cursor::new(bytes))
        .await
        .unwrap();

    let (source_applied, source_membership) = source.sm.applied_state().await.unwrap();
    let (restored_applied, restored_membership) = restored.sm.applied_state().await.unwrap();
    assert_eq!(restored_applied, source_applied);
    assert_eq!(restored_membership, source_membership);
    assert_eq!(restored.sm.last_delivered_index(), 3);

    assert_eq!(
        restored.configs.recv().await.unwrap(),
        ShardConfigCommand::CreateShard { shard_id: 1 }
    );
    assert_eq!(
        restored.configs.recv().await.unwrap(),
        ShardConfigCommand::AddRoute {
            module_name: "game".to_owned(),
            shard_id: 1
        }
    );
    assert!(restored.writes.try_recv().is_err());
}

#[compio::test]
async fn built_snapshot_is_cached_and_repeatable() {
    let mut h = harness();
    assert!(h.sm.get_current_snapshot().await.unwrap().is_none());

    h.sm.apply_entries(vec![write_entry(1, 1, 1, b"\x03a")])
        .await
        .unwrap();
    let built = h.sm.build_snapshot().await.unwrap();

    let cached = h.sm.get_current_snapshot().await.unwrap().unwrap();
    assert_eq!(cached.meta, built.meta);
    assert_eq!(cached.snapshot.get_ref(), built.snapshot.get_ref());
}

#[compio::test]
async fn out_of_order_delivery_never_skips_holes() {
    // 修复 P0-2 验证：乱序投递（先完成写 2、后完成写 1）时，连续水位绝不跨越未完成的写 1
    let mut h = harness();

    h.sm.apply_entries(vec![
        write_entry(1, 1, 1, b"\x03w1"),
        write_entry(1, 2, 1, b"\x03w2"),
        RaftEntry {
            log_id: log_id(1, 3),
            payload: EntryPayload::Blank,
        },
    ])
    .await
    .unwrap();

    assert_eq!(h.sm.last_delivered_index(), 0);

    // 写 2 先投递完成：由于写 1 仍在途，连续水位必须停留在 0
    h.sm.mark_delivered(2);
    assert_eq!(h.sm.last_delivered_index(), 0);

    // 写 1 投递完成：连续水位一次性跨越写 1、已完成的写 2 以及末尾的 Blank 3，推进至 3
    h.sm.mark_delivered(1);
    assert_eq!(h.sm.last_delivered_index(), 3);
}

#[compio::test]
async fn startup_replay_with_trailing_blank_never_jumps_watermark() {
    // 修复 P0-1 验证：重放区间含 [未投递写 1, Blank 2] 时，advance_delivered 不会跨越未投递写 1
    let mut h = harness();

    h.sm.apply_entries(vec![
        write_entry(1, 1, 1, b"\x03w1"),
        RaftEntry {
            log_id: log_id(1, 2),
            payload: EntryPayload::Blank,
        },
    ])
    .await
    .unwrap();

    // 模拟重启重放登记在途写 1 并调用 advance_delivered
    h.sm.register_in_flight_write(1);
    h.sm.advance_delivered();
    assert_eq!(h.sm.last_delivered_index(), 0);

    // 待转发器完成写 1 投递后，水位推进至 2
    h.sm.mark_delivered(1);
    assert_eq!(h.sm.last_delivered_index(), 2);
}

#[compio::test]
async fn stalled_in_flight_writes_are_detected_and_requeued() {
    let mut h = harness();

    h.sm.apply_entries(vec![
        write_entry(1, 1, 2, b"\x03stalled"),
        write_entry(1, 2, 2, b"\x03done"),
    ])
    .await
    .unwrap();
    let _ = h.writes.recv().await.unwrap();
    let _ = h.writes.recv().await.unwrap();

    // 写 2 完成，但写 1 滞留在途
    h.sm.mark_delivered(2);
    assert_eq!(h.sm.last_delivered_index(), 0);

    // 阈值为 0 时立即识别出滞留的写 1
    let stalled = h.sm.take_stalled_writes(Duration::ZERO);
    assert_eq!(stalled, vec![1]);

    // 模拟从日志重读并补投写 1
    h.sm.requeue_write(CommittedWrite {
        log_index: 1,
        origin_node_id: 0,
        db_path: DB_PATH.to_owned(),
        raw_message: Bytes::from_static(b"\x03stalled"),
    });
    let re_sent = h.writes.recv().await.unwrap();
    assert_eq!(re_sent.log_index, 1);

    h.sm.mark_delivered(1);
    assert_eq!(h.sm.last_delivered_index(), 2);
}
