//! 持久化日志存储（基于 `fjall`）：日志读写、截断、清理、分片目录与旧版迁移。

use fjall::Database;
use openraft::{
    LogId, RaftLogReader, Vote,
    entry::{EntryPayload, RaftEntry as _},
    storage::RaftLogStorage,
    vote::RaftLeaderId,
};
use spacetime_db_ha::raft::{
    persistent_log_store::PersistentLogStore,
    state_machine::StateMachineStore,
    types::{LeaderId, RaftEntry, RaftLogId},
};
use tempfile::TempDir;

fn log_id(term: u64, index: u64) -> RaftLogId {
    LogId::new(LeaderId::new(term, 0), index)
}

fn blank_entry(term: u64, index: u64) -> RaftEntry {
    RaftEntry {
        log_id: log_id(term, index),
        payload: EntryPayload::Blank,
    }
}

fn indices(entries: &[RaftEntry]) -> Vec<u64> {
    entries.iter().map(|e| e.log_id().index).collect()
}

#[compio::test]
async fn vote_survives_reopen() {
    let dir = TempDir::new().unwrap();

    {
        let mut store = PersistentLogStore::new(dir.path(), 0).unwrap();
        assert!(store.read_vote().await.unwrap().is_none());
        store.save_vote(&Vote::new(2, 1)).await.unwrap();
    }

    let mut store = PersistentLogStore::new(dir.path(), 0).unwrap();
    assert_eq!(store.read_vote().await.unwrap(), Some(Vote::new(2, 1)));
}

#[compio::test]
async fn appended_entries_are_readable_in_range() {
    let dir = TempDir::new().unwrap();
    let mut store = PersistentLogStore::new(dir.path(), 0).unwrap();
    store
        .append_entries(vec![
            blank_entry(1, 1),
            blank_entry(1, 2),
            blank_entry(1, 3),
        ])
        .unwrap();

    let entries = store.try_get_log_entries(1..3).await.unwrap();

    assert_eq!(indices(&entries), vec![1, 2]);
    let state = store.get_log_state().await.unwrap();
    assert_eq!(state.last_log_id, Some(log_id(1, 3)));
    assert!(state.last_purged_log_id.is_none());
}

#[compio::test]
async fn truncate_drops_the_tail() {
    let dir = TempDir::new().unwrap();
    let mut store = PersistentLogStore::new(dir.path(), 0).unwrap();
    store
        .append_entries(vec![
            blank_entry(1, 1),
            blank_entry(1, 2),
            blank_entry(1, 3),
        ])
        .unwrap();

    store.truncate_after(Some(log_id(1, 1))).await.unwrap();

    let remaining = store.try_get_log_entries(1..4).await.unwrap();
    assert_eq!(indices(&remaining), vec![1]);
}

#[compio::test]
async fn purge_drops_the_prefix_and_records_last_purged() {
    let dir = TempDir::new().unwrap();
    let mut store = PersistentLogStore::new(dir.path(), 0).unwrap();
    store
        .append_entries(vec![
            blank_entry(1, 1),
            blank_entry(1, 2),
            blank_entry(1, 3),
        ])
        .unwrap();

    store.purge(log_id(1, 2)).await.unwrap();

    let remaining = store.try_get_log_entries(1..4).await.unwrap();
    assert_eq!(indices(&remaining), vec![3]);

    let state = store.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(log_id(1, 2)));
    assert_eq!(state.last_log_id, Some(log_id(1, 3)));
}

#[compio::test]
async fn fully_purged_log_reports_last_purged_as_last_log_id() {
    let dir = TempDir::new().unwrap();
    let mut store = PersistentLogStore::new(dir.path(), 0).unwrap();
    store
        .append_entries(vec![blank_entry(1, 1), blank_entry(1, 2)])
        .unwrap();

    store.purge(log_id(1, 2)).await.unwrap();

    // 空表不能让 last_log_id 退回到 None，否则 openraft 判定状态回退
    let state = store.get_log_state().await.unwrap();
    assert_eq!(state.last_log_id, state.last_purged_log_id);
}

#[compio::test]
async fn each_shard_uses_its_own_database_file() {
    let dir = TempDir::new().unwrap();

    let _shard0 = PersistentLogStore::new(dir.path(), 0).unwrap();
    let _shard1 = PersistentLogStore::new(dir.path(), 1).unwrap();

    assert!(dir.path().join("shard-0/fjall").exists());
    assert!(dir.path().join("shard-1/fjall").exists());
}

#[compio::test]
async fn legacy_database_is_migrated_into_shard_dir() {
    let dir = TempDir::new().unwrap();
    let legacy = dir.path().join("fjall");
    // 旧版把数据库放在数据目录根部
    drop(Database::builder(&legacy).open().unwrap());
    assert!(legacy.exists());

    let _store = PersistentLogStore::new(dir.path(), 0).unwrap();

    assert!(!legacy.exists());
    assert!(dir.path().join("shard-0/fjall").exists());
}

#[compio::test]
async fn purge_clamps_both_deleted_entries_and_last_purged_meta_to_delivered() {
    let dir = TempDir::new().unwrap();
    let mut store = PersistentLogStore::new(dir.path(), 0).unwrap();
    let sm = StateMachineStore::open_with_db(store.db(), 0).unwrap();
    store.set_delivered_rx(sm.delivered_rx());

    store
        .append_entries(vec![
            blank_entry(1, 1),
            blank_entry(2, 2),
            blank_entry(2, 3),
        ])
        .unwrap();

    // 推进 delivered = 1
    sm.mark_delivered(1);

    // delivered = 1 时请求 purge(2, 3)：物理日志与元数据都必须夹紧到 (1, 1)，保留 2..=3
    store.purge(log_id(2, 3)).await.unwrap();
    let remaining = store.try_get_log_entries(1..4).await.unwrap();
    assert_eq!(indices(&remaining), vec![2, 3]);
    let state = store.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(log_id(1, 1)));
    assert_eq!(state.last_log_id, Some(log_id(2, 3)));

    // 推进 delivered = 3 后再次 purge(2, 3)，清理至 3
    sm.mark_delivered(3);
    store.purge(log_id(2, 3)).await.unwrap();
    let remaining_after = store.try_get_log_entries(1..4).await.unwrap();
    assert!(remaining_after.is_empty());
    let state_after = store.get_log_state().await.unwrap();
    assert_eq!(state_after.last_purged_log_id, Some(log_id(2, 3)));
}
