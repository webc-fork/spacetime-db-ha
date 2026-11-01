use std::{
    fs::{create_dir_all, rename},
    io::Error,
    ops::RangeBounds,
    path::Path,
    sync::Arc,
};

use openraft::{
    Entry, ErrorSubject, ErrorVerb, LogId, LogState, RaftLogId, RaftLogReader, StorageError, Vote,
    storage::{LogFlushed, RaftLogStorage},
};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::de::DeserializeOwned;
use tracing::debug;

use crate::raft::types::TypeConfig;

type C = TypeConfig;

/// redb table: log index (u64) -> serialized Entry (JSON bytes)
const LOG_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_log");

/// redb table: meta key (string) -> serialized value (JSON bytes)
const META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_meta");

const META_VOTE: &str = "vote";
const META_LAST_PURGED: &str = "last_purged";

/// Persistent log store backed by redb (pure Rust, ACID, zero C++ deps).
/// Survives node restarts — critical for production Raft clusters.
///
/// Each shard gets its own database file at `{data_dir}/shard-{shard_id}/raft-log.redb`.
#[derive(Clone)]
pub struct PersistentLogStore {
    db: Arc<Database>,
}

impl PersistentLogStore {
    /// Open or create a persistent log store for a specific shard.
    /// Creates `{data_dir}/shard-{shard_id}/raft-log.redb`.
    ///
    /// For shard 0, auto-migrates legacy `{data_dir}/raft-log.redb` if present.
    pub fn new(data_dir: &Path, shard_id: u64) -> Result<Self, StorageError<u64>> {
        let shard_dir = data_dir.join(format!("shard-{}", shard_id));

        // Migration: if shard 0 and old raft-log.redb exists at data_dir root, move it
        if shard_id == 0 {
            let old_path = data_dir.join("raft-log.redb");
            if old_path.exists() && !shard_dir.join("raft-log.redb").exists() {
                create_dir_all(&shard_dir).map_err(|e| {
                    StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Write, e)
                })?;
                let new_path = shard_dir.join("raft-log.redb");
                rename(&old_path, &new_path).map_err(|e| {
                    StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Write, e)
                })?;
                tracing::info!(
                    old = %old_path.display(),
                    new = %new_path.display(),
                    "Migrated legacy raft-log.redb to shard-0 directory"
                );
            }
        }

        create_dir_all(&shard_dir)
            .map_err(|e| StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Write, e))?;

        let db_path = shard_dir.join("raft-log.redb");
        let db = Database::create(&db_path).map_err(|e| {
            StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Write, Error::other(e))
        })?;

        // Ensure tables exist
        let write_txn = db.begin_write().map_err(|e| {
            StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Write, Error::other(e))
        })?;
        let _ = write_txn.open_table(LOG_TABLE);
        let _ = write_txn.open_table(META_TABLE);
        write_txn.commit().map_err(|e| {
            StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Write, Error::other(e))
        })?;

        debug!(path = %db_path.display(), shard_id, "Persistent log store opened");

        Ok(Self { db: Arc::new(db) })
    }

    fn read_meta<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, StorageError<u64>> {
        let read_txn = self.db.begin_read().map_err(|e| {
            StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Read, Error::other(e))
        })?;
        let table = read_txn.open_table(META_TABLE).map_err(|e| {
            StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Read, Error::other(e))
        })?;
        match table.get(key) {
            Ok(Some(value)) => {
                let bytes: &[u8] = value.value();
                let val: T = serde_json::from_slice(bytes).map_err(|e| {
                    StorageError::from_io_error(
                        ErrorSubject::Logs,
                        ErrorVerb::Read,
                        Error::other(e),
                    )
                })?;
                Ok(Some(val))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(StorageError::from_io_error(
                ErrorSubject::Logs,
                ErrorVerb::Read,
                Error::other(e),
            )),
        }
    }

    fn write_meta<T: serde::Serialize>(
        &self,
        key: &str,
        value: &T,
    ) -> Result<(), StorageError<u64>> {
        let json = serde_json::to_vec(value).map_err(|e| {
            StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Write, Error::other(e))
        })?;
        let write_txn = self.db.begin_write().map_err(|e| {
            StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Write, Error::other(e))
        })?;
        {
            let mut table = write_txn.open_table(META_TABLE).map_err(|e| {
                StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Write, Error::other(e))
            })?;
            table.insert(key, json.as_slice()).map_err(|e| {
                StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Write, Error::other(e))
            })?;
        }
        write_txn.commit().map_err(|e| {
            StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Write, Error::other(e))
        })?;
        Ok(())
    }
}

impl RaftLogReader<C> for PersistentLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<C>>, StorageError<u64>> {
        let read_txn = self.db.begin_read().map_err(|e| {
            StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Read, Error::other(e))
        })?;
        let table = read_txn.open_table(LOG_TABLE).map_err(|e| {
            StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Read, Error::other(e))
        })?;

        let mut entries = Vec::new();
        let iter = table.range::<u64>(range).map_err(|e| {
            StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Read, Error::other(e))
        })?;
        for item in iter {
            let (_key, value) = item.map_err(|e| {
                StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Read, Error::other(e))
            })?;
            let bytes: &[u8] = value.value();
            let entry: Entry<C> = serde_json::from_slice(bytes).map_err(|e| {
                StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Read, Error::other(e))
            })?;
            entries.push(entry);
        }
        Ok(entries)
    }
}

impl RaftLogStorage<C> for PersistentLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<u64>> {
        let last_purged: Option<LogId<u64>> = self.read_meta(META_LAST_PURGED)?;

        let read_txn = self.db.begin_read().map_err(|e| {
            StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Read, Error::other(e))
        })?;
        let table = read_txn.open_table(LOG_TABLE).map_err(|e| {
            StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Read, Error::other(e))
        })?;

        let last_log_id = match table.last() {
            Ok(Some((_key, value))) => {
                let bytes: &[u8] = value.value();
                let entry: Entry<C> = serde_json::from_slice(bytes).map_err(|e| {
                    StorageError::from_io_error(
                        ErrorSubject::Logs,
                        ErrorVerb::Read,
                        Error::other(e),
                    )
                })?;
                Some(*entry.get_log_id())
            }
            Ok(None) => None,
            Err(e) => {
                return Err(StorageError::from_io_error(
                    ErrorSubject::Logs,
                    ErrorVerb::Read,
                    Error::other(e),
                ));
            }
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        self.write_meta(META_VOTE, vote)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        self.read_meta(META_VOTE)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<C>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<C>> + Send,
        I::IntoIter: Send,
    {
        let write_txn = self.db.begin_write().map_err(|e| {
            StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Write, Error::other(e))
        })?;
        {
            let mut table = write_txn.open_table(LOG_TABLE).map_err(|e| {
                StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Write, Error::other(e))
            })?;
            for entry in entries {
                let idx = entry.get_log_id().index;
                let json = serde_json::to_vec(&entry).map_err(|e| {
                    StorageError::from_io_error(
                        ErrorSubject::Logs,
                        ErrorVerb::Write,
                        Error::other(e),
                    )
                })?;
                table.insert(idx, json.as_slice()).map_err(|e| {
                    StorageError::from_io_error(
                        ErrorSubject::Logs,
                        ErrorVerb::Write,
                        Error::other(e),
                    )
                })?;
            }
        }
        write_txn.commit().map_err(|e| {
            StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Write, Error::other(e))
        })?;

        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let write_txn = self.db.begin_write().map_err(|e| {
            StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Write, Error::other(e))
        })?;
        {
            let mut table = write_txn.open_table(LOG_TABLE).map_err(|e| {
                StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Write, Error::other(e))
            })?;
            // Collect keys to remove (from log_id.index to end)
            let keys: Vec<u64> = table
                .range(log_id.index..)
                .map_err(|e| {
                    StorageError::from_io_error(
                        ErrorSubject::Logs,
                        ErrorVerb::Read,
                        Error::other(e),
                    )
                })?
                .filter_map(|item| item.ok().map(|(k, _)| k.value()))
                .collect();
            for key in keys {
                let _ = table.remove(key);
            }
        }
        write_txn.commit().map_err(|e| {
            StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Write, Error::other(e))
        })?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let write_txn = self.db.begin_write().map_err(|e| {
            StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Write, Error::other(e))
        })?;
        {
            let mut table = write_txn.open_table(LOG_TABLE).map_err(|e| {
                StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Write, Error::other(e))
            })?;
            let keys: Vec<u64> = table
                .range(..=log_id.index)
                .map_err(|e| {
                    StorageError::from_io_error(
                        ErrorSubject::Logs,
                        ErrorVerb::Read,
                        Error::other(e),
                    )
                })?
                .filter_map(|item| item.ok().map(|(k, _)| k.value()))
                .collect();
            for key in keys {
                let _ = table.remove(key);
            }

            // Store last_purged in meta table within the same transaction
            let mut meta = write_txn.open_table(META_TABLE).map_err(|e| {
                StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Write, Error::other(e))
            })?;
            let json = serde_json::to_vec(&log_id).map_err(|e| {
                StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Write, Error::other(e))
            })?;
            meta.insert(META_LAST_PURGED, json.as_slice())
                .map_err(|e| {
                    StorageError::from_io_error(
                        ErrorSubject::Logs,
                        ErrorVerb::Write,
                        Error::other(e),
                    )
                })?;
        }
        write_txn.commit().map_err(|e| {
            StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Write, Error::other(e))
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use openraft::{CommittedLeaderId, entry::EntryPayload};
    use tempfile::TempDir;

    use super::*;

    fn make_entry(term: u64, index: u64) -> Entry<C> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(term, 0), index),
            payload: EntryPayload::Blank,
        }
    }

    /// Helper: write entries directly to redb (bypasses LogFlushed callback
    /// which is pub(crate) in openraft and can't be constructed in tests).
    fn write_entries_directly(store: &PersistentLogStore, entries: &[Entry<C>]) {
        let write_txn = store.db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(LOG_TABLE).unwrap();
            for entry in entries {
                let idx = entry.get_log_id().index;
                let json = serde_json::to_vec(entry).unwrap();
                table.insert(idx, json.as_slice()).unwrap();
            }
        }
        write_txn.commit().unwrap();
    }

    #[tokio::test]
    async fn test_persistent_store_vote_roundtrip() {
        let dir = TempDir::new().unwrap();
        let mut store = PersistentLogStore::new(dir.path(), 0).unwrap();

        assert!(store.read_vote().await.unwrap().is_none());

        let vote = Vote::new(1, 0);
        store.save_vote(&vote).await.unwrap();

        let restored = store.read_vote().await.unwrap().unwrap();
        assert_eq!(restored, vote);
    }

    #[tokio::test]
    async fn test_persistent_store_append_and_read() {
        let dir = TempDir::new().unwrap();
        let mut store = PersistentLogStore::new(dir.path(), 0).unwrap();

        let entries = vec![make_entry(1, 1), make_entry(1, 2), make_entry(1, 3)];
        write_entries_directly(&store, &entries);

        let read = store.try_get_log_entries(1..4).await.unwrap();
        assert_eq!(read.len(), 3);
        assert_eq!(read[0].get_log_id().index, 1);
        assert_eq!(read[2].get_log_id().index, 3);
    }

    #[tokio::test]
    async fn test_persistent_store_survives_reopen() {
        let dir = TempDir::new().unwrap();

        // Write some data
        {
            let mut store = PersistentLogStore::new(dir.path(), 0).unwrap();
            let vote = Vote::new(2, 1);
            store.save_vote(&vote).await.unwrap();

            let entries = vec![make_entry(2, 1), make_entry(2, 2)];
            write_entries_directly(&store, &entries);
        }

        // Reopen and verify data persisted
        {
            let mut store = PersistentLogStore::new(dir.path(), 0).unwrap();
            let vote = store.read_vote().await.unwrap().unwrap();
            assert_eq!(vote, Vote::new(2, 1));

            let entries = store.try_get_log_entries(1..3).await.unwrap();
            assert_eq!(entries.len(), 2);

            let state = store.get_log_state().await.unwrap();
            assert_eq!(state.last_log_id.unwrap().index, 2);
        }
    }

    #[tokio::test]
    async fn test_persistent_store_truncate() {
        let dir = TempDir::new().unwrap();
        let mut store = PersistentLogStore::new(dir.path(), 0).unwrap();

        let entries = vec![make_entry(1, 1), make_entry(1, 2), make_entry(1, 3)];
        write_entries_directly(&store, &entries);

        // Truncate from index 2 onward
        store
            .truncate(LogId::new(CommittedLeaderId::new(1, 0), 2))
            .await
            .unwrap();

        let remaining = store.try_get_log_entries(1..4).await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].get_log_id().index, 1);
    }

    #[tokio::test]
    async fn test_persistent_store_purge() {
        let dir = TempDir::new().unwrap();
        let mut store = PersistentLogStore::new(dir.path(), 0).unwrap();

        let entries = vec![make_entry(1, 1), make_entry(1, 2), make_entry(1, 3)];
        write_entries_directly(&store, &entries);

        // Purge up to index 2
        store
            .purge(LogId::new(CommittedLeaderId::new(1, 0), 2))
            .await
            .unwrap();

        let remaining = store.try_get_log_entries(1..4).await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].get_log_id().index, 3);

        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id.unwrap().index, 2);
    }

    #[tokio::test]
    async fn test_per_shard_path() {
        let dir = TempDir::new().unwrap();
        let _store0 = PersistentLogStore::new(dir.path(), 0).unwrap();
        let _store1 = PersistentLogStore::new(dir.path(), 1).unwrap();

        assert!(dir.path().join("shard-0/raft-log.redb").exists());
        assert!(dir.path().join("shard-1/raft-log.redb").exists());
    }

    #[tokio::test]
    async fn test_legacy_migration() {
        let dir = TempDir::new().unwrap();

        // Create a legacy raft-log.redb at the root
        {
            let db_path = dir.path().join("raft-log.redb");
            let _db = Database::create(&db_path).unwrap();
        }

        assert!(dir.path().join("raft-log.redb").exists());
        assert!(!dir.path().join("shard-0/raft-log.redb").exists());

        // Opening shard 0 should auto-migrate
        let _store = PersistentLogStore::new(dir.path(), 0).unwrap();

        assert!(!dir.path().join("raft-log.redb").exists());
        assert!(dir.path().join("shard-0/raft-log.redb").exists());
    }
}
