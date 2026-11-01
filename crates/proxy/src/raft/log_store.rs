use std::{
    collections::BTreeMap,
    ops::RangeBounds,
    sync::{Arc, Mutex},
};

use openraft::{
    Entry, LogId, LogState, RaftLogId, RaftLogReader, StorageError, Vote,
    storage::{LogFlushed, RaftLogStorage},
};

use crate::raft::types::TypeConfig;

type C = TypeConfig;

/// In-memory log store for development/testing.
/// Production uses PersistentLogStore (redb). Kept for reference.
#[derive(Clone, Default)]
pub struct LogStore {
    inner: Arc<Mutex<LogStoreInner>>,
}

#[derive(Default)]
struct LogStoreInner {
    vote: Option<Vote<u64>>,
    log: BTreeMap<u64, Entry<C>>,
    last_purged: Option<LogId<u64>>,
}

impl LogStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl RaftLogReader<C> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<C>>, StorageError<u64>> {
        let inner = self.inner.lock().unwrap();
        let entries: Vec<_> = inner.log.range(range).map(|(_, v)| v.clone()).collect();
        Ok(entries)
    }
}

impl RaftLogStorage<C> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<u64>> {
        let inner = self.inner.lock().unwrap();
        let last = inner.log.iter().next_back().map(|(_, v)| *v.get_log_id());
        Ok(LogState {
            last_purged_log_id: inner.last_purged,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.lock().unwrap();
        inner.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.vote)
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
        let mut inner = self.inner.lock().unwrap();
        for entry in entries {
            let idx = entry.get_log_id().index;
            inner.log.insert(idx, entry);
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.lock().unwrap();
        let to_remove: Vec<_> = inner.log.range(log_id.index..).map(|(k, _)| *k).collect();
        for key in to_remove {
            inner.log.remove(&key);
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.lock().unwrap();
        let to_remove: Vec<_> = inner.log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for key in to_remove {
            inner.log.remove(&key);
        }
        inner.last_purged = Some(log_id);
        Ok(())
    }
}
