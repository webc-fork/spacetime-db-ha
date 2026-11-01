use std::{
    fs,
    fs::create_dir_all,
    io,
    ops::{Bound, RangeBounds},
    path::{Path, PathBuf},
    sync::Arc,
};

use bitcode::{DecodeOwned, Encode};
use fjall::{Database, Keyspace, KeyspaceCreateOptions};
use openraft::{
    LogState, OptionalSend, RaftLogReader,
    storage::{IOFlushed, RaftLogStorage},
};
use parking_lot::Mutex;
use tracing::{debug, info};

use crate::{
    raft::{
        codec::{WireEntry, WireLogId, WireVote},
        error::{StoreError, storage_io_err},
        types::{RaftEntry, RaftLogId, RaftVote, TypeConfig},
    },
    transport::prefix_u64_key,
};

type C = TypeConfig;

/// fjall Keyspace：日志索引 (`u64::to_be_bytes()`) -> `bitcode` 序列化后的 `Entry`。
const LOG_KEYSPACE: &str = "raft_log";

/// fjall Keyspace：元数据键 -> `bitcode` 序列化值。
const META_KEYSPACE: &str = "raft_meta";

const META_VOTE: &str = "vote";
const META_LAST_PURGED: &str = "last_purged";

/// 每个分片统一使用单个 fjall 数据库目录（日志与状态机共用同一库，修复 P2-10）。
pub(crate) const DB_DIR: &str = "fjall";
const SHARD_DIR_PREFIX: &str = "shard-";

/// 将 `RangeBounds<u64>` 转换为大端序 8 字节数组区间，以保证字典序等于数值序。
fn to_be_bounds<RB: RangeBounds<u64>>(range: &RB) -> (Bound<[u8; 8]>, Bound<[u8; 8]>) {
    (
        range.start_bound().map(|&s| s.to_be_bytes()),
        range.end_bound().map(|&e| e.to_be_bytes()),
    )
}

/// 内存缓存的日志边界状态，消除 `get_log_state` 每次从磁盘读取并反序列化末尾条目的开销。
#[derive(Clone, Copy, Default)]
struct LogStateCache {
    last_purged: Option<RaftLogId>,
    last_log_id: Option<RaftLogId>,
}

/// fjall 支撑的持久化日志存储（纯 Rust LSM-tree、跨 Keyspace 原子批量提交）。
///
/// 每个分片一个数据库目录：`{data_dir}/shard-{shard_id}/fjall`。
#[derive(Clone)]
pub struct PersistentLogStore {
    db: Database,
    log_ks: Keyspace,
    meta_ks: Keyspace,
    cache: Arc<Mutex<LogStateCache>>,
}

impl PersistentLogStore {
    /// 打开或创建分片专属的 fjall 日志库，并在分片 0 上自动迁移旧版根目录。
    pub fn new(data_dir: &Path, shard_id: u64) -> Result<Self, StoreError> {
        let shard_dir = shard_dir(data_dir, shard_id);
        if shard_id == 0 {
            migrate_legacy_db(data_dir, &shard_dir)?;
        }
        create_dir_all(&shard_dir)?;

        let db_path = shard_dir.join(DB_DIR);
        let db = Database::builder(&db_path).open()?;
        let log_ks = db.keyspace(LOG_KEYSPACE, KeyspaceCreateOptions::default)?;
        let meta_ks = db.keyspace(META_KEYSPACE, KeyspaceCreateOptions::default)?;

        let store = Self {
            db,
            log_ks,
            meta_ks,
            cache: Arc::new(Mutex::new(LogStateCache::default())),
        };
        let last_purged = store
            .read_meta::<WireLogId>(META_LAST_PURGED)?
            .map(RaftLogId::from);
        let last_log_id = store
            .last_entry()?
            .map(|entry| entry.log_id)
            .or(last_purged);
        *store.cache.lock() = LogStateCache {
            last_purged,
            last_log_id,
        };

        debug!(path = %db_path.display(), shard_id, "Persistent fjall log store opened");
        Ok(store)
    }

    /// 返回底层 `fjall::Database` 引用，供同分片状态机共享同一数据库实例（修复 P2-10）。
    pub fn db(&self) -> &Database {
        &self.db
    }

    /// 读取当前记录的 `last_purged` 水位（O(1) 内存缓存读取）。
    pub fn last_purged_log_id(&self) -> Result<Option<RaftLogId>, StoreError> {
        Ok(self.cache.lock().last_purged)
    }

    /// 按闭区间读取日志条目，用于启动时重放 `(last_delivered, last_applied]`（修复 P0-2）。
    pub fn read_entries_range(
        &self,
        start_inclusive: u64,
        end_inclusive: u64,
    ) -> Result<Vec<RaftEntry>, StoreError> {
        if start_inclusive > end_inclusive {
            return Ok(Vec::new());
        }
        self.collect_range(start_inclusive..=end_inclusive)
    }

    /// 按任意区间读取并反序列化日志条目。
    fn collect_range<RB: RangeBounds<u64>>(&self, range: RB) -> Result<Vec<RaftEntry>, StoreError> {
        self.log_ks
            .range(to_be_bounds(&range))
            .map(|guard| {
                let value = guard.value()?;
                let wire: WireEntry = bitcode::decode(&value)?;
                Ok(RaftEntry::from(wire))
            })
            .collect()
    }

    /// 批量写入日志条目。`append` 与集成测试共用这一个入口。
    pub fn append_entries(
        &self,
        entries: impl IntoIterator<Item = RaftEntry>,
    ) -> Result<(), StoreError> {
        let mut batch = self.db.batch();
        let mut last_log_id = None;
        for entry in entries {
            let log_id = entry.log_id;
            last_log_id = Some(log_id);
            let bytes = bitcode::encode(&WireEntry::from(entry));
            batch.insert(&self.log_ks, log_id.index.to_be_bytes(), bytes);
        }
        let Some(log_id) = last_log_id else {
            return Ok(());
        };
        batch.commit()?;
        self.cache.lock().last_log_id = Some(log_id);
        Ok(())
    }

    /// 读元数据，缺省返回 None。
    fn read_meta<T: DecodeOwned>(&self, key: &str) -> Result<Option<T>, StoreError> {
        let Some(value) = self.meta_ks.get(key)? else {
            return Ok(None);
        };
        Ok(Some(bitcode::decode(&value)?))
    }

    /// 写元数据。
    fn write_meta<T: Encode>(&self, key: &str, value: &T) -> Result<(), StoreError> {
        let bytes = bitcode::encode(value);
        self.meta_ks.insert(key, bytes)?;
        Ok(())
    }

    /// 读出日志表最后一条条目，空表返回 None。
    fn last_entry(&self) -> Result<Option<RaftEntry>, StoreError> {
        let Some(guard) = self.log_ks.last_key_value() else {
            return Ok(None);
        };
        let value = guard.value()?;
        let wire: WireEntry = bitcode::decode(&value)?;
        Ok(Some(RaftEntry::from(wire)))
    }
}

/// 旧版把数据库放在数据目录根部；分片 0 首次启动时迁移到 `shard-0/`。
fn migrate_legacy_db(data_dir: &Path, shard_dir: &Path) -> Result<(), StoreError> {
    let legacy = data_dir.join(DB_DIR);
    let migrated = shard_dir.join(DB_DIR);
    if !legacy.exists() || migrated.exists() {
        return Ok(());
    }

    create_dir_all(shard_dir)?;
    fs::rename(&legacy, &migrated)?;
    info!(
        old = %legacy.display(),
        new = %migrated.display(),
        "Migrated legacy fjall database to shard-0 directory"
    );
    Ok(())
}

impl RaftLogReader<C> for PersistentLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<RaftEntry>, io::Error> {
        self.collect_range(range).map_err(storage_io_err)
    }

    async fn read_vote(&mut self) -> Result<Option<RaftVote>, io::Error> {
        self.read_meta::<WireVote>(META_VOTE)
            .map(|opt| opt.map(RaftVote::from))
            .map_err(storage_io_err)
    }
}

impl RaftLogStorage<C> for PersistentLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<C>, io::Error> {
        let cache = *self.cache.lock();
        Ok(LogState {
            last_purged_log_id: cache.last_purged,
            last_log_id: cache.last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &RaftVote) -> Result<(), io::Error> {
        self.write_meta(META_VOTE, &WireVote::from(*vote))
            .map_err(storage_io_err)
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<C>) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = RaftEntry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        self.append_entries(entries).map_err(storage_io_err)?;
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate_after(&mut self, last_log_id: Option<RaftLogId>) -> Result<(), io::Error> {
        let start_index = last_log_id.map_or(0, |id| id.index + 1);
        let mut batch = self.db.batch();
        for guard in self.log_ks.range(start_index.to_be_bytes()..) {
            let key = guard.key().map_err(storage_io_err)?;
            batch.remove(&self.log_ks, key);
        }
        batch.commit().map_err(storage_io_err)?;
        let mut cache = self.cache.lock();
        cache.last_log_id = last_log_id.or(cache.last_purged);
        Ok(())
    }

    async fn purge(&mut self, log_id: RaftLogId) -> Result<(), io::Error> {
        let mut batch = self.db.batch();
        for guard in self.log_ks.range(..=log_id.index.to_be_bytes()) {
            let key = guard.key().map_err(storage_io_err)?;
            batch.remove(&self.log_ks, key);
        }

        // last_purged 与日志删除放在同一个跨 Keyspace WriteBatch 中原子提交，保证崩溃后二者一致
        let bytes = bitcode::encode(&WireLogId::from(log_id));
        batch.insert(&self.meta_ks, META_LAST_PURGED, bytes);
        batch.commit().map_err(storage_io_err)?;
        let mut cache = self.cache.lock();
        cache.last_purged = Some(log_id);
        if cache
            .last_log_id
            .is_none_or(|last| last.index <= log_id.index)
        {
            cache.last_log_id = Some(log_id);
        }
        Ok(())
    }
}

/// 分片数据库目录，便于运维与测试定位（使用 `prefix_u64_key` 消除 `format!` 开销）。
pub(crate) fn shard_dir(data_dir: &Path, shard_id: u64) -> PathBuf {
    data_dir.join(prefix_u64_key(SHARD_DIR_PREFIX, shard_id))
}
