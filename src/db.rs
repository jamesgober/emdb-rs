// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Core database implementation.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

#[cfg(feature = "ttl")]
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::builder::EmdbBuilder;
use crate::index::{Index, Shard};
use crate::lockfile::LockFile;
use crate::storage::migrate::migrate_if_needed;
use crate::storage::page_store::PageStorage;
use crate::storage::{build_flags, Op, OpRef, SnapshotEntry, SnapshotIter, Storage};
#[cfg(feature = "ttl")]
use crate::ttl::{
    expires_from_ttl, is_expired, now_unix_millis, record_expires_at, record_set_persist,
    remaining_ttl, Ttl,
};
use crate::ttl::{record_into_value, record_new, record_value, Record};
use crate::Result;
use crate::{Error, FlushPolicy};

/// The primary embedded database handle.
///
/// `Emdb` is cheap to clone. Clones refer to the same underlying state.
pub struct Emdb {
    pub(crate) inner: Arc<Inner>,
}

/// Shared inner state behind [`Emdb`].
pub(crate) struct Inner {
    /// Sharded primary index. Reads on different shards are fully parallel;
    /// writes contend only on the target shard plus, for persistent databases,
    /// the backend mutex.
    pub(crate) index: Index,

    /// Highest committed transaction id, shared between handles and threads.
    pub(crate) last_tx_id: AtomicU64,

    /// Persistent storage. `None` means in-memory mode — every write skips
    /// both the mutex and the WAL append.
    pub(crate) backend: Option<Mutex<PageStorage>>,

    pub(crate) config: Config,
    _lock_file: Option<LockFile>,
}

/// Immutable runtime configuration.
pub(crate) struct Config {
    pub(crate) path: Option<PathBuf>,
    #[cfg(feature = "ttl")]
    pub(crate) default_ttl: Option<Duration>,
}

impl Clone for Emdb {
    fn clone(&self) -> Self {
        self.clone_handle()
    }
}

impl Emdb {
    /// Open a new in-memory database.
    #[must_use]
    pub fn open_in_memory() -> Self {
        Self {
            inner: Arc::new(Inner {
                index: Index::new(),
                last_tx_id: AtomicU64::new(0),
                backend: None,
                config: Config {
                    path: None,
                    #[cfg(feature = "ttl")]
                    default_ttl: None,
                },
                _lock_file: None,
            }),
        }
    }

    /// Open or create a persistent database file at `path`.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be opened, lock acquisition fails,
    /// format is incompatible, or replay fails.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Emdb::builder().path(path.as_ref().to_path_buf()).build()
    }

    /// Create a builder for configuring a database.
    #[must_use]
    pub fn builder() -> EmdbBuilder {
        EmdbBuilder::new()
    }

    /// Returns a cheap clone of this handle.
    #[must_use]
    pub fn clone_handle(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }

    /// Run a closure inside a transaction.
    ///
    /// The transaction commits when the closure returns `Ok(_)`, and
    /// rolls back when the closure returns `Err(_)` or panics.
    ///
    /// # Errors
    ///
    /// Returns any error from the closure or commit path.
    pub fn transaction<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut crate::transaction::Transaction<'_>) -> Result<T>,
    {
        let mut tx = crate::transaction::Transaction::new(self)?;
        let out = f(&mut tx)?;
        tx.commit()?;
        Ok(out)
    }

    /// Build a database from builder configuration.
    pub(crate) fn from_builder(builder: EmdbBuilder) -> Result<Self> {
        if matches!(builder.flush_policy, FlushPolicy::EveryN(0)) {
            return Err(Error::InvalidConfig("flush policy EveryN requires N > 0"));
        }

        let (backend, lock_file, path, last_tx_id, index) = if let Some(path) = builder.path {
            let lock_file = Some(LockFile::acquire(path.as_path())?);
            migrate_if_needed(path.as_path(), build_flags())?;
            let mut backend = PageStorage::new(
                path.clone(),
                builder.flush_policy,
                build_flags(),
                #[cfg(feature = "mmap")]
                builder.use_mmap,
            )?;
            let mut staged: HashMap<Vec<u8>, Record> = HashMap::new();
            backend.replay(&mut |op| {
                apply_replayed_op(&mut staged, op);
                Ok(())
            })?;
            let last_tx_id = backend.last_tx_id();
            let index = Index::from_records(staged);

            (
                Some(Mutex::new(backend)),
                lock_file,
                Some(path),
                last_tx_id,
                index,
            )
        } else {
            (None, None, None, 0, Index::new())
        };

        let db = Self {
            inner: Arc::new(Inner {
                index,
                last_tx_id: AtomicU64::new(last_tx_id),
                backend,
                config: Config {
                    path,
                    #[cfg(feature = "ttl")]
                    default_ttl: builder.default_ttl,
                },
                _lock_file: lock_file,
            }),
        };

        #[cfg(feature = "ttl")]
        {
            let _evicted = db.sweep_expired();
        }

        Ok(db)
    }

    /// Insert or replace a key/value pair.
    ///
    /// # Errors
    ///
    /// Returns an error when persistence append fails or lock acquisition fails.
    pub fn insert(&self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Result<()> {
        #[cfg(feature = "ttl")]
        {
            self.insert_with_ttl(key, value, Ttl::Default)
        }

        #[cfg(not(feature = "ttl"))]
        {
            let key = key.into();
            let value = value.into();
            self.write_record(key, value, None)
        }
    }

    /// Internal write path: append to WAL (if persistent), then update the
    /// target shard. The backend mutex is held across the shard write so the
    /// in-memory state never reorders relative to the durability log.
    pub(crate) fn write_record(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        expires_at: Option<u64>,
    ) -> Result<()> {
        let shard_idx = Index::shard_for_key(&key);

        match self.inner.backend.as_ref() {
            Some(backend_mtx) => {
                let mut backend = backend_mtx
                    .lock()
                    .map_err(|_poisoned| Error::LockPoisoned)?;
                backend.append(OpRef::Insert {
                    key: &key,
                    value: &value,
                    expires_at,
                })?;
                let mut shard = self.inner.index.write(shard_idx)?;
                let _previous = shard.insert(key, record_new(value, expires_at));
                drop(shard);
                drop(backend);
            }
            None => {
                let mut shard = self.inner.index.write(shard_idx)?;
                let _previous = shard.insert(key, record_new(value, expires_at));
            }
        }
        Ok(())
    }

    /// Fetch a value by key.
    ///
    /// # Errors
    ///
    /// Returns an error when lock acquisition fails.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        let key = key.as_ref();
        let shard_idx = Index::shard_for_key(key);
        let shard = self.inner.index.read(shard_idx)?;
        let Some(record) = shard.get(key) else {
            return Ok(None);
        };

        #[cfg(feature = "ttl")]
        {
            let now = now_unix_millis();
            if is_expired(record_expires_at(record), now) {
                return Ok(None);
            }
        }

        Ok(Some(record_value(record).to_vec()))
    }

    /// Remove a key from the database and return its previous value.
    ///
    /// # Errors
    ///
    /// Returns an error when persistence append fails or lock acquisition fails.
    pub fn remove(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        let key = key.as_ref();
        let shard_idx = Index::shard_for_key(key);

        let removed = match self.inner.backend.as_ref() {
            Some(backend_mtx) => {
                let mut backend = backend_mtx
                    .lock()
                    .map_err(|_poisoned| Error::LockPoisoned)?;
                backend.append(OpRef::Remove { key })?;
                let mut shard = self.inner.index.write(shard_idx)?;
                let removed = shard.remove(key);
                drop(shard);
                drop(backend);
                removed
            }
            None => {
                let mut shard = self.inner.index.write(shard_idx)?;
                shard.remove(key)
            }
        };

        let Some(record) = removed else {
            return Ok(None);
        };

        #[cfg(feature = "ttl")]
        {
            let now = now_unix_millis();
            if is_expired(record_expires_at(&record), now) {
                return Ok(None);
            }
        }

        Ok(Some(record_into_value(record)))
    }

    /// Return `true` if the given key exists.
    ///
    /// # Errors
    ///
    /// Returns an error when lock acquisition fails.
    pub fn contains_key(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        let key = key.as_ref();
        let shard_idx = Index::shard_for_key(key);
        let shard = self.inner.index.read(shard_idx)?;
        let Some(_record) = shard.get(key) else {
            return Ok(false);
        };

        #[cfg(feature = "ttl")]
        {
            let now = now_unix_millis();
            if is_expired(record_expires_at(_record), now) {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Return the number of currently-visible records.
    ///
    /// # Errors
    ///
    /// Returns an error when lock acquisition fails.
    pub fn len(&self) -> Result<usize> {
        let guards = self.inner.index.read_all()?;
        let total = self.count_visible(&guards);
        Ok(total)
    }

    /// Return `true` if no visible records are stored.
    ///
    /// # Errors
    ///
    /// Returns an error when lock acquisition fails.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Remove all records.
    ///
    /// # Errors
    ///
    /// Returns an error when persistence append fails or lock acquisition fails.
    pub fn clear(&self) -> Result<()> {
        match self.inner.backend.as_ref() {
            Some(backend_mtx) => {
                let mut backend = backend_mtx
                    .lock()
                    .map_err(|_poisoned| Error::LockPoisoned)?;
                backend.append(OpRef::Clear)?;
                let mut guards = self.inner.index.write_all()?;
                for shard in guards.iter_mut() {
                    shard.clear();
                }
                drop(guards);
                drop(backend);
            }
            None => {
                let mut guards = self.inner.index.write_all()?;
                for shard in guards.iter_mut() {
                    shard.clear();
                }
            }
        }
        Ok(())
    }

    /// Snapshot all visible records as owned `(key, value)` pairs.
    ///
    /// Iteration order is unspecified; the in-memory index is sharded for
    /// concurrent access and does not guarantee a stable ordering.
    ///
    /// # Errors
    ///
    /// Returns an error when lock acquisition fails.
    pub fn iter(&self) -> Result<std::vec::IntoIter<(Vec<u8>, Vec<u8>)>> {
        let guards = self.inner.index.read_all()?;
        let mut total = 0_usize;
        for shard in guards.iter() {
            total = total.saturating_add(shard.len());
        }
        let mut items = Vec::with_capacity(total);

        #[cfg(feature = "ttl")]
        let now = now_unix_millis();

        for shard in guards.iter() {
            for (key, record) in shard.iter() {
                #[cfg(feature = "ttl")]
                {
                    if is_expired(record_expires_at(record), now) {
                        continue;
                    }
                }
                items.push((key.clone(), record_value(record).to_vec()));
            }
        }
        drop(guards);
        Ok(items.into_iter())
    }

    /// Snapshot all visible keys as owned bytes.
    ///
    /// Iteration order is unspecified.
    ///
    /// # Errors
    ///
    /// Returns an error when lock acquisition fails.
    pub fn keys(&self) -> Result<std::vec::IntoIter<Vec<u8>>> {
        let guards = self.inner.index.read_all()?;
        let mut total = 0_usize;
        for shard in guards.iter() {
            total = total.saturating_add(shard.len());
        }
        let mut items = Vec::with_capacity(total);

        #[cfg(feature = "ttl")]
        let now = now_unix_millis();

        for shard in guards.iter() {
            for (key, _record) in shard.iter() {
                #[cfg(feature = "ttl")]
                {
                    if is_expired(record_expires_at(_record), now) {
                        continue;
                    }
                }
                items.push(key.clone());
            }
        }
        drop(guards);
        Ok(items.into_iter())
    }

    /// Flush pending storage writes.
    ///
    /// # Errors
    ///
    /// Returns an error when storage flush fails.
    pub fn flush(&self) -> Result<()> {
        let Some(backend_mtx) = self.inner.backend.as_ref() else {
            return Ok(());
        };
        let mut backend = backend_mtx
            .lock()
            .map_err(|_poisoned| Error::LockPoisoned)?;
        backend.flush()
    }

    /// Rewrite file storage from the current in-memory snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error when compaction fails.
    pub fn compact(&self) -> Result<()> {
        #[cfg(feature = "ttl")]
        {
            let _evicted = self.sweep_expired();
        }

        let Some(backend_mtx) = self.inner.backend.as_ref() else {
            return Ok(());
        };

        // Snapshot under read locks first so we minimise the amount of work
        // done under the backend mutex.
        let guards = self.inner.index.read_all()?;

        #[cfg(feature = "ttl")]
        let now = now_unix_millis();

        let mut owned: Vec<(Vec<u8>, Vec<u8>, Option<u64>)> = Vec::new();
        for shard in guards.iter() {
            for (key, record) in shard.iter() {
                #[cfg(feature = "ttl")]
                let expires_at = record_expires_at(record);
                #[cfg(not(feature = "ttl"))]
                let expires_at: Option<u64> = None;

                #[cfg(feature = "ttl")]
                {
                    if is_expired(expires_at, now) {
                        continue;
                    }
                }
                owned.push((key.clone(), record_value(record).to_vec(), expires_at));
            }
        }
        drop(guards);

        let snapshot: SnapshotIter<'_> =
            Box::new(owned.iter().map(|(key, value, expires_at)| SnapshotEntry {
                key: key.as_slice(),
                value: value.as_slice(),
                expires_at: *expires_at,
            }));

        let mut backend = backend_mtx
            .lock()
            .map_err(|_poisoned| Error::LockPoisoned)?;
        backend.compact(snapshot)
    }

    /// Force migration of an older-format file to the current page format.
    ///
    /// File-backed databases are auto-migrated during open, so calling this on
    /// an already-open handle is a no-op when the file is current. In-memory
    /// databases always return success.
    ///
    /// # Errors
    ///
    /// Returns an error when migration detection or the migration rewrite fails.
    pub fn migrate(&self) -> Result<()> {
        let Some(path) = self.path() else {
            return Ok(());
        };

        migrate_if_needed(path, build_flags())
    }

    /// Return file path when this database is file-backed.
    #[must_use]
    pub fn path(&self) -> Option<&Path> {
        self.inner.config.path.as_deref()
    }

    /// Insert or replace a key/value pair with explicit TTL behavior.
    ///
    /// # Errors
    ///
    /// Returns an error when TTL computation overflows, persistence fails, or
    /// lock acquisition fails.
    #[cfg(feature = "ttl")]
    pub fn insert_with_ttl(
        &self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        ttl: Ttl,
    ) -> Result<()> {
        let key = key.into();
        let value = value.into();

        let now = now_unix_millis();
        let expires_at = expires_from_ttl(ttl, self.inner.config.default_ttl, now)?;
        self.write_record(key, value, expires_at)
    }

    /// Returns the absolute expiration time for a key, if present.
    ///
    /// # Errors
    ///
    /// Returns an error when lock acquisition fails.
    #[cfg(feature = "ttl")]
    pub fn expires_at(&self, key: impl AsRef<[u8]>) -> Result<Option<SystemTime>> {
        let key = key.as_ref();
        let shard_idx = Index::shard_for_key(key);
        let shard = self.inner.index.read(shard_idx)?;
        let Some(record) = shard.get(key) else {
            return Ok(None);
        };

        let Some(expires_at) = record_expires_at(record) else {
            return Ok(None);
        };

        let now = now_unix_millis();
        if is_expired(Some(expires_at), now) {
            return Ok(None);
        }

        Ok(Some(UNIX_EPOCH + Duration::from_millis(expires_at)))
    }

    /// Returns remaining TTL for a key.
    ///
    /// # Errors
    ///
    /// Returns an error when lock acquisition fails.
    #[cfg(feature = "ttl")]
    pub fn ttl(&self, key: impl AsRef<[u8]>) -> Result<Option<Duration>> {
        let key = key.as_ref();
        let shard_idx = Index::shard_for_key(key);
        let shard = self.inner.index.read(shard_idx)?;
        let Some(record) = shard.get(key) else {
            return Ok(None);
        };

        let Some(expires_at) = record_expires_at(record) else {
            return Ok(None);
        };

        let now = now_unix_millis();
        Ok(remaining_ttl(expires_at, now))
    }

    /// Removes TTL from a key, making it permanent.
    ///
    /// # Errors
    ///
    /// Returns an error when persistence append fails or lock acquisition fails.
    #[cfg(feature = "ttl")]
    pub fn persist(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        let key = key.as_ref();
        let shard_idx = Index::shard_for_key(key);

        match self.inner.backend.as_ref() {
            Some(backend_mtx) => {
                let mut backend = backend_mtx
                    .lock()
                    .map_err(|_poisoned| Error::LockPoisoned)?;
                let mut shard = self.inner.index.write(shard_idx)?;
                let Some(record) = shard.get_mut(key) else {
                    return Ok(false);
                };
                let changed = record_set_persist(record);
                if !changed {
                    return Ok(false);
                }
                let value = record_value(record).to_vec();
                let key_vec = key.to_vec();
                drop(shard);
                backend.append(OpRef::Insert {
                    key: &key_vec,
                    value: &value,
                    expires_at: None,
                })?;
                drop(backend);
                Ok(true)
            }
            None => {
                let mut shard = self.inner.index.write(shard_idx)?;
                let Some(record) = shard.get_mut(key) else {
                    return Ok(false);
                };
                Ok(record_set_persist(record))
            }
        }
    }

    /// Evicts all currently expired records and returns the number removed.
    #[cfg(feature = "ttl")]
    #[must_use]
    pub fn sweep_expired(&self) -> usize {
        let now = now_unix_millis();
        let Ok(mut guards) = self.inner.index.write_all() else {
            return 0;
        };

        let mut removed = 0_usize;
        for shard in guards.iter_mut() {
            let before = shard.len();
            shard.retain(|_key, record| !is_expired(record_expires_at(record), now));
            removed = removed.saturating_add(before - shard.len());
        }
        removed
    }

    /// Acquire the persistent backend if one exists. Crate-internal helper for
    /// the transaction commit path.
    pub(crate) fn lock_backend(&self) -> Result<Option<MutexGuard<'_, PageStorage>>> {
        match self.inner.backend.as_ref() {
            Some(mtx) => mtx
                .lock()
                .map(Some)
                .map_err(|_poisoned| Error::LockPoisoned),
            None => Ok(None),
        }
    }

    /// Crate-internal helper used by transactions to peek at a record without
    /// allocating, without expiring it, and without releasing the shard lock.
    pub(crate) fn shard_for(&self, key: &[u8]) -> Result<std::sync::RwLockReadGuard<'_, Shard>> {
        let shard_idx = Index::shard_for_key(key);
        self.inner.index.read(shard_idx)
    }

    /// Crate-internal helper exposing the index to the transaction commit path.
    pub(crate) fn index(&self) -> &Index {
        &self.inner.index
    }

    /// Crate-internal helper for the transaction commit path: bump and return
    /// the next transaction id.
    pub(crate) fn next_tx_id(&self) -> Result<u64> {
        let prev = self.inner.last_tx_id.fetch_add(1, Ordering::AcqRel);
        prev.checked_add(1)
            .ok_or(Error::TransactionAborted("transaction id overflow"))
    }

    fn count_visible(&self, guards: &[std::sync::RwLockReadGuard<'_, Shard>]) -> usize {
        #[cfg(feature = "ttl")]
        {
            let now = now_unix_millis();
            let mut total = 0_usize;
            for shard in guards.iter() {
                for record in shard.values() {
                    if !is_expired(record_expires_at(record), now) {
                        total = total.saturating_add(1);
                    }
                }
            }
            total
        }

        #[cfg(not(feature = "ttl"))]
        {
            let mut total = 0_usize;
            for shard in guards.iter() {
                total = total.saturating_add(shard.len());
            }
            total
        }
    }
}

impl Drop for Emdb {
    fn drop(&mut self) {
        // Best-effort flush. We cannot return an error from Drop, and surfacing
        // it via panic would be worse: a clean handle drop must never poison
        // unrelated locks. Persistent users wanting guaranteed durability call
        // `flush()` before drop; in-memory drops are no-ops.
        let _ignored = self.flush();
    }
}

fn apply_replayed_op(storage: &mut HashMap<Vec<u8>, Record>, op: Op) {
    match op {
        Op::Insert {
            key,
            value,
            expires_at,
        } => {
            let _previous = storage.insert(key, record_new(value, expires_at));
        }
        Op::Remove { key } => {
            let _removed = storage.remove(&key);
        }
        Op::Clear => {
            storage.clear();
        }
        Op::Checkpoint { record_count: _ } => {}
        Op::BatchBegin {
            tx_id: _,
            op_count: _,
        } => {}
        Op::BatchEnd { tx_id: _ } => {}
    }
}

#[cfg(test)]
mod tests {
    use super::Emdb;
    use crate::storage::FlushPolicy;
    use crate::Result;

    #[cfg(feature = "ttl")]
    use crate::Ttl;

    fn tmp_path(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0_u128, |d| d.as_nanos());
        p.push(format!("emdb-{name}-{nanos}.emdb"));
        p
    }

    #[test]
    fn test_open_in_memory_returns_empty() {
        let db = Emdb::open_in_memory();
        assert!(matches!(db.len(), Ok(0)));
        assert!(matches!(db.is_empty(), Ok(true)));
    }

    #[test]
    fn test_builder_returns_empty_database() {
        let db = Emdb::builder().build();
        assert!(db.is_ok());
        let db = match db {
            Ok(db) => db,
            Err(err) => panic!("build should succeed: {err}"),
        };
        assert!(matches!(db.is_empty(), Ok(true)));
    }

    #[test]
    fn test_migrate_is_noop_for_in_memory_database() {
        let db = Emdb::open_in_memory();
        assert!(db.migrate().is_ok());
    }

    #[test]
    fn test_insert_get_remove_round_trip() -> Result<()> {
        let db = Emdb::open_in_memory();
        db.insert(b"k", b"v")?;

        let found = db.get(b"k")?;
        assert_eq!(found, Some(b"v".to_vec()));
        assert!(db.contains_key(b"k")?);

        let removed = db.remove(b"k")?;
        assert_eq!(removed, Some(b"v".to_vec()));
        assert!(!db.contains_key(b"k")?);
        Ok(())
    }

    #[test]
    fn test_empty_key_is_allowed() -> Result<()> {
        let db = Emdb::open_in_memory();
        db.insert([], b"value")?;
        assert_eq!(db.get([])?, Some(b"value".to_vec()));
        Ok(())
    }

    #[test]
    fn test_clear_iter_and_keys() -> Result<()> {
        let db = Emdb::open_in_memory();
        db.insert(b"a", b"1")?;
        db.insert(b"b", b"2")?;

        let key_count = db.keys()?.count();
        let iter_count = db.iter()?.count();
        assert_eq!(key_count, 2);
        assert_eq!(iter_count, 2);

        db.clear()?;
        assert!(matches!(db.len(), Ok(0)));
        assert!(matches!(db.is_empty(), Ok(true)));
        Ok(())
    }

    #[test]
    fn test_open_path_round_trip() -> Result<()> {
        let path = tmp_path("db-open");

        {
            let db = Emdb::open(&path)?;
            db.insert("k", "v")?;
            db.flush()?;
        }

        let db = Emdb::open(&path)?;
        assert_eq!(db.get("k")?, Some(b"v".to_vec()));
        assert_eq!(db.path(), Some(path.as_path()));
        let removed = std::fs::remove_file(path);
        assert!(removed.is_ok());
        Ok(())
    }

    #[test]
    fn test_builder_every_n_zero_is_invalid() {
        let db = Emdb::builder().flush_policy(FlushPolicy::EveryN(0)).build();
        assert!(db.is_err());
    }

    #[cfg(feature = "ttl")]
    #[test]
    fn test_ttl_after_zero_makes_key_immediately_invisible() -> Result<()> {
        use std::time::Duration;

        let db = Emdb::open_in_memory();
        db.insert_with_ttl(b"k", b"v", Ttl::After(Duration::ZERO))?;
        assert_eq!(db.get(b"k")?, None);
        Ok(())
    }

    #[cfg(feature = "ttl")]
    #[test]
    fn test_sweep_expired_empty_db_returns_zero() {
        let db = Emdb::open_in_memory();
        assert_eq!(db.sweep_expired(), 0);
    }
}
