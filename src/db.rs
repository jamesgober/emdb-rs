// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Core database implementation.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

#[cfg(feature = "ttl")]
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::builder::EmdbBuilder;
use crate::lockfile::LockFile;
use crate::storage::file::FileStorage;
use crate::storage::memory::MemoryStorage;
use crate::storage::{build_flags, Op, SnapshotEntry, SnapshotIter, Storage};
use crate::transaction::Transaction;
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
    pub(crate) state: RwLock<State>,
    pub(crate) backend: Mutex<Box<dyn Storage>>,
    pub(crate) config: Config,
    _lock_file: Option<LockFile>,
}

/// Immutable runtime configuration.
pub(crate) struct Config {
    pub(crate) path: Option<PathBuf>,
    #[cfg(feature = "ttl")]
    pub(crate) default_ttl: Option<Duration>,
}

/// Mutable in-memory state protected by a read/write lock.
pub(crate) struct State {
    pub(crate) storage: BTreeMap<Vec<u8>, Record>,
    pub(crate) last_tx_id: u64,
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
                state: RwLock::new(State {
                    storage: BTreeMap::new(),
                    last_tx_id: 0,
                }),
                backend: Mutex::new(Box::new(MemoryStorage)),
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
        F: FnOnce(&mut Transaction<'_>) -> Result<T>,
    {
        let mut tx = Transaction::new(self)?;
        let out = f(&mut tx)?;
        tx.commit()?;
        Ok(out)
    }

    /// Build a database from builder configuration.
    pub(crate) fn from_builder(builder: EmdbBuilder) -> Result<Self> {
        if matches!(builder.flush_policy, FlushPolicy::EveryN(0)) {
            return Err(Error::InvalidConfig("flush policy EveryN requires N > 0"));
        }

        let (backend, lock_file, path, last_tx_id, storage) = if let Some(path) = builder.path {
            let lock_file = Some(LockFile::acquire(path.as_path())?);
            let mut backend = FileStorage::new(path.clone(), builder.flush_policy, build_flags())?;
            let mut replayed = BTreeMap::new();
            backend.replay(&mut |op| {
                apply_replayed_op(&mut replayed, op);
                Ok(())
            })?;
            let last_tx_id = backend.last_tx_id();

            (
                Box::new(backend) as Box<dyn Storage>,
                lock_file,
                Some(path),
                last_tx_id,
                replayed,
            )
        } else {
            (
                Box::new(MemoryStorage) as Box<dyn Storage>,
                None,
                None,
                0,
                BTreeMap::new(),
            )
        };

        let db = Self {
            inner: Arc::new(Inner {
                state: RwLock::new(State {
                    storage,
                    last_tx_id,
                }),
                backend: Mutex::new(backend),
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

            {
                let mut backend = self.lock_backend()?;
                backend.append(&Op::Insert {
                    key: key.clone(),
                    value: value.clone(),
                    expires_at: None,
                })?;
            }

            let mut state = self.state_write()?;
            let _previous = state.storage.insert(key, record_new(value, None));
            Ok(())
        }
    }

    /// Fetch a value by key.
    ///
    /// # Errors
    ///
    /// Returns an error when lock acquisition fails.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        let state = self.state_read()?;
        let Some(record) = state.storage.get(key.as_ref()) else {
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
        let key_vec = key.as_ref().to_vec();

        {
            let mut backend = self.lock_backend()?;
            backend.append(&Op::Remove {
                key: key_vec.clone(),
            })?;
        }

        let mut state = self.state_write()?;
        let removed = state.storage.remove(key.as_ref());

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
        #[cfg(feature = "ttl")]
        {
            let state = self.state_read()?;
            let Some(record) = state.storage.get(key.as_ref()) else {
                return Ok(false);
            };
            let now = now_unix_millis();
            if is_expired(record_expires_at(record), now) {
                return Ok(false);
            }

            Ok(true)
        }

        #[cfg(not(feature = "ttl"))]
        {
            let state = self.state_read()?;
            Ok(state.storage.contains_key(key.as_ref()))
        }
    }

    /// Return the number of currently-visible records.
    ///
    /// # Errors
    ///
    /// Returns an error when lock acquisition fails.
    pub fn len(&self) -> Result<usize> {
        #[cfg(feature = "ttl")]
        {
            let state = self.state_read()?;
            let now = now_unix_millis();
            Ok(state
                .storage
                .values()
                .filter(|record| !is_expired(record_expires_at(record), now))
                .count())
        }

        #[cfg(not(feature = "ttl"))]
        {
            let state = self.state_read()?;
            Ok(state.storage.len())
        }
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
        {
            let mut backend = self.lock_backend()?;
            backend.append(&Op::Clear)?;
        }

        let mut state = self.state_write()?;
        state.storage.clear();
        Ok(())
    }

    /// Snapshot all visible records as owned `(key, value)` pairs.
    ///
    /// # Errors
    ///
    /// Returns an error when lock acquisition fails.
    pub fn iter(&self) -> Result<std::vec::IntoIter<(Vec<u8>, Vec<u8>)>> {
        #[cfg(feature = "ttl")]
        {
            let state = self.state_read()?;
            let now = now_unix_millis();
            let items = state
                .storage
                .iter()
                .filter_map(|(key, record)| {
                    if is_expired(record_expires_at(record), now) {
                        return None;
                    }
                    Some((key.clone(), record_value(record).to_vec()))
                })
                .collect::<Vec<_>>();
            Ok(items.into_iter())
        }

        #[cfg(not(feature = "ttl"))]
        {
            let state = self.state_read()?;
            let items = state
                .storage
                .iter()
                .map(|(key, record)| (key.clone(), record_value(record).to_vec()))
                .collect::<Vec<_>>();
            Ok(items.into_iter())
        }
    }

    /// Snapshot all visible keys as owned bytes.
    ///
    /// # Errors
    ///
    /// Returns an error when lock acquisition fails.
    pub fn keys(&self) -> Result<std::vec::IntoIter<Vec<u8>>> {
        let items = self.iter()?.map(|(key, _value)| key).collect::<Vec<_>>();
        Ok(items.into_iter())
    }

    /// Flush pending storage writes.
    ///
    /// # Errors
    ///
    /// Returns an error when storage flush fails.
    pub fn flush(&self) -> Result<()> {
        let mut backend = self.lock_backend()?;
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

        let state = self.state_read()?;
        let owned: Vec<(Vec<u8>, Vec<u8>, Option<u64>)> = state
            .storage
            .iter()
            .map(|(key, record)| {
                #[cfg(feature = "ttl")]
                let expires_at = record_expires_at(record);
                #[cfg(not(feature = "ttl"))]
                let expires_at = None;

                (key.clone(), record_value(record).to_vec(), expires_at)
            })
            .collect();
        drop(state);

        let snapshot: SnapshotIter<'_> =
            Box::new(owned.iter().map(|(key, value, expires_at)| SnapshotEntry {
                key: key.as_slice(),
                value: value.as_slice(),
                expires_at: *expires_at,
            }));

        let mut backend = self.lock_backend()?;
        backend.compact(snapshot)
    }

    /// Return file path when this database is file-backed.
    #[must_use]
    pub fn path(&self) -> Option<&Path> {
        self.inner.config.path.as_deref()
    }

    /// Insert or replace a key/value pair with explicit TTL behavior.
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

        {
            let mut backend = self.lock_backend()?;
            backend.append(&Op::Insert {
                key: key.clone(),
                value: value.clone(),
                expires_at,
            })?;
        }

        let mut state = self.state_write()?;
        let _previous = state.storage.insert(key, record_new(value, expires_at));
        Ok(())
    }

    /// Returns the absolute expiration time for a key, if present.
    #[cfg(feature = "ttl")]
    pub fn expires_at(&self, key: impl AsRef<[u8]>) -> Result<Option<SystemTime>> {
        let state = self.state_read()?;
        let Some(record) = state.storage.get(key.as_ref()) else {
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
    #[cfg(feature = "ttl")]
    pub fn ttl(&self, key: impl AsRef<[u8]>) -> Result<Option<Duration>> {
        let state = self.state_read()?;
        let Some(record) = state.storage.get(key.as_ref()) else {
            return Ok(None);
        };

        let Some(expires_at) = record_expires_at(record) else {
            return Ok(None);
        };

        let now = now_unix_millis();
        Ok(remaining_ttl(expires_at, now))
    }

    /// Removes TTL from a key, making it permanent.
    #[cfg(feature = "ttl")]
    pub fn persist(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        let key_slice = key.as_ref();
        let key_vec = key_slice.to_vec();

        let mut state = self.state_write()?;
        let Some(record) = state.storage.get_mut(key_slice) else {
            return Ok(false);
        };

        let changed = record_set_persist(record);
        if changed {
            let value = record_value(record).to_vec();
            drop(state);
            let mut backend = self.lock_backend()?;
            backend.append(&Op::Insert {
                key: key_vec,
                value,
                expires_at: None,
            })?;
        }

        Ok(changed)
    }

    /// Evicts all currently expired records and returns the number removed.
    #[cfg(feature = "ttl")]
    pub fn sweep_expired(&self) -> usize {
        let now = now_unix_millis();
        let Ok(mut state) = self.state_write() else {
            return 0;
        };
        let before = state.storage.len();
        state
            .storage
            .retain(|_key, record| !is_expired(record_expires_at(record), now));
        before - state.storage.len()
    }

    pub(crate) fn lock_backend(&self) -> Result<MutexGuard<'_, Box<dyn Storage>>> {
        self.inner
            .backend
            .lock()
            .map_err(|_poisoned| Error::LockPoisoned)
    }

    pub(crate) fn state_write(&self) -> Result<RwLockWriteGuard<'_, State>> {
        self.inner
            .state
            .write()
            .map_err(|_poisoned| Error::LockPoisoned)
    }

    fn state_read(&self) -> Result<RwLockReadGuard<'_, State>> {
        self.inner
            .state
            .read()
            .map_err(|_poisoned| Error::LockPoisoned)
    }
}

impl Drop for Emdb {
    fn drop(&mut self) {
        let _ignored = self.flush();
    }
}

fn apply_replayed_op(storage: &mut BTreeMap<Vec<u8>, Record>, op: Op) {
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
