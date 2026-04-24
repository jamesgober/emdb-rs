// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Core database implementation.

use std::collections::BTreeMap;
use std::path::Path;

#[cfg(feature = "ttl")]
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::builder::EmdbBuilder;
use crate::error::Error;
use crate::storage::file::FileStorage;
use crate::storage::memory::MemoryStorage;
use crate::storage::{build_flags, Op, SnapshotEntry, SnapshotIter, Storage};
#[cfg(feature = "ttl")]
use crate::ttl::{
    expires_from_ttl, is_expired, now_unix_millis, record_expires_at, record_set_persist,
    remaining_ttl, Ttl,
};
use crate::ttl::{record_into_value, record_new, record_value, Record};
use crate::Result;

/// The primary embedded database handle.
///
/// `Emdb` stores key/value records in memory and can optionally persist
/// operations to a single append-only file.
pub struct Emdb {
    storage: BTreeMap<Vec<u8>, Record>,
    backend: Box<dyn Storage>,
    #[cfg(feature = "ttl")]
    default_ttl: Option<Duration>,
}

impl Emdb {
    /// Open a new in-memory database.
    #[must_use]
    pub fn open_in_memory() -> Self {
        Self {
            storage: BTreeMap::new(),
            backend: Box::new(MemoryStorage),
            #[cfg(feature = "ttl")]
            default_ttl: None,
        }
    }

    /// Open or create a persistent database file at `path`.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be opened, has an incompatible
    /// format, or replay fails.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Emdb::builder().path(path.as_ref().to_path_buf()).build()
    }

    /// Create a builder for configuring a database.
    #[must_use]
    pub fn builder() -> EmdbBuilder {
        EmdbBuilder::new()
    }

    /// Build a database from builder configuration.
    pub(crate) fn from_builder(builder: EmdbBuilder) -> Result<Self> {
        if matches!(builder.flush_policy, crate::FlushPolicy::EveryN(0)) {
            return Err(Error::InvalidConfig("flush policy EveryN requires N > 0"));
        }

        #[cfg_attr(not(feature = "ttl"), allow(unused_mut))]
        let mut db = if let Some(path) = builder.path {
            let mut backend = FileStorage::new(path, builder.flush_policy, build_flags())?;
            let mut replayed = BTreeMap::new();

            backend.replay(&mut |op| {
                apply_replayed_op(&mut replayed, op);
                Ok(())
            })?;

            Self {
                storage: replayed,
                backend: Box::new(backend),
                #[cfg(feature = "ttl")]
                default_ttl: builder.default_ttl,
            }
        } else {
            Self {
                storage: BTreeMap::new(),
                backend: Box::new(MemoryStorage),
                #[cfg(feature = "ttl")]
                default_ttl: builder.default_ttl,
            }
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
    /// Returns an error when persistence append fails.
    pub fn insert(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Result<()> {
        #[cfg(feature = "ttl")]
        {
            self.insert_with_ttl(key, value, Ttl::Default)
        }

        #[cfg(not(feature = "ttl"))]
        {
            let key = key.into();
            let value = value.into();
            let _previous = self
                .storage
                .insert(key.clone(), record_new(value.clone(), None));
            self.backend.append(&Op::Insert {
                key,
                value,
                expires_at: None,
            })
        }
    }

    /// Fetch a value by key.
    ///
    /// # Errors
    ///
    /// Returns an error when conversion to visible state fails.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        let Some(record) = self.storage.get(key.as_ref()) else {
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
    /// Returns an error when persistence append fails.
    pub fn remove(&mut self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        let key_vec = key.as_ref().to_vec();
        let removed = self.storage.remove(key.as_ref());

        self.backend.append(&Op::Remove { key: key_vec })?;

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
    pub fn contains_key(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        #[cfg(feature = "ttl")]
        {
            let Some(record) = self.storage.get(key.as_ref()) else {
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
            Ok(self.storage.contains_key(key.as_ref()))
        }
    }

    /// Return the number of currently-visible records.
    #[must_use]
    pub fn len(&self) -> usize {
        #[cfg(feature = "ttl")]
        {
            let now = now_unix_millis();
            self.storage
                .values()
                .filter(|record| !is_expired(record_expires_at(record), now))
                .count()
        }

        #[cfg(not(feature = "ttl"))]
        {
            self.storage.len()
        }
    }

    /// Return `true` if no visible records are stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Remove all records.
    ///
    /// # Errors
    ///
    /// Returns an error when persistence append fails.
    pub fn clear(&mut self) -> Result<()> {
        self.storage.clear();
        self.backend.append(&Op::Clear)
    }

    /// Iterate over all visible records as key/value byte slices.
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> + '_ {
        #[cfg(feature = "ttl")]
        {
            let now = now_unix_millis();
            self.storage.iter().filter_map(move |(key, record)| {
                if is_expired(record_expires_at(record), now) {
                    return None;
                }
                Some((key.as_slice(), record_value(record)))
            })
        }

        #[cfg(not(feature = "ttl"))]
        {
            self.storage
                .iter()
                .map(|(key, record)| (key.as_slice(), record_value(record)))
        }
    }

    /// Iterate over all visible keys as byte slices.
    pub fn keys(&self) -> impl Iterator<Item = &[u8]> + '_ {
        self.iter().map(|(key, _value)| key)
    }

    /// Flush pending storage writes.
    ///
    /// # Errors
    ///
    /// Returns an error when storage flush fails.
    pub fn flush(&mut self) -> Result<()> {
        self.backend.flush()
    }

    /// Rewrite file storage from the current in-memory snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error when compaction fails.
    pub fn compact(&mut self) -> Result<()> {
        #[cfg(feature = "ttl")]
        {
            let _evicted = self.sweep_expired();
        }

        let owned: Vec<(Vec<u8>, Vec<u8>, Option<u64>)> = self
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

        let snapshot: SnapshotIter<'_> =
            Box::new(owned.iter().map(|(key, value, expires_at)| SnapshotEntry {
                key: key.as_slice(),
                value: value.as_slice(),
                expires_at: *expires_at,
            }));

        self.backend.compact(snapshot)
    }

    /// Return file path when this database is file-backed.
    #[must_use]
    pub fn path(&self) -> Option<&Path> {
        self.backend.path()
    }

    /// Insert or replace a key/value pair with explicit TTL behavior.
    #[cfg(feature = "ttl")]
    pub fn insert_with_ttl(
        &mut self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        ttl: Ttl,
    ) -> Result<()> {
        let key = key.into();
        let value = value.into();

        let now = now_unix_millis();
        let expires_at = expires_from_ttl(ttl, self.default_ttl, now)?;
        let _previous = self
            .storage
            .insert(key.clone(), record_new(value.clone(), expires_at));

        self.backend.append(&Op::Insert {
            key,
            value,
            expires_at,
        })
    }

    /// Returns the absolute expiration time for a key, if present.
    #[cfg(feature = "ttl")]
    pub fn expires_at(&self, key: impl AsRef<[u8]>) -> Result<Option<SystemTime>> {
        let Some(record) = self.storage.get(key.as_ref()) else {
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
        let Some(record) = self.storage.get(key.as_ref()) else {
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
    pub fn persist(&mut self, key: impl AsRef<[u8]>) -> Result<bool> {
        let Some(record) = self.storage.get_mut(key.as_ref()) else {
            return Ok(false);
        };

        let changed = record_set_persist(record);
        if changed {
            self.backend.append(&Op::Insert {
                key: key.as_ref().to_vec(),
                value: record_value(record).to_vec(),
                expires_at: None,
            })?;
        }

        Ok(changed)
    }

    /// Evicts all currently expired records and returns the number removed.
    #[cfg(feature = "ttl")]
    pub fn sweep_expired(&mut self) -> usize {
        let now = now_unix_millis();
        let before = self.storage.len();
        self.storage
            .retain(|_key, record| !is_expired(record_expires_at(record), now));
        before - self.storage.len()
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
        assert_eq!(db.len(), 0);
        assert!(db.is_empty());
    }

    #[test]
    fn test_builder_returns_empty_database() {
        let db = Emdb::builder().build();
        assert!(db.is_ok());
        let db = match db {
            Ok(db) => db,
            Err(err) => panic!("build should succeed: {err}"),
        };
        assert!(db.is_empty());
    }

    #[test]
    fn test_insert_get_remove_round_trip() -> Result<()> {
        let mut db = Emdb::open_in_memory();
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
        let mut db = Emdb::open_in_memory();
        db.insert([], b"value")?;
        assert_eq!(db.get([])?, Some(b"value".to_vec()));
        Ok(())
    }

    #[test]
    fn test_clear_iter_and_keys() -> Result<()> {
        let mut db = Emdb::open_in_memory();
        db.insert(b"a", b"1")?;
        db.insert(b"b", b"2")?;

        let key_count = db.keys().count();
        let iter_count = db.iter().count();
        assert_eq!(key_count, 2);
        assert_eq!(iter_count, 2);

        db.clear()?;
        assert_eq!(db.len(), 0);
        assert!(db.is_empty());
        Ok(())
    }

    #[test]
    fn test_open_path_round_trip() -> Result<()> {
        let path = tmp_path("db-open");

        {
            let mut db = Emdb::open(&path)?;
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

        let mut db = Emdb::open_in_memory();
        db.insert_with_ttl(b"k", b"v", Ttl::After(Duration::ZERO))?;
        assert_eq!(db.get(b"k")?, None);
        Ok(())
    }

    #[cfg(feature = "ttl")]
    #[test]
    fn test_sweep_expired_empty_db_returns_zero() {
        let mut db = Emdb::open_in_memory();
        assert_eq!(db.sweep_expired(), 0);
    }
}
