// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! `Emdb` — the public database handle.

use std::path::{Path, PathBuf};
use std::sync::Arc;

#[cfg(feature = "ttl")]
use std::time::Duration;

use crate::builder::EmdbBuilder;
use crate::lockfile::LockFile;
use crate::storage::{Engine, EngineConfig, RecordSnapshot, DEFAULT_NAMESPACE_ID};
use crate::Result;

#[cfg(feature = "ttl")]
use crate::ttl::{
    expires_from_ttl, is_expired, now_unix_millis, record_new, record_set_persist, remaining_ttl,
    Ttl,
};

#[cfg(feature = "encrypt")]
use crate::encryption::EncryptionInput;

/// The primary embedded database handle.
///
/// `Emdb` is cheap to clone — clones share the same underlying engine
/// via [`Arc`]. Pass clones across threads instead of synchronising
/// access to a single handle.
pub struct Emdb {
    pub(crate) inner: Arc<Inner>,
}

impl std::fmt::Debug for Emdb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Emdb")
            .field("path", &self.inner.path)
            .finish()
    }
}

/// Shared state behind one or more [`Emdb`] handles.
pub(crate) struct Inner {
    pub(crate) engine: Engine,
    pub(crate) path: PathBuf,
    /// Default TTL applied to inserts via [`Ttl::Default`].
    #[cfg(feature = "ttl")]
    pub(crate) default_ttl: Option<Duration>,
    _lock_file: LockFile,
    /// When true, the on-disk file (and its sidecars) are removed when
    /// the last handle drops. Set by [`Emdb::open_in_memory`].
    ephemeral: bool,
}

impl Drop for Inner {
    fn drop(&mut self) {
        if self.ephemeral {
            let path = &self.path;
            let display = path.display().to_string();
            let _ = std::fs::remove_file(path);
            let _ = std::fs::remove_file(format!("{display}.lock"));
        }
    }
}

impl Clone for Emdb {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Emdb {
    /// Open or create a persistent database file at `path`.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be opened, lock acquisition
    /// fails, format is incompatible, or recovery scan reports
    /// corruption.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        EmdbBuilder::new().path(path.as_ref().to_path_buf()).build()
    }

    /// Open an ephemeral database. The handle is backed by a unique
    /// temp-file path that is removed when the last clone drops. Useful
    /// for tests, REPLs, and anywhere a disposable in-memory-shaped
    /// store is wanted; behaviour is identical to [`Emdb::open`] except
    /// for the ephemeral cleanup.
    ///
    /// Panics if the temp directory is unwritable — this method is for
    /// tests/dev convenience and is not appropriate for production
    /// code paths that must surface I/O errors.
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn open_in_memory() -> Self {
        EmdbBuilder::new()
            .build()
            .expect("emdb open_in_memory: tempdir is writable")
    }

    /// Create a builder for configuring a database.
    #[must_use]
    pub fn builder() -> EmdbBuilder {
        EmdbBuilder::new()
    }

    /// Returns a cheap clone of this handle.
    #[must_use]
    pub fn clone_handle(&self) -> Self {
        self.clone()
    }

    /// Build an [`Emdb`] from a configured builder. Used internally by
    /// [`EmdbBuilder::build`].
    pub(crate) fn from_builder(builder: EmdbBuilder) -> Result<Self> {
        // Resolve OS-default path resolution.
        let mut path = builder.path.clone();
        let has_os_resolution = builder.data_root.is_some()
            || builder.app_name.is_some()
            || builder.database_name.is_some();
        if has_os_resolution {
            if path.is_some() {
                return Err(crate::Error::InvalidConfig(
                    "EmdbBuilder::path is mutually exclusive with app_name / database_name / data_root",
                ));
            }
            path = Some(crate::data_dir::resolve_database_path(
                builder.data_root.clone(),
                builder.app_name.as_deref(),
                builder.database_name.as_deref(),
            )?);
        }

        // No path supplied at all → ephemeral mode. Synthesise a unique
        // tempfile path and mark the resulting handle ephemeral so the
        // file is removed on Drop.
        let (path, ephemeral) = match path {
            Some(p) => (p, false),
            None => {
                let mut p = std::env::temp_dir();
                let nanos = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0_u128, |d| d.as_nanos());
                let tid = std::thread::current().id();
                p.push(format!("emdb-mem-{nanos}-{tid:?}.emdb"));
                (p, true)
            }
        };

        let lock_file = LockFile::acquire(path.as_path())?;

        let engine_config = EngineConfig {
            path: path.clone(),
            flags: 0,
            enable_range_scans: builder.enable_range_scans,
            #[cfg(feature = "encrypt")]
            encryption_key: builder.encryption_key,
            #[cfg(feature = "encrypt")]
            cipher: builder.cipher,
            #[cfg(feature = "encrypt")]
            encryption_passphrase: builder.encryption_passphrase.clone(),
        };
        let engine = Engine::open(engine_config)?;

        let db = Self {
            inner: Arc::new(Inner {
                engine,
                path,
                #[cfg(feature = "ttl")]
                default_ttl: builder.default_ttl,
                _lock_file: lock_file,
                ephemeral,
            }),
        };

        #[cfg(feature = "ttl")]
        {
            let _evicted = db.sweep_expired();
        }

        Ok(db)
    }

    /// On-disk path of this database.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    // ---- core key/value operations ----

    /// Insert or replace a key/value pair.
    pub fn insert(&self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Result<()> {
        let key = key.into();
        let value = value.into();
        #[cfg(feature = "ttl")]
        let expires_at = self.compute_default_expires_at()?;
        #[cfg(not(feature = "ttl"))]
        let expires_at = 0_u64;
        self.inner
            .engine
            .insert(DEFAULT_NAMESPACE_ID, &key, &value, expires_at)
    }

    /// Insert many key/value pairs in one writer-locked pass.
    pub fn insert_many<I, K, V>(&self, items: I) -> Result<()>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        #[cfg(feature = "ttl")]
        let expires_at = self.compute_default_expires_at()?;
        #[cfg(not(feature = "ttl"))]
        let expires_at = 0_u64;
        let owned: Vec<(Vec<u8>, Vec<u8>, u64)> = items
            .into_iter()
            .map(|(k, v)| (k.as_ref().to_vec(), v.as_ref().to_vec(), expires_at))
            .collect();
        self.inner.engine.insert_many(DEFAULT_NAMESPACE_ID, owned)
    }

    /// Fetch a value by key.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        let key = key.as_ref();
        #[cfg(feature = "ttl")]
        {
            match self.inner.engine.get_with_meta(DEFAULT_NAMESPACE_ID, key)? {
                None => Ok(None),
                Some((value, expires_at)) => {
                    if expires_at != 0 && is_expired(Some(expires_at), now_unix_millis()) {
                        Ok(None)
                    } else {
                        Ok(Some(value))
                    }
                }
            }
        }
        #[cfg(not(feature = "ttl"))]
        {
            self.inner.engine.get(DEFAULT_NAMESPACE_ID, key)
        }
    }

    /// Remove a key, returning the previously-stored value if any.
    pub fn remove(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        self.inner.engine.remove(DEFAULT_NAMESPACE_ID, key.as_ref())
    }

    /// Returns whether a key has a live record.
    pub fn contains_key(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        Ok(self.get(key)?.is_some())
    }

    /// Number of live records in the default namespace.
    pub fn len(&self) -> Result<usize> {
        let count = self.inner.engine.record_count(DEFAULT_NAMESPACE_ID)?;
        usize::try_from(count)
            .map_err(|_| crate::Error::InvalidConfig("record count exceeds usize on this target"))
    }

    /// Returns whether the database has zero live records.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Drop every record from the default namespace.
    pub fn clear(&self) -> Result<()> {
        self.inner.engine.clear_namespace(DEFAULT_NAMESPACE_ID)
    }

    /// Force pending writes to disk (`fdatasync`).
    pub fn flush(&self) -> Result<()> {
        self.inner.engine.flush()
    }

    /// Iterator over `(key, value)` pairs in the default namespace.
    pub fn iter(&self) -> Result<EmdbIter> {
        let snapshot = self.inner.engine.collect_records(DEFAULT_NAMESPACE_ID)?;
        Ok(EmdbIter {
            inner: snapshot.into_iter(),
        })
    }

    /// Iterator over keys in the default namespace.
    pub fn keys(&self) -> Result<EmdbKeyIter> {
        let snapshot = self.inner.engine.collect_records(DEFAULT_NAMESPACE_ID)?;
        Ok(EmdbKeyIter {
            inner: snapshot.into_iter(),
        })
    }

    /// Range-scan keys in the default namespace, returning `(key, value)`
    /// pairs in lexicographic order. Requires the database to have been
    /// opened with [`crate::EmdbBuilder::enable_range_scans`]`(true)`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use emdb::Emdb;
    ///
    /// let db = Emdb::builder().enable_range_scans(true).build()?;
    /// db.insert("user:001", "alice")?;
    /// db.insert("user:002", "bob")?;
    /// db.insert("session:abc", "x")?;
    ///
    /// let users: Vec<_> = db
    ///     .range(b"user:".to_vec()..b"user;".to_vec())?
    ///     .into_iter()
    ///     .collect();
    /// assert_eq!(users.len(), 2);
    /// # Ok::<(), emdb::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::InvalidConfig`] if range scans were not
    /// enabled at open time. Returns [`crate::Error::LockPoisoned`] on
    /// poisoned namespace lock.
    pub fn range<R>(&self, range: R) -> Result<Vec<(Vec<u8>, Vec<u8>)>>
    where
        R: std::ops::RangeBounds<Vec<u8>>,
    {
        self.inner.engine.range_scan(DEFAULT_NAMESPACE_ID, range)
    }

    /// Range-scan all keys with a given prefix in the default namespace.
    /// Convenience wrapper over [`Self::range`] that constructs a half-
    /// open `[prefix, prefix++)` range.
    ///
    /// # Errors
    ///
    /// Same as [`Self::range`].
    pub fn range_prefix(&self, prefix: impl AsRef<[u8]>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let prefix = prefix.as_ref();
        let start = prefix.to_vec();
        let end = next_prefix(prefix);
        match end {
            Some(end) => self.range(start..end),
            None => self.range(start..),
        }
    }

    // ---- TTL operations ----

    /// Insert with an explicit TTL.
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
        let expires_at = expires_from_ttl(ttl, self.inner.default_ttl, now)?.unwrap_or(0);
        self.inner
            .engine
            .insert(DEFAULT_NAMESPACE_ID, &key, &value, expires_at)
    }

    /// Look up the absolute expiry timestamp (unix-ms) for a key.
    #[cfg(feature = "ttl")]
    pub fn expires_at(&self, key: impl AsRef<[u8]>) -> Result<Option<u64>> {
        self.inner
            .engine_expires_at(DEFAULT_NAMESPACE_ID, key.as_ref())
    }

    /// Remaining TTL for a key, if it has one.
    #[cfg(feature = "ttl")]
    pub fn ttl(&self, key: impl AsRef<[u8]>) -> Result<Option<Duration>> {
        let exp = self.expires_at(key)?;
        match exp {
            Some(deadline) if deadline > 0 => Ok(remaining_ttl(deadline, now_unix_millis())),
            _ => Ok(None),
        }
    }

    /// Remove the TTL from a record (re-insert with `expires_at = 0`).
    /// Returns true if the record existed and previously had a TTL.
    #[cfg(feature = "ttl")]
    pub fn persist(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        let key = key.as_ref();
        let value = match self.inner.engine.get(DEFAULT_NAMESPACE_ID, key)? {
            Some(v) => v,
            None => return Ok(false),
        };
        let prev_exp = self
            .inner
            .engine_expires_at(DEFAULT_NAMESPACE_ID, key)?
            .unwrap_or(0);
        let had_ttl = prev_exp != 0;
        self.inner
            .engine
            .insert(DEFAULT_NAMESPACE_ID, key, &value, 0)?;
        // Use Record helpers so the warning policy stays satisfied.
        let mut probe = record_new(value, if had_ttl { Some(prev_exp) } else { None });
        let _flipped = record_set_persist(&mut probe);
        Ok(had_ttl)
    }

    /// Remove every record whose TTL has expired. Returns the count
    /// of evicted records. Errors during sweep are swallowed (returning
    /// the partial count) so callers can use this in best-effort
    /// background loops.
    #[cfg(feature = "ttl")]
    pub fn sweep_expired(&self) -> usize {
        let snapshot = match self.inner.engine.collect_records(DEFAULT_NAMESPACE_ID) {
            Ok(snap) => snap,
            Err(_) => return 0,
        };
        let now = now_unix_millis();
        let mut evicted = 0;
        for (key, _value, expires_at) in snapshot {
            if expires_at != 0 && is_expired(Some(expires_at), now) {
                if let Ok(Some(_)) = self.inner.engine.remove(DEFAULT_NAMESPACE_ID, &key) {
                    evicted += 1;
                }
            }
        }
        evicted
    }

    /// Compact the on-disk file by rewriting only live records and
    /// atomically swapping the new file in for the old.
    ///
    /// Tombstoned records (from `remove`) and superseded records (from
    /// `insert` overwriting an existing key) remain in the on-disk log
    /// until the next compaction. This call walks every namespace's
    /// live index, writes the surviving records into a sibling file
    /// (`<path>.compact.tmp`), syncs it, and atomically renames it
    /// over the original. Existing readers holding `Arc<Mmap>`
    /// snapshots from before the compaction continue to read from the
    /// old inode until they release; new reads see the compacted
    /// layout.
    ///
    /// This is a heavier operation than [`Self::flush`] — call it on
    /// maintenance windows, not on every write. After compaction the
    /// file size shrinks to the size of the live records plus the
    /// 4 KiB header.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from the rewrite, sync, or rename phases.
    /// On failure the original file is left untouched and the temp
    /// file is best-effort cleaned up.
    pub fn compact(&self) -> Result<()> {
        self.inner.engine.compact_in_place()
    }

    #[cfg(feature = "ttl")]
    fn compute_default_expires_at(&self) -> Result<u64> {
        let now = now_unix_millis();
        Ok(expires_from_ttl(Ttl::Default, self.inner.default_ttl, now)?.unwrap_or(0))
    }

    // ---- namespace operations ----

    /// Open or create a named namespace.
    pub fn namespace(&self, name: impl AsRef<str>) -> Result<crate::namespace::Namespace> {
        let name_ref = name.as_ref();
        let ns_id = self.inner.engine.create_or_open_namespace(name_ref)?;
        Ok(crate::namespace::Namespace::new(
            Arc::clone(&self.inner),
            ns_id,
            name_ref.to_string().into_boxed_str(),
        ))
    }

    /// Tombstone a named namespace.
    pub fn drop_namespace(&self, name: impl AsRef<str>) -> Result<bool> {
        self.inner.engine.drop_namespace(name.as_ref())
    }

    /// List every live namespace name.
    pub fn list_namespaces(&self) -> Result<Vec<String>> {
        let entries = self.inner.engine.list_namespaces()?;
        Ok(entries.into_iter().map(|(_, name)| name).collect())
    }

    // ---- transaction (simple buffered batch) ----

    /// Run a closure inside a buffered batch. The batch is committed
    /// when the closure returns `Ok(_)`; staged writes are dropped
    /// when it returns `Err(_)`.
    ///
    /// Note: the new mmap+append architecture does not provide
    /// **atomic** batches — individual records are atomic (per-record
    /// CRC) but a crash mid-commit leaves a prefix of the batch
    /// durable. This is a deliberate trade-off for write throughput.
    /// Callers that need true all-or-nothing must use external means.
    pub fn transaction<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut crate::transaction::Transaction<'_>) -> Result<T>,
    {
        let mut tx = crate::transaction::Transaction::new(self);
        let out = f(&mut tx)?;
        tx.commit()?;
        Ok(out)
    }

    // ---- encryption admin ----

    /// Convert an unencrypted database file to encrypted in place.
    #[cfg(feature = "encrypt")]
    pub fn enable_encryption(path: impl AsRef<Path>, target: EncryptionInput) -> Result<()> {
        crate::encryption_admin::enable_encryption(path, target)
    }

    /// Convert an encrypted database file to unencrypted in place.
    #[cfg(feature = "encrypt")]
    pub fn disable_encryption(path: impl AsRef<Path>, current: EncryptionInput) -> Result<()> {
        crate::encryption_admin::disable_encryption(path, current)
    }

    /// Re-encrypt every record under a new key.
    #[cfg(feature = "encrypt")]
    pub fn rotate_encryption_key(
        path: impl AsRef<Path>,
        from: EncryptionInput,
        to: EncryptionInput,
    ) -> Result<()> {
        crate::encryption_admin::rotate_encryption_key(path, from, to)
    }
}

impl Inner {
    /// Look up the absolute expiry timestamp for a key in `ns_id`. O(1)
    /// — single index probe + one record decode.
    #[cfg(feature = "ttl")]
    pub(crate) fn engine_expires_at(&self, ns_id: u32, key: &[u8]) -> Result<Option<u64>> {
        Ok(self
            .engine
            .get_with_meta(ns_id, key)?
            .map(|(_, expires_at)| expires_at))
    }
}

/// Iterator over `(key, value)` pairs from [`Emdb::iter`].
pub struct EmdbIter {
    inner: std::vec::IntoIter<RecordSnapshot>,
}

impl Iterator for EmdbIter {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k, v, _)| (k, v))
    }
}

/// Iterator over keys from [`Emdb::keys`].
pub struct EmdbKeyIter {
    inner: std::vec::IntoIter<RecordSnapshot>,
}

impl Iterator for EmdbKeyIter {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k, _, _)| k)
    }
}

/// Compute the lexicographic successor of `prefix` — the smallest byte
/// string that is strictly greater than every string starting with
/// `prefix`. Returns `None` when `prefix` is empty or consists entirely
/// of `0xFF` bytes (no representable successor; caller falls back to
/// an open-ended range).
pub(crate) fn next_prefix(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut out = prefix.to_vec();
    while let Some(byte) = out.last_mut() {
        if *byte < u8::MAX {
            *byte += 1;
            return Some(out);
        }
        let _ = out.pop();
    }
    None
}
