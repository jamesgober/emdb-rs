// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! `Emdb` — the public database handle.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[cfg(feature = "ttl")]
use std::time::Duration;

use crate::builder::EmdbBuilder;
use crate::lockfile::LockFile;
use crate::storage::engine::{is_live, RangeCursor};
use crate::storage::{Engine, EngineConfig, DEFAULT_NAMESPACE_ID};
use crate::Result;

#[cfg(feature = "ttl")]
use crate::ttl::{expires_from_ttl, is_expired, now_unix_millis, remaining_ttl, Ttl};

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
    /// Private temp directory of an ephemeral database
    /// ([`Emdb::open_in_memory`]). Declared last so it drops after the
    /// engine (whose store flushes and unmaps the file) and the lock
    /// file: removing the directory then takes every file emdb wrote
    /// into it, the data file and all its sidecars, while nothing
    /// holds them any more.
    _ephemeral_dir: Option<crate::data_dir::EphemeralDir>,
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

    /// Open an ephemeral database.
    ///
    /// Despite the name, the data is not held only in memory: the
    /// handle is backed by an ordinary emdb database file inside a
    /// fresh owner-only directory (mode `0o700` on Unix) under the OS
    /// temp directory, written through the same journal as
    /// [`Emdb::open`]. The directory and everything in it are removed
    /// when the last clone drops (best effort; a process that is
    /// killed leaves them behind). Records may reach swap or disk like
    /// any other database, so do not use it for data that must never
    /// touch storage. Useful for tests, REPLs, and anywhere a
    /// disposable store is wanted.
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

        // No path supplied at all → ephemeral mode: a database file
        // inside a fresh owner-only directory under the OS temp
        // directory, removed (directory and all) when the last handle
        // drops.
        let (path, ephemeral_dir) = match path {
            Some(p) => (p, None),
            None => {
                let (p, dir) = crate::data_dir::ephemeral_database_path()?;
                (p, Some(dir))
            }
        };

        // Lock, meta and journal paths derive from the canonical path,
        // so opening the same file through a symbolic link contends
        // for the same lock.
        let engine_path = crate::data_dir::canonical_database_path(&path)?;
        let lock_file = LockFile::acquire(&engine_path)?;
        // Complete an encryption admin rewrite a crash interrupted
        // before anything reads the files it renames. Runs before the
        // data file is created below, so a database that is missing
        // next to its `.encbak` copy is still recognised as such.
        crate::encryption_admin::finish_interrupted_rewrite(&engine_path)?;
        // Create a missing database file owner-only before the engine
        // opens it. An empty data file without a `.meta` sidecar is a
        // fresh database.
        let _created = crate::private_fs::create_private_file(&engine_path)?;

        let engine_config = EngineConfig {
            path: engine_path,
            flags: 0,
            enable_range_scans: builder.enable_range_scans,
            flush_policy: builder.flush_policy,
            iouring_sqpoll_idle_ms: builder.iouring_sqpoll_idle_ms,
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
                _ephemeral_dir: ephemeral_dir,
            }),
        };

        // The open-time expiry sweep now runs inside `Engine::open`.
        Ok(db)
    }

    /// On-disk path of this database.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    // ---- core key/value operations ----

    /// Insert or replace a key/value pair.
    ///
    /// Writes one frame to the journal; returns as soon as the bytes
    /// are in the OS page cache (the default `FlushPolicy::OnEachFlush`
    /// — durability is established only by a subsequent `flush`).
    ///
    /// # Examples
    ///
    /// ```
    /// use emdb::Emdb;
    ///
    /// let db = Emdb::open_in_memory();
    /// db.insert("name", "emdb")?;
    /// db.insert(b"key".to_vec(), b"value".to_vec())?;
    /// assert_eq!(db.get("name")?.as_deref(), Some(b"emdb".as_slice()));
    /// # Ok::<(), emdb::Error>(())
    /// ```
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

    /// Insert many key/value pairs in one vectored journal-append pass.
    ///
    /// Routes through `fsys::JournalHandle::append_batch` — one LSN
    /// reservation, one `pwrite` of the whole batch as a single
    /// contiguous buffer. Strictly faster than the equivalent
    /// insert-in-a-loop, especially under any flush policy that
    /// would otherwise pay per-record fsync.
    ///
    /// # Examples
    ///
    /// ```
    /// use emdb::Emdb;
    ///
    /// let db = Emdb::open_in_memory();
    /// let batch: Vec<(String, String)> = (0..1000)
    ///     .map(|i| (format!("k{i}"), format!("v{i}")))
    ///     .collect();
    /// db.insert_many(batch.iter().map(|(k, v)| (k.as_str(), v.as_str())))?;
    /// assert_eq!(db.len()?, 1000);
    /// # Ok::<(), emdb::Error>(())
    /// ```
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

    /// Zero-copy fetch: returns a [`crate::ValueRef`] that reads
    /// directly from the kernel-managed mmap region (no copy, no
    /// allocation) on unencrypted databases.
    ///
    /// Encrypted databases fall back to an owned plaintext buffer
    /// inside the [`crate::ValueRef`] — AEAD decryption necessarily
    /// allocates fresh bytes — but the caller-facing type is the
    /// same.
    ///
    /// The returned reference holds a strong handle to the mmap
    /// region, so it is safe to keep across writer activity (file
    /// growth, in-place updates) — the kernel keeps the original
    /// mapping alive until the last [`crate::ValueRef`] derived
    /// from it drops.
    ///
    /// # Errors
    ///
    /// Same as [`Self::get`].
    pub fn get_zerocopy(&self, key: impl AsRef<[u8]>) -> Result<Option<crate::ValueRef>> {
        let key = key.as_ref();
        match self.inner.engine.get_zerocopy(DEFAULT_NAMESPACE_ID, key)? {
            None => Ok(None),
            Some((value_ref, expires_at)) => {
                #[cfg(feature = "ttl")]
                {
                    if expires_at != 0 && is_expired(Some(expires_at), now_unix_millis()) {
                        return Ok(None);
                    }
                }
                #[cfg(not(feature = "ttl"))]
                let _ = expires_at;
                Ok(Some(value_ref))
            }
        }
    }

    /// Fetch a value by key.
    ///
    /// Allocates a fresh `Vec<u8>` for the returned value. For tight
    /// loops on small values where the allocation dominates, prefer
    /// [`Self::get_zerocopy`] — it borrows directly from the mmap.
    ///
    /// Returns `Ok(None)` for missing keys, expired records (when the
    /// `ttl` feature is on), and tombstoned slots. Returns `Err` only
    /// on I/O / decode failures.
    ///
    /// # Examples
    ///
    /// ```
    /// use emdb::Emdb;
    ///
    /// let db = Emdb::open_in_memory();
    /// db.insert("k", "v")?;
    /// assert_eq!(db.get("k")?.as_deref(), Some(b"v".as_slice()));
    /// assert_eq!(db.get("missing")?, None);
    /// # Ok::<(), emdb::Error>(())
    /// ```
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
            Ok(self
                .inner
                .engine
                .get_with_meta(DEFAULT_NAMESPACE_ID, key)?
                .map(|(value, _)| value))
        }
    }

    /// Remove a key, returning the previously-stored value if any.
    ///
    /// Writes a tombstone frame to the journal. The on-disk record is
    /// removed only at compaction time; lookups return `None` from
    /// the moment `remove` returns.
    ///
    /// # Examples
    ///
    /// ```
    /// use emdb::Emdb;
    ///
    /// let db = Emdb::open_in_memory();
    /// db.insert("k", "v")?;
    /// assert_eq!(db.remove("k")?.as_deref(), Some(b"v".as_slice()));
    /// assert!(db.get("k")?.is_none());
    /// assert!(db.remove("k")?.is_none()); // already removed
    /// # Ok::<(), emdb::Error>(())
    /// ```
    pub fn remove(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        self.inner.engine.remove(DEFAULT_NAMESPACE_ID, key.as_ref())
    }

    /// Returns whether a key has a live record.
    ///
    /// Records whose TTL has passed are reported as absent, the same
    /// as [`Self::get`]. Only the key and expiry are decoded; the value
    /// is not copied.
    pub fn contains_key(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        self.inner
            .engine
            .contains_live(DEFAULT_NAMESPACE_ID, key.as_ref(), expiry_clock())
    }

    /// Number of records in the default namespace.
    ///
    /// The count comes from the in-memory index and is exact with
    /// respect to completed writes. With the `ttl` feature, records
    /// whose TTL has passed are still counted until they are removed
    /// by [`Self::sweep_expired`] (or a `remove`); `get`, `contains_key`
    /// and the iterators already treat them as absent.
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
    ///
    /// Writes a remove record for every live key, so the clear
    /// survives a reopen on the same terms as [`Self::remove`] (durable
    /// after the next [`Self::flush`], or on return under
    /// `FlushPolicy::WriteThrough`). Writers on other threads wait
    /// while it runs. The space is reclaimed by [`Self::compact`].
    pub fn clear(&self) -> Result<()> {
        self.inner.engine.clear_namespace(DEFAULT_NAMESPACE_ID)
    }

    /// Force pending writes to disk (`fdatasync`).
    ///
    /// Insert calls return as soon as bytes are in the OS page cache;
    /// durability is established here. Concurrent `flush` calls from
    /// multiple threads coalesce through fsys's group-commit
    /// coordinator into a single sync syscall.
    ///
    /// # Examples
    ///
    /// ```
    /// use emdb::Emdb;
    ///
    /// let db = Emdb::open_in_memory();
    /// db.insert("k", "v")?;
    /// db.flush()?; // bytes are now durable (no-op on in-memory)
    /// # Ok::<(), emdb::Error>(())
    /// ```
    pub fn flush(&self) -> Result<()> {
        self.inner.engine.flush()
    }

    /// Snapshot a point-in-time [`crate::EmdbStats`] for monitoring,
    /// dashboards, or compaction-decision logic.
    ///
    /// O(namespaces) plus one filesystem `metadata` call. Cheap
    /// enough to call from a per-second health-check loop. Returns
    /// a `Copy` value type, so the result can be passed across
    /// thread boundaries without lifetime concerns.
    ///
    /// In an async context, prefer calling this directly rather
    /// than wrapping in `spawn_blocking` — the work is dominated by
    /// a fast filesystem stat and a few atomic loads, neither of
    /// which can stall the executor meaningfully.
    ///
    /// # Errors
    ///
    /// I/O errors from the metadata call are silently absorbed — the
    /// file size falls back to the in-memory `logical_size_bytes`,
    /// which is a strict lower bound. The engine's locks are
    /// `parking_lot`-backed and cannot poison.
    pub fn stats(&self) -> Result<crate::EmdbStats> {
        self.inner.engine.stats()
    }

    /// Sync the journal (like [`Self::flush`]) and rewrite the `.meta`
    /// sidecar through an atomic replace.
    ///
    /// The sidecar holds no recovery position: every [`Self::open`]
    /// scans the whole journal to rebuild the index, with or without
    /// a checkpoint, so this does not make the next open faster.
    /// Earlier documentation claimed otherwise. The sidecar is already
    /// written whenever its contents change, so `checkpoint` is mainly
    /// a durability barrier that also reports a poisoned journal.
    ///
    /// Dropping the last handle flushes as a best effort, but cannot
    /// report a failure; call this or [`Self::flush`] before shutdown
    /// when the outcome matters.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from the sync or the sidecar write. Fails
    /// when an earlier write or sync failure poisoned the journal.
    pub fn checkpoint(&self) -> Result<()> {
        self.inner.engine.checkpoint()
    }

    /// Iterator over `(key, value)` pairs in the default namespace,
    /// in no particular order.
    ///
    /// The call snapshots the file offsets of every record that is
    /// live at that moment (`O(N)` memory for `N` offsets) and pins the
    /// file mapping they point into; `next()` decodes one record at a
    /// time, so values are never all resident at once.
    ///
    /// Because the snapshot holds offsets into the append-only log,
    /// the iterator yields each snapshotted record with the value it
    /// had when `iter()` was called, even if the key is overwritten or
    /// removed while iterating. Keys inserted after the call are not
    /// yielded, and a compaction does not change what it returns (the
    /// pinned mapping of the old file stays readable until the
    /// iterator drops). With the `ttl` feature, records whose TTL has
    /// passed by the time `next()` reaches them are skipped.
    ///
    /// A record that fails to decode (I/O or corruption) is skipped
    /// silently; use [`Self::get`] on a specific key to see the error.
    pub fn iter(&self) -> Result<EmdbIter> {
        let offsets = self.inner.engine.snapshot_offsets(DEFAULT_NAMESPACE_ID)?;
        Ok(EmdbIter {
            cursor: OffsetCursor::new(Arc::clone(&self.inner), DEFAULT_NAMESPACE_ID, offsets),
        })
    }

    /// Iterator over keys in the default namespace.
    ///
    /// Same snapshot, expiry and error semantics as [`Self::iter`].
    /// Values are not decoded.
    pub fn keys(&self) -> Result<EmdbKeyIter> {
        let offsets = self.inner.engine.snapshot_offsets(DEFAULT_NAMESPACE_ID)?;
        Ok(EmdbKeyIter {
            cursor: OffsetCursor::new(Arc::clone(&self.inner), DEFAULT_NAMESPACE_ID, offsets),
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
    /// enabled at open time.
    pub fn range<R>(&self, range: R) -> Result<Vec<(Vec<u8>, Vec<u8>)>>
    where
        R: std::ops::RangeBounds<Vec<u8>>,
    {
        self.inner
            .engine
            .range_scan(DEFAULT_NAMESPACE_ID, range, expiry_clock())
    }

    /// Streaming range scan: same results as [`Self::range`], but
    /// returns an iterator that walks the sorted index lazily. Use it
    /// when only the first few elements are needed ("the next 10 keys
    /// at or after this prefix"): the cost is one index seek plus the
    /// elements actually consumed, independent of the range size.
    ///
    /// The iterator is a cursor over the live sorted index, not a
    /// snapshot. Keys come out in ascending order, each at most once.
    /// Keys inserted or removed ahead of the cursor while it runs may
    /// or may not be observed; keys behind the cursor are not
    /// revisited. A [`Self::compact`] while the iterator runs does not
    /// break this: the iterator continues after the last key it
    /// yielded, reading the compacted file. With the `ttl` feature,
    /// records whose TTL has passed are skipped. A record that fails to
    /// decode is skipped silently, and an I/O error while reading the
    /// journal ends the iteration.
    ///
    /// # Errors
    ///
    /// Same as [`Self::range`].
    pub fn range_iter<R>(&self, range: R) -> Result<EmdbRangeIter>
    where
        R: std::ops::RangeBounds<Vec<u8>>,
    {
        let cursor = self
            .inner
            .engine
            .range_cursor(DEFAULT_NAMESPACE_ID, range)?;
        Ok(EmdbRangeIter {
            state: RangeState::new(Arc::clone(&self.inner), DEFAULT_NAMESPACE_ID, cursor),
        })
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

    /// Streaming variant of [`Self::range_prefix`].
    ///
    /// # Errors
    ///
    /// Same as [`Self::range_iter`].
    pub fn range_prefix_iter(&self, prefix: impl AsRef<[u8]>) -> Result<EmdbRangeIter> {
        let prefix = prefix.as_ref();
        let start = prefix.to_vec();
        match next_prefix(prefix) {
            Some(end) => self.range_iter(start..end),
            None => self.range_iter(start..),
        }
    }

    /// Streaming iterator over keys at or after `start`, in
    /// lexicographic order. Requires the database to have been
    /// opened with [`crate::EmdbBuilder::enable_range_scans`]`(true)`.
    ///
    /// Useful for paginated APIs: pass the last-seen key as `start`
    /// on the next call to resume iteration. The iterator is lazy
    /// (decodes one record per `next()` call), so consumers paying
    /// for only the first N elements get O(N) work, not O(total).
    ///
    /// # Errors
    ///
    /// Same as [`Self::range_iter`].
    pub fn iter_from(&self, start: impl AsRef<[u8]>) -> Result<EmdbRangeIter> {
        self.range_iter(start.as_ref().to_vec()..)
    }

    /// Streaming iterator over keys strictly after `start`, in
    /// lexicographic order. Same as [`Self::iter_from`] but skips
    /// any record with a key equal to `start`. Useful for
    /// "give me the next page after this cursor" patterns where
    /// the cursor is the last key already seen.
    ///
    /// # Errors
    ///
    /// Same as [`Self::range_iter`].
    pub fn iter_after(&self, start: impl AsRef<[u8]>) -> Result<EmdbRangeIter> {
        let start = start.as_ref().to_vec();
        self.range_iter((std::ops::Bound::Excluded(start), std::ops::Bound::Unbounded))
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

    /// Remove the TTL from a record (rewrite it with no expiry).
    /// Returns true if the record was live and had a TTL.
    ///
    /// A record whose TTL has already passed is left alone and `false`
    /// is returned: an expired key is never brought back. The check
    /// and the rewrite run under the key's write lock, so a concurrent
    /// write to the same key is ordered before or after the whole
    /// call.
    #[cfg(feature = "ttl")]
    pub fn persist(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        self.inner
            .engine
            .clear_expiry(DEFAULT_NAMESPACE_ID, key.as_ref(), now_unix_millis())
    }

    /// Remove every record whose TTL has expired. Returns the count
    /// of evicted records. Errors during sweep are swallowed (returning
    /// the partial count) so callers can use this in best-effort
    /// background loops.
    ///
    /// The sweep scans keys and expiry times only (values are not
    /// loaded) and removes a record only if it is still the key's
    /// current record, so a key re-inserted while the sweep runs keeps
    /// its new value.
    #[cfg(feature = "ttl")]
    pub fn sweep_expired(&self) -> usize {
        sweep_namespace(&self.inner.engine, DEFAULT_NAMESPACE_ID)
    }

    /// Read the metadata of whoever currently holds the advisory
    /// lock on `path`, without trying to acquire the lock.
    ///
    /// Reads the `<path>.lock-meta` holder file (at most 4 KiB), which
    /// a holder writes after taking the lock and removes when it
    /// closes. The `<path>.lock` file itself stays in place between
    /// opens. Returns `Ok(None)` when no holder file exists (the
    /// database is unlocked). Returns `Ok(Some(holder))` when the
    /// holder file is present and well-formed, typically because
    /// some emdb instance is either currently using the database or
    /// died with the lock held. Symbolic links in `path` are resolved,
    /// so any name for the database reports the same holder.
    ///
    /// This is the diagnostic precondition for [`Self::break_lock`]:
    /// read the holder, confirm via OS tooling (`ps`, `Get-Process`,
    /// container inspection, etc.) that the PID is gone, then break
    /// the lock.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::LockfileError`] for I/O failures, or
    /// [`crate::Error::Corrupted`] when the sidecar exists but its
    /// body is malformed.
    pub fn lock_holder(path: impl AsRef<Path>) -> Result<Option<crate::LockHolder>> {
        crate::lockfile::LockFile::read_holder(path.as_ref())
    }

    /// Forcibly remove the `<path>.lock` and `<path>.lock-meta`
    /// sidecars. Normally not needed: the OS releases the advisory lock
    /// when the holding process exits, and the leftover lock file is
    /// reused by the next open.
    ///
    /// # Safety contract (read carefully)
    ///
    /// emdb is single-writer per file. The lockfile exists to stop
    /// two concurrent processes from corrupting the database. If
    /// you call `break_lock` while a live process is still holding
    /// the lock, that process and the next opener will both write
    /// to the same file and produce undefined results — torn
    /// records, lost updates, possibly an unrecoverable file.
    ///
    /// **Before calling this, you MUST confirm the holder is
    /// dead.** Use [`Self::lock_holder`] to read the PID, then
    /// confirm via OS tooling appropriate to your environment:
    ///
    /// - Linux/macOS: `ps -p <pid>` (silent exit code 1 means dead)
    /// - Windows: `Get-Process -Id <pid>` (errors mean dead)
    /// - Containers: confirm the container/pod is no longer
    ///   running before calling.
    ///
    /// emdb deliberately does not perform this check itself —
    /// portable PID-liveness on Windows + Unix would require
    /// adding an OS-FFI dependency, and a check based on stale
    /// timestamps is too easily wrong.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::LockfileError`] for I/O failures.
    /// Treats "lockfile already gone" as success — the operation
    /// is idempotent.
    pub fn break_lock(path: impl AsRef<Path>) -> Result<()> {
        crate::lockfile::LockFile::break_lock(path.as_ref())
    }

    /// Atomically snapshot the live record set into a self-contained
    /// backup file at `target`. The resulting file is a normal emdb
    /// database that can be opened with [`Self::open`] — it is not a
    /// dump format, archive, or proprietary blob.
    ///
    /// Implementation: the set of live records is captured with writers
    /// paused for the index walk only; the records are then copied to
    /// `<target>.backup.tmp`, its sidecar written to
    /// `<target>.backup.tmp.meta`, both synced, and both renamed over
    /// `<target>.meta` and `target` (an atomic replace, never a delete
    /// followed by a rename). Failure before the renames leaves
    /// `target` untouched and the temporaries are removed. The backup
    /// keeps the source's encryption salt and verification block, so
    /// it opens with the same key or passphrase.
    ///
    /// `target` must differ from the live database's own path. If
    /// `target` already exists, it is overwritten; emdb does not
    /// keep historical backups for you; callers wanting timestamped
    /// snapshots should incorporate the timestamp into `target`.
    ///
    /// This is a heavier operation than [`Self::flush`] — it walks
    /// every record in every namespace, encodes them, and writes
    /// the result. In an async context, call this via
    /// `tokio::task::spawn_blocking` (or your runtime's equivalent)
    /// to avoid stalling the executor; the work is bounded but
    /// proportional to the database size.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use emdb::Emdb;
    ///
    /// let db = Emdb::open_in_memory();
    /// db.insert("user:1", "alice")?;
    /// db.insert("user:2", "bob")?;
    ///
    /// let backup = std::env::temp_dir().join("emdb-backup-example.emdb");
    /// db.backup_to(&backup)?;
    ///
    /// // The backup is a fully-formed emdb database.
    /// let restored = Emdb::open(&backup)?;
    /// assert_eq!(restored.get("user:1")?, Some(b"alice".to_vec()));
    /// assert_eq!(restored.get("user:2")?, Some(b"bob".to_vec()));
    ///
    /// # drop(restored);
    /// # let _ = std::fs::remove_file(&backup);
    /// # let _ = std::fs::remove_file(format!("{}.lock", backup.display()));
    /// # Ok::<(), emdb::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::InvalidConfig`] if `target` equals the
    /// live database's path. Returns I/O errors from the rewrite,
    /// sync, or rename phases.
    pub fn backup_to(&self, target: impl AsRef<Path>) -> Result<()> {
        self.inner.engine.backup_to(target.as_ref())
    }

    /// Compact the on-disk file by rewriting only live records and
    /// atomically swapping the new file in for the old.
    ///
    /// Tombstoned records (from `remove`) and superseded records (from
    /// `insert` overwriting an existing key) remain in the on-disk log
    /// until the next compaction. This call walks every namespace's
    /// live index, writes the surviving records into a sibling file
    /// (`<path>.compact.tmp`), syncs it, and atomically renames it
    /// over the original. Writers on other threads wait for the whole
    /// compaction; readers keep running against the old file and switch
    /// to the new one atomically. `iter`/`keys` iterators and
    /// `ValueRef`s created before the compaction keep reading the
    /// snapshot they were created from; range iterators continue after
    /// their last key in the compacted file. Peak memory is bounded by a few MiB of batch
    /// buffers plus one offset per live record, not by the file size.
    ///
    /// This is a heavier operation than [`Self::flush`]: call it in
    /// maintenance windows, not on every write. After compaction the
    /// file holds only the live records (each in a 12-byte journal
    /// frame); there is no file header.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from the rewrite, sync, or rename phases.
    /// On failure the original file is left untouched and the temp
    /// file is best-effort cleaned up.
    ///
    /// # Examples
    ///
    /// ```
    /// use emdb::Emdb;
    ///
    /// let db = Emdb::open_in_memory();
    /// for i in 0_u32..100 { db.insert(format!("k{i}"), "v")?; }
    /// for i in 0_u32..100 { let _ = db.remove(format!("k{i}"))?; }
    /// db.compact()?; // reclaim space from the 100 tombstones
    /// assert_eq!(db.len()?, 0);
    /// # Ok::<(), emdb::Error>(())
    /// ```
    pub fn compact(&self) -> Result<()> {
        self.inner.engine.compact_in_place()
    }

    #[cfg(feature = "ttl")]
    fn compute_default_expires_at(&self) -> Result<u64> {
        self.inner.default_expires_at()
    }

    // ---- namespace operations ----

    /// Open or create a named namespace.
    ///
    /// Each named namespace has its own hash index, its own `len()`,
    /// and its own lifecycle. Use namespaces to isolate logical
    /// "tables" within one database file.
    ///
    /// # Examples
    ///
    /// ```
    /// use emdb::Emdb;
    ///
    /// let db = Emdb::open_in_memory();
    /// let users = db.namespace("users")?;
    /// users.insert("alice", "data")?;
    /// assert_eq!(users.len()?, 1);
    /// assert_eq!(db.len()?, 0); // default namespace is untouched
    /// # Ok::<(), emdb::Error>(())
    /// ```
    pub fn namespace(&self, name: impl AsRef<str>) -> Result<crate::namespace::Namespace> {
        let name_ref = name.as_ref();
        let ns_id = self.inner.engine.create_or_open_namespace(name_ref)?;
        Ok(crate::namespace::Namespace::new(
            Arc::clone(&self.inner),
            ns_id,
            name_ref.to_string().into_boxed_str(),
        ))
    }

    /// Drop a named namespace and every record in it.
    ///
    /// Writes a remove record for every key plus a record that unbinds
    /// the name, so neither the data nor the name comes back after a
    /// reopen (durable on the same terms as [`Self::remove`]). Returns
    /// `false` when no namespace has that name. A later
    /// [`Self::namespace`] call with the same name creates a new,
    /// empty namespace. emdb 1.0.2 and earlier, opening a file written
    /// after a drop, list the name again with no records in it.
    pub fn drop_namespace(&self, name: impl AsRef<str>) -> Result<bool> {
        self.inner.engine.drop_namespace(name.as_ref())
    }

    /// List every live namespace name.
    pub fn list_namespaces(&self) -> Result<Vec<String>> {
        let entries = self.inner.engine.list_namespaces()?;
        Ok(entries.into_iter().map(|(_, name)| name).collect())
    }

    // ---- transaction (simple buffered batch) ----

    /// Run a closure inside a buffered write batch. The batch is
    /// committed when the closure returns `Ok(_)`; staged writes are
    /// dropped when it returns `Err(_)`.
    ///
    /// This is a write batch, not an isolated transaction. What it
    /// guarantees:
    ///
    /// - **Rollback:** if the closure returns `Err`, nothing it staged
    ///   is written.
    /// - **Read-your-writes:** reads through the
    ///   [`crate::Transaction`] see the batch's own staged writes.
    /// - **Per-key ordering at commit:** the commit holds the write
    ///   lock of every key it touches while it appends and applies the
    ///   batch, so any other write to one of those keys happens
    ///   entirely before or entirely after the commit.
    ///
    /// What it does not guarantee:
    ///
    /// - **Isolation:** reads inside the closure see the live
    ///   database, and nothing stops another thread from changing a
    ///   key between that read and the commit. A read-modify-write
    ///   (such as incrementing a counter) can lose updates under
    ///   concurrency; serialise such updates yourself.
    /// - **Atomic visibility:** other threads can observe some of the
    ///   batch's keys updated and others not yet updated while the
    ///   commit is applying.
    /// - **Crash atomicity:** the batch is one journal append, but a
    ///   crash during it can leave a prefix of the batch durable
    ///   (each record is individually checksummed).
    ///
    /// # Examples
    ///
    /// ```
    /// use emdb::Emdb;
    ///
    /// let db = Emdb::open_in_memory();
    /// db.transaction(|tx| {
    ///     tx.insert("a", "1")?;
    ///     tx.insert("b", "2")?;
    ///     tx.insert("c", "3")?;
    ///     Ok(())
    /// })?;
    /// assert_eq!(db.len()?, 3);
    /// # Ok::<(), emdb::Error>(())
    /// ```
    ///
    /// Returning `Err` rolls back staged writes:
    ///
    /// ```
    /// use emdb::{Emdb, Error};
    ///
    /// let db = Emdb::open_in_memory();
    /// let result: Result<(), Error> = db.transaction(|tx| {
    ///     tx.insert("staged", "value")?;
    ///     Err(Error::InvalidConfig("rolling back"))
    /// });
    /// assert!(result.is_err());
    /// assert!(db.get("staged")?.is_none());
    /// # Ok::<(), emdb::Error>(())
    /// ```
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

#[cfg(feature = "ttl")]
impl Inner {
    /// Absolute expiry for a write that uses the default TTL, or 0
    /// when no default is configured. The clock is only read when a
    /// default TTL exists.
    pub(crate) fn default_expires_at(&self) -> Result<u64> {
        match self.default_ttl {
            None => Ok(0),
            Some(_) => Ok(
                expires_from_ttl(Ttl::Default, self.default_ttl, now_unix_millis())?.unwrap_or(0),
            ),
        }
    }
}

/// Current time for expiry checks on read paths, or 0 (no expiry
/// filtering) when the `ttl` feature is off.
#[inline]
pub(crate) fn expiry_clock() -> u64 {
    #[cfg(feature = "ttl")]
    {
        now_unix_millis()
    }
    #[cfg(not(feature = "ttl"))]
    {
        0
    }
}

/// Remove every expired record of `ns_id` that is still its key's
/// current record. Shared by [`Emdb::sweep_expired`] and
/// [`crate::Namespace::sweep_expired`]. Errors end the sweep early;
/// the count so far is returned.
#[cfg(feature = "ttl")]
pub(crate) fn sweep_namespace(engine: &crate::storage::Engine, ns_id: u32) -> usize {
    let Ok(expired) = engine.expired_entries(ns_id, now_unix_millis()) else {
        return 0;
    };
    let mut evicted = 0;
    for (key, offset) in expired {
        match engine.remove_if_unchanged(ns_id, &key, offset) {
            Ok(true) => evicted += 1,
            Ok(false) => {}
            Err(_) => break,
        }
    }
    evicted
}

/// State shared by the offset-snapshot iterators ([`EmdbIter`],
/// [`EmdbKeyIter`] and their namespace counterparts).
pub(crate) struct OffsetCursor {
    inner: Arc<Inner>,
    ns_id: u32,
    offsets: std::vec::IntoIter<u64>,
    view: crate::storage::ReadView,
}

impl OffsetCursor {
    pub(crate) fn new(
        inner: Arc<Inner>,
        ns_id: u32,
        (offsets, view): (Vec<u64>, crate::storage::ReadView),
    ) -> Self {
        Self {
            inner,
            ns_id,
            offsets: offsets.into_iter(),
            view,
        }
    }

    /// Next live `(key, value)`. Undecodable and expired records are
    /// skipped.
    pub(crate) fn next_record(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        for offset in self.offsets.by_ref() {
            if let Ok(Some((key, value, expires_at))) = self
                .inner
                .engine
                .decode_owned_in(&self.view, self.ns_id, offset)
            {
                if is_live(expires_at, expiry_clock()) {
                    return Some((key, value));
                }
            }
        }
        None
    }

    /// Next live key. Values are not decoded.
    pub(crate) fn next_key(&mut self) -> Option<Vec<u8>> {
        for offset in self.offsets.by_ref() {
            if let Ok(Some((key, expires_at))) = self
                .inner
                .engine
                .decode_key_in(&self.view, self.ns_id, offset)
            {
                if is_live(expires_at, expiry_clock()) {
                    return Some(key);
                }
            }
        }
        None
    }
}

/// First page size of a range iterator. Small so that `take(n)` for
/// small `n` touches only a few index entries.
const FIRST_RANGE_PAGE: usize = 16;
/// Upper bound of the page size, reached by doubling, so long scans
/// amortise the per-page index seek.
const MAX_RANGE_PAGE: usize = 512;

/// State shared by the range iterators ([`EmdbRangeIter`] and
/// [`crate::NamespaceRangeIter`]).
pub(crate) struct RangeState {
    inner: Arc<Inner>,
    ns_id: u32,
    cursor: RangeCursor,
    page: VecDeque<(Vec<u8>, u64)>,
    page_size: usize,
}

impl RangeState {
    pub(crate) fn new(inner: Arc<Inner>, ns_id: u32, cursor: RangeCursor) -> Self {
        Self {
            inner,
            ns_id,
            cursor,
            page: VecDeque::new(),
            page_size: FIRST_RANGE_PAGE,
        }
    }

    /// Next live `(key, value)` in ascending key order. A page that
    /// cannot be read (an I/O error while mapping the journal) ends the
    /// iteration, like the end of the range.
    pub(crate) fn next_pair(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        loop {
            if self.page.is_empty() {
                self.inner
                    .engine
                    .fill_range(&mut self.cursor, &mut self.page, self.page_size)
                    .ok()?;
                self.page_size = (self.page_size * 2).min(MAX_RANGE_PAGE);
            }
            let (key, offset) = self.page.pop_front()?;
            if let Ok(Some((value, expires_at))) =
                self.inner
                    .engine
                    .read_value_in(self.cursor.view(), self.ns_id, offset, &key)
            {
                if is_live(expires_at, expiry_clock()) {
                    return Some((key, value));
                }
            }
        }
    }
}

/// Iterator over `(key, value)` pairs from [`Emdb::iter`].
///
/// Walks a snapshot of record offsets taken when the iterator was
/// created and decodes one record per `next()`. See [`Emdb::iter`]
/// for exactly which records are yielded. Records that fail to decode
/// are skipped without an error.
pub struct EmdbIter {
    cursor: OffsetCursor,
}

impl Iterator for EmdbIter {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        self.cursor.next_record()
    }
}

/// Iterator over keys from [`Emdb::keys`].
///
/// Same snapshot semantics as [`EmdbIter`]; only keys are decoded.
pub struct EmdbKeyIter {
    cursor: OffsetCursor,
}

impl Iterator for EmdbKeyIter {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        self.cursor.next_key()
    }
}

/// Streaming range iterator returned by [`Emdb::range_iter`],
/// [`Emdb::range_prefix_iter`], [`Emdb::iter_from`] and
/// [`Emdb::iter_after`].
///
/// A cursor over the namespace's lock-free sorted index: each refill
/// seeks the index just past the last key yielded and takes a small
/// page of `(key, offset)` pairs (16 at first, doubling up to 512),
/// and each `next()` decodes one value from the mmap. No lock is held
/// between calls. See [`Emdb::range_iter`] for the consistency
/// guarantees. Records that fail to decode are skipped without an
/// error.
pub struct EmdbRangeIter {
    state: RangeState,
}

impl Iterator for EmdbRangeIter {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        self.state.next_pair()
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
