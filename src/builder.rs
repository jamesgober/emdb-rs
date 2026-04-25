// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Database builder.

use std::path::PathBuf;

#[cfg(feature = "ttl")]
use std::time::Duration;

use crate::storage::v4::io::IoMode;
use crate::storage::v4::wal::FlushPolicy as V4FlushPolicy;
use crate::storage::FlushPolicy;
use crate::Emdb;
use crate::Result;

/// Builder for constructing an in-memory [`Emdb`] instance.
#[derive(Debug, Clone)]
pub struct EmdbBuilder {
    #[cfg(feature = "ttl")]
    pub(crate) default_ttl: Option<Duration>,
    pub(crate) path: Option<PathBuf>,
    pub(crate) flush_policy: FlushPolicy,
    #[cfg(feature = "mmap")]
    pub(crate) use_mmap: bool,

    // v0.7 engine opt-in (path-backed only).
    pub(crate) prefer_v4: bool,
    pub(crate) page_io_mode: IoMode,
    pub(crate) wal_io_mode: IoMode,
    pub(crate) page_cache_pages: usize,
    pub(crate) value_cache_bytes: usize,
    pub(crate) bloom_initial_capacity: u64,
}

impl Default for EmdbBuilder {
    fn default() -> Self {
        Self {
            #[cfg(feature = "ttl")]
            default_ttl: None,
            path: None,
            flush_policy: FlushPolicy::default(),
            #[cfg(feature = "mmap")]
            use_mmap: false,
            prefer_v4: false,
            page_io_mode: IoMode::Buffered,
            wal_io_mode: IoMode::Buffered,
            // 0 = use the cache's own default (8 MB at 4 KB pages).
            page_cache_pages: 0,
            // 64 MB default value cache.
            value_cache_bytes: 64 * 1024 * 1024,
            // 1 K-key initial bloom; grows lazily as record_count climbs.
            bloom_initial_capacity: 1_024,
        }
    }
}

impl EmdbBuilder {
    /// Create a new builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the global default TTL for inserted records.
    #[cfg(feature = "ttl")]
    #[must_use]
    pub fn default_ttl(mut self, ttl: Duration) -> Self {
        self.default_ttl = Some(ttl);
        self
    }

    /// Set a file path for persistent storage.
    #[must_use]
    pub fn path(mut self, path: impl Into<PathBuf>) -> Self {
        self.path = Some(path.into());
        self
    }

    /// Set flush durability policy.
    #[must_use]
    pub fn flush_policy(mut self, policy: FlushPolicy) -> Self {
        self.flush_policy = policy;
        self
    }

    /// Enable or disable mmap-backed reads for persistent databases.
    #[cfg(feature = "mmap")]
    #[must_use]
    pub fn use_mmap(mut self, on: bool) -> Self {
        self.use_mmap = on;
        self
    }

    /// Opt into the v0.7 engine for path-backed databases.
    ///
    /// When set, [`EmdbBuilder::build`] uses the new packed-leaf storage
    /// engine for the configured path. New files start in v4 format;
    /// existing v3 files are migrated in place during open. In-memory
    /// databases are unaffected — they always use the v0.6 path.
    ///
    /// Default: `false` (v0.6 backend) for backward compatibility.
    /// Operations not yet ported to v0.7 (transactions, named
    /// namespaces) return [`crate::Error::InvalidConfig`] when this flag
    /// is set; revert to `prefer_v4(false)` if you need them.
    #[must_use]
    pub fn prefer_v4(mut self, on: bool) -> Self {
        self.prefer_v4 = on;
        self
    }

    /// Set the I/O mode for the page file when [`Self::prefer_v4`] is on.
    /// See [`IoMode`] for the trade-offs. Buffered is the default.
    #[must_use]
    pub fn page_io_mode(mut self, mode: IoMode) -> Self {
        self.page_io_mode = mode;
        self
    }

    /// Set the I/O mode for the WAL when [`Self::prefer_v4`] is on. On
    /// Windows, [`IoMode::Direct`] gives single-syscall durability via
    /// `WRITE_THROUGH`. On Linux/macOS, buffered is usually correct.
    #[must_use]
    pub fn wal_io_mode(mut self, mode: IoMode) -> Self {
        self.wal_io_mode = mode;
        self
    }

    /// Set the v0.7 page-cache size in pages. `0` (default) selects the
    /// cache's own default (~8 MB at 4 KB pages).
    #[must_use]
    pub fn page_cache_pages(mut self, pages: usize) -> Self {
        self.page_cache_pages = pages;
        self
    }

    /// Set the v0.7 value-cache size in bytes. `0` disables the cache.
    /// Default: 64 MB.
    #[must_use]
    pub fn value_cache_bytes(mut self, bytes: usize) -> Self {
        self.value_cache_bytes = bytes;
        self
    }

    /// Set the v0.7 bloom-filter initial capacity (in keys). The bloom
    /// auto-resizes as record_count grows. `0` disables the bloom.
    /// Default: 1 024.
    #[must_use]
    pub fn bloom_initial_capacity(mut self, capacity: u64) -> Self {
        self.bloom_initial_capacity = capacity;
        self
    }

    /// Translate the legacy [`FlushPolicy`] into the v0.7 WAL flush policy.
    /// `OnEachWrite` and `Manual` map directly; `EveryN(n)` maps to a
    /// group-commit `max_wait` proportional to `n` (a small heuristic
    /// that keeps the v0.7 default reasonable without a separate API).
    pub(crate) fn v4_flush_policy(&self) -> V4FlushPolicy {
        match self.flush_policy {
            FlushPolicy::OnEachWrite => V4FlushPolicy::OnEachWrite,
            FlushPolicy::Manual => V4FlushPolicy::Manual,
            FlushPolicy::EveryN(_) => V4FlushPolicy::Group {
                max_wait: std::time::Duration::from_micros(500),
            },
        }
    }

    /// Build an [`Emdb`] instance from the configured options.
    ///
    /// # Errors
    ///
    /// Returns an error when configuration is invalid (for example
    /// `FlushPolicy::EveryN(0)`) or storage initialization fails.
    pub fn build(self) -> Result<Emdb> {
        Emdb::from_builder(self)
    }
}

#[cfg(test)]
mod tests {
    use super::EmdbBuilder;
    use crate::FlushPolicy;

    #[test]
    fn test_build_returns_empty_database() {
        let db = EmdbBuilder::new().build();
        assert!(db.is_ok());
        let db = match db {
            Ok(db) => db,
            Err(err) => panic!("build should succeed: {err}"),
        };
        assert!(matches!(db.is_empty(), Ok(true)));
    }

    #[test]
    fn test_default_builder_builds_database() {
        let db = EmdbBuilder::default().build();
        assert!(db.is_ok());
        let db = match db {
            Ok(db) => db,
            Err(err) => panic!("build should succeed: {err}"),
        };
        assert!(matches!(db.len(), Ok(0)));
    }

    #[cfg(feature = "ttl")]
    #[test]
    fn test_default_ttl_builder_method_is_usable() {
        use std::time::Duration;

        let db = EmdbBuilder::new()
            .default_ttl(Duration::from_secs(1))
            .build();
        assert!(db.is_ok());
        let db = match db {
            Ok(db) => db,
            Err(err) => panic!("build should succeed: {err}"),
        };
        assert!(matches!(db.is_empty(), Ok(true)));
    }

    #[test]
    fn test_flush_policy_every_n_zero_errors() {
        let db = EmdbBuilder::new()
            .flush_policy(FlushPolicy::EveryN(0))
            .build();
        assert!(db.is_err());
    }
}
