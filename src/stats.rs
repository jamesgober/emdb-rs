// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Database introspection — the [`EmdbStats`] snapshot returned by
//! [`crate::Emdb::stats`].
//!
//! Stats are a point-in-time snapshot, not a live counter. Calling
//! `stats()` walks the per-namespace runtime state and asks the
//! filesystem for the file size; both reads are lock-free or
//! short-lock and complete in O(namespaces) time. Use this from
//! dashboards, health checks, or "should I compact now?" decision
//! logic.

/// Point-in-time database statistics.
///
/// Returned by [`crate::Emdb::stats`]. Every field is `pub` because
/// the type is a plain bag-of-numbers and exists purely so consumers
/// can read it; no constructor is exposed.
///
/// # Examples
///
/// ```rust
/// use emdb::Emdb;
///
/// let db = Emdb::open_in_memory();
/// db.insert("k1", "v1")?;
/// db.insert("k2", "v2")?;
///
/// let stats = db.stats()?;
/// assert_eq!(stats.live_records, 2);
/// assert!(stats.file_size_bytes >= stats.logical_size_bytes);
/// # Ok::<(), emdb::Error>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct EmdbStats {
    /// Live record count across every namespace, including the
    /// implicit default namespace.
    pub live_records: u64,
    /// Number of named namespaces (excludes the implicit default).
    pub namespace_count: usize,
    /// Logical end-of-data offset within the file. Bytes from 0 to
    /// `logical_size_bytes` are the active append-only log;
    /// anything past it is pre-allocated padding waiting for the
    /// next append.
    pub logical_size_bytes: u64,
    /// Total file size on disk including pre-allocated padding.
    /// Always `>= logical_size_bytes`.
    pub file_size_bytes: u64,
    /// Bytes pre-allocated past the logical tail. Equals
    /// `file_size_bytes - logical_size_bytes`. Higher values mean
    /// the file has room to grow without a `set_len` call; lower
    /// values mean the next append will trigger a grow.
    pub preallocated_bytes: u64,
    /// Whether the database was opened with the optional
    /// sorted-iteration secondary index enabled. When `true`, the
    /// `range` / `range_iter` / `iter_from` / `iter_after` APIs
    /// are available.
    pub range_scans_enabled: bool,
    /// Whether the database file is encrypted. When `true`, every
    /// record on disk is wrapped in an AEAD envelope; reads pay an
    /// additional decrypt step.
    pub encrypted: bool,
}
