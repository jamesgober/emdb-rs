// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Flush policy.
//!
//! v0.9 inherits the durability machinery from `fsys::JournalHandle`:
//!
//! - **Lock-free LSN reservation**: many threads can append
//!   concurrently with no mutex on the hot path.
//! - **Group-commit fsync**: concurrent `sync_through` calls
//!   coalesce into a single `fdatasync` (or NVMe passthrough
//!   flush, where the hardware permits).
//! - **Hardware-aware durability**: fsys's `Method::Auto` picks
//!   the best primitive for the host platform — io_uring on
//!   Linux ≥ 5.1, `WRITE_THROUGH` on Windows, plain `fdatasync`
//!   elsewhere.
//!
//! The previous `FlushPolicy::Group { max_wait, max_batch }`
//! tuning knobs are gone. fsys's coordinator runs on a different
//! shape (immediate-coalesce around an in-flight syscall, no
//! deadline-window) and the previous knobs do not map cleanly.
//! Concurrent flushers still share one syscall — that win is
//! preserved — but it now happens implicitly inside fsys.

/// How `db.flush()` interacts with concurrent flush requests
/// and how each `db.insert()` interacts with durability.
///
/// All three variants use the same underlying journal substrate
/// (`fsys::JournalHandle` in buffered mode); they differ in
/// when durability is established relative to the call.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FlushPolicy {
    /// Inserts return as soon as the bytes are in the OS page
    /// cache; durability is established only when `db.flush()`
    /// is called. Concurrent flushers coalesce through fsys's
    /// internal group-commit coordinator into a single sync
    /// call.
    ///
    /// Default, and the right choice for batched workloads:
    /// many inserts followed by a single `flush()`. The
    /// canonical WAL pattern.
    #[default]
    OnEachFlush,
    /// Same as `OnEachFlush`. Retained as a separate variant
    /// for source-compat with v0.8.x callers that selected
    /// `Group { .. }` explicitly. The previous `max_wait` /
    /// `max_batch` parameters are not exposed in v0.9 because
    /// fsys's coordinator runs without them.
    Group,
    /// Every `db.insert()` syncs before returning, so the
    /// record is durable on stable storage by the time the
    /// caller sees `Ok(())`. Use this for single-thread
    /// per-record-durability workloads where the caller cannot
    /// or will not call `flush()` after every record.
    ///
    /// `db.flush()` under this policy is a near-free no-op:
    /// the journal's tail is already synced past every
    /// previously-inserted record.
    WriteThrough,
}
