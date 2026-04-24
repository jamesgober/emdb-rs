// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Storage abstraction and operation log types.

use std::path::Path;

use crate::Result;

pub(crate) mod codec;
pub(crate) mod file;
pub(crate) mod memory;

/// Current on-disk emdb format version.
pub(crate) const FORMAT_VERSION: u32 = 2;

/// Header feature bit for `ttl` support.
#[cfg(feature = "ttl")]
pub(crate) const FLAG_TTL: u32 = 1 << 0;
/// Header feature bit for `nested` support.
#[cfg(feature = "nested")]
pub(crate) const FLAG_NESTED: u32 = 1 << 1;

/// Current build feature bitmask written into file headers.
#[must_use]
pub(crate) fn build_flags() -> u32 {
    ttl_flag() | nested_flag()
}

#[cfg(feature = "ttl")]
const fn ttl_flag() -> u32 {
    FLAG_TTL
}

#[cfg(not(feature = "ttl"))]
const fn ttl_flag() -> u32 {
    0
}

#[cfg(feature = "nested")]
const fn nested_flag() -> u32 {
    FLAG_NESTED
}

#[cfg(not(feature = "nested"))]
const fn nested_flag() -> u32 {
    0
}

/// A single operation persisted to storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Op {
    /// Insert or replace a key/value pair.
    Insert {
        /// Key bytes.
        key: Vec<u8>,
        /// Value bytes.
        value: Vec<u8>,
        /// Unix-millis expiration timestamp, if any.
        expires_at: Option<u64>,
    },
    /// Remove a key.
    Remove {
        /// Key bytes.
        key: Vec<u8>,
    },
    /// Remove all keys.
    Clear,
    /// Logical checkpoint marker used for replay sanity.
    Checkpoint {
        /// Number of live records represented at checkpoint time.
        record_count: u32,
    },
    /// Begin a transactional batch.
    BatchBegin {
        /// Monotonic transaction id.
        tx_id: u64,
        /// Number of operations expected before `BatchEnd`.
        op_count: u32,
    },
    /// End a transactional batch.
    BatchEnd {
        /// Monotonic transaction id.
        tx_id: u64,
    },
}

/// Flush durability policy for file-backed storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FlushPolicy {
    /// Flush and fsync after each appended operation.
    OnEachWrite,
    /// Flush and fsync after each `N` appended operations.
    EveryN(u32),
    /// Never auto-flush; caller must invoke `Emdb::flush`.
    Manual,
}

impl Default for FlushPolicy {
    fn default() -> Self {
        Self::EveryN(64)
    }
}

/// A snapshot entry used during compaction.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SnapshotEntry<'a> {
    /// Key bytes.
    pub(crate) key: &'a [u8],
    /// Value bytes.
    pub(crate) value: &'a [u8],
    /// Expiration timestamp in unix millis.
    pub(crate) expires_at: Option<u64>,
}

/// Iterator type passed to storage compaction.
pub(crate) type SnapshotIter<'a> = Box<dyn Iterator<Item = SnapshotEntry<'a>> + 'a>;

/// Persistence backend abstraction.
pub(crate) trait Storage: Send {
    /// Append an operation to durable storage.
    fn append(&mut self, op: &Op) -> Result<()>;

    /// Flush pending writes.
    fn flush(&mut self) -> Result<()>;

    /// Replay all persisted operations into `sink`.
    fn replay(&mut self, sink: &mut dyn FnMut(Op) -> Result<()>) -> Result<()>;

    /// Rewrite storage from a fresh snapshot.
    fn compact(&mut self, snapshot: SnapshotIter<'_>) -> Result<()>;

    /// File path for file-backed storage, if present.
    fn path(&self) -> Option<&Path>;

    /// Return the highest committed transaction id known by this backend.
    fn last_tx_id(&self) -> u64;

    /// Persist the highest committed transaction id.
    fn set_last_tx_id(&mut self, tx_id: u64) -> Result<()>;
}
