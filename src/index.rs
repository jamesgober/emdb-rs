// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Sharded in-memory primary index.
//!
//! The index splits keys across [`SHARD_COUNT`] independent shards so unrelated
//! writes do not contend on a single lock. Each shard is a `HashMap` guarded by
//! its own [`RwLock`], chosen over a `BTreeMap` because point lookups dominate
//! the workload and the public API does not promise ordered iteration.
//!
//! Shard selection uses a small FNV-1a hash. It is not cryptographically
//! strong — we never use it for security, only for distributing keys across
//! buckets — and it has no transitive dependencies, no allocations, and
//! inlines into a few instructions on every modern target.

use std::collections::HashMap;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::ttl::Record;
use crate::{Error, Result};

/// Number of shards used by the index. Must be a power of two so the
/// shard-index mask is a single AND, and chosen large enough that typical
/// thread counts (8–32) do not collide on every key.
pub(crate) const SHARD_COUNT: usize = 32;

const SHARD_MASK: u64 = (SHARD_COUNT as u64) - 1;
const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// One shard of the primary index.
pub(crate) type Shard = HashMap<Vec<u8>, Record>;

/// A sharded, lock-striped primary index.
#[derive(Debug)]
pub(crate) struct Index {
    shards: Box<[RwLock<Shard>; SHARD_COUNT]>,
}

impl Index {
    /// Construct an empty index with [`SHARD_COUNT`] empty shards.
    #[must_use]
    pub(crate) fn new() -> Self {
        // Build via std::array::from_fn to avoid forcing `Shard: Copy`.
        let shards = std::array::from_fn::<_, SHARD_COUNT, _>(|_| RwLock::new(Shard::new()));
        Self {
            shards: Box::new(shards),
        }
    }

    /// Pre-fill the index from an iterator of records, distributing across
    /// shards. Used during file replay before the database is exposed.
    pub(crate) fn from_records<I>(records: I) -> Self
    where
        I: IntoIterator<Item = (Vec<u8>, Record)>,
    {
        let index = Self::new();
        for (key, record) in records {
            let shard_idx = shard_for(&key);
            // SAFETY: index is local; no other thread can poison the lock.
            if let Ok(mut shard) = index.shards[shard_idx].write() {
                let _previous = shard.insert(key, record);
            }
        }
        index
    }

    /// Return the shard index for a given key.
    #[inline]
    #[must_use]
    pub(crate) fn shard_for_key(key: &[u8]) -> usize {
        shard_for(key)
    }

    /// Acquire a read guard for a single shard.
    pub(crate) fn read(&self, shard_idx: usize) -> Result<RwLockReadGuard<'_, Shard>> {
        self.shards[shard_idx]
            .read()
            .map_err(|_poisoned| Error::LockPoisoned)
    }

    /// Acquire a write guard for a single shard.
    pub(crate) fn write(&self, shard_idx: usize) -> Result<RwLockWriteGuard<'_, Shard>> {
        self.shards[shard_idx]
            .write()
            .map_err(|_poisoned| Error::LockPoisoned)
    }

    /// Acquire read guards for every shard, in stable shard order.
    ///
    /// Used by `len`, `iter`, and `keys`. Always taken in ascending shard order
    /// to match the write-side ordering and rule out lock-acquisition cycles
    /// between the two paths.
    pub(crate) fn read_all(&self) -> Result<Vec<RwLockReadGuard<'_, Shard>>> {
        let mut guards = Vec::with_capacity(SHARD_COUNT);
        for shard in self.shards.iter() {
            guards.push(shard.read().map_err(|_poisoned| Error::LockPoisoned)?);
        }
        Ok(guards)
    }

    /// Acquire write guards for every shard, in stable shard order.
    ///
    /// Used by `clear` and by transaction commit so the overlay is applied
    /// atomically with respect to readers and concurrent single-key writers.
    pub(crate) fn write_all(&self) -> Result<Vec<RwLockWriteGuard<'_, Shard>>> {
        let mut guards = Vec::with_capacity(SHARD_COUNT);
        for shard in self.shards.iter() {
            guards.push(shard.write().map_err(|_poisoned| Error::LockPoisoned)?);
        }
        Ok(guards)
    }
}

impl Default for Index {
    fn default() -> Self {
        Self::new()
    }
}

/// FNV-1a hash → shard index.
#[inline]
fn shard_for(bytes: &[u8]) -> usize {
    let mut hash = FNV_OFFSET;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    (hash & SHARD_MASK) as usize
}

#[cfg(test)]
mod tests {
    use super::{shard_for, Index, SHARD_COUNT};
    use crate::ttl::record_new;

    #[test]
    fn shard_for_distributes_uniformly_across_a_simple_key_space() {
        let mut counts = [0_usize; SHARD_COUNT];
        for i in 0..10_000_u32 {
            let key = format!("key-{i}").into_bytes();
            counts[shard_for(&key)] += 1;
        }

        // Every shard must have at least one key. With FNV-1a and 10k samples
        // this is essentially deterministic; any failure indicates a real bug.
        for count in counts {
            assert!(count > 0, "shard distribution missed a bucket");
        }
    }

    #[test]
    fn shard_for_is_deterministic() {
        let key = b"deterministic-key";
        let first = shard_for(key);
        let second = shard_for(key);
        assert_eq!(first, second);
    }

    #[test]
    fn index_reads_back_what_it_writes() {
        let index = Index::new();
        let key = b"hello".to_vec();
        let shard_idx = Index::shard_for_key(&key);

        let write_guard = index.write(shard_idx);
        assert!(write_guard.is_ok());
        let mut shard = match write_guard {
            Ok(guard) => guard,
            Err(err) => panic!("write guard should succeed: {err}"),
        };
        let _previous = shard.insert(key.clone(), record_new(b"world".to_vec(), None));
        drop(shard);

        let read_guard = index.read(shard_idx);
        assert!(read_guard.is_ok());
        let shard = match read_guard {
            Ok(guard) => guard,
            Err(err) => panic!("read guard should succeed: {err}"),
        };
        let record = match shard.get(key.as_slice()) {
            Some(record) => record,
            None => panic!("inserted record should be present"),
        };
        assert_eq!(crate::ttl::record_value(record), b"world");
    }

    #[test]
    fn read_all_and_write_all_return_one_guard_per_shard() {
        let index = Index::new();
        let reads = index.read_all();
        assert!(reads.is_ok());
        let reads = match reads {
            Ok(guards) => guards,
            Err(err) => panic!("read_all should succeed: {err}"),
        };
        assert_eq!(reads.len(), SHARD_COUNT);
        drop(reads);

        let writes = index.write_all();
        assert!(writes.is_ok());
        let writes = match writes {
            Ok(guards) => guards,
            Err(err) => panic!("write_all should succeed: {err}"),
        };
        assert_eq!(writes.len(), SHARD_COUNT);
    }
}
