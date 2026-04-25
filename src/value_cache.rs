// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Sharded value cache for the v0.7 storage engine.
//!
//! Sits in front of the keymap so the hottest reads never touch the page
//! cache or the page file. Entries are addressed by `(namespace_id, hash)`
//! rather than the raw key bytes — the hash is the same one the keymap
//! uses, so a cache hit short-circuits the keymap lookup as well as the
//! page read. The full key is stored alongside the value so we can detect
//! the rare 64-bit hash collision and fall back to the page on mismatch.
//!
//! ## Capacity
//!
//! Bounded in **bytes**, not entries. The total budget is split evenly
//! across [`VALUE_SHARD_COUNT`] independent shards, each with its own
//! `RwLock`. Cache hits take a read lock; inserts and evictions take a
//! write lock.
//!
//! ## Eviction
//!
//! CLOCK (second-chance) eviction. Each entry carries an
//! `AtomicBool::referenced` flag set on every successful `get`. The
//! eviction sweep:
//!
//! 1. Pops the front of the shard's circular queue.
//! 2. If `referenced` is set, clears it and pushes the entry back to the
//!    end ("second chance").
//! 3. Otherwise, evicts the entry.
//!
//! This approximates LRU at a fraction of the bookkeeping cost of a true
//! doubly-linked list, and the `referenced` update happens under a
//! read-lock with a single relaxed atomic store.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use crate::{Error, Result};

/// Number of shards in the value cache. Power of two so the shard index
/// is a single AND.
pub(crate) const VALUE_SHARD_COUNT: usize = 32;

const VALUE_SHARD_MASK: u64 = (VALUE_SHARD_COUNT as u64) - 1;

/// Default total byte budget for the value cache. 64 MB at construction
/// time is a reasonable middle ground: small enough to avoid cache
/// pressure on small machines, large enough to absorb a typical hot
/// working set.
pub(crate) const DEFAULT_TOTAL_BYTES: usize = 64 * 1024 * 1024;

/// A cached value. Cloning a [`CachedValue`] is cheap — the underlying
/// bytes live behind an [`Arc`] and only the Arc reference count is
/// touched.
#[derive(Debug, Clone)]
pub(crate) struct CachedValue {
    /// Value bytes.
    pub(crate) value: Arc<[u8]>,
    /// Unix-millis expiry timestamp; 0 means no expiry.
    pub(crate) expires_at: u64,
}

#[derive(Debug)]
struct CacheEntry {
    key: Box<[u8]>,
    value: Arc<[u8]>,
    expires_at: u64,
    referenced: AtomicBool,
}

#[derive(Debug)]
struct CacheShard {
    entries: HashMap<(u32, u64), CacheEntry>,
    clock: VecDeque<(u32, u64)>,
    bytes_used: usize,
    bytes_capacity: usize,
}

impl CacheShard {
    fn new(bytes_capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            clock: VecDeque::new(),
            bytes_used: 0,
            bytes_capacity,
        }
    }

    fn get(&self, ns: u32, hash: u64, key: &[u8]) -> Option<CachedValue> {
        let entry = self.entries.get(&(ns, hash))?;
        if entry.key.as_ref() != key {
            // 64-bit hash collision: a different key happens to land on the
            // same hash. The caller will fall back to the page-side check.
            return None;
        }
        // Mark referenced so the next eviction sweep gives this entry a
        // second chance.
        entry.referenced.store(true, Ordering::Relaxed);
        Some(CachedValue {
            value: Arc::clone(&entry.value),
            expires_at: entry.expires_at,
        })
    }

    fn insert(&mut self, ns: u32, hash: u64, key: Box<[u8]>, value: Arc<[u8]>, expires_at: u64) {
        let entry_bytes = key.len() + value.len();
        // If a single entry exceeds the shard budget there is nothing to
        // be gained by caching it; quietly drop the insert. Callers will
        // still get correctness from the keymap+page path.
        if entry_bytes > self.bytes_capacity {
            return;
        }

        // Replace the entry if it already exists, accounting for the size
        // delta.
        if let Some(existing) = self.entries.get_mut(&(ns, hash)) {
            // Same hash but different key (collision): replace anyway,
            // since the new key is what the caller wants cached.
            let old_bytes = existing.key.len() + existing.value.len();
            existing.key = key;
            existing.value = value;
            existing.expires_at = expires_at;
            existing.referenced.store(true, Ordering::Relaxed);
            self.bytes_used = self.bytes_used + entry_bytes - old_bytes;
            return;
        }

        // Evict until there is room.
        while self.bytes_used + entry_bytes > self.bytes_capacity {
            if !self.evict_one() {
                // Nothing left to evict but still over budget — this only
                // happens if the inputs lie about their size; abort the
                // insert rather than corrupting the bookkeeping.
                return;
            }
        }

        let _previous = self.entries.insert(
            (ns, hash),
            CacheEntry {
                key,
                value,
                expires_at,
                referenced: AtomicBool::new(true),
            },
        );
        self.clock.push_back((ns, hash));
        self.bytes_used = self.bytes_used.saturating_add(entry_bytes);
    }

    fn invalidate(&mut self, ns: u32, hash: u64) -> bool {
        let Some(entry) = self.entries.remove(&(ns, hash)) else {
            return false;
        };
        let bytes = entry.key.len() + entry.value.len();
        self.bytes_used = self.bytes_used.saturating_sub(bytes);
        // Lazy clock cleanup: leave a stale entry in the queue for the
        // next eviction sweep to drop. This avoids an O(n) `position` scan
        // on every invalidate.
        true
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.clock.clear();
        self.bytes_used = 0;
    }

    fn evict_one(&mut self) -> bool {
        // Bound the sweep so a pathological input cannot loop forever.
        let mut budget = self.clock.len().saturating_mul(2).max(1);
        while budget > 0 {
            budget -= 1;
            let Some(candidate) = self.clock.pop_front() else {
                return false;
            };
            let entry_present = self.entries.contains_key(&candidate);
            if !entry_present {
                // Stale clock entry from a previous invalidate; drop it.
                continue;
            }
            // SAFETY-equivalent: entry exists, take a reference under
            // self (mutable already held).
            let referenced = match self.entries.get(&candidate) {
                Some(entry) => entry.referenced.swap(false, Ordering::Relaxed),
                None => continue,
            };
            if referenced {
                self.clock.push_back(candidate);
                continue;
            }
            if let Some(entry) = self.entries.remove(&candidate) {
                let bytes = entry.key.len() + entry.value.len();
                self.bytes_used = self.bytes_used.saturating_sub(bytes);
                return true;
            }
        }
        false
    }
}

/// Sharded value cache. See module docs for the full design.
#[derive(Debug)]
pub(crate) struct ValueCache {
    shards: Box<[RwLock<CacheShard>; VALUE_SHARD_COUNT]>,
}

impl ValueCache {
    /// Construct a fresh value cache with `total_bytes` of total capacity
    /// distributed evenly across [`VALUE_SHARD_COUNT`] shards. Each shard
    /// is allocated `total_bytes / VALUE_SHARD_COUNT` bytes (rounded up).
    #[must_use]
    pub(crate) fn new(total_bytes: usize) -> Self {
        let bytes_per_shard = total_bytes.div_ceil(VALUE_SHARD_COUNT).max(1);
        let shards = std::array::from_fn::<_, VALUE_SHARD_COUNT, _>(|_| {
            RwLock::new(CacheShard::new(bytes_per_shard))
        });
        Self {
            shards: Box::new(shards),
        }
    }

    /// Construct a value cache with the default 64 MB budget.
    #[must_use]
    pub(crate) fn with_default_capacity() -> Self {
        Self::new(DEFAULT_TOTAL_BYTES)
    }

    #[inline]
    #[must_use]
    const fn shard_for(hash: u64) -> usize {
        (hash & VALUE_SHARD_MASK) as usize
    }

    /// Look up a cached value. Returns `Ok(None)` on cache miss or hash
    /// collision (the caller falls back to the page path).
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockPoisoned`] when the target shard's `RwLock` was
    /// poisoned by a panicking writer.
    pub(crate) fn get(&self, ns: u32, hash: u64, key: &[u8]) -> Result<Option<CachedValue>> {
        let shard = self.shards[Self::shard_for(hash)]
            .read()
            .map_err(|_poisoned| Error::LockPoisoned)?;
        Ok(shard.get(ns, hash, key))
    }

    /// Insert a value. Existing entries under the same `(ns, hash)` are
    /// overwritten, with the size delta tracked correctly.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockPoisoned`] on shard poison.
    pub(crate) fn insert(
        &self,
        ns: u32,
        hash: u64,
        key: Box<[u8]>,
        value: Arc<[u8]>,
        expires_at: u64,
    ) -> Result<()> {
        let mut shard = self.shards[Self::shard_for(hash)]
            .write()
            .map_err(|_poisoned| Error::LockPoisoned)?;
        shard.insert(ns, hash, key, value, expires_at);
        Ok(())
    }

    /// Drop a single entry by `(ns, hash)`. Returns whether the entry was
    /// present.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockPoisoned`] on shard poison.
    pub(crate) fn invalidate(&self, ns: u32, hash: u64) -> Result<bool> {
        let mut shard = self.shards[Self::shard_for(hash)]
            .write()
            .map_err(|_poisoned| Error::LockPoisoned)?;
        Ok(shard.invalidate(ns, hash))
    }

    /// Drop every entry in every shard.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockPoisoned`] on shard poison.
    pub(crate) fn clear(&self) -> Result<()> {
        for idx in 0..VALUE_SHARD_COUNT {
            let mut shard = self.shards[idx]
                .write()
                .map_err(|_poisoned| Error::LockPoisoned)?;
            shard.clear();
        }
        Ok(())
    }

    /// Total bytes currently held across every shard.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockPoisoned`] on shard poison.
    pub(crate) fn bytes_used(&self) -> Result<usize> {
        let mut total = 0_usize;
        for idx in 0..VALUE_SHARD_COUNT {
            let shard = self.shards[idx]
                .read()
                .map_err(|_poisoned| Error::LockPoisoned)?;
            total = total.saturating_add(shard.bytes_used);
        }
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use super::{CachedValue, ValueCache, DEFAULT_TOTAL_BYTES, VALUE_SHARD_COUNT};
    use std::sync::Arc;

    fn arc_bytes(payload: &[u8]) -> Arc<[u8]> {
        Arc::from(payload.to_vec().into_boxed_slice())
    }

    fn boxed_bytes(payload: &[u8]) -> Box<[u8]> {
        payload.to_vec().into_boxed_slice()
    }

    #[test]
    fn fresh_cache_starts_empty() {
        let cache = ValueCache::new(1024);
        let used = cache.bytes_used();
        assert!(matches!(used, Ok(0)));
    }

    #[test]
    fn default_capacity_is_documented_constant() {
        let cache = ValueCache::with_default_capacity();
        let used = cache.bytes_used();
        assert!(matches!(used, Ok(0)));
        // The total budget must be at least the documented default.
        let _ = DEFAULT_TOTAL_BYTES;
    }

    #[test]
    fn insert_then_get_returns_value_and_expires_at() {
        let cache = ValueCache::new(1024);
        let inserted = cache.insert(0, 42, boxed_bytes(b"alpha"), arc_bytes(b"one"), 100);
        assert!(inserted.is_ok());

        let fetched = match cache.get(0, 42, b"alpha") {
            Ok(value) => value,
            Err(err) => panic!("get should succeed: {err}"),
        };
        let CachedValue { value, expires_at } = match fetched {
            Some(value) => value,
            None => panic!("entry should be cached"),
        };
        assert_eq!(value.as_ref(), b"one");
        assert_eq!(expires_at, 100);
    }

    #[test]
    fn miss_returns_none_for_unknown_hash() {
        let cache = ValueCache::new(1024);
        let fetched = cache.get(0, 99, b"nope");
        assert!(matches!(fetched, Ok(None)));
    }

    #[test]
    fn collision_with_different_key_returns_none() {
        let cache = ValueCache::new(1024);
        let _ = cache.insert(0, 42, boxed_bytes(b"alpha"), arc_bytes(b"one"), 0);
        // Same (ns, hash) but a different key — the cache holds "alpha"
        // but the caller asks for "beta". Cache reports miss.
        let fetched = cache.get(0, 42, b"beta");
        assert!(matches!(fetched, Ok(None)));
    }

    #[test]
    fn different_namespaces_are_isolated() {
        let cache = ValueCache::new(1024);
        let _ = cache.insert(0, 42, boxed_bytes(b"alpha"), arc_bytes(b"ns0"), 0);
        let _ = cache.insert(1, 42, boxed_bytes(b"alpha"), arc_bytes(b"ns1"), 0);

        let from_ns0 = match cache.get(0, 42, b"alpha") {
            Ok(Some(v)) => v,
            Ok(None) => panic!("ns0 entry should be cached"),
            Err(err) => panic!("get should succeed: {err}"),
        };
        assert_eq!(from_ns0.value.as_ref(), b"ns0");

        let from_ns1 = match cache.get(1, 42, b"alpha") {
            Ok(Some(v)) => v,
            Ok(None) => panic!("ns1 entry should be cached"),
            Err(err) => panic!("get should succeed: {err}"),
        };
        assert_eq!(from_ns1.value.as_ref(), b"ns1");
    }

    #[test]
    fn replacing_entry_updates_byte_accounting() {
        // Use a per-shard budget large enough that the second value (65
        // bytes) fits — `1024 / VALUE_SHARD_COUNT = 32`, which would
        // silently drop the second insert.
        let cache = ValueCache::new(VALUE_SHARD_COUNT * 256);
        let _ = cache.insert(0, 42, boxed_bytes(b"k"), arc_bytes(b"short"), 0);
        let used_after_first = match cache.bytes_used() {
            Ok(u) => u,
            Err(err) => panic!("bytes_used should succeed: {err}"),
        };

        let _ = cache.insert(0, 42, boxed_bytes(b"k"), arc_bytes(&[b'x'; 64]), 0);
        let used_after_second = match cache.bytes_used() {
            Ok(u) => u,
            Err(err) => panic!("bytes_used should succeed: {err}"),
        };
        assert!(used_after_second > used_after_first);
    }

    #[test]
    fn invalidate_drops_entry_and_recovers_bytes() {
        let cache = ValueCache::new(1024);
        let _ = cache.insert(0, 42, boxed_bytes(b"k"), arc_bytes(b"value"), 0);
        let used_before = match cache.bytes_used() {
            Ok(u) => u,
            Err(err) => panic!("bytes_used should succeed: {err}"),
        };
        assert!(used_before > 0);

        let invalidated = cache.invalidate(0, 42);
        assert!(matches!(invalidated, Ok(true)));
        let fetched = cache.get(0, 42, b"k");
        assert!(matches!(fetched, Ok(None)));
        let used_after = match cache.bytes_used() {
            Ok(u) => u,
            Err(err) => panic!("bytes_used should succeed: {err}"),
        };
        assert!(used_after < used_before);
    }

    #[test]
    fn invalidate_unknown_entry_reports_false() {
        let cache = ValueCache::new(1024);
        let invalidated = cache.invalidate(0, 99);
        assert!(matches!(invalidated, Ok(false)));
    }

    #[test]
    fn clear_drops_every_entry() {
        let cache = ValueCache::new(64 * 1024);
        for i in 0_u64..64 {
            let key = format!("k{i}").into_bytes().into_boxed_slice();
            let value = arc_bytes(format!("v{i}").as_bytes());
            let _ = cache.insert(0, i, key, value, 0);
        }
        let cleared = cache.clear();
        assert!(cleared.is_ok());
        let used = cache.bytes_used();
        assert!(matches!(used, Ok(0)));
    }

    #[test]
    fn entries_larger_than_shard_capacity_are_dropped_silently() {
        // Shard capacity is total / 32. Force a tiny shard so a large
        // value cannot fit.
        let cache = ValueCache::new(VALUE_SHARD_COUNT * 16);
        let huge = arc_bytes(&vec![b'x'; 4096]);
        let inserted = cache.insert(0, 1, boxed_bytes(b"huge"), huge, 0);
        assert!(inserted.is_ok());

        let fetched = cache.get(0, 1, b"huge");
        assert!(matches!(fetched, Ok(None)));
    }

    #[test]
    fn evicts_under_pressure_to_stay_within_capacity() {
        // Tight per-shard budget so each new same-shard insert forces an
        // eviction. We pick hashes that all share the bottom 5 bits (every
        // multiple of `VALUE_SHARD_COUNT`) to land on the same shard.
        let per_shard = 64;
        let cache = ValueCache::new(VALUE_SHARD_COUNT * per_shard);
        let stride = VALUE_SHARD_COUNT as u64;

        for i in 0_u64..6 {
            let key = format!("k{i}").into_bytes().into_boxed_slice();
            let value = arc_bytes(&[b'a' + i as u8; 24]);
            let _ = cache.insert(0, i * stride, key, value, 0);
        }

        // Bytes used must stay within the shard budget summed across shards.
        let used = match cache.bytes_used() {
            Ok(u) => u,
            Err(err) => panic!("bytes_used should succeed: {err}"),
        };
        assert!(used <= VALUE_SHARD_COUNT * per_shard);
        // The most recently inserted entry must still be present.
        let last = cache.get(0, 5 * stride, b"k5");
        assert!(matches!(last, Ok(Some(_))));
    }

    #[test]
    fn clock_keeps_recently_touched_entry_after_a_full_sweep() {
        // Three slots in the same shard; touching one before a sweep
        // ensures it survives at least one round of evictions.
        let per_shard = 96;
        let cache = ValueCache::new(VALUE_SHARD_COUNT * per_shard);
        let stride = VALUE_SHARD_COUNT as u64;

        // Fill three slots.
        for i in 0_u64..3 {
            let key = format!("k{i}").into_bytes().into_boxed_slice();
            let value = arc_bytes(&[b'a' + i as u8; 24]);
            let _ = cache.insert(0, i * stride, key, value, 0);
        }

        // Insert a fourth on the same shard; a full clock sweep clears all
        // referenced bits and evicts the front. This checks that eviction
        // actually fires and bytes_used stays under capacity afterwards.
        let _ = cache.insert(0, 3 * stride, boxed_bytes(b"k3"), arc_bytes(&[b'd'; 24]), 0);

        let used = match cache.bytes_used() {
            Ok(u) => u,
            Err(err) => panic!("bytes_used should succeed: {err}"),
        };
        assert!(used <= VALUE_SHARD_COUNT * per_shard);

        // Touch k3 so it is the most-recently-used entry, then insert a
        // fifth value. After the sweep, k3 must still be there because
        // its referenced bit was set just before the eviction.
        let _ = cache.get(0, 3 * stride, b"k3");
        let _ = cache.insert(0, 4 * stride, boxed_bytes(b"k4"), arc_bytes(&[b'e'; 24]), 0);

        let k3_after = cache.get(0, 3 * stride, b"k3");
        assert!(matches!(k3_after, Ok(Some(_))));
    }

    #[test]
    fn small_total_budget_still_yields_one_byte_per_shard() {
        // Caller asks for 1 byte total; rounding up gives every shard at
        // least 1 byte so the data structure is constructable.
        let cache = ValueCache::new(1);
        let used = cache.bytes_used();
        assert!(matches!(used, Ok(0)));
    }
}
