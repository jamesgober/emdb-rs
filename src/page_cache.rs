// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Shared, sharded page cache for the v0.7 storage engine.
//!
//! Sits between the in-memory keymap (`hash → Rid`) and the page file:
//! reads check the cache first and fall back to disk only on miss; writes
//! install the new (or updated) page into the cache so the next read does
//! not pay the disk cost.
//!
//! ## Capacity
//!
//! The cache is bounded in pages. Total capacity is split evenly across
//! [`CACHE_SHARD_COUNT`] independent shards. When inserting into a full
//! shard, the oldest-inserted entry is evicted (FIFO). Each entry tracks an
//! atomic access count so callers can later swap the eviction policy from
//! FIFO to true LFU without changing the public surface — only
//! [`CacheShard::pick_victim`] needs to switch from "front of queue" to
//! "lowest access count among current entries".
//!
//! ## Concurrency
//!
//! Each shard is guarded by its own [`RwLock`]. Cache hits — by far the
//! hot path of every read — take a read lock and run in parallel; the
//! `access_count` bump under that read lock is a relaxed atomic store, so
//! readers never block readers. Inserts and evictions take the write lock.
//! With 32 shards the contention floor is one in-flight write per shard.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

use crate::storage::page::{Page, PageId};
use crate::{Error, Result};

/// Number of independent cache shards. Power of two so the index calculation
/// is a single AND.
pub(crate) const CACHE_SHARD_COUNT: usize = 32;

const CACHE_SHARD_MASK: u64 = (CACHE_SHARD_COUNT as u64) - 1;

/// Default per-shard capacity when the user does not specify one. With
/// `CACHE_SHARD_COUNT * DEFAULT_PER_SHARD = 32 * 64 = 2 048` pages, default
/// total cache size is 8 MB at 4 KB pages — small enough to fit in any L3,
/// large enough to keep typical hot indexes cached.
pub(crate) const DEFAULT_CAPACITY_PER_SHARD: usize = 64;

#[derive(Debug)]
struct CacheEntry {
    page: Arc<Page>,
    access_count: AtomicU32,
}

#[derive(Debug)]
struct CacheShard {
    entries: HashMap<PageId, CacheEntry>,
    insert_order: VecDeque<PageId>,
    capacity: usize,
}

impl CacheShard {
    fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::with_capacity(capacity),
            insert_order: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    fn get(&self, page_id: PageId) -> Option<Arc<Page>> {
        let entry = self.entries.get(&page_id)?;
        let _previous = entry.access_count.fetch_add(1, Ordering::Relaxed);
        Some(Arc::clone(&entry.page))
    }

    fn insert(&mut self, page_id: PageId, page: Arc<Page>) {
        if let Some(existing) = self.entries.get_mut(&page_id) {
            existing.page = page;
            return;
        }

        if self.entries.len() >= self.capacity {
            self.evict_one();
        }

        let _previous = self.entries.insert(
            page_id,
            CacheEntry {
                page,
                access_count: AtomicU32::new(1),
            },
        );
        self.insert_order.push_back(page_id);
    }

    fn invalidate(&mut self, page_id: PageId) -> bool {
        if self.entries.remove(&page_id).is_none() {
            return false;
        }
        if let Some(pos) = self.insert_order.iter().position(|id| *id == page_id) {
            let _removed = self.insert_order.remove(pos);
        }
        true
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.insert_order.clear();
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn evict_one(&mut self) {
        // FIFO for now. Front of `insert_order` is the oldest entry. The
        // entry was inserted when the shard had capacity; capacity is fixed
        // for the lifetime of the cache, so we always have something to pop.
        while let Some(victim_id) = self.insert_order.pop_front() {
            if self.entries.remove(&victim_id).is_some() {
                return;
            }
            // Otherwise the entry was already invalidated; keep popping until
            // we find an entry that was actually evictable.
        }
    }
}

/// Shared, sharded page cache.
#[derive(Debug)]
pub(crate) struct PageCache {
    shards: Box<[RwLock<CacheShard>; CACHE_SHARD_COUNT]>,
}

impl PageCache {
    /// Construct a fresh page cache with `total_capacity` pages distributed
    /// evenly across [`CACHE_SHARD_COUNT`] shards.
    ///
    /// `total_capacity` is rounded up to the nearest multiple of the shard
    /// count so each shard gets the same number of slots.
    #[must_use]
    pub(crate) fn new(total_capacity: usize) -> Self {
        let per_shard = total_capacity.div_ceil(CACHE_SHARD_COUNT).max(1);
        let shards = std::array::from_fn::<_, CACHE_SHARD_COUNT, _>(|_| {
            RwLock::new(CacheShard::new(per_shard))
        });
        Self {
            shards: Box::new(shards),
        }
    }

    /// Construct a cache with the default per-shard capacity.
    #[must_use]
    pub(crate) fn with_default_capacity() -> Self {
        Self::new(CACHE_SHARD_COUNT * DEFAULT_CAPACITY_PER_SHARD)
    }

    #[inline]
    #[must_use]
    const fn shard_for(page_id: PageId) -> usize {
        // The hash is just the raw u64. Page ids in our format are dense
        // small integers, so a `& MASK` distributes them across shards just
        // as well as a multiplicative hash would, with one fewer instruction.
        (page_id.get() & CACHE_SHARD_MASK) as usize
    }

    /// Look up a page by id. Returns `Ok(None)` on miss.
    ///
    /// On hit, an atomic counter on the entry is bumped so the future LFU
    /// implementation can use it to pick eviction victims.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockPoisoned`] when the target shard's mutex was
    /// poisoned by a panicking writer.
    pub(crate) fn get(&self, page_id: PageId) -> Result<Option<Arc<Page>>> {
        let shard = self.shards[Self::shard_for(page_id)]
            .read()
            .map_err(|_poisoned| Error::LockPoisoned)?;
        Ok(shard.get(page_id))
    }

    /// Insert a page (or replace an existing entry under the same id).
    ///
    /// When the target shard is at capacity, the oldest-inserted entry is
    /// evicted to make room.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockPoisoned`] on shard poison.
    pub(crate) fn insert(&self, page_id: PageId, page: Arc<Page>) -> Result<()> {
        let mut shard = self.shards[Self::shard_for(page_id)]
            .write()
            .map_err(|_poisoned| Error::LockPoisoned)?;
        shard.insert(page_id, page);
        Ok(())
    }

    /// Drop a single entry by id. Returns whether the entry was present.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockPoisoned`] on shard poison.
    pub(crate) fn invalidate(&self, page_id: PageId) -> Result<bool> {
        let mut shard = self.shards[Self::shard_for(page_id)]
            .write()
            .map_err(|_poisoned| Error::LockPoisoned)?;
        Ok(shard.invalidate(page_id))
    }

    /// Drop every entry across every shard.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockPoisoned`] on shard poison.
    pub(crate) fn clear(&self) -> Result<()> {
        for idx in 0..CACHE_SHARD_COUNT {
            let mut shard = self.shards[idx]
                .write()
                .map_err(|_poisoned| Error::LockPoisoned)?;
            shard.clear();
        }
        Ok(())
    }

    /// Sum of cached entries across every shard. Snapshot only — concurrent
    /// inserts and evictions can change the result before the caller reads it.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockPoisoned`] on shard poison.
    pub(crate) fn len(&self) -> Result<usize> {
        let mut total = 0_usize;
        for idx in 0..CACHE_SHARD_COUNT {
            let shard = self.shards[idx]
                .read()
                .map_err(|_poisoned| Error::LockPoisoned)?;
            total = total.saturating_add(shard.len());
        }
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use super::{PageCache, CACHE_SHARD_COUNT, DEFAULT_CAPACITY_PER_SHARD};
    use crate::storage::page::{Page, PageHeader, PageId, PageType};
    use std::sync::Arc;

    fn make_page(seed: u8) -> Arc<Page> {
        let mut page = Page::new(PageHeader::new(PageType::LeafSlotted));
        page.as_mut_bytes()[64] = seed;
        Arc::new(page)
    }

    #[test]
    fn new_cache_starts_empty() {
        let cache = PageCache::new(64);
        let len = cache.len();
        assert!(matches!(len, Ok(0)));
    }

    #[test]
    fn default_capacity_distributes_across_shards() {
        let cache = PageCache::with_default_capacity();
        // Hit every shard at least once.
        for shard_idx in 0..CACHE_SHARD_COUNT as u64 {
            let _inserted = cache.insert(PageId::new(shard_idx), make_page(0));
        }
        let len = cache.len();
        assert!(matches!(len, Ok(n) if n == CACHE_SHARD_COUNT));
    }

    #[test]
    fn insert_then_get_returns_same_arc() {
        let cache = PageCache::new(64);
        let original = make_page(7);
        let id = PageId::new(1);
        let inserted = cache.insert(id, Arc::clone(&original));
        assert!(inserted.is_ok());

        let fetched = match cache.get(id) {
            Ok(value) => value,
            Err(err) => panic!("get should succeed: {err}"),
        };
        let fetched = match fetched {
            Some(value) => value,
            None => panic!("entry should be cached"),
        };
        // Same Arc allocation: pointer-equal.
        assert!(Arc::ptr_eq(&fetched, &original));
    }

    #[test]
    fn get_returns_none_for_unknown_id() {
        let cache = PageCache::new(64);
        let fetched = cache.get(PageId::new(99));
        assert!(matches!(fetched, Ok(None)));
    }

    #[test]
    fn insert_replaces_existing_entry() {
        let cache = PageCache::new(64);
        let id = PageId::new(1);
        let _ = cache.insert(id, make_page(1));
        let _ = cache.insert(id, make_page(2));

        let fetched = match cache.get(id) {
            Ok(Some(value)) => value,
            Ok(None) => panic!("entry should be cached"),
            Err(err) => panic!("get should succeed: {err}"),
        };
        // Page bytes for the second insert should win.
        assert_eq!(fetched.as_bytes()[64], 2);
        // And we did not double-count.
        let len = cache.len();
        assert!(matches!(len, Ok(1)));
    }

    #[test]
    fn invalidate_removes_only_target_entry() {
        let cache = PageCache::new(64);
        let _ = cache.insert(PageId::new(1), make_page(1));
        let _ = cache.insert(PageId::new(2), make_page(2));

        let invalidated = cache.invalidate(PageId::new(1));
        assert!(matches!(invalidated, Ok(true)));

        let fetched = cache.get(PageId::new(1));
        assert!(matches!(fetched, Ok(None)));
        let fetched = cache.get(PageId::new(2));
        assert!(matches!(fetched, Ok(Some(_))));
    }

    #[test]
    fn invalidate_unknown_id_reports_false() {
        let cache = PageCache::new(64);
        let invalidated = cache.invalidate(PageId::new(99));
        assert!(matches!(invalidated, Ok(false)));
    }

    #[test]
    fn clear_drops_every_entry() {
        let cache = PageCache::with_default_capacity();
        for i in 0..1024_u64 {
            let _ = cache.insert(PageId::new(i), make_page((i & 0xFF) as u8));
        }
        let cleared = cache.clear();
        assert!(cleared.is_ok());
        let len = cache.len();
        assert!(matches!(len, Ok(0)));
    }

    #[test]
    fn evicts_oldest_when_shard_is_full() {
        // One shard, capacity 4. Insert 5 page ids known to land on the same
        // shard (multiples of CACHE_SHARD_COUNT). The oldest inserted id
        // must be evicted to make room.
        let cache = PageCache::new(CACHE_SHARD_COUNT * 4);

        let stride = CACHE_SHARD_COUNT as u64; // every stride-th id lands on shard 0
        for i in 0..4_u64 {
            let _ = cache.insert(PageId::new(i * stride), make_page(i as u8));
        }
        // All four are present.
        for i in 0..4_u64 {
            let fetched = cache.get(PageId::new(i * stride));
            assert!(matches!(fetched, Ok(Some(_))));
        }

        // Insert a fifth on the same shard. The oldest (id 0) should evict.
        let _ = cache.insert(PageId::new(4 * stride), make_page(4));
        let fetched = cache.get(PageId::new(0));
        assert!(
            matches!(fetched, Ok(None)),
            "oldest entry should have evicted"
        );
        let fetched = cache.get(PageId::new(4 * stride));
        assert!(matches!(fetched, Ok(Some(_))));
    }

    #[test]
    fn invalidated_entries_do_not_block_eviction() {
        let cache = PageCache::new(CACHE_SHARD_COUNT * 4);
        let stride = CACHE_SHARD_COUNT as u64;
        for i in 0..4_u64 {
            let _ = cache.insert(PageId::new(i * stride), make_page(i as u8));
        }
        // Invalidate the oldest, then insert a fifth. The next victim should
        // be the now-oldest live entry, not a stale id.
        let _ = cache.invalidate(PageId::new(0));
        let _ = cache.insert(PageId::new(4 * stride), make_page(4));

        // id stride (originally inserted second) should now be the oldest
        // remaining and evict next.
        let _ = cache.insert(PageId::new(5 * stride), make_page(5));
        let fetched = cache.get(PageId::new(stride));
        assert!(matches!(fetched, Ok(None)));
    }

    #[test]
    fn capacity_below_shard_count_still_yields_one_slot_per_shard() {
        // Caller asks for total capacity 1; we round up to give each shard
        // at least one slot so the cache can hold one page per shard rather
        // than refusing inserts entirely.
        let cache = PageCache::new(1);
        for i in 0..CACHE_SHARD_COUNT as u64 {
            let _ = cache.insert(PageId::new(i), make_page(0));
        }
        let len = cache.len();
        assert!(matches!(len, Ok(n) if n == CACHE_SHARD_COUNT));
    }

    // Compile-time check that the default per-shard capacity stays positive.
    // The cache becomes useless if this drops to zero, so we surface the
    // error at build time rather than at test time.
    const _: () = {
        assert!(DEFAULT_CAPACITY_PER_SHARD > 0);
    };
}
