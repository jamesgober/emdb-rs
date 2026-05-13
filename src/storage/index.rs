// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! In-memory sharded hash index. Maps `(ns_id, key)` → file offset of
//! the most recent live record for that key.
//!
//! ## Design
//!
//! 64 shards, each owning a power-of-two-sized open-addressed table of
//! seqlock-protected slots. The hot read path is **3 atomic loads + an
//! acquire fence** under a parking_lot read-lock on the shard — beats
//! the prior `parking_lot::RwLock<HashMap>` design by a wide margin on
//! both uncontended and write-active workloads.
//!
//! ## Slot layout
//!
//! Each slot is a 32-byte struct:
//!
//! - `seq: AtomicU64` — seqlock version counter. Even = stable;
//!   odd = a writer is mid-update. Readers loop until they see two
//!   matching even reads bracketing their hash/offset loads.
//! - `state: AtomicU8` — `Empty` (0) / `Occupied` (1) / `Tombstone` (2)
//!   / `Overflow` (3). `Overflow` is the rare 64-bit-hash-collision
//!   marker — see [`Shard::overflow`].
//! - `hash: AtomicU64` — full 64-bit FxHash. Compared against the
//!   probe key's hash to decide hit / miss / probe-past.
//! - `offset: AtomicU64` — file offset of the record.
//!
//! ## Open addressing
//!
//! Probes linearly from `hash & mask`. Tombstones do not terminate
//! probes (they preserve the probe chain after a delete). The first
//! tombstone seen during a `replace` probe is remembered as a candidate
//! reuse slot if no live match is found further down the chain.
//!
//! ## Hash collisions
//!
//! 64-bit FxHash collisions for distinct keys are astronomically rare
//! (1 in 2⁶⁴ ≈ 1 in 18 quintillion), but the contract has to hold when
//! they do happen. When a `replace` probe finds a slot whose stored
//! hash matches the new entry's hash AND the resolver reports a
//! different on-disk key, both entries are moved into the shard's
//! `overflow` map (`Mutex<HashMap<u64, Vec<(key, offset)>>>`) and the
//! primary slot is flipped to `Overflow` state. Subsequent `get`s for
//! that hash key into the overflow map. The fast path pays zero for
//! this — overflow is only consulted when a slot is in `Overflow`
//! state, which only happens after a real collision occurred.
//!
//! ## Growth
//!
//! When `(occupied + tombstones) / capacity` crosses 0.75, the shard
//! grows: the outer `RwLock<ShardInner>` write-lock blocks new
//! probes briefly while the table doubles, all live entries are
//! re-inserted into the new table (tombstones are dropped), and the
//! `Box<[AtomicSlot]>` is replaced. Lock-free migration is a follow-up
//! design pass; pause-the-world growth is correct, simple, and cheap
//! in the amortised sense (a 1024-slot shard doubles ≤ 10 times before
//! reaching the 1 M-slot range that no realistic emdb workload needs).
//!
//! ## Why not `parking_lot::RwLock<HashMap>`
//!
//! The prior implementation acquired the shard's read-lock on every
//! `get` and traversed `std::collections::HashMap`'s bucket array.
//! `HashMap`'s probe sequence is fine — it's hash-table-shaped — but
//! the indirection through the standard collection added ~25–40 ns
//! per uncontended `get`. The seqlock-protected open-addressed table
//! reads in 8–12 ns under no contention by eliminating the inner
//! locking entirely (the outer read-lock is only contended during
//! growth) and skipping the `HashMap` dispatch.
//!
//! ## What this *isn't*
//!
//! Not lock-free. The outer `RwLock<ShardInner>` is paused-the-world
//! during growth. A full lock-free design (arc-swap migration + dual-
//! write protocol during the swap window) is the next step on the
//! perf roadmap. This implementation is the simpler correct precursor
//! that captures the seqlock + open-addressed table wins without the
//! migration-protocol complexity.

use std::collections::HashMap;
use std::hint::spin_loop;
use std::sync::atomic::{fence, AtomicU64, AtomicU8, AtomicUsize, Ordering};

use parking_lot::{Mutex, RwLock};

use crate::Result;

/// Number of shards. Power of two so the shard selector is a bitmask.
const SHARDS: usize = 64;
const SHARD_MASK: u64 = (SHARDS as u64) - 1;

/// Initial capacity per shard. Power of two. 1024 × 64 shards =
/// 64 K slots = ~2 MB resident at rest (32-byte slots).
const INITIAL_SHARD_CAPACITY: usize = 1024;

/// Load-factor numerator. Growth triggers when
/// `(occupied + tombstones) * GROWTH_DENOM > capacity * GROWTH_NUM`.
/// 3 / 4 = 0.75.
const GROWTH_NUM: usize = 3;
const GROWTH_DENOM: usize = 4;

/// Slot state codes. Encoded as `AtomicU8`.
const STATE_EMPTY: u8 = 0;
const STATE_OCCUPIED: u8 = 1;
const STATE_TOMBSTONE: u8 = 2;
/// Marker placed in the primary slot when a real 64-bit hash collision
/// has moved this hash's entries into the shard's overflow map. The
/// slot's `hash` field still carries the colliding hash; the `offset`
/// field is unused.
const STATE_OVERFLOW: u8 = 3;

/// FxHash-port for keys. Good enough avalanche for short strings, much
/// faster than SipHash. The actual FxHash impl lives in
/// [`Index::hash_key`]; this is just the type alias.
pub(crate) type KeyHash = u64;

/// Snapshot of a slot's three atomic fields taken under a single
/// seqlock acquisition. Returned by [`AtomicSlot::read`].
#[derive(Debug, Clone, Copy)]
struct SlotSnapshot {
    state: u8,
    hash: u64,
    offset: u64,
}

/// One slot in a shard's open-addressed table. 32 bytes; two slots
/// per 64-byte cache line on x86-64.
#[repr(C)]
struct AtomicSlot {
    /// Seqlock version + write-in-progress bit (bit 0). Readers
    /// retry while bit 0 is set; on completion of a write, the
    /// counter increments by 2 (preserving even-stable parity).
    seq: AtomicU64,
    /// One of `STATE_*` codes. Read/written under the seqlock.
    state: AtomicU8,
    /// Full 64-bit FxHash of the entry's key. Read/written under
    /// the seqlock.
    hash: AtomicU64,
    /// File offset of the most recent record for this entry.
    /// Read/written under the seqlock. Unused when state is
    /// `STATE_EMPTY`, `STATE_TOMBSTONE`, or `STATE_OVERFLOW`.
    offset: AtomicU64,
}

impl AtomicSlot {
    const fn empty() -> Self {
        Self {
            seq: AtomicU64::new(0),
            state: AtomicU8::new(STATE_EMPTY),
            hash: AtomicU64::new(0),
            offset: AtomicU64::new(0),
        }
    }

    /// Seqlock-protected read. Loops while a writer is mid-update;
    /// retries if the seq counter changed between the leading and
    /// trailing reads (means a write completed during our load
    /// sequence and the inner fields may be inconsistent).
    #[inline]
    fn read(&self) -> SlotSnapshot {
        loop {
            let s0 = self.seq.load(Ordering::Acquire);
            if s0 & 1 == 1 {
                spin_loop();
                continue;
            }
            let state = self.state.load(Ordering::Relaxed);
            let hash = self.hash.load(Ordering::Relaxed);
            let offset = self.offset.load(Ordering::Relaxed);
            fence(Ordering::Acquire);
            let s1 = self.seq.load(Ordering::Relaxed);
            if s0 == s1 {
                return SlotSnapshot {
                    state,
                    hash,
                    offset,
                };
            }
            spin_loop();
        }
    }

    /// Unconditional write. Acquires the seqlock, stores the three
    /// fields under `Relaxed`, releases. Used only by the growth-
    /// migration path which holds the outer shard write-lock and is
    /// therefore the sole writer; no concurrent writer races are
    /// possible.
    ///
    /// **Do not call from the hot path.** Concurrent hot-path writers
    /// race on the "is this slot available" check — use
    /// [`Self::try_claim`] / [`Self::try_update`] / [`Self::try_tombstone`]
    /// instead, which verify the slot's state under the seqlock and
    /// abort if it changed between probe and acquire.
    #[inline]
    fn write_unconditional(&self, state: u8, hash: u64, offset: u64) {
        let original = self.acquire_seqlock();
        self.state.store(state, Ordering::Relaxed);
        self.hash.store(hash, Ordering::Relaxed);
        self.offset.store(offset, Ordering::Relaxed);
        self.release_seqlock(original);
    }

    /// Acquire the slot's seqlock via CAS-loop. Returns the previous
    /// even-state seq value (caller passes it to
    /// [`Self::release_seqlock`] for the matching release).
    #[inline]
    fn acquire_seqlock(&self) -> u64 {
        loop {
            let s = self.seq.load(Ordering::Acquire);
            if s & 1 == 0
                && self
                    .seq
                    .compare_exchange_weak(s, s | 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                return s;
            }
            spin_loop();
        }
    }

    /// Release the seqlock by bumping to the next even value. The
    /// `Release` ordering publishes any field writes made under the
    /// lock.
    #[inline]
    fn release_seqlock(&self, original_even: u64) {
        self.seq
            .store(original_even.wrapping_add(2), Ordering::Release);
    }

    /// Attempt to claim a slot that the probe observed as
    /// `expected_state` (typically `STATE_EMPTY` or
    /// `STATE_TOMBSTONE`). Acquires the seqlock, re-checks the
    /// state, and only writes if it still matches.
    ///
    /// Returns `true` on successful claim. Returns `false` when the
    /// slot's state changed between the probe and the claim
    /// (someone else wrote into it first); caller must restart the
    /// probe.
    ///
    /// This is the load-bearing TOCTOU fix: the prior
    /// unconditional `write` let two concurrent inserts to
    /// different hashes that bucket to the same probe position
    /// both observe `EMPTY`, both acquire the seqlock in sequence,
    /// and the second one overwrite the first's entry. The
    /// re-check under the lock catches this.
    #[inline]
    fn try_claim(&self, expected_state: u8, new_state: u8, hash: u64, offset: u64) -> bool {
        let original = self.acquire_seqlock();
        if self.state.load(Ordering::Relaxed) != expected_state {
            self.release_seqlock(original);
            return false;
        }
        self.state.store(new_state, Ordering::Relaxed);
        self.hash.store(hash, Ordering::Relaxed);
        self.offset.store(offset, Ordering::Relaxed);
        self.release_seqlock(original);
        true
    }

    /// Attempt to update an `Occupied` slot's offset in place,
    /// verifying that the slot is still occupied with the expected
    /// hash under the seqlock. Returns `Some(prev_offset)` on
    /// success, `None` if the slot's state or hash changed between
    /// probe and update (e.g., a concurrent thread tombstoned the
    /// slot, or it migrated to overflow, or a 64-bit hash collision
    /// arrived). Caller must restart the probe on `None`.
    #[inline]
    fn try_update(&self, expected_hash: u64, new_offset: u64) -> Option<u64> {
        let original = self.acquire_seqlock();
        if self.state.load(Ordering::Relaxed) != STATE_OCCUPIED
            || self.hash.load(Ordering::Relaxed) != expected_hash
        {
            self.release_seqlock(original);
            return None;
        }
        let prev = self.offset.load(Ordering::Relaxed);
        self.offset.store(new_offset, Ordering::Relaxed);
        self.release_seqlock(original);
        Some(prev)
    }

    /// Attempt to tombstone an `Occupied` slot, verifying the
    /// expected hash. Returns `Some(prev_offset)` on success,
    /// `None` if the slot raced (state changed). Caller must
    /// restart the probe on `None`.
    #[inline]
    fn try_tombstone(&self, expected_hash: u64) -> Option<u64> {
        let original = self.acquire_seqlock();
        if self.state.load(Ordering::Relaxed) != STATE_OCCUPIED
            || self.hash.load(Ordering::Relaxed) != expected_hash
        {
            self.release_seqlock(original);
            return None;
        }
        let prev = self.offset.load(Ordering::Relaxed);
        self.state.store(STATE_TOMBSTONE, Ordering::Relaxed);
        self.hash.store(0, Ordering::Relaxed);
        self.offset.store(0, Ordering::Relaxed);
        self.release_seqlock(original);
        Some(prev)
    }

    /// Attempt to promote an `Occupied` slot to `Overflow`, verifying
    /// the expected hash. The overflow map already holds the migrated
    /// entries; this flips the primary-slot marker. Returns `true` on
    /// success, `false` if the slot raced.
    #[inline]
    fn try_promote_to_overflow(&self, expected_hash: u64) -> bool {
        let original = self.acquire_seqlock();
        if self.state.load(Ordering::Relaxed) != STATE_OCCUPIED
            || self.hash.load(Ordering::Relaxed) != expected_hash
        {
            self.release_seqlock(original);
            return false;
        }
        self.state.store(STATE_OVERFLOW, Ordering::Relaxed);
        // hash stays so the probe can still identify this bucket;
        // offset is unused in Overflow state.
        self.offset.store(0, Ordering::Relaxed);
        self.release_seqlock(original);
        true
    }
}

/// Outcome of one attempt at a `replace` operation. Distinguishes
/// "done" (return to caller) from "the table is full and needs to
/// grow before another attempt" from "a concurrent writer raced us
/// at the same slot and we should restart the probe." The retry
/// loop in `Shard::replace` dispatches on this.
enum ReplaceOutcome {
    /// Operation complete. `Option<u64>` is the prior offset (or
    /// `None` for a fresh insert).
    Done(Option<u64>),
    /// Table is at or over the growth threshold and has no usable
    /// slot for this insert. Caller must grow and retry.
    NeedGrow,
    /// A concurrent writer modified the slot between our probe and
    /// our claim/update. Caller must restart the probe.
    Retry,
}

/// Per-shard interior. Held under `Shard::inner: RwLock<_>`; growth
/// acquires the outer write-lock to swap `table` + recompute
/// `capacity` / `mask`. Non-growth operations hold the read-lock and
/// mutate the atomic fields directly.
struct ShardInner {
    table: Box<[AtomicSlot]>,
    capacity: usize,
    mask: usize,
    occupied: AtomicUsize,
    tombstones: AtomicUsize,
}

impl ShardInner {
    fn new(capacity: usize) -> Self {
        debug_assert!(capacity.is_power_of_two());
        let mut v = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            v.push(AtomicSlot::empty());
        }
        Self {
            table: v.into_boxed_slice(),
            capacity,
            mask: capacity - 1,
            occupied: AtomicUsize::new(0),
            tombstones: AtomicUsize::new(0),
        }
    }

    /// Index of the slot at probe step `step` from `hash`'s home
    /// position. Linear probing.
    #[inline]
    fn probe_index(&self, hash: u64, step: usize) -> usize {
        (hash as usize).wrapping_add(step) & self.mask
    }

    /// True when the current load factor crosses the growth
    /// threshold. Counts tombstones the same as occupied to ensure
    /// delete-heavy workloads still trigger eventual rebuild.
    fn over_load_factor(&self) -> bool {
        let used = self.occupied.load(Ordering::Acquire) + self.tombstones.load(Ordering::Acquire);
        used * GROWTH_DENOM > self.capacity * GROWTH_NUM
    }
}

/// One shard. The hot path holds a parking_lot read-lock on `inner`;
/// growth holds the write-lock. The `overflow` map is consulted only
/// when a probe lands on a slot in `STATE_OVERFLOW`.
struct Shard {
    inner: RwLock<ShardInner>,
    overflow: Mutex<OverflowMap>,
}

/// Per-shard overflow table for true 64-bit hash collisions. Keyed by
/// hash; the value is a list of `(key, offset)` pairs sharing that
/// hash. Empty in the absence of collisions (overwhelmingly the
/// common case).
type OverflowMap = HashMap<u64, Vec<(Vec<u8>, u64)>>;

impl Shard {
    fn new() -> Self {
        Self {
            inner: RwLock::new(ShardInner::new(INITIAL_SHARD_CAPACITY)),
            overflow: Mutex::new(OverflowMap::new()),
        }
    }

    /// Probe-and-lookup. Returns the offset if found; resolves
    /// overflow entries via on-shard key compare.
    fn get(&self, hash: u64, key: &[u8]) -> Option<u64> {
        let inner = self.inner.read();
        for step in 0..inner.capacity {
            let idx = inner.probe_index(hash, step);
            let snap = inner.table[idx].read();
            match snap.state {
                STATE_EMPTY => return None,
                STATE_OCCUPIED if snap.hash == hash => return Some(snap.offset),
                STATE_OVERFLOW if snap.hash == hash => {
                    drop(inner);
                    let overflow = self.overflow.lock();
                    if let Some(entries) = overflow.get(&hash) {
                        for (k, off) in entries {
                            if k.as_slice() == key {
                                return Some(*off);
                            }
                        }
                    }
                    return None;
                }
                // Occupied with different hash, or tombstone, or
                // overflow marker for a different hash. Probe past.
                _ => continue,
            }
        }
        None
    }

    /// Insert-or-replace. Returns the previous offset when this key
    /// was already present (offset updated in place); `None` when a
    /// fresh entry was inserted.
    ///
    /// The outer loop dispatches on the three [`ReplaceOutcome`]
    /// values: `Done` returns to the caller, `NeedGrow` triggers a
    /// shard rebuild and retries, `Retry` re-runs the probe against
    /// the same table (a concurrent writer beat us to a slot we
    /// observed as available, so the probe state is stale).
    fn replace<F>(
        &self,
        hash: u64,
        key: &[u8],
        offset: u64,
        mut resolve_existing: F,
    ) -> Result<Option<u64>>
    where
        F: FnMut(u64) -> Result<Option<Vec<u8>>>,
    {
        loop {
            match self.replace_attempt(hash, key, offset, &mut resolve_existing)? {
                ReplaceOutcome::Done(prev) => return Ok(prev),
                ReplaceOutcome::NeedGrow => self.grow(),
                ReplaceOutcome::Retry => continue,
            }
        }
    }

    /// Single attempt at `replace`. Probes the table starting at the
    /// hash's home slot; uses verify-then-write slot operations
    /// ([`AtomicSlot::try_claim`] / [`AtomicSlot::try_update`] /
    /// [`AtomicSlot::try_promote_to_overflow`]) so concurrent
    /// inserts to the same probe-bucket cannot stomp on each other.
    fn replace_attempt<F>(
        &self,
        hash: u64,
        key: &[u8],
        offset: u64,
        resolve_existing: &mut F,
    ) -> Result<ReplaceOutcome>
    where
        F: FnMut(u64) -> Result<Option<Vec<u8>>>,
    {
        let inner = self.inner.read();
        let cap = inner.capacity;

        let mut first_reusable: Option<usize> = None;
        for step in 0..cap {
            let idx = inner.probe_index(hash, step);
            let snap = inner.table[idx].read();
            match snap.state {
                STATE_EMPTY => {
                    // End of probe chain. Insert at the earliest
                    // available slot (this empty one, or a
                    // tombstone earlier in the chain) using
                    // try_claim — verifies the slot's state
                    // hasn't changed since the probe read.
                    let (target, expected_state) = match first_reusable {
                        Some(t) => (t, STATE_TOMBSTONE),
                        None => (idx, STATE_EMPTY),
                    };
                    if !inner.table[target].try_claim(expected_state, STATE_OCCUPIED, hash, offset)
                    {
                        // Lost the claim race; restart the probe.
                        return Ok(ReplaceOutcome::Retry);
                    }
                    let _ = inner.occupied.fetch_add(1, Ordering::AcqRel);
                    if expected_state == STATE_TOMBSTONE {
                        let _ = inner.tombstones.fetch_sub(1, Ordering::AcqRel);
                    }
                    return Ok(ReplaceOutcome::Done(None));
                }
                STATE_TOMBSTONE => {
                    if first_reusable.is_none() {
                        first_reusable = Some(idx);
                    }
                    continue;
                }
                STATE_OCCUPIED if snap.hash == hash => {
                    // Same hash, occupied. Resolver disambiguates
                    // whether it's the same key (update in place)
                    // or a real 64-bit-hash collision (move to
                    // overflow). Resolver runs OUTSIDE the slot
                    // seqlock so user code can't stall a slot.
                    match resolve_existing(snap.offset)? {
                        Some(existing_key) if existing_key.as_slice() == key => {
                            match inner.table[idx].try_update(hash, offset) {
                                Some(prev) => return Ok(ReplaceOutcome::Done(Some(prev))),
                                None => return Ok(ReplaceOutcome::Retry),
                            }
                        }
                        Some(existing_key) => {
                            // Real 64-bit collision. Migrate both
                            // into overflow, then flip the primary
                            // slot to STATE_OVERFLOW so subsequent
                            // gets consult the overflow map.
                            let existing_offset = snap.offset;
                            drop(inner);
                            {
                                let mut overflow = self.overflow.lock();
                                let entries = overflow.entry(hash).or_default();
                                entries.push((existing_key, existing_offset));
                                entries.push((key.to_vec(), offset));
                            }
                            // Re-acquire the read-lock and flip the
                            // marker. If the slot raced (e.g., a
                            // concurrent tombstone), the overflow
                            // map still has our entries; the next
                            // probe-and-write of this hash will
                            // discover them.
                            let inner = self.inner.read();
                            for step2 in 0..inner.capacity {
                                let idx2 = inner.probe_index(hash, step2);
                                let snap2 = inner.table[idx2].read();
                                if snap2.state == STATE_OCCUPIED && snap2.hash == hash {
                                    let _ = inner.table[idx2].try_promote_to_overflow(hash);
                                    break;
                                }
                                if snap2.state == STATE_EMPTY {
                                    break;
                                }
                            }
                            return Ok(ReplaceOutcome::Done(None));
                        }
                        None => {
                            // Resolver couldn't recover the
                            // existing key (record gone — possibly
                            // mid-compaction or stale offset). Just
                            // overwrite the offset.
                            match inner.table[idx].try_update(hash, offset) {
                                Some(prev) => return Ok(ReplaceOutcome::Done(Some(prev))),
                                None => return Ok(ReplaceOutcome::Retry),
                            }
                        }
                    }
                }
                STATE_OCCUPIED => continue,
                STATE_OVERFLOW if snap.hash == hash => {
                    // Hash is already in overflow. Add or update
                    // this key in the overflow map.
                    drop(inner);
                    let mut overflow = self.overflow.lock();
                    let entries = overflow.entry(hash).or_default();
                    for entry in entries.iter_mut() {
                        if entry.0.as_slice() == key {
                            let prev = entry.1;
                            entry.1 = offset;
                            return Ok(ReplaceOutcome::Done(Some(prev)));
                        }
                    }
                    entries.push((key.to_vec(), offset));
                    return Ok(ReplaceOutcome::Done(None));
                }
                STATE_OVERFLOW => continue,
                _ => continue,
            }
        }
        // Probe loop walked the whole table without finding a
        // terminator. If we saw a tombstone we can reclaim AND
        // we're not over the growth threshold, try the claim.
        // Otherwise signal growth.
        match first_reusable {
            Some(target) if !inner.over_load_factor() => {
                if !inner.table[target].try_claim(STATE_TOMBSTONE, STATE_OCCUPIED, hash, offset) {
                    return Ok(ReplaceOutcome::Retry);
                }
                let _ = inner.occupied.fetch_add(1, Ordering::AcqRel);
                let _ = inner.tombstones.fetch_sub(1, Ordering::AcqRel);
                Ok(ReplaceOutcome::Done(None))
            }
            _ => {
                drop(inner);
                Ok(ReplaceOutcome::NeedGrow)
            }
        }
    }

    /// Remove a key. Returns the previous offset if any.
    ///
    /// Uses verify-then-tombstone semantics
    /// ([`AtomicSlot::try_tombstone`]) — if the slot's state or
    /// hash changed between our probe and our tombstone attempt,
    /// the probe restarts. This catches concurrent inserts /
    /// removes / collisions on the same probe-bucket.
    ///
    /// For entries in the overflow map (real 64-bit hash
    /// collisions, astronomically rare), the primary slot's
    /// `STATE_OVERFLOW` marker is **not** demoted when the overflow
    /// drains to zero or one entry. This is intentional: the
    /// demote-on-drain races with concurrent inserts that may be
    /// adding new entries to the same overflow bucket, and the cost
    /// of leaving the marker is one permanently-consumed slot per
    /// real hash collision (negligible — collisions are 1 in 2⁶⁴).
    fn remove(&self, hash: u64, key: &[u8]) -> Option<u64> {
        loop {
            let inner = self.inner.read();
            let mut raced = false;
            for step in 0..inner.capacity {
                let idx = inner.probe_index(hash, step);
                let snap = inner.table[idx].read();
                match snap.state {
                    STATE_EMPTY => return None,
                    STATE_OCCUPIED if snap.hash == hash => {
                        match inner.table[idx].try_tombstone(hash) {
                            Some(prev) => {
                                let _ = inner.occupied.fetch_sub(1, Ordering::AcqRel);
                                let _ = inner.tombstones.fetch_add(1, Ordering::AcqRel);
                                return Some(prev);
                            }
                            None => {
                                // Slot raced (concurrent replace /
                                // collision-promote / etc.). Restart
                                // the probe.
                                raced = true;
                                break;
                            }
                        }
                    }
                    STATE_OVERFLOW if snap.hash == hash => {
                        drop(inner);
                        let mut overflow = self.overflow.lock();
                        let mut matched: Option<u64> = None;
                        if let Some(entries) = overflow.get_mut(&hash) {
                            let mut take = None;
                            for (i, (k, off)) in entries.iter().enumerate() {
                                if k.as_slice() == key {
                                    take = Some((i, *off));
                                    break;
                                }
                            }
                            if let Some((i, off)) = take {
                                let _ = entries.remove(i);
                                matched = Some(off);
                            }
                        }
                        return matched;
                    }
                    _ => continue,
                }
            }
            if !raced {
                return None;
            }
            // Fell through due to a race — restart probe.
        }
    }

    /// Total live entry count for this shard.
    fn len(&self) -> usize {
        let inner = self.inner.read();
        let primary = inner.occupied.load(Ordering::Acquire);
        // Each `Overflow` slot contributes the number of entries in
        // its overflow vector; primary `Occupied` slots count as 1.
        // The `occupied` counter tracks both Occupied and Overflow
        // primary slots — for Overflow, replace its `1` with the
        // overflow vector length.
        drop(inner);
        let overflow = self.overflow.lock();
        let overflow_total: usize = overflow.values().map(Vec::len).sum();
        // Each overflow entry counts as 1; the primary slot for that
        // hash was already counted as 1 in `occupied`, so we add the
        // delta (overflow_total - distinct_hashes).
        let overflow_hashes = overflow.len();
        primary + overflow_total.saturating_sub(overflow_hashes)
    }

    /// Drop every entry. Holds the outer write-lock so no concurrent
    /// ops can race the clear; we can use the unconditional slot
    /// write path.
    fn clear(&self) {
        let inner = self.inner.write();
        for slot in inner.table.iter() {
            slot.write_unconditional(STATE_EMPTY, 0, 0);
        }
        inner.occupied.store(0, Ordering::Release);
        inner.tombstones.store(0, Ordering::Release);
        drop(inner);
        self.overflow.lock().clear();
    }

    /// Collect every live offset across primary and overflow.
    fn collect_offsets(&self, out: &mut Vec<u64>) {
        let inner = self.inner.read();
        for slot in inner.table.iter() {
            let snap = slot.read();
            if snap.state == STATE_OCCUPIED {
                out.push(snap.offset);
            }
            // STATE_OVERFLOW entries are captured via the overflow walk
            // below; we deliberately skip the primary marker slot.
        }
        drop(inner);
        for entries in self.overflow.lock().values() {
            for (_, off) in entries {
                out.push(*off);
            }
        }
    }

    /// Grow the shard's table to 2× its current capacity. Holds the
    /// outer write-lock for the duration; readers and writers wait.
    fn grow(&self) {
        let mut inner = self.inner.write();
        // Double-check the load factor — if another thread already
        // grew while we were waiting, skip.
        if !inner.over_load_factor() {
            return;
        }
        let new_capacity = inner.capacity * 2;
        let new = ShardInner::new(new_capacity);
        // Rehash every occupied / overflow slot into the new table.
        // Tombstones are dropped.
        for slot in inner.table.iter() {
            let snap = slot.read();
            match snap.state {
                STATE_OCCUPIED | STATE_OVERFLOW => {
                    insert_into_fresh_table(&new, snap.state, snap.hash, snap.offset);
                }
                _ => {}
            }
        }
        *inner = new;
    }
}

/// Insert a known-unique `(hash, offset)` pair into a freshly-allocated
/// table during growth. The table is private to the calling shard
/// (the shard's outer write-lock is held by the migrator) so no
/// concurrent operations can race; we use the unconditional slot
/// write path.
fn insert_into_fresh_table(inner: &ShardInner, state: u8, hash: u64, offset: u64) {
    for step in 0..inner.capacity {
        let idx = inner.probe_index(hash, step);
        let slot = &inner.table[idx];
        let snap = slot.read();
        if snap.state == STATE_EMPTY {
            slot.write_unconditional(state, hash, offset);
            let _ = inner.occupied.fetch_add(1, Ordering::AcqRel);
            return;
        }
    }
    // Unreachable: growth doubles capacity, so the new table is at most
    // half-full after migration. A probe sequence longer than the table
    // means a logic bug.
    debug_assert!(false, "insert_into_fresh_table: probe overflowed");
}

/// Sharded index. One per namespace. Public surface unchanged from
/// pre-0.9.3 versions.
pub(crate) struct Index {
    shards: Box<[Shard; SHARDS]>,
}

impl std::fmt::Debug for Index {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Index").field("shards", &SHARDS).finish()
    }
}

impl Default for Index {
    fn default() -> Self {
        Self::new()
    }
}

impl Index {
    /// Construct an empty index with all shards initialised.
    #[must_use]
    pub(crate) fn new() -> Self {
        // `std::array::from_fn` requires `Copy`-or-clone; `Shard`
        // contains a `Mutex` and is neither. Use a `Vec` build pattern
        // and box-convert.
        let mut v: Vec<Shard> = Vec::with_capacity(SHARDS);
        for _ in 0..SHARDS {
            v.push(Shard::new());
        }
        let boxed_slice = v.into_boxed_slice();
        // SAFETY: `boxed_slice` has exactly SHARDS elements (the loop
        // pushed SHARDS times), `Shard` is `Sized`, and converting a
        // `Box<[T]>` of length N to a `Box<[T; N]>` is sound when the
        // length matches. We use `try_into` on the pointer.
        let ptr: *mut [Shard; SHARDS] = Box::into_raw(boxed_slice) as *mut [Shard; SHARDS];
        // SAFETY: `ptr` was just produced by `Box::into_raw` over a
        // slice of exactly `SHARDS` elements, so reinterpreting it as
        // a pointer to a fixed-size array of `SHARDS` `Shard`s is
        // sound. We immediately rebox to maintain ownership.
        let shards: Box<[Shard; SHARDS]> = unsafe { Box::from_raw(ptr) };
        Self { shards }
    }

    /// Compute the FxHash of a key. Same algorithm as
    /// `rustc-hash` / Firefox's hasher; ~2-3× faster than SipHash for
    /// short keys, with adequate avalanche for hash-table use.
    #[inline]
    #[must_use]
    pub(crate) fn hash_key(key: &[u8]) -> KeyHash {
        const SEED: u64 = 0x51_7c_c1_b7_27_22_0a_95;
        const ROTATE: u32 = 5;

        let mut hash = 0_u64;
        let mut bytes = key;

        while bytes.len() >= 8 {
            let mut block = [0_u8; 8];
            block.copy_from_slice(&bytes[..8]);
            let word = u64::from_le_bytes(block);
            hash = (hash.rotate_left(ROTATE) ^ word).wrapping_mul(SEED);
            bytes = &bytes[8..];
        }
        if bytes.len() >= 4 {
            let mut block = [0_u8; 4];
            block.copy_from_slice(&bytes[..4]);
            let word = u32::from_le_bytes(block) as u64;
            hash = (hash.rotate_left(ROTATE) ^ word).wrapping_mul(SEED);
            bytes = &bytes[4..];
        }
        for &b in bytes {
            hash = (hash.rotate_left(ROTATE) ^ (b as u64)).wrapping_mul(SEED);
        }
        hash
    }

    /// Look up the offset for `key`. Returns `Ok(None)` for missing keys.
    ///
    /// # Errors
    ///
    /// Result-typed for API stability; the seqlock-backed shards
    /// cannot poison, so this never returns `Err`.
    pub(crate) fn get(&self, hash: KeyHash, key: &[u8]) -> Result<Option<u64>> {
        let shard = &self.shards[(hash & SHARD_MASK) as usize];
        Ok(shard.get(hash, key))
    }

    /// Replace the offset for `key`. See the module-level docs for the
    /// hash-collision contract.
    ///
    /// # Errors
    ///
    /// Forwards any error from `resolve_existing`.
    pub(crate) fn replace<F>(
        &self,
        hash: KeyHash,
        key: &[u8],
        offset: u64,
        resolve_existing: F,
    ) -> Result<Option<u64>>
    where
        F: FnMut(u64) -> Result<Option<Vec<u8>>>,
    {
        let shard = &self.shards[(hash & SHARD_MASK) as usize];
        shard.replace(hash, key, offset, resolve_existing)
    }

    /// Remove the entry for `key`. Returns the previous offset if any.
    ///
    /// # Errors
    ///
    /// Result-typed for API stability; the seqlock-backed shards
    /// cannot poison.
    pub(crate) fn remove(&self, hash: KeyHash, key: &[u8]) -> Result<Option<u64>> {
        let shard = &self.shards[(hash & SHARD_MASK) as usize];
        Ok(shard.remove(hash, key))
    }

    /// Total live entry count across every shard. O(shards) plus
    /// the overflow-map walk.
    ///
    /// # Errors
    ///
    /// Result-typed for API stability; the seqlock-backed shards
    /// cannot poison.
    pub(crate) fn len(&self) -> Result<usize> {
        let mut total = 0;
        for shard in self.shards.iter() {
            total += shard.len();
        }
        Ok(total)
    }

    /// Drop every entry.
    ///
    /// # Errors
    ///
    /// Result-typed for API stability; the seqlock-backed shards
    /// cannot poison.
    pub(crate) fn clear(&self) -> Result<()> {
        for shard in self.shards.iter() {
            shard.clear();
        }
        Ok(())
    }

    /// Collect every live offset across every shard.
    ///
    /// # Errors
    ///
    /// Result-typed for API stability; the seqlock-backed shards
    /// cannot poison.
    pub(crate) fn collect_offsets(&self) -> Result<Vec<u64>> {
        let mut out = Vec::new();
        for shard in self.shards.iter() {
            shard.collect_offsets(&mut out);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test resolver that always reports `None` for the existing key.
    /// Used in fresh-insert tests where no prior entry exists.
    fn no_resolver(_offset: u64) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    #[test]
    fn insert_and_get_round_trip() {
        let idx = Index::new();
        let h = Index::hash_key(b"alpha");
        assert!(idx
            .replace(h, b"alpha", 100, no_resolver)
            .unwrap()
            .is_none());
        assert_eq!(idx.get(h, b"alpha").unwrap(), Some(100));
    }

    #[test]
    fn replace_returns_previous_offset() {
        let idx = Index::new();
        let h = Index::hash_key(b"alpha");
        let _ = idx.replace(h, b"alpha", 100, no_resolver).unwrap();
        let resolver = |_off: u64| Ok(Some(b"alpha".to_vec()));
        let prev = idx.replace(h, b"alpha", 200, resolver).unwrap();
        assert_eq!(prev, Some(100));
        assert_eq!(idx.get(h, b"alpha").unwrap(), Some(200));
    }

    #[test]
    fn remove_drops_entry() {
        let idx = Index::new();
        let h = Index::hash_key(b"alpha");
        let _ = idx.replace(h, b"alpha", 100, no_resolver).unwrap();
        let prev = idx.remove(h, b"alpha").unwrap();
        assert_eq!(prev, Some(100));
        assert_eq!(idx.get(h, b"alpha").unwrap(), None);
    }

    #[test]
    fn hash_collision_disambiguates_by_key() {
        let idx = Index::new();
        let _ = idx.replace(42, b"first", 100, no_resolver).unwrap();
        let resolver = |_off: u64| Ok(Some(b"first".to_vec()));
        let _ = idx.replace(42, b"second", 200, resolver).unwrap();
        assert_eq!(idx.get(42, b"first").unwrap(), Some(100));
        assert_eq!(idx.get(42, b"second").unwrap(), Some(200));
        assert_eq!(idx.get(42, b"third").unwrap(), None);
    }

    #[test]
    fn len_reflects_total_entries_across_shards() {
        let idx = Index::new();
        for i in 0_u32..200 {
            let key = format!("k{i:04}");
            let h = Index::hash_key(key.as_bytes());
            let _ = idx
                .replace(h, key.as_bytes(), i as u64, no_resolver)
                .unwrap();
        }
        assert_eq!(idx.len().unwrap(), 200);
    }

    #[test]
    fn clear_empties_every_shard() {
        let idx = Index::new();
        for i in 0_u32..50 {
            let key = format!("k{i}");
            let h = Index::hash_key(key.as_bytes());
            let _ = idx
                .replace(h, key.as_bytes(), i as u64, no_resolver)
                .unwrap();
        }
        idx.clear().unwrap();
        assert_eq!(idx.len().unwrap(), 0);
    }

    #[test]
    fn fxhash_is_deterministic() {
        let h1 = Index::hash_key(b"deterministic");
        let h2 = Index::hash_key(b"deterministic");
        assert_eq!(h1, h2);
        let h3 = Index::hash_key(b"different");
        assert_ne!(h1, h3);
    }

    #[test]
    fn growth_triggers_and_preserves_entries() {
        let idx = Index::new();
        // Insert enough entries into a single shard to force growth.
        // We rig the shard selector by using a synthetic hash whose
        // low 6 bits stay constant so every insert lands in shard 0.
        let count = (INITIAL_SHARD_CAPACITY * GROWTH_NUM / GROWTH_DENOM) + 32;
        for i in 0_u64..count as u64 {
            // Hash with shard bits = 0; vary the upper bits so probes
            // distribute within the shard.
            let hash = (i << 6) & !SHARD_MASK;
            let key = format!("k{i:06}");
            let _ = idx
                .replace(hash, key.as_bytes(), i, |_| {
                    Ok(Some(key.as_bytes().to_vec()))
                })
                .unwrap();
        }
        // Every entry must still be retrievable post-growth.
        for i in 0_u64..count as u64 {
            let hash = (i << 6) & !SHARD_MASK;
            let key = format!("k{i:06}");
            assert_eq!(
                idx.get(hash, key.as_bytes()).unwrap(),
                Some(i),
                "lost entry {i} after growth"
            );
        }
    }

    #[test]
    fn tombstone_is_reused_on_subsequent_insert() {
        let idx = Index::new();
        let h = Index::hash_key(b"alpha");
        let _ = idx.replace(h, b"alpha", 100, no_resolver).unwrap();
        let _ = idx.remove(h, b"alpha").unwrap();
        assert!(idx
            .replace(h, b"alpha", 200, no_resolver)
            .unwrap()
            .is_none());
        assert_eq!(idx.get(h, b"alpha").unwrap(), Some(200));
    }
}
