// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! In-memory sharded hash index. Maps `(ns_id, key)` to the file
//! offset of the most recent live record for that key.
//!
//! ## Design
//!
//! 64 shards, each owning a power-of-two-sized open-addressed table of
//! seqlock-protected slots.
//!
//! - **Readers are lock-free.** A lookup pins the current
//!   [`crossbeam_epoch`] epoch, loads the shard's table pointer and
//!   probes slots with seqlock reads. It performs no read-modify-write
//!   on any shared cache line, so concurrent readers do not contend
//!   with each other.
//! - **Writers are serialised per shard.** Every mutation of a shard
//!   (insert, overwrite, remove, growth, clear) holds that shard's
//!   writer mutex. With a single writer per shard there is no
//!   claim-then-verify race between writers: a key can never land in
//!   two slots, and the collision overflow map can never lose or
//!   duplicate an entry. The engine additionally holds a per-key
//!   stripe lock across the journal append and the index update, so
//!   the order of records in the log matches the order of index
//!   updates for every key.
//!
//! ## Slot layout and the seqlock protocol
//!
//! Each slot holds `seq`, `state`, `hash` and `offset` atomics. `seq`
//! is even when the slot is stable and odd while the writer is
//! updating it.
//!
//! - Writer: `seq = odd` (relaxed), `fence(Release)`, field stores
//!   (relaxed), `seq = even` (release).
//! - Reader: `s0 = seq` (acquire), field loads (relaxed),
//!   `fence(Acquire)`, `s1 = seq` (relaxed); retry unless
//!   `s0 == s1` and `s0` is even.
//!
//! If the reader observes any field store of an update, the
//! release/acquire fence pair makes the writer's odd `seq` store
//! visible to the `s1` load, so the snapshot is rejected. Both fences
//! are required: without them a weak-memory target (ARM, POWER) can
//! return a torn snapshot. The protocol is checked by a `loom` model
//! (`cargo test --lib loom_` with `RUSTFLAGS="--cfg loom"`) and by Miri.
//!
//! ## Open addressing
//!
//! The shard is selected by the low 6 bits of the hash; the home slot
//! inside the shard comes from the remaining bits (`hash >> 6`), so
//! every slot of the table can be a home slot. Probing is linear.
//! Tombstones do not terminate probes; the first tombstone seen on a
//! fresh insert is reused.
//!
//! ## Growth
//!
//! The table is rebuilt before an insert would push
//! `(occupied + tombstones)` past 75% of capacity. The rebuild doubles
//! the capacity when live entries exceed half of it, and otherwise
//! rehashes at the same size to purge tombstones. The new table is
//! published with one pointer swap; readers that still hold the old
//! table finish their probe against it (no writer touches it any
//! more) and the old table is freed once every such reader has
//! unpinned.
//!
//! ## Hashing and collisions
//!
//! Keys are hashed with [`KeyHasher`], a folded-multiply hash keyed by
//! per-instance random secrets, so colliding key sets cannot be
//! computed offline. The index is never persisted; the hash is only
//! an in-memory placement function and a fresh secret is drawn on
//! every open.
//!
//! When two distinct keys do share a 64-bit hash, both entries move
//! into the shard's `overflow` map and the primary slot is marked
//! `Overflow`. Lookups of that hash consult the map under its read
//! lock; nothing else ever touches it.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_epoch::{self as epoch, Atomic, Guard, Owned};
use crossbeam_utils::CachePadded;
use parking_lot::{Mutex, RwLock};

use crate::Result;

/// Atomics used by the seqlock slots. Swapped for `loom`'s model
/// types when the crate's unit tests run under `--cfg loom`.
mod sync {
    #[cfg(all(test, loom))]
    pub(super) use loom::sync::atomic::{fence, AtomicU64, AtomicU8};
    #[cfg(not(all(test, loom)))]
    pub(super) use std::sync::atomic::{fence, AtomicU64, AtomicU8};

    /// Back-off inside a seqlock retry loop.
    #[inline]
    pub(super) fn spin() {
        #[cfg(all(test, loom))]
        loom::thread::yield_now();
        #[cfg(not(all(test, loom)))]
        std::hint::spin_loop();
    }
}

use sync::{fence, AtomicU64, AtomicU8};

/// Number of shards. Power of two so the shard selector is a bitmask.
const SHARDS: usize = 64;
/// Bits of the hash consumed by shard selection.
const SHARD_BITS: u32 = 6;
const SHARD_MASK: u64 = (SHARDS as u64) - 1;

/// Initial capacity per shard. Power of two. 16 x 64 shards = 1 K
/// slots (32 KiB) per namespace; tables double on demand, so a large
/// namespace pays a few early rebuilds instead of every namespace
/// paying 2 MiB up front.
const INITIAL_SHARD_CAPACITY: usize = 16;

/// A table is rebuilt before `(occupied + tombstones)` would exceed
/// `capacity * GROWTH_NUM / GROWTH_DENOM` (0.75).
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

/// 64-bit hash of a key, as produced by [`KeyHasher::hash`].
pub(crate) type KeyHash = u64;

/// Keyed hash for index placement.
///
/// The construction is a folded 64x64->128-bit multiply over 16-byte
/// blocks with every input word XORed with a per-instance secret (the
/// same shape as `foldhash`/`ahash`'s fallback). Without the secrets,
/// the high half of each product is unpredictable, so an attacker
/// cannot build colliding key sets offline the way the unkeyed
/// pre-1.0.3 mixer allowed. It is not a cryptographic MAC.
///
/// Secrets come from [`std::collections::hash_map::RandomState`],
/// which seeds from the operating system's RNG.
#[derive(Clone, Copy)]
pub(crate) struct KeyHasher {
    k0: u64,
    k1: u64,
    k2: u64,
}

impl std::fmt::Debug for KeyHasher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The secrets are deliberately not printed.
        f.debug_struct("KeyHasher").finish_non_exhaustive()
    }
}

/// Multiply as 128 bits and fold the halves together.
#[inline(always)]
fn folded_mul(a: u64, b: u64) -> u64 {
    let product = u128::from(a).wrapping_mul(u128::from(b));
    (product as u64) ^ ((product >> 64) as u64)
}

#[inline(always)]
fn read_u64_le(bytes: &[u8]) -> u64 {
    let mut word = [0_u8; 8];
    word.copy_from_slice(&bytes[..8]);
    u64::from_le_bytes(word)
}

#[inline(always)]
fn read_u32_le(bytes: &[u8]) -> u64 {
    let mut word = [0_u8; 4];
    word.copy_from_slice(&bytes[..4]);
    u64::from(u32::from_le_bytes(word))
}

impl KeyHasher {
    /// Constants from the PCG / wyhash literature; any odd constants
    /// with good bit dispersion work here.
    const C0: u64 = 0x243f_6a88_85a3_08d3;
    const C1: u64 = 0x5851_f42d_4c95_7f2d;

    /// Draw fresh secrets from the OS-seeded `RandomState`.
    #[must_use]
    pub(crate) fn random() -> Self {
        use std::hash::BuildHasher;
        let state = std::collections::hash_map::RandomState::new();
        Self {
            k0: state.hash_one(0x6b30_u64),
            k1: state.hash_one(0x6b31_u64) | 1,
            k2: state.hash_one(0x6b32_u64) | 1,
        }
    }

    /// Fixed secrets for deterministic unit tests.
    #[cfg(all(test, not(loom)))]
    #[must_use]
    pub(crate) const fn with_secrets(k0: u64, k1: u64, k2: u64) -> Self {
        Self { k0, k1, k2 }
    }

    /// Hash `key`.
    #[inline]
    #[must_use]
    pub(crate) fn hash(&self, key: &[u8]) -> KeyHash {
        let len = key.len();
        let mut state = self.k0 ^ (len as u64).wrapping_mul(Self::C0);
        let mut bytes = key;
        while bytes.len() > 16 {
            let a = read_u64_le(bytes);
            let b = read_u64_le(&bytes[8..]);
            state = folded_mul(a ^ self.k1, b ^ self.k2 ^ state);
            bytes = &bytes[16..];
        }
        // Final 0..=16 bytes, read as two possibly-overlapping words.
        // The length is already mixed into `state`, so the overlap
        // cannot make two keys of different lengths collide.
        let rest = bytes.len();
        let (a, b) = if rest >= 8 {
            (read_u64_le(bytes), read_u64_le(&bytes[rest - 8..]))
        } else if rest >= 4 {
            (read_u32_le(bytes), read_u32_le(&bytes[rest - 4..]))
        } else if rest > 0 {
            let lo = u64::from(bytes[0]);
            let mid = u64::from(bytes[rest / 2]) << 8;
            let hi = u64::from(bytes[rest - 1]) << 16;
            (lo | mid | hi, 0)
        } else {
            (0, 0)
        };
        state = folded_mul(a ^ self.k1, b ^ self.k2 ^ state);
        folded_mul(state ^ Self::C1, self.k1 ^ Self::C0)
    }
}

/// Outcome of comparing the key stored at an existing offset with the
/// key being written. Returned by the resolver closures passed to
/// [`Index::replace`] and [`Index::remove`].
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum KeyCheck {
    /// The record at the offset carries the same key.
    Same,
    /// The record at the offset carries a different key (a real
    /// 64-bit hash collision). The other key is returned so it can be
    /// moved into the overflow map.
    Other(Vec<u8>),
    /// The record could not be decoded. The slot is treated as stale
    /// and overwritten.
    Unreadable,
}

/// Snapshot of a slot's three atomic fields taken under a single
/// seqlock read. Returned by [`AtomicSlot::read`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SlotSnapshot {
    state: u8,
    hash: u64,
    offset: u64,
}

/// One slot in a shard's open-addressed table.
#[repr(C)]
struct AtomicSlot {
    /// Seqlock version: even when stable, odd while the shard's
    /// writer is updating the fields below.
    seq: AtomicU64,
    /// One of the `STATE_*` codes.
    state: AtomicU8,
    /// Full 64-bit key hash.
    hash: AtomicU64,
    /// File offset of the most recent record for this entry. Unused
    /// when the state is not `STATE_OCCUPIED`.
    offset: AtomicU64,
}

impl AtomicSlot {
    fn empty() -> Self {
        Self {
            seq: AtomicU64::new(0),
            state: AtomicU8::new(STATE_EMPTY),
            hash: AtomicU64::new(0),
            offset: AtomicU64::new(0),
        }
    }

    /// Seqlock read: returns a snapshot of `(state, hash, offset)`
    /// that was current at one instant.
    #[inline]
    fn read(&self) -> SlotSnapshot {
        loop {
            let s0 = self.seq.load(Ordering::Acquire);
            if s0 & 1 == 1 {
                sync::spin();
                continue;
            }
            let state = self.state.load(Ordering::Relaxed);
            let hash = self.hash.load(Ordering::Relaxed);
            let offset = self.offset.load(Ordering::Relaxed);
            // Orders the field loads before the `s1` load. Pairs with
            // the writer's release fence: if any field load above saw
            // a store from an update, `s1` sees that update's odd
            // `seq` (or a later value) and the snapshot is retried.
            fence(Ordering::Acquire);
            let s1 = self.seq.load(Ordering::Relaxed);
            if s0 == s1 {
                return SlotSnapshot {
                    state,
                    hash,
                    offset,
                };
            }
            sync::spin();
        }
    }

    /// Seqlock write. The caller must be the only writer of this slot
    /// (it holds the owning shard's writer mutex, or the table is not
    /// yet published).
    #[inline]
    fn write(&self, state: u8, hash: u64, offset: u64) {
        let seq = self.seq.load(Ordering::Relaxed);
        debug_assert_eq!(seq & 1, 0, "seqlock writer re-entered");
        self.seq.store(seq.wrapping_add(1), Ordering::Relaxed);
        // Orders the odd `seq` store before the field stores. Pairs
        // with the reader's acquire fence (see `read`).
        fence(Ordering::Release);
        self.state.store(state, Ordering::Relaxed);
        self.hash.store(hash, Ordering::Relaxed);
        self.offset.store(offset, Ordering::Relaxed);
        self.seq.store(seq.wrapping_add(2), Ordering::Release);
    }
}

/// One open-addressed table. Replaced as a whole on growth.
struct Table {
    slots: Box<[AtomicSlot]>,
    mask: usize,
}

impl Table {
    fn with_capacity(capacity: usize) -> Self {
        debug_assert!(capacity.is_power_of_two());
        let slots: Box<[AtomicSlot]> = (0..capacity).map(|_| AtomicSlot::empty()).collect();
        Self {
            slots,
            mask: capacity - 1,
        }
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.mask + 1
    }

    /// Home slot of `hash`. Uses the bits above the shard selector so
    /// that every slot of the table is a possible home slot.
    #[inline]
    fn home(&self, hash: u64) -> usize {
        ((hash >> SHARD_BITS) as usize) & self.mask
    }

    #[inline]
    fn next(&self, idx: usize) -> usize {
        (idx + 1) & self.mask
    }

    /// Place a known-unique entry into a table that has not been
    /// published yet.
    fn insert_unique(&self, state: u8, hash: u64, offset: u64) {
        let mut idx = self.home(hash);
        for _ in 0..self.capacity() {
            let slot = &self.slots[idx];
            if slot.state.load(Ordering::Relaxed) == STATE_EMPTY {
                slot.write(state, hash, offset);
                return;
            }
            idx = self.next(idx);
        }
        // Rebuild sizes the new table to at most half full, so a free
        // slot always exists.
        debug_assert!(false, "insert_unique: table full during rebuild");
    }
}

/// Per-shard overflow table for real 64-bit hash collisions. Keyed by
/// hash; the value lists the `(key, offset)` pairs sharing that hash.
type OverflowMap = HashMap<u64, Vec<(Box<[u8]>, u64)>>;

/// Slot accounting, owned by the shard's writer mutex.
#[derive(Debug, Default)]
struct WriterState {
    /// Slots in `STATE_OCCUPIED` or `STATE_OVERFLOW`.
    occupied: usize,
    /// Slots in `STATE_TOMBSTONE`.
    tombstones: usize,
}

/// Fields read on every lookup. Kept on their own cache line so
/// writer bookkeeping does not invalidate readers' copies.
struct ReadSide {
    table: Atomic<Table>,
}

/// Fields touched only by writers (and by `len`).
struct WriteSide {
    writer: Mutex<WriterState>,
    /// Live entries in this shard: primary `Occupied` slots plus every
    /// overflow entry. Written under `writer`, read lock-free.
    live: AtomicUsize,
}

/// One shard.
struct Shard {
    read: CachePadded<ReadSide>,
    write: CachePadded<WriteSide>,
    overflow: RwLock<OverflowMap>,
}

impl Shard {
    fn new() -> Self {
        Self {
            read: CachePadded::new(ReadSide {
                table: Atomic::new(Table::with_capacity(INITIAL_SHARD_CAPACITY)),
            }),
            write: CachePadded::new(WriteSide {
                writer: Mutex::new(WriterState::default()),
                live: AtomicUsize::new(0),
            }),
            overflow: RwLock::new(OverflowMap::new()),
        }
    }

    /// The current table, valid for as long as `guard` stays pinned.
    #[inline]
    fn table<'g>(&self, guard: &'g Guard) -> &'g Table {
        let shared = self.read.table.load(Ordering::Acquire, guard);
        // SAFETY: the pointer is never null: it is initialised in
        // `new` and only ever replaced (under the writer mutex) by a
        // freshly allocated table. A table that is swapped out is
        // released with `defer_destroy`, which waits until every
        // guard pinned before the swap is dropped, so the reference
        // cannot outlive the allocation while `guard` is held.
        unsafe { shared.deref() }
    }

    /// The current table, read by the shard's writer without pinning
    /// the epoch (pinning on every write costs a fence plus periodic
    /// global collection work that contends across writer threads).
    ///
    /// # Safety
    ///
    /// The caller must hold `self.write.writer` for as long as it uses
    /// the returned reference, and must not use the reference after
    /// calling [`Self::publish`] (directly or through `rebuild` or
    /// `clear`). Only `publish` retires a table, and it is only called
    /// with the writer mutex held, so under these rules the table
    /// cannot be retired, let alone freed, while it is borrowed.
    #[inline]
    unsafe fn writer_table(&self) -> &Table {
        // SAFETY: `unprotected` is sound here because the caller's
        // writer lock, not an epoch guard, keeps the table alive (see
        // the function's safety contract).
        let guard = unsafe { epoch::unprotected() };
        let shared = self.read.table.load(Ordering::Acquire, guard);
        // SAFETY: non-null (see `table`) and not retired while the
        // writer lock is held, per the caller's contract.
        unsafe { shared.deref() }
    }

    /// Lock-free lookup.
    fn get(&self, hash: u64, key: &[u8]) -> Option<u64> {
        let guard = epoch::pin();
        let table = self.table(&guard);
        let mut idx = table.home(hash);
        for _ in 0..table.capacity() {
            let snap = table.slots[idx].read();
            match snap.state {
                STATE_EMPTY => return None,
                STATE_OCCUPIED if snap.hash == hash => return Some(snap.offset),
                STATE_OVERFLOW if snap.hash == hash => return self.overflow_get(hash, key),
                _ => {}
            }
            idx = table.next(idx);
        }
        None
    }

    fn overflow_get(&self, hash: u64, key: &[u8]) -> Option<u64> {
        let overflow = self.overflow.read();
        overflow
            .get(&hash)?
            .iter()
            .find(|(k, _)| k.as_ref() == key)
            .map(|(_, off)| *off)
    }

    /// Insert or overwrite. Returns the previous offset for `key`, or
    /// `None` for a fresh insert.
    fn replace<F>(&self, hash: u64, key: &[u8], offset: u64, resolve: &mut F) -> Result<Option<u64>>
    where
        F: FnMut(u64, &[u8]) -> Result<KeyCheck>,
    {
        let mut state = self.write.writer.lock();
        loop {
            // SAFETY: `state` holds the writer lock for this whole
            // call, and `table` is re-read after every rebuild (the
            // only call below that publishes a new table).
            let table = unsafe { self.writer_table() };
            if let Some(prev) = self.replace_in(table, &mut state, hash, key, offset, resolve)? {
                return Ok(prev);
            }
            self.rebuild(&mut state);
        }
    }

    /// One insert attempt against `table`. Returns `Ok(None)` when the
    /// table must be rebuilt first.
    fn replace_in<F>(
        &self,
        table: &Table,
        state: &mut WriterState,
        hash: u64,
        key: &[u8],
        offset: u64,
        resolve: &mut F,
    ) -> Result<Option<Option<u64>>>
    where
        F: FnMut(u64, &[u8]) -> Result<KeyCheck>,
    {
        let mut idx = table.home(hash);
        let mut reusable: Option<usize> = None;
        let mut empty: Option<usize> = None;
        for _ in 0..table.capacity() {
            let snap = table.slots[idx].read();
            match snap.state {
                STATE_EMPTY => {
                    empty = Some(idx);
                    break;
                }
                STATE_TOMBSTONE => {
                    if reusable.is_none() {
                        reusable = Some(idx);
                    }
                }
                STATE_OCCUPIED if snap.hash == hash => {
                    return match resolve(snap.offset, key)? {
                        KeyCheck::Same | KeyCheck::Unreadable => {
                            table.slots[idx].write(STATE_OCCUPIED, hash, offset);
                            Ok(Some(Some(snap.offset)))
                        }
                        KeyCheck::Other(existing_key) => {
                            self.promote_to_overflow(
                                &table.slots[idx],
                                hash,
                                existing_key,
                                snap.offset,
                                key,
                                offset,
                            );
                            Ok(Some(None))
                        }
                    };
                }
                STATE_OVERFLOW if snap.hash == hash => {
                    return Ok(Some(self.overflow_upsert(hash, key, offset)));
                }
                _ => {}
            }
            idx = table.next(idx);
        }

        #[cfg(test)]
        test_hooks::before_claim();

        let target = match (reusable, empty) {
            (Some(tombstone), _) => {
                state.tombstones -= 1;
                tombstone
            }
            (None, Some(free)) => {
                let used = state.occupied + state.tombstones + 1;
                if used * GROWTH_DENOM > table.capacity() * GROWTH_NUM {
                    return Ok(None);
                }
                free
            }
            (None, None) => return Ok(None),
        };
        table.slots[target].write(STATE_OCCUPIED, hash, offset);
        state.occupied += 1;
        let _ = self.write.live.fetch_add(1, Ordering::Release);
        Ok(Some(None))
    }

    /// Move a colliding pair into the overflow map and mark the slot.
    /// The map is updated before the slot flips, so a concurrent
    /// reader either still sees the old `Occupied` entry (the new key
    /// is not visible yet) or finds both keys in the map.
    fn promote_to_overflow(
        &self,
        slot: &AtomicSlot,
        hash: u64,
        existing_key: Vec<u8>,
        existing_offset: u64,
        key: &[u8],
        offset: u64,
    ) {
        let added = {
            let mut overflow = self.overflow.write();
            let entries = overflow.entry(hash).or_default();
            // The existing key was counted while it sat in the primary
            // slot; only a newly added key changes the live count.
            let _ = upsert_entry(entries, &existing_key, existing_offset);
            upsert_entry(entries, key, offset).is_none()
        };
        slot.write(STATE_OVERFLOW, hash, 0);
        if added {
            let _ = self.write.live.fetch_add(1, Ordering::Release);
        }
    }

    /// Insert or update `key` in the overflow bucket for `hash`.
    fn overflow_upsert(&self, hash: u64, key: &[u8], offset: u64) -> Option<u64> {
        let mut overflow = self.overflow.write();
        let entries = overflow.entry(hash).or_default();
        let prev = upsert_entry(entries, key, offset);
        if prev.is_none() {
            let _ = self.write.live.fetch_add(1, Ordering::Release);
        }
        prev
    }

    /// Remove `key` if its current offset satisfies `matches`.
    /// `matches` receives the current offset and decides whether the
    /// primary-slot entry really is `key` (overflow entries are
    /// compared by key bytes directly).
    fn remove_where<F>(&self, hash: u64, key: &[u8], mut matches: F) -> Result<Option<u64>>
    where
        F: FnMut(u64) -> Result<bool>,
    {
        let mut state = self.write.writer.lock();
        // SAFETY: `state` holds the writer lock until this function
        // returns, and nothing below publishes a new table.
        let table = unsafe { self.writer_table() };
        let mut idx = table.home(hash);
        for _ in 0..table.capacity() {
            let snap = table.slots[idx].read();
            match snap.state {
                STATE_EMPTY => return Ok(None),
                STATE_OCCUPIED if snap.hash == hash => {
                    if !matches(snap.offset)? {
                        return Ok(None);
                    }
                    table.slots[idx].write(STATE_TOMBSTONE, 0, 0);
                    state.occupied -= 1;
                    state.tombstones += 1;
                    let _ = self.write.live.fetch_sub(1, Ordering::Release);
                    return Ok(Some(snap.offset));
                }
                STATE_OVERFLOW if snap.hash == hash => {
                    let mut overflow = self.overflow.write();
                    let Some(entries) = overflow.get_mut(&hash) else {
                        return Ok(None);
                    };
                    let Some(pos) = entries.iter().position(|(k, _)| k.as_ref() == key) else {
                        return Ok(None);
                    };
                    let current = entries[pos].1;
                    if !matches(current)? {
                        return Ok(None);
                    }
                    let _ = entries.swap_remove(pos);
                    let _ = self.write.live.fetch_sub(1, Ordering::Release);
                    if entries.is_empty() {
                        // Demote the marker so the slot can be reused.
                        // Readers that see the marker before the flip
                        // find no bucket and report a miss.
                        let _ = overflow.remove(&hash);
                        drop(overflow);
                        table.slots[idx].write(STATE_TOMBSTONE, 0, 0);
                        state.occupied -= 1;
                        state.tombstones += 1;
                    }
                    return Ok(Some(current));
                }
                _ => {}
            }
            idx = table.next(idx);
        }
        Ok(None)
    }

    /// Rebuild the table: double it when more than half of it would be
    /// live, otherwise rehash at the same size to drop tombstones.
    /// Caller holds the writer mutex.
    fn rebuild(&self, state: &mut WriterState) {
        let guard = epoch::pin();
        let old = self.table(&guard);
        let capacity = old.capacity();
        let new_capacity = if (state.occupied + 1) * 2 > capacity {
            capacity * 2
        } else {
            capacity
        };
        let fresh = Table::with_capacity(new_capacity);
        for slot in old.slots.iter() {
            let snap = slot.read();
            if snap.state == STATE_OCCUPIED || snap.state == STATE_OVERFLOW {
                fresh.insert_unique(snap.state, snap.hash, snap.offset);
            }
        }
        state.tombstones = 0;
        self.publish(fresh, &guard);
    }

    /// Swap in `table` and retire the previous one.
    fn publish(&self, table: Table, guard: &Guard) {
        let previous = self
            .read
            .table
            .swap(Owned::new(table), Ordering::AcqRel, guard);
        // SAFETY: `previous` was the published table and has just been
        // unlinked, so no reader that pins after this point can reach
        // it. Readers that loaded it earlier hold guards; the epoch
        // collector runs the destructor only after all of them unpin.
        unsafe { guard.defer_destroy(previous) };
        // Hand the retired table to the global collector now rather
        // than leaving it in this thread's local bag.
        guard.flush();
    }

    /// Drop every entry and shrink back to the initial capacity.
    fn clear(&self) {
        let mut state = self.write.writer.lock();
        let guard = epoch::pin();
        self.publish(Table::with_capacity(INITIAL_SHARD_CAPACITY), &guard);
        self.overflow.write().clear();
        *state = WriterState::default();
        self.write.live.store(0, Ordering::Release);
    }

    /// Append every live offset (primary and overflow) to `out`.
    fn collect_offsets(&self, out: &mut Vec<u64>) {
        let guard = epoch::pin();
        let table = self.table(&guard);
        for slot in table.slots.iter() {
            let snap = slot.read();
            if snap.state == STATE_OCCUPIED {
                out.push(snap.offset);
            }
        }
        drop(guard);
        for entries in self.overflow.read().values() {
            out.extend(entries.iter().map(|(_, off)| *off));
        }
    }

    fn live(&self) -> usize {
        self.write.live.load(Ordering::Acquire)
    }
}

impl Drop for Shard {
    fn drop(&mut self) {
        let table = std::mem::replace(&mut self.read.table, Atomic::null());
        // SAFETY: `&mut self` proves no reader or writer can access
        // this shard any more. The pointer is non-null (see `table`)
        // and uniquely owns the current table; retired tables were
        // handed to the epoch collector and are not reachable from it.
        drop(unsafe { table.into_owned() });
    }
}

/// Insert `key` into `entries` or update its offset in place.
/// Returns the previous offset when the key was already present.
fn upsert_entry(entries: &mut Vec<(Box<[u8]>, u64)>, key: &[u8], offset: u64) -> Option<u64> {
    if let Some(entry) = entries.iter_mut().find(|(k, _)| k.as_ref() == key) {
        return Some(std::mem::replace(&mut entry.1, offset));
    }
    entries.push((key.into(), offset));
    None
}

/// Sharded index. One per namespace.
pub(crate) struct Index {
    shards: Box<[Shard; SHARDS]>,
}

impl std::fmt::Debug for Index {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Index")
            .field("shards", &SHARDS)
            .field("len", &self.len())
            .finish()
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
        Self {
            shards: Box::new(std::array::from_fn(|_| Shard::new())),
        }
    }

    #[inline]
    fn shard(&self, hash: KeyHash) -> &Shard {
        &self.shards[(hash & SHARD_MASK) as usize]
    }

    /// Look up the offset for `key`. Lock-free.
    ///
    /// A hit on a primary slot is decided by the hash alone; callers
    /// verify the key bytes when they decode the record.
    ///
    /// # Errors
    ///
    /// Never fails; `Result`-typed for call-site uniformity.
    pub(crate) fn get(&self, hash: KeyHash, key: &[u8]) -> Result<Option<u64>> {
        Ok(self.shard(hash).get(hash, key))
    }

    /// Insert or overwrite the offset for `key`. Returns the previous
    /// offset, or `None` for a fresh insert.
    ///
    /// `resolve(existing_offset, key)` is called when a slot with the
    /// same hash is found, to tell an overwrite of `key` apart from a
    /// collision with another key. It runs under the shard's writer
    /// mutex and must not call back into this index.
    ///
    /// # Errors
    ///
    /// Forwards any error from `resolve`.
    pub(crate) fn replace<F>(
        &self,
        hash: KeyHash,
        key: &[u8],
        offset: u64,
        mut resolve: F,
    ) -> Result<Option<u64>>
    where
        F: FnMut(u64, &[u8]) -> Result<KeyCheck>,
    {
        self.shard(hash).replace(hash, key, offset, &mut resolve)
    }

    /// Remove `key`. Returns its previous offset if it was present.
    ///
    /// For a primary-slot hit, `resolve(offset, key)` confirms that the
    /// entry really is `key` before it is removed ([`KeyCheck::Other`]
    /// leaves the index unchanged).
    ///
    /// # Errors
    ///
    /// Forwards any error from `resolve`.
    pub(crate) fn remove<F>(&self, hash: KeyHash, key: &[u8], mut resolve: F) -> Result<Option<u64>>
    where
        F: FnMut(u64, &[u8]) -> Result<KeyCheck>,
    {
        self.shard(hash).remove_where(hash, key, |offset| {
            Ok(!matches!(resolve(offset, key)?, KeyCheck::Other(_)))
        })
    }

    /// Remove `key` only if its current offset is `expected`. Returns
    /// `true` when the entry was removed. This is the compare-and-
    /// remove used by `remove` (after the record at `expected` was
    /// verified) and by TTL sweeps (so a fresh re-insert is never
    /// deleted).
    pub(crate) fn remove_if_offset(&self, hash: KeyHash, key: &[u8], expected: u64) -> bool {
        matches!(
            self.shard(hash)
                .remove_where(hash, key, |offset| Ok(offset == expected)),
            Ok(Some(_))
        )
    }

    /// Number of live entries. Exact with respect to the index: every
    /// shard's counter is maintained under its writer mutex.
    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.shards.iter().map(Shard::live).sum()
    }

    /// Drop every entry.
    ///
    /// # Errors
    ///
    /// Never fails; `Result`-typed for call-site uniformity.
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
    /// Never fails; `Result`-typed for call-site uniformity.
    pub(crate) fn collect_offsets(&self) -> Result<Vec<u64>> {
        let mut out = Vec::with_capacity(self.len());
        for shard in self.shards.iter() {
            shard.collect_offsets(&mut out);
        }
        Ok(out)
    }
}

/// Test-only pause point between the probe and the slot claim of a
/// fresh insert. Used to force the interleaving behind the
/// duplicate-slot race fixed in 1.0.3.
#[cfg(test)]
mod test_hooks {
    use std::cell::Cell;

    thread_local! {
        static PAUSE_BEFORE_CLAIM: Cell<bool> = const { Cell::new(false) };
    }

    #[cfg(not(loom))]
    pub(super) fn set_pause_before_claim(on: bool) {
        PAUSE_BEFORE_CLAIM.with(|cell| cell.set(on));
    }

    pub(super) fn before_claim() {
        if PAUSE_BEFORE_CLAIM.with(Cell::get) {
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    const HASHER: KeyHasher = KeyHasher::with_secrets(1, 3, 5);

    fn h(key: &[u8]) -> u64 {
        HASHER.hash(key)
    }

    /// Resolver for tests: the key at `offset` is `keys[offset]`.
    fn journal<'a>(keys: &'a [&'a [u8]]) -> impl FnMut(u64, &[u8]) -> Result<KeyCheck> + 'a {
        move |offset, key| {
            let stored = keys[offset as usize];
            Ok(if stored == key {
                KeyCheck::Same
            } else {
                KeyCheck::Other(stored.to_vec())
            })
        }
    }

    fn unreadable(_offset: u64, _key: &[u8]) -> Result<KeyCheck> {
        Ok(KeyCheck::Unreadable)
    }

    /// Repetitions for the racy tests: small under Miri, overridable
    /// with `EMDB_RACE_ROUNDS` for soak runs.
    fn race_rounds() -> usize {
        if cfg!(miri) {
            return 4;
        }
        std::env::var("EMDB_RACE_ROUNDS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(2_000)
    }

    #[test]
    fn test_index_insert_then_get_round_trips() {
        let idx = Index::new();
        let hash = h(b"alpha");
        assert_eq!(idx.replace(hash, b"alpha", 7, unreadable).unwrap(), None);
        assert_eq!(idx.get(hash, b"alpha").unwrap(), Some(7));
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn test_index_get_missing_returns_none() {
        let idx = Index::new();
        assert_eq!(idx.get(h(b"nope"), b"nope").unwrap(), None);
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn test_index_overwrite_returns_previous_offset() {
        let keys: [&[u8]; 2] = [b"alpha", b"alpha"];
        let idx = Index::new();
        let hash = h(b"alpha");
        let _ = idx.replace(hash, b"alpha", 0, journal(&keys)).unwrap();
        assert_eq!(
            idx.replace(hash, b"alpha", 1, journal(&keys)).unwrap(),
            Some(0)
        );
        assert_eq!(idx.get(hash, b"alpha").unwrap(), Some(1));
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn test_index_remove_drops_entry_and_reuses_tombstone() {
        let keys: [&[u8]; 2] = [b"alpha", b"alpha"];
        let idx = Index::new();
        let hash = h(b"alpha");
        let _ = idx.replace(hash, b"alpha", 0, journal(&keys)).unwrap();
        assert_eq!(idx.remove(hash, b"alpha", journal(&keys)).unwrap(), Some(0));
        assert_eq!(idx.get(hash, b"alpha").unwrap(), None);
        assert_eq!(idx.len(), 0);
        assert_eq!(
            idx.replace(hash, b"alpha", 1, journal(&keys)).unwrap(),
            None
        );
        assert_eq!(idx.get(hash, b"alpha").unwrap(), Some(1));
    }

    #[test]
    fn test_index_new_index_allocates_small_tables() {
        // Each namespace owns an index; an empty one must stay cheap.
        let idx = Index::new();
        let guard = epoch::pin();
        let slots: usize = idx.shards.iter().map(|s| s.table(&guard).capacity()).sum();
        assert_eq!(slots, SHARDS * INITIAL_SHARD_CAPACITY);
        assert!(slots * std::mem::size_of::<AtomicSlot>() <= 32 * 1024);
    }

    #[test]
    fn test_index_remove_of_colliding_absent_key_keeps_entry() {
        // Regression: a primary-slot remove used to match on the hash
        // alone and could tombstone a different key with that hash.
        let keys: [&[u8]; 1] = [b"first"];
        let idx = Index::new();
        let _ = idx.replace(42, b"first", 0, journal(&keys)).unwrap();
        assert_eq!(idx.remove(42, b"other", journal(&keys)).unwrap(), None);
        assert_eq!(idx.get(42, b"first").unwrap(), Some(0));
    }

    #[test]
    fn test_index_remove_if_offset_only_matches_current() {
        let idx = Index::new();
        let hash = h(b"k");
        let _ = idx.replace(hash, b"k", 5, unreadable).unwrap();
        assert!(!idx.remove_if_offset(hash, b"k", 4));
        assert_eq!(idx.get(hash, b"k").unwrap(), Some(5));
        assert!(idx.remove_if_offset(hash, b"k", 5));
        assert_eq!(idx.get(hash, b"k").unwrap(), None);
        assert!(!idx.remove_if_offset(hash, b"k", 5));
    }

    #[test]
    fn test_index_hash_collision_disambiguates_by_key() {
        let keys: [&[u8]; 3] = [b"first", b"second", b"third"];
        let idx = Index::new();
        let _ = idx.replace(42, b"first", 0, journal(&keys)).unwrap();
        let _ = idx.replace(42, b"second", 1, journal(&keys)).unwrap();
        assert_eq!(idx.get(42, b"first").unwrap(), Some(0));
        assert_eq!(idx.get(42, b"second").unwrap(), Some(1));
        assert_eq!(idx.get(42, b"third").unwrap(), None);
        assert_eq!(idx.len(), 2);
        // Overwrite inside the overflow bucket keeps the count.
        assert_eq!(
            idx.replace(42, b"second", 1, journal(&keys)).unwrap(),
            Some(1)
        );
        assert_eq!(idx.len(), 2);
        assert_eq!(idx.remove(42, b"first", journal(&keys)).unwrap(), Some(0));
        assert_eq!(idx.remove(42, b"second", journal(&keys)).unwrap(), Some(1));
        assert_eq!(idx.len(), 0);
        assert_eq!(idx.get(42, b"second").unwrap(), None);
        // The demoted slot is reusable.
        assert_eq!(idx.replace(42, b"third", 2, journal(&keys)).unwrap(), None);
        assert_eq!(idx.get(42, b"third").unwrap(), Some(2));
    }

    #[test]
    fn test_index_overflow_migration_never_duplicates_keys() {
        // Re-promoting a hash whose bucket already holds the existing
        // key must not store it twice.
        let keys: [&[u8]; 2] = [b"a", b"b"];
        let idx = Index::new();
        let _ = idx.replace(9, b"a", 0, journal(&keys)).unwrap();
        let _ = idx.replace(9, b"b", 1, journal(&keys)).unwrap();
        let _ = idx.remove(9, b"b", journal(&keys)).unwrap();
        let _ = idx.remove(9, b"a", journal(&keys)).unwrap();
        let _ = idx.replace(9, b"a", 0, journal(&keys)).unwrap();
        let _ = idx.replace(9, b"b", 1, journal(&keys)).unwrap();
        assert_eq!(idx.len(), 2);
        let mut offsets = idx.collect_offsets().unwrap();
        offsets.sort_unstable();
        assert_eq!(offsets, vec![0, 1]);
    }

    #[test]
    fn test_index_len_reflects_total_entries_across_shards() {
        let idx = Index::new();
        for i in 0_u64..200 {
            let key = format!("k{i:04}");
            let _ = idx
                .replace(h(key.as_bytes()), key.as_bytes(), i, unreadable)
                .unwrap();
        }
        assert_eq!(idx.len(), 200);
        assert_eq!(idx.collect_offsets().unwrap().len(), 200);
    }

    #[test]
    fn test_index_clear_empties_every_shard() {
        let idx = Index::new();
        for i in 0_u64..50 {
            let key = format!("k{i}");
            let _ = idx
                .replace(h(key.as_bytes()), key.as_bytes(), i, unreadable)
                .unwrap();
        }
        idx.clear().unwrap();
        assert_eq!(idx.len(), 0);
        assert!(idx.collect_offsets().unwrap().is_empty());
        assert_eq!(idx.get(h(b"k1"), b"k1").unwrap(), None);
    }

    #[test]
    fn test_index_growth_preserves_entries() {
        let idx = Index::new();
        // Every hash lands in shard 0 (low 6 bits clear) with distinct
        // home bits, forcing several rebuilds of one shard.
        let count = if cfg!(miri) { 300 } else { 5_000 };
        for i in 0..count as u64 {
            let key = format!("k{i:06}");
            let _ = idx
                .replace(i << SHARD_BITS, key.as_bytes(), i, unreadable)
                .unwrap();
        }
        for i in 0..count as u64 {
            let key = format!("k{i:06}");
            assert_eq!(idx.get(i << SHARD_BITS, key.as_bytes()).unwrap(), Some(i));
        }
        assert_eq!(idx.len(), count);
    }

    #[test]
    fn test_index_delete_heavy_workload_rehashes_without_growing() {
        let idx = Index::new();
        for round in 0_u64..if cfg!(miri) { 3 } else { 20 } {
            for i in 0_u64..500 {
                let hash = (round * 1000 + i) << SHARD_BITS;
                let _ = idx.replace(hash, b"k", i, unreadable).unwrap();
                let _ = idx.remove_if_offset(hash, b"k", i);
            }
        }
        assert_eq!(idx.len(), 0);
        let guard = epoch::pin();
        assert_eq!(
            idx.shards[0].table(&guard).capacity(),
            INITIAL_SHARD_CAPACITY
        );
    }

    #[test]
    #[cfg_attr(miri, ignore = "200 K inserts; too slow under Miri")]
    fn test_index_load_factor_uses_every_home_slot() {
        // Regression for H14: probing from the raw hash used only
        // 1/64 of the home slots (the shard bits are constant inside
        // a shard) and the table grew only when completely full.
        let idx = Index::new();
        let hasher = KeyHasher::random();
        let n = 200_000_usize;
        let hashes: Vec<u64> = (0..n)
            .map(|i| {
                let key = format!("user:{i:08}");
                let hash = hasher.hash(key.as_bytes());
                let _ = idx
                    .replace(hash, key.as_bytes(), i as u64, unreadable)
                    .unwrap();
                hash
            })
            .collect();
        let guard = epoch::pin();
        let mut probes = 0_usize;
        for &hash in &hashes {
            let table = idx.shard(hash).table(&guard);
            let mut slot = table.home(hash);
            let mut steps = 1;
            while table.slots[slot].read().hash != hash {
                slot = table.next(slot);
                steps += 1;
            }
            probes += steps;
        }
        let average = probes as f64 / n as f64;
        assert!(average < 2.0, "average hit probe length {average}");
        for shard in idx.shards.iter() {
            let table = shard.table(&guard);
            assert!(shard.live() * 4 <= table.capacity() * 3);
        }
    }

    #[test]
    fn test_key_hasher_is_deterministic_per_instance() {
        let hasher = KeyHasher::random();
        assert_eq!(hasher.hash(b"deterministic"), hasher.hash(b"deterministic"));
        assert_ne!(hasher.hash(b"deterministic"), hasher.hash(b"different"));
        assert_ne!(hasher.hash(b""), hasher.hash(b"\0"));
        assert_ne!(hasher.hash(b"ab"), hasher.hash(b"ab\0"));
    }

    #[test]
    fn test_key_hasher_secrets_change_the_hash() {
        let a = KeyHasher::random();
        let b = KeyHasher::random();
        let same = (0..64_u32)
            .filter(|i| a.hash(&i.to_le_bytes()) == b.hash(&i.to_le_bytes()))
            .count();
        assert_eq!(same, 0);
    }

    #[test]
    #[cfg_attr(miri, ignore = "hash-quality check, no concurrency; slow under Miri")]
    fn test_key_hasher_has_no_collisions_on_structured_keys() {
        let hasher = KeyHasher::random();
        let mut by_hash: HashMap<u64, Vec<u8>> = HashMap::new();
        for len in [0_usize, 1, 3, 4, 7, 8, 12, 15, 16, 17, 31, 32, 33, 64] {
            for i in 0..2_000_u32 {
                // Vary the first and last bytes so short and long keys
                // both differ only in structured positions.
                let mut key = vec![b'#'; len];
                for (dst, src) in key.iter_mut().zip(i.to_le_bytes()) {
                    *dst = src;
                }
                if len >= 8 {
                    key[len - 4..].copy_from_slice(&i.to_be_bytes());
                }
                if let Some(previous) = by_hash.insert(hasher.hash(&key), key.clone()) {
                    assert_eq!(previous, key, "distinct keys collided");
                }
            }
        }
    }

    /// Algebraic collision generator for the pre-1.0.3 unkeyed hash
    /// (two-prime mixer). Every key returned hashed to the same value
    /// as `[0; 16]` under that function.
    fn legacy_colliding_key(w1: u64) -> [u8; 16] {
        const P1: u64 = 0xa076_1d64_78bd_642f;
        const P2: u64 = 0xe703_7ed1_a0b4_28db;
        fn inverse(a: u64) -> u64 {
            let mut x = a;
            for _ in 0..6 {
                x = x.wrapping_mul(2_u64.wrapping_sub(a.wrapping_mul(x)));
            }
            x
        }
        let need = 0_u64.wrapping_sub(w1.wrapping_mul(P1)).rotate_right(27);
        let w2 = need.wrapping_mul(inverse(P2));
        let mut key = [0_u8; 16];
        key[..8].copy_from_slice(&w1.to_le_bytes());
        key[8..].copy_from_slice(&w2.to_le_bytes());
        key
    }

    #[test]
    #[cfg_attr(miri, ignore = "hash-quality check, no concurrency; slow under Miri")]
    fn test_key_hasher_resists_precomputed_collisions() {
        // Regression for H11: 20 000 keys built offline to share one
        // hash under the old function must spread under the keyed one.
        let hasher = KeyHasher::random();
        let hashes: std::collections::HashSet<u64> = (1..=20_000_u64)
            .map(|w1| hasher.hash(&legacy_colliding_key(w1)))
            .collect();
        assert!(hashes.len() > 19_990, "{} distinct hashes", hashes.len());
    }

    #[test]
    fn test_index_duplicate_slot_race_is_impossible() {
        // Regression for H10. Thread A probes for K, sees X then an
        // empty slot, and pauses before claiming. Meanwhile X is
        // removed and K is inserted into X's tombstone. Before 1.0.3,
        // A then claimed the empty slot too, leaving K in two slots,
        // and `remove(K)` resurrected the stale copy.
        let hx: u64 = 0x1000_0000_0000_0000;
        let hk: u64 = 0x2000_0000_0000_0000;
        let keys: [&[u8]; 3] = [b"X", b"K", b"K"];
        let idx = Arc::new(Index::new());
        let _ = idx.replace(hx, b"X", 0, journal(&keys)).unwrap();
        let racer = {
            let idx = Arc::clone(&idx);
            std::thread::spawn(move || {
                let keys: [&[u8]; 3] = [b"X", b"K", b"K"];
                test_hooks::set_pause_before_claim(true);
                let _ = idx.replace(hk, b"K", 1, journal(&keys)).unwrap();
            })
        };
        std::thread::sleep(std::time::Duration::from_millis(10));
        let _ = idx.remove(hx, b"X", journal(&keys)).unwrap();
        let _ = idx.replace(hk, b"K", 2, journal(&keys)).unwrap();
        racer.join().unwrap();
        assert_eq!(idx.len(), 1);
        assert!(idx.remove(hk, b"K", journal(&keys)).unwrap().is_some());
        assert_eq!(idx.get(hk, b"K").unwrap(), None, "K resurrected");
    }

    #[test]
    fn test_index_duplicate_slot_race_stress() {
        let rounds = race_rounds();
        let hx: u64 = 0x1000_0000_0000_0000;
        let hk: u64 = 0x2000_0000_0000_0000;
        let idx = Arc::new(Index::new());
        for _ in 0..rounds {
            let keys: [&[u8]; 3] = [b"X", b"K", b"K"];
            let _ = idx.replace(hx, b"X", 0, journal(&keys)).unwrap();
            let barrier = Arc::new(Barrier::new(3));
            let mut handles = Vec::new();
            {
                let (idx, barrier) = (Arc::clone(&idx), Arc::clone(&barrier));
                handles.push(std::thread::spawn(move || {
                    let keys: [&[u8]; 3] = [b"X", b"K", b"K"];
                    let _ = barrier.wait();
                    let _ = idx.remove(hx, b"X", journal(&keys)).unwrap();
                }));
            }
            for offset in [1_u64, 2] {
                let (idx, barrier) = (Arc::clone(&idx), Arc::clone(&barrier));
                handles.push(std::thread::spawn(move || {
                    let keys: [&[u8]; 3] = [b"X", b"K", b"K"];
                    let _ = barrier.wait();
                    let _ = idx.replace(hk, b"K", offset, journal(&keys)).unwrap();
                }));
            }
            for handle in handles {
                handle.join().unwrap();
            }
            assert_eq!(idx.len(), 1);
            let _ = idx.remove(hk, b"K", journal(&keys)).unwrap();
            assert_eq!(idx.get(hk, b"K").unwrap(), None, "K resurrected");
            idx.clear().unwrap();
        }
    }

    #[test]
    fn test_index_concurrent_collisions_keep_overflow_consistent() {
        // Regression for H11: concurrent inserts of colliding keys used
        // to lose or duplicate overflow entries (a key survived
        // `remove`).
        let rounds = race_rounds();
        let keys: Arc<Vec<Vec<u8>>> =
            Arc::new(vec![b"k0".to_vec(), b"k1".to_vec(), b"k2".to_vec()]);
        let resolver = |keys: Arc<Vec<Vec<u8>>>| {
            move |offset: u64, key: &[u8]| -> Result<KeyCheck> {
                let stored = &keys[offset as usize];
                Ok(if stored.as_slice() == key {
                    KeyCheck::Same
                } else {
                    KeyCheck::Other(stored.clone())
                })
            }
        };
        for _ in 0..rounds {
            let idx = Arc::new(Index::new());
            let _ = idx
                .replace(77, &keys[0], 0, resolver(Arc::clone(&keys)))
                .unwrap();
            let barrier = Arc::new(Barrier::new(2));
            let handles: Vec<_> = [1_usize, 2]
                .into_iter()
                .map(|i| {
                    let (idx, barrier, keys) =
                        (Arc::clone(&idx), Arc::clone(&barrier), Arc::clone(&keys));
                    std::thread::spawn(move || {
                        let _ = barrier.wait();
                        let key = keys[i].clone();
                        let _ = idx.replace(77, &key, i as u64, resolver(keys)).unwrap();
                    })
                })
                .collect();
            for handle in handles {
                handle.join().unwrap();
            }
            for (i, key) in keys.iter().enumerate() {
                assert_eq!(idx.get(77, key).unwrap(), Some(i as u64));
            }
            assert_eq!(idx.len(), 3);
            for key in keys.iter() {
                assert!(idx
                    .remove(77, key, resolver(Arc::clone(&keys)))
                    .unwrap()
                    .is_some());
            }
            assert!(keys.iter().all(|k| idx.get(77, k).unwrap().is_none()));
            assert_eq!(idx.len(), 0);
        }
    }

    #[test]
    fn test_index_readers_see_consistent_state_during_writes() {
        // Readers race a writer that keeps overwriting, removing and
        // growing; a reader must only ever see an offset that was
        // written for its key.
        let (rounds, keys) = if cfg!(miri) { (3, 60) } else { (30, 4_000) };
        let idx = Arc::new(Index::new());
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writer = {
            let (idx, stop) = (Arc::clone(&idx), Arc::clone(&stop));
            std::thread::spawn(move || {
                for round in 0_u64..rounds {
                    for i in 0_u64..keys {
                        let hash = h(&i.to_le_bytes());
                        let _ = idx
                            .replace(hash, &i.to_le_bytes(), i * 1_000 + round, unreadable)
                            .unwrap();
                    }
                    for i in (0_u64..keys).step_by(3) {
                        let hash = h(&i.to_le_bytes());
                        let _ = idx.remove(hash, &i.to_le_bytes(), unreadable).unwrap();
                    }
                }
                stop.store(true, std::sync::atomic::Ordering::Release);
            })
        };
        let readers: Vec<_> = (0..3)
            .map(|_| {
                let (idx, stop) = (Arc::clone(&idx), Arc::clone(&stop));
                std::thread::spawn(move || {
                    while !stop.load(std::sync::atomic::Ordering::Acquire) {
                        for i in 0_u64..keys {
                            let hash = h(&i.to_le_bytes());
                            if let Some(offset) = idx.get(hash, &i.to_le_bytes()).unwrap() {
                                assert_eq!(offset / 1_000, i, "foreign offset");
                            }
                        }
                    }
                })
            })
            .collect();
        writer.join().unwrap();
        for reader in readers {
            reader.join().unwrap();
        }
    }

    /// Probe-length report at 1M keys (H14). Run with
    /// `cargo test --release --lib probe_length_report -- --ignored --nocapture`.
    #[test]
    #[ignore = "measurement, not a check"]
    fn probe_length_report() {
        let hasher = KeyHasher::random();
        for &n in &[10_000_usize, 200_000, 1_000_000] {
            let idx = Index::new();
            let hashes: Vec<u64> = (0..n)
                .map(|i| {
                    let key = format!("user:{i:08}");
                    let hash = hasher.hash(key.as_bytes());
                    let _ = idx
                        .replace(hash, key.as_bytes(), i as u64, unreadable)
                        .unwrap();
                    hash
                })
                .collect();
            let guard = epoch::pin();
            let probe = |hash: u64| {
                let table = idx.shard(hash).table(&guard);
                let mut slot = table.home(hash);
                let mut steps = 1_usize;
                loop {
                    let snap = table.slots[slot].read();
                    if snap.state == STATE_EMPTY || snap.hash == hash {
                        return steps;
                    }
                    slot = table.next(slot);
                    steps += 1;
                }
            };
            let hits: Vec<usize> = hashes.iter().map(|&x| probe(x)).collect();
            let misses: usize = (0..10_000_usize)
                .map(|i| probe(hasher.hash(format!("absent:{i:08}").as_bytes())))
                .sum();
            let table = idx.shards[0].table(&guard);
            eprintln!(
                "n={n:>8} shard0 cap={:>6} load={:.3} avg_hit={:.2} max_hit={} avg_miss={:.2}",
                table.capacity(),
                idx.shards[0].live() as f64 / table.capacity() as f64,
                hits.iter().sum::<usize>() as f64 / n as f64,
                hits.iter().max().copied().unwrap_or(0),
                misses as f64 / 10_000.0
            );
        }
    }
}

/// `loom` model of the slot seqlock. Run with
/// `RUSTFLAGS="--cfg loom" cargo test --release --lib loom_`.
/// Only the seqlock is modelled; everything else in the crate still
/// uses the real atomics and must not run under this cfg.
#[cfg(all(test, loom))]
mod loom_model {
    use super::*;

    const OLD: SlotSnapshot = SlotSnapshot {
        state: STATE_EMPTY,
        hash: 0,
        offset: 0,
    };

    #[test]
    fn loom_seqlock_reader_never_sees_a_torn_slot() {
        loom::model(|| {
            let slot = loom::sync::Arc::new(AtomicSlot::empty());
            let writer = {
                let slot = slot.clone();
                loom::thread::spawn(move || slot.write(STATE_OCCUPIED, 7, 7))
            };
            let snap = slot.read();
            let new = SlotSnapshot {
                state: STATE_OCCUPIED,
                hash: 7,
                offset: 7,
            };
            assert!(snap == OLD || snap == new, "torn read: {snap:?}");
            writer.join().unwrap();
        });
    }

    #[test]
    fn loom_seqlock_two_updates_reader_sees_one_of_three_states() {
        loom::model(|| {
            let slot = loom::sync::Arc::new(AtomicSlot::empty());
            let writer = {
                let slot = slot.clone();
                loom::thread::spawn(move || {
                    slot.write(STATE_OCCUPIED, 1, 1);
                    slot.write(STATE_TOMBSTONE, 2, 2);
                })
            };
            let snap = slot.read();
            assert_eq!(snap.hash, snap.offset, "torn read: {snap:?}");
            writer.join().unwrap();
        });
    }
}
