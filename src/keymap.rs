// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Per-namespace primary index for the v0.7 storage engine.
//!
//! A [`Keymap`] is a sharded `Shard` mapping each key's 64-bit
//! FxHash to one or more [`Rid`]s. Cloning a [`Slot`] out of the map is the
//! main read step — the lookup releases its shard read-lock immediately and
//! the caller verifies the actual key by reading the leaf page and comparing
//! key bytes. This handles the rare 64-bit hash collision without forcing
//! every read to walk a list.
//!
//! ## Why per-namespace?
//!
//! Phase F splits the database into independent namespaces (default empty,
//! plus user-created ones via `db.namespace("users")`). Each namespace gets
//! its own keymap, so iteration over a namespace touches only its keys, and
//! `clear`-style operations on one namespace do not contend with writes to
//! another.
//!
//! ## Sharding
//!
//! The keymap is split into [`SHARD_COUNT`] independent shards, each guarded
//! by its own [`RwLock`]. Shard selection takes the bottom [`SHARD_BITS`] of
//! the FxHash, so the FxHash distribution doubles as the shard distribution
//! and we never need a second hash. Reads on different keys are fully
//! parallel; writes contend only on the target shard.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::storage::fxhash;
use crate::storage::page::rid::Rid;
use crate::{Error, Result};

/// Hasher that returns the input `u64` unchanged.
///
/// The keymap's keys are already 64-bit FxHashes — running them through
/// `std::collections::hash_map::RandomState` (a SipHash) means we hash a
/// hash on every operation, doubling the per-operation CPU on the hottest
/// path in the engine. `IdentityHasher` drops the second hash entirely:
/// `HashMap` calls `Hasher::write_u64` for `u64` keys, so the bottom 64
/// bits of the input become the bucket selector directly. The bucket
/// selector quality is identical to FxHash's distribution, which is what
/// the keymap already relies on.
///
/// `write` (the catch-all bytewise path) is implemented for completeness
/// in case downstream callers ever feed the hasher an arbitrary byte
/// slice; it should not run on the hot path.
#[derive(Default)]
pub(crate) struct IdentityHasher(u64);

impl Hasher for IdentityHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline]
    fn write_u64(&mut self, value: u64) {
        self.0 = value;
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        // Slow path — `HashMap<u64, _>` should never reach here. Folds
        // bytes back into a u64 with a non-cryptographic mix so callers
        // that misuse the hasher still get a sane result.
        let mut state = self.0;
        for &byte in bytes {
            state = state.rotate_left(5) ^ u64::from(byte);
            state = state.wrapping_mul(0x100_0000_01b3);
        }
        self.0 = state;
    }
}

/// `BuildHasher` that produces [`IdentityHasher`] instances.
pub(crate) type IdentityBuildHasher = BuildHasherDefault<IdentityHasher>;

/// Inner shard map type. Using a [`HashMap`] with [`IdentityHasher`] turns
/// the bucket selection into a single `u64` modulo, since the input `u64`
/// is already a high-quality hash.
type Shard = HashMap<u64, Slot, IdentityBuildHasher>;

/// Number of bits in the FxHash used to pick a shard.
pub(crate) const SHARD_BITS: u32 = 5;

/// Number of independent keymap shards. Must equal `1 << SHARD_BITS`.
pub(crate) const SHARD_COUNT: usize = 1 << SHARD_BITS;

const SHARD_MASK: u64 = (SHARD_COUNT as u64) - 1;

/// Resolution of a keymap lookup.
///
/// In the common case there is exactly one [`Rid`] for a given hash; we
/// store it inline as [`Slot::Single`] to avoid a per-entry heap allocation.
/// On the (very rare) 64-bit hash collision the slot is promoted to
/// [`Slot::Multi`], which holds a small `Vec<Rid>`. Callers walking a slot
/// must check the actual key on the page side, since a [`Slot::Multi`] only
/// signals "one of these `Rid`s belongs to this hash bucket".
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Slot {
    /// One key (and one [`Rid`]) lives at this hash.
    Single(Rid),
    /// Multiple keys collided onto the same 64-bit hash; their [`Rid`]s are
    /// listed here. Order is insertion order; duplicates are not stored.
    Multi(Vec<Rid>),
}

impl Slot {
    /// Iterate every [`Rid`] referenced by this slot.
    pub(crate) fn iter(&self) -> SlotIter<'_> {
        match self {
            Self::Single(rid) => SlotIter::Single(Some(rid)),
            Self::Multi(rids) => SlotIter::Multi(rids.iter()),
        }
    }

    /// Return `true` when no [`Rid`]s are referenced. Should be unreachable
    /// in practice — a slot with zero entries is removed from its map — but
    /// the helper keeps callers honest.
    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        matches!(self, Self::Multi(rids) if rids.is_empty())
    }
}

/// Iterator over the [`Rid`]s of a [`Slot`].
pub(crate) enum SlotIter<'a> {
    /// One inline `Rid`, yielded once.
    Single(Option<&'a Rid>),
    /// Heap-backed list of colliding `Rid`s.
    Multi(core::slice::Iter<'a, Rid>),
}

impl<'a> Iterator for SlotIter<'a> {
    type Item = &'a Rid;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Single(slot) => slot.take(),
            Self::Multi(iter) => iter.next(),
        }
    }
}

/// Sharded primary index for a single namespace.
///
/// Cloning a [`Keymap`] is not cheap — each shard contains its own `RwLock`
/// and the storage is `Box<[...; SHARD_COUNT]>`. The owning [`Namespace`] is
/// itself wrapped in `Arc` so multiple handles share the same keymap.
#[derive(Debug)]
pub(crate) struct Keymap {
    shards: Box<[RwLock<Shard>; SHARD_COUNT]>,
}

impl Default for Keymap {
    fn default() -> Self {
        Self::new()
    }
}

impl Keymap {
    /// Construct a fresh empty keymap with [`SHARD_COUNT`] empty shards.
    #[must_use]
    pub(crate) fn new() -> Self {
        let shards = std::array::from_fn::<_, SHARD_COUNT, _>(|_| RwLock::new(Shard::default()));
        Self {
            shards: Box::new(shards),
        }
    }

    /// Hash a key with the keymap's hash function.
    #[inline]
    #[must_use]
    pub(crate) fn hash_key(key: &[u8]) -> u64 {
        fxhash::hash(key)
    }

    /// Pick the shard index for a given hash.
    #[inline]
    #[must_use]
    pub(crate) const fn shard_for(hash: u64) -> usize {
        (hash & SHARD_MASK) as usize
    }

    /// Read-lock a single shard and return its guard.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockPoisoned`] when the shard's `RwLock` was poisoned
    /// by a panicking writer.
    pub(crate) fn read_shard(&self, shard_idx: usize) -> Result<RwLockReadGuard<'_, Shard>> {
        self.shards[shard_idx]
            .read()
            .map_err(|_poisoned| Error::LockPoisoned)
    }

    /// Write-lock a single shard and return its guard.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockPoisoned`] on poison.
    pub(crate) fn write_shard(&self, shard_idx: usize) -> Result<RwLockWriteGuard<'_, Shard>> {
        self.shards[shard_idx]
            .write()
            .map_err(|_poisoned| Error::LockPoisoned)
    }

    /// Clone the slot for a key's hash, if any.
    ///
    /// This is the primary read entry-point: it acquires the shard read-lock,
    /// clones the [`Slot`] out, and releases the lock immediately so the
    /// caller can do disk I/O without blocking writers.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockPoisoned`] on shard poison.
    pub(crate) fn lookup(&self, hash: u64) -> Result<Option<Slot>> {
        let shard = self.read_shard(Self::shard_for(hash))?;
        Ok(shard.get(&hash).cloned())
    }

    /// Insert (or extend) the slot for a hash with a new [`Rid`].
    ///
    /// If the slot is currently absent, a new [`Slot::Single`] is created.
    /// If the slot exists as `Single` and the new `Rid` differs, the slot is
    /// promoted to `Multi`. If the slot exists as `Multi`, the new `Rid` is
    /// appended (duplicates are deduplicated in place).
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockPoisoned`] on shard poison.
    pub(crate) fn insert(&self, hash: u64, rid: Rid) -> Result<()> {
        let mut shard = self.write_shard(Self::shard_for(hash))?;
        match shard.get_mut(&hash) {
            None => {
                let _previous = shard.insert(hash, Slot::Single(rid));
            }
            Some(Slot::Single(existing)) => {
                if *existing != rid {
                    let old = *existing;
                    let _replaced = shard.insert(hash, Slot::Multi(vec![old, rid]));
                }
            }
            Some(Slot::Multi(rids)) => {
                if !rids.contains(&rid) {
                    rids.push(rid);
                }
            }
        }
        Ok(())
    }

    /// Replace the slot for a hash so it points only at `rid`.
    ///
    /// Used by the page-rewrite path (compact, split) where a key's `Rid` has
    /// moved to a new `(page, slot)` and any previous `Rid` for the same hash
    /// is no longer valid.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockPoisoned`] on shard poison.
    pub(crate) fn replace_single(&self, hash: u64, rid: Rid) -> Result<()> {
        let mut shard = self.write_shard(Self::shard_for(hash))?;
        let _previous = shard.insert(hash, Slot::Single(rid));
        Ok(())
    }

    /// Remove a specific [`Rid`] from a hash's slot. Returns whether the
    /// `Rid` was present.
    ///
    /// When removing the last `Rid` from a `Multi` slot the slot is collapsed
    /// to `Single` (or the entire entry is dropped if no `Rid`s remain).
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockPoisoned`] on shard poison.
    pub(crate) fn remove(&self, hash: u64, rid: Rid) -> Result<bool> {
        let mut shard = self.write_shard(Self::shard_for(hash))?;
        let Some(slot) = shard.get_mut(&hash) else {
            return Ok(false);
        };

        match slot {
            Slot::Single(existing) => {
                if *existing != rid {
                    return Ok(false);
                }
                let _removed = shard.remove(&hash);
                Ok(true)
            }
            Slot::Multi(rids) => {
                let Some(pos) = rids.iter().position(|r| *r == rid) else {
                    return Ok(false);
                };
                let _removed = rids.remove(pos);
                if rids.len() == 1 {
                    let last = rids[0];
                    let _replaced = shard.insert(hash, Slot::Single(last));
                } else if rids.is_empty() {
                    let _removed = shard.remove(&hash);
                }
                Ok(true)
            }
        }
    }

    /// Drop every entry across every shard.
    ///
    /// Acquires write locks in stable shard order so this composes safely
    /// with concurrent operations that follow the same ordering rule.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockPoisoned`] if any shard was poisoned.
    pub(crate) fn clear(&self) -> Result<()> {
        for shard_idx in 0..SHARD_COUNT {
            let mut shard = self.write_shard(shard_idx)?;
            shard.clear();
        }
        Ok(())
    }

    /// Sum of live entries across every shard.
    ///
    /// Each shard is read under its own guard; writers may race with this
    /// call so the returned count is a snapshot, not a transaction-consistent
    /// view.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockPoisoned`] on shard poison.
    pub(crate) fn len(&self) -> Result<usize> {
        let mut total = 0_usize;
        for shard_idx in 0..SHARD_COUNT {
            let shard = self.read_shard(shard_idx)?;
            total = total.saturating_add(shard.len());
        }
        Ok(total)
    }

    /// Acquire write guards on every shard, in stable shard order.
    ///
    /// Used by transaction commit and clear so the keymap mutates atomically
    /// with respect to readers and other writers.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockPoisoned`] on poison.
    pub(crate) fn write_all(&self) -> Result<Vec<RwLockWriteGuard<'_, Shard>>> {
        let mut guards = Vec::with_capacity(SHARD_COUNT);
        for shard_idx in 0..SHARD_COUNT {
            guards.push(self.write_shard(shard_idx)?);
        }
        Ok(guards)
    }

    /// Acquire read guards on every shard, in stable shard order.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockPoisoned`] on poison.
    pub(crate) fn read_all(&self) -> Result<Vec<RwLockReadGuard<'_, Shard>>> {
        let mut guards = Vec::with_capacity(SHARD_COUNT);
        for shard_idx in 0..SHARD_COUNT {
            guards.push(self.read_shard(shard_idx)?);
        }
        Ok(guards)
    }
}

/// A logical partition of the database with its own [`Keymap`].
///
/// In v0.7 the engine always has at least the default namespace (`name = ""`,
/// `id = 0`). Phase F adds the catalog persistence and the public
/// `db.namespace(name)` API; for now this struct is the in-memory shape we
/// build the rest of the engine around.
#[derive(Debug)]
pub(crate) struct Namespace {
    /// Stable identifier assigned at creation. Persisted in the catalog.
    pub(crate) id: u32,
    /// Human-readable name. Empty string for the default namespace.
    pub(crate) name: Box<str>,
    /// Per-namespace primary index.
    pub(crate) keymap: Keymap,
}

impl Namespace {
    /// Construct a fresh empty namespace.
    #[must_use]
    pub(crate) fn new(id: u32, name: impl Into<Box<str>>) -> Self {
        Self {
            id,
            name: name.into(),
            keymap: Keymap::new(),
        }
    }

    /// Construct the default (unnamed, id 0) namespace.
    #[must_use]
    pub(crate) fn default_ns() -> Self {
        Self::new(0, "")
    }
}

#[cfg(test)]
mod tests {
    use super::{Keymap, Namespace, Slot, SHARD_COUNT};
    use crate::storage::page::rid::Rid;

    #[test]
    fn fresh_keymap_reports_zero_length() {
        let map = Keymap::new();
        let len = map.len();
        assert!(matches!(len, Ok(0)));
    }

    #[test]
    fn lookup_returns_none_for_unknown_hash() {
        let map = Keymap::new();
        let h = Keymap::hash_key(b"missing");
        let lookup = map.lookup(h);
        assert!(matches!(lookup, Ok(None)));
    }

    #[test]
    fn insert_then_lookup_returns_single_slot() {
        let map = Keymap::new();
        let h = Keymap::hash_key(b"alpha");
        let rid = Rid::new(7, 3);

        let inserted = map.insert(h, rid);
        assert!(inserted.is_ok());

        let lookup = map.lookup(h);
        assert!(matches!(lookup, Ok(Some(Slot::Single(found))) if found == rid));
    }

    #[test]
    fn inserting_same_rid_twice_remains_single() {
        let map = Keymap::new();
        let h = Keymap::hash_key(b"alpha");
        let rid = Rid::new(1, 1);

        assert!(map.insert(h, rid).is_ok());
        assert!(map.insert(h, rid).is_ok());

        let lookup = map.lookup(h);
        assert!(matches!(lookup, Ok(Some(Slot::Single(found))) if found == rid));
    }

    #[test]
    fn collision_promotes_single_to_multi() {
        let map = Keymap::new();
        let h = Keymap::hash_key(b"alpha");
        let rid_a = Rid::new(1, 0);
        let rid_b = Rid::new(2, 0);

        assert!(map.insert(h, rid_a).is_ok());
        assert!(map.insert(h, rid_b).is_ok());

        let lookup = map.lookup(h);
        assert!(matches!(
            lookup,
            Ok(Some(Slot::Multi(ref rids))) if rids == &vec![rid_a, rid_b]
        ));
    }

    #[test]
    fn remove_collapses_multi_back_to_single_with_one_left() {
        let map = Keymap::new();
        let h = Keymap::hash_key(b"alpha");
        let rid_a = Rid::new(1, 0);
        let rid_b = Rid::new(2, 0);
        let rid_c = Rid::new(3, 0);

        assert!(map.insert(h, rid_a).is_ok());
        assert!(map.insert(h, rid_b).is_ok());
        assert!(map.insert(h, rid_c).is_ok());

        // Remove the middle Rid; should remain Multi with two entries.
        let removed = map.remove(h, rid_b);
        assert!(matches!(removed, Ok(true)));
        let lookup = map.lookup(h);
        assert!(matches!(
            lookup,
            Ok(Some(Slot::Multi(ref rids))) if rids == &vec![rid_a, rid_c]
        ));

        // Remove one more; should collapse to Single.
        let removed = map.remove(h, rid_a);
        assert!(matches!(removed, Ok(true)));
        let lookup = map.lookup(h);
        assert!(matches!(lookup, Ok(Some(Slot::Single(found))) if found == rid_c));

        // Remove the last; the entry disappears entirely.
        let removed = map.remove(h, rid_c);
        assert!(matches!(removed, Ok(true)));
        let lookup = map.lookup(h);
        assert!(matches!(lookup, Ok(None)));
    }

    #[test]
    fn remove_unknown_rid_reports_false() {
        let map = Keymap::new();
        let h = Keymap::hash_key(b"alpha");
        let rid = Rid::new(1, 0);
        assert!(map.insert(h, rid).is_ok());

        let removed = map.remove(h, Rid::new(99, 99));
        assert!(matches!(removed, Ok(false)));

        // Original Rid is still there.
        let lookup = map.lookup(h);
        assert!(matches!(lookup, Ok(Some(Slot::Single(found))) if found == rid));
    }

    #[test]
    fn replace_single_overwrites_existing_slot() {
        let map = Keymap::new();
        let h = Keymap::hash_key(b"alpha");
        assert!(map.insert(h, Rid::new(1, 0)).is_ok());
        assert!(map.insert(h, Rid::new(2, 0)).is_ok());

        // replace_single drops the Multi list and installs a fresh Single.
        assert!(map.replace_single(h, Rid::new(99, 99)).is_ok());
        let lookup = map.lookup(h);
        assert!(matches!(
            lookup,
            Ok(Some(Slot::Single(rid))) if rid == Rid::new(99, 99)
        ));
    }

    #[test]
    fn slot_iter_yields_every_rid() {
        let single = Slot::Single(Rid::new(7, 1));
        let collected: Vec<_> = single.iter().copied().collect();
        assert_eq!(collected, vec![Rid::new(7, 1)]);

        let multi = Slot::Multi(vec![Rid::new(1, 0), Rid::new(2, 0), Rid::new(3, 0)]);
        let collected: Vec<_> = multi.iter().copied().collect();
        assert_eq!(
            collected,
            vec![Rid::new(1, 0), Rid::new(2, 0), Rid::new(3, 0)]
        );
    }

    #[test]
    fn clear_drops_every_entry() {
        let map = Keymap::new();
        for i in 0_u32..1000 {
            let key = format!("k{i}");
            let _ = map.insert(Keymap::hash_key(key.as_bytes()), Rid::new(u64::from(i), 0));
        }
        let len = map.len();
        assert!(matches!(len, Ok(1000)));

        let cleared = map.clear();
        assert!(cleared.is_ok());
        let len = map.len();
        assert!(matches!(len, Ok(0)));
    }

    #[test]
    fn write_all_and_read_all_return_one_guard_per_shard() {
        let map = Keymap::new();
        let writes = map.write_all();
        assert!(matches!(writes, Ok(ref guards) if guards.len() == SHARD_COUNT));
        drop(writes);
        let reads = map.read_all();
        assert!(matches!(reads, Ok(ref guards) if guards.len() == SHARD_COUNT));
    }

    #[test]
    fn shard_for_distributes_uniformly_across_a_simple_key_space() {
        let mut counts = [0_usize; SHARD_COUNT];
        for i in 0..10_000_u32 {
            let key = format!("key-{i}");
            counts[Keymap::shard_for(Keymap::hash_key(key.as_bytes()))] += 1;
        }
        for count in counts {
            assert!(count > 0, "shard distribution missed a bucket");
        }
    }

    #[test]
    fn namespace_default_has_id_zero_and_empty_name() {
        let ns = Namespace::default_ns();
        assert_eq!(ns.id, 0);
        assert_eq!(&*ns.name, "");
        let len = ns.keymap.len();
        assert!(matches!(len, Ok(0)));
    }

    #[test]
    fn namespace_named_constructor_records_name() {
        let ns = Namespace::new(7, "users");
        assert_eq!(ns.id, 7);
        assert_eq!(&*ns.name, "users");
    }
}
