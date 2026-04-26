// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! In-memory sharded hash index. Maps `(ns_id, key)` → file offset of
//! the most recent live record for that key.
//!
//! Uses 64 shards with FxHash + an [`IdentityHasher`] over the precomputed
//! 64-bit hash so the per-op work is a single mod-64 + one HashMap probe.
//! No allocations on the hot lookup path; insert allocates only on
//! collision (when the same hash maps to multiple distinct keys).
//!
//! The index does **not** store the keys themselves — it stores file
//! offsets and lets the storage layer resolve hash collisions by reading
//! the on-disk record's key. This keeps RAM use proportional to the
//! number of records, not their total size.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::RwLock;

use crate::{Error, Result};

/// Number of shards. Power of two so the shard selector is a bitmask.
const SHARDS: usize = 64;
const SHARD_MASK: u64 = (SHARDS as u64) - 1;

/// `Hasher` that returns the input `u64` unchanged. Saves the cost of
/// re-hashing a key that already arrived as a 64-bit hash from FxHash.
#[derive(Default)]
pub(crate) struct IdentityHasher(u64);

impl Hasher for IdentityHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline]
    fn write(&mut self, _bytes: &[u8]) {
        // The HashMap dispatches via `write_u64` for u64 keys; this
        // path should not be exercised. Be defensive: leave the state
        // unchanged so a stray caller gets a deterministic (but
        // useless) hash rather than a panic.
    }

    #[inline]
    fn write_u64(&mut self, value: u64) {
        self.0 = value;
    }
}

type IdentityBuildHasher = BuildHasherDefault<IdentityHasher>;
type ShardMap = HashMap<u64, Slot, IdentityBuildHasher>;

/// Per-shard slot. `Single` is the common case; `Multi` handles 64-bit
/// hash collisions (extremely rare for typical workloads but must be
/// correct when they happen). Each `Multi` entry carries the key bytes
/// so the storage layer can disambiguate without re-reading the file.
#[derive(Debug, Clone)]
enum Slot {
    /// Single offset for this hash. Hot-path: ~99.99...% of slots.
    Single(u64),
    /// Multiple `(key, offset)` pairs that collided to the same 64-bit
    /// hash. Disambiguated by exact key compare on lookup.
    Multi(Vec<(Vec<u8>, u64)>),
}

/// FxHash-port for keys. Good enough avalanche for short strings, much
/// faster than SipHash. The actual FxHash impl lives in
/// [`Self::hash_key`]; this is just the type alias.
pub(crate) type KeyHash = u64;

/// Sharded index. One per namespace.
#[derive(Debug)]
pub(crate) struct Index {
    shards: Box<[RwLock<ShardMap>; SHARDS]>,
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
        let shards: [RwLock<ShardMap>; SHARDS] =
            std::array::from_fn(|_| RwLock::new(ShardMap::default()));
        Self {
            shards: Box::new(shards),
        }
    }

    /// Compute the FxHash of a key. Same algorithm as
    /// `rustc-hash` / Firefox's hasher; ~2-3x faster than SipHash for
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
    /// `resolve_collision` is called only when the hash slot holds
    /// multiple entries; it should compare each candidate key against
    /// the user's key and return the matching offset (or `None`).
    /// Single-entry hits skip the callback entirely.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockPoisoned`] when a shard's `RwLock` was
    /// poisoned.
    pub(crate) fn get(&self, hash: KeyHash, key: &[u8]) -> Result<Option<u64>> {
        let shard_idx = (hash & SHARD_MASK) as usize;
        let shard = self.shards[shard_idx]
            .read()
            .map_err(|_poisoned| Error::LockPoisoned)?;
        match shard.get(&hash) {
            None => Ok(None),
            Some(Slot::Single(offset)) => Ok(Some(*offset)),
            Some(Slot::Multi(entries)) => {
                for (k, off) in entries {
                    if k.as_slice() == key {
                        return Ok(Some(*off));
                    }
                }
                Ok(None)
            }
        }
    }

    /// Replace the offset for `key`, with a resolver callback that the
    /// index uses to disambiguate hash collisions. The resolver is
    /// invoked only when an existing `Single` slot needs to be checked
    /// against the new key — it should return the key bytes stored at
    /// the supplied offset (i.e., decode the on-disk record at that
    /// offset and return its key).
    ///
    /// Behaviour:
    /// - Empty slot → `Single(offset)`, returns `Ok(None)`.
    /// - `Single(existing_offset)` and resolver says key matches →
    ///   in-place update, returns `Ok(Some(existing_offset))`.
    /// - `Single(existing_offset)` and resolver says key differs →
    ///   promote to `Multi` carrying both `(existing_key, existing_offset)`
    ///   and `(new_key, offset)`. Returns `Ok(None)`.
    /// - `Multi(_)` → linear scan; matching key is replaced, otherwise
    ///   the new entry is pushed.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockPoisoned`] on poisoned shard lock, or any
    /// error returned by `resolve_existing`.
    pub(crate) fn replace<F>(
        &self,
        hash: KeyHash,
        key: &[u8],
        offset: u64,
        mut resolve_existing: F,
    ) -> Result<Option<u64>>
    where
        F: FnMut(u64) -> Result<Option<Vec<u8>>>,
    {
        let shard_idx = (hash & SHARD_MASK) as usize;
        let mut shard = self.shards[shard_idx]
            .write()
            .map_err(|_poisoned| Error::LockPoisoned)?;
        match shard.get_mut(&hash) {
            None => {
                let _prev = shard.insert(hash, Slot::Single(offset));
                Ok(None)
            }
            Some(slot) => match slot {
                Slot::Single(existing_offset) => {
                    let existing = *existing_offset;
                    // Resolve the existing offset's key. If unresolvable
                    // (record gone / decode failed), treat as a stale
                    // entry and overwrite in place.
                    match resolve_existing(existing)? {
                        Some(existing_key) if existing_key.as_slice() == key => {
                            *existing_offset = offset;
                            Ok(Some(existing))
                        }
                        Some(existing_key) => {
                            *slot =
                                Slot::Multi(vec![(existing_key, existing), (key.to_vec(), offset)]);
                            Ok(None)
                        }
                        None => {
                            *existing_offset = offset;
                            Ok(Some(existing))
                        }
                    }
                }
                Slot::Multi(entries) => {
                    for entry in entries.iter_mut() {
                        if entry.0.as_slice() == key {
                            let prev = entry.1;
                            entry.1 = offset;
                            return Ok(Some(prev));
                        }
                    }
                    entries.push((key.to_vec(), offset));
                    Ok(None)
                }
            },
        }
    }

    /// Remove the entry for `key`. Returns the previous offset if any.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockPoisoned`] on poisoned shard lock.
    pub(crate) fn remove(&self, hash: KeyHash, key: &[u8]) -> Result<Option<u64>> {
        let shard_idx = (hash & SHARD_MASK) as usize;
        let mut shard = self.shards[shard_idx]
            .write()
            .map_err(|_poisoned| Error::LockPoisoned)?;
        match shard.get_mut(&hash) {
            None => Ok(None),
            Some(slot) => match slot {
                Slot::Single(offset) => {
                    let prev = *offset;
                    let _removed = shard.remove(&hash);
                    Ok(Some(prev))
                }
                Slot::Multi(entries) => {
                    let mut matched: Option<u64> = None;
                    entries.retain(|(k, off)| {
                        if matched.is_none() && k.as_slice() == key {
                            matched = Some(*off);
                            false
                        } else {
                            true
                        }
                    });
                    if entries.is_empty() {
                        let _removed = shard.remove(&hash);
                    } else if entries.len() == 1 {
                        let (_k, off) = entries[0].clone();
                        *slot = Slot::Single(off);
                    }
                    Ok(matched)
                }
            },
        }
    }

    /// Total live entry count across every shard. O(shards).
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockPoisoned`] on poisoned shard lock.
    pub(crate) fn len(&self) -> Result<usize> {
        let mut total = 0;
        for shard in self.shards.iter() {
            let guard = shard.read().map_err(|_poisoned| Error::LockPoisoned)?;
            for slot in guard.values() {
                total += match slot {
                    Slot::Single(_) => 1,
                    Slot::Multi(entries) => entries.len(),
                };
            }
        }
        Ok(total)
    }

    /// Drop every entry. O(total).
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockPoisoned`] on poisoned shard lock.
    pub(crate) fn clear(&self) -> Result<()> {
        for shard in self.shards.iter() {
            let mut guard = shard.write().map_err(|_poisoned| Error::LockPoisoned)?;
            guard.clear();
        }
        Ok(())
    }

    /// Iterate every (key, offset) pair across every shard. The keys
    /// are reconstructed from the engine's record reads — this iter
    /// only emits offsets, and the engine resolves them to keys.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockPoisoned`] on poisoned shard lock.
    pub(crate) fn collect_offsets(&self) -> Result<Vec<u64>> {
        let mut out = Vec::new();
        for shard in self.shards.iter() {
            let guard = shard.read().map_err(|_poisoned| Error::LockPoisoned)?;
            for slot in guard.values() {
                match slot {
                    Slot::Single(off) => out.push(*off),
                    Slot::Multi(entries) => {
                        for (_, off) in entries {
                            out.push(*off);
                        }
                    }
                }
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test resolver that always reports the slot's existing key as
    /// the supplied `existing` for the test setup. Tests construct a
    /// closure-shaped resolver via [`make_resolver`].
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
        // Resolver returns the existing key so replace updates in-place.
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
        // Force a collision by inserting two distinct keys at the same hash.
        // First insert lands as Single (no existing slot).
        let _ = idx.replace(42, b"first", 100, no_resolver).unwrap();
        // Second insert sees the Single and the resolver supplies the
        // existing key as "first" — the index promotes to Multi with both.
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
}
