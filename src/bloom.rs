// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! In-memory Bloom filter for negative-lookup acceleration.
//!
//! The v0.7 read path consults a per-namespace Bloom filter before touching
//! the keymap. A negative answer is **definitive** ("the key is not in the
//! database"); a positive answer is probabilistic and the caller falls
//! through to the keymap and page-cache layers. False positives waste a
//! lookup, but never cause a wrong answer.
//!
//! ## Sizing
//!
//! Targeting a 1% false-positive rate the optimal parameters are:
//!
//! - `m / n ≈ 9.585` bits per key.
//! - `k = 7` hash functions.
//!
//! We use 10 bits per key (slightly conservative, friendly to byte
//! alignment) and 7 hashes. Memory per namespace: `1.25 MB` per million
//! keys, sized once at open time from the catalog's `record_count`.
//!
//! ## Hashing
//!
//! The two-hash trick (Kirsch & Mitzenmacher, 2006) lets us derive `k`
//! independent hashes from a single 64-bit hash. We split the keymap's
//! 64-bit FxHash into a `(low: u32, high: u32)` pair and compute hash
//! `i ∈ 0..k` as `low + i * high` modulo `m`. The bias is negligible at
//! `m / n ≈ 10`.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::{Error, Result};

/// Bits per key. Chosen to keep the false-positive rate near 1%.
pub(crate) const BITS_PER_KEY: u32 = 10;

/// Number of hash functions per insert/lookup.
pub(crate) const HASH_COUNT: u32 = 7;

/// Lower bound on the bit array size. A tiny namespace (a handful of keys)
/// still gets a usable bloom rather than one that saturates instantly.
/// Power-of-two so the modular index calculation reduces to a mask.
const MIN_BITS: u64 = 1024;

/// Compute the bit count for a fresh bloom sized for `keys` items.
///
/// The result is **always a power of two** so the per-lookup `bit_idx %
/// bit_count` reduces to `bit_idx & (bit_count - 1)` — a single AND
/// instruction on every hash function in the hot path.
fn sized_bits_for_keys(keys: u64) -> u64 {
    let raw = keys.saturating_mul(u64::from(BITS_PER_KEY));
    let lower_bound = raw.max(MIN_BITS);
    if lower_bound.is_power_of_two() {
        return lower_bound;
    }
    lower_bound
        .checked_next_power_of_two()
        .unwrap_or(1_u64 << 63)
}

/// Bloom filter implementation.
///
/// `Bloom` is `Sync` because mutations go through atomic word stores and
/// only set bits (never clear), so a missed atomic OR is safe — at worst we
/// observe a false negative briefly until the next set, which is impossible
/// because we never clear bits. The underlying invariant ("set bits do not
/// vanish") is what makes the filter correct under concurrent inserts and
/// concurrent reads without locks.
///
/// The bit array is held as a fixed-size `Box<[AtomicU64]>` rather than a
/// `Vec`: once constructed the bloom never resizes, and `Box<[_]>` is
/// 16 bytes lighter than `Vec` in the struct, which matters when
/// thousands of namespaces each carry their own bloom.
#[derive(Debug)]
pub(crate) struct Bloom {
    /// Bit array stored as `u64` words for cheap atomic OR.
    words: Box<[AtomicU64]>,
    /// Total bit count. Equal to `words.len() * 64`. Always a power of two
    /// so the per-hash modular reduction is a single AND.
    bit_count: u64,
    /// `bit_count - 1`. Pre-computed so the hot path does one AND, not an
    /// AND plus a subtract.
    bit_mask: u64,
}

impl Bloom {
    /// Construct a new bloom sized for at least `expected_keys` items.
    #[must_use]
    pub(crate) fn for_keys(expected_keys: u64) -> Self {
        let bit_count = sized_bits_for_keys(expected_keys);
        let word_count = (bit_count / 64) as usize;
        let mut words: Vec<AtomicU64> = Vec::with_capacity(word_count);
        for _ in 0..word_count {
            words.push(AtomicU64::new(0));
        }
        Self {
            words: words.into_boxed_slice(),
            bit_count,
            bit_mask: bit_count - 1,
        }
    }

    /// Construct from raw bytes produced by [`Bloom::encode`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::Corrupted`] when `bytes.len()` is not a multiple of
    /// 8 or when the resulting bit count is not a power of two.
    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() % 8 != 0 {
            return Err(Error::Corrupted {
                offset: 0,
                reason: "bloom byte length is not a multiple of 8",
            });
        }
        let word_count = bytes.len() / 8;
        let bit_count = (word_count * 64) as u64;
        if bit_count == 0 || !bit_count.is_power_of_two() {
            return Err(Error::Corrupted {
                offset: 0,
                reason: "bloom bit count must be a positive power of two",
            });
        }
        let mut words: Vec<AtomicU64> = Vec::with_capacity(word_count);
        for i in 0..word_count {
            let off = i * 8;
            let mut buf = [0_u8; 8];
            buf.copy_from_slice(&bytes[off..off + 8]);
            words.push(AtomicU64::new(u64::from_le_bytes(buf)));
        }
        Ok(Self {
            words: words.into_boxed_slice(),
            bit_count,
            bit_mask: bit_count - 1,
        })
    }

    /// Encode the filter as a contiguous byte buffer in little-endian order.
    /// Each `u64` word is written as eight bytes; `from_bytes` is the
    /// inverse.
    #[must_use]
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.words.len() * 8);
        for word in &self.words {
            out.extend_from_slice(&word.load(Ordering::Relaxed).to_le_bytes());
        }
        out
    }

    /// Total bit count.
    #[must_use]
    pub(crate) const fn bit_count(&self) -> u64 {
        self.bit_count
    }

    /// Total word count (`bit_count / 64`).
    #[must_use]
    pub(crate) fn word_count(&self) -> usize {
        self.words.len()
    }

    /// Insert a key (identified by its 64-bit FxHash) into the filter.
    ///
    /// Concurrent inserts and concurrent `contains` calls are safe; both
    /// proceed under `AtomicU64::fetch_or` and never block.
    pub(crate) fn insert(&self, hash: u64) {
        for bit_idx in self.bit_indices(hash) {
            let word = bit_idx / 64;
            let mask = 1_u64 << (bit_idx % 64);
            // SAFETY-equivalent: word index is bounded by `bit_indices`'s
            // modulo-`bit_count` arithmetic, which guarantees `< bit_count`,
            // so `word < words.len()`.
            let _previous = self.words[word as usize].fetch_or(mask, Ordering::Relaxed);
        }
    }

    /// `true` when the key may be present (subject to the configured FPR);
    /// `false` is a definitive "the key has never been inserted".
    #[must_use]
    pub(crate) fn contains(&self, hash: u64) -> bool {
        for bit_idx in self.bit_indices(hash) {
            let word = bit_idx / 64;
            let mask = 1_u64 << (bit_idx % 64);
            let current = self.words[word as usize].load(Ordering::Relaxed);
            if current & mask == 0 {
                return false;
            }
        }
        true
    }

    /// Clear every bit. Concurrent `contains` calls in flight at the time
    /// of `clear` may briefly return false negatives; this is acceptable
    /// because clear is only called during catastrophic operations
    /// (`db.clear`, namespace drop) where the underlying records are also
    /// gone.
    pub(crate) fn clear(&self) {
        for word in &self.words {
            word.store(0, Ordering::Relaxed);
        }
    }

    fn bit_indices(&self, hash: u64) -> impl Iterator<Item = u64> + '_ {
        // Two-hash trick (Kirsch & Mitzenmacher 2006): derive `k`
        // independent indices from one 64-bit hash by taking
        // `(low + i * high) mod bit_count`. With a power-of-two
        // bit_count, the modulo collapses to a mask.
        let low = hash as u32 as u64;
        let high = hash >> 32;
        let mask = self.bit_mask;
        (0..u64::from(HASH_COUNT)).map(move |i| low.wrapping_add(i.wrapping_mul(high)) & mask)
    }
}

#[cfg(test)]
mod tests {
    use super::{sized_bits_for_keys, Bloom, BITS_PER_KEY, HASH_COUNT, MIN_BITS};

    fn h(key: &[u8]) -> u64 {
        crate::storage::fxhash::hash(key)
    }

    #[test]
    fn fresh_bloom_reports_no_keys_present() {
        let bloom = Bloom::for_keys(1024);
        assert!(!bloom.contains(h(b"missing")));
        assert!(!bloom.contains(h(b"also-missing")));
    }

    #[test]
    fn inserted_key_is_definitely_reported_as_present() {
        let bloom = Bloom::for_keys(1024);
        bloom.insert(h(b"alpha"));
        assert!(bloom.contains(h(b"alpha")));
    }

    #[test]
    fn false_positive_rate_stays_below_two_percent_at_capacity() {
        // Insert exactly `n` distinct keys, then probe `n` distinct
        // never-inserted keys and count how many the filter flags as
        // "present". With 10 bits/key and 7 hashes the rate should be
        // around 1%; we allow 2% to leave room for the variance of small
        // sample sizes.
        let n: u64 = 5_000;
        let bloom = Bloom::for_keys(n);
        for i in 0..n {
            let key = format!("inserted-{i}");
            bloom.insert(h(key.as_bytes()));
        }

        let mut false_positives = 0_u64;
        for i in 0..n {
            let key = format!("never-inserted-{i}");
            if bloom.contains(h(key.as_bytes())) {
                false_positives += 1;
            }
        }
        let rate = false_positives as f64 / n as f64;
        assert!(rate < 0.02, "false-positive rate {rate} exceeded 2%");
    }

    #[test]
    fn bit_count_rounds_up_to_word_boundary_and_meets_minimum() {
        // Tiny key count forces the MIN_BITS floor.
        let bloom = Bloom::for_keys(1);
        assert!(bloom.bit_count() >= MIN_BITS);
        assert_eq!(bloom.bit_count() % 64, 0);

        // Larger key count uses BITS_PER_KEY directly.
        let bloom = Bloom::for_keys(10_000);
        let expected_min = u64::from(BITS_PER_KEY) * 10_000;
        assert!(bloom.bit_count() >= expected_min);
        assert_eq!(bloom.bit_count() % 64, 0);
    }

    #[test]
    fn encode_round_trips_through_from_bytes() {
        let bloom = Bloom::for_keys(1024);
        for i in 0..200_u64 {
            bloom.insert(h(format!("k{i}").as_bytes()));
        }
        let encoded = bloom.encode();
        let decoded = match Bloom::from_bytes(&encoded) {
            Ok(b) => b,
            Err(err) => panic!("from_bytes should succeed: {err}"),
        };
        for i in 0..200_u64 {
            assert!(decoded.contains(h(format!("k{i}").as_bytes())));
        }
        assert_eq!(decoded.bit_count(), bloom.bit_count());
    }

    #[test]
    fn from_bytes_rejects_unaligned_input() {
        let bytes = vec![0_u8; 7];
        let bloom = Bloom::from_bytes(&bytes);
        assert!(bloom.is_err());
    }

    #[test]
    fn clear_drops_every_inserted_key() {
        let bloom = Bloom::for_keys(1024);
        bloom.insert(h(b"alpha"));
        bloom.insert(h(b"beta"));
        assert!(bloom.contains(h(b"alpha")));
        bloom.clear();
        assert!(!bloom.contains(h(b"alpha")));
        assert!(!bloom.contains(h(b"beta")));
    }

    #[test]
    fn sized_bits_helper_returns_a_power_of_two() {
        // The mask shortcut on the hot path requires this. If sizing ever
        // drifts to a non-power-of-two, the per-hash modular reduction
        // silently produces wrong bit indices.
        for keys in [0_u64, 1, 100, 999, 10_000_000] {
            let bits = sized_bits_for_keys(keys);
            assert!(
                bits.is_power_of_two(),
                "{bits} bits is not a power of two for {keys} keys"
            );
            assert!(bits >= MIN_BITS);
        }
    }

    #[test]
    fn bit_mask_lookup_matches_modular_reference() {
        // Cross-check the optimised mask path against a slow modulo
        // implementation on a range of inputs.
        let bloom = Bloom::for_keys(4096);
        let bit_count = bloom.bit_count();
        for hash in [0_u64, 1, 0xFFFF_FFFF, 0xDEAD_BEEF_CAFE_BABE, u64::MAX] {
            let low = hash as u32 as u64;
            let high = hash >> 32;
            for i in 0..7_u64 {
                let modular = low.wrapping_add(i.wrapping_mul(high)) % bit_count;
                let masked = low.wrapping_add(i.wrapping_mul(high)) & (bit_count - 1);
                assert_eq!(modular, masked);
            }
        }
    }

    #[test]
    fn hash_count_constant_is_seven() {
        // Locked down because the design assumes k = 7.
        assert_eq!(HASH_COUNT, 7);
    }

    #[test]
    fn concurrent_inserts_do_not_lose_set_bits() {
        // Two threads inserting different keys must not race each other —
        // every inserted key must be `contains`-positive after both threads
        // join. Atomic `fetch_or` semantics make this trivially correct;
        // this test documents the invariant.
        use std::sync::Arc;
        let bloom = Arc::new(Bloom::for_keys(4096));
        let bloom_a = Arc::clone(&bloom);
        let bloom_b = Arc::clone(&bloom);

        let a = std::thread::spawn(move || {
            for i in 0..1024_u64 {
                bloom_a.insert(h(format!("a{i}").as_bytes()));
            }
        });
        let b = std::thread::spawn(move || {
            for i in 0..1024_u64 {
                bloom_b.insert(h(format!("b{i}").as_bytes()));
            }
        });
        let _ = a.join();
        let _ = b.join();

        for i in 0..1024_u64 {
            assert!(bloom.contains(h(format!("a{i}").as_bytes())));
            assert!(bloom.contains(h(format!("b{i}").as_bytes())));
        }
    }
}
