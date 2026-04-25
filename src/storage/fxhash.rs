// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! In-tree FxHash port — fast, deterministic, non-cryptographic 64-bit hash.
//!
//! Used as the keymap hash in the v0.7 storage engine. We need:
//!
//! - Fast on small keys (typical user-supplied bytes are 4–64 bytes long).
//! - Deterministic across runs so on-disk bloom filters stay valid.
//! - Good enough avalanche to spread keys across keymap shards uniformly.
//! - Zero dependencies.
//!
//! This is the same algorithm `rustc-hash` uses, written from scratch here so
//! we do not pull in another crate. It is **not** cryptographically strong —
//! we never use it for security, only for distributing keys across buckets
//! and shards.
//!
//! The hash processes the input 8 bytes at a time on 64-bit targets, with a
//! tail loop for the remaining 0–7 bytes. On modern CPUs this is roughly one
//! cycle per byte for keys long enough to amortise the loop overhead.

/// Magic constant from rustc-hash (a.k.a. the FxHash seed). The value comes
/// from `0x517c_c1b7_2722_0a95`; the literal is repeated here so we never
/// hard-code it more than once.
const FX_SEED64: u64 = 0x517c_c1b7_2722_0a95;

/// Number of bits to rotate the running state by between mixes. Chosen by
/// the original FxHash design to spread input bytes across all output bits.
const FX_ROTATE: u32 = 5;

/// Compute the FxHash of `bytes`.
///
/// Returns a 64-bit non-cryptographic hash with good avalanche on typical
/// key data. Empty input hashes to zero.
#[must_use]
pub(crate) fn hash(bytes: &[u8]) -> u64 {
    let mut state = 0_u64;
    let mut cursor = 0_usize;

    // Eight-byte stride: process whole little-endian words.
    while cursor + 8 <= bytes.len() {
        let mut word = [0_u8; 8];
        word.copy_from_slice(&bytes[cursor..cursor + 8]);
        state = mix(state, u64::from_le_bytes(word));
        cursor += 8;
    }

    // Tail: remaining 0..7 bytes folded one at a time.
    while cursor < bytes.len() {
        state = mix(state, u64::from(bytes[cursor]));
        cursor += 1;
    }

    state
}

#[inline]
const fn mix(state: u64, word: u64) -> u64 {
    (state.rotate_left(FX_ROTATE) ^ word).wrapping_mul(FX_SEED64)
}

#[cfg(test)]
mod tests {
    use super::hash;

    #[test]
    fn empty_input_hashes_to_zero() {
        assert_eq!(hash(&[]), 0);
    }

    #[test]
    fn hash_is_deterministic() {
        let key = b"the quick brown fox";
        assert_eq!(hash(key), hash(key));
    }

    #[test]
    fn different_inputs_produce_different_hashes() {
        // Not a guaranteed property of all inputs but a reasonable smoke test
        // for the small inputs the keymap actually sees.
        let mut seen = std::collections::HashSet::new();
        for i in 0_u32..1024 {
            let key = format!("key-{i}");
            let h = hash(key.as_bytes());
            assert!(seen.insert(h), "duplicate hash for {key}");
        }
    }

    #[test]
    fn distribution_across_low_bits_is_balanced() {
        // Smoke-test that the bottom 5 bits (used for shard selection) spread
        // a simple key space across all 32 buckets.
        let mut buckets = [0_u32; 32];
        for i in 0_u32..10_000 {
            let key = format!("k{i}");
            buckets[(hash(key.as_bytes()) & 31) as usize] += 1;
        }
        for count in buckets {
            assert!(count > 0, "shard distribution missed a bucket");
        }
    }

    #[test]
    fn matches_documented_seed_constant() {
        // The single-byte input `[1]` exercises the simplest possible mix:
        // state starts at 0, rotate_left(5) of 0 is 0, XOR with 1 is 1,
        // multiply by FX_SEED64 yields exactly FX_SEED64. This locks the
        // algorithm to the documented seed so accidental constant edits
        // surface immediately.
        assert_eq!(hash(&[1]), 0x517c_c1b7_2722_0a95);
        // Zero input through any number of mixes stays at zero.
        assert_eq!(hash(&[0]), 0);
        assert_eq!(hash(&[0; 16]), 0);
    }

    #[test]
    fn long_input_completes_in_eight_byte_strides_then_tail() {
        // Input of 13 bytes exercises both the 8-byte stride and the tail.
        let key = b"thirteen-byte";
        assert_eq!(key.len(), 13);
        let h = hash(key);
        assert_ne!(h, 0);
        // Hashing the same prefix twice (16 bytes vs 13) yields different results.
        let key2 = b"thirteen-bytefoo";
        assert_ne!(h, hash(key2));
    }
}
