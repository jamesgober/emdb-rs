// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Optional payload compression for WAL records and overflow values.
//!
//! When the `compress` feature is enabled this module exposes
//! [`compress_into`] / [`decompress_into`] backed by `lz4_flex`. When the
//! feature is disabled the same functions are present but reduce to
//! identity copies — every caller compiles unchanged regardless of the
//! feature state.
//!
//! ## Threshold
//!
//! Compression has a fixed per-call overhead (LZ4 block header, dictionary
//! warm-up). For payloads smaller than [`COMPRESS_MIN_BYTES`] the overhead
//! exceeds the saving, so [`compress_into`] returns `Compressed::Passthrough`
//! and the caller writes the original bytes unchanged. Decompression is
//! told via the `compressed` flag whether the bytes need decoding.

use crate::{Error, Result};

/// Minimum payload size that triggers compression. Below this the overhead
/// exceeds typical savings; the caller writes the raw bytes and a clear
/// "not compressed" flag.
pub(crate) const COMPRESS_MIN_BYTES: usize = 256;

/// Result of a compression attempt.
#[derive(Debug)]
pub(crate) enum Compressed<'a> {
    /// Input was below [`COMPRESS_MIN_BYTES`] or compressed worse than
    /// the original; the caller should write `bytes` unchanged with a
    /// "not compressed" flag.
    Passthrough { bytes: &'a [u8] },
    /// Compressed output. The caller should write `bytes` and record
    /// `original_len` so the decoder knows the destination size.
    Encoded { bytes: Vec<u8>, original_len: u32 },
}

/// Compress `input` if it is at least [`COMPRESS_MIN_BYTES`] AND the
/// compressed payload is strictly smaller than the original.
///
/// When the `compress` feature is disabled, always returns
/// [`Compressed::Passthrough`].
#[cfg(feature = "compress")]
#[must_use]
pub(crate) fn compress_into(input: &[u8]) -> Compressed<'_> {
    if input.len() < COMPRESS_MIN_BYTES {
        return Compressed::Passthrough { bytes: input };
    }
    let encoded = lz4_flex::compress(input);
    if encoded.len() >= input.len() {
        // Compression made things worse. Pass through.
        return Compressed::Passthrough { bytes: input };
    }
    Compressed::Encoded {
        bytes: encoded,
        original_len: input.len() as u32,
    }
}

/// Compress `input` (no-op when the feature is disabled).
#[cfg(not(feature = "compress"))]
#[must_use]
pub(crate) fn compress_into(input: &[u8]) -> Compressed<'_> {
    Compressed::Passthrough { bytes: input }
}

/// Decompress `input` into `out`. The `original_len` argument is only
/// consulted when `compressed` is true; for passthrough the caller can
/// pass any value.
///
/// # Errors
///
/// Returns [`Error::Corrupted`] when the LZ4 decoder rejects the input,
/// or when the decoded length differs from `original_len`.
#[cfg(feature = "compress")]
pub(crate) fn decompress_into(
    input: &[u8],
    compressed: bool,
    original_len: u32,
    out: &mut Vec<u8>,
) -> Result<()> {
    if !compressed {
        out.extend_from_slice(input);
        return Ok(());
    }

    let target = original_len as usize;
    let decoded =
        lz4_flex::decompress(input, target).map_err(|_decompress_err| Error::Corrupted {
            offset: 0,
            reason: "lz4 decompress rejected the input",
        })?;
    if decoded.len() != target {
        return Err(Error::Corrupted {
            offset: 0,
            reason: "decompressed length does not match original_len",
        });
    }
    out.extend_from_slice(&decoded);
    Ok(())
}

/// Decompress `input` into `out` (no-op when the feature is disabled —
/// `compressed` must always be `false`).
///
/// # Errors
///
/// Returns [`Error::InvalidConfig`] when called with `compressed = true`
/// but the `compress` feature is not enabled in this build.
#[cfg(not(feature = "compress"))]
pub(crate) fn decompress_into(
    input: &[u8],
    compressed: bool,
    _original_len: u32,
    out: &mut Vec<u8>,
) -> Result<()> {
    if compressed {
        return Err(Error::InvalidConfig(
            "record marked compressed but the `compress` feature is not enabled",
        ));
    }
    out.extend_from_slice(input);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{compress_into, decompress_into, Compressed, COMPRESS_MIN_BYTES};

    #[test]
    fn small_input_passes_through() {
        let small = b"hello";
        match compress_into(small) {
            Compressed::Passthrough { bytes } => assert_eq!(bytes, small),
            Compressed::Encoded { .. } => panic!("small input should not compress"),
        }
    }

    #[cfg(feature = "compress")]
    #[test]
    fn large_repetitive_input_compresses() {
        let payload = vec![b'x'; 4 * COMPRESS_MIN_BYTES];
        match compress_into(&payload) {
            Compressed::Passthrough { .. } => {
                panic!("large repetitive input should compress")
            }
            Compressed::Encoded {
                bytes,
                original_len,
            } => {
                assert!(bytes.len() < payload.len());
                assert_eq!(original_len as usize, payload.len());
                let mut out = Vec::new();
                let decoded = decompress_into(&bytes, true, original_len, &mut out);
                assert!(decoded.is_ok());
                assert_eq!(out, payload);
            }
        }
    }

    #[cfg(feature = "compress")]
    #[test]
    fn random_input_falls_back_to_passthrough_when_compression_grows_it() {
        // 256 bytes of unique content tends not to compress meaningfully.
        // The function returns Passthrough whenever encoded.len() >=
        // original.len(); we cannot guarantee which branch lz4 picks for
        // a random buffer, but we CAN guarantee correctness on either.
        let payload: Vec<u8> = (0..COMPRESS_MIN_BYTES as u8).collect();
        let (encoded_bytes, compressed_flag, original_len) = match compress_into(&payload) {
            Compressed::Passthrough { bytes } => (bytes.to_vec(), false, bytes.len() as u32),
            Compressed::Encoded {
                bytes,
                original_len,
            } => (bytes, true, original_len),
        };
        let mut out = Vec::new();
        let decoded = decompress_into(&encoded_bytes, compressed_flag, original_len, &mut out);
        assert!(decoded.is_ok());
        assert_eq!(out, payload);
    }

    #[test]
    fn passthrough_round_trips_with_compressed_false() {
        let payload = b"hello world";
        let mut out = Vec::new();
        let decoded = decompress_into(payload, false, payload.len() as u32, &mut out);
        assert!(decoded.is_ok());
        assert_eq!(out, payload);
    }

    #[cfg(not(feature = "compress"))]
    #[test]
    fn compressed_flag_without_feature_returns_invalid_config() {
        let mut out = Vec::new();
        let decoded = decompress_into(&[1, 2, 3], true, 3, &mut out);
        assert!(decoded.is_err());
    }

    #[test]
    fn threshold_constant_is_locked_for_format_compatibility() {
        // The on-disk format uses this threshold to decide whether to
        // encode the FLAG_COMPRESSED bit. Changing it requires a new
        // format version because old WALs would mis-decode their
        // header bits.
        assert_eq!(COMPRESS_MIN_BYTES, 256);
    }
}
