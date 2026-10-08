// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! On-disk record body format.
//!
//! v0.9 delegates outer framing (length prefix + CRC) to fsys's
//! journal. emdb's "record" is the *payload* fsys carries inside
//! a frame: a single tag byte plus a body. The body's shape
//! depends on the tag kind (insert / remove / namespace-name).
//!
//! ## Payload layout
//!
//! ```text
//!   bytes  field    notes
//!   -----  -----    -----
//!     0    tag      bit 0..6: kind (0=Insert, 1=Remove, 2=NamespaceName)
//!                   bit 7   : encrypted flag
//!     1+   body     payload-kind-specific bytes (see below);
//!                   for encrypted records this is `[nonce][ciphertext]`
//! ```
//!
//! Body for `TAG_INSERT` (plaintext):
//! `[ns_id u32][key_len u32][key][value_len u32][value][expires_at u64]`
//!
//! Body for `TAG_REMOVE` (plaintext):
//! `[ns_id u32][key_len u32][key]`
//!
//! Body for `TAG_NAMESPACE_NAME` (plaintext):
//! `[ns_id u32][name_len u32][name]`
//!
//! For encrypted records the body is `[nonce 12][ciphertext + AEAD tag]`.
//! The plaintext under the ciphertext has the same shape as an
//! unencrypted body of the same kind.
//!
//! ## Strict decoding
//!
//! Every body decoder requires the decoded fields to account for the
//! body exactly: a body with bytes left over after its last field is
//! rejected as [`Error::Corrupted`], and all offset arithmetic is
//! checked so a hostile length field cannot overflow `usize` on
//! 32-bit targets. emdb's encoders never write padding, so every
//! record a 1.0.x release wrote still decodes.
//!
//! The tag byte sits outside the AEAD ciphertext in the 1.0 format,
//! so it is not authenticated. Exact-length decoding is what stops an
//! attacker from flipping an encrypted `Insert` into a `Remove` or a
//! `NamespaceName` (both shorter layouts that would otherwise decode
//! from a prefix of the insert body). The engine's recovery scan adds
//! the namespace-binding checks that catch the remaining swaps.
//! Binding the tag into the AEAD associated data needs an on-disk
//! format revision and is planned for 1.1.

use crate::{Error, Result};

/// Record tag byte constants.
pub(crate) const TAG_INSERT: u8 = 0;
pub(crate) const TAG_REMOVE: u8 = 1;
/// Namespace-name binding. Body is
/// `[ns_id: u32][name_len: u32][name]`. Replayed on open to
/// rebuild the in-memory `name → id` map. The default
/// namespace (`ns_id = 0`, empty name) is implicit and never
/// emits a record of this kind.
pub(crate) const TAG_NAMESPACE_NAME: u8 = 2;
/// Set on the high bit of the tag byte for AEAD-encrypted
/// records.
pub(crate) const TAG_ENCRYPTED_FLAG: u8 = 0x80;
/// Mask for the tag's kind portion (bits 0..6).
pub(crate) const TAG_KIND_MASK: u8 = 0x7F;

/// AEAD nonce length in bytes (12-byte / 96-bit nonce).
pub(crate) const NONCE_LEN: usize = 12;
/// AEAD authentication tag length in bytes (16-byte / 128-bit tag).
pub(crate) const TAG_LEN: usize = 16;

/// Borrowed view of a decoded record body. Lifetime is tied to
/// the underlying buffer (mmap slice or in-memory Vec).
#[derive(Debug)]
pub(crate) enum RecordView<'a> {
    Insert {
        ns_id: u32,
        key: &'a [u8],
        value: &'a [u8],
        expires_at: u64,
    },
    Remove {
        ns_id: u32,
        key: &'a [u8],
    },
    NamespaceName {
        ns_id: u32,
        name: &'a [u8],
    },
}

/// Owned record (used when the source bytes are not directly
/// addressable — e.g. after AEAD decryption produces a fresh
/// `Vec<u8>`).
#[derive(Debug)]
pub(crate) enum OwnedRecord {
    Insert {
        ns_id: u32,
        key: Vec<u8>,
        value: Vec<u8>,
        expires_at: u64,
    },
    Remove {
        ns_id: u32,
        key: Vec<u8>,
    },
    NamespaceName {
        ns_id: u32,
        name: Vec<u8>,
    },
}

impl OwnedRecord {
    pub(crate) fn ns_id(&self) -> u32 {
        match self {
            Self::Insert { ns_id, .. }
            | Self::Remove { ns_id, .. }
            | Self::NamespaceName { ns_id, .. } => *ns_id,
        }
    }
}

// ─────────────────────────────────────────────────────────────────
// Primitive read/write helpers.
// ─────────────────────────────────────────────────────────────────

#[inline]
pub(crate) fn write_u32(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_le_bytes());
}

#[inline]
pub(crate) fn write_u64(buf: &mut Vec<u8>, value: u64) {
    buf.extend_from_slice(&value.to_le_bytes());
}

/// Return `start + len` when the range `start..start + len` lies
/// inside a buffer of `buf_len` bytes; `None` on overflow or overrun.
#[inline]
fn checked_end(start: usize, len: usize, buf_len: usize) -> Option<usize> {
    match start.checked_add(len) {
        Some(end) if end <= buf_len => Some(end),
        _ => None,
    }
}

#[inline]
pub(crate) fn read_u32(bytes: &[u8], offset: usize) -> Result<u32> {
    let end = checked_end(offset, 4, bytes.len()).ok_or(Error::Corrupted {
        offset: offset as u64,
        reason: "u32 read past end of buffer",
    })?;
    let mut buf = [0_u8; 4];
    buf.copy_from_slice(&bytes[offset..end]);
    Ok(u32::from_le_bytes(buf))
}

#[inline]
pub(crate) fn read_u64(bytes: &[u8], offset: usize) -> Result<u64> {
    let end = checked_end(offset, 8, bytes.len()).ok_or(Error::Corrupted {
        offset: offset as u64,
        reason: "u64 read past end of buffer",
    })?;
    let mut buf = [0_u8; 8];
    buf.copy_from_slice(&bytes[offset..end]);
    Ok(u64::from_le_bytes(buf))
}

/// Read a `[len: u32][bytes]` field starting at `offset` and return
/// the byte slice plus the offset just past it. `reason` names the
/// field for the error when the declared length runs past the body.
#[inline]
fn read_len_prefixed<'a>(
    body: &'a [u8],
    offset: usize,
    reason: &'static str,
) -> Result<(&'a [u8], usize)> {
    let len = read_u32(body, offset)? as usize;
    let truncated = || Error::Corrupted {
        offset: offset as u64,
        reason,
    };
    let start = checked_end(offset, 4, body.len()).ok_or_else(truncated)?;
    let end = checked_end(start, len, body.len()).ok_or_else(truncated)?;
    Ok((&body[start..end], end))
}

/// Reject a body whose decoded fields end before the body does.
#[inline]
fn require_exact_end(body: &[u8], end: usize, reason: &'static str) -> Result<()> {
    if end == body.len() {
        Ok(())
    } else {
        Err(Error::Corrupted {
            offset: end as u64,
            reason,
        })
    }
}

// ─────────────────────────────────────────────────────────────────
// Body encoders (used by Engine to produce payloads for `Store::append`).
// ─────────────────────────────────────────────────────────────────

/// Encode an `Insert` record body (plaintext payload, no tag byte
/// or framing). Caller prepends the tag byte before passing the
/// payload to [`crate::storage::store::Store::append`].
pub(crate) fn encode_insert_body(
    out: &mut Vec<u8>,
    ns_id: u32,
    key: &[u8],
    value: &[u8],
    expires_at: u64,
) {
    write_u32(out, ns_id);
    write_u32(out, key.len() as u32);
    out.extend_from_slice(key);
    write_u32(out, value.len() as u32);
    out.extend_from_slice(value);
    write_u64(out, expires_at);
}

/// Encode a `Remove` record body.
pub(crate) fn encode_remove_body(out: &mut Vec<u8>, ns_id: u32, key: &[u8]) {
    write_u32(out, ns_id);
    write_u32(out, key.len() as u32);
    out.extend_from_slice(key);
}

/// Encode a `NamespaceName` record body.
pub(crate) fn encode_namespace_name_body(out: &mut Vec<u8>, ns_id: u32, name: &[u8]) {
    write_u32(out, ns_id);
    write_u32(out, name.len() as u32);
    out.extend_from_slice(name);
}

// ─────────────────────────────────────────────────────────────────
// Body decoders.
// ─────────────────────────────────────────────────────────────────

/// Decode a plaintext `Insert` record body. The body must be exactly
/// `[ns_id][key_len][key][value_len][value][expires_at]`.
pub(crate) fn decode_insert_body(body: &[u8]) -> Result<RecordView<'_>> {
    let ns_id = read_u32(body, 0)?;
    let (key, key_end) = read_len_prefixed(body, 4, "insert body truncated mid-key")?;
    let (value, value_end) = read_len_prefixed(body, key_end, "insert body truncated mid-value")?;
    let expires_at = read_u64(body, value_end)?;
    let end = checked_end(value_end, 8, body.len()).ok_or(Error::Corrupted {
        offset: value_end as u64,
        reason: "u64 read past end of buffer",
    })?;
    require_exact_end(body, end, "insert body has trailing bytes")?;
    Ok(RecordView::Insert {
        ns_id,
        key,
        value,
        expires_at,
    })
}

/// Decode a plaintext `Remove` record body. The body must be exactly
/// `[ns_id][key_len][key]`.
pub(crate) fn decode_remove_body(body: &[u8]) -> Result<RecordView<'_>> {
    let ns_id = read_u32(body, 0)?;
    let (key, key_end) = read_len_prefixed(body, 4, "remove body truncated mid-key")?;
    require_exact_end(body, key_end, "remove body has trailing bytes")?;
    Ok(RecordView::Remove { ns_id, key })
}

/// Decode a plaintext `NamespaceName` record body. The body must be
/// exactly `[ns_id][name_len][name]`.
pub(crate) fn decode_namespace_name_body(body: &[u8]) -> Result<RecordView<'_>> {
    let ns_id = read_u32(body, 0)?;
    let (name, name_end) = read_len_prefixed(body, 4, "namespace-name body truncated mid-name")?;
    require_exact_end(body, name_end, "namespace-name body has trailing bytes")?;
    Ok(RecordView::NamespaceName { ns_id, name })
}

// ─────────────────────────────────────────────────────────────────
// Payload-level decoders (tag byte + body, no outer framing).
// ─────────────────────────────────────────────────────────────────

/// Decode a v0.9 plaintext payload (tag byte + body bytes,
/// stripped of fsys's outer frame).
pub(crate) fn decode_payload(payload: &[u8]) -> Result<RecordView<'_>> {
    if payload.is_empty() {
        return Err(Error::Corrupted {
            offset: 0,
            reason: "empty record payload",
        });
    }
    let tag = payload[0];
    if (tag & TAG_ENCRYPTED_FLAG) != 0 {
        return Err(Error::Corrupted {
            offset: 0,
            reason: "encrypted record passed to plaintext decoder",
        });
    }
    let body = &payload[1..];
    match tag & TAG_KIND_MASK {
        TAG_INSERT => decode_insert_body(body),
        TAG_REMOVE => decode_remove_body(body),
        TAG_NAMESPACE_NAME => decode_namespace_name_body(body),
        unknown => Err(Error::Corrupted {
            offset: 0,
            reason: kind_error_for(unknown),
        }),
    }
}

/// Decode a v0.9 encrypted payload via an AEAD callback. Returns
/// an `OwnedRecord` because the plaintext needs to outlive the
/// local decrypt buffer.
///
/// A plaintext record (tag without [`TAG_ENCRYPTED_FLAG`]) is
/// rejected: an encrypted database never contains one, so finding one
/// means the file was modified. Every caller runs after the database
/// key was checked against the meta sidecar's verification block, so
/// an AEAD authentication failure reported by `decrypt`
/// (`Error::EncryptionKeyMismatch`) means the record bytes were
/// modified or damaged; it is returned as [`Error::Corrupted`].
///
/// The decrypted plaintext buffer is wiped before it is freed when
/// the `encrypt` feature is enabled; the returned record holds its
/// own copies of the fields.
pub(crate) fn decode_payload_encrypted<F>(payload: &[u8], decrypt: F) -> Result<OwnedRecord>
where
    F: FnOnce(&[u8; NONCE_LEN], &[u8]) -> Result<Vec<u8>>,
{
    if payload.len() < 1 + NONCE_LEN + TAG_LEN {
        return Err(Error::Corrupted {
            offset: 0,
            reason: "encrypted payload shorter than nonce + AEAD tag",
        });
    }
    let tag = payload[0];
    if (tag & TAG_ENCRYPTED_FLAG) == 0 {
        return Err(Error::Corrupted {
            offset: 0,
            reason: "plaintext record passed to encrypted decoder",
        });
    }
    let kind = tag & TAG_KIND_MASK;
    let mut nonce = [0_u8; NONCE_LEN];
    nonce.copy_from_slice(&payload[1..1 + NONCE_LEN]);
    let ciphertext = &payload[1 + NONCE_LEN..];
    let plaintext = match decrypt(&nonce, ciphertext) {
        Ok(p) => PlaintextBuf(p),
        #[cfg(feature = "encrypt")]
        Err(Error::EncryptionKeyMismatch) => {
            return Err(Error::Corrupted {
                offset: 0,
                reason: "encrypted record failed authentication (modified or damaged)",
            });
        }
        Err(err) => return Err(err),
    };
    let plaintext: &[u8] = &plaintext.0;

    match kind {
        TAG_INSERT => match decode_insert_body(plaintext)? {
            RecordView::Insert {
                ns_id,
                key,
                value,
                expires_at,
            } => Ok(OwnedRecord::Insert {
                ns_id,
                key: key.to_vec(),
                value: value.to_vec(),
                expires_at,
            }),
            _ => Err(Error::Corrupted {
                offset: 0,
                reason: "encrypted body shape mismatched its tag",
            }),
        },
        TAG_REMOVE => match decode_remove_body(plaintext)? {
            RecordView::Remove { ns_id, key } => Ok(OwnedRecord::Remove {
                ns_id,
                key: key.to_vec(),
            }),
            _ => Err(Error::Corrupted {
                offset: 0,
                reason: "encrypted body shape mismatched its tag",
            }),
        },
        TAG_NAMESPACE_NAME => match decode_namespace_name_body(plaintext)? {
            RecordView::NamespaceName { ns_id, name } => Ok(OwnedRecord::NamespaceName {
                ns_id,
                name: name.to_vec(),
            }),
            _ => Err(Error::Corrupted {
                offset: 0,
                reason: "encrypted body shape mismatched its tag",
            }),
        },
        unknown => Err(Error::Corrupted {
            offset: 0,
            reason: kind_error_for(unknown),
        }),
    }
}

/// Decrypted record body. Wiped on drop when the `encrypt` feature
/// (and with it the `zeroize` dependency) is compiled in.
struct PlaintextBuf(Vec<u8>);

impl Drop for PlaintextBuf {
    fn drop(&mut self) {
        #[cfg(feature = "encrypt")]
        zeroize::Zeroize::zeroize(&mut self.0);
    }
}

/// Read a record's payload-byte length from fsys's frame length
/// field. The length field lives 4 bytes before the payload,
/// and is little-endian u32.
pub(crate) fn payload_len_at(bytes: &[u8], payload_start: usize) -> Result<usize> {
    if payload_start < 4 {
        return Err(Error::Corrupted {
            offset: payload_start as u64,
            reason: "payload_start within frame header",
        });
    }
    if payload_start > bytes.len() {
        return Err(Error::Corrupted {
            offset: payload_start as u64,
            reason: "payload_start past buffer end",
        });
    }
    Ok(read_u32(bytes, payload_start - 4)? as usize)
}

/// Slice a record's payload out of a buffer (typically the
/// journal mmap), using fsys's length field to bound the range.
pub(crate) fn payload_at(bytes: &[u8], payload_start: usize) -> Result<&[u8]> {
    let len = payload_len_at(bytes, payload_start)?;
    let end = payload_start.checked_add(len).ok_or(Error::Corrupted {
        offset: payload_start as u64,
        reason: "payload_start + length overflowed",
    })?;
    if end > bytes.len() {
        return Err(Error::Corrupted {
            offset: payload_start as u64,
            reason: "payload extends past buffer end",
        });
    }
    Ok(&bytes[payload_start..end])
}

#[inline]
fn kind_error_for(_kind: u8) -> &'static str {
    "unknown record tag kind"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_body_round_trips() {
        let mut body = Vec::new();
        encode_insert_body(&mut body, 7, b"key-bytes", b"value-bytes", 12345);
        match decode_insert_body(&body).expect("decode") {
            RecordView::Insert {
                ns_id,
                key,
                value,
                expires_at,
            } => {
                assert_eq!(ns_id, 7);
                assert_eq!(key, b"key-bytes");
                assert_eq!(value, b"value-bytes");
                assert_eq!(expires_at, 12345);
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn payload_round_trips_via_decode_payload() {
        // Build a payload exactly the way Engine::append_insert
        // would: tag byte + body bytes.
        let mut payload = vec![TAG_INSERT];
        encode_insert_body(&mut payload, 0, b"k", b"v", 0);
        match decode_payload(&payload).expect("decode") {
            RecordView::Insert {
                ns_id,
                key,
                value,
                expires_at,
            } => {
                assert_eq!(ns_id, 0);
                assert_eq!(key, b"k");
                assert_eq!(value, b"v");
                assert_eq!(expires_at, 0);
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn empty_payload_errors() {
        let result = decode_payload(&[]);
        assert!(matches!(result, Err(Error::Corrupted { .. })));
    }

    #[test]
    fn unknown_tag_errors() {
        let result = decode_payload(&[0x42_u8, 0, 0, 0, 0, 0, 0, 0, 0]);
        assert!(matches!(result, Err(Error::Corrupted { .. })));
    }

    #[test]
    fn encrypted_tag_to_plaintext_decoder_errors() {
        let payload = vec![TAG_INSERT | TAG_ENCRYPTED_FLAG];
        let result = decode_payload(&payload);
        assert!(matches!(result, Err(Error::Corrupted { .. })));
    }

    fn insert_body(key: &[u8], value: &[u8]) -> Vec<u8> {
        let mut body = Vec::new();
        encode_insert_body(&mut body, 3, key, value, 99);
        body
    }

    #[test]
    fn test_decoders_accept_every_encoder_output_exactly() {
        let body = insert_body(b"", b"");
        assert!(decode_insert_body(&body).is_ok());
        let body = insert_body(b"k", b"value");
        assert!(decode_insert_body(&body).is_ok());
        let mut rm = Vec::new();
        encode_remove_body(&mut rm, 1, b"k");
        assert!(decode_remove_body(&rm).is_ok());
        let mut empty_rm = Vec::new();
        encode_remove_body(&mut empty_rm, 0, b"");
        assert!(decode_remove_body(&empty_rm).is_ok());
        let mut nn = Vec::new();
        encode_namespace_name_body(&mut nn, 1, b"users");
        assert!(decode_namespace_name_body(&nn).is_ok());
    }

    #[test]
    fn test_decoders_with_trailing_bytes_return_corrupted() {
        let mut body = insert_body(b"k", b"v");
        body.push(0);
        assert!(matches!(
            decode_insert_body(&body),
            Err(Error::Corrupted { reason, .. }) if reason.contains("trailing")
        ));
        let mut rm = Vec::new();
        encode_remove_body(&mut rm, 1, b"k");
        rm.push(0);
        assert!(matches!(
            decode_remove_body(&rm),
            Err(Error::Corrupted { .. })
        ));
        let mut nn = Vec::new();
        encode_namespace_name_body(&mut nn, 1, b"users");
        nn.push(0);
        assert!(matches!(
            decode_namespace_name_body(&nn),
            Err(Error::Corrupted { .. })
        ));
    }

    #[test]
    fn test_insert_body_decoded_as_other_kind_returns_corrupted() {
        // The tag byte is not authenticated, so an attacker can relabel
        // an encrypted insert. Neither shorter layout may decode from
        // a prefix of an insert body.
        for (key, value) in [(&b"victim"[..], &b"important-value"[..]), (b"", b"")] {
            let body = insert_body(key, value);
            assert!(decode_remove_body(&body).is_err());
            assert!(decode_namespace_name_body(&body).is_err());
        }
        // And the shorter layouts never decode as an insert.
        let mut rm = Vec::new();
        encode_remove_body(&mut rm, 1, b"some-key");
        assert!(decode_insert_body(&rm).is_err());
    }

    #[test]
    fn test_decoders_with_max_lengths_do_not_overflow() {
        // key_len = u32::MAX: `8 + key_len` must not wrap on 32-bit
        // targets and must not panic anywhere.
        let mut body = Vec::new();
        write_u32(&mut body, 0);
        write_u32(&mut body, u32::MAX);
        body.extend_from_slice(&[0_u8; 16]);
        assert!(matches!(
            decode_insert_body(&body),
            Err(Error::Corrupted { .. })
        ));
        assert!(matches!(
            decode_remove_body(&body),
            Err(Error::Corrupted { .. })
        ));
        assert!(matches!(
            decode_namespace_name_body(&body),
            Err(Error::Corrupted { .. })
        ));
        // value_len = u32::MAX after a valid key.
        let mut body = Vec::new();
        write_u32(&mut body, 0);
        write_u32(&mut body, 1);
        body.push(b'k');
        write_u32(&mut body, u32::MAX);
        assert!(matches!(
            decode_insert_body(&body),
            Err(Error::Corrupted { .. })
        ));
        // Offsets at the top of the address space.
        assert!(read_u32(&[0_u8; 4], usize::MAX).is_err());
        assert!(read_u64(&[0_u8; 8], usize::MAX - 3).is_err());
        assert!(payload_at(&[0_u8; 8], usize::MAX).is_err());
    }

    #[test]
    fn test_decode_payload_encrypted_rejects_plaintext_tag() {
        let mut payload = vec![TAG_REMOVE];
        payload.extend_from_slice(&[0_u8; NONCE_LEN + TAG_LEN + 8]);
        let result = decode_payload_encrypted(&payload, |_, _| Ok(Vec::new()));
        assert!(matches!(result, Err(Error::Corrupted { .. })));
    }

    #[cfg(feature = "encrypt")]
    #[test]
    fn test_decode_payload_encrypted_auth_failure_returns_corrupted() {
        let mut payload = vec![TAG_INSERT | TAG_ENCRYPTED_FLAG];
        payload.extend_from_slice(&[0_u8; NONCE_LEN + TAG_LEN + 8]);
        let result = decode_payload_encrypted(&payload, |_, _| Err(Error::EncryptionKeyMismatch));
        assert!(matches!(result, Err(Error::Corrupted { .. })), "{result:?}");
    }

    #[test]
    fn payload_at_handles_basic_geometry() {
        // Build a buffer that mimics fsys's framed layout around
        // a 5-byte payload: [4 magic][4 length][5 payload][4 crc]
        let mut frame = Vec::new();
        frame.extend_from_slice(&0x4653_5901_u32.to_be_bytes()); // magic
        frame.extend_from_slice(&5_u32.to_le_bytes()); // length
        frame.extend_from_slice(b"hello"); // payload
        frame.extend_from_slice(&0_u32.to_le_bytes()); // crc placeholder

        let payload_start = 8;
        let payload = payload_at(&frame, payload_start).expect("payload_at");
        assert_eq!(payload, b"hello");
        assert_eq!(payload_len_at(&frame, payload_start).expect("len"), 5);
    }
}
