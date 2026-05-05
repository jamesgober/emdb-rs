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

#[inline]
pub(crate) fn read_u32(bytes: &[u8], offset: usize) -> Result<u32> {
    if offset + 4 > bytes.len() {
        return Err(Error::Corrupted {
            offset: offset as u64,
            reason: "u32 read past end of buffer",
        });
    }
    let mut buf = [0_u8; 4];
    buf.copy_from_slice(&bytes[offset..offset + 4]);
    Ok(u32::from_le_bytes(buf))
}

#[inline]
pub(crate) fn read_u64(bytes: &[u8], offset: usize) -> Result<u64> {
    if offset + 8 > bytes.len() {
        return Err(Error::Corrupted {
            offset: offset as u64,
            reason: "u64 read past end of buffer",
        });
    }
    let mut buf = [0_u8; 8];
    buf.copy_from_slice(&bytes[offset..offset + 8]);
    Ok(u64::from_le_bytes(buf))
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

/// Decode a plaintext `Insert` record body.
pub(crate) fn decode_insert_body(body: &[u8]) -> Result<RecordView<'_>> {
    let ns_id = read_u32(body, 0)?;
    let key_len = read_u32(body, 4)? as usize;
    let key_end = 8 + key_len;
    if key_end > body.len() {
        return Err(Error::Corrupted {
            offset: 8,
            reason: "insert body truncated mid-key",
        });
    }
    let key = &body[8..key_end];
    let value_len = read_u32(body, key_end)? as usize;
    let value_start = key_end + 4;
    let value_end = value_start + value_len;
    if value_end > body.len() {
        return Err(Error::Corrupted {
            offset: value_start as u64,
            reason: "insert body truncated mid-value",
        });
    }
    let value = &body[value_start..value_end];
    let expires_at = read_u64(body, value_end)?;
    Ok(RecordView::Insert {
        ns_id,
        key,
        value,
        expires_at,
    })
}

/// Decode a plaintext `Remove` record body.
pub(crate) fn decode_remove_body(body: &[u8]) -> Result<RecordView<'_>> {
    let ns_id = read_u32(body, 0)?;
    let key_len = read_u32(body, 4)? as usize;
    let key_end = 8 + key_len;
    if key_end > body.len() {
        return Err(Error::Corrupted {
            offset: 8,
            reason: "remove body truncated mid-key",
        });
    }
    let key = &body[8..key_end];
    Ok(RecordView::Remove { ns_id, key })
}

/// Decode a plaintext `NamespaceName` record body.
pub(crate) fn decode_namespace_name_body(body: &[u8]) -> Result<RecordView<'_>> {
    let ns_id = read_u32(body, 0)?;
    let name_len = read_u32(body, 4)? as usize;
    let name_end = 8 + name_len;
    if name_end > body.len() {
        return Err(Error::Corrupted {
            offset: 8,
            reason: "namespace-name body truncated mid-name",
        });
    }
    let name = &body[8..name_end];
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
    let plaintext = decrypt(&nonce, ciphertext)?;

    match kind {
        TAG_INSERT => match decode_insert_body(&plaintext)? {
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
        TAG_REMOVE => match decode_remove_body(&plaintext)? {
            RecordView::Remove { ns_id, key } => Ok(OwnedRecord::Remove {
                ns_id,
                key: key.to_vec(),
            }),
            _ => Err(Error::Corrupted {
                offset: 0,
                reason: "encrypted body shape mismatched its tag",
            }),
        },
        TAG_NAMESPACE_NAME => match decode_namespace_name_body(&plaintext)? {
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
pub(crate) fn payload_at<'a>(bytes: &'a [u8], payload_start: usize) -> Result<&'a [u8]> {
    let len = payload_len_at(bytes, payload_start)?;
    let end = payload_start
        .checked_add(len)
        .ok_or(Error::Corrupted {
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
