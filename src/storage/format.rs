// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! On-disk record format for the mmap+append storage engine.
//!
//! Every record on disk has the shape:
//!
//! ```text
//!   [record_len: u32 LE]   — bytes following, excluding self and trailing crc
//!   [tag: u8]              — bit 0..6: kind (0=Insert, 1=Remove); bit 7: encrypted
//!   [body: record_len-1 bytes]
//!   [crc: u32 LE]          — CRC32 over [tag .. body]
//! ```
//!
//! For unencrypted Insert records, the body is the plaintext payload:
//! `[ns_id: u32][key_len: u32][key][value_len: u32][value][expires_at: u64]`.
//!
//! For unencrypted Remove records, the body is just the lookup key:
//! `[ns_id: u32][key_len: u32][key]`.
//!
//! For encrypted records (`tag & 0x80`), the body is:
//! `[nonce: 12][ciphertext+tag]`. The plaintext that gets encrypted is the
//! same payload that appears in the unencrypted form (everything except
//! the leading `tag` byte).
//!
//! The CRC is computed over the bytes between `[record_len]` and `[crc]`
//! (exclusive of both). It catches torn writes on recovery scan and bit
//! rot in long-term storage. AEAD tampering is caught separately by the
//! GCM/Poly1305 tag inside the ciphertext.

use crate::{Error, Result};

/// On-disk magic at the start of every file. 16 bytes, padded with zero.
pub(crate) const MAGIC: [u8; 16] = *b"EMDB\0\0\0\0\0\0\0\0\0\0\0\0";
/// File-format version. Bumped on every breaking format change.
pub(crate) const FORMAT_VERSION: u32 = 1;
/// Header occupies a single 4 KB block at the start of the file.
pub(crate) const HEADER_LEN: usize = 4096;

/// Byte offsets within the header block.
pub(crate) const MAGIC_OFFSET: usize = 0;
pub(crate) const VERSION_OFFSET: usize = 16;
pub(crate) const FLAGS_OFFSET: usize = 20;
pub(crate) const CREATED_AT_OFFSET: usize = 24;
pub(crate) const TAIL_HINT_OFFSET: usize = 32;
pub(crate) const ENCRYPTION_SALT_OFFSET: usize = 40;
pub(crate) const ENCRYPTION_VERIFY_OFFSET: usize = 56;
pub(crate) const HEADER_CRC_OFFSET: usize = 116;
pub(crate) const HEADER_CRC_RANGE: usize = HEADER_CRC_OFFSET;

/// Size of the encryption verification block: 12-byte nonce + 32-byte
/// plaintext + 16-byte AEAD tag = 60 bytes.
pub(crate) const ENCRYPTION_VERIFY_LEN: usize = 60;
/// Size of the Argon2id salt persisted in the header.
pub(crate) const ENCRYPTION_SALT_LEN: usize = 16;
/// Fixed plaintext encrypted into the verification block. On open, the
/// engine decrypts this and confirms it matches; mismatch ⇒ wrong key.
pub(crate) const VERIFICATION_PLAINTEXT: &[u8; 32] =
    b"EMDB-ENCRYPT-OK\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0";

/// Header flag bit set on databases created with at-rest encryption.
pub(crate) const FLAG_ENCRYPTED: u32 = 1 << 0;
/// Header flag bit selecting ChaCha20-Poly1305 instead of AES-256-GCM.
pub(crate) const FLAG_CIPHER_CHACHA20: u32 = 1 << 1;

/// Record tag bytes.
pub(crate) const TAG_INSERT: u8 = 0;
pub(crate) const TAG_REMOVE: u8 = 1;
/// Namespace name → ID binding. Body: `[ns_id: u32][name_len: u32][name]`.
/// Emitted by the engine on first creation of a named namespace; replayed
/// on open to rebuild the in-memory `name → id` map. The default
/// namespace (`ns_id = 0`, `name = ""`) is implicit and never gets a
/// record of this kind.
pub(crate) const TAG_NAMESPACE_NAME: u8 = 2;
pub(crate) const TAG_ENCRYPTED_FLAG: u8 = 0x80;
pub(crate) const TAG_KIND_MASK: u8 = 0x7F;

/// Length of the AEAD nonce in bytes.
pub(crate) const NONCE_LEN: usize = 12;
/// Length of the AEAD authentication tag in bytes.
pub(crate) const TAG_LEN: usize = 16;

/// Encode a u32 little-endian into `buf` and advance the cursor.
#[inline]
pub(crate) fn write_u32(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_le_bytes());
}

/// Encode a u64 little-endian into `buf` and advance the cursor.
#[inline]
pub(crate) fn write_u64(buf: &mut Vec<u8>, value: u64) {
    buf.extend_from_slice(&value.to_le_bytes());
}

/// Read a u32 little-endian from `bytes[offset..offset+4]`.
///
/// # Errors
///
/// Returns [`Error::Corrupted`] if the slice is too short.
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

/// Read a u64 little-endian from `bytes[offset..offset+8]`.
///
/// # Errors
///
/// Returns [`Error::Corrupted`] if the slice is too short.
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

/// Borrowed view of a decoded record body. Lifetime is tied to the
/// underlying buffer (mmap slice or in-memory Vec).
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
    /// Namespace name → ID binding. Replayed on open to rebuild the
    /// in-memory `name → id` map. The default namespace is implicit.
    NamespaceName {
        ns_id: u32,
        name: &'a [u8],
    },
}

/// Owned record (used when the source bytes are not directly addressable
/// — e.g., after AEAD decryption produces a fresh Vec<u8>).
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

    pub(crate) fn key(&self) -> &[u8] {
        match self {
            Self::Insert { key, .. } | Self::Remove { key, .. } => key,
            // NamespaceName isn't keyed in the index; callers asking for
            // a key on this variant get an empty slice. The recovery
            // path special-cases this variant.
            Self::NamespaceName { .. } => &[],
        }
    }
}

/// Encode an Insert record body (the plaintext payload, excluding the
/// outer length, tag, and trailing CRC) into `out`.
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

/// Encode a Remove record body.
pub(crate) fn encode_remove_body(out: &mut Vec<u8>, ns_id: u32, key: &[u8]) {
    write_u32(out, ns_id);
    write_u32(out, key.len() as u32);
    out.extend_from_slice(key);
}

/// Encode a NamespaceName record body.
pub(crate) fn encode_namespace_name_body(out: &mut Vec<u8>, ns_id: u32, name: &[u8]) {
    write_u32(out, ns_id);
    write_u32(out, name.len() as u32);
    out.extend_from_slice(name);
}

/// Decode a NamespaceName body.
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

/// Decode an Insert body from raw bytes (already plaintext).
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

/// Decode a Remove body.
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

/// Compute the CRC32 over a record's `[tag .. body]` span (i.e., everything
/// between the leading `record_len` u32 and the trailing `crc` u32).
#[inline]
pub(crate) fn record_crc(span: &[u8]) -> u32 {
    crc32fast::hash(span)
}

/// Outcome of decoding a single on-disk record at `offset`.
#[derive(Debug)]
pub(crate) struct DecodedRecord<'a> {
    /// The decoded record contents (borrowed from the underlying buffer
    /// for unencrypted records; not used here for encrypted ones — the
    /// caller is expected to use `decode_record_owned` for those).
    pub(crate) view: RecordView<'a>,
    /// File offset immediately past the trailing CRC. Caller resumes
    /// scanning from here.
    pub(crate) next_offset: u64,
}

/// Try to decode a single unencrypted record at `bytes[start..]`.
///
/// `file_start` is the absolute offset of `bytes[start..]` inside the
/// file (only used for error reporting and `next_offset` calculation).
///
/// Returns `Ok(None)` when the buffer is too short to contain a full
/// record (i.e., we have hit the recovery truncation point cleanly).
/// Returns [`Error::Corrupted`] when a length prefix advertises bytes
/// beyond the buffer or when the trailing CRC fails — both of which
/// the caller treats as the recovery truncation point and stops.
pub(crate) fn try_decode_record<'a>(
    bytes: &'a [u8],
    start: usize,
    file_start: u64,
) -> Result<Option<DecodedRecord<'a>>> {
    // Need at least the length prefix to even look at this slot.
    if start + 4 > bytes.len() {
        return Ok(None);
    }
    let record_len = read_u32(bytes, start)? as usize;
    if record_len == 0 {
        // Length-zero records are not valid; they signal the end of the
        // useful prefix of the file (pre-zeroed pages, for example).
        return Ok(None);
    }
    let tag_offset = start + 4;
    let crc_offset = tag_offset + record_len;
    let end = crc_offset + 4;
    if end > bytes.len() {
        // Length prefix says the record extends past the buffer — torn
        // write at the tail of the file. Treat as truncation point.
        return Ok(None);
    }
    let stored_crc = read_u32(bytes, crc_offset)?;
    let actual_crc = record_crc(&bytes[tag_offset..crc_offset]);
    if stored_crc != actual_crc {
        // CRC mismatch ⇒ torn or rotted write. Treat as truncation point.
        return Ok(None);
    }

    let tag_byte = bytes[tag_offset];
    let kind = tag_byte & TAG_KIND_MASK;
    let encrypted = tag_byte & TAG_ENCRYPTED_FLAG != 0;
    if encrypted {
        // Encrypted records are not decodable by this fast path — the
        // caller (recovery scanner) handles decryption separately.
        return Err(Error::Corrupted {
            offset: file_start + tag_offset as u64,
            reason: "encrypted record encountered in plaintext decoder",
        });
    }

    let body = &bytes[tag_offset + 1..crc_offset];
    let view = match kind {
        TAG_INSERT => decode_insert_body(body)?,
        TAG_REMOVE => decode_remove_body(body)?,
        TAG_NAMESPACE_NAME => decode_namespace_name_body(body)?,
        _ => {
            return Err(Error::Corrupted {
                offset: file_start + tag_offset as u64,
                reason: "unknown record tag",
            });
        }
    };

    // `end` is the absolute index into `bytes` of the byte just after
    // this record (since we passed `start` as the absolute cursor).
    // Do not re-add `file_start`.
    let _ = file_start;
    Ok(Some(DecodedRecord {
        view,
        next_offset: end as u64,
    }))
}

/// Same as [`try_decode_record`] but for encrypted records: takes a
/// decryption callback that turns ciphertext bytes into plaintext.
///
/// The caller (engine) supplies the AEAD decryption closure so this
/// module stays cipher-agnostic.
///
/// Returns:
/// - `Ok(Some(decoded))` when a record was decoded successfully.
/// - `Ok(None)` when the bytes are too short / length-prefix is past
///   the buffer / CRC mismatches (i.e., recovery truncation point).
/// - `Err(_)` for AEAD failures or decode errors after decryption
///   (these are real corruption, not torn writes).
pub(crate) fn try_decode_encrypted_record<F>(
    bytes: &[u8],
    start: usize,
    file_start: u64,
    decrypt: F,
) -> Result<Option<(OwnedRecord, u64)>>
where
    F: FnOnce(&[u8; NONCE_LEN], &[u8]) -> Result<Vec<u8>>,
{
    if start + 4 > bytes.len() {
        return Ok(None);
    }
    let record_len = read_u32(bytes, start)? as usize;
    if record_len == 0 {
        return Ok(None);
    }
    let tag_offset = start + 4;
    let crc_offset = tag_offset + record_len;
    let end = crc_offset + 4;
    if end > bytes.len() {
        return Ok(None);
    }
    let stored_crc = read_u32(bytes, crc_offset)?;
    let actual_crc = record_crc(&bytes[tag_offset..crc_offset]);
    if stored_crc != actual_crc {
        return Ok(None);
    }

    let tag_byte = bytes[tag_offset];
    if tag_byte & TAG_ENCRYPTED_FLAG == 0 {
        return Err(Error::Corrupted {
            offset: file_start + tag_offset as u64,
            reason: "plaintext record encountered in encrypted decoder",
        });
    }
    let kind = tag_byte & TAG_KIND_MASK;

    let nonce_offset = tag_offset + 1;
    if nonce_offset + NONCE_LEN > crc_offset {
        return Err(Error::Corrupted {
            offset: file_start + nonce_offset as u64,
            reason: "encrypted record body too short for nonce",
        });
    }
    let mut nonce = [0_u8; NONCE_LEN];
    nonce.copy_from_slice(&bytes[nonce_offset..nonce_offset + NONCE_LEN]);
    let ciphertext = &bytes[nonce_offset + NONCE_LEN..crc_offset];

    let plaintext = decrypt(&nonce, ciphertext)?;
    let owned = match kind {
        TAG_INSERT => match decode_insert_body(&plaintext)? {
            RecordView::Insert {
                ns_id,
                key,
                value,
                expires_at,
            } => OwnedRecord::Insert {
                ns_id,
                key: key.to_vec(),
                value: value.to_vec(),
                expires_at,
            },
            _ => {
                return Err(Error::Corrupted {
                    offset: file_start + tag_offset as u64,
                    reason: "encrypted record body shape did not match its tag",
                });
            }
        },
        TAG_REMOVE => match decode_remove_body(&plaintext)? {
            RecordView::Remove { ns_id, key } => OwnedRecord::Remove {
                ns_id,
                key: key.to_vec(),
            },
            _ => {
                return Err(Error::Corrupted {
                    offset: file_start + tag_offset as u64,
                    reason: "encrypted record body shape did not match its tag",
                });
            }
        },
        TAG_NAMESPACE_NAME => match decode_namespace_name_body(&plaintext)? {
            RecordView::NamespaceName { ns_id, name } => OwnedRecord::NamespaceName {
                ns_id,
                name: name.to_vec(),
            },
            _ => {
                return Err(Error::Corrupted {
                    offset: file_start + tag_offset as u64,
                    reason: "encrypted record body shape did not match its tag",
                });
            }
        },
        _ => {
            return Err(Error::Corrupted {
                offset: file_start + tag_offset as u64,
                reason: "unknown record tag (encrypted)",
            });
        }
    };

    let _ = file_start;
    Ok(Some((owned, end as u64)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_unencrypted_insert(ns_id: u32, key: &[u8], value: &[u8], expires_at: u64) -> Vec<u8> {
        let mut body = Vec::new();
        encode_insert_body(&mut body, ns_id, key, value, expires_at);
        let record_len = (1 + body.len()) as u32; // tag + body
        let mut out = Vec::with_capacity(4 + record_len as usize + 4);
        write_u32(&mut out, record_len);
        out.push(TAG_INSERT);
        out.extend_from_slice(&body);
        let crc = record_crc(&out[4..]);
        write_u32(&mut out, crc);
        out
    }

    #[test]
    fn round_trip_insert_record() {
        let bytes = build_unencrypted_insert(0, b"alpha", b"one", 0);
        let decoded = try_decode_record(&bytes, 0, 0)
            .expect("decode ok")
            .expect("some");
        match decoded.view {
            RecordView::Insert {
                ns_id,
                key,
                value,
                expires_at,
            } => {
                assert_eq!(ns_id, 0);
                assert_eq!(key, b"alpha");
                assert_eq!(value, b"one");
                assert_eq!(expires_at, 0);
            }
            _ => panic!("expected Insert"),
        }
        assert_eq!(decoded.next_offset, bytes.len() as u64);
    }

    #[test]
    fn truncated_record_returns_none() {
        let bytes = build_unencrypted_insert(0, b"k", b"v", 0);
        // Drop the last byte to simulate a torn write.
        let truncated = &bytes[..bytes.len() - 1];
        let decoded = try_decode_record(truncated, 0, 0).expect("decode ok");
        assert!(decoded.is_none());
    }

    #[test]
    fn bit_flip_in_body_fails_crc() {
        let mut bytes = build_unencrypted_insert(0, b"k", b"v", 0);
        // Flip a bit in the value field.
        bytes[12] ^= 1;
        let decoded = try_decode_record(&bytes, 0, 0).expect("decode ok");
        assert!(
            decoded.is_none(),
            "CRC mismatch must surface as truncation point"
        );
    }

    #[test]
    fn empty_buffer_decodes_to_none() {
        let decoded = try_decode_record(&[], 0, 0).expect("decode ok");
        assert!(decoded.is_none());
    }

    #[test]
    fn zero_length_prefix_decodes_to_none() {
        let bytes = vec![0_u8; 8];
        let decoded = try_decode_record(&bytes, 0, 0).expect("decode ok");
        assert!(decoded.is_none());
    }

    #[test]
    fn unknown_tag_reports_corruption() {
        let mut bytes = build_unencrypted_insert(0, b"k", b"v", 0);
        bytes[4] = 0x42; // unknown tag, but legal flags
                         // recompute CRC so the only error is the unknown tag
        let len = read_u32(&bytes, 0).unwrap() as usize;
        let crc = record_crc(&bytes[4..4 + len]);
        let crc_offset = 4 + len;
        bytes[crc_offset..crc_offset + 4].copy_from_slice(&crc.to_le_bytes());
        let result = try_decode_record(&bytes, 0, 0);
        assert!(matches!(result, Err(Error::Corrupted { .. })));
    }
}
