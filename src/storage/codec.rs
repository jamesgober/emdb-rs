// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Binary codec for emdb file headers and operation records.

use std::io::{Read, Write};

use crc32fast::Hasher;

use crate::storage::{Op, OpRef};
use crate::{Error, Result};

pub(crate) const HEADER_LEN: usize = 64;
const MAGIC: [u8; 8] = *b"EMDB\0\0\0\0";

const OP_INSERT: u8 = 0;
const OP_REMOVE: u8 = 1;
const OP_CLEAR: u8 = 2;
const OP_CHECKPOINT: u8 = 3;
const OP_BATCH_BEGIN: u8 = 4;
const OP_BATCH_END: u8 = 5;

/// Parsed file header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Header {
    /// Format version.
    pub(crate) format_ver: u32,
    /// Feature flags.
    pub(crate) flags: u32,
    /// Creation timestamp in unix millis.
    pub(crate) created_at: u64,
    /// Highest committed transaction id.
    pub(crate) last_tx_id: u64,
}

/// Write a 64-byte emdb header.
pub(crate) fn write_header(
    w: &mut impl Write,
    format_ver: u32,
    flags: u32,
    last_tx_id: u64,
) -> Result<()> {
    let mut header = [0_u8; HEADER_LEN];
    header[0..8].copy_from_slice(&MAGIC);
    header[8..12].copy_from_slice(&format_ver.to_le_bytes());
    header[12..16].copy_from_slice(&flags.to_le_bytes());
    header[16..24].copy_from_slice(&now_unix_millis().to_le_bytes());
    header[32..40].copy_from_slice(&last_tx_id.to_le_bytes());
    w.write_all(&header)?;
    Ok(())
}

/// Read and validate a 64-byte emdb header.
pub(crate) fn read_header(r: &mut impl Read) -> Result<Header> {
    let mut header = [0_u8; HEADER_LEN];
    r.read_exact(&mut header)?;

    if header[0..8] != MAGIC {
        return Err(Error::MagicMismatch);
    }

    let format_ver = read_u32_le(&header[8..12]);
    let flags = read_u32_le(&header[12..16]);
    let created_at = read_u64_le(&header[16..24]);

    let last_tx_id = read_u64_le(&header[32..40]);

    Ok(Header {
        format_ver,
        flags,
        created_at,
        last_tx_id,
    })
}

/// Encode one operation record (length prefix, payload, CRC) into `buf`.
///
/// The payload is written directly into `buf` with no intermediate allocation:
/// the length prefix is reserved as four placeholder bytes, the payload is
/// appended in place, the length is patched in, the CRC is computed over the
/// payload slice already in `buf`, then appended.
pub(crate) fn encode_op(buf: &mut Vec<u8>, op: OpRef<'_>) {
    let len_at = buf.len();
    buf.extend_from_slice(&[0_u8; 4]);
    let payload_start = buf.len();

    buf.push(op_type_ref(op));
    buf.extend_from_slice(&now_unix_millis().to_le_bytes());

    match op {
        OpRef::Insert {
            key,
            value,
            expires_at,
        } => {
            buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
            buf.extend_from_slice(value);

            #[cfg(feature = "ttl")]
            {
                buf.extend_from_slice(&expires_at.unwrap_or(0).to_le_bytes());
            }

            #[cfg(not(feature = "ttl"))]
            {
                let _ = expires_at;
            }
        }
        OpRef::Remove { key } => {
            buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buf.extend_from_slice(key);
        }
        OpRef::Clear => {}
        OpRef::Checkpoint { record_count } => {
            buf.extend_from_slice(&record_count.to_le_bytes());
        }
        OpRef::BatchBegin { tx_id, op_count } => {
            buf.extend_from_slice(&tx_id.to_le_bytes());
            buf.extend_from_slice(&op_count.to_le_bytes());
        }
        OpRef::BatchEnd { tx_id } => {
            buf.extend_from_slice(&tx_id.to_le_bytes());
        }
    }

    let payload_end = buf.len();
    let rec_len = (payload_end - payload_start) as u32;
    buf[len_at..len_at + 4].copy_from_slice(&rec_len.to_le_bytes());

    let mut hasher = Hasher::new();
    hasher.update(&buf[payload_start..payload_end]);
    let crc = hasher.finalize();
    buf.extend_from_slice(&crc.to_le_bytes());
}

/// Decode one operation record from `buf` and return bytes consumed.
pub(crate) fn decode_op(buf: &[u8]) -> Result<(Op, usize)> {
    if buf.len() < 8 {
        return Err(Error::Corrupted {
            offset: 0,
            reason: "truncated record header",
        });
    }

    let rec_len = read_u32_le(&buf[0..4]) as usize;
    if rec_len < 9 {
        return Err(Error::Corrupted {
            offset: 0,
            reason: "invalid record length",
        });
    }

    let total_len = 4_usize
        .checked_add(rec_len)
        .and_then(|n| n.checked_add(4))
        .ok_or(Error::Corrupted {
            offset: 0,
            reason: "record length overflow",
        })?;

    if total_len > buf.len() {
        return Err(Error::Corrupted {
            offset: 0,
            reason: "truncated record body",
        });
    }

    let payload_start = 4;
    let payload_end = payload_start + rec_len;
    let payload = &buf[payload_start..payload_end];

    let expected_crc = read_u32_le(&buf[payload_end..payload_end + 4]);
    let mut hasher = Hasher::new();
    hasher.update(payload);
    let actual_crc = hasher.finalize();
    if expected_crc != actual_crc {
        return Err(Error::Corrupted {
            offset: 0,
            reason: "crc mismatch",
        });
    }

    let op_type = payload[0];
    let mut cursor = 1 + 8; // op_type + timestamp

    let op = match op_type {
        OP_INSERT => {
            let key_len = read_u32_payload(payload, &mut cursor)? as usize;
            let key = read_bytes_payload(payload, &mut cursor, key_len)?.to_vec();

            let value_len = read_u32_payload(payload, &mut cursor)? as usize;
            let value = read_bytes_payload(payload, &mut cursor, value_len)?.to_vec();

            #[cfg(feature = "ttl")]
            let expires_at = {
                let raw = read_u64_payload(payload, &mut cursor)?;
                if raw == 0 {
                    None
                } else {
                    Some(raw)
                }
            };

            #[cfg(not(feature = "ttl"))]
            let expires_at = None;

            Op::Insert {
                key,
                value,
                expires_at,
            }
        }
        OP_REMOVE => {
            let key_len = read_u32_payload(payload, &mut cursor)? as usize;
            let key = read_bytes_payload(payload, &mut cursor, key_len)?.to_vec();
            Op::Remove { key }
        }
        OP_CLEAR => Op::Clear,
        OP_CHECKPOINT => {
            let record_count = read_u32_payload(payload, &mut cursor)?;
            Op::Checkpoint { record_count }
        }
        OP_BATCH_BEGIN => {
            let tx_id = read_u64_payload(payload, &mut cursor)?;
            let op_count = read_u32_payload(payload, &mut cursor)?;
            Op::BatchBegin { tx_id, op_count }
        }
        OP_BATCH_END => {
            let tx_id = read_u64_payload(payload, &mut cursor)?;
            Op::BatchEnd { tx_id }
        }
        _ => {
            return Err(Error::Corrupted {
                offset: 0,
                reason: "unknown op type",
            });
        }
    };

    if cursor != payload.len() {
        return Err(Error::Corrupted {
            offset: 0,
            reason: "record trailing bytes",
        });
    }

    Ok((op, total_len))
}

fn op_type_ref(op: OpRef<'_>) -> u8 {
    match op {
        OpRef::Insert { .. } => OP_INSERT,
        OpRef::Remove { .. } => OP_REMOVE,
        OpRef::Clear => OP_CLEAR,
        OpRef::Checkpoint { .. } => OP_CHECKPOINT,
        OpRef::BatchBegin { .. } => OP_BATCH_BEGIN,
        OpRef::BatchEnd { .. } => OP_BATCH_END,
    }
}

fn read_u32_payload(payload: &[u8], cursor: &mut usize) -> Result<u32> {
    let bytes = read_bytes_payload(payload, cursor, 4)?;
    Ok(read_u32_le(bytes))
}

fn read_u64_payload(payload: &[u8], cursor: &mut usize) -> Result<u64> {
    let bytes = read_bytes_payload(payload, cursor, 8)?;
    Ok(read_u64_le(bytes))
}

fn read_bytes_payload<'a>(payload: &'a [u8], cursor: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = cursor.checked_add(len).ok_or(Error::Corrupted {
        offset: 0,
        reason: "payload length overflow",
    })?;
    if end > payload.len() {
        return Err(Error::Corrupted {
            offset: 0,
            reason: "payload out of bounds",
        });
    }

    let out = &payload[*cursor..end];
    *cursor = end;
    Ok(out)
}

fn read_u32_le(bytes: &[u8]) -> u32 {
    let mut arr = [0_u8; 4];
    arr.copy_from_slice(bytes);
    u32::from_le_bytes(arr)
}

fn read_u64_le(bytes: &[u8]) -> u64 {
    let mut arr = [0_u8; 8];
    arr.copy_from_slice(bytes);
    u64::from_le_bytes(arr)
}

fn now_unix_millis() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis().min(u64::MAX as u128) as u64,
        Err(_before_epoch) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::{decode_op, read_header, write_header, HEADER_LEN};
    use crate::storage::codec::encode_op;
    use crate::storage::{Op, OpRef, FORMAT_VERSION};

    #[test]
    fn round_trip_header() {
        let mut bytes = Vec::new();
        let wrote = write_header(&mut bytes, FORMAT_VERSION, 0x5, 99);
        assert!(wrote.is_ok());
        assert_eq!(bytes.len(), HEADER_LEN);

        let mut cursor = std::io::Cursor::new(bytes);
        let header = read_header(&mut cursor);
        assert!(header.is_ok());
        let header = match header {
            Ok(header) => header,
            Err(err) => panic!("header decode should succeed: {err}"),
        };
        assert_eq!(header.flags, 0x5);
        assert_eq!(header.last_tx_id, 99);
        assert_eq!(header.format_ver, FORMAT_VERSION);
    }

    #[test]
    fn round_trip_insert_remove_clear_checkpoint() {
        let ops = [
            Op::Insert {
                key: b"".to_vec(),
                value: b"value".to_vec(),
                expires_at: None,
            },
            Op::Remove { key: b"k".to_vec() },
            Op::Clear,
            Op::Checkpoint { record_count: 9 },
            Op::BatchBegin {
                tx_id: 11,
                op_count: 0,
            },
            Op::BatchBegin {
                tx_id: 12,
                op_count: 3,
            },
            Op::BatchEnd { tx_id: 12 },
        ];

        for op in ops {
            let mut buf = Vec::new();
            encode_op(&mut buf, OpRef::from(&op));
            let decoded = decode_op(&buf);
            assert!(decoded.is_ok());
            let (decoded_op, consumed) = match decoded {
                Ok(pair) => pair,
                Err(err) => panic!("decode should succeed: {err}"),
            };
            assert_eq!(decoded_op, op);
            assert_eq!(consumed, buf.len());
        }
    }

    #[test]
    fn decode_rejects_truncated_inputs() {
        let cases: [&[u8]; 2] = [&[], &[1, 2, 3]];
        for bytes in cases {
            let decoded = decode_op(bytes);
            assert!(decoded.is_err());
        }
    }

    #[test]
    fn decode_rejects_crc_mismatch() {
        let mut buf = Vec::new();
        encode_op(
            &mut buf,
            OpRef::Insert {
                key: b"a",
                value: b"b",
                expires_at: None,
            },
        );

        let last = buf.len() - 1;
        buf[last] ^= 0x01;

        let decoded = decode_op(&buf);
        assert!(decoded.is_err());
    }

    #[test]
    fn decode_rejects_length_overrun() {
        let mut buf = Vec::new();
        encode_op(&mut buf, OpRef::Clear);
        buf[0..4].copy_from_slice(&(u32::MAX).to_le_bytes());

        let decoded = decode_op(&buf);
        assert!(decoded.is_err());
    }

    #[test]
    fn decode_rejects_trailing_payload_bytes() {
        let mut buf = Vec::new();
        encode_op(&mut buf, OpRef::Clear);

        // Increase rec_len by one and patch CRC to keep crc check valid.
        let original_rec_len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        let new_len = (original_rec_len + 1) as u32;
        buf[0..4].copy_from_slice(&new_len.to_le_bytes());

        // Insert one byte before CRC so body still parses to total len.
        let crc_index = 4 + original_rec_len;
        buf.insert(crc_index, 0xff);

        // Recompute CRC over payload.
        let payload_end = 4 + new_len as usize;
        let payload = &buf[4..payload_end];
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(payload);
        let crc = hasher.finalize();
        buf[payload_end..payload_end + 4].copy_from_slice(&crc.to_le_bytes());

        let decoded = decode_op(&buf);
        assert!(decoded.is_err());
    }
}
