// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Classification of journal damage found by the recovery scan.
//!
//! fsys's open resumes a journal at the end of its last valid frame
//! and cuts everything after it (saving non-zero bytes to a
//! `<file>.corrupt-<offset>` sidecar). That is the right answer for
//! a torn tail, the state a crash leaves, but applied to a bit flip
//! in the middle of the file it silently discards every acknowledged
//! record after the flip.
//!
//! [`check_damage`] runs before fsys opens the journal and decides
//! which case the open is looking at:
//!
//! - **Nothing valid follows the damage.** A torn final frame (or
//!   trailing garbage). Accepted; fsys cuts it.
//! - **Only unwritten space precedes the next valid frame.**
//!   Concurrent appends write at reserved offsets, so a crash can
//!   leave a never-written (zero) reservation, or a partly written
//!   frame, in front of a frame another thread finished. Nothing at
//!   or after such a gap was ever covered by a successful sync,
//!   because fsys only advances its durable frontier over fully
//!   written bytes. Accepted.
//! - **Anything else.** A complete-looking frame that fails its
//!   checksum, or non-zero bytes that are not a frame, followed by
//!   valid frames. Refused with [`Error::Corrupted`]; the file is
//!   not modified.
//!
//! A frame "looks torn" when its trailing CRC field is all zero or
//! it contains an all-zero, 512-byte-aligned sector: the part of a
//! write that never reached the disk. A frame that has been fully
//! written and then damaged has neither.
//!
//! Valid-looking frames inside the declared extent of the damaged
//! frame are treated as its payload (a value may itself contain
//! journal bytes), except when they prove that the damaged frame is
//! complete and only its length field changed: then the checksum of
//! the frame, recomputed with the length implied by the next frame,
//! matches the stored one.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::storage::store::{FSYS_FRAME_MAGIC, FSYS_FRAME_OVERHEAD, FSYS_MAX_PAYLOAD};
use crate::{Error, Result};

/// Reason attached to [`Error::Corrupted`] when valid records follow
/// damaged bytes.
pub(crate) const MID_FILE_DAMAGE: &str =
    "journal damaged before its end with valid records after the damage; open refused so they \
     are not discarded (restore a backup, or truncate the file at this offset to accept the loss)";

/// Read size for scanning the damaged region.
const CHUNK: usize = 1 << 20;
/// Sector size used to recognise unwritten parts of a torn frame.
const SECTOR: u64 = 512;
/// Upper bound on the bytes checksummed by the length-field check, so
/// a pathological damaged frame cannot make the open quadratic.
const LENGTH_CHECK_BUDGET: u64 = 64 << 20;

/// Decide whether the journal at `path` may be opened when the
/// recovery scan stopped at `clean_end` before `file_len`.
///
/// # Errors
///
/// [`Error::Corrupted`] with [`MID_FILE_DAMAGE`] when valid frames
/// follow damage that is not a crash artifact; [`Error::Io`] when
/// the file cannot be read.
pub(crate) fn check_damage(path: &Path, clean_end: u64, file_len: u64) -> Result<()> {
    if clean_end >= file_len {
        return Ok(());
    }
    let mut scan = Scan::open(path, file_len)?;
    let declared_end = scan.plausible_frame_end(clean_end)?;
    let mut budget = LENGTH_CHECK_BUDGET;
    let mut from = clean_end + 1;
    loop {
        let Some(next) = scan.next_valid_frame(from)? else {
            // Nothing valid follows: a torn tail. fsys cuts it.
            return Ok(());
        };
        if scan.length_field_damaged(clean_end, next, &mut budget)? {
            return Err(corrupted(clean_end));
        }
        if declared_end.is_some_and(|end| next < end) {
            // Inside the damaged frame's own payload.
            from = next + 1;
            continue;
        }
        return if scan.is_crash_gap(clean_end, next)? {
            Ok(())
        } else {
            Err(corrupted(clean_end))
        };
    }
}

fn corrupted(offset: u64) -> Error {
    Error::Corrupted {
        offset,
        reason: MID_FILE_DAMAGE,
    }
}

/// Positioned reads over the journal file.
struct Scan {
    file: File,
    file_len: u64,
    buf: Vec<u8>,
}

impl Scan {
    fn open(path: &Path, file_len: u64) -> Result<Self> {
        Ok(Self {
            file: File::open(path)?,
            file_len,
            buf: Vec::new(),
        })
    }

    /// Read `[offset, offset + len)` (clamped to the file) into the
    /// scratch buffer and return it.
    fn read(&mut self, offset: u64, len: u64) -> Result<&[u8]> {
        let end = offset.saturating_add(len).min(self.file_len);
        let want = usize::try_from(end.saturating_sub(offset))
            .map_err(|_| Error::InvalidConfig("journal region exceeds the address space"))?;
        self.buf.clear();
        self.buf.resize(want, 0);
        let _pos = self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(&mut self.buf)?;
        Ok(&self.buf)
    }

    /// Payload length of the frame header at `offset`, if the magic
    /// matches and the length is within the fsys cap.
    fn header_len(&mut self, offset: u64) -> Result<Option<u64>> {
        let header = self.read(offset, 8)?;
        if header.len() < 8 || header[..4] != FSYS_FRAME_MAGIC {
            return Ok(None);
        }
        let len = u64::from(u32::from_le_bytes([
            header[4], header[5], header[6], header[7],
        ]));
        Ok((len <= FSYS_MAX_PAYLOAD).then_some(len))
    }

    /// End of the frame whose header sits at `offset`, if the header
    /// is plausible.
    fn plausible_frame_end(&mut self, offset: u64) -> Result<Option<u64>> {
        Ok(self
            .header_len(offset)?
            .and_then(|len| offset.checked_add(FSYS_FRAME_OVERHEAD + len)))
    }

    /// `true` when a complete frame with a valid CRC-32C starts at
    /// `offset`.
    fn valid_frame_at(&mut self, offset: u64) -> Result<bool> {
        let Some(len) = self.header_len(offset)? else {
            return Ok(false);
        };
        let Some(end) = offset.checked_add(FSYS_FRAME_OVERHEAD + len) else {
            return Ok(false);
        };
        if end > self.file_len {
            return Ok(false);
        }
        let frame = self.read(offset, FSYS_FRAME_OVERHEAD + len)?;
        let body_end = frame.len() - 4;
        let stored = u32::from_le_bytes([
            frame[body_end],
            frame[body_end + 1],
            frame[body_end + 2],
            frame[body_end + 3],
        ]);
        Ok(crc32c(&[&frame[..body_end]]) == stored)
    }

    /// First offset at or after `from` where a valid frame starts.
    fn next_valid_frame(&mut self, from: u64) -> Result<Option<u64>> {
        let mut base = from;
        while base < self.file_len {
            let chunk = self.read(base, CHUNK as u64)?.to_vec();
            let mut hits = Vec::new();
            for (i, window) in chunk.windows(4).enumerate() {
                if window == FSYS_FRAME_MAGIC {
                    hits.push(base + i as u64);
                }
            }
            for candidate in hits {
                if self.valid_frame_at(candidate)? {
                    return Ok(Some(candidate));
                }
            }
            if chunk.len() < CHUNK {
                break;
            }
            // Overlap by three bytes so a magic split across chunks
            // is still found.
            base += (chunk.len() - 3) as u64;
        }
        Ok(None)
    }

    /// `true` when the frame at `start` is complete and intact except
    /// for its length field: recomputing its checksum with the length
    /// implied by the valid frame at `next` matches the CRC stored in
    /// the four bytes in front of `next`.
    fn length_field_damaged(&mut self, start: u64, next: u64, budget: &mut u64) -> Result<bool> {
        let Some(span) = next.checked_sub(start) else {
            return Ok(false);
        };
        if span < FSYS_FRAME_OVERHEAD || span > *budget {
            return Ok(false);
        }
        let implied_len = span - FSYS_FRAME_OVERHEAD;
        if implied_len > FSYS_MAX_PAYLOAD {
            return Ok(false);
        }
        *budget -= span;
        let frame = self.read(start, span)?;
        let body_end = frame.len() - 4;
        let stored = u32::from_le_bytes([
            frame[body_end],
            frame[body_end + 1],
            frame[body_end + 2],
            frame[body_end + 3],
        ]);
        let len_field = u32::try_from(implied_len)
            .map_err(|_| Error::InvalidConfig("frame length exceeds u32"))?
            .to_le_bytes();
        Ok(crc32c(&[&FSYS_FRAME_MAGIC, &len_field, &frame[8..body_end]]) == stored)
    }

    /// `true` when `[start, next)` holds only what an interrupted
    /// write leaves: zero runs and frames that look torn.
    fn is_crash_gap(&mut self, start: u64, next: u64) -> Result<bool> {
        // A header torn inside its first eight bytes, then nothing.
        if self.all_zero(start.saturating_add(8).min(next), next)? {
            return Ok(true);
        }
        let mut cursor = start;
        while cursor < next {
            if self.read(cursor, 1)?.first() == Some(&0) {
                cursor = self.zero_run_end(cursor, next)?;
                continue;
            }
            let Some(end) = self.plausible_frame_end(cursor)? else {
                return Ok(false);
            };
            if end > next || !self.looks_torn(cursor, end)? {
                return Ok(false);
            }
            cursor = end;
        }
        Ok(true)
    }

    /// `true` when the frame `[start, end)` shows a part of its write
    /// that never reached the file: a zero CRC field, or an all-zero
    /// 512-byte-aligned sector inside its payload.
    fn looks_torn(&mut self, start: u64, end: u64) -> Result<bool> {
        if end - start < FSYS_FRAME_OVERHEAD {
            return Ok(false);
        }
        if self.all_zero(end - 4, end)? {
            return Ok(true);
        }
        let payload_start = start + 8;
        let payload_end = end - 4;
        let mut sector = payload_start.div_ceil(SECTOR) * SECTOR;
        while sector + SECTOR <= payload_end {
            if self.all_zero(sector, sector + SECTOR)? {
                return Ok(true);
            }
            sector += SECTOR;
        }
        Ok(false)
    }

    /// `true` when every byte in `[from, to)` is zero.
    fn all_zero(&mut self, from: u64, to: u64) -> Result<bool> {
        Ok(self.zero_run_end(from, to)? >= to)
    }

    /// First offset in `[from, to)` holding a non-zero byte, or `to`.
    fn zero_run_end(&mut self, from: u64, to: u64) -> Result<u64> {
        let mut cursor = from;
        while cursor < to {
            let chunk = self.read(cursor, (to - cursor).min(CHUNK as u64))?;
            if chunk.is_empty() {
                return Ok(to);
            }
            if let Some(i) = chunk.iter().position(|&b| b != 0) {
                return Ok(cursor + i as u64);
            }
            cursor += chunk.len() as u64;
        }
        Ok(to)
    }
}

/// CRC-32C (Castagnoli) lookup table, the checksum fsys frames carry.
const CRC32C_TABLE: [u32; 256] = crc32c_table();

const fn crc32c_table() -> [u32; 256] {
    let mut table = [0_u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0x82F6_3B78
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

/// CRC-32C over the concatenation of `parts`. Only used on the
/// damaged-journal path, so a table-driven implementation is enough.
fn crc32c(parts: &[&[u8]]) -> u32 {
    let mut crc = !0_u32;
    for part in parts {
        for &byte in *part {
            crc = CRC32C_TABLE[((crc ^ u32::from(byte)) & 0xFF) as usize] ^ (crc >> 8);
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&FSYS_FRAME_MAGIC);
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        out.extend_from_slice(payload);
        let crc = crc32c(&[&out]);
        out.extend_from_slice(&crc.to_le_bytes());
        out
    }

    fn write_tmp(label: &str, bytes: &[u8]) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0_u128, |d| d.as_nanos());
        let mut p = std::env::temp_dir();
        p.push(format!(
            "emdb-integrity-{label}-{}-{nanos}",
            std::process::id()
        ));
        std::fs::write(&p, bytes).expect("write");
        p
    }

    fn check(label: &str, bytes: &[u8], clean_end: usize) -> Result<()> {
        let path = write_tmp(label, bytes);
        let out = check_damage(&path, clean_end as u64, bytes.len() as u64);
        let _ = std::fs::remove_file(&path);
        out
    }

    #[test]
    fn test_crc32c_known_answers() {
        assert_eq!(crc32c(&[b""]), 0);
        assert_eq!(crc32c(&[b"123456789"]), 0xE306_9283);
        assert_eq!(crc32c(&[b"1234", b"56789"]), 0xE306_9283);
    }

    #[test]
    fn test_check_damage_torn_tail_accepted() {
        let mut bytes = frame(b"first");
        let clean = bytes.len();
        let torn = frame(b"second record payload");
        bytes.extend_from_slice(&torn[..torn.len() - 7]);
        assert!(check("torn", &bytes, clean).is_ok());
    }

    #[test]
    fn test_check_damage_bitflip_before_valid_frames_refused() {
        let mut bytes = frame(b"first");
        let clean = bytes.len();
        let mut damaged = frame(b"second record payload");
        damaged[12] ^= 0x01;
        bytes.extend_from_slice(&damaged);
        bytes.extend_from_slice(&frame(b"third"));
        assert!(matches!(
            check("flip", &bytes, clean),
            Err(Error::Corrupted { offset, .. }) if offset == clean as u64
        ));
    }

    #[test]
    fn test_check_damage_zero_reservation_before_frame_accepted() {
        let mut bytes = frame(b"first");
        let clean = bytes.len();
        bytes.extend_from_slice(&[0_u8; 40]);
        bytes.extend_from_slice(&frame(b"written by another thread"));
        assert!(check("hole", &bytes, clean).is_ok());
    }

    #[test]
    fn test_check_damage_partial_frame_with_zero_crc_accepted() {
        let mut bytes = frame(b"first");
        let clean = bytes.len();
        let mut partial = frame(&[7_u8; 64]);
        let n = partial.len();
        for b in &mut partial[n - 20..] {
            *b = 0;
        }
        bytes.extend_from_slice(&partial);
        bytes.extend_from_slice(&frame(b"later"));
        assert!(check("partial", &bytes, clean).is_ok());
    }

    #[test]
    fn test_check_damage_enlarged_length_field_refused() {
        let mut bytes = frame(b"first");
        let clean = bytes.len();
        let mut damaged = frame(b"second");
        damaged[6] ^= 0x10; // length grows past the end of the file
        bytes.extend_from_slice(&damaged);
        bytes.extend_from_slice(&frame(b"third"));
        assert!(matches!(
            check("len", &bytes, clean),
            Err(Error::Corrupted { .. })
        ));
    }

    #[test]
    fn test_check_damage_embedded_frame_in_torn_payload_accepted() {
        let mut bytes = frame(b"first");
        let clean = bytes.len();
        let mut payload = vec![9_u8; 32];
        payload.extend_from_slice(&frame(b"inner"));
        payload.extend_from_slice(&[9_u8; 32]);
        let torn = frame(&payload);
        bytes.extend_from_slice(&torn[..torn.len() - 10]);
        assert!(check("embedded", &bytes, clean).is_ok());
    }

    #[test]
    fn test_check_damage_foreign_bytes_before_frame_refused() {
        let mut bytes = frame(b"first");
        let clean = bytes.len();
        bytes.extend_from_slice(b"some text that is not a frame at all");
        bytes.extend_from_slice(&frame(b"third"));
        assert!(check("garbage", &bytes, clean).is_err());
    }

    #[test]
    fn test_check_damage_clean_end_at_eof_accepted() {
        let bytes = frame(b"only");
        assert!(check("eof", &bytes, bytes.len()).is_ok());
    }
}
