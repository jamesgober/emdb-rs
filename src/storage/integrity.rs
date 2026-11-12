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
//! - **Only what an interrupted write leaves precedes the next valid
//!   frame.** Concurrent appends write at reserved offsets, so a
//!   crash can leave a never-written (zero) reservation, or a frame
//!   whose write stopped part way, in front of a frame another thread
//!   finished. Nothing at or after such a gap was ever covered by a
//!   successful sync, because fsys only advances its durable
//!   frontier over fully written bytes. Accepted, when the valid
//!   frames after the gap fit in what one crash can leave unsynced
//!   ([`IN_FLIGHT_WINDOW`]).
//! - **Anything else.** A complete-looking frame that fails its
//!   checksum, zero bytes that no interrupted write leaves, or
//!   non-zero bytes that are not a frame, followed by valid frames.
//!   Refused with [`Error::Corrupted`]; the file is not modified.
//!
//! A write that stops part way stops at a sector boundary: the page
//! cache copies a write one page at a time and a device persists
//! whole sectors, and every supported sector size is a multiple of
//! 512 bytes. So a frame "looks torn" only when everything from some
//! 512-byte-aligned offset inside it to its end is zero. A frame that
//! was written in full and then damaged does not look like that: a
//! zeroed checksum field, a zeroed sector with written bytes after
//! it, or a zero run that does not reach back to a sector boundary
//! are all refused.
//!
//! Valid-looking frames inside the declared extent of the damaged
//! frame are treated as its payload (a value may itself contain
//! journal bytes), except when they prove that the damaged frame is
//! complete and only its length field changed: then the checksum of
//! the frame, recomputed with the length implied by the next frame,
//! matches the stored one.
//!
//! The search for valid frames after the damage checksums candidate
//! frames. Its total work is bounded by [`SEARCH_BUDGET_BASE`] plus
//! [`SEARCH_BUDGET_PER_BYTE`] times the size of the damaged region,
//! so a crafted file cannot make the open quadratic; an open that
//! would need more is refused with [`Error::Corrupted`].

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::storage::store::{FSYS_FRAME_MAGIC, FSYS_FRAME_OVERHEAD, FSYS_MAX_PAYLOAD};
use crate::{Error, Result};

/// Reason attached to [`Error::Corrupted`] when valid records follow
/// damaged bytes.
pub(crate) const MID_FILE_DAMAGE: &str =
    "journal damaged before its end with valid records after the damage; open refused so they \
     are not discarded (restore a backup, or open with EmdbBuilder::salvage(true) to keep the \
     records before this offset)";

/// Reason attached to [`Error::Corrupted`] when the damaged region is
/// too costly to classify.
pub(crate) const DAMAGE_TOO_COSTLY: &str =
    "journal damaged before its end and too costly to classify; open refused (restore a \
     backup, or open with EmdbBuilder::salvage(true) to keep the records before this offset)";

/// Read size for scanning the damaged region.
const CHUNK: usize = 1 << 20;
/// Sector size used to recognise where an interrupted write stopped.
const SECTOR: u64 = 512;
/// Upper bound on the bytes checksummed by the length-field check, so
/// a pathological damaged frame cannot make the open quadratic.
const LENGTH_CHECK_BUDGET: u64 = 64 << 20;
/// Most bytes of valid frames that may follow a crash gap. Appends
/// that complete while an earlier one is still being written are what
/// a crash can leave after such a gap; more than this is not a crash
/// artifact. Matches the largest in-flight window fsys bounds a
/// journal's writers to (`MAX_LOG_BUFFER_BYTES`, 64 MiB).
pub(crate) const IN_FLIGHT_WINDOW: u64 = 64 << 20;
/// Bytes the search for valid frames may checksum before the size of
/// the damaged region is taken into account.
pub(crate) const SEARCH_BUDGET_BASE: u64 = 16 << 20;
/// Bytes the search may checksum per byte of the damaged region.
pub(crate) const SEARCH_BUDGET_PER_BYTE: u64 = 2;

/// Tunable bounds, so unit tests can exercise them on small files.
#[derive(Clone, Copy)]
struct Limits {
    in_flight_window: u64,
    search_budget_base: u64,
}

impl Limits {
    const DEFAULT: Self = Self {
        in_flight_window: IN_FLIGHT_WINDOW,
        search_budget_base: SEARCH_BUDGET_BASE,
    };
}

/// Decide whether the journal at `path` may be opened when the
/// recovery scan stopped at `clean_end` before `file_len`.
///
/// # Errors
///
/// [`Error::Corrupted`] with [`MID_FILE_DAMAGE`] when valid frames
/// follow damage that is not a crash artifact, or with
/// [`DAMAGE_TOO_COSTLY`] when the search for valid frames ran out of
/// its work budget; [`Error::Io`] when the file cannot be read.
pub(crate) fn check_damage(path: &Path, clean_end: u64, file_len: u64) -> Result<()> {
    check_damage_with(path, clean_end, file_len, Limits::DEFAULT).map(|_work| ())
}

/// [`check_damage`] with explicit limits. Returns the number of bytes
/// the search for valid frames checksummed.
fn check_damage_with(path: &Path, clean_end: u64, file_len: u64, limits: Limits) -> Result<u64> {
    if clean_end >= file_len {
        return Ok(0);
    }
    let damaged = file_len - clean_end;
    let budget = limits
        .search_budget_base
        .saturating_add(damaged.saturating_mul(SEARCH_BUDGET_PER_BYTE));
    let mut scan = Scan::open(path, file_len, clean_end, budget)?;
    let verdict = classify(&mut scan, clean_end, file_len, limits);
    let work = budget - scan.budget;
    verdict.map(|()| work)
}

fn classify(scan: &mut Scan, clean_end: u64, file_len: u64, limits: Limits) -> Result<()> {
    let declared_end = scan.plausible_frame_end(clean_end)?;
    let mut length_budget = LENGTH_CHECK_BUDGET;
    let mut from = clean_end + 1;
    loop {
        let Some(next) = scan.next_valid_frame(from)? else {
            // Nothing valid follows: a torn tail. fsys cuts it.
            return Ok(());
        };
        if scan.length_field_damaged(clean_end, next, &mut length_budget)? {
            return Err(corrupted(clean_end, MID_FILE_DAMAGE));
        }
        if declared_end.is_some_and(|end| next < end) {
            // Inside the damaged frame's own payload.
            from = next + 1;
            continue;
        }
        // Valid frames follow. Accepting the gap cuts them, so they
        // must be what one crash can leave unsynced, and the gap must
        // be what an interrupted write leaves.
        if file_len - next > limits.in_flight_window {
            return Err(corrupted(clean_end, MID_FILE_DAMAGE));
        }
        return if scan.is_crash_gap(clean_end, next)? {
            Ok(())
        } else {
            Err(corrupted(clean_end, MID_FILE_DAMAGE))
        };
    }
}

fn corrupted(offset: u64, reason: &'static str) -> Error {
    Error::Corrupted { offset, reason }
}

/// Fill `buf` with `[offset, offset + len)` of `file`, clamped to
/// `file_len`.
fn read_into(
    file: &mut File,
    file_len: u64,
    offset: u64,
    len: u64,
    buf: &mut Vec<u8>,
) -> Result<()> {
    let end = offset.saturating_add(len).min(file_len);
    let want = usize::try_from(end.saturating_sub(offset))
        .map_err(|_| Error::InvalidConfig("journal region exceeds the address space"))?;
    buf.clear();
    buf.resize(want, 0);
    let _pos = file.seek(SeekFrom::Start(offset))?;
    file.read_exact(buf)?;
    Ok(())
}

/// Positioned reads over the journal file.
struct Scan {
    file: File,
    file_len: u64,
    /// Where the damage starts; reported by a budget refusal.
    clean_end: u64,
    /// Bytes the search for valid frames may still checksum.
    budget: u64,
    /// Scratch for single reads.
    buf: Vec<u8>,
    /// The chunk the magic search is walking, starting at
    /// `window_base`. Kept between calls so a search that resumes a
    /// few bytes further on does not read the chunk again.
    window: Vec<u8>,
    window_base: u64,
}

impl Scan {
    fn open(path: &Path, file_len: u64, clean_end: u64, budget: u64) -> Result<Self> {
        Ok(Self {
            file: File::open(path)?,
            file_len,
            clean_end,
            budget,
            buf: Vec::new(),
            window: Vec::new(),
            window_base: 0,
        })
    }

    /// Read `[offset, offset + len)` (clamped to the file) into the
    /// scratch buffer and return it.
    fn read(&mut self, offset: u64, len: u64) -> Result<&[u8]> {
        read_into(&mut self.file, self.file_len, offset, len, &mut self.buf)?;
        Ok(&self.buf)
    }

    /// Payload length of the frame header at `offset`, if the magic
    /// matches and the length is within the fsys cap.
    fn header_len(&mut self, offset: u64) -> Result<Option<u64>> {
        let header = self.read(offset, 8)?;
        Ok(parse_header(header))
    }

    /// End of the frame whose header sits at `offset`, if the header
    /// is plausible.
    fn plausible_frame_end(&mut self, offset: u64) -> Result<Option<u64>> {
        Ok(self
            .header_len(offset)?
            .and_then(|len| offset.checked_add(FSYS_FRAME_OVERHEAD + len)))
    }

    /// `true` when a complete frame of payload length `len` with a
    /// valid CRC-32C starts at `offset`. Charges the frame's size to
    /// the search budget.
    fn valid_frame_at(&mut self, offset: u64, len: u64) -> Result<bool> {
        let size = FSYS_FRAME_OVERHEAD + len;
        let Some(end) = offset.checked_add(size) else {
            return Ok(false);
        };
        if end > self.file_len {
            return Ok(false);
        }
        if size > self.budget {
            return Err(corrupted(self.clean_end, DAMAGE_TOO_COSTLY));
        }
        self.budget -= size;
        let frame = self.read(offset, size)?;
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
    ///
    /// Walks the file in [`CHUNK`]-sized windows, keeping the current
    /// window across calls, so a sequence of calls with increasing
    /// `from` reads each byte about once.
    fn next_valid_frame(&mut self, from: u64) -> Result<Option<u64>> {
        let mut at = from;
        while at < self.file_len {
            let window_end = self.window_base + self.window.len() as u64;
            // Keep three bytes of lookahead so a magic that starts
            // near the end of the window is still seen whole.
            if at < self.window_base || at + 3 >= window_end {
                read_into(
                    &mut self.file,
                    self.file_len,
                    at,
                    CHUNK as u64,
                    &mut self.window,
                )?;
                self.window_base = at;
            }
            let start = usize::try_from(at - self.window_base)
                .map_err(|_| Error::InvalidConfig("journal region exceeds the address space"))?;
            let last_window = self.window_base + self.window.len() as u64 >= self.file_len;
            let mut hit = None;
            for i in start..self.window.len().saturating_sub(3) {
                if self.window[i..i + 4] == FSYS_FRAME_MAGIC {
                    hit = Some(i);
                    break;
                }
            }
            let Some(i) = hit else {
                if last_window {
                    return Ok(None);
                }
                at = self.window_base + (self.window.len() - 3) as u64;
                continue;
            };
            let candidate = self.window_base + i as u64;
            let len = if i + 8 <= self.window.len() {
                parse_header(&self.window[i..i + 8])
            } else {
                self.header_len(candidate)?
            };
            if let Some(len) = len {
                if self.valid_frame_at(candidate, len)? {
                    return Ok(Some(candidate));
                }
            }
            at = candidate + 1;
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

    /// `true` when `[start, next)` holds only what interrupted writes
    /// leave: zero runs (reservations never written) and frames whose
    /// write stopped at a sector boundary.
    fn is_crash_gap(&mut self, start: u64, next: u64) -> Result<bool> {
        let mut cursor = start;
        while cursor < next {
            if self.read(cursor, 1)?.first() == Some(&0) {
                cursor = self.zero_run_end(cursor, next)?;
                continue;
            }
            match self.plausible_frame_end(cursor)? {
                Some(end) if end <= next => {
                    if !self.cut_at_sector(cursor, end)? {
                        return Ok(false);
                    }
                    cursor = end;
                }
                _ => {
                    // No usable length: the write must have stopped
                    // inside the header, at a sector boundary, with
                    // nothing but zeros after it.
                    let cut = self.zero_suffix_start(cursor, next)?.div_ceil(SECTOR) * SECTOR;
                    return Ok(cut > cursor && cut < cursor + 8 && cut < next);
                }
            }
        }
        Ok(true)
    }

    /// `true` when the frame `[start, end)` is zero from some
    /// 512-byte-aligned offset inside it to its end: the part of a
    /// write that never reached the file.
    fn cut_at_sector(&mut self, start: u64, end: u64) -> Result<bool> {
        if end - start < FSYS_FRAME_OVERHEAD {
            return Ok(false);
        }
        let cut = self.zero_suffix_start(start, end)?.div_ceil(SECTOR) * SECTOR;
        Ok(cut > start && cut < end)
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

    /// Start of the run of zero bytes that ends at `to`, not before
    /// `from`: one past the last non-zero byte in `[from, to)`, or
    /// `from` when the whole range is zero.
    fn zero_suffix_start(&mut self, from: u64, to: u64) -> Result<u64> {
        let mut end = to;
        while end > from {
            let len = (end - from).min(CHUNK as u64);
            let start = end - len;
            let chunk = self.read(start, len)?;
            if chunk.is_empty() {
                return Ok(from);
            }
            if let Some(i) = chunk.iter().rposition(|&b| b != 0) {
                return Ok(start + i as u64 + 1);
            }
            end = start;
        }
        Ok(from)
    }
}

/// Payload length of a frame header, if the magic matches and the
/// length is within the fsys cap.
fn parse_header(header: &[u8]) -> Option<u64> {
    if header.len() < 8 || header[..4] != FSYS_FRAME_MAGIC {
        return None;
    }
    let len = u64::from(u32::from_le_bytes([
        header[4], header[5], header[6], header[7],
    ]));
    (len <= FSYS_MAX_PAYLOAD).then_some(len)
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

    fn check_with(label: &str, bytes: &[u8], clean_end: usize, limits: Limits) -> Result<u64> {
        let path = write_tmp(label, bytes);
        let out = check_damage_with(&path, clean_end as u64, bytes.len() as u64, limits);
        let _ = std::fs::remove_file(&path);
        out
    }

    fn check(label: &str, bytes: &[u8], clean_end: usize) -> Result<()> {
        check_with(label, bytes, clean_end, Limits::DEFAULT).map(|_| ())
    }

    fn is_refused(result: &Result<()>, clean_end: usize) -> bool {
        matches!(
            result,
            Err(Error::Corrupted { offset, .. }) if *offset == clean_end as u64
        )
    }

    /// A first frame ending at `clean`, then a frame with a payload of
    /// `payload_len` bytes of `7`, then a later valid frame. Returns
    /// the bytes, `clean` and the middle frame's `(start, end)`.
    fn three_frames(payload_len: usize) -> (Vec<u8>, usize, (usize, usize)) {
        let mut bytes = frame(b"first");
        let clean = bytes.len();
        bytes.extend_from_slice(&frame(&vec![7_u8; payload_len]));
        let end = bytes.len();
        bytes.extend_from_slice(&frame(b"later"));
        (bytes, clean, (clean, end))
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
        assert!(is_refused(&check("flip", &bytes, clean), clean));
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
    fn test_check_damage_write_cut_at_sector_accepted() {
        // The middle frame's write stopped at the first sector
        // boundary inside it; the frame after it was written.
        let (mut bytes, clean, (start, end)) = three_frames(1500);
        let cut = (start + 100).div_ceil(512) * 512;
        bytes[cut..end].fill(0);
        assert!(check("cut", &bytes, clean).is_ok());
    }

    #[test]
    fn test_check_damage_write_cut_inside_checksum_accepted() {
        // The sector boundary falls inside the checksum field: two
        // checksum bytes were written, two were not.
        let mut bytes = frame(b"first");
        let clean = bytes.len();
        let target_end = 1024 + 2;
        let payload_len = target_end - clean - 12;
        let mut torn = frame(&vec![9_u8; payload_len]);
        let n = torn.len();
        torn[n - 2..].fill(0);
        bytes.extend_from_slice(&torn);
        assert_eq!(bytes.len(), target_end);
        bytes.extend_from_slice(&frame(b"later"));
        assert!(check("cut-crc", &bytes, clean).is_ok());
    }

    #[test]
    fn test_check_damage_zeroed_checksum_refused() {
        let (mut bytes, clean, (start, end)) = three_frames(1500);
        bytes[end - 4..end].fill(0);
        // The zeroed checksum is surrounded by written bytes of the
        // same sector, so it is not the unwritten end of a write.
        assert!((end - 4).div_ceil(512) * 512 >= end);
        assert!(start < end);
        assert!(is_refused(&check("zero-crc", &bytes, clean), clean));
    }

    #[test]
    fn test_check_damage_zeroed_inner_sector_refused() {
        let (mut bytes, clean, (start, end)) = three_frames(1500);
        let sector = (start + 8).div_ceil(512) * 512;
        assert!(sector + 512 <= end - 4);
        bytes[sector..sector + 512].fill(0);
        assert!(is_refused(&check("zero-sector", &bytes, clean), clean));
    }

    #[test]
    fn test_check_damage_unaligned_zero_tail_refused() {
        // Zeros from inside a sector to the end of the frame: the
        // bytes in front of them in the same sector were written.
        let (mut bytes, clean, (_, end)) = three_frames(1500);
        let from = (end - 1) / 512 * 512 + 1;
        assert!(from + 4 <= end);
        bytes[from..end].fill(0);
        assert!(is_refused(&check("zero-tail", &bytes, clean), clean));
    }

    #[test]
    fn test_check_damage_header_cut_at_sector_accepted() {
        // The header straddles a sector boundary and only its first
        // bytes were written.
        let first = frame(&vec![1_u8; 512 - 12 - 6]);
        assert_eq!(first.len(), 512 - 6);
        let mut bytes = first;
        let clean = bytes.len();
        let mut torn = frame(&[5_u8; 300]);
        torn[6..].fill(0);
        bytes.extend_from_slice(&torn);
        bytes.extend_from_slice(&frame(b"later"));
        assert!(check("cut-header", &bytes, clean).is_ok());
    }

    #[test]
    fn test_check_damage_garbage_header_then_zeros_refused() {
        // Eight written header bytes that do not describe a frame,
        // then zeros: not where an interrupted write stops.
        let mut bytes = frame(b"first");
        let clean = bytes.len();
        bytes.extend_from_slice(&[0x46, 0x53, 0x59, 0x01, 0xFF, 0xFF, 0xFF, 0xFF]);
        bytes.extend_from_slice(&[0_u8; 100]);
        bytes.extend_from_slice(&frame(b"later"));
        assert!(is_refused(&check("bad-header", &bytes, clean), clean));
    }

    #[test]
    fn test_check_damage_gap_followed_by_too_much_data_refused() {
        // An unwritten reservation is a crash artifact only when the
        // valid frames after it fit in one in-flight window.
        let mut bytes = frame(b"first");
        let clean = bytes.len();
        bytes.extend_from_slice(&[0_u8; 40]);
        for _ in 0..20 {
            bytes.extend_from_slice(&frame(&[3_u8; 100]));
        }
        let limits = Limits {
            in_flight_window: 1000,
            search_budget_base: SEARCH_BUDGET_BASE,
        };
        let result = check_with("window", &bytes, clean, limits).map(|_| ());
        assert!(is_refused(&result, clean));
        let roomy = Limits {
            in_flight_window: 10_000,
            search_budget_base: SEARCH_BUDGET_BASE,
        };
        assert!(check_with("window-ok", &bytes, clean, roomy).is_ok());
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

    /// One valid frame, a non-magic byte, then a frame header every
    /// 8 bytes whose length reaches the end of the file: each header
    /// is a candidate the search must checksum to the end.
    fn hostile(body: usize) -> (Vec<u8>, usize) {
        let mut out = frame(b"first");
        let clean = out.len();
        out.push(0x01);
        let total = out.len() + body;
        while out.len() + 8 <= total {
            let at = out.len();
            out.extend_from_slice(&FSYS_FRAME_MAGIC);
            let len = (total - at).saturating_sub(12) as u32;
            out.extend_from_slice(&len.to_le_bytes());
        }
        (out, clean)
    }

    #[test]
    fn test_check_damage_work_is_linear_on_crafted_input() {
        // Without a bound, the search checksums about n^2 / 16 bytes.
        // With it, the work is at most the base plus twice the damaged
        // region, and the open is refused once that is spent.
        let limits = Limits {
            in_flight_window: IN_FLIGHT_WINDOW,
            search_budget_base: 64 << 10,
        };
        for size in [64_usize << 10, 256 << 10, 1 << 20] {
            let (bytes, clean) = hostile(size);
            let damaged = (bytes.len() - clean) as u64;
            let bound = limits.search_budget_base + SEARCH_BUDGET_PER_BYTE * damaged;
            let path = write_tmp("hostile", &bytes);
            let result = check_damage_with(&path, clean as u64, bytes.len() as u64, limits);
            let _ = std::fs::remove_file(&path);
            assert!(
                matches!(
                    result,
                    Err(Error::Corrupted { offset, reason: DAMAGE_TOO_COSTLY }) if offset == clean as u64
                ),
                "{size}: {result:?}"
            );
            // The refusal happens before the budget would be overdrawn.
            let quadratic = (damaged * damaged) / 16;
            assert!(
                bound < quadratic / 8,
                "{size}: bound {bound} vs {quadratic}"
            );
        }
    }

    #[test]
    fn test_check_damage_work_counts_checksummed_bytes() {
        // A legitimate damaged region: one damaged frame, then a valid
        // one. The search checksums about the valid frame only.
        let (mut bytes, clean, (start, _)) = three_frames(4000);
        bytes[start + 600] ^= 0x01;
        let limits = Limits::DEFAULT;
        let result = check_with("work", &bytes, clean, limits);
        assert!(matches!(result, Err(Error::Corrupted { .. })));
        let mut ok = frame(b"first");
        let clean_ok = ok.len();
        ok.extend_from_slice(&[0_u8; 40]);
        let later = frame(&[4_u8; 3000]);
        ok.extend_from_slice(&later);
        let work = check_with("work-ok", &ok, clean_ok, limits).expect("crash gap accepted");
        assert_eq!(work, later.len() as u64);
    }

    #[test]
    fn test_check_damage_many_embedded_frames_read_once() {
        // A damaged frame whose declared length covers many small
        // valid frames: each is found once, without re-reading a full
        // chunk per frame.
        let mut bytes = frame(b"first");
        let clean = bytes.len();
        let one = frame(b"x");
        let inner: Vec<u8> = (0..2000).flat_map(|_| one.clone()).collect();
        let mut damaged = frame(&inner);
        // Breaks the outer frame's checksum and the first inner
        // frame's magic; the other 1999 inner frames stay valid.
        damaged[10] ^= 0x01;
        bytes.extend_from_slice(&damaged);
        let work = check_with("embedded-many", &bytes, clean, Limits::DEFAULT)
            .expect("nothing valid after the damaged frame");
        assert_eq!(work, (inner.len() - one.len()) as u64);
    }
}
