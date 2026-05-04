// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Mmap-backed append-only store. File reads go through an `Mmap`
//! (zero-copy from kernel-managed memory); writes go through `pwrite`
//! at a single tail offset.
//!
//! ## File layout
//!
//! ```text
//!   bytes 0..4096    Header (magic, version, flags, salt, verify block, CRC)
//!   bytes 4096..N    Records, framed as [len][tag+body][crc] (see format.rs)
//!   bytes N..        Pre-allocated zeros (capacity grows in chunks)
//! ```
//!
//! ## Concurrency
//!
//! - **Single writer**, multi-reader. The writer mutex serialises
//!   `pwrite` calls and tail-offset bumps.
//! - Readers borrow from the mmap directly. The mmap is wrapped in
//!   `Arc<Mmap>` and atomically swapped on file growth so readers
//!   holding an old `Arc<Mmap>` continue reading from the old mapping
//!   until they release it. Memory is unmapped automatically when the
//!   last `Arc` drops.
//!
//! ## Crash safety
//!
//! Recovery scan validates each record's CRC. The first bad CRC (or
//! length-prefix-past-end) is treated as the truncation point — that
//! record and everything after it is discarded. The header carries a
//! `tail_hint` updated lazily on flush; the scan starts from that hint
//! but always re-validates so a stale hint is harmless.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

#[cfg(unix)]
use std::os::unix::fs::{FileExt, OpenOptionsExt as _};
#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt as _;

use memmap2::Mmap;

/// Windows `FILE_FLAG_WRITE_THROUGH` — causes `WriteFile` to wait
/// for the data to reach non-volatile storage before returning,
/// bypassing the lazy write-behind cache. Hardcoded to avoid a
/// `windows-sys` dep for one constant. Source:
/// `winnt.h` in the Windows SDK.
#[cfg(windows)]
const FILE_FLAG_WRITE_THROUGH: u32 = 0x8000_0000;

/// Unix `O_SYNC` — POSIX flag that makes every write block until
/// the data plus its metadata is durable on disk. Hardcoded
/// per-platform because `std::os::unix::fs::OpenOptionsExt::custom_flags`
/// takes an `i32` and we'd otherwise pull in `libc` for one
/// constant.
///
/// Linux defines `O_SYNC` as `O_DSYNC | __O_SYNC = 0x101000`; the
/// BSD family uses `0x80`. `target_os` checks dispatch to the
/// right value.
#[cfg(target_os = "linux")]
const O_SYNC_FLAG: i32 = 0x101000;
#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "dragonfly",
    target_os = "openbsd",
    target_os = "netbsd"
))]
const O_SYNC_FLAG: i32 = 0x80;
// Other unix targets we don't have a vetted constant for fall back
// to 0 (no synchronous-write flag) and rely on explicit
// `sync_data` calls. This keeps the build green on tier-2/3
// targets we haven't certified.
#[cfg(all(
    unix,
    not(target_os = "linux"),
    not(target_os = "macos"),
    not(target_os = "ios"),
    not(target_os = "freebsd"),
    not(target_os = "dragonfly"),
    not(target_os = "openbsd"),
    not(target_os = "netbsd")
))]
const O_SYNC_FLAG: i32 = 0;

/// Positional write at `offset`. On Unix this uses `pwrite` so the
/// file pointer is not moved. On Windows we fall back to the
/// traditional `seek + write_all` path because `FileExt::seek_write`
/// is no faster than `seek + write_all` on the platform and adds an
/// internal lock; `seek + write_all` is cheaper in practice.
#[inline]
fn pwrite_all(file: &mut File, offset: u64, buf: &[u8]) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        file.write_all_at(buf, offset)
    }
    #[cfg(not(unix))]
    {
        let _seek = file.seek(SeekFrom::Start(offset))?;
        file.write_all(buf)
    }
}

use crate::storage::flush::{group_sync, FlushPolicy, GroupCoord};
use crate::storage::format::{
    self, FLAG_ENCRYPTED, FORMAT_VERSION, HEADER_CRC_OFFSET, HEADER_CRC_RANGE, HEADER_LEN, MAGIC,
    MAGIC_OFFSET,
};
use crate::{Error, Result};

/// Initial pre-allocated file size in bytes (1 MiB). The file grows in
/// chunks of `GROW_CHUNK_BYTES` whenever the writer would overflow.
const INITIAL_CAPACITY: u64 = 1 << 20;
/// File-growth chunk size (16 MiB). Bigger chunks reduce remap cost at
/// the price of more wasted tail bytes when the database is small.
const GROW_CHUNK_BYTES: u64 = 16 << 20;

/// Decoded view of the on-disk header.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Header {
    pub(crate) flags: u32,
    pub(crate) created_at: u64,
    pub(crate) tail_hint: u64,
    pub(crate) encryption_salt: [u8; format::ENCRYPTION_SALT_LEN],
    pub(crate) encryption_verify: [u8; format::ENCRYPTION_VERIFY_LEN],
}

impl Header {
    /// Construct a fresh header for a brand-new file.
    fn fresh(flags: u32) -> Self {
        Self {
            flags,
            created_at: now_unix_millis(),
            tail_hint: HEADER_LEN as u64,
            encryption_salt: [0_u8; format::ENCRYPTION_SALT_LEN],
            encryption_verify: [0_u8; format::ENCRYPTION_VERIFY_LEN],
        }
    }

    /// Encode the header into the first 4096 bytes of the file.
    pub(crate) fn encode_into(&self, buf: &mut [u8; HEADER_LEN]) {
        // Zero everything first so reserved bytes are deterministic.
        for byte in buf.iter_mut() {
            *byte = 0;
        }
        buf[MAGIC_OFFSET..MAGIC_OFFSET + 16].copy_from_slice(&MAGIC);
        buf[format::VERSION_OFFSET..format::VERSION_OFFSET + 4]
            .copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        buf[format::FLAGS_OFFSET..format::FLAGS_OFFSET + 4]
            .copy_from_slice(&self.flags.to_le_bytes());
        buf[format::CREATED_AT_OFFSET..format::CREATED_AT_OFFSET + 8]
            .copy_from_slice(&self.created_at.to_le_bytes());
        buf[format::TAIL_HINT_OFFSET..format::TAIL_HINT_OFFSET + 8]
            .copy_from_slice(&self.tail_hint.to_le_bytes());
        buf[format::ENCRYPTION_SALT_OFFSET
            ..format::ENCRYPTION_SALT_OFFSET + format::ENCRYPTION_SALT_LEN]
            .copy_from_slice(&self.encryption_salt);
        buf[format::ENCRYPTION_VERIFY_OFFSET
            ..format::ENCRYPTION_VERIFY_OFFSET + format::ENCRYPTION_VERIFY_LEN]
            .copy_from_slice(&self.encryption_verify);
        let crc = crc32fast::hash(&buf[..HEADER_CRC_RANGE]);
        buf[HEADER_CRC_OFFSET..HEADER_CRC_OFFSET + 4].copy_from_slice(&crc.to_le_bytes());
    }

    /// Decode the header from a 4 KB block.
    pub(crate) fn decode_from(buf: &[u8]) -> Result<Self> {
        if buf.len() < HEADER_LEN {
            return Err(Error::Corrupted {
                offset: 0,
                reason: "header buffer truncated",
            });
        }
        if buf[..16] != MAGIC {
            return Err(Error::MagicMismatch);
        }
        let version = format::read_u32(buf, format::VERSION_OFFSET)?;
        if version != FORMAT_VERSION {
            return Err(Error::VersionMismatch {
                found: version,
                expected: FORMAT_VERSION,
            });
        }
        let stored_crc = format::read_u32(buf, HEADER_CRC_OFFSET)?;
        let actual_crc = crc32fast::hash(&buf[..HEADER_CRC_RANGE]);
        if stored_crc != actual_crc {
            return Err(Error::Corrupted {
                offset: HEADER_CRC_OFFSET as u64,
                reason: "header CRC mismatch",
            });
        }

        let mut salt = [0_u8; format::ENCRYPTION_SALT_LEN];
        salt.copy_from_slice(
            &buf[format::ENCRYPTION_SALT_OFFSET
                ..format::ENCRYPTION_SALT_OFFSET + format::ENCRYPTION_SALT_LEN],
        );
        let mut verify = [0_u8; format::ENCRYPTION_VERIFY_LEN];
        verify.copy_from_slice(
            &buf[format::ENCRYPTION_VERIFY_OFFSET
                ..format::ENCRYPTION_VERIFY_OFFSET + format::ENCRYPTION_VERIFY_LEN],
        );

        Ok(Self {
            flags: format::read_u32(buf, format::FLAGS_OFFSET)?,
            created_at: format::read_u64(buf, format::CREATED_AT_OFFSET)?,
            tail_hint: format::read_u64(buf, format::TAIL_HINT_OFFSET)?,
            encryption_salt: salt,
            encryption_verify: verify,
        })
    }
}

/// Writer-side state: file handle + current tail offset + reusable
/// encoding buffer. Held inside a `Mutex` to serialise writers.
struct WriterState {
    file: File,
    tail: u64,
    capacity: u64,
    encode_buf: Vec<u8>,
}

/// Batch-encoding cursor exposed to closures passed into
/// [`Store::append_batch_with`]. Each call to [`Self::push_record`]
/// frames one record into the shared buffer and returns its absolute
/// file offset (relative to the start of the file).
pub(crate) struct BatchEncoder<'a> {
    buf: &'a mut Vec<u8>,
    base_offset: u64,
}

impl<'a> BatchEncoder<'a> {
    /// Append one framed record (`[len][body][crc]`) to the batch and
    /// return the file offset where it will land once the batch is
    /// flushed. The closure fills the body bytes (starting with the tag
    /// byte; everything between the length prefix and the CRC).
    ///
    /// # Errors
    ///
    /// Bubbles up any error returned by the closure.
    pub(crate) fn push_record<F>(&mut self, fill: F) -> Result<u64>
    where
        F: FnOnce(&mut Vec<u8>) -> Result<()>,
    {
        let record_start = self.buf.len();
        // Reserve the 4-byte length prefix. We patch it once we know
        // the body size.
        self.buf.extend_from_slice(&[0_u8; 4]);
        let body_start = self.buf.len();
        fill(self.buf)?;
        let body_end = self.buf.len();
        let body_len = (body_end - body_start) as u32;
        self.buf[record_start..record_start + 4].copy_from_slice(&body_len.to_le_bytes());
        let crc = crate::storage::format::record_crc(&self.buf[body_start..body_end]);
        self.buf.extend_from_slice(&crc.to_le_bytes());
        Ok(self.base_offset + record_start as u64)
    }
}

/// The store. Wrap in `Arc` and share between threads.
pub(crate) struct Store {
    path: PathBuf,
    header: Arc<RwLock<Header>>,
    /// Atomically swappable mmap handle. Readers grab a snapshot via
    /// `Arc::clone`; growth replaces the inner Arc (old readers
    /// continue with the old mapping until their Arc drops).
    mmap: RwLock<Arc<Mmap>>,
    writer: Mutex<WriterState>,
    /// Tail offset cached as an atomic so readers can answer "what's
    /// the largest valid offset right now?" without taking the writer
    /// lock. Updated under the writer mutex but readable lock-free.
    tail_atomic: AtomicU64,
    /// Sibling `File` handle (a `try_clone` of the writer's handle)
    /// used exclusively for `sync_data` calls. Holding this lets
    /// `flush()` issue an fsync **without** taking the writer mutex,
    /// so concurrent `pwrite` calls and `sync_data` calls can
    /// overlap. Without this clone every fsync would serialise
    /// through the writer mutex, and the group-commit policy could
    /// not actually fuse anything because OnEachFlush would already
    /// be serialising for it.
    ///
    /// Atomically swapped on file growth and on `swap_underlying`,
    /// same lifetime story as the mmap arc.
    sync_handle: RwLock<Arc<File>>,
    /// Active flush policy. `OnEachFlush` (the default) runs
    /// `flush()` as a single `sync_data` per call. `Group` routes
    /// through [`GroupCoord`] so concurrent flushers fuse their
    /// syncs.
    policy: FlushPolicy,
    /// Group-commit coordinator, populated only when
    /// `policy == FlushPolicy::Group`.
    coord: Option<Arc<GroupCoord>>,
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store")
            .field("path", &self.path)
            .field("tail", &self.tail_atomic.load(Ordering::Acquire))
            .finish()
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        // Best-effort fast-reopen checkpoint: persist the latest
        // `tail_hint` so the next open starts its recovery scan past
        // the bulk of the log instead of from the data-region start.
        // Errors here are swallowed because Drop cannot fail; if the
        // header doesn't make it to disk the next open just re-scans
        // the whole log (correct, just slower).
        let _ = self.persist_header();
    }
}

impl Store {
    /// Open or create a store at `path`. On a fresh file the header is
    /// initialised with the supplied `flags`. The returned store is
    /// ready for reads at `tail_hint` and writes at the same.
    ///
    /// The caller is expected to run a recovery scan after open via
    /// [`Self::recovery_scan_offsets`] — this function does not validate
    /// records past the header.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from open / extend / mmap.
    /// Returns [`Error::MagicMismatch`] / [`Error::VersionMismatch`] /
    /// [`Error::Corrupted`] for malformed headers.
    pub(crate) fn open(path: PathBuf, flags: u32) -> Result<Self> {
        Self::open_with_policy(path, flags, FlushPolicy::default())
    }

    /// Same as [`Self::open`] but with an explicit [`FlushPolicy`].
    /// `Group` policies attach a coordinator that fuses concurrent
    /// `flush()` calls into one `sync_data`. `WriteThrough` opens
    /// the file with platform-native synchronous-write flags so
    /// every `pwrite` is durable on return.
    pub(crate) fn open_with_policy(path: PathBuf, flags: u32, policy: FlushPolicy) -> Result<Self> {
        let mut file = open_options_for(&policy)
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        let len = file.metadata()?.len();
        let (header, capacity, tail) = if len == 0 {
            // Fresh file: write the header, pre-allocate to INITIAL_CAPACITY.
            let header = Header::fresh(flags);
            let mut buf = [0_u8; HEADER_LEN];
            header.encode_into(&mut buf);
            let _seek = file.seek(SeekFrom::Start(0))?;
            file.write_all(&buf)?;
            file.set_len(INITIAL_CAPACITY)?;
            file.sync_data()?;
            (header, INITIAL_CAPACITY, HEADER_LEN as u64)
        } else {
            // Existing file: read the header.
            let mut buf = [0_u8; HEADER_LEN];
            let _seek = file.seek(SeekFrom::Start(0))?;
            file.read_exact(&mut buf)?;
            let header = Header::decode_from(&buf)?;
            // Validate flag superset: flags must be a subset of what
            // the file already has, OR new bits the user is opting
            // into. For now we accept any user flags.
            let _ = flags;
            (header, len, header.tail_hint)
        };

        // SAFETY: The file backing the mmap is held open by
        // `WriterState::file` for the lifetime of the Store. The mmap
        // region covers the entire file at open time. We swap to a
        // fresh mmap whenever we grow the file (see `grow_locked`),
        // keeping the old `Arc<Mmap>` alive until existing readers
        // release it. No code path concurrently truncates or shrinks
        // the file, so the mapping never points at unmapped pages.
        let mmap = unsafe { Mmap::map(&file)? };

        let coord = match policy {
            FlushPolicy::OnEachFlush | FlushPolicy::WriteThrough => None,
            FlushPolicy::Group {
                max_wait,
                max_batch,
            } => Some(Arc::new(GroupCoord::new(max_wait, max_batch))),
        };

        // Clone the file handle for the sync path. `try_clone`
        // duplicates the underlying OS file descriptor / handle
        // without taking ownership; both handles point at the same
        // inode, and sync_data on either flushes the same dirty
        // pages.
        let sync_handle = file.try_clone()?;

        Ok(Self {
            path,
            header: Arc::new(RwLock::new(header)),
            mmap: RwLock::new(Arc::new(mmap)),
            writer: Mutex::new(WriterState {
                file,
                tail,
                capacity,
                encode_buf: Vec::with_capacity(256),
            }),
            tail_atomic: AtomicU64::new(tail),
            sync_handle: RwLock::new(Arc::new(sync_handle)),
            policy,
            coord,
        })
    }

    /// Path of the on-disk file.
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Read and decode the header of an on-disk database without opening
    /// it as a writable store. Returns `Ok(None)` if the file does not
    /// exist; `Ok(Some(header))` on a valid file; an error if the file
    /// exists but is too short or has a corrupt header.
    ///
    /// Used by the encryption admin tools to validate the source file's
    /// state before kicking off a rewrite.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from open/read, or
    /// [`Error::MagicMismatch`] / [`Error::VersionMismatch`] /
    /// [`Error::Corrupted`] for malformed headers.
    pub(crate) fn peek_header_path(path: &Path) -> Result<Option<Header>> {
        let mut file = match OpenOptions::new().read(true).open(path) {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(Error::from(err)),
        };
        let len = file.metadata()?.len();
        if len < HEADER_LEN as u64 {
            return Err(Error::Corrupted {
                offset: 0,
                reason: "file shorter than header",
            });
        }
        let mut buf = [0_u8; HEADER_LEN];
        let _seek = file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut buf)?;
        Ok(Some(Header::decode_from(&buf)?))
    }

    /// Snapshot the header.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockPoisoned`] on poisoned RwLock.
    pub(crate) fn header(&self) -> Result<Header> {
        let guard = self.header.read().map_err(|_| Error::LockPoisoned)?;
        Ok(*guard)
    }

    /// Update the header's encryption salt and verify block. Used at
    /// creation time when encryption is enabled. Persists immediately.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockPoisoned`] on poisoned lock or I/O on the
    /// write.
    pub(crate) fn set_encryption_metadata(
        &self,
        salt: [u8; format::ENCRYPTION_SALT_LEN],
        verify: [u8; format::ENCRYPTION_VERIFY_LEN],
    ) -> Result<()> {
        let mut header = self.header.write().map_err(|_| Error::LockPoisoned)?;
        header.encryption_salt = salt;
        header.encryption_verify = verify;
        header.flags |= FLAG_ENCRYPTED;
        let mut buf = [0_u8; HEADER_LEN];
        header.encode_into(&mut buf);

        let mut writer = self.writer.lock().map_err(|_| Error::LockPoisoned)?;
        let _seek = writer.file.seek(SeekFrom::Start(0))?;
        writer.file.write_all(&buf)?;
        writer.file.sync_data()?;
        // Refresh the mmap so subsequent reads pick up the new header.
        // SAFETY: the file is the same one we just wrote to and the
        // mapping covers the whole file; no external truncation
        // happens between sync_data and remap.
        let new_mmap = unsafe { Mmap::map(&writer.file)? };
        let mut mmap_guard = self.mmap.write().map_err(|_| Error::LockPoisoned)?;
        *mmap_guard = Arc::new(new_mmap);
        Ok(())
    }

    /// Borrow a snapshot of the current mmap.
    ///
    /// The returned `Arc<Mmap>` keeps the mapping alive for as long as
    /// the caller holds it, even if the writer subsequently grows the
    /// file and swaps in a new mapping.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockPoisoned`] on poisoned lock.
    pub(crate) fn mmap(&self) -> Result<Arc<Mmap>> {
        let guard = self.mmap.read().map_err(|_| Error::LockPoisoned)?;
        Ok(Arc::clone(&guard))
    }

    /// Current tail offset (one past the last valid byte). Lock-free
    /// read.
    pub(crate) fn tail(&self) -> u64 {
        self.tail_atomic.load(Ordering::Acquire)
    }

    /// Append a fully-encoded record (`[len][tag+body][crc]` bytes) to
    /// the file. Returns the offset where the record was written.
    ///
    /// The caller is expected to hand in already-framed bytes — this
    /// function does no encoding or CRC. See [`format::record_crc`] and
    /// the encode helpers in [`format`].
    ///
    /// # Errors
    ///
    /// Returns I/O errors from `pwrite` / file growth.
    pub(crate) fn append_raw(&self, framed: &[u8]) -> Result<u64> {
        let mut writer = self.writer.lock().map_err(|_| Error::LockPoisoned)?;
        let offset = writer.tail;
        let needed = offset.saturating_add(framed.len() as u64);
        if needed > writer.capacity {
            self.grow_locked(&mut writer, needed)?;
        }
        pwrite_all(&mut writer.file, offset, framed)?;
        writer.tail = needed;
        self.tail_atomic.store(needed, Ordering::Release);
        Ok(offset)
    }

    /// Append a record to the file using the writer's reusable encode
    /// buffer. The closure is given a `&mut Vec<u8>` to fill with the
    /// record body (everything between `[record_len]` and `[crc]`,
    /// exclusive of both — i.e., starts with the tag byte).
    ///
    /// This avoids the caller having to allocate a fresh `Vec` per
    /// insert; the writer's buffer is reused across calls.
    ///
    /// Returns the offset where the framed record was written.
    ///
    /// # Errors
    ///
    /// Same as [`Self::append_raw`].
    pub(crate) fn append_with<F>(&self, fill_body: F) -> Result<u64>
    where
        F: FnOnce(&mut Vec<u8>) -> Result<()>,
    {
        let mut writer = self.writer.lock().map_err(|_| Error::LockPoisoned)?;
        // Reset the reusable buffer.
        writer.encode_buf.clear();
        // Reserve space for the leading length prefix; we'll patch
        // it once we know the body size.
        writer.encode_buf.extend_from_slice(&[0_u8; 4]);

        // Fill the body via the closure. This is the only place where
        // user-controlled bytes touch the buffer.
        let body_start = writer.encode_buf.len();
        // Detach the buffer so the closure can borrow it mutably without
        // borrowing `writer` (which we'll need again below). The buffer
        // is moved back at the end.
        let mut buf = std::mem::take(&mut writer.encode_buf);
        let fill_result = fill_body(&mut buf);
        writer.encode_buf = buf;
        fill_result?;

        let body_end = writer.encode_buf.len();
        let body_len = body_end - body_start;
        // Patch the length prefix.
        let len_bytes = (body_len as u32).to_le_bytes();
        writer.encode_buf[0..4].copy_from_slice(&len_bytes);
        // Append the CRC.
        let crc = format::record_crc(&writer.encode_buf[body_start..body_end]);
        writer.encode_buf.extend_from_slice(&crc.to_le_bytes());

        // Write to disk via pwrite (no seek syscall).
        let offset = writer.tail;
        let total_len = writer.encode_buf.len() as u64;
        let needed = offset.saturating_add(total_len);
        if needed > writer.capacity {
            self.grow_locked(&mut writer, needed)?;
        }
        let WriterState {
            file, encode_buf, ..
        } = &mut *writer;
        pwrite_all(file, offset, encode_buf.as_slice())?;
        writer.tail = needed;
        self.tail_atomic.store(needed, Ordering::Release);
        Ok(offset)
    }

    /// Append a batch of records under a single writer-lock hold and a
    /// single `write_all` syscall. The closure is given a [`BatchEncoder`]
    /// with one method, [`BatchEncoder::push_record`], which appends one
    /// framed record and returns its absolute file offset. The encoder
    /// reuses the writer's buffer; the underlying file is grown once
    /// (if needed) before the write.
    ///
    /// Returns whatever the closure returns. Typically callers pass a
    /// closure that builds a `Vec<u64>` of offsets keyed by their
    /// record so the index can be updated after the write.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from grow / write, or any error returned by
    /// the closure.
    pub(crate) fn append_batch_with<F, T>(&self, fill: F) -> Result<T>
    where
        F: FnOnce(&mut BatchEncoder<'_>) -> Result<T>,
    {
        let mut writer = self.writer.lock().map_err(|_| Error::LockPoisoned)?;
        writer.encode_buf.clear();
        let base_offset = writer.tail;
        let result = {
            let mut enc = BatchEncoder {
                buf: &mut writer.encode_buf,
                base_offset,
            };
            fill(&mut enc)?
        };
        let total_len = writer.encode_buf.len() as u64;
        if total_len == 0 {
            return Ok(result);
        }
        let needed = base_offset.saturating_add(total_len);
        if needed > writer.capacity {
            self.grow_locked(&mut writer, needed)?;
        }
        let WriterState {
            file, encode_buf, ..
        } = &mut *writer;
        pwrite_all(file, base_offset, encode_buf.as_slice())?;
        writer.tail = needed;
        self.tail_atomic.store(needed, Ordering::Release);
        Ok(result)
    }

    /// Force pending writes to disk via `fdatasync`.
    ///
    /// Behaviour depends on the active [`FlushPolicy`]:
    ///
    /// - `OnEachFlush` (default): one `sync_data` per call. Same
    ///   shape as v0.7.x.
    /// - `Group`: routes through the coordinator so concurrent
    ///   flushers share one `sync_data`. See
    ///   [`crate::storage::flush`].
    ///
    /// In both cases the header's `tail_hint` is *not* rewritten by
    /// `flush()` — that is a separate cost paid by
    /// [`Self::persist_header`]. The recovery scan validates every
    /// record's CRC and discovers the real tail regardless, so a
    /// stale hint just costs a longer scan, never correctness or
    /// data loss.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from the sync, or [`Error::LockPoisoned`]
    /// on poisoned writer / coordinator lock.
    pub(crate) fn flush(&self) -> Result<()> {
        // Snapshot the sync handle out from under the rwlock so we
        // do not hold the lock across the (potentially long)
        // `sync_data` syscall.
        let sync_handle = {
            let guard = self.sync_handle.read().map_err(|_| Error::LockPoisoned)?;
            Arc::clone(&guard)
        };

        match (&self.policy, self.coord.as_ref()) {
            (FlushPolicy::OnEachFlush, _) | (_, None) => {
                // Run sync_data on the cloned handle; the writer
                // mutex is *not* held, so concurrent appends can
                // make progress while we sync.
                sync_handle.sync_data()?;
                Ok(())
            }
            (FlushPolicy::WriteThrough, _) => {
                // Under `WriteThrough` the OS commits each `pwrite`
                // synchronously, so most of what `sync_data` would
                // flush is already durable. We still call it as a
                // belt-and-braces guarantee — on Windows
                // `FILE_FLAG_WRITE_THROUGH` only covers the data
                // pages, not the FAT/MFT metadata; on Unix `O_SYNC`
                // covers both but the syscall is cheap if there's
                // nothing left dirty. The cost shifts: bulk loads
                // are slower (every `pwrite` waits), per-record
                // flushes are near-free.
                sync_handle.sync_data()?;
                Ok(())
            }
            (FlushPolicy::Group { .. }, Some(coord)) => {
                // The tail we want durable is whatever the writer
                // has appended up to right now. The coordinator
                // returns the snapshot we record at sync-issue time
                // as the durable boundary.
                let target = self.tail_atomic.load(Ordering::Acquire);
                let tail_atomic = &self.tail_atomic;
                coord.run(target, move || {
                    // Capture tail before issuing sync. Bytes up to
                    // this offset are guaranteed in the OS buffer
                    // (pwrite is synchronous). sync_data then
                    // flushes them; bytes appended *during* the
                    // syscall may or may not be covered, and will
                    // ride the next leader cycle.
                    let tail_at_sync = tail_atomic.load(Ordering::Acquire);
                    group_sync(&sync_handle, tail_at_sync)
                })
            }
        }
    }

    /// Persist the in-memory header (with the current `tail_hint`) to
    /// disk and `sync_data` afterwards. Cheap fast-reopen checkpoint:
    /// the next [`Self::open`] of this file will start its recovery
    /// scan from the persisted hint instead of from the data-region
    /// start. Called automatically on graceful drop; can be called
    /// explicitly by callers that want to trade flush latency for
    /// reopen latency.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from the write/sync, or
    /// [`Error::LockPoisoned`] on poisoned locks.
    pub(crate) fn persist_header(&self) -> Result<()> {
        let mut writer = self.writer.lock().map_err(|_| Error::LockPoisoned)?;
        let tail = writer.tail;
        let mut header = self.header.write().map_err(|_| Error::LockPoisoned)?;
        header.tail_hint = tail;
        let mut buf = [0_u8; HEADER_LEN];
        header.encode_into(&mut buf);
        drop(header);

        let _seek = writer.file.seek(SeekFrom::Start(0))?;
        writer.file.write_all(&buf)?;
        writer.file.sync_data()?;
        Ok(())
    }

    /// Grow the file (and the mmap mapping) so the writer can append
    /// at least `min_capacity` bytes total. Caller must hold the
    /// writer lock; mmap is swapped under the mmap lock.
    fn grow_locked(&self, writer: &mut WriterState, min_capacity: u64) -> Result<()> {
        // Round up to the next multiple of GROW_CHUNK_BYTES.
        let new_cap = min_capacity
            .div_ceil(GROW_CHUNK_BYTES)
            .saturating_mul(GROW_CHUNK_BYTES);
        writer.file.set_len(new_cap)?;
        writer.capacity = new_cap;
        // SAFETY: file is the same handle, no external truncation,
        // and we just extended (not shrunk) it.
        let new_mmap = unsafe { Mmap::map(&writer.file)? };
        let mut mmap_guard = self.mmap.write().map_err(|_| Error::LockPoisoned)?;
        *mmap_guard = Arc::new(new_mmap);
        // The cloned sync handle still points at the same inode and
        // remains valid across `set_len` (extending), so we leave it
        // as is. We only refresh it on `swap_underlying`, which
        // changes the underlying file altogether.
        Ok(())
    }

    /// Atomically replace the file backing this Store with the contents
    /// of `replacement_path`. The replacement file must already be
    /// fully written and synced to disk; this method renames it over
    /// the current path, reopens our File handle, and refreshes the
    /// mmap snapshot.
    ///
    /// Existing readers holding old `Arc<Mmap>` snapshots stay valid:
    /// on Linux/macOS the kernel keeps the original inode alive until
    /// the last mapping releases. On Windows the rename uses
    /// `MoveFileEx(MOVEFILE_REPLACE_EXISTING)` semantics — the old
    /// file's inode persists for the duration of any active mapping.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from rename / reopen / mmap, or
    /// [`Error::LockPoisoned`] on any poisoned lock. The header is
    /// re-decoded from the new file; a corrupt new header surfaces as
    /// the appropriate format error.
    pub(crate) fn swap_underlying(&self, replacement_path: &Path) -> Result<()> {
        let mut writer = self.writer.lock().map_err(|_| Error::LockPoisoned)?;
        let mut mmap_guard = self.mmap.write().map_err(|_| Error::LockPoisoned)?;

        // Drop our writer's File handle so the OS releases its lock on
        // `self.path`. On Windows this is required before `rename` can
        // overwrite. Existing reader-side mmaps stay alive via their
        // own `Arc<Mmap>` clones — the kernel keeps the inode pinned.
        let placeholder = OpenOptions::new().read(true).open(replacement_path)?;
        let old_file = std::mem::replace(&mut writer.file, placeholder);
        drop(old_file);

        // Atomic rename: replacement → original path.
        std::fs::rename(replacement_path, &self.path)?;

        // Reopen the writer's File handle on the (now new) original
        // path. Preserve the original `FlushPolicy` so a database
        // opened with `WriteThrough` keeps the synchronous-write
        // semantics across an atomic swap (e.g. compaction).
        let new_file = open_options_for(&self.policy)
            .read(true)
            .write(true)
            .open(&self.path)?;
        writer.file = new_file;

        // Re-read the new header to get its tail_hint and capacity.
        let new_len = writer.file.metadata()?.len();
        let mut header_buf = [0_u8; HEADER_LEN];
        let _seek = writer.file.seek(SeekFrom::Start(0))?;
        writer.file.read_exact(&mut header_buf)?;
        let new_header = Header::decode_from(&header_buf)?;

        writer.tail = new_header.tail_hint;
        writer.capacity = new_len;
        self.tail_atomic
            .store(new_header.tail_hint, Ordering::Release);

        // Refresh the in-memory header.
        let mut hdr = self.header.write().map_err(|_| Error::LockPoisoned)?;
        *hdr = new_header;
        drop(hdr);

        // Refresh the mmap. SAFETY: writer.file is the freshly-opened
        // handle; we hold the writer mutex so no concurrent writes;
        // the mapping covers the whole new file.
        let new_mmap = unsafe { Mmap::map(&writer.file)? };
        *mmap_guard = Arc::new(new_mmap);

        // The previous `sync_handle` was cloned from the *old* file;
        // after the rename it points at the now-orphaned inode. Replace
        // it with a clone of the new writer handle so future syncs land
        // on the canonical file.
        let new_sync = writer.file.try_clone()?;
        let mut sync_guard = self.sync_handle.write().map_err(|_| Error::LockPoisoned)?;
        *sync_guard = Arc::new(new_sync);

        Ok(())
    }

    /// Replace the in-memory tail with a recovery-scanned value. Used
    /// after open to truncate to the last valid record.
    ///
    /// Caller is expected to have already determined the truncation
    /// offset by walking the file and validating CRCs.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockPoisoned`] on poisoned lock or I/O on the
    /// truncation.
    pub(crate) fn set_tail_after_recovery(&self, new_tail: u64) -> Result<()> {
        let mut writer = self.writer.lock().map_err(|_| Error::LockPoisoned)?;
        writer.tail = new_tail;
        self.tail_atomic.store(new_tail, Ordering::Release);
        // Capacity stays the same — we don't shrink the file. The
        // truncated tail bytes will be overwritten by future appends.
        Ok(())
    }
}

#[inline]
fn now_unix_millis() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis().min(u64::MAX as u128) as u64)
}

/// Build an [`OpenOptions`] with the platform-specific
/// synchronous-write flag set when the active policy is
/// [`FlushPolicy::WriteThrough`].
///
/// Centralised so every code path that opens the data file (initial
/// open, post-`swap_underlying` reopen) agrees on which flags are
/// needed for which policy. Tier-1 platforms — Linux, macOS,
/// Windows — apply a real synchronous-write flag here; other Unix
/// targets fall back to a flag value of 0 and rely on explicit
/// `sync_data` calls (the `WriteThrough` semantics degrade to
/// `OnEachFlush` on uncertified targets, which is correct but
/// loses the perf win — documented in the policy's doc comment).
fn open_options_for(policy: &FlushPolicy) -> OpenOptions {
    let mut options = OpenOptions::new();
    if matches!(policy, FlushPolicy::WriteThrough) {
        #[cfg(windows)]
        {
            // SAFETY: `custom_flags` accepts arbitrary `u32` flag
            // values; FILE_FLAG_WRITE_THROUGH is documented in
            // CreateFile docs as a valid combination with all other
            // flags we use (read/write/create/no-truncate). The
            // value `0x80000000` is the documented constant.
            let _ = options.custom_flags(FILE_FLAG_WRITE_THROUGH);
        }
        #[cfg(unix)]
        {
            // On uncertified Unix targets `O_SYNC_FLAG` is `0`,
            // which `custom_flags(0)` is documented to mean "no
            // additional flags" — same as `OpenOptions::new()`
            // would produce. So we still call it unconditionally
            // and let the platform constant decide.
            let _ = options.custom_flags(O_SYNC_FLAG);
        }
    }
    options
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_path(label: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0_u128, |d| d.as_nanos());
        p.push(format!("emdb-store-{label}-{nanos}.emdb"));
        p
    }

    #[test]
    fn open_creates_fresh_file_with_header() {
        let path = tmp_path("create");
        let store = Store::open(path.clone(), 0).expect("open");
        let header = store.header().expect("header");
        assert_eq!(header.flags, 0);
        assert_eq!(header.tail_hint, HEADER_LEN as u64);
        assert_eq!(store.tail(), HEADER_LEN as u64);
        drop(store);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn append_with_writes_framed_record_and_advances_tail() {
        let path = tmp_path("append");
        let store = Store::open(path.clone(), 0).expect("open");
        let initial_tail = store.tail();

        let offset = store
            .append_with(|buf| {
                buf.push(format::TAG_INSERT);
                format::encode_insert_body(buf, 0, b"key", b"val", 0);
                Ok(())
            })
            .expect("append");

        assert_eq!(offset, initial_tail);
        let new_tail = store.tail();
        assert!(new_tail > initial_tail);

        // Read the record back through the mmap.
        let mmap = store.mmap().expect("mmap");
        let bytes = &mmap[offset as usize..new_tail as usize];
        let decoded = format::try_decode_record(bytes, 0, offset)
            .expect("decode")
            .expect("some");
        match decoded.view {
            format::RecordView::Insert {
                ns_id,
                key,
                value,
                expires_at,
            } => {
                assert_eq!(ns_id, 0);
                assert_eq!(key, b"key");
                assert_eq!(value, b"val");
                assert_eq!(expires_at, 0);
            }
            _ => panic!("expected Insert"),
        }

        drop(store);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn flush_persists_header_with_tail_hint() {
        let path = tmp_path("flush");
        let store = Store::open(path.clone(), 0).expect("open");
        let _ = store
            .append_with(|buf| {
                buf.push(format::TAG_INSERT);
                format::encode_insert_body(buf, 0, b"k", b"v", 0);
                Ok(())
            })
            .expect("append");
        let tail_before_flush = store.tail();
        store.flush().expect("flush");
        drop(store);

        // Reopen and check that the header's tail_hint matches.
        let reopened = Store::open(path.clone(), 0).expect("reopen");
        let header = reopened.header().expect("header");
        assert_eq!(header.tail_hint, tail_before_flush);
        drop(reopened);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn append_grows_file_when_needed() {
        let path = tmp_path("grow");
        let store = Store::open(path.clone(), 0).expect("open");
        // Hammer enough records to overflow the initial 1 MiB allocation.
        let value = vec![b'x'; 4096];
        for i in 0_u32..400 {
            let key = format!("k{i:04}").into_bytes();
            let _ = store
                .append_with(|buf| {
                    buf.push(format::TAG_INSERT);
                    format::encode_insert_body(buf, 0, &key, &value, 0);
                    Ok(())
                })
                .expect("append");
        }
        let tail = store.tail();
        assert!(
            tail > INITIAL_CAPACITY,
            "should have grown past initial capacity"
        );

        drop(store);
        let _ = std::fs::remove_file(&path);
    }
}
