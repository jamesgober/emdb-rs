// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Storage substrate. Wraps `fsys::JournalHandle` for the write
//! path and a shared `Arc<Mmap>` for zero-copy reads on the same
//! file.
//!
//! ## File layout
//!
//! Two files live alongside each database:
//!
//! - `<path>`: fsys journal file. Bytes 0..N are owned by fsys's
//!   frame format: `[4 magic][4 length][N payload][4 CRC-32C]` per
//!   record. Lock-free LSN reservation, group-commit fsync, NVMe
//!   passthrough flush when available.
//! - `<path>.meta`: emdb's sidecar metadata (encryption salt,
//!   verify block, flags). Written via `fsys::Handle::write` for
//!   atomic-replace updates.
//!
//! ## Concurrency
//!
//! - **Writes**: `fsys::JournalHandle` does lock-free LSN
//!   reservation + concurrent `pwrite`. The handle sits behind a
//!   `RwLock` only so compaction can install the handle for the
//!   rewritten file; appends take the lock shared.
//! - **Reads**: `Arc<Mmap>` over the journal file. Readers get a
//!   cheap clone of the Arc; the kernel keeps a mapping alive even
//!   after the writer grows the file or compaction replaces it, so
//!   readers holding an old snapshot continue uninterrupted.
//! - **Swaps**: compaction replaces the journal, the read file and
//!   the mapping together between [`Store::begin_swap`] and
//!   [`Store::end_swap`]. `swap_seq` is odd while a swap is in
//!   progress and even otherwise; readers that resolve an index
//!   offset against a mapping use [`Store::read_begin`] and
//!   [`Store::read_validate`] to detect that a swap happened in
//!   between and retry.
//!
//! ## Open sequence
//!
//! [`Store::open_with_policy`] validates the data file and opens the
//! read side only. The engine then runs its recovery scan, which
//! also decides whether damage in the journal is a torn tail (safe
//! to cut) or mid-file corruption (refused). Only after that does
//! [`Store::open_journal`] hand the file to fsys, whose open cuts a
//! torn tail. [`Store::finish_open`] persists a new or missing meta
//! sidecar last, so a failed open never writes metadata.

use std::fs::{File, OpenOptions};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{fence, AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use crossbeam_utils::CachePadded;
use memmap2::{Mmap, MmapOptions};
use parking_lot::{Mutex, RwLock};

use crate::error::from_fsys;
use crate::storage::flush::FlushPolicy;
use crate::storage::format;
use crate::storage::meta::{self, MetaHeader};
use crate::{Error, Result};

/// fsys frame magic as it appears on disk (big-endian `0x46535901`).
pub(crate) const FSYS_FRAME_MAGIC: [u8; 4] = [0x46, 0x53, 0x59, 0x01];
/// fsys frame overhead: 4 magic + 4 length + 4 CRC = 12 bytes.
/// Constant per fsys journal frame format (v1 wire format).
pub(crate) const FSYS_FRAME_OVERHEAD: u64 = 12;
/// Number of leading frame-header bytes before the payload starts.
/// 4 magic + 4 length = 8 bytes preceding the payload.
pub(crate) const FSYS_PRE_PAYLOAD_BYTES: u64 = 8;
/// Number of trailing frame bytes after the payload (the CRC).
const FSYS_POST_PAYLOAD_BYTES: u64 = 4;
/// Largest payload one fsys v1 frame can carry (256 MiB - 1).
pub(crate) const FSYS_MAX_PAYLOAD: u64 = (1 << 28) - 1;

/// Storage substrate handle.
///
/// Held inside an `Arc<Store>` by [`crate::storage::engine::Engine`];
/// every code path that needs to append, sync, or mmap-read goes
/// through this type.
pub(crate) struct Store {
    /// Canonical on-disk path of the journal file.
    path: PathBuf,
    /// fsys journal: the write path. `None` until
    /// [`Self::open_journal`] runs at the end of the open sequence.
    /// Replaced (under the write lock) by compaction.
    journal: RwLock<Option<fsys::JournalHandle>>,
    /// fsys top-level handle for sidecar (meta-file) writes and for
    /// the journals compaction and backup write.
    fs: fsys::Handle,
    /// Read-only `File` retained for re-mmap on file growth.
    /// Lock order: `read_file` before `mmap`. Both refresh and swap
    /// hold both locks while they install a mapping, so a refresh
    /// can never install a mapping of a file a swap already retired.
    read_file: Mutex<File>,
    /// Atomically-swapped read mapping. Readers grab a snapshot
    /// via `Arc::clone`.
    mmap: RwLock<Arc<Mmap>>,
    /// Byte length covered by the active mapping. Updated under the
    /// mmap write lock. `CachePadded` so the read-path load does not
    /// false-share with neighbouring fields.
    mmap_len: CachePadded<AtomicU64>,
    /// Swap sequence: even while stable, odd while compaction is
    /// replacing the file. Doubles as the file generation counter.
    swap_seq: CachePadded<AtomicU64>,
    /// Active flush policy.
    policy: FlushPolicy,
    /// Decoded meta sidecar.
    meta: RwLock<MetaHeader>,
    /// `true` while the in-memory meta has not been written to disk
    /// yet: a fresh database, or a journal whose sidecar is missing.
    /// Writes are deferred to [`Self::finish_open`] so that an open
    /// that fails (wrong key, corrupt journal) leaves no sidecar.
    meta_deferred: AtomicBool,
    /// `true` when this open created the data file, so
    /// [`Self::finish_open`] syncs the directory entry.
    created: bool,
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store")
            .field("path", &self.path)
            .field("policy", &self.policy)
            .field("tail", &self.tail())
            .finish()
    }
}

impl Store {
    /// Validate the data file and open the read side of a database.
    ///
    /// - A missing data file is created empty.
    /// - A non-empty data file must start with the fsys frame magic
    ///   (or with zero bytes, which a crash during the first append
    ///   can leave). Anything else is not an emdb journal and is
    ///   rejected with [`Error::MagicMismatch`] without touching it.
    /// - The meta sidecar is read and validated. When it is missing
    ///   a fresh header carrying `flags` is kept in memory and only
    ///   written by [`Self::finish_open`].
    ///
    /// The journal itself is not opened here; see
    /// [`Self::open_journal`].
    ///
    /// `iouring_sqpoll_idle_ms`, when `Some`, opts the journal's
    /// per-handle io_uring ring into Linux kernel-side `SQPOLL`
    /// submission polling with the given idle window. Ignored on
    /// macOS / Windows.
    pub(crate) fn open_with_policy(
        path: PathBuf,
        flags: u32,
        policy: FlushPolicy,
        iouring_sqpoll_idle_ms: Option<u32>,
    ) -> Result<Self> {
        // `Workload::Database` is the preset fsys ships for storage
        // engines (8 MiB buffer pool, 256-deep io_uring ring).
        let mut fs_builder = fsys::builder().tune_for(fsys::Workload::Database);
        if let Some(idle_ms) = iouring_sqpoll_idle_ms {
            fs_builder = fs_builder.sqpoll(idle_ms);
        }
        let fs = fs_builder.build().map_err(from_fsys)?;

        let created = prepare_data_file(&path)?;

        let (meta, meta_deferred) = match meta::read(&path)? {
            Some(existing) => (existing, false),
            None => (MetaHeader::fresh(flags), true),
        };

        let read_file = OpenOptions::new()
            .read(true)
            .open(&path)
            .map_err(Error::Io)?;
        // SAFETY: the mapping is created read-only over `read_file`,
        // which this store keeps open for as long as it may remap.
        // No process truncates the file while this mapping is
        // installed: emdb's lockfile excludes other emdb writers,
        // and `open_journal` (the only place fsys can cut a torn
        // tail) replaces this mapping with an empty one first.
        // Appends only grow the file, which leaves the mapped range
        // valid.
        let initial_mmap = unsafe { Mmap::map(&read_file)? };
        let mmap_len = initial_mmap.len() as u64;

        Ok(Self {
            path,
            journal: RwLock::new(None),
            fs,
            read_file: Mutex::new(read_file),
            mmap: RwLock::new(Arc::new(initial_mmap)),
            mmap_len: CachePadded::new(AtomicU64::new(mmap_len)),
            swap_seq: CachePadded::new(AtomicU64::new(0)),
            policy,
            meta: RwLock::new(meta),
            meta_deferred: AtomicBool::new(meta_deferred),
            created,
        })
    }

    /// Open the fsys journal for appends. Called by the engine after
    /// its recovery scan accepted the journal.
    ///
    /// fsys's open cuts a torn tail (saving any non-zero bytes to a
    /// `<file>.corrupt-<offset>` sidecar first). Windows refuses to
    /// shrink a file that has a mapped view, and on Unix touching a
    /// mapped page past the new end raises `SIGBUS`, so the read
    /// mapping is dropped for the duration of the open and rebuilt
    /// afterwards.
    pub(crate) fn open_journal(&self) -> Result<()> {
        {
            let file_guard = self.read_file.lock();
            let mut mmap_guard = self.mmap.write();
            *mmap_guard = Arc::new(empty_mapping(&file_guard)?);
            self.mmap_len.store(0, Ordering::Release);
        }

        // Long write-lifetime hint (Linux NVMe `F_SET_RW_HINT`) so the
        // SSD groups journal data into long-lived NAND blocks. No-op
        // on macOS / Windows. Buffered mode (default) keeps the
        // mmap-visibility invariant: once `append` returns, the
        // bytes are in the OS page cache.
        let journal = self
            .fs
            .journal_with(&self.path, journal_options())
            .map_err(from_fsys)?;
        *self.journal.write() = Some(journal);

        self.remap_exact()
    }

    /// Complete the open sequence: persist a meta sidecar that was
    /// held back (fresh database, or a journal whose sidecar was
    /// missing) and make a newly created data file's directory entry
    /// durable.
    pub(crate) fn finish_open(&self) -> Result<()> {
        let new_entries = self.created || self.meta_deferred.load(Ordering::Acquire);
        if self.meta_deferred.load(Ordering::Acquire) {
            let header = *self.meta.read();
            meta::write_with(&self.fs, &self.path, &header)?;
            self.meta_deferred.store(false, Ordering::Release);
        }
        // A new database: make both directory entries durable. (The
        // data file may also have been created empty by the caller
        // before the open; its first sidecar marks it as new.)
        if new_entries {
            sync_dir(&self.path)?;
        }
        Ok(())
    }

    /// On-disk path of the journal file.
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Read a snapshot of the meta sidecar.
    pub(crate) fn header(&self) -> Result<MetaHeader> {
        Ok(*self.meta.read())
    }

    /// Logical end-of-data byte offset within the journal file: the
    /// next LSN fsys hands out. Before the journal is open (during the
    /// open sequence) this is the length of the data file as mapped,
    /// so callers can tell an empty journal from a non-empty one.
    pub(crate) fn tail(&self) -> u64 {
        self.journal.read().as_ref().map_or_else(
            || self.mmap_len.load(Ordering::Acquire),
            |journal| journal.next_lsn().as_u64(),
        )
    }

    /// fsys top-level handle. Compaction and backup open their
    /// rewrite journals through it so they share its tuning.
    pub(crate) fn fs(&self) -> &fsys::Handle {
        &self.fs
    }

    /// Current mapping, without any refresh.
    fn current_mmap(&self) -> Arc<Mmap> {
        Arc::clone(&self.mmap.read())
    }

    /// Borrow a read mapping that covers at least up to byte
    /// `end_offset`. Refreshes the mapping once when the current one
    /// is shorter. Callers that read a record should use
    /// [`Self::mmap_for_payload`], which also covers the record's
    /// end.
    pub(crate) fn mmap_covering(&self, end_offset: u64) -> Result<Arc<Mmap>> {
        let cur_len = self.mmap_len.load(Ordering::Acquire);
        if end_offset > cur_len {
            self.refresh_mmap()?;
        }
        Ok(self.current_mmap())
    }

    /// Borrow a read mapping that covers the whole payload starting
    /// at `payload_start`, including its last byte.
    ///
    /// Reads the payload length from the fsys frame header in front
    /// of `payload_start` and refreshes the mapping when the payload
    /// ends past it. A mapping taken while a large append was still
    /// being written can cover a record's start but not its end;
    /// checking only the first byte would then report an
    /// acknowledged record as missing.
    pub(crate) fn mmap_for_payload(&self, payload_start: u64) -> Result<Arc<Mmap>> {
        let mmap = self.mmap_covering(payload_start)?;
        let Ok(start) = usize::try_from(payload_start) else {
            return Ok(mmap);
        };
        if let Ok(len) = format::payload_len_at(&mmap, start) {
            let end = payload_start.saturating_add(len as u64);
            if end > mmap.len() as u64 {
                self.refresh_mmap()?;
                return Ok(self.current_mmap());
            }
        }
        Ok(mmap)
    }

    /// Start a read that resolves an index offset against a mapping.
    /// Returns the swap sequence to hand to [`Self::read_validate`].
    /// Waits while a swap is in progress (the window covers a rename
    /// and a few pointer stores).
    pub(crate) fn read_begin(&self) -> u64 {
        loop {
            let seq = self.swap_seq.load(Ordering::Acquire);
            if seq & 1 == 0 {
                return seq;
            }
            std::thread::yield_now();
        }
    }

    /// `true` when no swap started since [`Self::read_begin`]
    /// returned `seq`, so every offset and mapping the read used
    /// belong to the same file.
    pub(crate) fn read_validate(&self, seq: u64) -> bool {
        // The acquire fence orders the reads the caller made (index
        // probes, mapping snapshot) before the sequence re-load, the
        // reader half of the classic seqlock protocol.
        fence(Ordering::Acquire);
        self.swap_seq.load(Ordering::Relaxed) == seq
    }

    /// Mark the start of a file swap. Must be paired with
    /// [`Self::end_swap`]; the caller holds the engine's write gate
    /// exclusively, so at most one swap runs at a time.
    pub(crate) fn begin_swap(&self) {
        let _previous = self.swap_seq.fetch_add(1, Ordering::AcqRel);
        fence(Ordering::Release);
    }

    /// Mark the end of a file swap started with [`Self::begin_swap`].
    pub(crate) fn end_swap(&self) {
        let _previous = self.swap_seq.fetch_add(1, Ordering::Release);
    }

    /// Install a rewritten journal file: the journal handle that
    /// wrote it, a read handle, and a mapping of it. The caller has
    /// already renamed the file onto [`Self::path`] and runs this
    /// between [`Self::begin_swap`] and [`Self::end_swap`].
    ///
    /// Infallible on purpose: once the rename is done the new file
    /// is the database, so nothing after it may fail half-way. The
    /// old journal is dropped (and its final sync runs) after the
    /// locks are released.
    pub(crate) fn install_file(&self, journal: fsys::JournalHandle, read_file: File, mmap: Mmap) {
        let old_journal = {
            let mut journal_guard = self.journal.write();
            journal_guard.replace(journal)
        };
        {
            let mut file_guard = self.read_file.lock();
            let mut mmap_guard = self.mmap.write();
            let new_len = mmap.len() as u64;
            *mmap_guard = Arc::new(mmap);
            *file_guard = read_file;
            self.mmap_len.store(new_len, Ordering::Release);
        }
        drop(old_journal);
    }

    /// Append a payload to the journal. Returns the byte offset
    /// of the payload's first byte within the journal file;
    /// this is what the engine stores in its in-memory index.
    ///
    /// Under [`FlushPolicy::WriteThrough`] the call also
    /// `sync_through`s the new tail, so the bytes are durable
    /// on stable storage before this returns.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] with [`std::io::ErrorKind::InvalidInput`] when
    /// the payload exceeds the 256 MiB fsys frame cap, or any I/O
    /// error from the write or sync.
    pub(crate) fn append(&self, payload: &[u8]) -> Result<u64> {
        let payload_len = payload.len() as u64;
        let guard = self.journal.read();
        let journal = guard
            .as_ref()
            .ok_or(Error::InvalidConfig(JOURNAL_NOT_OPEN))?;
        let end_lsn = journal.append(payload).map_err(from_fsys)?.as_u64();
        let payload_start = end_lsn - FSYS_POST_PAYLOAD_BYTES - payload_len;

        // The mmap is not refreshed here: readers refresh lazily via
        // `mmap_for_payload` (one remap per read after a write burst,
        // not one per append).
        if matches!(self.policy, FlushPolicy::WriteThrough) {
            journal
                .sync_through(fsys::Lsn::new(end_lsn))
                .map_err(from_fsys)?;
        }

        Ok(payload_start)
    }

    /// Closure-style append. Allocates a small `Vec<u8>` per
    /// call, hands it to `fill_payload` so the caller can encode
    /// the tag byte + body in place, then routes through
    /// [`Self::append`].
    pub(crate) fn append_with<F>(&self, fill_payload: F) -> Result<u64>
    where
        F: FnOnce(&mut Vec<u8>) -> Result<()>,
    {
        let mut buf = Vec::with_capacity(64);
        fill_payload(&mut buf)?;
        self.append(&buf)
    }

    /// Append a batch of payloads under a single vectored
    /// submission. Returns the per-payload start offsets in the
    /// same order, matching the input.
    ///
    /// Routes through `fsys::JournalHandle::append_batch` so the
    /// whole batch lands in one LSN reservation, one heap
    /// allocation for the concatenated frames, and one platform
    /// `pwrite`.
    pub(crate) fn append_batch<'a, I>(&self, payloads: I) -> Result<Vec<u64>>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        let payloads: Vec<&[u8]> = payloads.into_iter().collect();
        if payloads.is_empty() {
            return Ok(Vec::new());
        }

        let guard = self.journal.read();
        let journal = guard
            .as_ref()
            .ok_or(Error::InvalidConfig(JOURNAL_NOT_OPEN))?;
        let end_lsn = journal.append_batch(&payloads).map_err(from_fsys)?.as_u64();
        let starts = batch_payload_starts(end_lsn, &payloads);

        if matches!(self.policy, FlushPolicy::WriteThrough) {
            journal
                .sync_through(fsys::Lsn::new(end_lsn))
                .map_err(from_fsys)?;
        }

        Ok(starts)
    }

    /// Force pending writes durable to stable storage.
    ///
    /// Calls `fsys::JournalHandle::sync_through(next_lsn)`; fsys
    /// coalesces concurrent sync requests into one `fdatasync` (or
    /// NVMe passthrough flush). Fails when the journal is poisoned
    /// by an earlier failed write or sync, because the poisoned
    /// range can never become durable.
    pub(crate) fn flush(&self) -> Result<()> {
        let guard = self.journal.read();
        let Some(journal) = guard.as_ref() else {
            return Ok(());
        };
        let target = journal.next_lsn();
        journal.sync_through(target).map_err(from_fsys)
    }

    /// Persist the meta sidecar (headers + flags + encryption
    /// metadata) through an atomic replace.
    pub(crate) fn persist_meta(&self) -> Result<()> {
        let header = *self.meta.read();
        meta::write_with(&self.fs, &self.path, &header)
    }

    /// Update the meta sidecar's encryption metadata (salt +
    /// verification block).
    ///
    /// During the open sequence (before [`Self::finish_open`]) the
    /// change stays in memory and is written by `finish_open`, so
    /// the verification block reaches disk only after the recovery
    /// scan proved the key decrypts the journal. After the open it
    /// is persisted immediately.
    #[cfg(feature = "encrypt")]
    pub(crate) fn set_encryption_metadata(
        &self,
        salt: [u8; meta::META_SALT_LEN],
        verify: [u8; meta::META_VERIFY_LEN],
    ) -> Result<()> {
        {
            let mut guard = self.meta.write();
            guard.encryption_salt = salt;
            guard.encryption_verify = verify;
            guard.flags |= meta::FLAG_ENCRYPTED;
        }
        self.meta_deferred.store(true, Ordering::Release);
        if self.journal.read().is_some() {
            self.persist_meta()?;
            self.meta_deferred.store(false, Ordering::Release);
        }
        Ok(())
    }

    /// Refresh the mmap from the read file's current size.
    ///
    /// Holds the `read_file` lock while it installs the new mapping
    /// (lock order `read_file` then `mmap`, the same as
    /// [`Self::install_file`]), so a refresh racing a compaction
    /// swap cannot install a mapping of the retired file. Never
    /// replaces a mapping with a shorter one: two concurrent
    /// refreshes may finish in either order.
    fn refresh_mmap(&self) -> Result<()> {
        let file_guard = self.read_file.lock();
        // SAFETY: same invariants as the initial mmap in
        // `open_with_policy`: `file_guard` keeps the file open, the
        // mapping covers the file's size at map time, and nothing
        // shrinks the file while a mapping is installed.
        let new_mmap = unsafe { Mmap::map(&*file_guard)? };
        let new_len = new_mmap.len() as u64;
        let mut mmap_guard = self.mmap.write();
        if new_len > mmap_guard.len() as u64 {
            *mmap_guard = Arc::new(new_mmap);
            self.mmap_len.store(new_len, Ordering::Release);
        }
        Ok(())
    }

    /// Replace the mapping with one of the file's exact current size,
    /// shorter or not. Used after fsys's open may have cut a tail.
    fn remap_exact(&self) -> Result<()> {
        let file_guard = self.read_file.lock();
        // SAFETY: as in `refresh_mmap`; fsys's open has finished
        // cutting the tail, so the file only grows from here.
        let new_mmap = unsafe { Mmap::map(&*file_guard)? };
        let new_len = new_mmap.len() as u64;
        let mut mmap_guard = self.mmap.write();
        *mmap_guard = Arc::new(new_mmap);
        self.mmap_len.store(new_len, Ordering::Release);
        Ok(())
    }

    /// Run a fresh `fsys::JournalReader` over the on-disk journal.
    /// Used by the engine's recovery scan.
    pub(crate) fn open_reader(&self) -> Result<fsys::JournalReader> {
        fsys::JournalReader::open(&self.path).map_err(from_fsys)
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        // Best-effort flush so a graceful drop does not lose writes
        // that are already in the page cache. `Drop` cannot return
        // the error and emdb has no logging facade; callers that
        // need to observe flush failures call `Emdb::flush` before
        // dropping the last handle.
        let _ignored = self.flush();
    }
}

/// Error reason used when an append arrives before the journal is
/// open (only possible from inside the open sequence).
const JOURNAL_NOT_OPEN: &str = "journal is not open yet";

/// Journal options shared by the live journal and the rewrite
/// journals compaction and backup produce.
pub(crate) fn journal_options() -> fsys::JournalOptions {
    fsys::JournalOptions::new().write_lifetime_hint(Some(fsys::WriteLifetimeHint::Long))
}

/// Per-payload start offsets for a batch appended back-to-back whose
/// last frame ends at `end_lsn`. Each record contributes
/// `FSYS_FRAME_OVERHEAD + payload.len()` bytes and its payload begins
/// `FSYS_PRE_PAYLOAD_BYTES` into the frame.
pub(crate) fn batch_payload_starts(end_lsn: u64, payloads: &[&[u8]]) -> Vec<u64> {
    let total_frame_size: u64 = payloads
        .iter()
        .map(|p| FSYS_FRAME_OVERHEAD + p.len() as u64)
        .sum();
    let mut cursor = end_lsn - total_frame_size;
    let mut starts = Vec::with_capacity(payloads.len());
    for payload in payloads {
        starts.push(cursor + FSYS_PRE_PAYLOAD_BYTES);
        cursor += FSYS_FRAME_OVERHEAD + payload.len() as u64;
    }
    starts
}

/// Make sure the data file exists and looks like an fsys journal.
/// Returns `true` when this call created it.
///
/// A non-empty file must begin with the fsys frame magic. A leading
/// zero byte is accepted too: a crash while the very first appends
/// were in flight can leave an unwritten (zero) reservation at
/// offset 0, which the recovery scan classifies. Anything else is
/// refused before fsys gets a chance to treat it as a corrupt tail
/// and cut it, which would empty a file that was never a database.
fn prepare_data_file(path: &Path) -> Result<bool> {
    match std::fs::metadata(path) {
        Ok(meta) if meta.is_dir() => Err(Error::InvalidConfig(
            "database path names a directory, not a file",
        )),
        Ok(meta) if meta.len() == 0 => Ok(false),
        Ok(meta) => {
            let mut head = [0_u8; 4];
            let want = usize::try_from(meta.len().min(4)).unwrap_or(4);
            let mut file = File::open(path)?;
            file.read_exact(&mut head[..want])?;
            let head = &head[..want];
            if head[0] == 0 || head == &FSYS_FRAME_MAGIC[..want] {
                Ok(false)
            } else {
                Err(Error::MagicMismatch)
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            // SECURITY-MERGE: create_private_file (owner-only data file)
            let _file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(path)?;
            Ok(true)
        }
        Err(err) => Err(Error::Io(err)),
    }
}

/// A zero-length mapping of `file`: maps nothing on Windows and a
/// single never-read page on Unix, so fsys may shrink the file.
fn empty_mapping(file: &File) -> Result<Mmap> {
    // SAFETY: a zero-length mapping exposes an empty slice; no byte
    // of the file is ever read through it, so later truncation of
    // the file cannot be observed through this mapping.
    let mmap = unsafe { MmapOptions::new().len(0).map(file)? };
    Ok(mmap)
}

/// Make the directory entry of `path` durable (after a create or a
/// rename). Unix: `fsync` on the parent directory. Windows:
/// `FlushFileBuffers` on a directory handle opened with
/// `FILE_FLAG_BACKUP_SEMANTICS`; file systems that cannot flush a
/// directory (FAT, some network shares) report it as unsupported and
/// the call succeeds, because NTFS already journals the rename
/// itself. Other targets have no directory sync and return `Ok`.
pub(crate) fn sync_dir(path: &Path) -> Result<()> {
    let dir = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    sync_dir_handle(dir)
}

#[cfg(unix)]
fn sync_dir_handle(dir: &Path) -> Result<()> {
    File::open(dir)?.sync_all()?;
    Ok(())
}

#[cfg(windows)]
fn sync_dir_handle(dir: &Path) -> Result<()> {
    use std::os::windows::fs::OpenOptionsExt;
    // Win32 constants, spelled out to avoid a windows-sys dependency.
    const FILE_WRITE_DATA: u32 = 0x0000_0002;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    // ERROR_INVALID_FUNCTION, ERROR_NOT_SUPPORTED, ERROR_INVALID_PARAMETER:
    // what a file system without directory flush support returns.
    const UNSUPPORTED: [i32; 3] = [1, 50, 87];
    let handle = OpenOptions::new()
        .access_mode(FILE_WRITE_DATA)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(dir)?;
    match handle.sync_all() {
        Err(err) if err.raw_os_error().is_some_and(|c| UNSUPPORTED.contains(&c)) => Ok(()),
        other => other.map_err(Error::Io),
    }
}

#[cfg(not(any(unix, windows)))]
fn sync_dir_handle(_dir: &Path) -> Result<()> {
    // No portable directory sync exists on this target; renames are
    // as durable as the platform makes them.
    Ok(())
}

/// Remove `path` if it exists. `NotFound` is success.
pub(crate) fn remove_if_exists(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(Error::Io(err)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_dir(label: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0_u128, |d| d.as_nanos());
        let mut p = std::env::temp_dir();
        p.push(format!("emdb-store-{label}-{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&p).expect("mkdir");
        p
    }

    #[test]
    fn test_sync_dir_existing_directory_succeeds() {
        let dir = tmp_dir("syncdir");
        let file = dir.join("x");
        std::fs::write(&file, b"x").expect("write");
        sync_dir(&file).expect("sync dir");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_sync_dir_missing_directory_errors() {
        let dir = tmp_dir("syncdir-missing");
        let missing = dir.join("nope").join("x");
        assert!(sync_dir(&missing).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_prepare_data_file_foreign_bytes_rejected_untouched() {
        let dir = tmp_dir("foreign");
        let path = dir.join("notes.txt");
        std::fs::write(&path, b"hello, not a journal").expect("write");
        assert!(matches!(
            prepare_data_file(&path),
            Err(Error::MagicMismatch)
        ));
        assert_eq!(std::fs::read(&path).expect("read"), b"hello, not a journal");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_prepare_data_file_short_magic_prefix_accepted() {
        let dir = tmp_dir("short");
        let path = dir.join("db");
        std::fs::write(&path, &FSYS_FRAME_MAGIC[..2]).expect("write");
        assert!(!prepare_data_file(&path).expect("accepted"));
        std::fs::write(&path, [0_u8; 3]).expect("write");
        assert!(!prepare_data_file(&path).expect("accepted"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_prepare_data_file_missing_is_created() {
        let dir = tmp_dir("create");
        let path = dir.join("db");
        assert!(prepare_data_file(&path).expect("created"));
        assert_eq!(std::fs::metadata(&path).expect("meta").len(), 0);
        assert!(!prepare_data_file(&path).expect("exists"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_batch_payload_starts_layout() {
        let a: &[u8] = b"abc";
        let b: &[u8] = b"";
        let c: &[u8] = b"zz";
        // Three frames: 15 + 12 + 14 = 41 bytes ending at 141.
        let starts = batch_payload_starts(141, &[a, b, c]);
        assert_eq!(starts, vec![108, 123, 135]);
    }

    #[test]
    fn test_remove_if_exists_missing_is_ok() {
        let dir = tmp_dir("rm");
        remove_if_exists(&dir.join("absent")).expect("ok");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
