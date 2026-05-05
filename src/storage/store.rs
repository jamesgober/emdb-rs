// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Storage substrate. Wraps `fsys::JournalHandle` for the write
//! path and a shared `Arc<Mmap>` for zero-copy reads on the same
//! file.
//!
//! ## File layout
//!
//! Two files live alongside each database:
//!
//! - `<path>` — fsys journal file. Bytes 0..N are owned by fsys's
//!   frame format: `[4 magic][4 length][N payload][4 crc]` per
//!   record. Lock-free LSN reservation, group-commit fsync, NVMe
//!   passthrough flush when available.
//! - `<path>.meta` — emdb's sidecar metadata (encryption salt,
//!   verify block, flags). Written via `fsys::Handle::write` for
//!   atomic-replace updates.
//!
//! ## Concurrency
//!
//! - **Writes**: `fsys::JournalHandle` does lock-free LSN
//!   reservation + concurrent `pwrite`. The hot append path holds
//!   no mutex.
//! - **Reads**: `Arc<Mmap>` over the journal file. Readers get a
//!   cheap clone of the Arc; the kernel keeps the mapping alive
//!   even after the writer grows the file (we re-map post-append
//!   when the journal extends past the current mapping; old
//!   readers holding the old Arc continue uninterrupted).
//! - **Sync**: `flush()` calls `journal.sync_through(latest_lsn)`.
//!   fsys coalesces concurrent sync requests into a single
//!   `fdatasync` (or NVMe passthrough flush where supported).
//!
//! ## Crash safety
//!
//! Recovery is delegated to `fsys::JournalReader`: walks frames
//! forward, validates each CRC-32C, stops cleanly at the first
//! malformed tail. The reader's `JournalTailState` distinguishes
//! a clean shutdown from a torn write.

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use memmap2::Mmap;

use crate::storage::flush::FlushPolicy;
use crate::storage::meta::{self, MetaHeader};
use crate::{Error, Result};

/// fsys frame overhead: 4 magic + 4 length + 4 CRC = 12 bytes.
/// Constant per fsys 0.9.x v1 journal frame format.
const FSYS_FRAME_OVERHEAD: u64 = 12;
/// Number of leading frame-header bytes before the payload starts.
/// 4 magic + 4 length = 8 bytes preceding the payload.
const FSYS_PRE_PAYLOAD_BYTES: u64 = 8;
/// Number of trailing frame bytes after the payload (the CRC).
const FSYS_POST_PAYLOAD_BYTES: u64 = 4;

/// Storage substrate handle. Cheap-clone via `Arc`.
///
/// Held inside an `Arc<Store>` by [`crate::storage::engine::Engine`];
/// every code path that needs to append, sync, or mmap-read goes
/// through this type.
pub(crate) struct Store {
    /// Canonical on-disk path of the journal file.
    path: PathBuf,
    /// fsys journal — the write path. Lock-free append + group-
    /// commit fsync. `Arc` because we share the handle across
    /// engine threads via `Store`'s own `Arc`.
    journal: Arc<fsys::JournalHandle>,
    /// fsys top-level handle for sidecar (meta-file) writes.
    /// Re-used across meta writes so we don't pay the
    /// builder-init cost per write.
    fs: fsys::Handle,
    /// Read-only `File` retained for re-mmap on file growth.
    /// Mutex-guarded so we can re-stat + remap atomically without
    /// racing concurrent writers' growth.
    read_file: Mutex<File>,
    /// Atomically-swapped read mapping. Readers grab a snapshot
    /// via `Arc::clone`; writes remap when the journal extends
    /// past the current mapping length.
    mmap: RwLock<Arc<Mmap>>,
    /// Tracks the byte length covered by the active mapping.
    /// Updated under the mmap write-lock when remapping. Read
    /// lock-free on the writer's append fast path to decide
    /// whether to trigger a remap.
    mmap_len: AtomicU64,
    /// Active flush policy. Drives `flush()` semantics:
    /// `OnEachFlush` and `Group` both call `sync_through(latest)`;
    /// `WriteThrough` syncs after every append.
    policy: FlushPolicy,
    /// Decoded meta sidecar. Updated when encryption metadata
    /// changes (verification block on first encrypted open;
    /// salt rotation on key rotation).
    meta: Arc<RwLock<MetaHeader>>,
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store")
            .field("path", &self.path)
            .field("policy", &self.policy)
            .field("next_lsn", &self.journal.next_lsn().as_u64())
            .finish()
    }
}

impl Store {
    /// Open or create a database at `path` with default flush
    /// policy ([`FlushPolicy::OnEachFlush`]).
    ///
    /// `flags` are encryption / feature bits persisted in the
    /// meta sidecar on a fresh database; ignored on reopen
    /// (existing meta wins).
    pub(crate) fn open(path: PathBuf, flags: u32) -> Result<Self> {
        Self::open_with_policy(path, flags, FlushPolicy::default())
    }

    /// Open or create a database with explicit flush policy.
    ///
    /// On a fresh path: writes a meta sidecar with `flags` and
    /// opens an empty journal. On an existing path: reads the
    /// meta sidecar (validates magic + version + CRC) and opens
    /// the journal in append mode.
    pub(crate) fn open_with_policy(
        path: PathBuf,
        flags: u32,
        policy: FlushPolicy,
    ) -> Result<Self> {
        // Resolve or create the meta sidecar.
        let meta = match meta::read(&path)? {
            Some(existing) => existing,
            None => {
                let fresh = MetaHeader::fresh(flags);
                meta::write(&path, &fresh)?;
                fresh
            }
        };

        // Build a top-level fsys handle. `Method::Auto` picks the
        // best primitive for the host (NVMe passthrough flush /
        // io_uring on Linux / `WRITE_THROUGH` on Windows where
        // appropriate); we let fsys decide.
        let fs = fsys::builder()
            .build()
            .map_err(|err| Error::Io(std::io::Error::other(format!("fsys init: {err}"))))?;

        // Open the journal. Buffered mode (default) keeps the
        // mmap-visibility invariant: once `append` returns, the
        // bytes are in the OS page cache and any subsequent mmap
        // covering that offset will see them.
        let journal = fs
            .journal(&path)
            .map_err(|err| Error::Io(std::io::Error::other(format!("fsys journal: {err}"))))?;
        let journal = Arc::new(journal);

        // Open a read-only File handle for the mmap path. This
        // is a separate fd from fsys's internal writer; both
        // handles point at the same inode.
        let read_file = OpenOptions::new()
            .read(true)
            .open(&path)
            .map_err(Error::Io)?;
        // SAFETY: the file backing the mmap is held alive by
        // `read_file` for the duration of the mapping. The mmap
        // covers the file's current size at map time. Concurrent
        // writes via fsys are safe — fsys's pwrite extends the
        // file, but the mmap region stays mapped to its original
        // range. We re-mmap whenever the journal grows past the
        // current mapping; readers holding old `Arc<Mmap>`
        // snapshots continue to read from the old mapping until
        // they release.
        let initial_mmap = unsafe { Mmap::map(&read_file)? };
        let mmap_len = initial_mmap.len() as u64;

        Ok(Self {
            path,
            journal,
            fs,
            read_file: Mutex::new(read_file),
            mmap: RwLock::new(Arc::new(initial_mmap)),
            mmap_len: AtomicU64::new(mmap_len),
            policy,
            meta: Arc::new(RwLock::new(meta)),
        })
    }

    /// On-disk path of the journal file.
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Read a snapshot of the meta sidecar.
    pub(crate) fn header(&self) -> Result<MetaHeader> {
        let guard = self.meta.read().map_err(|_| Error::LockPoisoned)?;
        Ok(*guard)
    }

    /// Logical end-of-data byte offset within the journal file.
    /// Equivalent to "the size of the file at the moment of this
    /// call", since fsys's journal is append-only and never
    /// pre-allocates past the actual data.
    pub(crate) fn tail(&self) -> u64 {
        self.journal.next_lsn().as_u64()
    }

    /// Borrow a snapshot of the current read mapping. Cheap —
    /// returns an `Arc` clone.
    ///
    /// The returned mapping covers all records appended *up to
    /// the most recent post-append remap*. Records appended
    /// after the current mapping was issued may not be visible
    /// until the next remap fires (writers detect growth past
    /// the current mapping and remap before returning the LSN).
    pub(crate) fn mmap(&self) -> Result<Arc<Mmap>> {
        let guard = self.mmap.read().map_err(|_| Error::LockPoisoned)?;
        Ok(Arc::clone(&guard))
    }

    /// Append a payload to the journal. Returns the byte offset
    /// of the payload's first byte within the journal file —
    /// this is what the engine stores in its in-memory index.
    ///
    /// Bytes 0..N of `payload` become the bytes
    /// `[payload_start..payload_start + N]` of the journal file
    /// (visible via [`Self::mmap`] after this call returns).
    /// The fsys frame's magic + length prefix and trailing CRC
    /// surround the payload but are invisible to the index.
    ///
    /// Under [`FlushPolicy::WriteThrough`] the call also
    /// `sync_through`s the new tail, so the bytes are durable
    /// on stable storage before this returns.
    pub(crate) fn append(&self, payload: &[u8]) -> Result<u64> {
        let payload_len = payload.len() as u64;
        let end_lsn = self
            .journal
            .append(payload)
            .map_err(|err| Error::Io(std::io::Error::other(format!("fsys append: {err}"))))?
            .as_u64();
        let payload_start = end_lsn - FSYS_POST_PAYLOAD_BYTES - payload_len;

        // Refresh the mmap if the journal grew past the current
        // mapping. This happens after the FIRST append on a
        // freshly-opened journal (initial_mmap was empty), and
        // periodically as the file extends past prior capacity.
        let cur_len = self.mmap_len.load(Ordering::Acquire);
        if end_lsn > cur_len {
            self.refresh_mmap()?;
        }

        // Under WriteThrough policy, sync immediately so the
        // bytes are durable before this call returns.
        if matches!(self.policy, FlushPolicy::WriteThrough) {
            self.journal
                .sync_through(fsys::Lsn(end_lsn))
                .map_err(|err| Error::Io(std::io::Error::other(format!("fsys sync: {err}"))))?;
        }

        Ok(payload_start)
    }

    /// Closure-style append. Allocates a small `Vec<u8>` per
    /// call, hands it to `fill_payload` so the caller can encode
    /// the tag byte + body in place, then routes through
    /// [`Self::append`]. Convenience for engine call sites that
    /// want the v0.7-v0.8 closure shape.
    pub(crate) fn append_with<F>(&self, fill_payload: F) -> Result<u64>
    where
        F: FnOnce(&mut Vec<u8>) -> Result<()>,
    {
        let mut buf = Vec::with_capacity(64);
        fill_payload(&mut buf)?;
        self.append(&buf)
    }

    /// Closure-style batch append. The closure is given a
    /// `&mut Vec<Vec<u8>>` it can fill with one entry per
    /// record. After the closure returns, every entry is
    /// appended via [`Self::append`] in order; the per-record
    /// payload-start offsets are returned in the same order.
    pub(crate) fn append_batch_with<F>(&self, fill: F) -> Result<Vec<u64>>
    where
        F: FnOnce(&mut Vec<Vec<u8>>) -> Result<()>,
    {
        let mut payloads: Vec<Vec<u8>> = Vec::new();
        fill(&mut payloads)?;
        let slices: Vec<&[u8]> = payloads.iter().map(|v| v.as_slice()).collect();
        self.append_batch(slices)
    }

    /// Append a batch of payloads under a single concurrent-safe
    /// pass. Returns the per-payload start offsets in the same
    /// order, matching the input.
    ///
    /// fsys's lock-free LSN reservation makes a "batch" no
    /// faster than N independent `append` calls under no
    /// contention — the writer mutex was already gone in fsys
    /// 0.8 — but batch semantics simplify caller code (one
    /// allocation for the offsets vec, no in-loop error
    /// branching) and keep the API parity with the old
    /// `BatchEncoder` shape.
    pub(crate) fn append_batch<'a, I>(&self, payloads: I) -> Result<Vec<u64>>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        let payloads: Vec<&[u8]> = payloads.into_iter().collect();
        let mut starts = Vec::with_capacity(payloads.len());
        let mut last_end_lsn: u64 = 0;
        for payload in payloads {
            let end_lsn = self
                .journal
                .append(payload)
                .map_err(|err| Error::Io(std::io::Error::other(format!("fsys append: {err}"))))?
                .as_u64();
            let payload_start = end_lsn - FSYS_POST_PAYLOAD_BYTES - payload.len() as u64;
            starts.push(payload_start);
            last_end_lsn = end_lsn;
        }

        let cur_len = self.mmap_len.load(Ordering::Acquire);
        if last_end_lsn > cur_len {
            self.refresh_mmap()?;
        }

        if matches!(self.policy, FlushPolicy::WriteThrough) && last_end_lsn > 0 {
            self.journal
                .sync_through(fsys::Lsn(last_end_lsn))
                .map_err(|err| Error::Io(std::io::Error::other(format!("fsys sync: {err}"))))?;
        }

        Ok(starts)
    }

    /// Force pending writes durable to stable storage.
    ///
    /// Calls `fsys::JournalHandle::sync_through(next_lsn)` —
    /// fsys coalesces concurrent sync requests internally, so
    /// callers under N threads that all call `flush()` at once
    /// see exactly one `fdatasync` (or NVMe passthrough flush)
    /// covering everyone's writes.
    ///
    /// Under [`FlushPolicy::WriteThrough`], appends are already
    /// durable on return, so `flush()` is a near-free
    /// "sync the empty tail" call.
    pub(crate) fn flush(&self) -> Result<()> {
        let target = self.journal.next_lsn();
        self.journal
            .sync_through(target)
            .map_err(|err| Error::Io(std::io::Error::other(format!("fsys sync: {err}"))))?;
        Ok(())
    }

    /// Persist the meta sidecar (headers + flags + encryption
    /// metadata). Used on graceful drop and on encryption
    /// metadata changes.
    pub(crate) fn persist_meta(&self) -> Result<()> {
        let header = *self.meta.read().map_err(|_| Error::LockPoisoned)?;
        meta::write(&self.path, &header)?;
        Ok(())
    }

    /// Update the meta sidecar's encryption metadata (salt +
    /// verification block) and persist atomically.
    ///
    /// Used on the first open of an encrypted database (writes
    /// the verification block) and as part of key rotation.
    pub(crate) fn set_encryption_metadata(
        &self,
        salt: [u8; meta::META_SALT_LEN],
        verify: [u8; meta::META_VERIFY_LEN],
    ) -> Result<()> {
        {
            let mut guard = self.meta.write().map_err(|_| Error::LockPoisoned)?;
            guard.encryption_salt = salt;
            guard.encryption_verify = verify;
            guard.flags |= meta::FLAG_ENCRYPTED;
        }
        self.persist_meta()
    }

    /// Atomically replace the journal file with `replacement_path`.
    /// Used by compaction.
    ///
    /// Sequence:
    /// 1. Drop our own read-mmap and read-file handles so
    ///    Windows allows the rename.
    /// 2. Close the existing journal (final sync + drop).
    /// 3. Atomic-rename the replacement file over the canonical
    ///    path.
    /// 4. Reopen the journal on the new path; refresh the
    ///    read-mmap.
    ///
    /// Old `Arc<Mmap>` snapshots held by readers stay valid
    /// through the swap on every supported OS — Linux/macOS keep
    /// the original inode alive while a mapping references it,
    /// and Windows holds the file via the mapping handle.
    pub(crate) fn swap_underlying(self: &Arc<Self>, replacement_path: &Path) -> Result<()> {
        // Close the journal before renaming. We need exclusive
        // ownership of the journal handle, which means no other
        // Arc<Self> clones can be in flight. Caller (Engine) is
        // responsible for ensuring single-ownership at swap time.
        //
        // We can't actually close the journal here because we
        // hold an `Arc<JournalHandle>`. Instead, drop our own
        // strong reference to the journal by replacing it with
        // a journal pointing at the new file.

        // Lock the mmap so no concurrent readers grab the old
        // Arc while we swap.
        let mut mmap_guard = self.mmap.write().map_err(|_| Error::LockPoisoned)?;
        let mut file_guard = self.read_file.lock().map_err(|_| Error::LockPoisoned)?;

        // Drop the old read-file by replacing it with a placeholder
        // pointing at the replacement. Windows requires the file
        // to not be held when we rename over it.
        let placeholder = OpenOptions::new()
            .read(true)
            .open(replacement_path)
            .map_err(Error::Io)?;
        let _old_read_file = std::mem::replace(&mut *file_guard, placeholder);
        drop(_old_read_file);

        // Atomic rename: replacement → original path.
        std::fs::rename(replacement_path, &self.path).map_err(Error::Io)?;

        // Reopen our read-file on the new path.
        let new_read_file = OpenOptions::new()
            .read(true)
            .open(&self.path)
            .map_err(Error::Io)?;
        *file_guard = new_read_file;

        // Re-mmap from the new file.
        // SAFETY: same invariants as the initial mmap in
        // `open_with_policy` — file held alive by `file_guard`,
        // mapping covers the whole file at map time.
        let new_mmap = unsafe { Mmap::map(&*file_guard)? };
        let new_len = new_mmap.len() as u64;
        *mmap_guard = Arc::new(new_mmap);
        self.mmap_len.store(new_len, Ordering::Release);

        Ok(())
    }

    /// Refresh the mmap from the read-file's current state.
    /// Called when the journal extends past the current mapping
    /// (post-append) or after `swap_underlying` wires up a new
    /// file.
    fn refresh_mmap(&self) -> Result<()> {
        let file_guard = self.read_file.lock().map_err(|_| Error::LockPoisoned)?;
        // SAFETY: same invariants as the initial mmap in
        // `open_with_policy` — `file_guard` keeps the fd alive
        // for the duration of the mapping; the mapping covers
        // the file's current size at map time. Concurrent
        // writers via fsys are safe — pwrite extends the file
        // but the mapping stays valid for its mapped range.
        let new_mmap = unsafe { Mmap::map(&*file_guard)? };
        let new_len = new_mmap.len() as u64;
        drop(file_guard);
        let mut mmap_guard = self.mmap.write().map_err(|_| Error::LockPoisoned)?;
        *mmap_guard = Arc::new(new_mmap);
        self.mmap_len.store(new_len, Ordering::Release);
        Ok(())
    }

    /// Run a fresh `fsys::JournalReader` over the on-disk journal.
    /// Used by [`crate::storage::engine::Engine::recovery_scan`]
    /// to walk records and populate the in-memory index.
    pub(crate) fn open_reader(&self) -> Result<fsys::JournalReader> {
        fsys::JournalReader::open(&self.path)
            .map_err(|err| Error::Io(std::io::Error::other(format!("fsys reader: {err}"))))
    }

    /// fsys top-level handle, exposed for atomic-replace meta
    /// sidecar writes by the engine's encryption-admin path.
    pub(crate) fn fs(&self) -> &fsys::Handle {
        &self.fs
    }

    /// Helpers for callers that need to reason about fsys frame
    /// geometry (e.g. computing the byte range a record's
    /// payload occupies on disk given its `payload_start`).
    pub(crate) const fn frame_overhead() -> u64 {
        FSYS_FRAME_OVERHEAD
    }
    pub(crate) const fn pre_payload_bytes() -> u64 {
        FSYS_PRE_PAYLOAD_BYTES
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        // Best-effort: persist the meta sidecar one more time on
        // graceful drop in case it changed in flight (encryption
        // metadata changes during the lifetime of the handle).
        // Errors are swallowed because Drop cannot return them.
        let _ = self.persist_meta();
    }
}
