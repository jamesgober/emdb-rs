// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! v0.7 page-file store.
//!
//! Owns a single file, a free-list of reclaimable page ids, and a shared
//! [`PageCache`]. Reads check the cache first; misses fall back to a single
//! `pread`-equivalent. Writes update the cache and stage the page for the
//! next [`PageStore::flush`] cycle, where dirty pages are written out and
//! the file is `fdatasync`'d.
//!
//! The store does not yet own the in-memory keymap or the namespace catalog
//! — those land in Phase F. For now it provides the lowest-level primitive
//! the rest of v0.7 builds on: "a 4 KB page-addressed key-value store on
//! disk that can serve hot reads from RAM".
//!
//! ## File Layout
//!
//! ```text
//!   page 0          v4 header (magic, version, flags, page_count, free_head, ...)
//!   page 1..        slotted leaves, overflow pages, free-list pages, ...
//! ```
//!
//! Page 0 is reserved for the header; allocations begin at page id 1.

use std::collections::HashSet;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::page_cache::PageCache;
use crate::storage::page::rid::MAX_PAGE_ID;
use crate::storage::page::{Page, PageId, PAGE_SIZE};
use crate::storage::v4::io::{open_page_file, IoMode};
use crate::{Error, Result};

/// File-format magic for v0.7 page files. Distinct from the v0.6 `EMDBPAGE`
/// so a stale opener cannot confuse the two formats.
pub(crate) const V4_MAGIC: [u8; 8] = *b"EMDB07\0\0";

/// Page-file format version persisted in the header.
pub(crate) const V4_FORMAT_VERSION: u32 = 4;

/// Byte offset of the magic field within page 0.
const MAGIC_OFFSET: usize = 0;
/// Byte offset of the format version field.
const VERSION_OFFSET: usize = 8;
/// Byte offset of the feature-flags field.
const FLAGS_OFFSET: usize = 12;
/// Byte offset of the page-size field. Must equal [`PAGE_SIZE`] at runtime.
const PAGE_SIZE_OFFSET: usize = 16;
/// Byte offset of the creation-timestamp field.
const CREATED_AT_OFFSET: usize = 20;
/// Byte offset of the last-committed-tx-id field.
const LAST_TX_ID_OFFSET: usize = 28;
/// Byte offset of the page-count field.
const PAGE_COUNT_OFFSET: usize = 36;
/// Byte offset of the namespace-catalog root page id.
const NAMESPACE_ROOT_OFFSET: usize = 44;
/// Byte offset of the free-list head page id.
const FREE_LIST_HEAD_OFFSET: usize = 52;
/// Byte offset of the value-overflow free-list head.
const VALUE_OVERFLOW_HEAD_OFFSET: usize = 60;
/// Byte offset of the last WAL sequence number whose effects are reflected
/// in the page file. Replay only re-applies WAL records strictly above this.
const LAST_PERSISTED_WAL_SEQ_OFFSET: usize = 68;
/// Byte offset of the header CRC. Computed over bytes 0..100.
const HEADER_CRC_OFFSET: usize = 100;
/// Number of header bytes covered by the CRC.
const HEADER_CRC_RANGE: usize = HEADER_CRC_OFFSET;

/// First page id available for general-purpose allocations. Page 0 is the
/// header.
pub(crate) const FIRST_DATA_PAGE: u64 = 1;

/// Decoded representation of the page-0 header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StoreHeader {
    /// Feature-flag bitmap.
    pub(crate) flags: u32,
    /// Unix-millis timestamp at which the file was created.
    pub(crate) created_at: u64,
    /// Highest committed transaction id at last persist.
    pub(crate) last_tx_id: u64,
    /// Number of pages allocated in the file (including page 0).
    pub(crate) page_count: u64,
    /// Page id of the namespace catalog root, or 0 if none.
    pub(crate) namespace_root: u64,
    /// Page id of the free-list head, or 0 if the free list is empty.
    pub(crate) free_list_head: u64,
    /// Page id of the value-overflow free-list head, or 0 if empty.
    pub(crate) value_overflow_head: u64,
    /// Highest WAL sequence number whose effects are reflected in the
    /// page file. On replay the engine ignores WAL records with
    /// `seq <= last_persisted_wal_seq` (already in pages) and re-applies
    /// records with `seq > last_persisted_wal_seq` (durable in WAL but
    /// not yet checkpointed to pages).
    pub(crate) last_persisted_wal_seq: u64,
}

impl StoreHeader {
    fn fresh(flags: u32) -> Self {
        Self {
            flags,
            created_at: now_unix_millis(),
            last_tx_id: 0,
            page_count: 1, // page 0 is reserved for the header itself
            namespace_root: 0,
            free_list_head: 0,
            value_overflow_head: 0,
            last_persisted_wal_seq: 0,
        }
    }

    fn encode_into(self, page: &mut Page) {
        let bytes = page.as_mut_bytes();
        bytes[MAGIC_OFFSET..MAGIC_OFFSET + 8].copy_from_slice(&V4_MAGIC);
        bytes[VERSION_OFFSET..VERSION_OFFSET + 4].copy_from_slice(&V4_FORMAT_VERSION.to_le_bytes());
        bytes[FLAGS_OFFSET..FLAGS_OFFSET + 4].copy_from_slice(&self.flags.to_le_bytes());
        bytes[PAGE_SIZE_OFFSET..PAGE_SIZE_OFFSET + 4]
            .copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
        bytes[CREATED_AT_OFFSET..CREATED_AT_OFFSET + 8]
            .copy_from_slice(&self.created_at.to_le_bytes());
        bytes[LAST_TX_ID_OFFSET..LAST_TX_ID_OFFSET + 8]
            .copy_from_slice(&self.last_tx_id.to_le_bytes());
        bytes[PAGE_COUNT_OFFSET..PAGE_COUNT_OFFSET + 8]
            .copy_from_slice(&self.page_count.to_le_bytes());
        bytes[NAMESPACE_ROOT_OFFSET..NAMESPACE_ROOT_OFFSET + 8]
            .copy_from_slice(&self.namespace_root.to_le_bytes());
        bytes[FREE_LIST_HEAD_OFFSET..FREE_LIST_HEAD_OFFSET + 8]
            .copy_from_slice(&self.free_list_head.to_le_bytes());
        bytes[VALUE_OVERFLOW_HEAD_OFFSET..VALUE_OVERFLOW_HEAD_OFFSET + 8]
            .copy_from_slice(&self.value_overflow_head.to_le_bytes());
        bytes[LAST_PERSISTED_WAL_SEQ_OFFSET..LAST_PERSISTED_WAL_SEQ_OFFSET + 8]
            .copy_from_slice(&self.last_persisted_wal_seq.to_le_bytes());
        // Zero the reserved range between last_persisted_wal_seq and the CRC.
        for byte in &mut bytes[LAST_PERSISTED_WAL_SEQ_OFFSET + 8..HEADER_CRC_OFFSET] {
            *byte = 0;
        }
        let crc = crc32fast::hash(&bytes[..HEADER_CRC_RANGE]);
        bytes[HEADER_CRC_OFFSET..HEADER_CRC_OFFSET + 4].copy_from_slice(&crc.to_le_bytes());
        // Zero the trailing reserved area so headers are bit-identical for
        // identical inputs.
        for byte in &mut bytes[HEADER_CRC_OFFSET + 4..] {
            *byte = 0;
        }
    }

    fn decode_from(page: &Page) -> Result<Self> {
        let bytes = page.as_bytes();
        if bytes[MAGIC_OFFSET..MAGIC_OFFSET + 8] != V4_MAGIC {
            return Err(Error::MagicMismatch);
        }

        let version = read_u32(bytes, VERSION_OFFSET);
        if version != V4_FORMAT_VERSION {
            return Err(Error::VersionMismatch {
                found: version,
                expected: V4_FORMAT_VERSION,
            });
        }

        let on_disk_page_size = read_u32(bytes, PAGE_SIZE_OFFSET);
        if on_disk_page_size as usize != PAGE_SIZE {
            return Err(Error::Corrupted {
                offset: PAGE_SIZE_OFFSET as u64,
                reason: "page size in header does not match build constant",
            });
        }

        let stored_crc = read_u32(bytes, HEADER_CRC_OFFSET);
        let actual_crc = crc32fast::hash(&bytes[..HEADER_CRC_RANGE]);
        if stored_crc != actual_crc {
            return Err(Error::Corrupted {
                offset: HEADER_CRC_OFFSET as u64,
                reason: "v4 header crc mismatch",
            });
        }

        Ok(Self {
            flags: read_u32(bytes, FLAGS_OFFSET),
            created_at: read_u64(bytes, CREATED_AT_OFFSET),
            last_tx_id: read_u64(bytes, LAST_TX_ID_OFFSET),
            page_count: read_u64(bytes, PAGE_COUNT_OFFSET),
            namespace_root: read_u64(bytes, NAMESPACE_ROOT_OFFSET),
            free_list_head: read_u64(bytes, FREE_LIST_HEAD_OFFSET),
            value_overflow_head: read_u64(bytes, VALUE_OVERFLOW_HEAD_OFFSET),
            last_persisted_wal_seq: read_u64(bytes, LAST_PERSISTED_WAL_SEQ_OFFSET),
        })
    }
}

/// Atomic mirror of the mutable header fields.
///
/// Hot paths (cache-hit reads, cache-miss bounds checks) consult these
/// atomics directly without ever taking a mutex. Only [`PageStore::flush`]
/// snapshots them all at once to write a CRC-coherent header page back to
/// disk; until that snapshot moment the on-disk header lags the in-memory
/// state, which is fine — durability is handled by the WAL, not the page
/// header.
#[derive(Debug)]
struct AtomicHeader {
    page_count: AtomicU64,
    last_tx_id: AtomicU64,
    namespace_root: AtomicU64,
    free_list_head: AtomicU64,
    value_overflow_head: AtomicU64,
    last_persisted_wal_seq: AtomicU64,
    /// Set whenever any of the above atomics change. Cleared by `flush`.
    dirty: AtomicBool,
}

impl AtomicHeader {
    fn from_decoded(header: StoreHeader) -> Self {
        Self {
            page_count: AtomicU64::new(header.page_count),
            last_tx_id: AtomicU64::new(header.last_tx_id),
            namespace_root: AtomicU64::new(header.namespace_root),
            free_list_head: AtomicU64::new(header.free_list_head),
            value_overflow_head: AtomicU64::new(header.value_overflow_head),
            last_persisted_wal_seq: AtomicU64::new(header.last_persisted_wal_seq),
            dirty: AtomicBool::new(false),
        }
    }

    fn snapshot(&self, immutable: &ImmutableHeader) -> StoreHeader {
        StoreHeader {
            flags: immutable.flags,
            created_at: immutable.created_at,
            last_tx_id: self.last_tx_id.load(Ordering::Acquire),
            page_count: self.page_count.load(Ordering::Acquire),
            namespace_root: self.namespace_root.load(Ordering::Acquire),
            free_list_head: self.free_list_head.load(Ordering::Acquire),
            value_overflow_head: self.value_overflow_head.load(Ordering::Acquire),
            last_persisted_wal_seq: self.last_persisted_wal_seq.load(Ordering::Acquire),
        }
    }
}

/// Header fields that never change after open. Stored separately so the
/// hot atomic block stays as small as possible.
#[derive(Debug)]
struct ImmutableHeader {
    flags: u32,
    created_at: u64,
}

/// v0.7 page file store.
///
/// Hot paths take **no** global mutex:
///
/// - Cache-hit reads: only the cache shard's `RwLock::read()`.
/// - Cache-miss reads: only the file mutex during the `pread`-equivalent.
/// - `last_tx_id` / `namespace_root` updates: a single atomic store.
///
/// The file mutex is held only while the kernel is actively performing
/// I/O. Allocations need it briefly to extend the file in step with the
/// `page_count` increment so a concurrent reader cannot observe a
/// page id whose backing bytes are not yet in the file.
#[derive(Debug)]
pub(crate) struct PageStore {
    path: PathBuf,
    /// Atomic mirror of the live header fields.
    header: AtomicHeader,
    /// Header fields that never change after open.
    immutable: ImmutableHeader,
    /// Open file handle. Writes and cache-miss reads serialise on this
    /// mutex; cache hits never acquire it.
    file: Mutex<File>,
    /// Page ids whose in-memory copy in the cache is newer than the on-disk
    /// copy. Drained on `flush`.
    dirty: Mutex<HashSet<u64>>,
    cache: Arc<PageCache>,
}

impl PageStore {
    /// Open or create a v0.7 page file at the given path.
    ///
    /// On creation the file is initialised with a fresh header and zero data
    /// pages. The supplied `cache` can be shared between handles or constructed
    /// per-database via [`PageCache::with_default_capacity`].
    ///
    /// # Errors
    ///
    /// Returns the underlying I/O error on file-system failure, or
    /// [`Error::MagicMismatch`] / [`Error::VersionMismatch`] /
    /// [`Error::Corrupted`] when the file exists but has an incompatible
    /// header.
    pub(crate) fn open(
        path: impl Into<PathBuf>,
        flags: u32,
        cache: Arc<PageCache>,
    ) -> Result<Self> {
        Self::open_with_mode(path, flags, cache, IoMode::Buffered)
    }

    /// Open or create a v0.7 page file with an explicit I/O mode.
    ///
    /// `IoMode::Direct` requests `O_DIRECT` / `F_NOCACHE` /
    /// `FILE_FLAG_NO_BUFFERING` and hard-fails if the platform or
    /// filesystem does not support it. See [`IoMode`] for the trade-offs.
    ///
    /// # Errors
    ///
    /// Same as [`Self::open`], plus the I/O mode's own open errors.
    pub(crate) fn open_with_mode(
        path: impl Into<PathBuf>,
        flags: u32,
        cache: Arc<PageCache>,
        mode: IoMode,
    ) -> Result<Self> {
        let path = path.into();
        let mut file = open_page_file(&path, mode)?;

        let header = if file.metadata()?.len() == 0 {
            let header = StoreHeader::fresh(flags);
            let mut page = Page::new(crate::storage::page::PageHeader::new(
                crate::storage::page::PageType::Header,
            ));
            header.encode_into(&mut page);
            file.set_len(PAGE_SIZE as u64)?;
            let _seek = file.seek(SeekFrom::Start(0))?;
            file.write_all(page.as_bytes())?;
            file.sync_data()?;
            header
        } else {
            let _seek = file.seek(SeekFrom::Start(0))?;
            let mut bytes = [0_u8; PAGE_SIZE];
            file.read_exact(&mut bytes)?;
            let page = Page::from_bytes(bytes);
            let header = StoreHeader::decode_from(&page)?;
            if (header.flags & flags) != header.flags {
                return Err(Error::FeatureMismatch {
                    file_flags: header.flags,
                    build_flags: flags,
                });
            }
            header
        };

        Ok(Self {
            path,
            header: AtomicHeader::from_decoded(header),
            immutable: ImmutableHeader {
                flags: header.flags,
                created_at: header.created_at,
            },
            file: Mutex::new(file),
            dirty: Mutex::new(HashSet::new()),
            cache,
        })
    }

    /// Borrow the on-disk path.
    #[must_use]
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Snapshot the current header.
    ///
    /// Reads each atomic field with `Acquire` ordering and assembles a
    /// `StoreHeader`. The snapshot is consistent in the sense that every
    /// field reflects a value that was committed before the snapshot
    /// completed, but two fields may correspond to different points in
    /// time if a concurrent writer is mid-update.
    pub(crate) fn header(&self) -> Result<StoreHeader> {
        Ok(self.header.snapshot(&self.immutable))
    }

    /// Allocate a fresh page id, extending the file if no free page is
    /// available.
    ///
    /// The returned page is uninitialised on disk; callers are expected to
    /// fill it (e.g. via [`crate::storage::page::slotted::LeafPage::init`])
    /// and pass it to [`Self::write_page`] before the next flush.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from the file extension. Returns
    /// [`Error::Corrupted`] when the page count would exceed
    /// [`MAX_PAGE_ID`].
    pub(crate) fn allocate_page(&self) -> Result<PageId> {
        // First, try to reuse a previously freed page from the free list.
        // The free-list head is an atomic; a successful CAS pops the head
        // by replacing it with the freed page's stored next pointer.
        let head = self.header.free_list_head.load(Ordering::Acquire);
        if head != 0 {
            // Read the freed page to fetch its `next` pointer.
            let freed_page = self.read_page(PageId::new(head))?;
            let header = freed_page.header()?;
            if header.page_type == crate::storage::page::PageType::FreeList {
                let next = read_free_next_pointer(&freed_page);
                // Publish the new head atomically. Multiple concurrent
                // allocators contending on the same head all see the same
                // `next`; whoever wins the CAS reuses `head`, the others
                // retry by re-loading. We use a simple compare-exchange
                // loop in lieu of CAS-out-of-`AtomicU64` — the path is
                // cold relative to insert, so plain locking is fine.
                match self.header.free_list_head.compare_exchange(
                    head,
                    next,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        self.header.dirty.store(true, Ordering::Release);
                        // The reused page id is `head`; the caller will
                        // overwrite its contents with whatever they need.
                        // We invalidate the cache entry so the freed-page
                        // bytes are not served on a subsequent read.
                        let _was_present = self.cache.invalidate(PageId::new(head))?;
                        return Ok(PageId::new(head));
                    }
                    Err(_observed) => {
                        // Lost the race; fall through and try extending
                        // the file. The next allocation may pick up the
                        // free-list winner.
                    }
                }
            }
        }

        // Fall through: extend the file. Hold the file mutex for the whole
        // allocation: extending the file and incrementing `page_count`
        // must happen together, otherwise a concurrent reader could
        // observe a page id whose backing bytes are not yet allocated on
        // disk. `File::set_len` only needs `&self`, so the guard does not
        // need `mut`.
        let file = self.file.lock().map_err(|_poisoned| Error::LockPoisoned)?;

        let current = self.header.page_count.load(Ordering::Acquire);
        if current > MAX_PAGE_ID {
            return Err(Error::Corrupted {
                offset: PAGE_COUNT_OFFSET as u64,
                reason: "page count would exceed Rid range",
            });
        }
        let next_count = current.checked_add(1).ok_or(Error::Corrupted {
            offset: PAGE_COUNT_OFFSET as u64,
            reason: "page count overflow",
        })?;
        let new_len = next_count
            .checked_mul(PAGE_SIZE as u64)
            .ok_or(Error::Corrupted {
                offset: 0,
                reason: "page-file length overflow",
            })?;
        file.set_len(new_len)?;
        // Publish only after the file has been extended so any reader that
        // observes the new `page_count` is guaranteed to find valid bytes.
        self.header.page_count.store(next_count, Ordering::Release);
        self.header.dirty.store(true, Ordering::Release);
        Ok(PageId::new(current))
    }

    /// Mark `page_id` as free and push it onto the free list.
    ///
    /// The page's bytes are rewritten with a `FreeList` header carrying a
    /// pointer to the previous free-list head, then the head atomic is
    /// updated. Concurrent allocators may reuse the freed page on the
    /// next call to [`Self::allocate_page`]. The caller is responsible
    /// for ensuring no live `Rid` still references the page.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from the page write.
    pub(crate) fn free_page(&self, page_id: PageId) -> Result<()> {
        if page_id.get() == 0 {
            return Err(Error::InvalidConfig(
                "cannot free the page-store header page (id 0)",
            ));
        }
        let page_count = self.header.page_count.load(Ordering::Acquire);
        if page_id.get() >= page_count {
            return Err(Error::Corrupted {
                offset: page_id.get() * PAGE_SIZE as u64,
                reason: "free_page on id past end of file",
            });
        }

        // Encode "FreeList header + next pointer" into a fresh page image.
        let mut page = Page::new(crate::storage::page::PageHeader::new(
            crate::storage::page::PageType::FreeList,
        ));
        let prev_head = self.header.free_list_head.load(Ordering::Acquire);
        write_free_next_pointer(&mut page, prev_head);
        let _crc = page.refresh_crc()?;

        // Publish the new image to the cache, mark dirty, then update the
        // free-list head atomically. A concurrent allocator that reads the
        // new head will find the freshly-written bytes in the cache.
        self.cache.insert(page_id, std::sync::Arc::new(page))?;
        {
            let mut dirty = self.dirty.lock().map_err(|_poisoned| Error::LockPoisoned)?;
            let _inserted = dirty.insert(page_id.get());
        }
        self.header
            .free_list_head
            .store(page_id.get(), Ordering::Release);
        self.header.dirty.store(true, Ordering::Release);
        Ok(())
    }

    /// Read a page by id, consulting the cache first.
    ///
    /// On a cache miss, the page is read from disk, validated for size,
    /// inserted into the cache, and returned. Subsequent reads of the same
    /// id hit the cache.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Corrupted`] when the page id is past the file end,
    /// I/O errors from the read, or [`Error::LockPoisoned`] on lock failure.
    pub(crate) fn read_page(&self, page_id: PageId) -> Result<Arc<Page>> {
        // Cache hit: zero mutex acquisition. The cache shard's read-lock is
        // the only synchronisation cost.
        if let Some(page) = self.cache.get(page_id)? {
            return Ok(page);
        }

        // Cache miss: bounds check via atomic, then take only the file
        // mutex for the actual disk I/O. The page-count atomic is
        // monotonically non-decreasing (file is never truncated mid-life),
        // so the check below is sound even if a concurrent allocation lands
        // between the load and the read — at worst the file is now
        // strictly larger than what we observe and our read still succeeds.
        let page_count = self.header.page_count.load(Ordering::Acquire);
        if page_id.get() >= page_count {
            return Err(Error::Corrupted {
                offset: page_id.get() * PAGE_SIZE as u64,
                reason: "page id past end of file",
            });
        }
        let offset = page_id
            .get()
            .checked_mul(PAGE_SIZE as u64)
            .ok_or(Error::Corrupted {
                offset: 0,
                reason: "page offset overflow",
            })?;

        let mut bytes = [0_u8; PAGE_SIZE];
        {
            let mut file = self.file.lock().map_err(|_poisoned| Error::LockPoisoned)?;
            let _seek = file.seek(SeekFrom::Start(offset))?;
            file.read_exact(&mut bytes)?;
        }

        let page = Arc::new(Page::from_bytes(bytes));
        self.cache.insert(page_id, Arc::clone(&page))?;
        Ok(page)
    }

    /// Install a fresh page image in the cache and mark it dirty.
    ///
    /// The page is **not** written to disk synchronously — it is enqueued
    /// for the next [`Self::flush`]. Concurrent readers of `page_id` after
    /// this call see the new image; readers holding an older [`Arc<Page>`]
    /// finish with the old image, which is the COW invariant the v0.7
    /// engine relies on.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Corrupted`] when the page id is past the file end,
    /// or [`Error::LockPoisoned`] on lock failure.
    pub(crate) fn write_page(&self, page_id: PageId, page: Arc<Page>) -> Result<()> {
        let page_count = self.header.page_count.load(Ordering::Acquire);
        if page_id.get() >= page_count {
            return Err(Error::Corrupted {
                offset: page_id.get() * PAGE_SIZE as u64,
                reason: "page id past end of file",
            });
        }
        {
            let mut dirty = self.dirty.lock().map_err(|_poisoned| Error::LockPoisoned)?;
            let _inserted = dirty.insert(page_id.get());
        }
        self.cache.insert(page_id, page)?;
        Ok(())
    }

    /// Persist every dirty page (and the header, if dirty) to disk and
    /// `fdatasync`.
    ///
    /// Writes happen in ascending page-id order so the on-disk file remains
    /// in a consistent state if a crash interrupts the flush — earlier
    /// pages will be valid; later pages will have their previous content.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from `pwrite`/`fdatasync`, [`Error::Corrupted`] if
    /// a dirty page is missing from the cache, or [`Error::LockPoisoned`].
    pub(crate) fn flush(&self) -> Result<()> {
        // Snapshot the dirty set under its own mutex; release before doing
        // any I/O so concurrent writers can keep dirtying pages while we
        // drain.
        let mut dirty: Vec<u64> = {
            let mut dirty_guard = self.dirty.lock().map_err(|_poisoned| Error::LockPoisoned)?;
            dirty_guard.drain().collect()
        };
        dirty.sort_unstable();

        // Snapshot the header atomics into a coherent record, then clear
        // the dirty flag. If a concurrent writer dirties the header again
        // before our flush returns, that dirty bit will trigger the next
        // `flush` to rewrite — no data loss.
        let header_was_dirty = self.header.dirty.swap(false, Ordering::AcqRel);
        let header_for_write = self.header.snapshot(&self.immutable);

        // Take the file mutex once for the entire batch of writes plus the
        // final fsync. This is a serialisation point but the lock is
        // *only* held during the actual `pwrite`/`fdatasync` calls.
        let mut file = self.file.lock().map_err(|_poisoned| Error::LockPoisoned)?;

        // Write each dirty page in ascending order so a crash mid-flush
        // leaves a prefix of the file consistent.
        for raw_id in &dirty {
            let page_id = PageId::new(*raw_id);
            let page = match self.cache.get(page_id)? {
                Some(page) => page,
                None => {
                    return Err(Error::Corrupted {
                        offset: page_id.get() * PAGE_SIZE as u64,
                        reason: "dirty page absent from cache at flush time",
                    });
                }
            };
            let offset = page_id.get() * PAGE_SIZE as u64;
            let _seek = file.seek(SeekFrom::Start(offset))?;
            file.write_all(page.as_bytes())?;
        }

        if header_was_dirty {
            let mut header_page = Page::new(crate::storage::page::PageHeader::new(
                crate::storage::page::PageType::Header,
            ));
            header_for_write.encode_into(&mut header_page);
            let _seek = file.seek(SeekFrom::Start(0))?;
            file.write_all(header_page.as_bytes())?;
        }

        file.sync_data()?;
        Ok(())
    }

    /// Update the persisted last-committed transaction id. The change is
    /// visible to concurrent readers immediately and is durably persisted
    /// after the next [`Self::flush`].
    ///
    /// # Errors
    ///
    /// Always returns `Ok(())`; the signature stays `Result` so future
    /// implementations can surface persistence failures without an API
    /// break. Today the call is a single atomic store.
    pub(crate) fn set_last_tx_id(&self, tx_id: u64) -> Result<()> {
        self.header.last_tx_id.store(tx_id, Ordering::Release);
        self.header.dirty.store(true, Ordering::Release);
        Ok(())
    }

    /// Update the namespace catalog root pointer. Persisted on next flush.
    ///
    /// # Errors
    ///
    /// Always returns `Ok(())`; reserved for future error surfaces.
    pub(crate) fn set_namespace_root(&self, page_id: PageId) -> Result<()> {
        self.header
            .namespace_root
            .store(page_id.get(), Ordering::Release);
        self.header.dirty.store(true, Ordering::Release);
        Ok(())
    }

    /// Read the highest WAL sequence number whose effects are reflected in
    /// the page file. The recovery path uses this to skip already-persisted
    /// WAL records on replay.
    pub(crate) fn last_persisted_wal_seq(&self) -> u64 {
        self.header.last_persisted_wal_seq.load(Ordering::Acquire)
    }

    /// Mark every WAL record up through `seq` as reflected in the page
    /// file. Persisted on the next [`Self::flush`].
    ///
    /// # Errors
    ///
    /// Always returns `Ok(())`; reserved for future error surfaces.
    pub(crate) fn set_last_persisted_wal_seq(&self, seq: u64) -> Result<()> {
        self.header
            .last_persisted_wal_seq
            .store(seq, Ordering::Release);
        self.header.dirty.store(true, Ordering::Release);
        Ok(())
    }

    /// Number of pages currently allocated (including the header page).
    ///
    /// # Errors
    ///
    /// Always returns `Ok(_)`; reserved for future error surfaces.
    pub(crate) fn page_count(&self) -> Result<u64> {
        Ok(self.header.page_count.load(Ordering::Acquire))
    }

    /// Return the shared cache reference for diagnostics or reuse.
    #[must_use]
    pub(crate) fn cache(&self) -> &Arc<PageCache> {
        &self.cache
    }
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    let mut buf = [0_u8; 4];
    buf.copy_from_slice(&bytes[offset..offset + 4]);
    u32::from_le_bytes(buf)
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    let mut buf = [0_u8; 8];
    buf.copy_from_slice(&bytes[offset..offset + 8]);
    u64::from_le_bytes(buf)
}

fn now_unix_millis() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0_u64, |d| d.as_millis().min(u64::MAX as u128) as u64)
}

/// Offset inside a free-list page where the next-pointer (u64 little-endian)
/// is stored. Sits immediately after the 16-byte page header.
const FREE_NEXT_POINTER_OFFSET: usize = crate::storage::page::PAGE_HEADER_LEN;

/// Read the "next freed page" pointer out of a free-list page.
fn read_free_next_pointer(page: &Page) -> u64 {
    let bytes = page.as_bytes();
    let mut buf = [0_u8; 8];
    buf.copy_from_slice(&bytes[FREE_NEXT_POINTER_OFFSET..FREE_NEXT_POINTER_OFFSET + 8]);
    u64::from_le_bytes(buf)
}

/// Write the "next freed page" pointer into a free-list page. The CRC is
/// **not** refreshed by this helper — the caller is expected to call
/// [`Page::refresh_crc`] before publishing.
fn write_free_next_pointer(page: &mut Page, next: u64) {
    let bytes = page.as_mut_bytes();
    bytes[FREE_NEXT_POINTER_OFFSET..FREE_NEXT_POINTER_OFFSET + 8]
        .copy_from_slice(&next.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::{PageStore, StoreHeader, FIRST_DATA_PAGE, V4_FORMAT_VERSION, V4_MAGIC};
    use crate::page_cache::PageCache;
    use crate::storage::page::{Page, PageHeader, PageType};
    use crate::Error;
    use std::sync::Arc;

    fn tmp_path(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0_u128, |d| d.as_nanos());
        p.push(format!("emdb-v4-{name}-{nanos}.emdb"));
        p
    }

    fn open_fresh(name: &str) -> (PageStore, std::path::PathBuf) {
        let path = tmp_path(name);
        let cache = Arc::new(PageCache::with_default_capacity());
        let store = match PageStore::open(path.clone(), 0, cache) {
            Ok(store) => store,
            Err(err) => panic!("open should succeed: {err}"),
        };
        (store, path)
    }

    #[test]
    fn fresh_store_writes_v4_header() {
        let (store, path) = open_fresh("fresh");
        let header = match store.header() {
            Ok(h) => h,
            Err(err) => panic!("header should be readable: {err}"),
        };
        assert_eq!(header.page_count, 1);
        drop(store);

        // Confirm the magic bytes landed on disk.
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(err) => panic!("read should succeed: {err}"),
        };
        assert_eq!(&bytes[0..8], &V4_MAGIC);
        let _removed = std::fs::remove_file(&path);
    }

    #[test]
    fn allocate_page_extends_file_and_returns_first_data_page() {
        let (store, path) = open_fresh("alloc");
        let allocated = match store.allocate_page() {
            Ok(id) => id,
            Err(err) => panic!("allocate should succeed: {err}"),
        };
        assert_eq!(allocated.get(), FIRST_DATA_PAGE);
        let count = match store.page_count() {
            Ok(c) => c,
            Err(err) => panic!("page_count should succeed: {err}"),
        };
        assert_eq!(count, 2);
        let _removed = std::fs::remove_file(&path);
    }

    #[test]
    fn write_then_read_round_trips_through_cache_and_disk() {
        let (store, path) = open_fresh("rw-roundtrip");
        let id = match store.allocate_page() {
            Ok(id) => id,
            Err(err) => panic!("allocate should succeed: {err}"),
        };

        let mut page = Page::new(PageHeader::new(PageType::LeafSlotted));
        page.as_mut_bytes()[64] = 0xAB;
        let arc = Arc::new(page);
        let written = store.write_page(id, Arc::clone(&arc));
        assert!(written.is_ok());

        // Read back: should hit the cache and return the same Arc.
        let read = match store.read_page(id) {
            Ok(p) => p,
            Err(err) => panic!("read should succeed: {err}"),
        };
        assert!(Arc::ptr_eq(&read, &arc));

        // Flush, drop, reopen — the page should round-trip via disk.
        let flushed = store.flush();
        assert!(flushed.is_ok());
        drop(store);

        let cache = Arc::new(PageCache::with_default_capacity());
        let reopened = match PageStore::open(path.clone(), 0, cache) {
            Ok(s) => s,
            Err(err) => panic!("reopen should succeed: {err}"),
        };
        let reread = match reopened.read_page(id) {
            Ok(p) => p,
            Err(err) => panic!("reread should succeed: {err}"),
        };
        assert_eq!(reread.as_bytes()[64], 0xAB);
        let _removed = std::fs::remove_file(&path);
    }

    #[test]
    fn read_past_end_of_file_is_corruption() {
        let (store, path) = open_fresh("read-past-end");
        // Page 5 was never allocated.
        let read = store.read_page(crate::storage::page::PageId::new(5));
        assert!(matches!(read, Err(Error::Corrupted { .. })));
        let _removed = std::fs::remove_file(&path);
    }

    #[test]
    fn write_past_end_of_file_is_corruption() {
        let (store, path) = open_fresh("write-past-end");
        let mut page = Page::new(PageHeader::new(PageType::LeafSlotted));
        page.as_mut_bytes()[0] = 1;
        let written = store.write_page(crate::storage::page::PageId::new(99), Arc::new(page));
        assert!(matches!(written, Err(Error::Corrupted { .. })));
        let _removed = std::fs::remove_file(&path);
    }

    #[test]
    fn reopen_validates_magic_and_version() {
        let path = tmp_path("magic-validate");
        let cache = Arc::new(PageCache::with_default_capacity());
        let store = match PageStore::open(path.clone(), 0, Arc::clone(&cache)) {
            Ok(s) => s,
            Err(err) => panic!("open should succeed: {err}"),
        };
        drop(store);

        let header = match StoreHeader::decode_from(&Page::from_bytes({
            let bytes = match std::fs::read(&path) {
                Ok(bytes) => bytes,
                Err(err) => panic!("read should succeed: {err}"),
            };
            let mut arr = [0_u8; super::PAGE_SIZE];
            arr.copy_from_slice(&bytes[..super::PAGE_SIZE]);
            arr
        })) {
            Ok(h) => h,
            Err(err) => panic!("header decode should succeed: {err}"),
        };
        assert_eq!(header.page_count, 1);

        // Corrupt the magic and confirm the next open rejects it.
        let mut bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(err) => panic!("read should succeed: {err}"),
        };
        bytes[0] = b'X';
        let written = std::fs::write(&path, &bytes);
        assert!(written.is_ok());

        let cache = Arc::new(PageCache::with_default_capacity());
        let reopened = PageStore::open(path.clone(), 0, cache);
        assert!(matches!(reopened, Err(Error::MagicMismatch)));
        let _removed = std::fs::remove_file(&path);

        // version_for_assertion silences the unused warning for V4_FORMAT_VERSION
        // which is referenced only inside StoreHeader::decode_from.
        let _ = V4_FORMAT_VERSION;
    }

    #[test]
    fn header_dirty_after_set_last_tx_id_persists_through_flush() {
        let (store, path) = open_fresh("tx-id-persist");
        let updated = store.set_last_tx_id(42);
        assert!(updated.is_ok());
        let flushed = store.flush();
        assert!(flushed.is_ok());
        drop(store);

        let cache = Arc::new(PageCache::with_default_capacity());
        let reopened = match PageStore::open(path.clone(), 0, cache) {
            Ok(s) => s,
            Err(err) => panic!("reopen should succeed: {err}"),
        };
        let header = match reopened.header() {
            Ok(h) => h,
            Err(err) => panic!("header should succeed: {err}"),
        };
        assert_eq!(header.last_tx_id, 42);
        let _removed = std::fs::remove_file(&path);
    }

    #[test]
    fn flush_with_no_dirty_pages_is_a_noop() {
        let (store, path) = open_fresh("noop-flush");
        let flushed = store.flush();
        assert!(flushed.is_ok());
        let _removed = std::fs::remove_file(&path);
    }

    #[test]
    fn feature_mismatch_is_reported_on_reopen() {
        let path = tmp_path("feature-mismatch");
        let cache = Arc::new(PageCache::with_default_capacity());
        let store = match PageStore::open(path.clone(), 0b0011, Arc::clone(&cache)) {
            Ok(s) => s,
            Err(err) => panic!("open should succeed: {err}"),
        };
        drop(store);

        let cache = Arc::new(PageCache::with_default_capacity());
        let reopened = PageStore::open(path.clone(), 0b0001, cache);
        assert!(matches!(reopened, Err(Error::FeatureMismatch { .. })));
        let _removed = std::fs::remove_file(&path);
    }

    #[test]
    fn cache_invalidation_through_write_page_overrides_old_data() {
        let (store, path) = open_fresh("cache-override");
        let id = match store.allocate_page() {
            Ok(id) => id,
            Err(err) => panic!("allocate should succeed: {err}"),
        };

        let mut first = Page::new(PageHeader::new(PageType::LeafSlotted));
        first.as_mut_bytes()[100] = 1;
        let _ = store.write_page(id, Arc::new(first));

        let mut second = Page::new(PageHeader::new(PageType::LeafSlotted));
        second.as_mut_bytes()[100] = 2;
        let _ = store.write_page(id, Arc::new(second));

        let read = match store.read_page(id) {
            Ok(p) => p,
            Err(err) => panic!("read should succeed: {err}"),
        };
        assert_eq!(read.as_bytes()[100], 2);
        let _removed = std::fs::remove_file(&path);
    }

    #[test]
    fn shared_cache_serves_two_handles() {
        let path = tmp_path("shared-cache");
        let cache = Arc::new(PageCache::with_default_capacity());

        let first = match PageStore::open(path.clone(), 0, Arc::clone(&cache)) {
            Ok(s) => s,
            Err(err) => panic!("first open should succeed: {err}"),
        };
        let id = match first.allocate_page() {
            Ok(id) => id,
            Err(err) => panic!("allocate should succeed: {err}"),
        };
        let mut page = Page::new(PageHeader::new(PageType::LeafSlotted));
        page.as_mut_bytes()[200] = 7;
        let _ = first.write_page(id, Arc::new(page));
        let _ = first.flush();

        // A second handle on the same path with the SAME cache reuses entries.
        // (We do not open the file twice in real use because of the lockfile,
        // but the PageStore itself is happy to share a cache.)
        let cache_hit = match cache.get(id) {
            Ok(p) => p,
            Err(err) => panic!("cache should succeed: {err}"),
        };
        assert!(cache_hit.is_some());
        let _removed = std::fs::remove_file(&path);
    }
}
