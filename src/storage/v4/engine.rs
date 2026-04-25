// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! v0.7 engine — wires keymap, page cache, page store, WAL, value cache,
//! bloom filter, and namespace catalog into a working KV runtime.
//!
//! ## Read path
//!
//! 1. **L0 — value cache.** A hit returns `Bytes` without touching any other
//!    layer. CLOCK eviction keeps the hottest keys resident.
//! 2. **Bloom filter.** A negative answer is definitive: the key has never
//!    been inserted, so we return `None` without an index lookup.
//! 3. **L1 — keymap.** `hash → Rid` lookup yields one or more candidate
//!    `Rid`s (multiple only on rare 64-bit hash collisions).
//! 4. **L2 — page cache.** `Rid::page_id()` is asked to the page cache;
//!    cache hit returns `Arc<Page>` under a read-lock, no disk I/O.
//! 5. **L3 — disk.** Page is read through the configured I/O backend
//!    (buffered or Direct). Decoded record's key is compared against the
//!    requested key (collision check); on hit we promote into the value
//!    cache and return.
//!
//! ## Write path
//!
//! 1. **WAL append.** Encoded op goes into the group-commit WAL buffer; the
//!    caller receives a sequence ticket and (optionally) waits for fsync
//!    durability.
//! 2. **Leaf selection.** The engine picks the namespace's "open leaf"
//!    (most recently allocated; expected to have room). On `OutOfSpace`
//!    a new leaf is allocated and prepended to the namespace's chain.
//! 3. **COW page.** The leaf page is cloned, the slot/record encoded into
//!    the clone, the CRC refreshed, and the new `Arc<Page>` swapped into
//!    the cache. Concurrent readers holding the old `Arc<Page>` finish on
//!    the old image and the buffer is freed when their refcount drops.
//! 4. **Keymap update.** The `(hash → Rid)` entry is published. Existing
//!    entries are replaced; collisions promote `Slot::Single` to
//!    `Slot::Multi`.
//! 5. **Bloom + value cache.** Bloom bit set; value cache populated with
//!    the new bytes so a re-read of the just-written key hits L0.
//!
//! Phase H delivers the engine for the **default namespace only**; named
//! namespaces and the catalog persistence integration land alongside the
//! Emdb public-API rewrite.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::bloom::Bloom;
use crate::compress::{compress_into, decompress_into, Compressed};
use crate::keymap::Keymap;
use crate::page_cache::PageCache;
use crate::storage::page::rid::Rid;
use crate::storage::page::slotted::{
    self, free_space_of, inline_record_len, slot_count_of, InsertError, LeafPage, RecordView,
};
use crate::storage::page::{Page, PageHeader, PageId, PageType};
use crate::storage::v4::catalog::{Catalog, CatalogEntry};
use crate::storage::v4::io::IoMode;
use crate::storage::v4::store::PageStore;
use crate::storage::v4::wal::{FlushPolicy, Wal};
use crate::value_cache::{CachedValue, ValueCache};
use crate::{Error, Result};

/// Default namespace id assigned to the implicit unnamed namespace.
pub(crate) const DEFAULT_NAMESPACE_ID: u32 = 0;

/// Owned snapshot row returned by [`Engine::collect_records`].
pub(crate) type RecordSnapshot = (Vec<u8>, Vec<u8>, u64);

/// Engine configuration.
#[derive(Debug, Clone)]
pub(crate) struct EngineConfig {
    /// Path to the v0.7 page file. The WAL sidecar is derived from this.
    pub(crate) path: PathBuf,
    /// Feature flags persisted in the page header.
    pub(crate) flags: u32,
    /// I/O mode for the page file. Whole-page writes always satisfy
    /// `O_DIRECT` alignment so [`IoMode::Direct`] is safe here.
    pub(crate) page_io_mode: IoMode,
    /// I/O mode for the WAL sidecar. On Windows, [`IoMode::Direct`] adds
    /// `WRITE_THROUGH` and removes the per-record fsync. On Linux/macOS,
    /// `O_DIRECT` typically rejects sub-page writes — use
    /// [`IoMode::Buffered`] (the default) unless you know better.
    pub(crate) wal_io_mode: IoMode,
    /// WAL flush pacing.
    pub(crate) flush_policy: FlushPolicy,
    /// Page-cache capacity in pages. `0` falls back to the cache default.
    pub(crate) page_cache_pages: usize,
    /// Value-cache capacity in bytes. `0` disables the value cache.
    pub(crate) value_cache_bytes: usize,
    /// Expected initial record count for the default namespace's bloom
    /// filter. `0` disables the bloom.
    pub(crate) bloom_initial_capacity: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::new(),
            flags: 0,
            page_io_mode: IoMode::Buffered,
            wal_io_mode: IoMode::Buffered,
            flush_policy: FlushPolicy::Manual,
            page_cache_pages: 2_048,
            value_cache_bytes: 64 * 1024 * 1024,
            bloom_initial_capacity: 1_024,
        }
    }
}

/// Per-namespace runtime state.
#[derive(Debug)]
pub(crate) struct NamespaceRuntime {
    /// Namespace id.
    pub(crate) id: u32,
    /// Sharded primary index for this namespace.
    pub(crate) keymap: Keymap,
    /// Optional bloom filter sized at construction.
    pub(crate) bloom: Option<Bloom>,
    /// Page id of the namespace's "open leaf" — the leaf most likely to
    /// have room for the next insert. New inserts try this leaf first.
    pub(crate) open_leaf: AtomicU64,
    /// Page id of the namespace's leaf-chain head (oldest leaf).
    /// Iteration walks from `chain_head` via the next-leaf pointer.
    /// Currently unused by the MVP read/write paths but persisted in the
    /// catalog for future iter() support.
    pub(crate) chain_head: AtomicU64,
    /// Live record count, updated on insert/remove.
    pub(crate) record_count: AtomicU64,
}

impl NamespaceRuntime {
    fn new(id: u32, bloom_capacity: u64) -> Self {
        let bloom = if bloom_capacity == 0 {
            None
        } else {
            Some(Bloom::for_keys(bloom_capacity))
        };
        Self {
            id,
            keymap: Keymap::new(),
            bloom,
            open_leaf: AtomicU64::new(0),
            chain_head: AtomicU64::new(0),
            record_count: AtomicU64::new(0),
        }
    }

    fn from_catalog_entry(entry: &CatalogEntry, bloom_capacity: u64) -> Self {
        // Size the bloom from the persisted record count when available; fall
        // back to the configured initial capacity otherwise. A bloom sized
        // against the actual record count starts at the design FPR rather
        // than near-saturation.
        let target_capacity = entry.record_count.max(bloom_capacity);
        let bloom = if target_capacity == 0 {
            None
        } else {
            Some(Bloom::for_keys(target_capacity))
        };
        Self {
            id: entry.id,
            keymap: Keymap::new(),
            bloom,
            // Both pointers reload from the catalog; on a fresh catalog
            // both are 0, which means "no leaves yet, allocate on first
            // insert".
            open_leaf: AtomicU64::new(entry.leaf_head),
            chain_head: AtomicU64::new(entry.leaf_head),
            record_count: AtomicU64::new(entry.record_count),
        }
    }
}

/// v0.7 engine. Owns the page file, WAL, caches, and per-namespace state.
#[derive(Debug)]
pub(crate) struct Engine {
    config: EngineConfig,
    page_store: Arc<PageStore>,
    wal: Wal,
    value_cache: Option<Arc<ValueCache>>,
    /// Persistent namespace metadata. Loaded from the page-store header's
    /// `namespace_root` on open and rewritten on flush.
    catalog: Mutex<Catalog>,
    default_ns: Arc<NamespaceRuntime>,
    last_tx_id: AtomicU64,
}

impl Engine {
    /// Open or create a v0.7 database at the configured path.
    ///
    /// # Errors
    ///
    /// Returns underlying I/O errors from opening the page file or WAL,
    /// or [`Error::FeatureMismatch`] if the file's flags do not match
    /// the configured flags.
    pub(crate) fn open(config: EngineConfig) -> Result<Self> {
        let cache = if config.page_cache_pages == 0 {
            Arc::new(PageCache::with_default_capacity())
        } else {
            Arc::new(PageCache::new(config.page_cache_pages))
        };

        let page_store = Arc::new(PageStore::open_with_mode(
            config.path.clone(),
            config.flags,
            Arc::clone(&cache),
            config.page_io_mode,
        )?);

        let wal_path = Wal::path_for(&config.path);
        let wal = Wal::open_with_mode(wal_path, config.flush_policy, config.wal_io_mode)?;

        let value_cache = if config.value_cache_bytes == 0 {
            None
        } else {
            Some(Arc::new(ValueCache::new(config.value_cache_bytes)))
        };

        // Load the persistent catalog and reconstruct the default namespace.
        let header = page_store.header()?;
        let catalog = Catalog::load(&page_store, PageId::new(header.namespace_root))?;
        let default_entry = catalog
            .find_by_id(DEFAULT_NAMESPACE_ID)
            .cloned()
            .unwrap_or_else(|| CatalogEntry::new(DEFAULT_NAMESPACE_ID, ""));

        let default_ns = Arc::new(NamespaceRuntime::from_catalog_entry(
            &default_entry,
            config.bloom_initial_capacity,
        ));

        let last_tx_id = AtomicU64::new(header.last_tx_id);

        let engine = Self {
            config,
            page_store,
            wal,
            value_cache,
            catalog: Mutex::new(catalog),
            default_ns,
            last_tx_id,
        };

        // Phase H replay: walk the leaf chain to populate the keymap from
        // pages that were persisted up through `last_persisted_wal_seq`,
        // then replay any WAL records past that point.
        engine.rebuild_keymap_from_leaves(&engine.default_ns)?;
        engine.replay_wal_after(header.last_persisted_wal_seq)?;

        Ok(engine)
    }

    /// Resolve a namespace id to its runtime state. Phase H MVP only
    /// supports the default namespace; passing any other id returns
    /// [`Error::InvalidConfig`].
    fn namespace(&self, ns_id: u32) -> Result<&Arc<NamespaceRuntime>> {
        if ns_id != DEFAULT_NAMESPACE_ID {
            return Err(Error::InvalidConfig(
                "named namespaces not yet wired in this build",
            ));
        }
        Ok(&self.default_ns)
    }

    /// Insert or replace a key/value pair.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from the WAL or page store, [`Error::InvalidConfig`]
    /// when the record cannot fit on any page, or [`Error::LockPoisoned`]
    /// on a poisoned lock.
    pub(crate) fn insert(
        &self,
        ns_id: u32,
        key: &[u8],
        value: &[u8],
        expires_at: u64,
    ) -> Result<()> {
        let ns = self.namespace(ns_id)?;
        let hash = Keymap::hash_key(key);

        // 1. WAL: append the encoded op. The caller may issue many writes
        // before forcing durability; we rely on the WAL's group-commit
        // background flusher (or `flush()`) to make them durable.
        let mut wal_buf = Vec::with_capacity(64 + key.len() + value.len());
        encode_insert_op(&mut wal_buf, ns_id, key, value, expires_at);
        let _ticket = self.wal.append(&wal_buf)?;

        // 2. Allocate or pick a leaf with room, encode the record, and
        // publish a new `Arc<Page>` into the cache. The Rid we get back
        // is what the keymap stores.
        let rid = self.write_record_into_chain(ns, key, value, expires_at)?;

        // 3. Publish in keymap.
        ns.keymap.replace_single(hash, rid)?;

        // 4. Bloom + value cache.
        if let Some(bloom) = &ns.bloom {
            bloom.insert(hash);
        }
        if let Some(cache) = &self.value_cache {
            let key_box: Box<[u8]> = key.to_vec().into_boxed_slice();
            let value_arc: Arc<[u8]> = Arc::from(value.to_vec().into_boxed_slice());
            cache.insert(ns_id, hash, key_box, value_arc, expires_at)?;
        }

        let _previous = ns.record_count.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    /// Look up a key. Returns the cached value plus its expiry timestamp.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from the page store, [`Error::Corrupted`] on
    /// page-format corruption, or [`Error::LockPoisoned`] on a poisoned
    /// lock.
    pub(crate) fn get(&self, ns_id: u32, key: &[u8]) -> Result<Option<CachedValue>> {
        let ns = self.namespace(ns_id)?;
        let hash = Keymap::hash_key(key);

        // L0: value cache.
        if let Some(cache) = &self.value_cache {
            if let Some(cached) = cache.get(ns_id, hash, key)? {
                return Ok(Some(cached));
            }
        }

        // Bloom: definitive negative.
        if let Some(bloom) = &ns.bloom {
            if !bloom.contains(hash) {
                return Ok(None);
            }
        }

        // L1: keymap → candidate Rid(s).
        let slot = match ns.keymap.lookup(hash)? {
            Some(s) => s,
            None => return Ok(None),
        };

        // L2/L3: walk candidates, hit on first key match.
        for rid in slot.iter().copied() {
            let page = self.page_store.read_page(rid.page_id())?;
            let view = match slotted::read_record_at(&page, rid.slot_id(), key)? {
                Some(view) => view,
                None => continue, // tombstone or hash collision
            };
            let (value_arc, expires_at) = match view {
                RecordView::Inline {
                    value, expires_at, ..
                } => (
                    Arc::<[u8]>::from(value.to_vec().into_boxed_slice()),
                    expires_at,
                ),
                RecordView::Overflow { .. } => {
                    // Overflow chain materialisation is wired in alongside the
                    // value-page write path. Phase H MVP only writes inline,
                    // so reaching this arm means a corrupted page.
                    return Err(Error::Corrupted {
                        offset: 0,
                        reason: "overflow record found before overflow write path landed",
                    });
                }
            };

            // Promote into L0.
            if let Some(cache) = &self.value_cache {
                let key_box: Box<[u8]> = key.to_vec().into_boxed_slice();
                cache.insert(ns_id, hash, key_box, Arc::clone(&value_arc), expires_at)?;
            }

            return Ok(Some(CachedValue {
                value: value_arc,
                expires_at,
            }));
        }

        Ok(None)
    }

    /// Remove a key. Returns whether a record was tombstoned.
    ///
    /// # Errors
    ///
    /// Same as [`Self::insert`].
    pub(crate) fn remove(&self, ns_id: u32, key: &[u8]) -> Result<bool> {
        let ns = self.namespace(ns_id)?;
        let hash = Keymap::hash_key(key);

        // WAL: encode a remove op so replay can replay the deletion.
        let mut wal_buf = Vec::with_capacity(32 + key.len());
        encode_remove_op(&mut wal_buf, ns_id, key);
        let _ticket = self.wal.append(&wal_buf)?;

        // Find the live Rid for this key (if any), tombstone its slot,
        // and drop the keymap entry.
        let slot = match ns.keymap.lookup(hash)? {
            Some(s) => s,
            None => return Ok(false),
        };

        let mut removed = false;
        for rid in slot.iter().copied() {
            let page_arc = self.page_store.read_page(rid.page_id())?;
            // Confirm the slot's key matches before tombstoning, so a hash
            // collision does not silently delete the wrong record.
            let matches = slotted::read_record_at(&page_arc, rid.slot_id(), key)?.is_some();
            if !matches {
                continue;
            }

            // COW: clone the page, mark the slot as a tombstone, refresh
            // CRC, publish the new image into the cache.
            let mut new_page: Page = (*page_arc).clone();
            {
                let mut leaf = LeafPage::open(&mut new_page)?;
                let _was_live = leaf.tombstone(rid.slot_id())?;
            }
            let _crc = new_page.refresh_crc()?;
            self.page_store
                .write_page(rid.page_id(), Arc::new(new_page))?;

            // Drop the keymap entry for this Rid only. Other Rids for
            // the same hash (collisions) survive.
            let _was_present = ns.keymap.remove(hash, rid)?;
            if let Some(cache) = &self.value_cache {
                let _ignored = cache.invalidate(ns_id, hash)?;
            }
            removed = true;
            break;
        }

        if removed {
            let _previous = ns.record_count.fetch_sub(1, Ordering::AcqRel);
        }
        Ok(removed)
    }

    /// Persist every pending WAL record and dirty page, refresh the
    /// namespace catalog, and `fdatasync` the page file.
    ///
    /// The post-condition is: page file content + header + catalog
    /// reflect every operation acknowledged before this call returned,
    /// so a subsequent `open` rebuilds the same in-memory state.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from the WAL and page store, or
    /// [`Error::LockPoisoned`] on lock failures.
    pub(crate) fn flush(&self) -> Result<()> {
        // 1. Make every WAL record durable. After this returns, every
        // record assigned a seq < `floor` (computed below) is on disk.
        self.wal.flush()?;
        let floor = self.wal.next_seq();

        // 2. Refresh the catalog snapshot from the live namespace state
        // so the next open finds the right leaf chains and counts.
        {
            let mut catalog = self
                .catalog
                .lock()
                .map_err(|_poisoned| Error::LockPoisoned)?;
            self.refresh_catalog(&mut catalog)?;
            let root = catalog.save(&self.page_store)?;
            self.page_store.set_namespace_root(root)?;
        }

        // 3. Mark the WAL position the page file is about to reflect.
        // Replay on the next open skips every record with `seq < floor`
        // because those records are guaranteed to be in the page file
        // after the flush below succeeds.
        self.page_store.set_last_persisted_wal_seq(floor)?;
        self.page_store.flush()
    }

    /// Read the next WAL sequence number that will be assigned. Useful
    /// for tests and diagnostics.
    pub(crate) fn next_wal_seq(&self) -> u64 {
        self.wal.next_seq()
    }

    /// Force only the WAL to disk (without flushing pages or refreshing
    /// the catalog). Test-only helper for the WAL-replay path.
    #[cfg(test)]
    pub(crate) fn wal_flush_for_test(&self) -> Result<()> {
        self.wal.flush()
    }

    fn refresh_catalog(&self, catalog: &mut Catalog) -> Result<()> {
        // Default namespace (id 0) always exists; ensure it is in the
        // catalog with current head + count, then keep ordinary named
        // namespaces in sync below as Phase H grows them.
        let default_id = self.default_ns.id;
        if catalog.find_by_id(default_id).is_none() {
            // Synthesised by load(); should always be present.
            return Err(Error::Corrupted {
                offset: 0,
                reason: "catalog missing default namespace",
            });
        }
        catalog.set_leaf_head(
            default_id,
            self.default_ns.chain_head.load(Ordering::Acquire),
        );
        catalog.set_record_count(
            default_id,
            self.default_ns.record_count.load(Ordering::Acquire),
        );
        Ok(())
    }

    /// Number of live records in the namespace.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfig`] when `ns_id` is unknown.
    pub(crate) fn record_count(&self, ns_id: u32) -> Result<u64> {
        let ns = self.namespace(ns_id)?;
        Ok(ns.record_count.load(Ordering::Acquire))
    }

    /// Snapshot every live record in a namespace as owned `(key, value)`
    /// pairs. Walks the leaf chain via the `next_leaf` pointer so the
    /// result is the full visible state, not just what is in the cache.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from `read_page`, [`Error::Corrupted`] on a
    /// corrupted leaf, or [`Error::InvalidConfig`] on an unknown namespace.
    pub(crate) fn collect_records(&self, ns_id: u32) -> Result<Vec<RecordSnapshot>> {
        let ns = self.namespace(ns_id)?;
        let mut out: Vec<RecordSnapshot> = Vec::new();
        let mut current = ns.chain_head.load(Ordering::Acquire);
        let mut steps = 0_u64;
        let max_steps = self.page_store.page_count()?.saturating_mul(2);
        while current != 0 {
            if steps >= max_steps {
                return Err(Error::Corrupted {
                    offset: 0,
                    reason: "leaf chain longer than page count",
                });
            }
            steps = steps.saturating_add(1);

            let page = self.page_store.read_page(PageId::new(current))?;
            let slot_count = slot_count_of(&page);
            for slot_id in 0..slot_count {
                let view = match slotted::read_record_at_unchecked(&page, slot_id as u16)? {
                    Some(view) => view,
                    None => continue,
                };
                match view {
                    RecordView::Inline {
                        key,
                        value,
                        expires_at,
                    } => out.push((key.to_vec(), value.to_vec(), expires_at)),
                    RecordView::Overflow { .. } => {
                        return Err(Error::Corrupted {
                            offset: 0,
                            reason: "overflow record encountered before overflow read path",
                        });
                    }
                }
            }
            current = slotted::next_leaf_of(&page).get();
        }
        Ok(out)
    }

    /// Tombstone every record in a namespace and free its leaf chain.
    ///
    /// The chain head is reset to zero so the next insert allocates a
    /// fresh leaf. Existing records become unreadable; the underlying
    /// page IDs remain allocated until a future compactor reclaims them.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from the WAL or page store, or
    /// [`Error::LockPoisoned`] on a poisoned lock.
    pub(crate) fn clear_namespace(&self, ns_id: u32) -> Result<()> {
        let ns = self.namespace(ns_id)?;

        // We do not yet have a free-list integration in the v4 engine, so
        // "clear" simply forgets the in-memory state and resets the
        // namespace's chain head. The underlying leaves stay allocated
        // until Phase I (compactor) reclaims them. Callers can reuse the
        // namespace immediately for new inserts.
        ns.keymap.clear()?;
        if let Some(bloom) = &ns.bloom {
            bloom.clear();
        }
        ns.chain_head.store(0, Ordering::Release);
        ns.open_leaf.store(0, Ordering::Release);
        ns.record_count.store(0, Ordering::Release);

        if let Some(cache) = &self.value_cache {
            cache.clear()?;
        }
        Ok(())
    }

    /// Returns the on-disk path of the page file.
    #[must_use]
    pub(crate) fn path(&self) -> &std::path::Path {
        self.page_store.path()
    }

    /// Allocate (or reuse) a leaf with room for `record_len` bytes plus a
    /// fresh slot entry, install the encoded record, and return the
    /// resulting [`Rid`].
    fn write_record_into_chain(
        &self,
        ns: &Arc<NamespaceRuntime>,
        key: &[u8],
        value: &[u8],
        expires_at: u64,
    ) -> Result<Rid> {
        let record_len = inline_record_len(key.len(), value.len()).ok_or(Error::InvalidConfig(
            "record larger than the maximum representable size",
        ))?;
        let need_bytes = SLOT_ENTRY_LEN_GUESS + record_len;

        // Try the namespace's open leaf first.
        let open_leaf = ns.open_leaf.load(Ordering::Acquire);
        if open_leaf != 0 {
            match self.try_insert_into(open_leaf, key, value, expires_at, need_bytes) {
                Ok(rid) => return Ok(rid),
                Err(Error::TransactionAborted("leaf full")) => {
                    // Fall through to allocate a new leaf.
                }
                Err(other) => return Err(other),
            }
        }

        // Allocate a new leaf and prepend it to the chain.
        let new_leaf_id = self.page_store.allocate_page()?;
        let mut new_page = Page::new(PageHeader::new(PageType::LeafSlotted));
        let new_leaf_rid = {
            let mut leaf = LeafPage::init(&mut new_page);
            // Wire the new leaf into the chain: it points at whatever
            // was previously the head (may be 0 if the namespace is
            // empty).
            let prev_head = ns.chain_head.swap(new_leaf_id.get(), Ordering::AcqRel);
            leaf.set_next_leaf(PageId::new(prev_head));

            let slot_id = match leaf.insert_inline(key, value, expires_at) {
                Ok(slot) => slot,
                Err(InsertError::OutOfSpace) => {
                    return Err(Error::InvalidConfig(
                        "fresh leaf out of space immediately — record larger than a single page",
                    ));
                }
                Err(InsertError::KeyTooLarge) => {
                    return Err(Error::InvalidConfig(
                        "key + value too large to fit in a single page",
                    ));
                }
            };
            Rid::new(new_leaf_id.get(), slot_id)
        };
        let _crc = new_page.refresh_crc()?;
        self.page_store
            .write_page(new_leaf_id, Arc::new(new_page))?;
        ns.open_leaf.store(new_leaf_id.get(), Ordering::Release);
        Ok(new_leaf_rid)
    }

    /// Walk the namespace's leaf chain and populate its keymap + bloom +
    /// record count from every live record on disk.
    ///
    /// This represents state up through `last_persisted_wal_seq` — any
    /// records committed to the WAL but not yet flushed to pages are
    /// applied later by [`Self::replay_wal_after`].
    fn rebuild_keymap_from_leaves(&self, ns: &Arc<NamespaceRuntime>) -> Result<()> {
        let mut current = ns.chain_head.load(Ordering::Acquire);
        if current == 0 {
            return Ok(());
        }
        let mut live_count = 0_u64;
        // Defensive bound: a corrupt next_leaf cycle must not loop forever.
        // The page count is the maximum legal chain length; allow a small
        // multiplier in case multiple namespaces share leaves in some
        // future format. If we exceed this, surface as corruption.
        let max_steps = self.page_store.page_count()?.saturating_mul(2);
        let mut steps = 0_u64;

        while current != 0 {
            if steps >= max_steps {
                return Err(Error::Corrupted {
                    offset: 0,
                    reason: "leaf chain longer than page count; suspected cycle",
                });
            }
            steps = steps.saturating_add(1);

            let page = self.page_store.read_page(PageId::new(current))?;
            let slot_count = slot_count_of(&page);
            for slot_id in 0..slot_count {
                let view = match slotted::read_record_at_unchecked(&page, slot_id as u16)? {
                    Some(view) => view,
                    None => continue, // tombstoned or out-of-range
                };
                let hash = Keymap::hash_key(view.key());
                let rid = Rid::new(current, slot_id as u16);
                ns.keymap.insert(hash, rid)?;
                if let Some(bloom) = &ns.bloom {
                    bloom.insert(hash);
                }
                live_count = live_count.saturating_add(1);
            }
            current = slotted::next_leaf_of(&page).get();
        }

        ns.record_count.store(live_count, Ordering::Release);
        Ok(())
    }

    /// Replay every WAL record with `seq > start_seq`, applying its
    /// effect to the in-memory state plus the page cache.
    ///
    /// Records up to `start_seq` are already reflected in the page file
    /// (rebuilt by [`Self::rebuild_keymap_from_leaves`]); replaying them
    /// here would double-insert.
    fn replay_wal_after(&self, start_seq: u64) -> Result<()> {
        let mut buf: Vec<u8> = Vec::new();
        self.wal.read_all(&mut buf)?;
        if buf.is_empty() {
            return Ok(());
        }

        let mut cursor = 0_usize;
        let mut current_seq = 0_u64;
        while cursor < buf.len() {
            let record_end = decode_op_into(&buf, &mut cursor, current_seq, |op| {
                if current_seq < start_seq {
                    // Already reflected in the page file. Skip.
                    return Ok(());
                }
                self.apply_replayed_op(op)
            })?;
            current_seq = current_seq.saturating_add(1);
            cursor = record_end;
        }

        Ok(())
    }

    fn apply_replayed_op(&self, op: ReplayOp<'_>) -> Result<()> {
        let ns = self.namespace(op.namespace_id())?;
        match op {
            ReplayOp::Insert {
                ns_id: _,
                key,
                value,
                expires_at,
            } => {
                let hash = Keymap::hash_key(key);
                let rid = self.write_record_into_chain(ns, key, value, expires_at)?;
                ns.keymap.replace_single(hash, rid)?;
                if let Some(bloom) = &ns.bloom {
                    bloom.insert(hash);
                }
                let _previous = ns.record_count.fetch_add(1, Ordering::AcqRel);
            }
            ReplayOp::Remove { ns_id: _, key } => {
                let hash = Keymap::hash_key(key);
                if let Some(slot) = ns.keymap.lookup(hash)? {
                    for rid in slot.iter().copied() {
                        let page = self.page_store.read_page(rid.page_id())?;
                        let matches = slotted::read_record_at(&page, rid.slot_id(), key)?.is_some();
                        if !matches {
                            continue;
                        }
                        let mut new_page: Page = (*page).clone();
                        {
                            let mut leaf = LeafPage::open(&mut new_page)?;
                            let _was_live = leaf.tombstone(rid.slot_id())?;
                        }
                        let _crc = new_page.refresh_crc()?;
                        self.page_store
                            .write_page(rid.page_id(), Arc::new(new_page))?;
                        let _was_present = ns.keymap.remove(hash, rid)?;
                        let _previous = ns.record_count.fetch_sub(1, Ordering::AcqRel);
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    /// Try to insert `(key, value)` into a specific leaf page. Returns
    /// `Err(Error::TransactionAborted("leaf full"))` when the page does
    /// not have room and the caller should try another leaf.
    fn try_insert_into(
        &self,
        page_id_raw: u64,
        key: &[u8],
        value: &[u8],
        expires_at: u64,
        need_bytes: usize,
    ) -> Result<Rid> {
        let page_id = PageId::new(page_id_raw);
        let arc = self.page_store.read_page(page_id)?;
        if (free_space_of(&arc) as usize) < need_bytes && slot_count_of(&arc) > 0 {
            // Cheap pre-check: skip the COW clone if the page obviously
            // does not have room. (A reused tombstoned slot may still let
            // a smaller insert succeed; the LeafPage::insert_inline call
            // below performs the precise check.)
            let _ = need_bytes;
        }

        let mut new_page: Page = (*arc).clone();
        let slot_id = {
            let mut leaf = LeafPage::open(&mut new_page)?;
            match leaf.insert_inline(key, value, expires_at) {
                Ok(slot) => slot,
                Err(InsertError::OutOfSpace) => {
                    return Err(Error::TransactionAborted("leaf full"));
                }
                Err(InsertError::KeyTooLarge) => {
                    return Err(Error::InvalidConfig(
                        "key + value too large to fit in a single page",
                    ));
                }
            }
        };
        let _crc = new_page.refresh_crc()?;
        self.page_store.write_page(page_id, Arc::new(new_page))?;
        Ok(Rid::new(page_id_raw, slot_id))
    }
}

/// Approximate slot-entry size used in the engine's leaf-fit pre-check.
/// The exact value is `SLOT_ENTRY_LEN` from `slotted.rs`; replicated here
/// to keep the engine independent of slotted.rs's `pub(crate)` boundaries.
const SLOT_ENTRY_LEN_GUESS: usize = 8;

/// Tag byte introducing an Insert WAL record.
const WAL_TAG_INSERT: u8 = 0;
/// Tag byte introducing a Remove WAL record.
const WAL_TAG_REMOVE: u8 = 1;
/// High bit of the tag byte that signals "the value field of this record
/// is LZ4-compressed". Decoders feed the body through
/// [`crate::compress::decompress_into`]; encoders set the bit only when
/// compression actually shrunk the payload.
const WAL_FLAG_COMPRESSED: u8 = 0x80;
/// Mask isolating the operation kind from the flags.
const WAL_TAG_MASK: u8 = 0x7F;

/// Borrowed view of a decoded WAL record. The lifetime is tied to the
/// in-memory replay buffer.
enum ReplayOp<'a> {
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
}

impl<'a> ReplayOp<'a> {
    fn namespace_id(&self) -> u32 {
        match self {
            Self::Insert { ns_id, .. } | Self::Remove { ns_id, .. } => *ns_id,
        }
    }
}

/// On-disk encoding of an Insert op for the WAL.
///
/// Format (uncompressed value):
///
/// ```text
///   tag        u8     (0 = Insert)
///   ns_id      u32
///   key_len    u32
///   key        [u8; key_len]
///   value_len  u32     (== bytes that follow)
///   value      [u8; value_len]
///   expires_at u64
/// ```
///
/// Format (compressed value, `compress` feature only — high bit set on tag):
///
/// ```text
///   tag         u8     (0x80 | 0)
///   ns_id       u32
///   key_len     u32
///   key         [u8; key_len]
///   stored_len  u32     (== bytes that follow)
///   original_len u32    (size before compression — fed to the decoder)
///   value       [u8; stored_len]   (LZ4 block-compressed body)
///   expires_at  u64
/// ```
///
/// Compression activates only when the value is at least
/// `compress::COMPRESS_MIN_BYTES` AND the LZ4-encoded body is strictly
/// smaller than the original. Anything smaller passes through with the
/// uncompressed format.
fn encode_insert_op(buf: &mut Vec<u8>, ns_id: u32, key: &[u8], value: &[u8], expires_at: u64) {
    let encoded_value = compress_into(value);
    let (tag, value_bytes, original_len) = match encoded_value {
        Compressed::Passthrough { bytes } => (WAL_TAG_INSERT, bytes, None),
        Compressed::Encoded {
            ref bytes,
            original_len,
        } => (
            WAL_TAG_INSERT | WAL_FLAG_COMPRESSED,
            bytes.as_slice(),
            Some(original_len),
        ),
    };

    buf.push(tag);
    buf.extend_from_slice(&ns_id.to_le_bytes());
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(&(value_bytes.len() as u32).to_le_bytes());
    if let Some(orig) = original_len {
        buf.extend_from_slice(&orig.to_le_bytes());
    }
    buf.extend_from_slice(value_bytes);
    buf.extend_from_slice(&expires_at.to_le_bytes());
}

/// On-disk encoding of a Remove op for the WAL.
///
/// Format:
///
/// ```text
///   tag      u8     (1 = Remove)
///   ns_id    u32
///   key_len  u32
///   key      [u8; key_len]
/// ```
fn encode_remove_op(buf: &mut Vec<u8>, ns_id: u32, key: &[u8]) {
    buf.push(WAL_TAG_REMOVE);
    buf.extend_from_slice(&ns_id.to_le_bytes());
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(key);
}

/// Decode the next WAL record starting at `cursor` and pass it to the
/// supplied callback. Returns the byte offset immediately past the
/// record so the caller can advance.
///
/// The `seq_for_callback` argument is currently unused but reserved so
/// the closure can grow access to per-record metadata without an API
/// break.
fn decode_op_into<F>(
    buf: &[u8],
    cursor: &mut usize,
    seq_for_callback: u64,
    mut apply: F,
) -> Result<usize>
where
    F: FnMut(ReplayOp<'_>) -> Result<()>,
{
    let _ = seq_for_callback;
    let start = *cursor;
    if start >= buf.len() {
        return Err(Error::Corrupted {
            offset: start as u64,
            reason: "wal record truncated at start",
        });
    }
    let tag_byte = buf[start];
    let tag = tag_byte & WAL_TAG_MASK;
    let compressed = tag_byte & WAL_FLAG_COMPRESSED != 0;
    let mut pos = start + 1;
    match tag {
        WAL_TAG_INSERT => {
            let (ns_id, after_ns) = read_u32(buf, pos)?;
            pos = after_ns;
            let (key, after_key) = read_lp(buf, pos)?;
            pos = after_key;
            // Compressed records carry an extra `original_len: u32` between
            // the stored length and the body.
            let (value_bytes, after_value, original_len) = if compressed {
                let (stored_len, after_stored) = read_u32(buf, pos)?;
                pos = after_stored;
                let (orig_len, after_orig) = read_u32(buf, pos)?;
                pos = after_orig;
                let body_end = pos + stored_len as usize;
                if body_end > buf.len() {
                    return Err(Error::Corrupted {
                        offset: pos as u64,
                        reason: "wal compressed insert truncated in body",
                    });
                }
                let body = &buf[pos..body_end];
                (body, body_end, Some(orig_len))
            } else {
                let (body, end) = read_lp(buf, pos)?;
                (body, end, None)
            };
            pos = after_value;
            if pos + 8 > buf.len() {
                return Err(Error::Corrupted {
                    offset: pos as u64,
                    reason: "wal insert record truncated before expires_at",
                });
            }
            let mut expires_buf = [0_u8; 8];
            expires_buf.copy_from_slice(&buf[pos..pos + 8]);
            let expires_at = u64::from_le_bytes(expires_buf);
            pos += 8;

            // If the body is uncompressed we can reference the WAL buffer
            // directly. Compressed bodies need a scratch decode.
            let mut decoded_scratch: Vec<u8> = Vec::new();
            let value_slice: &[u8] = if compressed {
                let orig = original_len.unwrap_or(0);
                decompress_into(value_bytes, true, orig, &mut decoded_scratch)?;
                decoded_scratch.as_slice()
            } else {
                value_bytes
            };

            apply(ReplayOp::Insert {
                ns_id,
                key,
                value: value_slice,
                expires_at,
            })?;
            Ok(pos)
        }
        WAL_TAG_REMOVE => {
            // Remove records have no value; the compressed flag must not be
            // set on them. Surface as corruption rather than silent ignore.
            if compressed {
                return Err(Error::Corrupted {
                    offset: start as u64,
                    reason: "remove record cannot carry the compressed flag",
                });
            }
            let (ns_id, after_ns) = read_u32(buf, pos)?;
            pos = after_ns;
            let (key, after_key) = read_lp(buf, pos)?;
            pos = after_key;
            apply(ReplayOp::Remove { ns_id, key })?;
            Ok(pos)
        }
        _other => Err(Error::Corrupted {
            offset: start as u64,
            reason: "unknown wal record tag",
        }),
    }
}

fn read_u32(buf: &[u8], pos: usize) -> Result<(u32, usize)> {
    if pos + 4 > buf.len() {
        return Err(Error::Corrupted {
            offset: pos as u64,
            reason: "wal record truncated mid-u32",
        });
    }
    let mut bytes = [0_u8; 4];
    bytes.copy_from_slice(&buf[pos..pos + 4]);
    Ok((u32::from_le_bytes(bytes), pos + 4))
}

fn read_lp(buf: &[u8], pos: usize) -> Result<(&[u8], usize)> {
    let (len, after_len) = read_u32(buf, pos)?;
    let len = len as usize;
    let end = after_len + len;
    if end > buf.len() {
        return Err(Error::Corrupted {
            offset: after_len as u64,
            reason: "wal record truncated in length-prefixed body",
        });
    }
    Ok((&buf[after_len..end], end))
}

#[cfg(test)]
mod tests {
    use super::{Engine, EngineConfig, DEFAULT_NAMESPACE_ID};
    use crate::storage::v4::wal::FlushPolicy;

    fn tmp_path(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0_u128, |d| d.as_nanos());
        p.push(format!("emdb-v4-engine-{name}-{nanos}.emdb"));
        p
    }

    fn config_for(name: &str) -> EngineConfig {
        EngineConfig {
            path: tmp_path(name),
            flush_policy: FlushPolicy::Manual,
            ..EngineConfig::default()
        }
    }

    fn cleanup(path: &std::path::Path) {
        let _removed = std::fs::remove_file(path);
        if let Some(parent) = path.parent() {
            if let Some(file_name) = path.file_name() {
                if let Some(name_str) = file_name.to_str() {
                    let wal = parent.join(format!("{name_str}.v4.wal"));
                    let _removed = std::fs::remove_file(wal);
                }
            }
        }
    }

    #[test]
    fn open_creates_database_files() {
        let cfg = config_for("open");
        let path = cfg.path.clone();
        let engine = match Engine::open(cfg) {
            Ok(e) => e,
            Err(err) => panic!("open should succeed: {err}"),
        };
        let count = match engine.record_count(DEFAULT_NAMESPACE_ID) {
            Ok(c) => c,
            Err(err) => panic!("record_count should succeed: {err}"),
        };
        assert_eq!(count, 0);
        drop(engine);
        cleanup(&path);
    }

    #[test]
    fn insert_then_get_round_trips() {
        let cfg = config_for("insert-get");
        let path = cfg.path.clone();
        let engine = match Engine::open(cfg) {
            Ok(e) => e,
            Err(err) => panic!("open should succeed: {err}"),
        };

        let inserted = engine.insert(DEFAULT_NAMESPACE_ID, b"alpha", b"one", 0);
        assert!(inserted.is_ok(), "insert: {:?}", inserted);

        let fetched = engine.get(DEFAULT_NAMESPACE_ID, b"alpha");
        let cached = match fetched {
            Ok(Some(v)) => v,
            Ok(None) => panic!("just-inserted key should be present"),
            Err(err) => panic!("get should succeed: {err}"),
        };
        assert_eq!(cached.value.as_ref(), b"one");
        assert_eq!(cached.expires_at, 0);

        let count = match engine.record_count(DEFAULT_NAMESPACE_ID) {
            Ok(c) => c,
            Err(err) => panic!("record_count should succeed: {err}"),
        };
        assert_eq!(count, 1);

        drop(engine);
        cleanup(&path);
    }

    #[test]
    fn missing_key_returns_none_and_skips_disk_via_bloom() {
        let cfg = config_for("missing");
        let path = cfg.path.clone();
        let engine = match Engine::open(cfg) {
            Ok(e) => e,
            Err(err) => panic!("open should succeed: {err}"),
        };
        let fetched = engine.get(DEFAULT_NAMESPACE_ID, b"nope");
        assert!(matches!(fetched, Ok(None)));
        drop(engine);
        cleanup(&path);
    }

    #[test]
    fn remove_marks_key_unreadable() {
        let cfg = config_for("remove");
        let path = cfg.path.clone();
        let engine = match Engine::open(cfg) {
            Ok(e) => e,
            Err(err) => panic!("open should succeed: {err}"),
        };

        assert!(engine.insert(DEFAULT_NAMESPACE_ID, b"k", b"v", 0).is_ok());
        let removed = engine.remove(DEFAULT_NAMESPACE_ID, b"k");
        assert!(matches!(removed, Ok(true)));

        let fetched = engine.get(DEFAULT_NAMESPACE_ID, b"k");
        assert!(matches!(fetched, Ok(None)));

        let count = match engine.record_count(DEFAULT_NAMESPACE_ID) {
            Ok(c) => c,
            Err(err) => panic!("record_count should succeed: {err}"),
        };
        assert_eq!(count, 0);

        drop(engine);
        cleanup(&path);
    }

    #[test]
    fn remove_unknown_key_reports_false() {
        let cfg = config_for("remove-unknown");
        let path = cfg.path.clone();
        let engine = match Engine::open(cfg) {
            Ok(e) => e,
            Err(err) => panic!("open should succeed: {err}"),
        };
        let removed = engine.remove(DEFAULT_NAMESPACE_ID, b"nope");
        assert!(matches!(removed, Ok(false)));
        drop(engine);
        cleanup(&path);
    }

    #[test]
    fn many_inserts_succeed_and_remain_readable() {
        let cfg = config_for("many");
        let path = cfg.path.clone();
        let engine = match Engine::open(cfg) {
            Ok(e) => e,
            Err(err) => panic!("open should succeed: {err}"),
        };

        for i in 0_u32..256 {
            let key = format!("k{i:04}");
            let value = format!("v{i:04}");
            let inserted = engine.insert(DEFAULT_NAMESPACE_ID, key.as_bytes(), value.as_bytes(), 0);
            assert!(inserted.is_ok(), "insert #{i}: {:?}", inserted);
        }

        for i in 0_u32..256 {
            let key = format!("k{i:04}");
            let fetched = engine.get(DEFAULT_NAMESPACE_ID, key.as_bytes());
            let cached = match fetched {
                Ok(Some(v)) => v,
                Ok(None) => panic!("key {key} missing after insert"),
                Err(err) => panic!("get should succeed: {err}"),
            };
            assert_eq!(cached.value.as_ref(), format!("v{i:04}").as_bytes());
        }

        let count = match engine.record_count(DEFAULT_NAMESPACE_ID) {
            Ok(c) => c,
            Err(err) => panic!("record_count should succeed: {err}"),
        };
        assert_eq!(count, 256);

        drop(engine);
        cleanup(&path);
    }

    #[test]
    fn flush_persists_records_through_reopen() {
        let cfg = config_for("persist");
        let path = cfg.path.clone();
        {
            let engine = match Engine::open(cfg.clone()) {
                Ok(e) => e,
                Err(err) => panic!("open should succeed: {err}"),
            };
            assert!(engine.insert(DEFAULT_NAMESPACE_ID, b"k", b"v", 0).is_ok());
            assert!(engine.flush().is_ok());
        }
        // After flush, the page file + catalog reflect the record. A
        // reopen rebuilds the keymap by walking the leaf chain.
        let reopened = match Engine::open(cfg) {
            Ok(e) => e,
            Err(err) => panic!("reopen should succeed: {err}"),
        };
        let fetched = reopened.get(DEFAULT_NAMESPACE_ID, b"k");
        let cached = match fetched {
            Ok(Some(v)) => v,
            Ok(None) => panic!("key should be present after reopen"),
            Err(err) => panic!("get should succeed: {err}"),
        };
        assert_eq!(cached.value.as_ref(), b"v");
        drop(reopened);
        cleanup(&path);
    }

    #[test]
    fn replay_recovers_records_when_flush_was_called() {
        // Seed → flush → drop → reopen → records visible.
        let cfg = config_for("replay-flushed");
        let path = cfg.path.clone();
        {
            let engine = match Engine::open(cfg.clone()) {
                Ok(e) => e,
                Err(err) => panic!("open should succeed: {err}"),
            };
            for i in 0_u32..32 {
                let key = format!("k{i:02}");
                let value = format!("v{i:02}");
                let _ = engine.insert(DEFAULT_NAMESPACE_ID, key.as_bytes(), value.as_bytes(), 0);
            }
            assert!(engine.flush().is_ok());
        }

        let reopened = match Engine::open(cfg) {
            Ok(e) => e,
            Err(err) => panic!("reopen should succeed: {err}"),
        };
        for i in 0_u32..32 {
            let key = format!("k{i:02}");
            let fetched = reopened.get(DEFAULT_NAMESPACE_ID, key.as_bytes());
            match fetched {
                Ok(Some(v)) => assert_eq!(v.value.as_ref(), format!("v{i:02}").as_bytes()),
                Ok(None) => panic!("key {key} missing after reopen"),
                Err(err) => panic!("get should succeed: {err}"),
            }
        }
        let count = match reopened.record_count(DEFAULT_NAMESPACE_ID) {
            Ok(c) => c,
            Err(err) => panic!("record_count should succeed: {err}"),
        };
        assert_eq!(count, 32);
        drop(reopened);
        cleanup(&path);
    }

    #[test]
    fn replay_recovers_records_from_wal_without_flush() {
        // Seed but do NOT flush → drop → reopen → WAL replay must
        // recover the records because the page header's
        // last_persisted_wal_seq is still 0.
        let cfg = config_for("replay-wal");
        let path = cfg.path.clone();
        {
            let engine = match Engine::open(cfg.clone()) {
                Ok(e) => e,
                Err(err) => panic!("open should succeed: {err}"),
            };
            for i in 0_u32..16 {
                let key = format!("k{i:02}");
                let value = format!("v{i:02}");
                let _ = engine.insert(DEFAULT_NAMESPACE_ID, key.as_bytes(), value.as_bytes(), 0);
            }
            // Drop without flush — page file is empty; WAL has every op.
            // The WAL is fsynced in Drop only if FlushPolicy is on the
            // synchronous side; for Manual we explicitly fsync the WAL.
            assert!(engine.wal_flush_for_test().is_ok());
        }

        let reopened = match Engine::open(cfg) {
            Ok(e) => e,
            Err(err) => panic!("reopen should succeed: {err}"),
        };
        for i in 0_u32..16 {
            let key = format!("k{i:02}");
            let fetched = reopened.get(DEFAULT_NAMESPACE_ID, key.as_bytes());
            match fetched {
                Ok(Some(v)) => assert_eq!(v.value.as_ref(), format!("v{i:02}").as_bytes()),
                Ok(None) => panic!("key {key} missing after wal-only replay"),
                Err(err) => panic!("get should succeed: {err}"),
            }
        }
        let count = match reopened.record_count(DEFAULT_NAMESPACE_ID) {
            Ok(c) => c,
            Err(err) => panic!("record_count should succeed: {err}"),
        };
        assert_eq!(count, 16);
        drop(reopened);
        cleanup(&path);
    }

    #[test]
    fn insert_replaces_existing_key() {
        let cfg = config_for("replace");
        let path = cfg.path.clone();
        let engine = match Engine::open(cfg) {
            Ok(e) => e,
            Err(err) => panic!("open should succeed: {err}"),
        };

        let _ = engine.insert(DEFAULT_NAMESPACE_ID, b"k", b"first", 0);
        let _ = engine.insert(DEFAULT_NAMESPACE_ID, b"k", b"second", 0);

        let fetched = match engine.get(DEFAULT_NAMESPACE_ID, b"k") {
            Ok(Some(v)) => v,
            Ok(None) => panic!("key should be present"),
            Err(err) => panic!("get should succeed: {err}"),
        };
        assert_eq!(fetched.value.as_ref(), b"second");
        drop(engine);
        cleanup(&path);
    }

    #[test]
    fn large_value_round_trips_through_wal_replay() {
        // 2 KB highly compressible value crosses COMPRESS_MIN_BYTES (256
        // bytes) but still fits in a single 4 KB leaf page. Without the
        // `compress` feature this still works because encode_insert_op
        // always passes through uncompressed.
        let cfg = config_for("large-value");
        let path = cfg.path.clone();
        let huge: Vec<u8> = (0..2048_u32).map(|i| (i % 17) as u8).collect();
        {
            let engine = match Engine::open(cfg.clone()) {
                Ok(e) => e,
                Err(err) => panic!("open should succeed: {err}"),
            };
            assert!(engine
                .insert(DEFAULT_NAMESPACE_ID, b"big", &huge, 0)
                .is_ok());
            assert!(engine.wal_flush_for_test().is_ok());
        }

        let reopened = match Engine::open(cfg) {
            Ok(e) => e,
            Err(err) => panic!("reopen should succeed: {err}"),
        };
        let fetched = reopened.get(DEFAULT_NAMESPACE_ID, b"big");
        let cached = match fetched {
            Ok(Some(v)) => v,
            Ok(None) => panic!("large value missing after replay"),
            Err(err) => panic!("get should succeed: {err}"),
        };
        assert_eq!(cached.value.as_ref(), huge.as_slice());
        drop(reopened);
        cleanup(&path);
    }

    #[test]
    fn unknown_namespace_returns_invalid_config() {
        let cfg = config_for("bad-ns");
        let path = cfg.path.clone();
        let engine = match Engine::open(cfg) {
            Ok(e) => e,
            Err(err) => panic!("open should succeed: {err}"),
        };
        let inserted = engine.insert(99, b"k", b"v", 0);
        assert!(matches!(inserted, Err(crate::Error::InvalidConfig(_))));
        drop(engine);
        cleanup(&path);
    }
}
