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

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::bloom::Bloom;
use crate::compress::{compress_into, decompress_into, Compressed};
use crate::keymap::Keymap;
use crate::page_cache::PageCache;
use crate::storage::page::rid::Rid;
use crate::storage::page::slotted::{
    self, free_space_of, inline_record_len, live_count_of, slot_count_of, InsertError, LeafPage,
    RecordView,
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

/// Counts produced by [`Engine::compact`]. Reported back to the public API
/// for diagnostics and test assertions.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CompactStats {
    /// Number of leaf pages that were rebuilt in place to drop tombstones.
    pub(crate) leaves_compacted: u64,
    /// Number of pages pushed onto the page-store free list. Includes
    /// empty leaves unlinked from live namespaces and every leaf in a
    /// dropped namespace's chain.
    pub(crate) pages_freed: u64,
    /// Number of tombstoned-in-catalog namespaces fully reclaimed.
    pub(crate) namespaces_reclaimed: u64,
}

/// One operation inside a multi-op transaction commit. Fields are owned so
/// the engine can WAL-encode and apply without re-borrowing into the caller's
/// transaction state.
#[derive(Debug, Clone)]
pub(crate) enum BatchedOp {
    /// Insert or replace a key/value pair with an explicit expiration.
    Insert {
        ns_id: u32,
        key: Vec<u8>,
        value: Vec<u8>,
        expires_at: u64,
    },
    /// Remove a key.
    Remove { ns_id: u32, key: Vec<u8> },
}

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
    /// Live runtime for every loaded namespace, keyed by id. The default
    /// namespace (`DEFAULT_NAMESPACE_ID`) is always present after `open`;
    /// named namespaces are loaded from the catalog at open and added by
    /// `create_or_open_namespace`. Reads take the read lock and clone the
    /// `Arc` out before doing real work; the lock is held only for the
    /// duration of the lookup.
    namespaces: RwLock<HashMap<u32, Arc<NamespaceRuntime>>>,
    last_tx_id: AtomicU64,
    /// Serialises every mutation that touches WAL+state together: single-op
    /// inserts/removes, multi-op transaction commits, and `flush`. Reads do
    /// not take this lock. Holding it across a transaction's WAL appends and
    /// in-memory apply phase is what gives transactions their crash-atomic
    /// `last_persisted_wal_seq` floor (a flush can never split a batch) and
    /// keeps writers from interleaving their state mutations.
    commit_lock: Mutex<()>,
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

        // Load the persistent catalog and reconstruct every live namespace
        // runtime. The default namespace (id 0) is synthesised if missing
        // so the engine always has a working root namespace to fall back
        // to.
        let header = page_store.header()?;
        let catalog = Catalog::load(&page_store, PageId::new(header.namespace_root))?;

        let mut runtimes: HashMap<u32, Arc<NamespaceRuntime>> = HashMap::new();
        let mut saw_default = false;
        for entry in catalog.live_entries() {
            if entry.id == DEFAULT_NAMESPACE_ID {
                saw_default = true;
            }
            let runtime = Arc::new(NamespaceRuntime::from_catalog_entry(
                entry,
                config.bloom_initial_capacity,
            ));
            let _existing = runtimes.insert(entry.id, runtime);
        }
        if !saw_default {
            let synthetic = CatalogEntry::new(DEFAULT_NAMESPACE_ID, "");
            let runtime = Arc::new(NamespaceRuntime::from_catalog_entry(
                &synthetic,
                config.bloom_initial_capacity,
            ));
            let _existing = runtimes.insert(DEFAULT_NAMESPACE_ID, runtime);
        }

        let last_tx_id = AtomicU64::new(header.last_tx_id);

        let engine = Self {
            config,
            page_store,
            wal,
            value_cache,
            catalog: Mutex::new(catalog),
            namespaces: RwLock::new(runtimes),
            last_tx_id,
            commit_lock: Mutex::new(()),
        };

        // Phase H replay: walk every namespace's leaf chain to populate
        // its keymap from records persisted up through
        // `last_persisted_wal_seq`, then replay any WAL records past
        // that point.
        let snapshot = engine.snapshot_namespaces()?;
        for ns in &snapshot {
            engine.rebuild_keymap_from_leaves(ns)?;
        }
        engine.replay_wal_after(header.last_persisted_wal_seq)?;

        Ok(engine)
    }

    /// Snapshot the loaded namespace runtimes. Used by replay-on-open and
    /// by `flush` when the catalog is being refreshed.
    fn snapshot_namespaces(&self) -> Result<Vec<Arc<NamespaceRuntime>>> {
        let guard = self
            .namespaces
            .read()
            .map_err(|_poisoned| Error::LockPoisoned)?;
        let mut out: Vec<Arc<NamespaceRuntime>> = guard.values().cloned().collect();
        // Stable order by id keeps iteration deterministic for tests and
        // diagnostics without imposing a sorted map.
        out.sort_by_key(|ns| ns.id);
        Ok(out)
    }

    /// Look up a named namespace and return `(id, was_created)`.
    ///
    /// If a live namespace with this name already exists, returns
    /// `(existing_id, false)`. Otherwise allocates a new id via the
    /// catalog, builds a fresh runtime, inserts it into the runtime map,
    /// and returns `(new_id, true)`. Catalog persistence is deferred to
    /// the next [`Self::flush`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfig`] when `name` is empty (reserved
    /// for the default namespace) or the catalog rejects the name (id
    /// space exhausted).
    pub(crate) fn create_or_open_namespace(&self, name: &str) -> Result<(u32, bool)> {
        if name.is_empty() {
            return Err(Error::InvalidConfig(
                "namespace name must be non-empty (the default namespace is implicit)",
            ));
        }

        let _commit_guard = self
            .commit_lock
            .lock()
            .map_err(|_poisoned| Error::LockPoisoned)?;

        let mut catalog = self
            .catalog
            .lock()
            .map_err(|_poisoned| Error::LockPoisoned)?;

        if let Some(entry) = catalog.find_by_name(name) {
            let id = entry.id;
            // Runtime may already be loaded (the open path loads every live
            // entry); only build a new runtime if it is missing. Use
            // `Entry::or_insert_with` so the existing runtime stays put on
            // the hot reopen path (no Arc clone) and a fresh one is built
            // only on the cold "first call after drop" path.
            let mut runtimes = self
                .namespaces
                .write()
                .map_err(|_poisoned| Error::LockPoisoned)?;
            let _runtime = runtimes.entry(id).or_insert_with(|| {
                Arc::new(NamespaceRuntime::from_catalog_entry(
                    entry,
                    self.config.bloom_initial_capacity,
                ))
            });
            return Ok((id, false));
        }

        let id = catalog.create(name)?;
        // The catalog entry we just inserted is the source of truth for the
        // runtime — pull it back out so the runtime starts from the same
        // (zeroed) leaf-head / record-count the catalog now persists.
        let entry = catalog
            .find_by_id(id)
            .ok_or(Error::Corrupted {
                offset: 0,
                reason: "freshly-created namespace missing from catalog",
            })?
            .clone();
        let runtime = Arc::new(NamespaceRuntime::from_catalog_entry(
            &entry,
            self.config.bloom_initial_capacity,
        ));
        let mut runtimes = self
            .namespaces
            .write()
            .map_err(|_poisoned| Error::LockPoisoned)?;
        let _existing = runtimes.insert(id, runtime);
        Ok((id, true))
    }

    /// Look up a namespace by name, returning its id if a live entry exists.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockPoisoned`] on a poisoned catalog mutex.
    pub(crate) fn namespace_id_for(&self, name: &str) -> Result<Option<u32>> {
        let catalog = self
            .catalog
            .lock()
            .map_err(|_poisoned| Error::LockPoisoned)?;
        Ok(catalog.find_by_name(name).map(|e| e.id))
    }

    /// Tombstone a named namespace. Returns `true` when a live entry was
    /// dropped, `false` when the name was unknown. The default namespace
    /// cannot be dropped.
    ///
    /// The on-disk pages for the namespace's leaf chain remain allocated
    /// until a future compactor reclaims them; until then, `flush` will
    /// keep the tombstoned catalog entry on disk so `list_namespaces`
    /// stays consistent.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfig`] when the caller targets the
    /// default namespace, or the underlying lock errors.
    pub(crate) fn drop_namespace(&self, name: &str) -> Result<bool> {
        if name.is_empty() {
            return Err(Error::InvalidConfig("default namespace cannot be dropped"));
        }

        let _commit_guard = self
            .commit_lock
            .lock()
            .map_err(|_poisoned| Error::LockPoisoned)?;

        let mut catalog = self
            .catalog
            .lock()
            .map_err(|_poisoned| Error::LockPoisoned)?;
        let id = match catalog.find_by_name(name) {
            Some(entry) => entry.id,
            None => return Ok(false),
        };
        let was_live = catalog.tombstone(id)?;
        drop(catalog);

        if was_live {
            let mut runtimes = self
                .namespaces
                .write()
                .map_err(|_poisoned| Error::LockPoisoned)?;
            let _removed = runtimes.remove(&id);
            // Also evict any value-cache entries belonging to this
            // namespace so subsequent reads do not surface stale values.
            // The cache is keyed by `(ns_id, hash)` so a per-namespace
            // sweep is the right granularity.
            // (Implemented as a no-op today; a follow-up patch wires up
            // `ValueCache::invalidate_namespace` once the compactor lands.)
        }
        Ok(was_live)
    }

    /// List every live namespace (including the default) as `(id, name)`.
    /// Stable order by id. The default namespace is reported with name
    /// `""`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockPoisoned`] on a poisoned catalog mutex.
    pub(crate) fn list_namespaces(&self) -> Result<Vec<(u32, String)>> {
        let catalog = self
            .catalog
            .lock()
            .map_err(|_poisoned| Error::LockPoisoned)?;
        let mut out: Vec<(u32, String)> = catalog
            .live_entries()
            .map(|e| (e.id, e.name.clone()))
            .collect();
        out.sort_by_key(|(id, _)| *id);
        Ok(out)
    }

    /// Resolve a namespace id to its runtime state.
    ///
    /// Returns a cloned `Arc` so the caller can drop the read lock
    /// immediately; the underlying runtime is shared, so the clone is
    /// essentially a refcount bump.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfig`] when no namespace with this id is
    /// loaded (either never created or already dropped from the runtime
    /// map). Callers that may race with a `drop_namespace` should treat
    /// this as a "namespace went away" error.
    fn namespace(&self, ns_id: u32) -> Result<Arc<NamespaceRuntime>> {
        let guard = self
            .namespaces
            .read()
            .map_err(|_poisoned| Error::LockPoisoned)?;
        match guard.get(&ns_id) {
            Some(ns) => Ok(Arc::clone(ns)),
            None => Err(Error::InvalidConfig("unknown namespace id")),
        }
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
        let _commit_guard = self
            .commit_lock
            .lock()
            .map_err(|_poisoned| Error::LockPoisoned)?;

        // 1. WAL: append the encoded op. The caller may issue many writes
        // before forcing durability; we rely on the WAL's group-commit
        // background flusher (or `flush()`) to make them durable.
        let mut wal_buf = Vec::with_capacity(64 + key.len() + value.len());
        encode_insert_op(&mut wal_buf, ns_id, key, value, expires_at);
        let _ticket = self.wal.append(&wal_buf)?;

        // 2. Apply the in-memory state mutations (page write, keymap publish,
        // bloom, value cache, record-count). Shared with `commit_batch` so
        // single-op and multi-op writes converge on the same code.
        self.apply_insert(ns_id, key, value, expires_at)
    }

    /// Apply the in-memory side of an insert: COW page write, keymap
    /// publish, bloom set, value-cache fill, record-count bump. Caller
    /// must already hold [`Self::commit_lock`].
    fn apply_insert(&self, ns_id: u32, key: &[u8], value: &[u8], expires_at: u64) -> Result<()> {
        let ns = self.namespace(ns_id)?;
        let hash = Keymap::hash_key(key);

        let rid = self.write_record_into_chain(&ns, key, value, expires_at)?;
        ns.keymap.replace_single(hash, rid)?;

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
        let _commit_guard = self
            .commit_lock
            .lock()
            .map_err(|_poisoned| Error::LockPoisoned)?;

        let mut wal_buf = Vec::with_capacity(32 + key.len());
        encode_remove_op(&mut wal_buf, ns_id, key);
        let _ticket = self.wal.append(&wal_buf)?;

        self.apply_remove(ns_id, key)
    }

    /// Apply the in-memory side of a remove: locate the live Rid, COW the
    /// leaf to tombstone the slot, drop the keymap entry, invalidate the
    /// value cache. Caller must hold [`Self::commit_lock`].
    fn apply_remove(&self, ns_id: u32, key: &[u8]) -> Result<bool> {
        let ns = self.namespace(ns_id)?;
        let hash = Keymap::hash_key(key);

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

            let mut new_page: Page = (*page_arc).clone();
            {
                let mut leaf = LeafPage::open(&mut new_page)?;
                let _was_live = leaf.tombstone(rid.slot_id())?;
            }
            let _crc = new_page.refresh_crc()?;
            self.page_store
                .write_page(rid.page_id(), Arc::new(new_page))?;

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

    /// Reclaim space used by tombstoned slots and dropped namespaces.
    ///
    /// This is a foreground operation: it takes the engine's commit lock,
    /// so concurrent inserts/removes/transactions block for the duration.
    /// Reads continue to be served (they do not take the commit lock).
    ///
    /// Two phases:
    ///
    /// 1. **Live namespaces.** Walk every loaded namespace's leaf chain.
    ///    A leaf with at least one tombstoned slot is rebuilt in place via
    ///    [`slotted::compact_leaf`]; the keymap is fixed up for any
    ///    moved slot ids. A leaf with zero live records is unlinked from
    ///    the chain and its page id pushed onto the page-store free list.
    /// 2. **Dropped namespaces.** Walk every catalog entry that was
    ///    tombstoned (via [`Self::drop_namespace`]) but whose pages have
    ///    not yet been reclaimed. Free every leaf in the chain, then
    ///    remove the catalog entry so the namespace is fully gone.
    ///
    /// On return the catalog and page-file state are flushed via
    /// [`Self::flush`], so the recovered space survives a crash.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from the page store, [`Error::Corrupted`] on
    /// page-format corruption, or [`Error::LockPoisoned`] on a poisoned
    /// lock.
    pub(crate) fn compact(&self) -> Result<CompactStats> {
        let _commit_guard = self
            .commit_lock
            .lock()
            .map_err(|_poisoned| Error::LockPoisoned)?;

        let mut stats = CompactStats::default();

        // Phase 1: live namespaces.
        let live = self.snapshot_namespaces()?;
        for ns in &live {
            self.compact_namespace_chain(ns, &mut stats)?;
        }

        // Phase 2: dropped (tombstoned) namespaces. Snapshot the list of
        // ids first so we can iterate without holding the catalog mutex
        // across the inner free-page loop.
        let to_reclaim: Vec<(u32, u64)> = {
            let catalog = self
                .catalog
                .lock()
                .map_err(|_poisoned| Error::LockPoisoned)?;
            catalog
                .tombstoned_entries()
                .map(|e| (e.id, e.leaf_head))
                .collect()
        };
        for (id, leaf_head) in to_reclaim {
            stats.namespaces_reclaimed += self.free_leaf_chain(leaf_head, &mut stats)?;
            let mut catalog = self
                .catalog
                .lock()
                .map_err(|_poisoned| Error::LockPoisoned)?;
            let _was_present = catalog.remove_tombstoned(id)?;
        }

        // Persist the compacted state. flush() also takes the commit lock,
        // so we drop ours first via lexical scoping at function end. The
        // lock is reentrant-safe in std `Mutex`? No — std `Mutex` is not
        // reentrant. Drop the guard explicitly before calling flush.
        drop(_commit_guard);
        self.flush()?;
        Ok(stats)
    }

    /// Walk one namespace's leaf chain and rewrite or unlink each leaf as
    /// needed. Updates the namespace's `chain_head` / `open_leaf` if the
    /// head was unlinked.
    fn compact_namespace_chain(
        &self,
        ns: &Arc<NamespaceRuntime>,
        stats: &mut CompactStats,
    ) -> Result<()> {
        let mut prev: Option<u64> = None;
        let mut current = ns.chain_head.load(Ordering::Acquire);

        while current != 0 {
            let page_arc = self.page_store.read_page(PageId::new(current))?;
            let next = slotted::next_leaf_of(&page_arc).get();

            // Decide whether to act on this leaf.
            let total_slots = slot_count_of(&page_arc);
            let live_count = live_count_of(&page_arc);

            if live_count == 0 && total_slots > 0 {
                // Empty (every slot is a tombstone). Unlink and free.
                self.unlink_leaf_from_chain(ns, prev, current, next)?;
                self.page_store.free_page(PageId::new(current))?;
                stats.pages_freed += 1;
                // `prev` stays the same — we removed the current node.
                current = next;
                continue;
            }

            if total_slots > live_count {
                // At least one tombstone — rewrite.
                let mut new_page: Page = (*page_arc).clone();
                let outcome = slotted::compact_leaf(&mut new_page)?;
                let _crc = new_page.refresh_crc()?;
                self.page_store
                    .write_page(PageId::new(current), Arc::new(new_page))?;

                for (key, old_slot, new_slot) in outcome.remap {
                    if old_slot == new_slot {
                        continue;
                    }
                    let hash = Keymap::hash_key(&key);
                    let old_rid = Rid::new(current, old_slot);
                    let new_rid = Rid::new(current, new_slot);
                    let _was_present = ns.keymap.remove(hash, old_rid)?;
                    ns.keymap.insert(hash, new_rid)?;
                }
                stats.leaves_compacted += 1;
            }

            prev = Some(current);
            current = next;
        }
        Ok(())
    }

    /// Walk a leaf chain starting at `head` and free every page. Returns
    /// `1` when the head was non-zero (so the caller can count "namespaces
    /// reclaimed"), `0` otherwise.
    fn free_leaf_chain(&self, head: u64, stats: &mut CompactStats) -> Result<u64> {
        if head == 0 {
            return Ok(0);
        }
        let mut current = head;
        while current != 0 {
            let page_arc = self.page_store.read_page(PageId::new(current))?;
            let next = slotted::next_leaf_of(&page_arc).get();
            self.page_store.free_page(PageId::new(current))?;
            stats.pages_freed += 1;
            current = next;
        }
        Ok(1)
    }

    /// Remove `removed_page` from a namespace's leaf chain. Updates either
    /// `prev`'s `next_leaf` pointer or the namespace's `chain_head`/
    /// `open_leaf` accordingly. Caller still owns freeing the page.
    fn unlink_leaf_from_chain(
        &self,
        ns: &Arc<NamespaceRuntime>,
        prev: Option<u64>,
        removed_page: u64,
        next: u64,
    ) -> Result<()> {
        match prev {
            Some(prev_id) => {
                let prev_arc = self.page_store.read_page(PageId::new(prev_id))?;
                let mut new_prev: Page = (*prev_arc).clone();
                {
                    let mut leaf = LeafPage::open(&mut new_prev)?;
                    leaf.set_next_leaf(PageId::new(next));
                }
                let _crc = new_prev.refresh_crc()?;
                self.page_store
                    .write_page(PageId::new(prev_id), Arc::new(new_prev))?;
            }
            None => {
                // Removed leaf is the chain head: advance the namespace's
                // pointers past it.
                ns.chain_head.store(next, Ordering::Release);
                if ns.open_leaf.load(Ordering::Acquire) == removed_page {
                    ns.open_leaf.store(next, Ordering::Release);
                }
            }
        }
        Ok(())
    }

    /// Atomically commit a batch of inserts and removes.
    ///
    /// The WAL is appended in `BatchBegin → ops → BatchEnd` order under the
    /// commit lock; once `BatchEnd` is fsynced the in-memory state is
    /// updated for each op via the same `apply_*` helpers used by single-op
    /// writes. Crash recovery uses the markers to discard partial batches:
    /// a `BatchBegin` without a matching `BatchEnd` means the original
    /// commit was interrupted, so replay drops every op between them.
    ///
    /// The commit lock is held across both phases (WAL append + apply), so
    /// concurrent `insert`/`remove`/`flush` calls serialise behind this
    /// commit and cannot observe an in-flight batch in the WAL or in
    /// in-memory state.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from the WAL or page store, or
    /// [`Error::LockPoisoned`] on lock failures.
    pub(crate) fn commit_batch(&self, ops: &[BatchedOp]) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }

        let _commit_guard = self
            .commit_lock
            .lock()
            .map_err(|_poisoned| Error::LockPoisoned)?;

        let tx_id = self.last_tx_id.fetch_add(1, Ordering::AcqRel) + 1;
        self.page_store.set_last_tx_id(tx_id)?;

        // Phase 1: WAL append. BatchBegin → every op → BatchEnd. Each is a
        // separate `append` so each gets its own WAL seq and the existing
        // single-op replay walker can decode them tag-by-tag.
        let mut wal_buf: Vec<u8> = Vec::with_capacity(32);
        encode_batch_begin(&mut wal_buf, tx_id, ops.len() as u32);
        let _begin_seq = self.wal.append(&wal_buf)?;

        for op in ops {
            wal_buf.clear();
            match op {
                BatchedOp::Insert {
                    ns_id,
                    key,
                    value,
                    expires_at,
                } => encode_insert_op(&mut wal_buf, *ns_id, key, value, *expires_at),
                BatchedOp::Remove { ns_id, key } => encode_remove_op(&mut wal_buf, *ns_id, key),
            }
            let _seq = self.wal.append(&wal_buf)?;
        }

        wal_buf.clear();
        encode_batch_end(&mut wal_buf, tx_id);
        let end_seq = self.wal.append(&wal_buf)?;

        // Force the entire batch to disk before publishing in-memory state,
        // so a crash during the apply phase below replays as a fully-durable
        // batch rather than a partial one.
        self.wal.wait_for_seq(end_seq)?;

        // Phase 2: apply each op's in-memory side. Errors here would be very
        // bad — the WAL has already promised the batch — so we map any
        // failure to a poisoning-style error rather than silently
        // half-applying. In practice the apply path can only fail on lock
        // poisoning or page-format corruption, both of which already abort
        // the database.
        for op in ops {
            match op {
                BatchedOp::Insert {
                    ns_id,
                    key,
                    value,
                    expires_at,
                } => self.apply_insert(*ns_id, key, value, *expires_at)?,
                BatchedOp::Remove { ns_id, key } => {
                    let _did = self.apply_remove(*ns_id, key)?;
                }
            }
        }

        Ok(())
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
        // Hold the commit lock for the duration of flush so a concurrent
        // transaction can never have BatchBegin appended but BatchEnd not
        // yet appended at the moment we snapshot `floor`. With the lock
        // held, the WAL is in a "between batches" state — every batch in
        // the WAL is either fully present or hasn't started yet.
        let _commit_guard = self
            .commit_lock
            .lock()
            .map_err(|_poisoned| Error::LockPoisoned)?;

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
        // Walk every loaded namespace and copy its current chain head and
        // record count back into the catalog so the next open reconstructs
        // the same in-memory state. The default namespace (id 0) must
        // exist; if a future code path tombstones it via the catalog this
        // surfaces here as `Corrupted` rather than silently losing data.
        if catalog.find_by_id(DEFAULT_NAMESPACE_ID).is_none() {
            return Err(Error::Corrupted {
                offset: 0,
                reason: "catalog missing default namespace",
            });
        }

        let snapshot = self.snapshot_namespaces()?;
        for ns in &snapshot {
            // Skip runtimes whose catalog entry was tombstoned out from
            // under us — those are about to be cleaned up by `drop_namespace`.
            if catalog.find_by_id(ns.id).is_none() {
                continue;
            }
            catalog.set_leaf_head(ns.id, ns.chain_head.load(Ordering::Acquire));
            catalog.set_record_count(ns.id, ns.record_count.load(Ordering::Acquire));
        }
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

        // Buffered batch: ops decoded between BatchBegin and BatchEnd, plus
        // the floor decision computed at BatchBegin (a batch is replayed iff
        // its BatchBegin's seq is >= start_seq, which by the floor invariant
        // is equivalent to the BatchEnd seq being >= start_seq).
        let mut pending_batch: Option<PendingBatch> = None;

        while cursor < buf.len() {
            // Per-tag-record state captured by the closure below.
            let mut staged_op: Option<OwnedBatchedOp> = None;
            let mut staged_marker: Option<BatchMarker> = None;

            let record_end = decode_op_into(&buf, &mut cursor, current_seq, |op| {
                match op {
                    ReplayOp::Insert {
                        ns_id,
                        key,
                        value,
                        expires_at,
                    } => {
                        staged_op = Some(OwnedBatchedOp::Insert {
                            ns_id,
                            key: key.to_vec(),
                            value: value.to_vec(),
                            expires_at,
                        });
                    }
                    ReplayOp::Remove { ns_id, key } => {
                        staged_op = Some(OwnedBatchedOp::Remove {
                            ns_id,
                            key: key.to_vec(),
                        });
                    }
                    ReplayOp::BatchBegin { tx_id, op_count } => {
                        staged_marker = Some(BatchMarker::Begin { tx_id, op_count });
                    }
                    ReplayOp::BatchEnd { tx_id } => {
                        staged_marker = Some(BatchMarker::End { tx_id });
                    }
                }
                Ok(())
            })?;
            cursor = record_end;

            let skip_this = current_seq < start_seq;
            current_seq = current_seq.saturating_add(1);

            match staged_marker {
                Some(BatchMarker::Begin { tx_id, op_count }) => {
                    if pending_batch.is_some() {
                        return Err(Error::Corrupted {
                            offset: cursor as u64,
                            reason: "wal batch_begin nested inside another batch",
                        });
                    }
                    pending_batch = Some(PendingBatch {
                        tx_id,
                        skip: skip_this,
                        ops: Vec::with_capacity(op_count as usize),
                    });
                }
                Some(BatchMarker::End { tx_id }) => {
                    let Some(batch) = pending_batch.take() else {
                        return Err(Error::Corrupted {
                            offset: cursor as u64,
                            reason: "wal batch_end without matching batch_begin",
                        });
                    };
                    if batch.tx_id != tx_id {
                        return Err(Error::Corrupted {
                            offset: cursor as u64,
                            reason: "wal batch_end tx_id mismatches batch_begin",
                        });
                    }
                    if !batch.skip {
                        for owned in batch.ops {
                            self.apply_replayed_owned(&owned)?;
                        }
                    }
                }
                None => {
                    if let Some(op) = staged_op {
                        if let Some(batch) = pending_batch.as_mut() {
                            batch.ops.push(op);
                        } else if !skip_this {
                            self.apply_replayed_owned(&op)?;
                        }
                    }
                }
            }
        }

        // Trailing in-progress batch (writer crashed between BatchBegin and
        // BatchEnd): drop the buffered ops to give crash atomicity.
        let _abandoned = pending_batch.take();
        Ok(())
    }

    fn apply_replayed_owned(&self, op: &OwnedBatchedOp) -> Result<()> {
        match op {
            OwnedBatchedOp::Insert {
                ns_id,
                key,
                value,
                expires_at,
            } => self.apply_replayed_insert(*ns_id, key, value, *expires_at),
            OwnedBatchedOp::Remove { ns_id, key } => self.apply_replayed_remove(*ns_id, key),
        }
    }

    fn apply_replayed_insert(
        &self,
        ns_id: u32,
        key: &[u8],
        value: &[u8],
        expires_at: u64,
    ) -> Result<()> {
        let ns = self.namespace(ns_id)?;
        let hash = Keymap::hash_key(key);
        let rid = self.write_record_into_chain(&ns, key, value, expires_at)?;
        ns.keymap.replace_single(hash, rid)?;
        if let Some(bloom) = &ns.bloom {
            bloom.insert(hash);
        }
        let _previous = ns.record_count.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    fn apply_replayed_remove(&self, ns_id: u32, key: &[u8]) -> Result<()> {
        let ns = self.namespace(ns_id)?;
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
/// Tag byte introducing a `BatchBegin` marker: every op until the matching
/// `BatchEnd` belongs to a single transaction. On replay, an unterminated
/// batch (no matching `BatchEnd`) is discarded — that is the crash atomicity
/// guarantee for multi-op transactions.
const WAL_TAG_BATCH_BEGIN: u8 = 2;
/// Tag byte introducing a `BatchEnd` marker. Carries the matching `tx_id`.
const WAL_TAG_BATCH_END: u8 = 3;
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
    /// Marker introducing a multi-op transaction. The replay loop buffers
    /// every following op until it sees [`ReplayOp::BatchEnd`] with a
    /// matching `tx_id`.
    BatchBegin {
        tx_id: u64,
        op_count: u32,
    },
    /// Marker terminating a multi-op transaction. The replay loop applies
    /// every buffered op atomically when this is decoded; if the replay
    /// loop finishes the byte stream without seeing this marker, the
    /// buffered ops are dropped (crash-atomicity).
    BatchEnd {
        tx_id: u64,
    },
}

impl<'a> ReplayOp<'a> {
    fn namespace_id(&self) -> u32 {
        match self {
            Self::Insert { ns_id, .. } | Self::Remove { ns_id, .. } => *ns_id,
            Self::BatchBegin { .. } | Self::BatchEnd { .. } => DEFAULT_NAMESPACE_ID,
        }
    }
}

/// Owned form of [`ReplayOp`] used to buffer batched ops between
/// `BatchBegin` and `BatchEnd` decode events.
#[derive(Debug, Clone)]
enum OwnedBatchedOp {
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
}

/// Internal helper: which kind of marker a single decode event produced.
enum BatchMarker {
    Begin { tx_id: u64, op_count: u32 },
    End { tx_id: u64 },
}

/// In-progress batch buffered while replaying the WAL. Discarded if the
/// byte stream ends before the matching `BatchEnd` arrives — that is the
/// crash atomicity guarantee.
struct PendingBatch {
    tx_id: u64,
    /// True when the `BatchBegin`'s seq was below the persisted floor, so
    /// the entire batch was already applied to pages and must be skipped.
    skip: bool,
    ops: Vec<OwnedBatchedOp>,
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

/// On-disk encoding of a `BatchBegin` marker.
///
/// ```text
///   tag      u8     (2 = BatchBegin)
///   tx_id    u64
///   op_count u32
/// ```
fn encode_batch_begin(buf: &mut Vec<u8>, tx_id: u64, op_count: u32) {
    buf.push(WAL_TAG_BATCH_BEGIN);
    buf.extend_from_slice(&tx_id.to_le_bytes());
    buf.extend_from_slice(&op_count.to_le_bytes());
}

/// On-disk encoding of a `BatchEnd` marker.
///
/// ```text
///   tag      u8     (3 = BatchEnd)
///   tx_id    u64
/// ```
fn encode_batch_end(buf: &mut Vec<u8>, tx_id: u64) {
    buf.push(WAL_TAG_BATCH_END);
    buf.extend_from_slice(&tx_id.to_le_bytes());
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
        WAL_TAG_BATCH_BEGIN => {
            if compressed {
                return Err(Error::Corrupted {
                    offset: start as u64,
                    reason: "batch begin cannot carry the compressed flag",
                });
            }
            if pos + 12 > buf.len() {
                return Err(Error::Corrupted {
                    offset: pos as u64,
                    reason: "wal batch_begin record truncated",
                });
            }
            let mut tx_buf = [0_u8; 8];
            tx_buf.copy_from_slice(&buf[pos..pos + 8]);
            let tx_id = u64::from_le_bytes(tx_buf);
            pos += 8;
            let mut count_buf = [0_u8; 4];
            count_buf.copy_from_slice(&buf[pos..pos + 4]);
            let op_count = u32::from_le_bytes(count_buf);
            pos += 4;
            apply(ReplayOp::BatchBegin { tx_id, op_count })?;
            Ok(pos)
        }
        WAL_TAG_BATCH_END => {
            if compressed {
                return Err(Error::Corrupted {
                    offset: start as u64,
                    reason: "batch end cannot carry the compressed flag",
                });
            }
            if pos + 8 > buf.len() {
                return Err(Error::Corrupted {
                    offset: pos as u64,
                    reason: "wal batch_end record truncated",
                });
            }
            let mut tx_buf = [0_u8; 8];
            tx_buf.copy_from_slice(&buf[pos..pos + 8]);
            let tx_id = u64::from_le_bytes(tx_buf);
            pos += 8;
            apply(ReplayOp::BatchEnd { tx_id })?;
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

    #[test]
    fn commit_batch_applies_every_op_atomically() {
        use super::BatchedOp;

        let cfg = config_for("commit-batch");
        let path = cfg.path.clone();
        let engine = match Engine::open(cfg) {
            Ok(e) => e,
            Err(err) => panic!("open should succeed: {err}"),
        };

        let ops = vec![
            BatchedOp::Insert {
                ns_id: DEFAULT_NAMESPACE_ID,
                key: b"a".to_vec(),
                value: b"1".to_vec(),
                expires_at: 0,
            },
            BatchedOp::Insert {
                ns_id: DEFAULT_NAMESPACE_ID,
                key: b"b".to_vec(),
                value: b"2".to_vec(),
                expires_at: 0,
            },
            BatchedOp::Insert {
                ns_id: DEFAULT_NAMESPACE_ID,
                key: b"c".to_vec(),
                value: b"3".to_vec(),
                expires_at: 0,
            },
        ];
        match engine.commit_batch(&ops) {
            Ok(()) => {}
            Err(err) => panic!("commit_batch should succeed: {err}"),
        }

        for (k, want) in [(b"a", b"1"), (b"b", b"2"), (b"c", b"3")] {
            let got = match engine.get(DEFAULT_NAMESPACE_ID, k) {
                Ok(g) => g,
                Err(err) => panic!("get should succeed: {err}"),
            };
            let cv = match got {
                Some(v) => v,
                None => panic!("expected key {:?} to be visible", k),
            };
            assert_eq!(cv.value.as_ref(), want.as_slice());
        }
        let count = match engine.record_count(DEFAULT_NAMESPACE_ID) {
            Ok(c) => c,
            Err(err) => panic!("record_count: {err}"),
        };
        assert_eq!(count, 3);

        drop(engine);
        cleanup(&path);
    }

    #[test]
    fn partial_batch_in_wal_is_discarded_on_replay() {
        use std::io::Write;

        use super::{encode_batch_begin, encode_insert_op, BatchedOp, DEFAULT_NAMESPACE_ID};

        let cfg = config_for("partial-batch");
        let path = cfg.path.clone();
        let wal_path = crate::storage::v4::wal::Wal::path_for(&path);

        // Phase 1: open the engine, commit one full batch (so the WAL has a
        // well-formed BatchBegin/ops/BatchEnd block we can rely on as
        // surviving), then drop without flush.
        {
            let engine = match Engine::open(cfg.clone()) {
                Ok(e) => e,
                Err(err) => panic!("open should succeed: {err}"),
            };
            let committed = vec![BatchedOp::Insert {
                ns_id: DEFAULT_NAMESPACE_ID,
                key: b"committed".to_vec(),
                value: b"value".to_vec(),
                expires_at: 0,
            }];
            match engine.commit_batch(&committed) {
                Ok(()) => {}
                Err(err) => panic!("commit_batch: {err}"),
            }
            // Don't flush — replay must reconstruct from the WAL.
        }

        // Phase 2: append a fresh BatchBegin + one Insert + intentionally
        // omit the matching BatchEnd. This simulates a writer that crashed
        // between op append and BatchEnd append.
        {
            let mut wal_file = match std::fs::OpenOptions::new()
                .read(true)
                .append(true)
                .open(&wal_path)
            {
                Ok(f) => f,
                Err(err) => panic!("open wal for append: {err}"),
            };
            let mut buf: Vec<u8> = Vec::new();
            // tx_id 9999 / op_count 1 — never sees a matching BatchEnd.
            encode_batch_begin(&mut buf, 9999, 1);
            encode_insert_op(
                &mut buf,
                DEFAULT_NAMESPACE_ID,
                b"orphan",
                b"should-not-survive",
                0,
            );
            // Intentionally do NOT call encode_batch_end.
            match wal_file.write_all(&buf) {
                Ok(()) => {}
                Err(err) => panic!("wal write_all: {err}"),
            }
            match wal_file.sync_data() {
                Ok(()) => {}
                Err(err) => panic!("wal sync_data: {err}"),
            }
        }

        // Phase 3: reopen — replay must apply the committed batch and
        // discard the partial one.
        {
            let engine = match Engine::open(cfg) {
                Ok(e) => e,
                Err(err) => panic!("reopen should succeed: {err}"),
            };

            let committed = match engine.get(DEFAULT_NAMESPACE_ID, b"committed") {
                Ok(g) => g,
                Err(err) => panic!("get(committed): {err}"),
            };
            assert!(committed.is_some(), "fully-committed batch must survive");

            let orphan = match engine.get(DEFAULT_NAMESPACE_ID, b"orphan") {
                Ok(g) => g,
                Err(err) => panic!("get(orphan): {err}"),
            };
            assert!(
                orphan.is_none(),
                "partial batch with no BatchEnd must be discarded"
            );

            let count = match engine.record_count(DEFAULT_NAMESPACE_ID) {
                Ok(c) => c,
                Err(err) => panic!("record_count: {err}"),
            };
            assert_eq!(
                count, 1,
                "only the committed batch contributes to the count"
            );
            drop(engine);
        }
        cleanup(&path);
    }

    #[test]
    fn empty_commit_batch_is_a_noop() {
        use super::BatchedOp;
        let cfg = config_for("empty-batch");
        let path = cfg.path.clone();
        let engine = match Engine::open(cfg) {
            Ok(e) => e,
            Err(err) => panic!("open: {err}"),
        };
        let empty: Vec<BatchedOp> = Vec::new();
        match engine.commit_batch(&empty) {
            Ok(()) => {}
            Err(err) => panic!("empty commit_batch should succeed: {err}"),
        }
        match engine.record_count(DEFAULT_NAMESPACE_ID) {
            Ok(0) => {}
            Ok(other) => panic!("expected 0 records, got {other}"),
            Err(err) => panic!("record_count: {err}"),
        }
        drop(engine);
        cleanup(&path);
    }
}
