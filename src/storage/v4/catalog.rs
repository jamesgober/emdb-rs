// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Namespace catalog for the v0.7 storage engine.
//!
//! Persists, for each namespace:
//!
//! - `id`: stable u32 assigned at creation; never reused.
//! - `name`: human-readable name (empty string for the default namespace).
//! - `leaf_head`: page id of the namespace's first slotted leaf, or 0.
//! - `bloom_root`: page id of the persisted bloom-filter snapshot, or 0.
//! - `record_count`: live record count at the last persist point.
//! - `flags`: tombstone marker so deletes survive reopens.
//!
//! The catalog lives in one or more chained catalog pages on disk. A single
//! 4 KB page comfortably holds ~80 namespaces for 16-character names; the
//! chain handles workloads with thousands of namespaces without forcing a
//! larger page size.
//!
//! ## Layout
//!
//! ```text
//!   0..16    PageHeader  (page_type = LeafSlotted-equivalent, lsn, page_crc)
//!  16..20    entry_count  u32 LE
//!  20..28    next_page    u64 LE   page id of next catalog page (0 = end)
//!  28..      entries packed back-to-back
//! ```
//!
//! Each entry is variable-length:
//!
//! ```text
//!   ns_id        u32 LE
//!   name_len     u16 LE
//!   name         [u8; name_len]
//!   leaf_head    u64 LE
//!   bloom_root   u64 LE
//!   record_count u64 LE
//!   flags        u8       — bit 0 = tombstone
//!   reserved     u8 × 3
//! ```
//!
//! Entries are appended in id-order. Updating a namespace rewrites the
//! whole page (cheap because pages are 4 KB and the catalog is rarely
//! mutated). The page CRC is refreshed before the page is written.

use std::sync::Arc;

use crate::storage::page::{Page, PageHeader, PageId, PageType, PAGE_HEADER_LEN, PAGE_SIZE};
use crate::storage::v4::store::PageStore;
use crate::{Error, Result};

const ENTRY_COUNT_OFFSET: usize = PAGE_HEADER_LEN;
const NEXT_PAGE_OFFSET: usize = ENTRY_COUNT_OFFSET + 4;
const ENTRIES_OFFSET: usize = NEXT_PAGE_OFFSET + 8;
/// Fixed bytes per entry, excluding the variable-length name.
const ENTRY_FIXED_BYTES: usize = 4 + 2 + 8 + 8 + 8 + 1 + 3;

/// Bit 0 of [`CatalogEntry::flags`] indicates a tombstoned entry.
pub(crate) const ENTRY_FLAG_TOMBSTONE: u8 = 1 << 0;

/// One row in the catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogEntry {
    /// Namespace id assigned at creation.
    pub(crate) id: u32,
    /// Human-readable name. Empty for the default namespace.
    pub(crate) name: String,
    /// Page id of the first slotted leaf, or 0 if the namespace has no
    /// records.
    pub(crate) leaf_head: u64,
    /// Page id of the persisted bloom-filter snapshot, or 0.
    pub(crate) bloom_root: u64,
    /// Live record count at the last persist point.
    pub(crate) record_count: u64,
    /// Bit field; see [`ENTRY_FLAG_TOMBSTONE`].
    pub(crate) flags: u8,
}

impl CatalogEntry {
    /// Construct a fresh entry for a new namespace.
    #[must_use]
    pub(crate) fn new(id: u32, name: impl Into<String>) -> Self {
        Self {
            id,
            name: name.into(),
            leaf_head: 0,
            bloom_root: 0,
            record_count: 0,
            flags: 0,
        }
    }

    /// `true` if this entry has been tombstoned and should be ignored by
    /// the public catalog APIs.
    #[must_use]
    pub(crate) const fn is_tombstoned(&self) -> bool {
        self.flags & ENTRY_FLAG_TOMBSTONE != 0
    }

    fn encoded_len(&self) -> usize {
        ENTRY_FIXED_BYTES + self.name.len()
    }
}

/// In-memory representation of the persisted namespace catalog.
#[derive(Debug, Clone, Default)]
pub(crate) struct Catalog {
    entries: Vec<CatalogEntry>,
    /// `Some(_)` when this catalog was loaded from disk; persists as the
    /// root we should reuse on rewrite. `None` for a freshly-created
    /// catalog that has not yet been written.
    root_page: Option<PageId>,
}

impl Catalog {
    /// Create an empty catalog with only the implicit default namespace
    /// (id 0, empty name).
    #[must_use]
    pub(crate) fn fresh() -> Self {
        let mut catalog = Self::default();
        catalog.entries.push(CatalogEntry::new(0, ""));
        catalog
    }

    /// Load the catalog from a page store.
    ///
    /// `root` is the page id stored in the file header's `namespace_root`
    /// field; pass `PageId::new(0)` for "no catalog yet, use a fresh one".
    ///
    /// # Errors
    ///
    /// Returns [`Error::Corrupted`] when a catalog page fails CRC validation
    /// or when the on-disk encoding is malformed; propagates underlying I/O
    /// errors from [`PageStore::read_page`].
    pub(crate) fn load(store: &PageStore, root: PageId) -> Result<Self> {
        if root.get() == 0 {
            return Ok(Self::fresh());
        }

        let mut entries: Vec<CatalogEntry> = Vec::new();
        let mut current = root;
        while current.get() != 0 {
            let page = store.read_page(current)?;
            page.validate_crc()?;
            let bytes = page.as_bytes();

            let header = page.header()?;
            if header.page_type != PageType::LeafSlotted {
                return Err(Error::Corrupted {
                    offset: 0,
                    reason: "catalog page has unexpected page type",
                });
            }

            let entry_count = read_u32(bytes, ENTRY_COUNT_OFFSET) as usize;
            let next_page = read_u64(bytes, NEXT_PAGE_OFFSET);

            let mut cursor = ENTRIES_OFFSET;
            for _ in 0..entry_count {
                let entry = decode_entry(bytes, &mut cursor)?;
                entries.push(entry);
            }

            current = PageId::new(next_page);
        }

        // Ensure the default namespace is always present.
        if !entries.iter().any(|entry| entry.id == 0) {
            entries.insert(0, CatalogEntry::new(0, ""));
        }

        Ok(Self {
            entries,
            root_page: Some(root),
        })
    }

    /// Persist the catalog to the page store and return the new root page id.
    ///
    /// The catalog is rewritten in place when it fits in a single page;
    /// larger catalogs allocate a chain of pages. Old pages from a previous
    /// persist are not freed by this method (that requires the free-list
    /// integration coming in Phase I); over many cycles the catalog
    /// occupies at most one chain's worth of pages plus the previous
    /// generation's chain.
    ///
    /// # Errors
    ///
    /// Returns the underlying I/O error on write failure, or
    /// [`Error::InvalidConfig`] when an entry's encoded length exceeds the
    /// remaining bytes of a single page (an entry name longer than ~4 KB).
    pub(crate) fn save(&mut self, store: &PageStore) -> Result<PageId> {
        if self.entries.is_empty() {
            // Nothing to persist; return zero so the header can record
            // "no catalog".
            return Ok(PageId::new(0));
        }

        // Group entries into pages without splitting any entry across pages.
        let mut groups: Vec<Vec<&CatalogEntry>> = Vec::new();
        let mut current_group: Vec<&CatalogEntry> = Vec::new();
        let mut current_bytes = ENTRIES_OFFSET;

        for entry in &self.entries {
            let entry_len = entry.encoded_len();
            if entry_len > PAGE_SIZE - ENTRIES_OFFSET {
                return Err(Error::InvalidConfig(
                    "catalog entry encoded length exceeds single-page capacity",
                ));
            }
            if current_bytes + entry_len > PAGE_SIZE {
                groups.push(std::mem::take(&mut current_group));
                current_bytes = ENTRIES_OFFSET;
            }
            current_group.push(entry);
            current_bytes += entry_len;
        }
        if !current_group.is_empty() {
            groups.push(current_group);
        }

        // Allocate page ids for each group.
        let mut page_ids: Vec<PageId> = Vec::with_capacity(groups.len());
        for _ in 0..groups.len() {
            page_ids.push(store.allocate_page()?);
        }

        // Encode and write each group, chaining via next_page.
        for (idx, group) in groups.iter().enumerate() {
            let mut page = Page::new(PageHeader::new(PageType::LeafSlotted));
            {
                let bytes = page.as_mut_bytes();
                write_u32(bytes, ENTRY_COUNT_OFFSET, group.len() as u32);
                let next = page_ids
                    .get(idx + 1)
                    .copied()
                    .unwrap_or_else(|| PageId::new(0));
                write_u64(bytes, NEXT_PAGE_OFFSET, next.get());

                let mut cursor = ENTRIES_OFFSET;
                for entry in group {
                    encode_entry(bytes, &mut cursor, entry);
                }
            }
            let _crc = page.refresh_crc()?;
            store.write_page(page_ids[idx], Arc::new(page))?;
        }

        let root = page_ids[0];
        self.root_page = Some(root);
        Ok(root)
    }

    /// Number of non-tombstoned entries.
    #[must_use]
    pub(crate) fn live_count(&self) -> usize {
        self.entries.iter().filter(|e| !e.is_tombstoned()).count()
    }

    /// Total entry count including tombstones.
    #[must_use]
    pub(crate) fn raw_count(&self) -> usize {
        self.entries.len()
    }

    /// Iterate live (non-tombstoned) entries.
    pub(crate) fn live_entries(&self) -> impl Iterator<Item = &CatalogEntry> {
        self.entries.iter().filter(|e| !e.is_tombstoned())
    }

    /// Look up by id.
    #[must_use]
    pub(crate) fn find_by_id(&self, id: u32) -> Option<&CatalogEntry> {
        self.entries
            .iter()
            .find(|e| e.id == id && !e.is_tombstoned())
    }

    /// Look up by name.
    #[must_use]
    pub(crate) fn find_by_name(&self, name: &str) -> Option<&CatalogEntry> {
        self.entries
            .iter()
            .find(|e| e.name == name && !e.is_tombstoned())
    }

    /// Create a new namespace with the given name. Returns the assigned id.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfig`] when a namespace with this name
    /// already exists, or when the catalog has run out of u32 ids.
    pub(crate) fn create(&mut self, name: impl Into<String>) -> Result<u32> {
        let name = name.into();
        if self.find_by_name(&name).is_some() {
            return Err(Error::InvalidConfig("namespace already exists"));
        }

        let next_id = self
            .entries
            .iter()
            .map(|e| e.id)
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(Error::InvalidConfig("namespace id space exhausted"))?;

        self.entries.push(CatalogEntry::new(next_id, name));
        Ok(next_id)
    }

    /// Tombstone a namespace by id. Returns `true` if a live entry was
    /// found and tombstoned. The default namespace (id 0) cannot be
    /// tombstoned.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfig`] when the caller attempts to
    /// tombstone the default namespace.
    pub(crate) fn tombstone(&mut self, id: u32) -> Result<bool> {
        if id == 0 {
            return Err(Error::InvalidConfig(
                "default namespace cannot be tombstoned",
            ));
        }
        for entry in self.entries.iter_mut() {
            if entry.id == id && !entry.is_tombstoned() {
                entry.flags |= ENTRY_FLAG_TOMBSTONE;
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Update the leaf-chain head for a namespace. No-op if the id refers
    /// to a missing or tombstoned entry.
    pub(crate) fn set_leaf_head(&mut self, id: u32, head: u64) {
        for entry in self.entries.iter_mut() {
            if entry.id == id && !entry.is_tombstoned() {
                entry.leaf_head = head;
                return;
            }
        }
    }

    /// Update the persisted bloom-filter root for a namespace.
    pub(crate) fn set_bloom_root(&mut self, id: u32, root: u64) {
        for entry in self.entries.iter_mut() {
            if entry.id == id && !entry.is_tombstoned() {
                entry.bloom_root = root;
                return;
            }
        }
    }

    /// Update the persisted record count for a namespace.
    pub(crate) fn set_record_count(&mut self, id: u32, count: u64) {
        for entry in self.entries.iter_mut() {
            if entry.id == id && !entry.is_tombstoned() {
                entry.record_count = count;
                return;
            }
        }
    }
}

fn encode_entry(out: &mut [u8], cursor: &mut usize, entry: &CatalogEntry) {
    write_u32(out, *cursor, entry.id);
    *cursor += 4;
    write_u16(out, *cursor, entry.name.len() as u16);
    *cursor += 2;
    out[*cursor..*cursor + entry.name.len()].copy_from_slice(entry.name.as_bytes());
    *cursor += entry.name.len();
    write_u64(out, *cursor, entry.leaf_head);
    *cursor += 8;
    write_u64(out, *cursor, entry.bloom_root);
    *cursor += 8;
    write_u64(out, *cursor, entry.record_count);
    *cursor += 8;
    out[*cursor] = entry.flags;
    *cursor += 1;
    out[*cursor] = 0;
    out[*cursor + 1] = 0;
    out[*cursor + 2] = 0;
    *cursor += 3;
}

fn decode_entry(bytes: &[u8], cursor: &mut usize) -> Result<CatalogEntry> {
    if *cursor + ENTRY_FIXED_BYTES > bytes.len() {
        return Err(Error::Corrupted {
            offset: *cursor as u64,
            reason: "catalog entry truncated",
        });
    }
    let id = read_u32(bytes, *cursor);
    *cursor += 4;
    let name_len = read_u16(bytes, *cursor) as usize;
    *cursor += 2;
    if *cursor + name_len + (ENTRY_FIXED_BYTES - 6) > bytes.len() {
        return Err(Error::Corrupted {
            offset: *cursor as u64,
            reason: "catalog entry name extends past page",
        });
    }
    let name_bytes = &bytes[*cursor..*cursor + name_len];
    let name = match std::str::from_utf8(name_bytes) {
        Ok(s) => s.to_string(),
        Err(_invalid_utf8) => {
            return Err(Error::Corrupted {
                offset: *cursor as u64,
                reason: "catalog namespace name is not valid utf-8",
            });
        }
    };
    *cursor += name_len;
    let leaf_head = read_u64(bytes, *cursor);
    *cursor += 8;
    let bloom_root = read_u64(bytes, *cursor);
    *cursor += 8;
    let record_count = read_u64(bytes, *cursor);
    *cursor += 8;
    let flags = bytes[*cursor];
    *cursor += 1;
    // Skip three reserved bytes.
    *cursor += 3;
    Ok(CatalogEntry {
        id,
        name,
        leaf_head,
        bloom_root,
        record_count,
        flags,
    })
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    let mut buf = [0_u8; 2];
    buf.copy_from_slice(&bytes[offset..offset + 2]);
    u16::from_le_bytes(buf)
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

fn write_u16(out: &mut [u8], offset: usize, value: u16) {
    out[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn write_u32(out: &mut [u8], offset: usize, value: u32) {
    out[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn write_u64(out: &mut [u8], offset: usize, value: u64) {
    out[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::{Catalog, CatalogEntry};
    use crate::page_cache::PageCache;
    use crate::storage::page::PageId;
    use crate::storage::v4::store::PageStore;
    use std::sync::Arc;

    fn tmp_path(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0_u128, |d| d.as_nanos());
        p.push(format!("emdb-v4-catalog-{name}-{nanos}.emdb"));
        p
    }

    fn open_store(name: &str) -> (PageStore, std::path::PathBuf) {
        let path = tmp_path(name);
        let cache = Arc::new(PageCache::with_default_capacity());
        let store = match PageStore::open(path.clone(), 0, cache) {
            Ok(s) => s,
            Err(err) => panic!("open should succeed: {err}"),
        };
        (store, path)
    }

    #[test]
    fn fresh_catalog_only_has_default_namespace() {
        let catalog = Catalog::fresh();
        assert_eq!(catalog.live_count(), 1);
        let default = match catalog.find_by_id(0) {
            Some(e) => e,
            None => panic!("default namespace should be present"),
        };
        assert_eq!(default.name, "");
    }

    #[test]
    fn create_assigns_monotonic_ids() {
        let mut catalog = Catalog::fresh();
        let users_id = match catalog.create("users") {
            Ok(id) => id,
            Err(err) => panic!("create should succeed: {err}"),
        };
        let sessions_id = match catalog.create("sessions") {
            Ok(id) => id,
            Err(err) => panic!("create should succeed: {err}"),
        };
        assert_eq!(users_id, 1);
        assert_eq!(sessions_id, 2);
    }

    #[test]
    fn create_rejects_duplicate_names() {
        let mut catalog = Catalog::fresh();
        let _ = catalog.create("users");
        let dup = catalog.create("users");
        assert!(dup.is_err());
    }

    #[test]
    fn cannot_tombstone_default_namespace() {
        let mut catalog = Catalog::fresh();
        let result = catalog.tombstone(0);
        assert!(result.is_err());
    }

    #[test]
    fn tombstone_hides_entry_from_lookups() {
        let mut catalog = Catalog::fresh();
        let id = match catalog.create("temp") {
            Ok(id) => id,
            Err(err) => panic!("create should succeed: {err}"),
        };
        let by_name = catalog.find_by_name("temp");
        assert!(by_name.is_some());
        assert!(matches!(catalog.tombstone(id), Ok(true)));
        let by_name = catalog.find_by_name("temp");
        assert!(by_name.is_none());
        let by_id = catalog.find_by_id(id);
        assert!(by_id.is_none());
    }

    #[test]
    fn tombstone_unknown_id_reports_false() {
        let mut catalog = Catalog::fresh();
        assert!(matches!(catalog.tombstone(99), Ok(false)));
    }

    #[test]
    fn save_then_load_round_trips_via_page_store() {
        let (store, path) = open_store("round-trip");
        let mut catalog = Catalog::fresh();
        let _ = catalog.create("users");
        let _ = catalog.create("sessions");
        catalog.set_leaf_head(1, 42);
        catalog.set_bloom_root(2, 7);
        catalog.set_record_count(1, 1000);

        let root = match catalog.save(&store) {
            Ok(r) => r,
            Err(err) => panic!("save should succeed: {err}"),
        };
        assert_ne!(root.get(), 0);
        let _ = store.flush();

        let reloaded = match Catalog::load(&store, root) {
            Ok(c) => c,
            Err(err) => panic!("load should succeed: {err}"),
        };
        assert_eq!(reloaded.live_count(), 3);

        let users = match reloaded.find_by_name("users") {
            Some(e) => e,
            None => panic!("users entry missing after reload"),
        };
        assert_eq!(users.id, 1);
        assert_eq!(users.leaf_head, 42);
        assert_eq!(users.record_count, 1000);

        let sessions = match reloaded.find_by_name("sessions") {
            Some(e) => e,
            None => panic!("sessions entry missing after reload"),
        };
        assert_eq!(sessions.id, 2);
        assert_eq!(sessions.bloom_root, 7);

        let _removed = std::fs::remove_file(&path);
    }

    #[test]
    fn save_with_zero_namespaces_returns_zero_root() {
        // The default-fresh catalog always has the default namespace, so
        // we directly construct an empty one here to exercise the early
        // return path.
        let (store, path) = open_store("empty");
        let mut empty = Catalog::default();
        let root = match empty.save(&store) {
            Ok(r) => r,
            Err(err) => panic!("save should succeed: {err}"),
        };
        assert_eq!(root.get(), 0);
        let _removed = std::fs::remove_file(&path);
    }

    #[test]
    fn load_from_zero_root_returns_fresh_catalog() {
        let (store, path) = open_store("from-zero");
        let catalog = match Catalog::load(&store, PageId::new(0)) {
            Ok(c) => c,
            Err(err) => panic!("load should succeed: {err}"),
        };
        assert_eq!(catalog.live_count(), 1);
        let _removed = std::fs::remove_file(&path);
    }

    #[test]
    fn live_entries_skips_tombstones() {
        let mut catalog = Catalog::fresh();
        let temp_id = match catalog.create("temp") {
            Ok(id) => id,
            Err(err) => panic!("create should succeed: {err}"),
        };
        let _ = catalog.create("keep");
        let _ = catalog.tombstone(temp_id);

        let names: Vec<String> = catalog.live_entries().map(|e| e.name.clone()).collect();
        assert_eq!(names.len(), 2); // "" and "keep"
        assert!(names.iter().any(|n| n.is_empty()));
        assert!(names.iter().any(|n| n == "keep"));
    }

    #[test]
    fn save_chains_pages_when_catalog_overflows_one_page() {
        // Each entry with a 200-byte name uses 234 bytes. ~17 entries fill
        // a 4 KB page. Add 30 to force a chain.
        let (store, path) = open_store("chain");
        let mut catalog = Catalog::fresh();
        let big_name = "n".repeat(200);
        for i in 0..30_u32 {
            let _ = catalog.create(format!("{}{i}", big_name));
        }
        let root = match catalog.save(&store) {
            Ok(r) => r,
            Err(err) => panic!("save should succeed: {err}"),
        };
        let _ = store.flush();

        let reloaded = match Catalog::load(&store, root) {
            Ok(c) => c,
            Err(err) => panic!("load should succeed: {err}"),
        };
        assert_eq!(reloaded.live_count(), 31); // default + 30

        let _removed = std::fs::remove_file(&path);
    }

    #[test]
    fn entry_new_constructor_initialises_zero_metadata() {
        let entry = CatalogEntry::new(7, "thing");
        assert_eq!(entry.id, 7);
        assert_eq!(entry.name, "thing");
        assert_eq!(entry.leaf_head, 0);
        assert_eq!(entry.bloom_root, 0);
        assert_eq!(entry.record_count, 0);
        assert_eq!(entry.flags, 0);
        assert!(!entry.is_tombstoned());
    }
}
