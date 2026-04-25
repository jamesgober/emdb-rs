// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Page-backed B-tree index reconstruction for the v0.6 storage engine.

use std::collections::BTreeMap;

use crate::storage::page::free_list::FreeList;
use crate::storage::page::pager::BufferedPager;
use crate::storage::page::value::ValueRef;
use crate::storage::page::{Page, PageHeader, PageId, PageType, PAGE_HEADER_LEN, PAGE_SIZE};
use crate::{Error, Result};

const NODE_KIND_OFFSET: usize = PAGE_HEADER_LEN;
const ENTRY_COUNT_OFFSET: usize = NODE_KIND_OFFSET + 1;
const NEXT_OR_FIRST_CHILD_OFFSET: usize = ENTRY_COUNT_OFFSET + 2;
const ENTRIES_OFFSET: usize = NEXT_OR_FIRST_CHILD_OFFSET + 8;
const LEAF_NODE_KIND: u8 = 1;
const INTERNAL_NODE_KIND: u8 = 0;

/// Page-backed ordered index over keys.
pub(crate) struct BTreeIndex<'a> {
    pager: &'a mut BufferedPager,
    entries: BTreeMap<Vec<u8>, ValueRef>,
    page_ids: Vec<PageId>,
}

#[derive(Clone)]
struct LeafEntry {
    key: Vec<u8>,
    value: ValueRef,
}

#[derive(Clone)]
struct InternalEntry {
    key: Vec<u8>,
    child: PageId,
}

#[derive(Clone)]
struct LevelNode {
    first_key: Vec<u8>,
    page_id: PageId,
}

impl<'a> BTreeIndex<'a> {
    /// Open an index from the pager's current root page.
    pub(crate) fn open(pager: &'a mut BufferedPager) -> Result<Self> {
        let root = pager.root_page_id();
        let mut entries = BTreeMap::new();
        let mut page_ids = Vec::new();
        if root.get() != 0 {
            let leftmost = find_leftmost_leaf(pager, root, &mut page_ids)?;
            load_leaf_chain(pager, leftmost, &mut entries, &mut page_ids)?;
        }

        Ok(Self {
            pager,
            entries,
            page_ids,
        })
    }

    /// Insert or replace a key mapping.
    pub(crate) fn insert(&mut self, key: Vec<u8>, value: ValueRef) -> Result<()> {
        self.insert_deferred(key, value);
        self.rebuild()
    }

    /// Insert or replace a key mapping without rebuilding the page tree.
    pub(crate) fn insert_deferred(&mut self, key: Vec<u8>, value: ValueRef) {
        let _previous = self.entries.insert(key, value);
    }

    /// Read a key mapping.
    pub(crate) fn get(&self, key: &[u8]) -> Option<ValueRef> {
        self.entries.get(key).copied()
    }

    /// Remove a key mapping.
    pub(crate) fn remove(&mut self, key: &[u8]) -> Result<Option<ValueRef>> {
        let removed = self.remove_deferred(key);
        if removed.is_some() {
            self.rebuild()?;
        }
        Ok(removed)
    }

    /// Remove a key mapping without rebuilding the page tree.
    pub(crate) fn remove_deferred(&mut self, key: &[u8]) -> Option<ValueRef> {
        self.entries.remove(key)
    }

    /// Return an ordered range scan over the current key space.
    pub(crate) fn range_scan(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Vec<(Vec<u8>, ValueRef)> {
        self.entries
            .iter()
            .filter(|(key, _value)| {
                start.map_or(true, |lower| key.as_slice() >= lower)
                    && end.map_or(true, |upper| key.as_slice() < upper)
            })
            .map(|(key, value)| (key.clone(), *value))
            .collect()
    }

    /// Remove every key mapping from the index.
    pub(crate) fn clear(&mut self) -> Result<()> {
        self.clear_deferred();
        self.rebuild()
    }

    /// Remove every key mapping without rebuilding the page tree.
    pub(crate) fn clear_deferred(&mut self) {
        self.entries.clear();
    }

    /// Rebuild page-backed tree from deferred in-memory mutations.
    pub(crate) fn rebuild_from_deferred(&mut self) -> Result<()> {
        self.rebuild()
    }

    /// Return the current root page id.
    #[must_use]
    pub(crate) fn root_page_id(&self) -> PageId {
        self.pager.root_page_id()
    }

    /// Borrow the underlying pager.
    pub(crate) fn pager_mut(&mut self) -> &mut BufferedPager {
        self.pager
    }

    fn rebuild(&mut self) -> Result<()> {
        while let Some(page_id) = self.page_ids.pop() {
            let mut free_list = FreeList::new(self.pager);
            free_list.push(page_id)?;
        }

        if self.entries.is_empty() {
            self.pager.set_root_page_id(PageId::new(0))?;
            return Ok(());
        }

        let mut current_level = self.build_leaf_level()?;
        while current_level.len() > 1 {
            current_level = self.build_internal_level(current_level)?;
        }

        let root = current_level.pop().ok_or(Error::TransactionAborted(
            "btree root missing after rebuild",
        ))?;
        self.pager.set_root_page_id(root.page_id)
    }

    fn build_leaf_level(&mut self) -> Result<Vec<LevelNode>> {
        let entries = self
            .entries
            .iter()
            .map(|(key, value)| LeafEntry {
                key: key.clone(),
                value: *value,
            })
            .collect::<Vec<_>>();
        let groups = pack_leaf_groups(&entries)?;
        let page_ids = allocate_pages(self.pager, PageType::BTreeNode, groups.len())?;

        let mut level = Vec::with_capacity(groups.len());
        for (index, group) in groups.iter().enumerate() {
            let next_leaf = page_ids
                .get(index + 1)
                .copied()
                .unwrap_or_else(|| PageId::new(0));
            let page = encode_leaf_page(group, next_leaf)?;
            let page_id = page_ids[index];
            self.pager.write_page(page_id, &page)?;
            self.page_ids.push(page_id);
            level.push(LevelNode {
                first_key: group[0].key.clone(),
                page_id,
            });
        }
        Ok(level)
    }

    fn build_internal_level(&mut self, children: Vec<LevelNode>) -> Result<Vec<LevelNode>> {
        let groups = pack_internal_groups(&children)?;
        let page_ids = allocate_pages(self.pager, PageType::BTreeNode, groups.len())?;

        let mut level = Vec::with_capacity(groups.len());
        for (index, group) in groups.iter().enumerate() {
            let page = encode_internal_page(group)?;
            let page_id = page_ids[index];
            self.pager.write_page(page_id, &page)?;
            self.page_ids.push(page_id);
            level.push(LevelNode {
                first_key: group[0].first_key.clone(),
                page_id,
            });
        }

        Ok(level)
    }
}

fn allocate_pages(
    pager: &mut BufferedPager,
    page_type: PageType,
    count: usize,
) -> Result<Vec<PageId>> {
    let mut page_ids = Vec::with_capacity(count);
    for _ in 0..count {
        let reused = {
            let mut free_list = FreeList::new(pager);
            free_list.pop()?
        };
        let page_id = match reused {
            Some(page_id) => page_id,
            None => pager.allocate_page(page_type)?,
        };
        page_ids.push(page_id);
    }
    Ok(page_ids)
}

fn pack_leaf_groups(entries: &[LeafEntry]) -> Result<Vec<Vec<LeafEntry>>> {
    let mut groups = Vec::new();
    let mut current = Vec::new();
    let mut used = ENTRIES_OFFSET;

    for entry in entries {
        let entry_size = 2 + entry.key.len() + 8;
        if entry_size > PAGE_SIZE - ENTRIES_OFFSET {
            return Err(Error::InvalidConfig("key too large for leaf node page"));
        }
        if !current.is_empty() && used + entry_size > PAGE_SIZE {
            groups.push(current);
            current = Vec::new();
            used = ENTRIES_OFFSET;
        }
        used += entry_size;
        current.push(entry.clone());
    }

    if !current.is_empty() {
        groups.push(current);
    }
    Ok(groups)
}

fn pack_internal_groups(children: &[LevelNode]) -> Result<Vec<Vec<LevelNode>>> {
    let mut groups = Vec::new();
    let mut index = 0_usize;
    while index < children.len() {
        let mut current = Vec::new();
        let mut used = ENTRIES_OFFSET;
        while index < children.len() {
            let child = children[index].clone();
            let entry_size = if current.is_empty() {
                0
            } else {
                2 + child.first_key.len() + 8
            };
            if !current.is_empty() && used + entry_size > PAGE_SIZE {
                break;
            }
            if entry_size > PAGE_SIZE - ENTRIES_OFFSET {
                return Err(Error::InvalidConfig("key too large for internal node page"));
            }
            used += entry_size;
            current.push(child);
            index += 1;
        }
        groups.push(current);
    }
    Ok(groups)
}

fn encode_leaf_page(entries: &[LeafEntry], next_leaf: PageId) -> Result<Page> {
    let entry_count = u16::try_from(entries.len())
        .map_err(|_overflow| Error::TransactionAborted("leaf entry count overflow"))?;

    let mut page = Page::new(PageHeader::new(PageType::BTreeNode));
    page.as_mut_bytes()[NODE_KIND_OFFSET] = LEAF_NODE_KIND;
    page.as_mut_bytes()[ENTRY_COUNT_OFFSET..ENTRY_COUNT_OFFSET + 2]
        .copy_from_slice(&entry_count.to_le_bytes());
    page.as_mut_bytes()[NEXT_OR_FIRST_CHILD_OFFSET..NEXT_OR_FIRST_CHILD_OFFSET + 8]
        .copy_from_slice(&next_leaf.get().to_le_bytes());

    let mut cursor = ENTRIES_OFFSET;
    for entry in entries {
        let key_len = u16::try_from(entry.key.len())
            .map_err(|_overflow| Error::InvalidConfig("key too large for leaf node page"))?;
        page.as_mut_bytes()[cursor..cursor + 2].copy_from_slice(&key_len.to_le_bytes());
        cursor += 2;
        page.as_mut_bytes()[cursor..cursor + entry.key.len()].copy_from_slice(&entry.key);
        cursor += entry.key.len();
        page.as_mut_bytes()[cursor..cursor + 8]
            .copy_from_slice(&entry.value.head().get().to_le_bytes());
        cursor += 8;
    }

    let _crc = page.refresh_crc()?;
    Ok(page)
}

fn encode_internal_page(children: &[LevelNode]) -> Result<Page> {
    let entry_count = u16::try_from(children.len().saturating_sub(1))
        .map_err(|_overflow| Error::TransactionAborted("internal entry count overflow"))?;
    let first_child = children.first().ok_or(Error::TransactionAborted(
        "internal node missing first child",
    ))?;

    let mut page = Page::new(PageHeader::new(PageType::BTreeNode));
    page.as_mut_bytes()[NODE_KIND_OFFSET] = INTERNAL_NODE_KIND;
    page.as_mut_bytes()[ENTRY_COUNT_OFFSET..ENTRY_COUNT_OFFSET + 2]
        .copy_from_slice(&entry_count.to_le_bytes());
    page.as_mut_bytes()[NEXT_OR_FIRST_CHILD_OFFSET..NEXT_OR_FIRST_CHILD_OFFSET + 8]
        .copy_from_slice(&first_child.page_id.get().to_le_bytes());

    let mut cursor = ENTRIES_OFFSET;
    for child in &children[1..] {
        let key_len = u16::try_from(child.first_key.len())
            .map_err(|_overflow| Error::InvalidConfig("key too large for internal node page"))?;
        page.as_mut_bytes()[cursor..cursor + 2].copy_from_slice(&key_len.to_le_bytes());
        cursor += 2;
        page.as_mut_bytes()[cursor..cursor + child.first_key.len()]
            .copy_from_slice(&child.first_key);
        cursor += child.first_key.len();
        page.as_mut_bytes()[cursor..cursor + 8].copy_from_slice(&child.page_id.get().to_le_bytes());
        cursor += 8;
    }

    let _crc = page.refresh_crc()?;
    Ok(page)
}

fn find_leftmost_leaf(
    pager: &mut BufferedPager,
    mut page_id: PageId,
    seen_pages: &mut Vec<PageId>,
) -> Result<PageId> {
    loop {
        let page = pager.read_page(page_id)?;
        let header = page.header()?;
        if header.page_type != PageType::BTreeNode {
            return Err(Error::Corrupted {
                offset: 0,
                reason: "btree node page type mismatch",
            });
        }

        if !seen_pages.contains(&page_id) {
            seen_pages.push(page_id);
        }

        let kind = page.as_bytes()[NODE_KIND_OFFSET];
        if kind == LEAF_NODE_KIND {
            return Ok(page_id);
        }
        if kind != INTERNAL_NODE_KIND {
            return Err(Error::Corrupted {
                offset: 0,
                reason: "btree node kind invalid",
            });
        }

        page_id = PageId::new(read_u64(page.as_bytes(), NEXT_OR_FIRST_CHILD_OFFSET));
    }
}

fn load_leaf_chain(
    pager: &mut BufferedPager,
    mut page_id: PageId,
    entries: &mut BTreeMap<Vec<u8>, ValueRef>,
    seen_pages: &mut Vec<PageId>,
) -> Result<()> {
    while page_id.get() != 0 {
        let page = pager.read_page(page_id)?;
        let header = page.header()?;
        if header.page_type != PageType::BTreeNode {
            return Err(Error::Corrupted {
                offset: 0,
                reason: "btree node page type mismatch",
            });
        }

        if !seen_pages.contains(&page_id) {
            seen_pages.push(page_id);
        }

        if page.as_bytes()[NODE_KIND_OFFSET] != LEAF_NODE_KIND {
            return Err(Error::Corrupted {
                offset: 0,
                reason: "expected leaf node in chain",
            });
        }

        let entry_count = read_u16(page.as_bytes(), ENTRY_COUNT_OFFSET) as usize;
        let mut cursor = ENTRIES_OFFSET;
        for _ in 0..entry_count {
            let key_len = read_u16(page.as_bytes(), cursor) as usize;
            cursor += 2;
            let key = page.as_bytes()[cursor..cursor + key_len].to_vec();
            cursor += key_len;
            let value_page_id = PageId::new(read_u64(page.as_bytes(), cursor));
            cursor += 8;
            let _previous = entries.insert(
                key,
                ValueRef {
                    head: value_page_id,
                },
            );
        }

        page_id = PageId::new(read_u64(page.as_bytes(), NEXT_OR_FIRST_CHILD_OFFSET));
    }

    Ok(())
}

fn read_u16(bytes: &[u8; PAGE_SIZE], offset: usize) -> u16 {
    let mut raw = [0_u8; 2];
    raw.copy_from_slice(&bytes[offset..offset + 2]);
    u16::from_le_bytes(raw)
}

fn read_u64(bytes: &[u8; PAGE_SIZE], offset: usize) -> u64 {
    let mut raw = [0_u8; 8];
    raw.copy_from_slice(&bytes[offset..offset + 8]);
    u64::from_le_bytes(raw)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::BTreeIndex;
    use crate::storage::page::pager::BufferedPager;
    use crate::storage::page::value::write_value;

    fn tmp_path(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0_u128, |duration| duration.as_nanos());
        path.push(format!("emdb-btree-{name}-{nanos}.emdb"));
        path
    }

    #[test]
    fn test_insert_get_remove_round_trip() {
        let path = tmp_path("roundtrip");
        let pager = BufferedPager::open(&path, 0);
        assert!(pager.is_ok());
        let mut pager = match pager {
            Ok(pager) => pager,
            Err(err) => panic!("pager open should succeed: {err}"),
        };
        let mut index = match BTreeIndex::open(&mut pager) {
            Ok(index) => index,
            Err(err) => panic!("btree open should succeed: {err}"),
        };

        let value = match write_value(index.pager, b"v1", None) {
            Ok(value) => value,
            Err(err) => panic!("value write should succeed: {err}"),
        };
        assert!(index.insert(b"a".to_vec(), value).is_ok());
        assert_eq!(index.get(b"a"), Some(value));
        assert!(index.remove(b"a").is_ok());
        assert_eq!(index.get(b"a"), None);

        let _removed = std::fs::remove_file(path);
    }

    #[test]
    fn test_range_scan_returns_sorted_keys() {
        let path = tmp_path("range");
        let pager = BufferedPager::open(&path, 0);
        assert!(pager.is_ok());
        let mut pager = match pager {
            Ok(pager) => pager,
            Err(err) => panic!("pager open should succeed: {err}"),
        };
        let mut index = match BTreeIndex::open(&mut pager) {
            Ok(index) => index,
            Err(err) => panic!("btree open should succeed: {err}"),
        };

        for key in [b"a".as_slice(), b"b", b"c", b"d"] {
            let value = match write_value(index.pager, key, None) {
                Ok(value) => value,
                Err(err) => panic!("value write should succeed: {err}"),
            };
            assert!(index.insert(key.to_vec(), value).is_ok());
        }

        let scanned = index.range_scan(Some(b"b"), Some(b"d"));
        let keys = scanned
            .into_iter()
            .map(|(key, _value)| key)
            .collect::<Vec<_>>();
        assert_eq!(keys, vec![b"b".to_vec(), b"c".to_vec()]);

        let _removed = std::fs::remove_file(path);
    }

    #[test]
    fn test_random_sequences_match_oracle_and_reopen() {
        let path = tmp_path("oracle");
        let pager = BufferedPager::open(&path, 0);
        assert!(pager.is_ok());
        let mut pager = match pager {
            Ok(pager) => pager,
            Err(err) => panic!("pager open should succeed: {err}"),
        };
        {
            let mut index = match BTreeIndex::open(&mut pager) {
                Ok(index) => index,
                Err(err) => panic!("btree open should succeed: {err}"),
            };
            let mut oracle = BTreeMap::<Vec<u8>, u64>::new();
            let mut seed = 0x1234_5678_9ABC_DEF0_u64;

            for step in 0_u32..10_000 {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                let key = format!("k{}", seed % 256).into_bytes();
                if (seed >> 63) == 0 {
                    let value_bytes = format!("v{step}").into_bytes();
                    let value_ref = match write_value(index.pager, &value_bytes, None) {
                        Ok(value_ref) => value_ref,
                        Err(err) => panic!("value write should succeed: {err}"),
                    };
                    assert!(index.insert(key.clone(), value_ref).is_ok());
                    let _previous = oracle.insert(key, value_ref.head().get());
                } else {
                    let removed = index.remove(&key);
                    assert!(removed.is_ok());
                    let _previous = oracle.remove(&key);
                }
            }

            for (key, value_page_id) in &oracle {
                let actual = index.get(key);
                assert!(actual.is_some());
                assert_eq!(actual.map(|value| value.head().get()), Some(*value_page_id));
            }
            assert_eq!(index.range_scan(None, None).len(), oracle.len());
        }

        let pager = BufferedPager::open(&path, 0);
        assert!(pager.is_ok());
        let mut pager = match pager {
            Ok(pager) => pager,
            Err(err) => panic!("pager reopen should succeed: {err}"),
        };
        let reopened = BTreeIndex::open(&mut pager);
        assert!(reopened.is_ok());
        let reopened = match reopened {
            Ok(index) => index,
            Err(err) => panic!("btree reopen should succeed: {err}"),
        };
        assert!(reopened.root_page_id().get() == 0 || !reopened.range_scan(None, None).is_empty());

        let _removed = std::fs::remove_file(path);
    }
}
