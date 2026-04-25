// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Buffered page-file pager for the v0.6 storage engine.

use std::collections::{HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::storage::page::header::PageFileHeader;
#[cfg(feature = "mmap")]
use crate::storage::page::mmap::MmapView;
use crate::storage::page::{Page, PageHeader, PageId, PageType, PAGE_SIZE};
use crate::{Error, Result};

const DEFAULT_CACHE_CAPACITY: usize = 64;

#[derive(Debug)]
struct PageCache {
    capacity: usize,
    pages: HashMap<PageId, Arc<Page>>,
    lru: VecDeque<PageId>,
}

impl PageCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            pages: HashMap::new(),
            lru: VecDeque::new(),
        }
    }

    fn get(&mut self, page_id: PageId) -> Option<Arc<Page>> {
        let page = self.pages.get(&page_id).cloned()?;
        self.touch(page_id);
        Some(page)
    }

    fn insert(&mut self, page_id: PageId, page: Arc<Page>) {
        let _previous = self.pages.insert(page_id, page);
        self.touch(page_id);
        self.evict_if_needed();
    }

    fn touch(&mut self, page_id: PageId) {
        if let Some(pos) = self.lru.iter().position(|existing| *existing == page_id) {
            let _removed = self.lru.remove(pos);
        }
        self.lru.push_back(page_id);
    }

    fn evict_if_needed(&mut self) {
        while self.pages.len() > self.capacity {
            let Some(evict) = self.lru.pop_front() else {
                break;
            };
            let _removed = self.pages.remove(&evict);
        }
    }

    fn clear(&mut self) {
        self.pages.clear();
        self.lru.clear();
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.pages.len()
    }
}

/// Buffered file-backed pager with a small in-process page cache.
#[derive(Debug)]
pub(crate) struct BufferedPager {
    file: File,
    path: PathBuf,
    header: PageFileHeader,
    cache: PageCache,
    #[cfg(feature = "mmap")]
    mmap: Option<MmapView>,
    #[cfg(feature = "mmap")]
    mmap_stale: bool,
}

impl BufferedPager {
    /// Open or create a page file.
    pub(crate) fn open(path: impl Into<PathBuf>, flags: u32) -> Result<Self> {
        Self::open_with_options(path, flags, DEFAULT_CACHE_CAPACITY, false)
    }

    /// Open or create a page file with optional mmap-backed reads.
    #[cfg(feature = "mmap")]
    pub(crate) fn open_with_mmap(
        path: impl Into<PathBuf>,
        flags: u32,
        use_mmap: bool,
    ) -> Result<Self> {
        Self::open_with_options(path, flags, DEFAULT_CACHE_CAPACITY, use_mmap)
    }

    /// Open or create a page file with an explicit cache capacity.
    pub(crate) fn open_with_capacity(
        path: impl Into<PathBuf>,
        flags: u32,
        cache_capacity: usize,
    ) -> Result<Self> {
        Self::open_with_options(path, flags, cache_capacity, false)
    }

    fn open_with_options(
        path: impl Into<PathBuf>,
        flags: u32,
        cache_capacity: usize,
        #[cfg(feature = "mmap")] use_mmap: bool,
        #[cfg(not(feature = "mmap"))] _use_mmap: bool,
    ) -> Result<Self> {
        let path = path.into();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        let header = if file.metadata()?.len() == 0 {
            let header = PageFileHeader {
                flags,
                created_at: now_unix_millis(),
                ..PageFileHeader::default()
            };
            write_header_page(&mut file, header)?;
            header
        } else {
            let header = read_header_page(&mut file)?;
            if (header.flags & flags) != header.flags {
                return Err(Error::FeatureMismatch {
                    file_flags: header.flags,
                    build_flags: flags,
                });
            }
            header
        };

        #[cfg(feature = "mmap")]
        let mmap = if use_mmap {
            Some(MmapView::open(&file)?)
        } else {
            None
        };

        Ok(Self {
            file,
            path,
            header,
            cache: PageCache::new(cache_capacity),
            #[cfg(feature = "mmap")]
            mmap,
            #[cfg(feature = "mmap")]
            mmap_stale: false,
        })
    }

    /// Return the decoded page-file header.
    #[must_use]
    pub(crate) fn header(&self) -> PageFileHeader {
        self.header
    }

    /// Return the current free-list head page id.
    #[must_use]
    pub(crate) fn free_list_head(&self) -> PageId {
        self.header.free_list_head
    }

    /// Return the current B-tree root page id.
    #[must_use]
    pub(crate) fn root_page_id(&self) -> PageId {
        self.header.root_page_id
    }

    /// Return the highest committed transaction id persisted in the page header.
    #[must_use]
    pub(crate) fn last_tx_id(&self) -> u64 {
        self.header.last_tx_id
    }

    /// Update the free-list head page id in the persisted file header.
    pub(crate) fn set_free_list_head(&mut self, page_id: PageId) -> Result<()> {
        self.header.free_list_head = page_id;
        self.persist_header()
    }

    /// Update the B-tree root page id in the persisted file header.
    pub(crate) fn set_root_page_id(&mut self, page_id: PageId) -> Result<()> {
        self.header.root_page_id = page_id;
        self.persist_header()
    }

    /// Update the last committed transaction id in the persisted file header.
    pub(crate) fn set_last_tx_id(&mut self, tx_id: u64) -> Result<()> {
        self.header.last_tx_id = tx_id;
        self.persist_header()
    }

    /// Reset the page file to a fresh header while keeping the same path and cache settings.
    pub(crate) fn reset(&mut self, flags: u32) -> Result<()> {
        self.file.set_len(0)?;
        self.file.flush()?;
        self.header = PageFileHeader {
            flags,
            created_at: now_unix_millis(),
            ..PageFileHeader::default()
        };
        self.cache.clear();
        write_header_page(&mut self.file, self.header)?;
        #[cfg(feature = "mmap")]
        {
            self.mmap_stale = true;
        }
        self.refresh_mapping()
    }

    /// Allocate a new page by extending the file and return its page id.
    pub(crate) fn allocate_page(&mut self, page_type: PageType) -> Result<PageId> {
        let page_id = PageId::new(self.header.page_count);
        let mut page = Page::new(PageHeader::new(page_type));
        let _crc = page.refresh_crc()?;
        self.write_page(page_id, &page)?;
        self.header.page_count = self
            .header
            .page_count
            .checked_add(1)
            .ok_or(Error::Corrupted {
                offset: 0,
                reason: "page count overflow",
            })?;
        self.persist_header()?;
        Ok(page_id)
    }

    /// Read a page by id, consulting the in-memory cache first.
    pub(crate) fn read_page(&mut self, page_id: PageId) -> Result<Arc<Page>> {
        if let Some(page) = self.cache.get(page_id) {
            return Ok(page);
        }

        self.ensure_page_in_bounds(page_id)?;
        let offset = page_offset(page_id)?;
        #[cfg(feature = "mmap")]
        let bytes = if !self.mmap_stale {
            if let Some(map) = &self.mmap {
                map.read_page(offset as usize)?
            } else {
                let mut bytes = [0_u8; PAGE_SIZE];
                let _offset = self.file.seek(SeekFrom::Start(offset))?;
                self.file.read_exact(&mut bytes)?;
                bytes
            }
        } else {
            let mut bytes = [0_u8; PAGE_SIZE];
            let _offset = self.file.seek(SeekFrom::Start(offset))?;
            self.file.read_exact(&mut bytes)?;
            bytes
        };
        #[cfg(not(feature = "mmap"))]
        let bytes = {
            let mut bytes = [0_u8; PAGE_SIZE];
            let _offset = self.file.seek(SeekFrom::Start(offset))?;
            self.file.read_exact(&mut bytes)?;
            bytes
        };

        let page = Arc::new(Page::from_bytes(bytes));
        let _header = page.header()?;
        page.validate_crc()?;
        self.cache.insert(page_id, Arc::clone(&page));
        Ok(page)
    }

    /// Write a full page image by id and update the cache entry.
    pub(crate) fn write_page(&mut self, page_id: PageId, page: &Page) -> Result<()> {
        let offset = page_offset(page_id)?;
        let _offset = self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(page.as_bytes())?;
        #[cfg(feature = "mmap")]
        {
            self.mmap_stale = true;
        }
        self.cache.insert(page_id, Arc::new(page.clone()));
        Ok(())
    }

    /// Flush the underlying file handle.
    pub(crate) fn flush(&mut self) -> Result<()> {
        self.file.flush()?;
        self.file.sync_data()?;
        self.refresh_mapping()?;
        Ok(())
    }

    fn ensure_page_in_bounds(&self, page_id: PageId) -> Result<()> {
        if page_id.get() < self.header.page_count {
            return Ok(());
        }

        Err(Error::Corrupted {
            offset: page_id.get().saturating_mul(PAGE_SIZE as u64),
            reason: "page id out of bounds",
        })
    }

    fn persist_header(&mut self) -> Result<()> {
        write_header_page(&mut self.file, self.header)?;
        #[cfg(feature = "mmap")]
        {
            self.mmap_stale = true;
        }
        self.refresh_mapping()
    }

    fn refresh_mapping(&mut self) -> Result<()> {
        #[cfg(feature = "mmap")]
        {
            if self.mmap.is_some() && self.mmap_stale {
                self.mmap = Some(MmapView::open(&self.file)?);
                self.mmap_stale = false;
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn cache_len(&self) -> usize {
        self.cache.len()
    }

    #[cfg(test)]
    fn path(&self) -> &std::path::Path {
        &self.path
    }
}

fn page_offset(page_id: PageId) -> Result<u64> {
    page_id
        .get()
        .checked_mul(PAGE_SIZE as u64)
        .ok_or(Error::Corrupted {
            offset: 0,
            reason: "page offset overflow",
        })
}

fn write_header_page(file: &mut File, header: PageFileHeader) -> Result<()> {
    let _offset = file.seek(SeekFrom::Start(0))?;
    file.write_all(&header.encode())?;
    Ok(())
}

fn read_header_page(file: &mut File) -> Result<PageFileHeader> {
    let _offset = file.seek(SeekFrom::Start(0))?;
    let mut bytes = [0_u8; PAGE_SIZE];
    file.read_exact(&mut bytes)?;
    PageFileHeader::decode(&bytes)
}

fn now_unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0_u64, |duration| duration.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::BufferedPager;
    use crate::storage::page::{Page, PageHeader, PageType, PAGE_HEADER_LEN};

    fn tmp_path(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0_u128, |duration| duration.as_nanos());
        path.push(format!("emdb-page-pager-{name}-{nanos}.emdb"));
        path
    }

    #[test]
    fn test_open_fresh_pager_writes_header_page() {
        let path = tmp_path("fresh");
        let pager = BufferedPager::open(&path, 7);
        assert!(pager.is_ok());
        let pager = match pager {
            Ok(pager) => pager,
            Err(err) => panic!("pager open should succeed: {err}"),
        };

        assert_eq!(pager.header().flags, 7);
        assert_eq!(pager.header().page_count, 1);
        assert_eq!(pager.path(), path.as_path());

        let _removed = std::fs::remove_file(path);
    }

    #[test]
    fn test_allocate_write_read_round_trip() {
        let path = tmp_path("roundtrip");
        let pager = BufferedPager::open(&path, 0);
        assert!(pager.is_ok());
        let mut pager = match pager {
            Ok(pager) => pager,
            Err(err) => panic!("pager open should succeed: {err}"),
        };

        let page_id = pager.allocate_page(PageType::BTreeNode);
        assert!(page_id.is_ok());
        let page_id = match page_id {
            Ok(page_id) => page_id,
            Err(err) => panic!("page allocation should succeed: {err}"),
        };

        let mut page = Page::new(PageHeader::new(PageType::BTreeNode));
        page.as_mut_bytes()[PAGE_HEADER_LEN..PAGE_HEADER_LEN + 5].copy_from_slice(b"btree");
        let refreshed = page.refresh_crc();
        assert!(refreshed.is_ok());
        let written = pager.write_page(page_id, &page);
        assert!(written.is_ok());
        let flushed = pager.flush();
        assert!(flushed.is_ok());

        let read_back = pager.read_page(page_id);
        assert!(read_back.is_ok());
        let read_back = match read_back {
            Ok(page) => page,
            Err(err) => panic!("page read should succeed: {err}"),
        };
        assert_eq!(
            read_back.as_bytes()[PAGE_HEADER_LEN..PAGE_HEADER_LEN + 5],
            *b"btree"
        );
        assert_eq!(pager.header().page_count, 2);

        let _removed = std::fs::remove_file(path);
    }

    #[test]
    fn test_page_cache_limits_resident_pages() {
        let path = tmp_path("cache");
        let pager = BufferedPager::open_with_capacity(&path, 0, 2);
        assert!(pager.is_ok());
        let mut pager = match pager {
            Ok(pager) => pager,
            Err(err) => panic!("pager open should succeed: {err}"),
        };

        let first = pager.allocate_page(PageType::ValueLeaf);
        assert!(first.is_ok());
        let second = pager.allocate_page(PageType::ValueLeaf);
        assert!(second.is_ok());
        let third = pager.allocate_page(PageType::ValueLeaf);
        assert!(third.is_ok());

        let first = match first {
            Ok(page_id) => page_id,
            Err(err) => panic!("first allocation should succeed: {err}"),
        };
        let second = match second {
            Ok(page_id) => page_id,
            Err(err) => panic!("second allocation should succeed: {err}"),
        };
        let third = match third {
            Ok(page_id) => page_id,
            Err(err) => panic!("third allocation should succeed: {err}"),
        };

        assert!(pager.read_page(first).is_ok());
        assert!(pager.read_page(second).is_ok());
        assert!(pager.read_page(third).is_ok());
        assert!(pager.cache_len() <= 2);

        let _removed = std::fs::remove_file(path);
    }
}
