// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Free-page chain management for the v0.6 page file format.

use crate::storage::page::pager::BufferedPager;
use crate::storage::page::{Page, PageHeader, PageId, PageType, PAGE_HEADER_LEN};
use crate::{Error, Result};

const NEXT_FREE_OFFSET: usize = PAGE_HEADER_LEN;
const NEXT_FREE_LEN: usize = 8;

/// Persistent linked list of reusable page ids.
pub(crate) struct FreeList<'a> {
    pager: &'a mut BufferedPager,
}

impl<'a> FreeList<'a> {
    /// Bind a free-list manager to a buffered pager.
    pub(crate) fn new(pager: &'a mut BufferedPager) -> Self {
        Self { pager }
    }

    /// Push a page onto the reusable-page chain.
    pub(crate) fn push(&mut self, page_id: PageId) -> Result<()> {
        let next = self.pager.free_list_head();
        let page = free_list_page(next)?;
        self.pager.write_page(page_id, &page)?;
        self.pager.set_free_list_head(page_id)
    }

    /// Pop the next reusable page id from the chain.
    pub(crate) fn pop(&mut self) -> Result<Option<PageId>> {
        let head = self.pager.free_list_head();
        if head.get() == 0 {
            return Ok(None);
        }

        let page = self.pager.read_page(head)?;
        let next = decode_next_free(page.as_ref())?;
        self.pager.set_free_list_head(next)?;
        Ok(Some(head))
    }
}

fn free_list_page(next: PageId) -> Result<Page> {
    let mut page = Page::new(PageHeader::new(PageType::FreeList));
    page.as_mut_bytes()[NEXT_FREE_OFFSET..NEXT_FREE_OFFSET + NEXT_FREE_LEN]
        .copy_from_slice(&next.get().to_le_bytes());
    let _crc = page.refresh_crc()?;
    Ok(page)
}

fn decode_next_free(page: &Page) -> Result<PageId> {
    let header = page.header()?;
    if header.page_type != PageType::FreeList {
        return Err(Error::Corrupted {
            offset: 0,
            reason: "free list page type mismatch",
        });
    }

    let mut raw = [0_u8; NEXT_FREE_LEN];
    raw.copy_from_slice(&page.as_bytes()[NEXT_FREE_OFFSET..NEXT_FREE_OFFSET + NEXT_FREE_LEN]);
    Ok(PageId::new(u64::from_le_bytes(raw)))
}

#[cfg(test)]
mod tests {
    use super::FreeList;
    use crate::storage::page::pager::BufferedPager;
    use crate::storage::page::PageType;

    fn tmp_path(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0_u128, |duration| duration.as_nanos());
        path.push(format!("emdb-free-list-{name}-{nanos}.emdb"));
        path
    }

    #[test]
    fn test_push_and_pop_free_pages() {
        let path = tmp_path("push-pop");
        let pager = BufferedPager::open(&path, 0);
        assert!(pager.is_ok());
        let mut pager = match pager {
            Ok(pager) => pager,
            Err(err) => panic!("pager open should succeed: {err}"),
        };

        let first = pager.allocate_page(PageType::ValueLeaf);
        assert!(first.is_ok());
        let second = pager.allocate_page(PageType::Overflow);
        assert!(second.is_ok());

        let first = match first {
            Ok(page_id) => page_id,
            Err(err) => panic!("first allocation should succeed: {err}"),
        };
        let second = match second {
            Ok(page_id) => page_id,
            Err(err) => panic!("second allocation should succeed: {err}"),
        };

        let mut free_list = FreeList::new(&mut pager);
        assert!(free_list.push(first).is_ok());
        assert!(free_list.push(second).is_ok());

        let popped = free_list.pop();
        assert!(matches!(popped, Ok(Some(page_id)) if page_id == second));
        let popped = free_list.pop();
        assert!(matches!(popped, Ok(Some(page_id)) if page_id == first));
        let popped = free_list.pop();
        assert!(matches!(popped, Ok(None)));

        let _removed = std::fs::remove_file(path);
    }

    #[test]
    fn test_free_chain_persists_across_reopen() {
        let path = tmp_path("persist");
        {
            let pager = BufferedPager::open(&path, 0);
            assert!(pager.is_ok());
            let mut pager = match pager {
                Ok(pager) => pager,
                Err(err) => panic!("pager open should succeed: {err}"),
            };
            let first = pager.allocate_page(PageType::ValueLeaf);
            assert!(first.is_ok());
            let second = pager.allocate_page(PageType::Overflow);
            assert!(second.is_ok());

            let first = match first {
                Ok(page_id) => page_id,
                Err(err) => panic!("first allocation should succeed: {err}"),
            };
            let second = match second {
                Ok(page_id) => page_id,
                Err(err) => panic!("second allocation should succeed: {err}"),
            };

            let mut free_list = FreeList::new(&mut pager);
            assert!(free_list.push(first).is_ok());
            assert!(free_list.push(second).is_ok());
            assert!(pager.flush().is_ok());
        }

        let pager = BufferedPager::open(&path, 0);
        assert!(pager.is_ok());
        let mut pager = match pager {
            Ok(pager) => pager,
            Err(err) => panic!("pager reopen should succeed: {err}"),
        };
        let mut free_list = FreeList::new(&mut pager);

        let popped = free_list.pop();
        assert!(popped.is_ok());
        assert!(popped.ok().flatten().is_some());
        let popped = free_list.pop();
        assert!(popped.is_ok());
        assert!(popped.ok().flatten().is_some());
        let popped = free_list.pop();
        assert!(matches!(popped, Ok(None)));

        let _removed = std::fs::remove_file(path);
    }
}
