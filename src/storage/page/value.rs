// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Value-page encoding and overflow chaining for the v0.6 storage engine.

use crate::storage::page::free_list::FreeList;
use crate::storage::page::pager::BufferedPager;
use crate::storage::page::{Page, PageHeader, PageId, PageType, PAGE_HEADER_LEN, PAGE_SIZE};
use crate::{Error, Result};

const VALUE_LEN_OFFSET: usize = PAGE_HEADER_LEN;
const EXPIRES_AT_OFFSET: usize = VALUE_LEN_OFFSET + 4;
const OVERFLOW_HEAD_OFFSET: usize = EXPIRES_AT_OFFSET + 8;
const CHUNK_LEN_OFFSET: usize = OVERFLOW_HEAD_OFFSET + 8;
const VALUE_CHUNK_OFFSET: usize = CHUNK_LEN_OFFSET + 4;
const INLINE_CAPACITY: usize = PAGE_SIZE - VALUE_CHUNK_OFFSET;

const OVERFLOW_NEXT_OFFSET: usize = PAGE_HEADER_LEN;
const OVERFLOW_CHUNK_LEN_OFFSET: usize = OVERFLOW_NEXT_OFFSET + 8;
const OVERFLOW_CHUNK_OFFSET: usize = OVERFLOW_CHUNK_LEN_OFFSET + 4;
const OVERFLOW_CAPACITY: usize = PAGE_SIZE - OVERFLOW_CHUNK_OFFSET;

/// Stable reference to a value stored in the page file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ValueRef {
    /// Head ValueLeaf page id.
    pub(crate) head: PageId,
}

impl ValueRef {
    /// Return the head page id for this value.
    #[must_use]
    pub(crate) fn head(self) -> PageId {
        self.head
    }
}

/// Write a value into one ValueLeaf page and optional Overflow pages.
pub(crate) fn write_value(
    pager: &mut BufferedPager,
    value: &[u8],
    expires_at: Option<u64>,
) -> Result<ValueRef> {
    let head = allocate_storage_page(pager, PageType::ValueLeaf)?;

    let inline_len = value.len().min(INLINE_CAPACITY);
    let mut leaf = Page::new(PageHeader::new(PageType::ValueLeaf));
    leaf.as_mut_bytes()[VALUE_LEN_OFFSET..VALUE_LEN_OFFSET + 4]
        .copy_from_slice(&(value.len() as u32).to_le_bytes());
    leaf.as_mut_bytes()[EXPIRES_AT_OFFSET..EXPIRES_AT_OFFSET + 8]
        .copy_from_slice(&expires_at.unwrap_or(0).to_le_bytes());
    leaf.as_mut_bytes()[CHUNK_LEN_OFFSET..CHUNK_LEN_OFFSET + 4]
        .copy_from_slice(&(inline_len as u32).to_le_bytes());
    leaf.as_mut_bytes()[VALUE_CHUNK_OFFSET..VALUE_CHUNK_OFFSET + inline_len]
        .copy_from_slice(&value[..inline_len]);

    let overflow_head = if value.len() > inline_len {
        let overflow = write_overflow_chain(pager, &value[inline_len..])?;
        overflow.get()
    } else {
        0
    };
    leaf.as_mut_bytes()[OVERFLOW_HEAD_OFFSET..OVERFLOW_HEAD_OFFSET + 8]
        .copy_from_slice(&overflow_head.to_le_bytes());
    let _crc = leaf.refresh_crc()?;
    pager.write_page(head, &leaf)?;

    Ok(ValueRef { head })
}

/// Read a value and its TTL metadata back from the page file.
pub(crate) fn read_value(
    pager: &mut BufferedPager,
    value_ref: ValueRef,
) -> Result<(Vec<u8>, Option<u64>)> {
    let leaf = pager.read_page(value_ref.head)?;
    let header = leaf.header()?;
    if header.page_type != PageType::ValueLeaf {
        return Err(Error::Corrupted {
            offset: 0,
            reason: "value leaf page type mismatch",
        });
    }

    let total_len = read_u32(leaf.as_bytes(), VALUE_LEN_OFFSET) as usize;
    let expires_at = match read_u64(leaf.as_bytes(), EXPIRES_AT_OFFSET) {
        0 => None,
        value => Some(value),
    };
    let inline_len = read_u32(leaf.as_bytes(), CHUNK_LEN_OFFSET) as usize;
    if inline_len > INLINE_CAPACITY || inline_len > total_len {
        return Err(Error::Corrupted {
            offset: 0,
            reason: "value leaf inline length invalid",
        });
    }

    let mut value = Vec::with_capacity(total_len);
    value.extend_from_slice(&leaf.as_bytes()[VALUE_CHUNK_OFFSET..VALUE_CHUNK_OFFSET + inline_len]);

    let mut next = PageId::new(read_u64(leaf.as_bytes(), OVERFLOW_HEAD_OFFSET));
    while next.get() != 0 {
        let page = pager.read_page(next)?;
        let header = page.header()?;
        if header.page_type != PageType::Overflow {
            return Err(Error::Corrupted {
                offset: 0,
                reason: "overflow page type mismatch",
            });
        }

        let chunk_len = read_u32(page.as_bytes(), OVERFLOW_CHUNK_LEN_OFFSET) as usize;
        if chunk_len > OVERFLOW_CAPACITY {
            return Err(Error::Corrupted {
                offset: 0,
                reason: "overflow chunk length invalid",
            });
        }

        value.extend_from_slice(
            &page.as_bytes()[OVERFLOW_CHUNK_OFFSET..OVERFLOW_CHUNK_OFFSET + chunk_len],
        );
        next = PageId::new(read_u64(page.as_bytes(), OVERFLOW_NEXT_OFFSET));
    }

    if value.len() != total_len {
        return Err(Error::Corrupted {
            offset: 0,
            reason: "value length mismatch",
        });
    }

    Ok((value, expires_at))
}

/// Return all pages owned by a value back to the free list.
pub(crate) fn free_value(pager: &mut BufferedPager, value_ref: ValueRef) -> Result<()> {
    let leaf = pager.read_page(value_ref.head)?;
    let header = leaf.header()?;
    if header.page_type != PageType::ValueLeaf {
        return Err(Error::Corrupted {
            offset: 0,
            reason: "value leaf page type mismatch",
        });
    }

    let mut next = PageId::new(read_u64(leaf.as_bytes(), OVERFLOW_HEAD_OFFSET));
    while next.get() != 0 {
        let page = pager.read_page(next)?;
        let page_header = page.header()?;
        if page_header.page_type != PageType::Overflow {
            return Err(Error::Corrupted {
                offset: 0,
                reason: "overflow page type mismatch",
            });
        }
        let current = next;
        next = PageId::new(read_u64(page.as_bytes(), OVERFLOW_NEXT_OFFSET));
        let mut free_list = FreeList::new(pager);
        free_list.push(current)?;
    }

    let mut free_list = FreeList::new(pager);
    free_list.push(value_ref.head)
}

fn write_overflow_chain(pager: &mut BufferedPager, remaining: &[u8]) -> Result<PageId> {
    let mut chunks = remaining.chunks(OVERFLOW_CAPACITY).collect::<Vec<_>>();
    chunks.reverse();

    let mut next = PageId::new(0);
    for chunk in chunks {
        let page_id = allocate_storage_page(pager, PageType::Overflow)?;
        let mut page = Page::new(PageHeader::new(PageType::Overflow));
        page.as_mut_bytes()[OVERFLOW_NEXT_OFFSET..OVERFLOW_NEXT_OFFSET + 8]
            .copy_from_slice(&next.get().to_le_bytes());
        page.as_mut_bytes()[OVERFLOW_CHUNK_LEN_OFFSET..OVERFLOW_CHUNK_LEN_OFFSET + 4]
            .copy_from_slice(&(chunk.len() as u32).to_le_bytes());
        page.as_mut_bytes()[OVERFLOW_CHUNK_OFFSET..OVERFLOW_CHUNK_OFFSET + chunk.len()]
            .copy_from_slice(chunk);
        let _crc = page.refresh_crc()?;
        pager.write_page(page_id, &page)?;
        next = page_id;
    }

    Ok(next)
}

fn allocate_storage_page(pager: &mut BufferedPager, page_type: PageType) -> Result<PageId> {
    let reused = {
        let mut free_list = FreeList::new(pager);
        free_list.pop()?
    };
    match reused {
        Some(page_id) => Ok(page_id),
        None => pager.allocate_page(page_type),
    }
}

fn read_u32(bytes: &[u8; PAGE_SIZE], offset: usize) -> u32 {
    let mut raw = [0_u8; 4];
    raw.copy_from_slice(&bytes[offset..offset + 4]);
    u32::from_le_bytes(raw)
}

fn read_u64(bytes: &[u8; PAGE_SIZE], offset: usize) -> u64 {
    let mut raw = [0_u8; 8];
    raw.copy_from_slice(&bytes[offset..offset + 8]);
    u64::from_le_bytes(raw)
}

#[cfg(test)]
mod tests {
    use super::{read_value, write_value};
    use crate::storage::page::pager::BufferedPager;

    fn tmp_path(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0_u128, |duration| duration.as_nanos());
        path.push(format!("emdb-value-page-{name}-{nanos}.emdb"));
        path
    }

    #[test]
    fn test_inline_value_round_trip() {
        let path = tmp_path("inline");
        let pager = BufferedPager::open(&path, 0);
        assert!(pager.is_ok());
        let mut pager = match pager {
            Ok(pager) => pager,
            Err(err) => panic!("pager open should succeed: {err}"),
        };

        let value_ref = write_value(&mut pager, b"hello world", Some(99));
        assert!(value_ref.is_ok());
        let value_ref = match value_ref {
            Ok(value_ref) => value_ref,
            Err(err) => panic!("inline write should succeed: {err}"),
        };

        let read_back = read_value(&mut pager, value_ref);
        assert!(
            matches!(read_back, Ok((value, expires_at)) if value == b"hello world" && expires_at == Some(99))
        );

        let _removed = std::fs::remove_file(path);
    }

    #[test]
    fn test_overflow_value_round_trip() {
        let path = tmp_path("overflow");
        let pager = BufferedPager::open(&path, 0);
        assert!(pager.is_ok());
        let mut pager = match pager {
            Ok(pager) => pager,
            Err(err) => panic!("pager open should succeed: {err}"),
        };

        let value = vec![42_u8; 10_000];
        let value_ref = write_value(&mut pager, &value, None);
        assert!(value_ref.is_ok());
        let value_ref = match value_ref {
            Ok(value_ref) => value_ref,
            Err(err) => panic!("overflow write should succeed: {err}"),
        };

        let read_back = read_value(&mut pager, value_ref);
        assert!(
            matches!(read_back, Ok((read_value, expires_at)) if read_value == value && expires_at.is_none())
        );
        assert!(pager.header().page_count > 2);

        let _removed = std::fs::remove_file(path);
    }
}
