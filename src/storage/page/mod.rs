// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Page-format primitives for the v0.6 storage engine.

use crate::{Error, Result};

#[allow(dead_code)]
pub(crate) mod btree;
#[allow(dead_code)]
pub(crate) mod free_list;
#[allow(dead_code)]
pub(crate) mod header;
#[cfg(feature = "mmap")]
#[allow(dead_code)]
pub(crate) mod mmap;
#[allow(dead_code)]
pub(crate) mod pager;
#[allow(dead_code)]
pub(crate) mod rid;
#[allow(dead_code)]
pub(crate) mod slotted;
#[allow(dead_code)]
pub(crate) mod value;

/// Fixed page size used by the v0.6 page file format.
pub(crate) const PAGE_SIZE: usize = 4096;

/// Size in bytes of every non-header page prefix.
pub(crate) const PAGE_HEADER_LEN: usize = 16;

/// Stable identifier for a page in the page file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub(crate) struct PageId(u64);

impl PageId {
    /// Create a page identifier from its raw integer value.
    #[must_use]
    pub(crate) const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// Return the raw integer page identifier.
    #[must_use]
    pub(crate) const fn get(self) -> u64 {
        self.0
    }
}

/// Discriminant for the page layout stored in a page buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum PageType {
    /// Header page at page id zero.
    Header = 0,
    /// B-tree index node page.
    BTreeNode = 1,
    /// Value leaf page (v0.6 one-page-per-value format).
    ValueLeaf = 2,
    /// Free-list bookkeeping page.
    FreeList = 3,
    /// Overflow page for large values.
    Overflow = 4,
    /// Slotted leaf page (v0.7 packed-leaf format).
    LeafSlotted = 5,
}

impl PageType {
    /// Decode a page type from its on-disk discriminant.
    fn from_u8(raw: u8) -> Result<Self> {
        match raw {
            0 => Ok(Self::Header),
            1 => Ok(Self::BTreeNode),
            2 => Ok(Self::ValueLeaf),
            3 => Ok(Self::FreeList),
            4 => Ok(Self::Overflow),
            5 => Ok(Self::LeafSlotted),
            _ => Err(Error::Corrupted {
                offset: 0,
                reason: "invalid page type",
            }),
        }
    }
}

/// Common prefix present in every non-header page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PageHeader {
    /// Page payload kind.
    pub(crate) page_type: PageType,
    /// Type-specific flags.
    pub(crate) flags: u8,
    /// Last log sequence number that touched the page.
    pub(crate) lsn: u64,
    /// CRC32 over the bytes after the page header.
    pub(crate) page_crc: u32,
}

impl PageHeader {
    /// Construct a fresh header with zeroed sequence number and CRC.
    #[must_use]
    pub(crate) const fn new(page_type: PageType) -> Self {
        Self {
            page_type,
            flags: 0,
            lsn: 0,
            page_crc: 0,
        }
    }

    /// Encode this header into the fixed-size page header prefix.
    fn encode_into(self, out: &mut [u8]) {
        debug_assert!(out.len() >= PAGE_HEADER_LEN);
        out[..PAGE_HEADER_LEN].fill(0);
        out[0] = self.page_type as u8;
        out[1] = self.flags;
        out[4..12].copy_from_slice(&self.lsn.to_le_bytes());
        out[12..16].copy_from_slice(&self.page_crc.to_le_bytes());
    }

    /// Decode a header from the fixed-size page prefix.
    fn decode_from(input: &[u8]) -> Result<Self> {
        if input.len() < PAGE_HEADER_LEN {
            return Err(Error::Corrupted {
                offset: 0,
                reason: "page header truncated",
            });
        }

        let mut lsn = [0_u8; 8];
        lsn.copy_from_slice(&input[4..12]);

        let mut crc = [0_u8; 4];
        crc.copy_from_slice(&input[12..16]);

        Ok(Self {
            page_type: PageType::from_u8(input[0])?,
            flags: input[1],
            lsn: u64::from_le_bytes(lsn),
            page_crc: u32::from_le_bytes(crc),
        })
    }
}

/// Aligned in-memory page buffer.
#[derive(Debug, Clone)]
#[repr(C, align(4096))]
pub(crate) struct Page {
    bytes: [u8; PAGE_SIZE],
}

impl Page {
    /// Allocate a zeroed page with the supplied header.
    #[must_use]
    pub(crate) fn new(header: PageHeader) -> Self {
        let mut page = Self {
            bytes: [0_u8; PAGE_SIZE],
        };
        page.set_header(header);
        page
    }

    /// Reconstruct a page from an existing raw page buffer.
    #[must_use]
    pub(crate) fn from_bytes(bytes: [u8; PAGE_SIZE]) -> Self {
        Self { bytes }
    }

    /// Return the decoded page header.
    pub(crate) fn header(&self) -> Result<PageHeader> {
        PageHeader::decode_from(&self.bytes[..PAGE_HEADER_LEN])
    }

    /// Overwrite the page header prefix.
    pub(crate) fn set_header(&mut self, header: PageHeader) {
        header.encode_into(&mut self.bytes[..PAGE_HEADER_LEN]);
    }

    /// Borrow the raw page bytes.
    #[must_use]
    pub(crate) fn as_bytes(&self) -> &[u8; PAGE_SIZE] {
        &self.bytes
    }

    /// Borrow the raw page bytes mutably.
    #[must_use]
    pub(crate) fn as_mut_bytes(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.bytes
    }

    /// Recompute and store the payload CRC in the page header.
    pub(crate) fn refresh_crc(&mut self) -> Result<u32> {
        let mut header = self.header()?;
        header.page_crc = page_crc(&self.bytes);
        self.set_header(header);
        Ok(header.page_crc)
    }

    /// Validate the stored payload CRC against the current payload bytes.
    pub(crate) fn validate_crc(&self) -> Result<()> {
        let header = self.header()?;
        let actual = page_crc(&self.bytes);
        if header.page_crc == actual {
            return Ok(());
        }

        Err(Error::Corrupted {
            offset: 0,
            reason: "page crc mismatch",
        })
    }
}

/// Compute the payload CRC for a page buffer.
#[must_use]
pub(crate) fn page_crc(bytes: &[u8; PAGE_SIZE]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&bytes[PAGE_HEADER_LEN..]);
    hasher.finalize()
}

#[cfg(test)]
mod tests {
    use core::mem::{align_of, size_of};

    use super::{page_crc, Page, PageHeader, PageId, PageType, PAGE_HEADER_LEN, PAGE_SIZE};
    use crate::Error;

    #[test]
    fn test_page_id_round_trip() {
        let page_id = PageId::new(42);
        assert_eq!(page_id.get(), 42);
    }

    #[test]
    fn test_page_type_round_trip() {
        let variants = [
            (PageType::Header, 0_u8),
            (PageType::BTreeNode, 1_u8),
            (PageType::ValueLeaf, 2_u8),
            (PageType::FreeList, 3_u8),
            (PageType::Overflow, 4_u8),
        ];

        for (page_type, raw) in variants {
            let header = PageHeader {
                page_type,
                flags: 7,
                lsn: 11,
                page_crc: 13,
            };
            let mut encoded = [0_u8; PAGE_HEADER_LEN];
            header.encode_into(&mut encoded);
            assert_eq!(encoded[0], raw);
            let decoded = PageHeader::decode_from(&encoded);
            assert!(decoded.is_ok());
            assert!(matches!(
                decoded,
                Ok(PageHeader {
                    page_type: value,
                    ..
                }) if value == page_type
            ));
        }
    }

    #[test]
    fn test_page_header_round_trip() {
        let header = PageHeader {
            page_type: PageType::BTreeNode,
            flags: 0xA5,
            lsn: 0x0102_0304_0506_0708,
            page_crc: 0x1122_3344,
        };

        let mut encoded = [0_u8; PAGE_HEADER_LEN];
        header.encode_into(&mut encoded);
        let decoded = PageHeader::decode_from(&encoded);
        assert!(matches!(decoded, Ok(value) if value == header));
    }

    #[test]
    fn test_page_round_trip_with_crc() {
        let mut page = Page::new(PageHeader::new(PageType::ValueLeaf));
        page.as_mut_bytes()[PAGE_HEADER_LEN..PAGE_HEADER_LEN + 5].copy_from_slice(b"value");

        let refreshed = page.refresh_crc();
        assert!(refreshed.is_ok());
        let refreshed = match refreshed {
            Ok(value) => value,
            Err(err) => panic!("page crc refresh should succeed: {err}"),
        };
        assert!(page.validate_crc().is_ok());

        let header = page.header();
        assert!(header.is_ok());
        assert!(matches!(
            header,
            Ok(PageHeader {
                page_type: PageType::ValueLeaf,
                page_crc,
                ..
            }) if page_crc == refreshed
        ));
    }

    #[test]
    fn test_page_crc_detects_payload_corruption() {
        let mut page = Page::new(PageHeader::new(PageType::Overflow));
        page.as_mut_bytes()[PAGE_HEADER_LEN] = 9;
        let refreshed = page.refresh_crc();
        assert!(refreshed.is_ok());

        page.as_mut_bytes()[PAGE_HEADER_LEN] ^= 0xFF;
        let validated = page.validate_crc();
        assert!(matches!(
            validated,
            Err(Error::Corrupted {
                reason: "page crc mismatch",
                ..
            })
        ));
    }

    #[test]
    fn test_page_alignment_and_size() {
        assert_eq!(size_of::<Page>(), PAGE_SIZE);
        assert_eq!(align_of::<Page>(), PAGE_SIZE);
        let empty = [0_u8; PAGE_SIZE];
        assert_eq!(page_crc(&empty), crc32fast::hash(&empty[PAGE_HEADER_LEN..]));
    }
}
