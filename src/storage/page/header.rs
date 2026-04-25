// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Header-page encoding for the v0.6 page file format.

use crate::storage::page::{PageId, PAGE_SIZE};
use crate::{Error, Result};

const HEADER_CRC_COVERAGE_END: usize = 100;
const HEADER_CRC_OFFSET: usize = 100;
const PAGE_FILE_MAGIC: &[u8; 8] = b"EMDBPAGE";

/// Page-file format version introduced in v0.6.
pub(crate) const PAGE_FORMAT_VERSION: u32 = 3;

/// Logical contents of page zero in the v0.6 page file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PageFileHeader {
    /// Feature bits stored in the page file.
    pub(crate) flags: u32,
    /// Fixed page size expected by this build.
    pub(crate) page_size: u32,
    /// Creation timestamp in unix milliseconds.
    pub(crate) created_at: u64,
    /// Highest committed transaction id.
    pub(crate) last_tx_id: u64,
    /// Number of allocated pages in the page file.
    pub(crate) page_count: u64,
    /// Root page id for the B-tree index.
    pub(crate) root_page_id: PageId,
    /// Head of the free-page chain.
    pub(crate) free_list_head: PageId,
    /// Bump pointer for new value-page allocation.
    pub(crate) value_alloc_head: PageId,
}

impl Default for PageFileHeader {
    fn default() -> Self {
        Self {
            flags: 0,
            page_size: PAGE_SIZE as u32,
            created_at: 0,
            last_tx_id: 0,
            page_count: 1,
            root_page_id: PageId::new(0),
            free_list_head: PageId::new(0),
            value_alloc_head: PageId::new(1),
        }
    }
}

impl PageFileHeader {
    /// Encode the header into a full page-zero image with CRC protection.
    #[must_use]
    pub(crate) fn encode(self) -> [u8; PAGE_SIZE] {
        let mut bytes = [0_u8; PAGE_SIZE];
        bytes[0..8].copy_from_slice(PAGE_FILE_MAGIC);
        bytes[8..12].copy_from_slice(&PAGE_FORMAT_VERSION.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.flags.to_le_bytes());
        bytes[16..20].copy_from_slice(&self.page_size.to_le_bytes());
        bytes[20..28].copy_from_slice(&self.created_at.to_le_bytes());
        bytes[28..36].copy_from_slice(&self.last_tx_id.to_le_bytes());
        bytes[36..44].copy_from_slice(&self.page_count.to_le_bytes());
        bytes[44..52].copy_from_slice(&self.root_page_id.get().to_le_bytes());
        bytes[52..60].copy_from_slice(&self.free_list_head.get().to_le_bytes());
        bytes[60..68].copy_from_slice(&self.value_alloc_head.get().to_le_bytes());
        let crc = header_crc(&bytes);
        bytes[HEADER_CRC_OFFSET..HEADER_CRC_OFFSET + 4].copy_from_slice(&crc.to_le_bytes());
        bytes
    }

    /// Decode and validate a header page image.
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < PAGE_SIZE {
            return Err(Error::Corrupted {
                offset: 0,
                reason: "page header truncated",
            });
        }
        if &bytes[0..8] != PAGE_FILE_MAGIC {
            return Err(Error::MagicMismatch);
        }

        let format_ver = read_u32(bytes, 8);
        if format_ver != PAGE_FORMAT_VERSION {
            return Err(Error::VersionMismatch {
                found: format_ver,
                expected: PAGE_FORMAT_VERSION,
            });
        }

        let expected_crc = read_u32(bytes, HEADER_CRC_OFFSET);
        let actual_crc = header_crc_prefix(bytes);
        if expected_crc != actual_crc {
            return Err(Error::Corrupted {
                offset: HEADER_CRC_OFFSET as u64,
                reason: "page header crc mismatch",
            });
        }

        let page_size = read_u32(bytes, 16);
        if page_size != PAGE_SIZE as u32 {
            return Err(Error::Corrupted {
                offset: 16,
                reason: "page size mismatch",
            });
        }

        Ok(Self {
            flags: read_u32(bytes, 12),
            page_size,
            created_at: read_u64(bytes, 20),
            last_tx_id: read_u64(bytes, 28),
            page_count: read_u64(bytes, 36),
            root_page_id: PageId::new(read_u64(bytes, 44)),
            free_list_head: PageId::new(read_u64(bytes, 52)),
            value_alloc_head: PageId::new(read_u64(bytes, 60)),
        })
    }
}

fn header_crc(bytes: &[u8; PAGE_SIZE]) -> u32 {
    header_crc_prefix(bytes)
}

fn header_crc_prefix(bytes: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&bytes[0..HEADER_CRC_COVERAGE_END]);
    hasher.finalize()
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    let mut raw = [0_u8; 4];
    raw.copy_from_slice(&bytes[offset..offset + 4]);
    u32::from_le_bytes(raw)
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    let mut raw = [0_u8; 8];
    raw.copy_from_slice(&bytes[offset..offset + 8]);
    u64::from_le_bytes(raw)
}

#[cfg(test)]
mod tests {
    use super::{PageFileHeader, PAGE_FORMAT_VERSION};
    use crate::storage::page::{PageId, PAGE_SIZE};
    use crate::Error;

    #[test]
    fn test_write_fresh_header_and_read_back() {
        let header = PageFileHeader {
            flags: 3,
            page_size: PAGE_SIZE as u32,
            created_at: 11,
            last_tx_id: 19,
            page_count: 23,
            root_page_id: PageId::new(29),
            free_list_head: PageId::new(31),
            value_alloc_head: PageId::new(37),
        };

        let encoded = header.encode();
        let decoded = PageFileHeader::decode(&encoded);
        assert!(matches!(decoded, Ok(value) if value == header));
    }

    #[test]
    fn test_mutate_header_and_reread() {
        let mut header = PageFileHeader::default();
        let encoded = header.encode();
        let decoded = PageFileHeader::decode(&encoded);
        assert!(matches!(decoded, Ok(value) if value == header));

        header.last_tx_id = 77;
        header.page_count = 101;
        header.root_page_id = PageId::new(5);
        let encoded = header.encode();
        let decoded = PageFileHeader::decode(&encoded);
        assert!(matches!(decoded, Ok(value) if value == header));
    }

    #[test]
    fn test_header_crc_validation_rejects_corruption() {
        let header = PageFileHeader::default();
        let mut encoded = header.encode();
        encoded[20] ^= 0xFF;

        let decoded = PageFileHeader::decode(&encoded);
        assert!(matches!(
            decoded,
            Err(Error::Corrupted {
                reason: "page header crc mismatch",
                ..
            })
        ));
    }

    #[test]
    fn test_header_version_validation_rejects_mismatch() {
        let header = PageFileHeader::default();
        let mut encoded = header.encode();
        let wrong_version = (PAGE_FORMAT_VERSION + 1).to_le_bytes();
        encoded[8..12].copy_from_slice(&wrong_version);
        let crc = crc32fast::hash(&encoded[0..100]);
        encoded[100..104].copy_from_slice(&crc.to_le_bytes());

        let decoded = PageFileHeader::decode(&encoded);
        assert!(matches!(
            decoded,
            Err(Error::VersionMismatch {
                found,
                expected,
            }) if found == PAGE_FORMAT_VERSION + 1 && expected == PAGE_FORMAT_VERSION
        ));
    }

    #[test]
    fn test_header_magic_validation_rejects_non_page_file() {
        let mut encoded = PageFileHeader::default().encode();
        encoded[0] = b'X';

        let decoded = PageFileHeader::decode(&encoded);
        assert!(matches!(decoded, Err(Error::MagicMismatch)));
    }
}
