// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Optional memory-mapped read path for page files.

use std::fs::File;

use memmap2::{Mmap, MmapOptions};

use crate::storage::page::PAGE_SIZE;
use crate::{Error, Result};

/// Read-only memory-mapped view over the page file.
#[derive(Debug)]
pub(crate) struct MmapView {
    map: Mmap,
}

impl MmapView {
    /// Create a new read-only mapping for the entire page file.
    pub(crate) fn open(file: &File) -> Result<Self> {
        let file_len = file.metadata()?.len();
        if file_len < PAGE_SIZE as u64 {
            return Err(Error::Corrupted {
                offset: 0,
                reason: "page file too small for mmap",
            });
        }

        let map = {
            // SAFETY: The mapping is read-only and confined to the current file handle.
            // The pager remaps after every size-changing or content-changing write before
            // serving mmap-backed reads again, so no stale pointer is retained across
            // mutation boundaries.
            unsafe { MmapOptions::new().map(file)? }
        };
        Ok(Self { map })
    }

    /// Read one full page image from the current mapping.
    pub(crate) fn read_page(&self, offset: usize) -> Result<[u8; PAGE_SIZE]> {
        let end = offset.checked_add(PAGE_SIZE).ok_or(Error::Corrupted {
            offset: offset as u64,
            reason: "page offset overflow",
        })?;
        if end > self.map.len() {
            return Err(Error::Corrupted {
                offset: offset as u64,
                reason: "mmap page read out of bounds",
            });
        }

        let mut bytes = [0_u8; PAGE_SIZE];
        bytes.copy_from_slice(&self.map[offset..end]);
        Ok(bytes)
    }
}
