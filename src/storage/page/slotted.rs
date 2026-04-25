// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Slotted-page leaf format for the v0.7 storage engine.
//!
//! A slotted page packs many `(key, value)` records into a single 4 KB page.
//! Slots grow upward from the start of the page; record bodies grow downward
//! from the end. The free space between them is the page's remaining capacity.
//!
//! Layout (offsets are byte offsets within the 4 KB page):
//!
//! ```text
//!   0..16   PageHeader (page_type=LeafSlotted, lsn, page_crc — common prefix)
//!  16..24   next_leaf      u64 LE   page id of next leaf in chain (0 = last)
//!  24..28   slot_count     u32 LE   number of slot entries (live + tombstone)
//!  28..32   record_floor   u32 LE   lowest byte offset where a record begins
//!  32..40   reserved       u64 LE   zeroed; future use
//!  40..N    slot_array     8 bytes per slot, growing toward higher offsets
//!  N..F     free space
//!  F..4096  record bodies, growing toward lower offsets
//! ```
//!
//! ## Slot entry (8 bytes)
//!
//! ```text
//!   0..2   record_offset   u16 LE   offset within page where the record begins
//!   2..4   record_length   u16 LE   record body length, bytes
//!   4..5   flags           u8       slot kind: INLINE | OVERFLOW | TOMBSTONE
//!   5..8   reserved        u8 × 3   zeroed
//! ```
//!
//! ## Record body (INLINE)
//!
//! ```text
//!   key_len      u32 LE
//!   key          [u8; key_len]
//!   expires_at   u64 LE   0 = no expiry; always present (ttl-feature-agnostic)
//!   value_len    u32 LE
//!   value        [u8; value_len]
//! ```
//!
//! ## Record body (OVERFLOW)
//!
//! ```text
//!   key_len        u32 LE
//!   key            [u8; key_len]
//!   expires_at     u64 LE
//!   value_len      u32 LE   total value length across the overflow chain
//!   overflow_head  u64 LE   page id of first OverflowPage
//! ```
//!
//! Live records are addressable by slot id; tombstones survive in the slot
//! array until the next compaction so concurrent readers holding a stale
//! [`crate::storage::page::rid::Rid`] reliably see "not present" rather than
//! a different record at the same slot.

use crate::storage::page::rid::MAX_PAGE_ID;
use crate::storage::page::{
    page_crc, Page, PageHeader, PageId, PageType, PAGE_HEADER_LEN, PAGE_SIZE,
};
use crate::{Error, Result};

/// Slot flag: live record stored inline in this leaf page.
pub(crate) const FLAG_INLINE: u8 = 0;
/// Slot flag: live record whose value is stored in a chain of overflow pages.
pub(crate) const FLAG_OVERFLOW: u8 = 1;
/// Slot flag: deleted slot, retained until the next page compaction.
pub(crate) const FLAG_TOMBSTONE: u8 = 2;

/// Byte offset of the next-leaf pointer relative to the page start.
const NEXT_LEAF_OFFSET: usize = PAGE_HEADER_LEN;
/// Byte offset of the slot-count field.
const SLOT_COUNT_OFFSET: usize = NEXT_LEAF_OFFSET + 8;
/// Byte offset of the record-floor field.
const RECORD_FLOOR_OFFSET: usize = SLOT_COUNT_OFFSET + 4;
/// Byte offset of the reserved 8 bytes.
const RESERVED_OFFSET: usize = RECORD_FLOOR_OFFSET + 4;
/// Byte offset where the slot array begins.
const SLOT_ARRAY_OFFSET: usize = RESERVED_OFFSET + 8;
/// Byte length of a single slot entry.
const SLOT_ENTRY_LEN: usize = 8;
/// Maximum slot count addressable in a single page.
///
/// Capped by both the `record_floor` field and the practical limit of the
/// 16-bit slot id used inside [`crate::storage::page::rid::Rid`]. We use
/// the smaller of the two.
pub(crate) const MAX_SLOTS_PER_PAGE: usize = (PAGE_SIZE - SLOT_ARRAY_OFFSET) / SLOT_ENTRY_LEN;
/// Inline-record header size (`key_len` + `expires_at` + `value_len`).
const INLINE_RECORD_FIXED: usize = 4 + 8 + 4;
/// Overflow-record fixed footer size (`expires_at` + `value_len` + `overflow_head`).
const OVERFLOW_RECORD_FIXED: usize = 8 + 4 + 8;
/// Length of the `key_len` field.
const KEY_LEN_FIELD: usize = 4;

/// Decoded slot entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Slot {
    /// Byte offset within the page where the record body begins.
    pub(crate) record_offset: u16,
    /// Length of the record body.
    pub(crate) record_length: u16,
    /// Slot flags ([`FLAG_INLINE`] | [`FLAG_OVERFLOW`] | [`FLAG_TOMBSTONE`]).
    pub(crate) flags: u8,
}

impl Slot {
    /// Return true when the slot is live (not a tombstone).
    #[must_use]
    pub(crate) const fn is_live(self) -> bool {
        self.flags != FLAG_TOMBSTONE
    }

    /// Return true when the slot stores its value inline.
    #[must_use]
    pub(crate) const fn is_inline(self) -> bool {
        self.flags == FLAG_INLINE
    }

    /// Return true when the slot delegates to an overflow chain.
    #[must_use]
    pub(crate) const fn is_overflow(self) -> bool {
        self.flags == FLAG_OVERFLOW
    }
}

/// View of a record decoded from a slot.
///
/// Lifetimes are tied to the underlying page bytes; copies are pushed up to
/// callers when ownership is required (for example, when promoting a record
/// to the value cache).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecordView<'a> {
    /// Record stored entirely within the leaf page.
    Inline {
        /// Key bytes.
        key: &'a [u8],
        /// Value bytes.
        value: &'a [u8],
        /// Unix-millis expiry timestamp (0 = no expiry).
        expires_at: u64,
    },
    /// Record whose value lives in an overflow chain.
    Overflow {
        /// Key bytes.
        key: &'a [u8],
        /// Total value length across the overflow chain.
        value_len: u32,
        /// Page id of the first overflow page.
        overflow_head: u64,
        /// Unix-millis expiry timestamp (0 = no expiry).
        expires_at: u64,
    },
}

impl RecordView<'_> {
    /// Return the record's key bytes.
    #[must_use]
    pub(crate) fn key(&self) -> &[u8] {
        match self {
            Self::Inline { key, .. } | Self::Overflow { key, .. } => key,
        }
    }

    /// Return the unix-millis expiry timestamp (0 = no expiry).
    #[must_use]
    pub(crate) fn expires_at(&self) -> u64 {
        match self {
            Self::Inline { expires_at, .. } | Self::Overflow { expires_at, .. } => *expires_at,
        }
    }
}

/// Reasons an insert may fail without indicating corruption.
///
/// Distinguished from [`Error`] so the caller can choose to split the page
/// (capacity exhausted) or abort the write (key/value too large to ever fit).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InsertError {
    /// The page does not currently have room for this record. The caller may
    /// retry after a split or compaction.
    OutOfSpace,
    /// The key alone exceeds the maximum that can ever fit in a single page,
    /// even on a fresh page. Callers should reject the insert at the API.
    KeyTooLarge,
}

/// Read a slot entry from a leaf page held behind an immutable reference.
///
/// Equivalent to [`LeafPage::read_slot`] but bypasses the `&mut Page`
/// requirement. Used by the engine read path which holds pages as
/// `Arc<Page>` and never mutates them in place.
///
/// # Errors
///
/// Returns [`Error::Corrupted`] when `slot_id` is out of range.
pub(crate) fn read_slot_at(page: &Page, slot_id: u16) -> Result<Slot> {
    let bytes = page.as_bytes();
    let count = read_u32(bytes, SLOT_COUNT_OFFSET);
    if u32::from(slot_id) >= count {
        return Err(Error::Corrupted {
            offset: slot_id as u64,
            reason: "slot id out of range",
        });
    }
    let off = SLOT_ARRAY_OFFSET + (slot_id as usize) * SLOT_ENTRY_LEN;
    Ok(Slot {
        record_offset: read_u16(bytes, off),
        record_length: read_u16(bytes, off + 2),
        flags: bytes[off + 4],
    })
}

/// Read a record from an immutable leaf page, returning `Ok(None)` when
/// the slot is a tombstone or its key does not match `expected_key`.
///
/// # Errors
///
/// Returns [`Error::Corrupted`] when the slot id is out of range or the
/// underlying record body is malformed.
pub(crate) fn read_record_at<'a>(
    page: &'a Page,
    slot_id: u16,
    expected_key: &[u8],
) -> Result<Option<RecordView<'a>>> {
    let slot = read_slot_at(page, slot_id)?;
    if !slot.is_live() {
        return Ok(None);
    }
    let view = decode_record(page.as_bytes(), slot)?;
    if view.key() != expected_key {
        return Ok(None);
    }
    Ok(Some(view))
}

/// Read a record from an immutable leaf page **without** verifying the
/// key. Use only when the caller already knows the slot id corresponds
/// to an unambiguous record (e.g., during full-leaf iteration).
///
/// # Errors
///
/// Returns [`Error::Corrupted`] when the slot id is out of range, when
/// the slot is a tombstone, or when the record body is malformed.
pub(crate) fn read_record_at_unchecked(
    page: &Page,
    slot_id: u16,
) -> Result<Option<RecordView<'_>>> {
    let slot = read_slot_at(page, slot_id)?;
    if !slot.is_live() {
        return Ok(None);
    }
    let view = decode_record(page.as_bytes(), slot)?;
    Ok(Some(view))
}

/// Return the next-leaf chain pointer of an immutable leaf page.
#[must_use]
pub(crate) fn next_leaf_of(page: &Page) -> PageId {
    let bytes = page.as_bytes();
    PageId::new(read_u64(bytes, NEXT_LEAF_OFFSET))
}

/// Return the live (non-tombstoned) slot count of an immutable leaf page.
#[must_use]
pub(crate) fn slot_count_of(page: &Page) -> u32 {
    let bytes = page.as_bytes();
    read_u32(bytes, SLOT_COUNT_OFFSET)
}

/// Return the free-space byte count of an immutable leaf page.
#[must_use]
pub(crate) fn free_space_of(page: &Page) -> u32 {
    let bytes = page.as_bytes();
    let slot_count = read_u32(bytes, SLOT_COUNT_OFFSET) as usize;
    let record_floor = read_u32(bytes, RECORD_FLOOR_OFFSET) as usize;
    let slot_array_end = SLOT_ARRAY_OFFSET + slot_count * SLOT_ENTRY_LEN;
    record_floor.saturating_sub(slot_array_end) as u32
}

/// Return the number of live (non-tombstoned) slots on an immutable leaf
/// page. Mirror of [`LeafPage::live_count`] for use sites that only have
/// `&Page` (the compactor walks chains without mutating).
#[must_use]
pub(crate) fn live_count_of(page: &Page) -> u32 {
    let total = slot_count_of(page);
    let mut live = 0_u32;
    for slot_id in 0..total {
        if let Ok(slot) = read_slot_at(page, slot_id as u16) {
            if slot.is_live() {
                live = live.saturating_add(1);
            }
        }
    }
    live
}

/// Read-write view over a slotted leaf page held in a [`Page`] buffer.
///
/// `LeafPage` does not own its bytes; it borrows from a [`Page`] and writes
/// through. Constructing one validates only the page-type discriminant and
/// the structural invariants (slot count fits, record floor in range). The
/// CRC is the caller's responsibility — refresh after every batch of writes
/// and validate before trusting any read.
#[derive(Debug)]
pub(crate) struct LeafPage<'a> {
    page: &'a mut Page,
}

impl<'a> LeafPage<'a> {
    /// Initialise an empty slotted leaf in the supplied page buffer.
    pub(crate) fn init(page: &'a mut Page) -> Self {
        page.set_header(PageHeader::new(PageType::LeafSlotted));
        let bytes = page.as_mut_bytes();
        bytes[NEXT_LEAF_OFFSET..NEXT_LEAF_OFFSET + 8].fill(0);
        write_u32(bytes, SLOT_COUNT_OFFSET, 0);
        write_u32(bytes, RECORD_FLOOR_OFFSET, PAGE_SIZE as u32);
        bytes[RESERVED_OFFSET..RESERVED_OFFSET + 8].fill(0);
        Self { page }
    }

    /// Borrow an existing slotted leaf, validating its structural invariants.
    ///
    /// # Errors
    ///
    /// Returns `Error::Corrupted` when the page header reports a different
    /// page type, when the slot count or record floor are out of range, or
    /// when the slot array would overlap the record area.
    pub(crate) fn open(page: &'a mut Page) -> Result<Self> {
        let header = page.header()?;
        if header.page_type != PageType::LeafSlotted {
            return Err(Error::Corrupted {
                offset: 0,
                reason: "not a slotted leaf page",
            });
        }

        let bytes = page.as_bytes();
        let slot_count = read_u32(bytes, SLOT_COUNT_OFFSET) as usize;
        let record_floor = read_u32(bytes, RECORD_FLOOR_OFFSET) as usize;

        if slot_count > MAX_SLOTS_PER_PAGE {
            return Err(Error::Corrupted {
                offset: SLOT_COUNT_OFFSET as u64,
                reason: "slot count exceeds page capacity",
            });
        }

        let slot_array_end = SLOT_ARRAY_OFFSET + slot_count * SLOT_ENTRY_LEN;
        if record_floor > PAGE_SIZE || record_floor < slot_array_end {
            return Err(Error::Corrupted {
                offset: RECORD_FLOOR_OFFSET as u64,
                reason: "record floor out of range",
            });
        }

        Ok(Self { page })
    }

    /// Page id of the next leaf in the chain (0 if this leaf is last).
    #[must_use]
    pub(crate) fn next_leaf(&self) -> PageId {
        PageId::new(read_u64(self.page.as_bytes(), NEXT_LEAF_OFFSET))
    }

    /// Update the next-leaf chain pointer.
    pub(crate) fn set_next_leaf(&mut self, page_id: PageId) {
        write_u64(self.page.as_mut_bytes(), NEXT_LEAF_OFFSET, page_id.get());
    }

    /// Number of slot entries (live + tombstone).
    #[must_use]
    pub(crate) fn slot_count(&self) -> u32 {
        read_u32(self.page.as_bytes(), SLOT_COUNT_OFFSET)
    }

    /// Bytes currently free between the slot array and the record area.
    #[must_use]
    pub(crate) fn free_space(&self) -> u32 {
        let bytes = self.page.as_bytes();
        let slot_count = read_u32(bytes, SLOT_COUNT_OFFSET) as usize;
        let record_floor = read_u32(bytes, RECORD_FLOOR_OFFSET) as usize;
        let slot_array_end = SLOT_ARRAY_OFFSET + slot_count * SLOT_ENTRY_LEN;
        record_floor.saturating_sub(slot_array_end) as u32
    }

    /// Number of live (non-tombstone) slots.
    #[must_use]
    pub(crate) fn live_count(&self) -> u32 {
        let mut live = 0_u32;
        let count = self.slot_count();
        for slot_id in 0..count {
            if let Ok(slot) = self.read_slot(slot_id as u16) {
                if slot.is_live() {
                    live = live.saturating_add(1);
                }
            }
        }
        live
    }

    /// Read a slot entry by id without validating that the slot is live.
    ///
    /// # Errors
    ///
    /// Returns `Error::Corrupted` when `slot_id` is out of range.
    pub(crate) fn read_slot(&self, slot_id: u16) -> Result<Slot> {
        let count = self.slot_count();
        if u32::from(slot_id) >= count {
            return Err(Error::Corrupted {
                offset: slot_id as u64,
                reason: "slot id out of range",
            });
        }
        let off = SLOT_ARRAY_OFFSET + (slot_id as usize) * SLOT_ENTRY_LEN;
        let bytes = self.page.as_bytes();
        Ok(Slot {
            record_offset: read_u16(bytes, off),
            record_length: read_u16(bytes, off + 2),
            flags: bytes[off + 4],
        })
    }

    /// Decode the record at the given slot. Returns `Ok(None)` when the slot
    /// is a tombstone or its key does not match `expected_key`.
    pub(crate) fn read_record(
        &self,
        slot_id: u16,
        expected_key: &[u8],
    ) -> Result<Option<RecordView<'_>>> {
        let slot = self.read_slot(slot_id)?;
        if !slot.is_live() {
            return Ok(None);
        }

        let view = decode_record(self.page.as_bytes(), slot)?;
        if view.key() != expected_key {
            return Ok(None);
        }
        Ok(Some(view))
    }

    /// Decode the record at the given slot without checking its key.
    ///
    /// Used by iteration and split paths that already trust the slot id.
    pub(crate) fn read_record_unchecked(&self, slot_id: u16) -> Result<Option<RecordView<'_>>> {
        let slot = self.read_slot(slot_id)?;
        if !slot.is_live() {
            return Ok(None);
        }
        let view = decode_record(self.page.as_bytes(), slot)?;
        Ok(Some(view))
    }

    /// Find the lowest tombstoned slot id, if any.
    ///
    /// Used by [`Self::insert_inline`] and [`Self::insert_overflow`] to
    /// reclaim slot ids freed by [`Self::tombstone`] without growing the
    /// slot array. This is the optimisation that keeps delete-heavy
    /// workloads from prematurely splitting pages.
    fn first_tombstoned_slot(&self) -> Option<u16> {
        let count = self.slot_count();
        let bytes = self.page.as_bytes();
        for slot_id in 0..count {
            let off = SLOT_ARRAY_OFFSET + (slot_id as usize) * SLOT_ENTRY_LEN;
            if bytes[off + 4] == FLAG_TOMBSTONE {
                return Some(slot_id as u16);
            }
        }
        None
    }

    /// Insert a new inline record. Returns the assigned slot id.
    ///
    /// Live slots are not consulted: callers should remove the old slot
    /// first if updating a key, otherwise the leaf will hold two records
    /// for the same key. Tombstoned slots **are** reclaimed — the inserted
    /// record reuses the lowest tombstoned slot id rather than appending
    /// to the slot array, so delete-heavy workloads do not inflate
    /// `slot_count` and trigger premature splits.
    ///
    /// # Errors
    ///
    /// Returns `Err(InsertError::KeyTooLarge)` when the record cannot fit on
    /// any page even when fresh. Returns `Err(InsertError::OutOfSpace)` when
    /// the page is too full and the caller should split or compact first.
    pub(crate) fn insert_inline(
        &mut self,
        key: &[u8],
        value: &[u8],
        expires_at: u64,
    ) -> core::result::Result<u16, InsertError> {
        let record_len =
            inline_record_len(key.len(), value.len()).ok_or(InsertError::KeyTooLarge)?;
        let max_record_capacity = PAGE_SIZE - SLOT_ARRAY_OFFSET - SLOT_ENTRY_LEN;
        if record_len > max_record_capacity {
            return Err(InsertError::KeyTooLarge);
        }

        let reuse_slot = self.first_tombstoned_slot();
        // A reused slot consumes only record-area bytes; only a fresh slot
        // also needs `SLOT_ENTRY_LEN` for the new slot-array entry.
        let space_required = if reuse_slot.is_some() {
            record_len
        } else {
            SLOT_ENTRY_LEN + record_len
        };
        if (self.free_space() as usize) < space_required {
            return Err(InsertError::OutOfSpace);
        }

        let slot_count = self.slot_count() as usize;
        if reuse_slot.is_none() && slot_count >= MAX_SLOTS_PER_PAGE {
            return Err(InsertError::OutOfSpace);
        }

        let bytes = self.page.as_mut_bytes();
        let record_floor = read_u32(bytes, RECORD_FLOOR_OFFSET) as usize;
        let new_record_offset = record_floor - record_len;

        encode_inline_body(bytes, new_record_offset, key, value, expires_at);

        let assigned_slot = reuse_slot.unwrap_or(slot_count as u16);
        let slot_off = SLOT_ARRAY_OFFSET + (assigned_slot as usize) * SLOT_ENTRY_LEN;
        write_u16(bytes, slot_off, new_record_offset as u16);
        write_u16(bytes, slot_off + 2, record_len as u16);
        bytes[slot_off + 4] = FLAG_INLINE;
        bytes[slot_off + 5] = 0;
        bytes[slot_off + 6] = 0;
        bytes[slot_off + 7] = 0;

        if reuse_slot.is_none() {
            write_u32(bytes, SLOT_COUNT_OFFSET, (slot_count + 1) as u32);
        }
        write_u32(bytes, RECORD_FLOOR_OFFSET, new_record_offset as u32);

        Ok(assigned_slot)
    }

    /// Insert an overflow record (value lives in a chain of overflow pages).
    ///
    /// Reuses the lowest tombstoned slot id when one is available; see
    /// [`Self::insert_inline`] for details.
    ///
    /// # Errors
    ///
    /// Same shape as [`Self::insert_inline`].
    pub(crate) fn insert_overflow(
        &mut self,
        key: &[u8],
        value_len: u32,
        overflow_head: PageId,
        expires_at: u64,
    ) -> core::result::Result<u16, InsertError> {
        let record_len = overflow_record_len(key.len()).ok_or(InsertError::KeyTooLarge)?;
        let max_record_capacity = PAGE_SIZE - SLOT_ARRAY_OFFSET - SLOT_ENTRY_LEN;
        if record_len > max_record_capacity {
            return Err(InsertError::KeyTooLarge);
        }
        if overflow_head.get() > MAX_PAGE_ID {
            return Err(InsertError::KeyTooLarge);
        }

        let reuse_slot = self.first_tombstoned_slot();
        let space_required = if reuse_slot.is_some() {
            record_len
        } else {
            SLOT_ENTRY_LEN + record_len
        };
        if (self.free_space() as usize) < space_required {
            return Err(InsertError::OutOfSpace);
        }

        let slot_count = self.slot_count() as usize;
        if reuse_slot.is_none() && slot_count >= MAX_SLOTS_PER_PAGE {
            return Err(InsertError::OutOfSpace);
        }

        let bytes = self.page.as_mut_bytes();
        let record_floor = read_u32(bytes, RECORD_FLOOR_OFFSET) as usize;
        let new_record_offset = record_floor - record_len;

        encode_overflow_body(
            bytes,
            new_record_offset,
            key,
            expires_at,
            value_len,
            overflow_head.get(),
        );

        let assigned_slot = reuse_slot.unwrap_or(slot_count as u16);
        let slot_off = SLOT_ARRAY_OFFSET + (assigned_slot as usize) * SLOT_ENTRY_LEN;
        write_u16(bytes, slot_off, new_record_offset as u16);
        write_u16(bytes, slot_off + 2, record_len as u16);
        bytes[slot_off + 4] = FLAG_OVERFLOW;
        bytes[slot_off + 5] = 0;
        bytes[slot_off + 6] = 0;
        bytes[slot_off + 7] = 0;

        if reuse_slot.is_none() {
            write_u32(bytes, SLOT_COUNT_OFFSET, (slot_count + 1) as u32);
        }
        write_u32(bytes, RECORD_FLOOR_OFFSET, new_record_offset as u32);

        Ok(assigned_slot)
    }

    /// Mark a slot as a tombstone. Returns `true` when a live slot was found.
    ///
    /// The record bytes are not zeroed and the record area is not compacted.
    /// A subsequent [`Self::compact`] call rebuilds the page without dead
    /// records when free space drops below a threshold.
    pub(crate) fn tombstone(&mut self, slot_id: u16) -> Result<bool> {
        let count = self.slot_count();
        if u32::from(slot_id) >= count {
            return Ok(false);
        }
        let off = SLOT_ARRAY_OFFSET + (slot_id as usize) * SLOT_ENTRY_LEN;
        let bytes = self.page.as_mut_bytes();
        if bytes[off + 4] == FLAG_TOMBSTONE {
            return Ok(false);
        }
        bytes[off + 4] = FLAG_TOMBSTONE;
        Ok(true)
    }

    /// Borrow the underlying page buffer for CRC refresh.
    pub(crate) fn page_mut(&mut self) -> &mut Page {
        self.page
    }

    /// Borrow the underlying page buffer for read-only access.
    #[must_use]
    pub(crate) fn page(&self) -> &Page {
        self.page
    }

    /// Iterate every live record in slot-id order.
    pub(crate) fn iter_live(&self) -> LiveIter<'_> {
        LiveIter {
            page: self,
            slot_id: 0,
            count: self.slot_count() as u16,
        }
    }
}

/// Outcome of a [`split_leaf`] call.
///
/// Each entry maps a record's key bytes to its new slot id in either the
/// rebuilt original page (`kept`) or the freshly-initialised new page
/// (`moved`). Callers use these mappings to update the in-memory keymap
/// so existing RIDs continue to resolve correctly after the split.
#[derive(Debug)]
pub(crate) struct SplitOutcome {
    /// Records that remained in the original page, with their new slot ids.
    pub(crate) kept: Vec<(Vec<u8>, u16)>,
    /// Records that moved to the new page, with their slot ids in the new page.
    pub(crate) moved: Vec<(Vec<u8>, u16)>,
}

/// Outcome of a [`compact_leaf`] call: a remap of every live record's old
/// slot id to its new slot id in the rebuilt page.
#[derive(Debug)]
pub(crate) struct CompactOutcome {
    /// Per live record, `(key, old_slot_id, new_slot_id)`.
    pub(crate) remap: Vec<(Vec<u8>, u16, u16)>,
}

/// Owned snapshot of a record body. Internal helper used during page rebuilds.
#[derive(Debug)]
struct OwnedRecord {
    key: Vec<u8>,
    expires_at: u64,
    body: OwnedBody,
}

#[derive(Debug)]
enum OwnedBody {
    Inline(Vec<u8>),
    Overflow { value_len: u32, head: PageId },
}

impl OwnedRecord {
    fn from_view(view: &RecordView<'_>) -> Self {
        match view {
            RecordView::Inline {
                key,
                value,
                expires_at,
            } => Self {
                key: key.to_vec(),
                expires_at: *expires_at,
                body: OwnedBody::Inline(value.to_vec()),
            },
            RecordView::Overflow {
                key,
                value_len,
                overflow_head,
                expires_at,
            } => Self {
                key: key.to_vec(),
                expires_at: *expires_at,
                body: OwnedBody::Overflow {
                    value_len: *value_len,
                    head: PageId::new(*overflow_head),
                },
            },
        }
    }

    fn reinsert(&self, leaf: &mut LeafPage<'_>) -> core::result::Result<u16, InsertError> {
        match &self.body {
            OwnedBody::Inline(value) => leaf.insert_inline(&self.key, value, self.expires_at),
            OwnedBody::Overflow { value_len, head } => {
                leaf.insert_overflow(&self.key, *value_len, *head, self.expires_at)
            }
        }
    }
}

/// Rebuild a slotted leaf in place, dropping tombstones and reclaiming the
/// bytes they occupied. Returns the old-slot → new-slot remap so callers can
/// fix up the keymap.
///
/// # Errors
///
/// Propagates any decode error from the original page. Re-insertion cannot
/// fail with `OutOfSpace` because the rebuilt page only holds the records
/// that already fit before; if it does, the page state was inconsistent and
/// we surface a corrupted-page error.
pub(crate) fn compact_leaf(page: &mut Page) -> Result<CompactOutcome> {
    let live: Vec<(u16, OwnedRecord)> = {
        let leaf = LeafPage::open(page)?;
        leaf.iter_live()
            .map(|(slot_id, view)| (slot_id, OwnedRecord::from_view(&view)))
            .collect()
    };

    let next_leaf = {
        let leaf = LeafPage::open(page)?;
        leaf.next_leaf()
    };

    let mut leaf = LeafPage::init(page);
    leaf.set_next_leaf(next_leaf);

    let mut remap = Vec::with_capacity(live.len());
    for (old_slot_id, record) in live {
        let new_slot_id = record.reinsert(&mut leaf).map_err(|err| Error::Corrupted {
            offset: 0,
            reason: match err {
                InsertError::OutOfSpace => "compact reinsert overflowed page budget",
                InsertError::KeyTooLarge => "compact found key too large to fit",
            },
        })?;
        remap.push((record.key, old_slot_id, new_slot_id));
    }

    Ok(CompactOutcome { remap })
}

/// Split a slotted leaf at its midpoint. The original page is rebuilt with
/// the lower half of records; the upper half is written into `new_page`.
///
/// Both pages emerge in compact form (no tombstones). The caller is
/// responsible for:
///
/// 1. Wiring `new_page`'s `next_leaf` to whatever `original`'s previous chain
///    pointer was, then setting `original`'s `next_leaf` to the new page id.
/// 2. Updating the in-memory keymap from `kept`/`moved` so existing RIDs
///    resolve to the right page after the split.
///
/// # Errors
///
/// Returns `Error::Corrupted` if the original page is not a slotted leaf or
/// if a re-insert fails because of an inconsistent page state. A successful
/// return guarantees both pages are in valid form, but their CRCs are not
/// refreshed — the caller must call [`refresh_leaf_crc`] on each before
/// persisting.
pub(crate) fn split_leaf(original: &mut Page, new_page: &mut Page) -> Result<SplitOutcome> {
    let owned: Vec<OwnedRecord> = {
        let leaf = LeafPage::open(original)?;
        leaf.iter_live()
            .map(|(_slot_id, view)| OwnedRecord::from_view(&view))
            .collect()
    };

    let total = owned.len();
    let mid = total / 2;
    let chain_next = {
        let leaf = LeafPage::open(original)?;
        leaf.next_leaf()
    };

    // Rebuild original with the lower half.
    let mut keep_iter = owned;
    let upper = keep_iter.split_off(mid);
    let lower = keep_iter;

    let mut original_leaf = LeafPage::init(original);
    let mut kept = Vec::with_capacity(lower.len());
    for record in &lower {
        let slot = record
            .reinsert(&mut original_leaf)
            .map_err(|err| Error::Corrupted {
                offset: 0,
                reason: match err {
                    InsertError::OutOfSpace => "split kept-half overflowed page budget",
                    InsertError::KeyTooLarge => "split kept-half found key too large",
                },
            })?;
        kept.push((record.key.clone(), slot));
    }

    let mut new_leaf = LeafPage::init(new_page);
    new_leaf.set_next_leaf(chain_next);

    let mut moved = Vec::with_capacity(upper.len());
    for record in &upper {
        let slot = record
            .reinsert(&mut new_leaf)
            .map_err(|err| Error::Corrupted {
                offset: 0,
                reason: match err {
                    InsertError::OutOfSpace => "split moved-half overflowed page budget",
                    InsertError::KeyTooLarge => "split moved-half found key too large",
                },
            })?;
        moved.push((record.key.clone(), slot));
    }

    Ok(SplitOutcome { kept, moved })
}

/// Iterator over the live records of a [`LeafPage`].
pub(crate) struct LiveIter<'a> {
    page: &'a LeafPage<'a>,
    slot_id: u16,
    count: u16,
}

impl<'a> Iterator for LiveIter<'a> {
    type Item = (u16, RecordView<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        while self.slot_id < self.count {
            let id = self.slot_id;
            self.slot_id = self.slot_id.saturating_add(1);

            let Ok(slot) = self.page.read_slot(id) else {
                continue;
            };
            if !slot.is_live() {
                continue;
            }
            let Ok(view) = decode_record(self.page.page.as_bytes(), slot) else {
                continue;
            };
            return Some((id, view));
        }
        None
    }
}

/// Refresh the page CRC after one or more mutations. Callers must invoke this
/// before persisting the page to disk.
pub(crate) fn refresh_leaf_crc(page: &mut Page) -> Result<u32> {
    page.refresh_crc()
}

/// Validate a slotted leaf's CRC against its current bytes.
pub(crate) fn validate_leaf_crc(page: &Page) -> Result<()> {
    page.validate_crc()
}

/// Compute the total in-page length of an inline record.
#[must_use]
pub(crate) const fn inline_record_len(key_len: usize, value_len: usize) -> Option<usize> {
    let Some(key_part) = key_len.checked_add(KEY_LEN_FIELD) else {
        return None;
    };
    let Some(with_val_meta) = key_part.checked_add(INLINE_RECORD_FIXED - KEY_LEN_FIELD) else {
        return None;
    };
    with_val_meta.checked_add(value_len)
}

/// Compute the total in-page length of an overflow-record stub.
#[must_use]
pub(crate) const fn overflow_record_len(key_len: usize) -> Option<usize> {
    let Some(key_part) = key_len.checked_add(KEY_LEN_FIELD) else {
        return None;
    };
    key_part.checked_add(OVERFLOW_RECORD_FIXED)
}

fn encode_inline_body(out: &mut [u8], offset: usize, key: &[u8], value: &[u8], expires_at: u64) {
    let mut cursor = offset;
    write_u32(out, cursor, key.len() as u32);
    cursor += 4;
    out[cursor..cursor + key.len()].copy_from_slice(key);
    cursor += key.len();
    write_u64(out, cursor, expires_at);
    cursor += 8;
    write_u32(out, cursor, value.len() as u32);
    cursor += 4;
    out[cursor..cursor + value.len()].copy_from_slice(value);
}

fn encode_overflow_body(
    out: &mut [u8],
    offset: usize,
    key: &[u8],
    expires_at: u64,
    value_len: u32,
    overflow_head: u64,
) {
    let mut cursor = offset;
    write_u32(out, cursor, key.len() as u32);
    cursor += 4;
    out[cursor..cursor + key.len()].copy_from_slice(key);
    cursor += key.len();
    write_u64(out, cursor, expires_at);
    cursor += 8;
    write_u32(out, cursor, value_len);
    cursor += 4;
    write_u64(out, cursor, overflow_head);
}

fn decode_record(bytes: &[u8], slot: Slot) -> Result<RecordView<'_>> {
    let start = slot.record_offset as usize;
    let end = start
        .checked_add(slot.record_length as usize)
        .ok_or(Error::Corrupted {
            offset: 0,
            reason: "record offset overflow",
        })?;
    if end > PAGE_SIZE {
        return Err(Error::Corrupted {
            offset: 0,
            reason: "record extends past page end",
        });
    }
    if start < SLOT_ARRAY_OFFSET {
        return Err(Error::Corrupted {
            offset: 0,
            reason: "record overlaps slot array",
        });
    }

    let body = &bytes[start..end];
    let mut cursor = 0_usize;

    let key_len = take_u32(body, &mut cursor)? as usize;
    let key = take_bytes(body, &mut cursor, key_len)?;
    let expires_at = take_u64(body, &mut cursor)?;
    match slot.flags {
        FLAG_INLINE => {
            let value_len = take_u32(body, &mut cursor)? as usize;
            let value = take_bytes(body, &mut cursor, value_len)?;
            if cursor != body.len() {
                return Err(Error::Corrupted {
                    offset: 0,
                    reason: "trailing bytes in inline record",
                });
            }
            Ok(RecordView::Inline {
                key,
                value,
                expires_at,
            })
        }
        FLAG_OVERFLOW => {
            let value_len = take_u32(body, &mut cursor)?;
            let overflow_head = take_u64(body, &mut cursor)?;
            if cursor != body.len() {
                return Err(Error::Corrupted {
                    offset: 0,
                    reason: "trailing bytes in overflow record",
                });
            }
            Ok(RecordView::Overflow {
                key,
                value_len,
                overflow_head,
                expires_at,
            })
        }
        FLAG_TOMBSTONE => Err(Error::Corrupted {
            offset: 0,
            reason: "tombstone slot decoded as live",
        }),
        _ => Err(Error::Corrupted {
            offset: 0,
            reason: "unknown slot flag",
        }),
    }
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

fn take_u32(body: &[u8], cursor: &mut usize) -> Result<u32> {
    let bytes = take_bytes(body, cursor, 4)?;
    Ok(read_u32(bytes, 0))
}

fn take_u64(body: &[u8], cursor: &mut usize) -> Result<u64> {
    let bytes = take_bytes(body, cursor, 8)?;
    Ok(read_u64(bytes, 0))
}

fn take_bytes<'a>(body: &'a [u8], cursor: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = cursor.checked_add(len).ok_or(Error::Corrupted {
        offset: 0,
        reason: "record cursor overflow",
    })?;
    if end > body.len() {
        return Err(Error::Corrupted {
            offset: 0,
            reason: "record body truncated",
        });
    }
    let out = &body[*cursor..end];
    *cursor = end;
    Ok(out)
}

/// Free a `_unused_` reference to `page_crc` so unused-import lints stay quiet
/// while the leaf module is still being wired up. Callers should use
/// [`Page::refresh_crc`] / [`Page::validate_crc`] instead — kept here only to
/// document the intended dependency.
#[allow(dead_code)]
const fn _crc_dependency() {
    let _ = page_crc;
}

#[cfg(test)]
mod tests {
    use super::{
        decode_record, inline_record_len, overflow_record_len, refresh_leaf_crc, validate_leaf_crc,
        InsertError, LeafPage, RecordView, FLAG_INLINE, FLAG_OVERFLOW, FLAG_TOMBSTONE,
        SLOT_ARRAY_OFFSET, SLOT_ENTRY_LEN,
    };
    use crate::storage::page::{Page, PageHeader, PageId, PageType, PAGE_SIZE};

    fn fresh_leaf() -> Page {
        let mut page = Page::new(PageHeader::new(PageType::LeafSlotted));
        let _leaf = LeafPage::init(&mut page);
        page
    }

    #[test]
    fn fresh_page_reports_full_capacity() {
        let mut page = fresh_leaf();
        let leaf = match LeafPage::open(&mut page) {
            Ok(leaf) => leaf,
            Err(err) => panic!("open should succeed: {err}"),
        };
        assert_eq!(leaf.slot_count(), 0);
        let expected_free = PAGE_SIZE - SLOT_ARRAY_OFFSET;
        assert_eq!(leaf.free_space() as usize, expected_free);
        assert_eq!(leaf.next_leaf().get(), 0);
    }

    #[test]
    fn next_leaf_pointer_round_trips() {
        let mut page = fresh_leaf();
        {
            let mut leaf = match LeafPage::open(&mut page) {
                Ok(leaf) => leaf,
                Err(err) => panic!("open should succeed: {err}"),
            };
            leaf.set_next_leaf(PageId::new(0xCAFE_BABE));
        }
        let leaf = match LeafPage::open(&mut page) {
            Ok(leaf) => leaf,
            Err(err) => panic!("open should succeed: {err}"),
        };
        assert_eq!(leaf.next_leaf().get(), 0xCAFE_BABE);
    }

    #[test]
    fn inline_round_trip_returns_value_and_expiry() {
        let mut page = fresh_leaf();
        let mut leaf = match LeafPage::open(&mut page) {
            Ok(leaf) => leaf,
            Err(err) => panic!("open should succeed: {err}"),
        };

        let inserted = leaf.insert_inline(b"alpha", b"one", 12345);
        assert!(matches!(inserted, Ok(0)));

        let view = match leaf.read_record(0, b"alpha") {
            Ok(Some(view)) => view,
            Ok(None) => panic!("record should be present"),
            Err(err) => panic!("read should succeed: {err}"),
        };
        assert!(matches!(
            view,
            RecordView::Inline {
                key,
                value,
                expires_at,
            } if key == b"alpha" && value == b"one" && expires_at == 12345
        ));
    }

    #[test]
    fn read_record_returns_none_on_key_mismatch() {
        let mut page = fresh_leaf();
        let mut leaf = match LeafPage::open(&mut page) {
            Ok(leaf) => leaf,
            Err(err) => panic!("open should succeed: {err}"),
        };
        let _ = leaf.insert_inline(b"alpha", b"one", 0);

        let mismatch = match leaf.read_record(0, b"beta") {
            Ok(value) => value,
            Err(err) => panic!("read should succeed: {err}"),
        };
        assert!(mismatch.is_none());
    }

    #[test]
    fn many_inserts_each_get_unique_slot_ids() {
        let mut page = fresh_leaf();
        let mut leaf = match LeafPage::open(&mut page) {
            Ok(leaf) => leaf,
            Err(err) => panic!("open should succeed: {err}"),
        };

        for i in 0_u32..32 {
            let key = format!("k{i:03}");
            let value = format!("v{i:03}");
            let result = leaf.insert_inline(key.as_bytes(), value.as_bytes(), 0);
            assert!(matches!(result, Ok(slot) if u32::from(slot) == i));
        }

        assert_eq!(leaf.slot_count(), 32);
        assert_eq!(leaf.live_count(), 32);
    }

    #[test]
    fn inserting_until_full_returns_out_of_space_then_recovers_after_tombstone() {
        let mut page = fresh_leaf();
        let mut leaf = match LeafPage::open(&mut page) {
            Ok(leaf) => leaf,
            Err(err) => panic!("open should succeed: {err}"),
        };

        let value = vec![b'x'; 1000];
        let mut inserted = 0_usize;
        loop {
            let key = format!("key-{inserted:04}");
            let result = leaf.insert_inline(key.as_bytes(), &value, 0);
            if let Err(InsertError::OutOfSpace) = result {
                break;
            }
            assert!(result.is_ok(), "unexpected error: {:?}", result);
            inserted += 1;
        }
        assert!(inserted > 0);
        assert!(matches!(
            leaf.insert_inline(b"another", &value, 0),
            Err(InsertError::OutOfSpace)
        ));

        // Tombstoning frees logical entries but does not reclaim bytes; the
        // page should still report OutOfSpace until compaction lands.
        assert!(matches!(leaf.tombstone(0), Ok(true)));
        assert!(matches!(
            leaf.insert_inline(b"another", &value, 0),
            Err(InsertError::OutOfSpace)
        ));
    }

    #[test]
    fn key_too_large_is_distinguished_from_out_of_space() {
        let mut page = fresh_leaf();
        let mut leaf = match LeafPage::open(&mut page) {
            Ok(leaf) => leaf,
            Err(err) => panic!("open should succeed: {err}"),
        };

        // A key approaching the page size cannot ever fit.
        let huge_key = vec![b'k'; PAGE_SIZE];
        let result = leaf.insert_inline(&huge_key, b"v", 0);
        assert!(matches!(result, Err(InsertError::KeyTooLarge)));
    }

    #[test]
    fn tombstone_hides_record_from_read_record() {
        let mut page = fresh_leaf();
        let mut leaf = match LeafPage::open(&mut page) {
            Ok(leaf) => leaf,
            Err(err) => panic!("open should succeed: {err}"),
        };

        let slot = match leaf.insert_inline(b"alpha", b"one", 0) {
            Ok(slot) => slot,
            Err(err) => panic!("insert should succeed: {:?}", err),
        };

        let _ = leaf.tombstone(slot);
        let read = match leaf.read_record(slot, b"alpha") {
            Ok(value) => value,
            Err(err) => panic!("read should succeed: {err}"),
        };
        assert!(read.is_none());
        assert_eq!(leaf.live_count(), 0);
    }

    #[test]
    fn tombstone_reuse_recycles_slot_id_for_small_inserts() {
        // After tombstoning a small slot, a same-size insert should reuse
        // the same slot id rather than appending a new one. This avoids
        // inflating slot_count on delete-heavy workloads.
        let mut page = fresh_leaf();
        let mut leaf = match LeafPage::open(&mut page) {
            Ok(leaf) => leaf,
            Err(err) => panic!("open should succeed: {err}"),
        };

        let s0 = match leaf.insert_inline(b"a", b"1", 0) {
            Ok(s) => s,
            Err(err) => panic!("insert should succeed: {:?}", err),
        };
        let s1 = match leaf.insert_inline(b"b", b"2", 0) {
            Ok(s) => s,
            Err(err) => panic!("insert should succeed: {:?}", err),
        };
        assert_eq!(s0, 0);
        assert_eq!(s1, 1);
        assert_eq!(leaf.slot_count(), 2);

        // Tombstone slot 0, then insert a fresh small record.
        assert!(matches!(leaf.tombstone(s0), Ok(true)));
        let recycled = match leaf.insert_inline(b"c", b"3", 0) {
            Ok(s) => s,
            Err(err) => panic!("insert should succeed: {:?}", err),
        };

        // Crucially: slot 0 is reused, not slot 2.
        assert_eq!(recycled, 0);
        assert_eq!(leaf.slot_count(), 2, "slot_count must not grow on reuse");

        // The reused slot now holds the new key/value.
        let view = match leaf.read_record(recycled, b"c") {
            Ok(Some(view)) => view,
            Ok(None) => panic!("recycled slot should be live"),
            Err(err) => panic!("read should succeed: {err}"),
        };
        match view {
            RecordView::Inline { key, value, .. } => {
                assert_eq!(key, b"c");
                assert_eq!(value, b"3");
            }
            RecordView::Overflow { .. } => panic!("inline expected"),
        }
    }

    #[test]
    fn iter_live_skips_tombstones() {
        let mut page = fresh_leaf();
        let mut leaf = match LeafPage::open(&mut page) {
            Ok(leaf) => leaf,
            Err(err) => panic!("open should succeed: {err}"),
        };

        let _ = leaf.insert_inline(b"a", b"1", 0);
        let mid = match leaf.insert_inline(b"b", b"2", 0) {
            Ok(slot) => slot,
            Err(err) => panic!("insert should succeed: {:?}", err),
        };
        let _ = leaf.insert_inline(b"c", b"3", 0);
        let _ = leaf.tombstone(mid);

        let collected: Vec<_> = leaf
            .iter_live()
            .map(|(slot_id, view)| (slot_id, view.key().to_vec()))
            .collect();

        assert_eq!(collected.len(), 2);
        assert_eq!(collected[0].1, b"a".to_vec());
        assert_eq!(collected[1].1, b"c".to_vec());
    }

    #[test]
    fn overflow_round_trip_decodes_pointer_and_total_length() {
        let mut page = fresh_leaf();
        let mut leaf = match LeafPage::open(&mut page) {
            Ok(leaf) => leaf,
            Err(err) => panic!("open should succeed: {err}"),
        };

        let head = PageId::new(0x1234);
        let inserted = leaf.insert_overflow(b"big", 1_000_000, head, 7);
        assert!(matches!(inserted, Ok(0)));

        let slot = match leaf.read_slot(0) {
            Ok(slot) => slot,
            Err(err) => panic!("read slot should succeed: {err}"),
        };
        assert!(slot.is_overflow());

        let view = match leaf.read_record(0, b"big") {
            Ok(Some(view)) => view,
            Ok(None) => panic!("record should be present"),
            Err(err) => panic!("read should succeed: {err}"),
        };
        assert!(matches!(
            view,
            RecordView::Overflow {
                key,
                value_len: 1_000_000,
                overflow_head: 0x1234,
                expires_at: 7,
            } if key == b"big"
        ));
    }

    #[test]
    fn open_rejects_pages_of_wrong_type() {
        let mut page = Page::new(PageHeader::new(PageType::ValueLeaf));
        let opened = LeafPage::open(&mut page);
        assert!(opened.is_err());
    }

    #[test]
    fn open_rejects_pages_with_record_floor_below_slot_array() {
        let mut page = fresh_leaf();
        // Corrupt: write a record_floor that is below the slot array bound.
        let bytes = page.as_mut_bytes();
        bytes[super::RECORD_FLOOR_OFFSET..super::RECORD_FLOOR_OFFSET + 4]
            .copy_from_slice(&0_u32.to_le_bytes());
        let opened = LeafPage::open(&mut page);
        assert!(opened.is_err());
    }

    #[test]
    fn crc_round_trip_after_inserts_validates() {
        let mut page = fresh_leaf();
        {
            let mut leaf = match LeafPage::open(&mut page) {
                Ok(leaf) => leaf,
                Err(err) => panic!("open should succeed: {err}"),
            };
            let _ = leaf.insert_inline(b"k", b"v", 0);
        }
        let refreshed = refresh_leaf_crc(&mut page);
        assert!(refreshed.is_ok());
        assert!(validate_leaf_crc(&page).is_ok());
    }

    #[test]
    fn record_lengths_match_helper_calculations() {
        // INLINE: 4 (key_len) + 5 (key) + 8 (expires_at) + 4 (value_len) + 3 (value) = 24
        let inline = inline_record_len(5, 3);
        assert!(matches!(inline, Some(24)));

        // OVERFLOW: 4 (key_len) + 5 (key) + 8 (expires_at) + 4 (value_len) + 8 (overflow_head) = 29
        let overflow = overflow_record_len(5);
        assert!(matches!(overflow, Some(29)));
    }

    #[test]
    fn flag_constants_are_distinct() {
        assert_ne!(FLAG_INLINE, FLAG_OVERFLOW);
        assert_ne!(FLAG_INLINE, FLAG_TOMBSTONE);
        assert_ne!(FLAG_OVERFLOW, FLAG_TOMBSTONE);
    }

    #[test]
    fn slot_entry_length_is_eight_bytes() {
        // Hard-coded in the on-disk layout. If this changes the format
        // version must change too.
        assert_eq!(SLOT_ENTRY_LEN, 8);
    }

    #[test]
    fn decoder_rejects_record_extending_past_page() {
        let bytes = [0_u8; PAGE_SIZE];
        let bad_slot = super::Slot {
            record_offset: (PAGE_SIZE - 4) as u16,
            record_length: 64,
            flags: FLAG_INLINE,
        };
        let result = decode_record(&bytes, bad_slot);
        assert!(result.is_err());
    }

    #[test]
    fn compact_drops_tombstones_and_remaps_live_slots() {
        let mut page = fresh_leaf();

        // Build a leaf with a known pattern of live + tombstone slots.
        {
            let mut leaf = match LeafPage::open(&mut page) {
                Ok(leaf) => leaf,
                Err(err) => panic!("open should succeed: {err}"),
            };
            for i in 0_u32..6 {
                let key = format!("k{i}");
                let value = format!("v{i}");
                let _ = leaf.insert_inline(key.as_bytes(), value.as_bytes(), 0);
            }
            // Tombstone slots 1, 3, 5 (every other one).
            let _ = leaf.tombstone(1);
            let _ = leaf.tombstone(3);
            let _ = leaf.tombstone(5);
        }

        let outcome = match super::compact_leaf(&mut page) {
            Ok(outcome) => outcome,
            Err(err) => panic!("compact should succeed: {err}"),
        };

        // Three records remain. They were originally at slots 0, 2, 4.
        assert_eq!(outcome.remap.len(), 3);
        assert_eq!(outcome.remap[0].0, b"k0".to_vec());
        assert_eq!(outcome.remap[0].1, 0);
        assert_eq!(outcome.remap[0].2, 0);
        assert_eq!(outcome.remap[1].0, b"k2".to_vec());
        assert_eq!(outcome.remap[1].1, 2);
        assert_eq!(outcome.remap[1].2, 1);
        assert_eq!(outcome.remap[2].0, b"k4".to_vec());
        assert_eq!(outcome.remap[2].1, 4);
        assert_eq!(outcome.remap[2].2, 2);

        // Verify the rebuilt page reads correctly under the new slot ids.
        let leaf = match LeafPage::open(&mut page) {
            Ok(leaf) => leaf,
            Err(err) => panic!("open should succeed: {err}"),
        };
        assert_eq!(leaf.slot_count(), 3);
        for (key, _old, new_slot_id) in &outcome.remap {
            let view = match leaf.read_record(*new_slot_id, key) {
                Ok(Some(view)) => view,
                Ok(None) => panic!("compacted slot should be live"),
                Err(err) => panic!("read should succeed: {err}"),
            };
            assert_eq!(view.key(), key.as_slice());
        }
    }

    #[test]
    fn compact_preserves_next_leaf_pointer() {
        let mut page = fresh_leaf();
        {
            let mut leaf = match LeafPage::open(&mut page) {
                Ok(leaf) => leaf,
                Err(err) => panic!("open should succeed: {err}"),
            };
            leaf.set_next_leaf(PageId::new(0xBEEF));
            let _ = leaf.insert_inline(b"a", b"1", 0);
        }
        let _ = super::compact_leaf(&mut page);
        let leaf = match LeafPage::open(&mut page) {
            Ok(leaf) => leaf,
            Err(err) => panic!("open should succeed: {err}"),
        };
        assert_eq!(leaf.next_leaf().get(), 0xBEEF);
    }

    #[test]
    fn split_distributes_records_evenly_and_returns_remap() {
        let mut original = fresh_leaf();
        {
            let mut leaf = match LeafPage::open(&mut original) {
                Ok(leaf) => leaf,
                Err(err) => panic!("open should succeed: {err}"),
            };
            for i in 0_u32..10 {
                let key = format!("key-{i:02}");
                let value = format!("value-{i:02}");
                let _ = leaf.insert_inline(key.as_bytes(), value.as_bytes(), 0);
            }
            leaf.set_next_leaf(PageId::new(0x4242));
        }

        let mut new_page = Page::new(PageHeader::new(PageType::LeafSlotted));
        let outcome = match super::split_leaf(&mut original, &mut new_page) {
            Ok(outcome) => outcome,
            Err(err) => panic!("split should succeed: {err}"),
        };

        // 10 records → 5 kept, 5 moved.
        assert_eq!(outcome.kept.len(), 5);
        assert_eq!(outcome.moved.len(), 5);

        // Each kept entry resolves under its new slot id in the rebuilt
        // original; the original's next_leaf pointer stays untouched (caller
        // wires it to the new page id afterwards).
        let original_leaf = match LeafPage::open(&mut original) {
            Ok(leaf) => leaf,
            Err(err) => panic!("open original should succeed: {err}"),
        };
        for (key, slot) in &outcome.kept {
            let read = original_leaf.read_record(*slot, key);
            let view = match read {
                Ok(Some(view)) => view,
                Ok(None) => panic!("kept record should be present"),
                Err(err) => panic!("read should succeed: {err}"),
            };
            assert_eq!(view.key(), key.as_slice());
        }

        // Moved entries appear in the new page; new_leaf's next_leaf takes
        // over the original chain pointer so the linked list survives.
        let new_leaf = match LeafPage::open(&mut new_page) {
            Ok(leaf) => leaf,
            Err(err) => panic!("open new should succeed: {err}"),
        };
        assert_eq!(new_leaf.next_leaf().get(), 0x4242);
        for (key, slot) in &outcome.moved {
            let read = new_leaf.read_record(*slot, key);
            let view = match read {
                Ok(Some(view)) => view,
                Ok(None) => panic!("moved record should be present"),
                Err(err) => panic!("read should succeed: {err}"),
            };
            assert_eq!(view.key(), key.as_slice());
        }
    }

    #[test]
    fn split_of_empty_page_yields_empty_outcome() {
        let mut original = fresh_leaf();
        let mut new_page = Page::new(PageHeader::new(PageType::LeafSlotted));
        let outcome = match super::split_leaf(&mut original, &mut new_page) {
            Ok(outcome) => outcome,
            Err(err) => panic!("split should succeed: {err}"),
        };
        assert_eq!(outcome.kept.len(), 0);
        assert_eq!(outcome.moved.len(), 0);

        let leaf = match LeafPage::open(&mut original) {
            Ok(leaf) => leaf,
            Err(err) => panic!("open should succeed: {err}"),
        };
        assert_eq!(leaf.slot_count(), 0);
    }

    #[test]
    fn split_of_odd_count_keeps_lower_half() {
        let mut original = fresh_leaf();
        {
            let mut leaf = match LeafPage::open(&mut original) {
                Ok(leaf) => leaf,
                Err(err) => panic!("open should succeed: {err}"),
            };
            for i in 0_u32..7 {
                let key = format!("k{i}");
                let _ = leaf.insert_inline(key.as_bytes(), b"v", 0);
            }
        }
        let mut new_page = Page::new(PageHeader::new(PageType::LeafSlotted));
        let outcome = match super::split_leaf(&mut original, &mut new_page) {
            Ok(outcome) => outcome,
            Err(err) => panic!("split should succeed: {err}"),
        };
        assert_eq!(outcome.kept.len(), 3);
        assert_eq!(outcome.moved.len(), 4);
    }

    #[test]
    fn randomised_round_trip_via_in_tree_lcg() {
        // Deterministic seeded LCG so tests stay reproducible without
        // pulling in `rand` or `proptest` as dev-dependencies.
        let mut state: u64 = 0xdead_beef_cafe_babe;
        let next = |s: &mut u64| -> u64 {
            *s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            *s
        };

        let mut page = fresh_leaf();
        let mut expected: std::collections::BTreeMap<Vec<u8>, Vec<u8>> = Default::default();
        {
            let mut leaf = match LeafPage::open(&mut page) {
                Ok(leaf) => leaf,
                Err(err) => panic!("open should succeed: {err}"),
            };

            // Insert until the page rejects, occasionally tombstoning.
            let mut slot_for_key = std::collections::BTreeMap::new();
            for _ in 0..40 {
                let key_seed = next(&mut state);
                let val_seed = next(&mut state);
                let key = format!("rk-{:08x}", key_seed as u32).into_bytes();
                let value_len = 4 + (val_seed as usize % 32);
                let value = vec![(val_seed & 0xff) as u8; value_len];

                if let Ok(slot) = leaf.insert_inline(&key, &value, 0) {
                    let _ = expected.insert(key.clone(), value);
                    let _ = slot_for_key.insert(key, slot);
                }

                if next(&mut state) % 5 == 0 {
                    if let Some((k, slot)) =
                        slot_for_key.iter().next().map(|(k, s)| (k.clone(), *s))
                    {
                        let _ = leaf.tombstone(slot);
                        let _ = expected.remove(&k);
                        let _ = slot_for_key.remove(&k);
                    }
                }
            }
        }

        // Compact and verify every expected key is still present and reads back.
        let outcome = match super::compact_leaf(&mut page) {
            Ok(outcome) => outcome,
            Err(err) => panic!("compact should succeed: {err}"),
        };

        let leaf = match LeafPage::open(&mut page) {
            Ok(leaf) => leaf,
            Err(err) => panic!("open should succeed: {err}"),
        };
        assert_eq!(outcome.remap.len(), expected.len());

        for (key, _old, new_slot_id) in &outcome.remap {
            let view = match leaf.read_record(*new_slot_id, key) {
                Ok(Some(view)) => view,
                Ok(None) => panic!("compacted record missing"),
                Err(err) => panic!("read should succeed: {err}"),
            };
            let expected_value = match expected.get(key) {
                Some(value) => value,
                None => panic!("key absent from expected map"),
            };
            match view {
                RecordView::Inline { value, .. } => assert_eq!(value, expected_value.as_slice()),
                RecordView::Overflow { .. } => panic!("inline record decoded as overflow"),
            }
        }
    }
}
