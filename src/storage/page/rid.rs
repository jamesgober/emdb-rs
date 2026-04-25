// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Record Identifier (Rid) for the v0.7 storage engine.
//!
//! A `Rid` packs a `(page_id, slot_id)` pair into a single 64-bit value so the
//! in-memory keymap can store one entry as eight bytes. Layout:
//!
//! ```text
//!   bits 63..16 = page_id (48 bits) — addresses up to 2^48 pages
//!   bits 15..0  = slot_id (16 bits) — addresses up to 65 535 slots per page
//! ```
//!
//! 2^48 pages × 4 KB per page = 1 PB per file. 65 535 slots is far more than
//! any 4 KB page can ever hold (a slot is 8 bytes, so the slot array alone
//! would exceed the page at ~500 slots). The remaining bits are headroom.

use crate::storage::page::PageId;

/// Number of bits in a [`Rid`] reserved for the slot id.
pub(crate) const SLOT_BITS: u32 = 16;

/// Mask covering the slot id portion of a packed [`Rid`] value.
pub(crate) const SLOT_MASK: u64 = (1_u64 << SLOT_BITS) - 1;

/// Maximum representable page id given [`SLOT_BITS`].
pub(crate) const MAX_PAGE_ID: u64 = (1_u64 << (64 - SLOT_BITS)) - 1;

/// Maximum representable slot id given [`SLOT_BITS`].
pub(crate) const MAX_SLOT_ID: u16 = u16::MAX;

/// Stable identifier for a record stored in the v0.7 page file.
///
/// `Rid` is `Copy` and fits in a single CPU register. Cloning, hashing, and
/// equality comparison are all primitive-typed operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub(crate) struct Rid(u64);

impl Rid {
    /// Construct a [`Rid`] from a `(page_id, slot_id)` pair.
    ///
    /// Returns `None` when `page_id` exceeds [`MAX_PAGE_ID`]. The slot id is
    /// `u16` so it cannot overflow.
    #[must_use]
    pub(crate) const fn try_new(page_id: u64, slot_id: u16) -> Option<Self> {
        if page_id > MAX_PAGE_ID {
            return None;
        }
        Some(Self((page_id << SLOT_BITS) | (slot_id as u64)))
    }

    /// Construct a [`Rid`] from a `(page_id, slot_id)` pair.
    ///
    /// # Panics
    ///
    /// Debug-panics when `page_id` exceeds [`MAX_PAGE_ID`]. Release builds
    /// silently truncate the upper bits — callers are expected to validate
    /// `page_id` upstream when accepting untrusted input.
    #[must_use]
    pub(crate) const fn new(page_id: u64, slot_id: u16) -> Self {
        debug_assert!(page_id <= MAX_PAGE_ID);
        Self((page_id << SLOT_BITS) | (slot_id as u64))
    }

    /// Return the page id portion.
    #[must_use]
    pub(crate) const fn page_id(self) -> PageId {
        PageId::new(self.0 >> SLOT_BITS)
    }

    /// Return the slot id portion.
    #[must_use]
    pub(crate) const fn slot_id(self) -> u16 {
        (self.0 & SLOT_MASK) as u16
    }

    /// Return the raw 64-bit packed value. Used by serialisation.
    #[must_use]
    pub(crate) const fn raw(self) -> u64 {
        self.0
    }

    /// Reconstruct a [`Rid`] from a raw 64-bit value. Used by deserialisation.
    #[must_use]
    pub(crate) const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }
}

#[cfg(test)]
mod tests {
    use super::{Rid, MAX_PAGE_ID, MAX_SLOT_ID};

    #[test]
    fn round_trip_packs_and_unpacks() {
        let rid = Rid::new(1234, 56);
        assert_eq!(rid.page_id().get(), 1234);
        assert_eq!(rid.slot_id(), 56);
    }

    #[test]
    fn round_trip_at_boundaries() {
        let zero = Rid::new(0, 0);
        assert_eq!(zero.page_id().get(), 0);
        assert_eq!(zero.slot_id(), 0);

        let max = Rid::new(MAX_PAGE_ID, MAX_SLOT_ID);
        assert_eq!(max.page_id().get(), MAX_PAGE_ID);
        assert_eq!(max.slot_id(), MAX_SLOT_ID);
    }

    #[test]
    fn raw_round_trip_preserves_value() {
        let rid = Rid::new(0x0001_0203_0405, 0xABCD);
        let raw = rid.raw();
        let decoded = Rid::from_raw(raw);
        assert_eq!(decoded, rid);
    }

    #[test]
    fn try_new_rejects_oversized_page_id() {
        let too_big = Rid::try_new(MAX_PAGE_ID + 1, 0);
        assert!(too_big.is_none());
    }

    #[test]
    fn try_new_accepts_max_page_id() {
        let ok = Rid::try_new(MAX_PAGE_ID, MAX_SLOT_ID);
        assert!(ok.is_some());
    }

    #[test]
    fn rid_is_copy_and_eight_bytes() {
        // Rid is sometimes embedded in larger structures; size is part of the
        // contract because the keymap memory budget assumes 16 bytes per entry
        // (8-byte hash + 8-byte Rid).
        assert_eq!(core::mem::size_of::<Rid>(), 8);
        let a = Rid::new(7, 9);
        let b = a;
        assert_eq!(a, b);
    }

    #[test]
    fn ordering_matches_page_id_then_slot_id() {
        let a = Rid::new(1, 0);
        let b = Rid::new(1, 1);
        let c = Rid::new(2, 0);
        assert!(a < b);
        assert!(b < c);
        assert!(a < c);
    }
}
