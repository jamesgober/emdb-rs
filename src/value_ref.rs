// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Zero-copy value handles returned by [`crate::Emdb::get_zerocopy`]
//! and [`crate::Namespace::get_zerocopy`].
//!
//! A [`ValueRef`] is either a slice into the kernel-managed mmap
//! region (the plaintext-database fast path) or an owned `Vec<u8>`
//! holding the decrypted plaintext (the encrypted-database fallback,
//! since AEAD decryption necessarily produces fresh bytes).
//!
//! Either way, the type implements [`Deref<Target=[u8]>`] so callers
//! can treat it as a byte slice. The mmap variant keeps the
//! `Arc<Mmap>` alive for as long as the [`ValueRef`] exists, which
//! means an in-flight zero-copy reader does not pin the file against
//! growth (the writer swaps to a new mapping under an `Arc` when the
//! file grows; old readers continue with the old mapping until they
//! release it — see [`crate::storage::store`]).

use std::ops::Deref;
use std::sync::Arc;

use memmap2::Mmap;

/// Zero-copy reference to a value stored in the database.
///
/// The bytes can be accessed via [`Deref::deref`] (i.e. `&*value`),
/// [`Self::as_slice`], or by comparing directly with another byte
/// slice (the type implements `PartialEq<[u8]>`,
/// `PartialEq<Vec<u8>>`, and `PartialEq<&[u8]>`).
///
/// Two construction shapes:
///
/// - **mmap-backed** (unencrypted databases): the slice points
///   directly into the kernel-managed memory mapping. Reading is a
///   pointer dereference; no allocation, no copy. Holding this
///   variant keeps the underlying mapping alive (via `Arc<Mmap>`)
///   even if the writer grows the file and swaps the engine's
///   primary mapping in the meantime.
/// - **owned** (encrypted databases): AEAD decryption produces a
///   fresh `Vec<u8>` of plaintext, which the [`ValueRef`] takes
///   ownership of. Behaves identically to the mmap-backed variant
///   from the caller's perspective; the only difference is that
///   the bytes were copied once during decrypt.
///
/// # Examples
///
/// ```rust,no_run
/// use emdb::Emdb;
///
/// let db = Emdb::open_in_memory();
/// db.insert("k", "v")?;
/// if let Some(value) = db.get_zerocopy("k")? {
///     // `value` derefs to &[u8].
///     assert_eq!(&*value, b"v");
///     // Or compare directly:
///     assert!(value == *b"v");
/// }
/// # Ok::<(), emdb::Error>(())
/// ```
#[derive(Debug)]
pub struct ValueRef {
    repr: Repr,
}

#[derive(Debug)]
enum Repr {
    /// Bytes live inside the mmap; we hold the Arc so the mapping
    /// can't be unmapped while the slice is alive. `range` is the
    /// half-open `[start, end)` byte range within the mmap.
    Mmap {
        mmap: Arc<Mmap>,
        range: std::ops::Range<usize>,
    },
    /// Bytes were produced by an AEAD decrypt or otherwise
    /// allocated; we own the `Vec` directly.
    Owned(Vec<u8>),
}

impl ValueRef {
    /// Construct an mmap-backed reference.
    pub(crate) fn from_mmap(mmap: Arc<Mmap>, range: std::ops::Range<usize>) -> Self {
        debug_assert!(range.end <= mmap.len(), "ValueRef range past mmap end");
        Self {
            repr: Repr::Mmap { mmap, range },
        }
    }

    /// Construct an owned reference.
    pub(crate) fn from_owned(bytes: Vec<u8>) -> Self {
        Self {
            repr: Repr::Owned(bytes),
        }
    }

    /// Length of the value in bytes.
    #[must_use]
    pub fn len(&self) -> usize {
        match &self.repr {
            Repr::Mmap { range, .. } => range.end - range.start,
            Repr::Owned(v) => v.len(),
        }
    }

    /// Whether the value has zero bytes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Borrow the value as a byte slice.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        match &self.repr {
            Repr::Mmap { mmap, range } => &mmap[range.clone()],
            Repr::Owned(v) => v.as_slice(),
        }
    }

    /// Take ownership of the value as an allocated `Vec<u8>`.
    ///
    /// For the owned variant this is a move (no copy). For the
    /// mmap-backed variant this allocates and copies the bytes —
    /// use [`Self::as_slice`] / [`Deref::deref`] when you only
    /// need to read.
    #[must_use]
    pub fn into_vec(self) -> Vec<u8> {
        match self.repr {
            Repr::Owned(v) => v,
            Repr::Mmap { mmap, range } => mmap[range].to_vec(),
        }
    }
}

impl Deref for ValueRef {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsRef<[u8]> for ValueRef {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl PartialEq for ValueRef {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for ValueRef {}

impl PartialEq<[u8]> for ValueRef {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_slice() == other
    }
}

impl PartialEq<&[u8]> for ValueRef {
    fn eq(&self, other: &&[u8]) -> bool {
        self.as_slice() == *other
    }
}

impl PartialEq<Vec<u8>> for ValueRef {
    fn eq(&self, other: &Vec<u8>) -> bool {
        self.as_slice() == other.as_slice()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owned_round_trips_bytes_unchanged() {
        let v = ValueRef::from_owned(vec![1, 2, 3]);
        assert_eq!(v.len(), 3);
        assert!(!v.is_empty());
        assert_eq!(v.as_slice(), &[1, 2, 3]);
        assert_eq!(&*v, &[1, 2, 3][..]);
        assert_eq!(v.into_vec(), vec![1, 2, 3]);
    }

    #[test]
    fn empty_owned_reports_empty() {
        let v = ValueRef::from_owned(Vec::new());
        assert_eq!(v.len(), 0);
        assert!(v.is_empty());
    }

    #[test]
    fn equality_against_byte_slice() {
        let v = ValueRef::from_owned(b"hello".to_vec());
        assert!(v == *b"hello");
        let s: &[u8] = b"hello";
        assert!(v == s);
        assert!(v == b"hello".to_vec());
    }
}
