// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Lock-free publication of the journal's read mapping.
//!
//! The store replaces its `Mmap` whenever the journal grows past the
//! mapped length and when compaction swaps files. Before 1.0.3 the
//! mapping sat behind a `RwLock<Arc<Mmap>>`, so every `get` took the
//! lock and bumped the `Arc` refcount: two read-modify-writes on cache
//! lines shared by every reader thread.
//!
//! [`MmapCell`] publishes the current mapping through an atomic
//! pointer instead. A reader pins the epoch ([`crossbeam_epoch::pin`]),
//! loads the pointer and borrows the mapping for as long as the guard
//! lives, touching no shared counter. A replaced mapping moves to a
//! retired list owned by the cell and is released after every reader
//! that could still see it has unpinned. Dropping the cell releases
//! every mapping it still owns immediately, so no mapping of the file
//! outlives the store.

use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::{Arc, Weak};

use crossbeam_epoch::Guard;
use memmap2::Mmap;
use parking_lot::Mutex;

/// Mappings that have been replaced but may still be borrowed by a
/// pinned reader, keyed by a retirement id.
type RetiredList = Mutex<Vec<(u64, Arc<Mmap>)>>;

/// Atomically replaceable read mapping.
pub(crate) struct MmapCell {
    /// `Arc::into_raw` of the current mapping. The pointer owns one
    /// strong count, released when the mapping is replaced (the count
    /// moves into `retired`) or when the cell drops.
    current: AtomicPtr<Mmap>,
    retired: Arc<RetiredList>,
    next_retire_id: std::sync::atomic::AtomicU64,
}

impl std::fmt::Debug for MmapCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MmapCell")
            .field("retired", &self.retired.lock().len())
            .finish_non_exhaustive()
    }
}

/// A mapping borrowed from an [`MmapCell`]. Valid while both the cell
/// and the epoch guard it was loaded under are alive.
#[derive(Clone, Copy)]
pub(crate) struct MmapView<'a> {
    mmap: &'a Mmap,
}

impl<'a> MmapView<'a> {
    /// The mapped bytes.
    #[inline]
    pub(crate) fn bytes(&self) -> &'a [u8] {
        self.mmap
    }

    /// Take a strong reference to the mapping, for values that must
    /// outlive the guard (zero-copy [`crate::ValueRef`]s).
    pub(crate) fn to_arc(self) -> Arc<Mmap> {
        let ptr: *const Mmap = self.mmap;
        // SAFETY: `ptr` came from `Arc::into_raw` in `MmapCell::new` or
        // `MmapCell::store` (the view is only built from the cell's
        // `current` pointer). The cell or its retired list holds a
        // strong count for as long as this view can exist (see
        // `MmapCell::load`), so the count is at least 1 here and
        // incrementing it before `from_raw` leaves the cell's own
        // count untouched.
        unsafe {
            Arc::increment_strong_count(ptr);
            Arc::from_raw(ptr)
        }
    }
}

impl MmapCell {
    /// Publish `mmap` as the initial mapping.
    pub(crate) fn new(mmap: Arc<Mmap>) -> Self {
        Self {
            current: AtomicPtr::new(Arc::into_raw(mmap).cast_mut()),
            retired: Arc::new(Mutex::new(Vec::new())),
            next_retire_id: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Borrow the current mapping without touching any shared counter.
    #[inline]
    pub(crate) fn load<'a>(&'a self, _guard: &'a Guard) -> MmapView<'a> {
        let ptr = self.current.load(Ordering::Acquire);
        // SAFETY: `ptr` is non-null and points at a live `Mmap` inside
        // an `Arc` allocation: it is the `into_raw` pointer of the
        // current mapping, whose strong count `current` owns. When a
        // `store` replaces it, that count moves into `retired` and is
        // released only by a closure deferred on the epoch collector
        // after the swap, which runs once every guard pinned before it
        // was scheduled has been dropped; `_guard` is such a guard if
        // this load saw the old pointer. When the cell itself drops,
        // the `'a` borrow of `self` has ended, so no view remains.
        let mmap = unsafe { &*ptr };
        MmapView { mmap }
    }

    /// Clone the current mapping's `Arc`. Pays one refcount increment;
    /// meant for whole-file scans, not per-record reads.
    pub(crate) fn load_full(&self) -> Arc<Mmap> {
        let guard = crossbeam_epoch::pin();
        self.load(&guard).to_arc()
    }

    /// Replace the current mapping. Readers that already borrowed the
    /// old one keep using it until they unpin.
    pub(crate) fn store(&self, mmap: Arc<Mmap>) {
        let new_ptr = Arc::into_raw(mmap).cast_mut();
        let old_ptr = self.current.swap(new_ptr, Ordering::AcqRel);
        // SAFETY: `old_ptr` was the cell's `into_raw` pointer and the
        // swap transferred its strong count to us; nothing else will
        // reclaim it.
        let old = unsafe { Arc::from_raw(old_ptr) };
        let id = self.next_retire_id.fetch_add(1, Ordering::Relaxed);
        self.retired.lock().push((id, old));

        let retired: Weak<RetiredList> = Arc::downgrade(&self.retired);
        let guard = crossbeam_epoch::pin();
        guard.defer(move || {
            if let Some(list) = retired.upgrade() {
                list.lock().retain(|(entry, _)| *entry != id);
            }
        });
        guard.flush();
    }
}

impl Drop for MmapCell {
    fn drop(&mut self) {
        let ptr = *self.current.get_mut();
        // SAFETY: `ptr` is the cell's `into_raw` pointer and owns one
        // strong count. `&mut self` means no view is alive.
        drop(unsafe { Arc::from_raw(ptr) });
        // Retired mappings are released with `self.retired` when the
        // last strong reference (this one) drops; pending epoch
        // closures hold only `Weak` references.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn mapping(bytes: &[u8]) -> (std::path::PathBuf, Arc<Mmap>) {
        let mut path = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        path.push(format!("emdb-mmap-cell-{}-{nanos}", std::process::id()));
        let mut file = std::fs::File::create(&path).unwrap();
        file.write_all(bytes).unwrap();
        drop(file);
        let file = std::fs::File::open(&path).unwrap();
        // SAFETY: test-owned temp file, not modified while mapped.
        let mmap = unsafe { Mmap::map(&file).unwrap() };
        (path, Arc::new(mmap))
    }

    #[test]
    fn test_mmap_cell_store_publishes_new_mapping() {
        let (p1, m1) = mapping(b"one");
        let (p2, m2) = mapping(b"two!");
        let cell = MmapCell::new(m1);
        let guard = crossbeam_epoch::pin();
        let before = cell.load(&guard);
        cell.store(m2);
        // The borrowed view stays valid after the swap.
        assert_eq!(before.bytes(), b"one");
        assert_eq!(cell.load(&guard).bytes(), b"two!");
        drop(guard);
        assert_eq!(&cell.load_full()[..], b"two!");
        drop(cell);
        let _ = std::fs::remove_file(p1);
        let _ = std::fs::remove_file(p2);
    }

    #[test]
    fn test_mmap_cell_drop_releases_every_mapping() {
        let (p1, m1) = mapping(b"a");
        let (p2, m2) = mapping(b"b");
        let weak1 = Arc::downgrade(&m1);
        let weak2 = Arc::downgrade(&m2);
        let cell = MmapCell::new(m1);
        cell.store(m2);
        drop(cell);
        assert!(
            weak1.upgrade().is_none(),
            "retired mapping outlived the cell"
        );
        assert!(
            weak2.upgrade().is_none(),
            "current mapping outlived the cell"
        );
        let _ = std::fs::remove_file(p1);
        let _ = std::fs::remove_file(p2);
    }

    #[test]
    fn test_mmap_cell_to_arc_outlives_guard_and_cell() {
        let (p1, m1) = mapping(b"keep");
        let cell = MmapCell::new(m1);
        let held = {
            let guard = crossbeam_epoch::pin();
            cell.load(&guard).to_arc()
        };
        drop(cell);
        assert_eq!(&held[..], b"keep");
        drop(held);
        let _ = std::fs::remove_file(p1);
    }

    #[test]
    fn test_mmap_cell_retired_mappings_are_reclaimed() {
        let (p0, m0) = mapping(b"0");
        let cell = MmapCell::new(m0);
        let mut paths = vec![p0];
        for i in 0..200_u32 {
            let (p, m) = mapping(&i.to_le_bytes());
            paths.push(p);
            cell.store(m);
        }
        // Drive the collector until it catches up. Other tests may pin
        // concurrently, which only delays reclamation.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while cell.retired.lock().len() >= 200 && std::time::Instant::now() < deadline {
            crossbeam_epoch::pin().flush();
        }
        assert!(cell.retired.lock().len() < 200);
        drop(cell);
        for p in paths {
            let _ = std::fs::remove_file(p);
        }
    }
}
