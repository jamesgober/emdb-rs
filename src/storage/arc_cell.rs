// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Lock-free publication of shared, replaceable values.
//!
//! The store replaces its read mapping whenever the journal grows past
//! the mapped length and when compaction swaps files, and compaction
//! replaces the default namespace's runtime. Before 1.0.3 the mapping
//! sat behind a `RwLock<Arc<Mmap>>`, so every `get` took the lock and
//! bumped the `Arc` refcount: two read-modify-writes on cache lines
//! shared by every reader thread.
//!
//! [`ArcCell`] publishes the current value through an atomic pointer
//! instead. A reader pins the epoch ([`crossbeam_epoch::pin`]), loads
//! the pointer and borrows the value for as long as the guard lives,
//! touching no shared counter. A replaced value moves to a retired
//! list owned by the cell and is released after every reader that
//! could still see it has unpinned. Dropping the cell releases every
//! value it still owns immediately, so no mapping of the file outlives
//! the store.

use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use crossbeam_epoch::Guard;
use memmap2::Mmap;
use parking_lot::Mutex;

/// Values that have been replaced but may still be borrowed by a
/// pinned reader, keyed by a retirement id.
type RetiredList<T> = Mutex<Vec<(u64, Arc<T>)>>;

/// Atomically replaceable shared value.
pub(crate) struct ArcCell<T: Send + Sync + 'static> {
    /// `Arc::into_raw` of the current value. The pointer owns one
    /// strong count, released when the value is replaced (the count
    /// moves into `retired`) or when the cell drops.
    current: AtomicPtr<T>,
    retired: Arc<RetiredList<T>>,
    next_retire_id: AtomicU64,
}

impl<T: Send + Sync + 'static> std::fmt::Debug for ArcCell<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArcCell")
            .field("retired", &self.retired.lock().len())
            .finish_non_exhaustive()
    }
}

/// A value borrowed from an [`ArcCell`]. Valid while both the cell
/// and the epoch guard it was loaded under are alive.
pub(crate) struct CellRef<'a, T> {
    value: &'a T,
}

impl<T> Clone for CellRef<'_, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for CellRef<'_, T> {}

/// The journal mapping borrowed from the store's cell.
pub(crate) type MmapView<'a> = CellRef<'a, Mmap>;

impl<'a, T> CellRef<'a, T> {
    /// The borrowed value.
    #[inline]
    pub(crate) fn get(&self) -> &'a T {
        self.value
    }

    /// Take a strong reference to the value, for uses that must
    /// outlive the guard (zero-copy [`crate::ValueRef`]s, iterators).
    pub(crate) fn to_arc(self) -> Arc<T> {
        let ptr: *const T = self.value;
        // SAFETY: `ptr` came from `Arc::into_raw` in `ArcCell::new`,
        // `ArcCell::store` or `ArcCell::set_mut` (a `CellRef` is only
        // built by `ArcCell::load` from the cell's `current` pointer).
        // The cell or its retired list holds a strong count for as
        // long as this reference can exist (see `ArcCell::load`), so
        // the count is at least 1 here and incrementing it before
        // `from_raw` leaves the cell's own count untouched.
        unsafe {
            Arc::increment_strong_count(ptr);
            Arc::from_raw(ptr)
        }
    }
}

impl<'a> CellRef<'a, Mmap> {
    /// The mapped bytes.
    #[inline]
    pub(crate) fn bytes(&self) -> &'a [u8] {
        self.value
    }
}

impl<T: Send + Sync + 'static> ArcCell<T> {
    /// Publish `value` as the initial value.
    pub(crate) fn new(value: Arc<T>) -> Self {
        Self {
            current: AtomicPtr::new(Arc::into_raw(value).cast_mut()),
            retired: Arc::new(Mutex::new(Vec::new())),
            next_retire_id: AtomicU64::new(0),
        }
    }

    /// Borrow the current value without touching any shared counter.
    #[inline]
    pub(crate) fn load<'a>(&'a self, _guard: &'a Guard) -> CellRef<'a, T> {
        let ptr = self.current.load(Ordering::Acquire);
        // SAFETY: `ptr` is non-null and points at a live `T` inside an
        // `Arc` allocation: it is the `into_raw` pointer of the current
        // value, whose strong count `current` owns. When `store`
        // replaces it, that count moves into `retired` and is released
        // only by a closure deferred on the epoch collector after the
        // swap, which runs once every guard pinned before it was
        // scheduled has been dropped; `_guard` is such a guard if this
        // load saw the old pointer. `set_mut` and `Drop` take `&mut
        // self`, so they cannot run while the `'a` borrow of `self`
        // this reference carries is alive.
        let value = unsafe { &*ptr };
        CellRef { value }
    }

    /// Borrow the current value without an epoch guard.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that no [`Self::store`] on this cell
    /// runs while the returned reference is alive (for example by
    /// holding a lock that every `store` caller holds exclusively).
    #[inline]
    pub(crate) unsafe fn get_unpinned(&self) -> &T {
        let ptr = self.current.load(Ordering::Acquire);
        // SAFETY: `current` owns a strong count of the value it points
        // at, and gives it up only when `store` swaps the pointer out,
        // which the caller rules out for the reference's lifetime.
        // `set_mut` and `Drop` need `&mut self`, which the `&self`
        // borrow excludes.
        unsafe { &*ptr }
    }

    /// Clone the current value's `Arc`. Pays one refcount increment.
    #[cfg(test)]
    pub(crate) fn load_full(&self) -> Arc<T> {
        let guard = crossbeam_epoch::pin();
        self.load(&guard).to_arc()
    }

    /// Replace the current value. Readers that already borrowed the
    /// old one keep using it until they unpin.
    pub(crate) fn store(&self, value: Arc<T>) {
        let new_ptr = Arc::into_raw(value).cast_mut();
        let old_ptr = self.current.swap(new_ptr, Ordering::AcqRel);
        // SAFETY: `old_ptr` was the cell's `into_raw` pointer and the
        // swap transferred its strong count to us; nothing else will
        // reclaim it.
        let old = unsafe { Arc::from_raw(old_ptr) };
        let id = self.next_retire_id.fetch_add(1, Ordering::Relaxed);
        self.retired.lock().push((id, old));

        let retired: Weak<RetiredList<T>> = Arc::downgrade(&self.retired);
        let guard = crossbeam_epoch::pin();
        guard.defer(move || {
            if let Some(list) = retired.upgrade() {
                list.lock().retain(|(entry, _)| *entry != id);
            }
        });
        guard.flush();
    }

    /// Replace the current value and release the old one and every
    /// retired value now. `&mut self` proves no reader borrows any of
    /// them; the store uses this during open, before the engine is
    /// shared, so a mapping that must be gone before fsys may shrink
    /// the file (Windows refuses while a view is mapped) is gone.
    pub(crate) fn set_mut(&mut self, value: Arc<T>) {
        let new_ptr = Arc::into_raw(value).cast_mut();
        let old_ptr = std::mem::replace(self.current.get_mut(), new_ptr);
        // SAFETY: `old_ptr` was the cell's `into_raw` pointer and owned
        // one strong count, which is released here; `&mut self` means
        // no `CellRef` borrowed from it is alive.
        drop(unsafe { Arc::from_raw(old_ptr) });
        self.retired.lock().clear();
    }
}

impl<T: Send + Sync + 'static> Drop for ArcCell<T> {
    fn drop(&mut self) {
        let ptr = *self.current.get_mut();
        // SAFETY: `ptr` is the cell's `into_raw` pointer and owns one
        // strong count. `&mut self` means no `CellRef` is alive.
        drop(unsafe { Arc::from_raw(ptr) });
        // Release the retired values here, on this thread. Pending
        // epoch closures hold only `Weak` references to the list, but
        // one that is running on another thread right now has upgraded
        // its reference, and dropping our `Arc` alone would then leave
        // the list (and every mapping in it) alive until that closure
        // returns. Clearing under the lock waits for such a closure
        // and releases everything before `drop` returns.
        self.retired.lock().clear();
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
        path.push(format!(
            "emdb-arc-cell-{}-{nanos}-{}",
            std::process::id(),
            bytes.len()
        ));
        let mut file = std::fs::File::create(&path).unwrap();
        file.write_all(bytes).unwrap();
        drop(file);
        let file = std::fs::File::open(&path).unwrap();
        // SAFETY: test-owned temp file, not modified while mapped.
        let mmap = unsafe { Mmap::map(&file).unwrap() };
        (path, Arc::new(mmap))
    }

    #[test]
    fn test_arc_cell_store_publishes_new_mapping() {
        let (p1, m1) = mapping(b"one");
        let (p2, m2) = mapping(b"two!");
        let cell = ArcCell::new(m1);
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
    fn test_arc_cell_drop_releases_every_value() {
        let (p1, m1) = mapping(b"a");
        let (p2, m2) = mapping(b"bb");
        let weak1 = Arc::downgrade(&m1);
        let weak2 = Arc::downgrade(&m2);
        let cell = ArcCell::new(m1);
        cell.store(m2);
        drop(cell);
        assert!(weak1.upgrade().is_none(), "retired value outlived the cell");
        assert!(weak2.upgrade().is_none(), "current value outlived the cell");
        let _ = std::fs::remove_file(p1);
        let _ = std::fs::remove_file(p2);
    }

    #[test]
    fn test_arc_cell_set_mut_releases_old_and_retired_values_now() {
        let a = Arc::new(1_u32);
        let b = Arc::new(2_u32);
        let weak_a = Arc::downgrade(&a);
        let weak_b = Arc::downgrade(&b);
        let mut cell = ArcCell::new(a);
        cell.store(b);
        cell.set_mut(Arc::new(3));
        assert!(weak_a.upgrade().is_none(), "retired value kept");
        assert!(weak_b.upgrade().is_none(), "replaced value kept");
        assert_eq!(*cell.load_full(), 3);
    }

    #[test]
    fn test_arc_cell_to_arc_outlives_guard_and_cell() {
        let (p1, m1) = mapping(b"keep");
        let cell = ArcCell::new(m1);
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
    fn test_arc_cell_retired_values_are_reclaimed() {
        let cell = ArcCell::new(Arc::new(0_u32));
        for i in 0..200_u32 {
            cell.store(Arc::new(i));
        }
        // Drive the collector until it catches up. Other tests may pin
        // concurrently, which only delays reclamation.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while cell.retired.lock().len() >= 200 && std::time::Instant::now() < deadline {
            crossbeam_epoch::pin().flush();
        }
        assert!(cell.retired.lock().len() < 200);
    }
}
