// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Named-namespace handle.

use std::sync::Arc;

use crate::db::Inner;
use crate::storage::Engine;
use crate::Result;

/// Cheap-clone handle scoped to one named namespace inside a single
/// [`crate::Emdb`].
#[derive(Clone)]
pub struct Namespace {
    inner: Arc<Inner>,
    ns_id: u32,
    name: Box<str>,
}

impl Namespace {
    pub(crate) fn new(inner: Arc<Inner>, ns_id: u32, name: Box<str>) -> Self {
        Self { inner, ns_id, name }
    }

    fn engine(&self) -> &Engine {
        &self.inner.engine
    }

    /// Name this handle was created for.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Insert or replace a key/value pair.
    pub fn insert(&self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Result<()> {
        let key = key.into();
        let value = value.into();
        self.engine().insert(self.ns_id, &key, &value, 0)
    }

    /// Insert many key/value pairs in one writer-locked pass.
    pub fn insert_many<I, K, V>(&self, items: I) -> Result<()>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let owned: Vec<(Vec<u8>, Vec<u8>, u64)> = items
            .into_iter()
            .map(|(k, v)| (k.as_ref().to_vec(), v.as_ref().to_vec(), 0))
            .collect();
        self.engine().insert_many(self.ns_id, owned)
    }

    /// Fetch a value by key.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        self.engine().get(self.ns_id, key.as_ref())
    }

    /// Zero-copy fetch: returns a [`crate::ValueRef`] reading
    /// directly from the kernel-managed mmap region. See
    /// [`crate::Emdb::get_zerocopy`] for the trade-offs and the
    /// encrypted-database fallback behaviour.
    ///
    /// # Errors
    ///
    /// Same as [`Self::get`].
    pub fn get_zerocopy(&self, key: impl AsRef<[u8]>) -> Result<Option<crate::ValueRef>> {
        Ok(self
            .engine()
            .get_zerocopy(self.ns_id, key.as_ref())?
            .map(|(v, _)| v))
    }

    /// Remove a key, returning the previous value if any.
    pub fn remove(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        self.engine().remove(self.ns_id, key.as_ref())
    }

    /// Returns whether the key has a live record.
    pub fn contains_key(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        Ok(self.engine().get(self.ns_id, key.as_ref())?.is_some())
    }

    /// Live record count.
    pub fn len(&self) -> Result<usize> {
        let count = self.engine().record_count(self.ns_id)?;
        usize::try_from(count).map_err(|_| {
            crate::Error::InvalidConfig("namespace record count exceeds usize on this target")
        })
    }

    /// True iff the namespace has zero live records.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Drop every record in this namespace.
    pub fn clear(&self) -> Result<()> {
        self.engine().clear_namespace(self.ns_id)
    }

    /// Iterate over `(key, value)` pairs in this namespace.
    ///
    /// The iterator snapshots live record offsets at the time of
    /// this call and decodes records lazily on `next()`. Memory
    /// use scales with offset count, not total value size.
    pub fn iter(&self) -> Result<NamespaceIter> {
        let offsets = self.engine().snapshot_offsets(self.ns_id)?;
        Ok(NamespaceIter::new(Arc::clone(&self.inner), offsets))
    }

    /// Iterate every live key in this namespace. Same lazy
    /// semantics as [`Self::iter`].
    pub fn keys(&self) -> Result<NamespaceKeyIter> {
        let offsets = self.engine().snapshot_offsets(self.ns_id)?;
        Ok(NamespaceKeyIter::new(Arc::clone(&self.inner), offsets))
    }

    /// Range-scan keys in this namespace, returning `(key, value)`
    /// pairs in lexicographic order. Requires the database to have
    /// been opened with [`crate::EmdbBuilder::enable_range_scans`]`(true)`.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::InvalidConfig`] if range scans were not
    /// enabled at open time.
    pub fn range<R>(&self, range: R) -> Result<Vec<(Vec<u8>, Vec<u8>)>>
    where
        R: std::ops::RangeBounds<Vec<u8>>,
    {
        self.engine().range_scan(self.ns_id, range)
    }

    /// Range-scan all keys with a given prefix in this namespace.
    ///
    /// # Errors
    ///
    /// Same as [`Self::range`].
    pub fn range_prefix(&self, prefix: impl AsRef<[u8]>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let prefix = prefix.as_ref();
        let start = prefix.to_vec();
        match crate::db::next_prefix(prefix) {
            Some(end) => self.range(start..end),
            None => self.range(start..),
        }
    }

    /// Streaming range scan: same semantics as [`Self::range`] but
    /// returns an iterator that decodes values lazily on `next()`.
    /// See [`crate::Emdb::range_iter`] for details.
    ///
    /// # Errors
    ///
    /// Same as [`Self::range`].
    pub fn range_iter<R>(&self, range: R) -> Result<NamespaceRangeIter>
    where
        R: std::ops::RangeBounds<Vec<u8>>,
    {
        let pairs = self.engine().snapshot_range_offsets(self.ns_id, range)?;
        Ok(NamespaceRangeIter::new(Arc::clone(&self.inner), pairs))
    }

    /// Streaming variant of [`Self::range_prefix`].
    ///
    /// # Errors
    ///
    /// Same as [`Self::range_iter`].
    pub fn range_prefix_iter(&self, prefix: impl AsRef<[u8]>) -> Result<NamespaceRangeIter> {
        let prefix = prefix.as_ref();
        let start = prefix.to_vec();
        match crate::db::next_prefix(prefix) {
            Some(end) => self.range_iter(start..end),
            None => self.range_iter(start..),
        }
    }

    /// Streaming iterator over keys at or after `start` in this
    /// namespace, in lexicographic order. Mirrors
    /// [`crate::Emdb::iter_from`] for named namespaces.
    ///
    /// # Errors
    ///
    /// Same as [`Self::range_iter`].
    pub fn iter_from(&self, start: impl AsRef<[u8]>) -> Result<NamespaceRangeIter> {
        self.range_iter(start.as_ref().to_vec()..)
    }

    /// Streaming iterator over keys strictly after `start` in this
    /// namespace. Mirrors [`crate::Emdb::iter_after`].
    ///
    /// # Errors
    ///
    /// Same as [`Self::range_iter`].
    pub fn iter_after(&self, start: impl AsRef<[u8]>) -> Result<NamespaceRangeIter> {
        let start = start.as_ref().to_vec();
        self.range_iter((std::ops::Bound::Excluded(start), std::ops::Bound::Unbounded))
    }
}

/// Iterator over `(key, value)` pairs from [`Namespace::iter`].
///
/// Decodes records lazily from a snapshot of offsets captured at
/// `iter()` time.
pub struct NamespaceIter {
    inner: Arc<Inner>,
    offsets: std::vec::IntoIter<u64>,
}

impl NamespaceIter {
    fn new(inner: Arc<Inner>, offsets: Vec<u64>) -> Self {
        Self {
            inner,
            offsets: offsets.into_iter(),
        }
    }
}

impl Iterator for NamespaceIter {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        for offset in self.offsets.by_ref() {
            match self.inner.engine.decode_owned_at(offset) {
                Ok(Some((key, value, _))) => return Some((key, value)),
                Ok(None) => continue,
                Err(_) => continue,
            }
        }
        None
    }
}

/// Iterator over keys from [`Namespace::keys`].
pub struct NamespaceKeyIter {
    inner: Arc<Inner>,
    offsets: std::vec::IntoIter<u64>,
}

impl NamespaceKeyIter {
    fn new(inner: Arc<Inner>, offsets: Vec<u64>) -> Self {
        Self {
            inner,
            offsets: offsets.into_iter(),
        }
    }
}

impl Iterator for NamespaceKeyIter {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        for offset in self.offsets.by_ref() {
            match self.inner.engine.decode_owned_at(offset) {
                Ok(Some((key, _value, _))) => return Some(key),
                Ok(None) => continue,
                Err(_) => continue,
            }
        }
        None
    }
}

/// Streaming range iterator returned by
/// [`Namespace::range_iter`] / [`Namespace::range_prefix_iter`].
pub struct NamespaceRangeIter {
    inner: Arc<Inner>,
    pairs: std::vec::IntoIter<(Vec<u8>, u64)>,
}

impl NamespaceRangeIter {
    fn new(inner: Arc<Inner>, pairs: Vec<(Vec<u8>, u64)>) -> Self {
        Self {
            inner,
            pairs: pairs.into_iter(),
        }
    }
}

impl Iterator for NamespaceRangeIter {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        for (key, offset) in self.pairs.by_ref() {
            match self.inner.engine.read_value_with_meta_at(offset, &key) {
                Ok(Some((value, _expires))) => return Some((key, value)),
                Ok(None) => continue,
                Err(_) => continue,
            }
        }
        None
    }
}
