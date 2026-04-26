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
    pub fn insert(
        &self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Result<()> {
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

    /// Materialise every live record as `(key, value)` pairs.
    pub fn iter(&self) -> Result<NamespaceIter> {
        let snapshot = self.engine().collect_records(self.ns_id)?;
        Ok(NamespaceIter {
            inner: snapshot.into_iter(),
        })
    }

    /// Iterate every live key.
    pub fn keys(&self) -> Result<NamespaceKeyIter> {
        let snapshot = self.engine().collect_records(self.ns_id)?;
        Ok(NamespaceKeyIter {
            inner: snapshot.into_iter(),
        })
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
}

/// Iterator over `(key, value)` pairs from [`Namespace::iter`].
pub struct NamespaceIter {
    inner: std::vec::IntoIter<(Vec<u8>, Vec<u8>, u64)>,
}

impl Iterator for NamespaceIter {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k, v, _)| (k, v))
    }
}

/// Iterator over keys from [`Namespace::keys`].
pub struct NamespaceKeyIter {
    inner: std::vec::IntoIter<(Vec<u8>, Vec<u8>, u64)>,
}

impl Iterator for NamespaceKeyIter {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k, _, _)| k)
    }
}
