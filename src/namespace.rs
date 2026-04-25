// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Named-namespace handle for the v0.7 engine.
//!
//! A [`Namespace`] is a cheap clone-able handle scoped to one named
//! namespace inside a single [`crate::Emdb`] file. Each named namespace has
//! its own keymap, leaf chain, bloom filter, and record count — they are
//! fully isolated from each other and from the default namespace, so a
//! [`Namespace`] insert does not collide with an `Emdb::insert` of the same
//! key bytes.
//!
//! ## Lifetime
//!
//! Handles are produced by [`crate::Emdb::namespace`]. They are valid for the
//! lifetime of the `Emdb` handle they were derived from (cloning the
//! underlying file lock through the inner `Arc`). Dropping a `Namespace`
//! does not drop the namespace's data; use
//! [`crate::Emdb::drop_namespace`] for that.
//!
//! ## v0.6 path
//!
//! Named namespaces are a v0.7-only feature: the v0.6 page format has no
//! namespace catalog, so calling [`crate::Emdb::namespace`] on a v0.6
//! handle returns [`crate::Error::InvalidConfig`].

use std::sync::Arc;

use crate::db::V07Inner;
use crate::storage::v4::engine::Engine;
use crate::Result;

/// A handle scoped to one named namespace inside a [`crate::Emdb`] file.
///
/// Cheap to clone (two `Arc` bumps). Send + Sync — share between threads.
#[derive(Clone)]
pub struct Namespace {
    inner: Arc<V07Inner>,
    ns_id: u32,
    name: Box<str>,
}

impl Namespace {
    pub(crate) fn new(inner: Arc<V07Inner>, ns_id: u32, name: Box<str>) -> Self {
        Self { inner, ns_id, name }
    }

    fn engine(&self) -> &Engine {
        &self.inner.engine
    }

    /// The name this handle was created for.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Insert or replace a key/value pair in this namespace.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from the WAL or page store.
    pub fn insert(&self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Result<()> {
        let key = key.into();
        let value = value.into();
        self.engine().insert(self.ns_id, &key, &value, 0)
    }

    /// Fetch a value by key.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from the page store.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        let key = key.as_ref();
        let cached = self.engine().get(self.ns_id, key)?;
        Ok(cached.map(|c| c.value.to_vec()))
    }

    /// Remove a key. Returns the previous value, if any.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from the WAL or page store.
    pub fn remove(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        let key = key.as_ref();
        let previous = self.engine().get(self.ns_id, key)?;
        let _did = self.engine().remove(self.ns_id, key)?;
        Ok(previous.map(|c| c.value.to_vec()))
    }

    /// Returns whether the key has a live record.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from the page store.
    pub fn contains_key(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        Ok(self.engine().get(self.ns_id, key.as_ref())?.is_some())
    }

    /// Number of live records in this namespace.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::LockPoisoned`] on a poisoned lock.
    pub fn len(&self) -> Result<usize> {
        let count = self.engine().record_count(self.ns_id)?;
        usize::try_from(count).map_err(|_| {
            crate::Error::InvalidConfig("namespace record count exceeds usize on this target")
        })
    }

    /// Returns whether the namespace has zero live records.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::len`].
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Drop every record in this namespace. The namespace itself remains
    /// registered in the catalog; subsequent inserts allocate fresh leaf
    /// pages.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from the WAL or page store.
    pub fn clear(&self) -> Result<()> {
        self.engine().clear_namespace(self.ns_id)
    }

    /// Materialise every live record as `(key, value)` pairs. Walks the
    /// namespace's leaf chain so the result reflects what is on disk plus
    /// the latest in-memory updates.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from the page store.
    pub fn iter(&self) -> Result<NamespaceIter> {
        let snapshot = self.engine().collect_records(self.ns_id)?;
        Ok(NamespaceIter {
            inner: snapshot.into_iter(),
        })
    }

    /// Materialise every live key. Convenience wrapper over [`Self::iter`].
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::iter`].
    pub fn keys(&self) -> Result<NamespaceKeyIter> {
        let snapshot = self.engine().collect_records(self.ns_id)?;
        Ok(NamespaceKeyIter {
            inner: snapshot.into_iter(),
        })
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
