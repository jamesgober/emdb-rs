// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Core in-memory database implementation.

use std::collections::BTreeMap;

#[cfg(feature = "ttl")]
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::builder::EmdbBuilder;
#[cfg(feature = "ttl")]
use crate::ttl::{
    expires_from_ttl, is_expired, now_unix_millis, record_expires_at, record_set_persist,
    remaining_ttl, Ttl,
};
use crate::ttl::{record_into_value, record_new, record_value, Record};
use crate::Result;

/// The primary embedded database handle.
///
/// `Emdb` stores key/value records fully in memory using an ordered map.
/// Keys and values are both opaque byte arrays.
///
/// # Examples
///
/// ```rust
/// use emdb::Emdb;
///
/// let mut db = Emdb::open_in_memory();
/// db.insert("name", "emdb")?;
/// assert_eq!(db.get("name")?, Some(b"emdb".to_vec()));
/// # Ok::<(), emdb::Error>(())
/// ```
#[derive(Debug, Default)]
pub struct Emdb {
    storage: BTreeMap<Vec<u8>, Record>,
    #[cfg(feature = "ttl")]
    default_ttl: Option<Duration>,
}

impl Emdb {
    /// Open a new in-memory database.
    ///
    /// In-memory databases are volatile; all records are dropped when the
    /// instance is dropped.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use emdb::Emdb;
    ///
    /// let db = Emdb::open_in_memory();
    /// assert!(db.is_empty());
    /// ```
    #[must_use]
    pub fn open_in_memory() -> Self {
        Self::default()
    }

    /// Build a database from a builder configuration.
    #[must_use]
    pub(crate) fn from_builder(builder: EmdbBuilder) -> Self {
        #[cfg(not(feature = "ttl"))]
        {
            let _unused_builder = builder;
        }

        Self {
            storage: BTreeMap::new(),
            #[cfg(feature = "ttl")]
            default_ttl: builder.default_ttl,
        }
    }

    /// Create a builder for configuring a new in-memory database.
    #[must_use]
    pub fn builder() -> EmdbBuilder {
        EmdbBuilder::new()
    }

    /// Insert or replace a key/value pair.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use emdb::Emdb;
    ///
    /// let mut db = Emdb::open_in_memory();
    /// db.insert("k", "v")?;
    /// assert_eq!(db.get("k")?, Some(b"v".to_vec()));
    /// # Ok::<(), emdb::Error>(())
    /// ```
    pub fn insert(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Result<()> {
        #[cfg(feature = "ttl")]
        {
            self.insert_with_ttl(key, value, Ttl::Default)
        }

        #[cfg(not(feature = "ttl"))]
        {
            let _previous = self
                .storage
                .insert(key.into(), record_new(value.into(), None));
            Ok(())
        }
    }

    /// Fetch a value by key.
    ///
    /// When the `ttl` feature is enabled and the key is expired, this returns
    /// `Ok(None)`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use emdb::Emdb;
    ///
    /// let mut db = Emdb::open_in_memory();
    /// db.insert("k", "v")?;
    /// assert_eq!(db.get("k")?, Some(b"v".to_vec()));
    /// # Ok::<(), emdb::Error>(())
    /// ```
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        let Some(record) = self.storage.get(key.as_ref()) else {
            return Ok(None);
        };

        #[cfg(feature = "ttl")]
        {
            let now = now_unix_millis();
            if is_expired(record_expires_at(record), now) {
                return Ok(None);
            }
        }

        Ok(Some(record_value(record).to_vec()))
    }

    /// Remove a key from the database and return its previous value.
    pub fn remove(&mut self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        let removed = self.storage.remove(key.as_ref());
        let Some(record) = removed else {
            return Ok(None);
        };

        #[cfg(feature = "ttl")]
        {
            let now = now_unix_millis();
            if is_expired(record_expires_at(&record), now) {
                return Ok(None);
            }
        }

        Ok(Some(record_into_value(record)))
    }

    /// Return `true` if the given key exists.
    pub fn contains_key(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        #[cfg(feature = "ttl")]
        {
            let Some(record) = self.storage.get(key.as_ref()) else {
                return Ok(false);
            };
            let now = now_unix_millis();
            if is_expired(record_expires_at(record), now) {
                return Ok(false);
            }

            Ok(true)
        }

        #[cfg(not(feature = "ttl"))]
        {
            Ok(self.storage.contains_key(key.as_ref()))
        }
    }

    /// Return the number of currently-visible records.
    #[must_use]
    pub fn len(&self) -> usize {
        #[cfg(feature = "ttl")]
        {
            let now = now_unix_millis();
            self.storage
                .values()
                .filter(|record| !is_expired(record_expires_at(record), now))
                .count()
        }

        #[cfg(not(feature = "ttl"))]
        {
            self.storage.len()
        }
    }

    /// Return `true` if no visible records are stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Remove all records.
    pub fn clear(&mut self) {
        self.storage.clear();
    }

    /// Iterate over all visible records as key/value byte slices.
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> + '_ {
        #[cfg(feature = "ttl")]
        {
            let now = now_unix_millis();
            self.storage.iter().filter_map(move |(key, record)| {
                if is_expired(record_expires_at(record), now) {
                    return None;
                }
                Some((key.as_slice(), record_value(record)))
            })
        }

        #[cfg(not(feature = "ttl"))]
        {
            self.storage
                .iter()
                .map(|(key, record)| (key.as_slice(), record_value(record)))
        }
    }

    /// Iterate over all visible keys as byte slices.
    pub fn keys(&self) -> impl Iterator<Item = &[u8]> + '_ {
        self.iter().map(|(key, _value)| key)
    }

    /// Insert or replace a key/value pair with explicit TTL behavior.
    #[cfg(feature = "ttl")]
    pub fn insert_with_ttl(
        &mut self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        ttl: Ttl,
    ) -> Result<()> {
        let now = now_unix_millis();
        let expires_at = expires_from_ttl(ttl, self.default_ttl, now)?;
        let _previous = self
            .storage
            .insert(key.into(), record_new(value.into(), expires_at));
        Ok(())
    }

    /// Returns the absolute expiration time for a key, if present.
    #[cfg(feature = "ttl")]
    pub fn expires_at(&self, key: impl AsRef<[u8]>) -> Result<Option<SystemTime>> {
        let Some(record) = self.storage.get(key.as_ref()) else {
            return Ok(None);
        };

        let Some(expires_at) = record_expires_at(record) else {
            return Ok(None);
        };

        let now = now_unix_millis();
        if is_expired(Some(expires_at), now) {
            return Ok(None);
        }

        Ok(Some(UNIX_EPOCH + Duration::from_millis(expires_at)))
    }

    /// Returns remaining TTL for a key.
    #[cfg(feature = "ttl")]
    pub fn ttl(&self, key: impl AsRef<[u8]>) -> Result<Option<Duration>> {
        let Some(record) = self.storage.get(key.as_ref()) else {
            return Ok(None);
        };

        let Some(expires_at) = record_expires_at(record) else {
            return Ok(None);
        };

        let now = now_unix_millis();
        Ok(remaining_ttl(expires_at, now))
    }

    /// Removes TTL from a key, making it permanent.
    #[cfg(feature = "ttl")]
    pub fn persist(&mut self, key: impl AsRef<[u8]>) -> Result<bool> {
        let Some(record) = self.storage.get_mut(key.as_ref()) else {
            return Ok(false);
        };

        Ok(record_set_persist(record))
    }

    /// Evicts all currently expired records and returns the number removed.
    #[cfg(feature = "ttl")]
    pub fn sweep_expired(&mut self) -> usize {
        let now = now_unix_millis();
        let before = self.storage.len();
        self.storage
            .retain(|_key, record| !is_expired(record_expires_at(record), now));
        before - self.storage.len()
    }
}

#[cfg(test)]
mod tests {
    use super::Emdb;
    use crate::Result;

    #[cfg(feature = "ttl")]
    use crate::Ttl;

    #[test]
    fn test_open_in_memory_returns_empty() {
        let db = Emdb::open_in_memory();
        assert_eq!(db.len(), 0);
        assert!(db.is_empty());
    }

    #[test]
    fn test_builder_returns_empty_database() {
        let db = Emdb::builder().build();
        assert!(db.is_empty());
    }

    #[test]
    fn test_insert_get_remove_round_trip() -> Result<()> {
        let mut db = Emdb::open_in_memory();
        db.insert(b"k", b"v")?;

        let found = db.get(b"k")?;
        assert_eq!(found, Some(b"v".to_vec()));
        assert!(db.contains_key(b"k")?);

        let removed = db.remove(b"k")?;
        assert_eq!(removed, Some(b"v".to_vec()));
        assert!(!db.contains_key(b"k")?);
        Ok(())
    }

    #[test]
    fn test_empty_key_is_allowed() -> Result<()> {
        let mut db = Emdb::open_in_memory();
        db.insert([], b"value")?;
        assert_eq!(db.get([])?, Some(b"value".to_vec()));
        Ok(())
    }

    #[test]
    fn test_clear_iter_and_keys() -> Result<()> {
        let mut db = Emdb::open_in_memory();
        db.insert(b"a", b"1")?;
        db.insert(b"b", b"2")?;

        let key_count = db.keys().count();
        let iter_count = db.iter().count();
        assert_eq!(key_count, 2);
        assert_eq!(iter_count, 2);

        db.clear();
        assert_eq!(db.len(), 0);
        assert!(db.is_empty());
        Ok(())
    }

    #[cfg(feature = "ttl")]
    #[test]
    fn test_ttl_after_zero_makes_key_immediately_invisible() -> Result<()> {
        use std::time::Duration;

        let mut db = Emdb::open_in_memory();
        db.insert_with_ttl(b"k", b"v", Ttl::After(Duration::ZERO))?;
        assert_eq!(db.get(b"k")?, None);
        Ok(())
    }

    #[cfg(feature = "ttl")]
    #[test]
    fn test_sweep_expired_empty_db_returns_zero() {
        let mut db = Emdb::open_in_memory();
        assert_eq!(db.sweep_expired(), 0);
    }
}
