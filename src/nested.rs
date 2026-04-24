// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Nested key ergonomics based on dotted prefixes.

use crate::{Emdb, Error, Result};

#[cfg(feature = "ttl")]
use crate::Ttl;

fn join_prefix(prefix: &str, key: &str) -> Vec<u8> {
    if prefix.is_empty() {
        return key.as_bytes().to_vec();
    }

    let mut full = String::with_capacity(prefix.len() + 1 + key.len());
    full.push_str(prefix);
    full.push('.');
    full.push_str(key);
    full.into_bytes()
}

fn prefix_bytes(prefix: &str) -> Result<Vec<u8>> {
    if prefix.is_empty() {
        return Err(Error::InvalidPath);
    }

    let mut out = Vec::with_capacity(prefix.len() + 1);
    out.extend_from_slice(prefix.as_bytes());
    out.push(b'.');
    Ok(out)
}

impl Emdb {
    /// Returns an iterator over all keys starting with `prefix.`.
    ///
    /// # Errors
    ///
    /// Returns an error when lock acquisition fails.
    pub fn group(
        &self,
        prefix: impl AsRef<str>,
    ) -> Result<impl Iterator<Item = (Vec<u8>, Vec<u8>)>> {
        let prefix = prefix.as_ref().as_bytes().to_vec();
        let items = self
            .iter()?
            .filter(move |(key, _value)| {
                key.starts_with(prefix.as_slice()) && key.get(prefix.len()).copied() == Some(b'.')
            })
            .collect::<Vec<_>>();
        Ok(items.into_iter())
    }

    /// Deletes every key starting with `prefix.` and returns the number removed.
    ///
    /// # Errors
    ///
    /// Returns an error when the prefix is empty, key deletion fails, or lock
    /// acquisition fails.
    pub fn delete_group(&self, prefix: impl AsRef<str>) -> Result<usize> {
        let prefix = prefix_bytes(prefix.as_ref())?;
        let keys: Vec<Vec<u8>> = self
            .keys()?
            .filter(|key| key.starts_with(prefix.as_slice()))
            .collect();

        let mut deleted = 0_usize;
        for key in keys {
            if self.remove(key)?.is_some() {
                deleted += 1;
            }
        }

        Ok(deleted)
    }

    /// Returns a scoped handle that prefixes all keys with `prefix.`.
    #[must_use]
    pub fn focus(&self, prefix: impl Into<String>) -> Focus<'_> {
        Focus {
            db: self,
            prefix: prefix.into(),
        }
    }
}

/// Scoped database view that prefixes keys with a dotted path segment.
///
/// # Examples
///
/// ```rust
/// # #[cfg(feature = "nested")]
/// # {
/// use emdb::Emdb;
///
/// let db = Emdb::open_in_memory();
/// let user = db.focus("user");
/// user.set("name", "james")?;
/// assert_eq!(user.get("name")?, Some(b"james".to_vec()));
/// # }
/// # Ok::<(), emdb::Error>(())
/// ```
pub struct Focus<'a> {
    db: &'a Emdb,
    prefix: String,
}

impl<'a> Focus<'a> {
    /// Inserts a value under the current focus prefix.
    pub fn set(&self, key: &str, value: impl Into<Vec<u8>>) -> Result<()> {
        self.db.insert(join_prefix(&self.prefix, key), value)
    }

    /// Fetches a value under the current focus prefix.
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.db.get(join_prefix(&self.prefix, key))
    }

    /// Removes a value under the current focus prefix.
    pub fn remove(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.db.remove(join_prefix(&self.prefix, key))
    }

    /// Returns `true` when a focused key exists.
    pub fn contains_key(&self, key: &str) -> Result<bool> {
        self.db.contains_key(join_prefix(&self.prefix, key))
    }

    /// Creates a nested focus below the current one.
    #[must_use]
    pub fn focus(&self, sub: &str) -> Focus<'a> {
        let next = if self.prefix.is_empty() {
            sub.to_owned()
        } else {
            let mut merged = String::with_capacity(self.prefix.len() + 1 + sub.len());
            merged.push_str(&self.prefix);
            merged.push('.');
            merged.push_str(sub);
            merged
        };

        Focus {
            db: self.db,
            prefix: next,
        }
    }

    /// Iterates all keys under the current focus prefix.
    ///
    /// # Errors
    ///
    /// Returns an error when lock acquisition fails.
    pub fn iter(&self) -> Result<impl Iterator<Item = (Vec<u8>, Vec<u8>)>> {
        let items = self.db.group(self.prefix.as_str())?.collect::<Vec<_>>();
        Ok(items.into_iter())
    }

    /// Deletes every key under the current focus prefix.
    pub fn delete_all(&self) -> Result<usize> {
        self.db.delete_group(self.prefix.as_str())
    }
}

#[cfg(all(feature = "nested", feature = "ttl"))]
impl<'a> Focus<'a> {
    /// Inserts a value under the current focus prefix with explicit TTL.
    pub fn set_with_ttl(&self, key: &str, value: impl Into<Vec<u8>>, ttl: Ttl) -> Result<()> {
        self.db
            .insert_with_ttl(join_prefix(&self.prefix, key), value, ttl)
    }
}

#[cfg(test)]
mod tests {
    use crate::Emdb;

    #[test]
    fn test_group_filters_by_prefix() {
        let db = Emdb::open_in_memory();
        assert!(db.insert("product.name", "box").is_ok());
        assert!(db.insert("product.size", "l").is_ok());
        assert!(db.insert("products.name", "skip").is_ok());
        assert!(db.insert("product", "exact").is_ok());

        let found = db.group("product");
        assert!(found.is_ok());
        assert_eq!(found.map_or(0, Iterator::count), 2);
    }

    #[test]
    fn test_delete_group_empty_prefix_is_error() {
        let db = Emdb::open_in_memory();
        let deleted = db.delete_group("");
        assert!(deleted.is_err());
    }

    #[test]
    fn test_focus_chain_and_delete_all() {
        let db = Emdb::open_in_memory();

        {
            let product = db.focus("product");
            assert!(product.set("name", "phone").is_ok());

            let details = product.focus("details");
            assert!(details.set("weight", "100g").is_ok());

            let specs = details.focus("specs");
            assert!(specs.set("ram", "8gb").is_ok());
        }

        let grouped = db.group("product");
        assert!(grouped.is_ok());
        assert_eq!(grouped.map_or(0, Iterator::count), 3);

        {
            let details = db.focus("product.details");
            let deleted = details.delete_all();
            assert!(matches!(deleted, Ok(2)));
        }

        let grouped = db.group("product");
        assert!(grouped.is_ok());
        assert_eq!(grouped.map_or(0, Iterator::count), 1);
    }

    #[cfg(feature = "ttl")]
    #[test]
    fn test_focus_set_with_ttl_zero_expires() {
        use std::time::Duration;

        use crate::Ttl;

        let db = Emdb::open_in_memory();
        {
            let focus = db.focus("session");
            assert!(focus
                .set_with_ttl("token", "abc", Ttl::After(Duration::ZERO))
                .is_ok());
        }

        assert!(matches!(db.get("session.token"), Ok(None)));
    }
}
