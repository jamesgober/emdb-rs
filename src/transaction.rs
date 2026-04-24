// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Transaction support for atomic batch writes.

use std::collections::BTreeMap;
use std::sync::RwLockWriteGuard;

use crate::db::{Emdb, State};
use crate::storage::Op;
#[cfg(feature = "ttl")]
use crate::ttl::{expires_from_ttl, is_expired, now_unix_millis, record_expires_at, Ttl};
use crate::ttl::{record_new, record_value, Record};
use crate::{Error, Result};

/// A closure-scoped transaction over an [`Emdb`] instance.
///
/// Instances are created by [`Emdb::transaction`]. The transaction holds the
/// database write lock for its lifetime.
pub struct Transaction<'db> {
    db: &'db Emdb,
    state: RwLockWriteGuard<'db, State>,
    pending: Vec<Op>,
    overlay: BTreeMap<Vec<u8>, Option<Record>>,
}

impl<'db> Transaction<'db> {
    /// Create a transaction bound to the given database.
    ///
    /// # Errors
    ///
    /// Returns an error when lock acquisition fails.
    pub(crate) fn new(db: &'db Emdb) -> Result<Self> {
        Ok(Self {
            db,
            state: db.state_write()?,
            pending: Vec::new(),
            overlay: BTreeMap::new(),
        })
    }

    /// Insert or replace a key/value pair in this transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when key/value preparation fails.
    pub fn insert(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Result<()> {
        #[cfg(feature = "ttl")]
        {
            self.insert_with_ttl(key, value, Ttl::Default)
        }

        #[cfg(not(feature = "ttl"))]
        {
            let key = key.into();
            let value = value.into();
            let _old = self
                .overlay
                .insert(key.clone(), Some(record_new(value.clone(), None)));
            self.pending.push(Op::Insert {
                key,
                value,
                expires_at: None,
            });
            Ok(())
        }
    }

    /// Fetch a value by key from the transaction view.
    ///
    /// Read-your-writes semantics apply: transactional changes are visible.
    ///
    /// # Errors
    ///
    /// Returns an error when record conversion fails.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        if let Some(entry) = self.overlay.get(key.as_ref()) {
            return self.visible_record(entry.as_ref());
        }

        let Some(record) = self.state.storage.get(key.as_ref()) else {
            return Ok(None);
        };

        self.visible_record(Some(record))
    }

    /// Remove a key in this transaction and return previous visible value.
    ///
    /// # Errors
    ///
    /// Returns an error when visible value extraction fails.
    pub fn remove(&mut self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        let key_vec = key.as_ref().to_vec();
        let previous = if let Some(entry) = self.overlay.get(key.as_ref()) {
            self.visible_record(entry.as_ref())?
        } else {
            let base = self.state.storage.get(key.as_ref());
            self.visible_record(base)?
        };

        let _old = self.overlay.insert(key_vec.clone(), None);
        self.pending.push(Op::Remove { key: key_vec });
        Ok(previous)
    }

    /// Return true if a key is visible in this transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when value lookup fails.
    pub fn contains_key(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        Ok(self.get(key)?.is_some())
    }

    /// Insert or replace with explicit TTL behavior in this transaction.
    #[cfg(feature = "ttl")]
    pub fn insert_with_ttl(
        &mut self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        ttl: Ttl,
    ) -> Result<()> {
        let key = key.into();
        let value = value.into();

        let now = now_unix_millis();
        let expires_at = expires_from_ttl(ttl, self.db.inner.config.default_ttl, now)?;
        let _old = self
            .overlay
            .insert(key.clone(), Some(record_new(value.clone(), expires_at)));
        self.pending.push(Op::Insert {
            key,
            value,
            expires_at,
        });

        Ok(())
    }

    /// Commit this transaction.
    ///
    /// # Errors
    ///
    /// Returns an error if batch write or transaction metadata persistence fails.
    pub(crate) fn commit(&mut self) -> Result<()> {
        let tx_id = self
            .state
            .last_tx_id
            .checked_add(1)
            .ok_or(Error::TransactionAborted("transaction id overflow"))?;

        let op_count = u32::try_from(self.pending.len())
            .map_err(|_overflow| Error::TransactionAborted("operation count overflow"))?;

        {
            let mut backend = self.db.lock_backend()?;
            backend.append(&Op::BatchBegin { tx_id, op_count })?;

            let writes = std::mem::take(&mut self.pending);
            for op in writes {
                backend.append(&op)?;
            }

            backend.append(&Op::BatchEnd { tx_id })?;
            backend.set_last_tx_id(tx_id)?;
        }

        let updates = std::mem::take(&mut self.overlay);
        for (key, maybe_record) in updates {
            match maybe_record {
                Some(record) => {
                    let _old = self.state.storage.insert(key, record);
                }
                None => {
                    let _old = self.state.storage.remove(&key);
                }
            }
        }

        self.state.last_tx_id = tx_id;
        Ok(())
    }

    fn visible_record(&self, maybe_record: Option<&Record>) -> Result<Option<Vec<u8>>> {
        let Some(record) = maybe_record else {
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
}

#[cfg(test)]
mod tests {
    use crate::Emdb;

    #[test]
    fn transaction_commit_applies_overlay() {
        let db = Emdb::open_in_memory();
        let result = db.transaction(|tx| {
            tx.insert("a", "1")?;
            tx.insert("b", "2")?;
            Ok(())
        });

        assert!(result.is_ok());
        assert!(matches!(db.get("a"), Ok(Some(v)) if v == b"1".to_vec()));
        assert!(matches!(db.get("b"), Ok(Some(v)) if v == b"2".to_vec()));
    }

    #[test]
    fn transaction_rollback_discards_overlay() {
        let db = Emdb::open_in_memory();
        let result = db.transaction::<_, ()>(|tx| {
            tx.insert("a", "1")?;
            Err(crate::Error::TransactionAborted("rollback"))
        });

        assert!(result.is_err());
        assert!(matches!(db.get("a"), Ok(None)));
    }

    #[test]
    fn transaction_remove_reads_from_overlay() {
        let db = Emdb::open_in_memory();
        assert!(db.insert("a", "1").is_ok());

        let result = db.transaction(|tx| {
            let removed = tx.remove("a")?;
            assert!(matches!(removed, Some(v) if v == b"1".to_vec()));
            assert!(matches!(tx.get("a"), Ok(None)));
            Ok(())
        });

        assert!(result.is_ok());
        assert!(matches!(db.get("a"), Ok(None)));
    }
}
