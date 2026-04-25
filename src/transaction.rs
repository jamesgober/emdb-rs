// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Transaction support for atomic batch writes.
//!
//! A transaction stages writes in a closure-local overlay. Reads inside the
//! transaction see the overlay first and fall back to the live database state.
//! Writes are not visible to other handles until commit runs (commit happens
//! automatically when the closure passed to [`Emdb::transaction`] returns
//! `Ok(_)`); commit appends a `BatchBegin … BatchEnd` block to the WAL, then
//! applies the overlay to the in-memory index under per-shard write locks.
//!
//! This deliberately gives up snapshot isolation in exchange for letting other
//! readers and writers proceed while a transaction is open. Atomicity (all of
//! a transaction's writes survive a crash, or none do) is preserved by the WAL
//! batch markers and by acquiring every shard write lock before applying the
//! overlay.

use std::collections::BTreeMap;

use crate::db::Emdb;
use crate::storage::{Op, OpRef, Storage};
#[cfg(feature = "ttl")]
use crate::ttl::{expires_from_ttl, is_expired, now_unix_millis, record_expires_at, Ttl};
use crate::ttl::{record_new, record_value, Record};
use crate::{Error, Result};

/// A closure-scoped transaction over an [`Emdb`] instance.
///
/// Instances are created by [`Emdb::transaction`]. The transaction does not
/// hold any database lock for its lifetime; it only acquires locks at commit
/// time, which happens automatically when the closure passed to
/// [`Emdb::transaction`] returns `Ok(_)`.
pub struct Transaction<'db> {
    db: &'db Emdb,
    pending: Vec<Op>,
    overlay: BTreeMap<Vec<u8>, Option<Record>>,
}

impl<'db> Transaction<'db> {
    /// Create a transaction bound to the given database.
    ///
    /// # Errors
    ///
    /// Returns an error only when the embedding harness later wishes to
    /// validate transaction state at construction; no errors are produced
    /// today, but the signature is reserved.
    pub(crate) fn new(db: &'db Emdb) -> Result<Self> {
        Ok(Self {
            db,
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
    /// Returns an error when shard lock acquisition fails.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        let key = key.as_ref();
        if let Some(entry) = self.overlay.get(key) {
            return self.visible_record(entry.as_ref());
        }

        let shard = self.db.shard_for(key)?;
        let Some(record) = shard.get(key) else {
            return Ok(None);
        };

        // Clone out from under the lock to release the shard before returning.
        let cloned = record.clone();
        drop(shard);
        self.visible_record(Some(&cloned))
    }

    /// Remove a key in this transaction and return previous visible value.
    ///
    /// # Errors
    ///
    /// Returns an error when shard lock acquisition fails.
    pub fn remove(&mut self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        let key_vec = key.as_ref().to_vec();
        let previous = self.get(key.as_ref())?;
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
    ///
    /// # Errors
    ///
    /// Returns an error when TTL computation overflows.
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
    /// Atomicity rules:
    ///
    /// - A persistent database appends `BatchBegin`, every staged op, and
    ///   `BatchEnd` to the WAL before any in-memory mutation. A crash between
    ///   `BatchBegin` and `BatchEnd` discards the entire batch on recovery.
    /// - Both persistent and in-memory databases write every staged change
    ///   under per-shard write locks acquired across all shards, so concurrent
    ///   readers either see the full pre-commit state or the full post-commit
    ///   state — never a partial view.
    ///
    /// # Errors
    ///
    /// Returns an error if WAL append, lock acquisition, or transaction id
    /// allocation fails.
    pub(crate) fn commit(&mut self) -> Result<()> {
        let writes = std::mem::take(&mut self.pending);
        let updates = std::mem::take(&mut self.overlay);

        if writes.is_empty() && updates.is_empty() {
            return Ok(());
        }

        let op_count = u32::try_from(writes.len())
            .map_err(|_overflow| Error::TransactionAborted("operation count overflow"))?;
        let tx_id = self.db.next_tx_id()?;

        let mut backend_guard = self.db.lock_backend()?;
        if let Some(backend) = backend_guard.as_mut() {
            backend.append(OpRef::BatchBegin { tx_id, op_count })?;
            for op in &writes {
                backend.append(OpRef::from(op))?;
            }
            backend.append(OpRef::BatchEnd { tx_id })?;
            backend.set_last_tx_id(tx_id)?;
        }

        // Acquire every shard's write lock so the overlay applies atomically
        // relative to readers and concurrent single-key writers.
        let mut shards = self.db.index().write_all()?;
        for (key, maybe_record) in updates {
            let shard_idx = crate::index::Index::shard_for_key(&key);
            let shard = match shards.get_mut(shard_idx) {
                Some(shard) => shard,
                None => return Err(Error::TransactionAborted("shard index out of range")),
            };
            match maybe_record {
                Some(record) => {
                    let _old = shard.insert(key, record);
                }
                None => {
                    let _old = shard.remove(&key);
                }
            }
        }
        drop(shards);
        drop(backend_guard);

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
