// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Buffered write batch over an [`crate::Emdb`].
//!
//! A transaction stages writes in an ordered map (the last staged
//! write per key wins). Reads through the transaction see staged
//! writes first and fall through to the live database. If the closure
//! returns `Err`, the staged writes are dropped and nothing is
//! written.
//!
//! On commit the staged writes become one engine write batch: the
//! write lock of every staged key is taken (in ascending stripe
//! order), all records are appended with one vectored journal write,
//! and the in-memory index is updated before the locks are released.
//! Any other write to one of those keys is therefore ordered entirely
//! before or after the commit.
//!
//! That is the whole guarantee. There is no read isolation (reads in
//! the closure see concurrent writes, so read-modify-write sequences
//! can lose updates), no atomic visibility (other threads can see part
//! of a commit while it is being applied), and no crash atomicity (a
//! crash during the append can leave a prefix of the batch durable).
//! See [`crate::Emdb::transaction`].

use std::collections::BTreeMap;

use crate::storage::engine::BatchOp;
use crate::storage::DEFAULT_NAMESPACE_ID;
use crate::{Emdb, Result};

#[cfg(feature = "ttl")]
use crate::ttl::{expires_from_ttl, now_unix_millis, Ttl};

/// Staged write inside a transaction.
enum Staged {
    Insert { value: Vec<u8>, expires_at: u64 },
    Remove,
}

/// Closure-scoped write batch handed to [`Emdb::transaction`].
///
/// Stages inserts and removes and offers read-your-writes reads. See
/// [`Emdb::transaction`] for exactly what a commit guarantees; it is
/// not an isolated transaction.
pub struct Transaction<'db> {
    db: &'db Emdb,
    overlay: BTreeMap<Vec<u8>, Staged>,
}

impl<'db> Transaction<'db> {
    pub(crate) fn new(db: &'db Emdb) -> Self {
        Self {
            db,
            overlay: BTreeMap::new(),
        }
    }

    /// Stage an insert.
    pub fn insert(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Result<()> {
        let key = key.into();
        let value = value.into();
        #[cfg(feature = "ttl")]
        let expires_at = self.db.inner.default_expires_at()?;
        #[cfg(not(feature = "ttl"))]
        let expires_at = 0_u64;
        let _previous = self
            .overlay
            .insert(key, Staged::Insert { value, expires_at });
        Ok(())
    }

    /// Stage an insert with explicit TTL.
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
        let expires_at = expires_from_ttl(ttl, self.db.inner.default_ttl, now)?.unwrap_or(0);
        let _previous = self
            .overlay
            .insert(key, Staged::Insert { value, expires_at });
        Ok(())
    }

    /// Stage a remove. Returns the value visible to this transaction
    /// right now (staged or live); the key may still change before the
    /// commit.
    pub fn remove(&mut self, key: impl Into<Vec<u8>>) -> Result<Option<Vec<u8>>> {
        let key = key.into();
        let prev_visible = self.get(&key)?;
        let _previous = self.overlay.insert(key, Staged::Remove);
        Ok(prev_visible)
    }

    /// Read with read-your-writes semantics: a staged write for `key`
    /// wins, otherwise the live database is read (no snapshot).
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        let key = key.as_ref();
        if let Some(staged) = self.overlay.get(key) {
            return match staged {
                Staged::Insert { value, .. } => Ok(Some(value.clone())),
                Staged::Remove => Ok(None),
            };
        }
        self.db.get(key)
    }

    /// Whether the key is visible inside this transaction.
    pub fn contains_key(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        Ok(self.get(key)?.is_some())
    }

    pub(crate) fn commit(mut self) -> Result<()> {
        let staged = std::mem::take(&mut self.overlay);
        if staged.is_empty() {
            return Ok(());
        }
        let ops: Vec<BatchOp> = staged
            .into_iter()
            .map(|(key, staged)| match staged {
                Staged::Insert { value, expires_at } => BatchOp::Insert {
                    key,
                    value,
                    expires_at,
                },
                Staged::Remove => BatchOp::Remove { key },
            })
            .collect();
        self.db.inner.engine.write_batch(DEFAULT_NAMESPACE_ID, ops)
    }
}
