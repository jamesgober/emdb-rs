// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Buffered batch transaction over an [`crate::Emdb`].
//!
//! A transaction stages writes in a Vec; on commit, every staged write
//! is replayed against the engine under a single writer-mutex hold per
//! record. Reads inside the transaction see staged writes via the
//! overlay; on commit failure (closure returns Err), the staged writes
//! are dropped.
//!
//! Note: the new mmap+append architecture does **not** provide
//! all-or-nothing batch atomicity. Individual records are atomic
//! (per-record CRC) but a crash mid-commit leaves a prefix of the
//! batch durable. Callers that need true atomicity must arrange it
//! externally.

use std::collections::BTreeMap;

use crate::storage::DEFAULT_NAMESPACE_ID;
use crate::{Emdb, Result};

#[cfg(feature = "ttl")]
use crate::ttl::{expires_from_ttl, now_unix_millis, Ttl};

/// Staged write inside a transaction.
enum Staged {
    Insert {
        value: Vec<u8>,
        expires_at: u64,
    },
    Remove,
}

/// Closure-scoped transaction.
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
    pub fn insert(
        &mut self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Result<()> {
        let key = key.into();
        let value = value.into();
        #[cfg(feature = "ttl")]
        let expires_at = {
            let now = now_unix_millis();
            expires_from_ttl(Ttl::Default, self.db.inner.default_ttl, now)?.unwrap_or(0)
        };
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

    /// Stage a remove.
    pub fn remove(&mut self, key: impl Into<Vec<u8>>) -> Result<Option<Vec<u8>>> {
        let key = key.into();
        let prev_visible = self.get(&key)?;
        let _previous = self.overlay.insert(key, Staged::Remove);
        Ok(prev_visible)
    }

    /// Read with read-your-writes semantics.
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

        let engine = &self.db.inner.engine;
        // Bulk-route inserts via insert_many; removes go individually
        // because the engine's remove path returns the previous value.
        let mut bulk_inserts: Vec<(Vec<u8>, Vec<u8>, u64)> = Vec::new();
        for (key, staged) in staged {
            match staged {
                Staged::Insert { value, expires_at } => {
                    bulk_inserts.push((key, value, expires_at));
                }
                Staged::Remove => {
                    let _ = engine.remove(DEFAULT_NAMESPACE_ID, &key)?;
                }
            }
        }
        if !bulk_inserts.is_empty() {
            engine.insert_many(DEFAULT_NAMESPACE_ID, bulk_inserts)?;
        }
        Ok(())
    }
}
