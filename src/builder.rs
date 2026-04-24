// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Database builder.

use std::path::PathBuf;

#[cfg(feature = "ttl")]
use std::time::Duration;

use crate::storage::FlushPolicy;
use crate::Emdb;
use crate::Result;

/// Builder for constructing an in-memory [`Emdb`] instance.
#[derive(Debug, Clone, Default)]
pub struct EmdbBuilder {
    #[cfg(feature = "ttl")]
    pub(crate) default_ttl: Option<Duration>,
    pub(crate) path: Option<PathBuf>,
    pub(crate) flush_policy: FlushPolicy,
}

impl EmdbBuilder {
    /// Create a new builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the global default TTL for inserted records.
    #[cfg(feature = "ttl")]
    #[must_use]
    pub fn default_ttl(mut self, ttl: Duration) -> Self {
        self.default_ttl = Some(ttl);
        self
    }

    /// Set a file path for persistent storage.
    #[must_use]
    pub fn path(mut self, path: impl Into<PathBuf>) -> Self {
        self.path = Some(path.into());
        self
    }

    /// Set flush durability policy.
    #[must_use]
    pub fn flush_policy(mut self, policy: FlushPolicy) -> Self {
        self.flush_policy = policy;
        self
    }

    /// Build an [`Emdb`] instance from the configured options.
    ///
    /// # Errors
    ///
    /// Returns an error when configuration is invalid (for example
    /// `FlushPolicy::EveryN(0)`) or storage initialization fails.
    pub fn build(self) -> Result<Emdb> {
        Emdb::from_builder(self)
    }
}

#[cfg(test)]
mod tests {
    use super::EmdbBuilder;
    use crate::FlushPolicy;

    #[test]
    fn test_build_returns_empty_database() {
        let db = EmdbBuilder::new().build();
        assert!(db.is_ok());
        let db = match db {
            Ok(db) => db,
            Err(err) => panic!("build should succeed: {err}"),
        };
        assert!(db.is_empty());
    }

    #[test]
    fn test_default_builder_builds_database() {
        let db = EmdbBuilder::default().build();
        assert!(db.is_ok());
        let db = match db {
            Ok(db) => db,
            Err(err) => panic!("build should succeed: {err}"),
        };
        assert_eq!(db.len(), 0);
    }

    #[cfg(feature = "ttl")]
    #[test]
    fn test_default_ttl_builder_method_is_usable() {
        use std::time::Duration;

        let db = EmdbBuilder::new()
            .default_ttl(Duration::from_secs(1))
            .build();
        assert!(db.is_ok());
        let db = match db {
            Ok(db) => db,
            Err(err) => panic!("build should succeed: {err}"),
        };
        assert!(db.is_empty());
    }

    #[test]
    fn test_flush_policy_every_n_zero_errors() {
        let db = EmdbBuilder::new()
            .flush_policy(FlushPolicy::EveryN(0))
            .build();
        assert!(db.is_err());
    }
}
