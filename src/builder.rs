// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Database builder.

#[cfg(feature = "ttl")]
use std::time::Duration;

use crate::Emdb;

/// Builder for constructing an in-memory [`Emdb`] instance.
#[derive(Debug, Clone, Default)]
pub struct EmdbBuilder {
    #[cfg(feature = "ttl")]
    pub(crate) default_ttl: Option<Duration>,
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

    /// Build an in-memory [`Emdb`] instance.
    #[must_use]
    pub fn build(self) -> Emdb {
        Emdb::from_builder(self)
    }
}

#[cfg(test)]
mod tests {
    use super::EmdbBuilder;

    #[test]
    fn test_build_returns_empty_database() {
        let db = EmdbBuilder::new().build();
        assert!(db.is_empty());
    }

    #[test]
    fn test_default_builder_builds_database() {
        let db = EmdbBuilder::default().build();
        assert_eq!(db.len(), 0);
    }

    #[cfg(feature = "ttl")]
    #[test]
    fn test_default_ttl_builder_method_is_usable() {
        use std::time::Duration;

        let db = EmdbBuilder::new()
            .default_ttl(Duration::from_secs(1))
            .build();
        assert!(db.is_empty());
    }
}
