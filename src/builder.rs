// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Database builder.

use std::path::PathBuf;

#[cfg(feature = "ttl")]
use std::time::Duration;

use crate::Emdb;
use crate::Result;

/// Builder for constructing an [`Emdb`].
#[derive(Debug, Clone, Default)]
pub struct EmdbBuilder {
    pub(crate) path: Option<PathBuf>,
    #[cfg(feature = "ttl")]
    pub(crate) default_ttl: Option<Duration>,
    pub(crate) data_root: Option<PathBuf>,
    pub(crate) app_name: Option<String>,
    pub(crate) database_name: Option<String>,
    pub(crate) enable_range_scans: bool,
    #[cfg(feature = "encrypt")]
    pub(crate) encryption_key: Option<[u8; 32]>,
    #[cfg(feature = "encrypt")]
    pub(crate) encryption_passphrase: Option<String>,
    #[cfg(feature = "encrypt")]
    pub(crate) cipher: Option<crate::encryption::Cipher>,
}

impl EmdbBuilder {
    /// Create a new builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the explicit on-disk path. Mutually exclusive with the
    /// OS-resolution methods ([`Self::app_name`] / [`Self::database_name`]
    /// / [`Self::data_root`]).
    #[must_use]
    pub fn path(mut self, path: impl Into<PathBuf>) -> Self {
        self.path = Some(path.into());
        self
    }

    /// Subfolder name under the OS data root. Default `"emdb"`.
    #[must_use]
    pub fn app_name(mut self, name: impl Into<String>) -> Self {
        self.app_name = Some(name.into());
        self
    }

    /// Database filename. Default `"emdb-default.emdb"`.
    #[must_use]
    pub fn database_name(mut self, name: impl Into<String>) -> Self {
        self.database_name = Some(name.into());
        self
    }

    /// Override the OS data root. Mostly for tests / containers.
    #[must_use]
    pub fn data_root(mut self, root: impl Into<PathBuf>) -> Self {
        self.data_root = Some(root.into());
        self
    }

    /// Set a global default TTL applied to inserts using
    /// [`crate::Ttl::Default`].
    #[cfg(feature = "ttl")]
    #[must_use]
    pub fn default_ttl(mut self, ttl: Duration) -> Self {
        self.default_ttl = Some(ttl);
        self
    }

    /// Maintain a sorted secondary index alongside the hash index so
    /// `Emdb::range(...)` and `Namespace::range(...)` can iterate keys
    /// in lexicographic order. Off by default.
    ///
    /// Cost: one `Vec<u8>` clone of the key per insert plus the
    /// `BTreeMap` node overhead. Roughly doubles in-memory index size
    /// for a typical workload. Calling `range()` without enabling this
    /// at open time returns [`crate::Error::InvalidConfig`].
    #[must_use]
    pub fn enable_range_scans(mut self, enabled: bool) -> Self {
        self.enable_range_scans = enabled;
        self
    }

    /// Enable AES-256-GCM at-rest encryption with a raw 32-byte key.
    /// Mutually exclusive with [`Self::encryption_passphrase`].
    #[cfg(feature = "encrypt")]
    #[must_use]
    pub fn encryption_key(mut self, key: [u8; 32]) -> Self {
        self.encryption_key = Some(key);
        self
    }

    /// Enable encryption with a key derived from a UTF-8 passphrase
    /// via Argon2id. Mutually exclusive with [`Self::encryption_key`].
    #[cfg(feature = "encrypt")]
    #[must_use]
    pub fn encryption_passphrase(mut self, passphrase: impl Into<String>) -> Self {
        self.encryption_passphrase = Some(passphrase.into());
        self
    }

    /// Override the AEAD cipher. Default is AES-256-GCM; reopens
    /// inherit the cipher recorded in the file header.
    #[cfg(feature = "encrypt")]
    #[must_use]
    pub fn cipher(mut self, cipher: crate::encryption::Cipher) -> Self {
        self.cipher = Some(cipher);
        self
    }

    /// Construct the [`Emdb`].
    pub fn build(self) -> Result<Emdb> {
        Emdb::from_builder(self)
    }
}

#[cfg(test)]
mod tests {
    use super::EmdbBuilder;

    fn tmp_path(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0_u128, |d| d.as_nanos());
        p.push(format!("emdb-builder-{name}-{nanos}.emdb"));
        p
    }

    fn cleanup(path: &std::path::Path) {
        let _ = std::fs::remove_file(path);
        if let Some(parent) = path.parent() {
            if let Some(stem) = path.file_name().and_then(|n| n.to_str()) {
                let _ = std::fs::remove_file(parent.join(format!("{stem}.lock")));
            }
        }
    }

    #[test]
    fn build_persists_at_explicit_path() {
        let path = tmp_path("explicit");
        let result = EmdbBuilder::new().path(path.clone()).build();
        assert!(result.is_ok(), "build failed: {result:?}");
        let _db = result.unwrap_or_else(|err| panic!("{err}"));
        drop(_db);
        cleanup(&path);
    }
}
