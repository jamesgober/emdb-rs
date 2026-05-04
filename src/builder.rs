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
    pub(crate) flush_policy: crate::FlushPolicy,
    #[cfg(feature = "encrypt")]
    pub(crate) encryption_key: Option<crate::encryption::KeyBytes>,
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

    /// Set the flush policy.
    ///
    /// Default is [`crate::FlushPolicy::OnEachFlush`], which makes
    /// every `db.flush()` perform its own `fdatasync` (one sync per
    /// flush call — the v0.7.x behaviour).
    ///
    /// [`crate::FlushPolicy::Group`] enables the group-commit
    /// coordinator: concurrent `flush()` calls fuse into a single
    /// `fdatasync`. Pick this for multi-threaded workloads that
    /// flush per record. See the `FlushPolicy` documentation for
    /// the leader-follower protocol and tuning guidance.
    #[must_use]
    pub fn flush_policy(mut self, policy: crate::FlushPolicy) -> Self {
        self.flush_policy = policy;
        self
    }

    /// Enable AES-256-GCM at-rest encryption with a raw 32-byte key.
    /// Mutually exclusive with [`Self::encryption_passphrase`].
    ///
    /// The key bytes are wrapped in [`zeroize::Zeroizing`] internally
    /// so they clear on drop. The caller is still responsible for
    /// zeroizing their own copy of the key after passing it in —
    /// this method takes the array by value, so the caller's
    /// original is moved here, but a `Copy` wouldn't be (and
    /// `[u8; 32]` is `Copy`). Keep the original behind a
    /// `Zeroizing<[u8; 32]>` on the caller side if you can.
    #[cfg(feature = "encrypt")]
    #[must_use]
    pub fn encryption_key(mut self, key: [u8; 32]) -> Self {
        self.encryption_key = Some(crate::encryption::KeyBytes::from(key));
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
