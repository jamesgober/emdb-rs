// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Error types for the `emdb` crate.
//!
//! All fallible operations return [`Result<T>`] — an alias for
//! `core::result::Result<T, Error>`. The [`Error`] type enumerates every
//! failure mode the crate can produce. Error codes are reserved under the
//! `EM-XXXXX` prefix in the wider Hive error registry.

use core::fmt;
use std::path::PathBuf;

/// Convenient `Result` alias where the error type is fixed to [`Error`].
pub type Result<T> = core::result::Result<T, Error>;

/// The top-level error type returned by every fallible operation in `emdb`.
///
/// The variant set is intentionally small during the early development
/// phase. It will grow as concrete subsystems land (storage engine,
/// transaction manager, query layer, etc.).
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The operation is not yet implemented in this version of `emdb`.
    ///
    /// This variant exists to keep the public API surface stable while
    /// internal subsystems are still being built. It will be removed
    /// before the 1.0 release.
    NotImplemented,

    /// An invalid path was provided to a nested operation.
    ///
    /// This is returned when a nested API receives an empty prefix.
    /// Callers should provide a non-empty prefix and retry.
    #[cfg(feature = "nested")]
    InvalidPath,

    /// A TTL computation overflowed the representable `SystemTime` range.
    ///
    /// This occurs when adding a duration to the current wall clock exceeds
    /// the maximum representable timestamp. Callers should use a smaller TTL.
    #[cfg(feature = "ttl")]
    TtlOverflow,

    /// A lower-level I/O failure occurred.
    ///
    /// Callers should inspect the wrapped `std::io::ErrorKind` and decide
    /// whether retry, fallback, or surface-to-user behavior is appropriate.
    Io(std::io::Error),

    /// The file exists but does not contain the emdb magic header.
    ///
    /// This usually means a non-emdb file was opened by mistake.
    MagicMismatch,

    /// The on-disk format version does not match this build.
    ///
    /// The file was likely written by a newer or incompatible emdb version.
    VersionMismatch {
        /// Version found in the file header.
        found: u32,
        /// Version expected by this build.
        expected: u32,
    },

    /// The file requires features not enabled in this build.
    ///
    /// Rebuild with the required features or open a compatible database file.
    FeatureMismatch {
        /// Feature bitmask stored in file header.
        file_flags: u32,
        /// Feature bitmask compiled into this build.
        build_flags: u32,
    },

    /// Corrupted or truncated data was detected while parsing storage records.
    Corrupted {
        /// Byte offset where corruption was detected.
        offset: u64,
        /// Short reason string for diagnostics.
        reason: &'static str,
    },

    /// Invalid runtime configuration.
    ///
    /// This indicates programmer error when constructing the database.
    InvalidConfig(&'static str),

    /// A transaction operation was attempted outside a valid transaction context.
    TransactionInvalid,

    /// A transaction was aborted due to an internal invariant violation.
    TransactionAborted(&'static str),

    /// Another process currently holds the advisory lock for this database.
    LockBusy {
        /// Path to the lockfile that is currently held.
        path: PathBuf,
    },

    /// Lockfile acquisition or lockfile I/O failed.
    LockfileError(std::io::Error),

    /// A synchronization lock was poisoned due to panic while held.
    LockPoisoned,

    /// At-rest encryption configuration is invalid or AEAD failed
    /// internally.
    ///
    /// Distinct from [`Self::EncryptionKeyMismatch`]: this variant
    /// signals a problem the user cannot fix by supplying a
    /// different key (truncated buffer, malformed verification
    /// block, AEAD machinery failure). The database should be
    /// considered corrupt or the build mis-configured.
    #[cfg(feature = "encrypt")]
    Encryption(&'static str),

    /// The encryption key supplied to
    /// [`crate::EmdbBuilder::encryption_key`] does not match the key
    /// the database was created with.
    ///
    /// AEAD authentication failed on the verification block (or on a
    /// subsequent record). The database is fine; the caller supplied
    /// the wrong key.
    #[cfg(feature = "encrypt")]
    EncryptionKeyMismatch,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotImplemented => f.write_str("emdb: operation not yet implemented"),
            #[cfg(feature = "nested")]
            Self::InvalidPath => f.write_str("emdb: invalid nested path"),
            #[cfg(feature = "ttl")]
            Self::TtlOverflow => f.write_str("emdb: ttl overflow"),
            Self::Io(err) => write!(f, "emdb: io error ({})", err.kind()),
            Self::MagicMismatch => f.write_str("emdb: file magic mismatch"),
            Self::VersionMismatch { found, expected } => {
                write!(f, "emdb: format version mismatch (found {}, expected {})", found, expected)
            }
            Self::FeatureMismatch {
                file_flags,
                build_flags,
            } => write!(
                f,
                "emdb: feature mismatch (file flags 0x{file_flags:08x}, build flags 0x{build_flags:08x})"
            ),
            Self::Corrupted { offset, reason } => {
                write!(f, "emdb: corrupted data at offset {} ({})", offset, reason)
            }
            Self::InvalidConfig(msg) => write!(f, "emdb: invalid configuration ({msg})"),
            Self::TransactionInvalid => f.write_str("emdb: invalid transaction context"),
            Self::TransactionAborted(msg) => write!(f, "emdb: transaction aborted ({msg})"),
            Self::LockBusy { path } => {
                write!(f, "emdb: lock busy ({})", path.display())
            }
            Self::LockfileError(err) => {
                write!(f, "emdb: lockfile error ({})", err.kind())
            }
            Self::LockPoisoned => f.write_str("emdb: lock poisoned"),
            #[cfg(feature = "encrypt")]
            Self::Encryption(msg) => write!(f, "emdb: encryption error ({msg})"),
            #[cfg(feature = "encrypt")]
            Self::EncryptionKeyMismatch => f.write_str(
                "emdb: encryption key mismatch (file was created with a different key)",
            ),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_not_implemented_display_is_stable() {
        let msg = format!("{}", Error::NotImplemented);
        assert!(msg.contains("not yet implemented"));
    }

    #[test]
    fn test_error_implements_std_error() {
        fn assert_error<E: std::error::Error>() {}
        assert_error::<Error>();
    }

    #[test]
    fn test_io_error_display_does_not_leak_payload() {
        let err = Error::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "secret",
        ));
        let msg = format!("{}", err);
        assert!(msg.contains("permission denied") || msg.contains("PermissionDenied"));
        assert!(!msg.contains("secret"));
    }

    #[test]
    fn test_version_mismatch_display_is_stable() {
        let msg = format!(
            "{}",
            Error::VersionMismatch {
                found: 2,
                expected: 1,
            }
        );
        assert!(msg.contains("found 2"));
        assert!(msg.contains("expected 1"));
    }

    #[test]
    fn test_corrupted_display_includes_offset_and_reason() {
        let msg = format!(
            "{}",
            Error::Corrupted {
                offset: 42,
                reason: "crc mismatch",
            }
        );
        assert!(msg.contains("42"));
        assert!(msg.contains("crc mismatch"));
    }

    #[test]
    fn test_from_io_maps_to_io_variant() {
        let err: Error = std::io::Error::new(std::io::ErrorKind::NotFound, "missing").into();
        assert!(matches!(err, Error::Io(_)));
    }

    #[test]
    fn test_transaction_error_displays_are_stable() {
        let invalid = format!("{}", Error::TransactionInvalid);
        assert!(invalid.contains("transaction"));

        let aborted = format!("{}", Error::TransactionAborted("invariant"));
        assert!(aborted.contains("invariant"));
    }

    #[test]
    fn test_lock_errors_display_are_stable() {
        let busy = format!(
            "{}",
            Error::LockBusy {
                path: std::path::PathBuf::from("/tmp/demo.lock"),
            }
        );
        assert!(busy.contains("lock busy"));

        let io_msg = format!("{}", Error::LockfileError(std::io::Error::other("x")));
        assert!(io_msg.contains("lockfile error"));

        let poisoned = format!("{}", Error::LockPoisoned);
        assert!(poisoned.contains("lock poisoned"));
    }

    #[cfg(feature = "nested")]
    #[test]
    fn test_invalid_path_display_is_stable() {
        let msg = format!("{}", Error::InvalidPath);
        assert_eq!(msg, "emdb: invalid nested path");
    }

    #[cfg(feature = "ttl")]
    #[test]
    fn test_ttl_overflow_display_is_stable() {
        let msg = format!("{}", Error::TtlOverflow);
        assert_eq!(msg, "emdb: ttl overflow");
    }
}
