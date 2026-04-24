// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Error types for the `emdb` crate.
//!
//! All fallible operations return [`Result<T>`] — an alias for
//! `core::result::Result<T, Error>`. The [`Error`] type enumerates every
//! failure mode the crate can produce. Error codes are reserved under the
//! `EM-XXXXX` prefix in the wider Hive error registry.

use core::fmt;

/// Convenient `Result` alias where the error type is fixed to [`Error`].
pub type Result<T> = core::result::Result<T, Error>;

/// The top-level error type returned by every fallible operation in `emdb`.
///
/// The variant set is intentionally small during the early development
/// phase. It will grow as concrete subsystems land (storage engine,
/// transaction manager, query layer, etc.).
#[derive(Debug, Clone, PartialEq, Eq)]
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
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotImplemented => f.write_str("emdb: operation not yet implemented"),
            #[cfg(feature = "nested")]
            Self::InvalidPath => f.write_str("emdb: invalid nested path"),
            #[cfg(feature = "ttl")]
            Self::TtlOverflow => f.write_str("emdb: ttl overflow"),
        }
    }
}

impl std::error::Error for Error {}

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
