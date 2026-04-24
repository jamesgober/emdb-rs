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
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotImplemented => f.write_str("emdb: operation not yet implemented"),
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
}
