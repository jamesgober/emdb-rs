// Copyright 2026 James Gober.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0

//! # emdb
//!
//! A lightweight, high-performance embedded database for Rust.
//!
//! This crate is in early development. The API is unstable and will change
//! before the 1.0 release. See the repository for roadmap and status:
//! <https://github.com/jamesgober/emdb-rs>
//!
//! ## Example
//!
//! ```rust
//! use emdb::Emdb;
//!
//! let db = Emdb::open_in_memory();
//! assert_eq!(db.len(), 0);
//! ```

#![deny(warnings)]
#![deny(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]
#![deny(unused_must_use)]
#![deny(unused_results)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::todo)]
#![deny(clippy::unimplemented)]
#![deny(clippy::print_stdout)]
#![deny(clippy::print_stderr)]
#![deny(clippy::dbg_macro)]
#![deny(clippy::unreachable)]
#![cfg_attr(docsrs, feature(doc_cfg))]

mod error;

pub use error::{Error, Result};

/// The primary embedded database handle.
///
/// This type is the entry point for interacting with an `emdb` instance.
/// The current implementation is a stub — the stable API has not yet
/// landed. See the crate root documentation for status.
#[derive(Debug, Default)]
pub struct Emdb {
    len: usize,
}

impl Emdb {
    /// Open a new in-memory database.
    ///
    /// In-memory databases are volatile — all data is lost when the
    /// instance is dropped. Use this mode for tests, ephemeral caches,
    /// or scratch storage.
    #[must_use]
    pub const fn open_in_memory() -> Self {
        Self { len: 0 }
    }

    /// Return the number of records currently stored in the database.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Return `true` if the database contains no records.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_in_memory_returns_empty() {
        let db = Emdb::open_in_memory();
        assert_eq!(db.len(), 0);
        assert!(db.is_empty());
    }

    #[test]
    fn test_default_is_empty() {
        let db = Emdb::default();
        assert!(db.is_empty());
    }
}
