// Copyright 2026 James Gober.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0

//! # emdb
//!
//! A high-performance embedded key-value database for Rust.
//!
//! ## Architecture
//!
//! emdb is an **mmap-backed append-only KV** with a sharded in-memory
//! hash index. Writes go through `pwrite` at a single tail offset;
//! reads slice directly into the kernel-managed memory map (zero-copy).
//! Crash safety comes from per-record CRC32 framing — recovery scan
//! truncates at the first bad CRC. This is the Bitcask family of
//! storage engines, the same shape used by Riak, HaloDB, and others.
//!
//! ## Examples
//!
//! Persistent usage:
//!
//! ```no_run
//! use emdb::Emdb;
//!
//! let path = std::env::temp_dir().join("emdb-doc-example.emdb");
//! {
//!     let db = Emdb::open(&path)?;
//!     db.insert("name", "emdb")?;
//!     db.flush()?;
//! }
//! let db = Emdb::open(&path)?;
//! assert_eq!(db.get("name")?, Some(b"emdb".to_vec()));
//! # let _cleanup = std::fs::remove_file(path);
//! # Ok::<(), emdb::Error>(())
//! ```
//!
//! TTL usage:
//!
//! ```no_run
//! # #[cfg(feature = "ttl")]
//! # {
//! use std::time::Duration;
//!
//! use emdb::{Emdb, Ttl};
//!
//! let path = std::env::temp_dir().join("emdb-doc-ttl.emdb");
//! let db = Emdb::builder()
//!     .path(&path)
//!     .default_ttl(Duration::from_secs(60))
//!     .build()?;
//! db.insert_with_ttl("session", "token", Ttl::Default)?;
//! assert!(db.ttl("session")?.is_some());
//! # let _cleanup = std::fs::remove_file(path);
//! # }
//! # Ok::<(), emdb::Error>(())
//! ```
//!
//! ## Storage Path Resolution
//!
//! emdb does not pick a default path for you. You either pass an
//! explicit path, or opt into OS-aware resolution via the builder.
//!
//! ```no_run
//! use emdb::Emdb;
//!
//! // Resolves to:
//! //   Linux:   $XDG_DATA_HOME/hivedb-kv/sessions.emdb
//! //   macOS:   ~/Library/Application Support/hivedb-kv/sessions.emdb
//! //   Windows: %LOCALAPPDATA%\hivedb-kv\sessions.emdb
//! let db = Emdb::builder()
//!     .app_name("hivedb-kv")
//!     .database_name("sessions.emdb")
//!     .build()?;
//! # Ok::<(), emdb::Error>(())
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
// Test code is allowed to use the convenience panickers — the strict
// lint profile above is for production library code, not assertion
// scaffolding inside `#[cfg(test)] mod tests` blocks.
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::print_stdout,
        clippy::print_stderr
    )
)]
#![cfg_attr(docsrs, feature(doc_cfg))]

mod builder;
mod data_dir;
mod db;
#[cfg(feature = "encrypt")]
#[allow(dead_code)]
mod encryption;
#[cfg(feature = "encrypt")]
mod encryption_admin;
mod error;
mod lockfile;
mod namespace;
#[cfg(feature = "nested")]
mod nested;
mod storage;
mod transaction;
mod ttl;
mod value_ref;

pub use builder::EmdbBuilder;
pub use db::{Emdb, EmdbIter, EmdbKeyIter, EmdbRangeIter};
#[cfg(feature = "encrypt")]
pub use encryption::{Cipher, EncryptionInput};
pub use error::{Error, Result};
pub use namespace::{Namespace, NamespaceIter, NamespaceKeyIter, NamespaceRangeIter};
#[cfg(feature = "nested")]
pub use nested::Focus;
pub use storage::FlushPolicy;
pub use transaction::Transaction;
pub use ttl::Ttl;
pub use value_ref::ValueRef;
