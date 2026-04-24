// Copyright 2026 James Gober.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0

//! # emdb
//!
//! A lightweight, high-performance embedded database for Rust.
//!
//! This crate provides an in-memory key/value store with optional TTL handling
//! and nested-key ergonomics.
//!
//! The API is still pre-1.0 and may change. See the repository for roadmap and
//! status:
//! <https://github.com/jamesgober/emdb-rs>
//!
//! ## Examples
//!
//! Base key/value usage:
//!
//! ```rust
//! use emdb::Emdb;
//!
//! let db = Emdb::open_in_memory();
//! db.insert("name", "emdb")?;
//! assert_eq!(db.get("name")?, Some(b"emdb".to_vec()));
//! # Ok::<(), emdb::Error>(())
//! ```
//!
//! Persistent usage:
//!
//! ```rust
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
//! Transaction usage:
//!
//! ```rust
//! use emdb::Emdb;
//!
//! let db = Emdb::open_in_memory();
//! db.transaction(|tx| {
//!     tx.insert("a", "1")?;
//!     tx.insert("b", "2")?;
//!     Ok(())
//! })?;
//! assert_eq!(db.get("a")?, Some(b"1".to_vec()));
//! # Ok::<(), emdb::Error>(())
//! ```
//!
//! ## Crash Safety
//!
//! Transactions use atomic batch markers in the append-only log.
//! During replay, incomplete batches are discarded and complete batches
//! are applied in full.
//!
//! TTL usage:
//!
//! ```rust
//! # #[cfg(feature = "ttl")]
//! # {
//! use std::time::Duration;
//!
//! use emdb::{Emdb, Ttl};
//!
//! let db = Emdb::builder()
//!     .default_ttl(Duration::from_secs(60))
//!     .build()?;
//! db.insert_with_ttl("session", "token", Ttl::Default)?;
//! assert!(db.ttl("session")?.is_some());
//! # }
//! # Ok::<(), emdb::Error>(())
//! ```
//!
//! Nested usage:
//!
//! ```rust
//! # #[cfg(feature = "nested")]
//! # {
//! use emdb::Emdb;
//!
//! let db = Emdb::open_in_memory();
//! let profile = db.focus("profile");
//! profile.set("name", "james")?;
//! assert_eq!(profile.get("name")?, Some(b"james".to_vec()));
//! # }
//! # Ok::<(), emdb::Error>(())
//! ```

//! ## Concurrency Model
//!
//! `Emdb` is internally reference-counted and synchronized. `Clone` is cheap
//! and can be used to share one database handle across threads.
//!
//! - Reads (`get`, `contains_key`, `len`, `iter`) can run concurrently.
//! - Writes are serialized.
//! - Transactions hold the write lock for the closure lifetime.
//! - File-backed databases use an advisory lockfile to prevent two processes
//!   from opening the same path concurrently.

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

mod builder;
mod db;
mod error;
mod lockfile;
#[cfg(feature = "nested")]
mod nested;
mod storage;
mod transaction;
mod ttl;

pub use builder::EmdbBuilder;
pub use db::Emdb;
pub use error::{Error, Result};
#[cfg(feature = "nested")]
pub use nested::Focus;
pub use storage::FlushPolicy;
pub use transaction::Transaction;
pub use ttl::Ttl;
