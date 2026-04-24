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
//! let mut db = Emdb::open_in_memory();
//! db.insert("name", "emdb")?;
//! assert_eq!(db.get("name")?, Some(b"emdb".to_vec()));
//! # Ok::<(), emdb::Error>(())
//! ```
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
//! let mut db = Emdb::builder().default_ttl(Duration::from_secs(60)).build();
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
//! let mut db = Emdb::open_in_memory();
//! let mut profile = db.focus("profile");
//! profile.set("name", "james")?;
//! assert_eq!(profile.get("name")?, Some(b"james".to_vec()));
//! # }
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
#![cfg_attr(docsrs, feature(doc_cfg))]

mod builder;
mod db;
mod error;
#[cfg(feature = "nested")]
mod nested;
mod ttl;

pub use builder::EmdbBuilder;
pub use db::Emdb;
pub use error::{Error, Result};
#[cfg(feature = "nested")]
pub use nested::Focus;
pub use ttl::Ttl;
