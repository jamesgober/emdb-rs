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
//! emdb is an **fsys-journal-backed append-only KV** with a sharded
//! in-memory hash index. Writes go through `fsys::JournalHandle`'s
//! lock-free LSN reservation + group-commit fsync; reads slice
//! directly into a kernel-managed memory map of the same file
//! (zero-copy). Crash safety is delegated to fsys's CRC-32C frame
//! validation and five-state tail-truncation taxonomy.
//!
//! This is the Bitcask family of storage engines (one append-only
//! log + an in-memory index), built on top of fsys for the
//! filesystem substrate. fsys handles platform-specific durability
//! (NVMe passthrough flush on Linux + Windows, io_uring on Linux,
//! `WRITE_THROUGH` where appropriate); emdb handles the
//! engine-level concerns (per-namespace sharded indices,
//! encryption, range scans, TTL).
//!
//! **Reads** are lock-free — the 64-shard primary index plus the
//! `Arc<Mmap>` zero-copy read path scale to many millions of
//! operations per second on a single open handle. **Writes** are
//! lock-free via fsys's atomic LSN reservation; no writer mutex
//! on the hot append path. Producers can still batch through
//! [`Emdb::insert_many`] or [`Emdb::transaction`] when group
//! semantics matter.
//!
//! ## Quick start
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
//! Persistent file-backed:
//!
//! ```no_run
//! use emdb::Emdb;
//!
//! let path = std::env::temp_dir().join("emdb-doc-example.emdb");
//! {
//!     let db = Emdb::open(&path)?;
//!     db.insert("name", "emdb")?;
//!     db.flush()?;        // make record bytes durable
//!     db.checkpoint()?;   // persist tail_hint for fast reopen
//! }
//! let db = Emdb::open(&path)?;
//! assert_eq!(db.get("name")?, Some(b"emdb".to_vec()));
//! # let _cleanup = std::fs::remove_file(path);
//! # Ok::<(), emdb::Error>(())
//! ```
//!
//! TTL:
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
//! ## Zero-copy reads
//!
//! [`Emdb::get_zerocopy`] returns a [`ValueRef`] that points directly
//! into the kernel-managed mmap region — no allocation, no copy.
//! Encrypted databases fall back to an owned plaintext buffer inside
//! the same [`ValueRef`] type.
//!
//! ```rust
//! use emdb::Emdb;
//!
//! let db = Emdb::open_in_memory();
//! db.insert("k", "v")?;
//! if let Some(v) = db.get_zerocopy("k")? {
//!     let want: &[u8] = b"v";
//!     assert!(v == want);
//! }
//! # Ok::<(), emdb::Error>(())
//! ```
//!
//! ## Streaming iteration
//!
//! [`Emdb::iter`] / [`Emdb::keys`] yield records lazily, decoding one
//! record per `next()` call from a snapshot of offsets captured at
//! construction time. Memory use scales with the offset count, not
//! the total value size.
//!
//! Range queries are opt-in via
//! [`EmdbBuilder::enable_range_scans`]; once enabled,
//! [`Emdb::range_iter`] / [`Emdb::range_prefix_iter`] return streaming
//! iterators backed by a parallel `BTreeMap` secondary index.
//!
//! ## Group-commit durability
//!
//! Per-record `flush()` workloads with concurrent writers can opt
//! into the group-commit pipeline so multiple in-flight `flush()`
//! calls share a single `fdatasync`:
//!
//! ```no_run
//! use emdb::{Emdb, FlushPolicy};
//!
//! let db = Emdb::builder()
//!     .flush_policy(FlushPolicy::Group)
//!     .build()?;
//! # Ok::<(), emdb::Error>(())
//! ```
//!
//! Default policy is [`FlushPolicy::OnEachFlush`], which performs one
//! `fdatasync` per call — the right choice when there is only one
//! writer thread or when durability is already batched at the
//! application layer.
//!
//! ## Storage path resolution
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
//!
//! ## Operational APIs
//!
//! - [`Emdb::stats`] — point-in-time database introspection
//!   (record counts, file size, namespace count). Cheap to call
//!   from a per-second health-check loop.
//! - [`Emdb::backup_to`] — atomic snapshot to a sibling file. The
//!   result is a normal openable database, not a dump format.
//! - [`Emdb::lock_holder`] / [`Emdb::break_lock`] — diagnose and
//!   recover from stuck advisory lockfiles when a holder dies
//!   without releasing.
//! - [`Emdb::checkpoint`] — explicit fast-reopen checkpoint that
//!   persists the file header's `tail_hint`.
//!
//! ## Cargo features
//!
//! - `ttl` *(default)* — per-record expiration and `default_ttl`.
//! - `nested` — dotted-prefix group operations and `Focus` handles.
//! - `encrypt` — AES-256-GCM + ChaCha20-Poly1305 at-rest encryption
//!   with raw-key or Argon2id-derived passphrase.
//! - `bench-compare`, `bench-rocksdb`, `bench-redis` — comparative
//!   bench peers (dev-only, never required by application builds).

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
#![deny(clippy::undocumented_unsafe_blocks)]
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
mod stats;
mod storage;
mod transaction;
mod ttl;
mod value_ref;

pub use builder::EmdbBuilder;
pub use db::{Emdb, EmdbIter, EmdbKeyIter, EmdbRangeIter};
#[cfg(feature = "encrypt")]
pub use encryption::{Cipher, EncryptionInput};
pub use error::{Error, Result};
pub use lockfile::LockHolder;
pub use namespace::{Namespace, NamespaceIter, NamespaceKeyIter, NamespaceRangeIter};
#[cfg(feature = "nested")]
pub use nested::Focus;
pub use stats::EmdbStats;
pub use storage::FlushPolicy;
pub use transaction::Transaction;
pub use ttl::Ttl;
pub use value_ref::ValueRef;
