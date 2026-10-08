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
//! **Reads** of the default namespace take no lock and write no
//! shared cache line: the 64-shard primary index is probed with
//! seqlock reads and the journal mapping is borrowed under an epoch
//! guard, so aggregate `get` throughput grows with reader threads.
//! **Writes** to one key are linearizable: each write holds a
//! per-key stripe lock across the journal append and the index
//! update, so the log and memory agree on the order of writes to
//! every key. Writes to different keys proceed in parallel (index
//! shards are updated under short per-shard writer locks; on Windows
//! the journal append itself is serialised, see
//! `docs/PLATFORM-NOTES.md`). Producers can batch through
//! [`Emdb::insert_many`] or [`Emdb::transaction`]; neither is an
//! isolated or crash-atomic transaction (see [`Emdb::transaction`]).
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
//!     db.checkpoint()?;   // sync the journal and rewrite the sidecar
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
//! construction time: each record is yielded with the value it had
//! when the iterator was created. Memory use scales with the offset
//! count, not the total value size.
//!
//! Range queries are opt-in via
//! [`EmdbBuilder::enable_range_scans`]; once enabled,
//! [`Emdb::range_iter`] / [`Emdb::range_prefix_iter`] return cursors
//! over a lock-free `crossbeam_skiplist::SkipMap` secondary index.
//! They hold no snapshot, so `iter_from(k).take(10)` costs a seek
//! plus ten records however large the range is. A compaction while
//! a range iterator runs is transparent: it continues after its last
//! key in the compacted file.
//!
//! With the `ttl` feature, every read path (`get`, `contains_key`,
//! iterators, ranges) treats records whose TTL has passed as absent;
//! [`Emdb::len`] counts them until [`Emdb::sweep_expired`] removes
//! them. Iterators skip records that fail to decode without reporting
//! an error.
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
//! - [`Emdb::checkpoint`]: sync the journal and rewrite the `.meta`
//!   sidecar. It does not shorten the next open's recovery scan.
//!
//! ## Async surface
//!
//! Opt-in via the `async` feature. Wraps the sync API in
//! `tokio::task::spawn_blocking` so blocking I/O never stalls the
//! async-task scheduler. Exposes `AsyncEmdb` and `AsyncNamespace`,
//! plus `EmdbBuilder::build_async` for the builder path.
//!
//! ```ignore
//! # // gated behind `async` feature
//! use emdb::{AsyncEmdb, Emdb};
//!
//! # async fn ex() -> Result<(), emdb::Error> {
//! // Open via the simple constructor.
//! let db = AsyncEmdb::open("/tmp/users.emdb").await?;
//! db.insert("alice", "active").await?;
//! let value = db.get("alice").await?;
//!
//! // Or build with explicit configuration.
//! let configured = Emdb::builder()
//!     .path("/tmp/configured.emdb")
//!     .enable_range_scans(true)
//!     .build_async()
//!     .await?;
//! # let _ = (db, configured); Ok(())
//! # }
//! ```
//!
//! Every async method clones the underlying `Arc<Emdb>` into a
//! `spawn_blocking` closure; cheap, but each call allocates owned
//! `Vec<u8>` copies for key/value bytes so the closure can take
//! them by value. For latency-sensitive workloads where the
//! spawn dispatch overhead exceeds the sync cost (e.g. tight
//! `get` loops on a hot in-memory key), reach for the sync
//! handle via `AsyncEmdb::sync_handle` and batch via
//! `insert_many` / `range`.
//!
//! Large iterations come in two flavours. `iter` / `keys` / `range`
//! / `range_prefix` / `iter_from` / `iter_after` materialise the
//! full result into an owned `Vec` before resolving — convenient
//! for small queries. The `*_stream` variants
//! (`iter_stream`, `keys_stream`, `range_stream`,
//! `range_prefix_stream`, `iter_from_stream`, `iter_after_stream`)
//! return a `tokio_stream::wrappers::ReceiverStream` backed by a
//! bounded mpsc channel: records arrive incrementally, the
//! blocking pump task respects the consumer's backpressure, and
//! memory in flight is bounded by the channel depth rather than
//! the namespace size.
//!
//! ## Cargo features
//!
//! - `ttl` *(default)* — per-record expiration and `default_ttl`.
//! - `nested` — dotted-prefix group operations and `Focus` handles.
//! - `encrypt` — AES-256-GCM + ChaCha20-Poly1305 at-rest encryption
//!   with raw-key or Argon2id-derived passphrase.
//! - `async` — `AsyncEmdb` / `AsyncNamespace` wrappers via
//!   tokio's `spawn_blocking`, plus streaming-iterator variants
//!   backed by `tokio_stream::wrappers::ReceiverStream`. Pulls in
//!   `tokio` (`rt` + `rt-multi-thread` + `macros` + `sync`) and
//!   `tokio-stream`.
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

#[cfg(feature = "async")]
mod async_impl;
mod builder;
mod data_dir;
mod db;
#[cfg(feature = "encrypt")]
mod encryption;
mod encryption_admin;
mod error;
mod lockfile;
mod namespace;
#[cfg(feature = "nested")]
mod nested;
mod private_fs;
mod stats;
mod storage;
mod transaction;
mod ttl;
mod value_ref;

#[cfg(feature = "async")]
pub use async_impl::{AsyncEmdb, AsyncNamespace};
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

/// Entry points for the cargo-fuzz targets in `fuzz/`. Compiled only
/// under `--cfg fuzzing` (which cargo-fuzz sets), so it is not part of
/// the public API of any normal build.
#[cfg(fuzzing)]
#[doc(hidden)]
pub mod __fuzz {
    /// Plaintext payload decoder.
    pub fn decode_payload(bytes: &[u8]) {
        let _ = crate::storage::format::decode_payload(bytes);
    }

    /// Frame-length based payload slicing.
    pub fn payload_at(bytes: &[u8], offset: usize) {
        let _ = crate::storage::format::payload_at(bytes, offset);
    }

    /// Meta sidecar decoder; a decoded header must re-encode to an
    /// equal header.
    pub fn meta_decode(bytes: &[u8]) {
        if let Ok(header) = crate::storage::meta::MetaHeader::decode(bytes) {
            let encoded = header.encode();
            let again = crate::storage::meta::MetaHeader::decode(&encoded).ok();
            assert_eq!(again, Some(header), "meta header does not round-trip");
        }
    }

    /// Encrypted payload decoder with a fixed key.
    #[cfg(feature = "encrypt")]
    pub fn decode_encrypted_with_fixed_key(bytes: &[u8]) {
        let ctx = crate::encryption::EncryptionContext::from_key(&[7_u8; 32]);
        let _ = crate::storage::format::decode_payload_encrypted(bytes, |nonce, ct| {
            let mut input = nonce.to_vec();
            input.extend_from_slice(ct);
            ctx.decrypt(&input)
        });
    }

    /// Encrypt `plain` under the fixed fuzz key (`nonce || ct || tag`)
    /// so the post-AEAD body decoders are reachable.
    #[cfg(feature = "encrypt")]
    pub fn encrypt_fixed(plain: &[u8]) -> Vec<u8> {
        let ctx = crate::encryption::EncryptionContext::from_key(&[7_u8; 32]);
        ctx.encrypt(plain).unwrap_or_default()
    }
}
