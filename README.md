<h1 align="center">
    <strong>emdb</strong>
    <br>
    <sup><sub>EMBEDDED DATABASE FOR RUST</sub></sup>
</h1>

<p align="center">
    <a href="https://crates.io/crates/emdb"><img alt="crates.io" src="https://img.shields.io/crates/v/emdb.svg"></a>
    <a href="https://docs.rs/emdb"><img alt="docs.rs" src="https://docs.rs/emdb/badge.svg"></a>
    <a href="https://github.com/jamesgober/emdb-rs/actions"><img alt="CI" src="https://github.com/jamesgober/emdb-rs/actions/workflows/ci.yml/badge.svg"></a>
    <a href="https://github.com/jamesgober/emdb-rs/blob/main/LICENSE"><img alt="License" src="https://img.shields.io/badge/license-Apache--2.0-blue.svg"></a>
</p>

<p align="center">
    A lightweight, high-performance embedded database for Rust.
</p>

---

## Status

**Phase 3.** This crate now provides closure-based transactions with
atomic batch writes on top of file-backed persistence and crash recovery.
The API is still pre-1.0 and may change before 1.0.

Track progress and roadmap: <https://github.com/jamesgober/emdb-rs>

## Installation

```toml
[dependencies]
emdb = "0.4"
```

## Quick Start

```rust
use emdb::Emdb;

let mut db = Emdb::open_in_memory();
db.insert("name", "emdb")?;
assert_eq!(db.get("name")?, Some(b"emdb".to_vec()));
# Ok::<(), emdb::Error>(())
```

## Persistence

```rust
use emdb::{Emdb, FlushPolicy};

let path = std::env::temp_dir().join("app.emdb");

{
    let mut db = Emdb::builder()
        .path(path.clone())
        .flush_policy(FlushPolicy::EveryN(64))
        .build()?;

    db.insert("user:1", "james")?;
    db.flush()?;
}

let reopened = Emdb::open(&path)?;
assert_eq!(reopened.get("user:1")?, Some(b"james".to_vec()));
# let _cleanup = std::fs::remove_file(path);
# Ok::<(), emdb::Error>(())
```

Manual compaction:

```rust
use emdb::Emdb;

let path = std::env::temp_dir().join("compact.emdb");
let mut db = Emdb::open(&path)?;
db.insert("k", "v")?;
db.compact()?;
db.flush()?;
# let _cleanup = std::fs::remove_file(path);
# Ok::<(), emdb::Error>(())
```

## Transactions

Commit path:

```rust
use emdb::Emdb;

let mut db = Emdb::open_in_memory();
db.transaction(|tx| {
    tx.insert("user:1", "james")?;
    tx.insert("user:2", "alex")?;
    Ok(())
})?;

assert_eq!(db.get("user:1")?, Some(b"james".to_vec()));
assert_eq!(db.get("user:2")?, Some(b"alex".to_vec()));
# Ok::<(), emdb::Error>(())
```

Rollback path:

```rust
use emdb::{Emdb, Error};

let mut db = Emdb::open_in_memory();
let failed = db.transaction::<_, ()>(|tx| {
    tx.insert("temp", "value")?;
    Err(Error::TransactionAborted("rollback"))
});

assert!(failed.is_err());
assert_eq!(db.get("temp")?, None);
# Ok::<(), emdb::Error>(())
```

### Crash Safety

Transactions are written as `BatchBegin ... BatchEnd` records.
If a crash occurs before `BatchEnd`, the entire batch is discarded during
replay. If a crash occurs after `BatchEnd`, the entire batch is applied.

## Features

- `ttl` (default): per-record expiration and default TTL support.
- `nested`: dotted-prefix group operations and `Focus` handles.
- persistence (core): append-only file log, replay-on-open, flush policy,
  and compaction.
- transactions (core): closure-based atomic batches with read-your-writes
    and crash-safe replay.

### TTL Example

```rust
# #[cfg(feature = "ttl")]
# {
use std::time::Duration;

use emdb::{Emdb, Ttl};

let mut db = Emdb::builder()
    .default_ttl(Duration::from_secs(30))
    .build()?;
db.insert_with_ttl("session", "token", Ttl::Default)?;
assert!(db.ttl("session")?.is_some());
# }
# Ok::<(), emdb::Error>(())
```

### Nested Example

```rust
# #[cfg(feature = "nested")]
# {
use emdb::Emdb;

let mut db = Emdb::open_in_memory();
let mut product = db.focus("product");
product.set("name", "phone")?;
product.set("price", "799")?;

assert_eq!(product.get("name")?, Some(b"phone".to_vec()));
assert_eq!(db.group("product").count(), 2);
# }
# Ok::<(), emdb::Error>(())
```

## Goals

- **Embedded-first** — runs in-process; no separate server, no network.
- **High performance** — zero-copy reads, allocation-free hot paths, cache-friendly layout.
- **Safe** — strict `clippy` profile, no `unwrap` in library code, all `unsafe` documented.
- **Small footprint** — minimal dependency graph, fast compile times.
- **Portable** — Linux, macOS, Windows (x86_64 and ARM64).

## Non-Goals

- Client-server operation (use a dedicated DBMS for that).
- A full SQL dialect at this stage.
- Distributed replication at this stage.

## Related Projects

`emdb` is the Rust implementation. Implementations in other languages (Go, C, and others) are planned and will live under their own repositories.

## License

Licensed under the [Apache License, Version 2.0](./LICENSE).

Copyright &copy; 2026 James Gober.
