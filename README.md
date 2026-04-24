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

**Phase 1.** This crate now provides a functional in-memory key/value store.
The API is still pre-1.0 and may change before 1.0.

Track progress and roadmap: <https://github.com/jamesgober/emdb-rs>

## Installation

```toml
[dependencies]
emdb = "0.2"
```

## Quick Start

```rust
use emdb::Emdb;

let mut db = Emdb::open_in_memory();
db.insert("name", "emdb")?;
assert_eq!(db.get("name")?, Some(b"emdb".to_vec()));
# Ok::<(), emdb::Error>(())
```

## Features

- `ttl` (default): per-record expiration and default TTL support.
- `nested`: dotted-prefix group operations and `Focus` handles.

### TTL Example

```rust
# #[cfg(feature = "ttl")]
# {
use std::time::Duration;

use emdb::{Emdb, Ttl};

let mut db = Emdb::builder().default_ttl(Duration::from_secs(30)).build();
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
