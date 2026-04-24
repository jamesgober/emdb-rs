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

**Early development.** This crate is in its initial scaffolding phase. The public API is unstable and will change before the 1.0 release. The crate name on crates.io is reserved; do not depend on any specific behavior yet.

Track progress and roadmap: <https://github.com/jamesgober/emdb-rs>

## Installation

```toml
[dependencies]
emdb = "0.1"
```

## Quick Start

```rust
use emdb::Emdb;

let db = Emdb::open_in_memory();
assert_eq!(db.len(), 0);
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
