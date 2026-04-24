# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased](https://github.com/jamesgober/emdb-rs/compare/v0.4.0...HEAD)

## [0.4.0](https://github.com/jamesgober/emdb-rs/compare/v0.3.0...v0.4.0) — 2026-04-24

### Added

- Closure-based transaction API via `Emdb::transaction(|tx| ...)`.
- `Transaction` methods: `insert`, `get`, `remove`, `contains_key`.
- Atomic batch log markers: `BatchBegin` and `BatchEnd` op records.
- Crash-safe replay rules for incomplete or corrupted transactional batches.
- Transaction id tracking persisted in file header (`last_tx_id`).
- v0.4 integration tests for commit/rollback, read-your-writes, crash recovery, and tx id persistence.
- Transaction benchmarks for single-op and multi-op batch throughput.

### Changed

- On-disk format advanced to version `2` for new files.
- Reader accepts both v1 and v2 headers.
- `Error` expanded with transaction variants (`TransactionInvalid`, `TransactionAborted`).
- Crate version bumped to `0.4.0`.

### Fixed

- Replay now truncates and discards malformed or incomplete transactional batches at recovery boundaries.

## [0.3.0](https://github.com/jamesgober/emdb-rs/compare/v0.2.0...v0.3.0) — 2026-04-24

### Added

- Persistent file-backed open path via `Emdb::open(path)`.
- New `FlushPolicy` configuration with `OnEachWrite`, `EveryN`, and `Manual`.
- Storage backend abstraction (`Storage`) with file and memory implementations.
- Append-only operation log with CRC-32 record integrity checks.
- Crash recovery replay behavior for truncated/corrupted tail records.
- Public persistence control APIs: `flush`, `compact`, and `path`.
- Integration test coverage for persistence round-trip, recovery, format mismatches, and compaction.
- Persistence benchmark suite comparing write throughput across flush policies.

### Changed

- `EmdbBuilder::build()` is now fallible and returns `Result<Emdb>`.
- `Error` expanded with v0.3 persistence variants (`Io`, `MagicMismatch`, `VersionMismatch`, `FeatureMismatch`, `Corrupted`, `InvalidConfig`).
- README updated to Phase 2 and persistent usage examples.
- Crate version bumped to `0.3.0`.

### Fixed

- Recovery now truncates invalid trailing records and reopens successfully when crash tails are detected.

## [0.2.0](https://github.com/jamesgober/emdb-rs/compare/v0.1.0...v0.2.0) — 2026-04-24

### Added

- Functional in-memory key/value storage backed by `BTreeMap<Vec<u8>, Record>`.
- Core API methods: `insert`, `get`, `remove`, `contains_key`, `clear`, `iter`, and `keys`.
- `EmdbBuilder` with `build` and `default_ttl` (when `ttl` feature is enabled).
- `ttl` feature (enabled by default) with:
	- `Ttl` policy enum.
	- `insert_with_ttl`, `expires_at`, `ttl`, `persist`, and `sweep_expired` APIs.
	- Expiration-aware visibility for `get`, `contains_key`, `iter`, and `keys`.
- `nested` feature with dotted-prefix ergonomics:
	- `group`, `delete_group`, and `focus` on `Emdb`.
	- `Focus` scoped operations: `set`, `get`, `remove`, `contains_key`, `focus`, `iter`, and `delete_all`.
	- `Focus::set_with_ttl` when both `nested` and `ttl` are enabled.
- Integration tests for core, ttl, nested, and feature matrix behavior.
- Criterion benchmark scaffold in `benches/kv.rs` for insert/get/remove throughput.

### Changed

- Crate root docs now include base usage plus feature-gated ttl and nested examples.
- CI matrix now validates all feature combinations (`""`, `ttl`, `nested`, `ttl nested`) across Linux, macOS, and Windows.
- README updated to Phase 1 status and expanded with features plus ttl/nested examples.

### Fixed

- Feature-gated error variants and cfg hygiene so all feature combinations build and test cleanly.

## [0.1.0](https://github.com/jamesgober/emdb-rs/releases/tag/v0.1.0) — 2026-04-24

### Added

- Initial crate scaffold.
- `Emdb` struct with `open_in_memory`, `len`, and `is_empty` stubs.
- `Error` enum with `NotImplemented` placeholder variant.
- `Result<T>` type alias.
- Apache-2.0 license.
- CI workflow for Linux, macOS, and Windows.
- REPS (Rust Efficiency & Performance Standards) compliance at crate root.
