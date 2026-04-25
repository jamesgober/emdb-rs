# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased](https://github.com/jamesgober/emdb-rs/compare/v0.6.0...HEAD)

## [0.6.0](https://github.com/jamesgober/emdb-rs/compare/v0.5.0...v0.6.0) — 2026-04-25

### Added

- Page-oriented file format (v3 header) with 4 KB fixed pages for efficient sequential I/O.
- B-tree index over keys with O(log n) page-tree traversal during compaction
  and replay paths.
- Free-list management for efficient page reuse after deletions.
- Write-ahead log (WAL) sidecar with crash recovery and atomic updates.
- Automatic schema migration pipeline:
  - v1 (EMDB\0\0\0\0 magic) → v3 conversion on open.
  - v2 (v0.4 batch format) → v3 conversion on open.
  - Creates `.bak` backup on first migration; subsequent opens are idempotent.
- Public `Emdb::migrate()` API for explicit migration of legacy-format files.
- Optional memory-mapped read backend via `mmap` feature (uses `memmap2 0.9`).
- Page-format integration tests validating round-trip persistence and integrity.
- Migration integration tests covering v1→v3, v2→v3, and idempotence behavior.
- `OpRef<'_>` — borrowed view of `Op` for the storage append path. Constructing
  one is allocation-free; the previous owned `Op` forced two `Vec<u8>` clones
  per insert just to hand bytes to the WAL.
- Sharded primary index (`src/index.rs`) with 32 lock-striped shards keyed by
  FNV-1a. Reads on different keys do not block each other; concurrent writes
  contend only on the target shard.

### Changed

- On-disk format advanced to version `3` (page-oriented).
- Auto-migration occurs transparently on first open of legacy-format files.
- Reader accepts v1, v2, and v3 headers with automatic format detection.
- WAL writes go through a 64 KB userspace `BufWriter`. A burst of `append`
  calls becomes one syscall per buffer flush instead of one per record.
- `Storage::append` now takes `OpRef<'_>` instead of `&Op`. Callers no longer
  clone keys and values to construct an op; `transaction::commit` converts
  staged owned ops via `OpRef::from(&op)` at zero cost.
- `encode_op` writes directly into the output buffer (single pass) and
  CRCs over the bytes already written, eliminating the per-call payload
  `Vec` allocation.
- In-memory mode (`open_in_memory`) bypasses the storage backend entirely.
  Inserts touch only the target shard — no mutex acquisition, no dynamic
  dispatch, no WAL append.
- `Inner.backend` is now `Option<Mutex<PageStorage>>` (concrete type), not
  `Mutex<Box<dyn Storage>>`. Eliminates dynamic dispatch on every write.
- Primary index is now an unordered sharded `HashMap` instead of a single
  `RwLock<BTreeMap>`. Iteration order is unspecified — no API contract
  promised ordering, and the previous order was incidental.
- Transactions no longer hold a database-wide write lock for their lifetime.
  They stage in a closure-local overlay; commit briefly takes the backend
  mutex and every shard write lock to apply the overlay atomically.
- Insert path now holds the backend mutex across the shard write so the
  in-memory state never reorders relative to the durability log.
- Crate documentation updated to reflect persistent, migration-capable storage model.
- README phase status synchronised with v0.6 implementation checkpoint.
- Crate version bumped to `0.6.0`.

### Fixed

- Automatic migration preserves all records and metadata during v1/v2 → v3 conversion.
- `benches/concurrency.rs:87` no longer fails to compile (`&Arc<Vec<...>>`
  is not iterable; replaced with `writer_data.iter()`).
- `src/storage/page/btree.rs` MSRV violation: replaced `Option::is_none_or`
  (stable in 1.82) with `Option::map_or` to satisfy the declared 1.75 MSRV.
- `src/storage/page/btree.rs` `let _x = …` for unit-typed expression
  (clippy `let_unit_value`).
- `MemoryStorage` removed — every in-memory mode acquisition was a no-op
  Mutex and dynamic dispatch through a `Box<dyn Storage>` for nothing.

### Performance

- In-memory `kv_insert` (1,000 records): −44% wall time (~1.8× throughput).
- In-memory `kv_remove` (1,000 records): −49% wall time (~2× throughput).
- In-memory `kv_get` (1,000 records): −22% wall time (~1.3× throughput).
- Persistent `compare_read` (5,000 records): +30–53% throughput, peaking at
  ~10.5M elem/s — fastest of emdb / sled / redb / rocksdb on the reference
  workload.
- Persistent `compare_insert` (5,000 records): ~30% improvement (40K → 51K
  elem/s). The remaining gap to sled/redb is architectural (one 4 KB page
  per value) and is the primary target of the v0.7 storage redesign.

## [0.5.0](https://github.com/jamesgober/emdb-rs/compare/v0.4.0...v0.5.0) — 2026-04-24

### Added

- Cross-process lockfile exclusion via `fs4` for file-backed databases.
- Cheap `Clone` support for `Emdb` handles via shared inner state.
- Concurrency integration coverage (`tests/concurrency.rs`) for:
	- many-reader / one-writer execution,
	- concurrent transactions,
	- lock contention and lock release behavior,
	- clone-handle correctness across threads.
- Loom-gated lock-order test target (`tests/loom_tests.rs`).
- Concurrency benchmark suite (`benches/concurrency.rs`).

### Changed

- **BREAKING:** mutating `Emdb` APIs now take `&self` instead of `&mut self`.
- `Emdb` internals refactored to `Arc<Inner>` with `RwLock`-protected state and
	`Mutex`-serialized storage appends.
- Transactions now acquire and hold the state write lock for closure lifetime.
- `Error` expanded with lock-specific variants:
	`LockBusy`, `LockfileError`, and `LockPoisoned`.
- Crate version bumped to `0.5.0`.

### Fixed

- File-backed open now prevents concurrent process access to the same database
	path via advisory lockfile.

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
