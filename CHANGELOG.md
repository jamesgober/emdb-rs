# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased](https://github.com/jamesgober/emdb-rs/compare/v0.6.0...HEAD)

### Added — v0.7 Phase A–G scaffolding (engine wiring still pending)

The v0.7 redesign builds out its components alongside v0.6 so each subsystem
can be tested in isolation before the integration in Phase H. The v0.6 code
paths and public API are unchanged in this checkpoint.

- **Phase A — Slotted leaves + Rid.** New `src/storage/page/rid.rs` packs a
  `(page_id, slot_id)` pair into a single 8-byte `Rid`. New
  `src/storage/page/slotted.rs` adds the `LeafPage` slotted-page format
  (multiple records per 4 KB page, in-line and overflow flavours, slot
  tombstones, page split, in-place compact). 24 new tests including a
  randomised insert/tombstone/compact round-trip via an in-tree LCG.
- **Phase B — Per-namespace keymap.** New `src/storage/fxhash.rs` ports the
  rustc-hash FxHash algorithm in-tree (no dep). New `src/keymap.rs`
  introduces a per-namespace 32-shard `HashMap<u64, Slot>` where `Slot` is
  `Single(Rid)` or `Multi(Vec<Rid>)` to handle 64-bit hash collisions
  without losing data. 20 new tests.
- **Phase C — Page cache + v4 PageStore.** New `src/page_cache.rs` is a
  sharded `PageCache` with FIFO eviction and per-entry access counters
  (LFU swap is a one-method change). New `src/storage/v4/store.rs` opens a
  v4-magic page file (`EMDB07\0\0`), allocates pages from a free list,
  reads through the cache, COW-writes through the cache, and `fdatasync`s
  on flush. 25 new tests including disk round-trip, magic mismatch, and
  cache invalidation.
- **Phase D — Group-commit WAL.** New `src/storage/v4/wal.rs` adds an
  `append`/`wait_for_seq` API that lets multiple producers share a single
  fsync via a commit mutex; `FlushPolicy::Group { max_wait }` adds a
  background flusher thread that fsyncs on a deadline so producers never
  have to wait for explicit durability. 11 new tests including
  concurrent-burst behaviour.
- **Phase E — Value cache.** New `src/value_cache.rs` is a sharded value
  cache addressed by `(namespace_id, hash)`, bounded in bytes, with
  CLOCK (second-chance) eviction and `Arc<[u8]>` value sharing. Cache
  hits resolve under a read-lock with one atomic store. 14 new tests
  including pressure/eviction and second-chance behaviour.
- **Phase F — Namespace catalog.** New `src/storage/v4/catalog.rs` stores
  per-namespace metadata (id, name, leaf-chain head, bloom root, record
  count, tombstone flag) in chained 4 KB pages. Catalog round-trips
  through the page store with CRC validation; tombstoned namespaces are
  hidden from public lookups. 12 new tests including chained-page
  catalogs that overflow a single page.
- **Phase G — Bloom filter.** New `src/bloom.rs` is a lock-free atomic
  bloom (10 bits/key, 7 hashes via the Kirsch–Mitzenmacher two-hash
  trick). Concurrent inserts and reads progress under `fetch_or` with no
  locks. Bytes round-trip via `encode`/`from_bytes` for persistence. 10
  new tests including a false-positive-rate budget assertion.

### Optimised — pre-Phase-H performance pass

Before integrating the components into a working engine, every Phase A–G
module was audited for bottlenecks and rewritten where the implementation
was provably suboptimal. Each change is independently verified.

- **IdentityHasher for the keymap.** Keymap keys are already 64-bit
  FxHashes; running them through `RandomState`'s SipHash double-hashed on
  every operation. The new `BuildHasherDefault<IdentityHasher>` returns
  the input `u64` unchanged, halving keymap CPU on the hottest path.
- **Power-of-two bloom sizing.** `sized_bits_for_keys` now rounds up to
  a power of two so the per-hash modular reduction is `& (bit_count - 1)`
  instead of `% bit_count`. The precomputed `bit_mask` shaves one
  instruction off every of the seven probes.
- **PageCache `Mutex` → `RwLock`.** Cache hits — by far the hot path of
  every read — now take a read lock and are fully parallel; the
  `access_count` bump under that read lock is a relaxed atomic store, so
  readers never block readers.
- **PageStore atomic header.** `page_count`, `last_tx_id`,
  `namespace_root`, `free_list_head`, and `value_overflow_head` are now
  `AtomicU64` fields published with `Release` and read with `Acquire`.
  Cache-hit reads acquire **zero** mutexes; cache-miss reads acquire only
  the file mutex. `set_last_tx_id` / `set_namespace_root` reduce to a
  single atomic store.
- **Slotted-page tombstone reuse.** `LeafPage::insert_inline` and
  `insert_overflow` now scan for the lowest tombstoned slot id and
  reuse it before growing the slot array, so delete-heavy workloads do
  not inflate `slot_count` and prematurely trigger splits.
- **`Box<[AtomicU64]>` bloom storage.** Replaces the `Vec<AtomicU64>`
  backing store: 16 bytes lighter per bloom, asserts the immutable size
  at the type level. Combined with power-of-two sizing the bloom now
  carries a precomputed `bit_mask` for the hot path.
- **Cross-platform Direct I/O** (ported from the HiveDB reference).
  `IoMode::Direct` opens the page file with `O_DIRECT` (Linux),
  `F_NOCACHE` via `fcntl` (macOS), or `FILE_FLAG_NO_BUFFERING |
  FILE_FLAG_WRITE_THROUGH` (Windows). Bypasses the OS page cache for
  predictable p99 latency under load. Hard-fails on unsupported
  platforms/filesystems (REPS forbids silent degradation). Buffered
  remains the default.

### Phase H — v0.7 engine MVP

`src/storage/v4/engine.rs` ties every Phase A–G component into a working
runtime for the **default namespace**:

- **Open / flush / close** through `EngineConfig` (path, flags,
  `IoMode`, `FlushPolicy`, page-cache and value-cache budgets, bloom
  initial capacity).
- **Read path (5 layers).** L0 value cache → bloom (negative confirmation)
  → L1 keymap → L2 page cache → L3 disk via the configured I/O backend.
  Hits at L0 return without touching any other layer.
- **Write path.** WAL group-commit append → COW page mutation through
  the cache → keymap publish → bloom + value-cache populate. The
  namespace's "open leaf" is reused until `OutOfSpace`, then a new leaf
  is allocated and prepended to the chain.
- **Single-namespace MVP.** Named namespaces, replay-on-open, and the
  Emdb public-API integration land in the next checkpoint, alongside
  the v3 → v4 migrator.

### Phase H continuation — replay, catalog persistence, WAL Direct I/O, optional compression

This session pushed the v4 engine from "MVP that works in-process" toward
"real persistent KV that survives crashes". The crate version stays at
`0.6.0` while these changes accumulate; the bump conversation happens
when the v4 engine is wired through the public `Emdb` API.

- **Configurable WAL I/O mode.** New `Wal::open_with_mode` and
  `EngineConfig::wal_io_mode` accept `IoMode::Buffered` (default) or
  `IoMode::Direct`. On Windows, Direct mode adds `FILE_FLAG_WRITE_THROUGH`
  so each `write_all` is synchronously durable in one syscall. On
  Linux/macOS, Direct mode bypasses the OS page cache; the doc comment
  warns that sub-page WAL records may be rejected on filesystems that
  reject unaligned `O_DIRECT` writes — the buffered default is correct
  there.
- **Replay-on-open.** `Engine::open` now does the full crash-recovery
  ceremony: load the persisted catalog from `header.namespace_root`,
  walk every leaf in the default namespace's chain rebuilding the
  keymap and bloom from durable records, then replay any WAL records
  with `seq >= header.last_persisted_wal_seq` (the new u64 header
  field). The "leaf walk" path covers records that were checkpointed
  to pages; the "WAL replay" path covers records that were durable in
  the WAL but not yet flushed to pages. A test inserts 16 records,
  drops without `flush`, reopens, and asserts every record is
  recovered through the WAL replay alone.
- **Catalog persistence on flush.** `Engine::flush` now snapshots
  `next_seq` from the WAL, refreshes the catalog from the live
  namespace state, persists the catalog through the page store,
  records the snapshot as the new `last_persisted_wal_seq`, and only
  then drains dirty pages and `fdatasync`s. Recovery uses the
  persisted floor to skip already-applied WAL records.
- **Optional compression (`compress` feature).** New `compress` Cargo
  feature pulls in `lz4_flex` (≈30% the binary size of zstd, pure
  Rust, no `unsafe`). `encode_insert_op` calls `compress_into` on the
  value; payloads ≥ `COMPRESS_MIN_BYTES = 256` that LZ4-shrink set the
  high bit on the WAL tag (`WAL_FLAG_COMPRESSED`) and prepend a
  `original_len: u32` so the decoder can size its output buffer.
  Records below the threshold pass through unchanged. Without the
  feature, the WAL records are byte-identical to v0.6 but the decoder
  rejects any record carrying the compression flag with a
  documented `Error::InvalidConfig`. New round-trip test covers a
  2 KB highly-compressible value through replay, asserting both
  feature configurations recover the exact bytes.
- **Encryption feature flag (`encrypt`) reserved.** Cargo feature
  added without an implementation; the AES-GCM page-encryption design
  needs careful key-management work that is queued for its own
  focused session. The flag is harmless to enable today (no
  behavioural change yet) and reserves the name in the
  `bench-compare`-style feature matrix.
- **Engine extensions.** `Engine::collect_records` walks the leaf
  chain to materialise every live record as
  `Vec<(key, value, expires_at)>` for the future public-API `iter()`
  call. `Engine::clear_namespace` resets the keymap, bloom, chain
  pointers, and value cache for a namespace (the underlying leaves
  remain allocated until the future compactor reclaims them). New
  `Engine::path` accessor for the public-API wrapper to forward.
- **Storage-header field for replay floor.** `StoreHeader` gained
  `last_persisted_wal_seq: u64` plus its atomic mirror in
  `AtomicHeader`. New `set_last_persisted_wal_seq` /
  `last_persisted_wal_seq` accessors on `PageStore`. Header byte
  layout extended (offset 68); the new field is zeroed on v4 files
  that were created before this change, which means "replay from seq
  0" — correct fallback semantics.
- **Tests added.** `replay_recovers_records_when_flush_was_called`,
  `replay_recovers_records_from_wal_without_flush`,
  `large_value_round_trips_through_wal_replay` — plus refreshed
  bloom and slotted tests. Total: **88 v0.6 baseline → 230 lib
  tests + 60 integration = 290 passing across the full feature
  matrix** (`""`, `ttl`, `nested`, `mmap`, `compress`, `encrypt`,
  `bench-compare`, and combinations).

### Phase H continuation (II) — public-API wiring, v3→v4 migration, comparative benches

The v4 engine is now reachable from the existing `Emdb` public API as a
side-by-side path: existing callers see no change, but
`EmdbBuilder::prefer_v4(true)` opts the same handle into v0.7 routing
end-to-end. The crate version stays at `0.6.0` while these changes
accumulate.

- **Public-API integration without disruption.** `Emdb` grows an
  internal `v7: Option<Arc<V07Inner>>` field; every public method
  (`insert`, `get`, `remove`, `contains_key`, `len`, `is_empty`,
  `clear`, `iter`, `keys`, `flush`, `path`, plus the TTL surface)
  short-circuits through the v4 engine when that field is `Some`.
  When it is `None` the existing v0.6 code paths run unchanged, so
  every prior test and benchmark continues to exercise the v0.6
  engine bit-for-bit. `transaction()` returns
  `Error::InvalidConfig` on the v4 path (v4 transactions land in
  the next checkpoint); `compact()` falls back to `flush()`.
  `clone_handle` propagates the `Arc` so cheap-clone semantics
  carry across both engines.
- **`EmdbBuilder` v4 knobs.** New methods: `prefer_v4(bool)`,
  `page_io_mode(IoMode)`, `wal_io_mode(IoMode)`,
  `page_cache_pages(usize)`, `value_cache_bytes(usize)`,
  `bloom_initial_capacity(u64)`. The legacy `FlushPolicy` translates
  to the v4 group-commit policy via a new internal helper. `IoMode`
  is now a public re-export at the crate root with `#[non_exhaustive]`
  so we can extend it (e.g., `IoMode::DirectIfSupported`) without a
  breaking change.
- **In-place v3 → v4 migration.** New `src/storage/v4/migrate.rs`
  exposes `migrate_v3_to_v4_if_needed(path, flags)`. The migrator
  reads every record from the legacy file via the existing
  `PageStorage` reader, opens a fresh v4 engine on `<path>.v4tmp`,
  inserts every record, flushes, then atomically renames the v3 file
  to `<path>.v3bak` and the temp file into place. Already-v4 files
  are a no-op; missing files are a no-op. v1 and v2 files are
  chained through the existing v0.6 migrator (v1 → v3 → v4) so a
  user upgrading from any prior format converges on v4 in one open.
  3 new tests cover the no-op, fresh-v4, and full v3→v4 round-trip
  cases.
- **TTL through v4.** `insert_with_ttl`, `expires_at`, `ttl`,
  `persist`, and `sweep_expired` route through the v4 engine when
  the v7 field is set, matching v0.6 behaviour. The v4 record
  format already carries `expires_at: u64` so the TTL semantics
  are identity-mapped; `persist` clears the field by re-inserting
  with `expires_at = 0`. New `ttl_path_round_trips_through_v4`
  integration test covers the full TTL round-trip.
- **Comparative benchmarks rerun.** `benches/comparative.rs` now
  runs both `emdb_v06` and `emdb_v07` arms on the same dataset
  alongside sled and redb. Results at the default 20 K records,
  64-byte values, on a Windows 11 NVMe disk:

  | engine        | inserts (Kelem/s) | reads (Melem/s) |
  |---------------|------------------:|----------------:|
  | emdb v0.6     |               ~50 |            3.02 |
  | **emdb v0.7** |              ~349 |            3.05 |
  | sled          |              ~307 |            2.30 |
  | redb          |              ~589 |            4.28 |

  v0.7 is roughly **7×** faster than v0.6 on bulk inserts and
  comfortably beats sled on both axes; redb is still ahead and is
  the next perf target (page-coalescing, bloom-size tuning, and
  pre-warmed page cache are the obvious levers).
- **Tests.** New `tests/v4_public_api.rs` adds 6 integration
  tests that exercise the v4 path through the public `Emdb` API:
  round-trip insert/get/remove, replay-after-drop, iter+keys,
  clear, transaction-rejection, and TTL round-trip (cfg-gated).
  Every test cleans up `.v4.wal`, `.lock`, `.v3bak`, and
  `.v4tmp` siblings. Total: **232 lib tests + 58 integration
  tests + 7 doc tests = 297 passing across the full feature
  matrix** (`""`, `ttl`, `nested`, `mmap`, `compress`,
  `encrypt`, `bench-compare`, and combinations).

### Phase H continuation (III) — v0.7 transaction port

`Emdb::transaction(|tx| { ... })` is now a first-class operation on the v4
path. The previous `InvalidConfig` regression (returned for any tx call
when `prefer_v4(true)` was set) is gone — a v4-aware commit path runs
under the engine's own commit lock and persists batches with the same
crash-atomic guarantees as v0.6.

- **WAL batch markers.** Two new tag bytes in the v4 WAL:
  `WAL_TAG_BATCH_BEGIN = 2` (payload: `tx_id: u64, op_count: u32`) and
  `WAL_TAG_BATCH_END = 3` (payload: `tx_id: u64`). On replay, ops between
  a `BatchBegin` and its matching `BatchEnd` are buffered and applied
  atomically; a `BatchBegin` with no matching `BatchEnd` (writer crashed
  mid-batch) discards every buffered op. `tx_id` mismatch between begin
  and end surfaces as `Error::Corrupted` rather than silent data loss.
- **`Engine::commit_batch`.** New `BatchedOp` enum (Insert/Remove with
  owned bytes); `commit_batch(&[BatchedOp])` takes the engine's commit
  lock, appends `BatchBegin → ops → BatchEnd` to the WAL via separate
  `append` calls (each gets its own seq), `wait_for_seq`s the
  `BatchEnd` to make the entire batch durable, then runs the apply
  phase via the same `apply_insert`/`apply_remove` helpers single-op
  writes use. The WAL→apply order under one lock is what gives the
  `last_persisted_wal_seq` floor invariant for batches.
- **Engine commit lock.** New `Engine::commit_lock: Mutex<()>`
  serialises every mutation that touches WAL+state together: single-op
  `insert`/`remove`, transactional `commit_batch`, and `flush`. Reads
  do not take it. Holding it across `flush` is what guarantees flush
  can never split a batch when sampling
  `last_persisted_wal_seq = wal.next_seq()`.
- **Apply-phase refactor.** `Engine::insert`/`remove` now delegate
  their in-memory state mutations (page COW, keymap publish, bloom,
  value cache, record-count) to `apply_insert`/`apply_remove`
  helpers. `commit_batch` reuses those exact helpers, so single-op
  and multi-op writes share the entire apply path bit-for-bit.
- **Replay refactor.** `replay_wal_after` is now batch-aware: a
  decoded `BatchBegin` opens a `PendingBatch` accumulator, every op
  decoded while a batch is open buffers (no immediate apply), a
  matching `BatchEnd` flushes the buffer to the apply path, and a
  trailing in-progress batch at end-of-stream is dropped. Nested
  `BatchBegin`s and orphaned `BatchEnd`s surface as `Error::Corrupted`.
- **`Transaction` v0.7 wiring.** `Transaction::commit` now branches on
  `db.v7.is_some()`: the v4 path translates every staged `Op` to a
  `BatchedOp` for the default namespace and hands the batch to
  `engine.commit_batch`. `Transaction::get` falls through to the v4
  engine on cache miss so read-your-writes remains correct. `Op::Clear`
  / `Op::Checkpoint` / batch markers as staged ops are explicitly
  rejected on the v4 path with `TransactionAborted` — none of them are
  produced today, but rejecting future-incompatible ops surfaces bugs
  instead of dropping data.
- **Tests added.** Engine: `commit_batch_applies_every_op_atomically`,
  `partial_batch_in_wal_is_discarded_on_replay` (writes a real
  half-batch directly to the WAL file via append-mode `OpenOptions`,
  reopens, asserts the orphan record is gone and the prior committed
  batch survives), `empty_commit_batch_is_a_noop`. Public API:
  `transaction_commit_applies_overlay_via_v4`,
  `transaction_rollback_discards_overlay_via_v4`,
  `transaction_read_your_writes_via_v4`,
  `transaction_survives_drop_and_reopen_via_v4`,
  `empty_transaction_is_a_noop_via_v4`. The previous
  `transactions_on_v4_path_return_invalid_config` is removed; that
  contract no longer holds. Total: **236 lib + 62 integration + 7 doc
  = 305 tests passing** across the full feature matrix.

### Phase H continuation (IV) — named namespaces, compactor, free-list

The v0.7 engine grows two long-promised primitives in this checkpoint:
multi-namespace runtime support and a real foreground compactor backed by
a working page-store free list. Both ride on top of the catalog primitives
that have been quietly persisted since Phase F.

- **Multi-namespace runtime.** `Engine::default_ns: Arc<NamespaceRuntime>`
  is replaced by `namespaces: RwLock<HashMap<u32, Arc<NamespaceRuntime>>>`.
  On open, every live catalog entry is hydrated into its own runtime
  (keymap, bloom, leaf-chain pointers, record count) and the WAL replay
  walks every namespace's chain. `Engine::namespace(ns_id)` now returns
  a cloned `Arc` after a single read-locked map lookup; the lock is
  released before the call returns so reads do not contend with writers
  more than necessary. `refresh_catalog` walks the whole runtime map and
  pushes each namespace's current state back into the catalog before
  every flush.
- **Engine namespace lifecycle.** New methods on `Engine`:
  `create_or_open_namespace(name) -> (id, was_created)` (idempotent —
  re-opening an existing name is free), `drop_namespace(name)`
  (tombstones in catalog + drops the runtime; data pages are reclaimed
  on the next compact), `list_namespaces() -> Vec<(u32, String)>`, and
  `namespace_id_for(name)`. The default namespace (id 0, empty name) is
  reserved: empty-name creates and drop-default both surface
  `Error::InvalidConfig`. The catalog already supported tombstoning;
  this checkpoint adds `tombstoned_entries` and `remove_tombstoned`
  helpers that the compactor uses.
- **Public `Namespace` handle.** New top-level export
  `emdb::Namespace`, returned by `Emdb::namespace(name)`. The handle is
  cheap to clone (two `Arc` bumps), Send + Sync, and exposes the same
  surface as `Emdb` itself (`insert`, `get`, `remove`, `contains_key`,
  `len`, `is_empty`, `clear`, `iter`, `keys`, `name`). Each named
  namespace has its own keymap, leaf chain, and bloom filter — the
  same key bytes in the default namespace and in `Namespace`s "alpha"
  and "beta" resolve to three independent records, verified by the
  `namespaces_are_isolated_from_default_and_each_other` integration
  test. Plus `Emdb::drop_namespace(name)` and
  `Emdb::list_namespaces() -> Vec<String>` round out the lifecycle.
  Named namespaces are a v0.7-only feature: calling these on a v0.6
  handle returns `Error::InvalidConfig`.
- **Page-store free list.** `PageStore::free_page(page_id)` writes a
  `PageType::FreeList` page whose first 8 body bytes carry the
  previous free-list head, then atomically swaps
  `header.free_list_head` to the freed page id. `allocate_page` first
  attempts a CAS pop from the free-list head (cache-invalidates the
  freed-page bytes so a stale read does not surface as a slotted
  leaf), only falling back to extending the file when the free list
  is empty or the CAS lost. The integration test
  `compact_recovers_space_via_free_list_reuse` writes 200 records,
  removes them, compacts, then writes 50 fresh records and asserts
  the file does not grow — proof that freed page ids are actually
  reused.
- **Phase I compactor.** New `Engine::compact() -> CompactStats` runs
  under the engine's commit lock so concurrent inserts/transactions
  block for the duration; reads continue to be served. Two phases:
  1. **Live namespaces.** Walk every loaded chain. A leaf with
     tombstoned slots is rewritten in place via
     `slotted::compact_leaf` (already-existed primitive) and the
     keymap is fixed up using the `(key, old_slot, new_slot)` remap
     it returns. A leaf with zero live records is unlinked from the
     chain (head pointer or previous leaf's `next_leaf` updated)
     and pushed onto the free list.
  2. **Dropped namespaces.** Walk every catalog entry tombstoned by
     `drop_namespace`. Free every page in the chain, then call
     `Catalog::remove_tombstoned` so the entry stops appearing in
     listings.
  The compactor finishes by calling `flush()` — any reclaimed space
  survives a crash. Public hook: `Emdb::compact()` on a v7 handle
  now drives the engine compactor instead of the previous "alias for
  flush" placeholder.
- **`live_count_of` slotted helper.** Mirror of
  `LeafPage::live_count` for use sites that only have `&Page` (the
  compactor walks chains without mutating). Exists alongside the
  existing `slot_count_of` and `free_space_of` free functions.
- **Encryption deferred (feature gate kept).** AES-256-GCM page-body
  encryption needs a focused session: format decisions
  (page-body vs. WAL vs. value-only), key-management story
  (in-memory only, verification block, rotation), and migration from
  existing v0.7 files. The `encrypt` Cargo feature flag remains
  reserved so downstream `Cargo.toml` entries can opt in early
  without breaking when the implementation lands.
- **Tests added.** Engine: 4 namespace lifecycle + compaction + 5 in
  the existing integration suites. New integration files:
  `tests/v4_namespaces.rs` (12 tests: round-trip, isolation,
  idempotence, empty-name rejection, listing, drop + recreate, drop
  default rejected, drop unknown returns false, persist + reopen,
  iter/keys, clear, v0.6-handle rejection) and
  `tests/v4_compact.rs` (5 tests: tombstone reclamation, reopen
  preserves visible records, dropped-namespace page reclamation,
  free-list reuse, no-op when nothing to compact). Total:
  **236 lib + 79 integration + 7 doc = 322 tests passing** across
  the full feature matrix (`""`, `ttl`, `nested`, `mmap`,
  `compress`, `encrypt`, `bench-compare`, and combinations).

### Status

Component module count: 14 new modules across `src/`, `src/storage/`,
and `src/storage/v4/`. The v0.6 engine remains the default public
code path behind `Emdb`; the v4 engine is now reachable both
directly via `storage::v4::engine::Engine` and through the public
`Emdb` handle by opting in with `EmdbBuilder::prefer_v4(true)`,
including transactional commits, crash-atomic batch replay,
multi-namespace data isolation, and a working page-reclamation
compactor backed by a real free list. Every feature combination is
clippy-clean, fmt-clean, doc-clean, and test-clean.

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
