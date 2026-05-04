# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.8.5](https://github.com/jamesgober/emdb-rs/compare/v0.8.0...v0.8.5) — 2026-05-04

Production-polish beta. Five additive features, all small,
none touching the file format or the existing public surface.
Closes the operational gaps that come up the moment emdb is
deployed: "where do I get a backup?", "how do I tell who has
this lock?", "how do I monitor it?", "can I run with
synchronous-write durability?", "can I paginate iteration?".

### Added — `Emdb::stats()` returning `EmdbStats`

Point-in-time database introspection. Returns a `Copy`,
`#[non_exhaustive]` struct with:

- `live_records` — count across every namespace
- `namespace_count` — number of named namespaces
- `logical_size_bytes` — current writer tail
- `file_size_bytes` — total file size including pre-allocated
  padding
- `preallocated_bytes` — `file_size_bytes - logical_size_bytes`
- `range_scans_enabled` — whether the BTreeMap secondary index
  is live
- `encrypted` — whether the file is encrypted at rest

O(namespaces) plus one filesystem `metadata` call. Cheap enough
for per-second health-check polling. Documented as safe to call
inline from async contexts (no blocking cost).

### Added — `Emdb::backup_to(path)` atomic snapshot

Writes a self-contained, openable database file at `target`.
Same atomic-rename pattern the compactor uses: writes to
`<target>.backup.tmp`, `fdatasync`s, renames into place.

The result is a normal emdb file — open it with `Emdb::open` and
every record is there. No proprietary dump format, no archive,
no shell script wrappers. Refuses to back up to the live
database's own path. Overwrites an existing target by default
(callers wanting timestamped snapshots embed the timestamp in
the path).

Documented as a heavy operation worth wrapping in
`tokio::task::spawn_blocking` from async contexts; the work
scales with database size.

### Added — `Emdb::lock_holder` / `Emdb::break_lock` admin

Two static methods for diagnosing and recovering from stuck
lockfiles.

- `Emdb::lock_holder(path)` returns the metadata of whoever
  holds the advisory lock at `<path>.lock` — PID, acquired-at
  timestamp, and crate version. Returns `Ok(None)` when the
  database is unlocked.
- `Emdb::break_lock(path)` deletes the `.lock` and `.lock-meta`
  sidecars so a fresh `open` can succeed. Caller is documented
  as responsible for confirming the holder is dead before
  calling — the doc comment names specific OS tooling
  (`ps -p`, `Get-Process`) that production operators use.

The lockfile design changed to support this. The OS advisory
lock is now held on `<path>.lock` whose body stays empty;
holder metadata lives in a sibling `<path>.lock-meta` file that
is free to read regardless of whether the lock is held. The
split exists because Windows uses mandatory file locks — a
holder's `LockFileEx` blocks other handles' reads on the same
range. Putting metadata in a second file makes "show me who has
the lock" a portable, deadlock-free read.

The new `LockHolder` type is `pub` and `#[non_exhaustive]`.

### Added — `FlushPolicy::WriteThrough`

Third variant of `FlushPolicy`. Opens the file with
`FILE_FLAG_WRITE_THROUGH` (Windows) / `O_SYNC` (Linux, macOS,
BSDs) so every `pwrite` is durable on return; `flush()` becomes
a near-free `sync_data` belt-and-braces call.

For single-thread per-record-durability workloads the trade-off
is favourable on Windows: the bench's `individual writes` phase
calls `db.flush()` per record, which under `OnEachFlush` pays
one `FlushFileBuffers` per call (~27 ms on consumer NVMe).
`WriteThrough` lets the OS commit synchronously inside `pwrite`
instead, with cheaper per-record latency. The cost shifts: bulk
loads under `WriteThrough` are slower because every `pwrite`
waits for disk; the bulk-load path no longer benefits from the
OS write-back cache.

Cross-platform implementation:

- Windows: `FILE_FLAG_WRITE_THROUGH = 0x8000_0000` via
  `OpenOptionsExt::custom_flags`. Hardcoded constant — no
  `windows-sys` dependency for one bit.
- Linux: `O_SYNC = 0x101000`.
- macOS / BSDs: `O_SYNC = 0x80`.
- Other Unix targets we have not certified: flag value `0`.
  `WriteThrough` semantics degrade to `OnEachFlush` on those
  targets (correct, just no perf win — documented in the
  variant's doc comment).

The flag is propagated across `Store::swap_underlying`, so a
database opened with `WriteThrough` keeps the synchronous-write
semantics through a compaction.

### Added — `iter_from(start)` / `iter_after(start)` cursor iteration

Streaming iterators over keys at-or-after / strictly-after a
given start key, in lexicographic order. Mirrors on
`Emdb` and `Namespace`. Built on the existing `range_iter`
machinery — same lazy snapshot semantics, same one-decode-per-
`next()` cost model. Requires `enable_range_scans(true)` at
open time.

The motivating workload is paginated APIs: pass the last-seen
key as the cursor and resume iteration from the next record.
The integration tests include a complete pagination example
that walks 50 records 10 at a time.

### Lockfile body format

`<path>.lock-meta` body schema v1:

```text
emdb-lock v1
pid=<u32>
acquired_at=<unix-millis>
crate_version=<semver>
```

Lines past the version header are `key=value` pairs; unknown
keys are ignored on read for forward-compat. Documented in the
`lockfile` module docs.

### Tests

22 new integration tests in `tests/v0_8_5_features.rs` covering
every new feature. Each suite exercises happy path plus edge
cases: empty inputs, missing files, idempotency, self-target
rejection, cross-platform behaviour, concurrent writers,
snapshot-iterator semantics under contention, pagination
round-trips. Plus 6 new unit tests in `src/lockfile.rs`
covering the body parser and break_lock idempotency.

Test totals: **167 tests across all CI feature combinations**
(up from 145 in 0.8.0). Every combo (no-default-features, ttl,
nested, encrypt, ttl+nested, ttl+nested+encrypt) green.

### Notes

- No file format change. v0.7.x and v0.8.0 databases open
  unchanged in v0.8.5.
- No public-API breakage. All five additions are additive;
  `FlushPolicy` is `#[non_exhaustive]` so the new variant
  doesn't break exhaustive matches in caller code.
- One observable lockfile change: a new sibling file
  `<path>.lock-meta` appears alongside `<path>.lock` while a
  database is held. Cleanup tooling that scans for `*.lock`
  should also handle `*.lock-meta`. Both are removed on
  graceful drop.

## [0.8.0](https://github.com/jamesgober/emdb-rs/compare/v0.7.1...v0.8.0) — 2026-05-03

The release that closes the per-record-durability gap and turns the
mmap architecture into a public API. Three big additions, two
quality-of-implementation reworks, and a deeper test bench. Nothing
breaks the v0.7 file format.

### Added — `FlushPolicy::Group`, the group-commit pipeline

The headline feature. Many concurrent `db.flush()` calls now share
a single `fdatasync`. The protocol is a leader-follower scheme
([`crate::storage::flush`]):

1. Each flusher snapshots the writer's current tail offset.
2. If a recent leader's sync already covered that offset, the
   flusher returns immediately with no work.
3. Otherwise the flusher takes the coordinator lock. The first
   one in becomes the leader and waits up to `max_wait` (or until
   `max_batch` flushers have joined) before issuing the sync.
4. Followers park on a condvar until the leader's sync covers
   their snapshot.

For a workload of N independent producer threads each writing one
record then calling `flush` for per-record durability, this turns
`N × fsync_cost` into roughly `(N / max_batch) × fsync_cost`. The
new `benches/group_commit.rs` baseline measures **7.40× aggregate
write throughput** under `Group { max_wait: 500 µs, max_batch: 8 }`
vs. `OnEachFlush` on a 4-core consumer box with 8 producer threads
(see the README's bench section). Single-threaded flush is
unchanged from v0.7.x: with no follower to wait for, the leader's
`max_wait` window terminates as soon as no follower arrives.

Two implementation details that landed alongside `Group` so it
actually wins:

1. **Sync handle decoupled from the writer mutex.** The store now
   holds a `try_clone`'d sibling `File` exclusively for `sync_data`
   calls. This means an in-flight fsync no longer blocks concurrent
   `pwrite` from other threads — without this, every `flush()`
   already serialised through the writer mutex, leaving group
   commit nothing to fuse. The clone is refreshed on file rename
   (in `swap_underlying`); on file growth it stays valid because
   `set_len` does not invalidate cloned descriptors.
2. **Followers wake the leader on arrival.** The coordinator now
   `cv.notify_all()` after incrementing `pending`, so the leader
   immediately re-checks the `pending >= max_batch` exit condition
   instead of sleeping the full `max_wait` regardless of follower
   arrival.

`max_batch` should be set close to the expected concurrent flusher
count (typically `num_cpus::get()`). Setting it higher than the
real concurrency turns the leader's `max_wait` into pure tail
latency — the documentation on `FlushPolicy::Group` calls this out
explicitly.

Public surface:

- `EmdbBuilder::flush_policy(policy)` selects the policy at open
  time. Default is `OnEachFlush`, preserving v0.7.x semantics —
  upgrades are a non-event for callers that don't opt in.
- `FlushPolicy` is `#[non_exhaustive]` so we can add variants
  later (a hypothetical `Group::Adaptive` that adjusts
  `max_wait` based on observed batch sizes is the obvious one).

5 new integration tests in `tests/flush_policy.rs` cover: the
default-policy regression guard, single-thread no-deadlock
behaviour, 8 × 25 concurrent flushers all returning Ok with every
record durable after reopen, the `max_batch = 1` floor (every
flusher leads its own cycle, no underflow), and the builder
round-trip across every variant.

### Added — Streaming `iter` / `keys` / `range`

The v0.7.x iterators called `engine.collect_records()`, which
materialised every record's `(key, value, expires_at)` tuple as
an owned `Vec<u8>` triple before yielding the first item. For a
million-record database that meant a million heap allocations
plus the data itself sitting in transient memory.

v0.8 keeps the public iterator types ([`crate::EmdbIter`],
[`crate::EmdbKeyIter`], [`crate::NamespaceIter`],
[`crate::NamespaceKeyIter`]) but their internals are different:
they now hold a sorted snapshot of record offsets plus an
`Arc<Inner>`, and decode one record per `next()` call. Memory
use scales with offset count, not value size. Records inserted
after iterator construction are not visible (snapshot
semantics); records overwritten or removed since the snapshot
are skipped on decode.

For range queries, two new APIs deliver the same lazy semantics
to BTreeMap-backed scans:

- [`crate::Emdb::range_iter`] / [`crate::Emdb::range_prefix_iter`]
- [`crate::Namespace::range_iter`] / [`crate::Namespace::range_prefix_iter`]

These return [`crate::EmdbRangeIter`] /
[`crate::NamespaceRangeIter`]. The (key, offset) pairs are
snapshotted from the BTreeMap under one read-lock acquisition;
the lock is released immediately, and values are decoded on
demand as the caller pulls items. A consumer that calls
`.range_iter(...).take(10)` decodes 10 values, not the entire
range. The eager `range()` / `range_prefix()` are unchanged for
callers that want the full materialised result.

10 new integration tests in `tests/zerocopy.rs` cover the lazy
iterator paths alongside the zero-copy read tests below
(snapshot semantics, early-exit, `range_prefix_iter` parity
with `range_prefix`, the no-`enable_range_scans` guard).

### Added — `get_zerocopy` and the `ValueRef` type

The mmap-backed read path's whole point is that record bytes
already live in kernel-managed memory; allocating a `Vec<u8>` to
return them throws that away. v0.8 adds a parallel API:

- [`crate::Emdb::get_zerocopy`] / [`crate::Namespace::get_zerocopy`]
  return `Option<ValueRef>`.
- [`crate::ValueRef`] is either an `Arc<Mmap>` + byte range (the
  fast path) or an owned `Vec<u8>` (the encrypted-database
  fallback — AEAD decryption necessarily allocates).

Either way, `ValueRef` implements `Deref<Target = [u8]>`,
`AsRef<[u8]>`, and the obvious equality traits, so callers can
pass it where a byte slice is expected.

The mmap-backed variant holds a strong handle to the original
mapping. If the writer grows the file and swaps in a new
mapping, the old one stays alive until the last
`ValueRef` derived from it drops — there is no "reference
invalidation" hazard for in-flight readers.

10 integration tests cover: round-trip equivalence with `get`,
empty / missing inputs, survival across writer-driven mmap
swap, TTL-expired filtering, and the encrypted-database
fallback.

### Added — Crash-recovery integration tests (`tests/crash_recovery.rs`)

A real crash test requires `TerminateProcess` (Windows) or
`SIGKILL` (Unix). What we *can* test deterministically is the
recovery scan's behaviour on the file states a real crash leaves
behind: torn final record, bit-flipped CRC, garbage length
prefix, stale `tail_hint`. 5 new tests:

- Truncated final record is discarded; preceding records survive.
- A flipped CRC byte stops the scan at that record.
- A garbage length prefix (pointing past EOF) is treated as the
  truncation point.
- A stale `tail_hint` does not corrupt recovery — header CRC
  catches the inconsistency cleanly.
- Records survive the checkpoint + reopen path that crash
  recovery exercises.

A real fuzz harness via `cargo-fuzz` is queued for v1.0 (the
nightly-toolchain dependency makes it CI-cost-significant); these
deterministic tests are the stable-toolchain proxy.

### Added — Randomized decoder robustness (`tests/decoder_robustness.rs`)

64 iterations of "valid header + N random bytes" feed
[`crate::Emdb::open`]'s recovery scan and confirm: no panics, no
infinite loops (5 s wall-clock ceiling per iteration), no
out-of-bounds reads. Plus three deterministic shape tests:
empty data region opens to zero records, valid prefix followed
by random tail recovers exactly the prefix, and key/value sizes
from 0 bytes to 64 KiB round-trip cleanly.

### Changed — Iterator types reworked, public surface preserved

`EmdbIter` / `EmdbKeyIter` / `NamespaceIter` / `NamespaceKeyIter`
keep their public type identities and `Iterator` impls. Their
internal representation moved from `IntoIter<RecordSnapshot>` to
`(Arc<Inner>, IntoIter<u64>)` — reflects the shift to lazy
decode. No SemVer impact for callers using them via the
`Iterator` trait.

### Added — `benches/group_commit.rs`

A multi-thread per-record-flush benchmark that exercises the new
group-commit pipeline directly. Runs the same workload twice —
once under `OnEachFlush` and once under `Group { max_wait: 500 µs,
max_batch: 32 }` — and prints aggregate writes/sec plus a speedup
column. Tunable via env vars (`EMDB_BENCH_GC_THREADS`,
`EMDB_BENCH_GC_PER_THREAD`, `EMDB_BENCH_GC_MAX_WAIT_US`,
`EMDB_BENCH_GC_MAX_BATCH`) so callers can match their own deployment
profile.

### Security — `zeroize` on key material

`zeroize` is added as a direct optional dependency gated on the
`encrypt` feature (it was already present transitively via
`aes-gcm` / `argon2`; the explicit declaration just enforces the
relationship). A new `crate::encryption::KeyBytes` type alias
wraps `[u8; 32]` in `zeroize::Zeroizing`, and every internal
storage location for raw key material now uses it:

- `EmdbBuilder::encryption_key` field — was `Option<[u8; 32]>`,
  now `Option<KeyBytes>`. Public method signature still takes
  `[u8; 32]` by value; the bytes are wrapped immediately on
  entry.
- `EngineConfig::encryption_key` — same change.
- `derive_key_from_passphrase` return type — was `[u8; 32]`,
  now `KeyBytes`. The Argon2id `hash_password_into` call writes
  through the wrapper's mutable reference, so the derived key
  never exists on the heap outside the `Zeroizing` envelope.

The cipher state inside `EncryptionContext` already zeroized on
drop via RustCrypto's transitive `zeroize` integration. Together
these mean no copy of the raw key material remains in heap memory
once the `EmdbBuilder` and any derived `EngineConfig` /
`EncryptionContext` drop. Closes the REPS Security
"MUST use the `zeroize` crate" item.

The public `EncryptionInput::Key([u8; 32])` enum variant is
intentionally unchanged — switching its payload would be a
SemVer break without meaningful benefit (the bytes spend
microseconds on the stack between `EncryptionInput::Key`
construction and the builder wrapping them in `Zeroizing`).

### CI

- New `audit` job runs `cargo audit` against the RustSec advisory
  database and fails on warnings. Catches CVEs in transitive deps
  before they reach a release.
- New `deny` job runs `cargo deny check` against the policy in
  the new `deny.toml`. Enforces license, banned-crate, duplicate-
  version, and source-registry rules. Both `audit` and `deny` are
  required by REPS for production-track crates.
- `clippy::undocumented_unsafe_blocks` added to the crate-root lint
  set. Every `unsafe { ... }` block now carries a `// SAFETY:`
  comment that the compiler validates the *presence* of (not the
  content — that's still a human-review job).

### Documentation

- Crate-level docs (`src/lib.rs`) extended with sections on the new
  v0.8 surface: zero-copy reads, streaming iteration, range scans,
  group-commit durability, and the Cargo feature matrix.
  Doctest-validated; previous 6 doctest count is now 10.
- `// Phase 1` / `// Phase 2` comment style replaced throughout the
  storage and encryption-admin code paths with descriptive comments
  per REPS documentation guidance. Behaviour unchanged.

### Notes

- No file format change. v0.7.x databases open unchanged.
- No new runtime dependencies. The group-commit coordinator uses
  std `Mutex` + `Condvar`; no new crate, no MSRV impact.
- `fs4` stays at 0.8 for now; bumping forced an MSRV bump and
  none of the 0.13 features are needed yet. Revisit when an
  advisory or a needed feature appears.
- A `cargo-fuzz` target for the format decoder is queued for
  v1.0. The randomized + crafted tests added here are the
  stable-toolchain coverage in the meantime.

## [0.7.2](https://github.com/jamesgober/emdb-rs/compare/v0.7.1...v0.7.2) — 2026-05-03

A polish release ahead of v0.8's architectural work. Nothing here
breaks the v0.7 file format or the public API; everything is
additive or a documentation correction.

### Added

- **`Emdb::checkpoint()`** — explicit fast-reopen checkpoint. The
  `flush()` path was deliberately stripped of header rewriting in
  v0.7.1 because writing the 4 KB header on every per-record
  `flush()` dominates Windows `FlushFileBuffers` cost and dragged
  the per-record-durability column hard. The trade-off is that
  reopens after long writer sessions need to walk the data region
  from the last persisted `tail_hint` up to the actual tail. For
  large databases that can be slow.

  `checkpoint()` rewrites the header with the current tail and
  `fdatasync`s, so the next [`Emdb::open`] starts its recovery
  scan past the bulk of the log. Call it at quiescent points
  (after a bulk load, before a long idle period, on graceful
  shutdown). The drop of the last handle still attempts a
  checkpoint as a backstop, but `Drop` cannot return errors —
  long-lived processes that care about reopen latency should call
  `checkpoint()` explicitly so they can surface I/O errors to the
  caller.

  Backed by [`Store::persist_header`] under the hood; the new
  public API is the contract surface.

- **`tests/checkpoint.rs`** — five integration tests covering: a
  fresh database, post-flush checkpoints, repeated checkpoints
  with no intervening writes, the drop-time fallback path, and
  the round-trip-after-checkpoint correctness check (every
  inserted record is still readable after reopen).

### Changed

- **README — honest framing of the two columns where the
  comparison reads as a defect.** Both rows previously read as
  weaknesses; both deserve more nuance.

  - `individual writes (fsync/op)` reads "see note 1" instead of
    "37× slower". The note explains that the column is bounded by
    one `FlushFileBuffers` per record on Windows (≈27 ms / call,
    irrespective of dirty-page count) — that is an OS floor, not
    an emdb design defect — and references the v0.8 group-commit
    pipeline that closes the gap. Workloads that need per-record
    durability already have two paths that win in the aggregate
    columns: `db.transaction(|tx| ...)` (one fsync per
    transaction) and `db.insert_many(...)` (one fsync per batch).
  - `random range reads` reads "opt-in" instead of "N/A / feature
    gap". emdb does support range scans — they require
    [`EmdbBuilder::enable_range_scans(true)`] at open time
    (documented in the README's [Range scans](#range-scans)
    section). The phase reads as N/A only because the
    `lmdb_style` bench runs in hash-only mode by default. A fair
    head-to-head bench requires the streaming range API arriving
    in v0.8 (today's `range()` materialises the full result set,
    so the redb pattern of "range from key, take 10" would walk
    every match before returning).

  Both notes link forward to the v0.8 work that closes each gap.

- **README Status block** updated to v0.7.2; added a paragraph on
  the v0.8 / v1.0 roadmap so consumers can see what's queued.
- **README Persistence section** documents the new
  `checkpoint()` call alongside `flush()` so the durability /
  fast-reopen split is discoverable from the front page rather
  than only from the API docs.

### CI

- **MSRV job pinned to 1.75.** The `rust-version` in `Cargo.toml`
  has claimed 1.75 since v0.5; CI never enforced it. New job
  builds with both default and `ttl,nested,encrypt` feature sets
  on the pinned toolchain. Future MSRV bumps are now a deliberate
  CI change, not an accident.
- Build/test step renamed to clarify it covers unit + integration
  + doctests in one invocation.

### Notes

- No file format change. v0.7.1 databases open unchanged in
  v0.7.2.
- No new dependencies.
- No public API removed; `checkpoint()` is a pure addition.

## [0.7.1](https://github.com/jamesgober/emdb-rs/compare/v0.7.0...v0.7.1) — 2026-04-25

### Major change — storage engine rewritten as Bitcask-style mmap + append-only log

The slotted-leaf-page + WAL + page-cache + value-cache + bloom-filter
backend from v0.6 has been entirely replaced with a single
mmap-backed append-only file plus a sharded in-memory hash index.
This is the same shape used by Bitcask / HaloDB / Riak; the read path
is also the shape LMDB and redb use. The on-disk format resets to
**v1** of the new layout — v0.6 / v0.7-page databases must be exported
and reimported (the [`crate::Emdb::enable_encryption`] / `disable` /
`rotate` admin tools follow the same rewrite-then-rename shape and
can serve as a reference).

**Architecture.** One file per database. Bytes 0..4096 are the header
(magic, version, flags, encryption salt + verify block, header CRC).
Records are length-prefix + tag + body + CRC32, appended at the
tail. Reads slice directly from a kernel-managed `Arc<Mmap>` (zero
copy). Writes go through a single writer mutex and use `pwrite`
(Unix) / `seek + write_all` (Windows). File growth swaps the mmap
under an `Arc` so old readers continue with the old mapping until
they release. Crash recovery scans framed records from
`header.tail_hint`, validates each CRC, and treats the first failure
as the truncation point.

**Index.** 64-shard `RwLock<HashMap<u64, Slot>>` keyed by FxHash with
an identity-hashing inner hasher (no double-hashing on lookup).
`Slot::Single(u64)` for the common case (one offset per hash);
`Slot::Multi(Vec<(Vec<u8>, u64)>)` for hash collisions, disambiguated
by exact key compare. Disambiguation on insert uses a callback into
the engine so the hot path never allocates the key bytes for
non-colliding entries.

### Added — Range scans (opt-in BTreeMap secondary index)

The hash index doesn't support sorted iteration, so range / prefix
queries are now an opt-in feature.
`EmdbBuilder::enable_range_scans(true)` activates a parallel
`RwLock<BTreeMap<Vec<u8>, u64>>` secondary index per namespace.
Insert / replace / remove paths update both indexes; the recovery
scan rebuilds the BTreeMap from records on reopen, and compaction
preserves it through the atomic-swap rewrite.

Public surface:

- `Emdb::range(range)` and `Emdb::range_prefix(prefix)` on the default
  namespace. `range` accepts any `RangeBounds<Vec<u8>>`.
- `Namespace::range(range)` and `Namespace::range_prefix(prefix)` for
  named namespaces.
- Calling `range(...)` without `enable_range_scans(true)` at open time
  surfaces as `Error::InvalidConfig` rather than returning empty
  results.

Cost: one `Vec<u8>` clone of the key per insert plus the BTreeMap node
overhead (~40 bytes per entry on a 64-bit target). Roughly doubles
in-memory index size for typical workloads. The hash index hot path is
unchanged — users who don't enable range scans pay nothing.

7 new integration tests in `tests/range_scans.rs` cover: opt-out
default, sorted ordering, prefix helper edge cases, mutation
semantics, reopen, named namespaces, and survival through compaction.

### Added — Persistent namespace name → ID bindings (`TAG_NAMESPACE_NAME`)

Previously the `name → id` map was rebuilt on every reopen by
allocating IDs in record-encounter order. That accidentally worked
when names were created in the same order each session, but was a
real correctness bug (a different creation order on reopen would
hand back a different id than before, decoupling records from their
namespace handle).

Fixed: every named-namespace creation now appends a
`TAG_NAMESPACE_NAME` record (id 2 in the format) carrying
`(ns_id, name)`. The recovery scan replays these records before
applying inserts/removes, so reopens find the same `name → id`
mapping the writer used. Compaction re-emits the bindings in the
rewritten file. Encryption-aware path encrypts the binding the same
way it encrypts inserts.

5 new integration tests in `tests/namespaces.rs`: round-trip across
reopen, ID stability across reopens (the test that exposed the
original bug), survival through compaction, no-records-just-name
edge case, and encrypted-database variant.

### Added — `lmdb_style` apples-to-apples bench (vs `redb-bench/lmdb_benchmark.rs`)

Mirrors redb's published methodology: 5 M records, 24-byte random
keys, 150-byte random values, fastrand-seeded. Full phase set —
bulk load, individual writes, batch writes, nosync writes, len(),
random reads (1 M × 2), MT reads at 4 / 8 threads, removals,
uncompacted size, compaction, compacted size. Range reads recorded
as N/A (real feature gap; see range-scans entry above).

Set `EMDB_BENCH_RECORDS=5000000` to hit redb's published scale;
defaults to 1 M for faster local iteration.

#### 5 M-record results vs redb (Windows 11 NVMe, lower is better)

| phase                       |        emdb |    redb  |    sled  |  emdb vs redb     |
|-----------------------------|------------:|---------:|---------:|------------------:|
| bulk load                   |    **4498** |    74496 |    60807 |     16.6× faster  |
| batch writes                |    **2814** |    11043 |     1972 |      3.9× faster  |
| nosync writes               |     **220** |     1717 |     1136 |      7.8× faster  |
| random reads (1 M)          |     **596** |     5289 |    11197 |      8.9× faster  |
| random reads (4 threads)    |    **1083** |    17543 |    34605 |     16.2× faster  |
| random reads (8 threads)    |     **653** |    17160 |    33284 |   **26× faster**  |
| removals                    |   **11948** |    54905 |    46155 |      4.6× faster  |
| compaction                  |   **11490** |    16506 |      N/A |      1.4× faster  |
| uncompacted size            |    1.08 GiB | 4.00 GiB | 2.13 GiB |     3.7× smaller  |
| compacted size              | **498 MiB** | 1.64 GiB |      N/A |     3.4× smaller  |
| individual writes (fsync/op)|       27455 |  **734** |  **316** | 37× **slower**    |
| random range reads          |         N/A |     3958 |     9688 | feature gap       |

emdb wins every aggregate-throughput column — often by an order of
magnitude — and is 3-4× smaller on disk both compacted and not. The
`individual writes` column (each write fsync'd on its own commit) is
the one place emdb loses, dominated by Windows `FlushFileBuffers`
latency. Workloads that need per-record durability should batch
through `db.transaction(...)` or `db.insert_many(...)`, which amortise
the fsync cost.

### Documented — Single-writer model + multi-writer deferred to v1.0

The Concurrency section of the README now states the actual model
explicitly: lock-free reads (sharded hash index + `Arc<Mmap>`),
single-writer writes (one mutex around the encode-then-pwrite step).
This matches LMDB / redb / BoltDB. True multi-writer concurrency
requires either a recovery-model change (skip-bad-CRC, scan-forward)
or per-thread log segments; both have correctness trade-offs that
warrant the v1.0 design pass. Queued.

### Added — Real `Emdb::compact()` (live-record sibling rewrite + atomic swap)

`compact()` was a flush-shaped no-op in the initial rewrite. Now it actually
reclaims space:

1. Snapshot every namespace's live `(key, value, expires_at)` tuples by
   walking the in-memory indexes against the current mmap.
2. Write a fully-formed sealed file at `<path>.compact.tmp` directly via
   buffered `File` I/O (no mmap on the temp file, so Windows is happy
   shrinking the file size after writes).
3. `fdatasync` the temp file, then call [`Store::swap_underlying`] which
   drops our writer's File handle, atomic-renames the temp over the
   canonical path, reopens the writer, and refreshes the mmap.
4. Clear and rebuild every namespace index from the new layout via the
   same `recovery_scan` used at open time.

Existing readers holding `Arc<Mmap>` snapshots from before the compaction
keep reading from the old inode (the kernel pins it for the duration of
any active mapping); new reads see the compacted layout. Three new
integration tests in `tests/compact.rs` cover the size-shrinks path,
the empty-DB no-op, and namespace preservation through a compaction.

### Added — `concurrent_reads` bench (multi-thread read fan-out)

Single-thread `compare_read` undersells the lock-free `Arc<Mmap>` read
path because there's no contention to observe. New
`benches/concurrent_reads.rs` spawns N reader threads against a
pre-populated DB and measures aggregate throughput across thread counts
1, 2, 4, 8.

Numbers on the same Windows 11 NVMe box as the existing benches:

| reader threads | aggregate reads (Melem/s) |
|---------------:|--------------------------:|
| 1              |                      4.75 |
| 2              |                      6.57 |
| 4              |                      9.18 |
| 8              |                     11.97 |

Reads scale through 8 threads on a 4-core machine — the kernel-managed
mmap plus the 64-shard hash index keep the hot path lock-free, so the
only contention past core count is shared memory bandwidth.

### Changed — README rewrite for the new architecture

Dropped the v0.6/v0.7 dual-engine story, the `prefer_v4` opt-in, and
references to the (now removed) `FlushPolicy`, slotted-leaf chains, WAL,
and `BatchBegin`/`BatchEnd` markers. New README leads with the bench
numbers (single-thread + multi-reader), explains the
Bitcask-style architecture in two sentences, and documents the
`db.transaction()` / `db.insert_many()` choice for callers who want
the redb-style transaction-batched insert pattern.

### Added — `Emdb::insert_many` / `Namespace::insert_many` bulk-insert API

The fast path for bulk-loading. All records are framed into one buffer
under a single writer-mutex hold and written via a single `pwrite`
syscall.

- `Emdb::insert_many<I, K, V>(items)` where `I: IntoIterator<Item = (K, V)>`,
  `K: AsRef<[u8]>`, `V: AsRef<[u8]>`. Mirror on `Namespace::insert_many`
  for named namespaces.
- Records inside one `insert_many` call are written atomically *as
  individual records* (each gets its own CRC). They are **not**
  all-or-nothing as a group — a crash mid-batch leaves a CRC-validated
  prefix on disk. For all-or-nothing semantics use
  `db.transaction(|tx| ...)`, which buffers writes in an overlay and
  routes the commit through `insert_many` plus a final `flush` so the
  whole batch is durable together.

### Added — OS-default storage path resolution

`Emdb::builder()` now resolves a platform-appropriate database file path
when the caller opts in via `app_name` / `database_name` / `data_root`.
This closes the embedder-ergonomics gap that previously forced HiveDB and
every other consumer to know each platform's data-directory convention.

- **`src/data_dir.rs`.** Cross-platform resolver: Linux/BSD use
  `$XDG_DATA_HOME` (or `$HOME/.local/share`), macOS uses
  `$HOME/Library/Application Support`, Windows uses `%LOCALAPPDATA%`
  (falling back to `%APPDATA%` then `%USERPROFILE%\AppData\Local`).
  Last-resort fallback is the process current directory so the
  builder never panics.
- **Builder methods.** `app_name(name)` (single folder name, default
  `"emdb"`), `database_name(name)` (default `"emdb-default.emdb"`),
  `data_root(path)` (escape hatch for tests / containers / sandboxes).
  Resolved path is `<data_root>/<app_name>/<database_name>`.
- **Validation.** Path separators (`/`, `\`), `..`, and the empty
  string are rejected at build time so a stray value cannot escape
  the data root and behaviour stays identical on every platform.
- **Conflict detection.** Mixing `path()` with any of the
  OS-resolution methods returns `Error::InvalidConfig` — pass either
  an explicit path or the OS-resolution methods, never both.
- **Tests.** 7 unit + 10 integration covering round-trips through
  v0.6 and v0.7, default substitution, `mkdir -p` behaviour,
  multi-app coexistence under one root, and every rejection branch.

### Added — AES-256-GCM + ChaCha20-Poly1305 at-rest encryption

Opt-in via the `encrypt` Cargo feature plus
[`crate::EmdbBuilder::encryption_key`] (raw 32-byte key) or
[`crate::EmdbBuilder::encryption_passphrase`] (Argon2id KDF). Either
mode encrypts every record body; unencrypted records simply skip the
encryption path so unencrypted databases stay byte-identical to a
non-`encrypt` build.

- **Ciphers.** AES-256-GCM via `aes-gcm` 0.10 (default; AES-NI on
  modern x86, Crypto Extensions on ARMv8) and ChaCha20-Poly1305 via
  `chacha20poly1305` 0.10 (selectable via `EmdbBuilder::cipher(...)`
  for hardware without AES acceleration). Both use a 96-bit random
  nonce drawn fresh from `OsRng` per record. Counter-based nonces
  were rejected: durable counter state can roll back on
  backup-restore, and a rolled-back nonce with the same key is the
  one mistake AEAD ciphers do not survive.
- **Passphrase mode.** `EmdbBuilder::encryption_passphrase("...")`
  derives a 32-byte key via Argon2id (19 MiB memory, 2 iterations,
  1 lane — OWASP defaults for interactive use). The salt is a fresh
  random 16-byte block per database, persisted at header offsets
  40..56. Reopens read the salt and rerun the KDF; wrong passphrase
  surfaces as [`crate::Error::EncryptionKeyMismatch`] before any user
  data is touched. Mutually exclusive with `encryption_key()`.
- **Record envelope.** Every record carries the same outer framing
  (`[len][tag][body][crc]`); the encrypted variant sets bit 7 of the
  tag and the body becomes `[nonce: 12][ciphertext + AEAD tag]`. The
  CRC catches torn writes; the AEAD tag catches tampering. See
  [`crate::storage::format`] for the full layout.
- **Verification block.** Header bytes 56..116 hold an AEAD-encrypted
  copy of a fixed magic plaintext
  (`b"EMDB-ENCRYPT-OK\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"`). On open
  the engine decrypts and compares; mismatch surfaces as
  `Error::EncryptionKeyMismatch` before any user data is read.
- **Cipher selection on disk.** `FLAG_CIPHER_CHACHA20` (bit 1 of the
  header flags) records the cipher choice on creation. Reopens
  auto-dispatch from the on-disk flag; callers do not have to
  restate the cipher.
- **Offline admin APIs.** Three static methods on [`crate::Emdb`]:
  `enable_encryption(path, target)` (unencrypted → encrypted in
  place), `disable_encryption(path, current)` (the reverse), and
  `rotate_encryption_key(path, from, to)` (re-encrypt under a new
  key). All three use atomic rewrite-then-rename: the original is
  preserved at `<path>.encbak` on success and untouched on any
  failure. Each side accepts either a raw key or a passphrase via
  the new `EncryptionInput::{Key, Passphrase}` enum (re-exported
  at the crate root).
- **Error variants.** `Error::Encryption(&'static str)` for
  malformed buffers / AEAD-machinery failures (not user-recoverable);
  `Error::EncryptionKeyMismatch` for tag-validation failures (user
  supplied the wrong key). Both gated on `feature = "encrypt"`.
- **`Debug` does not leak keys.** `EncryptionContext::fmt` writes
  `"<redacted>"` instead of the cipher state.

### Removed

- **`FlushPolicy`.** Sync semantics are simpler now: `insert` writes
  to the OS buffer, `flush()` calls `fdatasync`. Callers that want
  per-record durability call `flush` after each insert; callers that
  want batched durability call `flush` after `insert_many` or at the
  end of a transaction.
- **`EmdbBuilder::prefer_v4(...)` + the v0.6 / v0.7 dual-engine
  dispatch.** There is exactly one engine.
- **`emdb-cli` binary + `cli` Cargo feature.** Not standard for
  embedded KV libraries; the `Emdb::enable_encryption` /
  `disable_encryption` / `rotate_encryption_key` library APIs cover
  the same need programmatically.
- **`compress` Cargo feature + the `lz4_flex` value-compression
  shim.** The new format does not include compressed-record
  framing.
- **The slotted-leaf-page + WAL + page-cache + value-cache + bloom
  filter modules** (`src/storage/v4/`, `src/storage/page/`,
  `src/keymap.rs`, `src/page_cache.rs`, `src/value_cache.rs`,
  `src/bloom.rs`, `src/index.rs`, `src/compress.rs`, the v0.6 v0.7
  migration scaffolding). Tests for the removed surface
  (`tests/v4_*.rs`, `tests/migration.rs`, `tests/page_format.rs`,
  `tests/recovery.rs`, `tests/transactions.rs`,
  `tests/concurrency.rs`) are gone too — their guarantees are
  covered by the new integration tests
  (`tests/persistence.rs`-style + `tests/compact.rs`,
  `tests/range_scans.rs`, `tests/namespaces.rs`).

### Tests + format

109 tests passing across `default`, `ttl,nested,encrypt`,
`--no-default-features`, `nested`-only, and `encrypt`-only feature
combos. Library is clippy-clean under the project's strict lint
profile (deny `unwrap_used`, `expect_used`, `unreachable`, `todo`,
`unimplemented`, `print_stdout`, `print_stderr`, `dbg_macro`,
`warnings`).

The on-disk format resets to **v1** of the new mmap+append layout.
v0.6 page-format files and the original v0.7 dual-engine page-format
files cannot be opened by this release. Migration path: open the
old file with the previous emdb release, export records, reimport
into a fresh v0.7.1 file. (No automated migration tool ships in
0.7.1; the encryption-admin rewrite primitive in
[`crate::encryption_admin`] is the reference shape for an external
exporter.)

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
