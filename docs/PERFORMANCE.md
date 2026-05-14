# emdb Performance Guide

Per-operation cost model + tuning knobs. Companion to
[BENCH.md](BENCH.md) (raw numbers + methodology) and
[ARCHITECTURE.md](ARCHITECTURE.md) (why the costs are what they
are).

The goal of this document is to make it possible to **predict
performance for a workload without running benchmarks** — so
you can know up front whether emdb fits the shape of the
problem.

---

## Contents

- [Hot-path cost model](#hot-path-cost-model)
- [When emdb is fastest](#when-emdb-is-fastest)
- [When emdb is slowest](#when-emdb-is-slowest)
- [Tuning knobs](#tuning-knobs)
- [Workload patterns](#workload-patterns)
- [Profiling emdb](#profiling-emdb)

---

## Hot-path cost model

Approximate per-call costs, measured on a 2024-era consumer
NVMe + 8-core CPU. Real numbers vary with cache state, hardware,
and contention; these are order-of-magnitude pricing.

### Reads

| Operation | Cost | Notes |
|---|---|---|
| `get` (hot key, no decode) | ~30 ns | 1 hash + 1 seqlock read + 1 `Vec::from(&[u8])` alloc |
| `get` (cold key, mmap miss) | 1 page-fault + decode | Kernel fetches from SSD; ~30 µs page-fault, ~1 µs decode |
| `get_zerocopy` | ~15 ns | Same hot path; no `Vec` allocation |
| `contains_key` | ~10 ns | No decode at all — index probe only |
| `len` | ~5 ns | `CachePadded<AtomicUsize>` read |

The `get_zerocopy` 2× win vs `get` is real and reproducible on
small values. For large values (≥ 1 KiB) the alloc cost stops
dominating; the two paths converge.

### Writes

| Operation | Cost | Notes |
|---|---|---|
| `insert` (no flush) | ~500 ns | 1 hash + 1 frame encode + 1 LSN reserve + 1 `pwrite` |
| `insert` (fsync) | adds fsync latency | NVMe: 50–500 µs; spinning disk: 5–50 ms |
| `insert_many` (per record) | ~150 ns | Vectored append amortises overhead |
| `remove` | ~500 ns | Same shape as `insert` (writes tombstone) |
| `flush` (`OnEachFlush`) | one fsync | NVMe: ~50–500 µs |
| `flush` (`Group`, leader) | one fsync, shared by N followers | Same fsync cost, divided by N |

### Range scans (`enable_range_scans = true`)

| Operation | Cost | Notes |
|---|---|---|
| `range(R)` | snapshot SkipMap + N decodes | Snapshot is O(matches), each decode is one mmap read |
| `range_iter(R)` | snapshot SkipMap + lazy decode | Same snapshot; consumer pays decode per `.next()` |
| `range_prefix(p)` | same as `range` | Internally builds `[p, p++)` range |

The SkipMap snapshot is the load-bearing cost. For a 1 M-record
namespace scanning 10 K records, expect ~1–2 ms snapshot +
10 K × ~1 µs decode = ~12 ms.

### Async overhead

| Add-on | Cost |
|---|---|
| `spawn_blocking` dispatch | ~1 µs (warm pool) |
| Owned-`Vec` clone for key + value | proportional to bytes |

So async `get` on a hot key: `30 ns + 1 µs ≈ 1 µs` — the
spawn dominates. Async `get` on a cold-mmap key:
`30 µs + 1 µs ≈ 31 µs` — the page-fault dominates. Match the
async surface to the I/O cost; for tight in-memory hot loops,
use the sync surface via `AsyncEmdb::sync_handle()`.

---

## When emdb is fastest

- **Read-heavy concurrent workloads.** Lock-free reads + shared
  mmap scale linearly with core count until memory bandwidth
  saturates. Comparative bench: 9.94 M reads/sec at 8 threads.
- **Bulk loading.** `insert_many` routes through fsys's
  vectored `append_batch` — one LSN reservation + one `pwrite`
  for the entire batch. Comparative bench: 3.2× faster than
  redb on the 5 M-record bulk load.
- **High-concurrency producer fan-in with group commit.**
  N concurrent `flush()` callers coalesce to one `fdatasync`.
  Comparative bench: 8.06× speedup on 8 threads × 200 writes.
- **Small-value hot-key reads.** `get_zerocopy` skips the `Vec`
  alloc; on tight loops over small values, you get raw mmap
  bandwidth.

---

## When emdb is slowest

- **Single-thread per-record-fsync workloads.** Every `flush`
  pays one full fsync. emdb's per-record fsync latency is
  competitive (1.3× faster than redb, 1.06× faster than sled
  on the comparative bench), but it's still hardware-bound.
  Mitigation: `FlushPolicy::Group` with more concurrent
  producers, or `insert_many` to batch.
- **Large-value workloads (≥ 1 MiB / record).** emdb is not
  optimised for blob storage. The journal layout favours many
  small records; large records bloat the journal and slow
  recovery. Consider a separate object store with emdb for
  the metadata index.
- **Range scans without opting in.** `EmdbBuilder::range` is
  `Error::InvalidConfig` if `enable_range_scans(true)` was not
  set at open time. The primary index is hash-only.
- **Cold-mmap random reads.** First touch of a page faults to
  disk. emdb has no application-level cache — the kernel page
  cache is the cache. On a working set larger than RAM, expect
  random-read latency to look like SSD random-read latency.

---

## Tuning knobs

Default settings are tuned for storage-engine workloads. The
opt-in knobs:

### `FlushPolicy::OnEachFlush` (default)

```rust,ignore
let db = Emdb::builder()
    .flush_policy(FlushPolicy::OnEachFlush)
    .build()?;
```

The default. `db.flush()` calls go through fsys's group-commit
coordinator automatically — when N threads call `flush()`
concurrently, they share one `fdatasync` (or platform
equivalent). No tuning knobs; the coordinator runs on an
"immediate-coalesce around an in-flight syscall" shape and
adapts to load.

### `FlushPolicy::Group`

```rust,ignore
let db = Emdb::builder()
    .flush_policy(FlushPolicy::Group)
    .build()?;
```

**Functionally identical to `OnEachFlush` in 0.9.x.** Both
share fsys's coalescer; the separate variant is kept for
source compatibility with v0.8.x callers who wrote
`FlushPolicy::Group { max_batch, wait_ns }`. The previous
tuning knobs are gone because fsys's coordinator runs without
them.

Use `OnEachFlush` in new code.

### `FlushPolicy::WriteThrough`

```rust,ignore
let db = Emdb::builder()
    .flush_policy(FlushPolicy::WriteThrough)
    .build()?;
```

Every `append` goes through with the OS's write-through flag
set. No explicit `flush` calls needed; durability is per-append.
On Windows this maps to `FILE_FLAG_WRITE_THROUGH`; on Linux to
`O_DSYNC`-equivalent (`RWF_DSYNC` per-write).

Use when: every record must be durable immediately and the
application can't batch. Don't use when: throughput matters
more than per-record durability latency.

### `iouring_sqpoll(idle_ms)` (Linux only)

```rust,ignore
let db = Emdb::builder()
    .iouring_sqpoll(10_000)  // 10 second idle timeout
    .build()?;
```

Linux io_uring kernel-side submission queue polling. The kernel
polls the submission queue without needing the user to call
`io_uring_enter`, so write syscalls become memory writes to
shared rings. The kernel thread idles after `idle_ms` of no
activity; submitting after that costs an extra syscall to wake
the kernel poller, then submission is free again.

Trade-off: SQPOLL is a CPU win on high-throughput workloads
(thousands of submissions/sec) — the kernel thread saturates a
core but eliminates the per-submission syscall overhead. On
low-throughput workloads it's a CPU waste (kernel thread idle
in the polling loop). Default: off.

### `enable_range_scans(true)`

```rust,ignore
let db = Emdb::builder()
    .enable_range_scans(true)
    .build()?;
```

Maintains a parallel `crossbeam_skiplist::SkipMap` per namespace.
Memory cost: one `Vec<u8>` clone of the key per insert plus the
SkipMap node — roughly doubles in-memory index size.

Use when: the workload genuinely needs range / prefix scans.
Don't use when: only point lookups are needed; the memory tax
is not worth it.

### `default_ttl(duration)`

```rust,ignore
let db = Emdb::builder()
    .default_ttl(Duration::from_secs(3600))
    .build()?;
```

Sets the default TTL for records inserted via plain `insert`
(without `insert_with_ttl`). Convenience for caches; the
per-record `insert_with_ttl` always overrides.

### Cipher choice (`encrypt` feature)

```rust,ignore
let db = Emdb::builder()
    .encryption_key([0u8; 32])
    .cipher(Cipher::ChaCha20Poly1305)
    .build()?;
```

AES-256-GCM is the default. Pick ChaCha20-Poly1305 only if the
platform lacks AES hardware acceleration (rare in 2026) or the
threat model prefers a non-AES primitive. ChaCha20 is roughly
2× slower than AES-GCM on modern x86 / ARM64.

---

## Workload patterns

### Cache-like workload

```rust,ignore
let db = Emdb::builder()
    .default_ttl(Duration::from_secs(3600))
    .flush_policy(FlushPolicy::OnEachFlush)
    .build()?;

// Periodic eager expiry
tokio::spawn(async move {
    let mut tick = tokio::time::interval(Duration::from_secs(60));
    loop {
        tick.tick().await;
        let _ = db.sweep_expired();
    }
});
```

The default `FlushPolicy::OnEachFlush` is fine — caches usually
don't need group commit. Eager `sweep_expired` keeps the
working-set memory bounded.

### Append-heavy log

```rust,ignore
let db = Emdb::builder()
    .flush_policy(FlushPolicy::Group { max_batch: 16, wait_ns: 100_000 })
    .build()?;

// Multiple producer threads
for thread_id in 0..16 {
    let db = db.clone();
    std::thread::spawn(move || {
        loop {
            db.insert(timestamp_key(), record_bytes()).unwrap();
            db.flush().unwrap();  // Coalesced by group commit
        }
    });
}
```

Group commit gives 8×+ throughput at 16 concurrent flushers
without sacrificing per-record durability.

### Bulk-load pipeline

```rust,ignore
let db = Emdb::open(path)?;

let batch: Vec<(Vec<u8>, Vec<u8>)> = build_batch();
db.insert_many(batch)?;     // One LSN reserve + one pwrite
db.flush()?;                // One fsync for the whole batch
```

`insert_many` is the right tool for any workload where the
records are available together — bulk import, ETL, replaying
a backup. 3×+ faster than the equivalent insert-in-a-loop on
the comparative bench.

### Read-only / read-heavy

```rust,ignore
let db = Arc::new(Emdb::open(path)?);

for _ in 0..num_cpus::get() {
    let db = Arc::clone(&db);
    std::thread::spawn(move || {
        for key in keys_to_query() {
            let _ = db.get(key);
        }
    });
}
```

No special configuration needed. The lock-free read path scales
to the core count. Use `get_zerocopy` instead of `get` if values
are small (≤ 256 bytes) and the workload is tight enough that
the `Vec` alloc shows up in profiles.

---

## Profiling emdb

emdb is small enough that flamegraphs over a workload usually
show the hot spots directly.

### Bench harness

`cargo bench --bench kv --features ttl` runs the
internal microbenchmarks (insert, get, range, group commit).
Output goes to `target/criterion/` as HTML reports.

For comparative numbers against redb, sled, RocksDB, Redis:
`cargo bench --bench comparative --features ttl,bench-compare`.

### Flamegraph

Linux:
```bash
cargo flamegraph --bench kv --features ttl -- --bench
```

Windows: capture with WPR + WPA, or use
[`perfview`](https://github.com/microsoft/perfview).

### Per-frame cost in CI

The `benches/index_hotpath.rs` bench targets the index alone —
useful for catching regressions in just the hash + slot probe
path without journal I/O noise.

---

## See also

- [BENCH.md](BENCH.md) — full benchmark numbers and methodology.
- [ARCHITECTURE.md](ARCHITECTURE.md) — the engine internals that
  produce these numbers.
- [PLATFORM-NOTES.md](PLATFORM-NOTES.md) — OS-specific
  performance behaviour.
