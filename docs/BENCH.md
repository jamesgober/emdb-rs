## Benchmarking emdb

This project uses Criterion for repeatable performance measurements.

### What is benchmarked

- Single-engine benchmarks in [benches/kv.rs](../benches/kv.rs), [benches/persistence.rs](../benches/persistence.rs), [benches/transactions.rs](../benches/transactions.rs), and [benches/concurrency.rs](../benches/concurrency.rs).
- Comparative benchmark in [benches/comparative.rs](../benches/comparative.rs):
  - emdb (always)
  - sled and redb (when feature `bench-compare` is enabled)
  - rocksdb (when feature `bench-rocksdb` is enabled)
  - redis (when feature `bench-redis` is enabled and a Redis server is reachable)

### Quick run

```powershell
cargo bench --bench comparative
```

This runs emdb-only comparison mode by default.

### Compare with embedded DBs

```powershell
cargo bench --bench comparative --features bench-compare
```

This adds sled and redb to the same insert/read workload.

### Compare with Redis

Start Redis locally (Docker example):

```powershell
docker run --rm -p 6379:6379 redis:7
```

Then run:

```powershell
$env:EMDB_REDIS_URL = "redis://127.0.0.1/"
cargo bench --bench comparative --features bench-redis
```

If Redis is unreachable, Redis benchmarks are skipped and emdb benchmarks still run.

### Compare with RocksDB

```powershell
cargo bench --bench comparative --features bench-rocksdb
```

To compare all embedded engines (sled + redb + rocksdb):

```powershell
cargo bench --bench comparative --features bench-compare,bench-rocksdb
```

### Tune workload size

The comparative benchmark defaults to 20,000 records.

```powershell
$env:EMDB_BENCH_RECORDS = "50000"
cargo bench --bench comparative --features bench-compare
```

### Reading results

Criterion outputs reports under [target/criterion](../target/criterion).

Key metrics:

- Throughput (elements/sec)
- Time per iteration (mean, p50, p95 where available)
- Relative comparison between benchmark IDs in the same group

### Reproducibility notes

For meaningful comparisons:

- Use release profile via `cargo bench`
- Close heavy background apps
- Keep the same CPU power mode
- Run multiple times and compare medians, not single best values
- Keep identical dataset size and value size when comparing engines

### Suggested reporting format

When logging benchmark output into README or release notes, record:

- CPU and memory
- OS + Rust version
- Command used
- Record count (`EMDB_BENCH_RECORDS`)
- Insert throughput and read throughput for each engine
- Any notable caveats (for example, Redis network overhead)

## Initial baseline (2026-04-24)

Command used:

```powershell
$env:EMDB_BENCH_RECORDS = "5000"
cargo bench --bench comparative --features bench-compare
```

Environment:

- CPU: AMD Ryzen 7 8700F 8-Core Processor
- Rust: rustc 1.93.1 (01f6ddf75 2026-02-11)
- OS: Windows

Results (Criterion ranges):

| Workload | emdb | sled | redb |
| --- | --- | --- | --- |
| Insert 5,000 records | 1.72s - 1.80s (2.78K - 2.90K elem/s) | 15.70ms - 16.73ms (298.81K - 318.44K elem/s) | 15.86ms - 16.42ms (304.51K - 315.27K elem/s) |
| Read 5,000 records | 575us - 724us (6.90M - 8.69M elem/s) | 1.43ms - 1.82ms (2.75M - 3.50M elem/s) | 833us - 870us (5.74M - 5.99M elem/s) |

Interpretation:

- Current emdb read throughput is strong in this workload.
- Current emdb write path is significantly slower than sled/redb in this baseline and is an optimization target.
- Treat this as a point-in-time baseline, not a universal conclusion. Repeat on your target hardware and workload profile.

## Post write-path optimization rerun (2026-04-24)

Commands used:

```powershell
$env:EMDB_BENCH_RECORDS = "5000"
cargo bench --bench comparative --features bench-compare
```

```powershell
$env:EMDB_BENCH_RECORDS = "5000"
cargo bench --bench comparative --features bench-rocksdb
```

```powershell
$env:EMDB_BENCH_RECORDS = "5000"
$env:EMDB_REDIS_URL = "redis://127.0.0.1:6379/"
cargo bench --bench comparative --features bench-redis
```

Results (Criterion ranges):

| Workload | emdb | sled | redb | rocksdb | redis |
| --- | --- | --- | --- | --- | --- |
| Insert 5,000 records | 123.91ms - 130.64ms (38.27K - 40.35K elem/s) | 18.23ms - 20.18ms (247.80K - 274.23K elem/s) | 16.39ms - 16.87ms (296.46K - 305.07K elem/s) | 43.33ms - 45.16ms (110.73K - 115.38K elem/s) | 2.688s - 2.733s (1.83K - 1.86K elem/s) |
| Read 5,000 records | 652.79us - 775.30us (6.45M - 7.66M elem/s) | 1.67ms - 1.99ms (2.51M - 3.00M elem/s) | 922.53us - 987.53us (5.06M - 5.42M elem/s) | 4.689ms - 4.771ms (1.05M - 1.07M elem/s) | 2.705s - 2.892s (1.73K - 1.85K elem/s) |

Interpretation:

- emdb write throughput improved significantly versus the initial baseline (roughly 14x improvement on this workload).
- emdb read throughput remains in the same high-throughput range; run-to-run variance is visible and expected for microbenchmarks.
- Redis in this comparative harness includes network/protocol overhead and is not directly comparable to embedded engines on absolute latency.
- Use these numbers as workload-specific snapshots; rerun on your target hardware and deployment topology for decision making.

## Post hot-path-rewrite rerun (2026-04-25)

Following an audit-and-rewrite pass that:

- buffered the WAL through a `BufWriter` (one syscall per buffer flush),
- replaced the owned `Op` enum on the storage append path with a borrowed
  `OpRef<'_>` so inserts no longer clone keys/values just to log them,
- rewrote `encode_op` to write directly into the output buffer with no
  intermediate `Vec` allocation,
- moved the in-memory primary index from a single `RwLock<BTreeMap>` to a
  32-shard `[RwLock<HashMap>; 32]` keyed by FNV-1a,
- replaced `Mutex<Box<dyn Storage>>` with `Option<Mutex<PageStorage>>` so
  in-memory mode acquires no mutex and pays no dynamic dispatch.

Commands used:

```powershell
$env:EMDB_BENCH_RECORDS = "5000"
cargo bench --bench comparative --features bench-compare
```

Results (Criterion ranges):

| Workload | emdb | sled | redb |
| --- | --- | --- | --- |
| Insert 5,000 records | 95.08ms - 99.78ms (50.10K - 52.59K elem/s) | 15.83ms - 16.20ms (308.70K - 315.90K elem/s) | 14.37ms - 16.15ms (309.58K - 347.97K elem/s) |
| Read 5,000 records | 454.22us - 501.55us (9.97M - 11.01M elem/s) | 1.48ms - 1.65ms (3.04M - 3.37M elem/s) | 828.32us - 854.16us (5.85M - 6.04M elem/s) |

Reads (in-memory, `benches/kv.rs`, 1,000 records per iteration):

| Workload | Wall time (median) | Δ vs prior |
| --- | --- | --- |
| `kv_insert` | 153.54µs | −44% (≈1.8× throughput) |
| `kv_get` | 81.61µs | −22% (≈1.3× throughput) |
| `kv_remove` | 217.53µs | −49% (≈2× throughput) |

Interpretation:

- emdb leads `compare_read` at ~10.5M elem/s — ahead of redb (5.97M),
  rocksdb (1.05M), sled (3.10M), and redis (1.85K). Sharded HashMap and
  in-memory mode bypass closed the per-read overhead.
- emdb `compare_insert` improves ~30% (40K → 51K elem/s) but a sled/redb
  gap remains. The remaining gap is **architectural** — the v0.6 page
  format allocates one 4 KB page per value, so a 5,000-record bench with
  64-byte values burns ~20 MB of pages. sled and redb pack many values
  per leaf. Closing this requires a value-packing change to the page
  format and is out of scope for the audit/perf pass that produced this
  rerun.
- In-memory throughput (no WAL, no pager) is now in the multi-million
  ops/sec range on this hardware: ~6.5M inserts/sec, ~12.3M reads/sec
  derived from `kv_insert` and `kv_get` wall times.
- Redis numbers carry network/protocol overhead and are not directly
  comparable to embedded engines on absolute latency.
