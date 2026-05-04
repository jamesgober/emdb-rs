# Benchmarking emdb

This project ships four Criterion / raw-timing benches:

- [`benches/kv.rs`](../benches/kv.rs) — focused micro-bench for the
  `insert` / `get` / `remove` hot paths, ephemeral DB, single thread.
- [`benches/comparative.rs`](../benches/comparative.rs) — emdb vs.
  `redb` / `sled`, single-thread workload, default value sizes.
- [`benches/concurrent_reads.rs`](../benches/concurrent_reads.rs) —
  multi-thread read fan-out (1 / 2 / 4 / 8 reader threads) against a
  pre-populated DB, showcases the lock-free `Arc<Mmap>` read path.
- [`benches/lmdb_style.rs`](../benches/lmdb_style.rs) —
  apples-to-apples mirror of redb's published `lmdb_benchmark.rs`
  workload (5 M records, 24-byte random keys, 150-byte random values,
  full phase set including bulk load, individual writes, batch writes,
  nosync writes, random reads × 2, MT reads at 4 / 8 threads,
  removals, uncompacted / compacted size). Range-read phases are
  recorded as `N/A` for emdb because the hash index does not support
  sorted iteration unless `EmdbBuilder::enable_range_scans(true)` is
  set.

## Quick runs

```powershell
# emdb-only micro-benches
cargo bench --bench kv --features ttl

# emdb vs sled vs redb at default scale (20 K records)
cargo bench --bench comparative --features ttl,bench-compare

# Read scaling under thread fan-out
cargo bench --bench concurrent_reads --features ttl

# Apples-to-apples vs redb's published methodology (5 M records)
$env:EMDB_BENCH_RECORDS = "5000000"
cargo bench --bench lmdb_style --features ttl,bench-compare
```

`EMDB_BENCH_RECORDS` defaults to 1 M for `lmdb_style` and 20 K for
`comparative`. Override the env var to scale up or down.

## Optional comparison peers

Add additional engines via Cargo features:

```powershell
# RocksDB
cargo bench --bench comparative --features ttl,bench-compare,bench-rocksdb

# Redis (network protocol; requires a running Redis server)
docker run --rm -p 6379:6379 redis:7   # in another terminal
$env:EMDB_REDIS_URL = "redis://127.0.0.1/"
cargo bench --bench comparative --features ttl,bench-redis
```

Redis numbers include network overhead and are not directly
comparable to embedded engines on absolute latency.

## Reading results

Criterion writes detailed reports to
[target/criterion](../target/criterion). Key metrics:

- Throughput (elements/sec)
- Time per iteration (mean, p50, p95)
- Relative comparison between benchmark IDs in the same group

The `lmdb_style` bench prints a markdown table to stdout with
millisecond totals per phase per engine. The `concurrent_reads` bench
is Criterion-driven and reports throughput per thread count.

## Reproducibility notes

For meaningful comparisons:

- Use the release profile via `cargo bench` (the dev profile is
  ~5-10× slower).
- Close heavy background apps; pin the CPU power mode if possible.
- Run multiple times and compare medians, not single-best values.
- Keep dataset size and value size identical when comparing engines.
- The `lmdb_style` bench seeds a deterministic RNG, so the same
  dataset is generated across runs and across engines within a run.

## Suggested reporting format

When logging benchmark output into README or release notes, record:

- CPU and memory
- OS + Rust version
- Command used
- Record count (`EMDB_BENCH_RECORDS`)
- Wall-time per phase, or throughput per phase, for each engine
- Any notable caveats (network overhead, hardware differences,
  background load)

## Reference baseline (5 M records, Windows 11 NVMe)

Captured 2026-05-03 on emdb v0.8.0 vs. redb 2.6 vs. sled 0.34 with:

```powershell
$env:EMDB_BENCH_RECORDS = "5000000"
cargo bench --bench lmdb_style --features ttl,bench-compare
```

Lower is better; numbers are wall-time milliseconds (or bytes for
the size rows).

| phase                       |        emdb |    redb  |    sled  |
|-----------------------------|------------:|---------:|---------:|
| bulk load                   |    **3089** |    48221 |    32994 |
| batch writes                |    **2752** |     6555 |     1325 |
| nosync writes               |     **125** |     1142 |      681 |
| random reads (1 M)          |     **351** |     3071 |     6255 |
| random reads (4 threads)    |     **799** |    14761 |    22692 |
| random reads (8 threads)    |     **503** |    14413 |    24372 |
| removals                    |    **6659** |    35388 |    56910 |
| compaction                  |    **7158** |    11473 |      N/A |
| uncompacted size            |    1.08 GiB | 4.00 GiB | 2.15 GiB |
| compacted size              | **498 MiB** | 1.64 GiB |      N/A |
| individual writes (fsync/op)|       26779 |  **611** |  **534** |
| random range reads          |         N/A |     2538 |     6164 |

Notes:

- emdb wins every aggregate-throughput phase, often by an order of
  magnitude. Aggregate read throughput at 8 threads is **~9.94 M
  reads/sec**.
- The `individual writes` phase syncs after every record from a
  single thread. emdb pays one Windows `FlushFileBuffers` per
  write, which dominates the result; redb / sled win this column
  because their commit machinery folds adjacent single-thread
  writes into a single sync. For multi-threaded
  per-record-durability workloads, opt into `FlushPolicy::Group` —
  see the group-commit baseline below for the 7× win on N=8
  concurrent flushers.
- `random range reads` is N/A because the bench runs in hash-only
  mode. Set `EmdbBuilder::enable_range_scans(true)` to enable the
  opt-in BTreeMap secondary index, then use `range_iter` /
  `range_prefix_iter` for streaming consumption.

These numbers are workload- and hardware-specific. Reproduce on
your target deployment for decision making.

## Group-commit baseline (8 threads × 200 writes, default policy)

Captured 2026-05-03 with:

```powershell
cargo bench --bench group_commit --features ttl
```

Each thread does `db.insert(); db.flush();` in a tight loop.
Aggregate throughput across all threads:

| policy         | wall time (ms) |   writes/sec |    speedup |
|----------------|---------------:|-------------:|-----------:|
| OnEachFlush    |          1490  |       1 073  |      1.00× |
| Group          |       **201**  |    **7 946** |  **7.40×** |

`Group` policy used `max_wait = 500 µs`, `max_batch = 8` (matching
the thread count). Tune via env vars `EMDB_BENCH_GC_THREADS`,
`EMDB_BENCH_GC_PER_THREAD`, `EMDB_BENCH_GC_MAX_WAIT_US`,
`EMDB_BENCH_GC_MAX_BATCH`.

`max_batch` set higher than the concurrent flusher count is a
performance trap — the leader waits the full `max_wait` for
followers that can never arrive. As a rule of thumb, set it to
`num_cpus::get()` for general server workloads.
