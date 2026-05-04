# Benchmarking emdb

This project ships six Criterion / raw-timing benches:

- [`benches/kv.rs`](../benches/kv.rs) — focused micro-bench for the
  `insert` / `get` / `remove` hot paths, ephemeral DB, single thread.
- [`benches/comparative.rs`](../benches/comparative.rs) — emdb vs.
  `redb` / `sled`, single-thread workload, default value sizes.
- [`benches/concurrent_reads.rs`](../benches/concurrent_reads.rs) —
  multi-thread read fan-out (1 / 2 / 4 / 8 reader threads) against
  a pre-populated DB, showcases the lock-free `Arc<Mmap>` read
  path.
- [`benches/group_commit.rs`](../benches/group_commit.rs) —
  multi-thread per-record-flush comparison between
  `FlushPolicy::OnEachFlush` and `FlushPolicy::Group`. Default
  workload: 8 producer threads × 200 writes each, with each thread
  inserting then flushing per record.
- [`benches/write_through.rs`](../benches/write_through.rs) —
  single-thread per-record-flush comparison between
  `FlushPolicy::OnEachFlush` and `FlushPolicy::WriteThrough`.
  Same shape as the `lmdb_style` `individual writes` phase
  (1 000 writes, 150-byte values), one record per `flush()` call.
- [`benches/lmdb_style.rs`](../benches/lmdb_style.rs) —
  apples-to-apples mirror of redb's published `lmdb_benchmark.rs`
  workload (5 M records, 24-byte random keys, 150-byte random
  values, full phase set including bulk load, individual writes,
  batch writes, nosync writes, random reads × 2, MT reads at 4 /
  8 threads, removals, uncompacted / compacted size). Range-read
  phases are recorded as `N/A` for emdb because the hash index
  does not support sorted iteration unless
  `EmdbBuilder::enable_range_scans(true)` is set.

## Quick runs

```powershell
# emdb-only micro-benches
cargo bench --bench kv --features ttl

# emdb vs sled vs redb at default scale (20 K records)
cargo bench --bench comparative --features ttl,bench-compare

# Read scaling under thread fan-out
cargo bench --bench concurrent_reads --features ttl

# Group commit (multi-thread fsync coalescing)
cargo bench --bench group_commit --features ttl

# WriteThrough (single-thread per-record durability)
cargo bench --bench write_through --features ttl

# Apples-to-apples vs redb's published methodology (5 M records)
$env:EMDB_BENCH_RECORDS = "5000000"
cargo bench --bench lmdb_style --features ttl,bench-compare
```

`EMDB_BENCH_RECORDS` defaults to 1 M for `lmdb_style` and 20 K
for `comparative`. The group-commit bench defaults to 8 threads
× 200 writes; the write-through bench defaults to 1 000 writes.
Override the per-bench env vars (`EMDB_BENCH_GC_*` and
`EMDB_BENCH_WT_*`) to scale either direction.

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

Captured 2026-05-04 on emdb v0.8.5 vs. redb 2.6 vs. sled 0.34 with:

```powershell
$env:EMDB_BENCH_RECORDS = "5000000"
cargo bench --bench lmdb_style --features ttl,bench-compare
```

Lower is better; numbers are wall-time milliseconds (or bytes for
the size rows).

| phase                       |        emdb |    redb  |    sled  |
|-----------------------------|------------:|---------:|---------:|
| bulk load                   |    **3086** |    68231 |    39506 |
| batch writes                |    **2616** |     6656 |     1370 |
| nosync writes               |     **131** |     1063 |      697 |
| random reads (1 M)          |     **332** |     2814 |     6201 |
| random reads (4 threads)    |     **817** |    11945 |    22813 |
| random reads (8 threads)    |     **511** |    12838 |    22891 |
| removals                    |    **6161** |    32840 |    25271 |
| compaction                  |    **6513** |    14163 |      N/A |
| uncompacted size            |    1.08 GiB | 4.00 GiB | 2.15 GiB |
| compacted size              | **498 MiB** | 1.64 GiB |      N/A |
| individual writes (fsync/op)|       25281 |  **644** |  **452** |
| random range reads          |         N/A |     2329 |     6247 |

Notes:

- emdb wins every aggregate-throughput phase, often by an order of
  magnitude. Aggregate read throughput at 8 threads is
  **~9.78 M reads/sec**.
- The `individual writes` phase syncs after every record from a
  single thread. emdb pays one Windows `FlushFileBuffers` per
  write, which dominates the result; redb / sled win this column
  because their commit machinery folds adjacent single-thread
  writes into a single sync. For multi-threaded
  per-record-durability workloads, opt into `FlushPolicy::Group`
  (see the group-commit baseline below for the 8× win on N = 8
  concurrent flushers); for single-thread per-record durability,
  benchmark `FlushPolicy::WriteThrough` on your data shape — see
  the write-through section.
- `random range reads` is N/A because the bench runs in hash-only
  mode. Set `EmdbBuilder::enable_range_scans(true)` to enable the
  opt-in BTreeMap secondary index, then use `range_iter` /
  `range_prefix_iter` for streaming consumption.

These numbers are workload- and hardware-specific. Reproduce on
your target deployment for decision making.

## Group-commit baseline (8 threads × 200 writes, default policy)

Captured 2026-05-04 with:

```powershell
cargo bench --bench group_commit --features ttl
```

Each thread does `db.insert(); db.flush();` in a tight loop.
Aggregate throughput across all threads:

| policy         | wall time (ms) |   writes/sec |    speedup |
|----------------|---------------:|-------------:|-----------:|
| OnEachFlush    |          2192  |         730  |      1.00× |
| Group          |       **272**  |    **5 880** |  **8.06×** |

`Group` policy used `max_wait = 500 µs`, `max_batch = 8` (matching
the thread count). Tune via env vars `EMDB_BENCH_GC_THREADS`,
`EMDB_BENCH_GC_PER_THREAD`, `EMDB_BENCH_GC_MAX_WAIT_US`,
`EMDB_BENCH_GC_MAX_BATCH`.

## Write-through baseline (1 000 single-thread writes, fresh file)

Captured 2026-05-04 with:

```powershell
cargo bench --bench write_through --features ttl
```

Single thread, one record per `flush()` call, fresh database each
run, 150-byte values:

| policy         | wall time (ms) |   writes/sec |    speedup |
|----------------|---------------:|-------------:|-----------:|
| OnEachFlush    |          1099  |         909  |      1.00× |
| WriteThrough   |          1270  |         787  |      0.87× |

Honest read of these numbers: on a fresh small file
(≤ 150 KB of data), Windows `FlushFileBuffers` is already cheap
(≈ 1 ms / call), so `WriteThrough`'s per-`pwrite`-waits-for-disk
cost (≈ 1.27 ms / call) is *higher* per record than the baseline.
`WriteThrough` is a win when `OnEachFlush`'s per-flush cost is
dominated by `FlushFileBuffers` latency on a particular workload —
typically larger files, larger dirty footprints, or specific NVMe
firmware behaviour. The mechanism is real and exposed; whether
the trade-off favours `WriteThrough` is workload-dependent and
should be benchmarked on the actual data shape that matters.
Tune the bench via `EMDB_BENCH_WT_WRITES` and
`EMDB_BENCH_WT_VALUE_BYTES`.

`max_batch` set higher than the concurrent flusher count is a
performance trap — the leader waits the full `max_wait` for
followers that can never arrive. As a rule of thumb, set it to
`num_cpus::get()` for general server workloads.
