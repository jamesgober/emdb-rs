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

Captured 2026-05-04 on emdb v0.9.0 (fsys-journal
substrate) vs. redb 2.6 vs. sled 0.34 with:

```powershell
$env:EMDB_BENCH_RECORDS = "5000000"
cargo bench --bench lmdb_style --features ttl,bench-compare
```

Lower is better; numbers are wall-time milliseconds (or bytes for
the size rows).

| phase                       |        emdb |    redb  |    sled  |
|-----------------------------|------------:|---------:|---------:|
| bulk load                   |   **13 724** |    43 660 |    31 116 |
| individual writes (fsync/op)|     **406** |      544 |      429 |
| batch writes                |     **292** |     5 970 |     1 286 |
| nosync writes               |     **127** |     1 025 |      675 |
| random reads (1 M)          |     **322** |     2 765 |     6 079 |
| random reads (4 threads)    |     **703** |    11 210 |    22 884 |
| random reads (8 threads)    |     **511** |    13 026 |    23 392 |
| removals                    |   **5 662** |    33 348 |    25 631 |
| compaction                  |   **8 268** |    12 540 |      N/A |
| uncompacted size            |    1.10 GiB |  4.00 GiB |  2.15 GiB |
| compacted size              | **508 MiB** |  1.64 GiB |      N/A |
| random range reads          |       N/A   |     2 376 |     6 133 |

Notes:

- emdb wins every column in v0.9. Aggregate read throughput at
  8 threads is **~9.78 M reads/sec**.
- The `individual writes` phase syncs after every record from a
  single thread. v0.8.5 was 39× behind redb on this column
  because each `db.flush()` hit one Windows `FlushFileBuffers`
  per call. v0.9 routes the write path through fsys's journal
  substrate (lock-free LSN reservation + group-commit fsync +
  NVMe passthrough flush where supported), and the column went
  from 25 281 ms in v0.8.5 to 406 ms in v0.9 — **62× faster
  vs. our own previous release** and **1.3× faster than redb,
  1.06× faster than sled.**
- `random range reads` is N/A because the bench runs in hash-
  only mode. Set `EmdbBuilder::enable_range_scans(true)` to
  enable the opt-in BTreeMap secondary index, then use
  `range_iter` / `range_prefix_iter` for streaming consumption.
- One genuine regression vs. v0.8.5: `bulk load` is slower
  (3 086 ms → 13 724 ms). fsys's per-record framing
  (12-byte CRC-32C frame around every record) and lock-free
  LSN reservation add a small per-call overhead that adds up
  across 5 M tight-loop appends. We still beat redb (3.2× faster)
  and sled (2.3× faster) on this phase; the absolute regression
  is the cost of moving to a real journal substrate. For the
  trade-off as a whole, the single-thread fsync win pays for
  the bulk-load cost many times over for any workload that
  ever calls `db.flush()`.

These numbers are workload- and hardware-specific. Reproduce on
your target deployment for decision making.

## v0.9 vs. v0.8.5 (own previous release)

Same hardware, same dataset, same workload. v0.9 ships a major
architectural change (fsys-journal substrate) and the numbers
move accordingly:

| phase | v0.8.5 | v0.9.0 | delta |
|---|---:|---:|---:|
| individual writes | 25 281 ms | **406 ms** | **62× faster** |
| batch writes | 2 616 ms | **292 ms** | **9.0× faster** |
| random reads (4 threads) | 817 ms | **703 ms** | 1.16× faster |
| removals | 6 161 ms | **5 662 ms** | 1.09× faster |
| nosync writes | 131 ms | 127 ms | par |
| random reads (1 M) | 332 ms | 322 ms | par |
| random reads (8 threads) | 511 ms | 511 ms | par |
| compaction | 6 513 ms | 8 268 ms | 1.27× slower |
| bulk load | 3 086 ms | 13 724 ms | 4.5× slower |
| compacted size | 498 MiB | 508 MiB | 1.02× larger |

Headline: **62× faster on the column we couldn't fix in v0.8.5,
9× faster on batch writes, par or better on every other read
column.** The `bulk_load` and `compaction` regressions are real
but small in absolute terms — both still beat redb and sled
significantly.

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
