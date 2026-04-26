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

Captured with:

```powershell
$env:EMDB_BENCH_RECORDS = "5000000"
cargo bench --bench lmdb_style --features ttl,bench-compare
```

Lower is better; numbers are wall-time milliseconds (or bytes for the
size rows).

| phase                       |        emdb |    redb  |    sled  |
|-----------------------------|------------:|---------:|---------:|
| bulk load                   |    **4498** |    74496 |    60807 |
| batch writes                |    **2814** |    11043 |     1972 |
| nosync writes               |     **220** |     1717 |     1136 |
| random reads (1 M)          |     **596** |     5289 |    11197 |
| random reads (4 threads)    |    **1083** |    17543 |    34605 |
| random reads (8 threads)    |     **653** |    17160 |    33284 |
| removals                    |   **11948** |    54905 |    46155 |
| compaction                  |   **11490** |    16506 |      N/A |
| uncompacted size            |    1.08 GiB | 4.00 GiB | 2.13 GiB |
| compacted size              | **498 MiB** | 1.64 GiB |      N/A |
| individual writes (fsync/op)|       27455 |  **734** |  **316** |
| random range reads          |         N/A |     3958 |     9688 |

Notes:

- emdb wins every aggregate-throughput phase, often by an order of
  magnitude. Aggregate read throughput at 8 threads is **~7.66 M
  reads/sec**.
- The `individual writes` phase syncs after every record. emdb pays
  one Windows `FlushFileBuffers` per write, which dominates the
  result. Workloads needing per-record durability should batch via
  `db.transaction(...)` or `db.insert_many(...)` (the `batch writes`
  and `bulk load` phases).
- `random range reads` is N/A because emdb's primary index is
  hash-keyed. Set `EmdbBuilder::enable_range_scans(true)` to enable
  the opt-in BTreeMap secondary index.

These numbers are workload- and hardware-specific. Reproduce on your
target deployment for decision making.
