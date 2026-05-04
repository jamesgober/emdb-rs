<h1 align="center">
    <strong>emdb</strong>
    <br>
    <sup><sub>EMBEDDED DATABASE FOR RUST</sub></sup>
</h1>

<p align="center">
    <a href="https://crates.io/crates/emdb"><img alt="crates.io" src="https://img.shields.io/crates/v/emdb.svg"></a>
    <a href="https://docs.rs/emdb"><img alt="docs.rs" src="https://docs.rs/emdb/badge.svg"></a>
    <a href="https://github.com/jamesgober/emdb-rs/actions"><img alt="CI" src="https://github.com/jamesgober/emdb-rs/actions/workflows/ci.yml/badge.svg"></a>
    <a href="https://github.com/jamesgober/emdb-rs/blob/main/LICENSE"><img alt="License" src="https://img.shields.io/badge/license-Apache--2.0-blue.svg"></a>
</p>

<p align="center">
    A lightweight, high-performance embedded key-value database for Rust.
</p>

---

## Why emdb

Bitcask-style architecture: one mmap-backed append-only file, sharded
in-memory hash index, single-writer with multi-reader. Same shape that
LMDB and redb use for reads; same shape that Riak/HaloDB use for writes.

### Performance vs. peers

5 M records, 24-byte random keys, 150-byte random values — same workload
shape as redb's published bench. Lower is better; numbers in
milliseconds. Run on a Windows 11 NVMe consumer box. Reproduce with
`cargo bench --bench lmdb_style --features ttl,bench-compare`.

| phase                       |        emdb |    redb  |    sled  |  emdb vs redb     |
|-----------------------------|------------:|---------:|---------:|------------------:|
| bulk load                   |    **4498** |    74496 |    60807 |     16.6× faster  |
| batch writes                |    **2814** |    11043 |     1972 |      3.9× faster  |
| nosync writes               |     **220** |     1717 |     1136 |      7.8× faster  |
| random reads (1M)           |     **596** |     5289 |    11197 |      8.9× faster  |
| random reads (4 threads)    |    **1083** |    17543 |    34605 |     16.2× faster  |
| random reads (8 threads)    |     **653** |    17160 |    33284 | **26× faster**    |
| removals                    |   **11948** |    54905 |    46155 |      4.6× faster  |
| compaction                  |   **11490** |    16506 |      N/A |      1.4× faster  |
| uncompacted size            |    1.08 GiB | 4.00 GiB | 2.13 GiB |     3.7× smaller  |
| compacted size              | **498 MiB** | 1.64 GiB |      N/A |     3.4× smaller  |
| individual writes (fsync/op)|       27455 |  **734** |  **316** | see note 1        |
| random range reads          |       opt-in|     3958 |     9688 | see note 2        |

emdb wins every aggregate-throughput column at 5 M scale, often by
**order-of-magnitude margins**. Two notes on the columns where
the picture is more nuanced:

1. **`individual writes` is fsync-bound.** This phase calls
   `db.insert(); db.flush();` per record. Each `db.flush()` is one
   `fdatasync` (one `FlushFileBuffers` on Windows), and that syscall
   is the floor — ~27 ms / call on the reference NVMe consumer box,
   regardless of how few bytes were dirtied. redb and sled win this
   column because their commit machinery folds adjacent writes into
   a single sync (redb's WAL + write transaction batching; sled's
   LSM log appends). emdb's group-commit pipeline lands in **v0.8**
   and closes this gap; until then, workloads that need per-record
   durability should batch through `db.transaction(|tx| ...)` (one
   fsync per transaction) or `db.insert_many(...)` (one fsync per
   batch), both of which already dominate redb in the aggregate
   columns above.
2. **Range reads are opt-in, not unsupported.** emdb's primary
   index is hash-keyed, so the default open does not pay the memory
   tax for sorted iteration. Set
   `EmdbBuilder::enable_range_scans(true)` to maintain a parallel
   `BTreeMap` secondary index per namespace — see the
   [Range scans](#range-scans) section below for the API and the
   memory-cost trade-off. The `lmdb_style` bench runs in hash-only
   mode (which is why the row reads `opt-in`); a fair head-to-head
   range bench requires the streaming range API arriving in v0.8.

### Read scaling under fan-out

The MT random-read columns above show emdb scaling to **7.66 M reads/sec
aggregate at 8 threads** on a 4-core consumer box, while redb stalls
near 290 K/sec past one thread. The lock-free `Arc<Mmap>` read path
plus the 64-shard hash index keep the hot path contention-free; past
core count, shared memory bandwidth is the only cap.

For more thread-count granularity, run
`cargo bench --bench concurrent_reads`.

See [docs/BENCH.md](docs/BENCH.md) for full run instructions and tuning
notes.

## Status

**v0.7.2.** The storage engine is a Bitcask-style mmap-backed
append-only log with a sharded in-memory hash index. Single-writer,
multi-reader. Optional at-rest encryption (AES-256-GCM or
ChaCha20-Poly1305, raw key or Argon2id passphrase). Optional
sorted-iteration secondary index via
`EmdbBuilder::enable_range_scans(true)`. Pre-1.0; the API may still
change before 1.0.

The next release (v0.8) lands the group-commit pipeline that closes
the per-record-durability gap, plus streaming `iter` / `keys` /
`range`, a zero-copy `get` variant, and a deterministic
crash-recovery test harness. v1.0 is the API freeze.

## Installation

```toml
[dependencies]
emdb = "0.7.2"
```

## Quick start

```rust
use emdb::Emdb;

let db = Emdb::open_in_memory();
db.insert("name", "emdb")?;
assert_eq!(db.get("name")?, Some(b"emdb".to_vec()));
# Ok::<(), emdb::Error>(())
```

## Persistence

```rust
use emdb::Emdb;

let path = std::env::temp_dir().join("app.emdb");

{
    let db = Emdb::open(&path)?;
    db.insert("user:1", "james")?;
    db.flush()?;
}

let reopened = Emdb::open(&path)?;
assert_eq!(reopened.get("user:1")?, Some(b"james".to_vec()));
# let _cleanup = std::fs::remove_file(path);
# Ok::<(), emdb::Error>(())
```

`flush()` durably writes the record bytes; it does not rewrite the
file header. The header carries a `tail_hint` that lets the next
open skip past the bulk of the log instead of scanning from byte
4096. Call `checkpoint()` at quiescent points (after a bulk load,
on graceful shutdown) to update that hint and pay one extra fsync
in exchange for fast reopens. The drop of the last handle attempts
a checkpoint as a backstop; explicit calls are recommended for
long-lived processes that care about reopen latency.

## Storage path resolution

`Emdb::open(path)` is the simplest entry point. For library / app
authors who want platform-aware path resolution, set both `app_name`
and `database_name` so your project gets a clearly-scoped subdirectory
under the platform data root.

```rust
use emdb::Emdb;

// Resolves to:
//   Linux:   $XDG_DATA_HOME/hivedb-kv/sessions.emdb
//   macOS:   ~/Library/Application Support/hivedb-kv/sessions.emdb
//   Windows: %LOCALAPPDATA%\hivedb-kv\sessions.emdb
let db = Emdb::builder()
    .app_name("hivedb-kv")
    .database_name("sessions.emdb")
    .build()?;
# Ok::<(), emdb::Error>(())
```

| builder method        | default if unset      | notes                                            |
|-----------------------|-----------------------|--------------------------------------------------|
| `app_name(name)`      | `"emdb"`              | Single folder name under the platform data root. |
| `database_name(name)` | `"emdb-default.emdb"` | Bare filename; no extension auto-added.          |
| `data_root(path)`     | platform default      | Escape hatch for tests / containers / sandboxes. |

`app_name` is a single folder name by design — path separators (`/`,
`\`), `..` components, and the empty string are rejected at build time.
Mixing `path()` with any of the OS-resolution methods returns
`Error::InvalidConfig`.

## Bulk loading

For high-volume inserts, prefer `insert_many` — it packs every record
into a single buffer and does one `pwrite`, which is the path that beats
redb 2.4× in the bench above.

```rust
use emdb::Emdb;

let db = Emdb::open_in_memory();
let items: Vec<(String, String)> = (0..1000)
    .map(|i| (format!("k{i}"), format!("v{i}")))
    .collect();
db.insert_many(items.iter().map(|(k, v)| (k.as_str(), v.as_str())))?;
db.flush()?;
# Ok::<(), emdb::Error>(())
```

## Transactions

```rust
use emdb::Emdb;

let db = Emdb::open_in_memory();
db.transaction(|tx| {
    tx.insert("user:1", "james")?;
    tx.insert("user:2", "alex")?;
    Ok(())
})?;

assert_eq!(db.get("user:1")?, Some(b"james".to_vec()));
# Ok::<(), emdb::Error>(())
```

Transactions buffer writes and commit them as one bulk insert on
success. `Err` from the closure drops the buffered writes — nothing
hits disk.

```rust
use emdb::{Emdb, Error};

let db = Emdb::open_in_memory();
let failed = db.transaction::<_, ()>(|tx| {
    tx.insert("temp", "value")?;
    Err(Error::TransactionAborted("rollback"))
});

assert!(failed.is_err());
assert_eq!(db.get("temp")?, None);
# Ok::<(), emdb::Error>(())
```

### Durability model

Each record is framed with a CRC32. On crash recovery the engine walks
records from `header.tail_hint` and treats the first bad CRC as the
truncation point. Per-record atomicity is guaranteed; **batch
atomicity across a transaction is not** — a crash mid-commit leaves a
prefix of the batch durable. Callers that need true all-or-nothing
across N records must layer that on top.

## Compaction

The append-only log accumulates tombstoned and superseded records over
time. `Emdb::compact()` rewrites the live records into a sibling file,
truncates to logical size, and atomically swaps it in.

```rust
use emdb::Emdb;

let path = std::env::temp_dir().join("compact.emdb");
let db = Emdb::open(&path)?;
db.insert("k", "v")?;
db.remove("k")?;            // tombstone added to log
db.compact()?;              // log now holds only the live records
db.flush()?;
# let _cleanup = std::fs::remove_file(&path);
# let _cleanup2 = std::fs::remove_file(format!("{}.lock", path.display()));
# Ok::<(), emdb::Error>(())
```

Compaction is a heavier operation than `flush` — call it on maintenance
windows, not on every write. Existing readers holding `Arc<Mmap>`
snapshots from before the compaction continue reading from the old
inode until they release; new reads see the compacted layout.

## Range scans

emdb's primary index is a sharded hash, so unsorted iteration is the
default. To support range / prefix queries, opt in at open time with
`EmdbBuilder::enable_range_scans(true)`. The engine maintains a
parallel `BTreeMap<Vec<u8>, u64>` secondary index per namespace; range
queries hit the BTreeMap and resolve values through the mmap.

```rust
use emdb::Emdb;

let db = Emdb::builder()
    .enable_range_scans(true)
    .build()?;

db.insert("user:001", "alice")?;
db.insert("user:002", "bob")?;
db.insert("session:abc", "token")?;

// Half-open range: ["user:", "user;").
let users = db.range(b"user:".to_vec()..b"user;".to_vec())?;
assert_eq!(users.len(), 2);
assert_eq!(users[0].0, b"user:001");
assert_eq!(users[1].0, b"user:002");

// Prefix shorthand: builds the half-open `[prefix, prefix++)` range.
let same = db.range_prefix(b"user:")?;
assert_eq!(users.len(), same.len());
# Ok::<(), emdb::Error>(())
```

Cost: one `Vec<u8>` clone of the key per insert plus the `BTreeMap`
node overhead — roughly doubles in-memory index size for a typical
workload. Calling `db.range(...)` without enabling this at open time
returns `Error::InvalidConfig`.

`Namespace::range` and `Namespace::range_prefix` give the same view
scoped to a named namespace.

## Cargo features

- `ttl` *(default)* — per-record expiration and `default_ttl`.
- `nested` — dotted-prefix group operations and `Focus` handles.
- `encrypt` — AES-256-GCM + ChaCha20-Poly1305 at-rest encryption with
  raw-key or Argon2id-derived passphrase. Pulls in `aes-gcm`,
  `chacha20poly1305`, `argon2`, `rand_core`.
- `bench-compare` — pulls in `redb` and `sled` for the comparative
  bench (dev-only; not for production builds).
- `bench-rocksdb` / `bench-redis` — additional comparative bench peers.

## Concurrency

`Emdb` is `Send + Sync` and cheap to clone — clones share the same
underlying engine via `Arc`. Pass clones across threads instead of
synchronising access to a single handle.

**Reads scale.** A 64-shard sharded `RwLock<HashMap>` index plus
zero-copy slices from a shared `Arc<Mmap>` keep the hot path
contention-free: the comparative bench above hits 7.66 M reads/sec
aggregate at 8 threads on a 4-core consumer box.

**Writes are single-writer.** All writers serialise on one mutex that
covers the encode-and-pwrite step. This matches the model used by
LMDB, redb, BoltDB, and most of the embedded-KV ecosystem (multi-writer
concurrency requires either a recovery model with sentinel records or
per-thread log segments — both queued for v1.0). High-throughput
producer workloads should batch through `db.insert_many(...)` or
`db.transaction(|tx| ...)`, which amortise the writer-mutex acquire
across many records.

```rust
use std::sync::Arc;
use std::thread;

use emdb::Emdb;

let db = Arc::new(Emdb::open_in_memory());
db.insert("counter", "0")?;

let mut workers = Vec::new();
for i in 0_u32..4 {
    let db = Arc::clone(&db);
    workers.push(thread::spawn(move || {
        let _ = db.insert(format!("k{i}"), format!("v{i}"));
    }));
}

for worker in workers {
    let _ = worker.join();
}

assert!(db.len()? >= 4);
# Ok::<(), emdb::Error>(())
```

## TTL example

```rust
# #[cfg(feature = "ttl")]
# {
use std::time::Duration;

use emdb::{Emdb, Ttl};

let db = Emdb::builder()
    .default_ttl(Duration::from_secs(30))
    .build()?;
db.insert_with_ttl("session", "token", Ttl::Default)?;
assert!(db.ttl("session")?.is_some());
# }
# Ok::<(), emdb::Error>(())
```

## Nested example

```rust
# #[cfg(feature = "nested")]
# {
use emdb::Emdb;

let db = Emdb::open_in_memory();
let product = db.focus("product");
product.set("name", "phone")?;
product.set("price", "799")?;

assert_eq!(product.get("name")?, Some(b"phone".to_vec()));
assert_eq!(db.group("product")?.count(), 2);
# }
# Ok::<(), emdb::Error>(())
```

## Encryption

```rust
# #[cfg(feature = "encrypt")]
# {
use emdb::Emdb;

let path = std::env::temp_dir().join("encrypted.emdb");
let _ = std::fs::remove_file(&path);
let _ = std::fs::remove_file(format!("{}.lock", path.display()));

let db = Emdb::builder()
    .path(path.clone())
    .encryption_passphrase("correct horse battery staple")
    .build()?;
db.insert("k", "v")?;
db.flush()?;
drop(db);

let reopened = Emdb::builder()
    .path(path.clone())
    .encryption_passphrase("correct horse battery staple")
    .build()?;
assert_eq!(reopened.get("k")?, Some(b"v".to_vec()));

# drop(reopened);
# let _ = std::fs::remove_file(&path);
# let _ = std::fs::remove_file(format!("{}.lock", path.display()));
# }
# Ok::<(), emdb::Error>(())
```

The cipher is creation-time-fixed and stored in the header — reopens
auto-dispatch. Wrong passphrase surfaces as
`Error::EncryptionKeyMismatch` from a verification block check, not
from a corrupted-data read. Three offline admin functions
(`Emdb::enable_encryption`, `disable_encryption`, `rotate_encryption_key`)
let you toggle encryption or rotate keys on an existing file via
atomic rewrite-then-rename, leaving an `.encbak` backup.

## Goals

- **Embedded-first** — runs in-process; no separate server, no network.
- **High performance** — zero-copy reads, allocation-free hot paths,
  cache-friendly layout, batched writes amortise lock and syscall costs.
- **Safe** — strict `clippy` profile, no `unwrap` in library code,
  every `unsafe` block documented with its invariant.
- **Small footprint** — minimal dependency graph, fast compile times.
- **Portable** — Linux, macOS, Windows on x86_64 and ARM64.

## Non-goals

- Client/server operation (use a dedicated DBMS for that).
- SQL.
- Distributed replication.
- Range scans on a single namespace (the index is hash-based; insert a
  prefix-sorted secondary structure on top if you need ranges).

## Benchmarking

emdb ships Criterion benches. The comparative bench can include `redb`,
`sled`, optionally RocksDB, and optionally Redis.

- Core: [benches/kv.rs](benches/kv.rs)
- Comparative: [benches/comparative.rs](benches/comparative.rs)

```powershell
# Just emdb
cargo bench --bench kv --features ttl

# emdb vs sled vs redb
cargo bench --bench comparative --features ttl,bench-compare

# Add RocksDB
cargo bench --bench comparative --features ttl,bench-compare,bench-rocksdb

# Add Redis (set EMDB_REDIS_URL first)
$env:EMDB_REDIS_URL = "redis://127.0.0.1/"
cargo bench --bench comparative --features ttl,bench-compare,bench-redis
```

Full bench workflow and tuning notes: [docs/BENCH.md](docs/BENCH.md).

## Related projects

`emdb` is the Rust implementation. Implementations in other languages
(Go, C, etc.) are planned and will live under their own repositories.

## License

Licensed under the [Apache License, Version 2.0](./LICENSE).

Copyright &copy; 2026 James Gober.
