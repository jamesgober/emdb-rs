# emdb Architecture

This document describes the engine internals — the on-disk
journal layout, the in-memory index, the read path, the write
path, and how the optional features (range scans, TTL,
encryption, async) layer on top.

The intended audience is contributors and downstream
integrators who want to reason about emdb's failure modes,
performance characteristics, and extension points. Application
authors should not need any of this to use emdb correctly — see
[API.md](API.md) for the user-facing surface.

---

## Contents

- [Overview](#overview)
- [On-disk format](#on-disk-format)
- [Storage substrate (fsys)](#storage-substrate-fsys)
- [The in-memory index](#the-in-memory-index)
- [The read path](#the-read-path)
- [The write path](#the-write-path)
- [Range scans (secondary index)](#range-scans-secondary-index)
- [TTL](#ttl)
- [Encryption](#encryption)
- [Async surface](#async-surface)
- [Crash recovery](#crash-recovery)
- [Compaction](#compaction)
- [Concurrency model](#concurrency-model)
- [Failure modes](#failure-modes)

---

## Overview

emdb is a Bitcask-style embedded key-value store: an append-only
log on disk, an in-memory hash index pointing into that log,
and a periodic compaction step that rewrites the log without
the dead records.

```
                ┌──────────────────────────────────────┐
                │             user code                │
                └──────────────────┬───────────────────┘
                                   │
                ┌──────────────────▼───────────────────┐
                │   public API: Emdb / Namespace       │
                │   (db.rs, namespace.rs)              │
                └──────────────────┬───────────────────┘
                                   │
                ┌──────────────────▼───────────────────┐
                │   engine: storage::engine            │
                │   - decode_owned_at / decode_zerocopy│
                │   - append / append_batch            │
                │   - per-namespace index registry     │
                └────────┬─────────────┬───────────────┘
                         │             │
            ┌────────────▼──┐   ┌──────▼─────────────┐
            │ sharded hash  │   │ optional SkipMap   │
            │ index         │   │ (range scans)      │
            │ (index.rs)    │   │                    │
            └───────────────┘   └────────────────────┘
                         │             │
                ┌────────▼─────────────▼──────────────┐
                │   storage::store                    │
                │   - Arc<Mmap> for reads             │
                │   - fsys::JournalHandle for writes  │
                └──────────────────┬──────────────────┘
                                   │
                ┌──────────────────▼──────────────────┐
                │   fsys (external crate)             │
                │   - LSN reservation                 │
                │   - group-commit fsync              │
                │   - io_uring (Linux)                │
                │   - WRITE_THROUGH (Windows)         │
                │   - NVMe passthrough flush          │
                └─────────────────────────────────────┘
```

The split is intentional. **emdb owns the engine** — the
key/value semantics, the index, the recovery logic, the
serialisation format. **fsys owns the substrate** — durability,
platform-specific I/O, group commit, vectored append. emdb gets
to focus on storage-engine concerns; fsys gets to be benchmarked
and stabilised independently.

---

## On-disk format

A database is one journal file plus two sidecar files:

| File | Purpose |
|---|---|
| `<path>` | The journal — append-only sequence of frames. |
| `<path>.lock` | OS-level advisory lockfile (one process at a time). |
| `<path>.meta` | Atomically-replaced metadata sidecar (checkpoint info, encryption header, etc.). |

The journal is **never modified in place** during normal
operation. The only writers are:

1. **Append** — `fsys::JournalHandle::append` reserves an LSN,
   writes the new frame at the journal tail, optionally fsyncs.
2. **Compaction** — writes a fresh journal under a temp name,
   atomically renames it over the original.

Reads are always from a memory-mapped view (`Arc<Mmap>`) over
the live journal file.

### Frame format

Each frame is a self-describing record. The layout is owned by
fsys (`fsys::frame`), but emdb's record types embed inside the
payload:

```
┌───────────┬──────────┬───────────────┬───────────┬──────────┐
│ magic (4) │ flags(2) │ payload_len(4)│ payload   │ crc32c(4)│
└───────────┴──────────┴───────────────┴───────────┴──────────┘
```

The CRC covers the full frame including header bytes. fsys's
decoder validates the magic + CRC before handing the payload to
emdb's decoder.

### Record payload

emdb encodes one of three payload types:

| Tag | Type | Contents |
|---|---|---|
| `0x01` | Insert | `(ns_id, key, value, optional_expires_at)` |
| `0x02` | Tombstone | `(ns_id, key)` |
| `0x03` | Namespace metadata | `(ns_id, name)` |

`ns_id` is a 4-byte namespace identifier. The default namespace
has `ns_id = 0`; named namespaces are assigned dense IDs in the
order they're first created.

### Length-prefix encoding

Keys and values are `(varint_length || bytes)`. Varints use the
SQLite-style 1–9 byte encoding — short lengths stay 1 byte, the
worst case is a 9-byte length for a `u64::MAX`-byte payload.
Most application keys and values fit in 1–2 byte length
prefixes.

---

## Storage substrate (fsys)

emdb opens its journal through `fsys::JournalHandle`:

```rust,ignore
let journal = fsys::JournalBuilder::new(path)
    .tune_for(fsys::Workload::Database)
    .write_lifetime_hint(fsys::WriteLifetimeHint::Long)
    .open()?;
```

`tune_for(Workload::Database)` sets:

- 8 MiB resident buffer pool — enough headroom that small bursts
  don't allocate.
- 256-deep io_uring submission ring (Linux) — keeps the kernel
  fed without saturating it.
- 4 K-deep batch queue — vectored `append_batch` can submit up
  to 4 096 records in one syscall.

`write_lifetime_hint(Long)` tells the kernel/SSD firmware that
journal data is durable-write data, not temp-file churn — modern
NVMe firmware uses this hint to group the data into long-lived
NAND blocks and avoid unnecessary garbage-collection churn.

### Why fsys

The substrate split makes a few things tractable:

- **Vectored append.** A 10 K-record `insert_many` becomes one
  LSN reservation + one `pwrite` of a single contiguous buffer.
  No per-record syscall overhead, no per-record lock
  contention.
- **Group commit.** N concurrent `flush()` callers coalesce to
  one `fdatasync` — fsys's leader/follower coordinator handles
  the rendezvous.
- **Platform durability.** Windows gets `FILE_FLAG_WRITE_THROUGH`
  where appropriate, Linux gets io_uring + `RWF_DSYNC`, macOS
  gets `F_FULLFSYNC`. emdb doesn't have to know about any of
  these — it just calls `journal.flush()`.

---

## The in-memory index

The hot data structure. One per namespace.

### Sharded open addressing

The index is a **64-shard open-addressed hash table** of
seqlock-protected slots:

```
Index
├── shards[0..64]: Shard
│   ├── slots: Vec<AtomicSlot>      ← open-addressing, linear probe
│   ├── overflow: HashMap<u64, …>   ← cold path: 64-bit collisions
│   └── record_count: CachePadded<AtomicUsize>
└── hash_key(key) → KeyHash
    → shard = hash & 63
    → probe = (hash >> 6) % slots.len()
```

Each `AtomicSlot` is three atomics packed into one cache line:

```
AtomicSlot {
    state: AtomicU8,    // EMPTY | OCCUPIED | TOMBSTONE | OVERFLOW
    hash:  AtomicU64,   // KeyHash::hash
    offset:AtomicU64,   // byte offset into the journal
    seq:   AtomicU64,   // seqlock counter
}
```

### The hash function (v0.9.6)

`hash_key` is a wyhash-style two-prime mixer with a Murmur3
`fmix64` finalizer:

```rust,ignore
const PRIME_1: u64 = 0xa076_1d64_78bd_642f;
const PRIME_2: u64 = 0xe703_7ed1_a0b4_28db;
```

Two 64-bit primes multiplied against alternating 8-byte halves
of each 16-byte block; tail handling for 8 / 4 / per-byte;
three rounds of `fmix64` at the end. On the v0.9.4 stress key
pattern (`"stress-key-{idx:08}"` × 64 000) it produces 0
collisions; the previous FxHash had 22 956 collisions on the
same pattern. See the [v0.9.6 release
notes](../.dev/release/v0.9.6.md) for the diagnosis trail.

### Reads (seqlock)

```rust,ignore
loop {
    let s0 = slot.seq.load(Acquire);
    if s0 & 1 == 1 { continue; }       // writer active, retry

    let state  = slot.state.load(Relaxed);
    let hash   = slot.hash.load(Relaxed);
    let offset = slot.offset.load(Relaxed);

    compiler_fence(Acquire);
    let s1 = slot.seq.load(Acquire);    // strict Acquire — was Relaxed+fence pre-0.9.6
    if s0 == s1 && s0 & 1 == 0 {
        return (state, hash, offset);
    }
    // raced, retry
}
```

The trailing `seq.load(Acquire)` is the post-0.9.6 fix — under
the formal memory model, the prior pattern allowed the Relaxed
loads to be reordered past the Acquire fence in principle. The
post-0.9.6 pattern is conservative and fast.

### Writes (CAS-claim, then publish)

Writers verify state under the seqlock, then bump `seq` to odd,
write the fields, bump `seq` back to even. The seqlock-protected
methods are:

| Method | Verify-then-write |
|---|---|
| `try_claim(hash, offset)` | slot must be EMPTY |
| `try_update(hash, offset)` | slot must be OCCUPIED with matching hash |
| `try_tombstone(hash)` | slot must be OCCUPIED with matching hash |
| `try_promote_to_overflow(hash)` | slot must be OCCUPIED with matching hash |
| `write_unconditional(state, hash, offset)` | bypass — used by reload |

Each one is a TOCTOU-safe primitive: it reads the seqlock-
protected state, verifies the precondition, and writes
atomically if the slot hasn't changed. The v0.9.3 race that
the v0.9.4 fix addressed was specifically a verify-then-write
gap in the old `replace` method.

### Overflow handling

When two distinct keys hash to the same 64-bit value (birthday-
bound, expected 0 on well-distributed keys), the slot is
promoted to `STATE_OVERFLOW` and a per-shard `HashMap<u64,
Vec<(Vec<u8>, u64)>>` resolves the collision by raw key compare.
The overflow path is correctness-critical but cold — a clean
hash function (post-0.9.6) keeps it effectively unused.

### Sharding

Shard selection uses the low 6 bits of the hash. Each shard's
slot table is independently lockable for resize, so concurrent
writers across shards never contend on each other's growth
events.

Empirical contention curve (`benches/concurrent_reads.rs`):
- 1 thread: ~10 ns / `get` (uncontended)
- 4 threads: ~12 ns / `get`
- 8 threads: ~14 ns / `get`
- 16 threads on a 4-core box: shared memory bandwidth is the
  cap, not lock contention.

---

## The read path

```
Emdb::get(key)
 ├─ ns_id = self.ns_id (default = 0)
 ├─ hash  = Index::hash_key(key)
 ├─ Index::get(ns_id, hash, key) → Option<u64>      (offset)
 │   - probe shard → slot → seqlock read
 │   - if OCCUPIED and offset != STATE_OVERFLOW: return offset
 │   - if OVERFLOW: walk overflow map by raw key compare
 │   - if EMPTY: return None
 │   - if TOMBSTONE in probe sequence: skip, continue probe
 └─ Engine::decode_owned_at(offset) → (key', value', expires)
     - read frame at offset from Arc<Mmap>
     - decode payload (Insert / Tombstone / Namespace)
     - verify key matches the requested key
     - if encryption is enabled, decrypt value
     - if expires is set and past now, return None (lazy expiry)
     - return Some(value.to_vec())
```

Two things to note:

1. **The mmap is shared across all readers.** No reader takes a
   lock on the journal file; `Arc<Mmap>` clones share the same
   underlying pages. The kernel page cache does the rest.
2. **The verify-key step on decode is what defends against hash
   collisions.** If two keys collide and the index returns the
   wrong offset, decode will see a key mismatch and return
   `None`. The OVERFLOW path is the explicit handler, but the
   verify-key step is a belt-and-suspenders guarantee.

### Zero-copy reads (`get_zerocopy`)

```rust,ignore
Emdb::get_zerocopy(key) → Option<ValueRef<'_>>
```

`ValueRef` borrows a slice directly into the `Arc<Mmap>` — no
allocation, no decoding (beyond frame validation). The lifetime
is tied to the underlying mmap; if compaction or growth swaps
the mmap, existing `ValueRef`s are invalidated by Rust's
borrow checker before the swap can happen.

This is the fastest read path in the library. On a 24-byte key,
150-byte value workload, `get_zerocopy` is roughly 2× faster
than `get` because it skips the `Vec<u8>` allocation.

---

## The write path

```
Emdb::insert(key, value)
 ├─ ns_id = self.ns_id
 ├─ hash  = Index::hash_key(key)
 ├─ encode payload (Insert frame)
 ├─ if encryption enabled, encrypt value
 ├─ JournalHandle::append(payload) → (lsn, offset)
 │   - fsys reserves the LSN with one atomic fetch_add
 │   - pwrite the frame at the reserved byte range
 │   - update fsys's resident buffer pool
 ├─ if FlushPolicy != Group: journal.flush()        (per-call durability)
 ├─ Index::insert_or_replace(ns_id, hash, key, offset)
 │   - probe to find slot for hash
 │   - if EMPTY: try_claim
 │   - if OCCUPIED + matching hash: try_update
 │   - if OCCUPIED + different hash + tombstone available: claim tombstone
 │   - if hash collides post-claim: promote to OVERFLOW
 │   - on slot table near capacity: grow (per-shard lock)
 └─ if range_index enabled: SkipMap::insert(key, offset)
```

### Why writes don't take a global lock

The hot append path is **lock-free**:

- `fsys::JournalHandle::append` reserves its byte range via one
  atomic `fetch_add` on the next-LSN counter; no writer mutex.
- N concurrent appenders issue independent `pwrite`s to their
  reserved byte ranges. The kernel handles the syscall-level
  serialisation, not us.
- The in-memory index is sharded; updates to different shards
  are independent. Updates within a shard contend only via the
  per-slot seqlock (tens of nanoseconds when uncontended).
- The optional SkipMap is `crossbeam_skiplist::SkipMap`, which
  is itself lock-free.

The only place a write can block on a lock is **shard growth**
— when a shard's slot table needs to double, it briefly holds
the shard's write lock to swap in the new table. Growth is
amortised: the slot table doubles each time, so a shard sees at
most `log₂(N)` growth events over N inserts.

### Group commit

When `FlushPolicy::Group` is configured, `flush()` calls don't
each `fdatasync` independently. Instead:

1. Caller A calls `flush()`. It's elected leader; it starts a
   short rendezvous window.
2. Callers B, C, D call `flush()` during the window; they
   register as followers.
3. The leader issues one `fdatasync` covering all four
   callers' pending writes.
4. All four `flush()` futures resolve together.

The bench `benches/group_commit.rs` measures **8.06× speedup**
on 8 producer threads × 200 writes/thread vs `OnEachFlush`.

---

## Range scans (secondary index)

When `EmdbBuilder::enable_range_scans(true)` is set, the engine
maintains a parallel `crossbeam_skiplist::SkipMap<Vec<u8>, u64>`
per namespace. The SkipMap is:

- **Sorted by key** — supports half-open range queries and
  prefix scans.
- **Lock-free** — inserts, removes, and range iteration are all
  concurrent-safe without any global lock.
- **Pointer-stable** — range iterators take a snapshot of the
  keys at construction, then resolve values through the mmap on
  each `next()`.

### Cost

- One `Vec<u8>` clone of the key per `insert`. For typical keys
  (24–64 bytes) this is ~50–100 bytes of allocator overhead per
  record.
- One SkipMap node per record. SkipMap nodes are heavier than
  hash table slots — empirically ~doubles in-memory index size.

For workloads that need range scans, this is the right
trade-off. For workloads that don't, the opt-out (default-off)
matters — emdb's default open does not pay this tax.

### API

| Method | Returns |
|---|---|
| `range(R)` | eager `Vec<(Vec<u8>, Vec<u8>)>` |
| `range_prefix(p)` | eager `Vec<(Vec<u8>, Vec<u8>)>` |
| `range_iter(R)` | lazy `EmdbRangeIter` |
| `range_prefix_iter(p)` | lazy `EmdbRangeIter` |
| `iter_from(start)` | lazy `EmdbRangeIter` (inclusive) |
| `iter_after(start)` | lazy `EmdbRangeIter` (exclusive) |

The lazy variants take a snapshot of `(key, offset)` pairs from
the SkipMap and decode values lazily on each `next()` — useful
for early-exit consumers that only read the first few records.

---

## TTL

Gated behind the `ttl` feature (on by default). When a record
is inserted with `insert_with_ttl(key, value, ttl)`, the
`expires_at` field in the frame payload is set to `now_ms +
ttl_ms`. The hash index and SkipMap both store the offset; the
expiration check happens at decode time.

### Lazy expiration

Reads check `expires_at` against the current wall clock; expired
records return `None` from `get` and aren't yielded by iterators.
The on-disk record isn't immediately removed; it stays in the
journal until compaction sweeps it.

### Eager expiration

`Emdb::sweep_expired()` walks the index, removes expired
entries, and writes tombstone frames so the expirations survive
restart. The sweep is cooperative — it never blocks readers.

### Why lazy + eager

- **Lazy** gives the right semantic guarantee (expired records
  are never visible to user code) without paying a sweep cost on
  every insert.
- **Eager** gives memory reclamation when the application
  needs it (e.g. a long-running session cache).

The two are independent. Code that wants pure-lazy can leave
`sweep_expired` unscheduled; code that wants pure-eager can call
it periodically (e.g. every minute on a tokio interval).

---

## Encryption

Gated behind the `encrypt` feature. When configured via
`EmdbBuilder::encryption_key([u8; 32])` or
`encryption_passphrase(s)`, every value byte stored in the
journal is encrypted at-rest with the chosen AEAD cipher
(AES-256-GCM by default; ChaCha20-Poly1305 via
`cipher(Cipher::ChaCha20Poly1305)`).

### Cipher

- **AES-256-GCM** — default. Hardware-accelerated on every
  modern CPU (AES-NI on x86, AES instructions on ARM64).
- **ChaCha20-Poly1305** — alternative for platforms without
  AES acceleration (rare in 2026) or when the threat model
  prefers a non-AES primitive.

### Nonce

A 12-byte nonce is generated per record from
`rand_core::OsRng`. The nonce is stored in the frame payload
alongside the ciphertext. Nonce reuse is not possible by
construction — each insert generates a fresh nonce.

### Key derivation (passphrase mode)

`encryption_passphrase(s)` runs the passphrase through Argon2id
with a per-database salt stored in `<path>.meta`. The salt is
generated on first open and persists across reopens. Default
Argon2id parameters are `m_cost=64MiB, t_cost=3, p_cost=4`.

### Key rotation

Three static methods rotate / enable / disable encryption
without rewriting the journal payload itself — they update the
key-wrapping layer in `<path>.meta` and re-encrypt only the
data-encryption key (DEK):

| Method | Effect |
|---|---|
| `Emdb::enable_encryption(path, target)` | Plaintext → encrypted. |
| `Emdb::disable_encryption(path, current)` | Encrypted → plaintext. |
| `Emdb::rotate_encryption_key(path, current, new)` | Rewrap the DEK under a new KEK. |

The DEK never leaves memory and never leaves
`zeroize::Zeroizing` ownership. Argon2id-derived keys and raw
keys are both wrapped the same way.

### Memory zeroing

Raw key material flows through `zeroize::Zeroizing<[u8; 32]>`
wrappers. When the wrapper drops, the underlying bytes are
written with `0x00` before deallocation. This defends against
heap-residue attacks on swap files or compromised process
memory.

---

## Async surface

Gated behind the `async` feature. See [the async section of
API.md](API.md#async-surface) for the user-facing surface; the
implementation strategy is:

- **Every async method calls one `spawn_blocking`.** The
  closure clones the `Arc<Emdb>` (cheap — one atomic increment)
  and moves it onto tokio's blocking pool, where the
  sync call runs to completion. The async caller awaits the
  `JoinHandle`.
- **Streaming methods are two-stage.** The outer
  `spawn_blocking` constructs the sync iterator (surfaces
  errors as a normal `Result::Err`). A second `spawn_blocking`
  task pumps records through a bounded `tokio::sync::mpsc`
  channel (capacity 64). The async caller polls a
  `ReceiverStream`.
- **Backpressure via `blocking_send`.** When the channel is
  full, the pump task's blocking thread suspends until the
  consumer drains a slot. No busy-wait.
- **Drop-aware.** When the consumer drops the stream, the next
  `blocking_send` returns `Err`, the pump task breaks out of
  its `for` loop, the iterator is dropped, the blocking thread
  exits.

The trade-off: every async call pays one `spawn_blocking`
dispatch (~1 µs on a warm pool) plus one ownership transfer for
key + value (`Vec<u8>` clone). For workloads where the sync cost
dominates (journal append, mmap decode, fsync), the spawn is
negligible. For workloads where the sync cost is a single
hash-table probe, the spawn dominates and the sync surface is
the right choice via `AsyncEmdb::sync_handle()`.

---

## Crash recovery

emdb is crash-safe **at the durability boundary fsys provides**.
The rules:

- Records written but not flushed are lost on crash. This is
  the standard contract — `flush` is what makes a record
  durable.
- Records that are flushed survive any crash short of NAND
  corruption. The frame format is CRC-32C protected; partial
  writes are detected and ignored.
- The index is rebuilt on every open by replaying the journal
  from the last checkpoint forward. No on-disk index format
  to corrupt.

### Recovery sequence

1. Acquire the lockfile (`Emdb::open` errors with
   `Error::AlreadyLocked` if held by another process).
2. Load the metadata sidecar (`<path>.meta`) — checkpoint LSN,
   encryption header, schema version.
3. Memory-map the journal file.
4. Walk frames from the checkpoint LSN forward. For each frame:
   - Validate CRC. If CRC fails, stop (truncate-and-recover):
     all bytes past the failure point are discarded.
   - Decode payload.
   - Apply to the index (insert / tombstone / namespace).
5. The database is now consistent up to the last fully-written
   frame.

### Checkpoints

`Emdb::checkpoint()` writes a snapshot of the current namespace
table and key-count to `<path>.meta` and updates the recovery
start LSN. On the next open, recovery resumes from the
checkpoint instead of from the journal start.

Checkpoints are a recovery-speed optimisation, not a durability
guarantee. Calling `checkpoint()` on a fresh database with
millions of records cuts open time from O(journal_size) to
O(post-checkpoint_size).

---

## Compaction

`Emdb::compact()` rewrites the journal in compacted form:

1. Snapshot the live index (every live offset).
2. Open a temporary journal file (`<path>.compact.tmp`).
3. Walk every live offset in arbitrary order; for each, decode
   the frame and append it to the temp journal.
4. fsync the temp journal.
5. Atomically rename the temp journal over `<path>` (POSIX
   rename / Windows `MoveFileExW(REPLACE_EXISTING)`).
6. Re-mmap the new file and rebuild the index from offsets in
   the new file.

Compaction is a **stop-the-world** operation: readers see the
old journal until step 5, then transparently see the new one.
Writers block from step 1 until step 5 (briefly, with the
write-side acquire of the compaction mutex). Read latency is
unaffected — the existing `Arc<Mmap>` keeps serving reads from
the old journal until the swap.

### Why not online compaction

Bitcask-style stores can do online compaction (dual-write to
both journals during the rebuild). emdb's current scheme is
stop-the-world because:

- The space win from compaction is often 50–80 % on real
  workloads. Once a week / once a day on a long-running
  database is enough.
- Online compaction would double the index data structure
  (during the dual-write window) and complicate the recovery
  path (which journal is authoritative if the process dies
  mid-compaction?). The simpler scheme avoids both.

A future release may add online compaction if profiling
indicates the stop-the-world pause is the bottleneck for any
real workload.

---

## Concurrency model

- **`Emdb` is `Send + Sync + Clone`.** Clones share the
  underlying `Arc<Inner>` — pass clones across threads instead
  of sharing one handle through a `Mutex`.
- **Reads scale to the core count.** The 64-shard index and the
  shared `Arc<Mmap>` keep the hot read path lock-free past
  shard-level granularity.
- **Writes don't serialise on a writer mutex.** fsys's LSN
  reservation is a single atomic; concurrent appenders issue
  independent `pwrite`s.
- **Compaction is the only stop-the-world operation.** All
  other operations are concurrent-safe.

The bench `benches/concurrent_reads.rs` measures **9.94 M
reads/sec aggregate at 8 threads on a 4-core consumer box** —
the lock-free read path scales until shared memory bandwidth
becomes the cap.

---

## Failure modes

| Failure | Detected by | Effect |
|---|---|---|
| **Disk full** | fsys's `pwrite` returns `ENOSPC` / `ERROR_DISK_FULL` | `Error::Io`; in-memory state unchanged, journal unchanged. |
| **Disk corruption** | CRC fail on frame decode | Recovery stops at first bad frame; all data after the bad frame is lost. |
| **Process kill mid-write** | First decode on next open hits a torn frame | Torn frame is treated as corruption — recovery stops there; pre-flush records are lost. |
| **Wrong encryption key** | AEAD `decrypt_in_place` fails | `Error::EncryptionError`; no partial reads. |
| **Wrong path / not an emdb file** | Magic mismatch on first frame | `Error::MagicMismatch`. |
| **Version mismatch** | Schema version in metadata sidecar doesn't match | `Error::VersionMismatch`. |
| **Lockfile held by dead process** | `Error::AlreadyLocked` | Use `Emdb::lock_holder` to diagnose; `Emdb::break_lock` if the holder is confirmed dead. |
| **Concurrent open by another process** | OS advisory lock acquisition fails | `Error::AlreadyLocked`. |
| **Out of memory** | Allocator failure | Panics (Rust's default `alloc_error_handler`). |
| **Hash collision** | OVERFLOW state in index; verify-key on decode | Handled transparently; cost is one extra raw-key compare per affected slot. |

The bias is **fail-fast and visible**, not silent recovery. A
corrupted journal will lose data, but it will lose it noisily
(returning errors), not by silently serving stale records.

---

## See also

- [API.md](API.md) — user-facing API reference.
- [BENCH.md](BENCH.md) — benchmark numbers and methodology.
- [PERFORMANCE.md](PERFORMANCE.md) — per-op cost model + tuning.
- [PLATFORM-NOTES.md](PLATFORM-NOTES.md) — OS-specific behaviour.
- [STABILITY-1.0.md](STABILITY-1.0.md) — 1.0 stability contract.
- [fsys-rs](https://github.com/jamesgober/fsys-rs) — storage substrate upstream.
