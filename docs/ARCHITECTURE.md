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
                │   - epoch-published Mmap for reads  │
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
| `<path>.lock` | OS-level advisory lockfile (one process at a time). Created owner-only; left in place on close. |
| `<path>.meta` | Atomically-replaced 112-byte metadata sidecar: format version, feature flags, creation time, encryption salt and key-verification block, CRC-32 (IEEE). |

There is no header inside the journal: byte 0 is the magic of the
first frame. A non-empty file that does not start with the frame
magic is refused with `Error::MagicMismatch` and left untouched.

Other files that can appear next to the database:

| File | Purpose |
|---|---|
| `<path>.compact.tmp` | Compaction's rewrite target; renamed over `<path>` on success, removed on failure and on the next open. |
| `<target>.backup.tmp`, `<target>.backup.tmp.meta` | `backup_to`'s rewrite target and sidecar before they are renamed over `<target>` and `<target>.meta`. |
| `<path>.enc.tmp`, `<path>.encadmin` | Encryption admin rewrite target and its swap marker (see [Encryption admin](#encryption-admin)). |
| `<path>.encbak`, `<path>.encbak.meta` | The database as it was before the last encryption admin operation. |
| `<path>.corrupt-<offset>` | Written by fsys when an open cuts a torn journal tail: the cut bytes, kept for forensics. Safe to delete once you have checked the database. |

The journal is **never modified in place** during normal
operation. The only writers are:

1. **Append**: `fsys::JournalHandle::append` reserves an LSN,
   writes the new frame at the journal tail, optionally fsyncs.
   `remove`, `clear` and `drop_namespace` append too (tombstones).
2. **Compaction**: writes a fresh journal under a temp name,
   atomically renames it over the original.
3. **Open**: fsys cuts a torn tail left by a crash (see
   [Crash recovery](#crash-recovery)).

Reads are always from a memory-mapped view of the live journal
file, published through an epoch cell (`storage/arc_cell.rs`):
point reads borrow it without touching a reference count, and
iterators and `ValueRef`s take an `Arc` of the mapping they read.

### Frame format

Each frame is a self-describing record. The layout is owned by
fsys, but emdb's record types embed inside the payload:

```
┌──────────────────┬───────────────┬───────────┬──────────┐
│ magic+ver (4)    │ payload_len(4)│ payload   │ crc32c(4)│
└──────────────────┴───────────────┴───────────┴──────────┘
```

12-byte overhead per frame (magic+ver = `0x46535901`, big-endian
on disk so a hexdump shows `46 53 59 01`). The trailing CRC-32C
covers `magic_and_ver || length || payload`. fsys's decoder
validates magic + length + CRC before handing the payload to
emdb's decoder. A frame's payload is capped at 256 MiB - 1 bytes;
an insert whose encoded record (key, value and about 21 bytes of
record header, plus 28 bytes of nonce and tag when encrypted) is
larger fails with `Error::Io` of kind `InvalidInput`.

### Record payload

emdb encodes one of three payload types (constants live in
[`src/storage/format.rs`](src/storage/format.rs)):

| Tag | Constant | Type | Contents |
|---|---|---|---|
| `0x00` | `TAG_INSERT` | Insert | `(ns_id, key, value, expires_at)` (`expires_at = 0` means no TTL) |
| `0x01` | `TAG_REMOVE` | Remove (tombstone) | `(ns_id, key)` |
| `0x02` | `TAG_NAMESPACE_NAME` | Namespace metadata | `(ns_id, name)` |

The high bit of the tag byte (`TAG_ENCRYPTED_FLAG = 0x80`) is set
on AEAD-encrypted records; bits 0..6 carry the kind. So an
encrypted Insert is `0x80`, an encrypted Remove is `0x81`, etc.

`ns_id` is a 4-byte namespace identifier. The default namespace
has `ns_id = 0`; named namespaces are assigned dense IDs in the
order they're first created.

### Field encoding

All integers are little-endian and fixed-width. Bodies are:

- Insert: `[ns_id u32][key_len u32][key][value_len u32][value][expires_at u64]`
- Remove: `[ns_id u32][key_len u32][key]`
- NamespaceName: `[ns_id u32][name_len u32][name]`

Decoding is strict: the fields must account for the whole body (no
trailing bytes), and all offset arithmetic is overflow-checked. A
record that breaks either rule is `Error::Corrupted`. Recovery also
requires every Insert/Remove to target the default namespace or an
id bound earlier in the log by a NamespaceName record, and rejects a
NamespaceName that binds id 0, id `u32::MAX`, an empty name, or an id
already bound to a different name. emdb's own writers always satisfy
these rules.

---

## Storage substrate (fsys)

emdb opens its journal through `fsys::JournalHandle`:

```rust,ignore
let fs = fsys::builder()
    .tune_for(fsys::Workload::Database)
    .build()?;
let opts = fsys::JournalOptions::new()
    .write_lifetime_hint(Some(fsys::WriteLifetimeHint::Long));
let journal = fs.journal_with(path, opts)?;
```

`tune_for(Workload::Database)` sets:

- 8 MiB resident buffer pool — enough headroom that small bursts
  don't allocate.
- 256-deep io_uring submission ring (Linux) — keeps the kernel
  fed without saturating it.
- 4 K-deep batch queue — vectored `append_batch` can submit up
  to 4,096 records in one syscall.

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

The hot data structure. One per namespace. Never persisted: it is
rebuilt by the recovery scan on every open.

### Sharded open addressing

The index is a **64-shard open-addressed hash table** of
seqlock-protected slots:

```
Index
└── shards[0..64]: Shard
    ├── table: Atomic<Table>        ← epoch-published; readers load it lock-free
    │   └── slots: Box<[AtomicSlot]> (linear probing)
    ├── writer: Mutex<counts>       ← one writer per shard at a time
    ├── live: AtomicUsize           ← exact live-entry count (len())
    └── overflow: RwLock<HashMap<u64, Vec<(key, offset)>>>  ← 64-bit collisions

shard = hash & 63
home  = (hash >> 6) & (capacity - 1)
```

```
AtomicSlot {
    seq:    AtomicU64,  // seqlock counter: even = stable, odd = writing
    state:  AtomicU8,   // EMPTY | OCCUPIED | TOMBSTONE | OVERFLOW
    hash:   AtomicU64,  // full 64-bit key hash
    offset: AtomicU64,  // byte offset of the record's payload
}
```

The home slot uses the bits above the shard selector. (Before
1.0.3 it used the raw hash, whose low 6 bits are the same for
every key in a shard, so only 1/64 of each table's slots could be
a home slot.) A table starts at 16 slots per shard (32 KiB per
namespace) and is rebuilt before an insert would take
`occupied + tombstones` past 75% of capacity: doubled when more
than half of it would be live, otherwise rehashed at the same size
to purge tombstones.

### The hash function (1.0.3)

`KeyHasher` is a folded-multiply hash (the `foldhash`/`ahash`
fallback construction): 16-byte blocks are XORed with
per-instance secrets and multiplied as 64x64->128 bits, and the
halves of each product are folded together. The secrets are drawn
from the OS-seeded `RandomState` every time a database is opened.

Without the secrets the high half of each product is
unpredictable, so colliding key sets cannot be computed offline.
The pre-1.0.3 mixer was unkeyed and algebraically invertible: an
attacker could build 20,000 keys with one hash in milliseconds and
drive every insert through the quadratic overflow path (reopen
time went from 13 ms to 1.9 s). The keyed hash is not a MAC; it
only makes placement unpredictable.

### Reads (seqlock)

```rust,ignore
loop {
    let s0 = slot.seq.load(Acquire);
    if s0 & 1 == 1 { continue; }       // writer active, retry

    let state  = slot.state.load(Relaxed);
    let hash   = slot.hash.load(Relaxed);
    let offset = slot.offset.load(Relaxed);

    fence(Acquire);                     // pairs with the writer's fence(Release)
    let s1 = slot.seq.load(Relaxed);
    if s0 == s1 {
        return (state, hash, offset);
    }
}
```

A lookup pins the `crossbeam-epoch` epoch (a thread-local
operation), loads the shard's table pointer and probes. It takes
no lock and writes no shared cache line.

### Writes (one writer per shard)

```rust,ignore
let seq = slot.seq.load(Relaxed);
slot.seq.store(seq + 1, Relaxed);       // odd: writing
fence(Release);
slot.state.store(state, Relaxed);
slot.hash.store(hash, Relaxed);
slot.offset.store(offset, Relaxed);
slot.seq.store(seq + 2, Release);       // even: stable
```

Every mutation of a shard (insert, overwrite, remove, overflow
promotion, rebuild, clear) holds that shard's writer mutex for the
duration of the slot update only (no I/O). With one writer per
shard there is no claim-then-verify race: before 1.0.3, two
concurrent inserts of one key racing a removal earlier in its
probe chain could leave the key in two slots, and a later
`remove` resurrected the stale copy.

Both fences are required. Without them, a reader on a
weak-memory CPU (ARM, POWER) can combine field values from before
and after an update and still see matching `seq` values. The
1.0.2 reader used a `compiler_fence`, which constrains only the
compiler. The protocol is checked by a `loom` model in
`src/storage/index.rs` (`RUSTFLAGS="--cfg loom" cargo test
--release --lib loom_`), which fails if either fence is removed,
and by Miri.

### Growth without stopping readers

A rebuild allocates the new table, copies the live entries, and
publishes it with one atomic pointer swap. Readers that already
loaded the old table finish their probe on it (no writer touches
it any more) and the old table is freed by the epoch collector
once they have all unpinned. Readers never wait for a rebuild.

### Overflow handling

When two distinct keys share a 64-bit hash, both entries move into
the shard's overflow map and the primary slot is marked
`STATE_OVERFLOW`. The map is written (under the writer mutex) before
the slot flips, and entries are deduplicated by key, so a reader
sees either the old single entry or both. Lookups of an overflow
hash take the map's read lock; nothing else touches it. With the
keyed hash this path is effectively never taken.

---

## The read path

```
Emdb::get(key)
 ├─ ns = default namespace (borrowed, no lock) or map lookup (named)
 ├─ guard = crossbeam_epoch::pin()                (thread-local)
 ├─ hash  = KeyHasher::hash(key)
 ├─ Index::get(hash, key) → Option<u64>           (offset)
 │   - load shard table pointer, probe with seqlock reads
 │   - OCCUPIED with matching hash: return offset
 │   - OVERFLOW: read-lock the overflow map, compare raw keys
 │   - EMPTY: return None; TOMBSTONE: keep probing
 └─ decode the record at offset
     - borrow the journal mapping under the guard (no Arc clone)
     - decode payload, require Insert + same namespace + same key
     - if encryption is enabled, decrypt
     - if expires_at has passed, return None (lazy expiry)
     - return Some(value.to_vec())
```

Two things to note:

1. **The mmap is shared across all readers.** The current mapping
   is published through an atomic pointer; readers borrow it under
   an epoch guard without touching its reference count. A mapping
   replaced by a remap or a compaction swap is released once every
   reader that could still see it has unpinned, and all mappings
   are released synchronously when the store closes.
2. **The verify-key step on decode is what defends against hash
   collisions.** If two keys collide and the index returns the
   wrong offset, decode will see a key mismatch and return
   `None`. The OVERFLOW path is the explicit handler, but the
   verify-key step is a belt-and-suspenders guarantee.

### Zero-copy reads (`get_zerocopy`)

```rust,ignore
Emdb::get_zerocopy(key) → Option<ValueRef>
```

`ValueRef` points directly into the mapping: no allocation, no
copy. It holds a strong reference to the mapping it was read from
(taken from the borrowed mapping when the `ValueRef` is built), so
it stays valid after a remap or a compaction swap replaces the
store's current mapping.

This is the fastest read path in the library. On a 24-byte key,
150-byte value workload, `get_zerocopy` is roughly 2× faster
than `get` because it skips the `Vec<u8>` allocation.

---

## The write path

```
Emdb::insert(key, value)
 ├─ hash = KeyHasher::hash(key)
 ├─ lock the key's write stripe (1 of 1024, chosen by namespace + hash)
 ├─ encode payload (Insert frame); encrypt if enabled
 ├─ JournalHandle::append(payload) → offset
 │   - fsys reserves the LSN with one atomic fetch_add
 │   - pwrite the frame at the reserved byte range
 │   - Windows only: reservation + write run under an append-order mutex
 ├─ Index::replace(hash, key, offset)               (shard writer mutex)
 ├─ if range_index enabled: SkipMap::insert(key, offset)
 └─ release the stripe
```

### Per-key write stripes

Every write to a key (insert, remove, batch, transaction commit,
TTL sweep, `persist`) holds the key's stripe from before the
journal append until the hash index and the range index are
updated. Consequences:

- **Writes to one key are linearizable,** and they reach the log in
  the same order as they reach memory. Before 1.0.3 the append and
  the index update were separate steps, so two concurrent writes to
  one key could be logged in one order and applied in the other:
  up to 279 of 400 contended keys reopened with a different value
  than the process had been serving, and two concurrent `remove`s
  could both return `Some`.
- **The range index agrees with the hash index** for every key once
  the write returns.
- **Batches** (`insert_many`, transaction commit) lock the stripes
  of all their keys in ascending stripe order, so batches cannot
  deadlock with each other or with single-key writes.

Writes to different keys almost always hold different stripes and
run in parallel. Readers never take stripes.

Lock order, outermost first: stripes (ascending) → index shard
writer mutex → store append-order mutex (Windows) → mmap refresh
lock. No code path acquires them in another order.

### Windows: ordered appends

On NTFS, a write that starts beyond the end of valid data makes the
file system zero-fill the gap below it synchronously, and the write
that later fills the gap pays again. Lock-free LSN reservation lets
concurrent appenders finish their writes out of order, so every
inverted pair takes this path. Measured with plain `seek_write`, one
thread writing 72-byte records in pairs, second first, drops from
~635 K to ~10 K writes/s whether or not the file is mapped; two
emdb writer threads collapsed from ~410 K to ~22 K inserts/s. On
Windows the store therefore holds a mutex around the reservation
and the write (not the fsync), which keeps file extension in order:
2 writer threads now sustain ~370 K inserts/s. Linux and macOS keep
the lock-free append.

### Group commit

In v1.x every `flush()` call goes through fsys's group-commit
coordinator, regardless of `FlushPolicy`. The mechanic:

1. Caller A calls `flush()`. fsys elects it leader for the
   coalesce window around the in-flight sync syscall.
2. Callers B, C, D call `flush()` during the window; they
   register as followers behind the leader's sync.
3. The leader issues one `fdatasync` (or platform equivalent —
   `FlushFileBuffers` on Windows, `F_FULLFSYNC` on macOS)
   covering all four callers' pending writes.
4. All four `flush()` calls return together.

`FlushPolicy::OnEachFlush` (default) and `FlushPolicy::Group`
are functionally identical here — both route through the same
coordinator. `FlushPolicy::Group` is retained only for source
compatibility with v0.8.x callers; new code should write
`OnEachFlush`. The bench `benches/group_commit.rs` exists to
detect future regressions in the coalescer's behaviour.

---

## Range scans (secondary index)

When `EmdbBuilder::enable_range_scans(true)` is set, the engine
maintains a parallel `crossbeam_skiplist::SkipMap<Vec<u8>, u64>`
per namespace. The SkipMap is:

- **Sorted by key** — supports half-open range queries and
  prefix scans.
- **Lock-free** — inserts, removes, and range iteration are all
  concurrent-safe without any global lock.
- **Consistent with the hash index**: it is updated under the
  same per-key write stripe.
- **Cursor-iterated**: range iterators keep only the last key
  yielded and re-seek the skiplist for each page, then resolve
  values through the mmap on each `next()`.

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

The lazy variants are cursors: each refill seeks just past the last
key yielded and takes a page of `(key, offset)` pairs (16 at
first, doubling to 512); values are decoded on each `next()`.
`iter_from(..).take(10)` over 1 M keys costs about 2 us (1.0.2
copied the whole range first: 50-120 ms). Iteration is weakly
consistent: ascending order, each key at most once, and keys
changed ahead of the cursor may or may not be observed.

---

## TTL

Gated behind the `ttl` feature (on by default). When a record
is inserted with `insert_with_ttl(key, value, ttl)`, the
`expires_at` field in the frame payload is set to `now_ms +
ttl_ms`. The hash index and SkipMap both store the offset; the
expiration check happens at decode time.

### Lazy expiration

Reads check `expires_at` against the current wall clock. Every
read path treats an expired record as absent: `get`,
`get_zerocopy`, `contains_key` (default and named namespaces),
`iter`, `keys`, `range*`, `group`. `persist` refuses to clear the
TTL of an expired record, so it cannot bring one back. `len()`
still counts expired records until they are swept. The on-disk
record stays in the journal until compaction.

### Eager expiration

`Emdb::sweep_expired()` walks the index decoding keys and expiry
times only, then removes each expired entry with a
compare-and-remove: the entry is removed (and a tombstone frame
written, so the expiry survives restart) only if the key still
points at the record the sweep saw. A key re-inserted while the
sweep runs keeps its new value. The sweep never blocks readers.

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
`encryption_passphrase(s)`, the body of every record in the journal
(key, value, TTL, namespace id, namespace name) is encrypted at rest
with the chosen AEAD cipher (AES-256-GCM by default;
ChaCha20-Poly1305 via `cipher(Cipher::ChaCha20Poly1305)`).

### Threat model

The target is an adversary who obtains a copy of the database file
(stolen disk, leaked backup, container image) and never observes the
running process.

What the encryption layer provides:

- **Confidentiality** of record bodies under a 256-bit key.
- **Per-record authenticity.** Each record body carries its own
  128-bit AEAD tag. A database opened with a key rejects any record
  without the encrypted flag, and a record whose tag fails after the
  key was verified is reported as `Error::Corrupted`.

What it does not provide:

- **Log integrity.** Records are authenticated one at a time, not as
  a sequence. Someone who can write the file can delete frames,
  reorder them, replay an older record (rolling a key back to a
  previous value), truncate the log, or restore an older copy of the
  whole file. None of this is detected.
- **Authentication of the record kind.** The tag byte (insert /
  remove / namespace name) sits outside the AEAD ciphertext in the
  1.0 format. Since 1.0.3, strict body-length decoding and the
  namespace-binding checks in recovery reject every kind swap but
  one narrow case: a remove of a key whose bytes equal the name of
  its own namespace can be relabelled as a repeat of that
  namespace binding, which drops the remove. Binding the tag, a
  database identifier and the record position into the AEAD
  associated data needs a format revision (planned for 1.1).
- **Metadata privacy.** Record sizes, record count, record order and
  kind, the meta sidecar fields (flags, cipher choice, creation
  time, Argon2 salt), and the PID, start time and crate version of
  the lock holder in `<path>.lock-meta` are stored in the clear.
- **Protection of process memory.** Decrypted values, mmap pages and
  in-flight writes are outside the scope of the storage layer.

### Cipher

- **AES-256-GCM** — default. Hardware-accelerated on every
  modern CPU (AES-NI on x86, AES instructions on ARM64).
- **ChaCha20-Poly1305** — alternative for platforms without
  AES acceleration (rare in 2026) or when the threat model
  prefers a non-AES primitive.

### Nonce

A 12-byte nonce is drawn from the OS RNG (`rand_core::OsRng`) for
every record and stored in front of the ciphertext. Random nonces
make reuse improbable, not impossible: the chance of any collision
among `n` nonces under one key is about `n^2 / 2^97`. NIST SP
800-38D limits random-nonce AES-GCM to 2^32 encryptions per key.
Every insert, remove, namespace creation, compaction rewrite and
backup counts, so rotate the key with `Emdb::rotate_encryption_key`
well before 2^32 total writes. An RNG failure is reported as
`Error::Encryption` rather than a panic.

### Key derivation (passphrase mode)

`encryption_passphrase(s)` runs the passphrase through Argon2id
(version 0x13) with a 16-byte per-database salt stored in
`<path>.meta`. The salt is generated on first open and persists
across reopens. The parameters are `m_cost = 19 MiB (19_456 KiB),
t_cost = 2, p_cost = 1`, 32-byte output. They are fixed in code and
not recorded in the sidecar, so they cannot be raised for an
existing database yet; a passphrase is only as strong as its
entropy. Prefer a random 32-byte key from a KMS or secret store.

### Key verification

`<path>.meta` holds a 60-byte verification block at offsets 48..108
(nonce, encrypted 32-byte magic, tag) and the salt at 32..48. On
open the block is decrypted; a failure is
`Error::EncryptionKeyMismatch` before any record is read. A keyed
open initialises encryption only for a database with no records:
a keyed open of an existing plaintext database is refused with
`Error::InvalidConfig` (convert it with `Emdb::enable_encryption`),
and a plain open of a database with encryption metadata is refused
the same way.

### Key rotation

There is no data-encryption-key / key-encryption-key split: the
supplied (or derived) key encrypts every record directly. The three
offline admin methods therefore rewrite every record into a new
file and swap it in:

| Method | Effect |
|---|---|
| `Emdb::enable_encryption(path, target)` | Plaintext → encrypted. |
| `Emdb::disable_encryption(path, current)` | Encrypted → plaintext. |
| `Emdb::rotate_encryption_key(path, current, new)` | Re-encrypt every record under a new key. |

The previous file is kept as `<path>.encbak` (plaintext after
`enable_encryption`, old-key ciphertext after a rotation); delete it
once the new file is verified.

### Memory zeroing

Raw keys and derived keys are held in `zeroize::Zeroizing<[u8; 32]>`
and passphrases in `Zeroizing<String>`, so the copies emdb holds are
wiped on drop. The expanded cipher state (AES key schedule,
GHASH/POLYVAL key, ChaCha20 and Poly1305 state) and the Argon2
working memory are built with the `zeroize` features of their crates
and wiped on drop as well. Plaintext record bodies are encoded into,
and decrypted into, scratch buffers that are wiped before they are
freed. Values returned to the caller, and the keys, passphrases and
`EncryptionInput` values the caller holds, are the caller to wipe.

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
- Records that are flushed survive any crash short of media
  corruption. The frame format is CRC-32C protected; a torn
  final frame is detected and cut.
- The index is rebuilt on every open by replaying the whole
  journal. There is no on-disk index to corrupt and no
  checkpoint that shortens the replay.

### Recovery sequence

1. Acquire the lockfile (`Emdb::open` errors with
   `Error::LockBusy` if another process holds it).
2. Finish an encryption admin swap a crash interrupted, if its
   marker is present.
3. Create a missing data file (empty, owner-only), check that the
   data file is empty or starts with the frame magic
   (`Error::MagicMismatch` otherwise), and load the
   metadata sidecar. A missing sidecar is recreated, but only
   after step 5 succeeds, so a failed open writes nothing.
4. Verify the encryption key against the sidecar's verification
   block (encrypted databases).
5. Walk every frame from offset 0, applying each payload to the
   index (insert / tombstone / namespace binding). When the walk
   stops before the end of the file, classify what follows:
   - **Nothing valid follows** (a torn final frame or trailing
     garbage): accepted.
   - **Only unwritten space precedes a valid frame** (zero bytes,
     or a partly written frame whose CRC field or a whole 512-byte
     sector is still zero): accepted. Concurrent appends write at
     reserved offsets, so a crash can leave such a gap in front of
     a frame another thread finished; no successful `flush` ever
     covered anything at or after the gap.
   - **Anything else followed by valid frames** (a bit flip in a
     complete frame, foreign bytes): the open fails with
     `Error::Corrupted { offset, .. }` and **nothing is modified**.
     Earlier releases cut the file here and silently dropped every
     record after the damage.
6. Hand the file to fsys, which cuts an accepted torn tail. A tail
   that is not all zero is first copied to
   `<path>.corrupt-<offset>`.
7. Write the sidecar if it was new, then (with the `ttl` feature)
   tombstone default-namespace records that had already expired.

The walk in step 5 also checks every record it applies. An insert or
remove must name the default namespace or an id that a namespace
record bound earlier; a namespace record must use an id other than 0
and `u32::MAX`, must not give a bound id a second name, and an empty
name (the record `drop_namespace` writes) must name a bound id. In an
encrypted database every record must carry the encrypted flag and
authenticate. A record that breaks a rule fails the open with
`Error::Corrupted`.

### Recovering from `Error::Corrupted`

The offset in the error is where the damage starts; every record
before it is intact. To keep those records and give up everything
from the damage on (the loss earlier releases imposed silently):

1. Copy the database file and its `.meta` sidecar somewhere safe.
2. Truncate the file at the reported offset, for example
   `truncate -s <offset> db.emdb` on Linux/macOS, or
   `fsutil file seteof db.emdb <offset>` on Windows.
3. Open the database again.

Restoring from a `backup_to` copy is the alternative when the
records after the damage matter. A builder option that performs
the truncation (after saving the cut bytes) is planned for 1.1.

### Checkpoints

`Emdb::checkpoint()` syncs the journal (like `flush`) and rewrites
`<path>.meta`. The sidecar holds no recovery position, so a
checkpoint does not shorten the next open; earlier documentation
said otherwise. It fails when the journal is poisoned by an
earlier write or sync failure.

---

## Compaction

`Emdb::compact()` rewrites the journal in compacted form. Every
writer (insert, remove, `insert_many`, transactions, namespace
creation) holds an engine-level write gate in shared mode for the
duration of its append and index update; compaction takes the
gate exclusively for the whole run:

1. Take the write gate exclusively (writers take it shared inside
   the per-key stripe lock helpers, before the stripes, so a write
   never holds it twice). Remove any stale
   `<path>.compact.tmp` and create it fresh (`create_new`).
2. Snapshot the live offsets of every namespace.
3. Copy each live record verbatim (encrypted records stay
   encrypted) into the temporary journal, in batches of about
   4 MiB read with positioned reads, and build each namespace's
   new index off to the side as the batches land.
4. fsync the temporary journal and map it.
5. Rename it over `<path>` (POSIX `rename` / Windows
   `MoveFileExW(REPLACE_EXISTING)`), then fsync the directory.
   If the rename fails, nothing has changed: the database keeps
   using the original file and the temporary is removed.
6. Install the new journal handle, read handle and mapping, and
   swap in the new namespace runtimes (indexes and skiplists), all
   between the two increments of the file generation counter. The
   journal handle that wrote the temporary becomes the live
   journal, so later writes land in the new file.

Readers are not blocked. A point read that raced the swap (old
index offset, new file, or the reverse) is detected through the
generation counter (a seqlock: odd while a swap runs) and retried.
`iter`/`keys` iterators pin the mapping they were created from and
keep yielding that snapshot; `ValueRef`s likewise stay valid.
Range iterators pin the mapping of each page they read; the next
page notices the new generation and re-attaches to the namespace's
new skiplist after the last key yielded, so a range iterator is
weakly consistent across a compaction, as it is across writes.
Peak memory is bounded by the batch size plus one offset per live
record, not by the database size.

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
- **Reads scale with cores.** A default-namespace `get` takes no
  lock and writes no shared cache line (epoch pin, seqlock probe,
  borrowed mapping). Named namespaces add one read-lock and one
  `Arc` clone for the namespace lookup.
- **Writes to one key are linearizable** (per-key stripes); writes
  to different keys run in parallel, except that Windows serialises
  the journal append itself.
- **Batches are not transactions.** `insert_many` and
  `transaction` lock their keys' stripes for the commit, but readers
  can see a commit partly applied and there is no read isolation.
- **Compaction stops writers, not readers.** `clear`,
  `drop_namespace` and the snapshot phase of `backup_to` also take
  the write gate exclusively; every other write holds it shared, and
  reads never take it.

Measured aggregate `get` throughput, default namespace, 100 K keys
(`tests/perf_probe`-style loop, release build, 1/4/8 threads):
Linux 16.8 M / 72.7 M / 131.7 M per second (1.0.2: 10.5 M / 10.9 M /
9.9 M); Windows 9.2 M / 56.4 M / 118.5 M (1.0.2: 8.1 M / 14.5 M /
15.4 M).

---

## Failure modes

| Failure | Detected by | Effect |
|---|---|---|
| **Disk full** | fsys's `pwrite` returns `ENOSPC` / `ERROR_DISK_FULL` | `Error::Io`; in-memory state unchanged, journal unchanged. |
| **Write or sync failure** | fsys returns the I/O error | `Error::Io` with the OS error kind; the journal is poisoned, so later writes, `flush` and `checkpoint` fail until the database is reopened. |
| **Disk corruption** | CRC fail on frame decode, valid frames after it | Open refused with `Error::Corrupted`; the file is not modified. See [Recovering from `Error::Corrupted`](#recovering-from-errorcorrupted). |
| **Process kill mid-write** | Torn final frame (or unwritten gap) on next open | Cut by the open; records that were not flushed may be lost, flushed ones are not. |
| **Wrong encryption key** | Verification block fails to decrypt | `Error::EncryptionKeyMismatch`; no partial reads, nothing written. |
| **Encrypted record modified** | AEAD check of the record fails after the key verified | `Error::Corrupted`. |
| **Plaintext record in an encrypted database** | Tag byte lacks the encrypted flag | `Error::Corrupted`. |
| **Wrong path / not an emdb file** | First bytes are not the frame magic | `Error::MagicMismatch`; the file is not modified. |
| **Version mismatch** | Sidecar format version doesn't match | `Error::VersionMismatch`. |
| **Lockfile held by dead process** | `Error::LockBusy` | Use `Emdb::lock_holder` to diagnose; `Emdb::break_lock` if the holder is confirmed dead. |
| **Concurrent open by another process** | OS advisory lock acquisition fails | `Error::LockBusy`. |
| **Out of memory** | Allocator failure | Panics (Rust's default `alloc_error_handler`). |
| **Hash collision** | OVERFLOW state in index; verify-key on decode | Handled transparently; cost is one extra raw-key compare per affected slot. |

The bias is **fail-fast and visible**, not silent recovery. A
corrupted journal is refused with an error that names the offset,
instead of being silently shortened.

---

## Encryption admin

`enable_encryption`, `disable_encryption` and
`rotate_encryption_key` rewrite the whole database while holding
its lock:

1. Copy every live, unexpired record (with its expiry) into
   `<path>.enc.tmp` under the destination key; sync it.
2. Keep the original as `<path>.encbak` / `<path>.encbak.meta`
   (hard links, or synced copies where hard links are not
   available).
3. Write the swap marker `<path>.encadmin` (synced), rename the
   new sidecar and journal over `<path>.meta` and `<path>`, sync
   the directory, remove the marker.

`<path>` never goes missing. If a crash interrupts step 3, the
next open (or admin call) finds the marker and completes the
renames from the synced temporaries. An open that finds `<path>`
missing or empty next to a non-empty `<path>.encbak` (the state an
interrupted emdb 1.0.2 rotation could leave) is refused rather than
creating an empty database.

**`<path>.encbak` is a plaintext copy after `enable_encryption`
and an old-key copy after a rotation.** Delete it once the new
database is verified.

---

## See also

- [API.md](API.md) — user-facing API reference.
- [BENCH.md](BENCH.md) — benchmark numbers and methodology.
- [PERFORMANCE.md](PERFORMANCE.md) — per-op cost model + tuning.
- [PLATFORM-NOTES.md](PLATFORM-NOTES.md) — OS-specific behaviour.
- [STABILITY-1.0.md](STABILITY-1.0.md) — 1.0 stability contract.
- [fsys-rs](https://github.com/jamesgober/fsys-rs) — storage substrate upstream.
