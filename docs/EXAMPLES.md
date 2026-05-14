# emdb Examples Guide

When-to-use guide for emdb's APIs. The
[`examples/`](../examples) directory has the runnable code; this
doc covers **why you would reach for each pattern**.

For the full API reference, see [API.md](API.md). For the
internals that make these patterns work, see
[ARCHITECTURE.md](ARCHITECTURE.md).

---

## Contents

- [Picking the right open method](#picking-the-right-open-method)
- [Choosing a flush policy](#choosing-a-flush-policy)
- [Eager vs streaming iteration](#eager-vs-streaming-iteration)
- [Sync vs async](#sync-vs-async)
- [Single namespace vs many namespaces vs nested groups](#single-namespace-vs-many-namespaces-vs-nested-groups)
- [TTL vs explicit removal](#ttl-vs-explicit-removal)
- [Raw key vs passphrase encryption](#raw-key-vs-passphrase-encryption)
- [`insert_many` vs `transaction` vs loop](#insert_many-vs-transaction-vs-loop)
- [`get` vs `get_zerocopy`](#get-vs-get_zerocopy)
- [`compact` and `checkpoint` cadence](#compact-and-checkpoint-cadence)

---

## Picking the right open method

| You want | Use |
|---|---|
| A simple file-backed database | `Emdb::open(path)` |
| A throwaway in-process database for tests / caches | `Emdb::open_in_memory()` |
| Any tuning beyond the defaults | `Emdb::builder()`...`.build()` |
| OS-aware path resolution (XDG / AppData / Application Support) | `Emdb::builder().app_name(...).database_name(...).build()` |
| Async access from a tokio runtime | `Emdb::builder()...build_async()` or `AsyncEmdb::open(path)` |

If the builder feels heavy for a quick prototype, `Emdb::open`
is fine — every default is chosen to be reasonable.

---

## Choosing a flush policy

`FlushPolicy` controls when emdb forces data to disk.

| Policy | When | Trade-off |
|---|---|---|
| `OnEachFlush` *(default)* | Single writer; or batching done at app layer | Simplest. Every `flush()` is one fsync. |
| `Group` | Many producers each calling `flush()` per record | Coalesces fsyncs across producers. 8×+ throughput at high concurrency. |
| `WriteThrough` | Every record must be durable on insert; no explicit `flush` calls | Highest per-record latency floor. No `flush()` needed. |

**Rule of thumb:**
- One writer thread → `OnEachFlush`.
- N writer threads each calling `flush()` → `Group`.
- Records must be durable before `insert` returns → `WriteThrough`.

If unsure, leave the default. `Group` only helps when there
are concurrent flush callers.

---

## Eager vs streaming iteration

Applies to both sync and async APIs (the async surface adds
`*_stream` variants in 0.9.7).

| Result size | Use |
|---|---|
| Small + bounded (≤ 1 K records) | Eager: `iter()`, `keys()`, `range()`, etc. Returns a `Vec`. |
| Large or unbounded | Streaming sync: `range_iter()`, `range_prefix_iter()`, lazy `iter()`. Lazy iterator. |
| Large or unbounded (async) | Streaming async: `iter_stream()`, `range_stream()`, etc. Bounded mpsc + `ReceiverStream`. |

The async streaming variants matter most when records are
forwarded to a downstream (socket, queue, another store) — the
consumer drives the rate, and emdb's blocking pump task
respects backpressure via `blocking_send`.

---

## Sync vs async

`async` is a feature, not the default. Use it when:

- The application is async-first (tokio runtime, network I/O).
- You want to interleave emdb I/O with other async work
  without stalling the runtime's task scheduler.

Don't use it when:

- The application is sync (no tokio runtime).
- The hot path is many `get`s on hot keys — the
  `spawn_blocking` overhead (~1 µs) dominates the actual
  operation (~30 ns). Use the sync surface via
  `AsyncEmdb::sync_handle()` for these.

Once `async` is in, the `*_stream` methods are the right tool
for any iteration where the result might be large.

---

## Single namespace vs many namespaces vs nested groups

Three ways to organise keys:

### One namespace, prefixed keys

```rust,ignore
db.insert("user:001", ...)?;
db.insert("session:abc", ...)?;
```

Simplest. Works fine for small numbers of "logical types".
Range scans with `enable_range_scans(true)` make
`range_prefix("user:")` cheap.

### Multiple namespaces

```rust,ignore
let users    = db.namespace("users")?;
let sessions = db.namespace("sessions")?;

users.insert("001", ...)?;
sessions.insert("abc", ...)?;
```

Use when:
- Each "type" has its own lifecycle (clear, compact, backup
  one without touching the others).
- The application benefits from `Namespace`-typed handles in
  function signatures.

Each namespace gets its own hash index + (optionally) its own
SkipMap. There's a small memory cost per namespace.

### Nested groups (`nested` feature)

```rust,ignore
let users = db.focus("user");
users.insert("001", ...)?;       // stored as "user.001"

let sessions = db.focus("session");
sessions.insert("abc", ...)?;    // stored as "session.abc"
```

Use when:
- You want namespace-like scoping but everything lives in the
  default namespace's index (one index to maintain).
- You want dotted-path APIs (`db.group("a.b.c")`,
  `db.delete_group("a.b")`).

The `Focus` handle is a cheap wrapper — no separate index, no
separate handle state. Pure key-prefixing convenience.

---

## TTL vs explicit removal

`ttl` is a default-on feature; per-record TTL adds a 8-byte
`expires_at` field to the frame payload.

| Pattern | Use |
|---|---|
| Records expire after a fixed duration | `default_ttl(d)` + plain `insert` |
| Some records expire, some don't | `insert_with_ttl(k, v, Ttl::After(d))` + plain `insert` for permanent |
| TTL extension on access | `insert_with_ttl(...)` on the read path |
| Application controls removal explicitly | No TTL; call `db.remove(key)` directly |

Lazy expiry happens automatically on read. Eager expiry
(`db.sweep_expired()`) reclaims memory + on-disk space; call it
on a timer if expired records accumulate.

---

## Raw key vs passphrase encryption

`encrypt` feature.

| Input | Use | Notes |
|---|---|---|
| 32-byte high-entropy key (from KMS, derived externally) | `EmdbBuilder::encryption_key([u8; 32])` | Fastest; no KDF. |
| User-typed passphrase | `EmdbBuilder::encryption_passphrase("...")` | Argon2id KDF derives a 32-byte key. Slow on first open (memory-hard); fast on subsequent reads. |

Argon2id parameters are sized for cold-open latency of ~1 s on a
2024-era CPU. Per-database salts are stored in `<path>.meta`
and generated on first encryption.

---

## `insert_many` vs `transaction` vs loop

Three patterns for writing N records at once.

### `insert_many(iter)`

Best for: known-up-front batches where there's nothing else to
do between records.

- One LSN reservation, one `pwrite`, one fsync.
- Strictly faster than the equivalent insert loop.

### `transaction(|tx| { ... })`

Best for: writes that must be atomic — either all succeed or
none are observable.

- Buffers all writes in memory until the closure returns.
- One LSN reservation + one `pwrite` at commit time.
- Roll back by returning `Err` from the closure.

### Loop

Best for: writes that need other logic between them (read +
decide + write patterns).

- N LSN reservations + N `pwrite`s.
- Use `FlushPolicy::Group` if multiple threads are doing this
  concurrently.

**Rule of thumb:**
- Atomic batch → `transaction`.
- Non-atomic batch → `insert_many`.
- Per-record decision logic → loop.

---

## `get` vs `get_zerocopy`

| API | Returns | Allocations |
|---|---|---|
| `get(key)` | `Option<Vec<u8>>` | One `Vec<u8>` per call |
| `get_zerocopy(key)` | `Option<ValueRef<'_>>` | None — borrows from the mmap |

`get_zerocopy` is roughly 2× faster on small values (≤ 256 B)
where the `Vec` alloc dominates. For larger values, the two
converge.

The `ValueRef<'_>` lifetime is tied to the mmap; you cannot
hold it across operations that swap the mmap (compaction,
growth). If you need to hold the value beyond the call frame,
use `get` or `value_ref.to_vec()`.

---

## `compact` and `checkpoint` cadence

Both are operational APIs.

### `checkpoint()`

What it does: writes a recovery-start LSN snapshot to
`<path>.meta`. On next open, recovery starts from the
checkpoint instead of the journal start — proportional to time
saved by skipping pre-checkpoint frames.

When to call:
- After a large bulk-load (so the next open doesn't replay it).
- Before a long-running idle period (so a crash during idle
  doesn't pay the replay cost).
- On a slow timer (every few minutes) for long-uptime
  processes.

Cost: a few `pwrite`s + one fsync of the metadata sidecar.
Negligible.

### `compact()`

What it does: rewrites the journal in compacted form, dropping
tombstoned and overwritten records. Atomically renames the new
journal over the old.

When to call:
- When journal size grows substantially larger than live-data
  size (3×+ is a reasonable trigger).
- During scheduled maintenance windows.
- Never during a latency-critical period — compaction is
  stop-the-world for writers (briefly).

Cost: proportional to live-data size, not journal size. Writers
block briefly at the rename; readers see no interruption.

For long-running services, a daily or weekly compaction is
usually enough.

---

## See also

- [API.md](API.md) — full API reference.
- [ARCHITECTURE.md](ARCHITECTURE.md) — engine internals.
- [PERFORMANCE.md](PERFORMANCE.md) — per-op cost model.
- [`examples/`](../examples) — runnable example programs.
