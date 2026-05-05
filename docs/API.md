# emdb API Reference

Companion to the rustdoc on [docs.rs/emdb](https://docs.rs/emdb).
This file documents every public method on every public type, with
the parameter notes, error semantics, and worked examples that
don't fit inside `///` comments.

The rustdoc is the authoritative type-signature reference. This
document focuses on **how to use the API correctly** — when to
prefer one method over another, what the trade-offs are, and what
the calling patterns look like in practice.

---

## Contents

- [Construction](#construction) — `Emdb::open`, `open_in_memory`, the builder
- [Core key/value operations](#core-keyvalue-operations) — `insert`, `get`, `remove`, …
- [Zero-copy reads](#zero-copy-reads) — `get_zerocopy` + `ValueRef`
- [Bulk insert](#bulk-insert) — `insert_many`
- [Iteration](#iteration) — `iter`, `keys`, `range`, cursor-style `iter_from`
- [Transactions](#transactions) — `transaction`
- [Namespaces](#namespaces) — `namespace`, `Namespace`
- [Nested groups](#nested-groups) — `Focus` (`nested` feature)
- [TTL](#ttl) — per-record expiration (`ttl` feature)
- [Persistence and durability](#persistence-and-durability) — `flush`, `checkpoint`, `FlushPolicy`
- [Operational APIs](#operational-apis) — `stats`, `backup_to`, `compact`, `clone_handle`
- [Lockfile recovery](#lockfile-recovery) — `lock_holder`, `break_lock`
- [Encryption](#encryption) — `encryption_key`, `encryption_passphrase`, key rotation (`encrypt` feature)
- [Types reference](#types-reference) — `FlushPolicy`, `Ttl`, `ValueRef`, `EmdbStats`, `LockHolder`
- [Errors](#errors) — `Error` variants and recovery patterns

---

## Construction

### `Emdb::open(path) -> Result<Emdb>`

Open or create a file-backed database at `path`. Creates the file
if it doesn't exist. Acquires an OS-level advisory lock on
`<path>.lock` to prevent two processes from opening the same file
concurrently.

**Parameters**

- `path` — anything that converts to `&Path`. Typically a
  `&str`, `String`, `PathBuf`, or `&Path`.

**Errors**

- `Error::Io` — filesystem error (permission denied, disk full,
  unreachable parent directory).
- `Error::AlreadyLocked` — another process holds the lock. See
  [Lockfile recovery](#lockfile-recovery) for diagnosis.
- `Error::MagicMismatch` / `Error::VersionMismatch` /
  `Error::Corrupted` — the file exists but is not a valid v0.9
  emdb database.

**Examples**

Open a fresh database:

```rust
use emdb::Emdb;

let path = std::env::temp_dir().join("emdb-doc-open.emdb");
{
    let db = Emdb::open(&path)?;
    db.insert("k", "v")?;
    db.flush()?;
}
let _ = std::fs::remove_file(&path);
let _ = std::fs::remove_file(format!("{}.lock", path.display()));
# Ok::<(), emdb::Error>(())
```

Reopen an existing database:

```rust,no_run
use emdb::Emdb;

let db = Emdb::open("/var/lib/myapp/sessions.emdb")?;
println!("{} records", db.len()?);
# Ok::<(), emdb::Error>(())
```

### `Emdb::open_in_memory() -> Emdb`

Open a non-persistent database that lives entirely in RAM. No
file is created, no lock is taken. Drops cleanly when the last
handle is dropped.

Use cases: unit tests, ephemeral caches, doctests.

```rust
use emdb::Emdb;

let db = Emdb::open_in_memory();
db.insert("name", "emdb")?;
assert_eq!(db.get("name")?, Some(b"emdb".to_vec()));
# Ok::<(), emdb::Error>(())
```

### `Emdb::builder() -> EmdbBuilder`

Returns a builder for opening a database with non-default
configuration. See [`EmdbBuilder`](#emdbbuilder) for the full
list of options.

```rust,no_run
use emdb::{Emdb, FlushPolicy};

let db = Emdb::builder()
    .path("/var/lib/myapp/sessions.emdb")
    .flush_policy(FlushPolicy::Group)
    .enable_range_scans(true)
    .build()?;
# Ok::<(), emdb::Error>(())
```

### `Emdb::clone_handle() -> Emdb`

Cheap clone of the database handle. Both handles share the same
underlying engine and lockfile — there is exactly one open file,
one lock, one in-memory index. Useful for handing the database
to a spawned task without wrapping it in `Arc`.

```rust
use emdb::Emdb;
use std::thread;

let db = Emdb::open_in_memory();
db.insert("k", "main")?;

let db_clone = db.clone_handle();
let h = thread::spawn(move || -> emdb::Result<()> {
    db_clone.insert("k", "worker")?;
    Ok(())
});
h.join().expect("thread panicked")?;

// Both handles see the worker's write.
assert_eq!(db.get("k")?, Some(b"worker".to_vec()));
# Ok::<(), emdb::Error>(())
```

### `Emdb::path() -> &Path`

Returns the on-disk path of this database. For in-memory
databases, returns the synthetic in-memory path used internally.

---

## Core key/value operations

### `insert(key, value) -> Result<()>`

Insert or replace a value. Both `key` and `value` accept anything
that converts into `Vec<u8>`: `&str`, `String`, `&[u8]`, `Vec<u8>`,
arrays, etc.

Behaviour:
- Replaces the previous value if `key` already exists.
- Records the operation to the journal (`fsys::JournalHandle::append`).
- Updates the in-memory hash index.
- Does **not** sync to disk on its own. Call `flush()` when you
  need durability.

```rust
use emdb::Emdb;

let db = Emdb::open_in_memory();

// String keys and values.
db.insert("user:1", "alice")?;

// Bytes.
db.insert(b"user:2", b"bob".to_vec())?;

// Replacement is silent.
db.insert("user:1", "alice-renamed")?;
assert_eq!(db.get("user:1")?, Some(b"alice-renamed".to_vec()));
# Ok::<(), emdb::Error>(())
```

### `get(key) -> Result<Option<Vec<u8>>>`

Fetch a value, returning `Ok(None)` if the key doesn't exist or
has expired (TTL feature). Allocates a fresh `Vec<u8>` for the
returned value.

For zero-copy reads, see [`get_zerocopy`](#get_zerocopykey---resultoptionvalueref).

```rust
use emdb::Emdb;

let db = Emdb::open_in_memory();
db.insert("k", "v")?;

assert_eq!(db.get("k")?, Some(b"v".to_vec()));
assert_eq!(db.get("missing")?, None);
# Ok::<(), emdb::Error>(())
```

### `remove(key) -> Result<Option<Vec<u8>>>`

Delete a key. Returns the prior value if one existed.

```rust
use emdb::Emdb;

let db = Emdb::open_in_memory();
db.insert("k", "v")?;

assert_eq!(db.remove("k")?, Some(b"v".to_vec()));
assert_eq!(db.remove("k")?, None);  // already gone
# Ok::<(), emdb::Error>(())
```

### `contains_key(key) -> Result<bool>`

Does the key exist? Equivalent to `get(key)?.is_some()` but skips
the value decode. For encrypted databases this matters — it
avoids the AEAD decryption path entirely.

```rust
use emdb::Emdb;

let db = Emdb::open_in_memory();
db.insert("k", "v")?;
assert!(db.contains_key("k")?);
# Ok::<(), emdb::Error>(())
```

### `len() -> Result<usize>`

Total live record count across the default namespace.

```rust
use emdb::Emdb;

let db = Emdb::open_in_memory();
db.insert("a", "1")?;
db.insert("b", "2")?;
assert_eq!(db.len()?, 2);
# Ok::<(), emdb::Error>(())
```

### `is_empty() -> Result<bool>`

`len()? == 0`, but cheaper since it short-circuits on the first
shard with any record.

### `clear() -> Result<()>`

Remove every record from the default namespace. Subsequently
`len()` returns `0`.

```rust
use emdb::Emdb;

let db = Emdb::open_in_memory();
db.insert("a", "1")?;
db.insert("b", "2")?;
db.clear()?;
assert_eq!(db.len()?, 0);
# Ok::<(), emdb::Error>(())
```

---

## Zero-copy reads

### `get_zerocopy(key) -> Result<Option<ValueRef>>`

Reads directly from the kernel-managed mmap — no allocation, no
copy. Returns a [`ValueRef`](#valueref) that holds a strong handle
to the mmap region, so the borrow is safe across subsequent writer
activity (file growth, in-place updates).

For encrypted databases, falls back to an owned plaintext buffer
inside the same `ValueRef` type — AEAD decryption necessarily
allocates fresh bytes.

```rust
use emdb::Emdb;

let db = Emdb::open_in_memory();
db.insert("greeting", "hello world")?;

if let Some(v) = db.get_zerocopy("greeting")? {
    // Compare against a byte slice without copying.
    let want: &[u8] = b"hello world";
    assert!(v == want);

    // Safe to keep across writes — kernel keeps the mapping.
    db.insert("other", "data")?;
    assert_eq!(v.as_slice(), b"hello world");
}
# Ok::<(), emdb::Error>(())
```

When to prefer it:
- Hot read paths reading large values where the allocation
  shows up in profiles.
- Read-only consumers passing the bytes onward (network, hash,
  compare) without owning them.
- Any path where the rest of the operation is a single byte
  scan.

When `get` is fine:
- The value will be modified.
- The value is small enough that the allocation is irrelevant.
- Encrypted databases — there's no zero-copy benefit.

---

## Bulk insert

### `insert_many<I, K, V>(items) -> Result<()>`

Insert many records under a single writer pass. Accepts any
iterator of `(K, V)` where both types implement `AsRef<[u8]>`.

The performance benefit comes from amortising the per-record
journal-append overhead: one batched encoder pass rather than N
individual `insert()` calls.

```rust
use emdb::Emdb;

let db = Emdb::open_in_memory();

let records = vec![
    ("user:1", "alice"),
    ("user:2", "bob"),
    ("user:3", "carol"),
];
db.insert_many(records)?;
assert_eq!(db.len()?, 3);
# Ok::<(), emdb::Error>(())
```

Compatible with any `IntoIterator`, including iterator
adaptors:

```rust
use emdb::Emdb;

let db = Emdb::open_in_memory();
db.insert_many((0..1000).map(|i| (format!("k{i}"), format!("v{i}"))))?;
assert_eq!(db.len()?, 1000);
# Ok::<(), emdb::Error>(())
```

---

## Iteration

emdb's primary index is hash-keyed. Iteration order is
*unspecified* unless [range scans](#range-iteration-opt-in) are
enabled.

### `iter() -> Result<EmdbIter>`

Streaming iterator over `(key, value)` pairs in unspecified
order. Captures a snapshot of offsets at construction time —
records inserted after `iter()` returns may or may not appear,
records removed after `iter()` returns may still appear (with
their pre-removal value).

Each `next()` decodes one record on demand, so memory use scales
with the offset count rather than total value size.

```rust
use emdb::Emdb;

let db = Emdb::open_in_memory();
db.insert("a", "1")?;
db.insert("b", "2")?;

let mut count = 0;
for (key, value) in db.iter()? {
    println!("{} = {}", String::from_utf8_lossy(&key), String::from_utf8_lossy(&value));
    count += 1;
}
assert_eq!(count, 2);
# Ok::<(), emdb::Error>(())
```

### `keys() -> Result<EmdbKeyIter>`

Streaming iterator over keys only. Doesn't decode values — the
right choice when you want the key set and don't care about
values.

```rust
use emdb::Emdb;

let db = Emdb::open_in_memory();
db.insert("a", "1")?;
db.insert("b", "2")?;

let keys: Vec<Vec<u8>> = db.keys()?.collect();
assert_eq!(keys.len(), 2);
# Ok::<(), emdb::Error>(())
```

### Range iteration (opt-in)

Range methods require `EmdbBuilder::enable_range_scans(true)` at
open time. They are backed by a parallel `BTreeMap` per
namespace; see the README for the memory-cost trade-off.

#### `range<R>(range) -> Result<Vec<(Vec<u8>, Vec<u8>)>>`

Eagerly collects every `(key, value)` pair in the given range.
For large ranges, prefer `range_iter` to stream.

```rust
use emdb::Emdb;

let db = Emdb::builder().enable_range_scans(true).build()?;
db.insert("a", "1")?;
db.insert("b", "2")?;
db.insert("c", "3")?;

// Inclusive..exclusive.
let pairs: Vec<_> = db.range(b"a"..b"c")?.into_iter().collect();
assert_eq!(pairs.len(), 2);
# Ok::<(), emdb::Error>(())
```

Range bounds accept any [`RangeBounds<&[u8]>`][rb] — half-open,
inclusive, full, or unbounded.

[rb]: https://doc.rust-lang.org/std/ops/trait.RangeBounds.html

#### `range_iter<R>(range) -> Result<EmdbRangeIter>`

Streaming variant. Same semantics as `range`, but yields lazily.

```rust,no_run
use emdb::Emdb;

let db = Emdb::builder().enable_range_scans(true).build()?;

// Process the first N matching records and exit early.
for (k, v) in db.range_iter(b"prefix-".as_slice()..b"prefix-z".as_slice())?.take(100) {
    let _ = (k, v);
}
# Ok::<(), emdb::Error>(())
```

#### `range_prefix(prefix) -> Result<Vec<(Vec<u8>, Vec<u8>)>>`

Convenience wrapper for "every key starting with `prefix`".

```rust
use emdb::Emdb;

let db = Emdb::builder().enable_range_scans(true).build()?;
db.insert("user:1", "alice")?;
db.insert("user:2", "bob")?;
db.insert("admin:1", "carol")?;

let users = db.range_prefix("user:")?;
assert_eq!(users.len(), 2);
# Ok::<(), emdb::Error>(())
```

#### `range_prefix_iter(prefix) -> Result<EmdbRangeIter>`

Streaming version of `range_prefix`.

#### `iter_from(start) -> Result<EmdbRangeIter>`

Iterate from `start` (inclusive) to the end of the keyspace.
The cursor primitive for paginated APIs.

#### `iter_after(start) -> Result<EmdbRangeIter>`

Iterate from `start` (exclusive). The "next page" form: pass
the last key from the previous page.

```rust
use emdb::Emdb;

let db = Emdb::builder().enable_range_scans(true).build()?;
for i in 0..50 {
    db.insert(format!("rec-{i:03}"), "v")?;
}

// Walk the entire keyspace ten records at a time.
let mut cursor: Option<Vec<u8>> = None;
let page_size = 10;
let mut pages = 0;
loop {
    let iter = match cursor.as_deref() {
        Some(c) => db.iter_after(c)?,
        None => db.iter_from("")?,
    };
    let page: Vec<Vec<u8>> = iter.take(page_size).map(|(k, _)| k).collect();
    if page.is_empty() {
        break;
    }
    cursor = page.last().cloned();
    pages += 1;
}
assert_eq!(pages, 5);
# Ok::<(), emdb::Error>(())
```

---

## Transactions

### `transaction<F, T>(f) -> Result<T>`

Run a closure with exclusive write access. Inside the closure,
inserts and removes are staged in a [`Transaction`](#transaction)
and applied atomically when the closure returns `Ok`. If the
closure returns `Err`, the staged changes are discarded.

emdb transactions are write-only and serialise with one another;
they do not provide MVCC snapshots for readers. Other readers
see the prior state until the transaction commits, then the
post-commit state.

```rust
use emdb::Emdb;

let db = Emdb::open_in_memory();
db.insert("balance:alice", b"100".to_vec())?;
db.insert("balance:bob", b"50".to_vec())?;

db.transaction(|tx| {
    let alice: u64 = std::str::from_utf8(&tx.get("balance:alice")?.unwrap())
        .unwrap_or("0")
        .parse()
        .unwrap_or(0);
    let bob: u64 = std::str::from_utf8(&tx.get("balance:bob")?.unwrap())
        .unwrap_or("0")
        .parse()
        .unwrap_or(0);

    tx.insert("balance:alice", (alice - 30).to_string())?;
    tx.insert("balance:bob", (bob + 30).to_string())?;
    Ok::<(), emdb::Error>(())
})?;

assert_eq!(db.get("balance:alice")?, Some(b"70".to_vec()));
assert_eq!(db.get("balance:bob")?, Some(b"80".to_vec()));
# Ok::<(), emdb::Error>(())
```

### `Transaction` methods

Inside the closure, `tx` is a `&mut Transaction` with these
methods:

- `insert(key, value)` — stage an insert.
- `insert_with_ttl(key, value, ttl)` — stage an insert with
  TTL.
- `remove(key)` — stage a removal. Returns the staged-or-
  underlying prior value.
- `get(key)` — read the live database, taking staged writes
  into account.
- `contains_key(key)` — same, key-only.

The transaction is dropped on return: there is no explicit
`commit()` — returning `Ok` from the closure commits, returning
`Err` rolls back.

---

## Namespaces

A namespace is an isolated keyspace inside the same database file.
Records in different namespaces share no keys, no iteration
order, and no TTL state. The default namespace is the one
addressed by `db.insert(...)` etc.; named namespaces sit alongside
it.

### `namespace(name) -> Result<Namespace>`

Open or create a named namespace. The returned handle is cheap
to clone and shares the underlying engine.

```rust
use emdb::Emdb;

let db = Emdb::open_in_memory();
let users = db.namespace("users")?;
let sessions = db.namespace("sessions")?;

users.insert("u1", "alice")?;
sessions.insert("u1", "abc-123")?;

// Different namespaces, different values for the same key.
assert_eq!(users.get("u1")?, Some(b"alice".to_vec()));
assert_eq!(sessions.get("u1")?, Some(b"abc-123".to_vec()));
# Ok::<(), emdb::Error>(())
```

### `drop_namespace(name) -> Result<bool>`

Remove every record in the named namespace. Returns `Ok(true)`
if the namespace existed and was dropped, `Ok(false)` if it
didn't exist.

### `list_namespaces() -> Result<Vec<String>>`

Return the names of every named namespace (excludes the default
namespace).

### `Namespace` methods

A `Namespace` exposes the same surface as `Emdb` for KV
operations: `insert`, `insert_many`, `get`, `get_zerocopy`,
`remove`, `contains_key`, `len`, `is_empty`, `clear`, `iter`,
`keys`, `range`, `range_iter`, `range_prefix`, `range_prefix_iter`,
`iter_from`, `iter_after`. Plus:

- `name() -> &str` — the namespace name.

There is **no** namespace-scoped `flush()`, `checkpoint()`,
`stats()`, `compact()`, etc. — those are file-wide operations
on the parent `Emdb`. Call them on the parent handle.

---

## Nested groups

*Requires the `nested` feature.*

The nested feature adds dotted-prefix views over a flat keyspace.
A `Focus` is a cheap wrapper that prepends a prefix to every key
operation, letting you treat `"users.alice.name"` and
`"users.alice.email"` as a logical "alice" group.

### `db.focus(prefix) -> Focus<'_>`

Get a focus on the given dotted prefix. Cheap — no I/O.

```rust,no_run
# #[cfg(feature = "nested")]
# {
use emdb::Emdb;

let db = Emdb::open_in_memory();
let alice = db.focus("users.alice");

alice.set("name", "Alice")?;
alice.set("email", "alice@example.com")?;

// Equivalent to:
// db.insert("users.alice.name", "Alice")?
// db.insert("users.alice.email", "alice@example.com")?
# }
# Ok::<(), emdb::Error>(())
```

### `db.group(prefix) / db.delete_group(prefix)`

Materialised views: `group` returns every record under the
prefix, `delete_group` removes them.

### `Focus` methods

- `set(key, value)` / `get(key)` / `remove(key)` /
  `contains_key(key)` — basic operations scoped to the prefix.
- `set_with_ttl(key, value, ttl)` — TTL variant.
- `iter()` / `delete_all()` — scoped operations.
- `focus(sub)` — drill in further (e.g. `db.focus("users").focus("alice")`).

---

## TTL

*Requires the `ttl` feature (default-enabled).*

### `insert_with_ttl(key, value, ttl) -> Result<()>`

Insert a record with a per-record expiration. The `ttl` argument
is a [`Ttl`](#ttl-1) enum:

- `Ttl::None` — never expires (override the builder's default).
- `Ttl::Default` — use the builder's `default_ttl`.
- `Ttl::Duration(Duration)` — expire after the given duration.
- `Ttl::ExpiresAt(u64)` — expire at the given Unix-millis timestamp.

```rust,no_run
# #[cfg(feature = "ttl")]
# {
use std::time::Duration;
use emdb::{Emdb, Ttl};

let db = Emdb::open_in_memory();

// Specific duration.
db.insert_with_ttl("session", "abc", Ttl::Duration(Duration::from_secs(60)))?;

// Use the builder default — no default set on this in-memory DB,
// so this is equivalent to Ttl::None.
db.insert_with_ttl("permanent", "data", Ttl::Default)?;
# }
# Ok::<(), emdb::Error>(())
```

### `expires_at(key) -> Result<Option<u64>>`

The Unix-millis expiration timestamp for a key, or `None` if the
key doesn't exist or has no expiration.

### `ttl(key) -> Result<Option<Duration>>`

Time-remaining-until-expiration for a key. Returns `None` if the
key doesn't exist or has no TTL. Returns `Some(Duration::ZERO)`
if the record has already expired but not yet been swept.

### `persist(key) -> Result<bool>`

Remove the TTL on a key, making it permanent. Returns `Ok(true)`
if the key exists, `Ok(false)` otherwise.

### `sweep_expired() -> usize`

Manually remove expired records. Returns the number of records
removed. emdb does not run a background sweeper — call this on a
schedule or after batch operations to reclaim space.

```rust,no_run
# #[cfg(feature = "ttl")]
# {
use std::time::Duration;
use emdb::{Emdb, Ttl};

let db = Emdb::open_in_memory();
db.insert_with_ttl("k", "v", Ttl::Duration(Duration::from_millis(1)))?;

std::thread::sleep(Duration::from_millis(10));
let evicted = db.sweep_expired();
assert_eq!(evicted, 1);
# }
# Ok::<(), emdb::Error>(())
```

---

## Persistence and durability

### `flush() -> Result<()>`

Make all writes since the last flush durable. Routes through
`fsys::JournalHandle::sync_through(next_lsn)`. Under
`FlushPolicy::Group`, concurrent flushers share a single fsync.

```rust,no_run
use emdb::Emdb;

let path = std::env::temp_dir().join("emdb-flush-example.emdb");
let db = Emdb::open(&path)?;
db.insert("k", "v")?;
db.flush()?;  // durable on disk
# let _ = std::fs::remove_file(&path);
# let _ = std::fs::remove_file(format!("{}.lock", path.display()));
# Ok::<(), emdb::Error>(())
```

### `checkpoint() -> Result<()>`

Update the sidecar metadata file so the next reopen can fast-skip
records already validated. Without a checkpoint, reopen scans the
entire journal end-to-end. With one, reopen starts from the last
checkpointed LSN.

Idiom:
- Call `flush()` after every important write (durability).
- Call `checkpoint()` periodically at quiescent points (every N
  writes, every M seconds, on graceful shutdown).

```rust,no_run
use emdb::Emdb;

let db = Emdb::open("data.emdb")?;
for i in 0..1000 {
    db.insert(format!("k{i}"), "v")?;
}
db.flush()?;
db.checkpoint()?;  // future reopens skip the 1000 records above
# Ok::<(), emdb::Error>(())
```

---

## Operational APIs

### `stats() -> Result<EmdbStats>`

Point-in-time database introspection. Cheap — `O(namespaces)`
plus one `metadata` syscall. Safe for per-second polling.

See [`EmdbStats`](#emdbstats) for the field list.

```rust
use emdb::Emdb;

let db = Emdb::open_in_memory();
db.insert("k", "v")?;

let stats = db.stats()?;
assert_eq!(stats.live_records, 1);
println!("logical size: {} bytes", stats.logical_size_bytes);
# Ok::<(), emdb::Error>(())
```

### `backup_to(target) -> Result<()>`

Atomic snapshot to a sibling file. Writes to `<target>.backup.tmp`,
fsyncs, renames into place. Failure at any step leaves `target`
untouched.

The result is a normal openable database — no proprietary dump
format.

```rust,no_run
use emdb::Emdb;

let db = Emdb::open("/var/lib/myapp/sessions.emdb")?;

let backup_path = format!(
    "/backups/sessions-{}.emdb",
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0),
);
db.backup_to(&backup_path)?;
# Ok::<(), emdb::Error>(())
```

Heavy operation. Wrap in `tokio::task::spawn_blocking` from async
contexts. Refuses self-target (target == source).

### `compact() -> Result<()>`

Rewrite the database file with only live records, reclaiming
space from removed and replaced entries. Atomic via the same
temp-file + rename pattern as `backup_to`.

```rust,no_run
use emdb::Emdb;

let db = Emdb::open("/var/lib/myapp/sessions.emdb")?;
println!("before: {}", db.stats()?.file_size_bytes);
db.compact()?;
println!("after:  {}", db.stats()?.file_size_bytes);
# Ok::<(), emdb::Error>(())
```

### `clone_handle() -> Emdb`

See [Construction](#construction). Cheap clone, shared engine.

### `path() -> &Path`

See [Construction](#construction). On-disk path.

---

## Lockfile recovery

When a process holding the database lock dies without releasing
(SIGKILL, panic during destructor, OOM kill), the lockfile
remains and subsequent `Emdb::open` calls fail with
`Error::AlreadyLocked`.

### `Emdb::lock_holder(path) -> Result<Option<LockHolder>>`

Inspect who holds the lock. Returns `Ok(None)` if the database
is unlocked.

```rust,no_run
use emdb::Emdb;

if let Some(holder) = Emdb::lock_holder("data.emdb")? {
    println!(
        "locked by pid {} since {} ms (crate {})",
        holder.pid, holder.acquired_at_ms, holder.crate_version,
    );
} else {
    println!("unlocked");
}
# Ok::<(), emdb::Error>(())
```

### `Emdb::break_lock(path) -> Result<()>`

Forcibly remove the lockfile. **Only call this after confirming
via OS tooling that the holder PID is dead** (`ps -p`,
`Get-Process -Id`, container inspection). emdb cannot perform
this check portably; getting it wrong corrupts the database when
two processes write concurrently.

```rust,no_run
use emdb::Emdb;

let path = "data.emdb";
if let Some(holder) = Emdb::lock_holder(path)? {
    // Verify the PID is dead via OS-specific means here.
    // Then:
    Emdb::break_lock(path)?;
}
let db = Emdb::open(path)?;
# let _ = db;
# Ok::<(), emdb::Error>(())
```

---

## Encryption

*Requires the `encrypt` feature.*

emdb supports at-rest encryption via AES-256-GCM (default) or
ChaCha20-Poly1305. Keys can be supplied raw (32 bytes) or derived
from a passphrase via Argon2id.

### `EmdbBuilder::encryption_key(key)`

Use a raw 32-byte key. The caller is responsible for key
management.

```rust,no_run
# #[cfg(feature = "encrypt")]
# {
use emdb::Emdb;

let key = [0u8; 32];  // load from your key store
let db = Emdb::builder()
    .path("/var/lib/myapp/encrypted.emdb")
    .encryption_key(key)
    .build()?;
# }
# Ok::<(), emdb::Error>(())
```

### `EmdbBuilder::encryption_passphrase(passphrase)`

Derive a key from a passphrase via Argon2id. The salt is stored
in the metadata sidecar; the passphrase is **never** stored.

```rust,no_run
# #[cfg(feature = "encrypt")]
# {
use emdb::Emdb;

let db = Emdb::builder()
    .path("/var/lib/myapp/encrypted.emdb")
    .encryption_passphrase("correct horse battery staple")
    .build()?;
# }
# Ok::<(), emdb::Error>(())
```

### `EmdbBuilder::cipher(cipher)`

Select between `Cipher::Aes256Gcm` (default) and
`Cipher::ChaCha20Poly1305`.

### `Emdb::enable_encryption(path, target)` (static)

Encrypt an existing plaintext database. Reads, re-encrypts, and
atomically replaces the file. Operates offline — the database
must not be open in any process.

### `Emdb::disable_encryption(path, current)` (static)

Decrypt an existing encrypted database to plaintext. Same
offline requirement.

### `Emdb::rotate_encryption_key(path, current, new)` (static)

Rotate the encryption key. Decrypts with `current`, re-encrypts
with `new`. Atomic via temp-file + rename.

```rust,no_run
# #[cfg(feature = "encrypt")]
# {
use emdb::{Emdb, EncryptionInput};

Emdb::rotate_encryption_key(
    "data.emdb",
    EncryptionInput::Passphrase("old passphrase".into()),
    EncryptionInput::Passphrase("new passphrase".into()),
)?;
# }
# Ok::<(), emdb::Error>(())
```

---

## EmdbBuilder

The builder is reached via `Emdb::builder()`. Every method
returns `self` so calls chain.

| Method | Purpose |
|---|---|
| `path(p)` | Explicit on-disk path. |
| `app_name(s)` | OS-aware path resolution: app name. |
| `database_name(s)` | OS-aware path resolution: database file name. |
| `data_root(p)` | Override the OS-aware data root. |
| `default_ttl(d)` | Default TTL for records inserted via plain `insert`. |
| `enable_range_scans(b)` | Maintain a parallel `BTreeMap` for range queries. Memory cost. |
| `flush_policy(p)` | `OnEachFlush` (default), `Group`, or `WriteThrough`. |
| `encryption_key(k)` | Raw 32-byte key (`encrypt` feature). |
| `encryption_passphrase(s)` | Argon2id-derived key (`encrypt` feature). |
| `cipher(c)` | AES-GCM (default) or ChaCha20-Poly1305 (`encrypt` feature). |
| `build()` | Open or create the database. |

Path resolution rules:
- If `path()` was set, use it.
- Else if `app_name()` and `database_name()` were both set,
  resolve to the OS-aware data dir (XDG on Linux,
  Application Support on macOS, %LOCALAPPDATA% on Windows).
- Else error: `Error::InvalidConfig`.

---

## Types reference

### `FlushPolicy`

```rust,ignore
pub enum FlushPolicy {
    OnEachFlush,
    Group,
    WriteThrough,
}
```

- `OnEachFlush` *(default)* — one fsync per `db.flush()` call.
  Right choice for single-thread workloads or when durability
  is already batched at the application layer.
- `Group` — concurrent `flush()` calls coalesce into one fsync
  via fsys's group-commit coordinator. Right for N-producer
  workloads with per-record durability.
- `WriteThrough` — open the file with platform-native
  synchronous-write flags (`FILE_FLAG_WRITE_THROUGH` on Windows,
  `O_SYNC` on Linux/BSD). Every `pwrite` is durable on return;
  `flush()` is near-free. Right when `OnEachFlush`'s
  `FlushFileBuffers` cost dominates the workload.

### `Ttl`

```rust,ignore
pub enum Ttl {
    None,
    Default,
    Duration(Duration),
    ExpiresAt(u64),
}
```

Per-record expiration, used by `insert_with_ttl`. `Ttl::Default`
delegates to the builder's `default_ttl`.

### `ValueRef`

The return type of `get_zerocopy`. Holds a strong handle to the
mmap region so the borrow is safe across writer activity.

```rust,ignore
impl ValueRef {
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    pub fn as_slice(&self) -> &[u8];
    pub fn into_vec(self) -> Vec<u8>;
}
```

`Deref<Target = [u8]>` and `PartialEq<[u8]>` impls are
available for transparent comparison.

### `EmdbStats`

```rust,ignore
#[non_exhaustive]
#[derive(Copy, Clone, Debug)]
pub struct EmdbStats {
    pub live_records: usize,
    pub namespace_count: usize,
    pub logical_size_bytes: u64,
    pub file_size_bytes: u64,
    pub preallocated_bytes: u64,
    pub range_scans_enabled: bool,
    pub encrypted: bool,
}
```

Returned by `Emdb::stats()`. `#[non_exhaustive]` so future fields
won't break exhaustive matches.

### `LockHolder`

```rust,ignore
#[non_exhaustive]
pub struct LockHolder {
    pub pid: u32,
    pub acquired_at_ms: u64,
    pub schema_version: u32,
    pub crate_version: String,
}
```

Returned by `Emdb::lock_holder()`. Use to diagnose stale locks
before calling `break_lock`.

### `Cipher` (`encrypt` feature)

```rust,ignore
#[non_exhaustive]
pub enum Cipher {
    Aes256Gcm,
    ChaCha20Poly1305,
}
```

### `EncryptionInput` (`encrypt` feature)

```rust,ignore
pub enum EncryptionInput {
    RawKey([u8; 32]),
    Passphrase(String),
}
```

Used by the static encryption-admin methods.

---

## Errors

`emdb::Error` is a non-exhaustive enum. The variants you need to
handle in production code:

| Variant | When | Recovery |
|---|---|---|
| `Io(io::Error)` | Filesystem error (disk full, permission denied, etc.). | Inspect the inner `io::Error`. |
| `AlreadyLocked` | `Emdb::open` saw a held lock. | See [Lockfile recovery](#lockfile-recovery). |
| `MagicMismatch` | File at the path is not an emdb v0.9 database. | Confirm the path. v0.7/v0.8 files are not compatible with v0.9. |
| `VersionMismatch { found, expected }` | File version doesn't match this emdb release. | Migrate via the previous emdb release. |
| `Corrupted { offset, reason }` | Frame validation or CRC mismatch. | Restore from backup. fsys's frame format is CRC-32C protected; this fires on hardware-level corruption or external tampering. |
| `InvalidConfig(reason)` | Builder configuration was inconsistent. | Fix the builder call site. |
| `KeyNotFound` | A method that requires the key (e.g., `expires_at` on a missing key) didn't find it. Most "lookup" APIs return `Ok(None)` instead. | Application logic. |
| `EncryptionError(reason)` | AEAD decryption failed (wrong key, tampered ciphertext). | Verify the key. |

`Result<T>` is `std::result::Result<T, emdb::Error>`.

```rust
use emdb::{Emdb, Error};

fn open_or_recover(path: &str) -> emdb::Result<Emdb> {
    match Emdb::open(path) {
        Ok(db) => Ok(db),
        Err(Error::AlreadyLocked) => {
            // Diagnose; potentially break_lock; retry.
            Err(Error::AlreadyLocked)
        }
        Err(e) => Err(e),
    }
}
```

---

## See also

- [README](../README.md) — quick-start and architecture.
- [BENCH.md](BENCH.md) — benchmark numbers and methodology.
- [CHANGELOG.md](../CHANGELOG.md) — release history.
- [docs.rs/emdb](https://docs.rs/emdb) — generated rustdoc.
