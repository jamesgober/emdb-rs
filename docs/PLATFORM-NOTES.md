# emdb Platform Notes

OS-specific behaviour. Most of this is fsys's substrate showing
through — emdb itself is platform-neutral, but the durability,
I/O, and locking primitives differ enough across platforms to
warrant explicit notes.

The short version: **emdb runs the same on every supported
platform; the substrate adapts under the hood**. Most callers
should not need any of this.

---

## Contents

- [Supported platforms](#supported-platforms)
- [Linux](#linux)
- [Windows](#windows)
- [macOS](#macos)
- [WSL2](#wsl2)
- [Cross-platform paths](#cross-platform-paths)
- [Cross-platform locking](#cross-platform-locking)
- [Cross-platform durability](#cross-platform-durability)

---

## Supported platforms

emdb's CI runs on:

- **Linux** (Ubuntu 22.04 + 24.04, x86_64) — primary target.
- **Windows** (Windows 11 Pro, x86_64) — primary target.
- **macOS** (macOS 14, ARM64) — best-effort; same Rust code as
  Linux except where noted.

Other targets (FreeBSD, OpenBSD, illumos, Linux on ARM64) are
not in CI but should compile and pass tests — they go through
the same `cfg(unix)` path as Linux. File bugs on encounter.

MSRV is **Rust 1.75**. See
[STABILITY-1.0.md](STABILITY-1.0.md) for the MSRV-bump policy.

---

## Linux

The fastest platform for emdb. fsys's substrate is most mature
here.

### io_uring

When the kernel supports io_uring (Linux 5.1+), fsys uses it for
journal `pwrite` submission. The user-facing effects:

- Submissions don't traverse the syscall path — they're memory
  writes into shared submission rings.
- Vectored `append_batch` (used by `insert_many`, transactions,
  compaction) submits the entire batch as one ring entry.
- Optional kernel-side polling via
  `EmdbBuilder::iouring_sqpoll(idle_ms)` removes the
  per-submission `io_uring_enter` syscall entirely; the kernel
  thread polls the submission ring. Trade-off: the kernel
  thread costs a core's worth of CPU while polling. Default:
  off.

io_uring is automatic — you don't need to ask for it. Older
kernels (< 5.1) fall back to `pwrite` / `pwritev`.

### NVMe passthrough flush

On NVMe devices, fsys can issue `nvme_passthrough` commands for
`fsync` instead of going through the block layer's
`blkdev_fsync`. The passthrough path skips the generic-block
fence and goes straight to NVMe's `FLUSH` opcode — measurable
latency win on `flush()` calls. Automatic when supported.

### `RWF_DSYNC` per-write durability

`FlushPolicy::WriteThrough` maps to `RWF_DSYNC` on each
`pwrite`. The kernel waits for the data to be durable before
returning from `pwrite`; no separate `fsync` call is needed.
This is the lowest-overhead per-record durability mode on
Linux.

### `posix_fadvise(POSIX_FADV_DONTNEED)` on compaction

Compaction rewrites the journal. After the rewrite, fsys advises
the kernel to drop the old journal's pages from the page cache —
they're about to be replaced via rename. Keeps the working set
honest after a compaction.

### `WriteLifetimeHint`

fsys passes `RWH_WRITE_LIFE_LONG` to the kernel for journal
writes. Modern NVMe firmware uses this hint to group long-lived
data into stable NAND blocks, reducing garbage-collection
churn over the SSD's lifetime.

---

## Windows

The Windows path uses `WriteFile` / `WriteFileGather` and
`FlushFileBuffers`. fsys handles the platform differences;
emdb's behaviour is the same.

### `FILE_FLAG_WRITE_THROUGH`

`FlushPolicy::WriteThrough` opens the journal with
`FILE_FLAG_WRITE_THROUGH`. Every `WriteFile` is durable on
return — no separate `FlushFileBuffers` needed. Functionally
equivalent to Linux's `RWF_DSYNC`.

### `MoveFileExW(REPLACE_EXISTING)`

Compaction and metadata sidecar updates use
`MoveFileExW(MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)`
for the atomic rename. On Windows, the destination must not
have any open handles; emdb manages its own handles so this
is internal.

### Lockfile semantics

Windows lockfiles use `LockFileEx` with
`LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY` on a
zero-byte region of `<path>.lock`. The lock is released when
the lockfile handle closes (process exit or explicit close).

If the process dies abruptly without closing the handle, Windows
releases the lock at handle cleanup time (usually within a few
seconds). emdb's `lock_holder` + `break_lock` APIs cover the
case where you need to recover faster.

### No io_uring; no NVMe passthrough

Windows has its own asynchronous I/O primitives
(`ReadFile`/`WriteFile` with `OVERLAPPED` + `GetQueuedCompletionStatus`)
but fsys uses synchronous `WriteFile` / `WriteFileGather` plus
`FlushFileBuffers` on Windows. The latency floor on consumer
NVMe is the SSD itself, not the OS API — overlapping I/O on
Windows would not measurably improve emdb's hot path.

The benchmark numbers in the README are captured on Windows 11
NVMe and reflect this.

### Path quirks

- Long paths (> 260 chars) require `\\?\` prefix or
  Windows 10 + opt-in long-path support. fsys does not
  auto-prefix; if your `Emdb::open(path)` path is long, ensure
  the path is correctly formatted up-stream.
- Backslashes vs forward slashes: both work; Rust's `Path`
  normalises.

---

## macOS

macOS uses a Linux-compatible POSIX subset plus a few
macOS-specific calls.

### `F_FULLFSYNC` durability

macOS's standard `fsync` does **not** guarantee that data has
reached non-volatile storage — it only flushes the OS write
cache to the device. The device may still have data in its
volatile cache.

For genuine durability, fsys uses `F_FULLFSYNC`, which requires
the device to flush its cache too. This is roughly 10×–100×
slower than plain `fsync` on macOS, but it's the only way to
get the durability semantics other platforms give by default.

emdb's `FlushPolicy::OnEachFlush` and `FlushPolicy::Group` both
use `F_FULLFSYNC` on macOS. `FlushPolicy::WriteThrough` maps to
opening with `O_SYNC` plus `F_FULLFSYNC` per write.

### No io_uring

macOS has no io_uring equivalent. fsys's macOS path uses
`pwrite` + `F_FULLFSYNC`. The lack of io_uring is the main
reason macOS benchmarks slower than Linux at equal hardware on
high-concurrency write workloads.

### Lockfile semantics

macOS uses `flock(2)` advisory locking on `<path>.lock`, same
as Linux. The lock is released when the lockfile fd closes.

### File system caveats

emdb runs on APFS (default macOS file system) and HFS+ (legacy).
APFS atomically renames files in O(1) regardless of size, which
helps compaction speed.

If the database lives on an SMB / NFS share, durability and
locking become network-protocol-dependent. Don't put emdb
databases on network shares — they're designed for local
storage.

---

## WSL2

Windows Subsystem for Linux v2 runs a real Linux kernel in a
lightweight VM. emdb on WSL2 has Linux semantics, with one
caveat:

### Cross-filesystem performance

If you mount the Windows host filesystem under `/mnt/c` and run
emdb against `/mnt/c/...`, every read and write goes through
the 9p file-system bridge — orders of magnitude slower than
native Linux ext4. **Always put emdb data files on the WSL2
ext4 filesystem (`~/...`), not on `/mnt/c/...`**.

### CI parity

The Linux CI runs are functionally indistinguishable from WSL2
runs at the application level, but on the same hardware the
WSL2 numbers will be slightly lower due to virtualisation
overhead.

---

## Cross-platform paths

`Emdb::open(path)` accepts anything that converts to `&Path`:

- `&str` / `String` — works on all platforms.
- `&Path` / `PathBuf` — preferred. Handles platform-specific
  separator normalisation.

For OS-aware path resolution (XDG on Linux, Application Support
on macOS, `%LOCALAPPDATA%` on Windows), use the builder:

```rust,ignore
let db = Emdb::builder()
    .app_name("acme")
    .database_name("sessions")
    .build()?;
```

This resolves to:
- Linux: `$XDG_DATA_HOME/acme/sessions.emdb` (defaults to
  `~/.local/share/acme/sessions.emdb`)
- macOS: `~/Library/Application Support/acme/sessions.emdb`
- Windows: `%LOCALAPPDATA%\acme\sessions.emdb`

The directory is created if it doesn't exist.

---

## Cross-platform locking

`<path>.lock` is the lockfile. Acquired on open, released on
drop.

| Platform | Mechanism |
|---|---|
| Linux | `flock(LOCK_EX \| LOCK_NB)` |
| macOS | `flock(LOCK_EX \| LOCK_NB)` |
| Windows | `LockFileEx(LOCKFILE_EXCLUSIVE_LOCK \| LOCKFILE_FAIL_IMMEDIATELY)` |

All three give the same semantic: one process at a time per
database file. If the holder process dies abruptly, the OS
releases the lock at handle cleanup time — within seconds on
Linux/macOS, also within seconds on Windows.

If you need to recover faster than the OS cleanup, the
`Emdb::lock_holder(path)` API returns the PID + hostname of the
last-known holder (written to a sibling file at lock
acquisition). If you can confirm the holder is dead,
`Emdb::break_lock(path)` removes the lock unilaterally — useful
for cluster recovery scripts. **Never call `break_lock` without
confirming the holder is dead** — concurrent access to the same
database from two processes will corrupt the journal.

---

## Cross-platform durability

| Operation | Linux | macOS | Windows |
|---|---|---|---|
| `flush()` (`OnEachFlush`) | `fdatasync` | `F_FULLFSYNC` | `FlushFileBuffers` |
| `flush()` (`Group` leader) | `fdatasync` | `F_FULLFSYNC` | `FlushFileBuffers` |
| `insert` w/ `WriteThrough` | `RWF_DSYNC` | `O_SYNC` + `F_FULLFSYNC` | `FILE_FLAG_WRITE_THROUGH` |
| `checkpoint()` (metadata) | `rename` is atomic | `rename` is atomic | `MoveFileExW(REPLACE_EXISTING \| WRITE_THROUGH)` |
| `compact()` (journal swap) | `rename` is atomic | `rename` is atomic | `MoveFileExW(REPLACE_EXISTING \| WRITE_THROUGH)` |

The semantics are equivalent — every platform guarantees
"data is durable after this call returns" — but absolute
latencies differ:

- **Linux NVMe**: 50–500 µs per `fdatasync`.
- **macOS NVMe**: 1–10 ms per `F_FULLFSYNC` (device-cache flush).
- **Windows NVMe**: 100 µs – 5 ms per `FlushFileBuffers`.

For workloads sensitive to absolute flush latency, prefer Linux.
For workloads that just need correctness on any platform, all
three work fine.

---

## See also

- [ARCHITECTURE.md](ARCHITECTURE.md) — engine internals.
- [PERFORMANCE.md](PERFORMANCE.md) — per-op cost model.
- [BENCH.md](BENCH.md) — full benchmark methodology.
- [fsys-rs](https://github.com/jamesgober/fsys-rs) — the storage
  substrate where the platform-specific code actually lives.
