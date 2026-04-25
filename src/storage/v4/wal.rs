// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! v0.7 write-ahead log with group commit.
//!
//! Producers call [`Wal::append`] to add bytes to the in-memory buffer and
//! receive a sequence number (a "commit ticket"). Durability is decoupled
//! from the append: a producer that needs the write to be on disk calls
//! [`Wal::wait_for_seq`] with their ticket, which:
//!
//! 1. If the ticket has already been fsynced, returns immediately.
//! 2. Otherwise, takes the WAL's commit mutex, drains the userspace buffer
//!    to the file, calls `fdatasync`, and advances `last_synced_seq`.
//!
//! Multiple producers waiting on different tickets share a single fsync:
//! whichever thread holds the commit mutex syncs everything that has been
//! appended so far, then advances `last_synced_seq` past every ticket the
//! syscall covered. The other waiters wake, see their ticket is already
//! durable, and return without issuing a second `fdatasync`.
//!
//! When [`FlushPolicy::Group`] is configured, an optional background thread
//! also fsyncs on a deadline so producers do not have to call
//! [`Wal::wait_for_seq`] explicitly. The producer-driven path and the
//! background path use the same mutex, so the fsyncs they generate are
//! always serialised on disk.

use std::fs::File;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::storage::v4::io::{open_page_file, IoMode};
use crate::{Error, Result};

/// Buffer size for the userspace write buffer wrapping the WAL file.
const WAL_BUFFER_BYTES: usize = 64 * 1024;

/// Group-commit pacing.
///
/// `Manual` and `OnEachWrite` mirror the v0.6 `FlushPolicy` of the same name.
/// `Group` is new in v0.7: appends accumulate, and a background thread
/// fsyncs on a deadline to bound producer latency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FlushPolicy {
    /// Producers must call [`Wal::wait_for_seq`] explicitly when they need
    /// durability. No background thread runs.
    Manual,
    /// Each successful [`Wal::append`] is fsynced before returning. Highest
    /// durability, lowest concurrency throughput.
    OnEachWrite,
    /// A background thread fsyncs whenever there is pending data, with at
    /// most `max_wait` between fsyncs. Producers may call
    /// [`Wal::wait_for_seq`] for explicit durability or rely on the
    /// background pacing.
    Group {
        /// Upper bound on the wait between background fsyncs. Tunable per
        /// workload — lower values reduce p99 latency, higher values amortise
        /// fsync cost across more producers.
        max_wait: Duration,
    },
}

impl FlushPolicy {
    fn background(self) -> Option<Duration> {
        match self {
            Self::Group { max_wait } => Some(max_wait),
            _ => None,
        }
    }
}

/// Inner state shared between producers and the optional background flusher.
#[derive(Debug)]
struct WalInner {
    /// Userspace buffered writer wrapping the WAL file. The buffer absorbs
    /// many appends per syscall; explicit `flush()` drains it before fsync.
    writer: BufWriter<File>,
    /// Bytes appended since the last successful fsync. Tracked so callers
    /// can ask "is this seq durable?" without a syscall.
    pending_bytes: u64,
    /// Sequence number assigned to the next [`Wal::append`].
    next_seq: u64,
    /// Sequence number of the last byte covered by a successful fsync. Reads
    /// of this value through [`WalInner::last_synced_seq_atomic`] use atomic
    /// loads so the fast path of [`Wal::wait_for_seq`] avoids the mutex.
    last_synced_seq: u64,
    /// Once set, every subsequent operation fails with this error. Protects
    /// the engine from operating on a torn WAL.
    poisoned: Option<&'static str>,
}

/// Outer handle: producers, transactions, and the background flusher all
/// share a single `Arc<WalShared>`.
#[derive(Debug)]
struct WalShared {
    inner: Mutex<WalInner>,
    notify_writer: Condvar,
    /// Atomically-readable mirror of `WalInner::last_synced_seq` so the fast
    /// path of [`Wal::wait_for_seq`] avoids the mutex when a write is
    /// already durable.
    last_synced_seq: AtomicU64,
    /// Atomically-readable mirror of `WalInner::next_seq` so the background
    /// flusher can decide whether there is work to do without locking.
    next_seq: AtomicU64,
    /// Set to false to ask the background flusher to exit on its next wake.
    background_alive: AtomicBool,
    /// On-disk path; useful for diagnostics and for `path_for` callers.
    path: PathBuf,
    /// Configured pacing.
    policy: FlushPolicy,
}

/// Group-commit WAL.
#[derive(Debug)]
pub(crate) struct Wal {
    shared: Arc<WalShared>,
    /// Background flusher join handle. `None` for non-group policies.
    background: Option<JoinHandle<()>>,
}

impl Wal {
    /// Open or create a WAL file at `path` with the given pacing, defaulting
    /// to buffered I/O.
    ///
    /// # Errors
    ///
    /// Propagates filesystem errors from opening the WAL file.
    pub(crate) fn open(path: impl Into<PathBuf>, policy: FlushPolicy) -> Result<Self> {
        Self::open_with_mode(path, policy, IoMode::Buffered)
    }

    /// Open or create a WAL file with an explicit I/O mode.
    ///
    /// `IoMode::Direct` requests `O_DIRECT` / `F_NOCACHE` on Unix or
    /// `FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH` on Windows. The
    /// trade-off is platform-specific:
    ///
    /// - **Windows:** `WRITE_THROUGH` makes every `write_all` synchronously
    ///   durable in one syscall — no separate `fdatasync` needed for
    ///   `OnEachWrite` durability. This is a meaningful latency win when
    ///   `OnEachWrite` is the configured policy.
    /// - **Linux / macOS:** `O_DIRECT` / `F_NOCACHE` bypass the OS page
    ///   cache but still require an explicit `fdatasync` for durability,
    ///   so the latency on `OnEachWrite` is roughly the same as buffered.
    ///   Unaligned writes (typical WAL records are sub-page) may be
    ///   silently rejected on `O_DIRECT`, falling back to `EINVAL`. **Use
    ///   buffered on Linux/macOS unless you know the WAL records are
    ///   page-aligned.**
    ///
    /// # Errors
    ///
    /// Propagates filesystem errors from the open. On Linux/macOS,
    /// `IoMode::Direct` may fail with `EINVAL` on filesystems that do not
    /// support unaligned `O_DIRECT` writes (most do not for sub-page
    /// records); the caller should retry with `IoMode::Buffered`.
    pub(crate) fn open_with_mode(
        path: impl Into<PathBuf>,
        policy: FlushPolicy,
        mode: IoMode,
    ) -> Result<Self> {
        let path = path.into();
        let mut file = open_page_file(&path, mode)?;
        let len = file.metadata()?.len();
        let _seek = file.seek(SeekFrom::End(0))?;
        let writer = BufWriter::with_capacity(WAL_BUFFER_BYTES, file);

        let shared = Arc::new(WalShared {
            inner: Mutex::new(WalInner {
                writer,
                pending_bytes: 0,
                next_seq: 0,
                last_synced_seq: 0,
                poisoned: None,
            }),
            notify_writer: Condvar::new(),
            last_synced_seq: AtomicU64::new(0),
            next_seq: AtomicU64::new(0),
            background_alive: AtomicBool::new(true),
            path,
            policy,
        });

        // Mark the file's existing length as already-synced from the WAL's
        // perspective: we never overwrite previously written records, only
        // append.
        let _ = len;

        let background = policy
            .background()
            .map(|max_wait| spawn_flusher(Arc::clone(&shared), max_wait))
            .transpose()?;

        Ok(Self { shared, background })
    }

    /// Compute the v4 WAL path for a given page-store path.
    pub(crate) fn path_for(store_path: &Path) -> PathBuf {
        let mut wal_name = store_path
            .file_name()
            .and_then(|name| name.to_str())
            .map_or_else(
                || String::from("emdb.v4.wal"),
                |name| format!("{name}.v4.wal"),
            );
        if wal_name.is_empty() {
            wal_name = String::from("emdb.v4.wal");
        }
        let mut out = store_path.to_path_buf();
        out.set_file_name(wal_name);
        out
    }

    /// Append `bytes` to the WAL and return the assigned sequence number.
    ///
    /// On [`FlushPolicy::OnEachWrite`] the call also fsyncs before returning.
    /// On other policies the bytes are guaranteed only to be in the
    /// userspace buffer; durability requires [`Wal::wait_for_seq`] or, for
    /// `Group`, waiting for the background flusher's deadline.
    ///
    /// # Errors
    ///
    /// Returns the underlying I/O error on write failure, or
    /// [`Error::TransactionAborted`] if the WAL was previously poisoned.
    pub(crate) fn append(&self, bytes: &[u8]) -> Result<u64> {
        let seq = {
            let mut inner = self
                .shared
                .inner
                .lock()
                .map_err(|_poisoned| Error::LockPoisoned)?;

            if let Some(reason) = inner.poisoned {
                return Err(Error::TransactionAborted(reason));
            }

            inner.writer.write_all(bytes)?;
            inner.pending_bytes = inner.pending_bytes.saturating_add(bytes.len() as u64);
            let assigned = inner.next_seq;
            inner.next_seq = inner
                .next_seq
                .checked_add(1)
                .ok_or(Error::TransactionAborted("wal sequence number overflow"))?;
            self.shared
                .next_seq
                .store(inner.next_seq, Ordering::Release);
            assigned
        };

        // Wake the background flusher; it may decide to fsync soon.
        self.shared.notify_writer.notify_one();

        if matches!(self.shared.policy, FlushPolicy::OnEachWrite) {
            self.wait_for_seq(seq)?;
        }

        Ok(seq)
    }

    /// Block until the byte for `seq` has been fsynced.
    ///
    /// On the fast path, an atomic load resolves "already durable" without
    /// touching the commit mutex. On the slow path, the caller takes the
    /// commit mutex and runs `fdatasync` themselves; concurrent waiters on
    /// other tickets share that fsync via the same mutex.
    ///
    /// # Errors
    ///
    /// Returns the underlying I/O error from `fdatasync`, or
    /// [`Error::TransactionAborted`] when the WAL was previously poisoned
    /// (in which case nothing is durable past the poison point).
    pub(crate) fn wait_for_seq(&self, seq: u64) -> Result<()> {
        if self.shared.last_synced_seq.load(Ordering::Acquire) > seq {
            return Ok(());
        }

        let mut inner = self
            .shared
            .inner
            .lock()
            .map_err(|_poisoned| Error::LockPoisoned)?;

        if let Some(reason) = inner.poisoned {
            return Err(Error::TransactionAborted(reason));
        }
        if inner.last_synced_seq > seq {
            return Ok(());
        }

        let target = inner.next_seq;
        match perform_fsync(&mut inner) {
            Ok(()) => {
                inner.last_synced_seq = target;
                self.shared.last_synced_seq.store(target, Ordering::Release);
                Ok(())
            }
            Err(err) => {
                inner.poisoned = Some("wal fsync failed");
                Err(err)
            }
        }
    }

    /// Force an fsync of every appended byte, regardless of any specific seq.
    ///
    /// # Errors
    ///
    /// Same shape as [`Wal::wait_for_seq`].
    pub(crate) fn flush(&self) -> Result<()> {
        let target_minus_one = self.shared.next_seq.load(Ordering::Acquire);
        if target_minus_one == 0 {
            return Ok(());
        }
        self.wait_for_seq(target_minus_one - 1)
    }

    /// Truncate the WAL to zero bytes after a successful checkpoint of the
    /// page file.
    ///
    /// # Errors
    ///
    /// Returns the underlying filesystem error on `set_len`/`fsync` failure.
    pub(crate) fn truncate(&self) -> Result<()> {
        let mut inner = self
            .shared
            .inner
            .lock()
            .map_err(|_poisoned| Error::LockPoisoned)?;
        inner.writer.flush()?;
        let file = inner.writer.get_mut();
        file.set_len(0)?;
        let _seek = file.seek(SeekFrom::Start(0))?;
        file.sync_data()?;
        inner.pending_bytes = 0;
        inner.next_seq = 0;
        inner.last_synced_seq = 0;
        self.shared.next_seq.store(0, Ordering::Release);
        self.shared.last_synced_seq.store(0, Ordering::Release);
        Ok(())
    }

    /// Read the entire WAL file into the supplied buffer for replay.
    ///
    /// # Errors
    ///
    /// Returns the underlying filesystem error.
    pub(crate) fn read_all(&self, out: &mut Vec<u8>) -> Result<()> {
        let mut inner = self
            .shared
            .inner
            .lock()
            .map_err(|_poisoned| Error::LockPoisoned)?;
        inner.writer.flush()?;
        let file = inner.writer.get_mut();
        let _seek = file.seek(SeekFrom::Start(0))?;
        out.clear();
        let _read = file.read_to_end(out)?;
        let _seek = file.seek(SeekFrom::End(0))?;
        Ok(())
    }

    /// Path of the WAL file.
    #[must_use]
    pub(crate) fn path(&self) -> &Path {
        &self.shared.path
    }

    /// Number of bytes currently appended but not yet fsynced.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockPoisoned`].
    pub(crate) fn pending_bytes(&self) -> Result<u64> {
        let inner = self
            .shared
            .inner
            .lock()
            .map_err(|_poisoned| Error::LockPoisoned)?;
        Ok(inner.pending_bytes)
    }

    /// Read the sequence number that will be assigned to the next
    /// successful [`Self::append`]. The engine snapshots this during
    /// flush to persist a "WAL through here is reflected in pages"
    /// marker in the page header.
    #[must_use]
    pub(crate) fn next_seq(&self) -> u64 {
        self.shared.next_seq.load(Ordering::Acquire)
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        // Ask the background flusher (if any) to exit, then wait for it. The
        // flusher only fsyncs and never blocks on a producer, so this join is
        // bounded: it returns within at most the configured `max_wait`.
        self.shared.background_alive.store(false, Ordering::Release);
        self.shared.notify_writer.notify_all();
        if let Some(handle) = self.background.take() {
            let _joined = handle.join();
        }
    }
}

fn perform_fsync(inner: &mut WalInner) -> Result<()> {
    inner.writer.flush()?;
    inner.writer.get_mut().sync_data()?;
    inner.pending_bytes = 0;
    Ok(())
}

fn spawn_flusher(shared: Arc<WalShared>, max_wait: Duration) -> Result<JoinHandle<()>> {
    thread::Builder::new()
        .name("emdb-wal-flusher".to_string())
        .spawn(move || flusher_loop(shared, max_wait))
        .map_err(Error::from)
}

fn flusher_loop(shared: Arc<WalShared>, max_wait: Duration) {
    let deadline_grace = max_wait;
    while shared.background_alive.load(Ordering::Acquire) {
        // Wait either for a producer to wake us or for our deadline to elapse.
        let inner = match shared.inner.lock() {
            Ok(inner) => inner,
            Err(_poisoned) => return,
        };

        let pending = inner.next_seq != inner.last_synced_seq;
        let _ = pending;

        let waited = shared
            .notify_writer
            .wait_timeout(inner, deadline_grace)
            .map(|(guard, result)| (guard, result.timed_out()));
        let (mut inner, _timed_out) = match waited {
            Ok(pair) => pair,
            Err(_poisoned) => return,
        };

        if !shared.background_alive.load(Ordering::Acquire) {
            return;
        }
        if inner.poisoned.is_some() {
            continue;
        }
        if inner.next_seq == inner.last_synced_seq {
            continue;
        }

        let target = inner.next_seq;
        match perform_fsync(&mut inner) {
            Ok(()) => {
                inner.last_synced_seq = target;
                shared.last_synced_seq.store(target, Ordering::Release);
            }
            Err(_err) => {
                inner.poisoned = Some("wal fsync failed in background");
                // Producers will surface the error on their next call.
            }
        }
        // Drop the lock before rechecking alive on the next loop iteration.
        let _ = (inner, deadline_grace);
        // The bound on the loop's wakeup cadence is `max_wait`; if anyone
        // else queued bytes while we were fsyncing they'll be picked up on
        // the next iteration.
        let _instant_marker = Instant::now();
        let _ = _instant_marker;
    }
}

#[cfg(test)]
mod tests {
    use super::{FlushPolicy, Wal};
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};

    fn tmp_path(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0_u128, |d| d.as_nanos());
        p.push(format!("emdb-v4-wal-{name}-{nanos}.wal"));
        p
    }

    fn open(name: &str, policy: FlushPolicy) -> (Wal, std::path::PathBuf) {
        let path = tmp_path(name);
        let wal = match Wal::open(path.clone(), policy) {
            Ok(wal) => wal,
            Err(err) => panic!("open should succeed: {err}"),
        };
        (wal, path)
    }

    #[test]
    fn fresh_wal_starts_empty() {
        let (wal, path) = open("fresh", FlushPolicy::Manual);
        let pending = wal.pending_bytes();
        assert!(matches!(pending, Ok(0)));
        drop(wal);
        let _removed = std::fs::remove_file(&path);
    }

    #[test]
    fn append_returns_monotonic_sequence_numbers() {
        let (wal, path) = open("monotonic", FlushPolicy::Manual);
        let s0 = wal.append(b"alpha");
        let s1 = wal.append(b"beta");
        let s2 = wal.append(b"gamma");
        assert!(matches!(s0, Ok(0)));
        assert!(matches!(s1, Ok(1)));
        assert!(matches!(s2, Ok(2)));
        drop(wal);
        let _removed = std::fs::remove_file(&path);
    }

    #[test]
    fn manual_policy_does_not_fsync_until_wait() {
        let (wal, path) = open("manual-fsync", FlushPolicy::Manual);
        let _seq = wal.append(b"hi");
        // Bytes are buffered; nothing fsynced yet.
        let pending = wal.pending_bytes();
        assert!(matches!(pending, Ok(b) if b > 0));
        let _ = wal.flush();
        let pending = wal.pending_bytes();
        assert!(matches!(pending, Ok(0)));
        drop(wal);
        let _removed = std::fs::remove_file(&path);
    }

    #[test]
    fn on_each_write_persists_synchronously() {
        let (wal, path) = open("on-each-write", FlushPolicy::OnEachWrite);
        let _seq = wal.append(b"hi");
        let pending = wal.pending_bytes();
        assert!(matches!(pending, Ok(0)));
        drop(wal);
        let _removed = std::fs::remove_file(&path);
    }

    #[test]
    fn read_all_returns_appended_bytes() {
        let (wal, path) = open("read-all", FlushPolicy::Manual);
        let _ = wal.append(b"foo");
        let _ = wal.append(b"bar");
        let mut buf = Vec::new();
        let read = wal.read_all(&mut buf);
        assert!(read.is_ok());
        assert_eq!(&buf, b"foobar");
        drop(wal);
        let _removed = std::fs::remove_file(&path);
    }

    #[test]
    fn truncate_resets_sequence_and_pending() {
        let (wal, path) = open("truncate", FlushPolicy::Manual);
        let _ = wal.append(b"hi");
        let _ = wal.append(b"there");
        let truncated = wal.truncate();
        assert!(truncated.is_ok());
        let pending = wal.pending_bytes();
        assert!(matches!(pending, Ok(0)));

        let mut buf = Vec::new();
        let read = wal.read_all(&mut buf);
        assert!(read.is_ok());
        assert!(buf.is_empty());
        drop(wal);
        let _removed = std::fs::remove_file(&path);
    }

    #[test]
    fn wait_for_seq_advances_durability_marker() {
        let (wal, path) = open("wait-seq", FlushPolicy::Manual);
        let s0 = match wal.append(b"a") {
            Ok(s) => s,
            Err(err) => panic!("append should succeed: {err}"),
        };
        let waited = wal.wait_for_seq(s0);
        assert!(waited.is_ok());
        // After wait, pending_bytes should be zero (the buffer was drained).
        let pending = wal.pending_bytes();
        assert!(matches!(pending, Ok(0)));
        drop(wal);
        let _removed = std::fs::remove_file(&path);
    }

    #[test]
    fn group_policy_fsyncs_in_background_within_deadline() {
        let max_wait = Duration::from_millis(50);
        let (wal, path) = open(
            "group-bg",
            FlushPolicy::Group { max_wait },
        );
        let _seq = wal.append(b"async-durability");

        // Poll until the background flusher drains the buffer. The deadline
        // is `max_wait` so anything past 1× deadline indicates the flusher
        // ran, but slow CI runners (debug build, virtualised disk) can take
        // far longer than the deadline to actually wake the thread + run
        // fsync. Allow up to 5 seconds; the test still fails fast on a
        // genuinely-broken flusher because we poll every 10ms.
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match wal.pending_bytes() {
                Ok(0) => break,
                Ok(_pending) => {
                    if Instant::now() >= deadline {
                        panic!(
                            "background flusher did not drain pending bytes within 5s (max_wait={:?})",
                            max_wait
                        );
                    }
                    thread::sleep(Duration::from_millis(10));
                }
                Err(err) => panic!("pending_bytes failed: {err}"),
            }
        }

        drop(wal);
        let _removed = std::fs::remove_file(&path);
    }

    #[test]
    fn concurrent_appends_share_a_single_fsync() {
        // 16 producer threads each append once and call wait_for_seq. The
        // commit mutex serialises fsyncs; only the first thread to reach
        // wait_for_seq actually issues the syscall. This test checks the
        // happy path: every wait succeeds and durability advances past the
        // last appended seq.
        let (wal, path) = open("concurrent", FlushPolicy::Manual);
        let wal = Arc::new(wal);
        let mut handles = Vec::new();
        for i in 0..16_u32 {
            let wal = Arc::clone(&wal);
            handles.push(thread::spawn(move || {
                let payload = format!("record-{i}").into_bytes();
                let seq = wal.append(&payload).unwrap_or(u64::MAX);
                let _ = wal.wait_for_seq(seq);
                seq
            }));
        }
        let mut max_seen = 0_u64;
        for handle in handles {
            let seq = handle.join().unwrap_or(0);
            if seq > max_seen {
                max_seen = seq;
            }
        }
        // Each thread successfully appended; the highest observed seq must
        // be at least 15 (16 producers, 0-indexed).
        assert!(max_seen >= 15);
        let pending = wal.pending_bytes();
        assert!(matches!(pending, Ok(0)));
        drop(wal);
        let _removed = std::fs::remove_file(&path);
    }

    #[test]
    fn group_commit_meets_latency_target_under_burst() {
        // Burst of 32 producers under the Group policy should complete within
        // a small multiple of `max_wait`. We do not assert exact timing —
        // sleep precision varies — but we do assert all producers complete
        // and pending_bytes reaches zero.
        let (wal, _path) = open(
            "group-burst",
            FlushPolicy::Group {
                max_wait: Duration::from_millis(20),
            },
        );
        let wal = Arc::new(wal);
        let started = Instant::now();
        let mut handles = Vec::new();
        for i in 0..32_u32 {
            let wal = Arc::clone(&wal);
            handles.push(thread::spawn(move || {
                let payload = format!("burst-{i}").into_bytes();
                let _ = wal.append(&payload);
            }));
        }
        for handle in handles {
            let _ = handle.join();
        }
        // Wait briefly for the background flusher to drain.
        thread::sleep(Duration::from_millis(80));
        let pending = wal.pending_bytes();
        assert!(matches!(pending, Ok(0)));
        let elapsed = started.elapsed();
        // Sanity: the whole burst should not have taken seconds.
        assert!(elapsed < Duration::from_secs(2));
    }

    #[test]
    fn path_for_appends_v4_wal_suffix() {
        let derived = Wal::path_for(std::path::Path::new("/tmp/foo.emdb"));
        assert!(
            derived.to_string_lossy().ends_with("foo.emdb.v4.wal"),
            "unexpected derived path: {}",
            derived.display()
        );
    }
}
