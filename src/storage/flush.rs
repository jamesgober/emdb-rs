// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Flush policy and the group-commit coordinator.
//!
//! The default policy is [`FlushPolicy::OnEachFlush`], which is what
//! v0.7.x did unconditionally: every `db.flush()` performs one
//! `fdatasync`. That is the safest, simplest semantics, and is the
//! correct choice when the caller already batches their fsyncs at
//! the application layer.
//!
//! [`FlushPolicy::Group`] adds a coordinator that fuses concurrent
//! `db.flush()` calls into one `fdatasync`. The motivating workload
//! is N independent producer threads each writing one record then
//! calling `flush` for per-record durability — a pattern where
//! `OnEachFlush` pays N syncs even though one would do.
//!
//! The protocol is a leader-follower scheme:
//!
//! 1. Every flusher snapshots the current tail offset (the byte
//!    position it wants durable).
//! 2. If the coordinator's recorded `durable_tail` already covers
//!    the snapshot, the flusher returns immediately with no work.
//! 3. Otherwise the flusher takes the coordinator lock. If no
//!    leader is currently running, it becomes the leader: it waits
//!    up to `max_wait`, or until `max_batch` flushers have joined,
//!    then performs one `sync_data` that covers everyone's writes.
//! 4. Followers wait on a condvar until `durable_tail` covers their
//!    snapshot.
//!
//! Failure handling: if the leader's `sync_data` returns an error,
//! the leader records the error in shared state and notifies all
//! followers. Each follower returns the same error. The next flush
//! cycle starts fresh — the coordinator has no concept of a "broken
//! state"; it just retries.

use std::fs::File;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::{Error, Result};

/// How `db.flush()` interacts with concurrent flush requests.
///
/// [`FlushPolicy::OnEachFlush`] is the default and matches the
/// v0.7.x behaviour: each call issues its own `fdatasync`. Pick
/// this when the caller already batches durability at the
/// application layer or when there is only one writer thread.
///
/// [`FlushPolicy::Group`] enables the coordinator described in
/// the module docs. Pick this when many threads independently
/// write records and call `flush` for per-record durability —
/// the typical request-handler pattern in a multi-threaded
/// service.
///
/// **Tuning.** `max_batch` should be set close to the expected
/// number of concurrent flushers (often `num_cpus::get()`). If
/// it is larger, the leader will wait the full `max_wait` for
/// followers that can never arrive, turning batching into pure
/// tail latency. `max_wait` should be small relative to typical
/// fsync latency — 500 µs is a reasonable default on commodity
/// SSDs. A useful sanity check: with K concurrent flushers and
/// a single fsync taking T ms, the policy should produce
/// roughly `K × throughput_of_OnEachFlush` once K reaches
/// `max_batch`.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default)]
pub enum FlushPolicy {
    /// Every `db.flush()` issues its own `fdatasync` immediately.
    /// One sync per flush. Default.
    #[default]
    OnEachFlush,
    /// Coalesce concurrent flushes. Each `db.flush()` returns once
    /// its writes are durable, but multiple in-flight flushes share
    /// a single `fdatasync` so per-record-durability workloads
    /// scale with sync throughput, not sync count.
    Group {
        /// Maximum time a leader will wait for additional flushers
        /// to join before issuing the sync. Smaller values reduce
        /// tail latency at the cost of fewer flushes batched per
        /// sync.
        max_wait: Duration,
        /// Maximum number of flushers to batch before firing the
        /// sync regardless of `max_wait`. Once `max_batch` flushers
        /// have joined the current cycle, the leader stops waiting
        /// and syncs immediately.
        max_batch: usize,
    },
    /// Open the file with platform-native synchronous-write flags
    /// (`FILE_FLAG_WRITE_THROUGH` on Windows, `O_SYNC` on Unix), so
    /// every `pwrite` is durable on return and `flush()` becomes a
    /// no-op fast path.
    ///
    /// Pick this for single-thread per-record-durability workloads
    /// where the dominant cost is `flush()`-after-every-record. On
    /// Windows that path normally pays one `FlushFileBuffers` per
    /// call (~27 ms on consumer NVMe); `WriteThrough` lets the OS
    /// commit each record synchronously inside `pwrite` instead,
    /// often giving lower per-op latency at the cost of higher
    /// per-op base cost (every write waits for disk, not just
    /// every flush).
    ///
    /// **Trade-off.** Bulk loads under this policy are slower than
    /// under `OnEachFlush` because every individual `pwrite` waits
    /// for the disk; the bulk-load path no longer benefits from
    /// the OS's write-back cache. For mixed workloads, prefer
    /// `Group` (multi-threaded per-record durability) or stick
    /// with the default and batch via `transaction()` /
    /// `insert_many()`.
    ///
    /// Behaviourally, `flush()` under this policy still calls
    /// `sync_data` (which is fast — most data is already durable),
    /// so callers who flip between policies see no semantic change.
    WriteThrough,
}

/// Coordinator state. One per Store; constructed only when the
/// effective policy is `Group`.
pub(crate) struct GroupCoord {
    state: Mutex<GroupState>,
    cv: Condvar,
    /// Tail offset the leader observed before issuing the most
    /// recent successful sync. Atomic so followers can do an
    /// early-out check without taking the mutex.
    durable_tail: AtomicU64,
    max_wait: Duration,
    max_batch: usize,
}

struct GroupState {
    /// Whether a leader is currently running a sync cycle.
    leader_active: bool,
    /// Number of flushers parked waiting for the in-flight cycle to
    /// finish (including the leader itself).
    pending: usize,
    /// Generation counter — bumped each time a sync cycle completes
    /// (success or error). Followers can use it to detect "a cycle
    /// has run since I started waiting". Wraparound is not a real
    /// concern at one-cycle-per-microsecond rates.
    cycle_seq: u64,
    /// Sticky error from the most recent failed sync. Cleared at the
    /// start of the next leader cycle. We keep the kind+message so
    /// followers can construct a matching `Error::Io`; cloning a
    /// `std::io::Error` is not free, but errors are rare.
    last_error: Option<(io::ErrorKind, String)>,
}

impl GroupCoord {
    pub(crate) fn new(max_wait: Duration, max_batch: usize) -> Self {
        // A `max_batch` of 0 would mean "never batch", which is
        // pathological. Clamp to 1 so the leader always at least
        // covers itself.
        let max_batch = max_batch.max(1);
        Self {
            state: Mutex::new(GroupState {
                leader_active: false,
                pending: 0,
                cycle_seq: 0,
                last_error: None,
            }),
            cv: Condvar::new(),
            durable_tail: AtomicU64::new(0),
            max_wait,
            max_batch,
        }
    }

    /// Run a flush request: ensure that bytes through `target_tail`
    /// are durable on disk before returning.
    ///
    /// `sync_call` is the closure that actually invokes `sync_data`
    /// on the file. The coordinator calls it once per leader cycle.
    /// `sync_call` should also return the tail offset that was
    /// snapshotted at the moment of the sync (i.e. the value of
    /// the writer's `tail_atomic` immediately before issuing
    /// `sync_data` — that is the offset durability is now
    /// guaranteed up to).
    pub(crate) fn run<F>(&self, target_tail: u64, sync_call: F) -> Result<()>
    where
        F: FnOnce() -> std::result::Result<u64, io::Error>,
    {
        // Fast path: the most recent successful sync already covers
        // our target. No lock, no syscall.
        if self.durable_tail.load(Ordering::Acquire) >= target_tail {
            return Ok(());
        }

        let mut state = self.state.lock().map_err(|_| Error::LockPoisoned)?;
        state.pending += 1;
        // Wake the leader (if any) so it can re-check the
        // `pending >= max_batch` exit condition. Without this,
        // followers join silently and the leader sleeps the full
        // `max_wait` regardless of how many followers have arrived —
        // turning group commit into a *latency tax* instead of a
        // throughput win.
        self.cv.notify_all();

        // Recheck after locking: the world may have moved while we
        // were unlocked.
        if self.durable_tail.load(Ordering::Acquire) >= target_tail {
            state.pending -= 1;
            return Ok(());
        }

        if !state.leader_active {
            // We become the leader. Wait for either max_wait to
            // elapse or for max_batch flushers to join.
            state.leader_active = true;
            // Clear any sticky error from the previous failed
            // cycle. New cycle, fresh slate.
            state.last_error = None;

            // We're already counted in `pending`. The leader waits
            // until `pending >= max_batch` (i.e. someone else joined
            // us) OR until max_wait elapses.
            let deadline = Instant::now() + self.max_wait;
            while state.pending < self.max_batch {
                let now = Instant::now();
                if now >= deadline {
                    break;
                }
                let remaining = deadline - now;
                let (st, timeout) = self
                    .cv
                    .wait_timeout(state, remaining)
                    .map_err(|_| Error::LockPoisoned)?;
                state = st;
                if timeout.timed_out() {
                    break;
                }
            }

            // Drop the lock so concurrent writers can keep
            // appending while we sync. The leader holds
            // `leader_active = true` so other flushers will park
            // as followers rather than starting a parallel cycle.
            drop(state);

            let sync_result = sync_call();

            let mut state = self.state.lock().map_err(|_| Error::LockPoisoned)?;
            state.leader_active = false;
            state.cycle_seq = state.cycle_seq.wrapping_add(1);
            let result = match sync_result {
                Ok(synced_through) => {
                    // Atomic store with Release so followers can
                    // load with Acquire and skip the lock.
                    self.durable_tail.store(synced_through, Ordering::Release);
                    Ok(())
                }
                Err(err) => {
                    state.last_error = Some((err.kind(), err.to_string()));
                    Err(Error::from(err))
                }
            };
            // We were one of the pending flushers — decrement.
            state.pending = state.pending.saturating_sub(1);
            self.cv.notify_all();
            return result;
        }

        // Follower: someone else is leading. Wait until either the
        // durable tail covers us, or the leader records an error
        // we should propagate. Use cycle_seq so we don't miss a
        // notify_all.
        let entered_cycle = state.cycle_seq;
        loop {
            // Did a sync cycle complete successfully past our target?
            if self.durable_tail.load(Ordering::Acquire) >= target_tail {
                state.pending = state.pending.saturating_sub(1);
                return Ok(());
            }

            // Did the most recent cycle error out? Propagate the
            // same kind of error to every follower of that cycle.
            if state.cycle_seq != entered_cycle {
                if let Some((kind, msg)) = state.last_error.as_ref() {
                    let err = io::Error::new(*kind, msg.clone());
                    state.pending = state.pending.saturating_sub(1);
                    return Err(Error::from(err));
                }
                // The cycle completed but didn't reach us. Try
                // again — we may need to start a new leader.
                if !state.leader_active {
                    // Promote ourselves: re-enter the leader path.
                    state.leader_active = true;
                    state.last_error = None;

                    // Same wait loop as above.
                    let deadline = Instant::now() + self.max_wait;
                    while state.pending < self.max_batch {
                        let now = Instant::now();
                        if now >= deadline {
                            break;
                        }
                        let remaining = deadline - now;
                        let (st, timeout) = self
                            .cv
                            .wait_timeout(state, remaining)
                            .map_err(|_| Error::LockPoisoned)?;
                        state = st;
                        if timeout.timed_out() {
                            break;
                        }
                    }

                    drop(state);
                    let sync_result = sync_call();
                    let mut state = self.state.lock().map_err(|_| Error::LockPoisoned)?;
                    state.leader_active = false;
                    state.cycle_seq = state.cycle_seq.wrapping_add(1);
                    let result = match sync_result {
                        Ok(synced_through) => {
                            self.durable_tail.store(synced_through, Ordering::Release);
                            Ok(())
                        }
                        Err(err) => {
                            state.last_error = Some((err.kind(), err.to_string()));
                            Err(Error::from(err))
                        }
                    };
                    state.pending = state.pending.saturating_sub(1);
                    self.cv.notify_all();
                    return result;
                }
                // Else: another follower beat us to leadership.
                // Loop and wait again.
            }

            // Wait for a state change. Condvar wakes us on
            // notify_all from the leader, or on a configurable
            // safety timeout to guard against missed wakeups.
            let (st, _timeout) = self
                .cv
                .wait_timeout(state, Duration::from_millis(50))
                .map_err(|_| Error::LockPoisoned)?;
            state = st;
        }
    }
}

/// Helper used by [`crate::storage::store::Store::flush`] when the
/// active policy is `Group`. Pulls the file's current tail from the
/// caller (so we don't have to expose `tail_atomic` to this module),
/// runs `sync_data` on `file`, and reports success with the
/// pre-sync tail (the offset that is now durable).
pub(crate) fn group_sync(
    file: &File,
    tail_before_sync: u64,
) -> std::result::Result<u64, io::Error> {
    file.sync_data()?;
    Ok(tail_before_sync)
}
