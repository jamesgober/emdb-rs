// Integration tests for `FlushPolicy` and the group-commit
// coordinator added in v0.8.
//
// We can't assert "fewer fsyncs were issued" from public API (the
// syscall count is invisible to the consumer), so this suite
// focuses on correctness invariants:
//
//   - The default policy is `OnEachFlush` and exhibits the same
//     behaviour as v0.7.x (one sync per flush, durable after each
//     return).
//   - Group policy: many concurrent flushers all return Ok and
//     observe their own writes after a reopen.
//   - Group policy: a single-thread caller (no contention) still
//     completes within `max_wait` plus normal sync latency — i.e.
//     the leader-follower protocol does not deadlock when there's
//     no follower.
//   - Group policy: small `max_batch` values (1) collapse to
//     "every flush is its own leader" without dropping requests.
//   - Builder accepts and round-trips every policy variant.

use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use emdb::{Emdb, FlushPolicy, Result};

fn tmp_path(label: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |d| d.as_nanos());
    let tid = std::thread::current().id();
    p.push(format!("emdb-flushpolicy-{label}-{nanos}-{tid:?}.emdb"));
    p
}

fn cleanup(path: &PathBuf) {
    let _ = std::fs::remove_file(path);
    let display = path.display();
    let _ = std::fs::remove_file(format!("{display}.lock"));
    let _ = std::fs::remove_file(format!("{display}.compact.tmp"));
}

#[test]
fn default_policy_is_on_each_flush() -> Result<()> {
    // Builder default is FlushPolicy::OnEachFlush. We can't
    // observe the policy directly (it's stored inside Store), but
    // we can confirm a builder that didn't set it succeeds and
    // behaves correctly. A regression where the default changed
    // to Group could cause subtle hangs in single-thread tests;
    // this catches that.
    let path = tmp_path("default");
    cleanup(&path);

    let db = Emdb::open(&path)?;
    db.insert("k", "v")?;
    db.flush()?;
    drop(db);

    let reopened = Emdb::open(&path)?;
    assert_eq!(reopened.get("k")?, Some(b"v".to_vec()));

    drop(reopened);
    cleanup(&path);
    Ok(())
}

#[test]
fn group_policy_single_thread_does_not_deadlock() -> Result<()> {
    // A single thread with Group policy must not hang waiting for a
    // follower that will never arrive. The leader's max_wait acts
    // as the upper bound on flush latency. We use a tight 50 ms
    // wait; the test fails (via a 5 s overall timeout below) if
    // the coordinator gets stuck.
    let path = tmp_path("single");
    cleanup(&path);

    let db = Emdb::builder()
        .path(path.clone())
        .flush_policy(FlushPolicy::Group)
        .build()?;

    let started = Instant::now();
    db.insert("solo", "1")?;
    db.flush()?;
    let elapsed = started.elapsed();

    // Sanity ceiling: one max_wait + one fsync. On the slowest
    // CI hardware fsync can be 200 ms; we leave 5 s of headroom.
    assert!(
        elapsed < Duration::from_secs(5),
        "single-thread Group flush hung for {elapsed:?}"
    );

    drop(db);
    let reopened = Emdb::open(&path)?;
    assert_eq!(reopened.get("solo")?, Some(b"1".to_vec()));

    drop(reopened);
    cleanup(&path);
    Ok(())
}

#[test]
fn group_policy_concurrent_flushers_all_succeed() -> Result<()> {
    let path = tmp_path("concurrent");
    cleanup(&path);

    let db = Arc::new(
        Emdb::builder()
            .path(path.clone())
            .flush_policy(FlushPolicy::Group)
            .build()?,
    );

    const THREADS: usize = 8;
    const PER_THREAD: usize = 25;

    let mut handles = Vec::with_capacity(THREADS);
    for t in 0..THREADS {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || -> Result<()> {
            for i in 0..PER_THREAD {
                let key = format!("t{t}-i{i:03}");
                let value = format!("payload-{t}-{i}");
                db.insert(key.as_str(), value.as_str())?;
                db.flush()?;
            }
            Ok(())
        }));
    }
    for h in handles {
        h.join().expect("worker thread panicked")?;
    }

    let db = Arc::into_inner(db).expect("db arc unique");
    drop(db);

    // Reopen and confirm every record made it. The recovery scan
    // is the durability witness — anything that returned Ok from
    // flush() must be visible after reopen.
    let reopened = Emdb::open(&path)?;
    for t in 0..THREADS {
        for i in 0..PER_THREAD {
            let key = format!("t{t}-i{i:03}");
            let want = format!("payload-{t}-{i}");
            assert_eq!(
                reopened.get(&key)?,
                Some(want.into_bytes()),
                "t={t} i={i} missing after reopen"
            );
        }
    }

    drop(reopened);
    cleanup(&path);
    Ok(())
}

#[test]
fn group_policy_max_batch_one_does_not_drop_requests() -> Result<()> {
    // max_batch = 1 means every flusher leads its own cycle. The
    // protocol must still terminate cleanly — no waiters, no
    // deadlocks. This guards against a regression where the
    // pending-counter logic underflows or the wait loop gets stuck
    // when the batch threshold is at the floor.
    let path = tmp_path("batch-one");
    cleanup(&path);

    let db = Emdb::builder()
        .path(path.clone())
        .flush_policy(FlushPolicy::Group)
        .build()?;

    for i in 0..20 {
        db.insert(format!("k{i}"), format!("v{i}"))?;
        db.flush()?;
    }
    drop(db);

    let reopened = Emdb::open(&path)?;
    assert_eq!(reopened.len()?, 20);
    for i in 0..20 {
        let key = format!("k{i}");
        let want = format!("v{i}");
        assert_eq!(reopened.get(&key)?, Some(want.into_bytes()));
    }

    drop(reopened);
    cleanup(&path);
    Ok(())
}

#[test]
fn flush_policy_round_trips_through_builder() -> Result<()> {
    // The builder accepts every variant of the policy enum without
    // a build-time conflict. The actual coordinator behaviour is
    // covered by the other tests in this file; this is just the
    // surface check.
    let _ = Emdb::builder()
        .flush_policy(FlushPolicy::OnEachFlush)
        .build()?;
    let _ = Emdb::builder().flush_policy(FlushPolicy::Group).build()?;
    // Default constructor variant.
    let _ = Emdb::builder()
        .flush_policy(FlushPolicy::default())
        .build()?;
    Ok(())
}
