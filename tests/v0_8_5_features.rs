// Integration tests for the v0.8.5 production-polish features:
//
//   1. `Emdb::stats()` / `EmdbStats`
//   2. `Emdb::backup_to(path)` atomic snapshot
//   3. `Emdb::lock_holder` / `Emdb::break_lock` lockfile admin
//   4. `FlushPolicy::WriteThrough` synchronous-write opt-in
//   5. `iter_from` / `iter_after` cursor-style iteration
//
// Each suite below covers the happy path plus the edge cases that
// real callers will hit: empty inputs, missing files, repeated
// calls, and cross-platform behaviour where it differs.

use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use emdb::{Emdb, FlushPolicy, Result};

fn tmp_path(label: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |d| d.as_nanos());
    let tid = std::thread::current().id();
    p.push(format!("emdb-085-{label}-{nanos}-{tid:?}.emdb"));
    p
}

fn cleanup(path: &PathBuf) {
    let _ = std::fs::remove_file(path);
    let display = path.display();
    let _ = std::fs::remove_file(format!("{display}.lock"));
    let _ = std::fs::remove_file(format!("{display}.lock-meta"));
    let _ = std::fs::remove_file(format!("{display}.compact.tmp"));
    let _ = std::fs::remove_file(format!("{display}.backup.tmp"));
}

// ---------- 1. stats() ------------------------------------------------

#[test]
fn stats_on_empty_db_reports_zero_records() -> Result<()> {
    let db = Emdb::open_in_memory();
    let stats = db.stats()?;
    assert_eq!(stats.live_records, 0);
    assert_eq!(stats.namespace_count, 0);
    assert!(stats.file_size_bytes >= stats.logical_size_bytes);
    assert!(!stats.encrypted);
    assert!(!stats.range_scans_enabled);
    Ok(())
}

#[test]
fn stats_counts_records_across_namespaces() -> Result<()> {
    let db = Emdb::open_in_memory();
    db.insert("default-1", "v")?;
    db.insert("default-2", "v")?;

    let users = db.namespace("users")?;
    users.insert("alice", "v")?;
    users.insert("bob", "v")?;
    users.insert("carol", "v")?;

    let stats = db.stats()?;
    assert_eq!(stats.live_records, 5);
    assert_eq!(stats.namespace_count, 1);
    Ok(())
}

#[test]
fn stats_reports_range_scans_flag_when_enabled() -> Result<()> {
    let db = Emdb::builder().enable_range_scans(true).build()?;
    let stats = db.stats()?;
    assert!(stats.range_scans_enabled);
    Ok(())
}

#[test]
fn stats_logical_size_grows_with_inserts() -> Result<()> {
    let path = tmp_path("stats-grow");
    cleanup(&path);
    let db = Emdb::open(&path)?;

    let initial = db.stats()?.logical_size_bytes;
    db.insert("k", &vec![b'x'; 256][..])?;
    let after = db.stats()?.logical_size_bytes;

    assert!(
        after > initial,
        "logical size should grow after insert: initial={initial}, after={after}"
    );

    drop(db);
    cleanup(&path);
    Ok(())
}

// ---------- 2. backup_to() --------------------------------------------

#[test]
fn backup_to_writes_a_loadable_database() -> Result<()> {
    let source = tmp_path("backup-source");
    let backup = tmp_path("backup-dest");
    cleanup(&source);
    cleanup(&backup);

    {
        let db = Emdb::open(&source)?;
        db.insert("alpha", "1")?;
        db.insert("beta", "2")?;
        db.insert("gamma", "3")?;
        db.flush()?;

        db.backup_to(&backup)?;
    }

    // The backup must be a complete, openable database carrying
    // every record from the source.
    let restored = Emdb::open(&backup)?;
    assert_eq!(restored.get("alpha")?, Some(b"1".to_vec()));
    assert_eq!(restored.get("beta")?, Some(b"2".to_vec()));
    assert_eq!(restored.get("gamma")?, Some(b"3".to_vec()));
    assert_eq!(restored.len()?, 3);

    drop(restored);
    cleanup(&source);
    cleanup(&backup);
    Ok(())
}

#[test]
fn backup_to_preserves_named_namespaces() -> Result<()> {
    let source = tmp_path("backup-ns-source");
    let backup = tmp_path("backup-ns-dest");
    cleanup(&source);
    cleanup(&backup);

    {
        let db = Emdb::open(&source)?;
        let users = db.namespace("users")?;
        users.insert("alice", "v1")?;
        users.insert("bob", "v2")?;
        let sessions = db.namespace("sessions")?;
        sessions.insert("token", "v3")?;
        db.flush()?;

        db.backup_to(&backup)?;
    }

    let restored = Emdb::open(&backup)?;
    let users = restored.namespace("users")?;
    let sessions = restored.namespace("sessions")?;
    assert_eq!(users.get("alice")?, Some(b"v1".to_vec()));
    assert_eq!(users.get("bob")?, Some(b"v2".to_vec()));
    assert_eq!(sessions.get("token")?, Some(b"v3".to_vec()));

    drop(restored);
    cleanup(&source);
    cleanup(&backup);
    Ok(())
}

#[test]
fn backup_to_self_path_is_rejected() -> Result<()> {
    use emdb::Error;
    let source = tmp_path("backup-self");
    cleanup(&source);
    let db = Emdb::open(&source)?;
    let result = db.backup_to(&source);
    assert!(
        matches!(result, Err(Error::InvalidConfig(_))),
        "expected InvalidConfig, got {result:?}"
    );
    drop(db);
    cleanup(&source);
    Ok(())
}

#[test]
fn backup_to_overwrites_existing_target() -> Result<()> {
    let source = tmp_path("backup-overwrite-source");
    let backup = tmp_path("backup-overwrite-dest");
    cleanup(&source);
    cleanup(&backup);

    // First backup with one set of records.
    {
        let db = Emdb::open(&source)?;
        db.insert("first", "v")?;
        db.flush()?;
        db.backup_to(&backup)?;
    }
    cleanup(&source);

    // Second backup of a different db — should overwrite.
    {
        let db = Emdb::open(&source)?;
        db.insert("second", "v")?;
        db.flush()?;
        db.backup_to(&backup)?;
    }

    let restored = Emdb::open(&backup)?;
    assert_eq!(restored.get("first")?, None);
    assert_eq!(restored.get("second")?, Some(b"v".to_vec()));
    drop(restored);
    cleanup(&source);
    cleanup(&backup);
    Ok(())
}

// ---------- 3. lockfile admin ------------------------------------------

#[test]
fn lock_holder_returns_pid_while_held() -> Result<()> {
    let path = tmp_path("lockfile-holder");
    cleanup(&path);

    let db = Emdb::open(&path)?;
    let holder = Emdb::lock_holder(&path)?.expect("holder present");
    assert_eq!(holder.pid, std::process::id());
    assert!(holder.acquired_at_unix_millis > 0);
    assert_eq!(holder.schema_version, 1);
    drop(db);

    // After graceful release, the meta sidecar is gone.
    let after = Emdb::lock_holder(&path)?;
    assert!(after.is_none());

    cleanup(&path);
    Ok(())
}

#[test]
fn lock_holder_on_unlocked_path_returns_none() -> Result<()> {
    let path = tmp_path("lockfile-no-holder");
    cleanup(&path);
    let holder = Emdb::lock_holder(&path)?;
    assert!(holder.is_none());
    Ok(())
}

#[test]
fn break_lock_recovers_a_stuck_database() -> Result<()> {
    let path = tmp_path("break-stuck");
    cleanup(&path);

    // Simulate a stuck lockfile by hand: open the db, acquire,
    // then forget the handle so Drop never runs.
    let db = Emdb::open(&path)?;
    db.insert("k", "v")?;
    db.flush()?;
    std::mem::forget(db);

    // A second open must fail because the lock is still held.
    let blocked = Emdb::open(&path);
    assert!(blocked.is_err(), "expected lock contention");

    // After breaking the lock manually, a fresh open succeeds.
    Emdb::break_lock(&path)?;
    let reopened = Emdb::open(&path)?;
    assert_eq!(reopened.get("k")?, Some(b"v".to_vec()));

    drop(reopened);
    cleanup(&path);
    Ok(())
}

#[test]
fn break_lock_is_idempotent() -> Result<()> {
    let path = tmp_path("break-idempotent");
    cleanup(&path);
    Emdb::break_lock(&path)?;
    Emdb::break_lock(&path)?;
    Emdb::break_lock(&path)?;
    Ok(())
}

// ---------- 4. FlushPolicy::WriteThrough --------------------------------

#[test]
fn write_through_round_trip_works() -> Result<()> {
    let path = tmp_path("write-through-rt");
    cleanup(&path);

    {
        let db = Emdb::builder()
            .path(path.clone())
            .flush_policy(FlushPolicy::WriteThrough)
            .build()?;
        db.insert("k1", "v1")?;
        db.insert("k2", "v2")?;
    }

    let reopened = Emdb::open(&path)?;
    assert_eq!(reopened.get("k1")?, Some(b"v1".to_vec()));
    assert_eq!(reopened.get("k2")?, Some(b"v2".to_vec()));
    drop(reopened);
    cleanup(&path);
    Ok(())
}

#[test]
fn write_through_concurrent_writers_remain_serialised() -> Result<()> {
    // The writer mutex is unchanged under WriteThrough — only the
    // file open flags differ. Concurrent inserts must still all
    // succeed and round-trip through reopen.
    let path = tmp_path("write-through-mt");
    cleanup(&path);

    let db = Arc::new(
        Emdb::builder()
            .path(path.clone())
            .flush_policy(FlushPolicy::WriteThrough)
            .build()?,
    );

    let mut handles = Vec::new();
    for thread_id in 0_u32..4 {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || -> Result<()> {
            for record_id in 0_u32..50 {
                db.insert(format!("t{thread_id}-r{record_id:03}"), "x")?;
            }
            Ok(())
        }));
    }
    for h in handles {
        h.join().expect("thread join")?;
    }

    let db = Arc::into_inner(db).expect("unique arc");
    drop(db);

    let reopened = Emdb::open(&path)?;
    assert_eq!(reopened.len()?, 4 * 50);
    drop(reopened);
    cleanup(&path);
    Ok(())
}

#[test]
fn write_through_does_not_deadlock_on_flush() -> Result<()> {
    // Belt-and-braces: even though `flush()` becomes near-free
    // under WriteThrough, calling it explicitly must still
    // terminate (not deadlock on its own sync handle).
    let path = tmp_path("write-through-flush");
    cleanup(&path);

    let db = Emdb::builder()
        .path(path.clone())
        .flush_policy(FlushPolicy::WriteThrough)
        .build()?;
    for i in 0..20 {
        db.insert(format!("k{i}"), "v")?;
        db.flush()?;
    }

    drop(db);
    cleanup(&path);
    Ok(())
}

// ---------- 5. iter_from / iter_after -----------------------------------

#[test]
fn iter_from_yields_keys_at_or_after_start() -> Result<()> {
    let db = Emdb::builder().enable_range_scans(true).build()?;
    db.insert("a", "1")?;
    db.insert("b", "2")?;
    db.insert("c", "3")?;
    db.insert("d", "4")?;

    let keys: Vec<Vec<u8>> = db.iter_from("b")?.map(|(k, _)| k).collect();
    assert_eq!(keys, vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]);
    Ok(())
}

#[test]
fn iter_after_skips_the_start_key() -> Result<()> {
    let db = Emdb::builder().enable_range_scans(true).build()?;
    db.insert("a", "1")?;
    db.insert("b", "2")?;
    db.insert("c", "3")?;

    let keys: Vec<Vec<u8>> = db.iter_after("a")?.map(|(k, _)| k).collect();
    assert_eq!(keys, vec![b"b".to_vec(), b"c".to_vec()]);
    Ok(())
}

#[test]
fn iter_from_supports_pagination_pattern() -> Result<()> {
    // Realistic use case: paginated listing where the cursor is
    // the last key already returned. Each page resumes via
    // `iter_after(cursor)`.
    let db = Emdb::builder().enable_range_scans(true).build()?;
    for i in 0_u32..50 {
        db.insert(format!("rec-{i:03}"), "v")?;
    }

    let mut cursor: Option<Vec<u8>> = None;
    let mut pages: Vec<Vec<Vec<u8>>> = Vec::new();
    let page_size = 10;
    loop {
        let iter = match cursor.as_ref() {
            Some(c) => db.iter_after(c)?,
            None => db.iter_from("")?,
        };
        let page: Vec<Vec<u8>> = iter.take(page_size).map(|(k, _)| k).collect();
        if page.is_empty() {
            break;
        }
        cursor = page.last().cloned();
        pages.push(page);
    }

    assert_eq!(pages.len(), 5, "50 records / 10 per page = 5 pages");
    assert_eq!(pages[0][0], b"rec-000".to_vec());
    assert_eq!(pages[4][9], b"rec-049".to_vec());
    Ok(())
}

#[test]
fn iter_from_empty_result_when_start_past_last_key() -> Result<()> {
    let db = Emdb::builder().enable_range_scans(true).build()?;
    db.insert("alpha", "1")?;
    db.insert("beta", "2")?;

    let keys: Vec<_> = db.iter_from("zzz")?.collect();
    assert!(keys.is_empty());
    Ok(())
}

#[test]
fn iter_from_without_range_scans_returns_invalid_config() -> Result<()> {
    use emdb::Error;
    let db = Emdb::open_in_memory();
    let result = db.iter_from("k");
    assert!(matches!(result, Err(Error::InvalidConfig(_))));
    Ok(())
}

#[test]
fn namespace_iter_from_works_independently() -> Result<()> {
    let db = Emdb::builder().enable_range_scans(true).build()?;
    let users = db.namespace("users")?;
    users.insert("a", "1")?;
    users.insert("b", "2")?;
    users.insert("c", "3")?;

    // Default namespace has unrelated content.
    db.insert("x", "y")?;

    let keys: Vec<_> = users.iter_from("b")?.map(|(k, _)| k).collect();
    assert_eq!(keys, vec![b"b".to_vec(), b"c".to_vec()]);
    Ok(())
}

#[test]
fn iter_from_thread_safe_under_concurrent_inserts() -> Result<()> {
    // The iterator snapshots offsets at construction time; new
    // inserts after construction are not visible. This test
    // confirms the snapshot semantics survive concurrent writers.
    let db = Arc::new(Emdb::builder().enable_range_scans(true).build()?);
    for i in 0_u32..100 {
        db.insert(format!("k{i:03}"), "v")?;
    }

    let iter_db = Arc::clone(&db);
    let reader = thread::spawn(move || -> Result<usize> {
        // Sleep a bit so the writer thread has started.
        thread::sleep(Duration::from_millis(10));
        let n = iter_db.iter_from("k000")?.count();
        Ok(n)
    });

    let writer_db = Arc::clone(&db);
    let writer = thread::spawn(move || -> Result<()> {
        for i in 100_u32..200 {
            writer_db.insert(format!("k{i:03}"), "v")?;
        }
        Ok(())
    });

    let count = reader.join().expect("reader join")?;
    writer.join().expect("writer join")?;

    // The snapshot saw at least the first 100 keys. Concurrent
    // writes add records the iterator may or may not see (snapshot
    // semantics permit either). What matters is no panic, no
    // deadlock, no lost records.
    assert!(count >= 100);

    let final_count = db.len()?;
    assert_eq!(final_count, 200);
    Ok(())
}
