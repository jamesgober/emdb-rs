// Integration tests for `Emdb::checkpoint()` — the explicit
// fast-reopen API added in v0.7.2. The contract under test is:
//
//   - Calling `checkpoint()` on an empty fresh database succeeds and
//     does not change observable state.
//   - After inserts, `checkpoint()` updates the on-disk header so
//     subsequent opens find a `tail_hint` matching the actual tail.
//   - Repeated `checkpoint()` calls with no intervening writes are
//     safe (idempotent).
//   - Records remain readable after a checkpoint + reopen cycle, with
//     no special handling on the consumer side.
//   - Drop-time backstop: even if the caller never calls
//     `checkpoint()` explicitly, dropping the last handle persists
//     the header so reopens still find the right tail. (This was
//     true before v0.7.2; the test guards against regressions in the
//     `Store::drop` path.)

use std::path::PathBuf;

use emdb::{Emdb, Result};

fn tmp_path(label: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |d| d.as_nanos());
    let tid = std::thread::current().id();
    p.push(format!("emdb-checkpoint-{label}-{nanos}-{tid:?}.emdb"));
    p
}

fn cleanup(path: &PathBuf) {
    let _ = std::fs::remove_file(path);
    let display = path.display();
    let _ = std::fs::remove_file(format!("{display}.lock"));
    let _ = std::fs::remove_file(format!("{display}.compact.tmp"));
}

#[test]
fn checkpoint_on_fresh_database_is_a_noop_for_observable_state() -> Result<()> {
    let path = tmp_path("fresh");
    cleanup(&path);

    let db = Emdb::open(&path)?;
    db.checkpoint()?;

    assert_eq!(db.len()?, 0);
    assert!(db.is_empty()?);

    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn checkpoint_after_inserts_lets_reopen_skip_the_log() -> Result<()> {
    let path = tmp_path("after-inserts");
    cleanup(&path);

    {
        let db = Emdb::open(&path)?;
        db.insert("k1", "v1")?;
        db.insert("k2", "v2")?;
        db.insert("k3", "v3")?;
        db.flush()?;
        db.checkpoint()?;
    }

    // Reopen and confirm everything is visible. The functional check
    // proves the recovery scan ends up in the same state regardless
    // of whether the hint was tight or loose; the practical effect
    // (faster scan) is hard to assert from public API but the
    // correctness invariant is the same one to guard.
    let reopened = Emdb::open(&path)?;
    assert_eq!(reopened.get("k1")?, Some(b"v1".to_vec()));
    assert_eq!(reopened.get("k2")?, Some(b"v2".to_vec()));
    assert_eq!(reopened.get("k3")?, Some(b"v3".to_vec()));
    assert_eq!(reopened.len()?, 3);

    drop(reopened);
    cleanup(&path);
    Ok(())
}

#[test]
fn repeated_checkpoint_is_idempotent() -> Result<()> {
    let path = tmp_path("idempotent");
    cleanup(&path);

    let db = Emdb::open(&path)?;
    db.insert("k", "v")?;
    db.flush()?;

    // Six calls in a row, no writes in between. Every one of them
    // should succeed and leave the file in the same state.
    for _ in 0..6 {
        db.checkpoint()?;
    }

    drop(db);

    let reopened = Emdb::open(&path)?;
    assert_eq!(reopened.get("k")?, Some(b"v".to_vec()));
    drop(reopened);

    cleanup(&path);
    Ok(())
}

#[test]
fn drop_time_backstop_persists_header_when_checkpoint_was_never_called() -> Result<()> {
    let path = tmp_path("drop-backstop");
    cleanup(&path);

    {
        let db = Emdb::open(&path)?;
        // Note: no explicit `checkpoint()` here. Only the drop-time
        // path should run. We do still call `flush()` so the record
        // bytes are durable; without that the OS might drop the
        // pwrite and the test isn't exercising what it thinks it
        // is.
        db.insert("user:1", "alice")?;
        db.insert("user:2", "bob")?;
        db.flush()?;
        // db drops here — the `Store::drop` impl best-effort-persists
        // the header.
    }

    let reopened = Emdb::open(&path)?;
    assert_eq!(reopened.get("user:1")?, Some(b"alice".to_vec()));
    assert_eq!(reopened.get("user:2")?, Some(b"bob".to_vec()));
    drop(reopened);

    cleanup(&path);
    Ok(())
}

#[test]
fn checkpoint_round_trip_preserves_every_record() -> Result<()> {
    let path = tmp_path("round-trip");
    cleanup(&path);

    // Spread enough records that the recovery scan has actual work
    // to do — small enough that the test stays fast, large enough
    // to exercise the "scan past header.tail_hint" branch in
    // `recovery_scan`.
    const N: u32 = 5_000;

    {
        let db = Emdb::open(&path)?;
        let pairs: Vec<(String, String)> = (0..N)
            .map(|i| (format!("key:{i:06}"), format!("value-{i}")))
            .collect();
        db.insert_many(pairs.iter().map(|(k, v)| (k.as_str(), v.as_str())))?;
        db.flush()?;
        db.checkpoint()?;
    }

    let db = Emdb::open(&path)?;
    assert_eq!(db.len()?, N as usize);
    for i in 0..N {
        let key = format!("key:{i:06}");
        let want = format!("value-{i}");
        assert_eq!(
            db.get(&key)?,
            Some(want.into_bytes()),
            "record {i} missing after reopen"
        );
    }
    drop(db);

    cleanup(&path);
    Ok(())
}
