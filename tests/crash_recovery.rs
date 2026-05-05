// Crash-recovery integration tests for v0.9.
//
// In v0.9 the journal substrate's framing + tail-truncation
// detection is owned by fsys (CRC-32C on every frame, five-state
// tail-state taxonomy). emdb's responsibility is to use fsys's
// reader correctly — i.e. records past a torn tail must not appear
// in the index after reopen.
//
// fsys ships its own per-method crash-safety test suite covering
// the lower-level concerns (write torn at any of three kill
// points, every durability method); duplicating that here would
// be wasted CI time. What we test here are the *integration*
// concerns specific to emdb:
//
//   - Truncated journal tail: file ends mid-frame. Recovery
//     surfaces only the records that fully landed; reopens
//     succeed.
//   - Stale checkpoint cursor: emdb's meta-sidecar `tail_hint`
//     equivalent is gone in v0.9 (fsys's resume-on-open uses
//     file size directly), so this scenario is no longer
//     applicable. The test we kept verifies the
//     checkpoint + reopen + read-everything contract on a
//     200-record dataset.

use std::fs::OpenOptions;
use std::path::PathBuf;

use emdb::{Emdb, Result};

fn tmp_path(label: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |d| d.as_nanos());
    let tid = std::thread::current().id();
    p.push(format!("emdb-crash-{label}-{nanos}-{tid:?}.emdb"));
    p
}

fn cleanup(path: &PathBuf) {
    let _ = std::fs::remove_file(path);
    let display = path.display();
    let _ = std::fs::remove_file(format!("{display}.lock"));
    let _ = std::fs::remove_file(format!("{display}.lock-meta"));
    let _ = std::fs::remove_file(format!("{display}.meta"));
    let _ = std::fs::remove_file(format!("{display}.compact.tmp"));
}

#[test]
fn truncated_journal_tail_recovers_only_complete_records() -> Result<()> {
    let path = tmp_path("truncated-tail");
    cleanup(&path);

    // Lay down a small set of records.
    {
        let db = Emdb::open(&path)?;
        for i in 0_u32..20 {
            db.insert(format!("k{i:02}"), format!("v{i}"))?;
        }
        db.flush()?;
    }

    // Truncate the journal mid-frame. Any byte count between
    // "first frame complete" and "tail of the file" is fine —
    // the goal is to cut a frame in half so fsys's reader has
    // to detect the partial tail. We pick a length that's
    // well into the file but not at a frame boundary.
    let original_size = std::fs::metadata(&path)?.len();
    let cut_to = (original_size - 5).max(20);
    {
        let file = OpenOptions::new().write(true).open(&path)?;
        file.set_len(cut_to)?;
    }

    // Reopen. fsys's JournalReader stops cleanly at the torn
    // frame. emdb sees only the records before the cut. The
    // exact number depends on payload size, but at least the
    // first record must survive and the reopen must succeed.
    let db = Emdb::open(&path)?;
    let recovered = db.len()?;
    assert!(
        recovered < 20,
        "expected fewer than 20 records after truncation, got {recovered}"
    );
    // The first record's payload is small; its frame fits in
    // any reasonable truncation point. Verify it's present.
    assert!(db.contains_key("k00")?);

    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn checkpoint_lets_recovery_skip_already_validated_records() -> Result<()> {
    // After `checkpoint()`, the meta sidecar's hint covers
    // everything written so far. The recovery scan still
    // validates every record (fsys's reader walks them all),
    // but the integration contract under test is: round-trip
    // through a checkpoint + reopen sees every record.
    let path = tmp_path("checkpoint-skip");
    cleanup(&path);

    {
        let db = Emdb::open(&path)?;
        for i in 0..200 {
            db.insert(format!("k{i:03}"), format!("v{i}"))?;
        }
        db.flush()?;
        db.checkpoint()?;
    }

    let db = Emdb::open(&path)?;
    assert_eq!(db.len()?, 200);
    for i in 0..200 {
        let key = format!("k{i:03}");
        let want = format!("v{i}");
        assert_eq!(db.get(&key)?, Some(want.into_bytes()));
    }

    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn reopen_after_clean_close_sees_every_record() -> Result<()> {
    // Sanity: a graceful close + reopen round-trips every
    // record. This is the canonical happy path that the
    // truncation tests above are the unhappy variant of.
    let path = tmp_path("clean-close");
    cleanup(&path);

    let mut expected: Vec<(String, String)> = Vec::new();
    {
        let db = Emdb::open(&path)?;
        for i in 0_u32..50 {
            let k = format!("rec-{i:03}");
            let v = format!("payload-{i}");
            db.insert(k.as_str(), v.as_str())?;
            expected.push((k, v));
        }
        db.flush()?;
        db.checkpoint()?;
    }

    let db = Emdb::open(&path)?;
    assert_eq!(db.len()?, expected.len());
    for (k, v) in &expected {
        assert_eq!(db.get(k.as_str())?, Some(v.as_bytes().to_vec()));
    }

    drop(db);
    cleanup(&path);
    Ok(())
}
