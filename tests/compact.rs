// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Integration tests for `Emdb::compact()`. The append-only log accumulates
//! tombstoned + superseded records until a compaction pass rewrites only
//! the live records and shrinks the file.

use std::collections::BTreeMap;

use emdb::{Emdb, Result};

fn tmp_path(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |d| d.as_nanos());
    p.push(format!("emdb-{name}-{nanos}.emdb"));
    p
}

fn cleanup(path: &std::path::Path) {
    let _ = std::fs::remove_file(path);
    let display = path.display();
    let _ = std::fs::remove_file(format!("{display}.lock"));
    let _ = std::fs::remove_file(format!("{display}.compact.tmp"));
    let _ = std::fs::remove_file(format!("{display}.compact.tmp.lock"));
}

#[test]
fn compact_shrinks_file_and_preserves_live_state() -> Result<()> {
    let path = tmp_path("compact-shrink");

    let mut expected = BTreeMap::new();
    {
        let db = Emdb::open(&path)?;

        for i in 0_u32..1_000 {
            let key = format!("k{i}");
            let value = format!("value-{i:04}-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
            db.insert(key.as_bytes(), value.as_bytes())?;
            let _ = expected.insert(key, value.into_bytes());
        }

        // Remove most of them — these add tombstone records to the log.
        for i in 0_u32..800 {
            let key = format!("k{i}");
            let _removed = db.remove(&key)?;
            let _ = expected.remove(&key);
        }

        // Overwrite a slice — these add superseded records.
        for i in 800_u32..900 {
            let key = format!("k{i}");
            let value = format!("updated-{i:04}-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
            db.insert(key.as_bytes(), value.as_bytes())?;
            let _ = expected.insert(key, value.into_bytes());
        }

        db.flush()?;

        let before = std::fs::metadata(&path)?.len();
        db.compact()?;
        db.flush()?;
        let after = std::fs::metadata(&path)?.len();

        assert!(
            after < before,
            "compaction should shrink file: before={before} after={after}"
        );

        // Live state is preserved through the in-process compaction.
        assert_eq!(db.len()?, expected.len());
        for (k, v) in &expected {
            assert_eq!(
                db.get(k.as_bytes())?,
                Some(v.clone()),
                "key {k:?} mismatch right after compact"
            );
        }
    }

    // Reopen — survives compaction across restarts.
    let db = Emdb::open(&path)?;
    assert_eq!(db.len()?, expected.len());
    for (k, v) in &expected {
        assert_eq!(
            db.get(k.as_bytes())?,
            Some(v.clone()),
            "key {k:?} mismatch after reopen"
        );
    }

    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn compact_on_empty_db_is_noop() -> Result<()> {
    let path = tmp_path("compact-empty");
    let db = Emdb::open(&path)?;
    db.compact()?;
    assert_eq!(db.len()?, 0);
    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn compact_preserves_named_namespaces() -> Result<()> {
    let path = tmp_path("compact-namespaces");
    let db = Emdb::open(&path)?;

    let users = db.namespace("users")?;
    let sessions = db.namespace("sessions")?;
    users.insert(b"alice", b"a")?;
    users.insert(b"bob", b"b")?;
    sessions.insert(b"sid-1", b"abc")?;
    sessions.insert(b"sid-2", b"def")?;
    sessions.remove(b"sid-1")?;

    db.flush()?;
    db.compact()?;

    // After compact the live state is preserved per namespace.
    let users = db.namespace("users")?;
    let sessions = db.namespace("sessions")?;
    assert_eq!(users.get(b"alice")?, Some(b"a".to_vec()));
    assert_eq!(users.get(b"bob")?, Some(b"b".to_vec()));
    assert_eq!(sessions.get(b"sid-1")?, None);
    assert_eq!(sessions.get(b"sid-2")?, Some(b"def".to_vec()));

    drop(db);
    cleanup(&path);
    Ok(())
}
