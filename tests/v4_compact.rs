// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Integration tests for the v0.7 compactor reached through
//! [`emdb::Emdb::compact`] on a `prefer_v4(true)` handle.

use emdb::{Emdb, EmdbBuilder, Result};

fn tmp_path(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |d| d.as_nanos());
    p.push(format!("emdb-v4-compact-{name}-{nanos}.emdb"));
    p
}

fn cleanup(path: &std::path::Path) {
    let _ = std::fs::remove_file(path);
    if let Some(parent) = path.parent() {
        if let Some(stem) = path.file_name().and_then(|n| n.to_str()) {
            let _ = std::fs::remove_file(parent.join(format!("{stem}.v4.wal")));
            let _ = std::fs::remove_file(parent.join(format!("{stem}.lock")));
            let _ = std::fs::remove_file(parent.join(format!("{stem}.wal")));
            let _ = std::fs::remove_file(parent.join(format!("{stem}.v3bak")));
            let _ = std::fs::remove_file(parent.join(format!("{stem}.v4tmp")));
        }
    }
}

fn open_v4(path: &std::path::Path) -> Result<Emdb> {
    EmdbBuilder::new()
        .path(path.to_path_buf())
        .prefer_v4(true)
        .build()
}

#[test]
fn compact_after_remove_keeps_remaining_records_readable() -> Result<()> {
    let path = tmp_path("after-remove");
    let db = open_v4(&path)?;

    // Insert a small set, remove half of them, compact, verify the
    // remaining keys still resolve.
    let total = 32_u32;
    for i in 0..total {
        db.insert(
            format!("k{i:03}").into_bytes(),
            format!("v{i:03}").into_bytes(),
        )?;
    }
    for i in 0..total {
        if i % 2 == 0 {
            let _ = db.remove(format!("k{i:03}").into_bytes())?;
        }
    }

    db.compact()?;

    for i in 0..total {
        let key = format!("k{i:03}");
        let want = format!("v{i:03}");
        let got = db.get(key.as_bytes())?;
        if i % 2 == 0 {
            assert!(
                got.is_none(),
                "even key {key} was removed; compact must not resurrect it"
            );
        } else {
            assert_eq!(
                got.as_deref(),
                Some(want.as_bytes()),
                "odd key {key} must survive compact"
            );
        }
    }
    let want_len = (total / 2) as usize;
    assert_eq!(db.len()?, want_len);

    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn compact_then_reopen_preserves_visible_records() -> Result<()> {
    let path = tmp_path("reopen");
    {
        let db = open_v4(&path)?;
        for i in 0_u32..16 {
            db.insert(
                format!("k{i:02}").into_bytes(),
                format!("v{i:02}").into_bytes(),
            )?;
        }
        for i in 0_u32..16 {
            if i.is_multiple_of(3) {
                let _ = db.remove(format!("k{i:02}").into_bytes())?;
            }
        }
        db.compact()?;
        // No explicit flush — compact() flushes internally.
    }

    let reopened = open_v4(&path)?;
    for i in 0_u32..16 {
        let key = format!("k{i:02}");
        let got = reopened.get(key.as_bytes())?;
        if i.is_multiple_of(3) {
            assert!(got.is_none(), "compacted-removed key {key} must not return");
        } else {
            let want = format!("v{i:02}");
            assert_eq!(
                got.as_deref(),
                Some(want.as_bytes()),
                "key {key} must survive compact + reopen"
            );
        }
    }
    drop(reopened);
    cleanup(&path);
    Ok(())
}

#[test]
fn compact_reclaims_dropped_namespace_pages() -> Result<()> {
    let path = tmp_path("dropped-ns");
    let db = open_v4(&path)?;

    // Allocate a few namespaces and seed them with data so each gets a
    // leaf chain. Drop one — its pages should be reclaimable on compact.
    let alpha = db.namespace("alpha")?;
    let beta = db.namespace("beta")?;
    for i in 0_u32..8 {
        alpha.insert(format!("a{i}").into_bytes(), b"x".to_vec())?;
        beta.insert(format!("b{i}").into_bytes(), b"y".to_vec())?;
    }

    let names_before = db.list_namespaces()?;
    assert!(names_before.contains(&"alpha".to_string()));
    assert!(names_before.contains(&"beta".to_string()));

    let dropped = db.drop_namespace("alpha")?;
    assert!(dropped);

    // After compact, the catalog should no longer carry the tombstoned
    // entry, and the surviving namespace should still answer reads.
    db.compact()?;
    let names_after = db.list_namespaces()?;
    assert!(
        !names_after.contains(&"alpha".to_string()),
        "compact should remove tombstoned catalog entry: {names_after:?}"
    );
    assert!(names_after.contains(&"beta".to_string()));

    let beta = db.namespace("beta")?;
    for i in 0_u32..8 {
        let key = format!("b{i}");
        let got = beta.get(key.as_bytes())?;
        assert_eq!(
            got.as_deref(),
            Some(b"y".as_slice()),
            "key {key} must survive"
        );
    }

    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn compact_recovers_space_via_free_list_reuse() -> Result<()> {
    let path = tmp_path("free-list");
    let db = open_v4(&path)?;

    // Fill a namespace with enough records to span multiple leaves, then
    // delete every record so each leaf becomes empty (every slot
    // tombstoned) — those leaves get unlinked and freed by compact().
    let scratch = db.namespace("scratch")?;
    let value = vec![b'p'; 256];
    for i in 0_u32..200 {
        scratch.insert(format!("k{i:04}").into_bytes(), value.clone())?;
    }
    db.flush()?;
    let bytes_after_fill = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

    for i in 0_u32..200 {
        let _ = scratch.remove(format!("k{i:04}").into_bytes())?;
    }
    db.compact()?;

    // The compactor reuses freed page ids when subsequent inserts run, so
    // a fresh round of inserts should not grow the page file by anywhere
    // near the original fill — proof that free-list reuse works.
    for i in 0_u32..50 {
        scratch.insert(format!("r{i:04}").into_bytes(), value.clone())?;
    }
    db.flush()?;
    let bytes_after_reuse = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

    assert!(
        bytes_after_reuse <= bytes_after_fill,
        "free-list reuse failed: {bytes_after_reuse} > {bytes_after_fill}"
    );

    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn compact_with_no_tombstones_is_a_noop() -> Result<()> {
    let path = tmp_path("noop");
    let db = open_v4(&path)?;
    db.insert(b"a", b"1")?;
    db.insert(b"b", b"2")?;
    db.compact()?;
    assert_eq!(db.get(b"a")?.as_deref(), Some(b"1".as_slice()));
    assert_eq!(db.get(b"b")?.as_deref(), Some(b"2".as_slice()));
    drop(db);
    cleanup(&path);
    Ok(())
}
