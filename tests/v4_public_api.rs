// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Integration tests for the v0.7 engine reached through the public
//! [`emdb::Emdb`] API.
//!
//! The v0.6 path (default builder) is exercised by every other
//! integration test in this directory; these tests pin behaviours that
//! are specific to `EmdbBuilder::prefer_v4(true)`.

use emdb::{Emdb, EmdbBuilder, Error, Result};

fn tmp_path(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |d| d.as_nanos());
    p.push(format!("emdb-v4-public-{name}-{nanos}.emdb"));
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
fn round_trip_insert_get_remove_via_v4() -> Result<()> {
    let path = tmp_path("round-trip");
    {
        let db = open_v4(&path)?;
        db.insert(b"alpha", b"one")?;
        db.insert(b"beta", b"two")?;
        db.insert(b"gamma", b"three")?;
        assert_eq!(db.get(b"alpha")?.as_deref(), Some(b"one".as_slice()));
        assert_eq!(db.get(b"beta")?.as_deref(), Some(b"two".as_slice()));
        assert_eq!(db.get(b"gamma")?.as_deref(), Some(b"three".as_slice()));
        assert!(db.contains_key(b"alpha")?);
        assert!(!db.contains_key(b"missing")?);
        assert_eq!(db.len()?, 3);

        let removed = db.remove(b"beta")?;
        assert_eq!(removed.as_deref(), Some(b"two".as_slice()));
        assert!(!db.contains_key(b"beta")?);
        assert_eq!(db.len()?, 2);

        db.flush()?;
    }
    cleanup(&path);
    Ok(())
}

#[test]
fn replay_recovers_durable_state_after_drop() -> Result<()> {
    let path = tmp_path("replay");
    {
        let db = open_v4(&path)?;
        for i in 0_u32..64 {
            let key = format!("k{i:03}");
            let value = format!("v{i:03}");
            db.insert(key.as_bytes(), value.as_bytes())?;
        }
        db.flush()?;
    }

    let reopened = open_v4(&path)?;
    for i in 0_u32..64 {
        let key = format!("k{i:03}");
        let fetched = reopened.get(key.as_bytes())?;
        let want = format!("v{i:03}");
        assert_eq!(fetched.as_deref(), Some(want.as_bytes()));
    }
    assert_eq!(reopened.len()?, 64);
    drop(reopened);
    cleanup(&path);
    Ok(())
}

#[test]
fn iter_and_keys_yield_every_record() -> Result<()> {
    let path = tmp_path("iter");
    let db = open_v4(&path)?;
    let inputs = [
        (b"a".to_vec(), b"1".to_vec()),
        (b"b".to_vec(), b"2".to_vec()),
        (b"c".to_vec(), b"3".to_vec()),
    ];
    for (k, v) in &inputs {
        db.insert(k.clone(), v.clone())?;
    }

    let mut iterated: Vec<(Vec<u8>, Vec<u8>)> = db.iter()?.collect();
    iterated.sort();
    let mut expected = inputs.to_vec();
    expected.sort();
    assert_eq!(iterated, expected);

    let mut keys: Vec<Vec<u8>> = db.keys()?.collect();
    keys.sort();
    let mut expected_keys: Vec<Vec<u8>> = expected.iter().map(|(k, _)| k.clone()).collect();
    expected_keys.sort();
    assert_eq!(keys, expected_keys);

    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn clear_drops_every_record_and_resets_len() -> Result<()> {
    let path = tmp_path("clear");
    let db = open_v4(&path)?;
    for i in 0_u32..16 {
        db.insert(format!("k{i}").into_bytes(), format!("v{i}").into_bytes())?;
    }
    assert_eq!(db.len()?, 16);
    db.clear()?;
    assert_eq!(db.len()?, 0);
    assert!(!db.contains_key(b"k0")?);
    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn transaction_commit_applies_overlay_via_v4() -> Result<()> {
    let path = tmp_path("tx-commit");
    let db = open_v4(&path)?;
    db.transaction(|tx| {
        tx.insert(b"a", b"1")?;
        tx.insert(b"b", b"2")?;
        tx.insert(b"c", b"3")?;
        Ok(())
    })?;
    assert_eq!(db.get(b"a")?.as_deref(), Some(b"1".as_slice()));
    assert_eq!(db.get(b"b")?.as_deref(), Some(b"2".as_slice()));
    assert_eq!(db.get(b"c")?.as_deref(), Some(b"3".as_slice()));
    assert_eq!(db.len()?, 3);
    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn transaction_rollback_discards_overlay_via_v4() -> Result<()> {
    let path = tmp_path("tx-rollback");
    let db = open_v4(&path)?;
    let result: Result<()> = db.transaction(|tx| {
        tx.insert(b"a", b"1")?;
        tx.insert(b"b", b"2")?;
        Err(Error::TransactionAborted("rollback"))
    });
    assert!(matches!(result, Err(Error::TransactionAborted(_))));
    assert!(
        db.get(b"a")?.is_none(),
        "rolled-back keys must not be visible"
    );
    assert!(db.get(b"b")?.is_none());
    assert_eq!(db.len()?, 0);
    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn transaction_read_your_writes_via_v4() -> Result<()> {
    let path = tmp_path("tx-ryw");
    let db = open_v4(&path)?;
    db.insert(b"persisted", b"old")?;

    db.transaction(|tx| {
        // The overlay shadows the persisted key.
        tx.insert(b"persisted", b"new")?;
        assert_eq!(tx.get(b"persisted")?.as_deref(), Some(b"new".as_slice()));

        // Keys not in the overlay fall through to the v4 engine.
        tx.insert(b"fresh", b"value")?;
        assert_eq!(tx.get(b"fresh")?.as_deref(), Some(b"value".as_slice()));

        // remove() returns the previously visible value.
        let removed = tx.remove(b"persisted")?;
        assert_eq!(removed.as_deref(), Some(b"new".as_slice()));
        assert!(tx.get(b"persisted")?.is_none());
        Ok(())
    })?;

    // After commit: removed key gone, fresh key present.
    assert!(db.get(b"persisted")?.is_none());
    assert_eq!(db.get(b"fresh")?.as_deref(), Some(b"value".as_slice()));
    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn transaction_survives_drop_and_reopen_via_v4() -> Result<()> {
    let path = tmp_path("tx-replay");
    {
        let db = open_v4(&path)?;
        db.transaction(|tx| {
            for i in 0_u32..16 {
                let key = format!("k{i:02}");
                let value = format!("v{i:02}");
                tx.insert(key.as_bytes(), value.as_bytes())?;
            }
            Ok(())
        })?;
        // No flush — rely on the WAL fsync inside commit_batch and on
        // replay-on-open to recover the batch.
    }

    let reopened = open_v4(&path)?;
    for i in 0_u32..16 {
        let key = format!("k{i:02}");
        let want = format!("v{i:02}");
        assert_eq!(
            reopened.get(key.as_bytes())?.as_deref(),
            Some(want.as_bytes()),
            "key {key} should be recovered through batch replay"
        );
    }
    assert_eq!(reopened.len()?, 16);
    drop(reopened);
    cleanup(&path);
    Ok(())
}

#[test]
fn empty_transaction_is_a_noop_via_v4() -> Result<()> {
    let path = tmp_path("tx-empty");
    let db = open_v4(&path)?;
    db.transaction(|_tx| Ok(()))?;
    assert_eq!(db.len()?, 0);
    drop(db);
    cleanup(&path);
    Ok(())
}

#[cfg(feature = "ttl")]
#[test]
fn ttl_path_round_trips_through_v4() -> Result<()> {
    use std::time::Duration;

    use emdb::Ttl;

    let path = tmp_path("ttl");
    let db = open_v4(&path)?;

    // Insert with explicit TTL; the engine stores expires_at on the
    // record and exposes it through expires_at()/ttl().
    db.insert_with_ttl(b"session", b"token", Ttl::After(Duration::from_secs(60)))?;
    let ttl = db.ttl(b"session")?;
    assert!(
        ttl.is_some(),
        "ttl should be reported for an insert with Ttl::After"
    );

    let exp = db.expires_at(b"session")?;
    assert!(
        exp.is_some(),
        "expires_at should be reported for a TTL'd record"
    );

    // persist() removes the TTL.
    let persisted = db.persist(b"session")?;
    assert!(
        persisted,
        "persist should report true on a previously TTL-bearing key"
    );
    let ttl_after = db.ttl(b"session")?;
    assert!(ttl_after.is_none(), "persisted record should have no TTL");

    drop(db);
    cleanup(&path);
    Ok(())
}
