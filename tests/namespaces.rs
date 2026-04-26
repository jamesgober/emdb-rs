// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Integration tests for named-namespace persistence across reopen
//! and compaction. Named namespaces map a string to a u32 id; the
//! mapping is persisted via `TAG_NAMESPACE_NAME` records so reopens
//! see the same id every time.

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
    let _ = std::fs::remove_file(format!("{display}.encbak"));
}

#[test]
fn namespace_records_survive_reopen() -> Result<()> {
    let path = tmp_path("ns-roundtrip");
    {
        let db = Emdb::open(&path)?;
        let users = db.namespace("users")?;
        let sessions = db.namespace("sessions")?;
        users.insert(b"alice", b"1")?;
        users.insert(b"bob", b"2")?;
        sessions.insert(b"sid-1", b"abc")?;
        db.flush()?;
    }

    let db = Emdb::open(&path)?;
    let mut names = db.list_namespaces()?;
    names.sort();
    assert_eq!(
        names,
        vec!["".to_string(), "sessions".to_string(), "users".to_string()],
        "named namespaces should reappear after reopen"
    );

    let users = db.namespace("users")?;
    let sessions = db.namespace("sessions")?;
    assert_eq!(users.get(b"alice")?, Some(b"1".to_vec()));
    assert_eq!(users.get(b"bob")?, Some(b"2".to_vec()));
    assert_eq!(sessions.get(b"sid-1")?, Some(b"abc".to_vec()));

    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn namespace_id_stable_across_reopens() -> Result<()> {
    // Insert with a specific allocation order, reopen, insert with
    // reversed order — the second reopen would have allocated a
    // *different* id under the old (in-memory-only) scheme.
    let path = tmp_path("ns-id-stable");
    let alice_id = {
        let db = Emdb::open(&path)?;
        let alice = db.namespace("alice")?;
        let bob = db.namespace("bob")?;
        alice.insert(b"k", b"alice-value")?;
        bob.insert(b"k", b"bob-value")?;
        db.flush()?;
        // Capture the order we get on initial creation.
        let names = db.list_namespaces()?;
        let alice_pos = names.iter().position(|n| n == "alice").unwrap_or(0);
        let bob_pos = names.iter().position(|n| n == "bob").unwrap_or(0);
        assert!(alice_pos < bob_pos, "alice was registered before bob");
        alice_pos
    };

    // Reopen and resolve in REVERSE order. The persisted record-tagged
    // bindings must hand back alice's original id, not bob's.
    let db = Emdb::open(&path)?;
    let bob = db.namespace("bob")?;
    let alice = db.namespace("alice")?;
    assert_eq!(alice.get(b"k")?, Some(b"alice-value".to_vec()));
    assert_eq!(bob.get(b"k")?, Some(b"bob-value".to_vec()));
    let _ = alice_id;

    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn namespace_name_record_survives_compaction() -> Result<()> {
    let path = tmp_path("ns-compact");
    {
        let db = Emdb::open(&path)?;
        let users = db.namespace("users")?;
        users.insert(b"alice", b"1")?;
        users.insert(b"bob", b"2")?;
        let _removed = users.remove(b"alice")?;
        db.flush()?;

        // Compaction rewrites only live records — but it must also
        // re-emit the namespace name binding so the next reopen finds
        // the name → id mapping.
        db.compact()?;
        db.flush()?;
        // `users` (Namespace) holds an Arc<Inner>; let it drop with
        // the surrounding block so the LockFile actually releases
        // before we reopen the same path on Windows.
    }

    let db = Emdb::open(&path)?;
    let names = db.list_namespaces()?;
    assert!(
        names.iter().any(|n| n == "users"),
        "users namespace must reappear after compaction + reopen, got {names:?}"
    );
    let users = db.namespace("users")?;
    assert_eq!(users.get(b"alice")?, None);
    assert_eq!(users.get(b"bob")?, Some(b"2".to_vec()));

    drop(users);
    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn namespace_name_persists_with_inserts_only() -> Result<()> {
    // Edge case: namespace registered, no inserts, then drop + reopen.
    // The name binding record should still be there.
    let path = tmp_path("ns-empty");
    {
        let db = Emdb::open(&path)?;
        let _empty = db.namespace("empty-ns")?;
        db.flush()?;
    }
    let db = Emdb::open(&path)?;
    assert!(
        db.list_namespaces()?.iter().any(|n| n == "empty-ns"),
        "empty namespace should still appear after reopen"
    );
    drop(db);
    cleanup(&path);
    Ok(())
}

#[cfg(feature = "encrypt")]
#[test]
fn namespace_persistence_works_with_encryption() -> Result<()> {
    use emdb::EmdbBuilder;

    let path = tmp_path("ns-encrypted");
    let key = [7_u8; 32];
    {
        let db = EmdbBuilder::new()
            .path(path.clone())
            .encryption_key(key)
            .build()?;
        let secrets = db.namespace("secrets")?;
        secrets.insert(b"k", b"v")?;
        db.flush()?;
    }

    let db = EmdbBuilder::new()
        .path(path.clone())
        .encryption_key(key)
        .build()?;
    let names = db.list_namespaces()?;
    assert!(names.iter().any(|n| n == "secrets"));
    let secrets = db.namespace("secrets")?;
    assert_eq!(secrets.get(b"k")?, Some(b"v".to_vec()));

    drop(db);
    cleanup(&path);
    Ok(())
}
