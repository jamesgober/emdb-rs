// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Integration tests for OS-default storage-path resolution exposed
//! via [`emdb::EmdbBuilder::app_name`], [`EmdbBuilder::database_name`],
//! and [`EmdbBuilder::data_root`].

use emdb::{EmdbBuilder, Error, Result};

fn temp_root(label: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |d| d.as_nanos());
    p.push(format!("emdb-storage-path-{label}-{nanos}"));
    p
}

#[test]
fn explicit_data_root_with_named_app_and_db_round_trips() -> Result<()> {
    let root = temp_root("v06-roundtrip");

    {
        let db = EmdbBuilder::new()
            .data_root(root.clone())
            .app_name("hive")
            .database_name("sessions.emdb")
            .build()?;
        db.insert(b"k", b"v")?;
        db.flush()?;
    }

    // The file landed exactly where the resolver promised it would.
    let expected = root.join("hive").join("sessions.emdb");
    assert!(
        expected.exists(),
        "database file should exist at {expected:?}"
    );

    // Reopening the same builder resolves to the same path and recovers
    // the previously-written record.
    {
        let reopened = EmdbBuilder::new()
            .data_root(root.clone())
            .app_name("hive")
            .database_name("sessions.emdb")
            .build()?;
        assert_eq!(reopened.get(b"k")?.as_deref(), Some(b"v".as_slice()));
    }

    let _removed = std::fs::remove_dir_all(&root);
    Ok(())
}

#[test]
fn dash_joined_app_name_round_trips() -> Result<()> {
    // Embedders that want a multi-level brand layout pre-compose it
    // with a single dash-joined identifier (or, for true nesting,
    // with `data_root` pointing at a parent folder). This test pins
    // the recommended pattern so it stays reachable through the
    // public API.
    let root = temp_root("dash-joined");

    {
        let db = EmdbBuilder::new()
            .data_root(root.clone())
            .app_name("hivedb-kv")
            .database_name("sessions.emdb")
            .build()?;
        db.insert(b"session-1", b"alpha")?;
        db.flush()?;
    }

    let expected = root.join("hivedb-kv").join("sessions.emdb");
    assert!(
        expected.exists(),
        "dash-joined app folder should land at {expected:?}"
    );

    let reopened = EmdbBuilder::new()
        .data_root(root.clone())
        .app_name("hivedb-kv")
        .database_name("sessions.emdb")
        .build()?;
    assert_eq!(
        reopened.get(b"session-1")?.as_deref(),
        Some(b"alpha".as_slice())
    );

    drop(reopened);
    let _removed = std::fs::remove_dir_all(&root);
    Ok(())
}

#[test]
fn explicit_data_root_round_trips_through_v07() -> Result<()> {
    let root = temp_root("v07-roundtrip");

    {
        let db = EmdbBuilder::new()
            .data_root(root.clone())
            .app_name("hive")
            .database_name("events.emdb")
            .build()?;
        db.insert(b"alpha", b"one")?;
        db.insert(b"beta", b"two")?;
        db.flush()?;
    }

    let expected = root.join("hive").join("events.emdb");
    assert!(expected.exists(), "v0.7 db should land at {expected:?}");

    let reopened = EmdbBuilder::new()
        .data_root(root.clone())
        .app_name("hive")
        .database_name("events.emdb")
        .build()?;
    assert_eq!(reopened.get(b"alpha")?.as_deref(), Some(b"one".as_slice()));
    assert_eq!(reopened.get(b"beta")?.as_deref(), Some(b"two".as_slice()));

    drop(reopened);
    let _removed = std::fs::remove_dir_all(&root);
    Ok(())
}

#[test]
fn defaults_apply_when_only_data_root_is_set() -> Result<()> {
    let root = temp_root("defaults");

    let _db = EmdbBuilder::new().data_root(root.clone()).build()?;

    // Default app_name = "emdb"; default database_name = "emdb-default.emdb".
    let expected = root.join("emdb").join("emdb-default.emdb");
    assert!(
        expected.exists(),
        "default-named database should exist at {expected:?}"
    );

    let _removed = std::fs::remove_dir_all(&root);
    Ok(())
}

#[test]
fn explicit_path_with_os_resolution_methods_is_rejected() {
    let result = EmdbBuilder::new()
        .path("/tmp/should-not-be-used.emdb")
        .app_name("hive")
        .build();
    assert!(matches!(result, Err(Error::InvalidConfig(_))));

    let result_db = EmdbBuilder::new()
        .path("/tmp/should-not-be-used.emdb")
        .database_name("foo.emdb")
        .build();
    assert!(matches!(result_db, Err(Error::InvalidConfig(_))));

    let result_root = EmdbBuilder::new()
        .path("/tmp/should-not-be-used.emdb")
        .data_root("/tmp/some-root")
        .build();
    assert!(matches!(result_root, Err(Error::InvalidConfig(_))));
}

#[test]
fn path_separator_in_app_name_is_rejected() {
    // Both `/` and `\` are rejected — `app_name` is intentionally a
    // single folder name to avoid platform-translation complexity.
    let root = temp_root("sep-app");
    for bad in ["hive/inner", "hive\\inner", "..", "data/.."] {
        let result = EmdbBuilder::new()
            .data_root(root.clone())
            .app_name(bad)
            .build();
        assert!(
            matches!(result, Err(Error::InvalidConfig(_))),
            "expected InvalidConfig for app_name={bad:?}"
        );
    }
    let _removed = std::fs::remove_dir_all(&root);
}

#[test]
fn path_separator_in_database_name_is_rejected() {
    let root = temp_root("sep-db");
    let result = EmdbBuilder::new()
        .data_root(root.clone())
        .database_name("nested/file.emdb")
        .build();
    assert!(matches!(result, Err(Error::InvalidConfig(_))));
    let _removed = std::fs::remove_dir_all(&root);
}

#[test]
fn missing_intermediate_directory_is_created() -> Result<()> {
    // The data_root itself does not yet exist; the resolver must
    // mkdir -p the full chain before opening the file.
    let root = temp_root("mkdir-p");
    assert!(!root.exists(), "precondition: root should not pre-exist");

    let _db = EmdbBuilder::new()
        .data_root(root.clone())
        .app_name("acme")
        .database_name("billing.emdb")
        .build()?;

    let expected = root.join("acme").join("billing.emdb");
    assert!(expected.exists(), "expected {expected:?} to exist");

    let _removed = std::fs::remove_dir_all(&root);
    Ok(())
}

#[test]
fn two_apps_under_one_root_do_not_collide() -> Result<()> {
    let root = temp_root("multi-app");

    let billing = EmdbBuilder::new()
        .data_root(root.clone())
        .app_name("acme-billing")
        .database_name("invoices.emdb")
        .build()?;
    billing.insert(b"id-1", b"$100")?;

    let analytics = EmdbBuilder::new()
        .data_root(root.clone())
        .app_name("acme-analytics")
        .database_name("events.emdb")
        .build()?;
    analytics.insert(b"id-1", b"signup")?;

    // Same key, two namespaces under different folders — independent values.
    assert_eq!(billing.get(b"id-1")?.as_deref(), Some(b"$100".as_slice()));
    assert_eq!(
        analytics.get(b"id-1")?.as_deref(),
        Some(b"signup".as_slice())
    );

    // Both folders exist on disk under the shared root.
    assert!(root.join("acme-billing").is_dir());
    assert!(root.join("acme-analytics").is_dir());

    drop(billing);
    drop(analytics);
    let _removed = std::fs::remove_dir_all(&root);
    Ok(())
}

#[test]
fn no_path_methods_set_yields_in_memory_database() -> Result<()> {
    // Backwards-compatibility check: bare `EmdbBuilder::new().build()`
    // continues to produce an in-memory database, no file creation.
    let db = EmdbBuilder::new().build()?;
    db.insert(b"k", b"v")?;
    assert_eq!(db.get(b"k")?.as_deref(), Some(b"v".as_slice()));
    // No persistence: a fresh builder yields an empty in-memory db.
    let other = EmdbBuilder::new().build()?;
    assert!(other.get(b"k")?.is_none());
    Ok(())
}
