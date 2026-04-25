// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Integration tests for the named-namespace feature exposed through
//! `EmdbBuilder::prefer_v4(true)` + `Emdb::namespace`.

use emdb::{Emdb, EmdbBuilder, Error, Result};

fn tmp_path(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |d| d.as_nanos());
    p.push(format!("emdb-v4-ns-{name}-{nanos}.emdb"));
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
fn named_namespace_round_trips() -> Result<()> {
    let path = tmp_path("round-trip");
    let db = open_v4(&path)?;

    let inbox = db.namespace("inbox")?;
    assert_eq!(inbox.name(), "inbox");
    inbox.insert(b"msg-1", b"hello")?;
    inbox.insert(b"msg-2", b"world")?;

    assert_eq!(inbox.get(b"msg-1")?.as_deref(), Some(b"hello".as_slice()));
    assert_eq!(inbox.get(b"msg-2")?.as_deref(), Some(b"world".as_slice()));
    assert_eq!(inbox.len()?, 2);
    assert!(inbox.contains_key(b"msg-1")?);

    let removed = inbox.remove(b"msg-1")?;
    assert_eq!(removed.as_deref(), Some(b"hello".as_slice()));
    assert!(!inbox.contains_key(b"msg-1")?);
    assert_eq!(inbox.len()?, 1);

    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn namespaces_are_isolated_from_default_and_each_other() -> Result<()> {
    let path = tmp_path("isolation");
    let db = open_v4(&path)?;

    // Same key bytes, three different namespaces.
    db.insert(b"k", b"default")?;
    let a = db.namespace("a")?;
    a.insert(b"k", b"alpha")?;
    let b = db.namespace("b")?;
    b.insert(b"k", b"bravo")?;

    assert_eq!(db.get(b"k")?.as_deref(), Some(b"default".as_slice()));
    assert_eq!(a.get(b"k")?.as_deref(), Some(b"alpha".as_slice()));
    assert_eq!(b.get(b"k")?.as_deref(), Some(b"bravo".as_slice()));

    // Removing in `a` does not affect `b` or default.
    let _ = a.remove(b"k")?;
    assert!(a.get(b"k")?.is_none());
    assert_eq!(b.get(b"k")?.as_deref(), Some(b"bravo".as_slice()));
    assert_eq!(db.get(b"k")?.as_deref(), Some(b"default".as_slice()));

    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn namespace_open_is_idempotent() -> Result<()> {
    let path = tmp_path("idempotent");
    let db = open_v4(&path)?;

    let first = db.namespace("shared")?;
    first.insert(b"k", b"v1")?;
    drop(first);

    // Re-opening the same name resolves to the same namespace id, so the
    // earlier write is visible through the second handle.
    let second = db.namespace("shared")?;
    assert_eq!(second.get(b"k")?.as_deref(), Some(b"v1".as_slice()));

    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn empty_namespace_name_is_rejected() -> Result<()> {
    let path = tmp_path("empty-name");
    let db = open_v4(&path)?;
    let result = db.namespace("");
    assert!(matches!(result, Err(Error::InvalidConfig(_))));
    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn list_namespaces_includes_default_and_named_in_id_order() -> Result<()> {
    let path = tmp_path("list");
    let db = open_v4(&path)?;
    let _ = db.namespace("first")?;
    let _ = db.namespace("second")?;
    let _ = db.namespace("third")?;

    let names = db.list_namespaces()?;
    // Default namespace is reported as "" with id 0; named ones follow.
    assert_eq!(names, vec!["", "first", "second", "third"]);

    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn drop_namespace_removes_from_list_and_blocks_reads() -> Result<()> {
    let path = tmp_path("drop");
    let db = open_v4(&path)?;
    let scratch = db.namespace("scratch")?;
    scratch.insert(b"k", b"v")?;

    assert!(db.list_namespaces()?.contains(&"scratch".to_string()));

    let was_dropped = db.drop_namespace("scratch")?;
    assert!(was_dropped);

    // The namespace no longer appears in the list.
    let after = db.list_namespaces()?;
    assert!(!after.contains(&"scratch".to_string()), "list: {after:?}");

    // Reads through the existing handle now error: the runtime is gone.
    let result = scratch.get(b"k");
    assert!(matches!(result, Err(Error::InvalidConfig(_))));

    // A re-create of the same name yields a fresh, empty namespace.
    let recreated = db.namespace("scratch")?;
    assert!(recreated.get(b"k")?.is_none());
    assert_eq!(recreated.len()?, 0);

    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn drop_default_namespace_is_rejected() -> Result<()> {
    let path = tmp_path("drop-default");
    let db = open_v4(&path)?;
    let result = db.drop_namespace("");
    assert!(matches!(result, Err(Error::InvalidConfig(_))));
    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn drop_unknown_namespace_returns_false() -> Result<()> {
    let path = tmp_path("drop-unknown");
    let db = open_v4(&path)?;
    assert!(!db.drop_namespace("never-existed")?);
    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn namespace_persists_through_drop_and_reopen() -> Result<()> {
    let path = tmp_path("persist");
    {
        let db = open_v4(&path)?;
        let projects = db.namespace("projects")?;
        projects.insert(b"emdb", b"rust")?;
        projects.insert(b"hivedb", b"rust")?;
        let drafts = db.namespace("drafts")?;
        drafts.insert(b"todo", b"finish v0.7")?;
        db.flush()?;
    }

    let reopened = open_v4(&path)?;
    let names = reopened.list_namespaces()?;
    assert!(names.contains(&"projects".to_string()), "names: {names:?}");
    assert!(names.contains(&"drafts".to_string()), "names: {names:?}");

    let projects = reopened.namespace("projects")?;
    assert_eq!(projects.get(b"emdb")?.as_deref(), Some(b"rust".as_slice()));
    assert_eq!(
        projects.get(b"hivedb")?.as_deref(),
        Some(b"rust".as_slice())
    );
    assert_eq!(projects.len()?, 2);

    let drafts = reopened.namespace("drafts")?;
    assert_eq!(
        drafts.get(b"todo")?.as_deref(),
        Some(b"finish v0.7".as_slice())
    );

    drop(reopened);
    cleanup(&path);
    Ok(())
}

#[test]
fn namespace_iter_and_keys_yield_only_their_records() -> Result<()> {
    let path = tmp_path("iter");
    let db = open_v4(&path)?;

    db.insert(b"d-key", b"d-value")?;
    let ns = db.namespace("alpha")?;
    ns.insert(b"a-1", b"alpha-1")?;
    ns.insert(b"a-2", b"alpha-2")?;

    let mut iterated: Vec<(Vec<u8>, Vec<u8>)> = ns.iter()?.collect();
    iterated.sort();
    let mut expected = vec![
        (b"a-1".to_vec(), b"alpha-1".to_vec()),
        (b"a-2".to_vec(), b"alpha-2".to_vec()),
    ];
    expected.sort();
    assert_eq!(iterated, expected);

    let mut keys: Vec<Vec<u8>> = ns.keys()?.collect();
    keys.sort();
    assert_eq!(keys, vec![b"a-1".to_vec(), b"a-2".to_vec()]);

    // The default namespace iter is unaffected.
    let mut default_iter: Vec<(Vec<u8>, Vec<u8>)> = db.iter()?.collect();
    default_iter.sort();
    assert_eq!(default_iter, vec![(b"d-key".to_vec(), b"d-value".to_vec())]);

    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn namespace_clear_drops_records_only_for_target() -> Result<()> {
    let path = tmp_path("clear");
    let db = open_v4(&path)?;
    db.insert(b"d", b"d-val")?;
    let ns = db.namespace("scratch")?;
    ns.insert(b"x", b"xv")?;
    ns.insert(b"y", b"yv")?;
    assert_eq!(ns.len()?, 2);

    ns.clear()?;
    assert_eq!(ns.len()?, 0);
    assert!(ns.get(b"x")?.is_none());

    // Default namespace is untouched.
    assert_eq!(db.get(b"d")?.as_deref(), Some(b"d-val".as_slice()));
    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn namespace_on_v06_handle_is_rejected() -> Result<()> {
    let path = tmp_path("v06-rejected");
    // v0.6 path: no prefer_v4 call.
    let db = EmdbBuilder::new().path(path.clone()).build()?;
    let result = db.namespace("anywhere");
    assert!(matches!(result, Err(Error::InvalidConfig(_))));
    let drop_result = db.drop_namespace("anywhere");
    assert!(matches!(drop_result, Err(Error::InvalidConfig(_))));
    let list_result = db.list_namespaces();
    assert!(matches!(list_result, Err(Error::InvalidConfig(_))));
    drop(db);
    cleanup(&path);
    Ok(())
}
