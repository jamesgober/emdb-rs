// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Integration tests for opt-in range scans.

use emdb::{EmdbBuilder, Result};

fn tmp_path(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |d| d.as_nanos());
    p.push(format!("emdb-range-{name}-{nanos}.emdb"));
    p
}

fn cleanup(path: &std::path::Path) {
    let _ = std::fs::remove_file(path);
    let display = path.display();
    let _ = std::fs::remove_file(format!("{display}.lock"));
    let _ = std::fs::remove_file(format!("{display}.compact.tmp"));
}

#[test]
fn range_scan_disabled_by_default() -> Result<()> {
    let path = tmp_path("disabled");
    let db = EmdbBuilder::new().path(path.clone()).build()?;
    db.insert("a", "1")?;
    let result = db.range(b"a".to_vec()..b"z".to_vec());
    assert!(
        matches!(result, Err(emdb::Error::InvalidConfig(_))),
        "range without enable_range_scans should error, got {result:?}"
    );
    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn range_scan_returns_sorted_keys() -> Result<()> {
    let path = tmp_path("sorted");
    let db = EmdbBuilder::new()
        .path(path.clone())
        .enable_range_scans(true)
        .build()?;

    // Insert in random order.
    db.insert("user:003", "carol")?;
    db.insert("user:001", "alice")?;
    db.insert("user:002", "bob")?;
    db.insert("session:abc", "xx")?;
    db.insert("session:def", "yy")?;

    let users = db.range(b"user:".to_vec()..b"user;".to_vec())?;
    let user_keys: Vec<&[u8]> = users.iter().map(|(k, _)| k.as_slice()).collect();
    assert_eq!(
        user_keys,
        vec![
            b"user:001".as_slice(),
            b"user:002".as_slice(),
            b"user:003".as_slice()
        ],
        "range scan must return keys in lexicographic order"
    );

    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn range_prefix_helper_matches_explicit_range() -> Result<()> {
    let path = tmp_path("prefix");
    let db = EmdbBuilder::new()
        .path(path.clone())
        .enable_range_scans(true)
        .build()?;

    db.insert("foo:1", "a")?;
    db.insert("foo:2", "b")?;
    db.insert("foobar", "c")?;
    db.insert("food", "d")?;
    db.insert("zzz", "z")?;

    let foo = db.range_prefix(b"foo:")?;
    assert_eq!(foo.len(), 2, "foo: prefix should match exactly two keys");

    let foo_all = db.range_prefix(b"foo")?;
    assert_eq!(
        foo_all.len(),
        4,
        "foo prefix should match foo:1, foo:2, foobar, food"
    );

    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn range_scan_survives_remove_and_overwrite() -> Result<()> {
    let path = tmp_path("mutate");
    let db = EmdbBuilder::new()
        .path(path.clone())
        .enable_range_scans(true)
        .build()?;

    db.insert("a", "1")?;
    db.insert("b", "2")?;
    db.insert("c", "3")?;
    let _removed = db.remove(b"b")?;
    db.insert("a", "1-updated")?;

    let all = db.range(b"a".to_vec()..b"z".to_vec())?;
    assert_eq!(all.len(), 2, "removed key should not appear in range");
    assert_eq!(all[0].0, b"a");
    assert_eq!(all[0].1, b"1-updated", "overwrite should be visible");
    assert_eq!(all[1].0, b"c");

    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn range_scan_survives_reopen() -> Result<()> {
    let path = tmp_path("reopen");
    {
        let db = EmdbBuilder::new()
            .path(path.clone())
            .enable_range_scans(true)
            .build()?;
        db.insert("a", "1")?;
        db.insert("c", "3")?;
        db.insert("b", "2")?;
        db.flush()?;
    }

    let db = EmdbBuilder::new()
        .path(path.clone())
        .enable_range_scans(true)
        .build()?;
    let all = db.range(b"a".to_vec()..b"z".to_vec())?;
    let keys: Vec<&[u8]> = all.iter().map(|(k, _)| k.as_slice()).collect();
    assert_eq!(
        keys,
        vec![b"a".as_slice(), b"b".as_slice(), b"c".as_slice()],
        "BTreeMap must be rebuilt from records on reopen"
    );

    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn range_scan_works_inside_named_namespace() -> Result<()> {
    let path = tmp_path("named");
    let db = EmdbBuilder::new()
        .path(path.clone())
        .enable_range_scans(true)
        .build()?;

    let users = db.namespace("users")?;
    users.insert("c", "carol")?;
    users.insert("a", "alice")?;
    users.insert("b", "bob")?;

    let all = users.range(b"a".to_vec()..b"z".to_vec())?;
    let keys: Vec<&[u8]> = all.iter().map(|(k, _)| k.as_slice()).collect();
    assert_eq!(
        keys,
        vec![b"a".as_slice(), b"b".as_slice(), b"c".as_slice()]
    );

    drop(users);
    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn range_scan_survives_compaction() -> Result<()> {
    let path = tmp_path("compact");
    {
        let db = EmdbBuilder::new()
            .path(path.clone())
            .enable_range_scans(true)
            .build()?;

        for i in 0_u32..50 {
            let key = format!("k{i:04}");
            db.insert(key.as_bytes(), b"v")?;
        }
        // Remove half of them so compaction has work to do.
        for i in 0_u32..25 {
            let key = format!("k{i:04}");
            let _removed = db.remove(key.as_bytes())?;
        }
        db.flush()?;
        db.compact()?;
        db.flush()?;
    }

    let db = EmdbBuilder::new()
        .path(path.clone())
        .enable_range_scans(true)
        .build()?;
    let all = db.range(b"k".to_vec()..b"l".to_vec())?;
    assert_eq!(all.len(), 25, "only the surviving 25 keys must remain");
    // Spot-check ordering.
    assert_eq!(all[0].0, b"k0025");
    assert_eq!(all[24].0, b"k0049");

    drop(db);
    cleanup(&path);
    Ok(())
}
