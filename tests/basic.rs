// Integration tests for the public `emdb` API.

use emdb::{Emdb, Result};

#[test]
fn open_in_memory_returns_empty_instance() {
    let db = Emdb::open_in_memory();
    assert_eq!(db.len(), 0);
    assert!(db.is_empty());
}

#[test]
fn insert_get_remove_round_trip() -> Result<()> {
    let mut db = Emdb::open_in_memory();
    db.insert(b"key", b"value")?;

    assert_eq!(db.get(b"key")?, Some(b"value".to_vec()));
    assert!(db.contains_key(b"key")?);

    assert_eq!(db.remove(b"key")?, Some(b"value".to_vec()));
    assert!(!db.contains_key(b"key")?);
    Ok(())
}

#[test]
fn empty_key_is_allowed() -> Result<()> {
    let mut db = Emdb::open_in_memory();
    db.insert([], b"v")?;
    assert_eq!(db.get([])?, Some(b"v".to_vec()));
    Ok(())
}
