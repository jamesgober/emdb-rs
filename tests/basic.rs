// Integration tests for the public `emdb` API.

use emdb::Emdb;

#[test]
fn open_in_memory_returns_empty_instance() {
    let db = Emdb::open_in_memory();
    assert_eq!(db.len(), 0);
    assert!(db.is_empty());
}

#[test]
fn default_is_empty() {
    let db = Emdb::default();
    assert!(db.is_empty());
    assert_eq!(db.len(), 0);
}
