#![cfg(feature = "ttl")]

use std::time::Duration;

use emdb::{Emdb, Result, Ttl};

#[test]
fn ttl_default_without_builder_default_never_expires() -> Result<()> {
    let mut db = Emdb::open_in_memory();
    db.insert_with_ttl("k", "v", Ttl::Default)?;
    assert_eq!(db.get("k")?, Some(b"v".to_vec()));
    Ok(())
}

#[test]
fn ttl_after_zero_is_immediately_expired() -> Result<()> {
    let mut db = Emdb::open_in_memory();
    db.insert_with_ttl("k", "v", Ttl::After(Duration::ZERO))?;
    assert_eq!(db.get("k")?, None);
    Ok(())
}

#[test]
fn reinsert_resets_ttl() -> Result<()> {
    let mut db = Emdb::open_in_memory();
    db.insert_with_ttl("k", "v1", Ttl::After(Duration::ZERO))?;
    db.insert_with_ttl("k", "v2", Ttl::Never)?;
    assert_eq!(db.get("k")?, Some(b"v2".to_vec()));
    Ok(())
}

#[test]
fn persist_on_missing_key_returns_false() -> Result<()> {
    let mut db = Emdb::open_in_memory();
    assert!(!db.persist("missing")?);
    Ok(())
}

#[test]
fn sweep_expired_on_empty_db_returns_zero() {
    let mut db = Emdb::open_in_memory();
    assert_eq!(db.sweep_expired(), 0);
}

#[test]
fn expiration_boundary_is_expired() -> Result<()> {
    let mut db = Emdb::open_in_memory();
    db.insert_with_ttl("k", "v", Ttl::After(Duration::ZERO))?;
    assert_eq!(db.ttl("k")?, None);
    Ok(())
}

#[test]
fn large_ttl_value_is_supported_when_representable() -> Result<()> {
    let mut db = Emdb::open_in_memory();
    db.insert_with_ttl("k", "v", Ttl::After(Duration::from_secs(86_400)))?;
    assert!(db.expires_at("k")?.is_some());
    Ok(())
}
