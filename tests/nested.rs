#![cfg(feature = "nested")]

#[cfg(feature = "ttl")]
use std::time::Duration;

#[cfg(feature = "ttl")]
use emdb::Ttl;
use emdb::{Emdb, Result};

#[test]
fn empty_prefix_returns_error() {
    let db = Emdb::open_in_memory();
    let result = db.delete_group("");
    assert!(result.is_err());
}

#[test]
fn prefix_matching_none_returns_zero() -> Result<()> {
    let db = Emdb::open_in_memory();
    db.insert("other.name", "x")?;
    assert_eq!(db.delete_group("product")?, 0);
    Ok(())
}

#[test]
fn prefix_matching_all_keys_under_group() -> Result<()> {
    let db = Emdb::open_in_memory();
    db.insert("product.a", "1")?;
    db.insert("product.b", "2")?;
    db.insert("product.c", "3")?;
    assert_eq!(db.group("product")?.count(), 3);
    Ok(())
}

#[test]
fn focus_chain_three_deep_works() -> Result<()> {
    let db = Emdb::open_in_memory();
    {
        let a = db.focus("a");
        let b = a.focus("b");
        let c = b.focus("c");
        c.set("d", "value")?;
    }
    assert_eq!(db.get("a.b.c.d")?, Some(b"value".to_vec()));
    Ok(())
}

#[test]
fn delete_all_on_empty_focus_returns_zero() -> Result<()> {
    let db = Emdb::open_in_memory();
    let focus = db.focus("nothing");
    assert_eq!(focus.delete_all()?, 0);
    Ok(())
}

#[test]
fn keys_with_dots_are_grouped_when_nested_used() -> Result<()> {
    let db = Emdb::open_in_memory();
    db.insert("literal.dot", "v")?;
    assert_eq!(db.group("literal")?.count(), 1);
    Ok(())
}

#[cfg(feature = "ttl")]
#[test]
fn focus_set_with_ttl_and_sweep_interoperate() -> Result<()> {
    let db = Emdb::open_in_memory();
    {
        let focus = db.focus("session");
        focus.set_with_ttl("a", "1", Ttl::After(Duration::ZERO))?;
        focus.set_with_ttl("b", "2", Ttl::Never)?;
    }

    assert_eq!(db.get("session.a")?, None);
    assert_eq!(db.get("session.b")?, Some(b"2".to_vec()));
    let swept = db.sweep_expired();
    assert_eq!(swept, 1);
    Ok(())
}
