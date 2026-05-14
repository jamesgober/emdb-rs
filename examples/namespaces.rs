//! Multi-tenant data via named namespaces. Each namespace has its own
//! hash index, its own `len()`, and its own lifecycle — clear or drop
//! one without affecting the others.
//!
//! Run with:
//! ```sh
//! cargo run --release --example namespaces
//! ```

use emdb::Emdb;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Emdb::open_in_memory();

    // Default namespace.
    db.insert("global:flag", "on")?;

    // Named namespaces — created lazily on first use.
    let users = db.namespace("users")?;
    let sessions = db.namespace("sessions")?;

    users.insert("alice", "user-data-alice")?;
    users.insert("bob", "user-data-bob")?;
    sessions.insert("session-token-1", "alice")?;
    sessions.insert("session-token-2", "alice")?;
    sessions.insert("session-token-3", "bob")?;

    println!("default namespace len = {}", db.len()?);
    println!("users namespace    len = {}", users.len()?);
    println!("sessions namespace len = {}", sessions.len()?);
    assert_eq!(db.len()?, 1);
    assert_eq!(users.len()?, 2);
    assert_eq!(sessions.len()?, 3);

    // Each namespace is fully isolated.
    assert!(db.get("alice")?.is_none()); // not in default
    assert!(users.get("session-token-1")?.is_none()); // not in users
    println!("\nisolation verified — keys don't cross namespaces");

    // List every named namespace.
    let names = db.list_namespaces()?;
    println!("\nlist_namespaces() = {names:?}");
    assert!(names.iter().any(|n| n == "users"));
    assert!(names.iter().any(|n| n == "sessions"));

    // Clear one namespace without touching the others.
    sessions.clear()?;
    println!("\nafter sessions.clear():");
    println!("  users    len = {}", users.len()?);
    println!("  sessions len = {}", sessions.len()?);
    assert_eq!(users.len()?, 2);
    assert_eq!(sessions.len()?, 0);

    // Drop the namespace entirely.
    let dropped = db.drop_namespace("sessions")?;
    println!("\ndrop_namespace('sessions') -> {dropped}");
    assert!(dropped);
    let names_after = db.list_namespaces()?;
    println!("list_namespaces() = {names_after:?}");
    assert!(!names_after.iter().any(|n| n == "sessions"));

    Ok(())
}
