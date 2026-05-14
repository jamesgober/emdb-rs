//! Cache pattern: insert records with a default TTL, let lazy expiry
//! filter reads, and periodically eagerly sweep to reclaim memory.
//!
//! Requires `--features ttl` (on by default).
//!
//! Run with:
//! ```sh
//! cargo run --release --example ttl_cache
//! ```

use std::thread;
use std::time::Duration;

use emdb::{Emdb, Ttl};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1-hour default TTL for everything inserted via plain `insert`.
    let db = Emdb::builder()
        .default_ttl(Duration::from_secs(3600))
        .build()?;

    // Plain insert picks up the default TTL.
    db.insert("session:alice", "active")?;
    db.insert("session:bob", "active")?;

    // Per-record TTL override — short expiration for demo purposes.
    db.insert_with_ttl("blip", "ghost", Ttl::After(Duration::from_millis(50)))?;
    db.insert_with_ttl("forever", "never-expires", Ttl::Never)?;

    println!("inserted 4 records, len = {}", db.len()?);
    assert!(db.get("blip")?.is_some());

    // Wait for the short-TTL record to expire.
    thread::sleep(Duration::from_millis(80));

    // Lazy expiry — `get` filters out the expired record.
    assert!(db.get("blip")?.is_none(), "blip should be expired");
    println!("after 80 ms wait, 'blip' returns None via lazy expiry");

    // But `len()` still counts the dead record until a sweep runs.
    println!("len() before sweep = {}", db.len()?);

    // Eager sweep reclaims the slot + writes a tombstone for durability.
    let swept = db.sweep_expired();
    println!("sweep_expired removed {swept} record(s)");
    println!("len() after sweep  = {}", db.len()?);

    // Permanent records survive the sweep untouched.
    assert_eq!(
        db.get("forever")?.as_deref(),
        Some(b"never-expires".as_slice())
    );

    // Inspect remaining TTLs.
    if let Some(remaining) = db.ttl("session:alice")? {
        println!("session:alice has {} s remaining", remaining.as_secs());
    }

    // Strip the TTL — record never expires again.
    let stripped = db.persist("session:alice")?;
    println!("persist('session:alice') = {stripped}");
    assert!(db.ttl("session:alice")?.is_none());

    Ok(())
}
