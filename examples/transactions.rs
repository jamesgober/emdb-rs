//! Atomic batches via `transaction`. All writes inside the closure
//! either commit together or none are visible. Roll back by returning
//! `Err`.
//!
//! Run with:
//! ```sh
//! cargo run --release --example transactions
//! ```

use emdb::{Emdb, Error};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Emdb::open_in_memory();

    db.insert("account:alice", "100")?;
    db.insert("account:bob", "50")?;
    println!("opening balances: alice=100, bob=50");

    // --- Successful transfer: 30 from alice to bob ---
    db.transaction(|tx| {
        tx.insert("account:alice", "70")?;
        tx.insert("account:bob", "80")?;
        Ok(())
    })?;
    println!(
        "after commit: alice={}, bob={}",
        String::from_utf8_lossy(&db.get("account:alice")?.unwrap_or_default()),
        String::from_utf8_lossy(&db.get("account:bob")?.unwrap_or_default()),
    );
    assert_eq!(db.get("account:alice")?.as_deref(), Some(b"70".as_slice()));
    assert_eq!(db.get("account:bob")?.as_deref(), Some(b"80".as_slice()));

    // --- Failing transfer: returning Err rolls back every write ---
    let result: Result<(), Error> = db.transaction(|tx| {
        tx.insert("account:alice", "0")?;
        tx.insert("account:bob", "999")?;
        // Imagine some invariant check failing here.
        Err(Error::InvalidConfig("balance reconciliation failed"))
    });
    assert!(result.is_err(), "transaction should have rolled back");
    println!("\nrolled back. balances unchanged:");
    println!(
        "  alice={}, bob={}",
        String::from_utf8_lossy(&db.get("account:alice")?.unwrap_or_default()),
        String::from_utf8_lossy(&db.get("account:bob")?.unwrap_or_default()),
    );
    assert_eq!(db.get("account:alice")?.as_deref(), Some(b"70".as_slice()));
    assert_eq!(db.get("account:bob")?.as_deref(), Some(b"80".as_slice()));

    // --- Returning a value from the closure ---
    let total: u64 = db.transaction(|tx| {
        let alice = String::from_utf8_lossy(&tx.get("account:alice")?.unwrap_or_default())
            .parse::<u64>()
            .unwrap_or(0);
        let bob = String::from_utf8_lossy(&tx.get("account:bob")?.unwrap_or_default())
            .parse::<u64>()
            .unwrap_or(0);
        Ok(alice + bob)
    })?;
    println!("\nlive total balance = {total}");
    assert_eq!(total, 150);

    Ok(())
}
