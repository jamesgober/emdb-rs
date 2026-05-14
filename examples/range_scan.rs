//! Range / prefix queries via the opt-in `enable_range_scans` builder
//! flag. Maintains a parallel lock-free `SkipMap` per namespace; pay
//! the memory cost only when sorted iteration is actually needed.
//!
//! Run with:
//! ```sh
//! cargo run --release --example range_scan
//! ```

use emdb::Emdb;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Emdb::builder().enable_range_scans(true).build()?;

    // Mixed key shapes, sequentially numbered.
    for i in 0..10 {
        db.insert(format!("user:{i:03}"), format!("u{i}"))?;
        db.insert(format!("session:{i:03}"), format!("s{i}"))?;
    }
    db.insert("aardvark", "out-of-band")?;
    db.insert("zebra", "out-of-band")?;

    // Half-open range query
    let users = db.range(b"user:".to_vec()..b"user;".to_vec())?;
    println!("range 'user:'..'user;' yields {} records:", users.len());
    for (k, v) in &users {
        println!(
            "  {} = {}",
            String::from_utf8_lossy(k),
            String::from_utf8_lossy(v)
        );
    }
    assert_eq!(users.len(), 10);

    // Prefix shorthand — same result, simpler call shape
    let sessions = db.range_prefix("session:")?;
    println!("\nprefix 'session:' yields {} records", sessions.len());
    assert_eq!(sessions.len(), 10);

    // Lazy iteration via range_iter — pay decode cost per record
    let lazy = db.range_iter(b"user:".to_vec()..b"user;".to_vec())?;
    let first_three: Vec<_> = lazy.take(3).collect();
    println!("\nlazy range_iter, first 3 only:");
    for (k, _v) in &first_three {
        println!("  {}", String::from_utf8_lossy(k));
    }
    assert_eq!(first_three.len(), 3);

    // iter_from for cursor-style sequential access
    let from = db.iter_from("user:005")?;
    let from_count = from.count();
    println!("\niter_from('user:005') yields {from_count} records (inclusive)");
    assert_eq!(from_count, 6); // user:005..user:009 (5 users) + zebra

    Ok(())
}
