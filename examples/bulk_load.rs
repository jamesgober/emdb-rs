//! Bulk-load 100 K records via `insert_many` — one LSN reservation, one
//! `pwrite`, one fsync. Strictly faster than the equivalent insert loop.
//!
//! Run with:
//! ```sh
//! cargo run --release --example bulk_load
//! ```

use std::time::Instant;

use emdb::Emdb;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Emdb::open_in_memory();

    const N: usize = 100_000;
    let batch: Vec<(Vec<u8>, Vec<u8>)> = (0..N)
        .map(|i| {
            (
                format!("key-{i:06}").into_bytes(),
                format!("value-{i}").into_bytes(),
            )
        })
        .collect();

    let t0 = Instant::now();
    db.insert_many(batch)?;
    db.flush()?;
    let elapsed = t0.elapsed();

    println!(
        "bulk-loaded {N} records in {:.2} ms ({:.0} records/sec)",
        elapsed.as_secs_f64() * 1000.0,
        N as f64 / elapsed.as_secs_f64()
    );
    assert_eq!(db.len()?, N);

    // Spot-check a few values came back correctly.
    assert_eq!(
        db.get("key-000000")?.as_deref(),
        Some(b"value-0".as_slice())
    );
    assert_eq!(
        db.get("key-099999")?.as_deref(),
        Some(b"value-99999".as_slice())
    );
    println!("verified first + last records round-trip");

    Ok(())
}
