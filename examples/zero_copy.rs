//! Zero-copy reads via `get_zerocopy`. Returns a `ValueRef<'_>` that
//! borrows directly from the mmap — no `Vec<u8>` allocation, no
//! decoding beyond frame validation. ~2× faster than `get` on small
//! values where the alloc dominates.
//!
//! Run with:
//! ```sh
//! cargo run --release --example zero_copy
//! ```

use emdb::Emdb;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Emdb::open_in_memory();
    db.insert("config:max_conns", "1024")?;
    db.insert("config:host", "127.0.0.1")?;

    // get_zerocopy returns Option<ValueRef<'_>> — the slice lives
    // inside the mmap. No allocation here.
    if let Some(value) = db.get_zerocopy("config:max_conns")? {
        let bytes: &[u8] = value.as_ref();
        let s = std::str::from_utf8(bytes)?;
        println!("max_conns = {s}");
        assert_eq!(s, "1024");
    }

    // Same byte-level result as `get`, with no Vec allocation.
    let zc = db.get_zerocopy("config:host")?.expect("present");
    let owned = db.get("config:host")?.expect("present");
    assert_eq!(zc.as_ref(), owned.as_slice());

    // ValueRef converts to owned when the caller needs ownership.
    // `into_vec` consumes the ValueRef; the underlying mmap survives
    // as long as the Emdb handle does.
    let host_owned: Vec<u8> = zc.into_vec();
    drop(db);
    println!("host = {}", String::from_utf8_lossy(&host_owned));

    Ok(())
}
