//! Minimal example — open an in-memory instance and report its state.
//!
//! Run with:
//! ```sh
//! cargo run --example basic
//! ```

use emdb::Emdb;

fn main() {
    let db = Emdb::open_in_memory();
    println!("emdb instance opened");
    println!("  len:      {}", db.len());
    println!("  is_empty: {}", db.is_empty());
}
