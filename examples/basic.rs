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
    let len = db.len();
    let empty = db.is_empty();

    match (len, empty) {
        (Ok(len), Ok(empty)) => {
            println!("  len:      {len}");
            println!("  is_empty: {empty}");
        }
        (Err(err), _) | (_, Err(err)) => {
            eprintln!("failed to inspect db state: {err}");
        }
    }
}
