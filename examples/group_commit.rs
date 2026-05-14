//! Flush-policy comparison: N producer threads each calling `flush()`
//! per record under different `FlushPolicy` settings.
//!
//! In v0.9.x, **concurrent flushers coalesce automatically** inside
//! `fsys::JournalHandle`'s group-commit coordinator — so
//! `FlushPolicy::OnEachFlush` and `FlushPolicy::Group` are
//! functionally identical, and both already give you fsync-coalescing
//! when multiple threads call `flush()` together. The interesting
//! contrast is between either of those and `FlushPolicy::WriteThrough`
//! (every `insert` is durable on return; no separate `flush` needed).
//!
//! Run with:
//! ```sh
//! cargo run --release --example group_commit
//! ```

use std::sync::Arc;
use std::thread;
use std::time::Instant;

use emdb::{Emdb, FlushPolicy};

const PRODUCERS: usize = 8;
const PER_THREAD: usize = 200;

fn run(label: &str, policy: FlushPolicy, do_flush: bool) -> std::io::Result<()> {
    let path = std::env::temp_dir().join(format!("emdb-gc-{label}.emdb"));
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{}.lock", path.display()));

    let db = Arc::new(
        Emdb::builder()
            .path(&path)
            .flush_policy(policy)
            .build()
            .expect("open"),
    );

    let t0 = Instant::now();
    let mut threads = Vec::with_capacity(PRODUCERS);
    for tid in 0..PRODUCERS {
        let db = Arc::clone(&db);
        threads.push(thread::spawn(move || {
            for i in 0..PER_THREAD {
                let key = format!("t{tid}-k{i:03}");
                db.insert(key.as_bytes(), b"v").expect("insert");
                if do_flush {
                    db.flush().expect("flush");
                }
            }
        }));
    }
    for t in threads {
        let _ = t.join();
    }
    let elapsed = t0.elapsed();
    let writes = (PRODUCERS * PER_THREAD) as f64;
    println!(
        "{label:<24} {:>7.2} ms   {:>7.0} writes/sec",
        elapsed.as_secs_f64() * 1000.0,
        writes / elapsed.as_secs_f64()
    );

    drop(db);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{}.lock", path.display()));
    Ok(())
}

fn main() -> std::io::Result<()> {
    println!("workload: {PRODUCERS} threads × {PER_THREAD} writes each\n");

    // Both of these get fsys's automatic group-commit coalescing.
    run("OnEachFlush + flush()", FlushPolicy::OnEachFlush, true)?;
    run("Group + flush()", FlushPolicy::Group, true)?;
    // WriteThrough: every insert is durable on return; no flush call.
    run("WriteThrough (no flush)", FlushPolicy::WriteThrough, false)?;

    println!("\nIn v0.9.x, OnEachFlush and Group both rely on fsys's");
    println!("automatic group-commit coordinator. WriteThrough opts every");
    println!("insert into per-record durability — higher latency floor,");
    println!("no separate flush call needed.");
    Ok(())
}
