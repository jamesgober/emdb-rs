//! Async surface basics: open, insert, get, namespace ops via
//! `AsyncEmdb`. Every method routes through `tokio::task::spawn_blocking`
//! so emdb's blocking I/O never stalls the runtime.
//!
//! Requires `--features async`.
//!
//! Run with:
//! ```sh
//! cargo run --release --example async_basics --features async
//! ```

use emdb::AsyncEmdb;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = AsyncEmdb::open_in_memory();

    // Single-record ops.
    db.insert("alpha", "one").await?;
    db.insert("beta", "two").await?;
    let alpha = db.get("alpha").await?;
    println!(
        "alpha = {:?}",
        alpha.as_deref().map(String::from_utf8_lossy)
    );
    assert_eq!(alpha.as_deref(), Some(b"one".as_slice()));

    // Vectored insert — one LSN reservation, one pwrite, one fsync.
    let batch: Vec<(String, String)> = (0..100)
        .map(|i| (format!("k{i:03}"), format!("v{i}")))
        .collect();
    db.insert_many(batch.iter().map(|(k, v)| (k.as_str(), v.as_str())))
        .await?;
    assert_eq!(db.len().await?, 102); // alpha + beta + 100
    println!(
        "inserted 100 records via insert_many; len = {}",
        db.len().await?
    );

    // Namespace handles — cheap clones, isolated indexes.
    let users = db.namespace("users").await?;
    users.insert("alice", "user-data").await?;
    users.insert("bob", "user-data").await?;
    assert_eq!(users.len().await?, 2);
    assert_eq!(db.len().await?, 102); // unchanged
    println!(
        "users namespace has {} records, default has {}",
        users.len().await?,
        db.len().await?
    );

    // Transactions run on the blocking pool; closure is sync.
    db.transaction(|tx| {
        tx.insert("atomic-1", "yes")?;
        tx.insert("atomic-2", "yes")?;
        Ok(())
    })
    .await?;
    println!("transaction committed; atomic-1 + atomic-2 visible");

    // Bridge to the sync handle when you need APIs the async surface
    // doesn't expose (zero-copy reads, custom iteration patterns).
    let sync_db = db.sync_handle();
    let count = tokio::task::spawn_blocking(move || sync_db.len()).await??;
    println!("via sync_handle, len = {count}");

    Ok(())
}
