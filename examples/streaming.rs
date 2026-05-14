//! Streaming async iterators (v0.9.7+). Records arrive incrementally
//! through a bounded `tokio::sync::mpsc` channel; memory in flight is
//! bounded by the channel depth (64), not the namespace size.
//!
//! Requires `--features async`.
//!
//! Run with:
//! ```sh
//! cargo run --release --example streaming --features async
//! ```

use emdb::Emdb;
use tokio_stream::StreamExt;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::temp_dir().join("emdb-streaming-example.emdb");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{}.lock", path.display()));

    let db = Emdb::builder()
        .path(&path)
        .enable_range_scans(true)
        .build_async()
        .await?;

    // Populate.
    const N: usize = 10_000;
    let batch: Vec<(String, String)> = (0..N)
        .map(|i| (format!("k{i:05}"), format!("v{i}")))
        .collect();
    db.insert_many(batch.iter().map(|(k, v)| (k.as_str(), v.as_str())))
        .await?;
    println!("populated {N} records");

    // --- iter_stream: full namespace, incrementally ---
    let mut stream = db.iter_stream().await?;
    let mut total_bytes: u64 = 0;
    let mut count = 0_usize;
    while let Some((_k, v)) = stream.next().await {
        total_bytes += v.len() as u64;
        count += 1;
    }
    println!("iter_stream yielded {count} records, {total_bytes} bytes of value");
    assert_eq!(count, N);

    // --- range_stream: half-open range, lexicographic order ---
    let mut stream = db
        .range_stream(b"k01000".to_vec()..b"k01100".to_vec())
        .await?;
    let mut keys = Vec::new();
    while let Some((k, _v)) = stream.next().await {
        keys.push(k);
    }
    println!("range_stream k01000..k01100 yielded {} records", keys.len());
    assert_eq!(keys.len(), 100);
    assert_eq!(keys[0], b"k01000");
    assert_eq!(keys[99], b"k01099");

    // --- range_prefix_stream: every record under the prefix ---
    // Keys are k00000..k09999 (5 digits). Prefix "k001" matches every
    // key whose digits start with 001 → k00100..k00199 = 100 records.
    let mut stream = db.range_prefix_stream("k001").await?;
    let mut count = 0;
    while stream.next().await.is_some() {
        count += 1;
    }
    println!("range_prefix_stream 'k001' yielded {count} records");
    assert_eq!(count, 100);

    // --- Early drop halts the blocking pump task cleanly ---
    let mut stream = db.iter_stream().await?;
    let _first = stream.next().await;
    let _second = stream.next().await;
    drop(stream); // Pump task sees blocking_send error and exits
    println!("early drop of stream — pump task halts cleanly");

    // Verify the runtime is still healthy.
    db.insert("after-drop", "ok").await?;
    assert_eq!(
        db.get("after-drop").await?.as_deref(),
        Some(b"ok".as_slice())
    );

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{}.lock", path.display()));
    Ok(())
}
