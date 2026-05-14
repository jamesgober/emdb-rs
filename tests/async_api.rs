// Copyright 2026 James Gober. Licensed under Apache-2.0.
//
// Integration tests for the v0.9.5 async surface (gated behind
// the `async` feature). Validates that `AsyncEmdb` /
// `AsyncNamespace` route every blocking emdb call through
// `tokio::task::spawn_blocking` correctly and that the round-trip
// preserves the same semantics as the sync API.

#![cfg(feature = "async")]

use std::sync::Arc;

use emdb::{AsyncEmdb, AsyncNamespace, Emdb};

fn tmp_path(label: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |d| d.as_nanos());
    p.push(format!("emdb-async-{label}-{nanos}.emdb"));
    p
}

fn cleanup(path: &std::path::Path) {
    let _ = std::fs::remove_file(path);
    let display = path.display();
    let _ = std::fs::remove_file(format!("{display}.lock"));
    let _ = std::fs::remove_file(format!("{display}.meta"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn open_in_memory_round_trip() {
    let db = AsyncEmdb::open_in_memory();
    db.insert("alpha", "first").await.expect("insert");
    db.insert("beta", "second").await.expect("insert");
    assert_eq!(
        db.get("alpha").await.expect("get").as_deref(),
        Some(b"first".as_slice())
    );
    assert_eq!(
        db.get("beta").await.expect("get").as_deref(),
        Some(b"second".as_slice())
    );
    assert!(db.get("missing").await.expect("get").is_none());
    assert_eq!(db.len().await.expect("len"), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn open_file_round_trip() {
    let path = tmp_path("file");
    cleanup(&path);
    let db = AsyncEmdb::open(&path).await.expect("open");
    db.insert("k1", "v1").await.expect("insert");
    db.flush().await.expect("flush");
    db.checkpoint().await.expect("checkpoint");
    let value = db.get("k1").await.expect("get");
    assert_eq!(value.as_deref(), Some(b"v1".as_slice()));
    drop(db);
    cleanup(&path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn builder_build_async() {
    let path = tmp_path("builder");
    cleanup(&path);
    let db = Emdb::builder()
        .path(&path)
        .enable_range_scans(true)
        .build_async()
        .await
        .expect("build_async");
    db.insert("alpha", "1").await.expect("insert");
    db.insert("beta", "2").await.expect("insert");
    let range = db.range_prefix("a").await.expect("range_prefix");
    assert_eq!(range.len(), 1);
    assert_eq!(range[0].0, b"alpha");
    drop(db);
    cleanup(&path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_many_round_trip() {
    let db = AsyncEmdb::open_in_memory();
    let items: Vec<(String, String)> = (0..50)
        .map(|i| (format!("k{i:03}"), format!("v{i}")))
        .collect();
    db.insert_many(items.iter().map(|(k, v)| (k.as_str(), v.as_str())))
        .await
        .expect("insert_many");
    assert_eq!(db.len().await.expect("len"), 50);
    for i in 0_u32..50 {
        let key = format!("k{i:03}");
        let expected = format!("v{i}");
        let got = db.get(&key).await.expect("get");
        assert_eq!(got.as_deref(), Some(expected.as_bytes()));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remove_round_trip() {
    let db = AsyncEmdb::open_in_memory();
    db.insert("alpha", "1").await.expect("insert");
    db.insert("beta", "2").await.expect("insert");
    let prev = db.remove("alpha").await.expect("remove");
    assert_eq!(prev.as_deref(), Some(b"1".as_slice()));
    assert!(db.get("alpha").await.expect("get").is_none());
    assert_eq!(db.len().await.expect("len"), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn iter_keys_collect() {
    let db = AsyncEmdb::open_in_memory();
    for i in 0_u32..10 {
        db.insert(format!("k{i:02}"), format!("v{i}"))
            .await
            .expect("insert");
    }
    let mut keys = db.keys().await.expect("keys");
    keys.sort();
    assert_eq!(keys.len(), 10);
    assert_eq!(keys[0], b"k00");
    assert_eq!(keys[9], b"k09");

    let mut pairs = db.iter().await.expect("iter");
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(pairs.len(), 10);
    assert_eq!(pairs[0].0, b"k00");
    assert_eq!(pairs[0].1, b"v0");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn namespace_round_trip() {
    let db = AsyncEmdb::open_in_memory();
    let ns: AsyncNamespace = db.namespace("session").await.expect("namespace");
    assert_eq!(ns.name(), "session");
    ns.insert("a", "1").await.expect("ns insert");
    ns.insert("b", "2").await.expect("ns insert");
    assert_eq!(
        ns.get("a").await.expect("ns get").as_deref(),
        Some(b"1".as_slice())
    );
    assert_eq!(ns.len().await.expect("ns len"), 2);
    // Default namespace is unaffected.
    assert_eq!(db.len().await.expect("default len"), 0);

    // List shows the named namespace.
    let names = db.list_namespaces().await.expect("list");
    assert!(names.iter().any(|n| n == "session"));

    // Drop removes it.
    let removed = db.drop_namespace("session").await.expect("drop_namespace");
    assert!(removed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transaction_commits_atomically() {
    let db = AsyncEmdb::open_in_memory();
    db.transaction(|tx| {
        tx.insert("alpha", "1")?;
        tx.insert("beta", "2")?;
        tx.insert("gamma", "3")?;
        Ok(())
    })
    .await
    .expect("transaction");
    assert_eq!(db.len().await.expect("len"), 3);
    assert_eq!(
        db.get("alpha").await.expect("get").as_deref(),
        Some(b"1".as_slice())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stats_returns_snapshot() {
    let db = AsyncEmdb::open_in_memory();
    db.insert("k1", "v1").await.expect("insert");
    db.insert("k2", "v2").await.expect("insert");
    let stats = db.stats().await.expect("stats");
    assert_eq!(stats.live_records, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_clones_share_state() {
    // Multiple AsyncEmdb clones must share the same underlying
    // database via Arc — a write via one clone must be visible to
    // a read via another.
    let db = Arc::new(AsyncEmdb::open_in_memory());
    let writer_handle = {
        let db = Arc::clone(&db);
        tokio::spawn(async move {
            for i in 0_u32..100 {
                db.insert(format!("k{i:03}"), format!("v{i}"))
                    .await
                    .expect("insert");
            }
        })
    };
    writer_handle.await.expect("writer task");

    // Read via a clone — should see all 100 writes.
    let reader = AsyncEmdb::clone(&db);
    for i in 0_u32..100 {
        let key = format!("k{i:03}");
        let got = reader.get(&key).await.expect("get");
        assert_eq!(got.as_deref(), Some(format!("v{i}").as_bytes()));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn range_scan_async() {
    let path = tmp_path("range");
    cleanup(&path);
    let db = Emdb::builder()
        .path(&path)
        .enable_range_scans(true)
        .build_async()
        .await
        .expect("build_async");
    for i in 0_u32..20 {
        db.insert(format!("k{i:03}"), format!("v{i}"))
            .await
            .expect("insert");
    }
    let pairs = db
        .range(b"k005".to_vec()..b"k010".to_vec())
        .await
        .expect("range");
    // Half-open: k005, k006, k007, k008, k009 — 5 entries.
    assert_eq!(pairs.len(), 5);
    assert_eq!(pairs[0].0, b"k005");
    assert_eq!(pairs[4].0, b"k009");
    drop(db);
    cleanup(&path);
}

#[cfg(feature = "ttl")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ttl_round_trip() {
    use emdb::Ttl;
    use std::time::Duration;
    let db = AsyncEmdb::open_in_memory();
    db.insert_with_ttl("temp", "value", Ttl::After(Duration::from_secs(3600)))
        .await
        .expect("insert_with_ttl");
    let ttl = db.ttl("temp").await.expect("ttl");
    assert!(ttl.is_some());
    let persisted = db.persist("temp").await.expect("persist");
    assert!(persisted);
    let ttl_after = db.ttl("temp").await.expect("ttl");
    assert!(ttl_after.is_none());
}

// -------------------------------------------------------------
// v0.9.7 streaming-iterator integration tests
// -------------------------------------------------------------

use futures_util::StreamExt;

/// `iter_stream` should yield every inserted record, regardless of
/// total count, with memory bounded by the channel depth rather than
/// the namespace size.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn iter_stream_yields_all_records() {
    let db = AsyncEmdb::open_in_memory();
    const N: usize = 500;
    for i in 0..N {
        db.insert(format!("k{i:04}"), format!("v{i}"))
            .await
            .expect("insert");
    }
    let mut stream = db.iter_stream().await.expect("iter_stream");
    let mut seen = std::collections::HashSet::new();
    while let Some((k, _)) = stream.next().await {
        let inserted = seen.insert(k);
        assert!(inserted, "duplicate key from iter_stream");
    }
    assert_eq!(seen.len(), N);
}

/// `keys_stream` should yield every inserted key (values discarded).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keys_stream_yields_all_keys() {
    let db = AsyncEmdb::open_in_memory();
    const N: usize = 200;
    for i in 0..N {
        db.insert(format!("key-{i:03}"), b"x")
            .await
            .expect("insert");
    }
    let mut stream = db.keys_stream().await.expect("keys_stream");
    let mut count = 0usize;
    while let Some(_k) = stream.next().await {
        count += 1;
    }
    assert_eq!(count, N);
}

/// `range_stream` should yield only the records inside the half-open
/// bounds, in lexicographic order, with no eager materialisation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn range_stream_half_open_bounds() {
    let path = tmp_path("range-stream");
    cleanup(&path);
    let db = Emdb::builder()
        .path(&path)
        .enable_range_scans(true)
        .build_async()
        .await
        .expect("build_async");
    for i in 0_u32..20 {
        db.insert(format!("k{i:03}"), format!("v{i}"))
            .await
            .expect("insert");
    }
    let mut stream = db
        .range_stream(b"k005".to_vec()..b"k010".to_vec())
        .await
        .expect("range_stream");
    let mut keys = Vec::new();
    while let Some((k, _)) = stream.next().await {
        keys.push(k);
    }
    assert_eq!(keys.len(), 5);
    assert_eq!(keys[0], b"k005");
    assert_eq!(keys[4], b"k009");
    drop(db);
    cleanup(&path);
}

/// `range_prefix_stream` should yield every record under the prefix
/// and nothing outside it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn range_prefix_stream_filters_correctly() {
    let path = tmp_path("range-prefix-stream");
    cleanup(&path);
    let db = Emdb::builder()
        .path(&path)
        .enable_range_scans(true)
        .build_async()
        .await
        .expect("build_async");
    for i in 0_u32..10 {
        db.insert(format!("alpha-{i}"), b"a").await.expect("insert");
        db.insert(format!("beta-{i}"), b"b").await.expect("insert");
    }
    let mut stream = db
        .range_prefix_stream("alpha-")
        .await
        .expect("range_prefix_stream");
    let mut count = 0usize;
    while let Some((k, v)) = stream.next().await {
        assert!(k.starts_with(b"alpha-"));
        assert_eq!(&v[..], b"a");
        count += 1;
    }
    assert_eq!(count, 10);
    drop(db);
    cleanup(&path);
}

/// `iter_from_stream` should yield records at or after the start key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn iter_from_stream_inclusive_start() {
    let path = tmp_path("iter-from-stream");
    cleanup(&path);
    let db = Emdb::builder()
        .path(&path)
        .enable_range_scans(true)
        .build_async()
        .await
        .expect("build_async");
    for i in 0_u32..10 {
        db.insert(format!("k{i}"), b"v").await.expect("insert");
    }
    let mut stream = db.iter_from_stream("k5").await.expect("iter_from_stream");
    let mut keys = Vec::new();
    while let Some((k, _)) = stream.next().await {
        keys.push(String::from_utf8(k).unwrap());
    }
    keys.sort();
    assert_eq!(keys, vec!["k5", "k6", "k7", "k8", "k9"]);
    drop(db);
    cleanup(&path);
}

/// `iter_after_stream` should yield records strictly after the start
/// key (exclusive).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn iter_after_stream_exclusive_start() {
    let path = tmp_path("iter-after-stream");
    cleanup(&path);
    let db = Emdb::builder()
        .path(&path)
        .enable_range_scans(true)
        .build_async()
        .await
        .expect("build_async");
    for i in 0_u32..5 {
        db.insert(format!("k{i}"), b"v").await.expect("insert");
    }
    let mut stream = db.iter_after_stream("k2").await.expect("iter_after_stream");
    let mut keys = Vec::new();
    while let Some((k, _)) = stream.next().await {
        keys.push(String::from_utf8(k).unwrap());
    }
    keys.sort();
    assert_eq!(keys, vec!["k3", "k4"]);
    drop(db);
    cleanup(&path);
}

/// Dropping the stream early should halt the blocking pump task.
/// We verify by reading just two records out of many, then dropping.
/// The blocking task will see `blocking_send` fail and exit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn iter_stream_drop_halts_pump() {
    let db = AsyncEmdb::open_in_memory();
    for i in 0..10_000 {
        db.insert(format!("k{i:05}"), b"x").await.expect("insert");
    }
    let mut stream = db.iter_stream().await.expect("iter_stream");
    let _first = stream.next().await.expect("first");
    let _second = stream.next().await.expect("second");
    // Drop without consuming the rest. No hang, no leak.
    drop(stream);
    // Round-trip a normal op to confirm the runtime is still healthy.
    db.insert("after-drop", "ok").await.expect("post-insert");
    assert_eq!(
        db.get("after-drop").await.expect("get").as_deref(),
        Some(b"ok".as_slice())
    );
}

/// Namespace streaming methods mirror the top-level ones and operate
/// only on the namespace they were created from.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn namespace_iter_stream_scoped_to_namespace() {
    let db = AsyncEmdb::open_in_memory();
    let ns: AsyncNamespace = db.namespace("events").await.expect("namespace");
    for i in 0..50 {
        ns.insert(format!("evt-{i:03}"), b"e")
            .await
            .expect("insert");
    }
    // Records inserted at the top level must not appear.
    for i in 0..10 {
        db.insert(format!("top-{i:03}"), b"t")
            .await
            .expect("insert");
    }
    let mut stream = ns.iter_stream().await.expect("ns iter_stream");
    let mut count = 0usize;
    while let Some((k, _)) = stream.next().await {
        assert!(
            k.starts_with(b"evt-"),
            "namespace stream leaked a top-level key: {k:?}"
        );
        count += 1;
    }
    assert_eq!(count, 50);
}

/// Streams must be `Send` so they can move across `.await` points
/// on multi-thread tokio runtimes. This is a compile-time check —
/// if the bound regresses, the test won't build.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streams_are_send() {
    fn assert_send<T: Send>(_: &T) {}
    let db = AsyncEmdb::open_in_memory();
    db.insert("k", "v").await.expect("insert");
    let stream = db.iter_stream().await.expect("iter_stream");
    assert_send(&stream);
    let key_stream = db.keys_stream().await.expect("keys_stream");
    assert_send(&key_stream);
}

/// Backpressure: with a large producer and a slow consumer, the
/// pump task should not race ahead unbounded. We don't measure
/// memory directly — we just verify the run completes correctly
/// when the consumer adds delay between reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn iter_stream_backpressure_round_trip() {
    let db = AsyncEmdb::open_in_memory();
    const N: usize = 256;
    for i in 0..N {
        db.insert(format!("k{i:04}"), b"x").await.expect("insert");
    }
    let mut stream = db.iter_stream().await.expect("iter_stream");
    let mut count = 0usize;
    while let Some(_pair) = stream.next().await {
        count += 1;
        // Force a yield every record so the pump task can't just
        // burn through to completion in one go.
        tokio::task::yield_now().await;
    }
    assert_eq!(count, N);
}

/// Cross-check: streaming and eager iteration must produce the same
/// set of records for the same database state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn iter_stream_matches_eager_iter() {
    let db = AsyncEmdb::open_in_memory();
    for i in 0..100 {
        db.insert(format!("k{i:03}"), format!("v{i}"))
            .await
            .expect("insert");
    }
    let eager = db.iter().await.expect("iter");
    let mut stream = db.iter_stream().await.expect("iter_stream");
    let mut streamed = Vec::new();
    while let Some(pair) = stream.next().await {
        streamed.push(pair);
    }
    let mut eager_sorted = eager;
    eager_sorted.sort();
    streamed.sort();
    assert_eq!(eager_sorted, streamed);
}
