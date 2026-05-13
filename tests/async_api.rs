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
