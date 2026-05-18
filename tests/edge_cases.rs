// Copyright 2026 James Gober. Licensed under Apache-2.0.
//
// Edge-case integration tests added in v0.9.10 (Tier 2 of the pre-1.0
// audit). These cover boundary inputs and feature compositions that
// the rest of the test suite didn't touch:
//
//   - Empty keys + empty values inside `insert_many` / `transaction`
//   - Range bounds with empty start and binary (non-UTF8) prefixes
//   - Range scans crossing tombstoned records
//   - TTL composed with `compact()`, `insert_many`, and crash recovery
//   - Crash recovery of encrypted databases
//   - Async stream cancellation under `tokio::select!`
//   - Empty-result async streams

use emdb::Emdb;

// =============================================================
// Empty / binary keys + values
// =============================================================

#[test]
fn insert_many_accepts_empty_key() {
    let db = Emdb::open_in_memory();
    let batch: Vec<(&[u8], &[u8])> = vec![(b"", b"empty-key-value"), (b"normal", b"v")];
    db.insert_many(batch).expect("insert_many");
    assert_eq!(
        db.get(b"".as_slice()).expect("get").as_deref(),
        Some(b"empty-key-value".as_slice())
    );
    assert_eq!(db.len().expect("len"), 2);
}

#[test]
fn transaction_accepts_empty_value() {
    let db = Emdb::open_in_memory();
    db.transaction(|tx| {
        tx.insert("k", "")?;
        Ok(())
    })
    .expect("commit");
    assert_eq!(db.get("k").expect("get").as_deref(), Some(b"".as_slice()));
    assert!(db.contains_key("k").expect("contains"));
}

#[test]
fn empty_key_and_empty_value_round_trip() {
    let db = Emdb::open_in_memory();
    db.insert(b"".to_vec(), b"".to_vec()).expect("insert");
    assert_eq!(
        db.get(b"".as_slice()).expect("get").as_deref(),
        Some(b"".as_slice())
    );
}

#[test]
fn binary_key_with_high_bytes_round_trips() {
    let db = Emdb::open_in_memory();
    let key: Vec<u8> = vec![0xFF, 0x00, 0xC3, 0xA9, 0xE2, 0x9C, 0x93];
    db.insert(key.clone(), b"binary-ok").expect("insert");
    assert_eq!(
        db.get(&key).expect("get").as_deref(),
        Some(b"binary-ok".as_slice())
    );
}

// =============================================================
// Range edge cases — require enable_range_scans
// =============================================================

fn open_range_db() -> Emdb {
    Emdb::builder()
        .enable_range_scans(true)
        .build()
        .expect("build")
}

#[test]
fn range_with_empty_start_bound_yields_every_record_below_end() {
    let db = open_range_db();
    db.insert("a", "1").expect("insert");
    db.insert("b", "2").expect("insert");
    db.insert("c", "3").expect("insert");

    // RangeBounds: include everything from the empty start to "c".
    let results = db.range(b"".to_vec()..b"c".to_vec()).expect("range");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, b"a");
    assert_eq!(results[1].0, b"b");
}

#[test]
fn range_scan_crosses_tombstones_correctly() {
    let db = open_range_db();
    for k in ["a", "b", "c", "d", "e"] {
        db.insert(k, "v").expect("insert");
    }
    // Remove some — those slots become tombstones in the journal but
    // the live secondary index must skip them.
    let _ = db.remove("b").expect("remove");
    let _ = db.remove("d").expect("remove");

    let results = db.range(b"a".to_vec()..b"f".to_vec()).expect("range");
    let keys: Vec<_> = results.iter().map(|(k, _)| k.clone()).collect();
    assert_eq!(keys, vec![b"a".to_vec(), b"c".to_vec(), b"e".to_vec()]);
}

#[test]
fn range_prefix_with_binary_non_utf8_prefix() {
    let db = open_range_db();

    let prefix: Vec<u8> = vec![0xFE, 0xFF];
    let mut k1 = prefix.clone();
    k1.push(0x01);
    let mut k2 = prefix.clone();
    k2.push(0x02);
    let outside: Vec<u8> = vec![0xFD, 0x00];

    db.insert(k1.clone(), "in").expect("insert");
    db.insert(k2.clone(), "in").expect("insert");
    db.insert(outside.clone(), "out").expect("insert");

    let results = db.range_prefix(&prefix).expect("range_prefix");
    assert_eq!(results.len(), 2);
    let keys: Vec<_> = results.iter().map(|(k, _)| k.clone()).collect();
    assert!(keys.contains(&k1));
    assert!(keys.contains(&k2));
    assert!(!keys.contains(&outside));
}

// =============================================================
// TTL composition
// =============================================================

#[cfg(feature = "ttl")]
#[test]
fn ttl_via_insert_many_inherits_default_ttl() {
    use std::thread;
    use std::time::Duration;

    let db = Emdb::builder()
        .default_ttl(Duration::from_millis(50))
        .build()
        .expect("build");

    let batch: Vec<(String, String)> = (0..20)
        .map(|i| (format!("k{i:02}"), format!("v{i}")))
        .collect();
    db.insert_many(batch.iter().map(|(k, v)| (k.as_str(), v.as_str())))
        .expect("insert_many");

    // Visible immediately.
    for i in 0..20 {
        assert!(db.get(format!("k{i:02}")).expect("get").is_some());
    }

    thread::sleep(Duration::from_millis(80));

    // All expired via lazy filter.
    for i in 0..20 {
        assert!(
            db.get(format!("k{i:02}")).expect("get").is_none(),
            "k{i:02} should have expired"
        );
    }
}

#[cfg(feature = "ttl")]
#[test]
fn compact_drops_expired_records() {
    use emdb::Ttl;
    use std::thread;
    use std::time::Duration;

    let db = Emdb::open_in_memory();

    // 10 records that expire fast.
    for i in 0..10 {
        db.insert_with_ttl(
            format!("expiring-{i}"),
            "v",
            Ttl::After(Duration::from_millis(50)),
        )
        .expect("insert");
    }
    // 5 records that never expire.
    for i in 0..5 {
        db.insert_with_ttl(format!("permanent-{i}"), "v", Ttl::Never)
            .expect("insert");
    }

    thread::sleep(Duration::from_millis(80));

    // Eager sweep evicts the expired set first so compaction has
    // tombstones to drop. (Compaction skips records whose live offset
    // is no longer in the index.)
    let swept = db.sweep_expired();
    assert_eq!(swept, 10, "sweep_expired should evict all 10");

    db.compact().expect("compact");

    // Permanents survive.
    for i in 0..5 {
        assert_eq!(
            db.get(format!("permanent-{i}")).expect("get").as_deref(),
            Some(b"v".as_slice())
        );
    }
    // Expirers are gone.
    for i in 0..10 {
        assert!(db.get(format!("expiring-{i}")).expect("get").is_none());
    }
    assert_eq!(db.len().expect("len"), 5);
}

// =============================================================
// Crash recovery composition (file-backed reopens)
// =============================================================

fn tmp_path(label: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |d| d.as_nanos());
    let tid = std::thread::current().id();
    p.push(format!("emdb-edge-{label}-{nanos}-{tid:?}.emdb"));
    p
}

fn cleanup(path: &std::path::Path) {
    let _ = std::fs::remove_file(path);
    let display = path.display();
    let _ = std::fs::remove_file(format!("{display}.lock"));
    let _ = std::fs::remove_file(format!("{display}.meta"));
    let _ = std::fs::remove_file(format!("{display}.compact.tmp"));
}

#[cfg(feature = "ttl")]
#[test]
fn ttl_records_survive_drop_and_reopen() {
    use emdb::Ttl;
    use std::time::Duration;

    let path = tmp_path("ttl-recovery");
    cleanup(&path);

    {
        let db = Emdb::open(&path).expect("open");
        db.insert_with_ttl("ephemeral", "v", Ttl::After(Duration::from_secs(3600)))
            .expect("insert");
        db.insert("permanent", "p").expect("insert");
        db.flush().expect("flush");
    } // drop here

    let db = Emdb::open(&path).expect("reopen");
    assert_eq!(
        db.get("ephemeral").expect("get").as_deref(),
        Some(b"v".as_slice())
    );
    assert_eq!(
        db.get("permanent").expect("get").as_deref(),
        Some(b"p".as_slice())
    );
    // TTL metadata also survives.
    let remaining = db.ttl("ephemeral").expect("ttl");
    assert!(
        remaining.is_some_and(|d| d.as_secs() >= 3590),
        "TTL should survive reopen"
    );
    drop(db);
    cleanup(&path);
}

#[cfg(all(feature = "encrypt", feature = "ttl"))]
#[test]
fn encrypted_db_with_ttl_survives_drop_and_reopen() {
    use emdb::Ttl;
    use std::time::Duration;

    const KEY: [u8; 32] = *b"recovery-key--32-bytes-12345678!";
    let path = tmp_path("enc-ttl-recovery");
    cleanup(&path);

    {
        let db = Emdb::builder()
            .path(&path)
            .encryption_key(KEY)
            .build()
            .expect("open");
        for i in 0..30_u32 {
            db.insert_with_ttl(
                format!("k{i:02}"),
                format!("v{i}"),
                Ttl::After(Duration::from_secs(3600)),
            )
            .expect("insert");
        }
        db.flush().expect("flush");
        db.checkpoint().expect("checkpoint");
    }

    let db = Emdb::builder()
        .path(&path)
        .encryption_key(KEY)
        .build()
        .expect("reopen encrypted");
    assert_eq!(db.len().expect("len"), 30);
    for i in 0..30_u32 {
        assert_eq!(
            db.get(format!("k{i:02}")).expect("get").as_deref(),
            Some(format!("v{i}").as_bytes()),
            "record {i} missing after reopen"
        );
        // TTL metadata round-trips through the encrypted envelope.
        assert!(db.ttl(format!("k{i:02}")).expect("ttl").is_some());
    }
    drop(db);
    cleanup(&path);
}

// =============================================================
// Async stream cancellation + empty-result streams
// =============================================================

#[cfg(feature = "async")]
mod async_edges {
    use super::*;
    use emdb::AsyncEmdb;
    use tokio_stream::StreamExt;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn empty_namespace_iter_stream_terminates_cleanly() {
        let db = AsyncEmdb::open_in_memory();
        let mut stream = db.iter_stream().await.expect("iter_stream");
        assert!(stream.next().await.is_none(), "empty stream yields None");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn keys_stream_on_empty_db_terminates() {
        let db = AsyncEmdb::open_in_memory();
        let mut stream = db.keys_stream().await.expect("keys_stream");
        assert!(stream.next().await.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stream_cancelled_via_select_does_not_panic() {
        let db = AsyncEmdb::open_in_memory();
        for i in 0_u32..1000 {
            db.insert(format!("k{i:04}"), b"v").await.expect("insert");
        }
        let mut stream = db.iter_stream().await.expect("iter_stream");

        // Race the stream against a short timer. The timer wins on
        // any realistic schedule; the stream is cancelled mid-iteration
        // by the `select!` losing branch dropping its future.
        let mut count = 0_usize;
        let result = tokio::select! {
            biased;
            _ = async {
                while let Some(_pair) = stream.next().await {
                    count += 1;
                    if count >= 50 {
                        // Yield to let the timer fire.
                        tokio::task::yield_now().await;
                    }
                }
            } => "stream finished",
            _ = tokio::time::sleep(std::time::Duration::from_millis(5)) => "timer fired",
        };
        // We don't care which branch wins — both are legal. The
        // contract is: the runtime stays healthy afterwards.
        let _ = result;

        // Round-trip a normal op to confirm the runtime + pump task
        // didn't get wedged.
        db.insert("post-cancel", "ok").await.expect("post insert");
        assert_eq!(
            db.get("post-cancel").await.expect("get").as_deref(),
            Some(b"ok".as_slice())
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn range_stream_on_empty_db_terminates() {
        let path = tmp_path("empty-range-stream");
        cleanup(&path);
        let db = Emdb::builder()
            .path(&path)
            .enable_range_scans(true)
            .build_async()
            .await
            .expect("build_async");
        let mut stream = db
            .range_stream(b"a".to_vec()..b"z".to_vec())
            .await
            .expect("range_stream");
        assert!(stream.next().await.is_none());
        drop(db);
        cleanup(&path);
    }
}
