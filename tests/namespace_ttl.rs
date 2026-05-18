// Copyright 2026 James Gober. Licensed under Apache-2.0.
//
// Integration tests for the v0.9.10 TTL surface on `Namespace`
// (and `AsyncNamespace` under the `async` feature). Pre-0.9.10
// these methods did not exist on the namespace handle even though
// docs/API.md claimed "the same surface as Emdb."

#![cfg(feature = "ttl")]

use std::thread;
use std::time::Duration;

use emdb::{Emdb, Ttl};

// -------------------------------------------------------------
// insert_with_ttl + lazy-expiry get
// -------------------------------------------------------------

#[test]
fn insert_with_ttl_expires_lazily_on_namespace_get() {
    let db = Emdb::open_in_memory();
    let ns = db.namespace("sessions").expect("namespace");

    ns.insert_with_ttl("alice", "data", Ttl::After(Duration::from_millis(50)))
        .expect("insert_with_ttl");
    assert_eq!(
        ns.get("alice").expect("get").as_deref(),
        Some(b"data".as_slice())
    );

    thread::sleep(Duration::from_millis(80));

    assert!(
        ns.get("alice").expect("get").is_none(),
        "namespace get must filter expired records"
    );
}

#[test]
fn ttl_never_records_survive_on_namespace() {
    let db = Emdb::open_in_memory();
    let ns = db.namespace("permanent").expect("namespace");

    ns.insert_with_ttl("forever", "yes", Ttl::Never)
        .expect("insert");
    thread::sleep(Duration::from_millis(20));
    assert_eq!(
        ns.get("forever").expect("get").as_deref(),
        Some(b"yes".as_slice())
    );
}

// -------------------------------------------------------------
// default_ttl inheritance
// -------------------------------------------------------------

#[test]
fn namespace_insert_inherits_parent_default_ttl() {
    let db = Emdb::builder()
        .default_ttl(Duration::from_millis(50))
        .build()
        .expect("build");
    let ns = db.namespace("cache").expect("namespace");

    ns.insert("alpha", "v").expect("insert");
    assert_eq!(
        ns.get("alpha").expect("get").as_deref(),
        Some(b"v".as_slice())
    );

    thread::sleep(Duration::from_millis(80));
    assert!(
        ns.get("alpha").expect("get").is_none(),
        "namespace insert should inherit parent default_ttl"
    );
}

#[test]
fn namespace_insert_many_inherits_parent_default_ttl() {
    let db = Emdb::builder()
        .default_ttl(Duration::from_millis(50))
        .build()
        .expect("build");
    let ns = db.namespace("batch").expect("namespace");

    let batch: Vec<(String, String)> = (0..10)
        .map(|i| (format!("k{i}"), format!("v{i}")))
        .collect();
    ns.insert_many(batch.iter().map(|(k, v)| (k.as_str(), v.as_str())))
        .expect("insert_many");

    // All visible immediately.
    for i in 0..10 {
        assert!(ns.get(format!("k{i}")).expect("get").is_some());
    }

    thread::sleep(Duration::from_millis(80));

    for i in 0..10 {
        assert!(
            ns.get(format!("k{i}")).expect("get").is_none(),
            "k{i} should have expired via default_ttl",
        );
    }
}

// -------------------------------------------------------------
// expires_at + ttl
// -------------------------------------------------------------

#[test]
fn expires_at_and_ttl_return_correct_values() {
    let db = Emdb::open_in_memory();
    let ns = db.namespace("times").expect("namespace");

    ns.insert_with_ttl("with-ttl", "v", Ttl::After(Duration::from_secs(3600)))
        .expect("insert_with_ttl");
    ns.insert("no-ttl", "v").expect("insert");

    let expires = ns.expires_at("with-ttl").expect("expires_at");
    assert!(expires.is_some_and(|e| e > 0), "expires_at should be > 0");
    assert_eq!(
        ns.expires_at("no-ttl").expect("expires_at"),
        Some(0),
        "no-TTL record should report expires_at = 0"
    );
    assert_eq!(
        ns.expires_at("missing").expect("expires_at"),
        None,
        "missing key should report None"
    );

    let remaining = ns.ttl("with-ttl").expect("ttl");
    assert!(remaining.is_some());
    let secs = remaining.unwrap().as_secs();
    assert!(
        (3590..=3600).contains(&secs),
        "remaining TTL should be near 3600s, got {secs}"
    );

    assert_eq!(ns.ttl("no-ttl").expect("ttl"), None);
    assert_eq!(ns.ttl("missing").expect("ttl"), None);
}

// -------------------------------------------------------------
// persist (strip TTL)
// -------------------------------------------------------------

#[test]
fn persist_strips_ttl_from_namespace_record() {
    let db = Emdb::open_in_memory();
    let ns = db.namespace("ns").expect("namespace");

    ns.insert_with_ttl("k", "v", Ttl::After(Duration::from_secs(3600)))
        .expect("insert");
    assert!(ns.ttl("k").expect("ttl").is_some());

    let was_set = ns.persist("k").expect("persist");
    assert!(was_set, "persist should return true when TTL was set");

    assert!(
        ns.ttl("k").expect("ttl").is_none(),
        "TTL should be stripped after persist"
    );
    assert_eq!(
        ns.get("k").expect("get").as_deref(),
        Some(b"v".as_slice()),
        "value should survive persist"
    );

    // Persisting an already-persistent key returns false.
    let was_set_again = ns.persist("k").expect("persist");
    assert!(!was_set_again);

    // Persisting a missing key returns false.
    assert!(!ns.persist("missing").expect("persist missing"));
}

// -------------------------------------------------------------
// sweep_expired scoped to namespace
// -------------------------------------------------------------

#[test]
fn sweep_expired_only_evicts_records_in_this_namespace() {
    let db = Emdb::open_in_memory();
    let alpha = db.namespace("alpha").expect("ns alpha");
    let beta = db.namespace("beta").expect("ns beta");

    // Five records in alpha, all about to expire.
    for i in 0..5 {
        alpha
            .insert_with_ttl(format!("k{i}"), "v", Ttl::After(Duration::from_millis(50)))
            .expect("insert");
    }
    // Five in beta, also about to expire — but we're not sweeping beta.
    for i in 0..5 {
        beta.insert_with_ttl(format!("k{i}"), "v", Ttl::After(Duration::from_millis(50)))
            .expect("insert");
    }

    thread::sleep(Duration::from_millis(80));

    let evicted = alpha.sweep_expired();
    assert_eq!(evicted, 5, "all 5 alpha records should be swept");

    // alpha is empty.
    assert_eq!(alpha.len().expect("len"), 0);
    // beta records are *lazily* expired (get returns None) but the
    // sweep didn't touch them — they're still counted in len() until
    // beta.sweep_expired() runs. That's the same semantic as the
    // top-level Emdb behaviour.
    assert_eq!(beta.len().expect("len"), 5);
    for i in 0..5 {
        assert!(beta.get(format!("k{i}")).expect("get").is_none());
    }
}

// -------------------------------------------------------------
// Async parity
// -------------------------------------------------------------

#[cfg(feature = "async")]
mod async_ttl {
    use super::*;
    use emdb::AsyncEmdb;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn async_namespace_ttl_round_trip() {
        let db = AsyncEmdb::open_in_memory();
        let ns = db.namespace("session").await.expect("namespace");

        ns.insert_with_ttl("alice", "data", Ttl::After(Duration::from_millis(50)))
            .await
            .expect("insert_with_ttl");

        let exp = ns.expires_at("alice").await.expect("expires_at");
        assert!(exp.is_some_and(|e| e > 0));

        let remaining = ns.ttl("alice").await.expect("ttl");
        assert!(remaining.is_some());

        tokio::time::sleep(Duration::from_millis(80)).await;

        assert!(ns.get("alice").await.expect("get").is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn async_namespace_persist_strips_ttl() {
        let db = AsyncEmdb::open_in_memory();
        let ns = db.namespace("ns").await.expect("namespace");

        ns.insert_with_ttl("k", "v", Ttl::After(Duration::from_secs(60)))
            .await
            .expect("insert");
        assert!(ns.ttl("k").await.expect("ttl").is_some());

        let was_set = ns.persist("k").await.expect("persist");
        assert!(was_set);
        assert!(ns.ttl("k").await.expect("ttl").is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn async_namespace_sweep_expired() {
        let db = AsyncEmdb::open_in_memory();
        let ns = db.namespace("temp").await.expect("namespace");

        for i in 0_u32..5 {
            ns.insert_with_ttl(format!("k{i}"), "v", Ttl::After(Duration::from_millis(50)))
                .await
                .expect("insert");
        }

        tokio::time::sleep(Duration::from_millis(80)).await;

        let evicted = ns.sweep_expired().await.expect("sweep");
        assert_eq!(evicted, 5);
        assert_eq!(ns.len().await.expect("len"), 0);
    }
}
