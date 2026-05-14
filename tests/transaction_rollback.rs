// Copyright 2026 James Gober. Licensed under Apache-2.0.
//
// Integration tests for transaction rollback semantics — both sync
// (`Emdb::transaction`) and async (`AsyncEmdb::transaction`).
//
// The contract under test:
//
//   1. When the closure returns `Ok(_)`, every staged write is
//      committed and visible.
//   2. When the closure returns `Err(_)`, *no* staged writes are
//      visible — the rollback is total.
//   3. Reads inside the transaction see staged writes (read-your-writes).
//   4. A failing transaction does not corrupt or alter pre-existing
//      database state.
//   5. The transaction closure receives a `&mut Transaction` and
//      can return arbitrary `T` (not just `()`).
//
// Pre-0.9.8 this surface had a `tests/loom_tests.rs` smoke test but
// no integration coverage of the rollback contract.

use emdb::{Emdb, Error};

// -------------------------------------------------------------
// Sync rollback
// -------------------------------------------------------------

#[test]
fn err_from_closure_drops_every_staged_write() {
    let db = Emdb::open_in_memory();

    let result: Result<(), Error> = db.transaction(|tx| {
        tx.insert("staged-1", "v1")?;
        tx.insert("staged-2", "v2")?;
        tx.insert("staged-3", "v3")?;
        Err(Error::InvalidConfig("simulated failure"))
    });

    assert!(result.is_err(), "transaction should have failed");
    assert!(
        db.get("staged-1").expect("get").is_none(),
        "staged-1 must not be visible after rollback"
    );
    assert!(
        db.get("staged-2").expect("get").is_none(),
        "staged-2 must not be visible after rollback"
    );
    assert!(
        db.get("staged-3").expect("get").is_none(),
        "staged-3 must not be visible after rollback"
    );
    assert_eq!(
        db.len().expect("len"),
        0,
        "no records should be live after rollback"
    );
}

#[test]
fn rollback_preserves_pre_transaction_state() {
    let db = Emdb::open_in_memory();
    db.insert("pre-existing", "original").expect("setup");

    let _ = db.transaction::<_, ()>(|tx| {
        // Overwrite the pre-existing record inside the txn.
        tx.insert("pre-existing", "modified")?;
        // Add a new one.
        tx.insert("new-key", "new-value")?;
        // Remove something that exists.
        let prev = tx.remove("pre-existing")?;
        assert_eq!(prev.as_deref(), Some(b"modified".as_slice()));
        // Now fail.
        Err(Error::InvalidConfig("abort"))
    });

    // The pre-existing record must still be the ORIGINAL value.
    assert_eq!(
        db.get("pre-existing").expect("get").as_deref(),
        Some(b"original".as_slice()),
        "rollback must restore the pre-transaction value"
    );
    assert!(
        db.get("new-key").expect("get").is_none(),
        "transaction-only writes must not survive rollback"
    );
    assert_eq!(db.len().expect("len"), 1);
}

#[test]
fn successful_commit_makes_every_write_visible() {
    let db = Emdb::open_in_memory();

    db.transaction(|tx| {
        tx.insert("a", "1")?;
        tx.insert("b", "2")?;
        tx.insert("c", "3")?;
        Ok(())
    })
    .expect("commit");

    assert_eq!(db.get("a").expect("get").as_deref(), Some(b"1".as_slice()));
    assert_eq!(db.get("b").expect("get").as_deref(), Some(b"2".as_slice()));
    assert_eq!(db.get("c").expect("get").as_deref(), Some(b"3".as_slice()));
    assert_eq!(db.len().expect("len"), 3);
}

#[test]
fn read_your_writes_inside_transaction() {
    let db = Emdb::open_in_memory();
    db.insert("k", "outside").expect("setup");

    db.transaction(|tx| {
        assert_eq!(
            tx.get("k")?.as_deref(),
            Some(b"outside".as_slice()),
            "txn read should see the pre-existing record"
        );
        tx.insert("k", "inside")?;
        assert_eq!(
            tx.get("k")?.as_deref(),
            Some(b"inside".as_slice()),
            "txn read after staged write should see the staged value"
        );
        tx.remove("k")?;
        assert!(
            tx.get("k")?.is_none(),
            "txn read after staged remove should return None"
        );
        Ok(())
    })
    .expect("commit");

    // After commit: the remove won.
    assert!(db.get("k").expect("get").is_none());
}

#[test]
fn closure_can_return_a_value() {
    let db = Emdb::open_in_memory();
    db.insert("counter", "10").expect("setup");

    let new_value: u64 = db
        .transaction(|tx| {
            let current = tx
                .get("counter")?
                .ok_or(Error::InvalidConfig("counter missing"))?;
            let parsed: u64 = std::str::from_utf8(&current)
                .map_err(|_| Error::InvalidConfig("counter not utf-8"))?
                .parse()
                .map_err(|_| Error::InvalidConfig("counter not u64"))?;
            let next = parsed + 1;
            tx.insert("counter", next.to_string())?;
            Ok(next)
        })
        .expect("commit");

    assert_eq!(new_value, 11);
    assert_eq!(
        db.get("counter").expect("get").as_deref(),
        Some(b"11".as_slice())
    );
}

#[test]
fn empty_transaction_is_a_noop() {
    let db = Emdb::open_in_memory();
    db.insert("k", "v").expect("setup");

    db.transaction(|_tx| -> Result<(), Error> { Ok(()) })
        .expect("commit empty");

    assert_eq!(db.len().expect("len"), 1);
    assert_eq!(db.get("k").expect("get").as_deref(), Some(b"v".as_slice()));
}

#[test]
fn multiple_writes_to_same_key_keep_the_last_write() {
    let db = Emdb::open_in_memory();

    db.transaction(|tx| {
        tx.insert("k", "v1")?;
        tx.insert("k", "v2")?;
        tx.insert("k", "v3")?;
        Ok(())
    })
    .expect("commit");

    assert_eq!(
        db.get("k").expect("get").as_deref(),
        Some(b"v3".as_slice()),
        "last staged insert should win"
    );
    assert_eq!(
        db.len().expect("len"),
        1,
        "three writes to the same key are one record"
    );
}

#[test]
fn insert_then_remove_inside_txn_yields_no_record() {
    let db = Emdb::open_in_memory();

    db.transaction(|tx| {
        tx.insert("temp", "ephemeral")?;
        let _ = tx.remove("temp")?;
        Ok(())
    })
    .expect("commit");

    assert!(db.get("temp").expect("get").is_none());
    assert_eq!(db.len().expect("len"), 0);
}

// -------------------------------------------------------------
// Sync rollback with TTL
// -------------------------------------------------------------

#[cfg(feature = "ttl")]
#[test]
fn rollback_drops_staged_insert_with_ttl() {
    use emdb::Ttl;
    use std::time::Duration;

    let db = Emdb::open_in_memory();

    let _ = db.transaction::<_, ()>(|tx| {
        tx.insert_with_ttl("session", "data", Ttl::After(Duration::from_secs(60)))?;
        Err(Error::InvalidConfig("abort"))
    });

    assert!(
        db.get("session").expect("get").is_none(),
        "TTL'd staged insert must not survive rollback"
    );
}

// -------------------------------------------------------------
// Async rollback
// -------------------------------------------------------------

#[cfg(feature = "async")]
mod async_tests {
    use super::*;
    use emdb::AsyncEmdb;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn async_err_from_closure_drops_every_staged_write() {
        let db = AsyncEmdb::open_in_memory();

        let result: Result<(), Error> = db
            .transaction(|tx| {
                tx.insert("a", "1")?;
                tx.insert("b", "2")?;
                Err(Error::InvalidConfig("simulated failure"))
            })
            .await;

        assert!(result.is_err());
        assert!(db.get("a").await.expect("get").is_none());
        assert!(db.get("b").await.expect("get").is_none());
        assert_eq!(db.len().await.expect("len"), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn async_rollback_preserves_pre_transaction_state() {
        let db = AsyncEmdb::open_in_memory();
        db.insert("k", "original").await.expect("setup");

        let _ = db
            .transaction::<_, ()>(|tx| {
                tx.insert("k", "modified")?;
                tx.insert("new", "extra")?;
                Err(Error::InvalidConfig("abort"))
            })
            .await;

        assert_eq!(
            db.get("k").await.expect("get").as_deref(),
            Some(b"original".as_slice())
        );
        assert!(db.get("new").await.expect("get").is_none());
        assert_eq!(db.len().await.expect("len"), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn async_successful_commit() {
        let db = AsyncEmdb::open_in_memory();

        let returned: u64 = db
            .transaction(|tx| {
                tx.insert("x", "10")?;
                tx.insert("y", "20")?;
                Ok(30_u64)
            })
            .await
            .expect("commit");

        assert_eq!(returned, 30);
        assert_eq!(
            db.get("x").await.expect("get").as_deref(),
            Some(b"10".as_slice())
        );
        assert_eq!(
            db.get("y").await.expect("get").as_deref(),
            Some(b"20".as_slice())
        );
    }
}
