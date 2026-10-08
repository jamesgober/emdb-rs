// Copyright 2026 James Gober. Licensed under Apache-2.0.
//
// Regression test for 1.0.3 (M6): an idle async stream used to park a
// blocking-pool thread in `blocking_send` until it was polled again,
// so a handful of slow consumers could exhaust the pool and stall
// every other async call.

#![cfg(feature = "async")]

use std::time::Duration;

use emdb::{AsyncEmdb, Result};
use futures_util::StreamExt;

#[test]
fn test_idle_streams_do_not_hold_blocking_threads() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(4)
        .enable_all()
        .build()
        .map_err(emdb::Error::Io)?;
    runtime.block_on(async {
        let db = AsyncEmdb::open_in_memory();
        let items: Vec<(String, String)> = (0..1_000)
            .map(|i| (format!("k{i:04}"), "v".to_string()))
            .collect();
        db.insert_many(items.iter().map(|(k, v)| (k.as_str(), v.as_str())))
            .await?;

        // More idle streams than blocking threads, none of them polled.
        let mut held = Vec::new();
        for _ in 0..8 {
            held.push(db.iter_stream().await?);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;

        let lookup = tokio::time::timeout(Duration::from_secs(5), db.get("k0001")).await;
        assert!(
            matches!(lookup, Ok(Ok(Some(_)))),
            "get stalled while idle streams were held"
        );

        // The held streams still deliver every record when polled.
        for stream in held {
            assert_eq!(stream.count().await, 1_000);
        }
        Ok(())
    })
}

#[test]
fn test_dropped_stream_stops_its_pump() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(2)
        .enable_all()
        .build()
        .map_err(emdb::Error::Io)?;
    runtime.block_on(async {
        let db = AsyncEmdb::open_in_memory();
        let items: Vec<(String, String)> = (0..5_000)
            .map(|i| (format!("k{i:05}"), "v".to_string()))
            .collect();
        db.insert_many(items.iter().map(|(k, v)| (k.as_str(), v.as_str())))
            .await?;
        for _ in 0..16 {
            let mut stream = db.keys_stream().await?;
            let _first = stream.next().await;
            drop(stream);
        }
        let lookup = tokio::time::timeout(Duration::from_secs(5), db.len()).await;
        assert!(matches!(lookup, Ok(Ok(5_000))));
        Ok(())
    })
}
