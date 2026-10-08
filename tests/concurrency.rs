// Copyright 2026 James Gober. Licensed under Apache-2.0.
//
// Concurrency regression tests for 1.0.3. Each test drives the
// public API from several threads and checks an invariant that a
// pre-1.0.3 build violated:
//
// - same-key writers left memory and the log disagreeing (H9),
// - two concurrent `remove`s of one key both returned `Some` (H9),
// - `len()` drifted from the real record count (M5),
// - the range index disagreed with the hash index (H9),
// - a TTL sweep deleted a fresh concurrent re-insert (M4),
// - batch commits interleaved with single-key writes per key (H13).
//
// The racy tests repeat their scenario many times per run; set
// `EMDB_RACE_ROUNDS` to scale them up for soak runs.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use emdb::{Emdb, Result};

fn rounds(default: usize) -> usize {
    std::env::var("EMDB_RACE_ROUNDS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

fn tmp_path(label: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |d| d.as_nanos());
    p.push(format!(
        "emdb-concurrency-{label}-{}-{nanos}.emdb",
        std::process::id()
    ));
    p
}

fn cleanup(path: &std::path::Path) {
    let display = path.display().to_string();
    for suffix in ["", ".lock", ".meta", ".compact.tmp"] {
        let _ = std::fs::remove_file(format!("{display}{suffix}"));
    }
}

fn join<T>(handle: thread::JoinHandle<T>) -> T {
    match handle.join() {
        Ok(value) => value,
        Err(_) => panic!("worker thread panicked"),
    }
}

#[test]
fn test_same_key_writers_memory_matches_log_after_reopen() -> Result<()> {
    let path = tmp_path("same-key");
    let rounds = rounds(200);
    let threads = 8;
    let in_memory: Vec<Option<Vec<u8>>> = {
        let db = Arc::new(Emdb::open(&path)?);
        for round in 0..rounds {
            let barrier = Arc::new(Barrier::new(threads));
            let handles: Vec<_> = (0..threads)
                .map(|t| {
                    let (db, barrier) = (Arc::clone(&db), Arc::clone(&barrier));
                    thread::spawn(move || -> Result<()> {
                        let _ = barrier.wait();
                        db.insert(format!("key{round}"), format!("t{t}"))
                    })
                })
                .collect();
            for handle in handles {
                join(handle)?;
            }
        }
        db.flush()?;
        (0..rounds)
            .map(|r| db.get(format!("key{r}")))
            .collect::<Result<_>>()?
    };
    let db = Emdb::open(&path)?;
    let mut diverged = 0;
    for (r, expected) in in_memory.iter().enumerate() {
        if &db.get(format!("key{r}"))? != expected {
            diverged += 1;
        }
    }
    drop(db);
    cleanup(&path);
    assert_eq!(diverged, 0, "{diverged}/{rounds} keys differ after reopen");
    Ok(())
}

#[test]
fn test_insert_remove_race_memory_matches_log_after_reopen() -> Result<()> {
    let path = tmp_path("ins-rem");
    let rounds = rounds(300);
    let in_memory: Vec<Option<Vec<u8>>> = {
        let db = Arc::new(Emdb::open(&path)?);
        for round in 0..rounds {
            let key = format!("k{round}");
            db.insert(key.clone(), "seed")?;
            let barrier = Arc::new(Barrier::new(2));
            let remover = {
                let (db, barrier, key) = (Arc::clone(&db), Arc::clone(&barrier), key.clone());
                thread::spawn(move || -> Result<()> {
                    let _ = barrier.wait();
                    db.remove(&key).map(|_| ())
                })
            };
            let inserter = {
                let (db, barrier) = (Arc::clone(&db), Arc::clone(&barrier));
                thread::spawn(move || -> Result<()> {
                    let _ = barrier.wait();
                    db.insert(key, "new")
                })
            };
            join(remover)?;
            join(inserter)?;
        }
        db.flush()?;
        (0..rounds)
            .map(|r| db.get(format!("k{r}")))
            .collect::<Result<_>>()?
    };
    let db = Emdb::open(&path)?;
    let mut diverged = 0;
    for (r, expected) in in_memory.iter().enumerate() {
        if &db.get(format!("k{r}"))? != expected {
            diverged += 1;
        }
    }
    drop(db);
    cleanup(&path);
    assert_eq!(diverged, 0, "{diverged}/{rounds} keys differ after reopen");
    Ok(())
}

#[test]
fn test_concurrent_removes_of_one_key_return_value_once() -> Result<()> {
    let db = Arc::new(Emdb::open_in_memory());
    for round in 0..rounds(500) {
        let key = format!("k{round}");
        db.insert(key.clone(), "v")?;
        let barrier = Arc::new(Barrier::new(2));
        let handles: Vec<_> = (0..2)
            .map(|_| {
                let (db, barrier, key) = (Arc::clone(&db), Arc::clone(&barrier), key.clone());
                thread::spawn(move || -> Result<bool> {
                    let _ = barrier.wait();
                    Ok(db.remove(&key)?.is_some())
                })
            })
            .collect();
        let mut winners = 0;
        for handle in handles {
            if join(handle)? {
                winners += 1;
            }
        }
        assert_eq!(winners, 1, "round {round}: {winners} removers got Some");
    }
    assert_eq!(db.len()?, 0);
    Ok(())
}

#[test]
fn test_len_matches_live_records_under_contention() -> Result<()> {
    let db = Arc::new(Emdb::open_in_memory());
    let keys = 64_u64;
    let threads = 8;
    let barrier = Arc::new(Barrier::new(threads));
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let (db, barrier) = (Arc::clone(&db), Arc::clone(&barrier));
            thread::spawn(move || -> Result<()> {
                let _ = barrier.wait();
                let mut state = 0x9E37_79B9_7F4A_7C15_u64 ^ t as u64;
                for _ in 0..(rounds(500) * 20) {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    let key = format!("k{}", state % keys);
                    if state & 0x100 == 0 {
                        db.insert(key, "v")?;
                    } else {
                        let _ = db.remove(key)?;
                    }
                }
                Ok(())
            })
        })
        .collect();
    for handle in handles {
        join(handle)?;
    }
    let live = (0..keys)
        .filter(|i| matches!(db.get(format!("k{i}")), Ok(Some(_))))
        .count();
    assert_eq!(db.len()?, live, "len() drifted from the live record count");
    assert_eq!(db.keys()?.count(), live);
    Ok(())
}

#[test]
fn test_clear_racing_inserts_keeps_len_consistent() -> Result<()> {
    for _ in 0..rounds(20) {
        let db = Arc::new(Emdb::open_in_memory());
        let stop = Arc::new(AtomicBool::new(false));
        let writer = {
            let (db, stop) = (Arc::clone(&db), Arc::clone(&stop));
            thread::spawn(move || -> Result<()> {
                let mut i = 0_u64;
                while !stop.load(Ordering::Relaxed) {
                    db.insert(format!("k{}", i % 2_000), "v")?;
                    i += 1;
                }
                Ok(())
            })
        };
        thread::sleep(std::time::Duration::from_millis(2));
        db.clear()?;
        stop.store(true, Ordering::Relaxed);
        join(writer)?;
        let actual = db.keys()?.count();
        assert_eq!(db.len()?, actual);
        for key in db.keys()?.collect::<Vec<_>>() {
            let _ = db.remove(key)?;
        }
        assert_eq!(db.len()?, 0, "len() must return to 0, never wrap");
    }
    Ok(())
}

#[test]
fn test_range_index_agrees_with_hash_index_after_races() -> Result<()> {
    let db = Arc::new(Emdb::builder().enable_range_scans(true).build()?);
    let keys = 32_u64;
    let threads = 6;
    let barrier = Arc::new(Barrier::new(threads));
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let (db, barrier) = (Arc::clone(&db), Arc::clone(&barrier));
            thread::spawn(move || -> Result<()> {
                let _ = barrier.wait();
                let mut state = 0xD1B5_4A32_D192_ED03_u64 ^ (t as u64 + 1);
                for i in 0..(rounds(500) * 10) {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    let key = format!("k{:02}", state % keys);
                    match state % 3 {
                        0 => {
                            let _ = db.remove(&key)?;
                        }
                        1 => db.insert(key, format!("v{t}-{i}"))?,
                        _ => db.transaction(|tx| {
                            tx.insert(key.clone(), format!("tx{t}-{i}"))?;
                            Ok(())
                        })?,
                    }
                }
                Ok(())
            })
        })
        .collect();
    for handle in handles {
        join(handle)?;
    }
    let ranged: Vec<(Vec<u8>, Vec<u8>)> = db.range(b"k".to_vec()..b"l".to_vec())?;
    let mut by_get = Vec::new();
    for i in 0..keys {
        let key = format!("k{i:02}").into_bytes();
        if let Some(value) = db.get(&key)? {
            by_get.push((key, value));
        }
    }
    assert_eq!(ranged, by_get, "range index and hash index disagree");
    assert_eq!(db.len()?, by_get.len());
    Ok(())
}

#[test]
fn test_transaction_commit_orders_whole_batch_against_single_writes() -> Result<()> {
    let path = tmp_path("tx-order");
    let rounds = rounds(200);
    type Pair = (Option<Vec<u8>>, Option<Vec<u8>>);
    let snapshot: Vec<Pair> = {
        let db = Arc::new(Emdb::open(&path)?);
        for round in 0..rounds {
            let (a, b) = (format!("a{round}"), format!("b{round}"));
            let barrier = Arc::new(Barrier::new(3));
            let committer = {
                let (db, barrier, a, b) =
                    (Arc::clone(&db), Arc::clone(&barrier), a.clone(), b.clone());
                thread::spawn(move || -> Result<()> {
                    let _ = barrier.wait();
                    db.transaction(|tx| {
                        tx.insert(a.clone(), "tx")?;
                        tx.insert(b.clone(), "tx")?;
                        Ok(())
                    })
                })
            };
            let writer_a = {
                let (db, barrier) = (Arc::clone(&db), Arc::clone(&barrier));
                thread::spawn(move || -> Result<()> {
                    let _ = barrier.wait();
                    db.insert(a, "single")
                })
            };
            let remover_b = {
                let (db, barrier) = (Arc::clone(&db), Arc::clone(&barrier));
                thread::spawn(move || -> Result<()> {
                    let _ = barrier.wait();
                    db.remove(b).map(|_| ())
                })
            };
            join(committer)?;
            join(writer_a)?;
            join(remover_b)?;
        }
        db.flush()?;
        (0..rounds)
            .map(|r| Ok((db.get(format!("a{r}"))?, db.get(format!("b{r}"))?)))
            .collect::<Result<_>>()?
    };
    let db = Emdb::open(&path)?;
    for (r, expected) in snapshot.iter().enumerate() {
        let got = (db.get(format!("a{r}"))?, db.get(format!("b{r}"))?);
        assert_eq!(&got, expected, "round {r}: memory and log disagree");
    }
    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn test_writers_readers_and_transactions_do_not_deadlock() -> Result<()> {
    // Formerly `tests/loom_tests.rs` (which never ran under loom):
    // exercises the stripe, shard and append lock orders together.
    let db = Arc::new(Emdb::open_in_memory());
    let writer = {
        let db = Arc::clone(&db);
        thread::spawn(move || -> Result<()> {
            for i in 0_u32..2_000 {
                db.insert(format!("k{i}"), format!("v{i}"))?;
                if i % 100 == 0 {
                    db.flush()?;
                }
            }
            Ok(())
        })
    };
    let batcher = {
        let db = Arc::clone(&db);
        thread::spawn(move || -> Result<()> {
            for i in 0_u32..500 {
                db.transaction(|tx| {
                    tx.insert(format!("k{}", i * 4 % 2_000), "tx")?;
                    tx.insert(format!("tx:{i}"), "tx")?;
                    let _ = tx.remove(format!("k{}", (i * 7 + 1) % 2_000))?;
                    Ok(())
                })?;
                db.insert_many(
                    (0..8_u32).map(|j| (format!("k{}", (i + j * 250) % 2_000), "many")),
                )?;
            }
            Ok(())
        })
    };
    let readers: Vec<_> = (0..2)
        .map(|_| {
            let db = Arc::clone(&db);
            thread::spawn(move || -> Result<usize> {
                let mut hits = 0;
                for i in 0_u32..20_000 {
                    if db.get(format!("k{}", i % 2_000))?.is_some() {
                        hits += 1;
                    }
                }
                Ok(hits)
            })
        })
        .collect();
    join(writer)?;
    join(batcher)?;
    for reader in readers {
        let _hits = join(reader)?;
    }
    Ok(())
}

#[test]
fn test_iter_yields_records_as_of_the_snapshot() -> Result<()> {
    // Documented behaviour: offsets are snapshotted at `iter()` time,
    // so a later remove or overwrite does not change what is yielded.
    let db = Emdb::open_in_memory();
    db.insert("a", "1")?;
    db.insert("b", "2")?;
    let iter = db.iter()?;
    let _ = db.remove("a")?;
    db.insert("b", "3")?;
    db.insert("c", "4")?;
    let mut got: Vec<(Vec<u8>, Vec<u8>)> = iter.collect();
    got.sort();
    assert_eq!(
        got,
        vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec())
        ]
    );
    Ok(())
}

#[test]
fn test_iter_does_not_yield_other_namespaces_records() -> Result<()> {
    let db = Emdb::open_in_memory();
    let other = db.namespace("other")?;
    for i in 0_u32..50 {
        db.insert(format!("d{i:03}"), "dv")?;
        other.insert(format!("o{i:03}"), "ov")?;
    }
    let keys: Vec<Vec<u8>> = db.keys()?.collect();
    assert_eq!(keys.len(), 50);
    assert!(keys.iter().all(|k| k.starts_with(b"d")));
    let other_keys: Vec<Vec<u8>> = other.keys()?.collect();
    assert_eq!(other_keys.len(), 50);
    assert!(other_keys.iter().all(|k| k.starts_with(b"o")));
    Ok(())
}

#[test]
fn test_range_iter_is_lazy_and_sees_keys_ahead_of_the_cursor() -> Result<()> {
    let db = Emdb::builder().enable_range_scans(true).build()?;
    let items: Vec<(String, String)> = (0..50_000_u32)
        .map(|i| (format!("user:{i:08}"), "v".to_string()))
        .collect();
    db.insert_many(items.iter().map(|(k, v)| (k.as_str(), v.as_str())))?;

    // Before 1.0.3 `iter_from` copied the whole range into a Vec up
    // front; now it is a cursor. Throughput is covered by the
    // benches; here the observable difference is that the iterator
    // sees a key inserted after it was created.
    let first: Vec<_> = db.iter_from("user:")?.take(10).collect();
    assert_eq!(first.len(), 10);
    assert_eq!(first[0].0, b"user:00000000".to_vec());
    assert_eq!(db.iter_from("user:")?.count(), 50_000);

    // The cursor is not a snapshot: a key inserted ahead of it shows up.
    let mut iter = db.range_prefix_iter("user:")?;
    let _ = iter.next();
    db.insert("user:99999999", "late")?;
    assert!(iter.any(|(k, _)| k == b"user:99999999".to_vec()));
    Ok(())
}

// ---------------------------------------------------------------------
// Compaction against concurrent writers, namespace drops and cursors.
// ---------------------------------------------------------------------

/// Wait for `workers` completion messages, failing (instead of hanging
/// the test run) when one does not arrive in time.
fn wait_all(done: &std::sync::mpsc::Receiver<&'static str>, workers: usize, what: &str) {
    let deadline = std::time::Duration::from_secs(120);
    for _ in 0..workers {
        match done.recv_timeout(deadline) {
            Ok(_) => {}
            Err(err) => panic!("{what}: a worker did not finish ({err:?}); deadlock?"),
        }
    }
}

/// Every write takes the engine write gate shared exactly once. If
/// `insert_many` (which delegates to the batch path) or a transaction
/// commit took it a second time while holding it, a compaction queued
/// for the gate in between would block the second acquisition forever
/// (the gate is a fair lock) and this test would time out.
#[test]
fn test_batches_and_transactions_racing_compaction_do_not_deadlock() -> Result<()> {
    let path = tmp_path("gate-once");
    cleanup(&path);
    let db = Arc::new(Emdb::open(&path)?);
    let stop = Arc::new(AtomicBool::new(false));
    let (done_tx, done_rx) = std::sync::mpsc::channel::<&'static str>();
    let rounds = rounds(300) as u32;

    let compactor = {
        let (db, stop, done) = (Arc::clone(&db), Arc::clone(&stop), done_tx.clone());
        thread::spawn(move || -> Result<()> {
            while !stop.load(Ordering::Relaxed) {
                db.compact()?;
            }
            let _ = done.send("compactor");
            Ok(())
        })
    };
    let workers: Vec<_> = (0..3_u32)
        .map(|t| {
            let (db, done) = (Arc::clone(&db), done_tx.clone());
            thread::spawn(move || -> Result<()> {
                for i in 0..rounds {
                    db.insert_many((0..32_u32).map(|j| (format!("t{t}:b{j}"), format!("{i}"))))?;
                    db.transaction(|tx| {
                        tx.insert(format!("t{t}:tx"), format!("{i}"))?;
                        let _ = tx.remove(format!("t{t}:b{}", i % 32))?;
                        Ok(())
                    })?;
                    db.insert(format!("t{t}:single"), format!("{i}"))?;
                    let _ = db.remove(format!("t{t}:gone"))?;
                }
                let _ = done.send("worker");
                Ok(())
            })
        })
        .collect();
    wait_all(&done_rx, workers.len(), "writers racing compaction");
    stop.store(true, Ordering::Relaxed);
    wait_all(&done_rx, 1, "compactor");
    join(compactor)?;
    for worker in workers {
        join(worker)?;
    }

    let last = format!("{}", rounds - 1);
    let check = |db: &Emdb| -> Result<()> {
        for t in 0..3_u32 {
            assert_eq!(db.get(format!("t{t}:tx"))?, Some(last.clone().into_bytes()));
            assert_eq!(
                db.get(format!("t{t}:single"))?,
                Some(last.clone().into_bytes())
            );
        }
        Ok(())
    };
    check(&db)?;
    db.flush()?;
    drop(db);
    let db = Emdb::open(&path)?;
    check(&db)?;
    drop(db);
    cleanup(&path);
    Ok(())
}

/// A write through a namespace handle looks the namespace up only
/// after it holds the write gate, so it either lands before
/// `drop_namespace` (and is tombstoned by it) or fails with the
/// namespace gone. Nothing reaches the log after the unbind record,
/// so the database reopens and the name stays dropped.
#[test]
fn test_writes_racing_drop_namespace_leave_a_clean_log() -> Result<()> {
    for round in 0..rounds(20) {
        let path = tmp_path(&format!("drop-race-{round}"));
        cleanup(&path);
        {
            let db = Arc::new(Emdb::open(&path)?);
            let ns = db.namespace("victim")?;
            ns.insert("seed", "v")?;
            let barrier = Arc::new(Barrier::new(3));
            let writers: Vec<_> = (0..2_u32)
                .map(|t| {
                    let (ns, barrier) = (ns.clone(), Arc::clone(&barrier));
                    thread::spawn(move || {
                        let _ = barrier.wait();
                        for i in 0_u32..20_000 {
                            let key = format!("t{t}:{}", i % 64);
                            let result = if i % 3 == 0 {
                                ns.remove(key).map(|_| ())
                            } else {
                                ns.insert(key, "v")
                            };
                            if result.is_err() {
                                return;
                            }
                        }
                    })
                })
                .collect();
            let _ = barrier.wait();
            thread::sleep(std::time::Duration::from_micros(200));
            assert!(db.drop_namespace("victim")?);
            for writer in writers {
                join(writer);
            }
            db.flush()?;
        }
        let db = Emdb::open(&path)?;
        assert_eq!(db.list_namespaces()?, vec![String::new()]);
        let ns = db.namespace("victim")?;
        assert_eq!(ns.len()?, 0, "dropped namespace came back with data");
        drop(db);
        cleanup(&path);
    }
    Ok(())
}

/// A range iterator that is part-way through when `compact()` runs
/// keeps its place: the page it already holds resolves against the
/// mapping pinned with it, and the next page re-attaches to the
/// namespace's new skiplist after the last key it handed out. No key
/// comes out twice or out of order, values are current, and records
/// of other namespaces never appear.
#[test]
fn test_range_iter_continues_across_compaction() -> Result<()> {
    let path = tmp_path("range-compact");
    cleanup(&path);
    let db = Emdb::builder()
        .path(&path)
        .enable_range_scans(true)
        .build()?;
    let ns = db.namespace("ns")?;
    for i in 0..200_u32 {
        let key = format!("k{i:03}");
        db.insert(key.clone(), "default")?;
        ns.insert(key.clone(), "v1")?;
        ns.insert(key, "v2")?;
    }
    // Pages are 16 then 32 entries: 40 items consumed means the first
    // 48 keys were fetched before the compaction.
    let mut iter = ns.range_iter(b"k".to_vec()..b"l".to_vec())?;
    let mut seen: Vec<(Vec<u8>, Vec<u8>)> = iter.by_ref().take(40).collect();
    let _ = ns.remove("k100")?;
    ns.insert("k150", "v3")?;
    ns.insert("k005x", "behind")?;
    db.compact()?;
    seen.extend(iter);

    for pair in seen.windows(2) {
        assert!(pair[0].0 < pair[1].0, "keys out of order or repeated");
    }
    let keys: Vec<String> = seen
        .iter()
        .map(|(k, _)| String::from_utf8_lossy(k).into_owned())
        .collect();
    assert!(!keys.contains(&"k100".to_string()), "removed key yielded");
    assert!(
        !keys.contains(&"k005x".to_string()),
        "key behind the cursor yielded"
    );
    assert_eq!(keys.len(), 199);
    for (key, value) in &seen {
        let expected: &[u8] = if key == b"k150" { b"v3" } else { b"v2" };
        assert_eq!(
            value.as_slice(),
            expected,
            "{}",
            String::from_utf8_lossy(key)
        );
    }
    drop(db);
    cleanup(&path);
    Ok(())
}

#[cfg(feature = "ttl")]
mod ttl {
    use super::*;
    use emdb::Ttl;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    #[test]
    fn test_expired_records_are_hidden_from_every_read_path() -> Result<()> {
        let db = Emdb::builder().enable_range_scans(true).build()?;
        db.insert_with_ttl("exp", "v", Ttl::After(Duration::from_millis(1)))?;
        db.insert("live", "v")?;
        let ns = db.namespace("n")?;
        ns.insert_with_ttl("exp", "v", Ttl::After(Duration::from_millis(1)))?;
        thread::sleep(Duration::from_millis(20));

        assert_eq!(db.get("exp")?, None);
        assert!(!db.contains_key("exp")?);
        assert!(db.contains_key("live")?);
        let keys: Vec<Vec<u8>> = db.keys()?.collect();
        assert_eq!(keys, vec![b"live".to_vec()]);
        let all: Vec<(Vec<u8>, Vec<u8>)> = db.iter()?.collect();
        assert_eq!(all.len(), 1);
        assert!(db.range_prefix("ex")?.is_empty());
        assert_eq!(db.range_prefix_iter("ex")?.count(), 0);
        assert_eq!(db.iter_from("e")?.count(), 1);
        assert!(ns.get_zerocopy("exp")?.is_none());
        assert!(!ns.contains_key("exp")?);
        assert_eq!(ns.iter()?.count(), 0);
        // Documented: expired records are counted until swept.
        assert_eq!(db.len()?, 2);
        assert_eq!(db.sweep_expired(), 1);
        assert_eq!(db.len()?, 1);
        Ok(())
    }

    #[cfg(feature = "nested")]
    #[test]
    fn test_group_skips_expired_records() -> Result<()> {
        let db = Emdb::open_in_memory();
        db.insert_with_ttl("g.exp", "v", Ttl::After(Duration::from_millis(1)))?;
        db.insert("g.live", "v")?;
        thread::sleep(Duration::from_millis(20));
        let keys: Vec<Vec<u8>> = db.group("g")?.map(|(k, _)| k).collect();
        assert_eq!(keys, vec![b"g.live".to_vec()]);
        Ok(())
    }

    #[test]
    fn test_persist_does_not_resurrect_an_expired_key() -> Result<()> {
        let db = Emdb::open_in_memory();
        db.insert_with_ttl("k", "v", Ttl::After(Duration::from_millis(1)))?;
        thread::sleep(Duration::from_millis(20));
        assert!(!db.persist("k")?);
        assert_eq!(db.get("k")?, None);

        db.insert_with_ttl("live", "v", Ttl::After(Duration::from_secs(60)))?;
        assert!(db.persist("live")?);
        assert_eq!(db.expires_at("live")?, Some(0));
        assert!(!db.persist("live")?, "no TTL left to remove");

        let ns = db.namespace("n")?;
        ns.insert_with_ttl("k", "v", Ttl::After(Duration::from_millis(1)))?;
        thread::sleep(Duration::from_millis(20));
        assert!(!ns.persist("k")?);
        assert_eq!(ns.get("k")?, None);
        Ok(())
    }

    #[test]
    fn test_sweep_never_deletes_a_fresh_concurrent_insert() -> Result<()> {
        for _ in 0..rounds(5) {
            let db = Arc::new(Emdb::open_in_memory());
            let n = 3_000_u32;
            for i in 0..n {
                db.insert_with_ttl(format!("k{i}"), "old", Ttl::After(Duration::from_millis(1)))?;
            }
            thread::sleep(Duration::from_millis(10));
            let barrier = Arc::new(Barrier::new(2));
            let sweeper = {
                let (db, barrier) = (Arc::clone(&db), Arc::clone(&barrier));
                thread::spawn(move || {
                    let _ = barrier.wait();
                    db.sweep_expired()
                })
            };
            let _ = barrier.wait();
            for i in 0..n {
                db.insert(format!("k{i}"), "fresh")?;
            }
            let _swept = join(sweeper);
            let lost = (0..n)
                .filter(|i| !matches!(db.get(format!("k{i}")), Ok(Some(_))))
                .count();
            assert_eq!(lost, 0, "sweep deleted {lost} fresh values");
            assert_eq!(db.len()?, n as usize);
        }
        Ok(())
    }

    #[test]
    fn test_concurrent_sweeps_count_each_record_once() -> Result<()> {
        let db = Arc::new(Emdb::open_in_memory());
        for i in 0_u32..2_000 {
            db.insert_with_ttl(format!("k{i}"), "v", Ttl::After(Duration::from_millis(1)))?;
        }
        thread::sleep(Duration::from_millis(10));
        let total = Arc::new(AtomicUsize::new(0));
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let (db, total) = (Arc::clone(&db), Arc::clone(&total));
                thread::spawn(move || {
                    let _ = total.fetch_add(db.sweep_expired(), Ordering::Relaxed);
                })
            })
            .collect();
        for handle in handles {
            join(handle);
        }
        assert_eq!(total.load(Ordering::Relaxed), 2_000);
        assert_eq!(db.len()?, 0);
        Ok(())
    }
}
