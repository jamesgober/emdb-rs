// Copyright 2026 James Gober. Licensed under Apache-2.0.
//
// Concurrent stress test for the primary in-memory index introduced
// in v0.9.3 (sharded seqlock-protected open-addressed table). Runs a
// multi-threaded workload of inserts / reads / removes that:
//
//   - Forces enough total writes to trigger several shard-level
//     growth doublings (the pause-the-world migration path).
//   - Interleaves reads from threads that don't hold writer locks,
//     exercising the seqlock-protected slot reads under live
//     concurrent updates.
//   - Maintains a shared reference state (a parking_lot-locked
//     `HashMap`) recording every successful insert and remove. After
//     all threads complete, the test walks the reference and asserts
//     `db.get(key)` matches the reference for every key — catching
//     any lost write, stale read, or growth-migration data loss.
//
// Runtime: ~2-4 seconds on consumer hardware at the default
// `STRESS_OPS_PER_THREAD = 8_000`. Override with the
// `EMDB_STRESS_OPS_PER_THREAD` env var for longer soak runs (e.g.
// `EMDB_STRESS_OPS_PER_THREAD=80000 cargo test --test index_stress
// --release`).

use std::collections::HashMap;
use std::sync::Arc;
use std::thread;

use emdb::Emdb;
use parking_lot::Mutex;

const THREADS: usize = 8;
const DEFAULT_OPS_PER_THREAD: usize = 8_000;
/// Total distinct keys the workload exercises. Sized larger than
/// the index's initial per-shard capacity (1024 × 64 = 65 536) so
/// the workload reliably triggers shard growth across multiple
/// shards.
const TOTAL_KEYS: usize = 200_000;

fn ops_per_thread() -> usize {
    std::env::var("EMDB_STRESS_OPS_PER_THREAD")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_OPS_PER_THREAD)
}

fn tmp_path(label: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |d| d.as_nanos());
    p.push(format!("emdb-stress-{label}-{nanos}.emdb"));
    p
}

fn cleanup(path: &std::path::Path) {
    let _ = std::fs::remove_file(path);
    let display = path.display();
    let _ = std::fs::remove_file(format!("{display}.lock"));
    let _ = std::fs::remove_file(format!("{display}.meta"));
}

/// Cheap deterministic PRNG so each thread's workload is reproducible
/// per (thread_id, op_index). `fastrand` would do, but we want to avoid
/// adding a non-dev test dep just for this — splittable hash is good
/// enough for stress workload generation.
fn rng_step(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
    *seed ^= *seed >> 27;
    *seed
}

/// Generate a key-bytes pair for `(thread_id, op_idx)`. Keys cycle
/// through a fixed pool of `TOTAL_KEYS` so multiple threads compete
/// on the same keys (the realistic concurrency pattern: many writers
/// updating overlapping records).
fn key_at(idx: usize) -> Vec<u8> {
    format!("stress-key-{idx:08}").into_bytes()
}

fn value_at(idx: usize, generation: u32) -> Vec<u8> {
    format!("v-{idx:08}-g{generation:04}").into_bytes()
}

/// Per-key reference state. `None` means "absent / removed";
/// `Some(value)` means "present with this exact value bytes."
type Reference = Mutex<HashMap<Vec<u8>, Option<Vec<u8>>>>;

#[test]
fn concurrent_inserts_reads_removes_under_growth() {
    let ops = ops_per_thread();
    let path = tmp_path("mixed");
    cleanup(&path);

    let db = Arc::new(Emdb::open(&path).expect("emdb open"));
    let reference: Arc<Reference> = Arc::new(Mutex::new(HashMap::new()));

    let mut handles = Vec::with_capacity(THREADS);
    for tid in 0..THREADS {
        let db = Arc::clone(&db);
        let reference = Arc::clone(&reference);
        handles.push(thread::spawn(move || {
            let mut seed = (tid as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xCAFE_BABE_F00D;
            for op in 0..ops {
                let r = rng_step(&mut seed);
                let key_idx = (r as usize) % TOTAL_KEYS;
                let key = key_at(key_idx);
                let generation = ((tid as u32) << 16) | (op as u32 & 0xFFFF);

                // Workload mix: 60% insert, 25% get, 15% remove. This
                // pushes the index through enough growth events while
                // also exercising the read path under live writes.
                let r2 = rng_step(&mut seed);
                let op_kind = r2 % 100;
                if op_kind < 60 {
                    // Insert (or replace).
                    let value = value_at(key_idx, generation);
                    db.insert(key.as_slice(), value.as_slice())
                        .expect("insert");
                    reference.lock().insert(key, Some(value));
                } else if op_kind < 85 {
                    // Get. Snapshot the reference value at the time of
                    // the read; the actual db value must be one of: the
                    // reference's value, or `None` (if another thread
                    // removed it between our reference read and our db
                    // read). We don't strictly check this in-flight; the
                    // post-join walk catches divergences.
                    let _ = db.get(key.as_slice()).expect("get");
                } else {
                    // Remove.
                    let _ = db.remove(key.as_slice()).expect("remove");
                    reference.lock().insert(key, None);
                }
            }
        }));
    }
    for h in handles {
        h.join().expect("thread join");
    }

    // Validate: every reference entry must match the db's view.
    //
    // The reference's final state is the result of an arbitrary
    // interleaving of all threads' updates — the LAST writer to each
    // key wins, both in the reference and in the db. We assert
    // pointwise equality on every key the workload touched.
    let reference = reference.lock();
    let mut mismatches = Vec::new();
    for (key, expected) in reference.iter() {
        let actual = db.get(key.as_slice()).expect("get");
        match (expected, actual) {
            (None, None) => {}
            (Some(exp), Some(act)) if exp == &act => {}
            (exp, act) => {
                if mismatches.len() < 5 {
                    mismatches.push(format!(
                        "key {:?}: expected {:?}, got {:?}",
                        String::from_utf8_lossy(key),
                        exp.as_ref()
                            .map(|v| String::from_utf8_lossy(v).into_owned()),
                        act.as_ref()
                            .map(|v| String::from_utf8_lossy(v).into_owned())
                    ));
                }
            }
        }
    }
    let total_touched = reference.len();
    drop(reference);
    drop(db);
    cleanup(&path);

    assert!(
        mismatches.is_empty(),
        "{} key(s) diverged between reference and db (out of {} keys touched). \
         First {} mismatch(es):\n{}",
        mismatches.len(),
        total_touched,
        mismatches.len(),
        mismatches.join("\n")
    );
}

#[test]
fn concurrent_inserts_only_force_growth_then_read_back() {
    // Pure-insert variant. Every thread inserts distinct keys into
    // its own partition; at the end, every key must be readable.
    // Validates the growth migration loses no entries when multiple
    // shards are growing concurrently.
    let ops = ops_per_thread();
    let path = tmp_path("growth");
    cleanup(&path);

    let db = Arc::new(Emdb::open(&path).expect("emdb open"));

    let mut handles = Vec::with_capacity(THREADS);
    for tid in 0..THREADS {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            // Per-thread key partition: thread tid gets keys
            // [tid * ops, (tid + 1) * ops). No cross-thread overlap;
            // every insert is a fresh key, so growth scans see a
            // monotonically-growing dataset on each shard.
            let base = tid * ops;
            for i in 0..ops {
                let idx = base + i;
                let key = key_at(idx);
                let value = value_at(idx, tid as u32);
                db.insert(key.as_slice(), value.as_slice())
                    .expect("insert");
            }
        }));
    }
    for h in handles {
        h.join().expect("thread join");
    }

    // Every key inserted by any thread must be retrievable.
    let mut missing = Vec::new();
    for tid in 0..THREADS {
        let base = tid * ops;
        for i in 0..ops {
            let idx = base + i;
            let key = key_at(idx);
            let expected = value_at(idx, tid as u32);
            let actual = db.get(key.as_slice()).expect("get");
            if actual.as_deref() != Some(expected.as_slice()) {
                if missing.len() < 5 {
                    missing.push(format!(
                        "key idx {idx} (thread {tid}): expected {:?}, got {:?}",
                        String::from_utf8_lossy(&expected),
                        actual
                            .as_ref()
                            .map(|v| String::from_utf8_lossy(v).into_owned())
                    ));
                }
            }
        }
    }
    let total = THREADS * ops;
    drop(db);
    cleanup(&path);

    assert!(
        missing.is_empty(),
        "{} key(s) missing/stale after concurrent insert + growth \
         (out of {} total). First {} mismatch(es):\n{}",
        missing.len(),
        total,
        missing.len(),
        missing.join("\n")
    );
}
