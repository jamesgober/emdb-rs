// Copyright 2026 James Gober. Licensed under Apache-2.0.
//
// Index hot-path microbenchmark. Exercises emdb's primary in-memory
// index (the sharded open-addressed seqlock table introduced in
// 0.9.3) by routing through the public `Emdb` API on a pre-populated
// in-memory database. The end-to-end `get` path is hash → shard
// probe → mmap slice → decode, of which the index is one component;
// the multi-threaded variants put the index on the critical
// concurrency path so its contention characteristics dominate.
//
// What this benchmark exists to measure:
//
//   - Single-thread `get`/`replace`/`mixed` ops/sec — establishes the
//     uncontended fast-path baseline for the seqlock read (3 atomic
//     loads + acquire fence) vs the prior `RwLock<HashMap>` design.
//   - Multi-thread `concurrent_get` and `concurrent_mixed` — measures
//     scaling under thread fan-out, where the prior RwLock-per-shard
//     queued under high write contention but the new design has no
//     shard-wide writer lock.
//
// All benchmarks pre-populate the DB and run in a tmpfs-style
// scratch path (system temp dir) with no per-op fsync so the
// measured time is dominated by in-process work (the index +
// mmap-decode), not durability.

use std::sync::Arc;
use std::thread;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use emdb::Emdb;

/// Pre-populated record count. Sized so the dataset spreads across
/// all 64 shards (~1.5 K records per shard at this scale) with a
/// load factor low enough that growth never fires during the
/// measured workload.
const RECORDS: usize = 100_000;
/// Value size per record. Small on purpose — the goal is to make
/// the index the dominant cost, not the value copy.
const VALUE_BYTES: usize = 32;
/// Ops issued per iteration on the single-threaded variants.
const SINGLE_OPS: usize = 10_000;
/// Ops issued per thread on the multi-threaded variants.
const MULTI_OPS_PER_THREAD: usize = 5_000;

fn dataset(records: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..records)
        .map(|i| {
            let key = format!("key-{i:08}").into_bytes();
            let mut value = vec![b'x'; VALUE_BYTES];
            let suffix = format!("-{i:08}");
            for (dst, src) in value.iter_mut().zip(suffix.as_bytes().iter().copied()) {
                *dst = src;
            }
            (key, value)
        })
        .collect()
}

fn tmp_path(label: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |d| d.as_nanos());
    p.push(format!("emdb-index-hotpath-{label}-{nanos}.emdb"));
    p
}

fn cleanup(path: &std::path::Path) {
    let _ = std::fs::remove_file(path);
    let display = path.display();
    let _ = std::fs::remove_file(format!("{display}.lock"));
    let _ = std::fs::remove_file(format!("{display}.meta"));
}

/// Open and pre-populate a fresh emdb at `path`. Records are bulk-
/// loaded via `insert_many` for speed; the index is populated to
/// `RECORDS` distinct entries before the benchmark starts.
fn fresh_populated_db(path: &std::path::Path) -> Arc<Emdb> {
    let _ = std::fs::remove_file(path);
    let db = Emdb::open(path).expect("emdb open");
    let data = dataset(RECORDS);
    db.insert_many(data.iter().map(|(k, v)| (k.as_slice(), v.as_slice())))
        .expect("insert_many");
    db.flush().expect("flush");
    Arc::new(db)
}

// ---- single-threaded ---------------------------------------------

fn bench_single_get(c: &mut Criterion) {
    let path = tmp_path("get");
    let db = fresh_populated_db(&path);
    let data = dataset(RECORDS);

    let mut group = c.benchmark_group("index_hotpath/single");
    group.throughput(Throughput::Elements(SINGLE_OPS as u64));
    group.bench_function("get", |b| {
        let mut cursor = 0_usize;
        b.iter(|| {
            for _ in 0..SINGLE_OPS {
                let (key, _) = &data[cursor];
                let _ = db.get(key.as_slice()).expect("get");
                cursor = (cursor + 1) % data.len();
            }
        });
    });
    group.finish();

    drop(db);
    cleanup(&path);
}

fn bench_single_replace(c: &mut Criterion) {
    let path = tmp_path("replace");
    let db = fresh_populated_db(&path);
    let data = dataset(RECORDS);

    let mut group = c.benchmark_group("index_hotpath/single");
    group.throughput(Throughput::Elements(SINGLE_OPS as u64));
    group.bench_function("replace", |b| {
        let mut cursor = 0_usize;
        b.iter(|| {
            for _ in 0..SINGLE_OPS {
                let (key, value) = &data[cursor];
                db.insert(key.as_slice(), value.as_slice()).expect("insert");
                cursor = (cursor + 1) % data.len();
            }
        });
    });
    group.finish();

    drop(db);
    cleanup(&path);
}

fn bench_single_mixed(c: &mut Criterion) {
    let path = tmp_path("mixed");
    let db = fresh_populated_db(&path);
    let data = dataset(RECORDS);

    let mut group = c.benchmark_group("index_hotpath/single");
    group.throughput(Throughput::Elements(SINGLE_OPS as u64));
    // 80% reads, 20% writes — typical KV workload shape.
    group.bench_function("mixed_80r_20w", |b| {
        let mut cursor = 0_usize;
        b.iter(|| {
            for i in 0..SINGLE_OPS {
                let (key, value) = &data[cursor];
                if i % 5 == 0 {
                    db.insert(key.as_slice(), value.as_slice()).expect("insert");
                } else {
                    let _ = db.get(key.as_slice()).expect("get");
                }
                cursor = (cursor + 1) % data.len();
            }
        });
    });
    group.finish();

    drop(db);
    cleanup(&path);
}

// ---- multi-threaded ----------------------------------------------

fn run_concurrent_get(db: &Arc<Emdb>, data: &Arc<Vec<(Vec<u8>, Vec<u8>)>>, threads: usize) {
    let mut handles = Vec::with_capacity(threads);
    for tid in 0..threads {
        let db = Arc::clone(db);
        let data = Arc::clone(data);
        handles.push(thread::spawn(move || {
            let stride = data.len() / threads.max(1);
            let start = tid.wrapping_mul(stride) % data.len().max(1);
            for i in 0..MULTI_OPS_PER_THREAD {
                let idx = (start + i) % data.len();
                let (key, _) = &data[idx];
                let _ = db.get(key.as_slice()).expect("get");
            }
        }));
    }
    for handle in handles {
        let _ = handle.join();
    }
}

fn run_concurrent_mixed(db: &Arc<Emdb>, data: &Arc<Vec<(Vec<u8>, Vec<u8>)>>, threads: usize) {
    let mut handles = Vec::with_capacity(threads);
    for tid in 0..threads {
        let db = Arc::clone(db);
        let data = Arc::clone(data);
        handles.push(thread::spawn(move || {
            let stride = data.len() / threads.max(1);
            let start = tid.wrapping_mul(stride) % data.len().max(1);
            for i in 0..MULTI_OPS_PER_THREAD {
                let idx = (start + i) % data.len();
                let (key, value) = &data[idx];
                if i % 5 == 0 {
                    let _ = db.insert(key.as_slice(), value.as_slice());
                } else {
                    let _ = db.get(key.as_slice()).expect("get");
                }
            }
        }));
    }
    for handle in handles {
        let _ = handle.join();
    }
}

fn bench_concurrent_get(c: &mut Criterion) {
    let path = tmp_path("concurrent-get");
    let db = fresh_populated_db(&path);
    let data = Arc::new(dataset(RECORDS));

    let mut group = c.benchmark_group("index_hotpath/concurrent_get");
    for threads in [1_usize, 2, 4, 8] {
        let total = (threads * MULTI_OPS_PER_THREAD) as u64;
        group.throughput(Throughput::Elements(total));
        group.bench_function(BenchmarkId::from_parameter(threads), |b| {
            b.iter(|| run_concurrent_get(&db, &data, threads));
        });
    }
    group.finish();

    drop(db);
    cleanup(&path);
}

fn bench_concurrent_mixed(c: &mut Criterion) {
    let path = tmp_path("concurrent-mixed");
    let db = fresh_populated_db(&path);
    let data = Arc::new(dataset(RECORDS));

    let mut group = c.benchmark_group("index_hotpath/concurrent_mixed_80r_20w");
    for threads in [1_usize, 2, 4, 8] {
        let total = (threads * MULTI_OPS_PER_THREAD) as u64;
        group.throughput(Throughput::Elements(total));
        group.bench_function(BenchmarkId::from_parameter(threads), |b| {
            b.iter(|| run_concurrent_mixed(&db, &data, threads));
        });
    }
    group.finish();

    drop(db);
    cleanup(&path);
}

criterion_group!(
    benches,
    bench_single_get,
    bench_single_replace,
    bench_single_mixed,
    bench_concurrent_get,
    bench_concurrent_mixed,
);
criterion_main!(benches);
