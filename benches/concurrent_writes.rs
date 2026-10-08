// Copyright 2026 James Gober. Licensed under Apache-2.0.
//
// Multi-writer bench. N threads insert fresh keys into one database
// whose journal is already mapped for reads (a `get` runs first), the
// shape that collapsed on Windows in 1.0.2 (out-of-order file
// extension under concurrent appends; see docs/PLATFORM-NOTES.md).
// Also covers the per-key stripe and shard-writer locking of 1.0.3.

use std::sync::{Arc, Barrier};
use std::thread;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use emdb::Emdb;

const INSERTS_PER_THREAD: usize = 5_000;
const VALUE_BYTES: usize = 32;

fn run_writers(threads: usize) {
    let db = Arc::new(Emdb::open_in_memory());
    db.insert("seed", "v").expect("seed insert");
    let _ = db.get("seed").expect("seed get maps the journal");
    let barrier = Arc::new(Barrier::new(threads));
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let (db, barrier) = (Arc::clone(&db), Arc::clone(&barrier));
            thread::spawn(move || {
                let value = vec![b'y'; VALUE_BYTES];
                let _ = barrier.wait();
                for i in 0..INSERTS_PER_THREAD {
                    db.insert(format!("t{t}-{i:08}"), value.as_slice())
                        .expect("insert should succeed");
                }
            })
        })
        .collect();
    for handle in handles {
        let _ = handle.join();
    }
}

fn bench_concurrent_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_writes/fresh_keys_mapped");
    for threads in [1_usize, 2, 4, 8] {
        group.throughput(Throughput::Elements((threads * INSERTS_PER_THREAD) as u64));
        group.bench_function(BenchmarkId::from_parameter(threads), |b| {
            b.iter(|| run_writers(threads));
        });
    }
    group.finish();
}

criterion_group!(
    name = concurrent_writes;
    config = Criterion::default().sample_size(10);
    targets = bench_concurrent_writes
);
criterion_main!(concurrent_writes);
