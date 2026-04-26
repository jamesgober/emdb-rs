// Copyright 2026 James Gober. Licensed under Apache-2.0.
//
// Multi-reader bench. Single-thread `compare_read` undersells the
// `Arc<Mmap>` read path because there is no way to observe the
// lock-free fast path without contention. This bench spawns N reader
// threads against a pre-populated DB and measures aggregate
// throughput. The `compare_read` arm in `comparative.rs` tells the
// single-thread story; this one shows what the engine does under
// real read fan-out.

use std::sync::Arc;
use std::thread;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use emdb::Emdb;

const RECORDS: usize = 20_000;
const VALUE_BYTES: usize = 64;
const READS_PER_THREAD: usize = 5_000;

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
    p.push(format!("emdb-concurrent-{label}-{nanos}.emdb"));
    p
}

fn cleanup(path: &std::path::Path) {
    let _ = std::fs::remove_file(path);
    let display = path.display();
    let _ = std::fs::remove_file(format!("{display}.lock"));
}

/// Launch `threads` reader threads, each issuing `READS_PER_THREAD`
/// `db.get` calls against a pre-populated database. Returns when every
/// thread has finished. The aggregate read count is what criterion
/// uses to compute throughput.
fn run_concurrent_reads(db: &Arc<Emdb>, data: &Arc<Vec<(Vec<u8>, Vec<u8>)>>, threads: usize) {
    let mut handles = Vec::with_capacity(threads);
    for tid in 0..threads {
        let db = Arc::clone(db);
        let data = Arc::clone(data);
        handles.push(thread::spawn(move || {
            // Stagger the per-thread starting index so threads don't
            // hammer the same shard at the same time.
            let stride = data.len() / threads.max(1);
            let start = tid.wrapping_mul(stride) % data.len().max(1);
            for i in 0..READS_PER_THREAD {
                let idx = (start + i) % data.len();
                let (key, expected) = &data[idx];
                let got = db.get(key.as_slice()).expect("get should succeed");
                debug_assert_eq!(got.as_deref(), Some(expected.as_slice()));
            }
        }));
    }
    for handle in handles {
        let _ = handle.join();
    }
}

fn bench_concurrent_reads(c: &mut Criterion) {
    let data: Arc<Vec<(Vec<u8>, Vec<u8>)>> = Arc::new(dataset(RECORDS));

    // Pre-populate one DB; reuse it across thread-count sweeps.
    let path = tmp_path("reads");
    let db = Emdb::open(&path).expect("emdb open should succeed");
    db.insert_many(data.iter().map(|(k, v)| (k.as_slice(), v.as_slice())))
        .expect("emdb insert_many should succeed");
    db.flush().expect("emdb flush should succeed");
    let db = Arc::new(db);

    let mut group = c.benchmark_group("concurrent_reads");
    for threads in [1_usize, 2, 4, 8] {
        let total_reads = (threads * READS_PER_THREAD) as u64;
        group.throughput(Throughput::Elements(total_reads));
        group.bench_function(BenchmarkId::new("emdb", threads), |b| {
            b.iter(|| run_concurrent_reads(&db, &data, threads));
        });
    }
    group.finish();

    drop(db);
    cleanup(&path);
}

criterion_group!(
    name = concurrent_reads;
    config = Criterion::default().sample_size(10);
    targets = bench_concurrent_reads
);
criterion_main!(concurrent_reads);
