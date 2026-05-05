// Copyright 2026 James Gober. Licensed under Apache-2.0.
//
// Group-commit benchmark: compare per-record-durability throughput
// under `FlushPolicy::OnEachFlush` (one fsync per flush call) vs
// `FlushPolicy::Group` (concurrent flushers fuse their fsyncs).
//
// Workload shape: N threads, each inserting M records with
// `db.flush()` after every record. This is the "per-record
// durability under contention" pattern that the group-commit
// pipeline was designed for. Numbers are aggregate writes/sec
// across all threads; higher is better.
//
// Run with:
//
// ```powershell
// cargo bench --bench group_commit --features ttl
// ```
//
// Override the workload via env vars:
//
// - `EMDB_BENCH_GC_THREADS` — thread count (default 8).
// - `EMDB_BENCH_GC_PER_THREAD` — records per thread (default 200).
// - `EMDB_BENCH_GC_MAX_WAIT_US` — Group policy `max_wait` in
//   microseconds (default 500).
// - `EMDB_BENCH_GC_MAX_BATCH` — Group policy `max_batch` (default
//   32).

use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use emdb::{Emdb, FlushPolicy};

fn read_env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

fn read_env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

fn tmp_path(label: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |d| d.as_nanos());
    p.push(format!("emdb-gc-bench-{label}-{nanos}.emdb"));
    p
}

fn cleanup(path: &Path) {
    let _ = std::fs::remove_file(path);
    let display = path.display();
    let _ = std::fs::remove_file(format!("{display}.lock"));
}

fn run_workload(db: Arc<Emdb>, threads: usize, per_thread: usize) -> Duration {
    let barrier = Arc::new(Barrier::new(threads + 1));
    let mut handles = Vec::with_capacity(threads);
    for t in 0..threads {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            for i in 0..per_thread {
                let key = format!("t{t}-i{i:06}");
                let value = format!("payload-{t}-{i}");
                db.insert(key.as_str(), value.as_str())
                    .expect("insert in workload");
                db.flush().expect("flush in workload");
            }
        }));
    }

    barrier.wait();
    let started = Instant::now();
    for h in handles {
        h.join().expect("worker thread");
    }
    started.elapsed()
}

fn main() {
    let threads = read_env_usize("EMDB_BENCH_GC_THREADS", 8);
    let per_thread = read_env_usize("EMDB_BENCH_GC_PER_THREAD", 200);
    let max_wait_us = read_env_u64("EMDB_BENCH_GC_MAX_WAIT_US", 500);
    // Default max_batch to match the thread count so the leader does
    // not wait for followers that can never arrive. Setting
    // `max_batch` higher than the number of concurrent flushers
    // turns the leader's `max_wait` into pure tail latency.
    let max_batch = read_env_usize("EMDB_BENCH_GC_MAX_BATCH", threads);

    let total_writes = (threads * per_thread) as u64;

    println!(
        "emdb group-commit bench: {threads} threads × {per_thread} writes/thread = {total_writes} total"
    );
    println!("Group policy: max_wait = {max_wait_us}µs, max_batch = {max_batch}\n");

    // Run each policy on a fresh DB so they don't share state. Same
    // workload shape on both for a clean apples-to-apples comparison.
    let _ = (max_wait_us, max_batch); // tuning knobs are advisory in v0.9
    let policies: &[(&str, FlushPolicy)] = &[
        ("OnEachFlush", FlushPolicy::OnEachFlush),
        ("Group", FlushPolicy::Group),
    ];

    println!(
        "| {:<14} | {:>14} | {:>16} | {:>10} |",
        "policy", "wall time (ms)", "writes/sec", "speedup"
    );
    println!("|{:-<16}|{:->16}|{:->18}|{:->12}|", "", "", "", "");

    let mut baseline_secs: Option<f64> = None;
    for (label, policy) in policies {
        let path = tmp_path(label);
        cleanup(&path);
        let db = Arc::new(
            Emdb::builder()
                .path(path.clone())
                .flush_policy(*policy)
                .build()
                .expect("emdb open"),
        );

        let elapsed = run_workload(Arc::clone(&db), threads, per_thread);
        let secs = elapsed.as_secs_f64();
        let writes_per_sec = total_writes as f64 / secs;
        let speedup = match baseline_secs {
            Some(base) => format!("{:.2}×", base / secs),
            None => {
                baseline_secs = Some(secs);
                "1.00×".to_string()
            }
        };

        println!(
            "| {:<14} | {:>14} | {:>16} | {:>10} |",
            label,
            format!("{}", elapsed.as_millis()),
            format!("{writes_per_sec:.0}"),
            speedup
        );

        // Drop and clean up before the next variant runs so the next
        // policy starts from an empty file (no extra capacity, no
        // pre-existing records).
        let db = Arc::into_inner(db).expect("db arc unique");
        drop(db);
        cleanup(&path);
    }
}
