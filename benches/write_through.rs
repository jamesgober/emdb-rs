// Copyright 2026 James Gober. Licensed under Apache-2.0.
//
// Compare per-record durability throughput under
// `FlushPolicy::OnEachFlush` and `FlushPolicy::WriteThrough` on
// the same workload — single-thread, one record per flush call.
//
// The shape mirrors the `individual writes` phase of
// `benches/lmdb_style.rs`: the OS pays for a synchronous data
// commit on every iteration. Under `OnEachFlush` that cost lands
// on `flush()` (one `FlushFileBuffers` per call on Windows; one
// `fdatasync` on Unix). Under `WriteThrough` the cost shifts into
// `pwrite` itself — the OS commits each record synchronously as
// it is written — and `flush()` becomes a near-free
// belt-and-braces sync.
//
// Run with:
//
// ```powershell
// cargo bench --bench write_through --features ttl
// ```
//
// Override the workload via env vars:
//
// - `EMDB_BENCH_WT_WRITES` — number of write+flush iterations
//   (default 1 000, matching `lmdb_style`).
// - `EMDB_BENCH_WT_VALUE_BYTES` — value size per record (default
//   150, matching `lmdb_style`).

use std::path::{Path, PathBuf};
use std::time::Instant;

use emdb::{Emdb, FlushPolicy};

fn read_env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

fn tmp_path(label: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |d| d.as_nanos());
    p.push(format!("emdb-wt-bench-{label}-{nanos}.emdb"));
    p
}

fn cleanup(path: &Path) {
    let _ = std::fs::remove_file(path);
    let display = path.display();
    let _ = std::fs::remove_file(format!("{display}.lock"));
    let _ = std::fs::remove_file(format!("{display}.lock-meta"));
}

fn run_workload(db: &Emdb, writes: usize, value_bytes: usize) -> std::time::Duration {
    let value = vec![b'x'; value_bytes];
    let started = Instant::now();
    for i in 0..writes {
        let key = format!("k{i:09}");
        db.insert(key.as_str(), value.as_slice())
            .expect("insert in workload");
        db.flush().expect("flush in workload");
    }
    started.elapsed()
}

fn main() {
    let writes = read_env_usize("EMDB_BENCH_WT_WRITES", 1_000);
    let value_bytes = read_env_usize("EMDB_BENCH_WT_VALUE_BYTES", 150);

    println!(
        "emdb write-through bench: {writes} writes, {value_bytes}-byte values, single thread\n"
    );

    let policies: &[(&str, FlushPolicy)] = &[
        ("OnEachFlush", FlushPolicy::OnEachFlush),
        ("WriteThrough", FlushPolicy::WriteThrough),
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
        let db = Emdb::builder()
            .path(path.clone())
            .flush_policy(*policy)
            .build()
            .expect("emdb open");

        let elapsed = run_workload(&db, writes, value_bytes);
        let secs = elapsed.as_secs_f64();
        let writes_per_sec = writes as f64 / secs;
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

        drop(db);
        cleanup(&path);
    }
}
