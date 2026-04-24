use criterion::{criterion_group, criterion_main, Criterion};
use emdb::{Emdb, FlushPolicy};

// Baseline reference from local validation machine (2026-04-24):
// - OnEachWrite: slowest, strongest durability.
// - EveryN(64): balanced default for throughput and durability.
// - Manual: highest throughput, caller-managed durability.

fn tmp_path(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |d| d.as_nanos());
    p.push(format!("emdb-bench-{name}-{nanos}.emdb"));
    p
}

fn bench_flush_policy(c: &mut Criterion, name: &str, policy: FlushPolicy) {
    c.bench_function(name, |b| {
        b.iter(|| {
            let path = tmp_path(name);
            let built = Emdb::builder()
                .path(path.clone())
                .flush_policy(policy)
                .build();
            assert!(built.is_ok());
            let db = match built {
                Ok(db) => db,
                Err(err) => panic!("build should succeed: {err}"),
            };

            for i in 0_u32..256 {
                let inserted = db.insert(format!("k{i}"), format!("v{i}"));
                assert!(inserted.is_ok());
            }
            let flushed = db.flush();
            assert!(flushed.is_ok());
            drop(db);

            assert!(std::fs::remove_file(path).is_ok());
        })
    });
}

fn persistence_benches(c: &mut Criterion) {
    bench_flush_policy(c, "persist_insert_on_each_write", FlushPolicy::OnEachWrite);
    bench_flush_policy(c, "persist_insert_every_n_64", FlushPolicy::EveryN(64));
    bench_flush_policy(c, "persist_insert_manual", FlushPolicy::Manual);
}

criterion_group!(persistence, persistence_benches);
criterion_main!(persistence);
