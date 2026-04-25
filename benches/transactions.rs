use criterion::{criterion_group, criterion_main, Criterion};
use emdb::Emdb;

fn tmp_path(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |d| d.as_nanos());
    p.push(format!("emdb-bench-{name}-{nanos}.emdb"));
    p
}

fn kv_dataset(count: u32) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..count)
        .map(|i| {
            let key = format!("k{i}").into_bytes();
            let value = format!("v{i}").into_bytes();
            (key, value)
        })
        .collect()
}

fn bench_outside_transaction(c: &mut Criterion) {
    c.bench_function("single_insert_outside_transaction", |b| {
        b.iter(|| {
            let path = tmp_path("outside");
            let built = Emdb::open(&path);
            assert!(built.is_ok());
            let db = match built {
                Ok(db) => db,
                Err(err) => panic!("open should succeed: {err}"),
            };

            let inserted = db.insert("k", "v");
            assert!(inserted.is_ok());
            let flushed = db.flush();
            assert!(flushed.is_ok());

            assert!(std::fs::remove_file(path).is_ok());
        })
    });
}

fn bench_inside_transaction(c: &mut Criterion) {
    c.bench_function("single_insert_inside_transaction", |b| {
        b.iter(|| {
            let path = tmp_path("inside");
            let opened = Emdb::open(&path);
            assert!(opened.is_ok());
            let db = match opened {
                Ok(db) => db,
                Err(err) => panic!("open should succeed: {err}"),
            };

            let tx_result = db.transaction(|tx| {
                tx.insert("k", "v")?;
                Ok(())
            });
            assert!(tx_result.is_ok());

            let flushed = db.flush();
            assert!(flushed.is_ok());
            assert!(std::fs::remove_file(path).is_ok());
        })
    });
}

fn bench_batch_sizes(c: &mut Criterion) {
    for size in [1_u32, 10, 100, 1000] {
        let data = kv_dataset(size);
        c.bench_function(&format!("batch_commit_size_{size}"), |b| {
            b.iter(|| {
                let path = tmp_path(&format!("batch-{size}"));
                let opened = Emdb::open(&path);
                assert!(opened.is_ok());
                let db = match opened {
                    Ok(db) => db,
                    Err(err) => panic!("open should succeed: {err}"),
                };

                let tx_result = db.transaction(|tx| {
                    for (key, value) in &data {
                        tx.insert(key.as_slice(), value.as_slice())?;
                    }
                    Ok(())
                });
                assert!(tx_result.is_ok());

                let flushed = db.flush();
                assert!(flushed.is_ok());
                assert!(std::fs::remove_file(path).is_ok());
            })
        });
    }
}

fn transaction_benches(c: &mut Criterion) {
    bench_outside_transaction(c);
    bench_inside_transaction(c);
    bench_batch_sizes(c);
}

criterion_group!(transactions, transaction_benches);
criterion_main!(transactions);
