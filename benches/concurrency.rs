use std::sync::Arc;
use std::thread;

use criterion::{criterion_group, criterion_main, Criterion};
use emdb::Emdb;

fn bench_read_only(c: &mut Criterion) {
    for readers in [1_usize, 4, 8, 16] {
        c.bench_function(&format!("concurrency_read_only_{readers}r"), |b| {
            b.iter(|| {
                let db = Arc::new(Emdb::open_in_memory());
                for i in 0_u32..10_000 {
                    let inserted = db.insert(format!("k{i}"), format!("v{i}"));
                    assert!(inserted.is_ok());
                }

                let mut handles = Vec::with_capacity(readers);
                for _ in 0..readers {
                    let db = Arc::clone(&db);
                    handles.push(thread::spawn(move || {
                        for i in 0_u32..20_000 {
                            let fetched = db.get(format!("k{}", i % 10_000));
                            assert!(fetched.is_ok());
                        }
                    }));
                }

                for handle in handles {
                    let joined = handle.join();
                    assert!(joined.is_ok());
                }
            })
        });
    }
}

fn bench_mixed_read_write(c: &mut Criterion) {
    c.bench_function("concurrency_mixed_8r_1w", |b| {
        b.iter(|| {
            let db = Arc::new(Emdb::open_in_memory());
            for i in 0_u32..5_000 {
                let inserted = db.insert(format!("k{i}"), format!("v{i}"));
                assert!(inserted.is_ok());
            }

            let writer_db = Arc::clone(&db);
            let writer = thread::spawn(move || {
                for i in 0_u32..2_500 {
                    let inserted = writer_db.insert(format!("w{i}"), format!("{i}"));
                    assert!(inserted.is_ok());
                }
            });

            let mut readers = Vec::new();
            for _ in 0_u32..8 {
                let reader_db = Arc::clone(&db);
                readers.push(thread::spawn(move || {
                    for i in 0_u32..10_000 {
                        let fetched = reader_db.get(format!("k{}", i % 5_000));
                        assert!(fetched.is_ok());
                    }
                }));
            }

            let writer_join = writer.join();
            assert!(writer_join.is_ok());

            for reader in readers {
                let reader_join = reader.join();
                assert!(reader_join.is_ok());
            }
        })
    });
}

fn concurrency_benches(c: &mut Criterion) {
    bench_read_only(c);
    bench_mixed_read_write(c);
}

criterion_group!(concurrency, concurrency_benches);
criterion_main!(concurrency);
