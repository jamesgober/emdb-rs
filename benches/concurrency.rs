use std::sync::Arc;
use std::thread;

use criterion::{criterion_group, criterion_main, Criterion};
use emdb::Emdb;

fn kv_dataset(count: u32) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..count)
        .map(|i| {
            let key = format!("k{i}").into_bytes();
            let value = format!("v{i}").into_bytes();
            (key, value)
        })
        .collect()
}

fn kv_dataset_with_prefix(
    prefix: &str,
    value_from_index: bool,
    count: u32,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..count)
        .map(|i| {
            let key = format!("{prefix}{i}").into_bytes();
            let value = if value_from_index {
                i.to_string().into_bytes()
            } else {
                format!("v{i}").into_bytes()
            };
            (key, value)
        })
        .collect()
}

fn key_cycle(prefix: &str, span: u32, count: u32) -> Vec<Vec<u8>> {
    (0..count)
        .map(|i| format!("{prefix}{}", i % span).into_bytes())
        .collect()
}

fn bench_read_only(c: &mut Criterion) {
    let preload = kv_dataset(10_000);
    let read_keys = Arc::new(key_cycle("k", 10_000, 20_000));

    for readers in [1_usize, 4, 8, 16] {
        c.bench_function(&format!("concurrency_read_only_{readers}r"), |b| {
            b.iter(|| {
                let db = Arc::new(Emdb::open_in_memory());
                for (key, value) in &preload {
                    let inserted = db.insert(key.as_slice(), value.as_slice());
                    assert!(inserted.is_ok());
                }

                let mut handles = Vec::with_capacity(readers);
                for _ in 0..readers {
                    let db = Arc::clone(&db);
                    let read_keys = Arc::clone(&read_keys);
                    handles.push(thread::spawn(move || {
                        for key in read_keys.iter() {
                            let fetched = db.get(key.as_slice());
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
    let preload = kv_dataset(5_000);
    let writer_data = Arc::new(kv_dataset_with_prefix("w", true, 2_500));
    let read_keys = Arc::new(key_cycle("k", 5_000, 10_000));

    c.bench_function("concurrency_mixed_8r_1w", |b| {
        b.iter(|| {
            let db = Arc::new(Emdb::open_in_memory());
            for (key, value) in &preload {
                let inserted = db.insert(key.as_slice(), value.as_slice());
                assert!(inserted.is_ok());
            }

            let writer_db = Arc::clone(&db);
            let writer_data = Arc::clone(&writer_data);
            let writer = thread::spawn(move || {
                for (key, value) in writer_data.iter() {
                    let inserted = writer_db.insert(key.as_slice(), value.as_slice());
                    assert!(inserted.is_ok());
                }
            });

            let mut readers = Vec::new();
            for _ in 0_u32..8 {
                let reader_db = Arc::clone(&db);
                let read_keys = Arc::clone(&read_keys);
                readers.push(thread::spawn(move || {
                    for key in read_keys.iter() {
                        let fetched = reader_db.get(key.as_slice());
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
