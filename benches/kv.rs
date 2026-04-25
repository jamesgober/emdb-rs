use criterion::{black_box, criterion_group, criterion_main, Criterion};
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

fn bench_insert(c: &mut Criterion) {
    let data = kv_dataset(1_000);
    c.bench_function("kv_insert", |b| {
        b.iter(|| {
            let db = Emdb::open_in_memory();
            for (key, value) in &data {
                let result = db.insert(key.as_slice(), value.as_slice());
                assert!(result.is_ok());
            }
            black_box(db.len())
        })
    });
}

fn bench_get(c: &mut Criterion) {
    let data = kv_dataset(1_000);
    let db = Emdb::open_in_memory();
    for (key, value) in &data {
        let result = db.insert(key.as_slice(), value.as_slice());
        assert!(result.is_ok());
    }

    c.bench_function("kv_get", |b| {
        b.iter(|| {
            for (key, _value) in &data {
                let result = db.get(key.as_slice());
                assert!(result.is_ok());
                let _ignored = black_box(result);
            }
        })
    });
}

fn bench_remove(c: &mut Criterion) {
    let data = kv_dataset(1_000);
    c.bench_function("kv_remove", |b| {
        b.iter(|| {
            let db = Emdb::open_in_memory();
            for (key, value) in &data {
                let result = db.insert(key.as_slice(), value.as_slice());
                assert!(result.is_ok());
            }

            for (key, _value) in &data {
                let removed = db.remove(key.as_slice());
                assert!(removed.is_ok());
                let _ignored = black_box(removed);
            }

            black_box(db.len())
        })
    });
}

criterion_group!(kv_benches, bench_insert, bench_get, bench_remove);
criterion_main!(kv_benches);
