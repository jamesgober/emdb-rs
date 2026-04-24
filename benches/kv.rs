use criterion::{black_box, criterion_group, criterion_main, Criterion};
use emdb::Emdb;

fn bench_insert(c: &mut Criterion) {
    c.bench_function("kv_insert", |b| {
        b.iter(|| {
            let mut db = Emdb::open_in_memory();
            for i in 0_u32..1_000 {
                let key = format!("k{i}");
                let value = format!("v{i}");
                let result = db.insert(key.as_bytes(), value.as_bytes());
                assert!(result.is_ok());
            }
            black_box(db.len())
        })
    });
}

fn bench_get(c: &mut Criterion) {
    let mut db = Emdb::open_in_memory();
    for i in 0_u32..1_000 {
        let key = format!("k{i}");
        let value = format!("v{i}");
        let result = db.insert(key.as_bytes(), value.as_bytes());
        assert!(result.is_ok());
    }

    c.bench_function("kv_get", |b| {
        b.iter(|| {
            for i in 0_u32..1_000 {
                let key = format!("k{i}");
                let result = db.get(key.as_bytes());
                assert!(result.is_ok());
                let _ignored = black_box(result);
            }
        })
    });
}

fn bench_remove(c: &mut Criterion) {
    c.bench_function("kv_remove", |b| {
        b.iter(|| {
            let mut db = Emdb::open_in_memory();
            for i in 0_u32..1_000 {
                let key = format!("k{i}");
                let value = format!("v{i}");
                let result = db.insert(key.as_bytes(), value.as_bytes());
                assert!(result.is_ok());
            }

            for i in 0_u32..1_000 {
                let key = format!("k{i}");
                let removed = db.remove(key.as_bytes());
                assert!(removed.is_ok());
                let _ignored = black_box(removed);
            }

            black_box(db.len())
        })
    });
}

criterion_group!(kv_benches, bench_insert, bench_get, bench_remove);
criterion_main!(kv_benches);
