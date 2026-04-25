use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use emdb::{Emdb, FlushPolicy};
use std::collections::BTreeMap;
use std::path::PathBuf;

const DEFAULT_RECORDS: usize = 20_000;
const VALUE_BYTES: usize = 64;

fn bench_records() -> usize {
    std::env::var("EMDB_BENCH_RECORDS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_RECORDS)
}

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

fn tmp_path(prefix: &str, ext: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |d| d.as_nanos());
    p.push(format!("emdb-compare-{prefix}-{nanos}.{ext}"));
    p
}

fn cleanup_emdb(path: &std::path::Path) {
    let _ = std::fs::remove_file(path);
    let display = path.display();
    let _ = std::fs::remove_file(format!("{display}.wal"));
    let _ = std::fs::remove_file(format!("{display}.v4.wal"));
    let _ = std::fs::remove_file(format!("{display}.bak"));
    let _ = std::fs::remove_file(format!("{display}.lock"));
    let _ = std::fs::remove_file(format!("{display}.v3bak"));
    let _ = std::fs::remove_file(format!("{display}.v4tmp"));
}

fn bench_emdb(c: &mut Criterion, data: &[(Vec<u8>, Vec<u8>)]) {
    let mut group = c.benchmark_group("compare_insert");
    group.throughput(Throughput::Elements(data.len() as u64));
    group.bench_function(BenchmarkId::new("emdb_v06", data.len()), |b| {
        b.iter(|| {
            let path = tmp_path("emdb", "db");
            let db = Emdb::builder()
                .path(path.clone())
                .flush_policy(FlushPolicy::Manual)
                .build()
                .expect("emdb open should succeed");

            for (key, value) in data {
                db.insert(key.as_slice(), value.as_slice())
                    .expect("emdb insert should succeed");
            }
            db.flush().expect("emdb flush should succeed");
            drop(db);

            cleanup_emdb(&path);
        })
    });
    group.bench_function(BenchmarkId::new("emdb_v07", data.len()), |b| {
        b.iter(|| {
            let path = tmp_path("emdb-v4", "db");
            let db = Emdb::builder()
                .path(path.clone())
                .prefer_v4(true)
                .flush_policy(FlushPolicy::Manual)
                .build()
                .expect("emdb v4 open should succeed");

            for (key, value) in data {
                db.insert(key.as_slice(), value.as_slice())
                    .expect("emdb v4 insert should succeed");
            }
            db.flush().expect("emdb v4 flush should succeed");
            drop(db);

            cleanup_emdb(&path);
        })
    });
    group.finish();

    let mut read_group = c.benchmark_group("compare_read");
    read_group.throughput(Throughput::Elements(data.len() as u64));
    read_group.bench_function(BenchmarkId::new("emdb_v06", data.len()), |b| {
        let path = tmp_path("emdb-read", "db");
        let db = Emdb::builder()
            .path(path.clone())
            .flush_policy(FlushPolicy::Manual)
            .build()
            .expect("emdb open should succeed");

        for (key, value) in data {
            db.insert(key.as_slice(), value.as_slice())
                .expect("emdb insert should succeed");
        }
        db.flush().expect("emdb flush should succeed");

        b.iter(|| {
            for (key, expected) in data {
                let got = db.get(key).expect("emdb get should succeed");
                assert_eq!(got.as_deref(), Some(expected.as_slice()));
            }
        });

        drop(db);
        cleanup_emdb(&path);
    });
    read_group.bench_function(BenchmarkId::new("emdb_v07", data.len()), |b| {
        let path = tmp_path("emdb-v4-read", "db");
        let db = Emdb::builder()
            .path(path.clone())
            .prefer_v4(true)
            .flush_policy(FlushPolicy::Manual)
            .build()
            .expect("emdb v4 open should succeed");

        for (key, value) in data {
            db.insert(key.as_slice(), value.as_slice())
                .expect("emdb v4 insert should succeed");
        }
        db.flush().expect("emdb v4 flush should succeed");

        b.iter(|| {
            for (key, expected) in data {
                let got = db.get(key).expect("emdb v4 get should succeed");
                assert_eq!(got.as_deref(), Some(expected.as_slice()));
            }
        });

        drop(db);
        cleanup_emdb(&path);
    });
    read_group.finish();
}

#[cfg(feature = "bench-compare")]
fn bench_sled(c: &mut Criterion, data: &[(Vec<u8>, Vec<u8>)]) {
    let mut group = c.benchmark_group("compare_insert");
    group.throughput(Throughput::Elements(data.len() as u64));
    group.bench_function(BenchmarkId::new("sled", data.len()), |b| {
        b.iter(|| {
            let path = tmp_path("sled", "dir");
            let db = sled::Config::new()
                .path(path.clone())
                .temporary(true)
                .open()
                .expect("sled open should succeed");

            for (key, value) in data {
                db.insert(key, value.as_slice())
                    .expect("sled insert should succeed");
            }
            db.flush().expect("sled flush should succeed");
        })
    });
    group.finish();

    let mut read_group = c.benchmark_group("compare_read");
    read_group.throughput(Throughput::Elements(data.len() as u64));
    read_group.bench_function(BenchmarkId::new("sled", data.len()), |b| {
        let path = tmp_path("sled-read", "dir");
        let db = sled::Config::new()
            .path(path)
            .temporary(true)
            .open()
            .expect("sled open should succeed");

        for (key, value) in data {
            db.insert(key, value.as_slice())
                .expect("sled insert should succeed");
        }
        db.flush().expect("sled flush should succeed");

        b.iter(|| {
            for (key, expected) in data {
                let got = db.get(key).expect("sled get should succeed");
                assert_eq!(got.as_deref(), Some(expected.as_slice()));
            }
        });
    });
    read_group.finish();
}

#[cfg(feature = "bench-compare")]
fn bench_redb(c: &mut Criterion, data: &[(Vec<u8>, Vec<u8>)]) {
    const TABLE: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("kv");

    let mut group = c.benchmark_group("compare_insert");
    group.throughput(Throughput::Elements(data.len() as u64));
    group.bench_function(BenchmarkId::new("redb", data.len()), |b| {
        b.iter(|| {
            let path = tmp_path("redb", "db");
            let db = redb::Database::create(path).expect("redb open should succeed");
            let write_txn = db.begin_write().expect("redb begin write");
            {
                let mut table = write_txn.open_table(TABLE).expect("redb table open");
                for (key, value) in data {
                    table
                        .insert(key.as_slice(), value.as_slice())
                        .expect("redb insert should succeed");
                }
            }
            write_txn.commit().expect("redb commit should succeed");
        })
    });
    group.finish();

    let mut read_group = c.benchmark_group("compare_read");
    read_group.throughput(Throughput::Elements(data.len() as u64));
    read_group.bench_function(BenchmarkId::new("redb", data.len()), |b| {
        let path = tmp_path("redb-read", "db");
        let db = redb::Database::create(path).expect("redb open should succeed");
        {
            let write_txn = db.begin_write().expect("redb begin write");
            {
                let mut table = write_txn.open_table(TABLE).expect("redb table open");
                for (key, value) in data {
                    table
                        .insert(key.as_slice(), value.as_slice())
                        .expect("redb insert should succeed");
                }
            }
            write_txn.commit().expect("redb commit should succeed");
        }

        b.iter(|| {
            let read_txn = db.begin_read().expect("redb begin read");
            let table = read_txn.open_table(TABLE).expect("redb table open");
            for (key, expected) in data {
                let got = table
                    .get(key.as_slice())
                    .expect("redb get should succeed")
                    .expect("redb value should exist");
                assert_eq!(got.value(), expected.as_slice());
            }
        });
    });
    read_group.finish();
}

#[cfg(feature = "bench-rocksdb")]
fn bench_rocksdb(c: &mut Criterion, data: &[(Vec<u8>, Vec<u8>)]) {
    use rocksdb::{Options, DB};

    let mut group = c.benchmark_group("compare_insert");
    group.throughput(Throughput::Elements(data.len() as u64));
    group.bench_function(BenchmarkId::new("rocksdb", data.len()), |b| {
        b.iter(|| {
            let path = tmp_path("rocksdb", "dir");
            let mut options = Options::default();
            options.create_if_missing(true);
            options.set_compression_type(rocksdb::DBCompressionType::None);

            let db = DB::open(&options, &path).expect("rocksdb open should succeed");
            for (key, value) in data {
                db.put(key.as_slice(), value.as_slice())
                    .expect("rocksdb put should succeed");
            }
            db.flush().expect("rocksdb flush should succeed");
            drop(db);
            let _ = std::fs::remove_dir_all(&path);
        })
    });
    group.finish();

    let mut read_group = c.benchmark_group("compare_read");
    read_group.throughput(Throughput::Elements(data.len() as u64));
    read_group.bench_function(BenchmarkId::new("rocksdb", data.len()), |b| {
        let path = tmp_path("rocksdb-read", "dir");
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_compression_type(rocksdb::DBCompressionType::None);

        let db = DB::open(&options, &path).expect("rocksdb open should succeed");
        for (key, value) in data {
            db.put(key.as_slice(), value.as_slice())
                .expect("rocksdb put should succeed");
        }
        db.flush().expect("rocksdb flush should succeed");

        b.iter(|| {
            for (key, expected) in data {
                let got = db
                    .get(key.as_slice())
                    .expect("rocksdb get should succeed")
                    .expect("rocksdb value should exist");
                assert_eq!(got.as_slice(), expected.as_slice());
            }
        });

        drop(db);
        let _ = std::fs::remove_dir_all(&path);
    });
    read_group.finish();
}

#[cfg(feature = "bench-redis")]
fn bench_redis(c: &mut Criterion, data: &[(Vec<u8>, Vec<u8>)]) {
    use redis::Commands;

    let url =
        std::env::var("EMDB_REDIS_URL").unwrap_or_else(|_| String::from("redis://127.0.0.1/"));
    let client = match redis::Client::open(url.as_str()) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("skipping redis benchmarks: could not create client: {err}");
            return;
        }
    };

    let mut conn = match client.get_connection() {
        Ok(conn) => conn,
        Err(err) => {
            eprintln!("skipping redis benchmarks: could not connect: {err}");
            return;
        }
    };

    let mut group = c.benchmark_group("compare_insert");
    group.throughput(Throughput::Elements(data.len() as u64));
    group.bench_function(BenchmarkId::new("redis", data.len()), |b| {
        b.iter(|| {
            let _: () = redis::cmd("FLUSHDB")
                .query(&mut conn)
                .expect("redis flushdb should succeed");
            for (key, value) in data {
                let _: () = conn
                    .set(key.as_slice(), value.as_slice())
                    .expect("redis set should succeed");
            }
        })
    });
    group.finish();

    let mut read_group = c.benchmark_group("compare_read");
    read_group.throughput(Throughput::Elements(data.len() as u64));
    read_group.bench_function(BenchmarkId::new("redis", data.len()), |b| {
        let _: () = redis::cmd("FLUSHDB")
            .query(&mut conn)
            .expect("redis flushdb should succeed");
        for (key, value) in data {
            let _: () = conn
                .set(key.as_slice(), value.as_slice())
                .expect("redis set should succeed");
        }

        b.iter(|| {
            for (key, expected) in data {
                let got: Vec<u8> = conn.get(key.as_slice()).expect("redis get should succeed");
                assert_eq!(got.as_slice(), expected.as_slice());
            }
        });
    });
    read_group.finish();
}

fn comparative_benches(c: &mut Criterion) {
    let records = bench_records();
    let data = dataset(records);

    bench_emdb(c, &data);

    #[cfg(feature = "bench-compare")]
    {
        bench_sled(c, &data);
        bench_redb(c, &data);
    }

    #[cfg(feature = "bench-rocksdb")]
    {
        bench_rocksdb(c, &data);
    }

    #[cfg(feature = "bench-redis")]
    {
        bench_redis(c, &data);
    }

    let mut totals = BTreeMap::new();
    totals.insert("records", records);
    for (name, count) in totals {
        println!("{name}: {count}");
    }
}

criterion_group!(
    name = comparative;
    config = Criterion::default().sample_size(10);
    targets = comparative_benches
);
criterion_main!(comparative);
