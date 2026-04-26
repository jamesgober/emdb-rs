// Copyright 2026 James Gober. Licensed under Apache-2.0.
//
// Apples-to-apples bench against redb's published methodology.
// Mirrors the workload shape of `redb-bench/benches/lmdb_benchmark.rs`
// (24-byte random keys, 150-byte random values, fastrand-seeded) so
// numbers are directly comparable to the table redb publishes in their
// README.
//
// Phases (matching redb's bench except where noted):
//   1. bulk load          — one big sequence of inserts + final fsync
//   2. individual writes  — N inserts, fsync per insert
//   3. batch writes       — N batches × M records each, fsync per batch
//   4. nosync writes      — inserts without fsync (OS buffer only)
//   5. len()              — record count probe
//   6. random reads       — 1M random point lookups (×2 iterations)
//   7. MT random reads    — 4 / 8 thread fan-out (skipped 16/32 for
//                           consumer hardware; redb publishes those on
//                           a 16-core box)
//   8. removals           — delete half the keys, fsync
//   9. uncompacted size   — file size after the above
//  10. compaction         — call compact(), measure time
//  11. compacted size     — file size after compaction
//
// Range-read phases are skipped: emdb's hash index does not support
// sorted iteration. That's a real feature gap, not a bench omission;
// the comparison table records it as N/A.
//
// Set `EMDB_BENCH_RECORDS=5000000` to run at redb's published scale.
// Defaults to 1M for faster local iteration.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use emdb::Emdb;
use fastrand::Rng;

const KEY_SIZE: usize = 24;
const VALUE_SIZE: usize = 150;
const RNG_SEED: u64 = 3;
const INDIVIDUAL_WRITES: usize = 1_000;
const BATCH_WRITES: usize = 100;
const BATCH_SIZE: usize = 1_000;
const NOSYNC_WRITES: usize = 50_000;
const NUM_READS: usize = 1_000_000;
const READ_ITERATIONS: usize = 2;
const READ_THREAD_COUNTS: &[usize] = &[4, 8];
const BULK_INSERT_CHUNK: usize = 50_000;

fn bulk_elements() -> usize {
    std::env::var("EMDB_BENCH_RECORDS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(1_000_000)
}

fn make_rng() -> Rng {
    let mut rng = Rng::new();
    rng.seed(RNG_SEED);
    rng
}

#[inline]
fn random_pair(rng: &mut Rng) -> ([u8; KEY_SIZE], Vec<u8>) {
    let mut key = [0_u8; KEY_SIZE];
    for byte in &mut key {
        *byte = rng.u8(..);
    }
    let mut value = vec![0_u8; VALUE_SIZE];
    for byte in &mut value {
        *byte = rng.u8(..);
    }
    (key, value)
}

fn make_rng_shards(shards: usize, elements: usize) -> Vec<Rng> {
    let mut rngs = Vec::with_capacity(shards);
    let elements_per_shard = elements / shards.max(1);
    for i in 0..shards {
        let mut rng = make_rng();
        for _ in 0..(i * elements_per_shard) {
            let _ = random_pair(&mut rng);
        }
        rngs.push(rng);
    }
    rngs
}

fn tmp_path(label: &str, ext: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |d| d.as_nanos());
    p.push(format!("emdb-lmdb-style-{label}-{nanos}.{ext}"));
    p
}

fn cleanup_emdb(path: &Path) {
    let _ = std::fs::remove_file(path);
    let display = path.display();
    let _ = std::fs::remove_file(format!("{display}.lock"));
    let _ = std::fs::remove_file(format!("{display}.compact.tmp"));
    let _ = std::fs::remove_file(format!("{display}.encbak"));
}

fn cleanup_dir(path: &Path) {
    let _ = std::fs::remove_dir_all(path);
}

#[derive(Debug, Clone)]
struct PhaseResult {
    name: String,
    duration: Option<Duration>,
    bytes: Option<u64>,
}

impl PhaseResult {
    fn duration(name: impl Into<String>, d: Duration) -> Self {
        Self {
            name: name.into(),
            duration: Some(d),
            bytes: None,
        }
    }
    fn bytes(name: impl Into<String>, b: u64) -> Self {
        Self {
            name: name.into(),
            duration: None,
            bytes: Some(b),
        }
    }
    fn na(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            duration: None,
            bytes: None,
        }
    }
    fn display(&self) -> String {
        if let Some(d) = self.duration {
            format!("{}ms", d.as_millis())
        } else if let Some(b) = self.bytes {
            format_bytes(b)
        } else {
            "N/A".to_string()
        }
    }
}

fn format_bytes(b: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    let bf = b as f64;
    if bf >= GB {
        format!("{:.2} GiB", bf / GB)
    } else if bf >= MB {
        format!("{:.2} MiB", bf / MB)
    } else {
        format!("{:.2} KiB", bf / KB)
    }
}

// ---- emdb bench ---------------------------------------------------

fn bench_emdb(elements: usize) -> Vec<PhaseResult> {
    let path = tmp_path("emdb", "db");
    cleanup_emdb(&path);
    let mut results = Vec::new();

    let db = Emdb::open(&path).expect("emdb open");
    let mut rng = make_rng();

    // Phase: bulk load (chunked insert_many to keep transient memory
    // bounded at ~10 MiB instead of ~1 GiB for 5M records).
    let start = Instant::now();
    {
        let mut chunk: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(BULK_INSERT_CHUNK);
        for _ in 0..elements {
            let (key, value) = random_pair(&mut rng);
            chunk.push((key.to_vec(), value));
            if chunk.len() == BULK_INSERT_CHUNK {
                db.insert_many(chunk.drain(..))
                    .expect("emdb insert_many");
            }
        }
        if !chunk.is_empty() {
            db.insert_many(chunk.drain(..))
                .expect("emdb insert_many tail");
        }
        db.flush().expect("emdb flush");
    }
    results.push(PhaseResult::duration("bulk load", start.elapsed()));

    // Phase: individual writes — fsync per record.
    let start = Instant::now();
    for _ in 0..INDIVIDUAL_WRITES {
        let (key, value) = random_pair(&mut rng);
        db.insert(key.as_slice(), value.as_slice())
            .expect("emdb insert");
        db.flush().expect("emdb per-insert flush");
    }
    results.push(PhaseResult::duration("individual writes", start.elapsed()));

    // Phase: batch writes — one transaction per batch, fsync after.
    let start = Instant::now();
    for _ in 0..BATCH_WRITES {
        let mut chunk: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(BATCH_SIZE);
        for _ in 0..BATCH_SIZE {
            let (key, value) = random_pair(&mut rng);
            chunk.push((key.to_vec(), value));
        }
        db.insert_many(chunk.into_iter()).expect("emdb batch insert");
        db.flush().expect("emdb batch flush");
    }
    results.push(PhaseResult::duration("batch writes", start.elapsed()));

    // Phase: nosync writes — inserts without flush.
    let start = Instant::now();
    for _ in 0..NOSYNC_WRITES {
        let (key, value) = random_pair(&mut rng);
        db.insert(key.as_slice(), value.as_slice())
            .expect("emdb insert nosync");
    }
    results.push(PhaseResult::duration("nosync writes", start.elapsed()));
    db.flush().expect("emdb post-nosync flush");

    let total_elements =
        elements + INDIVIDUAL_WRITES + BATCH_WRITES * BATCH_SIZE + NOSYNC_WRITES;

    // Phase: len()
    let start = Instant::now();
    let _len = db.len().expect("emdb len");
    results.push(PhaseResult::duration("len()", start.elapsed()));

    // Phase: random reads — 1M point lookups, ×READ_ITERATIONS.
    for _ in 0..READ_ITERATIONS {
        let mut rng = make_rng();
        let start = Instant::now();
        let mut hit = 0_u64;
        for _ in 0..NUM_READS {
            let (key, _value) = random_pair(&mut rng);
            if let Some(v) = db.get(key.as_slice()).expect("emdb get") {
                hit += v[0] as u64;
            }
        }
        // Touch `hit` so the optimiser can't elide the read loop.
        std::hint::black_box(hit);
        results.push(PhaseResult::duration("random reads", start.elapsed()));
    }

    // Range reads: emdb has no sorted iteration over a hash index.
    results.push(PhaseResult::na("random range reads"));
    results.push(PhaseResult::na("random range reads"));

    // Phase: MT random reads.
    let db_arc = Arc::new(db);
    for &threads in READ_THREAD_COUNTS {
        let mut rngs = make_rng_shards(threads, total_elements);
        let barrier = Arc::new(Barrier::new(threads));
        let start = Instant::now();
        let reads_per_thread = total_elements / threads;
        thread::scope(|s| {
            for _ in 0..threads {
                let barrier = Arc::clone(&barrier);
                let db = Arc::clone(&db_arc);
                let rng = rngs.pop().expect("rng shard");
                let _ = s.spawn(move || {
                    let mut rng = rng;
                    barrier.wait();
                    let mut hit = 0_u64;
                    for _ in 0..reads_per_thread {
                        let (key, _value) = random_pair(&mut rng);
                        if let Some(v) = db.get(key.as_slice()).expect("emdb get mt") {
                            hit += v[0] as u64;
                        }
                    }
                    std::hint::black_box(hit);
                });
            }
        });
        results.push(PhaseResult::duration(
            format!("random reads ({threads} threads)"),
            start.elapsed(),
        ));
    }
    let db = Arc::into_inner(db_arc).expect("unique db arc");

    // Phase: removals — remove half the bulk keys.
    let start = Instant::now();
    {
        let mut rng = make_rng();
        let to_delete = total_elements / 2;
        for _ in 0..to_delete {
            let (key, _value) = random_pair(&mut rng);
            let _ = db.remove(key.as_slice()).expect("emdb remove");
        }
        db.flush().expect("emdb post-remove flush");
    }
    results.push(PhaseResult::duration("removals", start.elapsed()));

    // Uncompacted size.
    let uncompacted = std::fs::metadata(&path)
        .map(|m| m.len())
        .unwrap_or_default();
    results.push(PhaseResult::bytes("uncompacted size", uncompacted));

    // Compaction.
    let start = Instant::now();
    db.compact().expect("emdb compact");
    db.flush().expect("emdb post-compact flush");
    results.push(PhaseResult::duration("compaction", start.elapsed()));

    let compacted = std::fs::metadata(&path)
        .map(|m| m.len())
        .unwrap_or_default();
    results.push(PhaseResult::bytes("compacted size", compacted));

    drop(db);
    cleanup_emdb(&path);
    results
}

// ---- redb bench ---------------------------------------------------

#[cfg(feature = "bench-compare")]
fn bench_redb(elements: usize) -> Vec<PhaseResult> {
    use redb::{Database, ReadableTableMetadata, TableDefinition};

    const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("kv");
    let path = tmp_path("redb", "db");
    let _ = std::fs::remove_file(&path);
    let mut results = Vec::new();

    let db = Database::create(&path).expect("redb create");
    let mut rng = make_rng();

    // Bulk load — one transaction.
    let start = Instant::now();
    {
        let txn = db.begin_write().expect("redb begin_write bulk");
        {
            let mut table = txn.open_table(TABLE).expect("redb open_table bulk");
            for _ in 0..elements {
                let (key, value) = random_pair(&mut rng);
                let _ = table
                    .insert(key.as_slice(), value.as_slice())
                    .expect("redb insert bulk");
            }
        }
        txn.commit().expect("redb commit bulk");
    }
    results.push(PhaseResult::duration("bulk load", start.elapsed()));

    // Individual writes — one transaction per insert.
    let start = Instant::now();
    for _ in 0..INDIVIDUAL_WRITES {
        let txn = db.begin_write().expect("redb begin_write indiv");
        {
            let mut table = txn.open_table(TABLE).expect("redb table indiv");
            let (key, value) = random_pair(&mut rng);
            let _ = table
                .insert(key.as_slice(), value.as_slice())
                .expect("redb insert indiv");
        }
        txn.commit().expect("redb commit indiv");
    }
    results.push(PhaseResult::duration("individual writes", start.elapsed()));

    // Batch writes — one transaction per batch.
    let start = Instant::now();
    for _ in 0..BATCH_WRITES {
        let txn = db.begin_write().expect("redb begin_write batch");
        {
            let mut table = txn.open_table(TABLE).expect("redb table batch");
            for _ in 0..BATCH_SIZE {
                let (key, value) = random_pair(&mut rng);
                let _ = table
                    .insert(key.as_slice(), value.as_slice())
                    .expect("redb insert batch");
            }
        }
        txn.commit().expect("redb commit batch");
    }
    results.push(PhaseResult::duration("batch writes", start.elapsed()));

    // Nosync writes — redb's `Durability::Eventual` skips fsync.
    let start = Instant::now();
    {
        let mut txn = db.begin_write().expect("redb begin_write nosync");
        txn.set_durability(redb::Durability::Eventual);
        {
            let mut table = txn.open_table(TABLE).expect("redb table nosync");
            for _ in 0..NOSYNC_WRITES {
                let (key, value) = random_pair(&mut rng);
                let _ = table
                    .insert(key.as_slice(), value.as_slice())
                    .expect("redb insert nosync");
            }
        }
        txn.commit().expect("redb commit nosync");
    }
    results.push(PhaseResult::duration("nosync writes", start.elapsed()));

    let total_elements =
        elements + INDIVIDUAL_WRITES + BATCH_WRITES * BATCH_SIZE + NOSYNC_WRITES;

    // len()
    let start = Instant::now();
    let _len = {
        let txn = db.begin_read().expect("redb begin_read len");
        let table = txn.open_table(TABLE).expect("redb table len");
        table.len().expect("redb len")
    };
    results.push(PhaseResult::duration("len()", start.elapsed()));

    // Random reads.
    for _ in 0..READ_ITERATIONS {
        let mut rng = make_rng();
        let start = Instant::now();
        let mut hit = 0_u64;
        {
            let txn = db.begin_read().expect("redb begin_read");
            let table = txn.open_table(TABLE).expect("redb table read");
            for _ in 0..NUM_READS {
                let (key, _value) = random_pair(&mut rng);
                if let Some(v) = table.get(key.as_slice()).expect("redb get") {
                    hit += v.value()[0] as u64;
                }
            }
        }
        std::hint::black_box(hit);
        results.push(PhaseResult::duration("random reads", start.elapsed()));
    }

    // Range reads — 500K range starts × 10 elements each.
    for _ in 0..READ_ITERATIONS {
        let mut rng = make_rng();
        let start = Instant::now();
        let mut sum = 0_u64;
        {
            let txn = db.begin_read().expect("redb begin_read range");
            let table = txn.open_table(TABLE).expect("redb table range");
            for _ in 0..500_000 {
                let (key, _value) = random_pair(&mut rng);
                let mut iter = table.range::<&[u8]>(key.as_slice()..).expect("redb range");
                for _ in 0..10 {
                    if let Some(Ok((_k, v))) = iter.next() {
                        sum += v.value()[0] as u64;
                    } else {
                        break;
                    }
                }
            }
        }
        std::hint::black_box(sum);
        results.push(PhaseResult::duration("random range reads", start.elapsed()));
    }

    // MT reads — share Arc<Database>.
    let db = Arc::new(db);
    for &threads in READ_THREAD_COUNTS {
        let mut rngs = make_rng_shards(threads, total_elements);
        let barrier = Arc::new(Barrier::new(threads));
        let start = Instant::now();
        let reads_per_thread = total_elements / threads;
        thread::scope(|s| {
            for _ in 0..threads {
                let barrier = Arc::clone(&barrier);
                let db = Arc::clone(&db);
                let rng = rngs.pop().expect("rng shard mt");
                let _ = s.spawn(move || {
                    let mut rng = rng;
                    barrier.wait();
                    let txn = db.begin_read().expect("redb mt begin_read");
                    let table = txn.open_table(TABLE).expect("redb mt table");
                    let mut hit = 0_u64;
                    for _ in 0..reads_per_thread {
                        let (key, _value) = random_pair(&mut rng);
                        if let Some(v) = table.get(key.as_slice()).expect("redb mt get") {
                            hit += v.value()[0] as u64;
                        }
                    }
                    std::hint::black_box(hit);
                });
            }
        });
        results.push(PhaseResult::duration(
            format!("random reads ({threads} threads)"),
            start.elapsed(),
        ));
    }
    let db = Arc::into_inner(db).expect("unique redb arc");

    // Removals.
    let start = Instant::now();
    {
        let mut rng = make_rng();
        let to_delete = total_elements / 2;
        let txn = db.begin_write().expect("redb begin_write remove");
        {
            let mut table = txn.open_table(TABLE).expect("redb table remove");
            for _ in 0..to_delete {
                let (key, _value) = random_pair(&mut rng);
                let _ = table.remove(key.as_slice()).expect("redb remove");
            }
        }
        txn.commit().expect("redb commit remove");
    }
    results.push(PhaseResult::duration("removals", start.elapsed()));

    // Uncompacted size.
    let uncompacted = std::fs::metadata(&path)
        .map(|m| m.len())
        .unwrap_or_default();
    results.push(PhaseResult::bytes("uncompacted size", uncompacted));

    // Compaction.
    let mut db = db;
    let start = Instant::now();
    let compacted_supported = db.compact().expect("redb compact");
    if compacted_supported {
        results.push(PhaseResult::duration("compaction", start.elapsed()));
        let compacted = std::fs::metadata(&path)
            .map(|m| m.len())
            .unwrap_or_default();
        results.push(PhaseResult::bytes("compacted size", compacted));
    } else {
        results.push(PhaseResult::na("compaction"));
        results.push(PhaseResult::na("compacted size"));
    }

    drop(db);
    let _ = std::fs::remove_file(&path);
    results
}

// ---- sled bench ---------------------------------------------------

#[cfg(feature = "bench-compare")]
fn bench_sled(elements: usize) -> Vec<PhaseResult> {
    let path = tmp_path("sled", "dir");
    cleanup_dir(&path);
    let mut results = Vec::new();

    let db = sled::Config::new()
        .path(&path)
        .open()
        .expect("sled open");
    let mut rng = make_rng();

    // Bulk load — one batch flush at the end.
    let start = Instant::now();
    {
        for _ in 0..elements {
            let (key, value) = random_pair(&mut rng);
            let _ = db
                .insert(key.as_slice(), value.as_slice())
                .expect("sled insert bulk");
        }
        db.flush().expect("sled flush bulk");
    }
    results.push(PhaseResult::duration("bulk load", start.elapsed()));

    // Individual writes — flush per insert.
    let start = Instant::now();
    for _ in 0..INDIVIDUAL_WRITES {
        let (key, value) = random_pair(&mut rng);
        let _ = db
            .insert(key.as_slice(), value.as_slice())
            .expect("sled insert indiv");
        db.flush().expect("sled flush indiv");
    }
    results.push(PhaseResult::duration("individual writes", start.elapsed()));

    // Batch writes — one apply per batch (sled batch -> apply -> flush).
    let start = Instant::now();
    for _ in 0..BATCH_WRITES {
        let mut batch = sled::Batch::default();
        for _ in 0..BATCH_SIZE {
            let (key, value) = random_pair(&mut rng);
            batch.insert(key.as_slice(), value.as_slice());
        }
        db.apply_batch(batch).expect("sled apply_batch");
        db.flush().expect("sled flush batch");
    }
    results.push(PhaseResult::duration("batch writes", start.elapsed()));

    // Nosync writes — no flush.
    let start = Instant::now();
    for _ in 0..NOSYNC_WRITES {
        let (key, value) = random_pair(&mut rng);
        let _ = db
            .insert(key.as_slice(), value.as_slice())
            .expect("sled insert nosync");
    }
    results.push(PhaseResult::duration("nosync writes", start.elapsed()));
    db.flush().expect("sled post-nosync flush");

    let total_elements =
        elements + INDIVIDUAL_WRITES + BATCH_WRITES * BATCH_SIZE + NOSYNC_WRITES;

    // len() — sled's len walks every key.
    let start = Instant::now();
    let _len = db.len();
    results.push(PhaseResult::duration("len()", start.elapsed()));

    // Random reads.
    for _ in 0..READ_ITERATIONS {
        let mut rng = make_rng();
        let start = Instant::now();
        let mut hit = 0_u64;
        for _ in 0..NUM_READS {
            let (key, _value) = random_pair(&mut rng);
            if let Some(v) = db.get(key.as_slice()).expect("sled get") {
                hit += v[0] as u64;
            }
        }
        std::hint::black_box(hit);
        results.push(PhaseResult::duration("random reads", start.elapsed()));
    }

    // Range reads.
    for _ in 0..READ_ITERATIONS {
        let mut rng = make_rng();
        let start = Instant::now();
        let mut sum = 0_u64;
        for _ in 0..500_000 {
            let (key, _value) = random_pair(&mut rng);
            let iter = db.range(key.as_slice()..);
            for (i, item) in iter.enumerate() {
                if i >= 10 {
                    break;
                }
                if let Ok((_k, v)) = item {
                    sum += v[0] as u64;
                }
            }
        }
        std::hint::black_box(sum);
        results.push(PhaseResult::duration("random range reads", start.elapsed()));
    }

    // MT reads.
    let db_arc = Arc::new(db);
    for &threads in READ_THREAD_COUNTS {
        let mut rngs = make_rng_shards(threads, total_elements);
        let barrier = Arc::new(Barrier::new(threads));
        let start = Instant::now();
        let reads_per_thread = total_elements / threads;
        thread::scope(|s| {
            for _ in 0..threads {
                let barrier = Arc::clone(&barrier);
                let db = Arc::clone(&db_arc);
                let rng = rngs.pop().expect("rng shard sled mt");
                let _ = s.spawn(move || {
                    let mut rng = rng;
                    barrier.wait();
                    let mut hit = 0_u64;
                    for _ in 0..reads_per_thread {
                        let (key, _value) = random_pair(&mut rng);
                        if let Some(v) = db.get(key.as_slice()).expect("sled mt get") {
                            hit += v[0] as u64;
                        }
                    }
                    std::hint::black_box(hit);
                });
            }
        });
        results.push(PhaseResult::duration(
            format!("random reads ({threads} threads)"),
            start.elapsed(),
        ));
    }
    let db = Arc::into_inner(db_arc).expect("unique sled arc");

    // Removals.
    let start = Instant::now();
    {
        let mut rng = make_rng();
        let to_delete = total_elements / 2;
        for _ in 0..to_delete {
            let (key, _value) = random_pair(&mut rng);
            let _ = db.remove(key.as_slice()).expect("sled remove");
        }
        db.flush().expect("sled post-remove flush");
    }
    results.push(PhaseResult::duration("removals", start.elapsed()));

    // Uncompacted size — directory total.
    let uncompacted = directory_size(&path);
    results.push(PhaseResult::bytes("uncompacted size", uncompacted));

    // sled has no in-process compactor; report N/A.
    results.push(PhaseResult::na("compaction"));
    results.push(PhaseResult::na("compacted size"));

    drop(db);
    cleanup_dir(&path);
    results
}

#[cfg(feature = "bench-compare")]
fn directory_size(path: &Path) -> u64 {
    fn walk(p: &Path, acc: &mut u64) {
        if let Ok(meta) = std::fs::metadata(p) {
            if meta.is_file() {
                *acc += meta.len();
                return;
            }
        }
        if let Ok(entries) = std::fs::read_dir(p) {
            for entry in entries.flatten() {
                walk(&entry.path(), acc);
            }
        }
    }
    let mut total = 0_u64;
    walk(path, &mut total);
    total
}

// ---- top-level ----------------------------------------------------

fn main() {
    let elements = bulk_elements();
    println!("emdb lmdb-style bench: {elements} bulk records, {KEY_SIZE}B keys, {VALUE_SIZE}B values\n");

    println!("running emdb...");
    let emdb_results = bench_emdb(elements);

    #[cfg(feature = "bench-compare")]
    let redb_results = {
        println!("running redb...");
        bench_redb(elements)
    };
    #[cfg(not(feature = "bench-compare"))]
    let redb_results: Vec<PhaseResult> = Vec::new();

    #[cfg(feature = "bench-compare")]
    let sled_results = {
        println!("running sled...");
        bench_sled(elements)
    };
    #[cfg(not(feature = "bench-compare"))]
    let sled_results: Vec<PhaseResult> = Vec::new();

    print_table(&emdb_results, &redb_results, &sled_results);
}

fn print_table(emdb: &[PhaseResult], redb: &[PhaseResult], sled: &[PhaseResult]) {
    println!();
    let columns = !redb.is_empty() && !sled.is_empty();
    if columns {
        println!(
            "| {:<28} | {:>14} | {:>14} | {:>14} |",
            "phase", "emdb", "redb", "sled"
        );
        println!(
            "|{:-<30}|{:->16}|{:->16}|{:->16}|",
            "", "", "", ""
        );
    } else {
        println!("| {:<28} | {:>14} |", "phase", "emdb");
        println!("|{:-<30}|{:->16}|", "", "");
    }
    for (i, row) in emdb.iter().enumerate() {
        let redb_cell = redb.get(i).map(PhaseResult::display).unwrap_or_default();
        let sled_cell = sled.get(i).map(PhaseResult::display).unwrap_or_default();
        if columns {
            println!(
                "| {:<28} | {:>14} | {:>14} | {:>14} |",
                row.name,
                row.display(),
                redb_cell,
                sled_cell
            );
        } else {
            println!("| {:<28} | {:>14} |", row.name, row.display());
        }
    }
    println!();
}
