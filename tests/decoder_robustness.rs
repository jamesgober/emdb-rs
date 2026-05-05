// Randomized decoder-robustness tests added in v0.8.
//
// Goal: drive the recovery scan with crafted-and-random byte
// sequences to exercise the framing parser's failure modes. A
// real fuzz harness (cargo-fuzz) is queued for v1.0; this is the
// stable-toolchain-friendly proxy. We don't have access to the
// internal `format::try_decode_record` from a tests/ file (it's
// `pub(crate)`), but [`Emdb::open`] runs the same decoder during
// its recovery scan, so we drive the parser end-to-end through
// the public API.
//
// Invariants under test:
//
//   - Random bytes after the header never cause a panic or an
//     out-of-bounds read.
//   - Random bytes never cause an infinite loop in the recovery
//     scan (we enforce a 5 s wall-clock ceiling per scenario).
//   - Empty data region (just the header) opens to zero records.
//   - A valid prefix followed by garbage exposes exactly the
//     valid prefix.
//   - Records of varying key/value sizes round-trip cleanly.

use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use emdb::{Emdb, Result};
use fastrand::Rng;

fn tmp_path(label: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |d| d.as_nanos());
    let tid = std::thread::current().id();
    p.push(format!("emdb-decoder-{label}-{nanos}-{tid:?}.emdb"));
    p
}

fn cleanup(path: &PathBuf) {
    let _ = std::fs::remove_file(path);
    let display = path.display();
    let _ = std::fs::remove_file(format!("{display}.lock"));
    let _ = std::fs::remove_file(format!("{display}.compact.tmp"));
}

#[test]
fn empty_data_region_opens_with_zero_records() -> Result<()> {
    let path = tmp_path("empty-data");
    cleanup(&path);

    // Create the file via Emdb so the header is valid.
    drop(Emdb::open(&path)?);

    let db = Emdb::open(&path)?;
    assert_eq!(db.len()?, 0);
    assert!(db.is_empty()?);

    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn random_bytes_after_header_never_panic() {
    // Seed a deterministic RNG so test failures are reproducible.
    // Each iteration generates a fresh "header + N random bytes"
    // file, opens it, walks every key, and confirms the open
    // returned within the timeout.
    let mut rng = Rng::new();
    rng.seed(0xEDB_DEC0_DEAD_BEEF);

    const ITERATIONS: usize = 64;
    const PER_ITERATION_TIMEOUT: Duration = Duration::from_secs(5);

    for iter in 0..ITERATIONS {
        let path = tmp_path(&format!("random-{iter}"));
        cleanup(&path);

        // Start with a valid header by opening + dropping.
        drop(Emdb::open(&path).expect("seed open"));

        // Append garbage of varying length.
        let garbage_len = rng.usize(0..2_048);
        let mut garbage = vec![0_u8; garbage_len];
        for byte in &mut garbage {
            *byte = rng.u8(..);
        }
        {
            let mut file = OpenOptions::new()
                .write(true)
                .open(&path)
                .expect("open for garbage");
            let _seek = file.seek(SeekFrom::Start(4096)).expect("seek past header");
            file.write_all(&garbage).expect("write garbage");
            file.sync_data().expect("sync garbage");
        }

        let started = Instant::now();
        let result = Emdb::open(&path);
        let elapsed = started.elapsed();

        assert!(
            elapsed < PER_ITERATION_TIMEOUT,
            "iter {iter}: open took {elapsed:?} on {garbage_len} random bytes — possible infinite loop"
        );

        // Either it opens (and len() returns *something*) or it
        // errors with a parsing error. Both are fine — what we
        // refuse to accept is a panic or a hang.
        match result {
            Ok(db) => {
                let _ = db.len();
                let _ = db.is_empty();
                // Walking the iterator must also terminate.
                let count = db.iter().expect("iter").take(10_000).count();
                std::hint::black_box(count);
                drop(db);
            }
            Err(_) => { /* parser rejected the garbage — fine */ }
        }

        cleanup(&path);
    }
}

#[test]
fn valid_prefix_then_garbage_recovers_only_the_prefix() -> Result<()> {
    let path = tmp_path("prefix-then-garbage");
    cleanup(&path);

    // Lay down a known-good prefix: 50 records via a clean Emdb
    // session.
    {
        let db = Emdb::open(&path)?;
        for i in 0..50 {
            db.insert(format!("k{i:03}"), format!("v{i}"))?;
        }
        db.flush()?;
        db.checkpoint()?;
    }

    // In v0.9 the journal file has no pre-allocated tail; the
    // file size after `flush()` equals the last frame's end LSN.
    // To exercise the "valid prefix then garbage" recovery path
    // we append random bytes to the journal's tail. fsys's
    // JournalReader walks frames forward; the moment it hits a
    // bad magic / bad CRC / truncated frame, it stops cleanly
    // and reports the tail state — and that's exactly what we
    // want recovery to detect.
    let tail_offset = std::fs::metadata(&path)?.len();
    {
        let mut file = OpenOptions::new().append(true).open(&path)?;
        let mut rng = Rng::new();
        rng.seed(42);
        let mut garbage = vec![0_u8; 256];
        for byte in &mut garbage {
            *byte = rng.u8(..);
        }
        file.write_all(&garbage)?;
        file.sync_data()?;
        let _ = tail_offset;
    }

    // Reopen — the garbage must be discarded by the recovery scan.
    let db = Emdb::open(&path)?;
    assert_eq!(db.len()?, 50, "only the valid prefix should be recovered");
    for i in 0..50 {
        let key = format!("k{i:03}");
        let want = format!("v{i}");
        assert_eq!(
            db.get(&key)?,
            Some(want.into_bytes()),
            "record {i} missing from valid prefix"
        );
    }

    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn varying_key_and_value_sizes_round_trip() -> Result<()> {
    // Defensive coverage of the encode/decode path against
    // non-standard sizes: zero-length values, long keys, long
    // values. The recovery scan must reproduce each insertion
    // verbatim.
    let path = tmp_path("size-variations");
    cleanup(&path);

    let cases: &[(&[u8], Vec<u8>)] = &[
        (b"", Vec::new()),
        (b"x", b"y".to_vec()),
        (b"empty-value", Vec::new()),
        (&[0_u8; 1], vec![0_u8; 1]),
        // 1 KiB key.
        (&[b'k'; 1024], vec![b'v'; 16]),
        // 64 KiB value.
        (b"big-value", vec![b'V'; 64 * 1024]),
        // Mixed binary content.
        (
            b"binary-key",
            (0_u8..=255).chain(0_u8..=255).collect::<Vec<_>>(),
        ),
    ];

    {
        let db = Emdb::open(&path)?;
        for (key, value) in cases {
            db.insert(*key, value.as_slice())?;
        }
        db.flush()?;
        db.checkpoint()?;
    }

    let db = Emdb::open(&path)?;
    for (key, value) in cases {
        assert_eq!(
            db.get(*key)?,
            Some(value.clone()),
            "round-trip mismatch for {} byte key",
            key.len()
        );
    }

    drop(db);
    cleanup(&path);
    Ok(())
}
