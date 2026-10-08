// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Regression tests for the 1.0.3 storage and durability fixes:
//! writes after compaction, compaction under concurrent load, opening
//! foreign or damaged files, persisted `clear` / `drop_namespace`,
//! backup sidecars, partial mappings, open-time memory, error
//! reporting and temporary-file hygiene.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

use emdb::{Emdb, Error, Result};

/// A fresh directory per test, removed by [`Dir::drop`].
struct Dir(PathBuf);

impl Dir {
    fn new(label: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0_u128, |d| d.as_nanos());
        let mut p = std::env::temp_dir();
        p.push(format!(
            "emdb-durability-{label}-{}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&p).expect("create test dir");
        Self(p)
    }

    fn join(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }

    fn files(&self) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(&self.0)
            .expect("read dir")
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn file_bytes(path: &Path) -> Vec<u8> {
    std::fs::read(path).expect("read file")
}

// ---------------------------------------------------------------- C1

#[test]
fn test_compact_then_write_survives_reopen() -> Result<()> {
    let dir = Dir::new("c1");
    let path = dir.join("db.emdb");
    {
        let db = Emdb::open(&path)?;
        for i in 0..200_u32 {
            db.insert(format!("k{i}"), format!("v{i}"))?;
        }
        for i in 0..150_u32 {
            let _ = db.remove(format!("k{i}"))?;
        }
        db.flush()?;
        db.compact()?;
        db.insert("after-compact", "must-survive")?;
        db.insert("k199", "updated")?;
        let _ = db.remove("k198")?;
        db.flush()?;
        assert_eq!(
            db.get("after-compact")?.as_deref(),
            Some(&b"must-survive"[..])
        );
        assert_eq!(db.get("k199")?.as_deref(), Some(&b"updated"[..]));
    }
    let db = Emdb::open(&path)?;
    assert_eq!(
        db.get("after-compact")?.as_deref(),
        Some(&b"must-survive"[..])
    );
    assert_eq!(db.get("k199")?.as_deref(), Some(&b"updated"[..]));
    assert!(
        db.get("k198")?.is_none(),
        "remove after compact resurrected"
    );
    assert_eq!(db.len()?, 50);
    Ok(())
}

#[test]
fn test_compact_twice_then_write_survives_reopen() -> Result<()> {
    let dir = Dir::new("c1-twice");
    let path = dir.join("db.emdb");
    {
        let db = Emdb::open(&path)?;
        db.insert("a", "1")?;
        db.compact()?;
        db.insert("b", "2")?;
        db.compact()?;
        db.insert("c", "3")?;
        db.flush()?;
    }
    let db = Emdb::open(&path)?;
    for (k, v) in [("a", "1"), ("b", "2"), ("c", "3")] {
        assert_eq!(db.get(k)?.as_deref(), Some(v.as_bytes()));
    }
    Ok(())
}

// ---------------------------------------------------------------- C2

#[test]
fn test_compact_concurrent_inserts_all_survive() -> Result<()> {
    let dir = Dir::new("c2-writers");
    let path = dir.join("db.emdb");
    let db = Emdb::open(&path)?;
    for i in 0..20_000_u32 {
        db.insert(format!("base-{i}"), vec![b'x'; 64])?;
    }
    let writer = {
        let db = db.clone();
        std::thread::spawn(move || {
            let mut acked = Vec::new();
            for i in 0..5_000_u32 {
                let k = format!("conc-{i}");
                if db.insert(k.clone(), "v").is_ok() {
                    acked.push(k);
                }
            }
            acked
        })
    };
    db.compact()?;
    db.compact()?;
    let acked = writer.join().expect("writer thread");
    assert_eq!(acked.len(), 5_000);
    for k in &acked {
        assert!(db.get(k)?.is_some(), "{k} missing in-process");
    }
    db.flush()?;
    drop(db);
    let db = Emdb::open(&path)?;
    for k in &acked {
        assert!(db.get(k)?.is_some(), "{k} missing after reopen");
    }
    for i in 0..20_000_u32 {
        assert!(db.get(format!("base-{i}"))?.is_some());
    }
    Ok(())
}

#[test]
fn test_compact_concurrent_readers_never_miss() -> Result<()> {
    let dir = Dir::new("c2-readers");
    let path = dir.join("db.emdb");
    let db = Emdb::open(&path)?;
    for i in 0..5_000_u32 {
        db.insert(format!("k{i}"), format!("value-{i}"))?;
        db.insert(format!("k{i}"), format!("value-{i}"))?; // garbage for compaction
    }
    let stop = Arc::new(AtomicBool::new(false));
    let misses = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let mut readers = Vec::new();
    for t in 0..4_u32 {
        let db = db.clone();
        let stop = Arc::clone(&stop);
        let misses = Arc::clone(&misses);
        let errors = Arc::clone(&errors);
        readers.push(std::thread::spawn(move || {
            let mut i = t;
            while !stop.load(Ordering::Acquire) {
                let k = format!("k{}", i % 5_000);
                match db.get(&k) {
                    Ok(Some(v)) => assert_eq!(v, format!("value-{}", i % 5_000).into_bytes()),
                    Ok(None) => {
                        let _ = misses.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        let _ = errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                i = i.wrapping_add(7);
            }
        }));
    }
    for _ in 0..20 {
        db.compact()?;
    }
    stop.store(true, Ordering::Release);
    for r in readers {
        r.join().expect("reader thread");
    }
    assert_eq!(misses.load(Ordering::Relaxed), 0, "spurious misses");
    assert_eq!(errors.load(Ordering::Relaxed), 0, "spurious errors");
    Ok(())
}

#[test]
fn test_iter_opened_before_compact_yields_snapshot() -> Result<()> {
    let dir = Dir::new("c2-iter");
    let path = dir.join("db.emdb");
    let db = Emdb::open(&path)?;
    for i in 0..100_u32 {
        db.insert(format!("key-{i:03}"), format!("val-{i:03}"))?;
    }
    for i in 0..100_u32 {
        db.insert(format!("key-{i:03}"), format!("VAL2-{i:03}"))?;
    }
    let it = db.iter()?;
    let keys = db.keys()?;
    db.compact()?;
    db.insert("key-new", "after")?;
    let items: Vec<(Vec<u8>, Vec<u8>)> = it.collect();
    assert_eq!(items.len(), 100);
    for (k, v) in &items {
        let k = String::from_utf8_lossy(k);
        assert_eq!(String::from_utf8_lossy(v), format!("VAL2-{}", &k[4..]));
    }
    assert_eq!(keys.count(), 100);
    Ok(())
}

#[test]
fn test_range_iter_opened_before_compact_yields_snapshot() -> Result<()> {
    let dir = Dir::new("c2-range");
    let path = dir.join("db.emdb");
    let db = Emdb::builder()
        .path(&path)
        .enable_range_scans(true)
        .build()?;
    for i in 0..50_u32 {
        db.insert(format!("r{i:02}"), format!("v{i}"))?;
        db.insert(format!("r{i:02}"), format!("w{i}"))?;
    }
    let it = db.range_iter(b"r".to_vec()..b"s".to_vec())?;
    db.compact()?;
    let items: Vec<_> = it.collect();
    assert_eq!(items.len(), 50);
    assert!(items.iter().all(|(_, v)| v.starts_with(b"w")));
    Ok(())
}

#[test]
fn test_valueref_across_compact_stays_valid() -> Result<()> {
    let dir = Dir::new("c2-valueref");
    let path = dir.join("db.emdb");
    let db = Emdb::open(&path)?;
    for i in 0..1_000_u32 {
        db.insert(format!("k{i}"), format!("value-{i}"))?;
    }
    for i in 0..900_u32 {
        let _ = db.remove(format!("k{i}"))?;
    }
    let vr = db.get_zerocopy("k950")?.expect("present");
    db.compact()?;
    db.compact()?;
    assert_eq!(vr.as_slice(), b"value-950");
    Ok(())
}

// ---------------------------------------------------------------- H1

#[test]
fn test_open_foreign_file_refused_and_untouched() {
    let dir = Dir::new("h1");
    let path = dir.join("notes.txt");
    let content = b"my important notes, not a database\n".repeat(100);
    std::fs::write(&path, &content).expect("write");
    let result = Emdb::open(&path);
    assert!(matches!(result, Err(Error::MagicMismatch)), "{result:?}");
    assert_eq!(file_bytes(&path), content, "foreign file modified");
    let files = dir.files();
    assert!(
        !files
            .iter()
            .any(|f| f.contains(".corrupt-") || f.ends_with(".meta")),
        "open left files behind: {files:?}"
    );
}

#[test]
fn test_open_journal_without_sidecar_still_opens() -> Result<()> {
    // emdb 1.0.2's backup_to left the backup without `<target>.meta`.
    let dir = Dir::new("h1-nosidecar");
    let path = dir.join("db.emdb");
    {
        let db = Emdb::open(&path)?;
        db.insert("k", "v")?;
        db.flush()?;
    }
    std::fs::remove_file(dir.join("db.emdb.meta")).expect("remove meta");
    let db = Emdb::open(&path)?;
    assert_eq!(db.get("k")?.as_deref(), Some(&b"v"[..]));
    drop(db);
    assert!(
        dir.join("db.emdb.meta").exists(),
        "sidecar recreated after open"
    );
    Ok(())
}

// ---------------------------------------------------------------- H2

#[test]
fn test_midfile_bitflip_refused_and_untouched() -> Result<()> {
    let dir = Dir::new("h2");
    let path = dir.join("db.emdb");
    {
        let db = Emdb::open(&path)?;
        for i in 0..1_000_u32 {
            db.insert(format!("k{i:04}"), format!("value-{i:04}"))?;
        }
        db.flush()?;
    }
    let mut bytes = file_bytes(&path);
    bytes[10 * 40 + 20] ^= 0x01;
    std::fs::write(&path, &bytes).expect("write damaged file");
    let result = Emdb::open(&path);
    assert!(
        matches!(result, Err(Error::Corrupted { .. })),
        "mid-file damage must be refused, got {result:?}"
    );
    assert_eq!(file_bytes(&path), bytes, "refused open modified the file");
    assert!(
        !dir.files().iter().any(|f| f.contains(".corrupt-")),
        "refused open wrote a corrupt-tail sidecar"
    );
    Ok(())
}

#[test]
fn test_torn_final_frame_still_recovers() -> Result<()> {
    let dir = Dir::new("h2-torn");
    let path = dir.join("db.emdb");
    {
        let db = Emdb::open(&path)?;
        for i in 0..100_u32 {
            db.insert(format!("k{i:03}"), format!("value-{i:03}"))?;
        }
        db.flush()?;
    }
    let len = std::fs::metadata(&path).expect("meta").len();
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .expect("open");
    file.set_len(len - 5).expect("truncate");
    drop(file);
    let db = Emdb::open(&path)?;
    assert_eq!(db.len()?, 99);
    Ok(())
}

// ---------------------------------------------------------------- H3

#[test]
fn test_clear_persists_across_reopen() -> Result<()> {
    let dir = Dir::new("h3-clear");
    let path = dir.join("db.emdb");
    {
        let db = Emdb::open(&path)?;
        db.insert("a", "1")?;
        db.insert("b", "2")?;
        let ns = db.namespace("other")?;
        ns.insert("x", "y")?;
        db.flush()?;
        db.clear()?;
        db.insert("c", "3")?;
        db.flush()?;
        assert_eq!(db.len()?, 1);
    }
    let db = Emdb::open(&path)?;
    assert_eq!(db.len()?, 1, "cleared records resurrected");
    assert!(db.get("a")?.is_none());
    assert_eq!(db.get("c")?.as_deref(), Some(&b"3"[..]));
    assert_eq!(
        db.namespace("other")?.len()?,
        1,
        "clear touched another namespace"
    );
    Ok(())
}

#[test]
fn test_namespace_clear_persists_across_reopen() -> Result<()> {
    let dir = Dir::new("h3-nsclear");
    let path = dir.join("db.emdb");
    {
        let db = Emdb::open(&path)?;
        let ns = db.namespace("users")?;
        for i in 0..100_u32 {
            ns.insert(format!("u{i}"), "x")?;
        }
        ns.clear()?;
        db.flush()?;
    }
    let db = Emdb::open(&path)?;
    assert_eq!(db.namespace("users")?.len()?, 0);
    Ok(())
}

#[test]
fn test_drop_namespace_persists_across_reopen() -> Result<()> {
    let dir = Dir::new("h3-drop");
    let path = dir.join("db.emdb");
    {
        let db = Emdb::open(&path)?;
        let ns = db.namespace("users")?;
        ns.insert("alice", "secret")?;
        let keep = db.namespace("keep")?;
        keep.insert("k", "v")?;
        db.flush()?;
        assert!(db.drop_namespace("users")?);
        db.flush()?;
    }
    {
        let db = Emdb::open(&path)?;
        let names = db.list_namespaces()?;
        assert!(
            !names.contains(&"users".to_string()),
            "dropped name came back: {names:?}"
        );
        assert!(names.contains(&"keep".to_string()));
        assert_eq!(db.stats()?.namespace_count, 1);
        let ns = db.namespace("users")?;
        assert!(
            ns.get("alice")?.is_none(),
            "dropped namespace data resurrected"
        );
        ns.insert("bob", "new")?;
        db.flush()?;
        db.compact()?;
    }
    let db = Emdb::open(&path)?;
    let ns = db.namespace("users")?;
    assert!(ns.get("alice")?.is_none());
    assert_eq!(ns.get("bob")?.as_deref(), Some(&b"new"[..]));
    assert_eq!(db.namespace("keep")?.get("k")?.as_deref(), Some(&b"v"[..]));
    Ok(())
}

#[cfg(feature = "encrypt")]
#[test]
fn test_encrypted_clear_and_drop_persist() -> Result<()> {
    let dir = Dir::new("h3-enc");
    let path = dir.join("db.emdb");
    let key = [5_u8; 32];
    {
        let db = Emdb::builder().path(&path).encryption_key(key).build()?;
        db.insert("a", "1")?;
        db.namespace("gone")?.insert("x", "y")?;
        db.clear()?;
        assert!(db.drop_namespace("gone")?);
        db.flush()?;
    }
    let db = Emdb::builder().path(&path).encryption_key(key).build()?;
    assert_eq!(db.len()?, 0);
    assert_eq!(db.list_namespaces()?, vec![String::new()]);
    Ok(())
}

// ---------------------------------------------------------------- H4

#[cfg(feature = "encrypt")]
#[test]
fn test_backup_passphrase_encrypted_opens() -> Result<()> {
    let dir = Dir::new("h4-pass");
    let path = dir.join("db.emdb");
    let backup = dir.join("backup.emdb");
    {
        let db = Emdb::builder()
            .path(&path)
            .encryption_passphrase("pw")
            .build()?;
        db.insert("k", "v")?;
        db.backup_to(&backup)?;
    }
    let files = dir.files();
    assert!(files.contains(&"backup.emdb.meta".to_string()), "{files:?}");
    assert!(!files.iter().any(|f| f.contains(".tmp")), "{files:?}");
    let db = Emdb::builder()
        .path(&backup)
        .encryption_passphrase("pw")
        .build()?;
    assert_eq!(db.get("k")?.as_deref(), Some(&b"v"[..]));
    Ok(())
}

#[cfg(feature = "encrypt")]
#[test]
fn test_backup_raw_key_rejects_wrong_key_without_damage() -> Result<()> {
    let dir = Dir::new("h4-raw");
    let path = dir.join("db.emdb");
    let backup = dir.join("backup.emdb");
    let key = [7_u8; 32];
    {
        let db = Emdb::builder().path(&path).encryption_key(key).build()?;
        db.insert("k", "v")?;
        db.backup_to(&backup)?;
    }
    let meta_before = file_bytes(&dir.join("backup.emdb.meta"));
    assert!(Emdb::builder()
        .path(&backup)
        .encryption_key([9_u8; 32])
        .build()
        .is_err());
    assert!(
        Emdb::open(&backup).is_err(),
        "unkeyed open of an encrypted backup"
    );
    assert_eq!(file_bytes(&dir.join("backup.emdb.meta")), meta_before);
    let db = Emdb::builder().path(&backup).encryption_key(key).build()?;
    assert_eq!(db.get("k")?.as_deref(), Some(&b"v"[..]));
    Ok(())
}

#[cfg(feature = "encrypt")]
#[test]
fn test_failed_open_without_sidecar_writes_nothing() -> Result<()> {
    // A journal whose sidecar is missing, opened with the wrong key,
    // must not get a sidecar carrying that key's verification block.
    let dir = Dir::new("h4-poison");
    let path = dir.join("db.emdb");
    let key = [7_u8; 32];
    {
        let db = Emdb::builder().path(&path).encryption_key(key).build()?;
        db.insert("k", "v")?;
        db.flush()?;
    }
    std::fs::remove_file(dir.join("db.emdb.meta")).expect("remove meta");
    let journal = file_bytes(&path);
    assert!(Emdb::builder()
        .path(&path)
        .encryption_key([9_u8; 32])
        .build()
        .is_err());
    assert!(
        !dir.join("db.emdb.meta").exists(),
        "failed open wrote a sidecar"
    );
    assert_eq!(
        file_bytes(&path),
        journal,
        "failed open modified the journal"
    );
    Ok(())
}

#[cfg(feature = "encrypt")]
#[test]
fn test_backup_over_existing_encrypted_target_replaces_sidecar() -> Result<()> {
    let dir = Dir::new("h4-over");
    let enc = dir.join("enc.emdb");
    let plain = dir.join("plain.emdb");
    let backup = dir.join("backup.emdb");
    {
        let db = Emdb::builder()
            .path(&enc)
            .encryption_key([3_u8; 32])
            .build()?;
        db.insert("x", "1")?;
        db.backup_to(&backup)?;
    }
    drop(
        Emdb::builder()
            .path(&backup)
            .encryption_key([3_u8; 32])
            .build()?,
    );
    {
        let db = Emdb::open(&plain)?;
        db.insert("y", "2")?;
        db.backup_to(&backup)?;
    }
    let db = Emdb::open(&backup)?;
    assert_eq!(db.get("y")?.as_deref(), Some(&b"2"[..]));
    assert!(db.get("x")?.is_none());
    Ok(())
}

#[test]
fn test_backup_removes_stale_temporaries() -> Result<()> {
    let dir = Dir::new("h4-stale");
    let path = dir.join("db.emdb");
    let backup = dir.join("backup.emdb");
    // What a crashed 1.0.2 backup and compaction left behind.
    std::fs::write(dir.join("backup.emdb.backup.tmp.meta"), b"stale").expect("write");
    std::fs::write(dir.join("db.emdb.compact.tmp.meta"), b"stale").expect("write");
    let db = Emdb::open(&path)?;
    db.insert("a", "1")?;
    db.backup_to(&backup)?;
    db.compact()?;
    let files = dir.files();
    assert!(!files.iter().any(|f| f.contains(".tmp")), "{files:?}");
    assert_eq!(Emdb::open(&backup)?.get("a")?.as_deref(), Some(&b"1"[..]));
    Ok(())
}

#[test]
fn test_compact_ignores_stale_temp_journal() -> Result<()> {
    let dir = Dir::new("compact-stale");
    let path = dir.join("db.emdb");
    let seed = dir.join("seed.emdb");
    {
        // A journal holding a key the live database has deleted.
        let other = Emdb::open(&seed)?;
        other.insert("deleted", "resurrected")?;
        other.flush()?;
    }
    let db = Emdb::open(&path)?;
    db.insert("deleted", "x")?;
    let _ = db.remove("deleted")?;
    db.insert("live", "1")?;
    // A leftover temporary from a crashed compaction.
    let _ = std::fs::copy(&seed, dir.join("db.emdb.compact.tmp")).expect("seed stale temp");
    db.compact()?;
    assert!(
        db.get("deleted")?.is_none(),
        "stale temp records resurrected"
    );
    drop(db);
    let db = Emdb::open(&path)?;
    assert!(db.get("deleted")?.is_none());
    assert_eq!(db.get("live")?.as_deref(), Some(&b"1"[..]));
    Ok(())
}

// ---------------------------------------------------------------- H5

#[test]
fn test_large_records_never_read_as_missing() -> Result<()> {
    let dir = Dir::new("h5");
    let path = dir.join("db.emdb");
    let db = Emdb::open(&path)?;
    let latest = Arc::new(AtomicI64::new(-1));
    let stop = Arc::new(AtomicBool::new(false));
    let misses = Arc::new(AtomicU64::new(0));
    let mut threads = Vec::new();
    for _ in 0..3 {
        let db = db.clone();
        let latest = Arc::clone(&latest);
        let stop = Arc::clone(&stop);
        let misses = Arc::clone(&misses);
        threads.push(std::thread::spawn(move || {
            while !stop.load(Ordering::Acquire) {
                let l = latest.load(Ordering::Acquire);
                if l >= 0 && db.get(format!("k{l}")).ok().flatten().is_none() {
                    let _ = misses.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }
    // Something that remaps while appends are in flight.
    {
        let ns = db.namespace("empty")?;
        let stop = Arc::clone(&stop);
        threads.push(std::thread::spawn(move || {
            while !stop.load(Ordering::Acquire) {
                let _ = ns.iter().map(Iterator::count);
            }
        }));
    }
    let big = vec![b'x'; 4 << 20];
    for i in 0..40_i64 {
        db.insert(format!("k{i}"), big.clone())?;
        latest.store(i, Ordering::Release);
    }
    stop.store(true, Ordering::Release);
    for t in threads {
        t.join().expect("thread");
    }
    assert_eq!(misses.load(Ordering::Relaxed), 0);
    Ok(())
}

// ---------------------------------------------------------------- H8

#[cfg(feature = "ttl")]
#[test]
fn test_open_sweeps_expired_records() -> Result<()> {
    use emdb::Ttl;
    let dir = Dir::new("h8");
    let path = dir.join("db.emdb");
    {
        let db = Emdb::open(&path)?;
        for i in 0..50_u32 {
            db.insert_with_ttl(
                format!("short{i}"),
                "x",
                Ttl::After(std::time::Duration::from_millis(1)),
            )?;
            db.insert(format!("keep{i}"), "y")?;
        }
        // Re-inserted without a TTL: must survive the sweep.
        db.insert_with_ttl(
            "revived",
            "x",
            Ttl::After(std::time::Duration::from_millis(1)),
        )?;
        db.insert("revived", "now permanent")?;
        db.flush()?;
    }
    std::thread::sleep(std::time::Duration::from_millis(20));
    {
        let db = Emdb::open(&path)?;
        assert_eq!(db.len()?, 51, "expired records not swept at open");
        assert_eq!(db.get("revived")?.as_deref(), Some(&b"now permanent"[..]));
    }
    let db = Emdb::open(&path)?;
    assert_eq!(db.len()?, 51, "open-time sweep not persisted");
    Ok(())
}

#[cfg(all(feature = "ttl", feature = "encrypt"))]
#[test]
fn test_open_sweeps_expired_records_encrypted() -> Result<()> {
    use emdb::Ttl;
    let dir = Dir::new("h8-enc");
    let path = dir.join("db.emdb");
    let key = [4_u8; 32];
    {
        let db = Emdb::builder().path(&path).encryption_key(key).build()?;
        for i in 0..20_u32 {
            db.insert_with_ttl(
                format!("short{i}"),
                "x",
                Ttl::After(std::time::Duration::from_millis(1)),
            )?;
            db.insert(format!("keep{i}"), "y")?;
        }
        db.flush()?;
    }
    std::thread::sleep(std::time::Duration::from_millis(20));
    let db = Emdb::builder().path(&path).encryption_key(key).build()?;
    assert_eq!(db.len()?, 20);
    Ok(())
}

// ---------------------------------------------------------- errors

#[test]
fn test_oversized_record_reports_invalid_input() {
    let db = Emdb::open_in_memory();
    let big = vec![0_u8; 256 * 1024 * 1024];
    match db.insert("big", big) {
        Err(Error::Io(err)) => {
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
            assert!(err.to_string().contains("256 MiB"), "{err}");
        }
        other => panic!("expected InvalidInput, got {other:?}"),
    }
    db.insert("small", "v").expect("journal still usable");
    assert_eq!(db.get("small").expect("get").as_deref(), Some(&b"v"[..]));
}

#[test]
fn test_io_error_source_is_exposed() {
    use std::error::Error as _;
    let err = Error::Io(std::io::Error::other("boom"));
    assert!(err.source().is_some());
    assert!(err.to_string().contains("boom"));
}

// ---------------------------------------------------------- hygiene

#[test]
fn test_open_in_memory_leaves_no_files() {
    let path;
    {
        let db = Emdb::open_in_memory();
        db.insert("k", "v").expect("insert");
        db.flush().expect("flush");
        path = db.path().to_path_buf();
    }
    let name = path
        .file_name()
        .expect("name")
        .to_string_lossy()
        .into_owned();
    let left: Vec<String> = std::fs::read_dir(std::env::temp_dir())
        .expect("read temp dir")
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with(&name))
        .collect();
    assert!(left.is_empty(), "ephemeral db left files: {left:?}");
}

#[test]
fn test_compact_leaves_no_temporaries() -> Result<()> {
    let dir = Dir::new("compact-files");
    let path = dir.join("db.emdb");
    let db = Emdb::open(&path)?;
    db.insert("a", "1")?;
    db.compact()?;
    let files = dir.files();
    assert!(
        !files.iter().any(|f| f.contains("compact.tmp")),
        "{files:?}"
    );
    Ok(())
}

#[test]
fn test_checkpoint_syncs_and_succeeds() -> Result<()> {
    let dir = Dir::new("checkpoint");
    let db = Emdb::open(dir.join("db.emdb"))?;
    db.insert("a", "1")?;
    db.checkpoint()?;
    Ok(())
}
