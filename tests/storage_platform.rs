// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Platform-specific storage regression tests: a compaction whose
//! final rename fails must leave the database on its original file,
//! and a journal poisoned by a failed write must fail `flush` and
//! `checkpoint` instead of reporting success.
//!
//! Making a rename fail needs a platform mechanism: a read-only
//! directory on Unix, a handle opened without `FILE_SHARE_DELETE` on
//! Windows. Forcing a write failure needs `RLIMIT_FSIZE`, which only
//! Unix has; the Windows build covers the rename case only.

use std::path::PathBuf;

use emdb::{Emdb, Result};

struct Dir(PathBuf);

impl Dir {
    fn new(label: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0_u128, |d| d.as_nanos());
        let mut p = std::env::temp_dir();
        p.push(format!(
            "emdb-platform-{label}-{}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&p).expect("create test dir");
        Self(p)
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// After a compaction failed at its rename, reads, writes and a
/// reopen all see the original data plus anything written since.
fn assert_usable_after_failed_compaction(db: Emdb, path: &std::path::Path) -> Result<()> {
    db.insert("new-key", "new-val")?;
    assert_eq!(db.get("new-key")?.as_deref(), Some(&b"new-val"[..]));
    let missing = (100_000..100_100_u32)
        .filter(|i| db.get(format!("k{i}")).ok().flatten().is_none())
        .count();
    assert_eq!(missing, 0, "old keys unreadable after failed compaction");
    assert_eq!(db.iter()?.count(), 100_001);
    db.flush()?;
    drop(db);
    let db = Emdb::open(path)?;
    assert_eq!(db.len()?, 100_001);
    assert_eq!(db.get("new-key")?.as_deref(), Some(&b"new-val"[..]));
    Ok(())
}

fn seeded(path: &std::path::Path) -> Result<Emdb> {
    let db = Emdb::open(path)?;
    let batch: Vec<(String, Vec<u8>)> = (0..200_000_u32)
        .map(|i| (format!("k{i}"), vec![b'x'; 100]))
        .collect();
    db.insert_many(batch.iter().map(|(k, v)| (k.as_bytes(), v.as_slice())))?;
    for i in 0..100_000_u32 {
        let _ = db.remove(format!("k{i}"))?;
    }
    db.flush()?;
    Ok(db)
}

#[cfg(windows)]
#[test]
fn test_compact_failed_rename_keeps_original_file() -> Result<()> {
    use std::os::windows::fs::OpenOptionsExt;
    const FILE_SHARE_READ: u32 = 0x1;
    const FILE_SHARE_WRITE: u32 = 0x2;
    let dir = Dir::new("rename");
    let path = dir.0.join("db.emdb");
    let db = seeded(&path)?;
    // A handle without FILE_SHARE_DELETE makes replacing the file fail.
    let blocker = std::fs::OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .open(&path)
        .expect("open blocker");
    let result = db.compact();
    drop(blocker);
    assert!(result.is_err(), "rename over a locked file must fail");
    assert!(
        !dir.0.join("db.emdb.compact.tmp").exists(),
        "temporary left behind"
    );
    assert_usable_after_failed_compaction(db, &path)
}

#[cfg(unix)]
#[test]
fn test_compact_failed_rename_keeps_original_file() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let set_mode = |p: &std::path::Path, mode: u32| {
        std::fs::set_permissions(p, std::fs::Permissions::from_mode(mode)).expect("chmod");
    };
    let dir = Dir::new("rename");
    let path = dir.0.join("db.emdb");
    let db = seeded(&path)?;
    // Once the temporary exists, make the directory read-only so the
    // rename fails. Root ignores directory permissions; skip there.
    let probe = dir.0.join("probe");
    set_mode(&dir.0, 0o555);
    let root = std::fs::write(&probe, b"x").is_ok();
    set_mode(&dir.0, 0o755);
    if root {
        return Ok(());
    }
    let watched = dir.0.clone();
    let watcher = std::thread::spawn(move || {
        let tmp = watched.join("db.emdb.compact.tmp");
        let start = std::time::Instant::now();
        while !tmp.exists() {
            if start.elapsed().as_secs() > 30 {
                return false;
            }
            std::hint::spin_loop();
        }
        set_mode(&watched, 0o555);
        true
    });
    let result = db.compact();
    let raced = watcher.join().expect("watcher");
    set_mode(&dir.0, 0o755);
    assert!(raced, "temporary never appeared");
    assert!(
        result.is_err(),
        "compaction finished before the directory became read-only"
    );
    assert_usable_after_failed_compaction(db, &path)
}

#[cfg(unix)]
#[test]
fn test_poisoned_journal_fails_flush_and_checkpoint() {
    use std::process::Command;
    if let Ok(path) = std::env::var("EMDB_POISON_CHILD") {
        poison_child(&PathBuf::from(path));
        return;
    }
    let dir = Dir::new("poison");
    let path = dir.0.join("db.emdb");
    let exe = std::env::current_exe().expect("exe");
    // Run this test again in a child limited to ~600 KiB per file,
    // with SIGXFSZ ignored so the over-limit write fails with EFBIG.
    let script = format!(
        "trap '' XFSZ; ulimit -f 600; exec '{}' test_poisoned_journal_fails_flush_and_checkpoint --exact --nocapture --test-threads=1",
        exe.display()
    );
    let output = Command::new("sh")
        .arg("-c")
        .arg(script)
        .env("EMDB_POISON_CHILD", &path)
        .output()
        .expect("run child");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "child failed: {stdout}");
    // The harness prints the test name on the same line as the child's
    // first output, so look for the marker anywhere in a line.
    let acked: u32 = stdout
        .lines()
        .find_map(|l| l.split("ACKED ").nth(1))
        .and_then(|n| n.split_whitespace().next())
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("child reported no ACKED line: {stdout}"));
    assert!(acked > 0, "no write succeeded before the limit: {stdout}");
    assert!(
        stdout.contains("CHECKPOINT err"),
        "checkpoint succeeded on a poisoned journal: {stdout}"
    );
    assert!(stdout.contains("FLUSH err"), "{stdout}");
    let db = Emdb::open(&path).expect("reopen after poison");
    for i in 0..acked {
        assert!(
            db.get(format!("k{i}")).expect("get").is_some(),
            "acknowledged k{i} lost"
        );
    }
}

#[cfg(unix)]
fn poison_child(path: &std::path::Path) {
    let db = Emdb::open(path).expect("open");
    let mut acked = 0;
    for i in 0..100_000_u32 {
        if db.insert(format!("k{i}"), vec![b'v'; 1000]).is_err() || db.flush().is_err() {
            break;
        }
        acked = i + 1;
    }
    println!("ACKED {acked}");
    let _ = db.insert("tiny", "x");
    let flush = if db.flush().is_ok() { "ok" } else { "err" };
    println!("FLUSH {flush}");
    let checkpoint = if db.checkpoint().is_ok() { "ok" } else { "err" };
    println!("CHECKPOINT {checkpoint}");
}
