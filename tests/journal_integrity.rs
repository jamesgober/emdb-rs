// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Open-time handling of damaged and foreign files, explicit salvage,
//! `backup_to` target checks and the meta sidecar size limit.
//!
//! - Zero bytes inside a fully written record (a zeroed checksum
//!   field, a zeroed sector) followed by valid records are damage,
//!   not a crash artifact: the open is refused and the file is left
//!   untouched.
//! - What a crash can leave is still accepted: an unwritten
//!   reservation, a record cut at a sector boundary, a torn final
//!   record.
//! - `EmdbBuilder::salvage(true)` opens a damaged journal by keeping
//!   the records before the damage and moving the rest into a
//!   `.corrupt-<offset>` sidecar.
//! - A crafted journal cannot make the open slow.
//! - A file that is not a database is refused even when it starts
//!   with a zero byte.
//! - `backup_to` refuses the database's own sidecar files.
//! - An oversized meta sidecar is refused without reading it whole.

use std::path::{Path, PathBuf};

use emdb::{Emdb, Error, Result};

const MAGIC: [u8; 4] = [0x46, 0x53, 0x59, 0x01];

/// A fresh directory per test, removed by [`Dir::drop`].
struct Dir(PathBuf);

impl Dir {
    fn new(label: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0_u128, |d| d.as_nanos());
        let mut p = std::env::temp_dir();
        p.push(format!(
            "emdb-integrity-{label}-{}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&p).expect("create test dir");
        Self(p)
    }

    fn join(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }

    fn sidecars(&self) -> Vec<String> {
        std::fs::read_dir(&self.0)
            .expect("read dir")
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.contains(".corrupt-"))
            .collect()
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

fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = !0_u32;
    for &b in bytes {
        crc ^= u32::from(b);
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0x82F6_3B78
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

/// `(start, end)` of every frame in an undamaged journal.
fn frames(bytes: &[u8]) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut at = 0;
    while at + 12 <= bytes.len() && bytes[at..at + 4] == MAGIC {
        let len = u32::from_le_bytes([bytes[at + 4], bytes[at + 5], bytes[at + 6], bytes[at + 7]])
            as usize;
        let end = at + 12 + len;
        out.push((at, end));
        at = end;
    }
    out
}

/// A small first record, then three records with 4 KiB values, all
/// flushed. Returns the journal path.
fn four_records(dir: &Dir) -> Result<PathBuf> {
    let path = dir.join("db.emdb");
    let db = Emdb::open(&path)?;
    db.insert("k0", "small")?;
    for k in ["k1", "k2", "k3"] {
        db.insert(k, vec![b'v'; 4096])?;
    }
    db.flush()?;
    db.checkpoint()?;
    drop(db);
    Ok(path)
}

fn present(db: &Emdb) -> Result<Vec<bool>> {
    ["k0", "k1", "k2", "k3"]
        .iter()
        .map(|k| db.get(k).map(|v| v.is_some()))
        .collect()
}

fn assert_refused_untouched(dir: &Dir, path: &Path, damaged: &[u8]) {
    let result = Emdb::open(path);
    assert!(
        matches!(result, Err(Error::Corrupted { .. })),
        "damage before valid records must be refused, got {:?}",
        result.as_ref().map(|_| "an open database")
    );
    assert_eq!(file_bytes(path), damaged, "refused open modified the file");
    assert!(
        dir.sidecars().is_empty(),
        "refused open wrote a sidecar: {:?}",
        dir.sidecars()
    );
}

// ------------------------------------------------------ zero damage

#[test]
fn test_zeroed_checksum_before_valid_records_is_refused() -> Result<()> {
    let dir = Dir::new("zero-crc");
    let path = four_records(&dir)?;
    let mut bytes = file_bytes(&path);
    let f = frames(&bytes);
    assert_eq!(f.len(), 4);
    let (_, end) = f[1];
    bytes[end - 4..end].fill(0);
    std::fs::write(&path, &bytes).expect("write");
    assert_refused_untouched(&dir, &path, &bytes);
    Ok(())
}

#[test]
fn test_zeroed_sector_before_valid_records_is_refused() -> Result<()> {
    let dir = Dir::new("zero-sector");
    let path = four_records(&dir)?;
    let mut bytes = file_bytes(&path);
    let f = frames(&bytes);
    let (start, end) = f[1];
    let sector = (start + 8).div_ceil(512) * 512;
    assert!(sector + 512 <= end - 4);
    bytes[sector..sector + 512].fill(0);
    std::fs::write(&path, &bytes).expect("write");
    assert_refused_untouched(&dir, &path, &bytes);
    Ok(())
}

#[test]
fn test_zeroed_record_tail_before_valid_records_is_refused() -> Result<()> {
    // The last 100 bytes of a record (value bytes and checksum) read
    // back as zeros, with no sector boundary inside the zero run: no
    // interrupted write leaves that.
    let dir = Dir::new("zero-tail");
    let path = four_records(&dir)?;
    let mut bytes = file_bytes(&path);
    let f = frames(&bytes);
    let (_, end) = f[1];
    let from = (end - 100).max((end - 1) / 512 * 512 + 1);
    assert!(from + 4 <= end, "test layout: the checksum is zeroed");
    bytes[from..end].fill(0);
    std::fs::write(&path, &bytes).expect("write");
    assert_refused_untouched(&dir, &path, &bytes);
    Ok(())
}

// ------------------------------------------- what a crash can leave

#[test]
fn test_record_cut_at_a_sector_boundary_still_opens() -> Result<()> {
    // A write interrupted at a sector boundary: the record's bytes
    // from that boundary on never reached the file, a later record
    // written by another thread did.
    let dir = Dir::new("cut");
    let path = four_records(&dir)?;
    let mut bytes = file_bytes(&path);
    let f = frames(&bytes);
    let (start, end) = f[1];
    let cut = (start + 600).div_ceil(512) * 512;
    bytes[cut..end].fill(0);
    std::fs::write(&path, &bytes).expect("write");
    let db = Emdb::open(&path)?;
    let seen = present(&db)?;
    assert!(seen[0], "records before the crash point are kept");
    assert!(!seen[1], "the torn record is dropped");
    Ok(())
}

#[test]
fn test_unwritten_reservation_before_valid_records_still_opens() -> Result<()> {
    let dir = Dir::new("hole");
    let path = four_records(&dir)?;
    let mut bytes = file_bytes(&path);
    let f = frames(&bytes);
    let (start, end) = f[1];
    bytes[start..end].fill(0);
    std::fs::write(&path, &bytes).expect("write");
    let db = Emdb::open(&path)?;
    let seen = present(&db)?;
    assert!(seen[0]);
    assert!(!seen[1]);
    Ok(())
}

#[test]
fn test_torn_final_record_still_opens() -> Result<()> {
    let dir = Dir::new("torn");
    let path = four_records(&dir)?;
    let bytes = file_bytes(&path);
    std::fs::write(&path, &bytes[..bytes.len() - 1000]).expect("write");
    let db = Emdb::open(&path)?;
    assert_eq!(present(&db)?, vec![true, true, true, false]);
    Ok(())
}

// ------------------------------------------------------------ salvage

#[test]
fn test_salvage_opens_a_damaged_journal() -> Result<()> {
    let dir = Dir::new("salvage");
    let path = four_records(&dir)?;
    let mut bytes = file_bytes(&path);
    let f = frames(&bytes);
    let (start, _) = f[1];
    bytes[start + 600] ^= 0x01;
    std::fs::write(&path, &bytes).expect("write");
    assert_refused_untouched(&dir, &path, &bytes);

    let db = Emdb::builder().path(&path).salvage(true).build()?;
    assert_eq!(present(&db)?, vec![true, false, false, false]);
    db.insert("after", "salvage")?;
    db.flush()?;
    drop(db);
    let sidecars = dir.sidecars();
    assert_eq!(sidecars.len(), 1, "cut bytes kept in a sidecar");
    let saved = file_bytes(&dir.join(&sidecars[0]));
    assert_eq!(saved, bytes[start..], "sidecar holds every cut byte");

    // The salvaged database opens normally afterwards.
    let db = Emdb::open(&path)?;
    assert_eq!(db.get("k0")?.as_deref(), Some(&b"small"[..]));
    assert_eq!(db.get("after")?.as_deref(), Some(&b"salvage"[..]));
    Ok(())
}

#[test]
fn test_salvage_on_an_intact_database_changes_nothing() -> Result<()> {
    let dir = Dir::new("salvage-clean");
    let path = four_records(&dir)?;
    let before = file_bytes(&path);
    let db = Emdb::builder().path(&path).salvage(true).build()?;
    assert_eq!(present(&db)?, vec![true, true, true, true]);
    drop(db);
    assert_eq!(file_bytes(&path), before);
    assert!(dir.sidecars().is_empty());
    Ok(())
}

#[test]
fn test_salvage_still_refuses_a_foreign_file() {
    let dir = Dir::new("salvage-foreign");
    let path = dir.join("notes.txt");
    let content = b"not a database\n".repeat(50);
    std::fs::write(&path, &content).expect("write");
    let result = Emdb::builder().path(&path).salvage(true).build();
    assert!(matches!(result, Err(Error::MagicMismatch)));
    assert_eq!(file_bytes(&path), content);
}

// ---------------------------------------------------- crafted journal

/// One valid record, a non-magic byte, then a fake frame header every
/// 8 bytes whose length reaches the end of the file.
fn hostile(path: &Path, body: usize) {
    let mut out = Vec::new();
    let mut payload = vec![0_u8];
    payload.extend_from_slice(&0_u32.to_le_bytes());
    payload.extend_from_slice(&1_u32.to_le_bytes());
    payload.push(b'k');
    payload.extend_from_slice(&1_u32.to_le_bytes());
    payload.push(b'v');
    payload.extend_from_slice(&0_u64.to_le_bytes());
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&payload);
    let crc = crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out.push(0x01);
    let total = out.len() + body;
    while out.len() + 8 <= total {
        let at = out.len();
        out.extend_from_slice(&MAGIC);
        let len = (total - at).saturating_sub(12) as u32;
        out.extend_from_slice(&len.to_le_bytes());
    }
    std::fs::write(path, &out).expect("write");
}

#[test]
fn test_crafted_journal_is_refused_untouched() {
    // Classifying this file would take about 256 KiB^2 / 16 = 4 GiB of
    // checksumming (several seconds); the open gives up after a fixed
    // budget instead and refuses it. The work bound itself is checked
    // by the unit tests of the damage classifier, which count the
    // bytes it checksums.
    let dir = Dir::new("hostile");
    let path = dir.join("db.emdb");
    hostile(&path, 256 * 1024);
    let before = file_bytes(&path);
    let result = Emdb::open(&path);
    assert!(
        matches!(result, Err(Error::Corrupted { .. })),
        "{:?}",
        result.as_ref().map(|_| "an open database")
    );
    assert_eq!(file_bytes(&path), before);
}

// ------------------------------------------------------- foreign files

#[test]
fn test_foreign_file_starting_with_zero_is_refused_untouched() {
    let dir = Dir::new("foreign-zero");
    let path = dir.join("movie.mp4");
    let mut bytes = vec![0x00, 0x00, 0x00, 0x18];
    bytes.extend_from_slice(b"ftypmp42");
    bytes.extend((0..8192_u32).map(|i| (i * 31 % 251) as u8 | 1));
    std::fs::write(&path, &bytes).expect("write");
    let result = Emdb::open(&path);
    assert!(matches!(result, Err(Error::MagicMismatch)), "{result:?}");
    assert_eq!(file_bytes(&path), bytes, "foreign file modified");
    assert!(dir.sidecars().is_empty());
    assert!(!dir.join("movie.mp4.meta").exists());
}

#[test]
fn test_all_zero_file_opens_as_empty() -> Result<()> {
    // A crash before the first write reached the file.
    let dir = Dir::new("all-zero");
    let path = dir.join("db.emdb");
    std::fs::write(&path, vec![0_u8; 4096]).expect("write");
    let db = Emdb::open(&path)?;
    assert_eq!(db.len()?, 0);
    db.insert("k", "v")?;
    Ok(())
}

#[test]
fn test_unwritten_first_reservation_still_opens() -> Result<()> {
    // The first record's reservation never reached the file, the
    // second one did; the sidecar is gone too, so only the shape of
    // the file says it is a journal.
    let dir = Dir::new("zero-head");
    let path = four_records(&dir)?;
    let mut bytes = file_bytes(&path);
    let f = frames(&bytes);
    let (start, end) = f[0];
    bytes[start..end].fill(0);
    std::fs::write(&path, &bytes).expect("write");
    std::fs::remove_file(dir.join("db.emdb.meta")).expect("remove meta");
    let db = Emdb::open(&path)?;
    assert!(db.get("k0")?.is_none());
    Ok(())
}

// ---------------------------------------------------- backup targets

#[cfg(feature = "encrypt")]
#[test]
fn test_backup_to_own_sidecars_is_refused() -> Result<()> {
    let dir = Dir::new("backup-sidecar");
    let path = dir.join("db.emdb");
    let db = Emdb::builder()
        .path(&path)
        .encryption_passphrase("correct horse")
        .build()?;
    db.insert("k", "secret")?;
    db.flush()?;
    for suffix in [
        ".meta",
        ".lock",
        ".lock-meta",
        ".compact.tmp",
        ".enc.tmp",
        ".encbak",
        ".encbak.meta",
        ".encadmin",
    ] {
        let target = dir.join(&format!("db.emdb{suffix}"));
        let result = db.backup_to(&target);
        assert!(
            matches!(result, Err(Error::InvalidConfig(_))),
            "backup_to(<db>{suffix}) must be refused, got {result:?}"
        );
    }
    // A target whose own sidecar would land on the database's
    // `.encbak.meta`.
    let result = db.backup_to(dir.join("db.emdb.encbak"));
    assert!(matches!(result, Err(Error::InvalidConfig(_))));
    drop(db);

    let reopened = Emdb::builder()
        .path(&path)
        .encryption_passphrase("correct horse")
        .build()?;
    assert_eq!(reopened.get("k")?.as_deref(), Some(&b"secret"[..]));
    Ok(())
}

#[test]
fn test_backup_to_meta_sidecar_is_refused() -> Result<()> {
    let dir = Dir::new("backup-meta");
    let path = dir.join("db.emdb");
    let db = Emdb::open(&path)?;
    db.insert("k", "v")?;
    db.flush()?;
    let meta_before = file_bytes(&dir.join("db.emdb.meta"));
    let result = db.backup_to(dir.join("db.emdb.meta"));
    assert!(matches!(result, Err(Error::InvalidConfig(_))), "{result:?}");
    assert_eq!(file_bytes(&dir.join("db.emdb.meta")), meta_before);
    // A different file next to the database still works.
    db.backup_to(dir.join("copy.emdb"))?;
    drop(db);
    let copy = Emdb::open(dir.join("copy.emdb"))?;
    assert_eq!(copy.get("k")?.as_deref(), Some(&b"v"[..]));
    Ok(())
}

// ------------------------------------------------------- meta sidecar

#[test]
fn test_oversized_meta_sidecar_is_refused() -> Result<()> {
    let dir = Dir::new("meta-size");
    let path = dir.join("db.emdb");
    {
        let db = Emdb::open(&path)?;
        db.insert("k", "v")?;
        db.flush()?;
    }
    let meta = dir.join("db.emdb.meta");
    let mut body = file_bytes(&meta);
    body.extend_from_slice(&[0_u8; 4096]);
    std::fs::write(&meta, &body).expect("write");
    let journal = file_bytes(&path);
    let result = Emdb::open(&path);
    assert!(
        matches!(result, Err(Error::Corrupted { .. })),
        "{:?}",
        result.as_ref().map(|_| "an open database")
    );
    assert_eq!(file_bytes(&path), journal);
    assert_eq!(file_bytes(&meta), body);
    Ok(())
}
