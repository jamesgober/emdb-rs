// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Regression tests for the encryption admin rewrite: expiry is kept,
//! expired records are dropped, the database lock is held, and an
//! interrupted swap is completed (or an orphaned `.encbak` protected)
//! on the next open.

#![cfg(feature = "encrypt")]

use std::path::PathBuf;

use emdb::{Emdb, EncryptionInput, Error, Result};

struct Dir(PathBuf);

impl Dir {
    fn new(label: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0_u128, |d| d.as_nanos());
        let mut p = std::env::temp_dir();
        p.push(format!("emdb-admin-{label}-{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&p).expect("create test dir");
        Self(p)
    }

    fn join(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

const OLD: [u8; 32] = [1_u8; 32];
const NEW: [u8; 32] = [2_u8; 32];

fn seed(path: &std::path::Path, n: u32) -> Result<()> {
    let db = Emdb::builder().path(path).encryption_key(OLD).build()?;
    for i in 0..n {
        db.insert(format!("k{i}"), "v")?;
    }
    db.namespace("ns")?.insert("nk", "nv")?;
    db.flush()
}

#[cfg(feature = "ttl")]
#[test]
fn test_rotate_preserves_ttl_and_drops_expired() -> Result<()> {
    use emdb::Ttl;
    let dir = Dir::new("ttl");
    let path = dir.join("db.emdb");
    {
        let db = Emdb::builder().path(&path).encryption_key(OLD).build()?;
        db.insert_with_ttl(
            "session",
            "tok",
            Ttl::After(std::time::Duration::from_secs(3600)),
        )?;
        db.insert_with_ttl("gone", "x", Ttl::After(std::time::Duration::from_millis(1)))?;
        db.insert("forever", "y")?;
        db.flush()?;
    }
    std::thread::sleep(std::time::Duration::from_millis(20));
    Emdb::rotate_encryption_key(&path, EncryptionInput::Key(OLD), EncryptionInput::Key(NEW))?;
    let db = Emdb::builder().path(&path).encryption_key(NEW).build()?;
    let ttl = db.ttl("session")?.expect("ttl kept");
    assert!(ttl > std::time::Duration::from_secs(3000), "{ttl:?}");
    assert!(db.ttl("forever")?.is_none());
    assert!(db.get("gone")?.is_none());
    assert_eq!(db.len()?, 2, "expired record copied");
    Ok(())
}

#[test]
fn test_admin_refused_while_database_open() -> Result<()> {
    let dir = Dir::new("lock");
    let path = dir.join("db.emdb");
    seed(&path, 5)?;
    let held = Emdb::builder().path(&path).encryption_key(OLD).build()?;
    let result =
        Emdb::rotate_encryption_key(&path, EncryptionInput::Key(OLD), EncryptionInput::Key(NEW));
    assert!(matches!(result, Err(Error::LockBusy { .. })), "{result:?}");
    drop(held);
    Emdb::rotate_encryption_key(&path, EncryptionInput::Key(OLD), EncryptionInput::Key(NEW))?;
    let db = Emdb::builder().path(&path).encryption_key(NEW).build()?;
    assert_eq!(db.len()?, 5);
    assert_eq!(db.namespace("ns")?.get("nk")?.as_deref(), Some(&b"nv"[..]));
    Ok(())
}

#[test]
fn test_old_release_interrupted_rotation_is_not_lost() -> Result<()> {
    // The state emdb 1.0.2 left when it crashed after moving the
    // original to `.encbak` and before moving the new file in.
    let dir = Dir::new("orphan");
    let path = dir.join("db.emdb");
    seed(&path, 10)?;
    std::fs::rename(&path, dir.join("db.emdb.encbak")).expect("rename");
    std::fs::rename(dir.join("db.emdb.meta"), dir.join("db.emdb.encbak.meta")).expect("rename");

    let open = Emdb::builder().path(&path).encryption_key(OLD).build();
    assert!(
        matches!(open, Err(Error::InvalidConfig(_))),
        "opened an empty db: {open:?}"
    );
    let retry =
        Emdb::rotate_encryption_key(&path, EncryptionInput::Key(OLD), EncryptionInput::Key(NEW));
    assert!(retry.is_err(), "retry must not proceed");

    let bak = Emdb::builder()
        .path(dir.join("db.emdb.encbak"))
        .encryption_key(OLD)
        .build()?;
    assert_eq!(bak.len()?, 10, "original data destroyed");
    Ok(())
}

#[test]
fn test_interrupted_swap_is_completed_on_open() -> Result<()> {
    // Reconstruct a crash between the sidecar rename and the journal
    // rename of a passphrase rotation: marker present, the new journal
    // still at `.enc.tmp`, the new sidecar already in place.
    let dir = Dir::new("marker");
    let path = dir.join("db.emdb");
    {
        let db = Emdb::builder()
            .path(&path)
            .encryption_passphrase("old")
            .build()?;
        db.insert("k", "v")?;
        db.flush()?;
    }
    let staging = dir.join("staging.emdb");
    std::fs::copy(&path, &staging).expect("copy");
    std::fs::copy(dir.join("db.emdb.meta"), dir.join("staging.emdb.meta")).expect("copy");
    Emdb::rotate_encryption_key(
        &staging,
        EncryptionInput::Passphrase("old".into()),
        EncryptionInput::Passphrase("new".into()),
    )?;
    std::fs::rename(&staging, dir.join("db.emdb.enc.tmp")).expect("stage journal");
    std::fs::rename(dir.join("staging.emdb.meta"), dir.join("db.emdb.meta")).expect("stage meta");
    std::fs::write(dir.join("db.emdb.encadmin"), b"x").expect("marker");

    let db = Emdb::builder()
        .path(&path)
        .encryption_passphrase("new")
        .build()?;
    assert_eq!(db.get("k")?.as_deref(), Some(&b"v"[..]));
    assert!(!dir.join("db.emdb.encadmin").exists());
    assert!(!dir.join("db.emdb.enc.tmp").exists());
    Ok(())
}

#[test]
fn test_rotation_keeps_old_key_copy() -> Result<()> {
    let dir = Dir::new("encbak");
    let path = dir.join("db.emdb");
    seed(&path, 3)?;
    Emdb::rotate_encryption_key(&path, EncryptionInput::Key(OLD), EncryptionInput::Key(NEW))?;
    let bak = Emdb::builder()
        .path(dir.join("db.emdb.encbak"))
        .encryption_key(OLD)
        .build()?;
    assert_eq!(bak.len()?, 3);
    drop(bak);
    // A second rotation replaces the copy with the then-current file.
    Emdb::rotate_encryption_key(&path, EncryptionInput::Key(NEW), EncryptionInput::Key(OLD))?;
    let bak = Emdb::builder()
        .path(dir.join("db.emdb.encbak"))
        .encryption_key(NEW)
        .build()?;
    assert_eq!(bak.len()?, 3);
    Ok(())
}
