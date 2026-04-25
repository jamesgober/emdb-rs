use std::fs::OpenOptions;
use std::io::Write;

use emdb::{Emdb, Result};

const HEADER_LEN: usize = 64;
const LEGACY_MAGIC: [u8; 8] = *b"EMDB\0\0\0\0";
const OP_INSERT: u8 = 0;
const OP_BATCH_BEGIN: u8 = 4;
const OP_BATCH_END: u8 = 5;

fn tmp_path(name: &str) -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |d| d.as_nanos());
    path.push(format!("emdb-migration-{name}-{nanos}.emdb"));
    path
}

fn cleanup(path: &std::path::Path) {
    let wal = std::path::PathBuf::from(format!("{}.wal", path.display()));
    let bak = std::path::PathBuf::from(format!("{}.bak", path.display()));
    let lock = std::path::PathBuf::from(format!("{}.lock", path.display()));

    let _db_removed = std::fs::remove_file(path);
    let _wal_removed = std::fs::remove_file(wal);
    let _bak_removed = std::fs::remove_file(bak);
    let _lock_removed = std::fs::remove_file(lock);
}

fn write_legacy_header(path: &std::path::Path, format_ver: u32, last_tx_id: u64) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)?;

    let mut header = [0_u8; HEADER_LEN];
    header[0..8].copy_from_slice(&LEGACY_MAGIC);
    header[8..12].copy_from_slice(&format_ver.to_le_bytes());
    header[32..40].copy_from_slice(&last_tx_id.to_le_bytes());
    file.write_all(&header)?;
    file.flush()?;
    Ok(())
}

fn append_record(path: &std::path::Path, op_type: u8, payload_tail: &[u8]) -> Result<()> {
    let mut payload = Vec::new();
    payload.push(op_type);
    payload.extend_from_slice(&0_u64.to_le_bytes());
    payload.extend_from_slice(payload_tail);

    let crc = crc32fast::hash(&payload);

    let mut record = Vec::new();
    record.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    record.extend_from_slice(&payload);
    record.extend_from_slice(&crc.to_le_bytes());

    let mut file = OpenOptions::new().append(true).open(path)?;
    file.write_all(&record)?;
    file.flush()?;
    Ok(())
}

fn append_insert(path: &std::path::Path, key: &[u8], value: &[u8]) -> Result<()> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
    payload.extend_from_slice(key);
    payload.extend_from_slice(&(value.len() as u32).to_le_bytes());
    payload.extend_from_slice(value);

    #[cfg(feature = "ttl")]
    payload.extend_from_slice(&0_u64.to_le_bytes());

    append_record(path, OP_INSERT, &payload)
}

fn append_batch_begin(path: &std::path::Path, tx_id: u64, op_count: u32) -> Result<()> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&tx_id.to_le_bytes());
    payload.extend_from_slice(&op_count.to_le_bytes());
    append_record(path, OP_BATCH_BEGIN, &payload)
}

fn append_batch_end(path: &std::path::Path, tx_id: u64) -> Result<()> {
    append_record(path, OP_BATCH_END, &tx_id.to_le_bytes())
}

fn assert_migrated_content(db: &Emdb) -> Result<()> {
    assert_eq!(db.get("alpha")?, Some(b"one".to_vec()));
    assert_eq!(db.get("beta")?, Some(b"two".to_vec()));
    Ok(())
}

#[test]
fn opening_v1_file_auto_migrates_to_v3_without_leaving_backup() -> Result<()> {
    let path = tmp_path("v1");
    write_legacy_header(path.as_path(), 1, 0)?;
    append_insert(path.as_path(), b"alpha", b"one")?;
    append_insert(path.as_path(), b"beta", b"two")?;

    let db = Emdb::open(&path)?;
    assert_migrated_content(&db)?;
    assert!(db.path().is_some());
    assert!(path.exists());
    assert!(!std::path::PathBuf::from(format!("{}.bak", path.display())).exists());

    cleanup(path.as_path());
    Ok(())
}

#[test]
fn opening_v2_file_auto_migrates_to_v3_without_leaving_backup() -> Result<()> {
    let path = tmp_path("v2");
    write_legacy_header(path.as_path(), 2, 41)?;
    append_batch_begin(path.as_path(), 42, 2)?;
    append_insert(path.as_path(), b"alpha", b"one")?;
    append_insert(path.as_path(), b"beta", b"two")?;
    append_batch_end(path.as_path(), 42)?;

    let db = Emdb::open(&path)?;
    assert_migrated_content(&db)?;
    assert!(db.path().is_some());
    assert!(path.exists());
    assert!(!std::path::PathBuf::from(format!("{}.bak", path.display())).exists());

    cleanup(path.as_path());
    Ok(())
}

#[test]
fn explicit_migrate_on_current_file_is_a_noop_for_backup_files() -> Result<()> {
    let path = tmp_path("explicit-noop");
    let db = Emdb::open(&path)?;
    db.insert("alpha", "one")?;
    db.flush()?;
    db.migrate()?;

    assert!(path.exists());
    assert!(!std::path::PathBuf::from(format!("{}.bak", path.display())).exists());

    cleanup(path.as_path());
    Ok(())
}
