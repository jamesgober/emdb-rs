use std::collections::BTreeMap;

use emdb::{Emdb, Error, Result};

const HEADER_LEN: usize = 64;
const OP_INSERT: u8 = 0;
const OP_BATCH_BEGIN: u8 = 4;
const OP_BATCH_END: u8 = 5;

fn tmp_path(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |d| d.as_nanos());
    p.push(format!("emdb-tx-{name}-{nanos}.emdb"));
    p
}

fn append_record(path: &std::path::Path, op_type: u8, payload_tail: &[u8]) -> Result<()> {
    let mut payload = Vec::new();
    payload.push(op_type);
    payload.extend_from_slice(&0_u64.to_le_bytes());
    payload.extend_from_slice(payload_tail);

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&payload);
    let crc = hasher.finalize();

    let mut record = Vec::new();
    record.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    record.extend_from_slice(&payload);
    record.extend_from_slice(&crc.to_le_bytes());

    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(path)?;
    std::io::Write::write_all(&mut file, &record)?;
    std::io::Write::flush(&mut file)?;
    Ok(())
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

fn append_insert(path: &std::path::Path, key: &[u8], value: &[u8]) -> Result<()> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
    payload.extend_from_slice(key);
    payload.extend_from_slice(&(value.len() as u32).to_le_bytes());
    payload.extend_from_slice(value);

    #[cfg(feature = "ttl")]
    {
        payload.extend_from_slice(&0_u64.to_le_bytes());
    }

    append_record(path, OP_INSERT, &payload)
}

fn read_header_last_tx_id(path: &std::path::Path) -> Result<u64> {
    let bytes = std::fs::read(path)?;
    let mut arr = [0_u8; 8];
    let offset = if bytes.get(0..8) == Some(b"EMDBPAGE") {
        28
    } else {
        32
    };
    arr.copy_from_slice(&bytes[offset..offset + 8]);
    Ok(u64::from_le_bytes(arr))
}

#[test]
fn basic_commit_inserts_all_keys() -> Result<()> {
    let path = tmp_path("basic-commit");
    let db = Emdb::open(&path)?;

    db.transaction(|tx| {
        tx.insert("a", "1")?;
        tx.insert("b", "2")?;
        tx.insert("c", "3")?;
        Ok(())
    })?;

    assert_eq!(db.get("a")?, Some(b"1".to_vec()));
    assert_eq!(db.get("b")?, Some(b"2".to_vec()));
    assert_eq!(db.get("c")?, Some(b"3".to_vec()));

    assert!(std::fs::remove_file(path).is_ok());
    Ok(())
}

#[test]
fn basic_rollback_discards_all_keys() -> Result<()> {
    let path = tmp_path("basic-rollback");
    let db = Emdb::open(&path)?;

    let result = db.transaction::<_, ()>(|tx| {
        tx.insert("a", "1")?;
        tx.insert("b", "2")?;
        tx.insert("c", "3")?;
        Err(Error::TransactionAborted("rollback"))
    });

    assert!(result.is_err());
    assert_eq!(db.get("a")?, None);
    assert_eq!(db.get("b")?, None);
    assert_eq!(db.get("c")?, None);

    assert!(std::fs::remove_file(path).is_ok());
    Ok(())
}

#[test]
fn read_your_writes_inside_transaction() -> Result<()> {
    let path = tmp_path("read-your-writes");
    let db = Emdb::open(&path)?;

    db.transaction(|tx| {
        tx.insert("session", "token")?;
        assert_eq!(tx.get("session")?, Some(b"token".to_vec()));
        Ok(())
    })?;

    assert!(std::fs::remove_file(path).is_ok());
    Ok(())
}

#[test]
fn remove_then_get_inside_transaction_returns_none() -> Result<()> {
    let path = tmp_path("remove-then-get");
    let db = Emdb::open(&path)?;
    db.insert("k", "v")?;

    db.transaction(|tx| {
        let removed = tx.remove("k")?;
        assert_eq!(removed, Some(b"v".to_vec()));
        assert_eq!(tx.get("k")?, None);
        Ok(())
    })?;

    assert_eq!(db.get("k")?, None);
    assert!(std::fs::remove_file(path).is_ok());
    Ok(())
}

#[test]
fn crash_before_batch_end_discards_batch_on_reopen() -> Result<()> {
    let path = tmp_path("crash-before-end");

    {
        let db = Emdb::open(&path)?;
        db.insert("stable", "ok")?;
        db.flush()?;
    }

    append_batch_begin(path.as_path(), 101, 2)?;
    append_insert(path.as_path(), b"x1", b"v1")?;
    append_insert(path.as_path(), b"x2", b"v2")?;

    let db = Emdb::open(&path)?;
    assert_eq!(db.get("stable")?, Some(b"ok".to_vec()));
    assert_eq!(db.get("x1")?, None);
    assert_eq!(db.get("x2")?, None);

    assert!(std::fs::remove_file(path).is_ok());
    Ok(())
}

#[test]
fn crash_after_batch_end_keeps_batch_on_reopen() -> Result<()> {
    let path = tmp_path("crash-after-end");

    {
        let db = Emdb::open(&path)?;
        db.insert("stable", "ok")?;
        db.flush()?;
    }

    append_batch_begin(path.as_path(), 102, 2)?;
    append_insert(path.as_path(), b"x1", b"v1")?;
    append_insert(path.as_path(), b"x2", b"v2")?;
    append_batch_end(path.as_path(), 102)?;

    let db = Emdb::open(&path)?;
    assert_eq!(db.get("stable")?, Some(b"ok".to_vec()));
    assert_eq!(db.get("x1")?, Some(b"v1".to_vec()));
    assert_eq!(db.get("x2")?, Some(b"v2".to_vec()));

    assert!(std::fs::remove_file(path).is_ok());
    Ok(())
}

#[test]
fn tx_id_monotonicity_holds_across_commits() -> Result<()> {
    let path = tmp_path("txid-monotonic");
    let db = Emdb::open(&path)?;

    let mut prev = 0_u64;
    for i in 0_u32..100 {
        db.transaction(|tx| {
            tx.insert(format!("k{i}"), format!("v{i}"))?;
            Ok(())
        })?;

        let current = read_header_last_tx_id(path.as_path())?;
        assert!(current > prev);
        prev = current;
    }

    assert!(std::fs::remove_file(path).is_ok());
    Ok(())
}

#[test]
fn empty_transaction_commits() -> Result<()> {
    let path = tmp_path("tx-empty");
    let db = Emdb::open(&path)?;

    db.transaction(|_tx| Ok(()))?;
    assert_eq!(db.len()?, 0);

    assert!(std::fs::remove_file(path).is_ok());
    Ok(())
}

#[test]
fn tx_id_persists_across_reopens() -> Result<()> {
    let path = tmp_path("txid-persist");

    let first = {
        let db = Emdb::open(&path)?;
        db.transaction(|tx| {
            tx.insert("a", "1")?;
            Ok(())
        })?;
        read_header_last_tx_id(path.as_path())?
    };

    let second = {
        let db = Emdb::open(&path)?;
        db.transaction(|tx| {
            tx.insert("b", "2")?;
            Ok(())
        })?;
        read_header_last_tx_id(path.as_path())?
    };

    assert!(second > first);
    assert!(std::fs::remove_file(path).is_ok());
    Ok(())
}

#[test]
fn v1_header_files_remain_readable_with_v2_batch_records() -> Result<()> {
    let path = tmp_path("mixed-v1-v2");

    let mut header = [0_u8; HEADER_LEN];
    header[0..8].copy_from_slice(b"EMDB\0\0\0\0");
    header[8..12].copy_from_slice(&1_u32.to_le_bytes());
    std::fs::write(&path, header)?;
    append_insert(path.as_path(), b"base", b"value")?;
    append_batch_begin(path.as_path(), 1, 1)?;
    append_insert(path.as_path(), b"tx", b"value")?;
    append_batch_end(path.as_path(), 1)?;

    let db = Emdb::open(&path)?;
    assert_eq!(db.get("base")?, Some(b"value".to_vec()));
    assert_eq!(db.get("tx")?, Some(b"value".to_vec()));

    assert!(std::fs::remove_file(path).is_ok());
    Ok(())
}

#[test]
fn deterministic_operation_sequence_matches_oracle() -> Result<()> {
    let path = tmp_path("oracle");
    let db = Emdb::open(&path)?;
    let mut oracle = BTreeMap::<Vec<u8>, Vec<u8>>::new();

    let mut seed = 0x1234_5678_9ABC_DEF0_u64;
    for step in 0_u32..512 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let op = (seed >> 33) % 4;
        let key = format!("k{}", (seed % 32) as u8).into_bytes();
        let val = format!("v{step}").into_bytes();

        if op == 0 {
            db.insert(key.clone(), val.clone())?;
            oracle.insert(key, val);
        } else if op == 1 {
            let _removed = db.remove(key.clone())?;
            oracle.remove(&key);
        } else {
            db.transaction(|tx| {
                if op == 2 {
                    tx.insert(key.clone(), val.clone())?;
                } else {
                    let _removed = tx.remove(key.clone())?;
                }
                Ok(())
            })?;

            if op == 2 {
                oracle.insert(key, val);
            } else {
                oracle.remove(&key);
            }
        }
    }

    for (k, v) in &oracle {
        assert_eq!(db.get(k)?, Some(v.clone()));
    }
    assert_eq!(db.len()?, oracle.len());

    assert!(std::fs::remove_file(path).is_ok());
    Ok(())
}

#[test]
fn crash_points_inside_transaction_discard_or_keep_correctly() -> Result<()> {
    let path = tmp_path("crash-points");

    {
        let db = Emdb::open(&path)?;
        db.flush()?;
    }

    // Crash at begin: batch begin only should be discarded.
    append_batch_begin(path.as_path(), 201, 1)?;
    {
        let reopened = Emdb::open(&path)?;
        assert!(reopened.is_empty()?);
    }

    // Crash mid-batch: begin + one op, no end should be discarded.
    append_batch_begin(path.as_path(), 202, 1)?;
    append_insert(path.as_path(), b"mid", b"value")?;
    {
        let reopened = Emdb::open(&path)?;
        assert_eq!(reopened.get("mid")?, None);
    }

    // Just-before-end crash should still discard.
    append_batch_begin(path.as_path(), 203, 1)?;
    append_insert(path.as_path(), b"almost", b"value")?;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)?;
    let len = file.metadata()?.len();
    file.set_len(len - 1)?;
    drop(file);
    {
        let reopened = Emdb::open(&path)?;
        assert_eq!(reopened.get("almost")?, None);
    }

    assert!(std::fs::remove_file(path).is_ok());
    Ok(())
}

#[test]
fn transaction_file_keeps_header_length_invariant() -> Result<()> {
    let path = tmp_path("header-len");
    let db = Emdb::open(&path)?;

    db.transaction(|tx| {
        tx.insert("k", "v")?;
        Ok(())
    })?;

    let bytes = std::fs::read(&path)?;
    assert!(bytes.len() >= HEADER_LEN);

    assert!(std::fs::remove_file(path).is_ok());
    Ok(())
}
