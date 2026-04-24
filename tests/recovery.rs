use emdb::{Emdb, Result};

fn tmp_path(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |d| d.as_nanos());
    p.push(format!("emdb-{name}-{nanos}.emdb"));
    p
}

fn find_last_record_start(bytes: &[u8]) -> Option<usize> {
    if bytes.len() < 64 {
        return None;
    }

    let mut cursor = 64_usize;
    let mut last = None;
    while cursor + 8 <= bytes.len() {
        let rec_len = u32::from_le_bytes([
            bytes[cursor],
            bytes[cursor + 1],
            bytes[cursor + 2],
            bytes[cursor + 3],
        ]) as usize;

        let Some(total) = 4_usize.checked_add(rec_len).and_then(|n| n.checked_add(4)) else {
            break;
        };

        if cursor + total > bytes.len() {
            break;
        }

        last = Some(cursor);
        cursor += total;
    }

    last
}

#[test]
fn truncation_recovery_keeps_previous_records() -> Result<()> {
    let path = tmp_path("recovery-truncate");

    {
        let db = Emdb::open(&path)?;
        db.insert("k1", "v1")?;
        db.insert("k2", "v2")?;
        db.flush()?;
    }

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)?;
    let len = file.metadata()?.len();
    assert!(len > 64);
    file.set_len(len - 5)?;
    drop(file);

    let db = Emdb::open(&path)?;
    assert_eq!(db.get("k1")?, Some(b"v1".to_vec()));

    let maybe_k2 = db.get("k2")?;
    if maybe_k2.is_some() {
        assert_eq!(maybe_k2, Some(b"v2".to_vec()));
    }

    assert!(std::fs::remove_file(path).is_ok());
    Ok(())
}

#[test]
fn crc_corruption_recovery_truncates_tail() -> Result<()> {
    let path = tmp_path("recovery-crc");

    {
        let db = Emdb::open(&path)?;
        db.insert("k1", "v1")?;
        db.insert("k2", "v2")?;
        db.flush()?;
    }

    let mut bytes = std::fs::read(&path)?;
    let last = find_last_record_start(&bytes).ok_or(emdb::Error::Corrupted {
        offset: 0,
        reason: "missing record",
    })?;

    let rec_len = u32::from_le_bytes([
        bytes[last],
        bytes[last + 1],
        bytes[last + 2],
        bytes[last + 3],
    ]) as usize;
    let crc_start = last + 4 + rec_len;
    bytes[crc_start] ^= 0x01;
    std::fs::write(&path, bytes)?;

    let db = Emdb::open(&path)?;
    assert_eq!(db.get("k1")?, Some(b"v1".to_vec()));
    assert_eq!(db.get("k2")?, None);

    assert!(std::fs::remove_file(path).is_ok());
    Ok(())
}

#[test]
fn replay_is_deterministic_across_reopens() -> Result<()> {
    let path = tmp_path("replay-deterministic");

    {
        let db = Emdb::open(&path)?;
        for i in 0_u32..128 {
            db.insert(format!("k{i}"), format!("v{i}"))?;
        }
        db.flush()?;
    }

    let a = Emdb::open(&path)?;
    let mut snapshot = Vec::new();
    for i in 0_u32..128 {
        let key = format!("k{i}");
        snapshot.push((key.clone(), a.get(&key)?));
    }
    let len_a = a.len()?;
    drop(a);

    let b = Emdb::open(&path)?;
    assert_eq!(len_a, b.len()?);
    for (key, expected) in snapshot {
        assert_eq!(expected, b.get(key)?);
    }

    assert!(std::fs::remove_file(path).is_ok());
    Ok(())
}
