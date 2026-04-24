use emdb::{Emdb, Error, Result};

fn tmp_path(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |d| d.as_nanos());
    p.push(format!("emdb-{name}-{nanos}.emdb"));
    p
}

#[test]
fn magic_mismatch_is_reported() {
    let path = tmp_path("magic");
    let wrote = std::fs::write(&path, [0xAA_u8; 64]);
    assert!(wrote.is_ok());

    let opened = Emdb::open(&path);
    assert!(matches!(opened, Err(Error::MagicMismatch)));

    assert!(std::fs::remove_file(path).is_ok());
}

#[test]
fn version_mismatch_is_reported() -> Result<()> {
    let path = tmp_path("version");
    {
        let mut db = Emdb::open(&path)?;
        db.insert("k", "v")?;
        db.flush()?;
    }

    let mut bytes = std::fs::read(&path)?;
    bytes[8..12].copy_from_slice(&999_u32.to_le_bytes());
    std::fs::write(&path, bytes)?;

    let opened = Emdb::open(&path);
    assert!(matches!(opened, Err(Error::VersionMismatch { .. })));

    assert!(std::fs::remove_file(path).is_ok());
    Ok(())
}

#[test]
fn feature_mismatch_is_reported() -> Result<()> {
    let path = tmp_path("feature");
    {
        let mut db = Emdb::open(&path)?;
        db.insert("k", "v")?;
        db.flush()?;
    }

    let mut bytes = std::fs::read(&path)?;
    bytes[12..16].copy_from_slice(&(1_u32 << 31).to_le_bytes());
    std::fs::write(&path, bytes)?;

    let opened = Emdb::open(&path);
    assert!(matches!(opened, Err(Error::FeatureMismatch { .. })));

    assert!(std::fs::remove_file(path).is_ok());
    Ok(())
}
