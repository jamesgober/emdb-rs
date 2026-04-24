use std::collections::BTreeMap;

use emdb::{Emdb, Result};

fn tmp_path(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |d| d.as_nanos());
    p.push(format!("emdb-{name}-{nanos}.emdb"));
    p
}

#[test]
fn compact_shrinks_file_and_preserves_state() -> Result<()> {
    let path = tmp_path("compact");

    let mut expected = BTreeMap::new();
    {
        let db = Emdb::open(&path)?;

        for i in 0_u32..1_000 {
            let key = format!("k{i}");
            let value = format!("value-{i:04}-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
            db.insert(key.as_bytes(), value.as_bytes())?;
            expected.insert(key, value.into_bytes());
        }

        for i in 0_u32..800 {
            let key = format!("k{i}");
            let _removed = db.remove(&key)?;
            expected.remove(&key);
        }

        for i in 800_u32..900 {
            let key = format!("k{i}");
            let value = format!("updated-{i:04}-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
            db.insert(key.as_bytes(), value.as_bytes())?;
            expected.insert(key, value.into_bytes());
        }

        db.flush()?;

        let before = std::fs::metadata(&path)?.len();
        db.compact()?;
        db.flush()?;
        let after = std::fs::metadata(&path)?.len();

        assert!(after < before);
    }

    let db = Emdb::open(&path)?;
    assert_eq!(db.len()?, expected.len());
    for (k, v) in expected {
        assert_eq!(db.get(k.as_bytes())?, Some(v));
    }

    assert!(std::fs::remove_file(path).is_ok());
    Ok(())
}
