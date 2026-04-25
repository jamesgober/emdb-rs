use emdb::{Emdb, FlushPolicy, Result};

const RECORD_COUNT: usize = 100_000;

fn tmp_path(name: &str) -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |d| d.as_nanos());
    path.push(format!("emdb-page-format-{name}-{nanos}.emdb"));
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

fn insert_records(db: &Emdb) -> Result<()> {
    for index in 0..RECORD_COUNT {
        db.insert(
            format!("key-{index:06}"),
            format!("value-{index:06}-payload"),
        )?;
    }
    db.flush()
}

fn verify_records(db: &Emdb) -> Result<()> {
    for index in 0..RECORD_COUNT {
        assert_eq!(
            db.get(format!("key-{index:06}"))?,
            Some(format!("value-{index:06}-payload").into_bytes())
        );
    }
    Ok(())
}

fn open_buffered(path: &std::path::Path) -> Result<Emdb> {
    Emdb::builder()
        .path(path.to_path_buf())
        .flush_policy(FlushPolicy::Manual)
        .build()
}

#[cfg(feature = "mmap")]
fn open_mmap(path: &std::path::Path) -> Result<Emdb> {
    Emdb::builder()
        .path(path.to_path_buf())
        .flush_policy(FlushPolicy::Manual)
        .use_mmap(true)
        .build()
}

#[test]
fn page_format_round_trip_preserves_100k_records_with_buffered_reads() -> Result<()> {
    let path = tmp_path("buffered");

    {
        let db = open_buffered(path.as_path())?;
        insert_records(&db)?;
    }

    let reopened = open_buffered(path.as_path())?;
    verify_records(&reopened)?;

    cleanup(path.as_path());
    Ok(())
}

#[cfg(feature = "mmap")]
#[test]
fn page_format_round_trip_preserves_100k_records_with_mmap_reads() -> Result<()> {
    let path = tmp_path("mmap");

    {
        let db = open_buffered(path.as_path())?;
        insert_records(&db)?;
    }

    let reopened = open_mmap(path.as_path())?;
    verify_records(&reopened)?;

    cleanup(path.as_path());
    Ok(())
}
