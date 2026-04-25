// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Migration from the legacy v0.3/v0.4 log format to the v0.6 page format.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::storage::file::FileStorage;
use crate::storage::page_store::PageStorage;
use crate::storage::{FlushPolicy, Op, SnapshotEntry, Storage};
#[cfg(feature = "ttl")]
use crate::ttl::record_expires_at;
use crate::ttl::{record_new, record_value, Record};
use crate::{Error, Result};

const PAGE_MAGIC: [u8; 8] = *b"EMDBPAGE";
const LOG_MAGIC: [u8; 8] = *b"EMDB\0\0\0\0";

/// Migrate a legacy v1/v2 database file to the current page format when needed.
pub(crate) fn migrate_if_needed(path: &Path, flags: u32) -> Result<()> {
    let Ok(metadata) = std::fs::metadata(path) else {
        return Ok(());
    };
    if metadata.len() == 0 {
        return Ok(());
    }

    let bytes = std::fs::read(path)?;
    if bytes.len() < 8 {
        return Err(Error::MagicMismatch);
    }

    if bytes[0..8] == PAGE_MAGIC {
        return Ok(());
    }
    if bytes[0..8] != LOG_MAGIC {
        return Err(Error::MagicMismatch);
    }

    migrate_legacy_file(path, flags)
}

fn migrate_legacy_file(path: &Path, flags: u32) -> Result<()> {
    let mut legacy = FileStorage::new(path.to_path_buf(), FlushPolicy::Manual, flags)?;
    let mut storage = BTreeMap::<Vec<u8>, Record>::new();
    legacy.replay(&mut |op| {
        apply_replayed_op(&mut storage, op);
        Ok(())
    })?;
    let last_tx_id = legacy.last_tx_id();
    drop(legacy);

    let backup = backup_path(path);
    if backup.exists() {
        std::fs::remove_file(&backup)?;
    }
    std::fs::rename(path, &backup)?;

    let migration_result = (|| -> Result<()> {
        let mut page_store = PageStorage::new(
            path.to_path_buf(),
            FlushPolicy::Manual,
            flags,
            #[cfg(feature = "mmap")]
            false,
        )?;
        page_store.set_last_tx_id(last_tx_id)?;
        let owned = storage
            .iter()
            .map(|(key, record)| {
                #[cfg(feature = "ttl")]
                let expires_at = record_expires_at(record);
                #[cfg(not(feature = "ttl"))]
                let expires_at: Option<u64> = None;
                (key.clone(), record_value(record).to_vec(), expires_at)
            })
            .collect::<Vec<_>>();
        let snapshot = Box::new(owned.iter().map(|(key, value, expires_at)| SnapshotEntry {
            key,
            value,
            expires_at: *expires_at,
        }));
        page_store.compact(snapshot)
    })();

    match migration_result {
        Ok(()) => {
            std::fs::remove_file(backup)?;
            Ok(())
        }
        Err(err) => {
            if path.exists() {
                let _removed = std::fs::remove_file(path);
            }
            std::fs::rename(&backup, path)?;
            Err(err)
        }
    }
}

fn backup_path(path: &Path) -> PathBuf {
    let mut backup = path.to_path_buf();
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .map_or_else(|| String::from("emdb.bak"), |name| format!("{name}.bak"));
    backup.set_file_name(file_name);
    backup
}

fn apply_replayed_op(storage: &mut BTreeMap<Vec<u8>, Record>, op: Op) {
    match op {
        Op::Insert {
            key,
            value,
            expires_at,
        } => {
            let _previous = storage.insert(key, record_new(value, expires_at));
        }
        Op::Remove { key } => {
            let _previous = storage.remove(&key);
        }
        Op::Clear => {
            storage.clear();
        }
        Op::Checkpoint { .. } => {}
        Op::BatchBegin { .. } | Op::BatchEnd { .. } => {}
    }
}

#[cfg(test)]
mod tests {
    use super::migrate_if_needed;
    use crate::storage::codec::write_header;
    use crate::storage::file::FileStorage;
    use crate::storage::{build_flags, FlushPolicy, OpRef, Storage};
    use crate::Emdb;
    use crate::Error;

    fn tmp_path(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0_u128, |duration| duration.as_nanos());
        path.push(format!("emdb-migrate-{name}-{nanos}.emdb"));
        path
    }

    #[test]
    fn test_migrate_v2_log_file_to_page_format() {
        let path = tmp_path("v2");
        {
            let legacy = FileStorage::new(path.clone(), FlushPolicy::Manual, build_flags());
            assert!(legacy.is_ok());
            let mut legacy = match legacy {
                Ok(storage) => storage,
                Err(err) => panic!("legacy storage open should succeed: {err}"),
            };
            assert!(legacy
                .append(OpRef::Insert {
                    key: b"k",
                    value: b"v",
                    expires_at: None,
                })
                .is_ok());
            assert!(legacy.flush().is_ok());
        }

        let migrated = migrate_if_needed(&path, build_flags());
        assert!(migrated.is_ok());
        let reopened = Emdb::open(&path);
        assert!(
            matches!(reopened.and_then(|db| db.get("k")), Ok(Some(value)) if value == b"v".to_vec())
        );

        let _removed = std::fs::remove_file(&path);
        let _removed = std::fs::remove_file(path.with_file_name(
            format!("{}.wal", path.file_name().and_then(|name| name.to_str()).unwrap_or("emdb")),
        ));
    }

    #[test]
    fn test_migrate_rejects_unknown_magic() {
        let path = tmp_path("badmagic");
        assert!(std::fs::write(&path, [0xAB_u8; 64]).is_ok());
        let migrated = migrate_if_needed(&path, build_flags());
        assert!(matches!(migrated, Err(Error::MagicMismatch)));
        let _removed = std::fs::remove_file(path);
    }

    #[test]
    fn test_migrate_detects_v1_header() {
        let path = tmp_path("v1");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path);
        assert!(file.is_ok());
        let mut file = match file {
            Ok(file) => file,
            Err(err) => panic!("file open should succeed: {err}"),
        };
        let wrote = write_header(&mut file, 1, build_flags(), 0);
        assert!(wrote.is_ok());
        let migrated = migrate_if_needed(&path, build_flags());
        assert!(migrated.is_ok());
        let bytes = std::fs::read(&path);
        assert!(bytes.is_ok());
        let bytes = bytes.unwrap_or_default();
        assert_eq!(&bytes[0..8], &super::PAGE_MAGIC);

        let _removed = std::fs::remove_file(&path);
        let _removed = std::fs::remove_file(path.with_file_name(
            format!("{}.wal", path.file_name().and_then(|name| name.to_str()).unwrap_or("emdb")),
        ));
    }
}
