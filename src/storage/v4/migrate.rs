// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! In-place migration from the v0.6 page format (v3) to the v0.7 packed
//! slotted-leaf format (v4).
//!
//! Triggered automatically by [`Emdb::open`](crate::Emdb::open) when the
//! builder has [`prefer_v4`](crate::EmdbBuilder::prefer_v4) set and the
//! existing file's magic identifies it as a v3 page file.
//!
//! ## Strategy
//!
//! 1. Open the existing v3 page file with the v0.6 [`PageStorage`] reader.
//! 2. Walk every record into memory as `(key, value, expires_at)`.
//! 3. Allocate a temporary `<path>.v4tmp` and open a fresh v4
//!    [`Engine`](crate::storage::v4::engine::Engine).
//! 4. Insert each record into the new engine.
//! 5. Flush the engine, drop it, then atomically rename
//!    `<path>.v4tmp` → `<path>` (after moving the original to
//!    `<path>.v3bak`).
//! 6. On success, delete the backup. On failure, the backup stays so the
//!    caller can re-run the migration after fixing the underlying issue.
//!
//! v1 / v2 → v3 migration is handled by the existing
//! [`migrate_if_needed`](crate::storage::migrate::migrate_if_needed) which
//! we invoke first to chain v1 → v3 → v4 in a single open call.

use std::path::{Path, PathBuf};

use crate::page_cache::PageCache;
use crate::storage::page::{PageType, PAGE_SIZE};
use crate::storage::v4::engine::{Engine, EngineConfig, DEFAULT_NAMESPACE_ID};
use crate::storage::v4::store::V4_MAGIC;
use crate::storage::v4::wal::FlushPolicy;
use crate::{Error, Result};

/// v0.6 page-file magic bytes (`EMDBPAGE`).
const V3_MAGIC: [u8; 8] = *b"EMDBPAGE";

/// v0.4 / v0.3 / v0.1 log-file magic bytes (`EMDB\0\0\0\0`). Any of these
/// route through [`crate::storage::migrate::migrate_if_needed`] which
/// promotes them to v3 first.
const LEGACY_LOG_MAGIC: [u8; 8] = *b"EMDB\0\0\0\0";

/// Inspect the page file at `path` and migrate it to the v4 format if it
/// is currently in v3 (or older) format. New files (no file at all, or a
/// file already in v4 format) are left alone.
///
/// # Errors
///
/// Returns I/O errors from the read/write/rename steps, or
/// [`Error::MagicMismatch`] when the file exists but is not in any
/// recognised emdb format.
pub(crate) fn migrate_v3_to_v4_if_needed(path: &Path, flags: u32) -> Result<()> {
    // Empty/missing file: nothing to migrate. The engine will create a
    // fresh v4 file on its own.
    let metadata = match std::fs::metadata(path) {
        Ok(meta) => meta,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(Error::Io(err)),
    };
    if metadata.len() == 0 {
        return Ok(());
    }

    let bytes = std::fs::read(path)?;
    if bytes.len() < 8 {
        return Err(Error::MagicMismatch);
    }

    if bytes[0..8] == V4_MAGIC {
        // Already v4. Nothing to do.
        return Ok(());
    }

    if bytes[0..8] == LEGACY_LOG_MAGIC {
        // Run the v0.6 migrator first to promote v1/v2 → v3, then fall
        // through to the v3 → v4 step below.
        crate::storage::migrate::migrate_if_needed(path, flags)?;
    } else if bytes[0..8] != V3_MAGIC {
        return Err(Error::MagicMismatch);
    }

    // At this point the file is in v3 page format. Migrate to v4.
    migrate_v3_to_v4(path, flags)
}

fn migrate_v3_to_v4(path: &Path, flags: u32) -> Result<()> {
    use std::sync::Arc;

    // Build the temporary v4 path: <path>.v4tmp.
    let tmp_path = with_suffix(path, "v4tmp");
    let backup_path = with_suffix(path, "v3bak");

    // Clear stale temp/backup files from a prior failed migration so we
    // start from a known-clean state.
    let _removed_tmp = std::fs::remove_file(&tmp_path);
    let _removed_backup = std::fs::remove_file(&backup_path);

    // Phase 1: read every live record from the v3 file.
    let owned: Vec<V3Record> = read_all_v3_records(path, flags)?;

    // Phase 2: open a fresh v4 engine on the temporary path and insert
    // every record. Manual flush policy keeps writes inside the cache
    // until we explicitly flush below — fewer fsyncs during the bulk
    // load.
    let engine_config = EngineConfig {
        path: tmp_path.clone(),
        flags,
        page_io_mode: crate::storage::v4::io::IoMode::Buffered,
        wal_io_mode: crate::storage::v4::io::IoMode::Buffered,
        flush_policy: FlushPolicy::Manual,
        page_cache_pages: 0,
        value_cache_bytes: 0,
        bloom_initial_capacity: owned.len() as u64,
    };
    let engine = match Engine::open(engine_config) {
        Ok(e) => e,
        Err(err) => {
            let _cleanup = std::fs::remove_file(&tmp_path);
            return Err(err);
        }
    };
    for (key, value, expires_at) in &owned {
        let exp = expires_at.unwrap_or(0);
        if let Err(err) = engine.insert(DEFAULT_NAMESPACE_ID, key, value, exp) {
            drop(engine);
            let _cleanup = std::fs::remove_file(&tmp_path);
            return Err(err);
        }
    }
    if let Err(err) = engine.flush() {
        drop(engine);
        let _cleanup = std::fs::remove_file(&tmp_path);
        return Err(err);
    }
    drop(engine);

    // Phase 3: atomic rename. Move the original out of the way first so
    // the rename of tmp → path cannot encounter a "destination exists"
    // failure on platforms that do not allow `rename` to overwrite.
    if let Err(err) = std::fs::rename(path, &backup_path) {
        let _cleanup = std::fs::remove_file(&tmp_path);
        return Err(Error::Io(err));
    }
    if let Err(err) = std::fs::rename(&tmp_path, path) {
        // Best effort: restore the original.
        let _restore = std::fs::rename(&backup_path, path);
        return Err(Error::Io(err));
    }

    // Migration succeeded. Drop the backup and any stale .wal sidecar
    // from the v3 file (the new v4 file uses a different .wal name).
    let _removed_backup = std::fs::remove_file(&backup_path);
    let v3_wal = with_suffix(path, "wal");
    let _removed_v3_wal = std::fs::remove_file(&v3_wal);

    let _ = (
        Arc::<PageCache>::new(PageCache::new(0)),
        PageType::Header,
        PAGE_SIZE,
    );
    Ok(())
}

/// `(key, value, expires_at)` triple read out of a v3 file by the
/// migrator. Aliased to keep clippy::type_complexity happy without
/// inventing a public name.
type V3Record = (Vec<u8>, Vec<u8>, Option<u64>);

fn read_all_v3_records(path: &Path, flags: u32) -> Result<Vec<V3Record>> {
    use crate::storage::page_store::PageStorage;
    use crate::storage::Storage;

    // Open the v3 page file with a Manual policy so opening does not
    // immediately rewrite the file.
    let mut store = PageStorage::new(
        path.to_path_buf(),
        crate::storage::FlushPolicy::Manual,
        flags,
        #[cfg(feature = "mmap")]
        false,
    )?;

    // Walk every replayable op and collect Insert records. The v0.6
    // replay path serves up the post-replay state, so duplicates and
    // tombstones are already resolved.
    use crate::storage::Op;
    let mut staged: std::collections::HashMap<Vec<u8>, (Vec<u8>, Option<u64>)> =
        std::collections::HashMap::new();
    store.replay(&mut |op: Op| -> Result<()> {
        match op {
            Op::Insert {
                key,
                value,
                expires_at,
            } => {
                let _previous = staged.insert(key, (value, expires_at));
            }
            Op::Remove { key } => {
                let _removed = staged.remove(&key);
            }
            Op::Clear => {
                staged.clear();
            }
            Op::Checkpoint { .. } | Op::BatchBegin { .. } | Op::BatchEnd { .. } => {}
        }
        Ok(())
    })?;

    Ok(staged
        .into_iter()
        .map(|(k, (v, exp))| (k, v, exp))
        .collect())
}

fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut out = path.to_path_buf();
    let original_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("emdb");
    out.set_file_name(format!("{original_name}.{suffix}"));
    out
}

#[cfg(test)]
mod tests {
    use super::{migrate_v3_to_v4_if_needed, V3_MAGIC};
    use crate::Emdb;

    fn tmp_path(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0_u128, |d| d.as_nanos());
        p.push(format!("emdb-v4-migrate-{name}-{nanos}.emdb"));
        p
    }

    #[test]
    fn missing_file_is_a_noop() {
        let path = tmp_path("noop");
        let result = migrate_v3_to_v4_if_needed(path.as_path(), 0);
        assert!(result.is_ok());
    }

    #[test]
    fn already_v4_file_is_a_noop() {
        // Open via the v4 builder which writes the v4 magic, then run
        // the migrator: should detect v4 and short-circuit.
        let path = tmp_path("already-v4");
        {
            let db = match Emdb::builder().path(path.clone()).prefer_v4(true).build() {
                Ok(db) => db,
                Err(err) => panic!("v4 build should succeed: {err}"),
            };
            assert!(db.flush().is_ok());
        }

        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(err) => panic!("read should succeed: {err}"),
        };
        assert_eq!(&bytes[0..8], &super::V4_MAGIC);

        let result = migrate_v3_to_v4_if_needed(path.as_path(), 0);
        assert!(result.is_ok());

        // Cleanup
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{}.v4.wal", path.display()));
        let _ = std::fs::remove_file(format!("{}.lock", path.display()));
    }

    #[test]
    fn v3_file_migrates_records_visibly_through_v4_open() {
        // Seed a v0.6 (v3) database, drop it, then open via the v4
        // builder which auto-migrates. Records should be readable.
        let path = tmp_path("v3-to-v4");
        {
            let db = match Emdb::open(&path) {
                Ok(db) => db,
                Err(err) => panic!("v0.6 open should succeed: {err}"),
            };
            for i in 0_u32..32 {
                let key = format!("k{i:02}");
                let value = format!("v{i:02}");
                let _ = db.insert(key.as_bytes(), value.as_bytes());
            }
            assert!(db.flush().is_ok());
        }

        // Confirm we wrote a v3 file before migration.
        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(err) => panic!("read should succeed: {err}"),
        };
        assert_eq!(&bytes[0..8], &V3_MAGIC);

        // Open via v4 builder. The builder triggers
        // `migrate_v3_to_v4_if_needed`; on completion the engine opens the
        // freshly-written v4 file.
        let db = match Emdb::builder().path(path.clone()).prefer_v4(true).build() {
            Ok(db) => db,
            Err(err) => panic!("v4 build (with migration) should succeed: {err}"),
        };

        for i in 0_u32..32 {
            let key = format!("k{i:02}");
            let fetched = db.get(key.as_bytes());
            match fetched {
                Ok(Some(v)) => assert_eq!(v.as_slice(), format!("v{i:02}").as_bytes()),
                Ok(None) => panic!("key {key} missing after v3 → v4 migration"),
                Err(err) => panic!("get should succeed: {err}"),
            }
        }
        let len = match db.len() {
            Ok(n) => n,
            Err(err) => panic!("len should succeed: {err}"),
        };
        assert_eq!(len, 32);

        // The on-disk file is now v4.
        drop(db);
        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(err) => panic!("read should succeed: {err}"),
        };
        assert_eq!(&bytes[0..8], &super::V4_MAGIC);

        // Cleanup
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{}.v4.wal", path.display()));
        let _ = std::fs::remove_file(format!("{}.lock", path.display()));
        let _ = std::fs::remove_file(format!("{}.wal", path.display()));
        let _ = std::fs::remove_file(format!("{}.v3bak", path.display()));
    }
}
