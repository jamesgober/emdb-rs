// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Offline admin operations for at-rest encryption: enable, disable,
//! rotate. All three are file-level rewriters. Each one:
//!
//! 1. Takes the database's lock and holds it until the operation has
//!    finished, so no other process can open the database mid-way.
//! 2. Reads every record of every namespace with the source key (or
//!    none) and writes it, with its expiry, into a fresh sibling file
//!    `<path>.enc.tmp` encrypted with the destination key (or none).
//!    Records that have already expired are not copied. A record that
//!    fails to decode or decrypt aborts the operation.
//! 3. Syncs the new file, then keeps the original as `<path>.encbak`
//!    (plus `<path>.encbak.meta`) and renames the new journal and its
//!    sidecar over `<path>` and `<path>.meta`. The path never goes
//!    missing: the original stays in place until the rename replaces
//!    it.
//!
//! The swap is bracketed by an intent marker, `<path>.encadmin`,
//! written (and synced) before the first rename and removed after the
//! last. If a crash interrupts the swap, the next open of the database
//! (or the next admin operation) finds the marker and completes the
//! swap from the synced `<path>.enc.tmp` files before doing anything
//! else.
//!
//! # The `.encbak` copy
//!
//! **`<path>.encbak` is the database as it was before the operation.**
//! After [`crate::Emdb::enable_encryption`] it is a complete
//! **plaintext** copy of the data you just encrypted; after
//! [`crate::Emdb::rotate_encryption_key`] it is a copy readable with
//! the **old** key. Delete `<path>.encbak` and `<path>.encbak.meta`
//! once you have verified the new database, or the protection the
//! operation was meant to add does not exist on that disk. An option
//! to skip keeping the copy is planned for 1.1.
//!
//! These primitives are exposed publicly as [`crate::Emdb::enable_encryption`],
//! [`crate::Emdb::disable_encryption`], and [`crate::Emdb::rotate_encryption_key`].
//! The crash-completion half ([`finish_interrupted_rewrite`]) is
//! compiled into every build, because any build may open a database an
//! interrupted operation left behind.

use std::path::{Path, PathBuf};

use crate::storage::meta::meta_path_for;
use crate::storage::store::{remove_if_exists, sync_dir};
use crate::{Error, Result};

#[cfg(feature = "encrypt")]
use crate::encryption::EncryptionInput;
#[cfg(feature = "encrypt")]
use crate::storage::{Engine, EngineConfig, DEFAULT_NAMESPACE_ID};

/// `<path><suffix>` next to `path`, for any file name.
fn sibling(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path
        .file_name()
        .map_or_else(|| std::ffi::OsString::from("emdb"), |n| n.to_os_string());
    name.push(suffix);
    path.with_file_name(name)
}

/// Sibling-file path used as the rewrite scratch area.
fn temp_path_for(path: &Path) -> PathBuf {
    sibling(path, ".enc.tmp")
}

/// Sibling-file path the original is kept under after a rewrite.
fn backup_path_for(path: &Path) -> PathBuf {
    sibling(path, ".encbak")
}

/// Intent marker present while the final renames are in progress.
fn marker_path_for(path: &Path) -> PathBuf {
    sibling(path, ".encadmin")
}

/// Complete an admin rewrite whose final renames a crash interrupted,
/// and refuse to open a database an older release's interrupted
/// rewrite left without data. Runs on every open, with the database
/// lock held, before the journal is read.
///
/// The marker is only ever written after the new journal and its
/// sidecar were synced, so finishing the renames is always safe.
///
/// # Errors
///
/// [`Error::Io`] when a rename or directory sync fails (the marker
/// stays, so the next open retries); [`Error::InvalidConfig`] when the
/// database file is missing or empty while a non-empty
/// `<path>.encbak` exists and no marker explains it, the state emdb
/// 1.0.2 and earlier could leave behind. Opening would create an
/// empty database next to the only copy of the data; rename
/// `<path>.encbak` and `<path>.encbak.meta` back to `<path>` and
/// `<path>.meta` instead.
pub(crate) fn finish_interrupted_rewrite(path: &Path) -> Result<()> {
    let marker = marker_path_for(path);
    if !marker.exists() {
        return refuse_orphaned_backup(path);
    }
    let tmp = temp_path_for(path);
    let tmp_meta = meta_path_for(&tmp);
    if tmp_meta.exists() {
        std::fs::rename(&tmp_meta, meta_path_for(path))?;
    }
    if tmp.exists() {
        std::fs::rename(&tmp, path)?;
    }
    sync_dir(path)?;
    remove_if_exists(&marker)?;
    sync_dir(path)
}

/// See [`finish_interrupted_rewrite`].
fn refuse_orphaned_backup(path: &Path) -> Result<()> {
    let data_len = match std::fs::metadata(path) {
        Ok(meta) => meta.len(),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => 0,
        Err(err) => return Err(Error::Io(err)),
    };
    if data_len > 0 {
        return Ok(());
    }
    match std::fs::metadata(backup_path_for(path)) {
        Ok(bak) if bak.len() > 0 => Err(Error::InvalidConfig(
            "database file is missing or empty but <path>.encbak holds data from an interrupted \
             encryption admin operation; rename <path>.encbak and <path>.encbak.meta back to \
             <path> and <path>.meta before opening",
        )),
        _ => Ok(()),
    }
}

/// Engine configuration for one side of a rewrite.
#[cfg(feature = "encrypt")]
fn engine_config(path: &Path, mode: Option<&EncryptionInput>) -> EngineConfig {
    let mut config = EngineConfig {
        path: path.to_path_buf(),
        ..EngineConfig::default()
    };
    match mode {
        None => {}
        Some(EncryptionInput::Key(key)) => {
            config.encryption_key = Some(crate::encryption::KeyBytes::from(*key));
        }
        Some(EncryptionInput::Passphrase(passphrase)) => {
            config.encryption_passphrase = Some(passphrase.clone());
        }
    }
    config
}

/// Copy every live, unexpired record of `path` (read with `from`) into
/// a new database at `tmp` (written with `to`), keeping each record's
/// expiry. Both engines are opened directly: the caller holds the
/// database lock, which a second `Emdb::open` would contend for.
#[cfg(feature = "encrypt")]
fn copy_records(
    path: &Path,
    tmp: &Path,
    from: Option<&EncryptionInput>,
    to: Option<&EncryptionInput>,
) -> Result<()> {
    let src = Engine::open(engine_config(path, from))?;
    let dst = Engine::open(engine_config(tmp, to))?;
    #[cfg(feature = "ttl")]
    let now = crate::ttl::now_unix_millis();
    for (ns_id, name) in src.list_namespaces()? {
        let dst_ns = if ns_id == DEFAULT_NAMESPACE_ID {
            DEFAULT_NAMESPACE_ID
        } else {
            dst.create_or_open_namespace(&name)?
        };
        src.for_each_record_batch(ns_id, |batch| {
            #[cfg(feature = "ttl")]
            let batch: Vec<_> = batch
                .into_iter()
                .filter(|(_, _, expires_at)| *expires_at == 0 || *expires_at > now)
                .collect();
            dst.insert_many(dst_ns, batch)
        })?;
    }
    dst.flush()
}

/// Make `to` a second name for `from` (a hard link), or a synced copy
/// where the file system has no hard links.
#[cfg(feature = "encrypt")]
fn link_or_copy(from: &Path, to: &Path) -> Result<()> {
    if std::fs::hard_link(from, to).is_ok() {
        return Ok(());
    }
    // SECURITY-MERGE: create_private_file(to) before the copy
    let _bytes = std::fs::copy(from, to)?;
    std::fs::File::open(to)?.sync_all()?;
    Ok(())
}

/// Create the intent marker and make it durable.
#[cfg(feature = "encrypt")]
fn write_marker(marker: &Path) -> Result<()> {
    use std::io::Write;
    // SECURITY-MERGE: open_or_create_private_file (owner-only marker)
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(marker)?;
    file.write_all(b"emdb encryption admin: rename <path>.enc.tmp over <path>\n")?;
    file.sync_all()?;
    sync_dir(marker)
}

/// Core rewrite. Source mode (`from`) describes how to read the
/// existing file; destination mode (`to`) describes how the rewritten
/// file is encrypted. Either may be `None` (unencrypted).
///
/// The admin functions below pre-validate the source/destination pair
/// so callers see a clean error rather than a redundant rewrite.
#[cfg(feature = "encrypt")]
pub(crate) fn rewrite_database(
    path: &Path,
    from: Option<&EncryptionInput>,
    to: Option<&EncryptionInput>,
) -> Result<()> {
    let _lock = crate::lockfile::LockFile::acquire(path)?;
    finish_interrupted_rewrite(path)?;
    if !path.exists() {
        return Err(Error::InvalidConfig(
            "encryption admin: source database file does not exist",
        ));
    }

    let tmp = temp_path_for(path);
    let tmp_meta = meta_path_for(&tmp);
    let bak = backup_path_for(path);
    let bak_meta = meta_path_for(&bak);
    let marker = marker_path_for(path);

    // Leftovers of an earlier failed run, including the lock sidecars
    // emdb 1.0.2 and earlier created for the temporary database. They
    // are recreated or unused below, so a failed removal of the lock
    // sidecars is harmless; the temporary itself must go.
    remove_if_exists(&tmp)?;
    remove_if_exists(&tmp_meta)?;
    let _ignored = remove_if_exists(&sibling(&tmp, ".lock"));
    let _ignored = remove_if_exists(&sibling(&tmp, ".lock-meta"));

    // SECURITY-MERGE: create_private_file(&tmp) before copy_records
    // (Engine::open creates `.enc.tmp` through the store otherwise).
    if let Err(err) = copy_records(path, &tmp, from, to) {
        let _ignored = remove_if_exists(&tmp);
        let _ignored = remove_if_exists(&tmp_meta);
        return Err(err);
    }
    sync_dir(path)?;

    // Keep the original under `.encbak`. The previous `.encbak` is
    // older than the database that was just read in full, so it is
    // replaced; the source being complete was proven by the copy.
    remove_if_exists(&bak)?;
    remove_if_exists(&bak_meta)?;
    link_or_copy(path, &bak)?;
    let path_meta = meta_path_for(path);
    if path_meta.exists() {
        link_or_copy(&path_meta, &bak_meta)?;
    }
    sync_dir(path)?;

    // From here on an interrupted swap is completed by the next open.
    write_marker(&marker)?;
    std::fs::rename(&tmp_meta, &path_meta)?;
    std::fs::rename(&tmp, path)?;
    sync_dir(path)?;
    remove_if_exists(&marker)?;
    sync_dir(path)
}

/// Convert an unencrypted database file to encrypted, in place.
///
/// # Warning: the plaintext copy
///
/// On success the original file is kept as `<path>.encbak` (with
/// `<path>.encbak.meta`), and **that copy is not encrypted**: it holds
/// every record in plaintext. Delete both files once the new database
/// is verified. See the module documentation for the crash-safety
/// sequence.
///
/// The new file uses the same path the caller passed in, so existing
/// handles / configs continue to work after reopening. The database
/// must not be open in any process (the operation takes its lock).
/// Record expiry is preserved; already-expired records are dropped.
///
/// # Errors
///
/// - [`Error::InvalidConfig`] when the path does not exist or already
///   refers to an encrypted database.
/// - [`Error::LockBusy`] when the database is open elsewhere.
/// - [`Error::EncryptionKeyMismatch`] / [`Error::Encryption`] from the
///   destination engine if AEAD setup fails.
/// - [`Error::Corrupted`] when a record cannot be decoded.
/// - [`Error::Io`] from the rename / write path.
#[cfg(feature = "encrypt")]
pub fn enable_encryption(path: impl AsRef<Path>, target: EncryptionInput) -> Result<()> {
    let path = path.as_ref();
    // Pre-flight: the source must be unencrypted.
    if let Some(header) = crate::storage::meta::read(path)? {
        if header.flags & crate::storage::meta::FLAG_ENCRYPTED != 0 {
            return Err(Error::InvalidConfig(
                "enable_encryption: file is already encrypted",
            ));
        }
    } else {
        return Err(Error::InvalidConfig(
            "enable_encryption: file does not exist",
        ));
    }
    rewrite_database(path, None, Some(&target))
}

/// Convert an encrypted database file to unencrypted, in place.
///
/// Same lock, crash-safety and `.encbak` semantics as
/// [`enable_encryption`]; here `<path>.encbak` is the encrypted
/// original. Use carefully: the resulting file is readable by anyone
/// with disk access.
///
/// # Errors
///
/// - [`Error::InvalidConfig`] when the path does not exist or the
///   file is not encrypted.
/// - [`Error::LockBusy`] when the database is open elsewhere.
/// - [`Error::EncryptionKeyMismatch`] when `current` does not match
///   the file's existing key.
/// - [`Error::Corrupted`] when a record cannot be decoded.
/// - [`Error::Io`] from the rename / write path.
#[cfg(feature = "encrypt")]
pub fn disable_encryption(path: impl AsRef<Path>, current: EncryptionInput) -> Result<()> {
    let path = path.as_ref();
    if let Some(header) = crate::storage::meta::read(path)? {
        if header.flags & crate::storage::meta::FLAG_ENCRYPTED == 0 {
            return Err(Error::InvalidConfig(
                "disable_encryption: file is already unencrypted",
            ));
        }
    } else {
        return Err(Error::InvalidConfig(
            "disable_encryption: file does not exist",
        ));
    }
    rewrite_database(path, Some(&current), None)
}

/// Re-encrypt every record under a new key.
///
/// # Warning: the old-key copy
///
/// On success the original file is kept as `<path>.encbak` (with
/// `<path>.encbak.meta`), **still readable with the old key**. If the
/// rotation is meant to retire a compromised key, delete both files
/// once the new database is verified.
///
/// The original key (or passphrase) is supplied via `from`; the new
/// key (or passphrase) via `to`. Either side may be a raw key or a
/// passphrase. Same lock and crash-safety semantics as
/// [`enable_encryption`].
///
/// # Errors
///
/// - [`Error::InvalidConfig`] when the path does not exist or the
///   file is not encrypted.
/// - [`Error::LockBusy`] when the database is open elsewhere.
/// - [`Error::EncryptionKeyMismatch`] when `from` does not match the
///   file's existing key.
/// - [`Error::Corrupted`] when a record cannot be decoded.
/// - [`Error::Io`] from the rename / write path.
#[cfg(feature = "encrypt")]
pub fn rotate_encryption_key(
    path: impl AsRef<Path>,
    from: EncryptionInput,
    to: EncryptionInput,
) -> Result<()> {
    let path = path.as_ref();
    if let Some(header) = crate::storage::meta::read(path)? {
        if header.flags & crate::storage::meta::FLAG_ENCRYPTED == 0 {
            return Err(Error::InvalidConfig(
                "rotate_encryption_key: file is not encrypted; use enable_encryption \
                 to add encryption to an unencrypted database",
            ));
        }
    } else {
        return Err(Error::InvalidConfig(
            "rotate_encryption_key: file does not exist",
        ));
    }
    rewrite_database(path, Some(&from), Some(&to))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_dir(label: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0_u128, |d| d.as_nanos());
        let mut p = std::env::temp_dir();
        p.push(format!("emdb-admin-{label}-{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&p).expect("mkdir");
        p
    }

    #[test]
    fn test_finish_interrupted_rewrite_completes_pending_swap() {
        let dir = tmp_dir("finish");
        let path = dir.join("db");
        std::fs::write(&path, b"old").expect("write");
        std::fs::write(meta_path_for(&path), b"old-meta").expect("write");
        std::fs::write(temp_path_for(&path), b"new").expect("write");
        std::fs::write(meta_path_for(&temp_path_for(&path)), b"new-meta").expect("write");
        std::fs::write(marker_path_for(&path), b"x").expect("write");

        finish_interrupted_rewrite(&path).expect("finish");

        assert_eq!(std::fs::read(&path).expect("read"), b"new");
        assert_eq!(
            std::fs::read(meta_path_for(&path)).expect("read"),
            b"new-meta"
        );
        assert!(!marker_path_for(&path).exists());
        assert!(!temp_path_for(&path).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_finish_interrupted_rewrite_without_marker_is_noop() {
        let dir = tmp_dir("noop");
        let path = dir.join("db");
        std::fs::write(&path, b"data").expect("write");
        std::fs::write(temp_path_for(&path), b"stale").expect("write");
        finish_interrupted_rewrite(&path).expect("noop");
        assert_eq!(std::fs::read(&path).expect("read"), b"data");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_orphaned_encbak_with_missing_database_refused() {
        let dir = tmp_dir("orphan");
        let path = dir.join("db");
        std::fs::write(backup_path_for(&path), b"precious").expect("write");
        assert!(matches!(
            finish_interrupted_rewrite(&path),
            Err(Error::InvalidConfig(_))
        ));
        std::fs::write(&path, b"").expect("write");
        assert!(finish_interrupted_rewrite(&path).is_err());
        std::fs::write(&path, b"data").expect("write");
        assert!(finish_interrupted_rewrite(&path).is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
