// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Offline admin operations for at-rest encryption: enable, disable,
//! rotate. All three are file-level rewriters — the database file
//! must not currently be open by any other process. Each function:
//!
//! 1. Opens the source database with the supplied source key /
//!    passphrase (or none, if the source is unencrypted).
//! 2. Streams every record from every namespace into a freshly-created
//!    sibling file (`<path>.enc.tmp`) configured with the destination
//!    key / passphrase (or none).
//! 3. Closes both handles, atomically renames the original to
//!    `<path>.encbak` and the new file into the original's place. The
//!    backup is kept on success so callers can verify and clean up
//!    manually; if a failure interrupts the rewrite, the original
//!    file is unchanged and the partial `.enc.tmp` is removed.
//!
//! These primitives are exposed publicly as [`crate::Emdb::enable_encryption`],
//! [`crate::Emdb::disable_encryption`], and [`crate::Emdb::rotate_encryption_key`].

use std::path::{Path, PathBuf};

use crate::encryption::EncryptionInput;
use crate::{Emdb, EmdbBuilder, Error, Result};

/// One namespace's snapshot during a rewrite: name plus every live
/// `(key, value)` pair. Aliased to satisfy `clippy::type_complexity`
/// without inventing a public name (the admin module is the only
/// caller).
type NamespaceSnapshot = (String, Vec<(Vec<u8>, Vec<u8>)>);

/// Build an `Emdb` configured with the given encryption mode at the
/// given path. The output handle is path-backed and v0.7-only.
fn open_with_mode(path: &Path, mode: Option<&EncryptionInput>) -> Result<Emdb> {
    let mut builder = EmdbBuilder::new().path(path.to_path_buf());
    match mode {
        None => {}
        Some(EncryptionInput::Key(k)) => {
            builder = builder.encryption_key(*k);
        }
        Some(EncryptionInput::Passphrase(p)) => {
            builder = builder.encryption_passphrase(p.clone());
        }
    }
    builder.build()
}

/// Sibling-file path used as the rewrite scratch area.
fn temp_path_for(path: &Path) -> PathBuf {
    let mut out = path.to_path_buf();
    let original_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("emdb");
    out.set_file_name(format!("{original_name}.enc.tmp"));
    out
}

/// Sibling-file path used as the original's backup once the rewrite
/// succeeds.
fn backup_path_for(path: &Path) -> PathBuf {
    let mut out = path.to_path_buf();
    let original_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("emdb");
    out.set_file_name(format!("{original_name}.encbak"));
    out
}

/// Best-effort cleanup of the lockfile sidecar for a given path. The
/// mmap+append engine has no separate WAL or page-store sidecar; only
/// the `.lock` file from [`crate::lockfile::LockFile`] needs scrubbing
/// when an admin operation aborts.
fn remove_sidecars(path: &Path) {
    let display = path.display().to_string();
    let _ = std::fs::remove_file(format!("{display}.lock"));
}

/// Core rewrite. Source mode (`from`) describes how to *read* the
/// existing file; destination mode (`to`) describes how the rewritten
/// file should be encrypted. Either may be `None` (unencrypted) or
/// `Some(EncryptionInput::{Key,Passphrase})`.
///
/// Same source and destination → the rewrite still runs (records are
/// streamed through; this is essentially a defragmentation pass). The
/// admin functions below pre-validate the source/destination pair so
/// callers see a clean error rather than a redundant rewrite.
pub(crate) fn rewrite_database(
    path: &Path,
    from: Option<&EncryptionInput>,
    to: Option<&EncryptionInput>,
) -> Result<()> {
    if !path.exists() {
        return Err(Error::InvalidConfig(
            "encryption admin: source database file does not exist",
        ));
    }

    let tmp = temp_path_for(path);
    let bak = backup_path_for(path);
    // Clean any stale leftover from a prior failed run.
    let _ = std::fs::remove_file(&tmp);
    remove_sidecars(&tmp);

    // Phase 1: open the source database in read mode and snapshot
    // every namespace's records. `flush()` here is harmless (no
    // pending writes on a freshly-opened handle) but ensures any OS
    // page-cache buffering settles before we read.
    let src = open_with_mode(path, from)?;
    src.flush()?;

    let default_records: Vec<(Vec<u8>, Vec<u8>)> = src.iter()?.collect();
    let named_namespaces: Vec<String> = src
        .list_namespaces()?
        .into_iter()
        .filter(|n| !n.is_empty())
        .collect();
    let mut named_records: Vec<NamespaceSnapshot> = Vec::with_capacity(named_namespaces.len());
    for ns_name in &named_namespaces {
        let ns = src.namespace(ns_name)?;
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = ns.iter()?.collect();
        named_records.push((ns_name.clone(), pairs));
    }
    drop(src);

    // Phase 2: open destination at the temp path with the new mode and
    // stream every record across.
    let dst = match open_with_mode(&tmp, to) {
        Ok(d) => d,
        Err(err) => {
            let _ = std::fs::remove_file(&tmp);
            remove_sidecars(&tmp);
            return Err(err);
        }
    };

    let copy_result = (|| -> Result<()> {
        for (k, v) in default_records {
            dst.insert(k, v)?;
        }
        for (ns_name, pairs) in named_records {
            let ns = dst.namespace(&ns_name)?;
            for (k, v) in pairs {
                ns.insert(k, v)?;
            }
        }
        dst.flush()
    })();
    drop(dst);
    if let Err(err) = copy_result {
        let _ = std::fs::remove_file(&tmp);
        remove_sidecars(&tmp);
        return Err(err);
    }

    // Phase 3: atomic swap. Rename original to `<path>.encbak`, then
    // rename `<path>.enc.tmp` to original. The .encbak is left behind
    // for caller verification; failure to rename surfaces as Io and
    // leaves either the original (rename1 failed) or the new file
    // (rename2 failed but original is gone — caller can recover from
    // .encbak) on disk.
    if bak.exists() {
        let _ = std::fs::remove_file(&bak);
    }
    std::fs::rename(path, &bak).map_err(Error::from)?;
    if let Err(err) = std::fs::rename(&tmp, path) {
        // Best-effort: swap the backup back so the database is still
        // openable from the original path.
        let _ = std::fs::rename(&bak, path);
        let _ = std::fs::remove_file(&tmp);
        return Err(Error::from(err));
    }

    Ok(())
}

/// Convert an unencrypted database file to encrypted, in place.
///
/// The original file is renamed to `<path>.encbak` on success (kept
/// for caller verification; safe to delete once the new file is
/// validated). The new file uses the same path the caller passed in,
/// so existing handles / configs continue to work after reopening.
///
/// # Errors
///
/// - [`Error::InvalidConfig`] when the path does not exist or already
///   refers to an encrypted database.
/// - [`Error::EncryptionKeyMismatch`] / [`Error::Encryption`] from the
///   destination engine if AEAD setup fails.
/// - [`Error::Io`] from the rename / write path.
pub fn enable_encryption(path: impl AsRef<Path>, target: EncryptionInput) -> Result<()> {
    let path = path.as_ref();
    // Pre-flight: the source must be unencrypted.
    if let Some(header) = crate::storage::store::Store::peek_header_path(path)? {
        if header.flags & crate::storage::format::FLAG_ENCRYPTED != 0 {
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
/// Same atomic-rename + `.encbak` semantics as
/// [`enable_encryption`]. Use carefully — the resulting file is
/// readable by anyone with disk access.
///
/// # Errors
///
/// - [`Error::InvalidConfig`] when the path does not exist or the
///   file is not encrypted.
/// - [`Error::EncryptionKeyMismatch`] when `current` does not match
///   the file's existing key.
/// - [`Error::Io`] from the rename / write path.
pub fn disable_encryption(path: impl AsRef<Path>, current: EncryptionInput) -> Result<()> {
    let path = path.as_ref();
    if let Some(header) = crate::storage::store::Store::peek_header_path(path)? {
        if header.flags & crate::storage::format::FLAG_ENCRYPTED == 0 {
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
/// The original key (or passphrase) is supplied via `from`; the new
/// key (or passphrase) via `to`. Either side may be a raw key or a
/// passphrase; the on-disk encryption status stays "encrypted"
/// throughout (this is not a transition).
///
/// # Errors
///
/// - [`Error::InvalidConfig`] when the path does not exist or the
///   file is not encrypted.
/// - [`Error::EncryptionKeyMismatch`] when `from` does not match the
///   file's existing key.
/// - [`Error::Io`] from the rename / write path.
pub fn rotate_encryption_key(
    path: impl AsRef<Path>,
    from: EncryptionInput,
    to: EncryptionInput,
) -> Result<()> {
    let path = path.as_ref();
    if let Some(header) = crate::storage::store::Store::peek_header_path(path)? {
        if header.flags & crate::storage::format::FLAG_ENCRYPTED == 0 {
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
