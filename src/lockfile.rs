// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Cross-process advisory lockfile support.
//!
//! emdb is a single-writer engine. To stop two processes from
//! opening the same on-disk database at once, every persistent
//! [`crate::Emdb`] instance acquires an exclusive advisory lock on
//! a `<path>.lock` sidecar via [`fs4`].
//!
//! ## Lockfile vs. holder metadata
//!
//! Two files live alongside the database:
//!
//! - `<path>.lock` — the file that carries the OS advisory lock.
//!   Its contents are deliberately empty; no consumer reads the
//!   body. Held for the lifetime of the [`crate::Emdb`] handle and
//!   removed on graceful drop.
//! - `<path>.lock-meta` — a sibling plaintext file that identifies
//!   the holder. Written by the holder immediately after the lock
//!   is acquired. Free to read by other processes (no OS lock on
//!   it), so [`LockFile::read_holder`] works regardless of
//!   platform.
//!
//! The split exists because Windows uses mandatory file locks: a
//! handle holding `LockFileEx` blocks every other handle's reads
//! on the locked range, even from the same process. Putting the
//! metadata in a separate file makes "show me who has the lock"
//! a portable, deadlock-free read.
//!
//! ## `<path>.lock-meta` body format
//!
//! ```text
//!   emdb-lock v1
//!   pid=<u32>
//!   acquired_at=<unix-millis>
//!   crate_version=<semver>
//! ```
//!
//! Lines past the version header are `key=value` pairs separated
//! by `\n`. Unknown keys are ignored on read (forward-compat).
//! [`LockFile::read_holder`] parses this body for
//! [`crate::Emdb::break_lock`].
//!
//! ## Breaking stuck locks
//!
//! When a process dies with the lock held — kill -9, OOM, panic
//! escaping the runtime — the OS releases the advisory lock on
//! file descriptor close, but on some platforms this is not
//! immediate or visible to a different process trying to open the
//! same file. The body lets a human (or admin tool) confirm "yes,
//! that PID is gone" and call [`crate::Emdb::break_lock`] to
//! delete the sidecar, after which a fresh open succeeds.
//!
//! Calling `break_lock` on a path whose holder is still alive is
//! a footgun and PROHIBITED in REPS terms — but the library cannot
//! safely tell the holder is alive without portable PID-liveness
//! checks (which would require `libc`/`windows-sys` deps), so the
//! call is offered as an explicit admin operation. The caller is
//! responsible for confirming the holder is dead.

use std::fs::{remove_file, File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

use fs4::FileExt;

use crate::{Error, Result};

/// Lockfile body schema version. Bumped on incompatible body
/// format changes; readers tolerate unknown keys, so additive
/// changes do not require a bump.
const LOCKFILE_SCHEMA_VERSION: u32 = 1;

/// Magic prefix on the first line of every lockfile body. Lets
/// [`LockFile::read_holder`] reject a non-emdb `.lock` file
/// (e.g. one created by an unrelated tool that happened to pick
/// the same path).
const LOCKFILE_MAGIC: &str = "emdb-lock v";

/// Holder metadata read out of an existing lockfile body. Returned
/// by [`crate::Emdb::lock_holder`] for the
/// [`crate::Emdb::break_lock`] admin path.
///
/// Fields not present in the body decode as `None`. Forward-compat:
/// future schema versions may add fields; readers tolerate unknown
/// keys.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct LockHolder {
    /// Schema version of the lockfile body. Currently always `1`.
    pub schema_version: u32,
    /// Process ID of the holder when the lock was acquired.
    /// Cross-platform `process::id()` value; on Windows this is
    /// the DWORD process ID, on Unix the pid_t.
    pub pid: u32,
    /// Wall-clock time the lock was acquired, as Unix epoch
    /// milliseconds. Useful for "this lock has been held for X
    /// hours" diagnostics.
    pub acquired_at_unix_millis: u64,
    /// emdb crate version that wrote the lockfile body, e.g.
    /// `"0.8.5"`. Helps diagnose mixed-version deployments where a
    /// stale binary is holding a lock the new binary can't
    /// understand.
    pub crate_version: Option<String>,
}

/// Process-scoped advisory lockfile guard.
///
/// Lifecycle:
///
/// - [`Self::acquire`] opens the sidecar, takes an exclusive
///   advisory lock, and writes the holder metadata.
/// - The guard holds the file descriptor for the lifetime of the
///   handle. Dropping it releases the lock and removes the
///   sidecar.
///
/// Multi-process safety: `fs4`'s [`FileExt::try_lock_exclusive`]
/// uses `flock(LOCK_EX | LOCK_NB)` on Unix and `LockFileEx`
/// (LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY) on
/// Windows. Both error with `WouldBlock`/`PermissionDenied` if a
/// different process holds the lock; the engine maps that to
/// [`Error::LockBusy`].
#[derive(Debug)]
pub(crate) struct LockFile {
    file: File,
    lock_path: PathBuf,
    meta_path: PathBuf,
}

impl LockFile {
    /// Acquire an exclusive lockfile for a database path.
    ///
    /// On success a sibling `<db_path>.lock-meta` file is written
    /// with this process's identity (PID + timestamp + crate
    /// version). The lockfile body itself stays empty — only the
    /// OS advisory lock matters.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockBusy`] when another process holds the
    /// lock, or [`Error::LockfileError`] for other OS-level
    /// lock/open failures.
    pub(crate) fn acquire(db_path: &Path) -> Result<Self> {
        let lock_path = lock_path_for(db_path);
        let meta_path = meta_path_for(db_path);

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(Error::LockfileError)?;

        match file.try_lock_exclusive() {
            Ok(()) => {
                // Now that we hold the OS lock, write the holder
                // identity to the sibling meta file. Errors writing
                // metadata are best-effort — the lock itself is
                // held; metadata is diagnostic-only.
                let _ = write_holder_meta(&meta_path);
                Ok(Self {
                    file,
                    lock_path,
                    meta_path,
                })
            }
            Err(err)
                if err.kind() == ErrorKind::WouldBlock
                    || err.kind() == ErrorKind::PermissionDenied =>
            {
                Err(Error::LockBusy { path: lock_path })
            }
            Err(err) => Err(Error::LockfileError(err)),
        }
    }

    /// Read the holder metadata for `db_path`'s lock, if any.
    ///
    /// Reads `<db_path>.lock-meta` — a separate file from the
    /// OS-locked `<db_path>.lock`, so the read works regardless of
    /// whether the lock is currently held.
    ///
    /// Returns `Ok(None)` when no metadata file exists (database
    /// is unlocked, or the holder crashed before writing
    /// metadata). Returns `Ok(Some(_))` for a well-formed body.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockfileError`] for I/O failures, or
    /// [`Error::Corrupted`] for an unreadable body.
    pub(crate) fn read_holder(db_path: &Path) -> Result<Option<LockHolder>> {
        let meta_path = meta_path_for(db_path);
        let mut file = match OpenOptions::new().read(true).open(&meta_path) {
            Ok(f) => f,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(Error::LockfileError(err)),
        };
        let mut body = String::new();
        let _bytes_read = file
            .read_to_string(&mut body)
            .map_err(Error::LockfileError)?;
        if body.is_empty() {
            return Ok(None);
        }
        parse_holder_body(&body).map(Some)
    }

    /// Forcibly remove a stuck lockfile pair. Used by the
    /// [`crate::Emdb::break_lock`] admin entry point.
    ///
    /// Removes both `<db_path>.lock` and `<db_path>.lock-meta` if
    /// either exists. Treats "already gone" as success — the
    /// operation is idempotent.
    ///
    /// # Safety contract
    ///
    /// The caller MUST have confirmed that no live process is
    /// holding the lock. Calling this while a holder is still
    /// running corrupts the multi-process exclusion guarantee.
    /// emdb cannot perform this check portably; the responsibility
    /// is the caller's. Read [`LockFile::read_holder`] first and
    /// confirm the PID is gone.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockfileError`] for I/O failures other
    /// than "file does not exist".
    pub(crate) fn break_lock(db_path: &Path) -> Result<()> {
        let lock_path = lock_path_for(db_path);
        let meta_path = meta_path_for(db_path);
        let mut last_err: Option<std::io::Error> = None;
        for path in [lock_path, meta_path] {
            match remove_file(&path) {
                Ok(()) => {}
                Err(err) if err.kind() == ErrorKind::NotFound => {}
                Err(err) => last_err = Some(err),
            }
        }
        match last_err {
            None => Ok(()),
            Some(err) => Err(Error::LockfileError(err)),
        }
    }
}

impl Drop for LockFile {
    fn drop(&mut self) {
        let _unlock_result = fs4::FileExt::unlock(&self.file);
        let _remove_lock = remove_file(&self.lock_path);
        let _remove_meta = remove_file(&self.meta_path);
    }
}

/// Compute `<db_path>.lock` — the OS-locked sentinel file.
fn lock_path_for(db_path: &Path) -> PathBuf {
    let mut lock_path = db_path.as_os_str().to_owned();
    lock_path.push(".lock");
    PathBuf::from(lock_path)
}

/// Compute `<db_path>.lock-meta` — the holder-identity sidecar
/// kept separate from the OS-locked file so reads work cross-
/// platform regardless of whether the lock is held.
fn meta_path_for(db_path: &Path) -> PathBuf {
    let mut meta_path = db_path.as_os_str().to_owned();
    meta_path.push(".lock-meta");
    PathBuf::from(meta_path)
}

/// Write this process's holder metadata to `meta_path`,
/// truncating any prior content. Called immediately after the OS
/// lock is acquired, so we know we are the only writer.
fn write_holder_meta(meta_path: &Path) -> std::io::Result<()> {
    let pid = process::id();
    let acquired_at_unix_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0_u64, |d| d.as_millis().min(u64::MAX as u128) as u64);
    let crate_version = env!("CARGO_PKG_VERSION");

    let body = format!(
        "{LOCKFILE_MAGIC}{LOCKFILE_SCHEMA_VERSION}\n\
         pid={pid}\n\
         acquired_at={acquired_at_unix_millis}\n\
         crate_version={crate_version}\n"
    );

    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(meta_path)?;
    file.write_all(body.as_bytes())?;
    file.sync_data()?;
    Ok(())
}

/// Parse the lockfile body produced by [`write_holder_body`].
fn parse_holder_body(body: &str) -> Result<LockHolder> {
    let mut lines = body.lines();
    let header = lines.next().ok_or(Error::Corrupted {
        offset: 0,
        reason: "lockfile body is empty",
    })?;
    let schema_version_str = header
        .strip_prefix(LOCKFILE_MAGIC)
        .ok_or(Error::Corrupted {
            offset: 0,
            reason: "lockfile body has wrong magic",
        })?;
    let schema_version: u32 = schema_version_str.parse().map_err(|_| Error::Corrupted {
        offset: 0,
        reason: "lockfile body has unparseable schema version",
    })?;

    let mut pid: Option<u32> = None;
    let mut acquired_at_unix_millis: Option<u64> = None;
    let mut crate_version: Option<String> = None;

    for line in lines {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            // Unknown line shape; tolerate for forward-compat.
            continue;
        };
        match key.trim() {
            "pid" => pid = value.trim().parse().ok(),
            "acquired_at" => acquired_at_unix_millis = value.trim().parse().ok(),
            "crate_version" => crate_version = Some(value.trim().to_string()),
            _ => {} // unknown key — tolerate.
        }
    }

    let pid = pid.ok_or(Error::Corrupted {
        offset: 0,
        reason: "lockfile body missing pid",
    })?;
    let acquired_at_unix_millis = acquired_at_unix_millis.unwrap_or(0);

    Ok(LockHolder {
        schema_version,
        pid,
        acquired_at_unix_millis,
        crate_version,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        lock_path_for, meta_path_for, parse_holder_body, write_holder_meta, LockFile,
        LOCKFILE_MAGIC, LOCKFILE_SCHEMA_VERSION,
    };

    fn tmp_path(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0_u128, |d| d.as_nanos());
        let tid = std::thread::current().id();
        p.push(format!("emdb-lock-{name}-{nanos}-{tid:?}.emdb"));
        p
    }

    #[test]
    fn acquire_fresh_then_release_then_reacquire() {
        let db_path = tmp_path("acquire");
        let first = LockFile::acquire(db_path.as_path());
        assert!(first.is_ok());
        drop(first);

        let second = LockFile::acquire(db_path.as_path());
        assert!(second.is_ok());
        drop(second);
    }

    #[test]
    fn second_acquire_while_held_fails() {
        let db_path = tmp_path("contention");
        let first = LockFile::acquire(db_path.as_path());
        assert!(first.is_ok());

        let second = LockFile::acquire(db_path.as_path());
        assert!(second.is_err());

        drop(first);
    }

    #[test]
    fn acquire_writes_holder_metadata() {
        let db_path = tmp_path("metadata");
        let guard = LockFile::acquire(db_path.as_path()).expect("acquire");

        let holder = LockFile::read_holder(db_path.as_path())
            .expect("read holder")
            .expect("holder present while held");
        assert_eq!(holder.schema_version, LOCKFILE_SCHEMA_VERSION);
        assert_eq!(holder.pid, std::process::id());
        assert!(holder.acquired_at_unix_millis > 0);
        assert_eq!(
            holder.crate_version.as_deref(),
            Some(env!("CARGO_PKG_VERSION"))
        );

        drop(guard);
    }

    #[test]
    fn read_holder_on_missing_lockfile_returns_none() {
        let db_path = tmp_path("missing");
        let holder = LockFile::read_holder(db_path.as_path()).expect("read missing");
        assert!(holder.is_none());
    }

    #[test]
    fn break_lock_removes_lockfile_and_metadata() {
        let db_path = tmp_path("break");
        let lock_path = lock_path_for(db_path.as_path());
        let meta_path = meta_path_for(db_path.as_path());
        let _ = std::fs::remove_file(&lock_path);
        let _ = std::fs::remove_file(&meta_path);

        // Create both sidecars manually so we can break them
        // without a live holder racing us.
        std::fs::write(&lock_path, b"").expect("create lockfile");
        write_holder_meta(meta_path.as_path()).expect("write meta");
        assert!(lock_path.exists());
        assert!(meta_path.exists());

        LockFile::break_lock(db_path.as_path()).expect("break lock");
        assert!(!lock_path.exists());
        assert!(!meta_path.exists());

        // Idempotent — breaking an already-gone pair is fine.
        LockFile::break_lock(db_path.as_path()).expect("idempotent break");
    }

    #[test]
    fn parse_tolerates_unknown_keys() {
        let body = format!(
            "{LOCKFILE_MAGIC}1\n\
             pid=42\n\
             acquired_at=1234567890\n\
             crate_version=0.8.5\n\
             future_field=ignored\n"
        );
        let holder = parse_holder_body(&body).expect("parse");
        assert_eq!(holder.pid, 42);
        assert_eq!(holder.acquired_at_unix_millis, 1_234_567_890);
        assert_eq!(holder.crate_version.as_deref(), Some("0.8.5"));
    }

    #[test]
    fn parse_rejects_wrong_magic() {
        let body = "not-an-emdb-lock v1\npid=42\n";
        assert!(parse_holder_body(body).is_err());
    }

    #[test]
    fn parse_rejects_missing_pid() {
        let body = format!("{LOCKFILE_MAGIC}1\nacquired_at=42\n");
        assert!(parse_holder_body(&body).is_err());
    }
}
