// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Cross-process advisory lockfile support.

use std::fs::{remove_file, File, OpenOptions};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use fs4::FileExt;

use crate::{Error, Result};

/// Process-scoped advisory lockfile guard.
#[derive(Debug)]
pub(crate) struct LockFile {
    file: File,
    path: PathBuf,
}

impl LockFile {
    /// Acquire an exclusive lockfile for a database path.
    ///
    /// The lockfile path is `<db_path>.lock`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockBusy`] when another process holds the lock,
    /// or [`Error::LockfileError`] for other OS-level lock/open failures.
    pub(crate) fn acquire(db_path: &Path) -> Result<Self> {
        let mut lock_path = db_path.as_os_str().to_owned();
        lock_path.push(".lock");
        let lock_path = PathBuf::from(lock_path);

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(Error::LockfileError)?;

        match file.try_lock_exclusive() {
            Ok(()) => Ok(Self {
                file,
                path: lock_path,
            }),
            Err(err)
                if err.kind() == ErrorKind::WouldBlock
                    || err.kind() == ErrorKind::PermissionDenied =>
            {
                Err(Error::LockBusy { path: lock_path })
            }
            Err(err) => Err(Error::LockfileError(err)),
        }
    }
}

impl Drop for LockFile {
    fn drop(&mut self) {
        let _unlock_result = fs4::FileExt::unlock(&self.file);
        let _remove_result = remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::LockFile;

    fn tmp_path(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0_u128, |d| d.as_nanos());
        p.push(format!("emdb-lock-{name}-{nanos}.emdb"));
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
}
