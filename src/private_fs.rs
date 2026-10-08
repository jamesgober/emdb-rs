// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Owner-only creation of the files and directories emdb makes.
//!
//! A database file, its lock sidecars, and the temporary files written
//! by compaction and backup hold (or describe) user data, so on Unix
//! emdb creates them with mode `0o600` and creates its own directories
//! with mode `0o700`. The process umask can only narrow these modes
//! further. Files that already exist keep whatever permissions their
//! owner gave them.
//!
//! On Windows the modes do not apply: new files and directories
//! inherit the ACL of the directory they are created in, which for the
//! per-user data and temp directories already limits access to the
//! user. The helpers still provide the "create, never follow an
//! existing entry" behaviour there.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

/// Permission bits for files emdb creates on Unix.
#[cfg(unix)]
const PRIVATE_FILE_MODE: u32 = 0o600;

/// Permission bits for directories emdb creates on Unix.
#[cfg(unix)]
const PRIVATE_DIR_MODE: u32 = 0o700;

/// `OpenOptions` for a brand-new, owner-only, read-write file. Uses
/// `create_new` (`O_CREAT | O_EXCL`), which never follows a symbolic
/// link at `path` and fails if anything already exists there.
fn new_private_file_options() -> OpenOptions {
    let mut opts = OpenOptions::new();
    let _ = opts.read(true).write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let _ = opts.mode(PRIVATE_FILE_MODE);
    }
    opts
}

/// Create an empty file at `path` that only the current user can read
/// or write (mode `0o600` on Unix, before the umask).
///
/// Returns `Ok(true)` when the file was created and `Ok(false)` when
/// something already exists at `path`. An existing entry is left
/// untouched, including its permissions; that entry may be a symbolic
/// link, so a caller that needs a fresh file of its own (a temporary
/// file it just removed, for example) must treat `Ok(false)` as an
/// error rather than open the path afterwards.
///
/// Intended use: call it right before handing `path` to code that
/// opens with "create if missing" (fsys journals), so the file that
/// code opens already carries owner-only permissions.
///
/// # Errors
///
/// Any I/O error other than "already exists" from creating the file.
pub(crate) fn create_private_file(path: &Path) -> io::Result<bool> {
    match new_private_file_options().open(path) {
        Ok(_file) => Ok(true),
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => Ok(false),
        Err(err) => Err(err),
    }
}

/// Create a brand-new owner-only file at `path` and return the open
/// handle. Fails with `AlreadyExists` if anything (including a
/// symbolic link) is already at `path`.
pub(crate) fn create_new_private_file(path: &Path) -> io::Result<File> {
    new_private_file_options().open(path)
}

/// Open `path` read-write, creating it owner-only if it does not
/// exist. An existing file is opened as-is and is not truncated.
pub(crate) fn open_or_create_private_file(path: &Path) -> io::Result<File> {
    let mut opts = OpenOptions::new();
    let _ = opts.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let _ = opts.mode(PRIVATE_FILE_MODE);
    }
    opts.open(path)
}

/// `DirBuilder` that creates owner-only directories (mode `0o700` on
/// Unix).
fn private_dir_builder(recursive: bool) -> std::fs::DirBuilder {
    let mut builder = std::fs::DirBuilder::new();
    let _ = builder.recursive(recursive);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        let _ = builder.mode(PRIVATE_DIR_MODE);
    }
    builder
}

/// Create `path` and any missing parents. Directories created here get
/// mode `0o700` on Unix; directories that already exist are left alone.
pub(crate) fn create_private_dir_all(path: &Path) -> io::Result<()> {
    private_dir_builder(true).create(path)
}

/// Create the single directory `path` (its parent must exist) with
/// mode `0o700` on Unix. Fails with `AlreadyExists` if anything is
/// already at `path`, so the caller knows the directory is its own.
pub(crate) fn create_new_private_dir(path: &Path) -> io::Result<()> {
    private_dir_builder(false).create(path)
}

#[cfg(test)]
mod tests {
    use super::{
        create_new_private_dir, create_new_private_file, create_private_dir_all,
        create_private_file, open_or_create_private_file,
    };

    fn scratch(name: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0_u128, |d| d.as_nanos());
        std::env::temp_dir().join(format!(
            "emdb-private-fs-{name}-{}-{nanos}",
            std::process::id()
        ))
    }

    #[test]
    fn test_create_private_file_new_path_returns_true() {
        let path = scratch("new");
        assert!(create_private_file(&path).unwrap());
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode & 0o077, 0, "group/other bits set: {mode:o}");
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_create_private_file_existing_path_returns_false_and_keeps_content() {
        let path = scratch("existing");
        std::fs::write(&path, b"keep me").unwrap();
        assert!(!create_private_file(&path).unwrap());
        assert_eq!(std::fs::read(&path).unwrap(), b"keep me");
        assert!(create_new_private_file(&path).is_err());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_create_private_file_missing_parent_returns_err() {
        let path = scratch("missing-parent").join("child.emdb");
        assert!(create_private_file(&path).is_err());
    }

    #[test]
    fn test_open_or_create_private_file_does_not_truncate() {
        let path = scratch("open-or-create");
        std::fs::write(&path, b"abc").unwrap();
        drop(open_or_create_private_file(&path).unwrap());
        assert_eq!(std::fs::read(&path).unwrap(), b"abc");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_private_dirs_are_owner_only_and_new_dir_is_exclusive() {
        let root = scratch("dirs");
        let nested = root.join("a").join("b");
        create_private_dir_all(&nested).unwrap();
        create_private_dir_all(&nested).unwrap(); // idempotent
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for dir in [&root, &nested] {
                let mode = std::fs::metadata(dir).unwrap().permissions().mode() & 0o777;
                assert_eq!(mode & 0o077, 0, "{dir:?} mode {mode:o}");
            }
        }
        let single = root.join("single");
        create_new_private_dir(&single).unwrap();
        assert!(create_new_private_dir(&single).is_err());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[cfg(unix)]
    #[test]
    fn test_create_private_file_does_not_follow_symlink() {
        let target = scratch("symlink-target");
        let link = scratch("symlink-link");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        // Dangling link: O_EXCL refuses to create through it.
        assert!(!create_private_file(&link).unwrap());
        assert!(!target.exists());
        let _ = std::fs::remove_file(&link);
    }
}
