// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Cross-platform Direct I/O for the v0.7 page file.
//!
//! The page-file file handle is a plain `std::fs::File` for both buffered
//! and Direct I/O modes — the seek/read/write/sync code path is identical.
//! What differs is the open flags. Direct I/O bypasses the OS page cache
//! so the engine's own [`crate::page_cache::PageCache`] is the only RAM
//! cache for page contents, which gives:
//!
//! - **Predictable p99 latency.** No surprise eviction by unrelated
//!   processes; no double-caching with the OS page cache.
//! - **No cache pollution.** A backup tool reading the file does not
//!   evict hot pages from anyone else's working set.
//! - **Truly synchronous writes.** On Windows, `FILE_FLAG_WRITE_THROUGH`
//!   makes every `write_all` durable without a separate `FlushFileBuffers`
//!   call.
//!
//! The trade-off is alignment. Direct I/O requires the buffer address,
//! buffer length, and file offset all to be multiples of the filesystem
//! block size (typically 512 B or 4 KB). emdb's [`crate::storage::page::Page`]
//! is `#[repr(C, align(4096))]` and operates exclusively on whole pages
//! at page-aligned offsets, so every requirement is met statically.
//!
//! ## Modes
//!
//! - [`IoMode::Buffered`] — `std::fs::File` with default flags. Always
//!   works, leans on the OS page cache. The default.
//! - [`IoMode::Direct`] — opens with `O_DIRECT` (Linux), `F_NOCACHE`
//!   (macOS), or `FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH`
//!   (Windows). Fails loudly if the platform or filesystem does not
//!   support it (REPS forbids silent degradation).

use std::fs::{File, OpenOptions};
use std::path::Path;

use crate::{Error, Result};

/// Selects how the v0.7 page file is opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum IoMode {
    /// Use the OS page cache. The default; works on every platform and
    /// every filesystem.
    #[default]
    Buffered,
    /// Bypass the OS page cache via `O_DIRECT` / `F_NOCACHE` /
    /// `FILE_FLAG_NO_BUFFERING`. Hard-fails if the platform or
    /// filesystem does not support it.
    Direct,
}

/// Open or create the page file at `path` using the requested mode.
///
/// # Errors
///
/// Returns the underlying I/O error if the open fails. For
/// [`IoMode::Direct`] this also covers the "not supported on this
/// filesystem" case (Linux returns `EINVAL` from `open(O_DIRECT)` on
/// `tmpfs`; macOS returns `ENOTSUP` from `fcntl(F_NOCACHE)`; Windows
/// returns `ERROR_INVALID_PARAMETER` for unsupported volumes).
pub(crate) fn open_page_file(path: &Path, mode: IoMode) -> Result<File> {
    match mode {
        IoMode::Buffered => open_buffered(path),
        IoMode::Direct => open_direct(path),
    }
}

fn open_buffered(path: &Path) -> Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .map_err(Error::from)
}

#[cfg(target_os = "linux")]
fn open_direct(path: &Path) -> Result<File> {
    use std::os::unix::fs::OpenOptionsExt;
    // `O_DIRECT` is `0x4000` on every Linux ABI we care about. Hardcoded
    // because `libc::O_DIRECT` would pull in a dep just to name a
    // constant.
    const O_DIRECT: i32 = 0x4000;
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .custom_flags(O_DIRECT)
        .open(path)
        .map_err(Error::from)
}

#[cfg(target_os = "macos")]
fn open_direct(path: &Path) -> Result<File> {
    // macOS has no equivalent of `O_DIRECT`. We open buffered and apply
    // `F_NOCACHE` via `fcntl`, which prevents the kernel from caching
    // file data — equivalent semantic for our purposes.
    use std::os::unix::io::AsRawFd;
    extern "C" {
        fn fcntl(
            fd: std::ffi::c_int,
            cmd: std::ffi::c_int,
            arg: std::ffi::c_int,
        ) -> std::ffi::c_int;
    }
    /// `F_NOCACHE` from `<sys/fcntl.h>`. Hardcoded to avoid pulling in
    /// a libc-style dep just for a constant.
    const F_NOCACHE: std::ffi::c_int = 48;

    let file = open_buffered(path)?;
    let fd = file.as_raw_fd();
    // SAFETY: `fcntl` is a POSIX system call. The file descriptor we
    // pass is owned by `file` (alive for the duration of the call) and
    // is open for read/write. `F_NOCACHE` accepts an `int` argument
    // (1 = enable, 0 = disable). The call has no aliasing or memory
    // requirements beyond a valid fd.
    let result = unsafe { fcntl(fd, F_NOCACHE, 1) };
    if result < 0 {
        return Err(Error::Io(std::io::Error::last_os_error()));
    }
    Ok(file)
}

#[cfg(target_os = "windows")]
fn open_direct(path: &Path) -> Result<File> {
    use std::os::windows::fs::OpenOptionsExt;
    /// `FILE_FLAG_NO_BUFFERING` skips the system cache; the application
    /// must perform aligned I/O. emdb's [`crate::storage::page::Page`]
    /// is `align(4096)`, so this is satisfied for every read/write the
    /// engine issues.
    const FILE_FLAG_NO_BUFFERING: u32 = 0x2000_0000;
    /// `FILE_FLAG_WRITE_THROUGH` makes every write durable without a
    /// separate `FlushFileBuffers`. Combined with `NO_BUFFERING` we get
    /// "synchronous, uncached, durable I/O" — the closest Windows
    /// analogue to `O_DIRECT | O_DSYNC`.
    const FILE_FLAG_WRITE_THROUGH: u32 = 0x8000_0000;

    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .custom_flags(FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH)
        .open(path)
        .map_err(Error::from)
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn open_direct(_path: &Path) -> Result<File> {
    Err(Error::InvalidConfig(
        "direct I/O is not implemented on this platform; use IoMode::Buffered",
    ))
}

#[cfg(test)]
mod tests {
    use super::{open_page_file, IoMode};

    fn tmp_path(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0_u128, |d| d.as_nanos());
        p.push(format!("emdb-v4-io-{name}-{nanos}.bin"));
        p
    }

    #[test]
    fn buffered_open_creates_file() {
        let path = tmp_path("buffered");
        let opened = open_page_file(&path, IoMode::Buffered);
        assert!(opened.is_ok());
        let _removed = std::fs::remove_file(&path);
    }

    #[test]
    fn buffered_round_trip_through_seek_and_write() {
        use std::io::{Read, Seek, SeekFrom, Write};
        let path = tmp_path("buffered-rw");
        let mut file = match open_page_file(&path, IoMode::Buffered) {
            Ok(f) => f,
            Err(err) => panic!("open should succeed: {err}"),
        };
        let payload = b"emdb v4 io".to_vec();
        let _ = file.write_all(&payload);
        let _ = file.sync_data();
        let _ = file.seek(SeekFrom::Start(0));
        let mut readback = vec![0_u8; payload.len()];
        let _ = file.read_exact(&mut readback);
        assert_eq!(readback, payload);
        let _removed = std::fs::remove_file(&path);
    }

    #[test]
    fn io_mode_default_is_buffered() {
        assert_eq!(IoMode::default(), IoMode::Buffered);
    }

    // Direct I/O is platform-conditional and may fail on the test runner's
    // filesystem (e.g., tmpfs on Linux, certain VM-backed FS on Windows).
    // We only assert that the function is reachable and that the error,
    // when present, is a recognised I/O failure rather than a panic.
    #[test]
    fn direct_open_does_not_panic() {
        let path = tmp_path("direct");
        let result = open_page_file(&path, IoMode::Direct);
        // Either it succeeds (real disk filesystem) or it returns an
        // I/O error (tmpfs, network FS, etc.). Both are acceptable.
        match result {
            Ok(_file) => {}
            Err(crate::Error::Io(_)) => {}
            Err(crate::Error::InvalidConfig(_)) => {}
            Err(other) => panic!("unexpected error variant: {other:?}"),
        }
        let _removed = std::fs::remove_file(&path);
    }
}
