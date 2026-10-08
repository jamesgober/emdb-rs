// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Cross-platform resolution of the default database storage path.
//!
//! The library does not, by default, choose a path on disk for the user —
//! [`crate::Emdb::open`] and [`crate::EmdbBuilder::path`] both take an
//! explicit path. But for embedded callers (HiveDB, any application using
//! emdb as its KV layer) the right path is OS-dependent and easy to get
//! wrong: macOS uses `~/Library/Application Support`, Linux uses
//! `$XDG_DATA_HOME`, Windows uses `%LOCALAPPDATA%`. Picking the wrong
//! root can land the database in `/tmp` (cleared on reboot) or under the
//! current working directory (lost when the process moves).
//!
//! This module owns the resolution logic. The public API is on
//! [`crate::EmdbBuilder`]: `app_name`, `database_name`, and `data_root`
//! together compute a platform-appropriate path. This module is the
//! implementation behind those builder methods.
//!
//! ## Resolution order
//!
//! 1. Explicit override via [`crate::EmdbBuilder::data_root`] (mostly for
//!    tests and Docker setups that need to point at a mounted volume).
//! 2. Platform default:
//!    - **Linux / BSD / unknown Unix:** `$XDG_DATA_HOME` if set, else
//!      `$HOME/.local/share` per the XDG Base Directory specification.
//!    - **macOS:** `$HOME/Library/Application Support`.
//!    - **Windows:** `%LOCALAPPDATA%` if set, else `%APPDATA%`, else
//!      `%USERPROFILE%\AppData\Local`.
//! 3. Last resort if every probe above fails: the process's current
//!    working directory. This is documented as "you almost certainly
//!    want to fix your environment instead" — it is correct only as a
//!    desperation fallback.
//!
//! Once the root is fixed, the full path is
//! `<root>/<app_name>/<database_name>`. Both the app subdirectory and
//! the database file are created on demand by the builder.

use std::path::{Component, Path, PathBuf};

use crate::{Error, Result};

/// Default subfolder name when [`crate::EmdbBuilder::app_name`] is not
/// set. Picked to be clearly recognisable as the library's own scratch
/// directory rather than something the embedder owns.
pub(crate) const DEFAULT_APP_NAME: &str = "emdb";

/// Default database filename when
/// [`crate::EmdbBuilder::database_name`] is not set. The recognisable
/// `emdb-default` prefix surfaces the "you forgot to name this" mistake
/// in directory listings rather than hiding it behind a generic
/// `database.db`.
pub(crate) const DEFAULT_DATABASE_NAME: &str = "emdb-default.emdb";

/// Resolve the platform's default data-storage root.
///
/// Returns `None` only when every platform probe fails *and* the
/// process has no current directory either — pathological. Callers
/// that want a hard error rather than the cwd fallback should test
/// [`Option::is_some`] on the result of [`platform_data_root`]
/// instead and surface their own error.
pub(crate) fn default_data_root() -> Option<PathBuf> {
    if let Some(p) = platform_data_root() {
        return Some(p);
    }
    // Last-resort fallback so the builder never returns an opaque
    // "no path" error. Callers can detect this by passing
    // [`Self::data_root`] explicitly.
    std::env::current_dir().ok()
}

/// Probe the platform-native data root without falling back to the
/// process current directory. `None` means the standard environment
/// variables for this platform were all unset.
///
/// The four `cfg`-gated branches below are mutually exclusive at
/// compile time — exactly one is active per target — so the `return`
/// keywords are load-bearing rather than redundant. Clippy reads each
/// branch in isolation, hence the local allow.
#[allow(clippy::needless_return)]
fn platform_data_root() -> Option<PathBuf> {
    #[cfg(target_os = "linux")]
    {
        return linux_or_xdg_root();
    }
    #[cfg(target_os = "macos")]
    {
        if let Some(home) = std::env::var_os("HOME") {
            return Some(
                PathBuf::from(home)
                    .join("Library")
                    .join("Application Support"),
            );
        }
        return None;
    }
    #[cfg(target_os = "windows")]
    {
        // %LOCALAPPDATA% is the right answer for "data this machine
        // owns and does not roam". %APPDATA% (Roaming) is acceptable
        // when LOCALAPPDATA is missing — uncommon but possible on
        // headless / sandboxed accounts. %USERPROFILE% is the last
        // probe before we fall through.
        if let Some(local) = std::env::var_os("LOCALAPPDATA") {
            if !local.is_empty() {
                return Some(PathBuf::from(local));
            }
        }
        if let Some(roaming) = std::env::var_os("APPDATA") {
            if !roaming.is_empty() {
                return Some(PathBuf::from(roaming));
            }
        }
        if let Some(profile) = std::env::var_os("USERPROFILE") {
            if !profile.is_empty() {
                return Some(PathBuf::from(profile).join("AppData").join("Local"));
            }
        }
        return None;
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    {
        // BSD, illumos, and other Unix variants follow the XDG spec.
        return linux_or_xdg_root();
    }
}

/// `$XDG_DATA_HOME` if set, else `$HOME/.local/share`. Shared between
/// Linux and "unknown Unix" arms so the spec implementation lives in
/// one place.
#[cfg(any(
    target_os = "linux",
    not(any(target_os = "linux", target_os = "macos", target_os = "windows"))
))]
fn linux_or_xdg_root() -> Option<PathBuf> {
    if let Some(xdg) = std::env::var_os("XDG_DATA_HOME") {
        if !xdg.is_empty() {
            return Some(PathBuf::from(xdg));
        }
    }
    if let Some(home) = std::env::var_os("HOME") {
        if !home.is_empty() {
            return Some(PathBuf::from(home).join(".local").join("share"));
        }
    }
    None
}

/// Compose the full database path from a builder's OS-resolution
/// fields and create the parent directory tree if it does not yet
/// exist.
///
/// `data_root_override` is what [`crate::EmdbBuilder::data_root`]
/// supplies; when `None`, the platform default is used.
///
/// `app_name` and `database_name` are stripped of leading/trailing
/// whitespace; an empty value (after trim) is treated as "not set"
/// and the corresponding default is substituted. Leading path
/// separators are rejected so a malicious or accidental
/// `app_name("../etc")` cannot escape the data root.
///
/// # Errors
///
/// - [`Error::InvalidConfig`] when neither the platform default nor
///   the cwd fallback yielded a usable root.
/// - [`Error::InvalidConfig`] when `app_name` or `database_name`
///   contains a path separator (`/`, `\`, or starts with `..`).
/// - [`Error::Io`] when creating the application subdirectory fails
///   for reasons other than "already exists".
pub(crate) fn resolve_database_path(
    data_root_override: Option<PathBuf>,
    app_name: Option<&str>,
    database_name: Option<&str>,
) -> Result<PathBuf> {
    let root = match data_root_override {
        Some(p) => p,
        None => default_data_root().ok_or(Error::InvalidConfig(
            "could not resolve a default data directory; \
             pass an explicit path via EmdbBuilder::path or \
             EmdbBuilder::data_root",
        ))?,
    };

    let app = match app_name.map(str::trim).filter(|s| !s.is_empty()) {
        Some(s) => s,
        None => DEFAULT_APP_NAME,
    };
    validate_path_component(app, "app_name")?;

    let file = match database_name.map(str::trim).filter(|s| !s.is_empty()) {
        Some(s) => s,
        None => DEFAULT_DATABASE_NAME,
    };
    validate_path_component(file, "database_name")?;

    let dir = root.join(app);
    // Create the app subdirectory (owner-only on Unix). Already-exists
    // is fine; anything else surfaces.
    crate::private_fs::create_private_dir_all(&dir).map_err(Error::from)?;

    Ok(dir.join(file))
}

/// Windows device names. Opening `NUL`, `CON`, `COM1`, ... (with or
/// without an extension) reaches the device instead of a file.
const WINDOWS_RESERVED_NAMES: [&str; 22] = [
    "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
    "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
];

/// True when `name` is a Windows device name, ignoring case, an
/// extension (`nul.emdb`) and trailing spaces before it.
fn is_windows_reserved_name(name: &str) -> bool {
    let stem = name.split('.').next().unwrap_or(name).trim_end_matches(' ');
    WINDOWS_RESERVED_NAMES
        .iter()
        .any(|reserved| reserved.eq_ignore_ascii_case(stem))
}

/// Validate a single path component used as either `app_name` or
/// `database_name`. The check is intentionally conservative and
/// identical on every platform, so the resolved path can never escape
/// the data root and a name that is safe on one OS is safe on all:
///
/// - path separators (`/`, `\`) are rejected;
/// - `:` is rejected (on Windows `C:name` is a drive-relative path
///   that ignores the data root, and `name:stream` addresses an NTFS
///   alternate data stream);
/// - `.`, `..` and any name made only of dots are rejected, as are
///   names ending in `.` or a space (Windows strips those, so `app.`
///   and `app` would name the same directory);
/// - Windows device names (`CON`, `NUL`, `COM1`, `LPT1`, ... with or
///   without an extension) are rejected;
/// - control characters are rejected;
/// - what remains must parse as exactly one normal path component.
///
/// Callers that want nested folders should pre-compose them with
/// [`crate::EmdbBuilder::data_root`] (their own platform-side join
/// logic), or pick a single dash-joined name like `"hivedb-kv"`.
fn validate_path_component(value: &str, field: &'static str) -> Result<()> {
    if value.contains('/') || value.contains('\\') {
        return Err(Error::InvalidConfig(match field {
            "app_name" => {
                "app_name must not contain path separators \
                           (/ or \\); use a single dash-joined name like \
                           \"hivedb-kv\", or compose nested paths with \
                           data_root() yourself"
            }
            "database_name" => "database_name must not contain path separators (/ or \\)",
            _ => "path component must not contain path separators",
        }));
    }
    if value.chars().all(|c| c == '.') {
        return Err(Error::InvalidConfig(match field {
            "app_name" => "app_name must not be . or .. (or only dots)",
            "database_name" => "database_name must not be . or .. (or only dots)",
            _ => "path component must not be . or ..",
        }));
    }
    if value.contains(':') {
        return Err(Error::InvalidConfig(match field {
            "app_name" => "app_name must not contain : (drive prefix or alternate data stream)",
            "database_name" => {
                "database_name must not contain : (drive prefix or alternate data stream)"
            }
            _ => "path component must not contain :",
        }));
    }
    if value.chars().any(char::is_control) {
        return Err(Error::InvalidConfig(match field {
            "app_name" => "app_name must not contain control characters",
            "database_name" => "database_name must not contain control characters",
            _ => "path component must not contain control characters",
        }));
    }
    if value.ends_with('.') || value.ends_with(' ') {
        return Err(Error::InvalidConfig(match field {
            "app_name" => "app_name must not end with . or a space",
            "database_name" => "database_name must not end with . or a space",
            _ => "path component must not end with . or a space",
        }));
    }
    if is_windows_reserved_name(value) {
        return Err(Error::InvalidConfig(match field {
            "app_name" => "app_name must not be a Windows device name (CON, NUL, COM1, LPT1, ...)",
            "database_name" => {
                "database_name must not be a Windows device name (CON, NUL, COM1, LPT1, ...)"
            }
            _ => "path component must not be a Windows device name",
        }));
    }
    let mut components = Path::new(value).components();
    let single_normal = matches!(
        (components.next(), components.next()),
        (Some(Component::Normal(_)), None)
    );
    if !single_normal {
        return Err(Error::InvalidConfig(match field {
            "app_name" => "app_name must be a single plain file-system name",
            "database_name" => "database_name must be a single plain file-system name",
            _ => "path component must be a single plain file-system name",
        }));
    }
    Ok(())
}

/// Resolve the path used to derive a database's lock, meta and journal
/// file names, following symbolic links so that two opens of the same
/// file through different names share one lock.
///
/// - An existing path is canonicalized.
/// - A path that does not exist yet is joined onto its canonicalized
///   parent directory (the file itself is created later).
/// - A dangling symbolic link is rejected: emdb would create the
///   link's target while locking the link's name.
/// - When even the parent cannot be canonicalized (it does not exist,
///   or a component is unreadable) the path is returned unchanged so
///   the open reports its own I/O error.
///
/// On Windows the `\\?\` prefix that `canonicalize` adds is dropped
/// for ordinary drive paths, so error messages and sidecar names stay
/// readable.
///
/// # Errors
///
/// [`Error::InvalidConfig`] for a dangling symbolic link.
pub(crate) fn canonical_database_path(path: &Path) -> Result<PathBuf> {
    match std::fs::canonicalize(path) {
        Ok(canonical) => return Ok(simplify_canonical(canonical)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Ok(path.to_path_buf()),
    }
    if std::fs::symlink_metadata(path).is_ok() {
        // Something exists at `path`, but following it fails: a
        // symbolic link whose target is missing.
        return Err(Error::InvalidConfig(
            "database path is a symbolic link whose target does not exist",
        ));
    }
    let Some(file_name) = path.file_name() else {
        return Ok(path.to_path_buf());
    };
    let parent = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    match std::fs::canonicalize(parent) {
        Ok(dir) => Ok(simplify_canonical(dir).join(file_name)),
        Err(_) => Ok(path.to_path_buf()),
    }
}

/// Strip the verbatim `\\?\` prefix from a canonical Windows drive
/// path when the plain form means the same file: the path is shorter
/// than `MAX_PATH` and contains no device-name component. UNC and
/// other verbatim forms are kept. Unix paths pass through unchanged.
fn simplify_canonical(path: PathBuf) -> PathBuf {
    #[cfg(windows)]
    {
        use std::path::Prefix;
        let mut comps = path.components();
        if let Some(Component::Prefix(prefix)) = comps.next() {
            if let Prefix::VerbatimDisk(letter) = prefix.kind() {
                let rest: Vec<Component<'_>> = comps.collect();
                let plain_safe = rest.iter().all(|c| match c {
                    Component::RootDir => true,
                    Component::Normal(name) => {
                        name.to_str().is_some_and(|n| !is_windows_reserved_name(n))
                    }
                    _ => false,
                });
                if plain_safe {
                    let mut out = PathBuf::from(format!("{}:\\", char::from(letter)));
                    for c in rest {
                        if let Component::Normal(name) = c {
                            out.push(name);
                        }
                    }
                    if out.as_os_str().len() < 260 {
                        return out;
                    }
                }
            }
        }
    }
    path
}

/// Owner of the private temporary directory behind an
/// [`crate::Emdb::open_in_memory`] database. Dropping it removes the
/// directory and everything emdb wrote into it (journal, meta and lock
/// sidecars, compaction leftovers). It must drop after the engine and
/// the lock file so no handle is still writing into the directory.
#[derive(Debug)]
pub(crate) struct EphemeralDir {
    dir: PathBuf,
}

impl Drop for EphemeralDir {
    fn drop(&mut self) {
        // Best-effort: on Windows a file that a live `ValueRef` still
        // maps cannot be deleted yet. The directory is owner-only and
        // lives in the OS temp directory, so a leftover is cleaned up
        // with the rest of the temp directory.
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Create a fresh owner-only directory under the OS temp directory and
/// return the database path inside it, plus the guard that removes the
/// directory again.
///
/// The directory is created with `create_new` semantics (mode `0o700`
/// on Unix), so it cannot be a directory or symbolic link another user
/// planted in a shared `/tmp`, and nothing else can create files in it.
///
/// # Errors
///
/// [`Error::Io`] when the temp directory is not writable.
pub(crate) fn ephemeral_database_path() -> Result<(PathBuf, EphemeralDir)> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static MEMORY_COUNTER: AtomicU64 = AtomicU64::new(0);

    let pid = std::process::id();
    let base = std::env::temp_dir();
    let mut last_err = None;
    // A collision needs another process with our pid and the same
    // counter and clock values, or a name planted in advance; a few
    // retries with fresh counter values cover both.
    for _ in 0..16 {
        let counter = MEMORY_COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0_u128, |d| d.as_nanos());
        let dir = base.join(format!("emdb-mem-{pid}-{counter}-{nanos}"));
        match crate::private_fs::create_new_private_dir(&dir) {
            Ok(()) => {
                let path = dir.join("emdb-mem.emdb");
                return Ok((path, EphemeralDir { dir }));
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => last_err = Some(err),
            Err(err) => return Err(Error::from(err)),
        }
    }
    Err(Error::from(last_err.unwrap_or_else(|| {
        std::io::Error::other("could not create a unique temporary directory")
    })))
}

#[cfg(test)]
mod tests {
    use super::{
        resolve_database_path, validate_path_component, DEFAULT_APP_NAME, DEFAULT_DATABASE_NAME,
    };

    fn temp_root() -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0_u128, |d| d.as_nanos());
        p.push(format!("emdb-data-dir-test-{nanos}"));
        p
    }

    #[test]
    fn resolve_uses_explicit_root_and_creates_subdir() {
        let root = temp_root();
        let path = match resolve_database_path(Some(root.clone()), Some("hive"), Some("core.emdb"))
        {
            Ok(p) => p,
            Err(err) => panic!("resolve should succeed: {err}"),
        };

        assert_eq!(path, root.join("hive").join("core.emdb"));
        // The app subdirectory was created.
        assert!(path.parent().map(std::path::Path::is_dir).unwrap_or(false));

        let _removed = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn resolve_substitutes_defaults_when_args_unset() {
        let root = temp_root();
        let path = match resolve_database_path(Some(root.clone()), None, None) {
            Ok(p) => p,
            Err(err) => panic!("resolve should succeed: {err}"),
        };
        assert_eq!(
            path,
            root.join(DEFAULT_APP_NAME).join(DEFAULT_DATABASE_NAME)
        );
        let _removed = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn resolve_substitutes_defaults_for_whitespace_only_values() {
        let root = temp_root();
        let path = match resolve_database_path(Some(root.clone()), Some("   "), Some("\t\n")) {
            Ok(p) => p,
            Err(err) => panic!("resolve should succeed: {err}"),
        };
        assert_eq!(
            path,
            root.join(DEFAULT_APP_NAME).join(DEFAULT_DATABASE_NAME)
        );
        let _removed = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn validate_rejects_path_separators() {
        // Single-segment-only by design: forward slashes and backslashes
        // are both rejected so behaviour is identical on every platform
        // and so users cannot accidentally escape the data root.
        assert!(validate_path_component("hive/inner", "app_name").is_err());
        assert!(validate_path_component("hive\\inner", "app_name").is_err());
        assert!(validate_path_component("inner/file.emdb", "database_name").is_err());
    }

    #[test]
    fn validate_rejects_dotdot() {
        assert!(validate_path_component("..", "app_name").is_err());
    }

    #[test]
    fn test_validate_rejects_windows_escapes_and_aliases() {
        for bad in [
            ".",
            "...",
            "C:escaped",
            "C:",
            "x.emdb:ads",
            "NUL",
            "nul",
            "nul.emdb",
            "CON",
            "Con .txt",
            "PRN",
            "AUX",
            "COM1",
            "com9.db",
            "LPT1",
            "app.",
            "app ",
            "tab\there",
            "nul\u{0}byte",
        ] {
            assert!(
                validate_path_component(bad, "database_name").is_err(),
                "{bad:?} accepted"
            );
        }
    }

    #[test]
    fn test_validate_accepts_plain_names() {
        for good in [
            "emdb",
            "hive-kv",
            "core.emdb",
            "v1.2.emdb",
            ".hidden",
            "COM10",
            "console",
            "nullable.db",
            "LPT",
            "a b",
        ] {
            assert!(
                validate_path_component(good, "database_name").is_ok(),
                "{good:?} rejected"
            );
        }
    }

    #[test]
    fn test_canonical_database_path_resolves_missing_file_against_parent() {
        let root = temp_root();
        std::fs::create_dir_all(&root).unwrap();
        let canonical = super::canonical_database_path(&root.join("db.emdb")).unwrap();
        assert_eq!(canonical.file_name().unwrap(), "db.emdb");
        assert!(canonical.is_absolute());
        // Same answer once the file exists.
        std::fs::write(root.join("db.emdb"), b"").unwrap();
        let existing = super::canonical_database_path(&root.join("db.emdb")).unwrap();
        assert_eq!(canonical, existing);
        // A dotted detour resolves to the same path.
        std::fs::create_dir_all(root.join("sub")).unwrap();
        let detour = root.join("sub").join("..").join("db.emdb");
        assert_eq!(super::canonical_database_path(&detour).unwrap(), existing);
        #[cfg(windows)]
        assert!(!existing.to_string_lossy().starts_with(r"\\?\"));
        let _removed = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn test_canonical_database_path_missing_parent_is_returned_unchanged() {
        let missing = temp_root().join("no-such-dir").join("db.emdb");
        assert_eq!(super::canonical_database_path(&missing).unwrap(), missing);
    }

    #[test]
    fn test_ephemeral_database_path_is_unique_and_cleaned_up() {
        let (p1, g1) = super::ephemeral_database_path().unwrap();
        let (p2, g2) = super::ephemeral_database_path().unwrap();
        assert_ne!(p1.parent(), p2.parent());
        let dir = p1.parent().unwrap().to_path_buf();
        assert!(dir.is_dir());
        std::fs::write(&p1, b"data").unwrap();
        drop(g1);
        drop(g2);
        assert!(!dir.exists());
    }

    #[test]
    fn resolve_rejects_separator_in_app_name() {
        let root = temp_root();
        let result = resolve_database_path(Some(root.clone()), Some("a/b"), None);
        assert!(result.is_err());
        let _removed = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn default_root_is_some_in_typical_environment() {
        // Smoke test only — on a typical dev machine at least one of
        // HOME, USERPROFILE, or the cwd resolves. This guards against
        // a regression that returns None universally.
        let root = super::default_data_root();
        assert!(root.is_some(), "default_data_root should yield a path");
    }
}
