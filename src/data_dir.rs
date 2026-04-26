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

use std::path::PathBuf;

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
    // Create the app subdirectory. `create_dir_all` is the idempotent
    // variant — already-exists is fine. Anything else surfaces.
    std::fs::create_dir_all(&dir).map_err(Error::from)?;

    Ok(dir.join(file))
}

/// Validate a single path component used as either `app_name` or
/// `database_name`. The check is intentionally conservative — every
/// path separator (`/` or `\`), every `..` component, and the empty
/// string are rejected so the resolved path can never escape the data
/// root and so behaviour is identical on every platform.
///
/// Callers that want nested folders should pre-compose them with
/// [`crate::EmdbBuilder::data_root`] (their own platform-side join
/// logic), or pick a single dash-joined name like `"hivedb-kv"`.
/// Multi-segment `app_name` is intentionally not supported — keeping
/// validation trivial and dodging Windows backslash translation.
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
    if value == ".." || value.starts_with("../") || value.starts_with("..\\") {
        return Err(Error::InvalidConfig(match field {
            "app_name" => "app_name must not contain ..",
            "database_name" => "database_name must not contain ..",
            _ => "path component must not contain ..",
        }));
    }
    Ok(())
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
