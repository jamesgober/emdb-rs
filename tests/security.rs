// Copyright 2026 James Gober. Licensed under Apache-2.0.
//
// Security regression tests. Each test reproduces a finding from the
// 1.0.2 security audit (proof-of-concept file edits, crafted journals,
// lock races, path tricks) and asserts the fixed behaviour.
//
// Journals are crafted by hand: an fsys frame is
// `[magic u32 BE][payload_len u32 LE][payload][crc32c u32 LE]`, with
// the CRC-32C taken over magic, length and payload.

use std::path::{Path, PathBuf};

use emdb::{Emdb, Error};

const FSYS_MAGIC: u32 = 0x4653_5901;

fn tmp_path(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join("emdb-security-tests");
    std::fs::create_dir_all(&dir).expect("create test dir");
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |d| d.as_nanos());
    let tid = std::thread::current().id();
    dir.join(format!(
        "{label}-{}-{nanos}-{tid:?}.emdb",
        std::process::id()
    ))
}

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    PathBuf::from(format!("{}{suffix}", path.display()))
}

fn cleanup(path: &Path) {
    for suffix in ["", ".meta", ".lock", ".lock-meta", ".compact.tmp"] {
        let _ = std::fs::remove_file(sidecar(path, suffix));
    }
    if let (Some(dir), Some(name)) = (path.parent(), path.file_name()) {
        let prefix = format!("{}.corrupt-", name.to_string_lossy());
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                if entry.file_name().to_string_lossy().starts_with(&prefix) {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }
    }
}

/// CRC-32C (Castagnoli), bitwise. Matches fsys's frame checksum.
fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFF_u32;
    for &b in bytes {
        crc ^= u32::from(b);
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0x82F6_3B78
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

fn encode_frame(out: &mut Vec<u8>, payload: &[u8]) {
    let start = out.len();
    out.extend_from_slice(&FSYS_MAGIC.to_be_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(payload);
    let crc = crc32c(&out[start..]);
    out.extend_from_slice(&crc.to_le_bytes());
}

fn rebuild(payloads: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    for p in payloads {
        encode_frame(&mut out, p);
    }
    out
}

/// Split a journal into its frame payloads.
#[cfg(feature = "encrypt")]
fn payloads_of(path: &Path) -> Vec<Vec<u8>> {
    let bytes = std::fs::read(path).expect("read journal");
    let mut out = Vec::new();
    let mut off = 0;
    while off + 12 <= bytes.len() {
        let magic = u32::from_be_bytes(bytes[off..off + 4].try_into().expect("4 bytes"));
        if magic != FSYS_MAGIC {
            break;
        }
        let len = u32::from_le_bytes(bytes[off + 4..off + 8].try_into().expect("4 bytes")) as usize;
        out.push(bytes[off + 8..off + 8 + len].to_vec());
        off += 12 + len;
    }
    out
}

fn plain_remove(ns_id: u32, key: &[u8]) -> Vec<u8> {
    let mut p = vec![0x01_u8];
    p.extend_from_slice(&ns_id.to_le_bytes());
    p.extend_from_slice(&(key.len() as u32).to_le_bytes());
    p.extend_from_slice(key);
    p
}

fn plain_namespace_name(ns_id: u32, name: &[u8]) -> Vec<u8> {
    let mut p = vec![0x02_u8];
    p.extend_from_slice(&ns_id.to_le_bytes());
    p.extend_from_slice(&(name.len() as u32).to_le_bytes());
    p.extend_from_slice(name);
    p
}

fn plain_insert(ns_id: u32, key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut p = vec![0x00_u8];
    p.extend_from_slice(&ns_id.to_le_bytes());
    p.extend_from_slice(&(key.len() as u32).to_le_bytes());
    p.extend_from_slice(key);
    p.extend_from_slice(&(value.len() as u32).to_le_bytes());
    p.extend_from_slice(value);
    p.extend_from_slice(&0_u64.to_le_bytes());
    p
}

fn assert_corrupted<T: std::fmt::Debug>(result: emdb::Result<T>, context: &str) {
    match result {
        Err(Error::Corrupted { .. }) => {}
        other => panic!("{context}: expected Error::Corrupted, got {other:?}"),
    }
}

// ---------------------------------------------------------------------
// Namespace id bounds (E-S7, E-S8). Plaintext databases.
// ---------------------------------------------------------------------

/// A 21 KB journal mentioning 1000 unbound namespace ids used to make
/// open allocate a 2 MiB index per id (about 2 GB). Records for ids no
/// `NamespaceName` record bound are now rejected.
#[test]
fn test_open_records_for_unbound_namespace_ids_returns_corrupted() {
    let path = tmp_path("ns-bomb");
    cleanup(&path);
    let payloads: Vec<Vec<u8>> = (1..=1000_u32).map(|ns| plain_remove(ns, b"")).collect();
    std::fs::write(&path, rebuild(&payloads)).expect("write journal");

    #[cfg(target_os = "linux")]
    let before = rss_kib();
    let result = Emdb::open(&path);
    #[cfg(target_os = "linux")]
    {
        let grown_mib = rss_kib().saturating_sub(before) / 1024;
        assert!(grown_mib < 64, "open grew RSS by {grown_mib} MiB");
    }
    assert_corrupted(result, "unbound namespace ids");
    cleanup(&path);
}

#[cfg(target_os = "linux")]
fn rss_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .find(|l| l.starts_with("VmRSS:"))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// A record with `ns_id = u32::MAX` used to push the id allocator past
/// `u32::MAX`, so the next `namespace()` call wrapped onto id 0 and
/// aliased the default namespace.
#[test]
fn test_open_namespace_id_u32_max_returns_corrupted() {
    let path = tmp_path("ns-wrap");
    cleanup(&path);
    std::fs::write(&path, rebuild(&[plain_remove(u32::MAX, b"")])).expect("write");
    assert_corrupted(Emdb::open(&path), "remove in ns u32::MAX");

    std::fs::write(&path, rebuild(&[plain_namespace_name(u32::MAX, b"x")])).expect("write");
    assert_corrupted(Emdb::open(&path), "binding of ns u32::MAX");
    cleanup(&path);
}

#[test]
fn test_open_namespace_name_for_default_namespace_returns_corrupted() {
    let path = tmp_path("ns-zero");
    cleanup(&path);
    std::fs::write(&path, rebuild(&[plain_namespace_name(0, b"alias")])).expect("write");
    assert_corrupted(Emdb::open(&path), "binding of ns 0");
    cleanup(&path);
}

#[test]
fn test_open_namespace_id_rebound_to_other_name_returns_corrupted() {
    let path = tmp_path("ns-rebind");
    cleanup(&path);
    let payloads = vec![
        plain_namespace_name(1, b"secrets"),
        plain_insert(1, b"k", b"v"),
        plain_namespace_name(1, b"public"),
    ];
    std::fs::write(&path, rebuild(&payloads)).expect("write");
    assert_corrupted(Emdb::open(&path), "rebinding ns 1");
    cleanup(&path);
}

/// Files emdb itself writes keep opening: several namespaces, a
/// namespace dropped and re-created (same name, new id), compaction.
#[test]
fn test_valid_namespace_histories_still_open() -> emdb::Result<()> {
    let path = tmp_path("ns-valid");
    cleanup(&path);
    {
        let db = Emdb::open(&path)?;
        db.insert("root", "r")?;
        let a = db.namespace("a")?;
        a.insert("k", "a1")?;
        let b = db.namespace("b")?;
        b.insert("k", "b1")?;
        let _ = b.remove("k")?;
        assert!(db.drop_namespace("a")?);
        let a2 = db.namespace("a")?;
        a2.insert("k", "a2")?;
        db.flush()?;
    }
    {
        let db = Emdb::open(&path)?;
        assert_eq!(db.get("root")?, Some(b"r".to_vec()));
        assert_eq!(db.namespace("a")?.get("k")?, Some(b"a2".to_vec()));
        assert_eq!(db.namespace("b")?.get("k")?, None);
        db.compact()?;
        db.flush()?;
    }
    let db = Emdb::open(&path)?;
    assert_eq!(db.namespace("a")?.get("k")?, Some(b"a2".to_vec()));
    let c = db.namespace("c")?;
    c.insert("x", "y")?;
    assert_eq!(c.get("x")?, Some(b"y".to_vec()));
    assert_eq!(
        db.get("x")?,
        None,
        "new namespace must not alias the default"
    );
    drop(db);
    cleanup(&path);
    Ok(())
}

// ---------------------------------------------------------------------
// Strict record decoding. Plaintext databases.
// ---------------------------------------------------------------------

#[test]
fn test_open_record_with_trailing_bytes_returns_corrupted() {
    let path = tmp_path("trailing");
    cleanup(&path);
    let mut rm = plain_remove(0, b"k");
    rm.push(0xAA);
    std::fs::write(&path, rebuild(&[plain_insert(0, b"k", b"v"), rm])).expect("write");
    assert_corrupted(Emdb::open(&path), "remove with trailing byte");
    cleanup(&path);
}

#[test]
fn test_open_record_with_huge_length_field_returns_corrupted() {
    let path = tmp_path("huge-len");
    cleanup(&path);
    let mut p = vec![0x00_u8];
    p.extend_from_slice(&0_u32.to_le_bytes());
    p.extend_from_slice(&u32::MAX.to_le_bytes());
    p.extend_from_slice(b"short");
    std::fs::write(&path, rebuild(&[p])).expect("write");
    assert_corrupted(Emdb::open(&path), "insert with key_len u32::MAX");
    cleanup(&path);
}

// ---------------------------------------------------------------------
// Data-directory names (E-S13).
// ---------------------------------------------------------------------

/// `C:name` is drive-relative on Windows and ignores the data root;
/// `name:stream` is an NTFS alternate data stream; `NUL`/`CON` are
/// devices; `...` and `app.` collapse onto other names. All rejected
/// on every platform so behaviour is the same everywhere.
#[test]
fn test_data_dir_names_that_escape_or_alias_are_rejected() {
    let root = tmp_path("data-root");
    std::fs::create_dir_all(&root).expect("root");
    let cases = [
        ("C:escaped-app", "x.emdb"),
        ("app", "C:escaped.emdb"),
        ("app", "x.emdb:ads"),
        ("app", "NUL"),
        ("app", "nul.emdb"),
        ("CON", "x.emdb"),
        ("app", "COM1"),
        ("app", "lpt9.txt"),
        ("...", "x.emdb"),
        (".", "x.emdb"),
        ("..", "x.emdb"),
        ("app.", "x.emdb"),
        ("app", "x.emdb."),
        ("a\u{0}b", "x.emdb"),
        ("a/b", "x.emdb"),
        ("app", "..\\x.emdb"),
    ];
    for (app, db_name) in cases {
        let result = Emdb::builder()
            .data_root(&root)
            .app_name(app)
            .database_name(db_name)
            .build();
        assert!(
            matches!(result, Err(Error::InvalidConfig(_))),
            "app={app:?} db={db_name:?} accepted: {:?}",
            result.as_ref().map(Emdb::path)
        );
    }
    // Nothing was created anywhere: the root has no children.
    let children = std::fs::read_dir(&root).expect("read root").count();
    assert_eq!(children, 0, "a rejected name still created something");

    // Ordinary names keep working.
    let db = Emdb::builder()
        .data_root(&root)
        .app_name("hive-kv")
        .database_name("core.v2.emdb")
        .build()
        .expect("plain names");
    assert!(db.path().starts_with(&root));
    drop(db);
    let _ = std::fs::remove_dir_all(&root);
}

// ---------------------------------------------------------------------
// open_in_memory and file permissions (E-S9).
// ---------------------------------------------------------------------

#[test]
fn test_open_in_memory_removes_its_directory_on_drop() -> emdb::Result<()> {
    let db = Emdb::open_in_memory();
    db.insert("secret", "in-memory-only")?;
    db.flush()?;
    let path = db.path().to_path_buf();
    let dir = path.parent().expect("parent").to_path_buf();
    assert!(path.exists());
    assert_ne!(
        dir,
        std::env::temp_dir(),
        "file must live in its own directory"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&dir)?.permissions().mode() & 0o777;
        assert_eq!(mode & 0o077, 0, "ephemeral dir mode {mode:o}");
        let mode = std::fs::metadata(&path)?.permissions().mode() & 0o777;
        assert_eq!(mode & 0o077, 0, "ephemeral file mode {mode:o}");
    }
    let clone = db.clone();
    drop(db);
    assert!(path.exists(), "a live clone keeps the database");
    drop(clone);
    assert!(!dir.exists(), "directory {dir:?} left behind");
    Ok(())
}

#[cfg(unix)]
#[test]
fn test_created_files_and_dirs_are_owner_only() -> emdb::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mode_of = |p: &Path| {
        std::fs::metadata(p)
            .unwrap_or_else(|e| panic!("{p:?}: {e}"))
            .permissions()
            .mode()
            & 0o777
    };

    let path = tmp_path("modes");
    cleanup(&path);
    let db = Emdb::open(&path)?;
    db.insert("a", "b")?;
    db.flush()?;
    for suffix in ["", ".lock", ".lock-meta"] {
        let p = sidecar(&path, suffix);
        let mode = mode_of(&p);
        assert_eq!(mode & 0o077, 0, "{p:?} mode {mode:o}");
    }
    drop(db);
    cleanup(&path);

    let root = tmp_path("modes-root");
    let db = Emdb::builder()
        .data_root(&root)
        .app_name("owner-only")
        .build()?;
    let app_dir = db.path().parent().expect("app dir").to_path_buf();
    assert_eq!(
        mode_of(&app_dir) & 0o077,
        0,
        "app dir {:o}",
        mode_of(&app_dir)
    );
    assert_eq!(mode_of(&root) & 0o077, 0, "root {:o}", mode_of(&root));
    drop(db);
    let _ = std::fs::remove_dir_all(&root);
    Ok(())
}

/// Permissions of an existing file are the owner's business: emdb
/// does not tighten (or loosen) them.
#[cfg(unix)]
#[test]
fn test_existing_database_file_mode_is_preserved() -> emdb::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let path = tmp_path("modes-existing");
    cleanup(&path);
    drop(Emdb::open(&path)?);
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640))?;
    drop(Emdb::open(&path)?);
    let mode = std::fs::metadata(&path)?.permissions().mode() & 0o777;
    assert_eq!(mode, 0o640);
    cleanup(&path);
    Ok(())
}

/// Compaction renames a rewritten file over the database: it keeps the
/// owner's mode. A backup is a new file, so it is owner-only.
#[cfg(unix)]
#[test]
fn test_compaction_keeps_mode_and_backup_is_owner_only() -> emdb::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let path = tmp_path("modes-rewrite");
    let backup = tmp_path("modes-rewrite-backup");
    cleanup(&path);
    cleanup(&backup);
    let db = Emdb::open(&path)?;
    db.insert("a", "1")?;
    db.insert("a", "2")?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640))?;
    db.compact()?;
    let mode = std::fs::metadata(&path)?.permissions().mode() & 0o777;
    assert_eq!(mode, 0o640, "compaction changed the mode");
    db.backup_to(&backup)?;
    let mode = std::fs::metadata(&backup)?.permissions().mode() & 0o777;
    assert_eq!(mode & 0o077, 0, "backup mode {mode:o}");
    drop(db);
    cleanup(&path);
    cleanup(&backup);
    Ok(())
}

// ---------------------------------------------------------------------
// Lockfile (E-S10).
// ---------------------------------------------------------------------

/// 1.0.2 unlinked `<path>.lock` after unlocking it. A second process
/// that had opened the old file could then lock the orphaned inode
/// while a third created and locked a new file: two "exclusive"
/// holders. The lock file now stays in place.
#[test]
fn test_lockfile_unlock_then_unlink_race_leaves_one_holder() {
    use fs4::FileExt;

    let path = tmp_path("lock-race");
    cleanup(&path);
    let lock = sidecar(&path, ".lock");

    let a = Emdb::open(&path).expect("A opens");
    // B runs the open() step of its acquire while A still holds the lock.
    let b_fd = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&lock)
        .expect("B opens lock file");
    drop(a);
    assert!(lock.exists(), "lock file must survive the holder's drop");
    // B's lock attempt now succeeds on the real lock file...
    b_fd.try_lock_exclusive().expect("B locks");
    // ...so C must see the database as busy.
    let c = Emdb::open(&path);
    assert!(
        matches!(c, Err(Error::LockBusy { .. })),
        "two holders: C opened while B holds the lock ({:?})",
        c.as_ref().map(|_| ())
    );
    let _ = fs4::FileExt::unlock(&b_fd);
    drop(b_fd);
    let d = Emdb::open(&path).expect("D opens after B releases");
    drop(d);
    cleanup(&path);
}

/// A second open of a database that is already open in this process
/// reports `LockBusy` on every platform (Windows used to surface
/// ERROR_LOCK_VIOLATION as `LockfileError`).
#[test]
fn test_second_open_in_same_process_returns_lock_busy() {
    let path = tmp_path("lock-same-process");
    cleanup(&path);
    let first = Emdb::open(&path).expect("first open");
    let second = Emdb::open(&path);
    assert!(
        matches!(second, Err(Error::LockBusy { .. })),
        "second open: {:?}",
        second.as_ref().map(|_| ())
    );
    drop(first);
    drop(Emdb::open(&path).expect("open after the first handle dropped"));
    cleanup(&path);
}

/// Opening the same database through a symbolic link used to derive a
/// different lock path, so the link and the real path could both be
/// opened at once.
#[cfg(unix)]
#[test]
fn test_open_through_symlink_shares_the_lock() {
    let real = tmp_path("symlink-real");
    let link = tmp_path("symlink-link");
    cleanup(&real);
    let _ = std::fs::remove_file(&link);

    let db = Emdb::open(&real).expect("open real");
    std::os::unix::fs::symlink(&real, &link).expect("symlink");
    let second = Emdb::open(&link);
    assert!(
        matches!(second, Err(Error::LockBusy { .. })),
        "symlinked open bypassed the lock: {:?}",
        second.as_ref().map(|_| ())
    );
    drop(db);

    // Via the link alone it works, and the data lands in the real file.
    {
        let via_link = Emdb::open(&link).expect("open via link");
        via_link.insert("k", "v").expect("insert");
        via_link.flush().expect("flush");
        assert!(
            Emdb::lock_holder(&real).expect("holder").is_some(),
            "holder metadata is keyed by the real path"
        );
    }
    let db = Emdb::open(&real).expect("reopen real");
    assert_eq!(db.get("k").expect("get"), Some(b"v".to_vec()));
    drop(db);
    let _ = std::fs::remove_file(&link);
    cleanup(&real);
}

#[cfg(unix)]
#[test]
fn test_open_dangling_symlink_returns_invalid_config() {
    let target = tmp_path("dangling-target");
    let link = tmp_path("dangling-link");
    let _ = std::fs::remove_file(&link);
    std::os::unix::fs::symlink(&target, &link).expect("symlink");
    let result = Emdb::open(&link);
    assert!(
        matches!(result, Err(Error::InvalidConfig(_))),
        "{:?}",
        result.as_ref().map(|_| ())
    );
    assert!(!target.exists(), "dangling link target was created");
    let _ = std::fs::remove_file(&link);
}

// ---------------------------------------------------------------------
// Encryption (E-S1, E-S2, E-S4, AEAD failure reporting).
// ---------------------------------------------------------------------

#[cfg(feature = "encrypt")]
mod encrypted {
    use super::*;
    use emdb::EncryptionInput;

    const KEY: [u8; 32] = [7_u8; 32];

    fn open_key(p: &Path) -> emdb::Result<Emdb> {
        Emdb::builder().path(p).encryption_key(KEY).build()
    }

    /// Debug output of the builder and of `EncryptionInput` used to
    /// print the raw key bytes and the passphrase.
    #[test]
    fn test_debug_output_contains_no_key_or_passphrase() {
        let builder = Emdb::builder()
            .encryption_key([0xAB; 32])
            .encryption_passphrase("hunter2-super-secret");
        for rendered in [format!("{builder:?}"), format!("{builder:#?}")] {
            assert!(!rendered.contains("171"), "key byte 0xAB: {rendered}");
            assert!(!rendered.to_lowercase().contains("0xab"), "{rendered}");
            assert!(!rendered.contains("hunter2"), "{rendered}");
            assert!(rendered.contains("redacted"), "{rendered}");
        }
        let unset = format!("{:?}", Emdb::builder());
        assert!(unset.contains("encryption_key: None"), "{unset}");

        let pass = EncryptionInput::Passphrase("pw-in-debug".into());
        let key = EncryptionInput::Key([0xCD; 32]);
        let rendered = format!("{pass:?} {key:?} {pass:#?} {key:#?}");
        assert!(!rendered.contains("pw-in-debug"), "{rendered}");
        assert!(!rendered.contains("205"), "{rendered}");
    }

    /// The kind byte is outside the AEAD tag. Flipping an encrypted
    /// Insert into a Remove used to delete the key on open; strict body
    /// length decoding now rejects the record.
    #[test]
    fn test_tag_flip_insert_to_remove_returns_corrupted() {
        let p = tmp_path("tagflip-remove");
        cleanup(&p);
        {
            let db = open_key(&p).expect("open");
            db.insert("victim", "important-value").expect("insert");
            db.insert("other", "v2").expect("insert");
            db.flush().expect("flush");
        }
        let mut pl = payloads_of(&p);
        assert_eq!(pl.len(), 2);
        assert_eq!(pl[0][0], 0x80, "encrypted insert");
        pl[0][0] = 0x81;
        std::fs::write(&p, rebuild(&pl)).expect("write");
        assert_corrupted(open_key(&p), "insert flipped to remove");
        cleanup(&p);
    }

    #[test]
    fn test_tag_flip_insert_to_namespace_name_returns_corrupted() {
        let p = tmp_path("tagflip-nsname");
        cleanup(&p);
        {
            let db = open_key(&p).expect("open");
            db.insert("victim", "important-value").expect("insert");
            db.flush().expect("flush");
        }
        let mut pl = payloads_of(&p);
        pl[0][0] = 0x82;
        std::fs::write(&p, rebuild(&pl)).expect("write");
        assert_corrupted(open_key(&p), "insert flipped to namespace-name");
        cleanup(&p);
    }

    /// A Remove and a NamespaceName have the same body layout. Flipping
    /// a remove in a named namespace into a NamespaceName would rebind
    /// that namespace id to a new name.
    #[test]
    fn test_tag_flip_remove_to_namespace_name_returns_corrupted() {
        let p = tmp_path("tagflip-rm-nsname");
        cleanup(&p);
        {
            let db = open_key(&p).expect("open");
            let ns = db.namespace("tenant").expect("ns");
            ns.insert("k", "v").expect("insert");
            let _ = ns.remove("k").expect("remove");
            db.flush().expect("flush");
        }
        let mut pl = payloads_of(&p);
        assert_eq!(pl.len(), 3, "binding, insert, remove");
        assert_eq!(pl[2][0], 0x81, "encrypted remove");
        pl[2][0] = 0x82;
        std::fs::write(&p, rebuild(&pl)).expect("write");
        assert_corrupted(open_key(&p), "remove flipped to namespace-name");

        // And the reverse: the namespace binding flipped into a remove
        // leaves the namespace's records without a binding.
        let mut pl = payloads_of(&p);
        pl[2][0] = 0x81;
        pl[0][0] = 0x81;
        std::fs::write(&p, rebuild(&pl)).expect("write");
        assert_corrupted(open_key(&p), "namespace-name flipped to remove");
        cleanup(&p);
    }

    /// Plaintext records appended to an encrypted database were applied
    /// without authentication: a forged Remove deleted keys and a
    /// forged NamespaceName exposed a secret namespace under a new name.
    #[test]
    fn test_plaintext_records_in_encrypted_db_return_corrupted() {
        let p = tmp_path("plain-inject");
        cleanup(&p);
        {
            let db = open_key(&p).expect("open");
            db.insert("admin-token", "s3cr3t").expect("insert");
            let ns = db.namespace("secrets").expect("ns");
            ns.insert("k", "secret-ns-value").expect("insert");
            db.flush().expect("flush");
        }
        let original = payloads_of(&p);
        let injections = [
            plain_remove(0, b"admin-token"),
            plain_namespace_name(1, b"public"),
            plain_insert(0, b"admin-token", b"attacker"),
        ];
        for forged in injections {
            let mut pl = original.clone();
            pl.push(forged.clone());
            std::fs::write(&p, rebuild(&pl)).expect("write");
            assert_corrupted(open_key(&p), &format!("forged plaintext {forged:?}"));
        }
        // The untouched log still opens and reads back.
        std::fs::write(&p, rebuild(&original)).expect("restore");
        let db = open_key(&p).expect("reopen original");
        assert_eq!(
            db.get("admin-token").expect("get"),
            Some(b"s3cr3t".to_vec())
        );
        drop(db);
        cleanup(&p);
    }

    /// A keyed open of an existing plaintext database used to mark it
    /// encrypted while its records stayed plaintext, after which neither
    /// a keyed nor a plain open worked. It is now refused up front and
    /// the database is left untouched.
    #[test]
    fn test_keyed_open_of_plaintext_db_is_refused_and_db_stays_usable() {
        let p = tmp_path("plain-subst");
        cleanup(&p);
        {
            let db = Emdb::open(&p).expect("plain open");
            db.insert("role:mallory", "admin").expect("insert");
            db.flush().expect("flush");
        }
        let meta_before = std::fs::read(sidecar(&p, ".meta")).expect("meta");
        let result = open_key(&p);
        match &result {
            Err(Error::InvalidConfig(msg)) => {
                assert!(msg.contains("enable_encryption"), "{msg}");
            }
            other => panic!(
                "expected InvalidConfig, got {:?}",
                other.as_ref().map(|_| ())
            ),
        }
        drop(result);
        let passphrase = Emdb::builder().path(&p).encryption_passphrase("pw").build();
        assert!(matches!(passphrase, Err(Error::InvalidConfig(_))));
        drop(passphrase);
        assert_eq!(
            std::fs::read(sidecar(&p, ".meta")).expect("meta"),
            meta_before,
            "meta sidecar was modified"
        );
        let db = Emdb::open(&p).expect("plain reopen still works");
        assert_eq!(
            db.get("role:mallory").expect("get"),
            Some(b"admin".to_vec())
        );
        drop(db);
        cleanup(&p);
    }

    /// An empty plaintext database (meta written, no records) can still
    /// be turned into an encrypted one by a keyed open, with either a
    /// raw key or a passphrase.
    #[test]
    fn test_keyed_open_of_empty_plaintext_db_initialises_encryption() {
        for use_passphrase in [false, true] {
            let p = tmp_path("empty-then-keyed");
            cleanup(&p);
            drop(Emdb::open(&p).expect("plain create"));
            let build = |p: &Path| {
                let b = Emdb::builder().path(p);
                if use_passphrase {
                    b.encryption_passphrase("pw").build()
                } else {
                    b.encryption_key(KEY).build()
                }
            };
            {
                let db = build(&p).expect("keyed open of empty db");
                db.insert("k", "v").expect("insert");
                db.flush().expect("flush");
            }
            let db = build(&p).expect("keyed reopen");
            assert_eq!(db.get("k").expect("get"), Some(b"v".to_vec()));
            assert!(db.stats().expect("stats").encrypted);
            drop(db);
            assert!(Emdb::open(&p).is_err(), "plain open of encrypted db");
            cleanup(&p);
        }
    }

    /// Deleting the meta sidecar of a passphrase database used to make
    /// the next keyed open write a fresh salt and verify block over it.
    #[test]
    fn test_keyed_open_without_meta_is_refused_without_writing_meta() {
        let p = tmp_path("meta-deleted");
        cleanup(&p);
        {
            let db = Emdb::builder()
                .path(&p)
                .encryption_passphrase("pw")
                .build()
                .expect("open");
            db.insert("k", "v").expect("insert");
            db.flush().expect("flush");
        }
        let meta = sidecar(&p, ".meta");
        let original = std::fs::read(&meta).expect("meta");
        std::fs::remove_file(&meta).expect("remove meta");
        let result = Emdb::builder().path(&p).encryption_passphrase("pw").build();
        assert!(matches!(result, Err(Error::InvalidConfig(_))));
        drop(result);
        assert!(!meta.exists(), "refused open must not write a new meta");
        std::fs::write(&meta, &original).expect("restore meta");
        let db = Emdb::builder()
            .path(&p)
            .encryption_passphrase("pw")
            .build()
            .expect("open after restoring meta");
        assert_eq!(db.get("k").expect("get"), Some(b"v".to_vec()));
        drop(db);
        cleanup(&p);
    }

    /// Clearing the encrypted flag in the meta sidecar (and fixing its
    /// CRC) must not let a plain open treat the database as plaintext.
    #[test]
    fn test_plain_open_with_stripped_meta_flag_is_refused() {
        let p = tmp_path("flag-strip");
        cleanup(&p);
        drop(open_key(&p).expect("create"));
        let meta = sidecar(&p, ".meta");
        let mut m = std::fs::read(&meta).expect("meta");
        m[20] &= !1;
        let crc = crc32_ieee(&m[..108]);
        m[108..112].copy_from_slice(&crc.to_le_bytes());
        std::fs::write(&meta, &m).expect("write meta");
        let result = Emdb::open(&p);
        assert!(
            matches!(result, Err(Error::InvalidConfig(_))),
            "{:?}",
            result.as_ref().map(|_| ())
        );
        cleanup(&p);
    }

    fn crc32_ieee(bytes: &[u8]) -> u32 {
        let mut crc = 0xFFFF_FFFF_u32;
        for &b in bytes {
            crc ^= u32::from(b);
            for _ in 0..8 {
                crc = if crc & 1 != 0 {
                    (crc >> 1) ^ 0xEDB8_8320
                } else {
                    crc >> 1
                };
            }
        }
        !crc
    }

    /// After the key was verified at open, a record whose AEAD tag fails
    /// is damage or tampering, not a wrong key.
    #[test]
    fn test_tampered_record_after_verified_key_returns_corrupted() {
        let p = tmp_path("tamper-record");
        cleanup(&p);
        {
            let db = open_key(&p).expect("open");
            db.insert("a", "1").expect("insert");
            db.insert("b", "2").expect("insert");
            db.flush().expect("flush");
        }
        let mut pl = payloads_of(&p);
        let last = pl[1].len() - 1;
        pl[1][last] ^= 0x01; // flip a bit of the AEAD tag
        std::fs::write(&p, rebuild(&pl)).expect("write");
        assert_corrupted(open_key(&p), "tampered record");

        let wrong = Emdb::builder().path(&p).encryption_key([9_u8; 32]).build();
        assert!(matches!(wrong, Err(Error::EncryptionKeyMismatch)));
        cleanup(&p);
    }

    /// Every record kind an encrypted database writes still opens.
    #[test]
    fn test_encrypted_db_with_every_record_kind_round_trips() {
        for cipher in [emdb::Cipher::Aes256Gcm, emdb::Cipher::ChaCha20Poly1305] {
            let p = tmp_path("enc-all-kinds");
            cleanup(&p);
            {
                let db = Emdb::builder()
                    .path(&p)
                    .encryption_key(KEY)
                    .cipher(cipher)
                    .build()
                    .expect("open");
                db.insert("", "empty key").expect("insert");
                db.insert("k", "").expect("insert");
                let ns = db.namespace("n").expect("ns");
                ns.insert("x", "y").expect("insert");
                let _ = ns.remove("x").expect("remove");
                ns.insert("x", "z").expect("insert");
                db.insert_many(vec![("m1", "1"), ("m2", "2")])
                    .expect("many");
                db.flush().expect("flush");
            }
            {
                let db = open_key(&p).expect("reopen");
                assert_eq!(db.get("").expect("get"), Some(b"empty key".to_vec()));
                assert_eq!(db.get("k").expect("get"), Some(Vec::new()));
                assert_eq!(
                    db.namespace("n").expect("ns").get("x").expect("get"),
                    Some(b"z".to_vec())
                );
                db.compact().expect("compact");
            }
            let db = open_key(&p).expect("reopen after compact");
            assert_eq!(db.get("m2").expect("get"), Some(b"2".to_vec()));
            drop(db);
            cleanup(&p);
        }
    }
}
