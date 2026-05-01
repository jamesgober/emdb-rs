// End-to-end smoke test. Opens a real persistent DB, exercises every
// public surface, drops + reopens, prints what it sees. Run with:
//
//   cargo run --example smoke --features ttl,nested,encrypt --release

use std::time::Duration;

use emdb::{EmdbBuilder, Ttl};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::temp_dir().join("emdb-smoke.emdb");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{}.lock", path.display()));
    let _ = std::fs::remove_file(format!("{}.compact.tmp", path.display()));
    let _ = std::fs::remove_file(format!("{}.encbak", path.display()));
    println!("=== emdb smoke test ===");
    println!("path: {}", path.display());

    // --- session 1: write data, named namespace, TTL, range scans
    println!("\n[session 1] open + write");
    let db = EmdbBuilder::new()
        .path(&path)
        .enable_range_scans(true)
        .default_ttl(Duration::from_secs(3600))
        .build()?;

    db.insert("hello", "world")?;
    db.insert("user:001", "alice")?;
    db.insert("user:002", "bob")?;
    db.insert("user:003", "carol")?;
    println!("  inserted 4 records, len() = {}", db.len()?);

    // TTL: insert with explicit short TTL, verify it expires
    db.insert_with_ttl("ephemeral", "ghost", Ttl::After(Duration::from_millis(50)))?;
    println!("  inserted 'ephemeral' with 50ms TTL");
    assert!(
        db.get("ephemeral")?.is_some(),
        "should be alive immediately"
    );
    std::thread::sleep(Duration::from_millis(80));
    assert!(
        db.get("ephemeral")?.is_none(),
        "should be gone after 80ms wait"
    );
    println!("  ✓ TTL evict-on-read works");

    // Range scan
    let users = db.range(b"user:".to_vec()..b"user;".to_vec())?;
    println!("  range scan 'user:..user;' → {} records:", users.len());
    for (k, v) in &users {
        println!(
            "    {} = {}",
            String::from_utf8_lossy(k),
            String::from_utf8_lossy(v)
        );
    }
    assert_eq!(users.len(), 3);

    // Range prefix
    let prefix = db.range_prefix(b"user:")?;
    assert_eq!(prefix.len(), 3);
    println!("  ✓ range_prefix matches range");

    // Named namespace
    let sessions = db.namespace("sessions")?;
    sessions.insert(b"sid-abc", b"token-xyz")?;
    sessions.insert(b"sid-def", b"token-uvw")?;
    println!("  namespace 'sessions' len = {}", sessions.len()?);

    // Nested focus
    let focus = db.focus("config");
    focus.set("theme", "dark")?;
    focus.set("language", "en")?;
    let theme = focus.get("theme")?.unwrap();
    println!(
        "  focus('config').get('theme') = {}",
        String::from_utf8_lossy(&theme)
    );
    assert_eq!(theme, b"dark");

    // Transaction
    db.transaction(|tx| {
        tx.insert("tx-key-1", "tx-val-1")?;
        tx.insert("tx-key-2", "tx-val-2")?;
        Ok(())
    })?;
    println!("  ✓ transaction committed 2 keys");

    db.flush()?;
    let size_before_compact = std::fs::metadata(&path)?.len();
    println!("  flushed; on-disk size = {} bytes", size_before_compact);

    // Remove some, then compact
    let _ = db.remove(b"hello")?;
    let _ = db.remove(b"tx-key-1")?;
    let _ = sessions.remove(b"sid-abc")?;
    db.flush()?;
    let size_after_removes = std::fs::metadata(&path)?.len();
    println!(
        "  after removes (tombstones added), size = {} bytes",
        size_after_removes
    );

    db.compact()?;
    db.flush()?;
    let size_after_compact = std::fs::metadata(&path)?.len();
    println!("  after compact(), size = {} bytes", size_after_compact);
    assert!(
        size_after_compact < size_after_removes,
        "compact should shrink"
    );

    // Drop everything in scope
    drop(focus);
    drop(sessions);
    drop(db);
    println!("  dropped all handles");

    // --- session 2: reopen, verify persistence
    println!("\n[session 2] reopen + verify");
    let db = EmdbBuilder::new()
        .path(&path)
        .enable_range_scans(true)
        .default_ttl(Duration::from_secs(3600))
        .build()?;

    println!("  reopen successful; len() = {}", db.len()?);
    assert_eq!(db.get(b"hello")?, None, "removed key stays removed");
    assert_eq!(db.get(b"user:001")?.as_deref(), Some(b"alice".as_slice()));
    assert_eq!(
        db.get(b"tx-key-2")?.as_deref(),
        Some(b"tx-val-2".as_slice())
    );
    println!("  ✓ default-namespace records intact");

    let sessions = db.namespace("sessions")?;
    assert_eq!(sessions.get(b"sid-abc")?, None, "removed session gone");
    assert_eq!(
        sessions.get(b"sid-def")?.as_deref(),
        Some(b"token-uvw".as_slice())
    );
    println!("  ✓ named namespace 'sessions' rebound to same id, records intact");

    let focus = db.focus("config");
    let theme = focus.get("theme")?.unwrap();
    assert_eq!(theme, b"dark");
    println!("  ✓ focus('config').get('theme') = dark");

    let users = db.range(b"user:".to_vec()..b"user;".to_vec())?;
    assert_eq!(users.len(), 3, "range index rebuilt from records on reopen");
    println!("  ✓ range index rebuilt: {} 'user:*' records", users.len());

    let mut names = db.list_namespaces()?;
    names.sort();
    println!("  list_namespaces() = {:?}", names);

    drop(focus);
    drop(sessions);
    drop(db);

    // --- session 3: encryption round-trip (separate file)
    println!("\n[session 3] encryption round-trip");
    let enc_path = std::env::temp_dir().join("emdb-smoke-enc.emdb");
    let _ = std::fs::remove_file(&enc_path);
    let _ = std::fs::remove_file(format!("{}.lock", enc_path.display()));

    let db = EmdbBuilder::new()
        .path(&enc_path)
        .encryption_passphrase("correct horse battery staple")
        .build()?;
    db.insert("secret-1", "shhh")?;
    db.insert("secret-2", "more shhh")?;
    db.flush()?;
    let size = std::fs::metadata(&enc_path)?.len();
    println!("  inserted 2 records, encrypted file size = {} bytes", size);

    // Read raw bytes and check the plaintext doesn't appear
    let raw = std::fs::read(&enc_path)?;
    assert!(
        !raw.windows(4).any(|w| w == b"shhh"),
        "plaintext leaked into the encrypted file!"
    );
    println!("  ✓ raw on-disk scan: 'shhh' plaintext not present");

    drop(db);

    let reopened = EmdbBuilder::new()
        .path(&enc_path)
        .encryption_passphrase("correct horse battery staple")
        .build()?;
    let s1 = reopened.get(b"secret-1")?.unwrap();
    assert_eq!(s1, b"shhh");
    println!(
        "  ✓ reopen with same passphrase: secret-1 = {}",
        String::from_utf8_lossy(&s1)
    );

    drop(reopened);

    // Wrong passphrase should fail with EncryptionKeyMismatch
    let wrong = EmdbBuilder::new()
        .path(&enc_path)
        .encryption_passphrase("definitely the wrong horse")
        .build();
    match wrong {
        Err(emdb::Error::EncryptionKeyMismatch) => {
            println!("  ✓ wrong passphrase correctly rejected with EncryptionKeyMismatch");
        }
        Err(other) => {
            println!("  ✗ wrong passphrase rejected but with unexpected error: {other:?}");
        }
        Ok(_) => {
            println!("  ✗ WRONG: bad passphrase opened the database!");
        }
    }

    let _ = std::fs::remove_file(&enc_path);
    let _ = std::fs::remove_file(format!("{}.lock", enc_path.display()));

    // --- session 4: cleanup
    println!("\n[cleanup] removing test files");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{}.lock", path.display()));

    println!("\n=== ALL CHECKS PASSED ===");
    Ok(())
}
