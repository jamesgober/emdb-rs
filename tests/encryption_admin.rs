// Copyright 2026 James Gober. Licensed under Apache-2.0.
//
// Integration tests for the encryption-admin surface: the three
// static methods `Emdb::enable_encryption`, `Emdb::disable_encryption`,
// and `Emdb::rotate_encryption_key`. These are the operational paths
// that downstream users hit on first-time key adoption, key rotation,
// and decommissioning — they need real coverage before the 1.0
// stability commitment locks in.
//
// Also covers the negative paths the rest of the test suite never
// touched: wrong key on reopen, tampered ciphertext, and the
// encryption + TTL feature combination.

#![cfg(all(feature = "encrypt", feature = "ttl"))]

use std::fs;
use std::path::PathBuf;

use emdb::{Cipher, Emdb, EncryptionInput, Error};

const KEY_A: [u8; 32] = *b"alpha-key--32-bytes-exactly-1234";
const KEY_B: [u8; 32] = *b"bravo-key--32-bytes-exactly-5678";

fn tmp_path(label: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |d| d.as_nanos());
    let tid = std::thread::current().id();
    p.push(format!("emdb-enc-admin-{label}-{nanos}-{tid:?}.emdb"));
    p
}

fn cleanup(path: &PathBuf) {
    let _ = fs::remove_file(path);
    let display = path.display();
    let _ = fs::remove_file(format!("{display}.lock"));
    let _ = fs::remove_file(format!("{display}.enc.tmp"));
    let _ = fs::remove_file(format!("{display}.enc.tmp.lock"));
    let _ = fs::remove_file(format!("{display}.encbak"));
    let _ = fs::remove_file(format!("{display}.encbak.lock"));
    let _ = fs::remove_file(format!("{display}.compact.tmp"));
}

// -------------------------------------------------------------
// enable_encryption: plaintext → encrypted round-trip
// -------------------------------------------------------------

#[test]
fn enable_encryption_round_trips_every_record() {
    let path = tmp_path("enable-roundtrip");
    cleanup(&path);

    // Phase 1: write to a plaintext database.
    {
        let db = Emdb::open(&path).expect("open plaintext");
        for i in 0_u32..50 {
            db.insert(format!("k{i:03}"), format!("v{i}"))
                .expect("insert");
        }
        db.flush().expect("flush");
    }

    // Phase 2: enable encryption with a raw key.
    Emdb::enable_encryption(&path, EncryptionInput::Key(KEY_A)).expect("enable_encryption");

    // Phase 3: reopen with the same key and verify every record is
    // intact.
    let db = Emdb::builder()
        .path(&path)
        .encryption_key(KEY_A)
        .build()
        .expect("reopen encrypted");
    assert_eq!(db.len().expect("len"), 50);
    for i in 0_u32..50 {
        assert_eq!(
            db.get(format!("k{i:03}")).expect("get").as_deref(),
            Some(format!("v{i}").as_bytes()),
            "record {i} missing after enable_encryption"
        );
    }
    drop(db);

    // Phase 4: opening without the key now fails (the file is encrypted).
    let open_unencrypted = Emdb::open(&path);
    assert!(
        open_unencrypted.is_err(),
        "opening encrypted file without key should fail"
    );

    cleanup(&path);
}

// -------------------------------------------------------------
// disable_encryption: encrypted → plaintext round-trip
// -------------------------------------------------------------

#[test]
fn disable_encryption_round_trips_every_record() {
    let path = tmp_path("disable-roundtrip");
    cleanup(&path);

    // Phase 1: write to an encrypted database.
    {
        let db = Emdb::builder()
            .path(&path)
            .encryption_key(KEY_A)
            .build()
            .expect("open encrypted");
        for i in 0_u32..30 {
            db.insert(format!("k{i:03}"), format!("v{i}"))
                .expect("insert");
        }
        db.flush().expect("flush");
    }

    // Phase 2: disable encryption (need the source key to decrypt).
    Emdb::disable_encryption(&path, EncryptionInput::Key(KEY_A)).expect("disable_encryption");

    // Phase 3: reopen as plaintext, every record intact.
    let db = Emdb::open(&path).expect("reopen plaintext");
    assert_eq!(db.len().expect("len"), 30);
    for i in 0_u32..30 {
        assert_eq!(
            db.get(format!("k{i:03}")).expect("get").as_deref(),
            Some(format!("v{i}").as_bytes()),
            "record {i} missing after disable_encryption"
        );
    }

    cleanup(&path);
}

// -------------------------------------------------------------
// rotate_encryption_key: round-trip + reject old key
// -------------------------------------------------------------

#[test]
fn rotate_encryption_key_swaps_keys_atomically() {
    let path = tmp_path("rotate-roundtrip");
    cleanup(&path);

    // Phase 1: write to a DB encrypted under KEY_A.
    {
        let db = Emdb::builder()
            .path(&path)
            .encryption_key(KEY_A)
            .build()
            .expect("open with KEY_A");
        for i in 0_u32..20 {
            db.insert(format!("k{i:03}"), format!("v{i}"))
                .expect("insert");
        }
        db.flush().expect("flush");
    }

    // Phase 2: rotate KEY_A → KEY_B.
    Emdb::rotate_encryption_key(
        &path,
        EncryptionInput::Key(KEY_A),
        EncryptionInput::Key(KEY_B),
    )
    .expect("rotate_encryption_key");

    // Phase 3: KEY_B opens and reads cleanly.
    {
        let db = Emdb::builder()
            .path(&path)
            .encryption_key(KEY_B)
            .build()
            .expect("reopen with KEY_B");
        assert_eq!(db.len().expect("len"), 20);
        for i in 0_u32..20 {
            assert_eq!(
                db.get(format!("k{i:03}")).expect("get").as_deref(),
                Some(format!("v{i}").as_bytes())
            );
        }
    }

    // Phase 4: KEY_A is now rejected.
    let stale_open = Emdb::builder().path(&path).encryption_key(KEY_A).build();
    assert!(
        matches!(stale_open, Err(Error::EncryptionKeyMismatch)),
        "old key should be rejected after rotation, got {:?}",
        stale_open.err()
    );

    cleanup(&path);
}

// -------------------------------------------------------------
// Wrong key on reopen
// -------------------------------------------------------------

#[test]
fn wrong_key_on_reopen_surfaces_encryption_key_mismatch() {
    let path = tmp_path("wrong-key");
    cleanup(&path);

    {
        let db = Emdb::builder()
            .path(&path)
            .encryption_key(KEY_A)
            .build()
            .expect("open with KEY_A");
        db.insert("secret", "ciphertext-on-disk").expect("insert");
        db.flush().expect("flush");
    }

    let wrong_open = Emdb::builder().path(&path).encryption_key(KEY_B).build();
    assert!(
        matches!(wrong_open, Err(Error::EncryptionKeyMismatch)),
        "wrong key should surface as EncryptionKeyMismatch, got {:?}",
        wrong_open.err()
    );

    cleanup(&path);
}

// -------------------------------------------------------------
// Tampered ciphertext: flip one byte mid-frame, expect failure
// on read.
// -------------------------------------------------------------

#[test]
fn tampered_ciphertext_is_rejected() {
    let path = tmp_path("tampered");
    cleanup(&path);

    // Write enough records that the journal file is well past any
    // header region — gives the tamper a guaranteed-record-region
    // byte to flip.
    {
        let db = Emdb::builder()
            .path(&path)
            .encryption_key(KEY_A)
            .build()
            .expect("open");
        for i in 0_u32..200 {
            db.insert(
                format!("key-{i:04}"),
                format!("value-{i}-padding-padding-padding-padding"),
            )
            .expect("insert");
        }
        db.flush().expect("flush");
        db.checkpoint().expect("checkpoint");
    }

    // Tamper: flip a byte near the end of the file. With 200 records
    // each ~50 bytes plaintext (plus AEAD overhead, framing, CRC),
    // the file is comfortably > 10 KiB and the last 64 bytes are
    // inside an encrypted record body.
    {
        let mut bytes = fs::read(&path).expect("read file");
        assert!(
            bytes.len() > 4096,
            "file should be > 4 KiB after 200 records, got {} bytes",
            bytes.len()
        );
        let tamper_at = bytes.len() - 32;
        bytes[tamper_at] ^= 0x01;
        fs::write(&path, &bytes).expect("write tampered file");
    }

    // Reopen with the correct key; either open fails outright or
    // some specific record fails to read. Both are acceptable — the
    // contract is "tampering is detected", not "detected at a specific
    // moment".
    let reopen = Emdb::builder().path(&path).encryption_key(KEY_A).build();
    match reopen {
        Err(_) => {
            // Detected at open time via recovery scan or verification.
        }
        Ok(db) => {
            // Scan every record. At least one should report tampering
            // (AEAD authentication failure) — either an Err on read or
            // a missing record where we expect one.
            let mut found_corruption = false;
            for i in 0_u32..200 {
                let key = format!("key-{i:04}");
                match db.get(&key) {
                    Err(_) => {
                        found_corruption = true;
                        break;
                    }
                    Ok(None) => {
                        found_corruption = true;
                        break;
                    }
                    Ok(Some(v)) => {
                        let expected =
                            format!("value-{i}-padding-padding-padding-padding").into_bytes();
                        if v != expected {
                            found_corruption = true;
                            break;
                        }
                    }
                }
            }
            assert!(
                found_corruption,
                "AEAD should have detected the flipped byte"
            );
        }
    }

    cleanup(&path);
}

// -------------------------------------------------------------
// Encryption + TTL combination
// -------------------------------------------------------------

#[test]
fn encryption_plus_ttl_round_trip() {
    use emdb::Ttl;
    use std::time::Duration;

    let path = tmp_path("encrypt-ttl");
    cleanup(&path);

    let db = Emdb::builder()
        .path(&path)
        .encryption_key(KEY_A)
        .default_ttl(Duration::from_secs(3600))
        .build()
        .expect("open");

    db.insert("permanent", "value").expect("insert");
    db.insert_with_ttl("ephemeral", "ghost", Ttl::After(Duration::from_millis(50)))
        .expect("insert_with_ttl");
    db.flush().expect("flush");

    // Both readable immediately.
    assert_eq!(
        db.get("permanent").expect("get").as_deref(),
        Some(b"value".as_slice())
    );
    assert_eq!(
        db.get("ephemeral").expect("get").as_deref(),
        Some(b"ghost".as_slice())
    );

    // Wait past the ephemeral TTL.
    std::thread::sleep(Duration::from_millis(80));
    assert_eq!(
        db.get("permanent").expect("get").as_deref(),
        Some(b"value".as_slice())
    );
    assert!(
        db.get("ephemeral").expect("get").is_none(),
        "ephemeral record should have expired"
    );

    drop(db);
    cleanup(&path);
}

// -------------------------------------------------------------
// Cipher choice: ChaCha20-Poly1305 round-trip
// -------------------------------------------------------------

#[test]
fn chacha20_cipher_round_trip() {
    let path = tmp_path("chacha20");
    cleanup(&path);

    {
        let db = Emdb::builder()
            .path(&path)
            .encryption_key(KEY_A)
            .cipher(Cipher::ChaCha20Poly1305)
            .build()
            .expect("open chacha");
        for i in 0_u32..10 {
            db.insert(format!("k{i}"), format!("v{i}")).expect("insert");
        }
        db.flush().expect("flush");
    }

    let db = Emdb::builder()
        .path(&path)
        .encryption_key(KEY_A)
        .cipher(Cipher::ChaCha20Poly1305)
        .build()
        .expect("reopen chacha");
    for i in 0_u32..10 {
        assert_eq!(
            db.get(format!("k{i}")).expect("get").as_deref(),
            Some(format!("v{i}").as_bytes())
        );
    }
    drop(db);

    cleanup(&path);
}

// -------------------------------------------------------------
// Passphrase-derived key round-trip (Argon2id)
// -------------------------------------------------------------

#[test]
fn passphrase_round_trip() {
    let path = tmp_path("passphrase");
    cleanup(&path);

    {
        let db = Emdb::builder()
            .path(&path)
            .encryption_passphrase("correct-horse-battery-staple")
            .build()
            .expect("open with passphrase");
        db.insert("k", "v").expect("insert");
        db.flush().expect("flush");
    }

    let db = Emdb::builder()
        .path(&path)
        .encryption_passphrase("correct-horse-battery-staple")
        .build()
        .expect("reopen with same passphrase");
    assert_eq!(db.get("k").expect("get").as_deref(), Some(b"v".as_slice()));

    cleanup(&path);
}

#[test]
fn wrong_passphrase_is_rejected() {
    let path = tmp_path("wrong-passphrase");
    cleanup(&path);

    {
        let db = Emdb::builder()
            .path(&path)
            .encryption_passphrase("correct")
            .build()
            .expect("open");
        db.insert("k", "v").expect("insert");
        db.flush().expect("flush");
    }

    let wrong = Emdb::builder()
        .path(&path)
        .encryption_passphrase("WRONG")
        .build();
    assert!(wrong.is_err(), "wrong passphrase should fail; got Ok",);

    cleanup(&path);
}
