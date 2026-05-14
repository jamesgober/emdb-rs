//! At-rest encryption: AES-256-GCM (default) or ChaCha20-Poly1305,
//! with raw-key or Argon2id-derived passphrase. Requires
//! `--features encrypt`.
//!
//! Run with:
//! ```sh
//! cargo run --release --example encryption --features encrypt
//! ```

use emdb::{Cipher, Emdb};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::temp_dir().join("emdb-encryption-example");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;

    // ---- Raw 32-byte key (e.g. from a KMS) ---------------------------
    let key: [u8; 32] = *b"this-is-a-thirty-two-byte-key!!!";
    let path_raw = dir.join("raw-key.emdb");
    {
        let db = Emdb::builder()
            .path(&path_raw)
            .encryption_key(key)
            .build()?;
        db.insert("secret", "ciphertext-on-disk")?;
        db.flush()?;
        println!("[raw-key] wrote secret; on-disk bytes are encrypted");
        drop(db);
    }
    // Reopen with the same key — round-trips cleanly.
    {
        let db = Emdb::builder()
            .path(&path_raw)
            .encryption_key(key)
            .build()?;
        let value = db.get("secret")?.expect("present");
        assert_eq!(&value, b"ciphertext-on-disk");
        println!("[raw-key] reopen with same key: round-trip OK");
    }

    // ---- Passphrase (Argon2id-derived) -------------------------------
    let path_pass = dir.join("passphrase.emdb");
    {
        let db = Emdb::builder()
            .path(&path_pass)
            .encryption_passphrase("correct-horse-battery-staple")
            .cipher(Cipher::ChaCha20Poly1305) // explicit cipher choice
            .build()?;
        db.insert("api-token", "sk_test_xxxxxxxxxxxx")?;
        db.flush()?;
        println!("[passphrase] wrote token; KDF salt persisted in .meta");
        drop(db);
    }
    // Wrong passphrase fails to decrypt — surfaces as a hard error.
    {
        let result = Emdb::builder()
            .path(&path_pass)
            .encryption_passphrase("WRONG-passphrase")
            .cipher(Cipher::ChaCha20Poly1305)
            .build();
        match result {
            Err(err) => println!("[passphrase] wrong key rejected: {err}"),
            Ok(_) => {
                // Some build configurations may surface the error only
                // on the first read instead of at open time. Try a read.
                let db = result.unwrap();
                let read = db.get("api-token");
                assert!(read.is_err(), "decrypt should have failed");
                println!("[passphrase] wrong key rejected on first read");
            }
        }
    }
    // Correct passphrase round-trips.
    {
        let db = Emdb::builder()
            .path(&path_pass)
            .encryption_passphrase("correct-horse-battery-staple")
            .cipher(Cipher::ChaCha20Poly1305)
            .build()?;
        let value = db.get("api-token")?.expect("present");
        assert_eq!(&value, b"sk_test_xxxxxxxxxxxx");
        println!("[passphrase] correct passphrase round-trips OK");
    }

    println!("\nartifacts under {}", dir.display());
    Ok(())
}
