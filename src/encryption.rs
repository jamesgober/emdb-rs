// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! AES-256-GCM and ChaCha20-Poly1305 at-rest encryption for the
//! mmap+append storage engine.
//!
//! Active only when the `encrypt` Cargo feature is enabled and a key
//! is supplied via [`crate::EmdbBuilder::encryption_key`] or
//! [`crate::EmdbBuilder::encryption_passphrase`]. Unencrypted files
//! skip every code path here.
//!
//! ## Threat model
//!
//! Targets the "stolen disk" / "shared backup" / "leaked container
//! image" threat models: an adversary who can read raw bytes off the
//! database file but never observes a live process must not be able
//! to recover keys, values, namespace metadata, or recent writes.
//! Anything in RAM (mmap pages, in-flight writes before encryption)
//! is out of scope — that is process-isolation's job, not the storage
//! layer's.
//!
//! ## Cipher
//!
//! AES-256-GCM via `aes-gcm` is the default, hardware-accelerated on
//! every current x86 (AES-NI) and ARM (Crypto Extensions) target.
//! ChaCha20-Poly1305 via `chacha20poly1305` is selectable via
//! [`crate::EmdbBuilder::cipher`] for hardware that lacks AES
//! acceleration. Both use a 32-byte key, 96-bit nonce, 128-bit tag.
//!
//! ## Nonce strategy
//!
//! Every encryption uses a fresh **random** 96-bit nonce drawn from
//! the OS RNG via `rand_core`. The birthday bound for 96-bit random
//! nonces is ≈ 2^48 encryptions before collision risk crosses the
//! NIST-acceptable threshold; that is far beyond what any single emdb
//! database will see in its lifetime. Counter-based nonces were
//! considered and rejected: durable counter state can roll back on
//! restore-from-backup, and rolled-back nonces with the same key are
//! the one mistake AEAD ciphers do not survive.
//!
//! ## Encrypted record framing
//!
//! Each record in the append-only log carries the same outer envelope
//! whether or not the database is encrypted:
//!
//! ```text
//!   [record_len: u32 LE]
//!   [tag: u8]                — bit 7 set when this record is encrypted
//!   [body: record_len-1 bytes]
//!   [crc: u32 LE]            — CRC32 over [tag .. body]
//! ```
//!
//! For encrypted records the body is `[nonce: 12][ciphertext+aead_tag]`
//! and the plaintext payload is the same shape an unencrypted record
//! would have. The CRC catches torn writes; the AEAD tag catches
//! tampering. See [`crate::storage::format`] for the full layout.
//!
//! ## Key verification
//!
//! On a fresh encrypted database, the file header carries an encrypted
//! 32-byte magic plaintext ([`VERIFICATION_PLAINTEXT`]) at offsets
//! 56..116. On open, the engine decrypts that block and compares;
//! mismatch surfaces as [`crate::Error::EncryptionKeyMismatch`] before
//! any user data is touched. Passphrase mode uses Argon2id over a
//! 16-byte salt persisted at header offsets 40..56 to derive the key.

use std::sync::Arc;

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Key as AesKey, Nonce as AesNonce};
use chacha20poly1305::{ChaCha20Poly1305, Key as ChaChaKey, Nonce as ChaChaNonce};
use rand_core::{OsRng, RngCore};
use zeroize::Zeroizing;

use crate::{Error, Result};

/// Key bytes wrapped so they zero on drop. Used for any internal
/// storage of raw key material (builder fields, KDF outputs, the
/// resolved key passed to the cipher constructor). The cipher state
/// itself zeroizes via `aes-gcm` / `chacha20poly1305`'s dependency
/// on `zeroize`, so once the cipher is constructed and the
/// `KeyBytes` drops, no copy of the raw key remains in heap memory.
pub(crate) type KeyBytes = Zeroizing<[u8; 32]>;

/// Length of the AES-GCM nonce in bytes (96-bit random nonce).
pub(crate) const NONCE_LEN: usize = 12;
/// Length of the AES-GCM authentication tag in bytes (128-bit tag).
pub(crate) const TAG_LEN: usize = 16;
/// Per-encryption overhead: nonce + tag.
pub(crate) const ENCRYPTION_OVERHEAD: usize = NONCE_LEN + TAG_LEN;
/// Length of the Argon2id salt persisted in the page-store header.
/// 16 bytes is the OWASP-recommended minimum for password-derived keys
/// and matches the previously-zeroed reserved range at header offset
/// 84..100 — passphrase support fits without a header layout break.
pub(crate) const SALT_LEN: usize = 16;

/// Fixed plaintext written to the verification page on database creation
/// and read back on open. The exact bytes do not matter for security —
/// what matters is that GCM's tag will only validate when the key is
/// correct, so a successful decrypt-and-compare proves the key matches.
/// We pick a recognisable string so the tail of a hex-dumped verification
/// page reveals what it is.
pub(crate) const VERIFICATION_PLAINTEXT: &[u8; 32] =
    b"EMDB-ENCRYPT-OK\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0";

/// Selectable AEAD cipher. Both options use the same 32-byte key, 96-bit
/// nonce, and 128-bit tag, so the on-disk envelope is byte-identical
/// across choices — only the cipher-id bit in the page-store flags
/// differs.
///
/// **Default:** [`Cipher::Aes256Gcm`]. Modern x86 (AES-NI) and ARMv8
/// (Crypto Extensions) targets accelerate AES in hardware, beating
/// ChaCha20-Poly1305 on raw throughput by 2–4×.
///
/// **Pick [`Cipher::ChaCha20Poly1305`]** when the target platform
/// lacks hardware AES (older ARM, some embedded targets) — the
/// software ChaCha20 implementation is faster than software AES, and
/// it is constant-time by construction (no cache-timing surface).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum Cipher {
    /// AES-256-GCM. Hardware-accelerated on every modern x86 / ARMv8
    /// target.
    #[default]
    Aes256Gcm,
    /// ChaCha20-Poly1305. Pure software, faster on hardware without
    /// AES-NI / Crypto Extensions.
    ChaCha20Poly1305,
}

/// Internal cipher dispatch. Both arms expose the same surface
/// (32-byte key, 12-byte nonce, 16-byte tag) so the rest of the
/// engine treats the choice as opaque. The cipher state structs
/// are large (≈1 KB of expanded round keys for AES; smaller for
/// ChaCha but the variant size dominates) so they live behind
/// `Box`es to keep `EncryptionContext` small enough to satisfy
/// `clippy::large_enum_variant`.
#[derive(Clone)]
enum CipherImpl {
    Aes(Box<Aes256Gcm>),
    ChaCha(Box<ChaCha20Poly1305>),
}

/// Cached AEAD cipher state. Cheap to `Arc<EncryptionContext>`-share
/// between the engine and any worker that needs to encrypt or
/// decrypt records.
#[derive(Clone)]
pub(crate) struct EncryptionContext {
    cipher: CipherImpl,
    /// Cipher kind, stored so the file-header writer can mark the
    /// file with the correct cipher bit on creation.
    kind: Cipher,
}

impl std::fmt::Debug for EncryptionContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptionContext")
            .field("key", &"<redacted>")
            .field("cipher", &self.kind)
            .finish()
    }
}

impl EncryptionContext {
    /// Construct a context from a 32-byte raw key, defaulting to
    /// [`Cipher::Aes256Gcm`]. Used by the existing test helpers and
    /// by callers that don't care about cipher choice.
    pub(crate) fn from_key(key: &[u8; 32]) -> Self {
        Self::from_key_with_cipher(key, Cipher::Aes256Gcm)
    }

    /// Construct a context from a 32-byte raw key and an explicit
    /// cipher choice. Used by the engine when reopening a file —
    /// the cipher is read from the page-store header so the same
    /// AEAD that wrote the bytes is the one that decrypts them.
    pub(crate) fn from_key_with_cipher(key: &[u8; 32], kind: Cipher) -> Self {
        let cipher = match kind {
            Cipher::Aes256Gcm => CipherImpl::Aes(Box::new(Aes256Gcm::new(
                AesKey::<Aes256Gcm>::from_slice(key),
            ))),
            Cipher::ChaCha20Poly1305 => {
                CipherImpl::ChaCha(Box::new(ChaCha20Poly1305::new(ChaChaKey::from_slice(key))))
            }
        };
        Self { cipher, kind }
    }

    /// Cipher this context will encrypt/decrypt with.
    pub(crate) fn kind(&self) -> Cipher {
        self.kind
    }

    /// Encrypt `plaintext`, returning `nonce_bytes || ciphertext` where
    /// `ciphertext.len() == plaintext.len() + TAG_LEN` (the AEAD tag is
    /// appended by the cipher implementation).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Encryption`] on AEAD failure. AEAD failure is
    /// catastrophic (key/cipher invariant violation) and not user-
    /// recoverable; the database is unsafe to continue using.
    pub(crate) fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>> {
        let mut nonce_bytes = [0_u8; NONCE_LEN];
        OsRng.fill_bytes(&mut nonce_bytes);
        let ciphertext = match &self.cipher {
            CipherImpl::Aes(c) => c
                .encrypt(AesNonce::from_slice(&nonce_bytes), plaintext)
                .map_err(|_| Error::Encryption("aead encrypt failed"))?,
            CipherImpl::ChaCha(c) => c
                .encrypt(ChaChaNonce::from_slice(&nonce_bytes), plaintext)
                .map_err(|_| Error::Encryption("aead encrypt failed"))?,
        };

        let mut out = Vec::with_capacity(NONCE_LEN + ciphertext.len());
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    /// Decrypt a buffer produced by [`Self::encrypt`]. Splits the leading
    /// 12-byte nonce from the AEAD ciphertext, authenticates, and
    /// returns the plaintext.
    ///
    /// # Errors
    ///
    /// Returns [`Error::EncryptionKeyMismatch`] when the AEAD tag fails
    /// to verify — either the bytes were tampered with, the supplied
    /// key is wrong, or the wrong cipher was used (e.g. an AES-GCM
    /// reader against a ChaCha20-Poly1305-encrypted file). Returns
    /// [`Error::Encryption`] on a malformed buffer (too short to hold
    /// nonce + tag).
    pub(crate) fn decrypt(&self, encrypted: &[u8]) -> Result<Vec<u8>> {
        if encrypted.len() < NONCE_LEN + TAG_LEN {
            return Err(Error::Encryption(
                "encrypted buffer too short to hold nonce + tag",
            ));
        }
        let (nonce_bytes, ciphertext) = encrypted.split_at(NONCE_LEN);
        match &self.cipher {
            CipherImpl::Aes(c) => c
                .decrypt(AesNonce::from_slice(nonce_bytes), ciphertext)
                .map_err(|_| Error::EncryptionKeyMismatch),
            CipherImpl::ChaCha(c) => c
                .decrypt(ChaChaNonce::from_slice(nonce_bytes), ciphertext)
                .map_err(|_| Error::EncryptionKeyMismatch),
        }
    }
}

/// Optional encryption context shared by the engine across every
/// record encrypt / decrypt site. `None` means the database is
/// unencrypted and every encryption code path becomes a no-op.
pub(crate) type SharedEncryption = Option<Arc<EncryptionContext>>;

/// User-supplied keying material for the offline admin operations
/// [`crate::Emdb::enable_encryption`] / [`crate::Emdb::disable_encryption`] /
/// [`crate::Emdb::rotate_encryption_key`] and for the CLI tool.
///
/// Same pair of inputs the builder accepts: a raw 32-byte key (e.g. from
/// a KMS) or a UTF-8 passphrase fed through Argon2id.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum EncryptionInput {
    /// Raw 32-byte AES-256 key. Used as-is.
    Key([u8; 32]),
    /// UTF-8 passphrase derived to a 32-byte AES-256 key via Argon2id.
    /// On a fresh database the salt is generated; on a reopen it is
    /// read from the page-store header.
    Passphrase(String),
}

/// Generate a fresh 16-byte Argon2id salt from the OS RNG. Called at
/// database creation time; the salt is persisted in the page-store
/// header and is **not secret** — it just needs to be unique per
/// database so the same passphrase produces different keys for
/// different databases.
pub(crate) fn random_salt() -> [u8; SALT_LEN] {
    let mut out = [0_u8; SALT_LEN];
    OsRng.fill_bytes(&mut out);
    out
}

/// Derive a 32-byte AES-256 key from a UTF-8 passphrase plus a
/// per-database salt via Argon2id with the parameters listed below.
/// The same passphrase and salt always produce the same key, which
/// is the property we rely on for "open, validate verification block,
/// succeed".
///
/// ## Parameters
///
/// - **Variant:** Argon2id (the OWASP-recommended default — hybrid of
///   the i and d variants, resistant to both side-channel and
///   GPU-cracking attacks).
/// - **Memory cost (m_cost):** 19 MiB (19_456 KiB) — OWASP minimum
///   for interactive scenarios. Tunable upward for high-security
///   deployments by re-deriving with a larger value if we ever expose
///   `EmdbBuilder::kdf_memory_cost`.
/// - **Time cost (t_cost):** 2 iterations.
/// - **Parallelism (p_cost):** 1 — single-threaded; we are not in a
///   throughput-critical path (this runs once per `open`).
/// - **Output length:** 32 bytes (matches AES-256 key size).
///
/// On a typical desktop CPU, this takes ~50–150 ms — long enough to
/// frustrate offline brute-force, short enough that interactive opens
/// stay snappy.
///
/// # Errors
///
/// Returns [`Error::Encryption`] when Argon2 itself reports an
/// error. Argon2 only fails on impossible parameter combinations (we
/// hardcode valid ones), so this branch is unreachable in practice.
pub(crate) fn derive_key_from_passphrase(
    passphrase: &str,
    salt: &[u8; SALT_LEN],
) -> Result<KeyBytes> {
    use argon2::{Algorithm, Argon2, Params, Version};

    if passphrase.is_empty() {
        return Err(Error::InvalidConfig(
            "encryption_passphrase must not be empty",
        ));
    }

    // 19 MiB / 2 iterations / 1 lane / 32-byte output. Hardcoded so
    // every emdb caller derives the same key from the same
    // (passphrase, salt) pair regardless of build / version. If we
    // ever raise the cost factor we have to add a kdf_version field
    // to the header so old files keep deriving with the original
    // parameters.
    let params = Params::new(19_456, 2, 1, Some(32))
        .map_err(|_| Error::Encryption("argon2 params construction failed"))?;
    let argon = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);

    // Derive into a `Zeroizing<[u8; 32]>` so the bytes clear on
    // drop. The argon2 `hash_password_into` call writes through the
    // mutable reference; once the caller hands the wrapper to the
    // cipher constructor and lets it drop, nothing readable from the
    // KDF remains on the heap.
    let mut key: KeyBytes = Zeroizing::new([0_u8; 32]);
    argon
        .hash_password_into(passphrase.as_bytes(), salt, key.as_mut_slice())
        .map_err(|_| Error::Encryption("argon2 key derivation failed"))?;
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::{EncryptionContext, VERIFICATION_PLAINTEXT};
    use crate::Error;

    fn key_a() -> [u8; 32] {
        let mut k = [0_u8; 32];
        for (i, b) in k.iter_mut().enumerate() {
            *b = i as u8;
        }
        k
    }

    fn key_b() -> [u8; 32] {
        [0xFF_u8; 32]
    }

    #[test]
    fn round_trip_recovers_plaintext() {
        let ctx = EncryptionContext::from_key(&key_a());
        let plaintext = b"the quick brown fox jumps over the lazy dog";
        let ct = match ctx.encrypt(plaintext) {
            Ok(c) => c,
            Err(err) => panic!("encrypt should succeed: {err}"),
        };
        // Output is always nonce (12) + plaintext_len + tag (16).
        assert_eq!(ct.len(), 12 + plaintext.len() + 16);

        let pt = match ctx.decrypt(&ct) {
            Ok(p) => p,
            Err(err) => panic!("decrypt should succeed: {err}"),
        };
        assert_eq!(pt, plaintext);
    }

    #[test]
    fn distinct_nonces_for_repeated_calls() {
        // Random nonces: encrypting the same plaintext twice produces
        // two different ciphertexts. Catches a regression that
        // accidentally hardcodes a fixed nonce.
        let ctx = EncryptionContext::from_key(&key_a());
        let pt = b"identical-input";
        let ct1 = ctx.encrypt(pt).unwrap_or_else(|err| panic!("{err}"));
        let ct2 = ctx.encrypt(pt).unwrap_or_else(|err| panic!("{err}"));
        assert_ne!(ct1, ct2, "repeated encryption must use fresh nonces");
        // Both decrypt to the same plaintext.
        let pt1 = ctx.decrypt(&ct1).unwrap_or_else(|err| panic!("{err}"));
        let pt2 = ctx.decrypt(&ct2).unwrap_or_else(|err| panic!("{err}"));
        assert_eq!(pt1, pt2);
        assert_eq!(pt1.as_slice(), pt);
    }

    #[test]
    fn wrong_key_fails_with_mismatch_error() {
        let producer = EncryptionContext::from_key(&key_a());
        let consumer = EncryptionContext::from_key(&key_b());
        let ct = producer
            .encrypt(b"secret")
            .unwrap_or_else(|err| panic!("{err}"));
        let result = consumer.decrypt(&ct);
        assert!(matches!(result, Err(Error::EncryptionKeyMismatch)));
    }

    #[test]
    fn tampered_ciphertext_fails_with_mismatch_error() {
        let ctx = EncryptionContext::from_key(&key_a());
        let mut ct = ctx
            .encrypt(b"do not modify")
            .unwrap_or_else(|err| panic!("{err}"));
        // Flip a bit in the middle of the ciphertext (after the 12-byte
        // nonce). GCM's tag must catch this.
        ct[15] ^= 0x01;
        let result = ctx.decrypt(&ct);
        assert!(
            matches!(result, Err(Error::EncryptionKeyMismatch)),
            "tampered ciphertext must fail authentication: {result:?}"
        );
    }

    #[test]
    fn truncated_buffer_fails_with_encryption_error() {
        let ctx = EncryptionContext::from_key(&key_a());
        let too_short = [0_u8; 10]; // less than nonce + tag = 28
        let result = ctx.decrypt(&too_short);
        assert!(matches!(result, Err(Error::Encryption(_))));
    }

    #[test]
    fn verification_plaintext_is_thirty_two_bytes() {
        // The verification page format depends on this being exactly
        // 32 bytes. Catch a typo refactor that breaks it.
        assert_eq!(VERIFICATION_PLAINTEXT.len(), 32);
    }

    #[test]
    fn debug_does_not_leak_key() {
        let ctx = EncryptionContext::from_key(&key_a());
        let debug_str = format!("{ctx:?}");
        assert!(
            !debug_str.contains("\\x01\\x02"),
            "Debug output must not leak key bytes: {debug_str}"
        );
        assert!(debug_str.contains("redacted"));
    }

    #[test]
    fn kdf_is_deterministic_for_fixed_passphrase_and_salt() {
        let salt = [0xAA_u8; super::SALT_LEN];
        let k1 = match super::derive_key_from_passphrase("hunter2", &salt) {
            Ok(k) => k,
            Err(err) => panic!("derive should succeed: {err}"),
        };
        let k2 = match super::derive_key_from_passphrase("hunter2", &salt) {
            Ok(k) => k,
            Err(err) => panic!("derive should succeed: {err}"),
        };
        assert_eq!(k1, k2, "same passphrase + salt must produce same key");
    }

    #[test]
    fn kdf_diverges_for_different_salts() {
        let s1 = [0x11_u8; super::SALT_LEN];
        let s2 = [0x22_u8; super::SALT_LEN];
        let k1 =
            super::derive_key_from_passphrase("hunter2", &s1).unwrap_or_else(|e| panic!("{e}"));
        let k2 =
            super::derive_key_from_passphrase("hunter2", &s2).unwrap_or_else(|e| panic!("{e}"));
        assert_ne!(k1, k2, "different salts must produce different keys");
    }

    #[test]
    fn kdf_diverges_for_different_passphrases() {
        let salt = [0x33_u8; super::SALT_LEN];
        let k1 =
            super::derive_key_from_passphrase("alpha", &salt).unwrap_or_else(|e| panic!("{e}"));
        let k2 =
            super::derive_key_from_passphrase("bravo", &salt).unwrap_or_else(|e| panic!("{e}"));
        assert_ne!(k1, k2, "different passphrases must produce different keys");
    }

    #[test]
    fn kdf_rejects_empty_passphrase() {
        let salt = [0x44_u8; super::SALT_LEN];
        let result = super::derive_key_from_passphrase("", &salt);
        assert!(matches!(result, Err(Error::InvalidConfig(_))));
    }

    #[test]
    fn random_salt_is_fresh_each_call() {
        // Defends against an accidental hardcoded salt.
        let s1 = super::random_salt();
        let s2 = super::random_salt();
        assert_ne!(s1, s2, "random_salt must use the OS RNG");
    }
}
