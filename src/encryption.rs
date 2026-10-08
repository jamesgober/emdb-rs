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
//! image" threat models: an adversary who obtains a copy of the
//! database file must not be able to recover keys, values, or
//! namespace names. What the layer provides, and what it does not:
//!
//! - **Confidentiality** of every record body (key, value, TTL,
//!   namespace id and namespace name) under a 256-bit key.
//! - **Per-record authenticity.** Each record body carries its own
//!   AEAD tag, so a modified ciphertext fails to decrypt. A
//!   database opened with a key rejects any record that is not
//!   encrypted.
//! - **No log integrity.** Records are authenticated one at a time,
//!   not as a sequence. An adversary who can write the file can
//!   delete frames, reorder them, replay an older encrypted record
//!   (rolling a key back to a previous value), or truncate the log,
//!   and none of this is detected. Restoring an old copy of the
//!   whole file is likewise undetected.
//! - **The record kind byte is not covered by the AEAD tag** in the
//!   1.0 on-disk format. emdb 1.0.3 rejects record bodies whose
//!   length does not match their kind and namespace bindings that
//!   contradict earlier ones, which turns every kind swap into an
//!   open-time error except one narrow case (a remove of a key whose
//!   bytes equal its own namespace's name). Binding the tag, the
//!   database identity and the record position into the AEAD
//!   associated data requires an on-disk format revision and is
//!   planned for 1.1.
//! - **Visible metadata.** Record sizes, record count, record order
//!   and kind (insert / remove / namespace name), the meta sidecar
//!   fields (flags, cipher choice, creation time, Argon2 salt) and
//!   the lock holder's PID and start time in `<path>.lock-meta` are
//!   all stored in the clear.
//! - **Out of scope:** anything in process memory (mmap pages,
//!   decrypted values handed to the caller, in-flight writes before
//!   encryption). That is process isolation's job.
//!
//! ## Cipher
//!
//! AES-256-GCM via `aes-gcm` is the default, hardware-accelerated on
//! every current x86 (AES-NI) and ARM (Crypto Extensions) target.
//! ChaCha20-Poly1305 via `chacha20poly1305` is selectable via
//! [`crate::EmdbBuilder::cipher`] for hardware that lacks AES
//! acceleration. Both use a 32-byte key, 96-bit nonce, 128-bit tag.
//! There is no data-encryption-key / key-encryption-key split: the
//! supplied (or derived) key encrypts every record directly, so a key
//! rotation rewrites every record.
//!
//! ## Nonce strategy
//!
//! Every encryption uses a fresh **random** 96-bit nonce drawn from
//! the OS RNG via `rand_core`. Random nonces make reuse improbable,
//! not impossible: by the birthday bound the chance of any collision
//! among `n` nonces is about `n^2 / 2^97`. NIST SP 800-38D caps
//! random-nonce AES-GCM at 2^32 encryptions per key to keep that
//! probability below 2^-32. Every insert, remove, namespace creation,
//! compaction rewrite and backup counts against the budget, so a
//! database that will see billions of writes under one key should be
//! rotated with [`crate::Emdb::rotate_encryption_key`] well before
//! 2^32 total writes. Counter-based nonces were considered and
//! rejected: durable counter state can roll back on
//! restore-from-backup, and rolled-back nonces with the same key are
//! the one mistake AEAD ciphers do not survive.
//!
//! ## Encrypted record framing
//!
//! Records live inside fsys journal frames:
//!
//! ```text
//!   [magic: u32 BE][payload_len: u32 LE][payload][crc32c: u32 LE]
//!   payload = [tag: u8][body]   tag bit 7 set when the body is encrypted
//! ```
//!
//! For encrypted records the body is `[nonce: 12][ciphertext+aead_tag]`
//! and the plaintext under the ciphertext has the same shape an
//! unencrypted body of the same kind would have. The CRC-32C catches
//! torn writes and accidental corruption; the AEAD tag catches
//! modification of the body. See [`crate::storage::format`] for the
//! body layouts.
//!
//! ## Key verification
//!
//! The `<path>.meta` sidecar carries an encrypted 32-byte magic
//! plaintext ([`VERIFICATION_PLAINTEXT`]) at offsets 48..108 (nonce,
//! ciphertext, tag). On open, the engine decrypts that block and
//! compares; a mismatch surfaces as
//! [`crate::Error::EncryptionKeyMismatch`] before any user data is
//! touched. Once the key has been verified, a record that fails AEAD
//! authentication is reported as [`crate::Error::Corrupted`]: the key
//! is right, so the record bytes were modified or damaged. Passphrase
//! mode uses Argon2id over a 16-byte salt persisted at sidecar offsets
//! 32..48 to derive the key.

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Key as AesKey, Nonce as AesNonce};
use chacha20poly1305::{ChaCha20Poly1305, Key as ChaChaKey, Nonce as ChaChaNonce};
use rand_core::{OsRng, RngCore};
use zeroize::Zeroizing;

use crate::storage::format::TAG_ENCRYPTED_FLAG;
use crate::{Error, Result};

/// Key bytes wrapped so they zero on drop. Used for any internal
/// storage of raw key material (builder fields, KDF outputs, the
/// resolved key passed to the cipher constructor). The expanded
/// cipher state zeroizes on drop too: the crate enables the `zeroize`
/// features of `aes`, `ghash`, `polyval`, `chacha20` and `poly1305`,
/// so once the cipher and the `KeyBytes` drop, no copy of the key or
/// its schedule remains in memory that emdb owns.
pub(crate) type KeyBytes = Zeroizing<[u8; 32]>;

/// Passphrase text wrapped so it is wiped on drop. The public API
/// takes `impl Into<String>`; emdb moves the string into this wrapper
/// as soon as it receives it.
pub(crate) type Passphrase = Zeroizing<String>;

/// Length of the AES-GCM nonce in bytes (96-bit random nonce).
pub(crate) const NONCE_LEN: usize = 12;
/// Length of the AES-GCM authentication tag in bytes (128-bit tag).
pub(crate) const TAG_LEN: usize = 16;
/// Length of the Argon2id salt persisted in the meta sidecar
/// (offsets 32..48). 16 bytes is the OWASP-recommended minimum for
/// password-derived keys.
pub(crate) const SALT_LEN: usize = 16;

/// Fixed plaintext written to the verification block on database
/// creation and read back on open. The exact bytes do not matter for
/// security: the AEAD tag only validates when the key is correct, so a
/// successful decrypt-and-compare proves the key matches. The string is
/// recognisable so a hex dump of a decrypted block reveals what it is.
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
    /// Cipher kind, reported by the redacting `Debug` impl.
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
    /// Construct a context from a 32-byte raw key with
    /// [`Cipher::Aes256Gcm`]. Test and fuzz helper.
    #[cfg(any(test, fuzzing))]
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

    /// Encrypt `plaintext`, returning `nonce_bytes || ciphertext` where
    /// `ciphertext.len() == plaintext.len() + TAG_LEN` (the AEAD tag is
    /// appended by the cipher implementation).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Encryption`] when the OS RNG cannot supply a
    /// nonce, or on AEAD failure. AEAD failure is a key/cipher
    /// invariant violation and not user-recoverable; the database is
    /// unsafe to continue using.
    pub(crate) fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>> {
        let mut nonce_bytes = [0_u8; NONCE_LEN];
        fill_random(&mut nonce_bytes)?;
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

    /// Build a complete encrypted record payload,
    /// `[kind | TAG_ENCRYPTED_FLAG][nonce][ciphertext + tag]`.
    ///
    /// `encode` writes the plaintext record body into a scratch buffer
    /// that is wiped before this call returns, so the plaintext body
    /// (key, value, namespace name) does not linger in freed heap
    /// memory. `kind` is one of the `format::TAG_*` kind values
    /// without the encrypted flag.
    ///
    /// # Errors
    ///
    /// Same as [`Self::encrypt`].
    pub(crate) fn seal_record<F>(&self, kind: u8, encode: F) -> Result<Vec<u8>>
    where
        F: FnOnce(&mut Vec<u8>),
    {
        let mut plain: Zeroizing<Vec<u8>> = Zeroizing::new(Vec::with_capacity(64));
        encode(&mut plain);
        let nonce_then_ct = self.encrypt(&plain)?;
        let mut payload = Vec::with_capacity(1 + nonce_then_ct.len());
        payload.push(kind | TAG_ENCRYPTED_FLAG);
        payload.extend_from_slice(&nonce_then_ct);
        Ok(payload)
    }

    /// Decrypt a buffer produced by [`Self::encrypt`]. Splits the leading
    /// 12-byte nonce from the AEAD ciphertext, authenticates, and
    /// returns the plaintext.
    ///
    /// # Errors
    ///
    /// Returns [`Error::EncryptionKeyMismatch`] when the AEAD tag fails
    /// to verify: either the bytes were modified, the supplied key is
    /// wrong, or the wrong cipher was used (e.g. an AES-GCM reader
    /// against a ChaCha20-Poly1305-encrypted file). The record decoder
    /// [`crate::storage::format::decode_payload_encrypted`] runs only
    /// after the key was verified at open and reports this case as
    /// [`Error::Corrupted`] instead. Returns [`Error::Encryption`] on a
    /// malformed buffer (too short to hold nonce + tag).
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

/// User-supplied keying material for the offline admin operations
/// [`crate::Emdb::enable_encryption`] / [`crate::Emdb::disable_encryption`] /
/// [`crate::Emdb::rotate_encryption_key`] and for the CLI tool.
///
/// Same pair of inputs the builder accepts: a raw 32-byte key (e.g. from
/// a KMS) or a UTF-8 passphrase fed through Argon2id.
///
/// The `Debug` output never shows the key bytes or the passphrase.
/// emdb wipes its own internal copies of the key and passphrase when it
/// is done with them; this value belongs to the caller, who should wipe
/// it (for example with the `zeroize` crate) once the admin call
/// returns.
#[derive(Clone)]
#[non_exhaustive]
pub enum EncryptionInput {
    /// Raw 32-byte AES-256 key. Used as-is.
    Key([u8; 32]),
    /// UTF-8 passphrase derived to a 32-byte AES-256 key via Argon2id.
    /// On a fresh database the salt is generated; on a reopen it is
    /// read from the meta sidecar.
    Passphrase(String),
}

impl std::fmt::Debug for EncryptionInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Key(_) => f.write_str("Key(<redacted>)"),
            Self::Passphrase(_) => f.write_str("Passphrase(<redacted>)"),
        }
    }
}

/// Fill `out` from the OS RNG. An RNG failure (for example a sandbox
/// without a usable entropy source) is reported as an error;
/// `RngCore::fill_bytes` on `OsRng` would panic instead.
fn fill_random(out: &mut [u8]) -> Result<()> {
    OsRng
        .try_fill_bytes(out)
        .map_err(|_| Error::Encryption("operating system random number generator failed"))
}

/// Generate a fresh 16-byte Argon2id salt from the OS RNG. Called at
/// database creation time; the salt is persisted in the meta sidecar
/// and is **not secret**. It only needs to be unique per database so
/// the same passphrase produces different keys for different databases.
///
/// # Errors
///
/// Returns [`Error::Encryption`] when the OS RNG fails.
pub(crate) fn random_salt() -> Result<[u8; SALT_LEN]> {
    let mut out = [0_u8; SALT_LEN];
    fill_random(&mut out)?;
    Ok(out)
}

/// Derive a 32-byte AES-256 key from a UTF-8 passphrase plus a
/// per-database salt via Argon2id with the parameters listed below.
/// The same passphrase and salt always produce the same key, which
/// is the property we rely on for "open, validate verification block,
/// succeed".
///
/// ## Parameters
///
/// - **Variant:** Argon2id, version 0x13.
/// - **Memory cost (m_cost):** 19 MiB (19_456 KiB), the OWASP
///   minimum recommendation for Argon2id.
/// - **Time cost (t_cost):** 2 iterations.
/// - **Parallelism (p_cost):** 1 lane.
/// - **Output length:** 32 bytes (matches the AES-256 key size).
///
/// The parameters are fixed in code and are not recorded in the meta
/// sidecar, so they cannot be raised for an existing database without
/// a format change. A weak passphrase stays weak: the KDF only slows
/// each guess (roughly 50-150 ms on a desktop CPU, far less on
/// dedicated cracking hardware). A random 32-byte key from a KMS or
/// secret store is the stronger choice.
///
/// # Errors
///
/// Returns [`Error::InvalidConfig`] for an empty passphrase, and
/// [`Error::Encryption`] when Argon2 itself reports an error. Argon2
/// only fails on impossible parameter combinations (we hardcode valid
/// ones), so that branch does not occur in practice.
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
    // (passphrase, salt) pair regardless of build / version. Raising
    // the cost needs a KDF-parameter field in the meta sidecar so old
    // files keep deriving with the original parameters.
    let params = Params::new(19_456, 2, 1, Some(32))
        .map_err(|_| Error::Encryption("argon2 params construction failed"))?;
    let argon = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);

    // Derive into a `Zeroizing<[u8; 32]>` so the bytes clear on
    // drop. argon2 is built with its `zeroize` feature, so its
    // working memory blocks are wiped before they are freed.
    let mut key: KeyBytes = Zeroizing::new([0_u8; 32]);
    argon
        .hash_password_into(passphrase.as_bytes(), salt, key.as_mut_slice())
        .map_err(|_| Error::Encryption("argon2 key derivation failed"))?;
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::{EncryptionContext, EncryptionInput, VERIFICATION_PLAINTEXT};
    use crate::storage::format::{TAG_ENCRYPTED_FLAG, TAG_INSERT};
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
    fn test_encryption_input_debug_redacts_key_and_passphrase() {
        let key = EncryptionInput::Key([0xCD; 32]);
        let pass = EncryptionInput::Passphrase("pw-in-debug".to_string());
        let key_dbg = format!("{key:?}");
        let pass_dbg = format!("{pass:?}");
        assert_eq!(key_dbg, "Key(<redacted>)");
        assert_eq!(pass_dbg, "Passphrase(<redacted>)");
        // 0xCD = 205: no decimal or hex rendering of the key bytes.
        assert!(!key_dbg.contains("205") && !key_dbg.to_lowercase().contains("cd"));
        assert!(!pass_dbg.contains("pw-in-debug"));
        let alt = format!("{key:#?} {pass:#?}");
        assert!(!alt.contains("205") && !alt.contains("pw-in-debug"));
    }

    #[test]
    fn test_seal_record_sets_flag_and_round_trips() {
        let ctx = EncryptionContext::from_key(&key_a());
        let payload = ctx
            .seal_record(TAG_INSERT, |buf| buf.extend_from_slice(b"body-bytes"))
            .unwrap_or_else(|err| panic!("{err}"));
        assert_eq!(payload[0], TAG_INSERT | TAG_ENCRYPTED_FLAG);
        let plain = ctx
            .decrypt(&payload[1..])
            .unwrap_or_else(|err| panic!("{err}"));
        assert_eq!(plain, b"body-bytes");
    }

    /// Compile-time check that the cipher state wipes itself on drop.
    /// `aes::Aes256` only implements `ZeroizeOnDrop` when the `aes`
    /// crate is built with its `zeroize` feature, which emdb turns on
    /// through its direct `aes` dependency. aes-gcm 0.10 does not
    /// implement `ZeroizeOnDrop` for `AesGcm` itself; its fields
    /// (`Aes256` and the polyval-backed GHASH state) wipe themselves.
    #[test]
    fn test_cipher_state_is_zeroize_on_drop() {
        fn assert_zeroize_on_drop<T: zeroize::ZeroizeOnDrop>() {}
        assert_zeroize_on_drop::<chacha20poly1305::ChaCha20Poly1305>();
        assert_zeroize_on_drop::<aes::Aes256>();
        assert_zeroize_on_drop::<aes_gcm::aes::Aes256>();
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
        let s1 = super::random_salt().unwrap_or_else(|e| panic!("{e}"));
        let s2 = super::random_salt().unwrap_or_else(|e| panic!("{e}"));
        assert_ne!(s1, s2, "random_salt must use the OS RNG");
    }
}
