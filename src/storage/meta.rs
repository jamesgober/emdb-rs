// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Sidecar metadata file (`<path>.meta`).
//!
//! Pre-`v0.9` emdb databases carried metadata in a 4 KiB header at
//! offset 0 of the data file. With the v0.9 fsys-journal substrate
//! the data file's bytes 0..N are owned by fsys's frame format —
//! we no longer have a place to put a header inside the data file.
//! Metadata moves to a sibling `<path>.meta` file written via
//! `fsys::Handle::write` for atomic-replace updates.
//!
//! ## Wire format
//!
//! ```text
//!   bytes  field             notes
//!   -----  -----             -----
//!    0..16 magic             b"EMDB-META\0\0\0\0\0\0\0"
//!   16..20 format_ver        u32 LE — currently 1
//!   20..24 flags             u32 LE — feature bits (encryption, etc.)
//!   24..32 created_at_ms     u64 LE — Unix milliseconds at first open
//!   32..48 encryption_salt   16-byte Argon2id salt; zeroed when not in use
//!   48..108 encryption_verify 60-byte AEAD verification block; zeroed when not in use
//!  108..112 body_crc          u32 LE — CRC32 of bytes 0..108
//! ```
//!
//! Total: **112 bytes**. Fixed-size, single-version v1. Future
//! revisions bump `format_ver` and grow the body; readers reject
//! unknown versions explicitly.
//!
//! Atomicity comes from the writer: every save uses
//! [`fsys::Handle::write`] which takes a temp-file + atomic rename
//! path. A torn write either leaves the previous body intact or
//! produces a complete new body — never a partial one.

use std::path::Path;

use crate::{Error, Result};

/// Magic prefix identifying the sidecar as an emdb meta file.
/// 16 bytes, padded with NULs.
pub(crate) const META_MAGIC: [u8; 16] = *b"EMDB-META\0\0\0\0\0\0\0";

/// Current meta format version.
pub(crate) const META_FORMAT_VERSION: u32 = 1;

/// Argon2id salt length (bytes).
pub(crate) const META_SALT_LEN: usize = 16;

/// AEAD verification block length (bytes): 12-byte nonce + 32-byte
/// ciphertext + 16-byte tag.
pub(crate) const META_VERIFY_LEN: usize = 60;

/// Total sidecar body length on disk.
pub(crate) const META_BODY_LEN: usize = 112;

/// Header flag bit indicating the database is encrypted at rest.
pub(crate) const FLAG_ENCRYPTED: u32 = 1 << 0;
/// Header flag bit selecting ChaCha20-Poly1305 (vs AES-256-GCM).
pub(crate) const FLAG_CIPHER_CHACHA20: u32 = 1 << 1;

/// Decoded metadata header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MetaHeader {
    /// Feature-flag bitmap (encryption bits, etc.).
    pub(crate) flags: u32,
    /// Wall-clock time at first creation, Unix milliseconds.
    pub(crate) created_at_ms: u64,
    /// Argon2id salt for passphrase-derived encryption keys.
    /// All-zero when encryption is disabled or when a raw key
    /// (not a passphrase) was supplied.
    pub(crate) encryption_salt: [u8; META_SALT_LEN],
    /// AEAD verification block — `nonce || ciphertext || tag` for
    /// the magic plaintext. All-zero when encryption is disabled.
    pub(crate) encryption_verify: [u8; META_VERIFY_LEN],
}

impl MetaHeader {
    /// Construct a fresh header for a brand-new database.
    pub(crate) fn fresh(flags: u32) -> Self {
        Self {
            flags,
            created_at_ms: now_unix_millis(),
            encryption_salt: [0_u8; META_SALT_LEN],
            encryption_verify: [0_u8; META_VERIFY_LEN],
        }
    }

    /// Encode the header into its on-disk byte representation.
    pub(crate) fn encode(&self) -> [u8; META_BODY_LEN] {
        let mut buf = [0_u8; META_BODY_LEN];
        buf[0..16].copy_from_slice(&META_MAGIC);
        buf[16..20].copy_from_slice(&META_FORMAT_VERSION.to_le_bytes());
        buf[20..24].copy_from_slice(&self.flags.to_le_bytes());
        buf[24..32].copy_from_slice(&self.created_at_ms.to_le_bytes());
        buf[32..48].copy_from_slice(&self.encryption_salt);
        buf[48..108].copy_from_slice(&self.encryption_verify);
        let crc = crc32fast::hash(&buf[..108]);
        buf[108..112].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Decode an on-disk meta body. Validates magic, version, and
    /// CRC; returns [`Error::MagicMismatch`], [`Error::VersionMismatch`],
    /// or [`Error::Corrupted`] on mismatch.
    pub(crate) fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() < META_BODY_LEN {
            return Err(Error::Corrupted {
                offset: 0,
                reason: "meta sidecar shorter than the v1 body",
            });
        }
        if buf[..16] != META_MAGIC {
            return Err(Error::MagicMismatch);
        }
        let version = u32::from_le_bytes(read_4(buf, 16));
        if version != META_FORMAT_VERSION {
            return Err(Error::VersionMismatch {
                found: version,
                expected: META_FORMAT_VERSION,
            });
        }
        let stored_crc = u32::from_le_bytes(read_4(buf, 108));
        let actual_crc = crc32fast::hash(&buf[..108]);
        if stored_crc != actual_crc {
            return Err(Error::Corrupted {
                offset: 108,
                reason: "meta sidecar CRC mismatch",
            });
        }

        let flags = u32::from_le_bytes(read_4(buf, 20));
        let created_at_ms = u64::from_le_bytes(read_8(buf, 24));
        let mut encryption_salt = [0_u8; META_SALT_LEN];
        encryption_salt.copy_from_slice(&buf[32..48]);
        let mut encryption_verify = [0_u8; META_VERIFY_LEN];
        encryption_verify.copy_from_slice(&buf[48..108]);

        Ok(Self {
            flags,
            created_at_ms,
            encryption_salt,
            encryption_verify,
        })
    }
}

/// Compute the path of the metadata sidecar for a database file.
///
/// `<db_path>` → `<db_path>.meta`.
pub(crate) fn meta_path_for(db_path: &Path) -> std::path::PathBuf {
    let mut p = db_path.as_os_str().to_owned();
    p.push(".meta");
    std::path::PathBuf::from(p)
}

/// Read the sidecar metadata file. Returns `Ok(None)` when the
/// sidecar does not exist (fresh database). Returns
/// `Ok(Some(_))` for a well-formed body. Errors on malformed
/// magic / version / CRC.
pub(crate) fn read(db_path: &Path) -> Result<Option<MetaHeader>> {
    let path = meta_path_for(db_path);
    match std::fs::read(&path) {
        Ok(bytes) => Ok(Some(MetaHeader::decode(&bytes)?)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(Error::from(err)),
    }
}

/// Write the sidecar metadata file atomically. Routes through
/// [`fsys::Handle::write`]'s temp-file + atomic-rename path so
/// torn writes either leave the previous body intact or produce
/// the complete new body.
pub(crate) fn write(db_path: &Path, header: &MetaHeader) -> Result<()> {
    let path = meta_path_for(db_path);
    let body = header.encode();
    let fs = fsys::builder()
        .build()
        .map_err(|err| Error::Io(std::io::Error::other(format!("fsys init: {err}"))))?;
    fs.write(&path, &body)
        .map_err(|err| Error::Io(std::io::Error::other(format!("fsys write meta: {err}"))))?;
    Ok(())
}

#[inline]
fn read_4(buf: &[u8], offset: usize) -> [u8; 4] {
    let mut out = [0_u8; 4];
    out.copy_from_slice(&buf[offset..offset + 4]);
    out
}

#[inline]
fn read_8(buf: &[u8], offset: usize) -> [u8; 8] {
    let mut out = [0_u8; 8];
    out.copy_from_slice(&buf[offset..offset + 8]);
    out
}

fn now_unix_millis() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis().min(u64::MAX as u128) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_default_header() {
        let h = MetaHeader::fresh(0);
        let buf = h.encode();
        let decoded = MetaHeader::decode(&buf).expect("decode");
        assert_eq!(h, decoded);
    }

    #[test]
    fn round_trip_with_encryption_payload() {
        let mut salt = [0_u8; META_SALT_LEN];
        for (i, b) in salt.iter_mut().enumerate() {
            *b = i as u8;
        }
        let mut verify = [0_u8; META_VERIFY_LEN];
        for (i, b) in verify.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        let h = MetaHeader {
            flags: FLAG_ENCRYPTED | FLAG_CIPHER_CHACHA20,
            created_at_ms: 1_700_000_000_123,
            encryption_salt: salt,
            encryption_verify: verify,
        };
        let buf = h.encode();
        assert_eq!(buf.len(), META_BODY_LEN);
        let decoded = MetaHeader::decode(&buf).expect("decode");
        assert_eq!(h, decoded);
    }

    #[test]
    fn rejects_bad_magic() {
        let mut buf = MetaHeader::fresh(0).encode();
        buf[0] ^= 0x01;
        assert!(matches!(MetaHeader::decode(&buf), Err(Error::MagicMismatch)));
    }

    #[test]
    fn rejects_bad_version() {
        let mut buf = MetaHeader::fresh(0).encode();
        buf[16] = 99;
        assert!(matches!(
            MetaHeader::decode(&buf),
            Err(Error::VersionMismatch { .. })
        ));
    }

    #[test]
    fn rejects_bad_crc() {
        let mut buf = MetaHeader::fresh(0).encode();
        buf[24] ^= 0x01; // corrupt the timestamp; CRC won't match
        assert!(matches!(
            MetaHeader::decode(&buf),
            Err(Error::Corrupted { .. })
        ));
    }
}
