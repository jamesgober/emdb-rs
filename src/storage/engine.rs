// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Storage engine: the public-facing API used by `Emdb`. Wraps the
//! mmap-backed [`Store`] and the per-namespace [`Index`]s. Provides
//! `insert`, `get`, `remove`, `len`, `iter`, plus namespace lifecycle
//! and the optional encryption integration.
//!
//! # Hot paths
//!
//! - **Insert**: encode record into the writer's reusable buffer,
//!   `pwrite` once, update the in-memory index. ~250-400ns/insert
//!   under no contention.
//! - **Get**: hash key, probe the namespace's sharded index, slice
//!   into the mmap, decode the record body. ~80-200ns/get under no
//!   contention.
//! - **Remove**: append a tombstone record (so a future recovery scan
//!   sees the removal), drop the in-memory index entry. ~250-400ns.
//!
//! On encrypted databases, AEAD encrypt/decrypt is added on top
//! (~200-400ns extra per record on commodity AES-NI hardware).

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crossbeam_skiplist::SkipMap;
use crossbeam_utils::CachePadded;
use memmap2::Mmap;
use parking_lot::{RwLock, RwLockReadGuard};

use crate::error::from_fsys;
use crate::storage::flush::FlushPolicy;
use crate::storage::format::{self, RecordView};
#[cfg(feature = "encrypt")]
use crate::storage::format::{OwnedRecord, NONCE_LEN};
use crate::storage::index::Index;
use crate::storage::integrity;
use crate::storage::meta::{self, FLAG_ENCRYPTED};
#[cfg(feature = "encrypt")]
use crate::storage::meta::{MetaHeader, FLAG_CIPHER_CHACHA20};
use crate::storage::store::{
    self, batch_payload_starts, remove_if_exists, sync_dir, Store, FSYS_MAX_PAYLOAD,
    FSYS_PRE_PAYLOAD_BYTES,
};
use crate::{Error, Result};

/// Payload bytes per batch when compaction, backup, `clear`,
/// `drop_namespace` and the encryption admin rewrite stream records.
/// Bounds their peak memory independently of the database size.
pub(crate) const REWRITE_CHUNK_BYTES: usize = 4 << 20;

/// Default namespace id (the implicit unnamed namespace).
pub(crate) const DEFAULT_NAMESPACE_ID: u32 = 0;

/// Per-namespace runtime state. The `index` maps `(hash, key) → file
/// offset`; `record_count` tracks live records for cheap `len` queries.
/// When the engine was opened with `enable_range_scans(true)`,
/// `range_index` carries a sorted secondary index — a lock-free
/// `crossbeam_skiplist::SkipMap` keyed by the actual key bytes so
/// concurrent inserts + range iteration scale without acquiring a
/// global lock. The hot `record_count` atomic is cache-padded so
/// inserts in one namespace don't false-share with reads in another.
struct NamespaceRuntime {
    index: Index,
    record_count: CachePadded<AtomicU64>,
    range_index: Option<Arc<SkipMap<Vec<u8>, u64>>>,
}

impl NamespaceRuntime {
    fn new(range_scans_enabled: bool) -> Self {
        Self {
            index: Index::new(),
            record_count: CachePadded::new(AtomicU64::new(0)),
            range_index: range_scans_enabled.then(|| Arc::new(SkipMap::new())),
        }
    }
}

impl std::fmt::Debug for NamespaceRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NamespaceRuntime")
            .field("len", &self.record_count.load(Ordering::Acquire))
            .finish()
    }
}

/// Optional encryption context. Held inside an `Arc` so `Engine` is
/// `Send + Sync` and the cipher state is shared between callers.
#[cfg(feature = "encrypt")]
pub(crate) type SharedEncryption = Option<Arc<crate::encryption::EncryptionContext>>;

/// Configuration handed to [`Engine::open`] by the builder.
#[derive(Debug, Clone)]
pub(crate) struct EngineConfig {
    pub(crate) path: PathBuf,
    /// Feature-flag bitmap persisted in the file header.
    pub(crate) flags: u32,
    /// Maintain a sorted secondary index alongside the hash index so
    /// `Emdb::range(...)` can return keys in lexicographic order.
    /// Off by default — adds a `Vec<u8>` clone per insert and roughly
    /// doubles index memory.
    pub(crate) enable_range_scans: bool,
    /// How `db.flush()` interacts with concurrent flush requests.
    /// Defaults to `OnEachFlush` to preserve v0.7.x semantics.
    pub(crate) flush_policy: FlushPolicy,
    /// Optional Linux io_uring `SQPOLL` idle window in milliseconds.
    /// `None` keeps the conservative non-SQPOLL submission path;
    /// `Some(idle_ms)` opts the journal's per-handle ring into
    /// kernel-side polling. Linux-only; ignored elsewhere.
    pub(crate) iouring_sqpoll_idle_ms: Option<u32>,
    /// Optional 32-byte AES-256 key (post-KDF). `None` for
    /// unencrypted. Stored in a [`zeroize::Zeroizing`] wrapper so
    /// the bytes clear when the config is dropped.
    #[cfg(feature = "encrypt")]
    pub(crate) encryption_key: Option<crate::encryption::KeyBytes>,
    /// Optional cipher choice. `None` defaults to AES-256-GCM on fresh
    /// files; reopens read the cipher from the header's flag bit.
    #[cfg(feature = "encrypt")]
    pub(crate) cipher: Option<crate::encryption::Cipher>,
    /// Argon2id-derived passphrase. The engine peeks the header for the
    /// salt, derives the key, and then proceeds as if `encryption_key`
    /// were set.
    #[cfg(feature = "encrypt")]
    pub(crate) encryption_passphrase: Option<String>,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::new(),
            flags: 0,
            enable_range_scans: false,
            flush_policy: FlushPolicy::default(),
            iouring_sqpoll_idle_ms: None,
            #[cfg(feature = "encrypt")]
            encryption_key: None,
            #[cfg(feature = "encrypt")]
            cipher: None,
            #[cfg(feature = "encrypt")]
            encryption_passphrase: None,
        }
    }
}

/// The engine. Cheap to clone (every field is `Arc`-shared internally).
pub(crate) struct Engine {
    store: Arc<Store>,
    /// Write gate. Every mutation of the journal plus index holds it
    /// shared (see [`Self::write_gate`]); compaction, `clear`,
    /// `drop_namespace` and the snapshot phase of backup hold it
    /// exclusively, so they see no half-applied write and no write
    /// can land in a file compaction is about to retire.
    write_gate: RwLock<()>,
    /// Map of `namespace_id → runtime state`. The default namespace is
    /// always present at id 0; named namespaces are added via
    /// [`Self::create_or_open_namespace`].
    namespaces: RwLock<HashMap<u32, Arc<NamespaceRuntime>>>,
    /// Map of `namespace_name → namespace_id`. Empty string is the
    /// default namespace and is not stored here.
    namespace_names: RwLock<HashMap<String, u32>>,
    /// Counter for the next-allocated namespace id.
    next_namespace_id: AtomicU64,
    /// Cached copy of the open-time `enable_range_scans` flag so new
    /// namespaces created post-open get the same secondary-index
    /// behaviour.
    range_scans_enabled: bool,
    #[cfg(feature = "encrypt")]
    encryption: SharedEncryption,
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("store", &self.store)
            .finish()
    }
}

/// Owned snapshot row used by `iter` / `keys`.
pub(crate) type RecordSnapshot = (Vec<u8>, Vec<u8>, u64);

/// Guard returned by [`Engine::write_gate`].
pub(crate) type WriteGuard<'a> = RwLockReadGuard<'a, ()>;

/// A read mapping pinned for the lifetime of an iterator. Offsets an
/// iterator snapshots are resolved against the mapping taken in the
/// same consistent step, so a compaction that replaces the file
/// while the iterator runs does not change what it yields: it keeps
/// reading the retired file, whose mapping stays valid while pinned.
pub(crate) type ReadView = Arc<Mmap>;

/// `(key, offset)` pairs of a range query plus the mapping they
/// resolve against. See [`Engine::snapshot_range_offsets`].
pub(crate) type RangeSnapshot = (Vec<(Vec<u8>, u64)>, ReadView);

/// Output of [`Engine::resolve_encryption`]: the resolved 32-byte key
/// (wrapped in `Zeroizing` so it clears on drop), an optional fresh
/// salt to persist for new passphrase databases, and the requested
/// cipher (if explicitly set).
#[cfg(feature = "encrypt")]
type ResolvedEncryption = (
    Option<crate::encryption::KeyBytes>,
    Option<[u8; meta::META_SALT_LEN]>,
    Option<crate::encryption::Cipher>,
);

/// One decoded record's payload, as the recovery scan needs it. The
/// scan calls `apply_recovered_action` with this plus the offset where
/// the record was framed and the next-cursor it should resume from.
enum RecoveryAction {
    Insert { ns_id: u32, key: Vec<u8> },
    Remove { ns_id: u32, key: Vec<u8> },
    NamespaceName { ns_id: u32, name: Vec<u8> },
}

impl Engine {
    /// Open or create a database at `config.path`.
    pub(crate) fn open(config: EngineConfig) -> Result<Self> {
        // For encrypted databases we may need to peek the header
        // before opening the store with the right key. This branch is
        // entirely cfg-gated.
        #[cfg(feature = "encrypt")]
        let (resolved_key, fresh_salt, resolved_cipher) = Self::resolve_encryption(&config)?;

        #[cfg(feature = "encrypt")]
        let flags = {
            let mut f = config.flags;
            if resolved_key.is_some() {
                f |= FLAG_ENCRYPTED;
                if let Some(crate::encryption::Cipher::ChaCha20Poly1305) = resolved_cipher {
                    f |= FLAG_CIPHER_CHACHA20;
                }
            }
            f
        };
        #[cfg(not(feature = "encrypt"))]
        let flags = config.flags;

        let store = Arc::new(Store::open_with_policy(
            config.path.clone(),
            flags,
            config.flush_policy,
            config.iouring_sqpoll_idle_ms,
        )?);
        let header = store.header()?;

        // Build the encryption context (if any). On fresh files we
        // also write the verification block; on existing files we
        // validate it.
        #[cfg(feature = "encrypt")]
        let encryption: SharedEncryption = match resolved_key {
            None => None,
            Some(key) => {
                let cipher = resolved_cipher
                    .or_else(|| Some(Self::cipher_from_flags(header.flags)))
                    .unwrap_or(crate::encryption::Cipher::Aes256Gcm);
                let ctx = crate::encryption::EncryptionContext::from_key_with_cipher(&key, cipher);
                let arc = Arc::new(ctx);

                // Validate or initialise the verification block.
                Self::handle_verification(&store, &arc, fresh_salt, &header)?;
                Some(arc)
            }
        };

        // Validate that an unencrypted-build reader is not opening an
        // encrypted file (would just read garbage).
        #[cfg(not(feature = "encrypt"))]
        if header.flags & FLAG_ENCRYPTED != 0 {
            return Err(Error::InvalidConfig(
                "this database was created with encryption; rebuild with the `encrypt` feature",
            ));
        }

        let range_scans_enabled = config.enable_range_scans;
        let engine = Self {
            store,
            write_gate: RwLock::new(()),
            namespaces: RwLock::new(HashMap::new()),
            namespace_names: RwLock::new(HashMap::new()),
            next_namespace_id: AtomicU64::new(1),
            range_scans_enabled,
            #[cfg(feature = "encrypt")]
            encryption,
        };

        // Always create the default namespace runtime.
        {
            let mut guard = engine.namespaces.write();
            let _existing = guard.insert(
                DEFAULT_NAMESPACE_ID,
                Arc::new(NamespaceRuntime::new(range_scans_enabled)),
            );
        }

        // Recovery scan: walk every record from the start of the
        // journal to its last valid frame, populating namespace
        // indexes, and refuse the open if valid records follow damage
        // (mid-file corruption) instead of letting fsys cut them.
        #[cfg(feature = "ttl")]
        let expired = engine.recovery_scan()?;
        #[cfg(not(feature = "ttl"))]
        engine.recovery_scan()?;

        // Only now hand the file to fsys (which cuts a torn tail) and
        // persist a meta sidecar that was held back: a failed open
        // above (wrong key, corrupt journal) leaves the files as they
        // were.
        engine.store.open_journal()?;
        engine.store.finish_open()?;
        engine.remove_stale_rewrite_files();

        #[cfg(feature = "ttl")]
        engine.sweep_expired_on_open(expired);

        Ok(engine)
    }

    /// Shared write gate. Every code path that appends to the journal
    /// and then updates an index holds the returned guard for the
    /// whole operation, so compaction (which takes the gate
    /// exclusively) never interleaves with a half-applied write.
    ///
    /// The lock is fair: do not acquire it twice on one thread (a
    /// waiting compaction would deadlock the second acquisition).
    #[inline]
    pub(crate) fn write_gate(&self) -> WriteGuard<'_> {
        self.write_gate.read()
    }

    /// Run `read` until it completes without a compaction swap
    /// happening in the middle of it. Every read that resolves an
    /// index offset against a mapping goes through here: an offset
    /// taken from the old index and decoded against the new file (or
    /// the reverse) is detected and the read is retried.
    fn consistent<T>(&self, mut read: impl FnMut() -> Result<T>) -> Result<T> {
        loop {
            let seq = self.store.read_begin();
            let out = read();
            if self.store.read_validate(seq) {
                return out;
            }
        }
    }

    /// Remove temporaries a crash or an older release left next to
    /// the database: `<path>.compact.tmp` and the `.meta` sidecar
    /// emdb 1.0.2 and earlier wrote (and leaked) for it. The caller
    /// holds the database lock, so no rewrite is running. Failures
    /// are ignored: the files are never read, and the next
    /// compaction removes them again before it starts.
    fn remove_stale_rewrite_files(&self) {
        let tmp = compaction_temp_path(self.store.path());
        let _ignored = remove_if_exists(&meta::meta_path_for(&tmp));
        let _ignored = remove_if_exists(&tmp);
    }

    /// Resolve which encryption key (if any) to use, including the KDF
    /// dance for passphrase mode. Returns `(key, fresh_salt, cipher)`.
    /// `fresh_salt` is `Some(_)` only when this is a brand-new
    /// passphrase-encrypted file and we need to generate + persist a
    /// salt.
    #[cfg(feature = "encrypt")]
    fn resolve_encryption(config: &EngineConfig) -> Result<ResolvedEncryption> {
        if config.encryption_key.is_some() && config.encryption_passphrase.is_some() {
            return Err(Error::InvalidConfig(
                "encryption_key and encryption_passphrase are mutually exclusive — pick one",
            ));
        }

        // Peek the header (if the file exists) so we can read the salt
        // for passphrase mode and the cipher bit for both modes.
        let peeked = peek_header(&config.path)?;
        let on_disk_cipher = peeked.map(|h| Self::cipher_from_flags(h.flags));

        // Cipher: explicit override OR on-disk cipher OR default. If
        // the user supplied an explicit choice that disagrees with the
        // on-disk cipher, surface InvalidConfig early.
        let cipher = match (config.cipher, on_disk_cipher) {
            (Some(requested), Some(disk)) if requested != disk => {
                return Err(Error::InvalidConfig(
                    "EmdbBuilder::cipher disagrees with the cipher this database was created with",
                ));
            }
            (Some(requested), _) => Some(requested),
            (None, Some(disk)) => Some(disk),
            (None, None) => None,
        };

        if let Some(passphrase) = config.encryption_passphrase.as_ref() {
            let (salt, fresh) = match peeked {
                Some(header) => {
                    if header.encryption_salt == [0_u8; meta::META_SALT_LEN] {
                        return Err(Error::InvalidConfig(
                            "this database was created with a raw encryption_key; supply via encryption_key, not encryption_passphrase",
                        ));
                    }
                    (header.encryption_salt, None)
                }
                None => {
                    let s = crate::encryption::random_salt();
                    (s, Some(s))
                }
            };
            let derived = crate::encryption::derive_key_from_passphrase(passphrase, &salt)?;
            return Ok((Some(derived), fresh, cipher));
        }

        if let Some(key) = config.encryption_key.as_ref() {
            if let Some(header) = peeked {
                if header.encryption_salt != [0_u8; meta::META_SALT_LEN] {
                    return Err(Error::InvalidConfig(
                        "this database was created with an encryption_passphrase; supply via encryption_passphrase, not encryption_key",
                    ));
                }
            }
            // Cloning a `Zeroizing<[u8; 32]>` copies the bytes; each
            // clone independently zeroizes on drop, so the original
            // inside the config and the resolved copy returned here
            // both clear when their owners go out of scope.
            return Ok((Some(key.clone()), None, cipher));
        }

        // Unencrypted opens. If the file is encrypted, fail loudly.
        if let Some(header) = peeked {
            if header.flags & FLAG_ENCRYPTED != 0 {
                return Err(Error::InvalidConfig(
                    "this database was created with at-rest encryption; supply encryption_key or encryption_passphrase",
                ));
            }
        }
        Ok((None, None, None))
    }

    /// Decode the cipher selector from a header's flags field.
    #[cfg(feature = "encrypt")]
    fn cipher_from_flags(flags: u32) -> crate::encryption::Cipher {
        if flags & FLAG_CIPHER_CHACHA20 != 0 {
            crate::encryption::Cipher::ChaCha20Poly1305
        } else {
            crate::encryption::Cipher::Aes256Gcm
        }
    }

    /// On a fresh encrypted file, generate the verification block; on
    /// reopen, validate it.
    #[cfg(feature = "encrypt")]
    fn handle_verification(
        store: &Store,
        ctx: &Arc<crate::encryption::EncryptionContext>,
        fresh_salt: Option<[u8; meta::META_SALT_LEN]>,
        existing_header: &MetaHeader,
    ) -> Result<()> {
        // If the on-disk verify block is all-zero, we treat this as a
        // fresh file and write a new verification block + salt.
        if existing_header.encryption_verify == [0_u8; meta::META_VERIFY_LEN] {
            let salt = fresh_salt.unwrap_or([0_u8; meta::META_SALT_LEN]);
            // Encrypt the well-known verification plaintext.
            let nonce_then_ct = ctx.encrypt(crate::encryption::VERIFICATION_PLAINTEXT)?;
            // nonce_then_ct = [nonce(12) | ciphertext(32) + tag(16)] = 60 bytes
            debug_assert_eq!(nonce_then_ct.len(), meta::META_VERIFY_LEN);
            let mut verify = [0_u8; meta::META_VERIFY_LEN];
            verify.copy_from_slice(&nonce_then_ct);
            store.set_encryption_metadata(salt, verify)?;
            return Ok(());
        }

        // Existing encrypted file: decrypt and compare.
        let plaintext = ctx.decrypt(&existing_header.encryption_verify)?;
        if plaintext.as_slice() != crate::encryption::VERIFICATION_PLAINTEXT {
            return Err(Error::EncryptionKeyMismatch);
        }
        Ok(())
    }

    /// Walk every record in the journal and rebuild the in-memory
    /// index. Delegates frame iteration + CRC validation to
    /// `fsys::JournalReader`; emdb decodes each record's payload
    /// (`tag + body`) and routes it to the right namespace runtime.
    ///
    /// When the reader stops before the end of the file, the bytes
    /// after its stop point are classified by
    /// [`integrity::check_damage`]: a torn tail is accepted (fsys cuts
    /// it when the journal opens), damage followed by valid records
    /// fails the open with [`Error::Corrupted`] before anything is
    /// cut.
    ///
    /// `JournalReader::lsn` is the byte offset of the frame's first
    /// byte (the magic). emdb's index stores the `payload_start`,
    /// which sits 8 bytes past the frame start (4 magic + 4 length).
    ///
    /// With the `ttl` feature, also returns the default-namespace
    /// keys whose live record had already expired (see
    /// [`OpenExpired`]).
    fn recovery_scan(&self) -> Result<OpenExpired> {
        let mut reader = self.store.open_reader()?;
        // Advisory read-ahead hint: a platform that rejects it reads
        // the same bytes with its default read-ahead window.
        let _advice = reader.advise_sequential();
        #[cfg(feature = "ttl")]
        let mut expiry = ExpiryTracker::new(crate::ttl::now_unix_millis());
        for record_result in reader.iter() {
            let record = record_result.map_err(from_fsys)?;
            let payload_start = record.lsn.as_u64() + FSYS_PRE_PAYLOAD_BYTES;
            self.apply_recovered_payload(&record.payload, payload_start)?;
            #[cfg(feature = "ttl")]
            expiry.observe(&record.payload);
        }
        if reader.tail_state() != fsys::JournalTailState::CleanEnd {
            integrity::check_damage(
                self.store.path(),
                reader.position().as_u64(),
                reader.file_size(),
            )?;
        }
        #[cfg(feature = "ttl")]
        return Ok(expiry.finish());
        #[cfg(not(feature = "ttl"))]
        Ok(())
    }

    /// Decode a single recovered payload (`[tag][body]`) and apply
    /// it to the in-memory index. Used exclusively by
    /// [`Self::recovery_scan`].
    fn apply_recovered_payload(&self, payload: &[u8], payload_start: u64) -> Result<()> {
        if payload.is_empty() {
            return Err(Error::Corrupted {
                offset: payload_start,
                reason: "empty record payload during recovery",
            });
        }
        let tag = payload[0];
        let encrypted = (tag & format::TAG_ENCRYPTED_FLAG) != 0;

        let action = if encrypted {
            #[cfg(feature = "encrypt")]
            {
                let ctx = match self.encryption.as_ref() {
                    Some(c) => Arc::clone(c),
                    None => {
                        return Err(Error::InvalidConfig(
                            "encrypted record encountered while opening unencrypted database",
                        ));
                    }
                };
                let owned = format::decode_payload_encrypted(payload, |nonce, ct| {
                    let mut input = Vec::with_capacity(NONCE_LEN + ct.len());
                    input.extend_from_slice(nonce);
                    input.extend_from_slice(ct);
                    ctx.decrypt(&input)
                })?;
                match owned {
                    OwnedRecord::Insert { ns_id, key, .. } => RecoveryAction::Insert { ns_id, key },
                    OwnedRecord::Remove { ns_id, key } => RecoveryAction::Remove { ns_id, key },
                    OwnedRecord::NamespaceName { ns_id, name } => {
                        RecoveryAction::NamespaceName { ns_id, name }
                    }
                }
            }
            #[cfg(not(feature = "encrypt"))]
            {
                let _ = payload_start;
                return Err(Error::InvalidConfig(
                    "encrypted record present but the `encrypt` feature is not compiled in",
                ));
            }
        } else {
            match format::decode_payload(payload)? {
                RecordView::Insert { ns_id, key, .. } => RecoveryAction::Insert {
                    ns_id,
                    key: key.to_vec(),
                },
                RecordView::Remove { ns_id, key } => RecoveryAction::Remove {
                    ns_id,
                    key: key.to_vec(),
                },
                RecordView::NamespaceName { ns_id, name } => RecoveryAction::NamespaceName {
                    ns_id,
                    name: name.to_vec(),
                },
            }
        };

        self.apply_recovered_action(action, payload_start)
    }

    fn apply_recovered_action(&self, action: RecoveryAction, offset: u64) -> Result<()> {
        match action {
            RecoveryAction::Insert { ns_id, key } => {
                let ns = self.ensure_namespace_runtime(ns_id)?;
                let key_hash = Index::hash_key(&key);
                let prev = ns
                    .index
                    .replace(key_hash, &key, offset, |off| self.key_at_offset(off))?;
                if prev.is_none() {
                    let _ = ns.record_count.fetch_add(1, Ordering::AcqRel);
                }
                if let Some(range_map) = ns.range_index.as_ref() {
                    let _ = range_map.insert(key, offset);
                }
            }
            RecoveryAction::Remove { ns_id, key } => {
                let ns = self.ensure_namespace_runtime(ns_id)?;
                let key_hash = Index::hash_key(&key);
                if ns.index.remove(key_hash, &key)?.is_some() {
                    let _ = ns.record_count.fetch_sub(1, Ordering::AcqRel);
                }
                if let Some(range_map) = ns.range_index.as_ref() {
                    let _ = range_map.remove(&key);
                }
            }
            RecoveryAction::NamespaceName { ns_id, name } => {
                if name.is_empty() && ns_id != DEFAULT_NAMESPACE_ID {
                    // An empty name is the record `drop_namespace`
                    // writes: forget the binding and the runtime.
                    self.unbind_namespace(ns_id);
                    return Ok(());
                }
                if ns_id == DEFAULT_NAMESPACE_ID || name.is_empty() {
                    // Defensive: the engine never emits a NamespaceName
                    // for the default namespace. Skip if we somehow find
                    // one (e.g., bit-flipped record that passed CRC).
                    return Ok(());
                }
                let name_str = match std::str::from_utf8(&name) {
                    Ok(s) => s.to_string(),
                    Err(_) => {
                        return Err(Error::Corrupted {
                            offset,
                            reason: "namespace-name record carried non-UTF-8 name",
                        });
                    }
                };
                // Register the runtime if absent so subsequent inserts
                // into this ns_id land in the right place. Then bind
                // the name → id mapping.
                let _ = self.ensure_namespace_runtime(ns_id)?;
                let mut name_guard = self.namespace_names.write();
                let _existing = name_guard.insert(name_str, ns_id);
                drop(name_guard);
                // Bump the id allocator past this id.
                if ns_id as u64 >= self.next_namespace_id.load(Ordering::Acquire) {
                    self.next_namespace_id
                        .store(ns_id as u64 + 1, Ordering::Release);
                }
            }
        }
        Ok(())
    }

    /// Decode the key bytes of the record at `offset`. Used as a
    /// hash-collision resolver for [`Index::replace`]: when the index
    /// finds an existing `Single` slot at the same hash, it asks the
    /// engine what key currently lives there so it can disambiguate
    /// between a true replacement and a hash collision.
    ///
    /// Returns `Ok(None)` when the record cannot be decoded (corrupt
    /// or already tombstoned in some way) — the index treats that as
    /// "the existing entry is stale; overwrite in place."
    fn key_at_offset(&self, offset: u64) -> Result<Option<Vec<u8>>> {
        let mmap = self.store.mmap_for_payload(offset)?;
        let bytes: &[u8] = &mmap;
        let payload = match format::payload_at(bytes, offset as usize) {
            Ok(p) => p,
            Err(_) => return Ok(None),
        };
        self.key_from_payload(payload)
    }

    /// Decode the key of an insert payload (`[tag][body]`), decrypting
    /// it on encrypted databases. `Ok(None)` for other record kinds.
    fn key_from_payload(&self, payload: &[u8]) -> Result<Option<Vec<u8>>> {
        #[cfg(feature = "encrypt")]
        if let Some(ctx) = self.encryption.as_ref() {
            let ctx = Arc::clone(ctx);
            let owned = format::decode_payload_encrypted(payload, |nonce, ct| {
                let mut input = Vec::with_capacity(NONCE_LEN + ct.len());
                input.extend_from_slice(nonce);
                input.extend_from_slice(ct);
                ctx.decrypt(&input)
            })?;
            return Ok(match owned {
                OwnedRecord::Insert { key, .. } => Some(key),
                _ => None,
            });
        }

        Ok(match format::decode_payload(payload)? {
            RecordView::Insert { key, .. } => Some(key.to_vec()),
            _ => None,
        })
    }

    fn ensure_namespace_runtime(&self, ns_id: u32) -> Result<Arc<NamespaceRuntime>> {
        {
            let guard = self.namespaces.read();
            if let Some(ns) = guard.get(&ns_id) {
                return Ok(Arc::clone(ns));
            }
        }
        let mut guard = self.namespaces.write();
        let range_scans = self.range_scans_enabled;
        let entry = guard
            .entry(ns_id)
            .or_insert_with(|| Arc::new(NamespaceRuntime::new(range_scans)));
        // Bump next_namespace_id past whatever ns_id we just created so a
        // fresh `create_or_open_namespace` call won't reuse it.
        if ns_id as u64 >= self.next_namespace_id.load(Ordering::Acquire) {
            self.next_namespace_id
                .store(ns_id as u64 + 1, Ordering::Release);
        }
        Ok(Arc::clone(entry))
    }

    fn namespace(&self, ns_id: u32) -> Result<Arc<NamespaceRuntime>> {
        self.namespaces
            .read()
            .get(&ns_id)
            .map(Arc::clone)
            .ok_or(Error::InvalidConfig("unknown namespace id"))
    }

    /// Insert or replace a key/value pair.
    pub(crate) fn insert(
        &self,
        ns_id: u32,
        key: &[u8],
        value: &[u8],
        expires_at: u64,
    ) -> Result<()> {
        let _gate = self.write_gate();
        let ns = self.namespace(ns_id)?;
        let key_hash = Index::hash_key(key);

        let offset = self.append_insert(ns_id, key, value, expires_at)?;

        let prev = ns
            .index
            .replace(key_hash, key, offset, |off| self.key_at_offset(off))?;
        if prev.is_none() {
            let _ = ns.record_count.fetch_add(1, Ordering::AcqRel);
        }
        if let Some(range_map) = ns.range_index.as_ref() {
            let _ = range_map.insert(key.to_vec(), offset);
        }
        Ok(())
    }

    /// Bulk insert multiple records via fsys's vectored
    /// `JournalHandle::append_batch`: one LSN reservation, one heap
    /// allocation for the concatenated frames, one platform `pwrite`
    /// covering the whole batch. Records are NOT atomic as a group
    /// (no Begin/End markers); for atomic batches use the
    /// transaction API.
    pub(crate) fn insert_many(
        &self,
        ns_id: u32,
        items: impl IntoIterator<Item = (Vec<u8>, Vec<u8>, u64)>,
    ) -> Result<()> {
        let _gate = self.write_gate();
        let ns = self.namespace(ns_id)?;
        let items: Vec<(Vec<u8>, Vec<u8>, u64)> = items.into_iter().collect();
        if items.is_empty() {
            return Ok(());
        }

        #[cfg(feature = "encrypt")]
        let encryption = self.encryption.clone();

        // Pre-encode every record into one `Vec<Vec<u8>>`, then
        // submit the whole batch via fsys's vectored
        // `append_batch`: one LSN reservation, one heap
        // allocation for the concatenated frames, one platform
        // `pwrite`. fsync cost is amortised by group-commit
        // when the engine flushes at the end of the batch.
        let mut payloads: Vec<Vec<u8>> = Vec::with_capacity(items.len());
        for (key, value, expires_at) in &items {
            let payload: Vec<u8> = {
                #[cfg(feature = "encrypt")]
                {
                    if let Some(ctx) = encryption.as_ref() {
                        let mut plain = Vec::with_capacity(20 + key.len() + value.len());
                        format::encode_insert_body(&mut plain, ns_id, key, value, *expires_at);
                        let nonce_then_ct = ctx.encrypt(&plain)?;
                        let mut frame = Vec::with_capacity(1 + nonce_then_ct.len());
                        frame.push(format::TAG_INSERT | format::TAG_ENCRYPTED_FLAG);
                        frame.extend_from_slice(&nonce_then_ct);
                        frame
                    } else {
                        let mut frame = Vec::with_capacity(1 + 20 + key.len() + value.len());
                        frame.push(format::TAG_INSERT);
                        format::encode_insert_body(&mut frame, ns_id, key, value, *expires_at);
                        frame
                    }
                }
                #[cfg(not(feature = "encrypt"))]
                {
                    let mut frame = Vec::with_capacity(1 + 20 + key.len() + value.len());
                    frame.push(format::TAG_INSERT);
                    format::encode_insert_body(&mut frame, ns_id, key, value, *expires_at);
                    frame
                }
            };
            payloads.push(payload);
        }
        let offsets = self
            .store
            .append_batch(payloads.iter().map(Vec::as_slice))?;

        // Now update the index. Records are already on disk; this just
        // bumps the in-memory map. The SkipMap is lock-free so no
        // guard acquisition is needed across the iteration.
        let range_map = ns.range_index.as_ref();
        for ((key, _value, _exp), offset) in items.iter().zip(offsets.iter()) {
            let key_hash = Index::hash_key(key);
            let prev = ns
                .index
                .replace(key_hash, key, *offset, |off| self.key_at_offset(off))?;
            if prev.is_none() {
                let _ = ns.record_count.fetch_add(1, Ordering::AcqRel);
            }
            if let Some(range_map) = range_map {
                let _ = range_map.insert(key.clone(), *offset);
            }
        }
        Ok(())
    }

    fn append_insert(&self, ns_id: u32, key: &[u8], value: &[u8], expires_at: u64) -> Result<u64> {
        #[cfg(feature = "encrypt")]
        if let Some(ctx) = self.encryption.as_ref() {
            // Build the plaintext payload.
            let mut payload = Vec::with_capacity(20 + key.len() + value.len());
            format::encode_insert_body(&mut payload, ns_id, key, value, expires_at);
            // AEAD encrypt; ctx.encrypt returns nonce || ciphertext+tag.
            let nonce_then_ct = ctx.encrypt(&payload)?;
            return self.store.append_with(|buf| {
                buf.push(format::TAG_INSERT | format::TAG_ENCRYPTED_FLAG);
                buf.extend_from_slice(&nonce_then_ct);
                Ok(())
            });
        }

        self.store.append_with(|buf| {
            buf.push(format::TAG_INSERT);
            format::encode_insert_body(buf, ns_id, key, value, expires_at);
            Ok(())
        })
    }

    fn append_remove(&self, ns_id: u32, key: &[u8]) -> Result<u64> {
        #[cfg(feature = "encrypt")]
        if let Some(ctx) = self.encryption.as_ref() {
            let mut payload = Vec::with_capacity(8 + key.len());
            format::encode_remove_body(&mut payload, ns_id, key);
            let nonce_then_ct = ctx.encrypt(&payload)?;
            return self.store.append_with(|buf| {
                buf.push(format::TAG_REMOVE | format::TAG_ENCRYPTED_FLAG);
                buf.extend_from_slice(&nonce_then_ct);
                Ok(())
            });
        }

        self.store.append_with(|buf| {
            buf.push(format::TAG_REMOVE);
            format::encode_remove_body(buf, ns_id, key);
            Ok(())
        })
    }

    /// Look up a key. Returns `Ok(None)` when not present, expired, or
    /// hash-collided to a different key.
    pub(crate) fn get(&self, ns_id: u32, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.get_with_meta(ns_id, key)?.map(|(v, _)| v))
    }

    /// Zero-copy variant of [`Self::get`]. Returns the value as a
    /// [`crate::ValueRef`] that holds a strong reference to the
    /// mmap region, so the bytes can be read without allocation.
    /// Encrypted databases fall back to an owned plaintext buffer
    /// inside the [`crate::ValueRef`] (zero-copy is impossible
    /// across an AEAD boundary).
    pub(crate) fn get_zerocopy(
        &self,
        ns_id: u32,
        key: &[u8],
    ) -> Result<Option<(crate::ValueRef, u64)>> {
        self.consistent(|| {
            let ns = self.namespace(ns_id)?;
            let key_hash = Index::hash_key(key);
            let offset = match ns.index.get(key_hash, key)? {
                Some(o) => o,
                None => return Ok(None),
            };
            self.read_zerocopy_at(offset, key)
        })
    }

    /// Decode the record at `offset`, returning a [`crate::ValueRef`]
    /// pointing at the value bytes (or carrying owned plaintext for
    /// encrypted databases) plus the record's `expires_at`.
    /// Returns `Ok(None)` if the offset's record is no longer a
    /// live `Insert` whose key matches `expected_key`.
    fn read_zerocopy_at(
        &self,
        offset: u64,
        expected_key: &[u8],
    ) -> Result<Option<(crate::ValueRef, u64)>> {
        let mmap = self.store.mmap_for_payload(offset)?;
        let bytes: &[u8] = &mmap;
        let payload = match format::payload_at(bytes, offset as usize) {
            Ok(p) => p,
            Err(_) => return Ok(None),
        };

        #[cfg(feature = "encrypt")]
        if let Some(ctx) = self.encryption.as_ref() {
            let ctx = Arc::clone(ctx);
            let owned = format::decode_payload_encrypted(payload, |nonce, ct| {
                let mut input = Vec::with_capacity(NONCE_LEN + ct.len());
                input.extend_from_slice(nonce);
                input.extend_from_slice(ct);
                ctx.decrypt(&input)
            })?;
            return Ok(match owned {
                OwnedRecord::Insert {
                    key,
                    value,
                    expires_at,
                    ..
                } => {
                    if key.as_slice() == expected_key {
                        Some((crate::ValueRef::from_owned(value), expires_at))
                    } else {
                        None
                    }
                }
                _ => None,
            });
        }

        // Plaintext fast path: derive the value's mmap byte range
        // from the borrowed `value` slice and build an mmap-backed
        // ValueRef so reads are zero-copy.
        let (value_range, expires_at) = match format::decode_payload(payload)? {
            RecordView::Insert {
                key,
                value,
                expires_at,
                ..
            } => {
                if key != expected_key {
                    return Ok(None);
                }
                // Subtract base pointers to recover the absolute
                // byte offset of `value` inside the mmap.
                let base = bytes.as_ptr() as usize;
                let val_start = value.as_ptr() as usize - base;
                let val_end = val_start + value.len();
                (val_start..val_end, expires_at)
            }
            _ => return Ok(None),
        };

        Ok(Some((
            crate::ValueRef::from_mmap(mmap, value_range),
            expires_at,
        )))
    }

    /// Fetch value + expires_at for a key in one pass. Used by the TTL
    /// path in `Emdb::get` so it doesn't have to make two record reads.
    pub(crate) fn get_with_meta(&self, ns_id: u32, key: &[u8]) -> Result<Option<(Vec<u8>, u64)>> {
        self.consistent(|| {
            let ns = self.namespace(ns_id)?;
            let key_hash = Index::hash_key(key);
            let offset = match ns.index.get(key_hash, key)? {
                Some(o) => o,
                None => return Ok(None),
            };
            self.read_value_at(offset, key)
        })
    }

    fn read_value_at(&self, offset: u64, expected_key: &[u8]) -> Result<Option<(Vec<u8>, u64)>> {
        let mmap = self.store.mmap_for_payload(offset)?;
        self.read_value_in(&mmap, offset, expected_key)
    }

    /// Decode the value at `offset` from `view`, validating that the
    /// record's key is `expected_key`. Used directly by range
    /// iterators with the mapping they pinned at snapshot time.
    pub(crate) fn read_value_in(
        &self,
        view: &ReadView,
        offset: u64,
        expected_key: &[u8],
    ) -> Result<Option<(Vec<u8>, u64)>> {
        let bytes: &[u8] = view;

        #[cfg(feature = "encrypt")]
        if let Some(ctx) = self.encryption.as_ref() {
            let payload = match format::payload_at(bytes, offset as usize) {
                Ok(p) => p,
                Err(_) => return Ok(None),
            };
            let ctx = Arc::clone(ctx);
            let owned = format::decode_payload_encrypted(payload, |nonce, ct| {
                let mut input = Vec::with_capacity(NONCE_LEN + ct.len());
                input.extend_from_slice(nonce);
                input.extend_from_slice(ct);
                ctx.decrypt(&input)
            })?;
            return Ok(match owned {
                OwnedRecord::Insert {
                    key,
                    value,
                    expires_at,
                    ..
                } => {
                    if key.as_slice() == expected_key {
                        Some((value, expires_at))
                    } else {
                        None
                    }
                }
                _ => None,
            });
        }

        let payload = match format::payload_at(bytes, offset as usize) {
            Ok(p) => p,
            Err(_) => return Ok(None),
        };
        Ok(match format::decode_payload(payload)? {
            RecordView::Insert {
                key,
                value,
                expires_at,
                ..
            } => {
                if key == expected_key {
                    Some((value.to_vec(), expires_at))
                } else {
                    None
                }
            }
            _ => None,
        })
    }

    /// Remove a key. Returns the previously-associated value, if any.
    pub(crate) fn remove(&self, ns_id: u32, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let _gate = self.write_gate();
        let prev = self.get(ns_id, key)?;
        if prev.is_some() {
            let _offset = self.append_remove(ns_id, key)?;
            let ns = self.namespace(ns_id)?;
            let key_hash = Index::hash_key(key);
            if ns.index.remove(key_hash, key)?.is_some() {
                let _ = ns.record_count.fetch_sub(1, Ordering::AcqRel);
            }
            if let Some(range_map) = ns.range_index.as_ref() {
                let _ = range_map.remove(key);
            }
        }
        Ok(prev)
    }

    /// Number of live records in `ns_id`.
    pub(crate) fn record_count(&self, ns_id: u32) -> Result<u64> {
        let ns = self.namespace(ns_id)?;
        Ok(ns.record_count.load(Ordering::Acquire))
    }

    /// Force pending writes to disk.
    pub(crate) fn flush(&self) -> Result<()> {
        self.store.flush()
    }

    /// Sync the journal, then rewrite the meta sidecar. Backs
    /// [`crate::Emdb::checkpoint`]. Fails when the journal is poisoned
    /// by an earlier write or sync failure.
    pub(crate) fn checkpoint(&self) -> Result<()> {
        self.store.flush()?;
        self.store.persist_meta()
    }

    /// Compute a [`crate::EmdbStats`] snapshot. O(namespaces) plus
    /// one filesystem `metadata` call. Lock contention with active
    /// writers is brief — record counts are atomic loads.
    pub(crate) fn stats(&self) -> Result<crate::EmdbStats> {
        let mut live_records: u64 = 0;
        let mut named_namespace_count: usize = 0;
        {
            let guard = self.namespaces.read();
            for (ns_id, ns) in guard.iter() {
                live_records = live_records.saturating_add(ns.record_count.load(Ordering::Acquire));
                if *ns_id != DEFAULT_NAMESPACE_ID {
                    named_namespace_count += 1;
                }
            }
        }

        let logical_size_bytes = self.store.tail();
        let file_size_bytes = std::fs::metadata(self.store.path())
            .map(|m| m.len())
            .unwrap_or(logical_size_bytes);
        let preallocated_bytes = file_size_bytes.saturating_sub(logical_size_bytes);

        let header = self.store.header()?;
        let encrypted = (header.flags & meta::FLAG_ENCRYPTED) != 0;

        Ok(crate::EmdbStats {
            live_records,
            namespace_count: named_namespace_count,
            logical_size_bytes,
            file_size_bytes,
            preallocated_bytes,
            range_scans_enabled: self.range_scans_enabled,
            encrypted,
        })
    }

    /// Compact the on-disk file by rewriting only live records, then
    /// atomically swapping the new file in for the old.
    ///
    /// Steps, all under the exclusive write gate (writers wait for the
    /// whole compaction; readers continue against the old file):
    ///  1. Create `<path>.compact.tmp` fresh (`create_new`), so a
    ///     stale temporary can never be appended to.
    ///  2. Stream every live record into it in bounded batches,
    ///     reading the source with positioned reads, and build each
    ///     namespace's new index off to the side as the batches land.
    ///  3. Sync the temporary, map it, and rename it over the
    ///     database path; then sync the directory.
    ///  4. Between [`Store::begin_swap`] and [`Store::end_swap`],
    ///     install the new journal, read file and mapping, and swap
    ///     the namespace runtimes in one assignment. Readers never see
    ///     an empty or half-built index.
    ///
    /// # Errors
    ///
    /// I/O errors from the rewrite, sync, or rename steps, and
    /// [`Error::Corrupted`] when a live record cannot be read back. On
    /// any failure before the rename completes, the database keeps
    /// using the original file and the temporary is removed.
    pub(crate) fn compact_in_place(&self) -> Result<()> {
        let _gate = self.write_gate.write();
        let path = self.store.path().to_path_buf();
        let tmp = compaction_temp_path(&path);
        // emdb 1.0.2 and earlier wrote this sidecar and never removed it.
        remove_if_exists(&meta::meta_path_for(&tmp))?;

        let namespaces = self.list_namespaces()?;
        let live = self.live_offsets(&namespaces)?;
        let mut source = SourceReader::open(&path)?;
        let rewrite = Rewrite::create(self.store.fs(), &tmp)?;
        let built = match self
            .write_live(&rewrite, &mut source, &live, true)
            .and_then(|built| rewrite.sync().map(|()| built))
        {
            Ok(built) => built,
            Err(err) => return Err(rewrite.abandon(err)),
        };
        let (read_file, mmap) = match rewrite.open_for_reading() {
            Ok(opened) => opened,
            Err(err) => return Err(rewrite.abandon(err)),
        };

        // Rename first and swap handles only once it succeeded: a
        // failed rename leaves the store on the original file.
        if let Err(err) = std::fs::rename(&tmp, &path) {
            drop((read_file, mmap));
            return Err(rewrite.abandon(Error::Io(err)));
        }
        let dir_synced = sync_dir(&path);

        self.store.begin_swap();
        self.store
            .install_file(rewrite.into_journal(), read_file, mmap);
        *self.namespaces.write() = built;
        self.store.end_swap();

        // The swap is done either way; report a failed directory sync
        // so the caller knows the rename may not survive a power loss.
        dir_synced
    }

    /// Write a snapshot of the live record set to `target`,
    /// producing a self-contained, openable database: the journal at
    /// `target` and its meta sidecar at `<target>.meta`. Backs
    /// [`crate::Emdb::backup_to`].
    ///
    /// The set of live records is captured under the exclusive write
    /// gate (a brief index walk); the records are then copied without
    /// holding it, from a file handle opened during the snapshot, so a
    /// concurrent compaction cannot change what the backup contains.
    ///
    /// The copy goes to `<target>.backup.tmp` (plus its sidecar),
    /// both are synced, then the sidecar and the journal are renamed
    /// over `<target>.meta` and `target` (rename-over, never remove
    /// then rename) and the directory is synced. Encrypted databases
    /// keep their salt and verification block, so the backup opens
    /// with the same key or passphrase.
    ///
    /// Refuses to write to the database's own path.
    pub(crate) fn backup_to(&self, target: &Path) -> Result<()> {
        let source_path = self.store.path().to_path_buf();
        let target_canonical = match target.canonicalize() {
            Ok(p) => p,
            // If the target doesn't exist yet (the common case for a
            // backup), canonicalize fails; in that case compare the
            // raw path against the source. We only canonicalise the
            // source side so symlink shenanigans can't trick the
            // check.
            Err(_) => target.to_path_buf(),
        };
        if let Ok(source_canonical) = source_path.canonicalize() {
            if target_canonical == source_canonical || target == source_path {
                return Err(Error::InvalidConfig(
                    "backup target must differ from the source database path",
                ));
            }
        } else if target == source_path {
            return Err(Error::InvalidConfig(
                "backup target must differ from the source database path",
            ));
        }

        let (live, mut source, header) = {
            let _gate = self.write_gate.write();
            let namespaces = self.list_namespaces()?;
            let live = self.live_offsets(&namespaces)?;
            // Opened while no compaction can run, so it refers to the
            // file the offsets belong to even if one runs later.
            let source = SourceReader::open(&source_path)?;
            (live, source, self.store.header()?)
        };

        let tmp = backup_temp_path(target);
        let tmp_meta = meta::meta_path_for(&tmp);
        let target_meta = meta::meta_path_for(target);
        remove_if_exists(&tmp_meta)?;
        let rewrite = Rewrite::create(self.store.fs(), &tmp)?;
        let written = self
            .write_live(&rewrite, &mut source, &live, false)
            .and_then(|_| rewrite.sync())
            .and_then(|()| meta::write_to(self.store.fs(), &tmp_meta, &header));
        if let Err(err) = written {
            let _ignored = remove_if_exists(&tmp_meta);
            return Err(rewrite.abandon(err));
        }
        drop(rewrite.into_journal());

        // The sidecar goes first: once `target` names the new journal,
        // `<target>.meta` already describes it.
        let committed =
            std::fs::rename(&tmp_meta, &target_meta).and_then(|()| std::fs::rename(&tmp, target));
        if let Err(err) = committed {
            let _ignored = remove_if_exists(&tmp);
            let _ignored = remove_if_exists(&tmp_meta);
            return Err(Error::Io(err));
        }
        sync_dir(target)
    }

    /// Sorted live offsets of every namespace in `namespaces`. The
    /// caller holds the write gate exclusively.
    fn live_offsets(&self, namespaces: &[(u32, String)]) -> Result<Vec<LiveNamespace>> {
        let guard = self.namespaces.read();
        let mut out = Vec::with_capacity(namespaces.len());
        for (ns_id, name) in namespaces {
            let Some(ns) = guard.get(ns_id) else {
                continue;
            };
            let mut offsets = ns.index.collect_offsets()?;
            offsets.sort_unstable();
            out.push(LiveNamespace {
                ns_id: *ns_id,
                name: name.clone(),
                offsets,
            });
        }
        Ok(out)
    }

    /// Copy every record in `live` from `source` into `rewrite` in
    /// batches of about [`REWRITE_CHUNK_BYTES`]. Namespace-name
    /// records go first so a reopen binds every name before it sees
    /// the namespace's records. Payloads are copied verbatim (an
    /// encrypted record stays encrypted under the same key).
    ///
    /// With `build_index`, also builds a fresh runtime per namespace
    /// whose index points at the records' offsets in the new file.
    fn write_live(
        &self,
        rewrite: &Rewrite,
        source: &mut SourceReader,
        live: &[LiveNamespace],
        build_index: bool,
    ) -> Result<HashMap<u32, Arc<NamespaceRuntime>>> {
        let mut names = Vec::new();
        for ns in live {
            if ns.ns_id != DEFAULT_NAMESPACE_ID && !ns.name.is_empty() {
                names.push(self.encode_namespace_name_payload(ns.ns_id, ns.name.as_bytes())?);
            }
        }
        let name_refs: Vec<&[u8]> = names.iter().map(Vec::as_slice).collect();
        let _name_offsets = rewrite.append_batch(&name_refs)?;

        // Resolves hash collisions while the new index is built; reads
        // the temporary file, where the new offsets point.
        let mut written = if build_index {
            Some(SourceReader::open(rewrite.path())?)
        } else {
            None
        };
        let mut built = HashMap::with_capacity(live.len());
        let mut chunk: Vec<u8> = Vec::new();
        let mut ranges: Vec<std::ops::Range<usize>> = Vec::new();
        for ns in live {
            let runtime = NamespaceRuntime::new(self.range_scans_enabled);
            let target = build_index.then_some(&runtime);
            for &offset in &ns.offsets {
                ranges.push(source.append_payload(offset, &mut chunk)?);
                if chunk.len() >= REWRITE_CHUNK_BYTES {
                    self.write_chunk(rewrite, &chunk, &ranges, target, written.as_mut())?;
                    chunk.clear();
                    ranges.clear();
                }
            }
            if !ranges.is_empty() {
                self.write_chunk(rewrite, &chunk, &ranges, target, written.as_mut())?;
                chunk.clear();
                ranges.clear();
            }
            if build_index {
                let _previous = built.insert(ns.ns_id, Arc::new(runtime));
            }
        }
        Ok(built)
    }

    /// Append one batch of payloads to `rewrite` and, when `runtime`
    /// is given, index them at their new offsets.
    fn write_chunk(
        &self,
        rewrite: &Rewrite,
        chunk: &[u8],
        ranges: &[std::ops::Range<usize>],
        runtime: Option<&NamespaceRuntime>,
        written: Option<&mut SourceReader>,
    ) -> Result<()> {
        let payloads: Vec<&[u8]> = ranges.iter().map(|r| &chunk[r.clone()]).collect();
        let starts = rewrite.append_batch(&payloads)?;
        let (Some(runtime), Some(written)) = (runtime, written) else {
            return Ok(());
        };
        for (payload, start) in payloads.iter().zip(starts) {
            let key = self.key_from_payload(payload)?.ok_or(Error::Corrupted {
                offset: start,
                reason: "live index entry does not hold an insert record",
            })?;
            let key_hash = Index::hash_key(&key);
            let prev = runtime.index.replace(key_hash, &key, start, |off| {
                let payload = written.payload(off)?;
                self.key_from_payload(payload)
            })?;
            if prev.is_none() {
                let _ = runtime.record_count.fetch_add(1, Ordering::AcqRel);
            }
            if let Some(range_map) = runtime.range_index.as_ref() {
                let _ = range_map.insert(key, start);
            }
        }
        Ok(())
    }

    /// Encode a `[tag][body]` payload for a namespace-name binding
    /// record. Encrypted databases route the body through the
    /// AEAD path; the resulting payload is `[tag | 0x80][nonce][ct]`.
    /// An empty `name` encodes the record `drop_namespace` writes.
    fn encode_namespace_name_payload(&self, ns_id: u32, name: &[u8]) -> Result<Vec<u8>> {
        #[cfg(feature = "encrypt")]
        if let Some(ctx) = self.encryption.as_ref() {
            let mut body = Vec::with_capacity(8 + name.len());
            format::encode_namespace_name_body(&mut body, ns_id, name);
            let nonce_then_ct = ctx.encrypt(&body)?;
            let mut payload = Vec::with_capacity(1 + nonce_then_ct.len());
            payload.push(format::TAG_NAMESPACE_NAME | format::TAG_ENCRYPTED_FLAG);
            payload.extend_from_slice(&nonce_then_ct);
            return Ok(payload);
        }

        let mut payload = Vec::with_capacity(1 + 8 + name.len());
        payload.push(format::TAG_NAMESPACE_NAME);
        format::encode_namespace_name_body(&mut payload, ns_id, name);
        Ok(payload)
    }

    /// Encode a `[tag][body]` payload for a remove (tombstone) record.
    fn encode_remove_payload(&self, ns_id: u32, key: &[u8]) -> Result<Vec<u8>> {
        #[cfg(feature = "encrypt")]
        if let Some(ctx) = self.encryption.as_ref() {
            let mut body = Vec::with_capacity(8 + key.len());
            format::encode_remove_body(&mut body, ns_id, key);
            let nonce_then_ct = ctx.encrypt(&body)?;
            let mut payload = Vec::with_capacity(1 + nonce_then_ct.len());
            payload.push(format::TAG_REMOVE | format::TAG_ENCRYPTED_FLAG);
            payload.extend_from_slice(&nonce_then_ct);
            return Ok(payload);
        }

        let mut payload = Vec::with_capacity(1 + 8 + key.len());
        payload.push(format::TAG_REMOVE);
        format::encode_remove_body(&mut payload, ns_id, key);
        Ok(payload)
    }

    /// Clear every record in `ns_id`.
    ///
    /// Appends a remove (tombstone) record for every live key, in
    /// bounded batches, and drops each batch's keys from the index as
    /// it lands, so the clear survives a reopen under the same flush
    /// policy as any other remove. Holds the write gate exclusively,
    /// so no insert interleaves with it. Disk space is reclaimed by
    /// the next compaction.
    pub(crate) fn clear_namespace(&self, ns_id: u32) -> Result<()> {
        let _gate = self.write_gate.write();
        let ns = self.namespace(ns_id)?;
        self.tombstone_all(ns_id, &ns)
    }

    /// Tombstone every live key of `ns`. Caller holds the write gate
    /// exclusively.
    fn tombstone_all(&self, ns_id: u32, ns: &NamespaceRuntime) -> Result<()> {
        let mut offsets = ns.index.collect_offsets()?;
        offsets.sort_unstable();
        let mut source = SourceReader::open(self.store.path())?;
        let mut keys: Vec<Vec<u8>> = Vec::new();
        let mut batch_bytes = 0_usize;
        for offset in offsets {
            let payload = source.payload(offset)?;
            let key = self.key_from_payload(payload)?.ok_or(Error::Corrupted {
                offset,
                reason: "live index entry does not hold an insert record",
            })?;
            batch_bytes += key.len() + TOMBSTONE_OVERHEAD;
            keys.push(key);
            if batch_bytes >= REWRITE_CHUNK_BYTES {
                self.tombstone_batch(ns_id, ns, &keys)?;
                keys.clear();
                batch_bytes = 0;
            }
        }
        self.tombstone_batch(ns_id, ns, &keys)
    }

    /// Append tombstones for `keys` as one batch, then drop the keys
    /// from `ns`'s indexes.
    fn tombstone_batch(&self, ns_id: u32, ns: &NamespaceRuntime, keys: &[Vec<u8>]) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let payloads = keys
            .iter()
            .map(|key| self.encode_remove_payload(ns_id, key))
            .collect::<Result<Vec<_>>>()?;
        let _offsets = self
            .store
            .append_batch(payloads.iter().map(Vec::as_slice))?;
        for key in keys {
            let key_hash = Index::hash_key(key);
            if ns.index.remove(key_hash, key)?.is_some() {
                let _ = ns.record_count.fetch_sub(1, Ordering::AcqRel);
            }
            if let Some(range_map) = ns.range_index.as_ref() {
                let _ = range_map.remove(key.as_slice());
            }
        }
        Ok(())
    }

    /// Forget namespace `ns_id` during recovery: its name binding and
    /// its runtime. Applied when the scan meets the empty-name record
    /// `drop_namespace` writes.
    fn unbind_namespace(&self, ns_id: u32) {
        self.namespace_names.write().retain(|_, id| *id != ns_id);
        let _runtime = self.namespaces.write().remove(&ns_id);
        if ns_id as u64 >= self.next_namespace_id.load(Ordering::Acquire) {
            self.next_namespace_id
                .store(ns_id as u64 + 1, Ordering::Release);
        }
    }

    /// Remove the default-namespace keys whose records had expired
    /// when the database was opened. Mirrors the eager sweep earlier
    /// releases ran at open, without materialising every record:
    /// plaintext journals report the expired keys from the recovery
    /// scan itself; encrypted journals are re-read one record at a
    /// time.
    ///
    /// Best effort, like the sweep it replaces: expiry is enforced on
    /// every read regardless, the sweep only keeps `len()` exact and
    /// frees index slots, and failing the open over it would make an
    /// otherwise readable database (for example on a full disk)
    /// impossible to open.
    #[cfg(feature = "ttl")]
    fn sweep_expired_on_open(&self, expired: OpenExpired) {
        let keys = match expired {
            Some(keys) => keys,
            None => match self.expired_keys_by_reading(DEFAULT_NAMESPACE_ID) {
                Ok(keys) => keys,
                Err(_) => return,
            },
        };
        if keys.is_empty() {
            return;
        }
        let _gate = self.write_gate.write();
        let Ok(ns) = self.namespace(DEFAULT_NAMESPACE_ID) else {
            return;
        };
        for batch in keys.chunks(TOMBSTONE_BATCH_KEYS) {
            if self
                .tombstone_batch(DEFAULT_NAMESPACE_ID, &ns, batch)
                .is_err()
            {
                return;
            }
        }
    }

    /// Keys of the live records in `ns_id` that have expired, found
    /// by reading each record from the file (bounded memory).
    #[cfg(feature = "ttl")]
    fn expired_keys_by_reading(&self, ns_id: u32) -> Result<Vec<Vec<u8>>> {
        let now = crate::ttl::now_unix_millis();
        let ns = self.namespace(ns_id)?;
        let mut offsets = ns.index.collect_offsets()?;
        offsets.sort_unstable();
        let mut source = SourceReader::open(self.store.path())?;
        let mut out = Vec::new();
        for offset in offsets {
            let payload = source.payload(offset)?;
            if let Some((key, _value, expires_at)) = self.decode_triple(payload)? {
                if expires_at != 0 && expires_at <= now {
                    out.push(key);
                }
            }
        }
        Ok(out)
    }

    /// Range-scan a namespace's secondary index. Returns `(key, value)`
    /// pairs sorted lexicographically by key. Requires the engine to
    /// have been opened with `enable_range_scans(true)`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfig`] if range scans were not enabled
    /// at open time.
    pub(crate) fn range_scan<R>(&self, ns_id: u32, range: R) -> Result<Vec<(Vec<u8>, Vec<u8>)>>
    where
        R: RangeBounds<Vec<u8>>,
    {
        let (pairs, view) = self.snapshot_range_offsets(ns_id, range)?;

        // Now resolve each offset to its value via the pinned mapping.
        // Using `read_value_in` keeps the encryption-aware decode path.
        let mut out = Vec::with_capacity(pairs.len());
        for (key, offset) in pairs {
            if let Some((value, _expires)) = self.read_value_in(&view, offset, &key)? {
                out.push((key, value));
            }
        }
        Ok(out)
    }

    /// Snapshot the live record offsets in `ns_id`, sorted ascending,
    /// together with a mapping that covers all of them. Used by lazy
    /// iterators (`iter`, `keys`), which decode records on demand with
    /// [`Self::decode_owned_in`] against the returned [`ReadView`].
    pub(crate) fn snapshot_offsets(&self, ns_id: u32) -> Result<(Vec<u64>, ReadView)> {
        self.consistent(|| {
            let ns = self.namespace(ns_id)?;
            let mut offsets = ns.index.collect_offsets()?;
            offsets.sort_unstable();
            let view = self.store.mmap_covering(self.store.tail())?;
            Ok((offsets, view))
        })
    }

    /// Snapshot the (key, offset) pairs in a `range` query via the
    /// lock-free SkipMap iterator, together with a mapping that covers
    /// every offset. Used by lazy range iterators so no lock is held
    /// across the caller's iteration; values are decoded with
    /// [`Self::read_value_in`] against the returned [`ReadView`].
    pub(crate) fn snapshot_range_offsets<R>(&self, ns_id: u32, range: R) -> Result<RangeSnapshot>
    where
        R: RangeBounds<Vec<u8>>,
    {
        self.consistent(|| {
            let ns = self.namespace(ns_id)?;
            let range_map = ns.range_index.as_ref().ok_or(Error::InvalidConfig(
                "range scans not enabled; pass `EmdbBuilder::enable_range_scans(true)` at open time",
            ))?;
            let pairs = skipmap_range_snapshot(range_map, &range);
            let view = self.store.mmap_covering(self.store.tail())?;
            Ok((pairs, view))
        })
    }

    /// Decode the record at `offset` in `view` into an owned tuple.
    /// Used by the lazy iterator's `next()`. Returns `Ok(None)` when
    /// the bytes at `offset` are not a complete `Insert` record.
    pub(crate) fn decode_owned_in(
        &self,
        view: &ReadView,
        offset: u64,
    ) -> Result<Option<RecordSnapshot>> {
        let bytes: &[u8] = view;
        let payload = match format::payload_at(bytes, offset as usize) {
            Ok(p) => p,
            Err(_) => return Ok(None),
        };
        self.decode_triple(payload)
    }

    /// Materialise every live record in `ns_id` as `(key, value, expires_at)`.
    #[cfg(feature = "ttl")]
    pub(crate) fn collect_records(&self, ns_id: u32) -> Result<Vec<RecordSnapshot>> {
        let (offsets, view) = self.snapshot_offsets(ns_id)?;
        let mut out = Vec::with_capacity(offsets.len());
        for offset in offsets {
            if let Some(triple) = self.decode_owned_in(&view, offset)? {
                out.push(triple);
            }
        }
        Ok(out)
    }

    /// Stream every live record in `ns_id` to `sink` in batches of
    /// about [`REWRITE_CHUNK_BYTES`]. Records are read from a mapping
    /// pinned at the start, so the batches form one consistent
    /// snapshot. Unlike [`Self::collect_records`], a live index entry
    /// that does not decode is an error, not a skipped record.
    #[cfg(feature = "encrypt")]
    pub(crate) fn for_each_record_batch<F>(&self, ns_id: u32, mut sink: F) -> Result<()>
    where
        F: FnMut(Vec<RecordSnapshot>) -> Result<()>,
    {
        let (offsets, view) = self.snapshot_offsets(ns_id)?;
        let mut batch = Vec::new();
        let mut batch_bytes = 0_usize;
        for offset in offsets {
            let triple = self
                .decode_owned_in(&view, offset)?
                .ok_or(Error::Corrupted {
                    offset,
                    reason: "live index entry does not hold an insert record",
                })?;
            batch_bytes += triple.0.len() + triple.1.len();
            batch.push(triple);
            if batch_bytes >= REWRITE_CHUNK_BYTES {
                sink(std::mem::take(&mut batch))?;
                batch_bytes = 0;
            }
        }
        if !batch.is_empty() {
            sink(batch)?;
        }
        Ok(())
    }

    /// Decode an insert payload (`[tag][body]`) into an owned
    /// `(key, value, expires_at)` triple, decrypting it on encrypted
    /// databases. `Ok(None)` for other record kinds.
    fn decode_triple(&self, payload: &[u8]) -> Result<Option<RecordSnapshot>> {
        #[cfg(feature = "encrypt")]
        if let Some(ctx) = self.encryption.as_ref() {
            let ctx = Arc::clone(ctx);
            let owned = format::decode_payload_encrypted(payload, |nonce, ct| {
                let mut input = Vec::with_capacity(NONCE_LEN + ct.len());
                input.extend_from_slice(nonce);
                input.extend_from_slice(ct);
                ctx.decrypt(&input)
            })?;
            return Ok(match owned {
                OwnedRecord::Insert {
                    key,
                    value,
                    expires_at,
                    ..
                } => Some((key, value, expires_at)),
                _ => None,
            });
        }

        Ok(match format::decode_payload(payload)? {
            RecordView::Insert {
                key,
                value,
                expires_at,
                ..
            } => Some((key.to_vec(), value.to_vec(), expires_at)),
            _ => None,
        })
    }

    /// Open or create a named namespace. Returns the assigned id.
    pub(crate) fn create_or_open_namespace(&self, name: &str) -> Result<u32> {
        if name.is_empty() {
            return Err(Error::InvalidConfig(
                "namespace name must be non-empty (default namespace is implicit)",
            ));
        }
        let _gate = self.write_gate();
        // Lookup first.
        {
            let guard = self.namespace_names.read();
            if let Some(id) = guard.get(name) {
                return Ok(*id);
            }
        }
        // Allocate a fresh id and persist the name → id binding to disk.
        // We persist BEFORE inserting into the in-memory map so that a
        // crash between the two leaves no in-memory entry without a
        // corresponding on-disk record.
        let mut name_guard = self.namespace_names.write();
        if let Some(id) = name_guard.get(name) {
            return Ok(*id);
        }
        let id = self.next_namespace_id.fetch_add(1, Ordering::AcqRel) as u32;
        // Append the namespace-name binding record. Encrypted databases
        // route through the AEAD path; plaintext databases write the
        // body directly.
        let _record_offset = self.append_namespace_name(id, name)?;
        let _ = name_guard.insert(name.to_string(), id);
        let mut runtimes = self.namespaces.write();
        let _ = runtimes.insert(
            id,
            Arc::new(NamespaceRuntime::new(self.range_scans_enabled)),
        );
        Ok(id)
    }

    /// Append a `TAG_NAMESPACE_NAME` record binding `id` to `name`.
    /// Encrypted databases encrypt the body the same way they encrypt
    /// inserts; the on-disk verification + reopen path naturally
    /// reuses the existing decrypt machinery.
    fn append_namespace_name(&self, ns_id: u32, name: &str) -> Result<u64> {
        #[cfg(feature = "encrypt")]
        if let Some(ctx) = self.encryption.as_ref() {
            let mut payload = Vec::with_capacity(8 + name.len());
            format::encode_namespace_name_body(&mut payload, ns_id, name.as_bytes());
            let nonce_then_ct = ctx.encrypt(&payload)?;
            return self.store.append_with(|buf| {
                buf.push(format::TAG_NAMESPACE_NAME | format::TAG_ENCRYPTED_FLAG);
                buf.extend_from_slice(&nonce_then_ct);
                Ok(())
            });
        }

        self.store.append_with(|buf| {
            buf.push(format::TAG_NAMESPACE_NAME);
            format::encode_namespace_name_body(buf, ns_id, name.as_bytes());
            Ok(())
        })
    }

    /// Drop a named namespace.
    ///
    /// Appends a remove (tombstone) record for every live key, then a
    /// namespace-name record with an empty name, which recovery reads
    /// as "forget this namespace". All of it lands in the journal
    /// under the current flush policy, so neither the data nor the
    /// name comes back after a reopen. emdb 1.0.2 and earlier ignore
    /// the empty-name record; they see the namespace again, empty.
    /// Disk space is reclaimed by the next compaction.
    pub(crate) fn drop_namespace(&self, name: &str) -> Result<bool> {
        if name.is_empty() {
            return Err(Error::InvalidConfig("default namespace cannot be dropped"));
        }
        let _gate = self.write_gate.write();
        let Some(id) = self.namespace_names.read().get(name).copied() else {
            return Ok(false);
        };
        if let Ok(ns) = self.namespace(id) {
            self.tombstone_all(id, &ns)?;
        }
        let unbind = self.encode_namespace_name_payload(id, b"")?;
        let _offset = self.store.append(&unbind)?;
        let _name = self.namespace_names.write().remove(name);
        let _runtime = self.namespaces.write().remove(&id);
        Ok(true)
    }

    /// Enumerate every live namespace as `(id, name)`. The default
    /// namespace is reported with name `""`.
    pub(crate) fn list_namespaces(&self) -> Result<Vec<(u32, String)>> {
        let guard = self.namespace_names.read();
        let mut out: Vec<(u32, String)> = vec![(DEFAULT_NAMESPACE_ID, String::new())];
        for (name, id) in guard.iter() {
            out.push((*id, name.clone()));
        }
        out.sort_by_key(|(id, _)| *id);
        Ok(out)
    }
}

/// Snapshot the `(key, offset)` entries of a `SkipMap` falling inside
/// `bounds` into an owned `Vec`. The skiplist iterator is lock-free,
/// so this lets callers materialise a stable view without holding any
/// lock while they walk the mmap for value bytes.
fn skipmap_range_snapshot<R>(map: &SkipMap<Vec<u8>, u64>, bounds: &R) -> Vec<(Vec<u8>, u64)>
where
    R: RangeBounds<Vec<u8>>,
{
    let start = match bounds.start_bound() {
        Bound::Included(v) => Bound::Included(v.as_slice()),
        Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
        Bound::Unbounded => Bound::Unbounded,
    };
    let end = match bounds.end_bound() {
        Bound::Included(v) => Bound::Included(v.as_slice()),
        Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
        Bound::Unbounded => Bound::Unbounded,
    };
    map.range::<[u8], _>((start, end))
        .map(|entry| (entry.key().clone(), *entry.value()))
        .collect()
}

/// Read-only meta-sidecar peek without opening a full Store.
/// Used by the engine to extract the encryption salt before
/// opening the file with the right key.
#[cfg(feature = "encrypt")]
fn peek_header(path: &Path) -> Result<Option<MetaHeader>> {
    meta::read(path)
}

/// `<path><suffix>` in the same directory, for any file name
/// (including names that are not valid UTF-8).
fn sibling_path(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path
        .file_name()
        .map_or_else(|| std::ffi::OsString::from("emdb"), |n| n.to_os_string());
    name.push(suffix);
    path.with_file_name(name)
}

/// Sibling-file path used by [`Engine::compact_in_place`] as the
/// rewrite target before the atomic rename.
fn compaction_temp_path(path: &Path) -> PathBuf {
    sibling_path(path, ".compact.tmp")
}

/// Sibling-file path used by [`Engine::backup_to`] as the rewrite
/// target before it is renamed onto the backup path.
fn backup_temp_path(target: &Path) -> PathBuf {
    sibling_path(target, ".backup.tmp")
}

/// Encoded size of a tombstone beyond its key: tag, namespace id, key
/// length, and the fsys frame around it. Used to size batches.
const TOMBSTONE_OVERHEAD: usize = 1 + 4 + 4 + 12;

/// Keys per tombstone batch when the keys are already in memory.
#[cfg(feature = "ttl")]
const TOMBSTONE_BATCH_KEYS: usize = 4096;

/// One namespace's live records, as compaction and backup copy them.
struct LiveNamespace {
    ns_id: u32,
    name: String,
    /// Payload offsets of the live records, ascending.
    offsets: Vec<u64>,
}

/// Positioned reads of record payloads straight from a journal file.
///
/// Compaction, backup, `clear` and the encrypted open-time sweep read
/// every live record once. Reading through the shared mapping would
/// fault the whole file into the process's resident set; positioned
/// reads into one reused buffer keep memory bounded by the largest
/// record.
struct SourceReader {
    file: File,
    buf: Vec<u8>,
}

impl SourceReader {
    fn open(path: &Path) -> Result<Self> {
        Ok(Self {
            file: File::open(path)?,
            buf: Vec::new(),
        })
    }

    /// Validate the frame header in front of `payload_start` and
    /// return the payload length.
    fn payload_len(&mut self, payload_start: u64) -> Result<usize> {
        let corrupt = |reason| Error::Corrupted {
            offset: payload_start,
            reason,
        };
        let frame_start = payload_start
            .checked_sub(FSYS_PRE_PAYLOAD_BYTES)
            .ok_or_else(|| corrupt("record offset inside the frame header"))?;
        let mut header = [0_u8; 8];
        let _pos = self.file.seek(SeekFrom::Start(frame_start))?;
        self.file.read_exact(&mut header)?;
        if header[..4] != store::FSYS_FRAME_MAGIC {
            return Err(corrupt("record offset does not point at a journal frame"));
        }
        let len = u64::from(u32::from_le_bytes([
            header[4], header[5], header[6], header[7],
        ]));
        if len > FSYS_MAX_PAYLOAD {
            return Err(corrupt("journal frame length exceeds the 256 MiB cap"));
        }
        usize::try_from(len).map_err(|_| corrupt("journal frame larger than the address space"))
    }

    /// The payload starting at `payload_start`.
    fn payload(&mut self, payload_start: u64) -> Result<&[u8]> {
        let len = self.payload_len(payload_start)?;
        self.buf.resize(len, 0);
        self.file.read_exact(&mut self.buf)?;
        Ok(&self.buf)
    }

    /// Append the payload starting at `payload_start` to `out` and
    /// return where it landed.
    fn append_payload(
        &mut self,
        payload_start: u64,
        out: &mut Vec<u8>,
    ) -> Result<std::ops::Range<usize>> {
        let len = self.payload_len(payload_start)?;
        let start = out.len();
        out.resize(start + len, 0);
        self.file.read_exact(&mut out[start..])?;
        Ok(start..start + len)
    }
}

/// A fresh journal written at a temporary path by compaction or
/// backup.
struct Rewrite {
    path: PathBuf,
    journal: fsys::JournalHandle,
}

impl Rewrite {
    /// Create `path` as a new, empty journal. A leftover file from a
    /// crashed run is removed first and the file is then created with
    /// `create_new`: fsys opens journals for append, so reusing a
    /// stale temporary would carry its records (including keys
    /// removed since) into the rewrite.
    fn create(fs: &fsys::Handle, path: &Path) -> Result<Self> {
        remove_if_exists(path)?;
        // SECURITY-MERGE: create_private_file (owner-only temporary;
        // Ok(false) must be an error here, like `create_new` below)
        drop(OpenOptions::new().write(true).create_new(true).open(path)?);
        match fs.journal_with(path, store::journal_options()) {
            Ok(journal) => Ok(Self {
                path: path.to_path_buf(),
                journal,
            }),
            Err(err) => {
                let _ignored = remove_if_exists(path);
                Err(from_fsys(err))
            }
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    /// Append `payloads` as one batch; returns their payload offsets.
    fn append_batch(&self, payloads: &[&[u8]]) -> Result<Vec<u64>> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }
        let end = self.journal.append_batch(payloads).map_err(from_fsys)?;
        Ok(batch_payload_starts(end.as_u64(), payloads))
    }

    /// Make everything written so far durable.
    fn sync(&self) -> Result<()> {
        self.journal
            .sync_through(self.journal.next_lsn())
            .map_err(from_fsys)
    }

    /// Open a read handle on the rewritten file and map it.
    fn open_for_reading(&self) -> Result<(File, Mmap)> {
        let file = File::open(&self.path)?;
        // SAFETY: read-only mapping of a file this process just wrote
        // and synced. Nothing truncates it: the database lock keeps
        // other emdb instances out, and from here on the file only
        // grows (it becomes the live journal).
        let mmap = unsafe { Mmap::map(&file)? };
        Ok((file, mmap))
    }

    /// Hand over the journal handle (it becomes the live journal after
    /// a compaction, or is closed after a backup).
    fn into_journal(self) -> fsys::JournalHandle {
        self.journal
    }

    /// Give up on the rewrite: close the journal, remove the
    /// temporary, and return `err`. A failed removal is ignored
    /// because `err` is what the caller needs, and the next rewrite
    /// removes the leftover before it starts.
    fn abandon(self, err: Error) -> Error {
        let Self { path, journal } = self;
        drop(journal);
        let _ignored = remove_if_exists(&path);
        err
    }
}

/// What the recovery scan learned about expired records, for the
/// open-time sweep: the default-namespace keys whose live record had
/// already expired, or `None` when the journal holds encrypted records
/// (their expiry is only readable after decrypting them again).
#[cfg(feature = "ttl")]
type OpenExpired = Option<Vec<Vec<u8>>>;
#[cfg(not(feature = "ttl"))]
type OpenExpired = ();

/// Tracks, while the recovery scan replays the journal, which
/// default-namespace keys currently hold an expired record. Reads the
/// plaintext payload the scan already has in memory, so the open-time
/// sweep needs no second pass over the file.
#[cfg(feature = "ttl")]
struct ExpiryTracker {
    now: u64,
    expired: std::collections::HashSet<Vec<u8>>,
    encrypted: bool,
}

#[cfg(feature = "ttl")]
impl ExpiryTracker {
    fn new(now: u64) -> Self {
        Self {
            now,
            expired: std::collections::HashSet::new(),
            encrypted: false,
        }
    }

    fn observe(&mut self, payload: &[u8]) {
        if payload
            .first()
            .is_some_and(|tag| tag & format::TAG_ENCRYPTED_FLAG != 0)
        {
            self.encrypted = true;
            return;
        }
        match format::decode_payload(payload) {
            Ok(RecordView::Insert {
                ns_id: DEFAULT_NAMESPACE_ID,
                key,
                expires_at,
                ..
            }) => {
                if expires_at != 0 && expires_at <= self.now {
                    let _new = self.expired.insert(key.to_vec());
                } else {
                    let _was = self.expired.remove(key);
                }
            }
            Ok(RecordView::Remove {
                ns_id: DEFAULT_NAMESPACE_ID,
                key,
            }) => {
                let _was = self.expired.remove(key);
            }
            _ => {}
        }
    }

    fn finish(self) -> OpenExpired {
        if self.encrypted {
            None
        } else {
            Some(self.expired.into_iter().collect())
        }
    }
}
