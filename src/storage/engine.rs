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

use std::collections::{BTreeMap, HashMap};
use std::ops::{Bound, RangeBounds};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use memmap2::Mmap;

use crate::storage::flush::FlushPolicy;
#[cfg(feature = "encrypt")]
use crate::storage::format::FLAG_CIPHER_CHACHA20;
use crate::storage::format::{self, RecordView, FLAG_ENCRYPTED};
#[cfg(feature = "encrypt")]
use crate::storage::format::{OwnedRecord, NONCE_LEN};
use crate::storage::index::Index;
use crate::storage::store::{Header, Store};
use crate::{Error, Result};

/// Default namespace id (the implicit unnamed namespace).
pub(crate) const DEFAULT_NAMESPACE_ID: u32 = 0;

/// Per-namespace runtime state. The `index` maps `(hash, key) → file
/// offset`; `record_count` tracks live records for cheap `len` queries.
/// When the engine was opened with `enable_range_scans(true)`,
/// `range_index` carries a sorted secondary index (BTreeMap keyed by
/// the actual key bytes) so callers can iterate keys in sorted order.
struct NamespaceRuntime {
    index: Index,
    record_count: AtomicU64,
    range_index: Option<RwLock<BTreeMap<Vec<u8>, u64>>>,
}

impl NamespaceRuntime {
    fn new(range_scans_enabled: bool) -> Self {
        Self {
            index: Index::new(),
            record_count: AtomicU64::new(0),
            range_index: range_scans_enabled.then(|| RwLock::new(BTreeMap::new())),
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

#[cfg(not(feature = "encrypt"))]
pub(crate) type SharedEncryption = ();

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
    /// Optional 32-byte AES-256 key (post-KDF). `None` for unencrypted.
    #[cfg(feature = "encrypt")]
    pub(crate) encryption_key: Option<[u8; 32]>,
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

/// Output of [`Engine::resolve_encryption`]: the resolved 32-byte key,
/// an optional fresh salt to persist for new passphrase databases, and
/// the requested cipher (if explicitly set).
#[cfg(feature = "encrypt")]
type ResolvedEncryption = (
    Option<[u8; 32]>,
    Option<[u8; format::ENCRYPTION_SALT_LEN]>,
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

/// Decoded record tuple emitted by [`Engine::decode_plaintext_at`] /
/// [`Engine::decode_encrypted_at`] during recovery scan:
/// `(action, next_cursor)`.
type RecoveryDecoded = (RecoveryAction, u64);

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
            namespaces: RwLock::new(HashMap::new()),
            namespace_names: RwLock::new(HashMap::new()),
            next_namespace_id: AtomicU64::new(1),
            range_scans_enabled,
            #[cfg(feature = "encrypt")]
            encryption,
        };

        // Always create the default namespace runtime.
        {
            let mut guard = engine.namespaces.write().map_err(|_| Error::LockPoisoned)?;
            let _existing = guard.insert(
                DEFAULT_NAMESPACE_ID,
                Arc::new(NamespaceRuntime::new(range_scans_enabled)),
            );
        }

        // Recovery scan: walk every record from the start of the data
        // region to the on-disk tail (or until the first bad CRC),
        // populating namespace indexes.
        engine.recovery_scan()?;

        Ok(engine)
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
                    if header.encryption_salt == [0_u8; format::ENCRYPTION_SALT_LEN] {
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

        if let Some(key) = config.encryption_key {
            if let Some(header) = peeked {
                if header.encryption_salt != [0_u8; format::ENCRYPTION_SALT_LEN] {
                    return Err(Error::InvalidConfig(
                        "this database was created with an encryption_passphrase; supply via encryption_passphrase, not encryption_key",
                    ));
                }
            }
            return Ok((Some(key), None, cipher));
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
        fresh_salt: Option<[u8; format::ENCRYPTION_SALT_LEN]>,
        existing_header: &Header,
    ) -> Result<()> {
        // If the on-disk verify block is all-zero, we treat this as a
        // fresh file and write a new verification block + salt.
        if existing_header.encryption_verify == [0_u8; format::ENCRYPTION_VERIFY_LEN] {
            let salt = fresh_salt.unwrap_or([0_u8; format::ENCRYPTION_SALT_LEN]);
            // Encrypt the well-known verification plaintext.
            let nonce_then_ct = ctx.encrypt(format::VERIFICATION_PLAINTEXT)?;
            // nonce_then_ct = [nonce(12) | ciphertext(32) + tag(16)] = 60 bytes
            debug_assert_eq!(nonce_then_ct.len(), format::ENCRYPTION_VERIFY_LEN);
            let mut verify = [0_u8; format::ENCRYPTION_VERIFY_LEN];
            verify.copy_from_slice(&nonce_then_ct);
            store.set_encryption_metadata(salt, verify)?;
            return Ok(());
        }

        // Existing encrypted file: decrypt and compare.
        let plaintext = ctx.decrypt(&existing_header.encryption_verify)?;
        if plaintext.as_slice() != format::VERIFICATION_PLAINTEXT {
            return Err(Error::EncryptionKeyMismatch);
        }
        Ok(())
    }

    /// Walk every record from the data region into the index.
    fn recovery_scan(&self) -> Result<()> {
        let mmap = self.store.mmap()?;
        let mut cursor = format::HEADER_LEN as u64;
        let tail_hint = self.store.tail();
        let bytes: &[u8] = &mmap;

        loop {
            if cursor as usize >= bytes.len() {
                break;
            }
            // Stop at the recorded tail unless we're going to find more
            // records past it (post-crash).
            #[cfg(feature = "encrypt")]
            let encrypted = self.encryption.is_some();
            #[cfg(not(feature = "encrypt"))]
            let encrypted = false;
            let result = if encrypted {
                self.decode_encrypted_at(bytes, cursor)?
            } else {
                self.decode_plaintext_at(bytes, cursor)?
            };
            match result {
                Some((action, next)) => {
                    self.apply_recovered_action(action, cursor)?;
                    cursor = next;
                }
                None => break,
            }
            // Defensive: if hint says we're done and we've already walked
            // a full record past it, stop. Otherwise keep going to catch
            // post-crash entries the hint didn't track.
            if cursor > tail_hint && cursor >= bytes.len() as u64 {
                break;
            }
        }

        // Update the writer's tail to where we actually stopped.
        self.store.set_tail_after_recovery(cursor)?;
        Ok(())
    }

    /// Decode a plaintext record at `bytes[cursor..]`. Returns the
    /// decoded action plus the next cursor to resume from.
    fn decode_plaintext_at(&self, bytes: &[u8], cursor: u64) -> Result<Option<RecoveryDecoded>> {
        let start = cursor as usize;
        let decoded = format::try_decode_record(bytes, start, cursor)?;
        match decoded {
            None => Ok(None),
            Some(d) => {
                let action = match d.view {
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
                };
                Ok(Some((action, d.next_offset)))
            }
        }
    }

    /// Decode an encrypted record at `bytes[cursor..]` via the engine's
    /// AEAD context.
    #[cfg(feature = "encrypt")]
    fn decode_encrypted_at(&self, bytes: &[u8], cursor: u64) -> Result<Option<RecoveryDecoded>> {
        let ctx = match self.encryption.as_ref() {
            Some(c) => Arc::clone(c),
            None => {
                // Mixed mode: encrypted scan called with no context.
                // Fall through to plaintext scan.
                return self.decode_plaintext_at(bytes, cursor);
            }
        };
        let start = cursor as usize;
        let result = format::try_decode_encrypted_record(bytes, start, cursor, |nonce, ct| {
            let mut input = Vec::with_capacity(NONCE_LEN + ct.len());
            input.extend_from_slice(nonce);
            input.extend_from_slice(ct);
            ctx.decrypt(&input)
        })?;
        match result {
            None => Ok(None),
            Some((owned, next)) => {
                let action = match owned {
                    OwnedRecord::Insert { ns_id, key, .. } => RecoveryAction::Insert { ns_id, key },
                    OwnedRecord::Remove { ns_id, key } => RecoveryAction::Remove { ns_id, key },
                    OwnedRecord::NamespaceName { ns_id, name } => {
                        RecoveryAction::NamespaceName { ns_id, name }
                    }
                };
                Ok(Some((action, next)))
            }
        }
    }

    #[cfg(not(feature = "encrypt"))]
    fn decode_encrypted_at(&self, bytes: &[u8], cursor: u64) -> Result<Option<RecoveryDecoded>> {
        // No encryption support compiled in; this path should be
        // unreachable, but stay defensive.
        self.decode_plaintext_at(bytes, cursor)
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
                if let Some(range_lock) = ns.range_index.as_ref() {
                    let mut range = range_lock.write().map_err(|_| Error::LockPoisoned)?;
                    let _ = range.insert(key, offset);
                }
            }
            RecoveryAction::Remove { ns_id, key } => {
                let ns = self.ensure_namespace_runtime(ns_id)?;
                let key_hash = Index::hash_key(&key);
                if ns.index.remove(key_hash, &key)?.is_some() {
                    let _ = ns.record_count.fetch_sub(1, Ordering::AcqRel);
                }
                if let Some(range_lock) = ns.range_index.as_ref() {
                    let mut range = range_lock.write().map_err(|_| Error::LockPoisoned)?;
                    let _ = range.remove(&key);
                }
            }
            RecoveryAction::NamespaceName { ns_id, name } => {
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
                let mut name_guard = self
                    .namespace_names
                    .write()
                    .map_err(|_| Error::LockPoisoned)?;
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
        let mmap = self.store.mmap()?;
        let bytes: &[u8] = &mmap;

        #[cfg(feature = "encrypt")]
        if let Some(ctx) = self.encryption.as_ref() {
            let ctx = Arc::clone(ctx);
            return match format::try_decode_encrypted_record(
                bytes,
                offset as usize,
                offset,
                |nonce, ct| {
                    let mut input = Vec::with_capacity(NONCE_LEN + ct.len());
                    input.extend_from_slice(nonce);
                    input.extend_from_slice(ct);
                    ctx.decrypt(&input)
                },
            )? {
                Some((OwnedRecord::Insert { key, .. }, _)) => Ok(Some(key)),
                _ => Ok(None),
            };
        }

        let decoded = format::try_decode_record(bytes, offset as usize, offset)?;
        Ok(match decoded {
            Some(d) => match d.view {
                RecordView::Insert { key, .. } => Some(key.to_vec()),
                _ => None,
            },
            None => None,
        })
    }

    fn ensure_namespace_runtime(&self, ns_id: u32) -> Result<Arc<NamespaceRuntime>> {
        {
            let guard = self.namespaces.read().map_err(|_| Error::LockPoisoned)?;
            if let Some(ns) = guard.get(&ns_id) {
                return Ok(Arc::clone(ns));
            }
        }
        let mut guard = self.namespaces.write().map_err(|_| Error::LockPoisoned)?;
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
        let guard = self.namespaces.read().map_err(|_| Error::LockPoisoned)?;
        guard
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
        let ns = self.namespace(ns_id)?;
        let key_hash = Index::hash_key(key);

        let offset = self.append_insert(ns_id, key, value, expires_at)?;

        let prev = ns
            .index
            .replace(key_hash, key, offset, |off| self.key_at_offset(off))?;
        if prev.is_none() {
            let _ = ns.record_count.fetch_add(1, Ordering::AcqRel);
        }
        if let Some(range_lock) = ns.range_index.as_ref() {
            let mut range = range_lock.write().map_err(|_| Error::LockPoisoned)?;
            let _ = range.insert(key.to_vec(), offset);
        }
        Ok(())
    }

    /// Bulk insert multiple records under a single writer-lock hold.
    /// All records are framed into one buffer and written via a single
    /// `write_all` syscall. Records are NOT atomic as a group (no
    /// Begin/End markers); for atomic batches use the transaction API.
    pub(crate) fn insert_many(
        &self,
        ns_id: u32,
        items: impl IntoIterator<Item = (Vec<u8>, Vec<u8>, u64)>,
    ) -> Result<()> {
        let ns = self.namespace(ns_id)?;
        let items: Vec<(Vec<u8>, Vec<u8>, u64)> = items.into_iter().collect();
        if items.is_empty() {
            return Ok(());
        }

        #[cfg(feature = "encrypt")]
        let encryption = self.encryption.clone();
        #[cfg(not(feature = "encrypt"))]
        let encryption: Option<()> = None;

        // Write all records in a single batched I/O, collecting offsets.
        let offsets = self.store.append_batch_with(|enc| {
            let mut offs = Vec::with_capacity(items.len());
            for (key, value, expires_at) in &items {
                let off = {
                    #[cfg(feature = "encrypt")]
                    {
                        if let Some(ctx) = encryption.as_ref() {
                            let mut payload = Vec::with_capacity(20 + key.len() + value.len());
                            format::encode_insert_body(
                                &mut payload,
                                ns_id,
                                key,
                                value,
                                *expires_at,
                            );
                            let nonce_then_ct = ctx.encrypt(&payload)?;
                            enc.push_record(|buf| {
                                buf.push(format::TAG_INSERT | format::TAG_ENCRYPTED_FLAG);
                                buf.extend_from_slice(&nonce_then_ct);
                                Ok(())
                            })?
                        } else {
                            enc.push_record(|buf| {
                                buf.push(format::TAG_INSERT);
                                format::encode_insert_body(buf, ns_id, key, value, *expires_at);
                                Ok(())
                            })?
                        }
                    }
                    #[cfg(not(feature = "encrypt"))]
                    {
                        let _ = encryption;
                        enc.push_record(|buf| {
                            buf.push(format::TAG_INSERT);
                            format::encode_insert_body(buf, ns_id, key, value, *expires_at);
                            Ok(())
                        })?
                    }
                };
                offs.push(off);
            }
            Ok(offs)
        })?;

        // Now update the index. Records are already on disk; this just
        // bumps the in-memory map.
        let mut range_guard = match ns.range_index.as_ref() {
            Some(lock) => Some(lock.write().map_err(|_| Error::LockPoisoned)?),
            None => None,
        };
        for ((key, _value, _exp), offset) in items.iter().zip(offsets.iter()) {
            let key_hash = Index::hash_key(key);
            let prev = ns
                .index
                .replace(key_hash, key, *offset, |off| self.key_at_offset(off))?;
            if prev.is_none() {
                let _ = ns.record_count.fetch_add(1, Ordering::AcqRel);
            }
            if let Some(range) = range_guard.as_deref_mut() {
                let _ = range.insert(key.clone(), *offset);
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
        let ns = self.namespace(ns_id)?;
        let key_hash = Index::hash_key(key);
        let offset = match ns.index.get(key_hash, key)? {
            Some(o) => o,
            None => return Ok(None),
        };
        self.read_zerocopy_at(offset, key)
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
        let mmap = self.store.mmap()?;
        let bytes: &[u8] = &mmap;

        #[cfg(feature = "encrypt")]
        if let Some(ctx) = self.encryption.as_ref() {
            let ctx = Arc::clone(ctx);
            let result = format::try_decode_encrypted_record(
                bytes,
                offset as usize,
                offset,
                |nonce, ct| {
                    let mut input = Vec::with_capacity(NONCE_LEN + ct.len());
                    input.extend_from_slice(nonce);
                    input.extend_from_slice(ct);
                    ctx.decrypt(&input)
                },
            )?;
            return Ok(match result {
                Some((
                    OwnedRecord::Insert {
                        key,
                        value,
                        expires_at,
                        ..
                    },
                    _,
                )) => {
                    if key.as_slice() == expected_key {
                        Some((crate::ValueRef::from_owned(value), expires_at))
                    } else {
                        None
                    }
                }
                _ => None,
            });
        }

        // Plaintext path: figure out the byte range of the value
        // inside the mmap, then construct an mmap-backed ValueRef.
        let decoded = format::try_decode_record(bytes, offset as usize, offset)?;
        let (value_range, expires_at) = match decoded {
            Some(d) => match d.view {
                RecordView::Insert {
                    key,
                    value,
                    expires_at,
                    ..
                } => {
                    if key != expected_key {
                        return Ok(None);
                    }
                    // Compute the absolute byte range of `value`
                    // inside `mmap` via pointer arithmetic. The
                    // slice `value` was borrowed directly from
                    // `bytes` (== `&mmap`), so subtracting base
                    // pointers is the right way to recover the
                    // offset without re-parsing the framing.
                    let base = bytes.as_ptr() as usize;
                    let val_start = value.as_ptr() as usize - base;
                    let val_end = val_start + value.len();
                    (val_start..val_end, expires_at)
                }
                _ => return Ok(None),
            },
            None => return Ok(None),
        };

        // The `bytes`/`decoded` borrows of `mmap` end here under NLL,
        // so moving `mmap` into `ValueRef` is fine.
        Ok(Some((
            crate::ValueRef::from_mmap(mmap, value_range),
            expires_at,
        )))
    }

    /// Fetch value + expires_at for a key in one pass. Used by the TTL
    /// path in `Emdb::get` so it doesn't have to make two record reads.
    pub(crate) fn get_with_meta(&self, ns_id: u32, key: &[u8]) -> Result<Option<(Vec<u8>, u64)>> {
        let ns = self.namespace(ns_id)?;
        let key_hash = Index::hash_key(key);
        let offset = match ns.index.get(key_hash, key)? {
            Some(o) => o,
            None => return Ok(None),
        };
        self.read_value_at(offset, key)
    }

    fn read_value_at(&self, offset: u64, expected_key: &[u8]) -> Result<Option<(Vec<u8>, u64)>> {
        let mmap = self.store.mmap()?;
        let bytes: &[u8] = &mmap;

        #[cfg(feature = "encrypt")]
        if let Some(ctx) = self.encryption.as_ref() {
            let ctx = Arc::clone(ctx);
            let result = format::try_decode_encrypted_record(
                bytes,
                offset as usize,
                offset,
                |nonce, ct| {
                    let mut input = Vec::with_capacity(NONCE_LEN + ct.len());
                    input.extend_from_slice(nonce);
                    input.extend_from_slice(ct);
                    ctx.decrypt(&input)
                },
            )?;
            return match result {
                Some((
                    OwnedRecord::Insert {
                        key,
                        value,
                        expires_at,
                        ..
                    },
                    _,
                )) => {
                    if key.as_slice() == expected_key {
                        Ok(Some((value, expires_at)))
                    } else {
                        Ok(None)
                    }
                }
                _ => Ok(None),
            };
        }

        let decoded = format::try_decode_record(bytes, offset as usize, offset)?;
        match decoded {
            Some(d) => match d.view {
                RecordView::Insert {
                    key,
                    value,
                    expires_at,
                    ..
                } => {
                    if key == expected_key {
                        Ok(Some((value.to_vec(), expires_at)))
                    } else {
                        Ok(None)
                    }
                }
                _ => Ok(None),
            },
            None => Ok(None),
        }
    }

    /// Remove a key. Returns the previously-associated value, if any.
    pub(crate) fn remove(&self, ns_id: u32, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let prev = self.get(ns_id, key)?;
        if prev.is_some() {
            let _offset = self.append_remove(ns_id, key)?;
            let ns = self.namespace(ns_id)?;
            let key_hash = Index::hash_key(key);
            if ns.index.remove(key_hash, key)?.is_some() {
                let _ = ns.record_count.fetch_sub(1, Ordering::AcqRel);
            }
            if let Some(range_lock) = ns.range_index.as_ref() {
                let mut range = range_lock.write().map_err(|_| Error::LockPoisoned)?;
                let _ = range.remove(key);
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

    /// Persist the in-memory header (with current `tail_hint`) to disk.
    /// Implements the fast-reopen checkpoint exposed via
    /// [`crate::Emdb::checkpoint`].
    pub(crate) fn checkpoint(&self) -> Result<()> {
        self.store.persist_header()
    }

    /// Compact the on-disk file by rewriting only live records, then
    /// atomically swapping the new file in for the old.
    ///
    /// Steps:
    ///  1. Snapshot every namespace's `(key, value, expires_at)` tuples
    ///     by walking the live indexes (one mmap read per record).
    ///  2. Open a fresh [`Store`] at `<path>.compact.tmp` carrying the
    ///     same flags + encryption metadata as the current file.
    ///  3. Bulk-write every snapshotted record into the temp store via
    ///     a single batched `pwrite`.
    ///  4. Sync the temp store, drop its handle, then ask our own
    ///     [`Store::swap_underlying`] to rename the temp file into the
    ///     canonical path and refresh our writer / mmap.
    ///  5. Clear and rebuild every namespace's index from the new
    ///     post-compaction record offsets.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from any of the rewrite, sync, or rename
    /// steps; on failure, the original file is left untouched (the
    /// temp file is best-effort cleaned up). Returns
    /// [`Error::LockPoisoned`] on any poisoned engine lock.
    pub(crate) fn compact_in_place(&self) -> Result<()> {
        // Phase 1: snapshot every namespace + its name (so we can
        // re-emit the name → id binding in the compacted file).
        let namespaces: Vec<(u32, String)> = self.list_namespaces()?;
        let mut snapshots: Vec<(u32, String, Vec<RecordSnapshot>)> =
            Vec::with_capacity(namespaces.len());
        for (ns_id, name) in &namespaces {
            let records = self.collect_records(*ns_id)?;
            snapshots.push((*ns_id, name.clone(), records));
        }

        // Phase 2: write the compacted file directly (no mmap on the
        // temp file; we just need bytes on disk that `Store` can later
        // open). This avoids the Windows "can't shrink a mapped file"
        // problem that would otherwise come up if we routed the temp
        // file through `Store::open` (which pre-allocates 1 MiB).
        let path = self.store.path().to_path_buf();
        let tmp_path = compaction_temp_path(&path);
        let _ = std::fs::remove_file(&tmp_path);

        let header = self.store.header()?;
        if let Err(err) = self.write_compacted_file(&tmp_path, &header, &snapshots) {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(err);
        }

        // Phase 3: atomic swap. After this returns, `self.store` is
        // backed by the new compacted file; old readers' `Arc<Mmap>`
        // snapshots stay valid until they release.
        if let Err(err) = self.store.swap_underlying(&tmp_path) {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(err);
        }

        // Phase 4: clear and rebuild every namespace index from the
        // newly-laid-out file via the recovery scan.
        for (ns_id, _) in &namespaces {
            let ns = self.namespace(*ns_id)?;
            ns.index.clear()?;
            ns.record_count.store(0, Ordering::Release);
            if let Some(range_lock) = ns.range_index.as_ref() {
                let mut range = range_lock.write().map_err(|_| Error::LockPoisoned)?;
                range.clear();
            }
        }
        self.recovery_scan()?;

        Ok(())
    }

    /// Write a fully-formed, sealed database file at `path` containing
    /// just the supplied namespace snapshots. The file is sized exactly
    /// to fit (4 KiB header + framed records, no pre-allocated tail),
    /// then `fdatasync`'d before return. Used by compaction; the file
    /// is then renamed over the canonical path by [`Store::swap_underlying`].
    fn write_compacted_file(
        &self,
        path: &std::path::Path,
        header_template: &Header,
        snapshots: &[(u32, String, Vec<RecordSnapshot>)],
    ) -> Result<()> {
        use std::fs::OpenOptions;
        use std::io::{Seek, SeekFrom, Write};

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        // Write header at offset 0. We'll overwrite it at the end with
        // the correct tail_hint.
        let mut header_buf = [0_u8; format::HEADER_LEN];
        header_template.encode_into(&mut header_buf);
        file.write_all(&header_buf)?;

        // Write records sequentially with framing. We frame manually
        // here because we don't want to go through `BatchEncoder` (it's
        // bound to a `Store`).
        let mut tail = format::HEADER_LEN as u64;
        let mut frame_buf: Vec<u8> = Vec::with_capacity(512);

        // First emit name-binding records for every non-default
        // namespace so a reopen of the compacted file restores the
        // `name → id` map before it walks the data records.
        for (ns_id, name, _) in snapshots {
            if *ns_id == DEFAULT_NAMESPACE_ID || name.is_empty() {
                continue;
            }
            frame_buf.clear();
            frame_buf.extend_from_slice(&[0_u8; 4]);
            let body_start = frame_buf.len();

            #[cfg(feature = "encrypt")]
            {
                if let Some(ctx) = self.encryption.as_ref() {
                    let mut payload = Vec::with_capacity(8 + name.len());
                    format::encode_namespace_name_body(&mut payload, *ns_id, name.as_bytes());
                    let nonce_then_ct = ctx.encrypt(&payload)?;
                    frame_buf.push(format::TAG_NAMESPACE_NAME | format::TAG_ENCRYPTED_FLAG);
                    frame_buf.extend_from_slice(&nonce_then_ct);
                } else {
                    frame_buf.push(format::TAG_NAMESPACE_NAME);
                    format::encode_namespace_name_body(&mut frame_buf, *ns_id, name.as_bytes());
                }
            }
            #[cfg(not(feature = "encrypt"))]
            {
                frame_buf.push(format::TAG_NAMESPACE_NAME);
                format::encode_namespace_name_body(&mut frame_buf, *ns_id, name.as_bytes());
            }

            let body_end = frame_buf.len();
            let body_len = (body_end - body_start) as u32;
            frame_buf[0..4].copy_from_slice(&body_len.to_le_bytes());
            let crc = format::record_crc(&frame_buf[body_start..body_end]);
            frame_buf.extend_from_slice(&crc.to_le_bytes());

            file.write_all(&frame_buf)?;
            tail += frame_buf.len() as u64;
        }

        // Then emit the data records (insert frames) for every namespace.
        for (ns_id, _, records) in snapshots {
            for (key, value, expires_at) in records {
                frame_buf.clear();
                // 4-byte length prefix placeholder.
                frame_buf.extend_from_slice(&[0_u8; 4]);
                let body_start = frame_buf.len();

                #[cfg(feature = "encrypt")]
                {
                    if let Some(ctx) = self.encryption.as_ref() {
                        let mut payload = Vec::with_capacity(20 + key.len() + value.len());
                        format::encode_insert_body(&mut payload, *ns_id, key, value, *expires_at);
                        let nonce_then_ct = ctx.encrypt(&payload)?;
                        frame_buf.push(format::TAG_INSERT | format::TAG_ENCRYPTED_FLAG);
                        frame_buf.extend_from_slice(&nonce_then_ct);
                    } else {
                        frame_buf.push(format::TAG_INSERT);
                        format::encode_insert_body(&mut frame_buf, *ns_id, key, value, *expires_at);
                    }
                }
                #[cfg(not(feature = "encrypt"))]
                {
                    frame_buf.push(format::TAG_INSERT);
                    format::encode_insert_body(&mut frame_buf, *ns_id, key, value, *expires_at);
                }

                let body_end = frame_buf.len();
                let body_len = (body_end - body_start) as u32;
                frame_buf[0..4].copy_from_slice(&body_len.to_le_bytes());
                let crc = format::record_crc(&frame_buf[body_start..body_end]);
                frame_buf.extend_from_slice(&crc.to_le_bytes());

                file.write_all(&frame_buf)?;
                tail += frame_buf.len() as u64;
            }
        }

        // Finalise: rewrite the header with the correct tail_hint and
        // truncate the file to exactly `tail` bytes (no pre-allocated
        // tail; the next `Store::open` will pad on demand if writes
        // come in after the swap).
        let mut final_header = *header_template;
        final_header.tail_hint = tail;
        let mut header_buf = [0_u8; format::HEADER_LEN];
        final_header.encode_into(&mut header_buf);
        let _seek = file.seek(SeekFrom::Start(0))?;
        file.write_all(&header_buf)?;
        file.set_len(tail)?;
        file.sync_data()?;
        Ok(())
    }

    /// On-disk path of the database file.
    pub(crate) fn path(&self) -> &std::path::Path {
        self.store.path()
    }

    /// Clear every record in `ns_id`. Implemented as an index-only
    /// drop plus a flush; the on-disk records remain until compaction.
    pub(crate) fn clear_namespace(&self, ns_id: u32) -> Result<()> {
        let ns = self.namespace(ns_id)?;
        ns.index.clear()?;
        ns.record_count.store(0, Ordering::Release);
        if let Some(range_lock) = ns.range_index.as_ref() {
            let mut range = range_lock.write().map_err(|_| Error::LockPoisoned)?;
            range.clear();
        }
        Ok(())
    }

    /// Range-scan a namespace's secondary index. Returns `(key, value)`
    /// pairs sorted lexicographically by key. Requires the engine to
    /// have been opened with `enable_range_scans(true)`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfig`] if range scans were not enabled
    /// at open time, or [`Error::LockPoisoned`] on poisoned namespace
    /// lock.
    pub(crate) fn range_scan<R>(&self, ns_id: u32, range: R) -> Result<Vec<(Vec<u8>, Vec<u8>)>>
    where
        R: RangeBounds<Vec<u8>>,
    {
        let ns = self.namespace(ns_id)?;
        let range_lock = ns.range_index.as_ref().ok_or(Error::InvalidConfig(
            "range scans not enabled; pass `EmdbBuilder::enable_range_scans(true)` at open time",
        ))?;

        // Snapshot (key, offset) pairs under a read lock, drop the lock
        // before we touch the mmap.
        let pairs: Vec<(Vec<u8>, u64)> = {
            let guard = range_lock.read().map_err(|_| Error::LockPoisoned)?;
            // BTreeMap::range needs `RangeBounds<&[u8]>` semantics; we
            // adapt by converting the user's bounds into byte-slice
            // bounds.
            let start = match range.start_bound() {
                Bound::Included(v) => Bound::Included(v.as_slice()),
                Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
                Bound::Unbounded => Bound::Unbounded,
            };
            let end = match range.end_bound() {
                Bound::Included(v) => Bound::Included(v.as_slice()),
                Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
                Bound::Unbounded => Bound::Unbounded,
            };
            guard
                .range::<[u8], _>((start, end))
                .map(|(k, off)| (k.clone(), *off))
                .collect()
        };

        // Now resolve each offset to its value via the mmap. Using
        // `read_value_at` keeps the encryption-aware decode path.
        let mut out = Vec::with_capacity(pairs.len());
        for (key, offset) in pairs {
            if let Some((value, _expires)) = self.read_value_at(offset, &key)? {
                out.push((key, value));
            }
        }
        Ok(out)
    }

    /// Snapshot the live record offsets in `ns_id`, sorted ascending.
    /// Used by lazy iterators (`iter`, `keys`) so they can decode
    /// records on demand instead of materialising everything up front.
    pub(crate) fn snapshot_offsets(&self, ns_id: u32) -> Result<Vec<u64>> {
        let ns = self.namespace(ns_id)?;
        let mut offsets = ns.index.collect_offsets()?;
        offsets.sort_unstable();
        Ok(offsets)
    }

    /// Snapshot the (key, offset) pairs in a `range` query under a
    /// single read-lock acquisition, sorted by key. Used by lazy
    /// range iterators so the BTreeMap lock isn't held across the
    /// caller's iteration. The keys are cloned out of the BTreeMap
    /// (cheap relative to value reads); offsets are looked up in
    /// the mmap on each `next()`.
    pub(crate) fn snapshot_range_offsets<R>(
        &self,
        ns_id: u32,
        range: R,
    ) -> Result<Vec<(Vec<u8>, u64)>>
    where
        R: RangeBounds<Vec<u8>>,
    {
        let ns = self.namespace(ns_id)?;
        let range_lock = ns.range_index.as_ref().ok_or(Error::InvalidConfig(
            "range scans not enabled; pass `EmdbBuilder::enable_range_scans(true)` at open time",
        ))?;

        let guard = range_lock.read().map_err(|_| Error::LockPoisoned)?;
        let start = match range.start_bound() {
            Bound::Included(v) => Bound::Included(v.as_slice()),
            Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end = match range.end_bound() {
            Bound::Included(v) => Bound::Included(v.as_slice()),
            Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
            Bound::Unbounded => Bound::Unbounded,
        };
        Ok(guard
            .range::<[u8], _>((start, end))
            .map(|(k, off)| (k.clone(), *off))
            .collect())
    }

    /// Decode a single record at `offset` into an owned tuple. Used by
    /// the lazy iterator's `next()`. Returns `Ok(None)` when the
    /// record is no longer a live `Insert` (overwritten in place,
    /// tombstoned, or unrelated record kind at the offset).
    pub(crate) fn decode_owned_at(&self, offset: u64) -> Result<Option<RecordSnapshot>> {
        let mmap = self.store.mmap()?;
        let bytes: &[u8] = &mmap;

        #[cfg(feature = "encrypt")]
        if let Some(ctx) = self.encryption.as_ref() {
            let ctx = Arc::clone(ctx);
            let result = format::try_decode_encrypted_record(
                bytes,
                offset as usize,
                offset,
                |nonce, ct| {
                    let mut input = Vec::with_capacity(NONCE_LEN + ct.len());
                    input.extend_from_slice(nonce);
                    input.extend_from_slice(ct);
                    ctx.decrypt(&input)
                },
            )?;
            return Ok(match result {
                Some((
                    OwnedRecord::Insert {
                        key,
                        value,
                        expires_at,
                        ..
                    },
                    _,
                )) => Some((key, value, expires_at)),
                _ => None,
            });
        }

        Self::decode_plaintext_into_triple(bytes, offset)
    }

    /// Read just the value at `offset`, validating the on-disk key
    /// matches `expected_key`. Returns `Ok(None)` if the record was
    /// overwritten by a later record with a different key (hash
    /// collision repaired) or is no longer an `Insert`. Used by
    /// lazy range iterators that already know the key from the
    /// BTreeMap snapshot.
    pub(crate) fn read_value_with_meta_at(
        &self,
        offset: u64,
        expected_key: &[u8],
    ) -> Result<Option<(Vec<u8>, u64)>> {
        self.read_value_at(offset, expected_key)
    }

    /// Materialise every live record in `ns_id` as `(key, value, expires_at)`.
    pub(crate) fn collect_records(&self, ns_id: u32) -> Result<Vec<RecordSnapshot>> {
        let ns = self.namespace(ns_id)?;
        let mut offsets = ns.index.collect_offsets()?;
        offsets.sort_unstable();
        let mut out = Vec::with_capacity(offsets.len());
        let mmap = self.store.mmap()?;
        let bytes: &[u8] = &mmap;

        for offset in offsets {
            #[cfg(feature = "encrypt")]
            let triple = if let Some(ctx) = self.encryption.as_ref() {
                let ctx = Arc::clone(ctx);
                match format::try_decode_encrypted_record(
                    bytes,
                    offset as usize,
                    offset,
                    |nonce, ct| {
                        let mut input = Vec::with_capacity(NONCE_LEN + ct.len());
                        input.extend_from_slice(nonce);
                        input.extend_from_slice(ct);
                        ctx.decrypt(&input)
                    },
                )? {
                    Some((
                        OwnedRecord::Insert {
                            key,
                            value,
                            expires_at,
                            ..
                        },
                        _,
                    )) => Some((key, value, expires_at)),
                    _ => None,
                }
            } else {
                Self::decode_plaintext_into_triple(bytes, offset)?
            };
            #[cfg(not(feature = "encrypt"))]
            let triple = Self::decode_plaintext_into_triple(bytes, offset)?;

            if let Some(t) = triple {
                out.push(t);
            }
        }
        Ok(out)
    }

    fn decode_plaintext_into_triple(bytes: &[u8], offset: u64) -> Result<Option<RecordSnapshot>> {
        let decoded = format::try_decode_record(bytes, offset as usize, offset)?;
        Ok(match decoded {
            Some(d) => match d.view {
                RecordView::Insert {
                    key,
                    value,
                    expires_at,
                    ..
                } => Some((key.to_vec(), value.to_vec(), expires_at)),
                _ => None,
            },
            None => None,
        })
    }

    /// Open or create a named namespace. Returns the assigned id.
    pub(crate) fn create_or_open_namespace(&self, name: &str) -> Result<u32> {
        if name.is_empty() {
            return Err(Error::InvalidConfig(
                "namespace name must be non-empty (default namespace is implicit)",
            ));
        }
        // Lookup first.
        {
            let guard = self
                .namespace_names
                .read()
                .map_err(|_| Error::LockPoisoned)?;
            if let Some(id) = guard.get(name) {
                return Ok(*id);
            }
        }
        // Allocate a fresh id and persist the name → id binding to disk.
        // We persist BEFORE inserting into the in-memory map so that a
        // crash between the two leaves no in-memory entry without a
        // corresponding on-disk record.
        let mut name_guard = self
            .namespace_names
            .write()
            .map_err(|_| Error::LockPoisoned)?;
        if let Some(id) = name_guard.get(name) {
            return Ok(*id);
        }
        let id = self.next_namespace_id.fetch_add(1, Ordering::AcqRel) as u32;
        // Append the namespace-name binding record. Encrypted databases
        // route through the AEAD path; plaintext databases write the
        // body directly.
        let _record_offset = self.append_namespace_name(id, name)?;
        let _ = name_guard.insert(name.to_string(), id);
        let mut runtimes = self.namespaces.write().map_err(|_| Error::LockPoisoned)?;
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

    /// Tombstone a namespace. Records remain on disk until compaction.
    pub(crate) fn drop_namespace(&self, name: &str) -> Result<bool> {
        if name.is_empty() {
            return Err(Error::InvalidConfig("default namespace cannot be dropped"));
        }
        let mut name_guard = self
            .namespace_names
            .write()
            .map_err(|_| Error::LockPoisoned)?;
        let id = match name_guard.remove(name) {
            Some(id) => id,
            None => return Ok(false),
        };
        let mut runtimes = self.namespaces.write().map_err(|_| Error::LockPoisoned)?;
        let _ = runtimes.remove(&id);
        Ok(true)
    }

    /// Enumerate every live namespace as `(id, name)`. The default
    /// namespace is reported with name `""`.
    pub(crate) fn list_namespaces(&self) -> Result<Vec<(u32, String)>> {
        let guard = self
            .namespace_names
            .read()
            .map_err(|_| Error::LockPoisoned)?;
        let mut out: Vec<(u32, String)> = vec![(DEFAULT_NAMESPACE_ID, String::new())];
        for (name, id) in guard.iter() {
            out.push((*id, name.clone()));
        }
        out.sort_by_key(|(id, _)| *id);
        Ok(out)
    }
}

/// Read-only header peek without opening a full Store. Used by the
/// engine to extract the encryption salt before opening the file with
/// the right key.
fn peek_header(path: &std::path::Path) -> Result<Option<Header>> {
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom};

    match OpenOptions::new().read(true).open(path) {
        Ok(mut file) => {
            if file.metadata()?.len() < format::HEADER_LEN as u64 {
                return Ok(None);
            }
            let mut buf = [0_u8; format::HEADER_LEN];
            let _seek = file.seek(SeekFrom::Start(0))?;
            file.read_exact(&mut buf)?;
            Ok(Some(Header::decode_from(&buf)?))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(Error::from(err)),
    }
}

/// Sibling-file path used by [`Engine::compact_in_place`] as the
/// rewrite target before the atomic rename.
fn compaction_temp_path(path: &std::path::Path) -> std::path::PathBuf {
    let mut out = path.to_path_buf();
    let original_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("emdb");
    out.set_file_name(format!("{original_name}.compact.tmp"));
    out
}

// Suppress unused warning for the import on builds where neither encrypt
// branch references Mmap directly.
#[allow(dead_code)]
fn _mmap_type_anchor() -> Option<Arc<Mmap>> {
    None
}
