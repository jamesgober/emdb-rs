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

use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::ops::{Bound, Deref, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crossbeam_epoch::{self as epoch, Guard};
use crossbeam_skiplist::SkipMap;
use crossbeam_utils::CachePadded;
use memmap2::Mmap;
use parking_lot::{Mutex, MutexGuard, RwLock, RwLockReadGuard};

use crate::error::from_fsys;
use crate::storage::arc_cell::ArcCell;
use crate::storage::flush::FlushPolicy;
use crate::storage::format::{self, RecordView};
#[cfg(feature = "encrypt")]
use crate::storage::format::{OwnedRecord, NONCE_LEN};
use crate::storage::index::{Index, KeyCheck, KeyHash, KeyHasher};
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

/// Per-namespace runtime state. The `index` maps `(hash, key)` to the
/// file offset of the key's live record and also provides the live
/// record count (`Index::len`), so the count can never drift from the
/// index. When the engine was opened with `enable_range_scans(true)`,
/// `range_index` carries a sorted secondary index: a lock-free
/// `crossbeam_skiplist::SkipMap` keyed by the key bytes. Both are
/// updated under the key's write stripe, so for every key the skiplist
/// agrees with the hash index once the write returns.
///
/// Compaction builds a new runtime per namespace (the offsets change)
/// and installs it in one step; a runtime is never cleared in place.
pub(crate) struct NamespaceRuntime {
    index: Index,
    range_index: Option<Arc<SkipMap<Vec<u8>, u64>>>,
}

impl NamespaceRuntime {
    fn new(range_scans_enabled: bool) -> Self {
        Self {
            index: Index::new(),
            range_index: range_scans_enabled.then(|| Arc::new(SkipMap::new())),
        }
    }
}

impl std::fmt::Debug for NamespaceRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NamespaceRuntime")
            .field("len", &self.index.len())
            .finish()
    }
}

/// A namespace runtime handed out for one operation. The default
/// namespace is borrowed straight from the engine's cell, so the hot
/// paths take no lock and touch no reference count; named namespaces
/// are looked up in the map and held by `Arc`.
enum NsRef<'a> {
    Default(&'a NamespaceRuntime),
    Named(Arc<NamespaceRuntime>),
}

impl Deref for NsRef<'_> {
    type Target = NamespaceRuntime;

    #[inline]
    fn deref(&self) -> &NamespaceRuntime {
        match self {
            Self::Default(ns) => ns,
            Self::Named(ns) => ns,
        }
    }
}

/// Number of per-key write stripes. Power of two.
const WRITE_STRIPES: usize = 1024;

/// Striped per-key write locks.
///
/// Every write to a key (insert, remove, batch, TTL sweep, persist)
/// holds the key's stripe across the journal append and the index and
/// skiplist updates. Writes to the same key are therefore
/// linearizable, and they reach the log in the same order as they
/// reach the in-memory index, which is the order recovery replays.
/// Batches lock their stripes in ascending order, so two batches
/// cannot deadlock. Readers never take these locks.
///
/// Lock order, outermost first: the engine write gate (shared mode),
/// stripes in ascending order, then the index shard writer mutex and
/// the store's append-order mutex. Nothing that holds a stripe waits
/// for another stripe out of order, and nothing that holds a stripe or
/// the gate takes the gate again (see [`Engine::lock_key`]).
struct WriteStripes {
    stripes: Box<[CachePadded<Mutex<()>>]>,
}

/// The write gate (shared) and the stripes held by one write
/// operation; released on drop, stripes first. Every engine write
/// entry point that touches keys acquires it through
/// [`Engine::lock_key`] or [`Engine::lock_keys`].
#[must_use = "the write gate and stripes are released when this guard drops"]
pub(crate) struct WriteGuard<'a> {
    _stripes: StripeSet<'a>,
    _gate: RwLockReadGuard<'a, ()>,
}

/// The stripe guards are held only to be dropped.
enum StripeSet<'a> {
    One { _guard: MutexGuard<'a, ()> },
    Many { _guards: Vec<MutexGuard<'a, ()>> },
}

impl WriteStripes {
    fn new() -> Self {
        Self {
            stripes: (0..WRITE_STRIPES)
                .map(|_| CachePadded::new(Mutex::new(())))
                .collect(),
        }
    }

    /// Stripe of `(ns_id, hash)`. Mixes hash bits that neither the
    /// shard selector nor the home slot depend on.
    #[inline]
    fn stripe_of(ns_id: u32, hash: KeyHash) -> usize {
        let mixed = hash.rotate_right(24) ^ u64::from(ns_id).wrapping_mul(0x9e37_79b9_7f4a_7c15);
        (mixed as usize) & (WRITE_STRIPES - 1)
    }

    fn lock_one(&self, ns_id: u32, hash: KeyHash) -> StripeSet<'_> {
        StripeSet::One {
            _guard: self.stripes[Self::stripe_of(ns_id, hash)].lock(),
        }
    }

    /// Lock the stripes of every hash in `hashes`, each once, in
    /// ascending stripe order.
    fn lock_many(&self, ns_id: u32, hashes: &[KeyHash]) -> StripeSet<'_> {
        let mut wanted = [0_u64; WRITE_STRIPES / 64];
        for &hash in hashes {
            let stripe = Self::stripe_of(ns_id, hash);
            wanted[stripe / 64] |= 1 << (stripe % 64);
        }
        let mut guards = Vec::with_capacity(hashes.len().min(WRITE_STRIPES));
        for (word_index, &word) in wanted.iter().enumerate() {
            let mut bits = word;
            while bits != 0 {
                let bit = bits.trailing_zeros() as usize;
                bits &= bits - 1;
                guards.push(self.stripes[word_index * 64 + bit].lock());
            }
        }
        StripeSet::Many { _guards: guards }
    }
}

/// One operation of an [`Engine::write_batch`].
#[derive(Debug)]
pub(crate) enum BatchOp {
    Insert {
        key: Vec<u8>,
        value: Vec<u8>,
        expires_at: u64,
    },
    Remove {
        key: Vec<u8>,
    },
}

impl BatchOp {
    fn key(&self) -> &[u8] {
        match self {
            Self::Insert { key, .. } | Self::Remove { key } => key,
        }
    }
}

/// A decoded live `Insert` record. Plaintext records borrow the key
/// and value from the bytes they were decoded from; encrypted records
/// own the decrypted plaintext.
enum Decoded<'a> {
    Borrowed {
        key: &'a [u8],
        value: &'a [u8],
        expires_at: u64,
    },
    // Constructed only by the encrypted read path.
    #[cfg_attr(not(feature = "encrypt"), allow(dead_code))]
    Owned {
        key: Vec<u8>,
        value: Vec<u8>,
        expires_at: u64,
    },
}

impl Decoded<'_> {
    fn key(&self) -> &[u8] {
        match self {
            Self::Borrowed { key, .. } => key,
            Self::Owned { key, .. } => key,
        }
    }

    fn expires_at(&self) -> u64 {
        match self {
            Self::Borrowed { expires_at, .. } | Self::Owned { expires_at, .. } => *expires_at,
        }
    }

    fn into_value(self) -> Vec<u8> {
        match self {
            Self::Borrowed { value, .. } => value.to_vec(),
            Self::Owned { value, .. } => value,
        }
    }

    fn into_triple(self) -> RecordSnapshot {
        match self {
            Self::Borrowed {
                key,
                value,
                expires_at,
            } => (key.to_vec(), value.to_vec(), expires_at),
            Self::Owned {
                key,
                value,
                expires_at,
            } => (key, value, expires_at),
        }
    }
}

/// True when a record with `expires_at` is live at `now_ms`.
/// `expires_at == 0` means "no TTL"; `now_ms == 0` disables expiry
/// checks (builds without the `ttl` feature pass 0).
#[inline]
pub(crate) fn is_live(expires_at: u64, now_ms: u64) -> bool {
    expires_at == 0 || now_ms == 0 || expires_at > now_ms
}

/// Cursor over a namespace's sorted secondary index.
///
/// Holds no snapshot of the keys: each [`Engine::fill_range`] seeks
/// the skiplist from the last key handed out, so
/// `iter_from(..).take(10)` costs one seek and ten entries however
/// many keys follow. Iteration is weakly consistent: keys come out in
/// ascending order, each at most once; keys inserted or removed ahead
/// of the cursor while it runs may or may not be observed.
///
/// The offsets a page holds are resolved against [`Self::view`], a
/// mapping pinned in the same consistent step as the page, so a
/// compaction that runs while the page is consumed does not change
/// what it yields. A compaction does change the skiplist itself (the
/// offsets in the old one point into the retired file), so the next
/// fill notices the new file generation and re-attaches to the
/// namespace's current skiplist, continuing after the last key it
/// handed out.
pub(crate) struct RangeCursor {
    ns_id: u32,
    map: Arc<SkipMap<Vec<u8>, u64>>,
    /// File generation `map` belongs to (see [`Store::read_begin`]).
    generation: u64,
    view: ReadView,
    next: Bound<Vec<u8>>,
    end: Bound<Vec<u8>>,
    done: bool,
}

impl std::fmt::Debug for RangeCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RangeCursor")
            .field("ns_id", &self.ns_id)
            .field("generation", &self.generation)
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl RangeCursor {
    /// The mapping the most recent page's offsets resolve against.
    pub(crate) fn view(&self) -> &ReadView {
        &self.view
    }
}

fn bound_as_slice(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match bound {
        Bound::Included(v) => Bound::Included(v.as_slice()),
        Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn owned_bound(bound: Bound<&Vec<u8>>) -> Bound<Vec<u8>> {
    match bound {
        Bound::Included(v) => Bound::Included(v.clone()),
        Bound::Excluded(v) => Bound::Excluded(v.clone()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// Optional encryption context. Held inside an `Arc` so `Engine` is
/// `Send + Sync` and the cipher state is shared between callers.
#[cfg(feature = "encrypt")]
pub(crate) type SharedEncryption = Option<Arc<crate::encryption::EncryptionContext>>;

/// Configuration handed to [`Engine::open`] by the builder.
#[derive(Clone)]
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
    /// were set. Wiped on drop.
    #[cfg(feature = "encrypt")]
    pub(crate) encryption_passphrase: Option<crate::encryption::Passphrase>,
}

/// Hand-written so the key and passphrase never reach a log line.
/// `Zeroizing<T>` forwards `Debug` to the wrapped bytes, so a derived
/// impl would print the raw key.
impl std::fmt::Debug for EngineConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("EngineConfig");
        let _ = s
            .field("path", &self.path)
            .field("flags", &self.flags)
            .field("enable_range_scans", &self.enable_range_scans)
            .field("flush_policy", &self.flush_policy)
            .field("iouring_sqpoll_idle_ms", &self.iouring_sqpoll_idle_ms);
        #[cfg(feature = "encrypt")]
        {
            let _ = s
                .field(
                    "encryption_key",
                    &self.encryption_key.as_ref().map(|_| "<redacted>"),
                )
                .field("cipher", &self.cipher)
                .field(
                    "encryption_passphrase",
                    &self.encryption_passphrase.as_ref().map(|_| "<redacted>"),
                );
        }
        s.finish()
    }
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

/// The storage engine behind one database handle.
pub(crate) struct Engine {
    store: Arc<Store>,
    /// Write gate. Every write holds it shared for the whole append
    /// plus index update: key writes through [`Self::lock_key`] /
    /// [`Self::lock_keys`], namespace creation directly. Compaction,
    /// `clear`, `drop_namespace` and the snapshot phase of backup hold
    /// it exclusively, so they see no half-applied write and no write
    /// can land in a file compaction is about to retire. Readers never
    /// take it.
    write_gate: RwLock<()>,
    /// The default namespace's runtime, the same `Arc` that
    /// `namespaces` holds under id 0. Published through an epoch cell
    /// so the hot paths reach it without the map's lock; replaced only
    /// by compaction, under the exclusive write gate.
    default_ns: ArcCell<NamespaceRuntime>,
    /// Keyed hash shared by every namespace index and the write
    /// stripes. Fresh secrets per open; the index is never persisted.
    hasher: KeyHasher,
    /// Per-key write stripes (see [`WriteStripes`]).
    write_stripes: WriteStripes,
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

/// A read mapping pinned for the lifetime of an iterator (or of one
/// page of a range cursor). Offsets are resolved against the mapping
/// taken in the same consistent step, so a compaction that replaces
/// the file while the iterator runs does not change what it yields:
/// it keeps reading the retired file, whose mapping stays valid while
/// pinned.
pub(crate) type ReadView = Arc<Mmap>;

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

/// What the recovery scan remembers between records.
#[derive(Default)]
struct RecoveryState {
    /// Namespaces unbound by a `drop_namespace` record, by id, with
    /// the name they had (empty when the id had no name left).
    dropped: HashMap<u32, String>,
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
        let default_ns = Arc::new(NamespaceRuntime::new(range_scans_enabled));
        let mut namespaces = HashMap::new();
        let _none = namespaces.insert(DEFAULT_NAMESPACE_ID, Arc::clone(&default_ns));
        let mut engine = Self {
            store,
            write_gate: RwLock::new(()),
            default_ns: ArcCell::new(default_ns),
            hasher: KeyHasher::random(),
            write_stripes: WriteStripes::new(),
            namespaces: RwLock::new(namespaces),
            namespace_names: RwLock::new(HashMap::new()),
            next_namespace_id: AtomicU64::new(1),
            range_scans_enabled,
            #[cfg(feature = "encrypt")]
            encryption,
        };

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
        Arc::get_mut(&mut engine.store)
            .ok_or(Error::InvalidConfig(
                "store shared before the open finished",
            ))?
            .open_journal()?;
        engine.store.finish_open()?;
        engine.remove_stale_rewrite_files();

        #[cfg(feature = "ttl")]
        engine.sweep_expired_on_open(expired);

        Ok(engine)
    }

    /// Acquire the write gate (shared) and the write stripe for one
    /// key. Every single-key write entry point goes through here.
    ///
    /// The gate is a fair `RwLock`: a thread that already holds it
    /// shared must not take it again, because a compaction waiting for
    /// it exclusively would block the second acquisition forever. So
    /// it is taken here and nowhere a caller could already hold it:
    /// batches go through [`Self::lock_keys`] once (`insert_many`
    /// delegates to [`Self::write_batch`] without locking), and
    /// nothing called with a [`WriteGuard`] held locks again.
    fn lock_key(&self, ns_id: u32, hash: KeyHash) -> WriteGuard<'_> {
        let gate = self.write_gate.read();
        WriteGuard {
            _stripes: self.write_stripes.lock_one(ns_id, hash),
            _gate: gate,
        }
    }

    /// Acquire the write gate (shared) and the write stripes for a
    /// batch, in ascending stripe order. Same rule as
    /// [`Self::lock_key`].
    fn lock_keys(&self, ns_id: u32, hashes: &[KeyHash]) -> WriteGuard<'_> {
        let gate = self.write_gate.read();
        WriteGuard {
            _stripes: self.write_stripes.lock_many(ns_id, hashes),
            _gate: gate,
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
        let keyed = config.encryption_key.is_some() || config.encryption_passphrase.is_some();
        if !keyed {
            // Unencrypted opens. If the file carries any encryption
            // metadata, fail loudly instead of appending plaintext
            // records to an encrypted log.
            if let Some(header) = peeked {
                if header.flags & FLAG_ENCRYPTED != 0
                    || header.encryption_verify != [0_u8; meta::META_VERIFY_LEN]
                {
                    return Err(Error::InvalidConfig(
                        "this database was created with at-rest encryption; supply encryption_key or encryption_passphrase",
                    ));
                }
            }
            return Ok((None, None, None));
        }

        // A keyed open initialises encryption only on a database that
        // has no records yet. An existing database without a
        // verification block is a plaintext database (or one whose
        // meta sidecar was lost). Accepting it would mark it encrypted
        // while its records stay plaintext, and every later open would
        // have to trust unauthenticated records. Refuse before anything
        // is written, so the database stays usable without a key.
        let has_verify_block =
            peeked.is_some_and(|h| h.encryption_verify != [0_u8; meta::META_VERIFY_LEN]);
        if !has_verify_block && journal_has_bytes(&config.path)? {
            return Err(Error::InvalidConfig(
                "this database is not encrypted (it has records but no encryption metadata); open it without a key, or convert it first with Emdb::enable_encryption",
            ));
        }
        let on_disk_cipher = peeked
            .filter(|_| has_verify_block)
            .map(|h| Self::cipher_from_flags(h.flags));

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
                Some(header) if has_verify_block => {
                    if header.encryption_salt == [0_u8; meta::META_SALT_LEN] {
                        return Err(Error::InvalidConfig(
                            "this database was created with a raw encryption_key; supply via encryption_key, not encryption_passphrase",
                        ));
                    }
                    (header.encryption_salt, None)
                }
                // No verification block and no records: a new
                // database. Generate the salt it will keep.
                _ => {
                    let s = crate::encryption::random_salt()?;
                    (s, Some(s))
                }
            };
            let derived = crate::encryption::derive_key_from_passphrase(passphrase, &salt)?;
            return Ok((Some(derived), fresh, cipher));
        }

        if let Some(key) = config.encryption_key.as_ref() {
            if let Some(header) = peeked {
                if has_verify_block && header.encryption_salt != [0_u8; meta::META_SALT_LEN] {
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
        // An all-zero verify block on a journal with no records is a
        // fresh file: write a new verification block + salt. With
        // records present it is a plaintext database, which a keyed
        // open must not adopt (`resolve_encryption` already refuses
        // this before the store opens; checked again here against the
        // journal the store actually opened).
        if existing_header.encryption_verify == [0_u8; meta::META_VERIFY_LEN] {
            if store.tail() != 0 {
                return Err(Error::InvalidConfig(
                    "this database is not encrypted (it has records but no encryption metadata); open it without a key, or convert it first with Emdb::enable_encryption",
                ));
            }
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
        let mut state = RecoveryState::default();
        #[cfg(feature = "ttl")]
        let mut expiry = ExpiryTracker::new(crate::ttl::now_unix_millis());
        for record_result in reader.iter() {
            let record = record_result.map_err(from_fsys)?;
            let payload_start = record.lsn.as_u64() + FSYS_PRE_PAYLOAD_BYTES;
            self.apply_recovered_payload(&record.payload, payload_start, &mut state)?;
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
    ///
    /// In an encrypted database every record must carry the encrypted
    /// flag. A plaintext record there was not written by emdb (the
    /// engine encrypts every record once a key is set), so it is
    /// rejected as [`Error::Corrupted`] instead of being applied
    /// without authentication.
    fn apply_recovered_payload(
        &self,
        payload: &[u8],
        payload_start: u64,
        state: &mut RecoveryState,
    ) -> Result<()> {
        if payload.is_empty() {
            return Err(Error::Corrupted {
                offset: payload_start,
                reason: "empty record payload during recovery",
            });
        }
        let tag = payload[0];
        let encrypted = (tag & format::TAG_ENCRYPTED_FLAG) != 0;

        #[cfg(feature = "encrypt")]
        if !encrypted && self.encryption.is_some() {
            return Err(Error::Corrupted {
                offset: payload_start,
                reason: "plaintext record in an encrypted database",
            });
        }

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
                })
                .map_err(|err| relocate_corruption(err, payload_start))?;
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
                return Err(Error::InvalidConfig(
                    "encrypted record present but the `encrypt` feature is not compiled in",
                ));
            }
        } else {
            match format::decode_payload(payload)
                .map_err(|err| relocate_corruption(err, payload_start))?
            {
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

        self.apply_recovered_action(action, payload_start, state)
    }

    /// Apply one decoded record during recovery.
    ///
    /// Namespace rules (every file emdb writes satisfies them, because
    /// a `NamespaceName` record is appended before the first record of
    /// a namespace and ids are never reused):
    ///
    /// - An `Insert` or `Remove` must target the default namespace or
    ///   an id bound earlier in the log by a `NamespaceName` record.
    ///   Anything else is [`Error::Corrupted`]. This stops a small
    ///   crafted file from creating a namespace runtime (and its index
    ///   allocation) for every id it mentions.
    /// - A `NamespaceName` must bind a non-empty UTF-8 name to an id in
    ///   `1..u32::MAX`, and must not rebind an id that is already bound
    ///   to a different name or that an earlier record unbound.
    /// - A `NamespaceName` with an empty name is the unbind record
    ///   [`Self::drop_namespace`] appends after the tombstones of every
    ///   key. It must name an id that is bound at that point.
    /// - An `Insert` or `Remove` for an id unbound earlier in the log
    ///   comes from emdb 1.0.2 or older, which skips the unbind record
    ///   and still lists the dropped namespace (empty) under its old
    ///   name. Such a record binds the id to that name again, as the
    ///   older release saw it; if the name now belongs to another id,
    ///   the record is [`Error::Corrupted`].
    fn apply_recovered_action(
        &self,
        action: RecoveryAction,
        offset: u64,
        state: &mut RecoveryState,
    ) -> Result<()> {
        match action {
            RecoveryAction::Insert { ns_id, key } => {
                let ns = self.recovered_namespace(ns_id, offset, state)?;
                let key_hash = self.hasher.hash(&key);
                self.index_insert(&ns, ns_id, key_hash, &key, offset)?;
            }
            RecoveryAction::Remove { ns_id, key } => {
                let ns = self.recovered_namespace(ns_id, offset, state)?;
                let key_hash = self.hasher.hash(&key);
                self.index_remove(&ns, ns_id, key_hash, &key)?;
            }
            RecoveryAction::NamespaceName { ns_id, name } => {
                if ns_id == DEFAULT_NAMESPACE_ID || ns_id == u32::MAX {
                    // The engine never binds the default namespace and
                    // never allocates u32::MAX.
                    return Err(Error::Corrupted {
                        offset,
                        reason: "namespace-name record with a reserved id",
                    });
                }
                if name.is_empty() {
                    // The record `drop_namespace` writes after the
                    // tombstones of every key: forget the binding.
                    return self.unbind_recovered_namespace(ns_id, offset, state);
                }
                if state.dropped.contains_key(&ns_id) {
                    // Ids are never reused within one log, so a new
                    // name for a dropped id was not written by emdb.
                    return Err(Error::Corrupted {
                        offset,
                        reason: "namespace-name record rebinds a dropped namespace id",
                    });
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
                let mut name_guard = self.namespace_names.write();
                // An id that already has a runtime was bound by an
                // earlier record. Repeating the same binding is
                // harmless; binding it to a different name is not
                // something emdb ever writes.
                let already_bound = self.namespaces.read().contains_key(&ns_id);
                if already_bound && name_guard.get(name_str.as_str()) != Some(&ns_id) {
                    return Err(Error::Corrupted {
                        offset,
                        reason: "namespace id bound to a second name",
                    });
                }
                // Register the runtime if absent so subsequent inserts
                // into this ns_id land in the right place. Then bind
                // the name → id mapping.
                let _ = self.ensure_namespace_runtime(ns_id)?;
                let _existing = name_guard.insert(name_str, ns_id);
                drop(name_guard);
            }
        }
        Ok(())
    }

    /// Runtime for a namespace referenced by a recovered `Insert` or
    /// `Remove`. Only the default namespace and ids already bound by a
    /// `NamespaceName` record have one; an unknown id means the log
    /// was not written by emdb.
    fn recovered_namespace(
        &self,
        ns_id: u32,
        offset: u64,
        state: &mut RecoveryState,
    ) -> Result<Arc<NamespaceRuntime>> {
        if let Some(ns) = self.namespaces.read().get(&ns_id) {
            return Ok(Arc::clone(ns));
        }
        let Some(name) = state.dropped.remove(&ns_id) else {
            return Err(Error::Corrupted {
                offset,
                reason: "record references a namespace id with no namespace-name binding",
            });
        };
        let mut name_guard = self.namespace_names.write();
        if name.is_empty() || name_guard.contains_key(name.as_str()) {
            return Err(Error::Corrupted {
                offset,
                reason: "record references a dropped namespace whose name was reused",
            });
        }
        let ns = self.ensure_namespace_runtime(ns_id)?;
        let _previous = name_guard.insert(name, ns_id);
        Ok(ns)
    }

    /// Apply the empty-name record [`Self::drop_namespace`] writes:
    /// forget the name binding and the runtime of `ns_id`, and
    /// remember the name in `state` for records an older release may
    /// have appended to the namespace afterwards (see
    /// [`Self::apply_recovered_action`]). The id allocator moved past
    /// `ns_id` when it was bound, so the id is never handed out again
    /// for this log.
    fn unbind_recovered_namespace(
        &self,
        ns_id: u32,
        offset: u64,
        state: &mut RecoveryState,
    ) -> Result<()> {
        if self.namespaces.write().remove(&ns_id).is_none() {
            return Err(Error::Corrupted {
                offset,
                reason: "namespace unbind record for an id that is not bound",
            });
        }
        let mut name_guard = self.namespace_names.write();
        let name = name_guard
            .iter()
            .find_map(|(name, id)| (*id == ns_id).then(|| name.clone()))
            .unwrap_or_default();
        let _bound = name_guard.remove(name.as_str());
        let _previous = state.dropped.insert(ns_id, name);
        Ok(())
    }

    /// Index resolver: compare the key of the record at `offset` with
    /// `key` without allocating in the common (same key) case. Used by
    /// [`Index::replace`] and [`Index::remove`] to tell an overwrite
    /// apart from a 64-bit hash collision. Callers hold the key's
    /// write stripe and the write gate (or run single-threaded during
    /// recovery), so no compaction can swap the file underneath.
    fn check_key_at(&self, ns_id: u32, offset: u64, key: &[u8]) -> Result<KeyCheck> {
        let guard = epoch::pin();
        Ok(match self.decode_insert_at(ns_id, offset, &guard)? {
            Some(record) if record.key() == key => KeyCheck::Same,
            Some(record) => KeyCheck::Other(record.key().to_vec()),
            None => KeyCheck::Unreadable,
        })
    }

    /// Decode the record at `offset` from the live mapping if it is an
    /// `Insert` in `ns_id`. Returns `Ok(None)` for other record kinds,
    /// other namespaces and offsets that do not frame a whole record.
    /// This is the read path's single choke point; callers that took
    /// `offset` from an index without holding the write gate run it
    /// inside [`Self::consistent`].
    fn decode_insert_at<'a>(
        &'a self,
        ns_id: u32,
        offset: u64,
        guard: &'a Guard,
    ) -> Result<Option<Decoded<'a>>> {
        match self.store.payload(offset, guard)? {
            Some((payload, _view)) => self.decode_insert_payload(payload, ns_id),
            None => Ok(None),
        }
    }

    /// Decode the record at `offset` in a pinned `view` if it is an
    /// `Insert` in `ns_id` (see [`Self::decode_insert_at`]).
    fn decode_insert_in<'a>(
        &self,
        view: &'a ReadView,
        ns_id: u32,
        offset: u64,
    ) -> Result<Option<Decoded<'a>>> {
        let Ok(start) = usize::try_from(offset) else {
            return Ok(None);
        };
        match format::payload_at(view, start) {
            Ok(payload) => self.decode_insert_payload(payload, ns_id),
            Err(_) => Ok(None),
        }
    }

    /// Decode an `[tag][body]` payload if it is an `Insert` in `ns_id`,
    /// decrypting it on encrypted databases. A record of another
    /// namespace is `Ok(None)`: an offset is only ever looked up in
    /// the namespace whose index holds it, so a mismatch means a stale
    /// or foreign offset, never a record to return.
    fn decode_insert_payload<'a>(
        &self,
        payload: &'a [u8],
        ns_id: u32,
    ) -> Result<Option<Decoded<'a>>> {
        #[cfg(feature = "encrypt")]
        if let Some(ctx) = self.encryption.as_ref() {
            let owned = format::decode_payload_encrypted(payload, |nonce, ct| {
                let mut input = Vec::with_capacity(NONCE_LEN + ct.len());
                input.extend_from_slice(nonce);
                input.extend_from_slice(ct);
                ctx.decrypt(&input)
            })?;
            return Ok(match owned {
                OwnedRecord::Insert {
                    ns_id: record_ns,
                    key,
                    value,
                    expires_at,
                } if record_ns == ns_id => Some(Decoded::Owned {
                    key,
                    value,
                    expires_at,
                }),
                _ => None,
            });
        }

        Ok(match format::decode_payload(payload)? {
            RecordView::Insert {
                ns_id: record_ns,
                key,
                value,
                expires_at,
            } if record_ns == ns_id => Some(Decoded::Borrowed {
                key,
                value,
                expires_at,
            }),
            _ => None,
        })
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

    /// Create (or return) the runtime for a namespace id bound by a
    /// recovered `NamespaceName` record, and move the id allocator
    /// past it so a later [`Self::create_or_open_namespace`] cannot
    /// reuse the id. Callers have already rejected id 0 and
    /// `u32::MAX`, so `ns_id + 1` stays within `u32`.
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
        let next = u64::from(ns_id) + 1;
        if next > self.next_namespace_id.load(Ordering::Acquire) {
            self.next_namespace_id.store(next, Ordering::Release);
        }
        Ok(Arc::clone(entry))
    }

    /// Runtime of `ns_id` for a reader. The default namespace is
    /// borrowed from its cell under `guard`, with no lock and no
    /// reference-count traffic.
    #[inline]
    fn namespace_in<'a>(&'a self, ns_id: u32, guard: &'a Guard) -> Result<NsRef<'a>> {
        if ns_id == DEFAULT_NAMESPACE_ID {
            return Ok(NsRef::Default(self.default_ns.load(guard).get()));
        }
        self.named_namespace(ns_id)
    }

    /// Runtime of `ns_id` for a writer holding `_write`. The default
    /// namespace is borrowed for as long as the guard lives.
    #[inline]
    fn namespace_for_write<'a>(
        &'a self,
        ns_id: u32,
        _write: &'a WriteGuard<'a>,
    ) -> Result<NsRef<'a>> {
        if ns_id == DEFAULT_NAMESPACE_ID {
            // SAFETY: `_write` holds the write gate shared for `'a`.
            // The default runtime is replaced only by compaction, which
            // holds the gate exclusively while it swaps, so no `store`
            // on the cell runs while the returned reference lives.
            return Ok(NsRef::Default(unsafe { self.default_ns.get_unpinned() }));
        }
        self.named_namespace(ns_id)
    }

    /// Runtime of a named namespace (also answers id 0, from the map).
    fn named_namespace(&self, ns_id: u32) -> Result<NsRef<'static>> {
        self.namespaces
            .read()
            .get(&ns_id)
            .map(|ns| NsRef::Named(Arc::clone(ns)))
            .ok_or(Error::InvalidConfig("unknown namespace id"))
    }

    /// Owned runtime of `ns_id`, for operations that are not on a hot
    /// path (snapshots, maintenance).
    fn namespace(&self, ns_id: u32) -> Result<Arc<NamespaceRuntime>> {
        self.namespaces
            .read()
            .get(&ns_id)
            .map(Arc::clone)
            .ok_or(Error::InvalidConfig("unknown namespace id"))
    }

    /// Point `key` at `offset` in the hash index and the range index.
    /// Callers hold the key's write stripe, or the write gate
    /// exclusively, or run single-threaded during recovery.
    fn index_insert(
        &self,
        ns: &NamespaceRuntime,
        ns_id: u32,
        hash: KeyHash,
        key: &[u8],
        offset: u64,
    ) -> Result<()> {
        let _previous = ns.index.replace(hash, key, offset, |existing, key| {
            self.check_key_at(ns_id, existing, key)
        })?;
        if let Some(range_map) = ns.range_index.as_ref() {
            let _ = range_map.insert(key.to_vec(), offset);
        }
        Ok(())
    }

    /// Drop `key` from the hash index and the range index. Same
    /// locking rule as [`Self::index_insert`].
    fn index_remove(
        &self,
        ns: &NamespaceRuntime,
        ns_id: u32,
        hash: KeyHash,
        key: &[u8],
    ) -> Result<()> {
        let _previous = ns.index.remove(hash, key, |existing, key| {
            self.check_key_at(ns_id, existing, key)
        })?;
        if let Some(range_map) = ns.range_index.as_ref() {
            let _ = range_map.remove(key);
        }
        Ok(())
    }

    /// Insert or replace a key/value pair.
    ///
    /// The append and the index update run under the write gate
    /// (shared) and the key's write stripe, so concurrent writes to
    /// one key are applied to the log and to memory in the same order,
    /// and none of them lands in a file compaction is retiring. The
    /// namespace is looked up after the locks are taken, so a write
    /// cannot follow a concurrent `drop_namespace` into the log.
    pub(crate) fn insert(
        &self,
        ns_id: u32,
        key: &[u8],
        value: &[u8],
        expires_at: u64,
    ) -> Result<()> {
        let hash = self.hasher.hash(key);
        let write = self.lock_key(ns_id, hash);
        let ns = self.namespace_for_write(ns_id, &write)?;
        let offset = self.append_insert(ns_id, key, value, expires_at)?;
        self.index_insert(&ns, ns_id, hash, key, offset)
    }

    /// Bulk insert. See [`Self::write_batch`]; takes no lock itself,
    /// so the write gate is acquired once, by the batch.
    pub(crate) fn insert_many(
        &self,
        ns_id: u32,
        items: impl IntoIterator<Item = (Vec<u8>, Vec<u8>, u64)>,
    ) -> Result<()> {
        let ops: Vec<BatchOp> = items
            .into_iter()
            .map(|(key, value, expires_at)| BatchOp::Insert {
                key,
                value,
                expires_at,
            })
            .collect();
        self.write_batch(ns_id, ops)
    }

    /// Apply a batch of inserts and removes through one vectored
    /// journal append (`JournalHandle::append_batch`: one LSN
    /// reservation, one `pwrite`).
    ///
    /// The write gate (shared) and the write stripes of every key in
    /// the batch are held, stripes in ascending order, from before the
    /// append until every index update is done. A concurrent
    /// single-key write to any of these keys is therefore ordered
    /// entirely before or entirely after the batch. Readers take no
    /// locks, so they can observe some of the batch's keys updated and
    /// others not yet.
    ///
    /// The batch is not atomic on disk: there are no begin/commit
    /// markers, so a crash during the append can leave a prefix of the
    /// batch durable. A `Remove` of a key that is absent writes no
    /// record, the same as [`Self::remove`].
    pub(crate) fn write_batch(&self, ns_id: u32, ops: Vec<BatchOp>) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let hashes: Vec<KeyHash> = ops.iter().map(|op| self.hasher.hash(op.key())).collect();
        let write = self.lock_keys(ns_id, &hashes);
        let ns = self.namespace_for_write(ns_id, &write)?;

        // Keys inserted earlier in this batch count as present for a
        // later `Remove` of the same key.
        let mut inserted: std::collections::HashSet<&[u8]> = std::collections::HashSet::new();
        let has_removes = ops.iter().any(|op| matches!(op, BatchOp::Remove { .. }));
        let mut payloads: Vec<Vec<u8>> = Vec::with_capacity(ops.len());
        let mut written: Vec<usize> = Vec::with_capacity(ops.len());
        for (i, op) in ops.iter().enumerate() {
            match op {
                BatchOp::Insert {
                    key,
                    value,
                    expires_at,
                } => {
                    payloads.push(self.encode_insert_payload(ns_id, key, value, *expires_at)?);
                    if has_removes {
                        let _ = inserted.insert(key.as_slice());
                    }
                }
                BatchOp::Remove { key } => {
                    let present =
                        inserted.remove(key.as_slice()) || ns.index.get(hashes[i], key)?.is_some();
                    if !present {
                        continue;
                    }
                    payloads.push(self.encode_remove_payload(ns_id, key)?);
                }
            }
            written.push(i);
        }
        if payloads.is_empty() {
            return Ok(());
        }
        let offsets = self
            .store
            .append_batch(payloads.iter().map(Vec::as_slice))?;

        for (&i, &offset) in written.iter().zip(offsets.iter()) {
            match &ops[i] {
                BatchOp::Insert { key, .. } => {
                    self.index_insert(&ns, ns_id, hashes[i], key, offset)?;
                }
                BatchOp::Remove { key } => self.index_remove(&ns, ns_id, hashes[i], key)?,
            }
        }
        Ok(())
    }

    fn append_insert(&self, ns_id: u32, key: &[u8], value: &[u8], expires_at: u64) -> Result<u64> {
        #[cfg(feature = "encrypt")]
        if let Some(ctx) = self.encryption.as_ref() {
            let payload = ctx.seal_record(format::TAG_INSERT, |body| {
                format::encode_insert_body(body, ns_id, key, value, expires_at);
            })?;
            return self.store.append(&payload);
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
            let payload = ctx.seal_record(format::TAG_REMOVE, |body| {
                format::encode_remove_body(body, ns_id, key);
            })?;
            return self.store.append(&payload);
        }

        self.store.append_with(|buf| {
            buf.push(format::TAG_REMOVE);
            format::encode_remove_body(buf, ns_id, key);
            Ok(())
        })
    }

    /// Run `read` until it completes without a compaction swap
    /// happening in the middle of it. Every read that resolves an
    /// index offset against a mapping without holding the write gate
    /// goes through here: an offset taken from the old index and
    /// decoded against the new file (or the reverse) is detected and
    /// the read is retried.
    #[inline]
    fn consistent<T>(&self, mut read: impl FnMut() -> Result<T>) -> Result<T> {
        loop {
            let seq = self.store.read_begin();
            let out = read();
            if self.store.read_validate(seq) {
                return out;
            }
        }
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
        let hash = self.hasher.hash(key);
        self.consistent(|| {
            let guard = epoch::pin();
            let ns = self.namespace_in(ns_id, &guard)?;
            let Some(offset) = ns.index.get(hash, key)? else {
                return Ok(None);
            };
            self.read_zerocopy_at(ns_id, offset, key, &guard)
        })
    }

    /// Decode the record at `offset`, returning a [`crate::ValueRef`]
    /// pointing at the value bytes (or carrying owned plaintext for
    /// encrypted databases) plus the record's `expires_at`.
    /// Returns `Ok(None)` if the offset's record is no longer a
    /// live `Insert` whose key matches `expected_key`.
    fn read_zerocopy_at(
        &self,
        ns_id: u32,
        offset: u64,
        expected_key: &[u8],
        guard: &Guard,
    ) -> Result<Option<(crate::ValueRef, u64)>> {
        let Some((payload, view)) = self.store.payload(offset, guard)? else {
            return Ok(None);
        };
        let Some(record) = self.decode_insert_payload(payload, ns_id)? else {
            return Ok(None);
        };
        if record.key() != expected_key {
            return Ok(None);
        }
        Ok(Some(match record {
            Decoded::Borrowed {
                value, expires_at, ..
            } => {
                // Recover the value's byte range inside the mapping
                // from the borrowed slice.
                let start = value.as_ptr() as usize - view.bytes().as_ptr() as usize;
                let range = start..start + value.len();
                (crate::ValueRef::from_mmap(view.to_arc(), range), expires_at)
            }
            Decoded::Owned {
                value, expires_at, ..
            } => (crate::ValueRef::from_owned(value), expires_at),
        }))
    }

    /// Fetch value + expires_at for a key in one pass. Used by the TTL
    /// path in `Emdb::get` so it doesn't have to make two record reads.
    pub(crate) fn get_with_meta(&self, ns_id: u32, key: &[u8]) -> Result<Option<(Vec<u8>, u64)>> {
        let hash = self.hasher.hash(key);
        self.consistent(|| {
            let guard = epoch::pin();
            let ns = self.namespace_in(ns_id, &guard)?;
            let Some(offset) = ns.index.get(hash, key)? else {
                return Ok(None);
            };
            self.read_value_at(ns_id, offset, key, &guard)
        })
    }

    /// Whether `key` has a record that is live at `now_ms` (see
    /// [`is_live`]). Decodes only the key and expiry; the value is
    /// never copied.
    pub(crate) fn contains_live(&self, ns_id: u32, key: &[u8], now_ms: u64) -> Result<bool> {
        let hash = self.hasher.hash(key);
        self.consistent(|| {
            let guard = epoch::pin();
            let ns = self.namespace_in(ns_id, &guard)?;
            let Some(offset) = ns.index.get(hash, key)? else {
                return Ok(false);
            };
            Ok(match self.decode_insert_at(ns_id, offset, &guard)? {
                Some(record) => record.key() == key && is_live(record.expires_at(), now_ms),
                None => false,
            })
        })
    }

    fn read_value_at(
        &self,
        ns_id: u32,
        offset: u64,
        expected_key: &[u8],
        guard: &Guard,
    ) -> Result<Option<(Vec<u8>, u64)>> {
        Ok(match self.decode_insert_at(ns_id, offset, guard)? {
            Some(record) if record.key() == expected_key => {
                let expires_at = record.expires_at();
                Some((record.into_value(), expires_at))
            }
            _ => None,
        })
    }

    /// Remove a key. Returns the previously-associated value, if any.
    ///
    /// Runs entirely under the write gate (shared) and the key's write
    /// stripe: of two concurrent `remove`s of one key, exactly one
    /// returns `Some`.
    pub(crate) fn remove(&self, ns_id: u32, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let hash = self.hasher.hash(key);
        let write = self.lock_key(ns_id, hash);
        let ns = self.namespace_for_write(ns_id, &write)?;
        let Some(offset) = ns.index.get(hash, key)? else {
            return Ok(None);
        };
        let previous = {
            let guard = epoch::pin();
            self.read_value_at(ns_id, offset, key, &guard)?
        };
        let Some((value, _expires_at)) = previous else {
            return Ok(None);
        };
        let _remove_offset = self.append_remove(ns_id, key)?;
        let _removed = ns.index.remove_if_offset(hash, key, offset);
        if let Some(range_map) = ns.range_index.as_ref() {
            let _ = range_map.remove(key);
        }
        Ok(Some(value))
    }

    /// Remove `key` only if its live record is still the one at
    /// `expected_offset`. Returns whether a remove record was written.
    /// Used by TTL sweeps so a fresh concurrent re-insert is never
    /// deleted. An offset from before a compaction no longer matches,
    /// so such a key is left for the next sweep.
    #[cfg(feature = "ttl")]
    pub(crate) fn remove_if_unchanged(
        &self,
        ns_id: u32,
        key: &[u8],
        expected_offset: u64,
    ) -> Result<bool> {
        let hash = self.hasher.hash(key);
        let write = self.lock_key(ns_id, hash);
        let ns = self.namespace_for_write(ns_id, &write)?;
        if ns.index.get(hash, key)? != Some(expected_offset) {
            return Ok(false);
        }
        let _remove_offset = self.append_remove(ns_id, key)?;
        let _removed = ns.index.remove_if_offset(hash, key, expected_offset);
        if let Some(range_map) = ns.range_index.as_ref() {
            let _ = range_map.remove(key);
        }
        Ok(true)
    }

    /// Rewrite `key` without a TTL if it currently has one and is
    /// still live at `now_ms`. Returns whether the record was
    /// rewritten. An expired record is left alone (it is never
    /// resurrected), and a record without a TTL needs no rewrite.
    #[cfg(feature = "ttl")]
    pub(crate) fn clear_expiry(&self, ns_id: u32, key: &[u8], now_ms: u64) -> Result<bool> {
        let hash = self.hasher.hash(key);
        let write = self.lock_key(ns_id, hash);
        let ns = self.namespace_for_write(ns_id, &write)?;
        let Some(offset) = ns.index.get(hash, key)? else {
            return Ok(false);
        };
        let current = {
            let guard = epoch::pin();
            self.read_value_at(ns_id, offset, key, &guard)?
        };
        let Some((value, expires_at)) = current else {
            return Ok(false);
        };
        if expires_at == 0 || !is_live(expires_at, now_ms) {
            return Ok(false);
        }
        let new_offset = self.append_insert(ns_id, key, &value, 0)?;
        self.index_insert(&ns, ns_id, hash, key, new_offset)?;
        Ok(true)
    }

    /// `(key, offset)` of every record in `ns_id` whose TTL has passed
    /// at `now_ms`. Decodes keys and expiry only, one record at a time
    /// from a pinned snapshot; values are never copied.
    #[cfg(feature = "ttl")]
    pub(crate) fn expired_entries(&self, ns_id: u32, now_ms: u64) -> Result<Vec<(Vec<u8>, u64)>> {
        let (offsets, view) = self.snapshot_offsets(ns_id)?;
        let mut expired = Vec::new();
        for offset in offsets {
            if let Some(record) = self.decode_insert_in(&view, ns_id, offset)? {
                if !is_live(record.expires_at(), now_ms) {
                    expired.push((record.key().to_vec(), offset));
                }
            }
        }
        Ok(expired)
    }

    /// Number of live records in `ns_id`, derived from the hash index.
    /// Records whose TTL has passed are counted until they are swept.
    pub(crate) fn record_count(&self, ns_id: u32) -> Result<u64> {
        let guard = epoch::pin();
        let ns = self.namespace_in(ns_id, &guard)?;
        Ok(ns.index.len() as u64)
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
    /// one filesystem `metadata` call. Record counts are atomic loads
    /// of the index's per-shard counters.
    pub(crate) fn stats(&self) -> Result<crate::EmdbStats> {
        let mut live_records: u64 = 0;
        let mut named_namespace_count: usize = 0;
        {
            let guard = self.namespaces.read();
            for (ns_id, ns) in guard.iter() {
                live_records = live_records.saturating_add(ns.index.len() as u64);
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
        let Some(default_ns) = built.get(&DEFAULT_NAMESPACE_ID).map(Arc::clone) else {
            return Err(rewrite.abandon(Error::InvalidConfig(
                "compaction rebuilt no default namespace",
            )));
        };

        // Rename first and swap handles only once it succeeded: a
        // failed rename leaves the store on the original file. The
        // rewrite takes over the permissions of the file it replaces.
        if let Err(err) = crate::private_fs::keep_permissions(&path, &tmp)
            .and_then(|()| std::fs::rename(&tmp, &path))
        {
            drop((read_file, mmap));
            return Err(rewrite.abandon(Error::Io(err)));
        }
        let dir_synced = sync_dir(&path);

        self.store.begin_swap();
        self.store
            .install_file(rewrite.into_journal(), read_file, mmap);
        self.default_ns.store(default_ns);
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
            let target = build_index.then_some((ns.ns_id, &runtime));
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
        runtime: Option<(u32, &NamespaceRuntime)>,
        written: Option<&mut SourceReader>,
    ) -> Result<()> {
        let payloads: Vec<&[u8]> = ranges.iter().map(|r| &chunk[r.clone()]).collect();
        let starts = rewrite.append_batch(&payloads)?;
        let (Some((ns_id, runtime)), Some(written)) = (runtime, written) else {
            return Ok(());
        };
        for (payload, start) in payloads.iter().zip(starts) {
            let key = self.key_from_payload(payload)?.ok_or(Error::Corrupted {
                offset: start,
                reason: "live index entry does not hold an insert record",
            })?;
            let key_hash = self.hasher.hash(&key);
            // Collisions are resolved against the temporary file,
            // where the new offsets point.
            let _previous = runtime
                .index
                .replace(key_hash, &key, start, |existing, key| {
                    let payload = written.payload(existing)?;
                    Ok(match self.decode_insert_payload(payload, ns_id)? {
                        Some(record) if record.key() == key => KeyCheck::Same,
                        Some(record) => KeyCheck::Other(record.key().to_vec()),
                        None => KeyCheck::Unreadable,
                    })
                })?;
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
            return ctx.seal_record(format::TAG_NAMESPACE_NAME, |body| {
                format::encode_namespace_name_body(body, ns_id, name);
            });
        }

        let mut payload = Vec::with_capacity(1 + 8 + name.len());
        payload.push(format::TAG_NAMESPACE_NAME);
        format::encode_namespace_name_body(&mut payload, ns_id, name);
        Ok(payload)
    }

    /// Encode a `[tag][body]` payload for an insert record, sealed on
    /// encrypted databases.
    fn encode_insert_payload(
        &self,
        ns_id: u32,
        key: &[u8],
        value: &[u8],
        expires_at: u64,
    ) -> Result<Vec<u8>> {
        #[cfg(feature = "encrypt")]
        if let Some(ctx) = self.encryption.as_ref() {
            return ctx.seal_record(format::TAG_INSERT, |body| {
                format::encode_insert_body(body, ns_id, key, value, expires_at);
            });
        }

        let mut payload = Vec::with_capacity(1 + 20 + key.len() + value.len());
        payload.push(format::TAG_INSERT);
        format::encode_insert_body(&mut payload, ns_id, key, value, expires_at);
        Ok(payload)
    }

    /// Encode a `[tag][body]` payload for a remove (tombstone) record.
    fn encode_remove_payload(&self, ns_id: u32, key: &[u8]) -> Result<Vec<u8>> {
        #[cfg(feature = "encrypt")]
        if let Some(ctx) = self.encryption.as_ref() {
            return ctx.seal_record(format::TAG_REMOVE, |body| {
                format::encode_remove_body(body, ns_id, key);
            });
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
            let key_hash = self.hasher.hash(key);
            self.index_remove(ns, ns_id, key_hash, key)?;
        }
        Ok(())
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
    /// pairs sorted lexicographically by key, skipping records that
    /// are not live at `now_ms` (see [`is_live`]). Requires the engine
    /// to have been opened with `enable_range_scans(true)`. Same
    /// consistency as the range iterators (see [`RangeCursor`]).
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfig`] if range scans were not enabled
    /// at open time.
    pub(crate) fn range_scan<R>(
        &self,
        ns_id: u32,
        range: R,
        now_ms: u64,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>>
    where
        R: RangeBounds<Vec<u8>>,
    {
        let mut cursor = self.range_cursor(ns_id, range)?;
        let mut page = VecDeque::new();
        let mut out = Vec::new();
        loop {
            self.fill_range(&mut cursor, &mut page, 256)?;
            if page.is_empty() {
                return Ok(out);
            }
            for (key, offset) in page.drain(..) {
                if let Some((value, expires_at)) =
                    self.read_value_in(cursor.view(), ns_id, offset, &key)?
                {
                    if is_live(expires_at, now_ms) {
                        out.push((key, value));
                    }
                }
            }
        }
    }

    /// Snapshot the live record offsets in `ns_id`, sorted ascending,
    /// together with a mapping that covers all of them, taken in one
    /// consistent step. Used by the snapshot iterators (`iter`,
    /// `keys`) and the TTL sweep, which decode records on demand
    /// against the returned [`ReadView`].
    pub(crate) fn snapshot_offsets(&self, ns_id: u32) -> Result<(Vec<u64>, ReadView)> {
        self.consistent(|| {
            let mut offsets = {
                let guard = epoch::pin();
                self.namespace_in(ns_id, &guard)?.index.collect_offsets()?
            };
            offsets.sort_unstable();
            // Taken after the offsets: every record they point at was
            // appended before its index entry existed, so the tail
            // mapping covers it.
            let view = self.store.pinned_mapping()?;
            Ok((offsets, view))
        })
    }

    /// Open a lazy cursor over `range` in `ns_id`'s secondary index.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfig`] if range scans were not enabled
    /// at open time.
    pub(crate) fn range_cursor<R>(&self, ns_id: u32, range: R) -> Result<RangeCursor>
    where
        R: RangeBounds<Vec<u8>>,
    {
        let (map, generation) = self.consistent(|| {
            let generation = self.store.read_begin();
            let guard = epoch::pin();
            let ns = self.namespace_in(ns_id, &guard)?;
            let map = ns.range_index.as_ref().ok_or(Error::InvalidConfig(
                "range scans not enabled; pass `EmdbBuilder::enable_range_scans(true)` at open time",
            ))?;
            Ok((Arc::clone(map), generation))
        })?;
        Ok(RangeCursor {
            ns_id,
            map,
            generation,
            view: self.store.pinned_mapping()?,
            next: owned_bound(range.start_bound()),
            end: owned_bound(range.end_bound()),
            done: false,
        })
    }

    /// Append up to `limit` `(key, offset)` pairs that follow `cursor`
    /// to `out`, advance the cursor past them, and pin a mapping in
    /// [`RangeCursor::view`] that covers them, all within one file
    /// generation.
    ///
    /// When a compaction replaced the file since the cursor's last
    /// fill, the cursor first re-attaches to the namespace's current
    /// skiplist: the old one holds offsets into the retired file and
    /// no longer receives writes. A cursor whose namespace was dropped
    /// ends.
    pub(crate) fn fill_range(
        &self,
        cursor: &mut RangeCursor,
        out: &mut VecDeque<(Vec<u8>, u64)>,
        limit: usize,
    ) -> Result<()> {
        if cursor.done {
            return Ok(());
        }
        let before = out.len();
        loop {
            let seq = self.store.read_begin();
            if seq != cursor.generation {
                let current = {
                    let guard = epoch::pin();
                    match self.namespace_in(cursor.ns_id, &guard) {
                        Ok(ns) => ns.range_index.as_ref().map(Arc::clone),
                        Err(_) => None,
                    }
                };
                let Some(map) = current else {
                    cursor.done = true;
                    return Ok(());
                };
                cursor.map = map;
                cursor.generation = seq;
            }
            out.extend(
                cursor
                    .map
                    .range::<[u8], _>((bound_as_slice(&cursor.next), bound_as_slice(&cursor.end)))
                    .take(limit)
                    .map(|entry| (entry.key().clone(), *entry.value())),
            );
            // Taken after the page: every offset in it was appended
            // before its skiplist entry existed.
            let view = self.store.pinned_mapping()?;
            if self.store.read_validate(seq) {
                cursor.view = view;
                break;
            }
            // A compaction started while the page was read: its offsets
            // may belong to either file. Read the page again.
            out.truncate(before);
        }
        let added = out.len() - before;
        if added < limit {
            cursor.done = true;
        }
        if let Some((last, _)) = out.back().filter(|_| added > 0) {
            cursor.next = Bound::Excluded(last.clone());
        }
        Ok(())
    }

    /// Decode the record at `offset` in `view` into an owned `(key,
    /// value, expires_at)` tuple. Used by the snapshot iterators.
    /// Returns `Ok(None)` when the offset does not hold an `Insert` of
    /// `ns_id`.
    pub(crate) fn decode_owned_in(
        &self,
        view: &ReadView,
        ns_id: u32,
        offset: u64,
    ) -> Result<Option<RecordSnapshot>> {
        Ok(self
            .decode_insert_in(view, ns_id, offset)?
            .map(Decoded::into_triple))
    }

    /// Decode only the key and expiry of the record at `offset` in
    /// `view`. Used by key iterators so values are never copied.
    pub(crate) fn decode_key_in(
        &self,
        view: &ReadView,
        ns_id: u32,
        offset: u64,
    ) -> Result<Option<(Vec<u8>, u64)>> {
        Ok(self
            .decode_insert_in(view, ns_id, offset)?
            .map(|record| (record.key().to_vec(), record.expires_at())))
    }

    /// Read the value and expiry at `offset` in `view`, validating that
    /// the record is an `Insert` of `expected_key` in `ns_id`. Used by
    /// range iterators, which know the key from the skiplist.
    pub(crate) fn read_value_in(
        &self,
        view: &ReadView,
        ns_id: u32,
        offset: u64,
        expected_key: &[u8],
    ) -> Result<Option<(Vec<u8>, u64)>> {
        Ok(match self.decode_insert_in(view, ns_id, offset)? {
            Some(record) if record.key() == expected_key => {
                let expires_at = record.expires_at();
                Some((record.into_value(), expires_at))
            }
            _ => None,
        })
    }

    /// Stream every live record in `ns_id` to `sink` in batches of
    /// about [`REWRITE_CHUNK_BYTES`]. Records are read from a mapping
    /// pinned at the start, so the batches form one consistent
    /// snapshot. Unlike the iterators, a live index entry that does
    /// not decode is an error, not a skipped record.
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
                .decode_owned_in(&view, ns_id, offset)?
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
    #[cfg(feature = "ttl")]
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
        let _gate = self.write_gate.read();
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
        // Every allocation (and the recovery-time bump in
        // `ensure_namespace_runtime`) happens under the
        // `namespace_names` write lock or before the engine is shared,
        // so a plain load + store cannot race. Id 0 is the default
        // namespace and u32::MAX is reserved, so a counter that would
        // hand out either is exhausted rather than wrapped onto the
        // default namespace.
        let next = self.next_namespace_id.load(Ordering::Acquire);
        let id = match u32::try_from(next) {
            Ok(id) if id != DEFAULT_NAMESPACE_ID && id != u32::MAX => id,
            _ => {
                return Err(Error::InvalidConfig(
                    "namespace id space exhausted; no more namespaces can be created in this database",
                ));
            }
        };
        self.next_namespace_id.store(next + 1, Ordering::Release);
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
            let payload = ctx.seal_record(format::TAG_NAMESPACE_NAME, |body| {
                format::encode_namespace_name_body(body, ns_id, name.as_bytes());
            })?;
            return self.store.append(&payload);
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

/// Read-only meta-sidecar peek without opening a full Store.
/// Used by the engine to extract the encryption salt before
/// opening the file with the right key.
#[cfg(feature = "encrypt")]
fn peek_header(path: &Path) -> Result<Option<MetaHeader>> {
    meta::read(path)
}

/// Rebase a decoder's `Corrupted` offset (relative to the record
/// payload) onto the record's absolute file offset so the error points
/// at the damaged record. Other errors pass through unchanged.
fn relocate_corruption(err: Error, payload_start: u64) -> Error {
    match err {
        Error::Corrupted { offset, reason } => Error::Corrupted {
            offset: payload_start.saturating_add(offset),
            reason,
        },
        other => other,
    }
}

/// True when the journal file at `path` exists and is not empty. fsys
/// journals carry no file header and do not pre-allocate, so any byte
/// means at least one record (or a torn first record) was written.
#[cfg(feature = "encrypt")]
fn journal_has_bytes(path: &std::path::Path) -> Result<bool> {
    match std::fs::metadata(path) {
        Ok(m) => Ok(m.len() != 0),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(Error::from(err)),
    }
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
        // Owner-only, and `create_new`: a file or link that appears
        // after the removal is refused rather than written through.
        drop(crate::private_fs::create_new_private_file(path)?);
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
