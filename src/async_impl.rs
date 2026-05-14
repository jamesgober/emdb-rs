// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Async surface for emdb. Gated behind the `async` feature.
//!
//! Wraps the sync [`Emdb`] / [`Namespace`] API in
//! [`tokio::task::spawn_blocking`] so every operation that performs
//! emdb's blocking I/O (journal append, mmap read, fsync, file
//! rename, etc.) runs on tokio's blocking-task pool rather than
//! stalling the async-task scheduler.
//!
//! ## Design
//!
//! - [`AsyncEmdb`] is a `Clone`-able handle that holds an
//!   `Arc<Emdb>` internally. Every async method clones the Arc and
//!   moves the clone into a `spawn_blocking` closure.
//! - [`AsyncNamespace`] mirrors [`Namespace`] the same way.
//! - [`EmdbBuilder::build_async`] (defined in `builder.rs` under
//!   `#[cfg(feature = "async")]`) is the async-context entry point
//!   that mirrors `EmdbBuilder::build`.
//! - The sync handle behind any [`AsyncEmdb`] is reachable via
//!   [`AsyncEmdb::sync_handle`], for callers that need APIs the
//!   async surface doesn't yet wrap (e.g. zero-copy reads, which
//!   return references with a lifetime tied to the call frame and
//!   can't survive a `spawn_blocking` round-trip).
//!
//! ## Cost model
//!
//! Each async op pays one `spawn_blocking` dispatch (tokio
//! schedules the closure onto its blocking pool) plus one ownership
//! transfer (key + value cloned to owned `Vec<u8>` so the closure
//! can take them by value). On a warm blocking pool the dispatch is
//! sub-microsecond; the value clone is `O(key + value bytes)`.
//!
//! When the sync cost dominates (record decode, fsync, mmap remap),
//! the spawn_blocking overhead is negligible. When the sync cost is
//! a single hash-table probe (`get` on an in-memory hot key), the
//! spawn_blocking overhead may dominate — for those workloads,
//! prefer the sync surface or batch via `insert_many` / `range`.
//!
//! ## Streaming iterators
//!
//! Two flavours of async iteration are exposed:
//!
//! - **Eager** (`iter`, `keys`, `range`, `range_prefix`, `iter_from`,
//!   `iter_after`) — runs the sync iterator to completion inside a
//!   single `spawn_blocking` and returns an owned `Vec`. Convenient
//!   for small result sets; the entire result is resident before the
//!   first await completes.
//! - **Streaming** (`iter_stream`, `keys_stream`, `range_stream`,
//!   `range_prefix_stream`, `iter_from_stream`, `iter_after_stream`)
//!   — drives the sync iterator on a dedicated blocking task that
//!   pushes items through a bounded `tokio::sync::mpsc` channel
//!   (capacity 64). The async caller polls the returned
//!   [`tokio_stream::wrappers::ReceiverStream`], applying natural
//!   backpressure to the blocking pump task and bounding memory at
//!   the channel depth × per-record size rather than the full
//!   namespace footprint.
//!
//! Pick streaming whenever the result set is large enough that
//! materialising it before the first record is unacceptable, or
//! when the caller wants to process records as they arrive (forward
//! them to a network socket, fold them into a running aggregate,
//! etc.). Pick eager for small fixed-size queries where the
//! channel/spawn cost outweighs the win.

use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[cfg(feature = "ttl")]
use std::time::Duration;

use tokio::task::spawn_blocking;

#[cfg(feature = "ttl")]
use crate::Ttl;
use crate::{Emdb, EmdbStats, Error, Result};

/// Convert a tokio `JoinError` into an emdb `Error`. A join failure
/// means the blocking task panicked (or was cancelled, which we
/// never do from this module) — surface it as `Error::Io` with a
/// descriptive message so callers' error-handling paths see a
/// uniform shape.
fn join_err(err: tokio::task::JoinError) -> Error {
    Error::Io(std::io::Error::other(format!("async join: {err}")))
}

/// Run `f` on tokio's blocking pool and forward both join errors
/// and emdb errors back through one `Result`.
async fn blocking<F, R>(f: F) -> Result<R>
where
    F: FnOnce() -> Result<R> + Send + 'static,
    R: Send + 'static,
{
    spawn_blocking(f).await.map_err(join_err)?
}

/// Run `f` on tokio's blocking pool. `f` itself is infallible; only
/// the join can fail (panic / cancel). Gated behind the same
/// feature flag as its only caller (`sweep_expired`) so it doesn't
/// show up as dead code under feature combinations that exclude
/// `ttl`.
#[cfg(feature = "ttl")]
async fn blocking_infallible<F, R>(f: F) -> Result<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    spawn_blocking(f).await.map_err(join_err)
}

/// Backpressure depth for the async streaming-iterator channels.
///
/// 64 owned records in flight is the right point on the
/// memory/throughput curve for typical record sizes: large enough
/// that the async consumer rarely starves the blocking pump task
/// on a per-record basis, small enough that the absolute footprint
/// stays bounded (≲ a few MiB for kilobyte records). Tuning this
/// up doesn't help once the consumer is the bottleneck; tuning it
/// down trades throughput for tighter memory.
const STREAM_CHANNEL_CAPACITY: usize = 64;

/// Drive a sync `Iterator` on tokio's blocking pool and surface its
/// items as a [`tokio_stream::wrappers::ReceiverStream`]. The pump
/// task halts the moment the consumer drops the stream
/// (`blocking_send` returns `Err`); the iterator is dropped on the
/// blocking thread, releasing its `Arc` references.
fn spawn_iter_stream<I, T>(iter: I) -> tokio_stream::wrappers::ReceiverStream<T>
where
    I: Iterator<Item = T> + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = tokio::sync::mpsc::channel::<T>(STREAM_CHANNEL_CAPACITY);
    // JoinHandle is intentionally dropped: the pump task is fire-and-forget
    // and self-terminating (channel-closed-on-receiver-drop or iterator
    // exhausted). No caller needs to .await the handle.
    let _pump: tokio::task::JoinHandle<()> = spawn_blocking(move || {
        for item in iter {
            if tx.blocking_send(item).is_err() {
                break;
            }
        }
    });
    tokio_stream::wrappers::ReceiverStream::new(rx)
}

/// Cheap-clone async handle to an [`Emdb`]. Every method routes
/// through `tokio::task::spawn_blocking` so emdb's blocking I/O
/// never stalls the async-task scheduler.
///
/// **Cost model:** each call dispatches one `spawn_blocking` task
/// (sub-microsecond on a warm pool) and clones key + value bytes
/// to owned `Vec<u8>` so the closure can take them by value. For
/// workloads where this overhead matters more than the underlying
/// I/O cost, reach for the sync handle via
/// [`AsyncEmdb::sync_handle`].
///
/// **Iterators:** two flavours. The eager methods (`iter`, `keys`,
/// `range`, …) collect into an owned `Vec` before resolving. The
/// streaming methods (`iter_stream`, `keys_stream`, `range_stream`,
/// …) return a [`tokio_stream::wrappers::ReceiverStream`] backed by
/// a bounded mpsc channel — items arrive incrementally and memory
/// is bounded by the channel depth, not the namespace size.
#[derive(Clone, Debug)]
pub struct AsyncEmdb {
    inner: Arc<Emdb>,
}

impl AsyncEmdb {
    /// Open or create an emdb database at `path`.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path: PathBuf = path.as_ref().to_path_buf();
        let emdb = blocking(move || Emdb::open(&path)).await?;
        Ok(Self {
            inner: Arc::new(emdb),
        })
    }

    /// Open an in-memory emdb database. Synchronous because the
    /// in-memory open path doesn't touch disk; nothing to spawn.
    #[must_use]
    pub fn open_in_memory() -> Self {
        Self {
            inner: Arc::new(Emdb::open_in_memory()),
        }
    }

    /// Returns a fresh [`crate::EmdbBuilder`]. Call
    /// [`crate::EmdbBuilder::build_async`] to construct an
    /// `AsyncEmdb`.
    #[must_use]
    pub fn builder() -> crate::EmdbBuilder {
        crate::EmdbBuilder::new()
    }

    /// Wrap an already-constructed sync [`Emdb`].
    #[must_use]
    pub fn from_sync(emdb: Emdb) -> Self {
        Self {
            inner: Arc::new(emdb),
        }
    }

    /// Access the underlying sync handle. Useful for APIs the async
    /// surface doesn't expose (zero-copy reads, streaming
    /// iterators) — wrap them in your own `spawn_blocking` calls if
    /// you need to call them from an async context.
    #[must_use]
    pub fn sync_handle(&self) -> Arc<Emdb> {
        Arc::clone(&self.inner)
    }

    /// On-disk path of the database (or a sentinel for in-memory).
    #[must_use]
    pub fn path(&self) -> &Path {
        self.inner.path()
    }

    /// Insert or replace a key/value pair.
    pub async fn insert<K, V>(&self, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let inner = Arc::clone(&self.inner);
        let key = key.as_ref().to_vec();
        let value = value.as_ref().to_vec();
        blocking(move || inner.insert(key, value)).await
    }

    /// Insert many key/value pairs in one vectored journal-append
    /// pass. See [`Emdb::insert_many`] for the durability shape.
    pub async fn insert_many<I, K, V>(&self, items: I) -> Result<()>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let inner = Arc::clone(&self.inner);
        let owned: Vec<(Vec<u8>, Vec<u8>)> = items
            .into_iter()
            .map(|(k, v)| (k.as_ref().to_vec(), v.as_ref().to_vec()))
            .collect();
        blocking(move || inner.insert_many(owned)).await
    }

    /// Fetch a value by key.
    pub async fn get<K>(&self, key: K) -> Result<Option<Vec<u8>>>
    where
        K: AsRef<[u8]>,
    {
        let inner = Arc::clone(&self.inner);
        let key = key.as_ref().to_vec();
        blocking(move || inner.get(key)).await
    }

    /// Remove a key, returning the previous value if any.
    pub async fn remove<K>(&self, key: K) -> Result<Option<Vec<u8>>>
    where
        K: AsRef<[u8]>,
    {
        let inner = Arc::clone(&self.inner);
        let key = key.as_ref().to_vec();
        blocking(move || inner.remove(key)).await
    }

    /// Returns whether the key has a live record.
    pub async fn contains_key<K>(&self, key: K) -> Result<bool>
    where
        K: AsRef<[u8]>,
    {
        let inner = Arc::clone(&self.inner);
        let key = key.as_ref().to_vec();
        blocking(move || inner.contains_key(key)).await
    }

    /// Live record count in the default namespace.
    pub async fn len(&self) -> Result<usize> {
        let inner = Arc::clone(&self.inner);
        blocking(move || inner.len()).await
    }

    /// True iff the default namespace has zero live records.
    pub async fn is_empty(&self) -> Result<bool> {
        let inner = Arc::clone(&self.inner);
        blocking(move || inner.is_empty()).await
    }

    /// Drop every record in the default namespace.
    pub async fn clear(&self) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        blocking(move || inner.clear()).await
    }

    /// Force pending writes durable.
    pub async fn flush(&self) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        blocking(move || inner.flush()).await
    }

    /// Persist a fast-reopen checkpoint.
    pub async fn checkpoint(&self) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        blocking(move || inner.checkpoint()).await
    }

    /// Point-in-time database introspection.
    pub async fn stats(&self) -> Result<EmdbStats> {
        let inner = Arc::clone(&self.inner);
        blocking(move || inner.stats()).await
    }

    /// Atomic snapshot of the database to a sibling path.
    pub async fn backup_to(&self, target: impl AsRef<Path>) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        let target: PathBuf = target.as_ref().to_path_buf();
        blocking(move || inner.backup_to(&target)).await
    }

    /// Rewrite the journal in compacted form, dropping tombstoned
    /// and overwritten records.
    pub async fn compact(&self) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        blocking(move || inner.compact()).await
    }

    /// Eagerly collect every `(key, value)` pair in the default
    /// namespace. See the module-level docs for the
    /// streaming-iterator caveat.
    pub async fn iter(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let inner = Arc::clone(&self.inner);
        blocking(move || Ok(inner.iter()?.collect())).await
    }

    /// Eagerly collect every key in the default namespace.
    pub async fn keys(&self) -> Result<Vec<Vec<u8>>> {
        let inner = Arc::clone(&self.inner);
        blocking(move || Ok(inner.keys()?.collect())).await
    }

    /// Eagerly collect every `(key, value)` pair in the half-open
    /// `range`, sorted lexicographically. Requires range scans
    /// enabled at builder time.
    pub async fn range<R>(&self, range: R) -> Result<Vec<(Vec<u8>, Vec<u8>)>>
    where
        R: RangeBounds<Vec<u8>> + Send + 'static,
    {
        let inner = Arc::clone(&self.inner);
        blocking(move || inner.range(range)).await
    }

    /// Eagerly collect every `(key, value)` pair starting with
    /// `prefix`. Requires range scans enabled at builder time.
    pub async fn range_prefix<K>(&self, prefix: K) -> Result<Vec<(Vec<u8>, Vec<u8>)>>
    where
        K: AsRef<[u8]>,
    {
        let inner = Arc::clone(&self.inner);
        let prefix = prefix.as_ref().to_vec();
        blocking(move || inner.range_prefix(prefix)).await
    }

    /// Eagerly collect keys at or after `start`. Requires range
    /// scans enabled at builder time.
    pub async fn iter_from<K>(&self, start: K) -> Result<Vec<(Vec<u8>, Vec<u8>)>>
    where
        K: AsRef<[u8]>,
    {
        let inner = Arc::clone(&self.inner);
        let start = start.as_ref().to_vec();
        blocking(move || Ok(inner.iter_from(start)?.collect())).await
    }

    /// Eagerly collect keys strictly after `start`. Requires range
    /// scans enabled at builder time.
    pub async fn iter_after<K>(&self, start: K) -> Result<Vec<(Vec<u8>, Vec<u8>)>>
    where
        K: AsRef<[u8]>,
    {
        let inner = Arc::clone(&self.inner);
        let start = start.as_ref().to_vec();
        blocking(move || Ok(inner.iter_after(start)?.collect())).await
    }

    /// Stream every `(key, value)` pair in the default namespace.
    ///
    /// The snapshot is taken synchronously inside one
    /// `spawn_blocking`, then a second `spawn_blocking` task pumps
    /// records into a bounded mpsc channel (capacity 64) which is
    /// wrapped as a [`tokio_stream::wrappers::ReceiverStream`].
    /// Memory in flight is bounded by the channel depth, not the
    /// namespace size. Dropping the stream halts the pump task on
    /// the next send.
    pub async fn iter_stream(
        &self,
    ) -> Result<tokio_stream::wrappers::ReceiverStream<(Vec<u8>, Vec<u8>)>> {
        let inner = Arc::clone(&self.inner);
        let iter = blocking(move || inner.iter()).await?;
        Ok(spawn_iter_stream(iter))
    }

    /// Stream every key in the default namespace. Same semantics as
    /// [`AsyncEmdb::iter_stream`].
    pub async fn keys_stream(&self) -> Result<tokio_stream::wrappers::ReceiverStream<Vec<u8>>> {
        let inner = Arc::clone(&self.inner);
        let iter = blocking(move || inner.keys()).await?;
        Ok(spawn_iter_stream(iter))
    }

    /// Stream every `(key, value)` pair in the half-open `range`,
    /// sorted lexicographically. Requires range scans enabled at
    /// builder time.
    pub async fn range_stream<R>(
        &self,
        range: R,
    ) -> Result<tokio_stream::wrappers::ReceiverStream<(Vec<u8>, Vec<u8>)>>
    where
        R: RangeBounds<Vec<u8>> + Send + 'static,
    {
        let inner = Arc::clone(&self.inner);
        let iter = blocking(move || inner.range_iter(range)).await?;
        Ok(spawn_iter_stream(iter))
    }

    /// Stream every `(key, value)` pair starting with `prefix`.
    /// Requires range scans enabled at builder time.
    pub async fn range_prefix_stream<K>(
        &self,
        prefix: K,
    ) -> Result<tokio_stream::wrappers::ReceiverStream<(Vec<u8>, Vec<u8>)>>
    where
        K: AsRef<[u8]>,
    {
        let inner = Arc::clone(&self.inner);
        let prefix = prefix.as_ref().to_vec();
        let iter = blocking(move || inner.range_prefix_iter(prefix)).await?;
        Ok(spawn_iter_stream(iter))
    }

    /// Stream keys at or after `start`. Requires range scans
    /// enabled at builder time.
    pub async fn iter_from_stream<K>(
        &self,
        start: K,
    ) -> Result<tokio_stream::wrappers::ReceiverStream<(Vec<u8>, Vec<u8>)>>
    where
        K: AsRef<[u8]>,
    {
        let inner = Arc::clone(&self.inner);
        let start = start.as_ref().to_vec();
        let iter = blocking(move || inner.iter_from(start)).await?;
        Ok(spawn_iter_stream(iter))
    }

    /// Stream keys strictly after `start`. Requires range scans
    /// enabled at builder time.
    pub async fn iter_after_stream<K>(
        &self,
        start: K,
    ) -> Result<tokio_stream::wrappers::ReceiverStream<(Vec<u8>, Vec<u8>)>>
    where
        K: AsRef<[u8]>,
    {
        let inner = Arc::clone(&self.inner);
        let start = start.as_ref().to_vec();
        let iter = blocking(move || inner.iter_after(start)).await?;
        Ok(spawn_iter_stream(iter))
    }

    /// Insert with TTL.
    #[cfg(feature = "ttl")]
    pub async fn insert_with_ttl<K, V>(&self, key: K, value: V, ttl: Ttl) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let inner = Arc::clone(&self.inner);
        let key = key.as_ref().to_vec();
        let value = value.as_ref().to_vec();
        blocking(move || inner.insert_with_ttl(key, value, ttl)).await
    }

    /// Expiration timestamp (Unix ms) for the key, if any.
    #[cfg(feature = "ttl")]
    pub async fn expires_at<K>(&self, key: K) -> Result<Option<u64>>
    where
        K: AsRef<[u8]>,
    {
        let inner = Arc::clone(&self.inner);
        let key = key.as_ref().to_vec();
        blocking(move || inner.expires_at(key)).await
    }

    /// Remaining TTL for the key, if any.
    #[cfg(feature = "ttl")]
    pub async fn ttl<K>(&self, key: K) -> Result<Option<Duration>>
    where
        K: AsRef<[u8]>,
    {
        let inner = Arc::clone(&self.inner);
        let key = key.as_ref().to_vec();
        blocking(move || inner.ttl(key)).await
    }

    /// Strip the expiration from the key.
    #[cfg(feature = "ttl")]
    pub async fn persist<K>(&self, key: K) -> Result<bool>
    where
        K: AsRef<[u8]>,
    {
        let inner = Arc::clone(&self.inner);
        let key = key.as_ref().to_vec();
        blocking(move || inner.persist(key)).await
    }

    /// Sweep expired records eagerly. Returns the count removed.
    #[cfg(feature = "ttl")]
    pub async fn sweep_expired(&self) -> Result<usize> {
        let inner = Arc::clone(&self.inner);
        blocking_infallible(move || inner.sweep_expired()).await
    }

    /// Open (or create) a named namespace handle.
    pub async fn namespace<N: AsRef<str>>(&self, name: N) -> Result<AsyncNamespace> {
        let inner = Arc::clone(&self.inner);
        let name = name.as_ref().to_owned();
        let ns = blocking(move || inner.namespace(name)).await?;
        Ok(AsyncNamespace::from_sync(ns))
    }

    /// Drop a named namespace.
    pub async fn drop_namespace<N: AsRef<str>>(&self, name: N) -> Result<bool> {
        let inner = Arc::clone(&self.inner);
        let name = name.as_ref().to_owned();
        blocking(move || inner.drop_namespace(name)).await
    }

    /// List every live namespace.
    pub async fn list_namespaces(&self) -> Result<Vec<String>> {
        let inner = Arc::clone(&self.inner);
        blocking(move || inner.list_namespaces()).await
    }

    /// Run a synchronous transaction closure on the blocking pool.
    ///
    /// The closure receives a `&mut Transaction` and runs to
    /// completion before this method's future resolves. Async work
    /// is not supported inside the closure — the transaction is
    /// committed (or rolled back) when the closure returns. For
    /// async work that must happen alongside DB updates, run the
    /// async piece first, capture the result, then call this with
    /// a closure that uses the captured value.
    pub async fn transaction<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut crate::Transaction<'_>) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let inner = Arc::clone(&self.inner);
        blocking(move || inner.transaction(f)).await
    }
}

/// Cheap-clone async handle scoped to one named namespace inside
/// an [`AsyncEmdb`]. The sync [`crate::Namespace`] is already
/// `Clone` with `Arc`-shared internals; this type simply wraps it
/// and threads every call through `spawn_blocking`.
#[derive(Clone)]
pub struct AsyncNamespace {
    inner: crate::Namespace,
}

impl std::fmt::Debug for AsyncNamespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncNamespace")
            .field("name", &self.inner.name())
            .finish()
    }
}

impl AsyncNamespace {
    pub(crate) fn from_sync(ns: crate::Namespace) -> Self {
        Self { inner: ns }
    }

    /// Name this handle was created for.
    #[must_use]
    pub fn name(&self) -> &str {
        self.inner.name()
    }

    /// Bridge to the sync `Namespace`. Cheap-clone — the
    /// underlying state is `Arc`-shared.
    #[must_use]
    pub fn sync_handle(&self) -> crate::Namespace {
        self.inner.clone()
    }

    /// Insert or replace a key/value pair.
    pub async fn insert<K, V>(&self, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let ns = self.inner.clone();
        let key = key.as_ref().to_vec();
        let value = value.as_ref().to_vec();
        blocking(move || ns.insert(key, value)).await
    }

    /// Insert many key/value pairs in one vectored journal-append
    /// pass.
    pub async fn insert_many<I, K, V>(&self, items: I) -> Result<()>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let ns = self.inner.clone();
        let owned: Vec<(Vec<u8>, Vec<u8>)> = items
            .into_iter()
            .map(|(k, v)| (k.as_ref().to_vec(), v.as_ref().to_vec()))
            .collect();
        blocking(move || ns.insert_many(owned)).await
    }

    /// Fetch a value by key.
    pub async fn get<K>(&self, key: K) -> Result<Option<Vec<u8>>>
    where
        K: AsRef<[u8]>,
    {
        let ns = self.inner.clone();
        let key = key.as_ref().to_vec();
        blocking(move || ns.get(key)).await
    }

    /// Remove a key, returning the previous value if any.
    pub async fn remove<K>(&self, key: K) -> Result<Option<Vec<u8>>>
    where
        K: AsRef<[u8]>,
    {
        let ns = self.inner.clone();
        let key = key.as_ref().to_vec();
        blocking(move || ns.remove(key)).await
    }

    /// Returns whether the key has a live record.
    pub async fn contains_key<K>(&self, key: K) -> Result<bool>
    where
        K: AsRef<[u8]>,
    {
        let ns = self.inner.clone();
        let key = key.as_ref().to_vec();
        blocking(move || ns.contains_key(key)).await
    }

    /// Live record count.
    pub async fn len(&self) -> Result<usize> {
        let ns = self.inner.clone();
        blocking(move || ns.len()).await
    }

    /// True iff the namespace has zero live records.
    pub async fn is_empty(&self) -> Result<bool> {
        let ns = self.inner.clone();
        blocking(move || ns.is_empty()).await
    }

    /// Drop every record in this namespace.
    pub async fn clear(&self) -> Result<()> {
        let ns = self.inner.clone();
        blocking(move || ns.clear()).await
    }

    /// Eagerly collect every `(key, value)` pair in this namespace.
    pub async fn iter(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let ns = self.inner.clone();
        blocking(move || Ok(ns.iter()?.collect())).await
    }

    /// Eagerly collect every key in this namespace.
    pub async fn keys(&self) -> Result<Vec<Vec<u8>>> {
        let ns = self.inner.clone();
        blocking(move || Ok(ns.keys()?.collect())).await
    }

    /// Eagerly collect every `(key, value)` pair in the half-open
    /// `range`, sorted lexicographically. Requires range scans
    /// enabled at builder time.
    pub async fn range<R>(&self, range: R) -> Result<Vec<(Vec<u8>, Vec<u8>)>>
    where
        R: RangeBounds<Vec<u8>> + Send + 'static,
    {
        let ns = self.inner.clone();
        blocking(move || ns.range(range)).await
    }

    /// Eagerly collect every `(key, value)` pair starting with
    /// `prefix`. Requires range scans enabled at builder time.
    pub async fn range_prefix<K>(&self, prefix: K) -> Result<Vec<(Vec<u8>, Vec<u8>)>>
    where
        K: AsRef<[u8]>,
    {
        let ns = self.inner.clone();
        let prefix = prefix.as_ref().to_vec();
        blocking(move || ns.range_prefix(prefix)).await
    }

    /// Eagerly collect keys at or after `start`. Requires range
    /// scans enabled at builder time.
    pub async fn iter_from<K>(&self, start: K) -> Result<Vec<(Vec<u8>, Vec<u8>)>>
    where
        K: AsRef<[u8]>,
    {
        let ns = self.inner.clone();
        let start = start.as_ref().to_vec();
        blocking(move || Ok(ns.iter_from(start)?.collect())).await
    }

    /// Eagerly collect keys strictly after `start`. Requires range
    /// scans enabled at builder time.
    pub async fn iter_after<K>(&self, start: K) -> Result<Vec<(Vec<u8>, Vec<u8>)>>
    where
        K: AsRef<[u8]>,
    {
        let ns = self.inner.clone();
        let start = start.as_ref().to_vec();
        blocking(move || Ok(ns.iter_after(start)?.collect())).await
    }

    /// Stream every `(key, value)` pair in this namespace. See
    /// [`AsyncEmdb::iter_stream`] for the channel-backed
    /// backpressure model.
    pub async fn iter_stream(
        &self,
    ) -> Result<tokio_stream::wrappers::ReceiverStream<(Vec<u8>, Vec<u8>)>> {
        let ns = self.inner.clone();
        let iter = blocking(move || ns.iter()).await?;
        Ok(spawn_iter_stream(iter))
    }

    /// Stream every key in this namespace.
    pub async fn keys_stream(&self) -> Result<tokio_stream::wrappers::ReceiverStream<Vec<u8>>> {
        let ns = self.inner.clone();
        let iter = blocking(move || ns.keys()).await?;
        Ok(spawn_iter_stream(iter))
    }

    /// Stream every `(key, value)` pair in the half-open `range`,
    /// sorted lexicographically. Requires range scans enabled at
    /// builder time.
    pub async fn range_stream<R>(
        &self,
        range: R,
    ) -> Result<tokio_stream::wrappers::ReceiverStream<(Vec<u8>, Vec<u8>)>>
    where
        R: RangeBounds<Vec<u8>> + Send + 'static,
    {
        let ns = self.inner.clone();
        let iter = blocking(move || ns.range_iter(range)).await?;
        Ok(spawn_iter_stream(iter))
    }

    /// Stream every `(key, value)` pair starting with `prefix`.
    /// Requires range scans enabled at builder time.
    pub async fn range_prefix_stream<K>(
        &self,
        prefix: K,
    ) -> Result<tokio_stream::wrappers::ReceiverStream<(Vec<u8>, Vec<u8>)>>
    where
        K: AsRef<[u8]>,
    {
        let ns = self.inner.clone();
        let prefix = prefix.as_ref().to_vec();
        let iter = blocking(move || ns.range_prefix_iter(prefix)).await?;
        Ok(spawn_iter_stream(iter))
    }

    /// Stream keys at or after `start`. Requires range scans
    /// enabled at builder time.
    pub async fn iter_from_stream<K>(
        &self,
        start: K,
    ) -> Result<tokio_stream::wrappers::ReceiverStream<(Vec<u8>, Vec<u8>)>>
    where
        K: AsRef<[u8]>,
    {
        let ns = self.inner.clone();
        let start = start.as_ref().to_vec();
        let iter = blocking(move || ns.iter_from(start)).await?;
        Ok(spawn_iter_stream(iter))
    }

    /// Stream keys strictly after `start`. Requires range scans
    /// enabled at builder time.
    pub async fn iter_after_stream<K>(
        &self,
        start: K,
    ) -> Result<tokio_stream::wrappers::ReceiverStream<(Vec<u8>, Vec<u8>)>>
    where
        K: AsRef<[u8]>,
    {
        let ns = self.inner.clone();
        let start = start.as_ref().to_vec();
        let iter = blocking(move || ns.iter_after(start)).await?;
        Ok(spawn_iter_stream(iter))
    }
}
