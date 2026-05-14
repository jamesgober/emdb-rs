# emdb 1.0 Stability Commitment

This document defines what emdb commits to preserving across
the 1.x release line, what it explicitly does not commit to,
and how breaking changes will be communicated.

The 0.9.x line is **API-stable and on-disk-format-stable** —
every 0.9.x release is a drop-in upgrade from any prior 0.9.x.
0.9.7 and 0.9.8 are the last 0.9.x releases planned before 1.0;
the 1.0 commitment below makes the 0.9.x guarantees explicit
and durable.

---

## Contents

- [What 1.0 means](#what-10-means)
- [The stability contract](#the-stability-contract)
- [What we explicitly don't promise](#what-we-explicitly-dont-promise)
- [Versioning](#versioning)
- [MSRV policy](#msrv-policy)
- [Deprecation policy](#deprecation-policy)
- [On-disk format](#on-disk-format)
- [Cargo features](#cargo-features)
- [Errors](#errors)
- [Yanked releases](#yanked-releases)
- [Security advisories](#security-advisories)

---

## What 1.0 means

emdb 1.0 is a **stability commitment**, not a feature release.
The 0.9.x line has been carrying the engineering work; 1.0 is
the version that says "this is the surface, we'll keep it,
build production code against it."

The substantive engineering — the lock-free engine, the sharded
index, the journal substrate via fsys, the async surface, the
streaming iterators — is all already in the 0.9.x line by the
time 1.0 ships. 1.0 itself adds **no new features**. It is the
point where:

- Every public item enters the SemVer contract below.
- The on-disk format is frozen for the 1.x line.
- Breaking changes start to need a major-version bump.
- The MSRV bump policy applies (see below).

---

## The stability contract

For the lifetime of the 1.x line:

### Public API

Every item exported from the `emdb::` namespace is **API-stable**:

- Type names won't be renamed or removed.
- Method signatures won't change in ways that break source
  compatibility.
- Method behaviour won't change in ways that break semantic
  compatibility.
- New methods may be added (1.x minor versions).
- New non-`#[non_exhaustive]` enum variants will not be added
  to existing public enums (would break exhaustive matches).
- New fields on public structs marked `#[non_exhaustive]` may
  be added.

### Concretely

These are all part of the 1.x contract:

```text
emdb::Emdb                  — every method, including builder accessors
emdb::EmdbBuilder           — every fluent method
emdb::Namespace             — every method
emdb::Transaction           — every method
emdb::Focus                 — every method (nested feature)
emdb::AsyncEmdb             — every method (async feature)
emdb::AsyncNamespace        — every method (async feature)
emdb::FlushPolicy           — variants
emdb::Ttl                   — variants
emdb::Error                 — variants (the enum is #[non_exhaustive])
emdb::Result<T>             — alias signature
emdb::ValueRef<'_>          — Deref, AsRef, into_owned
emdb::EmdbStats              — fields
emdb::LockHolder            — fields
emdb::Cipher                — variants (encrypt feature)
emdb::EncryptionInput       — fields (encrypt feature)
emdb::EmdbIter / KeyIter / RangeIter — Iterator impls
emdb::NamespaceIter / KeyIter / RangeIter — Iterator impls
```

If anything in the list above changes incompatibly without a
major-version bump, that's a stability-contract bug worth
filing.

---

## What we explicitly don't promise

The following are explicitly **not** part of the stability
contract, even in 1.x:

### Internal modules

Anything under `pub(crate)` or private modules. The internal
layout of:

- `src/storage/*`
- `src/db::Inner`
- `src/namespace::NamespaceInner`
- Per-shard hash function constants
- Frame format internals (those are fsys's contract, not
  emdb's)

is free to change at any point.

### Performance characteristics

- Latency / throughput numbers in any release notes are
  point-in-time benchmarks. They are not guarantees.
- Performance regressions in patch releases are treated as
  bugs and will be fixed in subsequent patches, but they are
  not contract violations.
- New hardware / new OS versions / new tokio versions may
  shift relative performance.

### Error messages

The `Display` output of `emdb::Error` and inner
`io::Error::Display` text may change between releases. The
enum **variants** are stable; the human-readable messages are
not.

### Cargo dependencies

Direct dependencies (`fsys`, `parking_lot`, `crossbeam-skiplist`,
`tokio`, etc.) may be bumped to new minor versions, swapped out,
or removed entirely. The public API stays the same.

### Bench harness output

The Criterion HTML reports under `target/criterion/` are an
internal tool; their layout and metric names are not stable.

### Examples

The contents of `examples/*.rs` may change to reflect current
best practice. The examples are documentation, not API.

### Doctest output

Doctests embedded in `///` comments are validated by the test
suite; their text may be edited at any release.

---

## Versioning

emdb follows [Semantic Versioning 2.0](https://semver.org).

| Bump | Meaning |
|---|---|
| **Patch** (1.0.0 → 1.0.1) | Bug fixes, perf improvements, doc updates. No API additions. No on-disk format changes. |
| **Minor** (1.0.x → 1.1.0) | New APIs added (backward compatible). On-disk format may add optional fields readable by older 1.x. |
| **Major** (1.x → 2.0.0) | Breaking API or on-disk format changes. Migration path documented. |

Pre-release versions (`1.0.0-rc.1`, `1.0.0-beta.2`) use the
SemVer pre-release suffix syntax and are explicitly not
covered by the stability contract.

---

## MSRV policy

Minimum Supported Rust Version is **Rust 1.75** through the
0.9.x line and at least the start of the 1.x line.

The MSRV-bump policy:

- MSRV bumps are **minor-version** events, not patch.
- We will not bump MSRV unless one of the following applies:
  1. A stable Rust feature genuinely simplifies the engine
     code (not cosmetic — load-bearing).
  2. A dependency bumps its MSRV and emdb can't pin around it.
  3. A correctness or perf fix requires a newer compiler
     feature.
- MSRV-bumping releases will document the new MSRV in the
  CHANGELOG and on the release notes.
- The CI matrix tests `stable`, `MSRV`, and the prior MSRV for
  at least one minor-version transition.

Patch releases (1.0.0 → 1.0.1) **never** bump MSRV.

---

## Deprecation policy

When a 1.x API is going away:

1. **Mark it `#[deprecated]`** in the release that introduces
   the replacement. Include a `note = "..."` explaining the
   migration.
2. **Keep the deprecated API working** for at least one full
   minor cycle (i.e. if deprecated in 1.3, it still works in
   1.4 and 1.5).
3. **Remove the API** only in the next major version
   (1.x → 2.0).

In short: deprecation is a soft warning across minor cycles,
not a hard removal. Patch releases never deprecate.

---

## On-disk format

The 1.x on-disk format is **forward-compatible within 1.x**:

- Files written by 1.0.x can be read by 1.1, 1.2, ..., 1.x.
- Files written by 1.1+ may use newer optional fields; 1.0.x
  will either read them ignoring unknown fields or refuse to
  open with `Error::VersionMismatch`. The choice is
  documented per-field in the release that adds it.
- The frame format magic + checksum scheme is owned by fsys;
  the same forward-compatibility commitment applies at that
  layer.

The format is **not** backward-compatible across major versions
without explicit migration:

- 2.0 may introduce a new frame layout. A migration tool will
  be provided.
- 0.x → 1.x: 0.9.x files are read by 1.0 without migration
  (0.9 is the 1.0 stability rehearsal). Pre-0.9 files
  (0.7, 0.8) are not compatible and need to be migrated via
  the latest 0.8.x release as a stepping stone.

---

## Cargo features

The following features are stable for the 1.x line:

- `ttl` — per-record expiration. Default-on.
- `nested` — `Focus` and dotted-prefix groups.
- `encrypt` — at-rest encryption (AES-256-GCM + ChaCha20-Poly1305).
- `async` — `AsyncEmdb` / `AsyncNamespace` + streaming iterators.

Bench-only features (`bench-compare`, `bench-redis`,
`bench-rocksdb`) are dev-time tools and may change without
notice.

Adding a new feature is **non-breaking** as long as the default
feature set doesn't change. Removing or renaming an existing
feature is a major-version event.

---

## Errors

`emdb::Error` is `#[non_exhaustive]`, so new variants may be
added in any 1.x minor release. Callers should never write
exhaustive match arms over `Error`; use:

```rust,ignore
match err {
    Error::Io(io_err) => ...,
    Error::AlreadyLocked => ...,
    // ... known variants ...
    _ => panic!("unexpected emdb error: {err}"),
}
```

The variant names and their semantics are stable. Adding a
variant for a new failure mode is a minor-version event;
renaming or removing a variant is a major-version event.

---

## Yanked releases

If a published release contains a serious bug (corruption,
data loss, soundness), we yank it from crates.io:

1. The release is marked `cargo yank`ed — new builds resolving
   `emdb = "1.0"` won't pick the yanked version.
2. Existing `Cargo.lock` files still build (yank is advisory).
3. A patch release with the fix is published as soon as
   practical.
4. The CHANGELOG entry for the yanked version is annotated
   with "**YANKED — upgrade to <fix>**".

Yanks are reserved for **correctness** issues, not perf
regressions or cosmetic bugs.

---

## Security advisories

Vulnerabilities are coordinated through:

- The GitHub repository's [Security tab](https://github.com/jamesgober/emdb-rs/security)
  (private disclosure).
- The [RustSec Advisory Database](https://rustsec.org/) once
  a fix is released.

Disclosures should go to **james@hivedb.com**. The minimum
target is a fix release within 30 days of confirmed report; the
public advisory follows the fix release by a 24-hour
grace window for downstream upgraders.

---

## Roadmap visibility

The CHANGELOG records what shipped. The README's `Status`
section records what's outstanding. Neither is a guarantee —
roadmap items shift based on what the engineering work
reveals.

If you're planning around emdb for a production deployment and
need stability assurances beyond what this document covers,
file an issue at the repository describing your shape; concrete
shapes get concrete answers.

---

## See also

- [API.md](API.md) — full API reference.
- [ARCHITECTURE.md](ARCHITECTURE.md) — engine internals (not
  part of the stability contract).
- [CHANGELOG.md](../CHANGELOG.md) — release-by-release record.
