// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! v0.7 storage engine: packed slotted leaves, in-memory keymap, layered
//! caches, group-commit WAL.
//!
//! This submodule houses every piece of the v0.7 redesign so it can be built
//! out incrementally without disturbing the v0.6 code paths under
//! `src/storage/{file,page,page_store,wal}.rs`. Once Phase H lands and the
//! migrator is wired up, the v0.6 modules become legacy reader-only.

#[allow(dead_code)]
pub(crate) mod catalog;
#[allow(dead_code)]
pub(crate) mod engine;
#[allow(dead_code)]
pub(crate) mod io;
#[allow(dead_code)]
pub(crate) mod store;
#[allow(dead_code)]
pub(crate) mod wal;
