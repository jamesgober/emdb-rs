// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Storage engine. Mmap-backed append-only file with a sharded
//! in-memory hash index.
//!
//! This module is the entire on-disk backend; there is no alternate
//! path. The public `Emdb` handle wraps a single [`Engine`] instance.

pub(crate) mod arc_cell;
pub(crate) mod engine;
pub(crate) mod flush;
// The encrypted-record decoder and its types are only reached with the
// `encrypt` feature (and from the fuzz targets).
#[cfg_attr(not(feature = "encrypt"), allow(dead_code))]
pub(crate) mod format;
pub(crate) mod index;
pub(crate) mod integrity;
pub(crate) mod meta;
pub(crate) mod store;

pub(crate) use engine::{Engine, EngineConfig, ReadView, DEFAULT_NAMESPACE_ID};
pub use flush::FlushPolicy;
