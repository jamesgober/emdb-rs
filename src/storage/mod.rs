// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Storage engine. Mmap-backed append-only file with a sharded
//! in-memory hash index.
//!
//! This module is the entire on-disk backend; there is no alternate
//! path. The public `Emdb` handle wraps a single [`Engine`] instance.

#[allow(dead_code)]
pub(crate) mod engine;
#[allow(dead_code)]
pub(crate) mod flush;
#[allow(dead_code)]
pub(crate) mod format;
#[allow(dead_code)]
pub(crate) mod index;
#[allow(dead_code)]
pub(crate) mod store;

pub(crate) use engine::{Engine, EngineConfig, DEFAULT_NAMESPACE_ID};
pub use flush::FlushPolicy;
