// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! In-memory no-op storage backend.

use std::path::Path;

use crate::storage::{Op, SnapshotIter, Storage};
use crate::Result;

/// In-memory storage backend that performs no persistence.
#[derive(Debug, Default)]
pub(crate) struct MemoryStorage;

impl Storage for MemoryStorage {
    fn append(&mut self, _op: &Op) -> Result<()> {
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn replay(&mut self, _sink: &mut dyn FnMut(Op) -> Result<()>) -> Result<()> {
        Ok(())
    }

    fn compact(&mut self, _snapshot: SnapshotIter<'_>) -> Result<()> {
        Ok(())
    }

    fn path(&self) -> Option<&Path> {
        None
    }

    fn last_tx_id(&self) -> u64 {
        0
    }

    fn set_last_tx_id(&mut self, _tx_id: u64) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::memory::MemoryStorage;
    use crate::storage::{Op, Storage};

    #[test]
    fn memory_storage_replay_is_empty() {
        let mut storage = MemoryStorage;
        let mut count = 0_usize;
        let replayed = storage.replay(&mut |_op| {
            count += 1;
            Ok(())
        });

        assert!(replayed.is_ok());
        assert_eq!(count, 0);
    }

    #[test]
    fn memory_storage_append_flush_and_compact_are_noop() {
        let mut storage = MemoryStorage;

        let appended = storage.append(&Op::Clear);
        let flushed = storage.flush();
        let compacted = storage.compact(Box::new(std::iter::empty()));

        assert!(appended.is_ok());
        assert!(flushed.is_ok());
        assert!(compacted.is_ok());
        assert!(storage.path().is_none());
    }
}
