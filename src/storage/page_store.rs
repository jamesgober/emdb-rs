// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Page-file storage backend backed by a sidecar WAL.

use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

use crate::storage::codec::decode_op;
use crate::storage::page::btree::BTreeIndex;
use crate::storage::page::pager::BufferedPager;
use crate::storage::page::value::{free_value, read_value, write_value};
use crate::storage::page::PAGE_SIZE;
use crate::storage::wal::Wal;
use crate::storage::{FlushPolicy, Op, OpRef, SnapshotIter, Storage};
use crate::{Error, Result};

#[derive(Debug)]
struct OpenBatch {
    tx_id: u64,
    expected_count: u32,
    buffered_ops: Vec<Op>,
    start_cursor: usize,
}

/// Page-file storage with a sidecar WAL.
#[derive(Debug)]
pub(crate) struct PageStorage {
    path: PathBuf,
    flags: u32,
    pager: BufferedPager,
    wal: Wal,
    flush_policy: FlushPolicy,
    write_count: u32,
    last_tx_id: u64,
}

impl PageStorage {
    /// Open or create a page-file backend and its sidecar WAL.
    pub(crate) fn new(
        path: impl Into<PathBuf>,
        flush_policy: FlushPolicy,
        flags: u32,
        #[cfg(feature = "mmap")] use_mmap: bool,
    ) -> Result<Self> {
        if matches!(flush_policy, FlushPolicy::EveryN(0)) {
            return Err(Error::InvalidConfig("flush policy EveryN requires N > 0"));
        }

        let path = path.into();
        #[cfg(feature = "mmap")]
        let pager = BufferedPager::open_with_mmap(path.clone(), flags, use_mmap)?;
        #[cfg(not(feature = "mmap"))]
        let pager = BufferedPager::open(path.clone(), flags)?;
        let wal = Wal::open(Wal::path_for(&path))?;
        let last_tx_id = pager.last_tx_id();

        Ok(Self {
            path,
            flags,
            pager,
            wal,
            flush_policy,
            write_count: 0,
            last_tx_id,
        })
    }

    fn maybe_auto_flush(&mut self) -> Result<()> {
        match self.flush_policy {
            FlushPolicy::OnEachWrite => self.flush(),
            FlushPolicy::EveryN(n) => {
                if self.write_count % n == 0 {
                    self.flush()?;
                }
                Ok(())
            }
            FlushPolicy::Manual => Ok(()),
        }
    }

    fn apply_ops(&mut self, ops: &[Op]) -> Result<()> {
        let mut index = BTreeIndex::open(&mut self.pager)?;
        let mut changed = false;
        for op in ops {
            match op {
                Op::Insert {
                    key,
                    value,
                    expires_at,
                } => {
                    if let Some(existing) = index.get(key) {
                        free_value(index.pager_mut(), existing)?;
                    }
                    let value_ref = write_value(index.pager_mut(), value, *expires_at)?;
                    index.insert_deferred(key.clone(), value_ref);
                    changed = true;
                }
                Op::Remove { key } => {
                    if let Some(existing) = index.get(key) {
                        free_value(index.pager_mut(), existing)?;
                    }
                    let _removed = index.remove_deferred(key);
                    changed = changed || _removed.is_some();
                }
                Op::Clear => {
                    let existing = index.range_scan(None, None);
                    for (_key, value_ref) in existing {
                        free_value(index.pager_mut(), value_ref)?;
                    }
                    index.clear_deferred();
                    changed = true;
                }
                Op::Checkpoint { .. } => {}
                Op::BatchBegin { .. } | Op::BatchEnd { .. } => {
                    return Err(Error::Corrupted {
                        offset: 0,
                        reason: "raw batch markers reached page applier",
                    });
                }
            }
        }
        if changed {
            index.rebuild_from_deferred()?;
        }
        Ok(())
    }

    fn replay_page_file(&mut self, sink: &mut dyn FnMut(Op) -> Result<()>) -> Result<()> {
        let index = BTreeIndex::open(&mut self.pager)?;
        for (key, value_ref) in index.range_scan(None, None) {
            let (value, expires_at) = read_value(&mut self.pager, value_ref)?;
            sink(Op::Insert {
                key,
                value,
                expires_at,
            })?;
        }
        Ok(())
    }

    fn replay_legacy_tail(&mut self, sink: &mut dyn FnMut(Op) -> Result<()>) -> Result<()> {
        let page_bytes_len = self
            .pager
            .header()
            .page_count
            .checked_mul(PAGE_SIZE as u64)
            .ok_or(Error::Corrupted {
                offset: 0,
                reason: "page file length overflow",
            })?;

        let file_len = std::fs::metadata(&self.path)?.len();
        if file_len <= page_bytes_len {
            return Ok(());
        }

        let mut file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        let _offset = file.seek(SeekFrom::Start(page_bytes_len))?;
        let mut tail = Vec::new();
        let _read = file.read_to_end(&mut tail)?;

        let mut cursor = 0_usize;
        let mut open_batch: Option<OpenBatch> = None;
        while cursor < tail.len() {
            let record_start = cursor;
            match decode_op(&tail[cursor..]) {
                Ok((op, consumed)) => {
                    cursor = cursor.checked_add(consumed).ok_or(Error::Corrupted {
                        offset: cursor as u64,
                        reason: "cursor overflow",
                    })?;
                    match op {
                        Op::BatchBegin { tx_id, op_count } => {
                            if let Some(existing) = &open_batch {
                                file.set_len(page_bytes_len + existing.start_cursor as u64)?;
                                return Ok(());
                            }
                            open_batch = Some(OpenBatch {
                                tx_id,
                                expected_count: op_count,
                                buffered_ops: Vec::new(),
                                start_cursor: record_start,
                            });
                        }
                        Op::BatchEnd { tx_id } => {
                            let Some(batch) = open_batch.take() else {
                                file.set_len(page_bytes_len + record_start as u64)?;
                                return Ok(());
                            };
                            if batch.tx_id != tx_id
                                || batch.buffered_ops.len() != batch.expected_count as usize
                            {
                                file.set_len(page_bytes_len + batch.start_cursor as u64)?;
                                return Ok(());
                            }
                            for buffered in batch.buffered_ops {
                                sink(buffered)?;
                            }
                        }
                        other => {
                            if let Some(batch) = &mut open_batch {
                                batch.buffered_ops.push(other);
                            } else {
                                sink(other)?;
                            }
                        }
                    }
                }
                Err(Error::Corrupted { .. }) => {
                    let truncate_cursor = open_batch
                        .as_ref()
                        .map_or(cursor, |batch| batch.start_cursor);
                    file.set_len(page_bytes_len + truncate_cursor as u64)?;
                    return Ok(());
                }
                Err(other) => return Err(other),
            }
        }

        if let Some(batch) = open_batch {
            file.set_len(page_bytes_len + batch.start_cursor as u64)?;
            return Ok(());
        }

        file.set_len(page_bytes_len)?;
        Ok(())
    }
}

impl Storage for PageStorage {
    fn append(&mut self, op: OpRef<'_>) -> Result<()> {
        self.wal.append(op)?;
        self.write_count = self.write_count.saturating_add(1);
        self.maybe_auto_flush()
    }

    fn flush(&mut self) -> Result<()> {
        if self.wal.is_empty()? {
            self.pager.flush()?;
            return self.wal.flush();
        }

        let mut ops = Vec::new();
        self.wal.replay(&mut |op| {
            ops.push(op);
            Ok(())
        })?;
        self.apply_ops(&ops)?;
        self.pager.set_last_tx_id(self.last_tx_id)?;
        self.pager.flush()?;
        self.wal.truncate()
    }

    fn replay(&mut self, sink: &mut dyn FnMut(Op) -> Result<()>) -> Result<()> {
        self.replay_page_file(sink)?;
        self.replay_legacy_tail(sink)?;
        self.wal.replay(sink)
    }

    fn compact(&mut self, snapshot: SnapshotIter<'_>) -> Result<()> {
        let owned = snapshot
            .map(|entry| (entry.key.to_vec(), entry.value.to_vec(), entry.expires_at))
            .collect::<Vec<_>>();

        self.pager.reset(self.flags)?;
        {
            let mut index = BTreeIndex::open(&mut self.pager)?;
            for (key, value, expires_at) in &owned {
                let value_ref = write_value(index.pager_mut(), value, *expires_at)?;
                index.insert(key.clone(), value_ref)?;
            }
        }
        self.pager.set_last_tx_id(self.last_tx_id)?;
        self.pager.flush()?;
        self.wal.truncate()
    }

    fn last_tx_id(&self) -> u64 {
        self.last_tx_id
    }

    fn set_last_tx_id(&mut self, tx_id: u64) -> Result<()> {
        self.last_tx_id = tx_id;
        self.wal.flush()?;
        self.pager.set_last_tx_id(tx_id)
    }
}

#[cfg(test)]
mod tests {
    use super::PageStorage;
    use crate::storage::{FlushPolicy, OpRef, SnapshotEntry, Storage};

    fn test_path(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0_u128, |duration| duration.as_nanos());
        path.push(format!("emdb-page-store-{name}-{nanos}.emdb"));
        path
    }

    #[test]
    fn test_page_storage_replay_includes_page_file_and_wal() {
        let path = test_path("replay");
        let storage = PageStorage::new(
            &path,
            FlushPolicy::Manual,
            0,
            #[cfg(feature = "mmap")]
            false,
        );
        assert!(storage.is_ok());
        let mut storage = match storage {
            Ok(storage) => storage,
            Err(err) => panic!("page storage open should succeed: {err}"),
        };

        assert!(storage
            .append(OpRef::Insert {
                key: b"a",
                value: b"1",
                expires_at: None,
            })
            .is_ok());
        assert!(storage.flush().is_ok());
        assert!(storage
            .append(OpRef::Insert {
                key: b"b",
                value: b"2",
                expires_at: None,
            })
            .is_ok());

        let mut seen = Vec::new();
        let replayed = storage.replay(&mut |op| {
            seen.push(op);
            Ok(())
        });
        assert!(replayed.is_ok());
        assert_eq!(seen.len(), 2);

        let _removed = std::fs::remove_file(&path);
        let wal_path = super::Wal::path_for(&path);
        let _removed = std::fs::remove_file(wal_path);
    }

    #[test]
    fn test_page_storage_compact_rewrites_smaller_page_file() {
        let path = test_path("compact");
        let storage = PageStorage::new(
            &path,
            FlushPolicy::Manual,
            0,
            #[cfg(feature = "mmap")]
            false,
        );
        assert!(storage.is_ok());
        let mut storage = match storage {
            Ok(storage) => storage,
            Err(err) => panic!("page storage open should succeed: {err}"),
        };

        for index in 0..128_u32 {
            let key = format!("k{index}").into_bytes();
            let value = vec![b'x'; 64];
            assert!(storage
                .append(OpRef::Insert {
                    key: &key,
                    value: &value,
                    expires_at: None,
                })
                .is_ok());
        }
        assert!(storage.flush().is_ok());
        let before = std::fs::metadata(&path).map(|meta| meta.len());
        assert!(before.is_ok());
        let before = before.unwrap_or(u64::MAX);

        let owned = [(b"one".to_vec(), b"1".to_vec(), None)];
        let snapshot = Box::new(owned.iter().map(|(key, value, expires_at)| SnapshotEntry {
            key,
            value,
            expires_at: *expires_at,
        }));
        assert!(storage.compact(snapshot).is_ok());
        let after = std::fs::metadata(&path).map(|meta| meta.len());
        assert!(after.is_ok());
        let after = after.unwrap_or(u64::MAX);
        assert!(after < before);

        let _removed = std::fs::remove_file(&path);
        let wal_path = super::Wal::path_for(&path);
        let _removed = std::fs::remove_file(wal_path);
    }
}
