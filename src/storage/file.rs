// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! File-backed storage backend.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::storage::codec::{decode_op, encode_op, read_header, write_header, HEADER_LEN};
use crate::storage::{build_flags, FlushPolicy, Op, OpRef, SnapshotIter, Storage, FORMAT_VERSION};
use crate::{Error, Result};

const FORMAT_VERSION_V1: u32 = 1;
const LAST_TX_ID_OFFSET: u64 = 32;

#[derive(Debug)]
struct OpenBatch {
    tx_id: u64,
    expected_count: u32,
    buffered_ops: Vec<Op>,
    start_cursor: usize,
}

/// File-backed storage implementation.
#[derive(Debug)]
pub(crate) struct FileStorage {
    file: File,
    path: PathBuf,
    flags: u32,
    last_tx_id: u64,
    flush_policy: FlushPolicy,
    write_count: u32,
}

impl FileStorage {
    /// Open or create a file-backed storage backend.
    pub(crate) fn new(
        path: impl Into<PathBuf>,
        flush_policy: FlushPolicy,
        flags: u32,
    ) -> Result<Self> {
        if matches!(flush_policy, FlushPolicy::EveryN(0)) {
            return Err(Error::InvalidConfig("flush policy EveryN requires N > 0"));
        }

        let path = path.into();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        let meta_len = file.metadata()?.len();
        let mut effective_flags = flags;
        let mut last_tx_id = 0_u64;
        if meta_len == 0 {
            write_header(&mut file, FORMAT_VERSION, flags, 0)?;
            file.flush()?;
            file.sync_data()?;
        } else {
            let _offset = file.seek(SeekFrom::Start(0))?;
            let header = read_header(&mut file)?;

            if header.format_ver != FORMAT_VERSION_V1 && header.format_ver != FORMAT_VERSION {
                return Err(Error::VersionMismatch {
                    found: header.format_ver,
                    expected: FORMAT_VERSION,
                });
            }

            let current_flags = build_flags();
            if (header.flags & current_flags) != header.flags {
                return Err(Error::FeatureMismatch {
                    file_flags: header.flags,
                    build_flags: current_flags,
                });
            }

            effective_flags = header.flags;
            last_tx_id = header.last_tx_id;
        }

        let _offset = file.seek(SeekFrom::End(0))?;

        Ok(Self {
            file,
            path,
            flags: effective_flags,
            last_tx_id,
            flush_policy,
            write_count: 0,
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

    fn replay_impl(&mut self, sink: &mut dyn FnMut(Op) -> Result<()>) -> Result<()> {
        let _offset = self.file.seek(SeekFrom::Start(HEADER_LEN as u64))?;
        let mut bytes = Vec::new();
        let _read = self.file.read_to_end(&mut bytes)?;

        let mut cursor = 0_usize;
        let mut open_batch: Option<OpenBatch> = None;
        while cursor < bytes.len() {
            let record_start = cursor;
            match decode_op(&bytes[cursor..]) {
                Ok((op, consumed)) => {
                    cursor = cursor.checked_add(consumed).ok_or(Error::Corrupted {
                        offset: cursor as u64,
                        reason: "cursor overflow",
                    })?;

                    match op {
                        Op::BatchBegin { tx_id, op_count } => {
                            if let Some(existing) = &open_batch {
                                self.truncate_to(existing.start_cursor)?;
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
                                self.truncate_to(record_start)?;
                                return Ok(());
                            };

                            if batch.tx_id != tx_id {
                                self.truncate_to(batch.start_cursor)?;
                                return Ok(());
                            }

                            if batch.buffered_ops.len() != batch.expected_count as usize {
                                self.truncate_to(batch.start_cursor)?;
                                return Ok(());
                            }

                            for buffered in batch.buffered_ops {
                                sink(buffered)?;
                            }

                            self.last_tx_id = self.last_tx_id.max(tx_id);
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
                Err(Error::Corrupted { reason, .. }) => {
                    let truncate_cursor = match &open_batch {
                        Some(batch) => batch.start_cursor,
                        None => cursor,
                    };
                    self.truncate_to(truncate_cursor)?;
                    let _ignored_reason = reason;
                    return Ok(());
                }
                Err(other) => return Err(other),
            }
        }

        if let Some(batch) = open_batch {
            self.truncate_to(batch.start_cursor)?;
            return Ok(());
        }

        let _offset = self.file.seek(SeekFrom::End(0))?;
        Ok(())
    }

    fn truncate_to(&mut self, cursor: usize) -> Result<()> {
        let truncate_to = (HEADER_LEN + cursor) as u64;
        self.file.set_len(truncate_to)?;
        self.file.flush()?;
        self.file.sync_data()?;
        let _offset = self.file.seek(SeekFrom::End(0))?;
        Ok(())
    }

    fn atomic_replace_with_tmp(&mut self, tmp_path: &Path) -> Result<()> {
        std::fs::rename(tmp_path, &self.path)?;
        self.file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        let _offset = self.file.seek(SeekFrom::End(0))?;
        Ok(())
    }

    fn write_last_tx_id_to_header(&mut self) -> Result<()> {
        let _offset = self.file.seek(SeekFrom::Start(LAST_TX_ID_OFFSET))?;
        self.file.write_all(&self.last_tx_id.to_le_bytes())?;
        self.file.flush()?;
        self.file.sync_data()?;
        let _offset = self.file.seek(SeekFrom::End(0))?;
        Ok(())
    }
}

impl Storage for FileStorage {
    fn append(&mut self, op: OpRef<'_>) -> Result<()> {
        let mut bytes = Vec::new();
        encode_op(&mut bytes, op);
        self.file.write_all(&bytes)?;
        self.write_count = self.write_count.saturating_add(1);
        self.maybe_auto_flush()
    }

    fn flush(&mut self) -> Result<()> {
        self.file.flush()?;
        self.file.sync_data()?;
        Ok(())
    }

    fn replay(&mut self, sink: &mut dyn FnMut(Op) -> Result<()>) -> Result<()> {
        self.replay_impl(sink)
    }

    fn compact(&mut self, snapshot: SnapshotIter<'_>) -> Result<()> {
        let mut tmp_path = self.path.clone();
        let tmp_name = format!(
            "{}.tmp",
            self.path
                .file_name()
                .and_then(|s| s.to_str())
                .ok_or(Error::InvalidConfig("invalid file name"))?
        );
        tmp_path.set_file_name(tmp_name);

        let mut tmp = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;

        write_header(&mut tmp, FORMAT_VERSION, self.flags, self.last_tx_id)?;

        let mut record = Vec::new();
        let mut count = 0_u32;
        for entry in snapshot {
            record.clear();
            encode_op(
                &mut record,
                OpRef::Insert {
                    key: entry.key,
                    value: entry.value,
                    expires_at: entry.expires_at,
                },
            );
            tmp.write_all(&record)?;
            count = count.saturating_add(1);
        }

        record.clear();
        encode_op(
            &mut record,
            OpRef::Checkpoint {
                record_count: count,
            },
        );
        tmp.write_all(&record)?;
        tmp.flush()?;
        tmp.sync_data()?;
        drop(tmp);

        self.atomic_replace_with_tmp(&tmp_path)
    }

    fn last_tx_id(&self) -> u64 {
        self.last_tx_id
    }

    fn set_last_tx_id(&mut self, tx_id: u64) -> Result<()> {
        self.last_tx_id = tx_id;
        self.write_last_tx_id_to_header()
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::file::FileStorage;
    use crate::storage::{FlushPolicy, Op, OpRef, Storage};

    fn test_path(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0_u128, |d| d.as_nanos());
        p.push(format!("emdb-{name}-{nanos}.emdb"));
        p
    }

    #[test]
    fn file_storage_round_trip_replay() {
        let path = test_path("roundtrip");
        let storage = FileStorage::new(&path, FlushPolicy::OnEachWrite, 0);
        assert!(storage.is_ok());
        let mut storage = match storage {
            Ok(storage) => storage,
            Err(err) => panic!("storage open should succeed: {err}"),
        };

        assert!(storage
            .append(OpRef::Insert {
                key: b"k",
                value: b"v",
                expires_at: None,
            })
            .is_ok());
        assert!(storage.append(OpRef::Remove { key: b"k" }).is_ok());
        drop(storage);

        let reopen = FileStorage::new(&path, FlushPolicy::Manual, 0);
        assert!(reopen.is_ok());
        let mut reopen = match reopen {
            Ok(storage) => storage,
            Err(err) => panic!("storage reopen should succeed: {err}"),
        };

        let mut seen = 0_usize;
        let replayed = reopen.replay(&mut |_op| {
            seen += 1;
            Ok(())
        });

        assert!(replayed.is_ok());
        assert_eq!(seen, 2);
        assert!(std::fs::remove_file(path).is_ok());
    }

    fn replay_ops(path: &std::path::Path) -> Vec<Op> {
        let reopen = FileStorage::new(path, FlushPolicy::Manual, 0);
        assert!(reopen.is_ok());
        let mut reopen = match reopen {
            Ok(storage) => storage,
            Err(err) => panic!("storage reopen should succeed: {err}"),
        };

        let mut seen = Vec::new();
        let replayed = reopen.replay(&mut |op| {
            seen.push(op);
            Ok(())
        });
        assert!(replayed.is_ok());
        seen
    }

    #[test]
    fn replay_applies_complete_batch() {
        let path = test_path("batch-complete");
        let storage = FileStorage::new(&path, FlushPolicy::Manual, 0);
        assert!(storage.is_ok());
        let mut storage = match storage {
            Ok(storage) => storage,
            Err(err) => panic!("storage open should succeed: {err}"),
        };

        assert!(storage
            .append(OpRef::BatchBegin {
                tx_id: 7,
                op_count: 1,
            })
            .is_ok());
        assert!(storage
            .append(OpRef::Insert {
                key: b"k",
                value: b"v",
                expires_at: None,
            })
            .is_ok());
        assert!(storage.append(OpRef::BatchEnd { tx_id: 7 }).is_ok());
        assert!(storage.set_last_tx_id(7).is_ok());
        drop(storage);

        let seen = replay_ops(path.as_path());
        assert_eq!(seen.len(), 1);
        assert!(matches!(&seen[0], Op::Insert { .. }));
        assert!(std::fs::remove_file(path).is_ok());
    }

    #[test]
    fn replay_discards_batch_missing_end() {
        let path = test_path("batch-missing-end");
        let storage = FileStorage::new(&path, FlushPolicy::Manual, 0);
        assert!(storage.is_ok());
        let mut storage = match storage {
            Ok(storage) => storage,
            Err(err) => panic!("storage open should succeed: {err}"),
        };

        assert!(storage
            .append(OpRef::BatchBegin {
                tx_id: 8,
                op_count: 1,
            })
            .is_ok());
        assert!(storage
            .append(OpRef::Insert {
                key: b"k",
                value: b"v",
                expires_at: None,
            })
            .is_ok());
        drop(storage);

        let seen = replay_ops(path.as_path());
        assert!(seen.is_empty());
        assert!(std::fs::remove_file(path).is_ok());
    }

    #[test]
    fn replay_discards_batch_with_mismatched_op_count() {
        let path = test_path("batch-op-count-mismatch");
        let storage = FileStorage::new(&path, FlushPolicy::Manual, 0);
        assert!(storage.is_ok());
        let mut storage = match storage {
            Ok(storage) => storage,
            Err(err) => panic!("storage open should succeed: {err}"),
        };

        assert!(storage
            .append(OpRef::BatchBegin {
                tx_id: 9,
                op_count: 2,
            })
            .is_ok());
        assert!(storage
            .append(OpRef::Insert {
                key: b"k",
                value: b"v",
                expires_at: None,
            })
            .is_ok());
        assert!(storage.append(OpRef::BatchEnd { tx_id: 9 }).is_ok());
        drop(storage);

        let seen = replay_ops(path.as_path());
        assert!(seen.is_empty());
        assert!(std::fs::remove_file(path).is_ok());
    }

    #[test]
    fn replay_discards_batch_with_mismatched_tx_id() {
        let path = test_path("batch-txid-mismatch");
        let storage = FileStorage::new(&path, FlushPolicy::Manual, 0);
        assert!(storage.is_ok());
        let mut storage = match storage {
            Ok(storage) => storage,
            Err(err) => panic!("storage open should succeed: {err}"),
        };

        assert!(storage
            .append(OpRef::BatchBegin {
                tx_id: 10,
                op_count: 1,
            })
            .is_ok());
        assert!(storage
            .append(OpRef::Insert {
                key: b"k",
                value: b"v",
                expires_at: None,
            })
            .is_ok());
        assert!(storage.append(OpRef::BatchEnd { tx_id: 11 }).is_ok());
        drop(storage);

        let seen = replay_ops(path.as_path());
        assert!(seen.is_empty());
        assert!(std::fs::remove_file(path).is_ok());
    }

    #[test]
    fn replay_discards_nested_batch_begin() {
        let path = test_path("batch-nested-begin");
        let storage = FileStorage::new(&path, FlushPolicy::Manual, 0);
        assert!(storage.is_ok());
        let mut storage = match storage {
            Ok(storage) => storage,
            Err(err) => panic!("storage open should succeed: {err}"),
        };

        assert!(storage
            .append(OpRef::BatchBegin {
                tx_id: 12,
                op_count: 1,
            })
            .is_ok());
        assert!(storage
            .append(OpRef::BatchBegin {
                tx_id: 13,
                op_count: 1,
            })
            .is_ok());
        drop(storage);

        let seen = replay_ops(path.as_path());
        assert!(seen.is_empty());
        assert!(std::fs::remove_file(path).is_ok());
    }

    #[test]
    fn replay_discards_batch_interrupted_by_eof() {
        let path = test_path("batch-eof");
        let storage = FileStorage::new(&path, FlushPolicy::Manual, 0);
        assert!(storage.is_ok());
        let mut storage = match storage {
            Ok(storage) => storage,
            Err(err) => panic!("storage open should succeed: {err}"),
        };

        assert!(storage
            .append(OpRef::BatchBegin {
                tx_id: 14,
                op_count: 1,
            })
            .is_ok());
        assert!(storage
            .append(OpRef::Insert {
                key: b"k",
                value: b"v",
                expires_at: None,
            })
            .is_ok());
        drop(storage);

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path);
        assert!(file.is_ok());
        let file = match file {
            Ok(file) => file,
            Err(err) => panic!("open should succeed: {err}"),
        };

        let len = file.metadata().map(|m| m.len());
        assert!(len.is_ok());
        let len = match len {
            Ok(len) => len,
            Err(err) => panic!("metadata should succeed: {err}"),
        };
        assert!(file.set_len(len - 2).is_ok());
        drop(file);

        let seen = replay_ops(path.as_path());
        assert!(seen.is_empty());
        assert!(std::fs::remove_file(path).is_ok());
    }

    #[test]
    fn every_n_zero_is_invalid() {
        let path = test_path("invalid-policy");
        let result = FileStorage::new(path, FlushPolicy::EveryN(0), 0);
        assert!(result.is_err());
    }
}
