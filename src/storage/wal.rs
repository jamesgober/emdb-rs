// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Separate write-ahead log used by the page-format engine.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::storage::codec::{decode_op, encode_op};
use crate::storage::{Op, OpRef};
use crate::{Error, Result};

/// Userspace WAL write buffer size. Sized to amortize per-op syscall overhead
/// while remaining small enough not to dwarf typical key/value records.
const WAL_BUFFER_BYTES: usize = 64 * 1024;

#[derive(Debug)]
struct OpenBatch {
    tx_id: u64,
    expected_count: u32,
    buffered_ops: Vec<Op>,
    start_cursor: usize,
}

/// Write-ahead log for page-backed storage.
///
/// Writes go through a userspace `BufWriter` so a burst of `append` calls
/// becomes one syscall per buffer flush instead of one per record. Reads
/// (`replay`, `is_empty`, `truncate`) drain the buffer first.
#[derive(Debug)]
pub(crate) struct Wal {
    writer: BufWriter<File>,
    path: PathBuf,
    /// Reused encode buffer to keep `append` allocation-free in steady state.
    scratch: Vec<u8>,
    /// Bytes appended since the last on-disk flush.
    ///
    /// Tracked in userspace so [`Wal::is_empty`] reflects unflushed records
    /// without forcing a buffer drain just to ask the filesystem.
    pending: u64,
}

impl Wal {
    /// Open or create the WAL file at the provided path.
    pub(crate) fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        let _offset = file.seek(SeekFrom::End(0))?;
        let writer = BufWriter::with_capacity(WAL_BUFFER_BYTES, file);
        Ok(Self {
            writer,
            path,
            scratch: Vec::with_capacity(256),
            pending: 0,
        })
    }

    /// Derive the WAL path from a page file path.
    pub(crate) fn path_for(page_path: &Path) -> PathBuf {
        let mut wal_name = page_path
            .file_name()
            .and_then(|name| name.to_str())
            .map_or_else(|| String::from("emdb.wal"), |name| format!("{name}.wal"));
        if wal_name.is_empty() {
            wal_name = String::from("emdb.wal");
        }

        let mut wal_path = page_path.to_path_buf();
        wal_path.set_file_name(wal_name);
        wal_path
    }

    /// Append one logical operation through the userspace buffer.
    pub(crate) fn append(&mut self, op: OpRef<'_>) -> Result<()> {
        self.scratch.clear();
        encode_op(&mut self.scratch, op);
        self.writer.write_all(&self.scratch)?;
        self.pending = self.pending.saturating_add(self.scratch.len() as u64);
        Ok(())
    }

    /// Flush and fsync the WAL: drain the userspace buffer, then `sync_data`.
    pub(crate) fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_mut().sync_data()?;
        self.pending = 0;
        Ok(())
    }

    /// Return true when the WAL contains no records, including unflushed.
    pub(crate) fn is_empty(&self) -> Result<bool> {
        if self.pending != 0 {
            return Ok(false);
        }
        Ok(self.writer.get_ref().metadata()?.len() == 0)
    }

    /// Replay all durable records, truncating a torn or incomplete tail if needed.
    pub(crate) fn replay(&mut self, sink: &mut dyn FnMut(Op) -> Result<()>) -> Result<()> {
        self.replay_impl(sink)
    }

    /// Replay durable records and truncate the WAL to zero bytes after success.
    pub(crate) fn apply_and_truncate(
        &mut self,
        sink: &mut dyn FnMut(Op) -> Result<()>,
    ) -> Result<()> {
        self.replay_impl(sink)?;
        self.truncate()
    }

    /// Truncate the WAL to zero bytes.
    pub(crate) fn truncate(&mut self) -> Result<()> {
        self.writer.flush()?;
        let file = self.writer.get_mut();
        file.set_len(0)?;
        let _offset = file.seek(SeekFrom::Start(0))?;
        self.pending = 0;
        self.flush()
    }

    /// Return the on-disk WAL path.
    #[must_use]
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    fn replay_impl(&mut self, sink: &mut dyn FnMut(Op) -> Result<()>) -> Result<()> {
        // Drain the userspace buffer so the file reflects every appended op.
        self.writer.flush()?;
        let file = self.writer.get_mut();
        let _offset = file.seek(SeekFrom::Start(0))?;
        let mut bytes = Vec::new();
        let _read = file.read_to_end(&mut bytes)?;

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
                    self.truncate_to(truncate_cursor)?;
                    return Ok(());
                }
                Err(other) => return Err(other),
            }
        }

        if let Some(batch) = open_batch {
            self.truncate_to(batch.start_cursor)?;
            return Ok(());
        }

        let _offset = self.writer.get_mut().seek(SeekFrom::End(0))?;
        Ok(())
    }

    fn truncate_to(&mut self, cursor: usize) -> Result<()> {
        let file = self.writer.get_mut();
        file.set_len(cursor as u64)?;
        let _offset = file.seek(SeekFrom::Start(cursor as u64))?;
        self.pending = 0;
        self.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::Wal;
    use crate::storage::codec::encode_op;
    use crate::storage::OpRef;

    fn test_path(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0_u128, |duration| duration.as_nanos());
        path.push(format!("emdb-wal-{name}-{nanos}.emdb.wal"));
        path
    }

    #[test]
    fn test_append_and_replay_round_trip() {
        let path = test_path("replay");
        let wal = Wal::open(&path);
        assert!(wal.is_ok());
        let mut wal = match wal {
            Ok(wal) => wal,
            Err(err) => panic!("wal open should succeed: {err}"),
        };

        assert!(wal
            .append(OpRef::Insert {
                key: b"a",
                value: b"1",
                expires_at: None,
            })
            .is_ok());
        assert!(wal.append(OpRef::Remove { key: b"a" }).is_ok());
        assert!(wal.flush().is_ok());

        let mut seen = Vec::new();
        let replayed = wal.replay(&mut |op| {
            seen.push(op);
            Ok(())
        });
        assert!(replayed.is_ok());
        assert_eq!(seen.len(), 2);

        let _removed = std::fs::remove_file(path);
    }

    #[test]
    fn test_apply_and_truncate_clears_file() {
        let path = test_path("truncate");
        let wal = Wal::open(&path);
        assert!(wal.is_ok());
        let mut wal = match wal {
            Ok(wal) => wal,
            Err(err) => panic!("wal open should succeed: {err}"),
        };

        assert!(wal.append(OpRef::Clear).is_ok());
        assert!(wal.flush().is_ok());

        let mut seen = 0_usize;
        let applied = wal.apply_and_truncate(&mut |_op| {
            seen += 1;
            Ok(())
        });
        assert!(applied.is_ok());
        assert_eq!(seen, 1);
        let wal_len = std::fs::metadata(&path).map(|meta| meta.len());
        assert!(wal_len.is_ok());
        assert_eq!(wal_len.unwrap_or(u64::MAX), 0);

        let _removed = std::fs::remove_file(path);
    }

    #[test]
    fn test_torn_tail_is_truncated_on_replay() {
        let path = test_path("torn");
        let wal = Wal::open(&path);
        assert!(wal.is_ok());
        let mut wal = match wal {
            Ok(wal) => wal,
            Err(err) => panic!("wal open should succeed: {err}"),
        };

        let mut record = Vec::new();
        encode_op(
            &mut record,
            OpRef::Insert {
                key: b"k",
                value: b"v",
                expires_at: None,
            },
        );
        assert!(std::fs::write(&path, &record[..record.len() - 3]).is_ok());

        let replayed = wal.replay(&mut |_op| Ok(()));
        assert!(replayed.is_ok());
        let wal_len = std::fs::metadata(&path).map(|meta| meta.len());
        assert!(wal_len.is_ok());
        assert_eq!(wal_len.unwrap_or(u64::MAX), 0);

        let _removed = std::fs::remove_file(path);
    }

    #[test]
    fn test_incomplete_batch_is_truncated_without_applying() {
        let path = test_path("batch");
        let wal = Wal::open(&path);
        assert!(wal.is_ok());
        let mut wal = match wal {
            Ok(wal) => wal,
            Err(err) => panic!("wal open should succeed: {err}"),
        };

        assert!(wal
            .append(OpRef::BatchBegin {
                tx_id: 7,
                op_count: 1,
            })
            .is_ok());
        assert!(wal
            .append(OpRef::Insert {
                key: b"k",
                value: b"v",
                expires_at: None,
            })
            .is_ok());
        assert!(wal.flush().is_ok());

        let mut seen = 0_usize;
        let replayed = wal.replay(&mut |_op| {
            seen += 1;
            Ok(())
        });
        assert!(replayed.is_ok());
        assert_eq!(seen, 0);
        let wal_len = std::fs::metadata(&path).map(|meta| meta.len());
        assert!(wal_len.is_ok());
        assert_eq!(wal_len.unwrap_or(u64::MAX), 0);

        let _removed = std::fs::remove_file(path);
    }
}
