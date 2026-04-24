// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! File-backed storage backend.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::storage::codec::{decode_op, encode_op, read_header, write_header, HEADER_LEN};
use crate::storage::{build_flags, FlushPolicy, Op, SnapshotIter, Storage, FORMAT_VERSION};
use crate::{Error, Result};

/// File-backed storage implementation.
#[derive(Debug)]
pub(crate) struct FileStorage {
    file: File,
    path: PathBuf,
    flags: u32,
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
        if meta_len == 0 {
            write_header(&mut file, flags)?;
            file.flush()?;
            file.sync_data()?;
        } else {
            let _offset = file.seek(SeekFrom::Start(0))?;
            let header = read_header(&mut file)?;

            if header.format_ver != FORMAT_VERSION {
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
        }

        let _offset = file.seek(SeekFrom::End(0))?;

        Ok(Self {
            file,
            path,
            flags: effective_flags,
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
        while cursor < bytes.len() {
            match decode_op(&bytes[cursor..]) {
                Ok((op, consumed)) => {
                    sink(op)?;
                    cursor = cursor.checked_add(consumed).ok_or(Error::Corrupted {
                        offset: cursor as u64,
                        reason: "cursor overflow",
                    })?;
                }
                Err(Error::Corrupted { reason, .. }) => {
                    let truncate_to = (HEADER_LEN + cursor) as u64;
                    self.file.set_len(truncate_to)?;
                    self.file.flush()?;
                    self.file.sync_data()?;
                    let _offset = self.file.seek(SeekFrom::End(0))?;
                    // Recovery is automatic; keep data up to last good record.
                    let _ignored_reason = reason;
                    return Ok(());
                }
                Err(other) => return Err(other),
            }
        }

        let _offset = self.file.seek(SeekFrom::End(0))?;
        Ok(())
    }

    fn atomic_replace_with_tmp(&mut self, tmp_path: &Path) -> Result<()> {
        std::fs::rename(tmp_path, &self.path)?;
        self.file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        let _offset = self.file.seek(SeekFrom::End(0))?;
        Ok(())
    }
}

impl Storage for FileStorage {
    fn append(&mut self, op: &Op) -> Result<()> {
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

        write_header(&mut tmp, self.flags)?;

        let mut count = 0_u32;
        for entry in snapshot {
            let mut record = Vec::new();
            encode_op(
                &mut record,
                &Op::Insert {
                    key: entry.key.to_vec(),
                    value: entry.value.to_vec(),
                    expires_at: entry.expires_at,
                },
            );
            tmp.write_all(&record)?;
            count = count.saturating_add(1);
        }

        let mut checkpoint = Vec::new();
        encode_op(
            &mut checkpoint,
            &Op::Checkpoint {
                record_count: count,
            },
        );
        tmp.write_all(&checkpoint)?;
        tmp.flush()?;
        tmp.sync_data()?;
        drop(tmp);

        self.atomic_replace_with_tmp(&tmp_path)
    }

    fn path(&self) -> Option<&Path> {
        Some(self.path.as_path())
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::file::FileStorage;
    use crate::storage::{FlushPolicy, Op, Storage};

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
            .append(&Op::Insert {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                expires_at: None,
            })
            .is_ok());
        assert!(storage.append(&Op::Remove { key: b"k".to_vec() }).is_ok());
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

    #[test]
    fn every_n_zero_is_invalid() {
        let path = test_path("invalid-policy");
        let result = FileStorage::new(path, FlushPolicy::EveryN(0), 0);
        assert!(result.is_err());
    }
}
