use std::collections::BTreeSet;
use std::sync::Arc;
use std::thread;

use emdb::{Emdb, Error, Result};

fn tmp_path(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |d| d.as_nanos());
    p.push(format!("emdb-concurrency-{name}-{nanos}.emdb"));
    p
}

fn read_header_last_tx_id(path: &std::path::Path) -> Result<u64> {
    let bytes = std::fs::read(path)?;
    let mut arr = [0_u8; 8];
    arr.copy_from_slice(&bytes[32..40]);
    Ok(u64::from_le_bytes(arr))
}

#[test]
fn many_readers_one_writer_no_deadlock_and_correct_state() -> Result<()> {
    let db = Arc::new(Emdb::open_in_memory());

    for i in 0_u32..100 {
        db.insert(format!("seed:{i}"), format!("v{i}"))?;
    }

    let writer_db = Arc::clone(&db);
    let writer = thread::spawn(move || -> Result<()> {
        for i in 0_u32..1_000 {
            writer_db.insert(format!("w:{i}"), format!("{i}"))?;
        }
        Ok(())
    });

    let mut readers = Vec::new();
    for _ in 0_u32..10 {
        let reader_db = Arc::clone(&db);
        readers.push(thread::spawn(move || -> Result<()> {
            let mut hits = 0_usize;
            for i in 0_u32..10_000 {
                let key = format!("seed:{}", i % 100);
                if reader_db.get(key)?.is_some() {
                    hits += 1;
                }
            }
            if hits == 0 {
                return Err(Error::TransactionAborted("reader observed no visible keys"));
            }
            Ok(())
        }));
    }

    let writer_result = writer.join();
    match writer_result {
        Ok(inner) => inner?,
        Err(_panic) => return Err(Error::TransactionAborted("writer thread panicked")),
    }

    for handle in readers {
        let join_result = handle.join();
        match join_result {
            Ok(inner) => inner?,
            Err(_panic) => return Err(Error::TransactionAborted("reader thread panicked")),
        }
    }

    assert_eq!(db.len()?, 1_100);
    Ok(())
}

#[test]
fn concurrent_transactions_are_serialized_with_monotonic_tx_ids() -> Result<()> {
    let path = tmp_path("tx-serialized");
    let db = Arc::new(Emdb::open(&path)?);

    let mut workers = Vec::new();
    for t in 0_u32..2 {
        let db = Arc::clone(&db);
        workers.push(thread::spawn(move || -> Result<()> {
            for i in 0_u32..100 {
                db.transaction(|tx| {
                    tx.insert(format!("t{t}:{i}"), format!("v{i}"))?;
                    Ok(())
                })?;
            }
            Ok(())
        }));
    }

    for worker in workers {
        let join_result = worker.join();
        match join_result {
            Ok(inner) => inner?,
            Err(_panic) => return Err(Error::TransactionAborted("transaction worker panicked")),
        }
    }

    assert_eq!(db.len()?, 200);
    assert_eq!(read_header_last_tx_id(path.as_path())?, 200);

    assert!(std::fs::remove_file(path).is_ok());
    Ok(())
}

#[test]
fn lockfile_contention_and_release_work() -> Result<()> {
    let path = tmp_path("lockfile-contention");

    let first = Emdb::open(&path)?;
    let second = Emdb::open(&path);
    if second.is_ok() {
        return Err(Error::TransactionAborted(
            "second open unexpectedly succeeded",
        ));
    }

    drop(first);

    let reopened = Emdb::open(&path)?;
    drop(reopened);

    let mut lock_path = path.as_os_str().to_owned();
    lock_path.push(".lock");
    let lock_path = std::path::PathBuf::from(lock_path);

    let _removed_db = std::fs::remove_file(path);
    let _removed_lock = std::fs::remove_file(lock_path);
    Ok(())
}

#[test]
fn lockfile_released_on_panic() -> Result<()> {
    let path = tmp_path("lockfile-panic");

    let unwind = std::panic::catch_unwind({
        let path = path.clone();
        move || {
            let opened = Emdb::open(&path);
            match opened {
                Ok(db) => {
                    let _keep_alive = db;
                }
                Err(err) => panic!("unexpected open error before panic path: {err}"),
            }
            panic!("intentional panic to test lockfile drop");
        }
    });
    assert!(unwind.is_err());

    let reopened = Emdb::open(&path)?;
    drop(reopened);

    let mut lock_path = path.as_os_str().to_owned();
    lock_path.push(".lock");
    let lock_path = std::path::PathBuf::from(lock_path);

    let _removed_db = std::fs::remove_file(path);
    let _removed_lock = std::fs::remove_file(lock_path);
    Ok(())
}

#[test]
fn clone_handle_across_threads_preserves_correctness() -> Result<()> {
    let db = Emdb::open_in_memory();

    let mut workers = Vec::new();
    for i in 0_u32..4 {
        let db = db.clone_handle();
        workers.push(thread::spawn(move || -> Result<()> {
            for j in 0_u32..250 {
                db.insert(format!("k{i}:{j}"), format!("v{j}"))?;
            }
            Ok(())
        }));
    }

    for worker in workers {
        let join_result = worker.join();
        match join_result {
            Ok(inner) => inner?,
            Err(_panic) => return Err(Error::TransactionAborted("clone worker panicked")),
        }
    }

    assert_eq!(db.len()?, 1_000);

    let mut keys = BTreeSet::new();
    for (k, _v) in db.iter()? {
        let _inserted = keys.insert(k);
    }
    assert_eq!(keys.len(), 1_000);
    Ok(())
}
