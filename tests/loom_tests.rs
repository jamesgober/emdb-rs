#![allow(unexpected_cfgs)]

#[cfg(loom)]
mod loom_like {
    use std::sync::Arc;
    use std::thread;

    use emdb::{Emdb, Error, Result};

    #[test]
    fn lock_order_no_deadlock_between_state_and_storage_paths() -> Result<()> {
        let db = Arc::new(Emdb::open_in_memory());

        let writer_db = Arc::clone(&db);
        let writer = thread::spawn(move || -> Result<()> {
            for i in 0_u32..1_000 {
                writer_db.insert(format!("k{i}"), format!("v{i}"))?;
                if i % 100 == 0 {
                    writer_db.flush()?;
                }
            }
            Ok(())
        });

        let tx_db = Arc::clone(&db);
        let tx = thread::spawn(move || -> Result<()> {
            for i in 0_u32..250 {
                tx_db.transaction(|txn| {
                    txn.insert(format!("tx:{i}"), format!("v{i}"))?;
                    Ok(())
                })?;
            }
            Ok(())
        });

        let reader_db = Arc::clone(&db);
        let reader = thread::spawn(move || -> Result<()> {
            for i in 0_u32..5_000 {
                let _value = reader_db.get(format!("k{}", i % 1000))?;
            }
            Ok(())
        });

        match writer.join() {
            Ok(inner) => inner?,
            Err(_panic) => return Err(Error::TransactionAborted("loom writer panicked")),
        }
        match tx.join() {
            Ok(inner) => inner?,
            Err(_panic) => return Err(Error::TransactionAborted("loom tx panicked")),
        }
        match reader.join() {
            Ok(inner) => inner?,
            Err(_panic) => return Err(Error::TransactionAborted("loom reader panicked")),
        }

        Ok(())
    }
}

#[cfg(not(loom))]
#[test]
fn loom_cfg_not_enabled() {
    let enabled = cfg!(loom);
    assert!(!enabled);
}
