use emdb::{Emdb, FlushPolicy, Result};

#[cfg(feature = "ttl")]
use std::time::Duration;

fn tmp_path(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |d| d.as_nanos());
    p.push(format!("emdb-{name}-{nanos}.emdb"));
    p
}

#[test]
fn persistence_round_trip_reopen_restores_values() -> Result<()> {
    let path = tmp_path("persist-roundtrip");

    {
        let mut db = Emdb::open(&path)?;
        for i in 0_u32..64 {
            db.insert(format!("k{i}"), format!("v{i}"))?;
        }
        db.flush()?;
    }

    let db = Emdb::open(&path)?;
    for i in 0_u32..64 {
        assert_eq!(db.get(format!("k{i}"))?, Some(format!("v{i}").into_bytes()));
    }

    assert!(std::fs::remove_file(path).is_ok());
    Ok(())
}

#[cfg(feature = "ttl")]
#[test]
fn ttl_persistence_retains_remaining_lifetime() -> Result<()> {
    use emdb::Ttl;

    let path = tmp_path("persist-ttl");

    {
        let mut db = Emdb::builder()
            .path(path.clone())
            .flush_policy(FlushPolicy::OnEachWrite)
            .build()?;

        db.insert_with_ttl("session", "token", Ttl::After(Duration::from_secs(2)))?;
        db.flush()?;
    }

    let db = Emdb::open(&path)?;
    let remaining = db.ttl("session")?;
    assert!(remaining.is_some());

    let ttl = remaining.unwrap_or(Duration::ZERO);
    assert!(ttl > Duration::from_millis(1));
    assert!(ttl <= Duration::from_secs(2));

    assert!(std::fs::remove_file(path).is_ok());
    Ok(())
}

#[test]
fn flush_policy_variants_are_usable() -> Result<()> {
    let base = tmp_path("flush-policies");

    let policies = [
        FlushPolicy::OnEachWrite,
        FlushPolicy::EveryN(2),
        FlushPolicy::Manual,
    ];

    for (idx, policy) in policies.into_iter().enumerate() {
        let mut path = base.clone();
        path.set_extension(format!("{idx}.emdb"));

        {
            let mut db = Emdb::builder()
                .path(path.clone())
                .flush_policy(policy)
                .build()?;
            db.insert("k", "v")?;
            db.flush()?;
        }

        let db = Emdb::open(&path)?;
        assert_eq!(db.get("k")?, Some(b"v".to_vec()));
        assert!(std::fs::remove_file(path).is_ok());
    }

    Ok(())
}
