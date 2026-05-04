// Integration tests for `get_zerocopy` and the lazy iterators added
// in v0.8. Coverage focuses on:
//
//   - Round-trip equivalence with the existing `get` API.
//   - Empty / missing / boundary inputs.
//   - Holding a `ValueRef` across subsequent writes (the file may
//     grow and the engine may swap its primary mmap; the returned
//     reference must keep its bytes alive).
//   - TTL-expired reads are filtered out by `get_zerocopy`.
//   - Lazy iter / keys snapshot semantics: records inserted after
//     the iterator was constructed are not visible.
//   - Lazy range_iter early-exit: dropping the iterator after a few
//     `next()` calls does not pay decode cost on the rest of the
//     range.
//
// Encrypted-database coverage lives separately in tests that
// require the `encrypt` feature.

use std::path::PathBuf;

use emdb::{Emdb, Result};

fn tmp_path(label: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |d| d.as_nanos());
    let tid = std::thread::current().id();
    p.push(format!("emdb-zerocopy-{label}-{nanos}-{tid:?}.emdb"));
    p
}

fn cleanup(path: &PathBuf) {
    let _ = std::fs::remove_file(path);
    let display = path.display();
    let _ = std::fs::remove_file(format!("{display}.lock"));
    let _ = std::fs::remove_file(format!("{display}.compact.tmp"));
}

#[test]
fn get_zerocopy_returns_same_bytes_as_get() -> Result<()> {
    let db = Emdb::open_in_memory();
    db.insert("alpha", "first")?;
    db.insert("beta", "second")?;
    db.insert("gamma", &[0xff_u8, 0x00, 0x10][..])?;

    for key in ["alpha", "beta", "gamma"] {
        let plain = db.get(key)?.expect("plain get");
        let zc = db.get_zerocopy(key)?.expect("zc get");
        assert_eq!(plain.as_slice(), zc.as_slice(), "{key}");
        assert_eq!(zc.len(), plain.len(), "{key}");
        assert!(!zc.is_empty(), "{key}");
        assert_eq!(zc.into_vec(), plain, "{key}");
    }
    Ok(())
}

#[test]
fn get_zerocopy_missing_key_returns_none() -> Result<()> {
    let db = Emdb::open_in_memory();
    db.insert("present", "x")?;
    assert!(db.get_zerocopy("absent")?.is_none());
    Ok(())
}

#[test]
fn get_zerocopy_empty_value_returns_empty_ref() -> Result<()> {
    let db = Emdb::open_in_memory();
    db.insert("empty", b"")?;
    let v = db.get_zerocopy("empty")?.expect("zc empty");
    assert!(v.is_empty());
    assert_eq!(v.len(), 0);
    assert_eq!(v.as_slice(), b"");
    Ok(())
}

#[test]
fn value_ref_survives_writer_growth() -> Result<()> {
    // The whole point of holding `Arc<Mmap>` inside `ValueRef` is
    // that subsequent file growth (which swaps the engine's
    // primary mmap) does not invalidate the reference. Force a
    // grow by writing enough bytes after the read to overflow the
    // initial 1 MiB capacity.
    let path = tmp_path("survives-growth");
    cleanup(&path);

    let db = Emdb::open(&path)?;
    db.insert("anchor", "I should still be readable after growth")?;
    db.flush()?;

    let anchor = db.get_zerocopy("anchor")?.expect("anchor read");

    // 4 KiB values × 300 = 1.2 MiB, comfortably past INITIAL_CAPACITY.
    let big_value = vec![b'x'; 4096];
    for i in 0_u32..300 {
        let key = format!("filler-{i:04}");
        db.insert(key.as_str(), big_value.as_slice())?;
    }
    db.flush()?;

    // The original reference must still read its bytes — the
    // `Arc<Mmap>` inside the ValueRef pinned the original mapping.
    assert_eq!(anchor.as_slice(), b"I should still be readable after growth");

    drop(anchor);
    drop(db);
    cleanup(&path);
    Ok(())
}

#[test]
fn get_zerocopy_filters_expired_records() -> Result<()> {
    use std::time::Duration;

    use emdb::Ttl;

    let db = Emdb::open_in_memory();
    db.insert_with_ttl("ephemeral", "gone", Ttl::After(Duration::from_millis(1)))?;
    // Sleep just past the TTL boundary. 50 ms gives wall-clock
    // drift on slow CI runners enough headroom.
    std::thread::sleep(Duration::from_millis(50));
    assert!(db.get_zerocopy("ephemeral")?.is_none());
    Ok(())
}

#[test]
fn iter_snapshots_at_construction_time() -> Result<()> {
    let db = Emdb::open_in_memory();
    db.insert("a", "1")?;
    db.insert("b", "2")?;

    let iter = db.iter()?;

    // Inserted after the snapshot — not visible.
    db.insert("c", "3")?;

    let mut seen: Vec<(Vec<u8>, Vec<u8>)> = iter.collect();
    seen.sort();

    assert_eq!(
        seen,
        vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec())
        ]
    );

    // The post-snapshot insert is visible if you take a fresh
    // iterator.
    let after: Vec<_> = db.iter()?.collect();
    assert_eq!(after.len(), 3);
    Ok(())
}

#[test]
fn keys_iter_lazy_decode_yields_all_keys() -> Result<()> {
    let db = Emdb::open_in_memory();
    for i in 0..50 {
        db.insert(format!("k{i:02}"), "v")?;
    }
    let mut keys: Vec<_> = db.keys()?.collect();
    keys.sort();
    assert_eq!(keys.len(), 50);
    assert_eq!(keys[0], b"k00".to_vec());
    assert_eq!(keys[49], b"k49".to_vec());
    Ok(())
}

#[test]
fn range_iter_early_exit_works() -> Result<()> {
    let db = Emdb::builder().enable_range_scans(true).build()?;
    for i in 0_u32..1_000 {
        db.insert(format!("user:{i:04}"), format!("data-{i}"))?;
    }

    // Early-exit after 5 elements — the lazy iterator should not
    // decode the remaining 995 records. We can't easily assert
    // "no work was done" but we can at least assert the take(5)
    // shape produces sorted, contiguous keys.
    let first_five: Vec<_> = db.range_iter(b"user:".to_vec()..b"user;".to_vec())?
        .take(5)
        .collect();
    assert_eq!(first_five.len(), 5);
    assert_eq!(first_five[0].0, b"user:0000");
    assert_eq!(first_five[4].0, b"user:0004");
    Ok(())
}

#[test]
fn range_prefix_iter_matches_range_prefix() -> Result<()> {
    let db = Emdb::builder().enable_range_scans(true).build()?;
    db.insert("session:a", "1")?;
    db.insert("session:b", "2")?;
    db.insert("session:c", "3")?;
    db.insert("user:a", "4")?;

    let from_iter: Vec<_> = db.range_prefix_iter("session:")?.collect();
    let from_eager = db.range_prefix("session:")?;
    assert_eq!(from_iter, from_eager);
    assert_eq!(from_iter.len(), 3);
    Ok(())
}

#[test]
fn range_iter_without_enable_range_scans_returns_invalid_config() -> Result<()> {
    use emdb::Error;

    let db = Emdb::open_in_memory();
    let result = db.range_iter(b"a".to_vec()..b"z".to_vec());
    assert!(matches!(result, Err(Error::InvalidConfig(_))));
    Ok(())
}
