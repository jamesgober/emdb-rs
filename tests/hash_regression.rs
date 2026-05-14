// Copyright 2026 James Gober. Licensed under Apache-2.0.
//
// Regression test for the v0.9.6 hash function fix.
//
// **The bug v0.9.6 fixed:** the index hash function was FxHash, which
// has a known weakness on structured byte inputs. On the exact stress
// pattern this test uses (`"stress-key-{idx:08}"` over 0..64_000),
// FxHash produced **22 956 collisions over 64 000 distinct keys —
// a 36 % collision rate**. The collision storm saturated the
// overflow-handling path and surfaced concurrency edges there that
// caused silent post-insert misses on Linux multi-thread runs.
//
// **The fix:** `Index::hash_key` was rewritten as a wyhash-style
// two-prime mixer with a Murmur3 fmix64 finalizer. Standalone analysis
// on the same 64 000-key pattern produces 0 collisions (birthday-bound).
//
// **What this test guards:** if the hash function ever regresses —
// either back to FxHash, or to any other mixer that produces a
// collision storm on this specific structured-key pattern — the
// reads at the end of this test will start returning `None` for keys
// that were just inserted.
//
// Single-threaded by design. The v0.9.6 bug only *manifested* under
// multi-threaded contention (concurrency edges in the overflow path),
// but the root cause was the hash distribution itself, which is a
// deterministic property of the function. A single-threaded
// insert+verify catches a regressed hash function without needing
// CI-flake-prone multi-threaded reproduction.
//
// See `.dev/release/v0.9.6.md` for the full diagnosis trail.

use emdb::Emdb;

const STRESS_KEY_COUNT: usize = 64_000;

fn stress_key(i: usize) -> String {
    format!("stress-key-{i:08}")
}

#[test]
fn v0_9_6_hash_function_handles_stress_pattern_without_misses() {
    let db = Emdb::open_in_memory();

    // Insert every stress key with its index serialised as the value
    // (so verification catches not just presence but identity).
    for i in 0..STRESS_KEY_COUNT {
        let key = stress_key(i);
        let value = format!("v{i}");
        db.insert(key.as_bytes(), value.as_bytes())
            .expect("insert should always succeed on a healthy hash");
    }
    assert_eq!(db.len().expect("len"), STRESS_KEY_COUNT);

    // Read every key back. Under the v0.9.4/v0.9.5 hash, this is the
    // step that returned `None` for thousands of keys on multi-thread
    // runs because the index returned a colliding writer's offset and
    // the decode-side key-verify rejected the mismatch. Single-thread
    // shouldn't manifest the multi-thread race per se, but the *root
    // cause* (collision rate) is hash-distribution-only and would
    // still cause `None` returns if the in-shard probe sequence
    // resolves to a collided slot before the OVERFLOW promotion.
    let mut missing: Vec<String> = Vec::new();
    let mut wrong_value: Vec<String> = Vec::new();
    for i in 0..STRESS_KEY_COUNT {
        let key = stress_key(i);
        let expected = format!("v{i}");
        match db.get(key.as_bytes()).expect("get must not error") {
            None => missing.push(key),
            Some(v) if v != expected.as_bytes() => wrong_value.push(key),
            Some(_) => {}
        }
    }

    assert!(
        missing.is_empty(),
        "{} keys went missing post-insert (regressed hash function?). \
         First few: {:?}",
        missing.len(),
        &missing[..missing.len().min(5)]
    );
    assert!(
        wrong_value.is_empty(),
        "{} keys returned the wrong value (hash collision unhandled?). \
         First few: {:?}",
        wrong_value.len(),
        &wrong_value[..wrong_value.len().min(5)]
    );
}

#[test]
fn v0_9_6_hash_function_distinct_offsets_for_distinct_stress_keys() {
    // Companion test: the v0.9.6 fix was specifically that the new
    // hash produces 0 collisions on this pattern. We can't reach the
    // private `Index::hash_key` from an integration test, but we can
    // assert the *behavioural* consequence: every record overwrites
    // its own previous version, and double-inserts replace cleanly.
    let db = Emdb::open_in_memory();

    // First pass: insert.
    for i in 0..STRESS_KEY_COUNT {
        db.insert(stress_key(i).as_bytes(), b"v1")
            .expect("insert v1");
    }
    assert_eq!(db.len().expect("len after v1"), STRESS_KEY_COUNT);

    // Second pass: overwrite every key with a new value. If the hash
    // function collided records together, the overwrites would
    // overwrite the *wrong* records and the live-count would
    // collapse.
    for i in 0..STRESS_KEY_COUNT {
        db.insert(stress_key(i).as_bytes(), b"v2")
            .expect("insert v2");
    }
    assert_eq!(
        db.len().expect("len after v2"),
        STRESS_KEY_COUNT,
        "overwriting every stress key must keep the live count constant"
    );

    // Spot-check the overwrite landed on the right key.
    for i in [0_usize, 1, 100, 1_000, 32_000, 63_999] {
        assert_eq!(
            db.get(stress_key(i).as_bytes()).expect("get").as_deref(),
            Some(b"v2".as_slice()),
            "stress-key-{i:08} should be v2 after overwrite"
        );
    }
}
