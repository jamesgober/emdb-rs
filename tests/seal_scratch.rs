// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! The plaintext body of an encrypted record is built in a scratch
//! buffer that is wiped before the write returns. The buffer must be
//! allocated at its final size: if it grew by reallocation, the
//! smaller block freed on the way would still hold the start of the
//! body (the key, or the whole value).
//!
//! A counting global allocator inspects every block freed while one
//! write runs. It counts blocks smaller than the write's record body
//! that hold an encoded field of that body: a little-endian length
//! followed by the marker the key or value starts with. The caller's
//! own key and value buffers carry no length prefix, so they are
//! never counted.

#![cfg(feature = "encrypt")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use emdb::{Emdb, Result};

const MARKER: &[u8] = b"PLAINTEXT-MARKER";

struct Spy;

static ARMED: AtomicBool = AtomicBool::new(false);
/// Size of the largest record body the current write encodes.
static BODY_LEN: AtomicUsize = AtomicUsize::new(0);
static LEAKED: AtomicUsize = AtomicUsize::new(0);

/// `true` when `block` holds `[len as u32 LE][MARKER...]` with a
/// plausible field length.
fn holds_encoded_field(block: &[u8]) -> bool {
    block.windows(4 + MARKER.len()).any(|w| {
        let len = u32::from_le_bytes([w[0], w[1], w[2], w[3]]) as usize;
        &w[4..] == MARKER && (MARKER.len()..1 << 20).contains(&len)
    })
}

// SAFETY: every call is forwarded to `System` with the same arguments;
// `dealloc` only reads the block it is about to free.
unsafe impl GlobalAlloc for Spy {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded unchanged; the caller upholds `alloc`'s
        // contract.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if ARMED.load(Ordering::Relaxed) && layout.size() < BODY_LEN.load(Ordering::Relaxed) {
            // SAFETY: `ptr` is a live block of `layout.size()` bytes
            // (the caller's contract for `dealloc`); it is read
            // before it is released.
            let block = unsafe { std::slice::from_raw_parts(ptr, layout.size()) };
            if holds_encoded_field(block) {
                let _ = LEAKED.fetch_add(1, Ordering::Relaxed);
            }
        }
        // SAFETY: forwarded unchanged.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: Spy = Spy;

/// Run one write whose largest record body is `body_len` bytes and
/// return the number of freed blocks that held part of it.
fn leaks_during(body_len: usize, f: impl FnOnce() -> Result<()>) -> Result<usize> {
    let _ = LEAKED.swap(0, Ordering::SeqCst);
    BODY_LEN.store(body_len, Ordering::SeqCst);
    ARMED.store(true, Ordering::SeqCst);
    let out = f();
    ARMED.store(false, Ordering::SeqCst);
    out?;
    Ok(LEAKED.swap(0, Ordering::SeqCst))
}

/// Body of an insert record: ns, key len, key, value len, value, expiry.
fn insert_body(key: &[u8], value: &[u8]) -> usize {
    4 + 4 + key.len() + 4 + value.len() + 8
}

fn marked(len: usize, fill: u8, tag: usize) -> Vec<u8> {
    let mut out = MARKER.to_vec();
    out.extend_from_slice(format!("{tag:04}").as_bytes());
    out.resize(len.max(out.len()), fill);
    out
}

#[test]
fn test_encrypted_writes_leave_no_plaintext_in_freed_blocks() -> Result<()> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |d| d.as_nanos());
    let mut dir = std::env::temp_dir();
    dir.push(format!("emdb-seal-{}-{nanos}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create dir");
    let db = Emdb::builder()
        .path(dir.join("db.emdb"))
        .encryption_key([7_u8; 32])
        .build()?;
    let ns = db.namespace("names")?;

    // Keys and values around the sizes where a growing buffer
    // reallocates.
    let mut tag = 0_usize;
    let mut cases: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for value_len in [20_usize, 40, 51, 52, 64, 100, 300, 5000] {
        tag += 1;
        cases.push((format!("k{tag}").into_bytes(), marked(value_len, b'.', tag)));
    }
    for (key_len, value_len) in [(48_usize, 300_usize), (48, 3), (70, 70), (200, 10)] {
        tag += 1;
        cases.push((marked(key_len, b'#', tag), vec![b'v'; value_len]));
    }

    let mut leaks = Vec::new();
    for (key, value) in &cases {
        let body = insert_body(key, value);
        let n = leaks_during(body, || db.insert(key.as_slice(), value.as_slice()))?;
        leaks.push(("insert", key.len(), value.len(), n));
        let n = leaks_during(body, || ns.insert(key.as_slice(), value.as_slice()))?;
        leaks.push(("namespace insert", key.len(), value.len(), n));
    }
    for (key, _) in cases.iter().filter(|(k, _)| k.starts_with(MARKER)) {
        let n = leaks_during(8 + key.len(), || db.remove(key.as_slice()).map(|_| ()))?;
        leaks.push(("remove", key.len(), 0, n));
    }

    let batch: Vec<(Vec<u8>, Vec<u8>)> = cases
        .iter()
        .map(|(k, v)| {
            let mut k = k.clone();
            k.extend_from_slice(b"-batch");
            (k, v.clone())
        })
        .collect();
    let largest = batch
        .iter()
        .map(|(k, v)| insert_body(k, v))
        .max()
        .unwrap_or(0);
    let n = leaks_during(largest, || db.insert_many(batch))?;
    leaks.push(("insert_many", 0, 0, n));

    tag += 1;
    let tx_key = marked(90, b'%', tag);
    let tx_value = vec![b'w'; 70];
    let n = leaks_during(insert_body(&tx_key, &tx_value), || {
        db.transaction(|tx| {
            tx.insert(tx_key.as_slice(), tx_value.as_slice())?;
            Ok(())
        })
    })?;
    leaks.push(("transaction", tx_key.len(), tx_value.len(), n));

    tag += 1;
    let name = String::from_utf8(marked(80, b'n', tag)).expect("utf8");
    let n = leaks_during(8 + name.len(), || db.namespace(name.as_str()).map(|_| ()))?;
    leaks.push(("namespace name", name.len(), 0, n));

    let bad: Vec<_> = leaks.iter().filter(|l| l.3 > 0).collect();
    assert!(
        bad.is_empty(),
        "freed blocks still holding plaintext (op, key len, value len, blocks): {bad:?}"
    );
    drop(ns);
    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
