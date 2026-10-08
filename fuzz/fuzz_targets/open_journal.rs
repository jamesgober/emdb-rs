#![no_main]
//! End-to-end: the input is a sequence of [flags u8][len u16][payload]
//! records. The harness frames each with a valid fsys header + CRC-32C
//! (optionally AEAD-encrypting the body under the fixed key), writes the
//! journal, then opens the database (plain or encrypted), and exercises
//! the read surface. Any panic is a finding.
use libfuzzer_sys::fuzz_target;
use std::sync::atomic::{AtomicU64, Ordering};

static N: AtomicU64 = AtomicU64::new(0);

fn frame(out: &mut Vec<u8>, payload: &[u8]) {
    let start = out.len();
    out.extend_from_slice(&0x4653_5901u32.to_be_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(payload);
    let crc = crc32c::crc32c(&out[start..]);
    out.extend_from_slice(&crc.to_le_bytes());
}

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    let encrypted_db = data[0] & 1 == 1;
    let mut i = 1;
    let mut journal = Vec::new();
    while i + 3 <= data.len() {
        let flags = data[i];
        let len = u16::from_le_bytes([data[i + 1], data[i + 2]]) as usize;
        i += 3;
        let end = (i + len).min(data.len());
        let body = &data[i..end];
        i = end;
        if flags & 0x80 != 0 && !body.is_empty() {
            // body[0] = tag kind; rest = plaintext to encrypt
            let ct = emdb::__fuzz::encrypt_fixed(&body[1..]);
            let mut p = vec![body[0] | 0x80];
            p.extend_from_slice(&ct);
            frame(&mut journal, &p);
        } else {
            // Raw bytes in a CRC-valid frame.
            frame(&mut journal, body);
        }
    }
    if data[0] & 2 == 2 && !journal.is_empty() {
        // trailing torn bytes
        let cut = (data[0] as usize) % journal.len();
        journal.truncate(journal.len() - cut / 2);
    }
    let n = N.fetch_add(1, Ordering::Relaxed);
    let shm = std::path::Path::new("/dev/shm");
    let base = if shm.is_dir() {
        shm.to_path_buf()
    } else {
        std::env::temp_dir()
    };
    let dir = base.join("emdb-fuzz");
    let _ = std::fs::create_dir_all(&dir);
    let p = dir.join(format!("j-{}-{}.emdb", std::process::id(), n % 4));
    for s in ["", ".meta", ".lock", ".lock-meta"] {
        let _ = std::fs::remove_file(format!("{}{}", p.display(), s));
    }
    for e in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
        if e.file_name().to_string_lossy().contains(".corrupt-") {
            let _ = std::fs::remove_file(e.path());
        }
    }
    std::fs::write(&p, &journal).unwrap();
    let mut b = emdb::Emdb::builder()
        .path(&p)
        .enable_range_scans(data[0] & 4 == 4);
    if encrypted_db {
        b = b.encryption_key([7u8; 32]);
    }
    if let Ok(db) = b.build() {
        let _ = db.len();
        let keys: Vec<Vec<u8>> = db.keys().map(|k| k.collect()).unwrap_or_default();
        for k in keys.iter().take(16) {
            let _ = db.get(k);
            let _ = db.get_zerocopy(k);
            let _ = db.expires_at(k);
        }
        if let Ok(names) = db.list_namespaces() {
            for name in names.iter().filter(|n| !n.is_empty()).take(8) {
                if let Ok(ns) = db.namespace(name) {
                    let _ = ns.iter().map(|it| it.count());
                    let _ = ns.len();
                }
            }
        }
        let _ = db.range_prefix(b"");
        let _ = db.stats();
        let _ = db.sweep_expired();
        let _ = db.insert(b"fuzz-after".to_vec(), b"v".to_vec());
        let _ = db.flush();
    }
});
