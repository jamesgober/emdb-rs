#![no_main]
//! Meta sidecar decoder on raw bytes and on bytes with a valid magic,
//! version and CRC patched in. A decoded header must round-trip.
use libfuzzer_sys::fuzz_target;
fuzz_target!(|data: &[u8]| {
    // Raw input, plus a variant with a valid CRC patched in so the
    // fuzzer reaches the post-CRC field decoding.
    emdb::__fuzz::meta_decode(data);
    if data.len() >= 112 {
        let mut b = data.to_vec();
        b[..16].copy_from_slice(b"EMDB-META\0\0\0\0\0\0\0");
        b[16..20].copy_from_slice(&1u32.to_le_bytes());
        let crc = {
            let mut c = 0xFFFF_FFFFu32;
            for &x in &b[..108] {
                c ^= x as u32;
                for _ in 0..8 {
                    c = if c & 1 != 0 {
                        (c >> 1) ^ 0xEDB8_8320
                    } else {
                        c >> 1
                    };
                }
            }
            !c
        };
        b[108..112].copy_from_slice(&crc.to_le_bytes());
        emdb::__fuzz::meta_decode(&b);
    }
});
