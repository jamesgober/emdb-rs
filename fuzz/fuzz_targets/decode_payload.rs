#![no_main]
//! Plaintext record decoder and frame-length payload slicing on
//! arbitrary bytes. Any panic is a finding.
use libfuzzer_sys::fuzz_target;
fuzz_target!(|data: &[u8]| {
    emdb::__fuzz::decode_payload(data);
    if data.len() >= 2 {
        let off = (data[0] as usize) % (data.len() + 8);
        emdb::__fuzz::payload_at(&data[1..], off);
    }
});
