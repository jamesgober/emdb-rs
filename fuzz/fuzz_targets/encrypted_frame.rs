#![no_main]
//! Encrypted record decoder: raw bytes (the AEAD must reject them) and
//! bodies encrypted under a fixed key so the post-AEAD decoders run on
//! arbitrary plaintext. Any panic is a finding.
use libfuzzer_sys::fuzz_target;
fuzz_target!(|data: &[u8]| {
    // 1) raw bytes straight into the encrypted decoder (AEAD should reject).
    emdb::__fuzz::decode_encrypted_with_fixed_key(data);
    // 2) structure-aware: first byte = tag, rest = plaintext body that we
    //    encrypt under the fixed key so the post-AEAD body decoders run.
    if let Some((&tag, body)) = data.split_first() {
        let ct = emdb::__fuzz::encrypt_fixed(body);
        let mut payload = vec![tag | 0x80];
        payload.extend_from_slice(&ct);
        emdb::__fuzz::decode_encrypted_with_fixed_key(&payload);
    }
});
