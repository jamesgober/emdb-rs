#[test]
fn feature_compile_smoke() {
    let _db = emdb::Emdb::open_in_memory();
    let _policy = emdb::FlushPolicy::Manual;

    #[cfg(feature = "ttl")]
    {
        let _ttl = emdb::Ttl::Never;
    }

    #[cfg(feature = "nested")]
    {
        let mut db = emdb::Emdb::open_in_memory();
        let _focus = db.focus("scope");
    }
}
