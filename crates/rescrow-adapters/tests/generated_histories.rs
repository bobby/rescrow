//! The same generated contract runs for every authority, without adapter mocks.
use rescrow_adapters::authority_memory::MemoryAuthority;
use rescrow_test_support::{Config, FileFailurePersistence, generated};
#[test]
fn memory_generated_histories() {
    let config = Config {
        failure_persistence: Some(Box::new(FileFailurePersistence::Direct(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/proptest-regressions/generated_histories.txt"
        )))),
        ..Config::default()
    };
    generated::run(MemoryAuthority::default, config);
}
