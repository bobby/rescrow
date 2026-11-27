//! Adding an authority needs its port implementation and a test file like this.
use rescrow_adapters::{authority_memory::MemoryAuthority, file::FileStore};
use rescrow_test_support::{Config, FileFailurePersistence, concurrent, generated};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        let parent = std::env::var_os("RESCROW_TEST_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/conformance")
            });
        let root = parent.join(format!(
            "suite-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        Self(root)
    }
    fn store(&self) -> FileStore {
        FileStore::new(
            self.0
                .join(format!("store-{}", NEXT.fetch_add(1, Ordering::Relaxed))),
        )
        .unwrap()
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }
}
#[test]
fn memory_concurrent_clients() {
    let scratch = Scratch::new();
    concurrent::run(MemoryAuthority::default, &scratch.0);
}
#[test]
fn file_concurrent_clients() {
    let scratch = Scratch::new();
    concurrent::run(|| scratch.store(), &scratch.0);
}
#[test]
fn file_generated_histories() {
    let scratch = Scratch::new();
    let defaults = Config::default();
    let cases = if std::env::var_os("PROPTEST_CASES").is_some() {
        defaults.cases
    } else {
        32
    };
    let config = Config {
        cases,
        failure_persistence: Some(Box::new(FileFailurePersistence::Direct(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/proptest-regressions/file_generated_histories.txt"
        )))),
        ..defaults
    };
    generated::run(|| scratch.store(), config);
}

#[test]
fn declared_memory_capabilities_and_tenant_isolation() {
    use rescrow_application::authority::TransactionalStore;
    use rescrow_test_support::qualification::{self, Hooks};
    let store = MemoryAuthority::default();
    store.initialize().unwrap();
    qualification::run(
        &store,
        Hooks {
            cross_process: None,
            crash_reopen: None,
        },
    );
    qualification::tenancy(MemoryAuthority::default());
}

#[test]
fn file_readiness_probes_the_store_and_reports_actual_local_profile() {
    use rescrow_application::{
        Application,
        capabilities::{DurabilityProfile, Readiness},
        dto,
        transactional::ReadyApplication,
    };
    let scratch = Scratch::new();
    let store = scratch.store();
    assert!(store.readiness().is_err());
    Application::new(store.clone()).initialize().unwrap();
    assert!(
        ReadyApplication::start(
            Application::new(store.clone()),
            DurabilityProfile::EnterpriseMr
        )
        .is_err()
    );
    let ready =
        ReadyApplication::start(Application::new(store.clone()), DurabilityProfile::Local).unwrap();
    let response = ready
        .execute(dto::Command::ListResources, DurabilityProfile::Local)
        .unwrap();
    assert_eq!(response.durability_profile, DurabilityProfile::Local);
    assert!(ready.capabilities().cross_process);
    assert_eq!(
        ready.capabilities().max_record_bytes,
        Some(rescrow_adapters::file::MAX_RECORD_BYTES)
    );
    std::fs::remove_file(store.directory().join("live.json")).unwrap();
    assert!(ReadyApplication::start(Application::new(store), DurabilityProfile::Local).is_err());
}
