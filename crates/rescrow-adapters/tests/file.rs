#![cfg(target_os = "linux")]
use rescrow_adapters::file::{FileStore, MAX_SNAPSHOT_BYTES, Stage};
use rescrow_application::{Application, Category, Publication, SnapshotStore, StoreError, dto::*};
use std::{
    fs::{self, File},
    io,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::PathBuf,
    process::Command as Process,
    sync::atomic::{AtomicU64, Ordering},
    thread,
    time::{Duration, Instant},
};
static SEQ: AtomicU64 = AtomicU64::new(0);
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        let p = std::env::var_os("RESCROW_TEST_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir)
            .join(format!(
                "rescrow-file-{}-{}",
                std::process::id(),
                SEQ.fetch_add(1, Ordering::Relaxed)
            ));
        fs::create_dir(&p).unwrap();
        Self(p)
    }
    fn store(&self) -> PathBuf {
        self.0.join("store")
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn create() -> rescrow_application::dto::Command {
    rescrow_application::dto::Command::CreateResource(ResourceSpec {
        resource_id: "r".into(),
        lower: "0".into(),
        upper: Some("10".into()),
        initial: "5".into(),
        unit: "u".into(),
        scale: 0,
    })
}
fn prepare() -> rescrow_application::dto::Command {
    rescrow_application::dto::Command::Prepare(Prepare {
        transaction_id: "t".into(),
        operations: vec![Operation {
            operation_id: "o".into(),
            resource_id: "r".into(),
            delta: "-3".into(),
        }],
    })
}
fn ready(temp: &Temp) -> FileStore {
    let store = FileStore::new(temp.store()).unwrap();
    let app = Application::new(FileStore::new(temp.store()).unwrap());
    app.initialize().unwrap();
    app.execute(create()).unwrap();
    store
}
#[test]
fn initialize_permissions_missing_files_and_stable_lock_inode() {
    let temp = Temp::new();
    let store = FileStore::new(temp.store()).unwrap();
    assert_eq!(store.load().unwrap_err(), StoreError::Missing);
    let app = Application::new(FileStore::new(temp.store()).unwrap());
    app.initialize().unwrap();
    let lock = fs::metadata(temp.store().join("state.lock")).unwrap().ino();
    assert_eq!(
        fs::metadata(temp.store()).unwrap().permissions().mode() & 0o777,
        0o700
    );
    for name in ["state.json", "state.lock"] {
        assert_eq!(
            fs::metadata(temp.store().join(name))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
    assert_eq!(
        app.initialize().unwrap_err().code,
        rescrow_application::Code::AlreadyInitialized
    );
    app.execute(create()).unwrap();
    app.execute(prepare()).unwrap();
    assert_eq!(
        fs::metadata(temp.store().join("state.lock")).unwrap().ino(),
        lock
    );
    fs::remove_file(temp.store().join("state.json")).unwrap();
    fs::write(temp.store().join(".state-orphan.tmp"), b"not a snapshot").unwrap();
    assert_eq!(store.load().unwrap_err(), StoreError::Missing);
    assert!(temp.store().join(".state-orphan.tmp").exists());
    assert_eq!(
        app.initialize().unwrap_err().code,
        rescrow_application::Code::MissingStore
    );
    assert!(!temp.store().join("state.json").exists());
}
#[test]
fn stale_revision_corrupt_unsupported_truncated_and_oversized_fail_closed() {
    let temp = Temp::new();
    let store = ready(&temp);
    let old = store.load().unwrap().to_dto();
    Application::new(FileStore::new(temp.store()).unwrap())
        .execute(prepare())
        .unwrap();
    assert_eq!(
        store
            .replace(
                old.revision,
                &Snapshot {
                    revision: old.revision + 1,
                    ..old.clone()
                }
                .try_into()
                .unwrap()
            )
            .unwrap(),
        Publication::Conflict
    );
    let good = store.load().unwrap().to_dto();
    for bytes in [b"{".to_vec(), b"{\"schema_version\":99}".to_vec(), {
        let mut bad = good.clone();
        bad.resources[0].quantity = "6".into();
        serde_json::to_vec(&bad).unwrap()
    }] {
        fs::write(temp.store().join("state.json"), &bytes).unwrap();
        assert!(matches!(
            store.load(),
            Err(StoreError::Corrupt(_) | StoreError::Unsupported(99))
        ));
        assert_eq!(fs::read(temp.store().join("state.json")).unwrap(), bytes);
    }
    let file = File::options()
        .write(true)
        .open(temp.store().join("state.json"))
        .unwrap();
    file.set_len(MAX_SNAPSHOT_BYTES + 1).unwrap();
    assert_eq!(store.load().unwrap_err(), StoreError::TooLarge);
    fs::write(
        temp.store().join("state.json"),
        serde_json::to_vec(&good).unwrap(),
    )
    .unwrap();
    assert_eq!(store.load().unwrap().to_dto(), good);
    let bad_revision = Snapshot {
        revision: good.revision,
        ..good.clone()
    };
    assert!(matches!(
        store.replace(good.revision, &bad_revision.try_into().unwrap()),
        Err(StoreError::Corrupt(_))
    ));
}
#[test]
fn contention_has_a_deadline_and_permission_errors_preserve_state() {
    let temp = Temp::new();
    let store = ready(&temp).with_lock_timeout(Duration::from_millis(30));
    let lock = File::options()
        .read(true)
        .write(true)
        .open(temp.store().join("state.lock"))
        .unwrap();
    lock.lock().unwrap();
    let now = Instant::now();
    assert_eq!(store.load().unwrap_err(), StoreError::Contention);
    assert!(now.elapsed() < Duration::from_secs(1));
    assert_eq!(
        store
            .replace(
                1,
                &Snapshot {
                    revision: 2,
                    ..Snapshot::from_ledger(&Default::default(), 1)
                }
                .try_into()
                .unwrap()
            )
            .unwrap_err(),
        StoreError::Contention
    );
    drop(lock);
    let good = store.load().unwrap().to_dto();
    fs::set_permissions(
        temp.store().join("state.json"),
        fs::Permissions::from_mode(0o644),
    )
    .unwrap();
    assert!(matches!(store.load(), Err(StoreError::Io(_))));
    fs::set_permissions(
        temp.store().join("state.json"),
        fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    // No write permission on the directory causes an actual pre-publication I/O
    // failure for ordinary users. Root bypasses DAC and is tested separately below.
    fs::set_permissions(temp.store(), fs::Permissions::from_mode(0o500)).unwrap();
    let attempt = Application::new(FileStore::new(temp.store()).unwrap()).execute(prepare());
    fs::set_permissions(temp.store(), fs::Permissions::from_mode(0o700)).unwrap();
    if attempt.is_err() {
        assert_eq!(store.load().unwrap().to_dto(), good);
    } else {
        assert_eq!(std::env::var("USER").as_deref(), Ok("root"));
    }
}
fn hook(stage: Stage) -> io::Result<()> {
    let target = std::env::var("RESCROW_TEST_STAGE").unwrap_or_default();
    if format!("{stage:?}") != target {
        return Ok(());
    }
    if std::env::var("RESCROW_TEST_MODE").as_deref() == Ok("kill") {
        fs::write(std::env::var("RESCROW_TEST_MARKER").unwrap(), b"reached").unwrap();
        loop {
            thread::park();
        }
    }
    Err(io::Error::other("injected storage failure"))
}
#[test]
fn child_worker() {
    let Ok(path) = std::env::var("RESCROW_TEST_STORE") else {
        return;
    };
    let app = Application::new(FileStore::new(path).unwrap().with_fault_hook(hook));
    let result = if std::env::var("RESCROW_TEST_ACTION").as_deref() == Ok("init") {
        app.initialize().map(|()| Output::Initialized)
    } else {
        app.execute(prepare())
    };
    let expected = std::env::var("RESCROW_TEST_EXPECT").unwrap();
    match expected.as_str() {
        "unknown" => assert_eq!(result.unwrap_err().category, Category::UnknownOutcome),
        "io" => assert_eq!(result.unwrap_err().code, rescrow_application::Code::Io),
        _ => panic!("invalid expectation"),
    }
}
fn child(temp: &Temp, stage: Stage, mode: &str) -> std::process::Child {
    let post = matches!(
        stage,
        Stage::Renamed | Stage::DirectorySync | Stage::BeforeResponse | Stage::ReadSync
    );
    Process::new(std::env::current_exe().unwrap())
        .args(["--exact", "child_worker", "--nocapture"])
        .env("RESCROW_TEST_STORE", temp.store())
        .env("RESCROW_TEST_STAGE", format!("{stage:?}"))
        .env("RESCROW_TEST_MODE", mode)
        .env("RESCROW_TEST_MARKER", temp.0.join("reached"))
        .env("RESCROW_TEST_EXPECT", if post { "unknown" } else { "io" })
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap()
}
#[test]
fn kill_at_publication_boundaries_exposes_complete_old_or_new_state() {
    for stage in [
        Stage::BeforeWrite,
        Stage::DuringWrite,
        Stage::FileSynced,
        Stage::Renamed,
        Stage::BeforeResponse,
    ] {
        let temp = Temp::new();
        let store = ready(&temp);
        let old = store.load().unwrap().to_dto();
        let mut child = child(&temp, stage, "kill");
        let deadline = Instant::now() + Duration::from_secs(10);
        while !temp.0.join("reached").exists() {
            if let Some(status) = child.try_wait().unwrap() {
                panic!("child exited early: {status}");
            }
            if Instant::now() > deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("checkpoint timeout at {stage:?}");
            }
            thread::sleep(Duration::from_millis(5));
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let next = store.load().unwrap().to_dto(); // also proves the OS released the killed owner's lock
        if matches!(stage, Stage::Renamed | Stage::BeforeResponse) {
            assert_eq!(next.transactions.len(), 1);
            assert_eq!(next.resources[0].reserved_down, "3");
        } else {
            assert_eq!(next, old);
        }
        let app = Application::new(FileStore::new(temp.store()).unwrap());
        app.execute(prepare()).unwrap();
        app.execute(rescrow_application::dto::Command::Commit(Commit {
            transaction_id: "t".into(),
            actuals: None,
        }))
        .unwrap();
        let committed = store.load().unwrap().to_dto();
        app.execute(rescrow_application::dto::Command::Commit(Commit {
            transaction_id: "t".into(),
            actuals: None,
        }))
        .unwrap();
        assert_eq!(store.load().unwrap().to_dto(), committed);
        assert_eq!(committed.resources[0].quantity, "2");
    }
}
#[test]
fn injected_io_and_directory_sync_failures_preserve_replay_identity() {
    for stage in [
        Stage::BeforeWrite,
        Stage::DuringWrite,
        Stage::FileSynced,
        Stage::Renamed,
        Stage::DirectorySync,
        Stage::BeforeResponse,
        Stage::ReadSync,
    ] {
        let temp = Temp::new();
        let store = ready(&temp);
        let old = store.load().unwrap().to_dto();
        assert!(child(&temp, stage, "error").wait().unwrap().success());
        let next = store.load().unwrap().to_dto();
        if matches!(
            stage,
            Stage::Renamed | Stage::DirectorySync | Stage::BeforeResponse
        ) {
            assert_eq!(next.transactions.len(), 1);
        } else {
            assert_eq!(next, old);
        }
        Application::new(FileStore::new(temp.store()).unwrap())
            .execute(prepare())
            .unwrap();
        assert_eq!(
            store.load().unwrap().to_dto().resources[0].reserved_down,
            "3"
        );
    }
}

#[test]
fn initialization_failures_never_reset_a_missing_or_published_authority() {
    for stage in [
        Stage::BeforeWrite,
        Stage::FileSynced,
        Stage::Renamed,
        Stage::DirectorySync,
        Stage::BeforeResponse,
    ] {
        let temp = Temp::new();
        let mut process = Process::new(std::env::current_exe().unwrap());
        let post = matches!(
            stage,
            Stage::Renamed | Stage::DirectorySync | Stage::BeforeResponse
        );
        let status = process
            .args(["--exact", "child_worker", "--nocapture"])
            .env("RESCROW_TEST_STORE", temp.store())
            .env("RESCROW_TEST_ACTION", "init")
            .env("RESCROW_TEST_STAGE", format!("{stage:?}"))
            .env("RESCROW_TEST_MODE", "error")
            .env("RESCROW_TEST_EXPECT", if post { "unknown" } else { "io" })
            .stdout(std::process::Stdio::null())
            .status()
            .unwrap();
        assert!(status.success());
        let store = FileStore::new(temp.store()).unwrap();
        let app = Application::new(FileStore::new(temp.store()).unwrap());
        if post {
            let snapshot = store.load().unwrap().to_dto();
            assert_eq!(snapshot.revision, 0);
            assert!(snapshot.resources.is_empty() && snapshot.transactions.is_empty());
            assert_eq!(
                app.initialize().unwrap_err().code,
                rescrow_application::Code::AlreadyInitialized
            );
            assert_eq!(store.load().unwrap().to_dto(), snapshot);
        } else {
            assert_eq!(store.load().unwrap_err(), StoreError::Missing);
            assert_eq!(
                app.initialize().unwrap_err().code,
                rescrow_application::Code::MissingStore
            );
        }
    }
}

#[test]
fn readers_share_the_lock_and_exclude_publishers() {
    let temp = Temp::new();
    let store = ready(&temp).with_lock_timeout(Duration::from_millis(30));
    let lock = File::options()
        .read(true)
        .write(true)
        .open(temp.store().join("state.lock"))
        .unwrap();
    lock.lock_shared().unwrap();
    let current = store.load().unwrap().to_dto();
    assert_eq!(
        store
            .replace(
                current.revision,
                &Snapshot {
                    revision: current.revision + 1,
                    ..current.clone()
                }
                .try_into()
                .unwrap()
            )
            .unwrap_err(),
        StoreError::Contention
    );
    drop(lock);
    assert_eq!(store.load().unwrap().to_dto(), current);
}
