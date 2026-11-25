#![cfg(target_os = "linux")]
use rescrow_adapters::file::{FileStore, MAX_RECORD_BYTES, Stage};
use rescrow_application::{Application, Category, Code, LogStore, Publication, StoreError, dto::*};
use std::{
    fs::{self, File},
    io::{self, Write},
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::PathBuf,
    process::Command as Process,
    sync::atomic::{AtomicU64, Ordering},
    thread,
    time::{Duration, Instant},
};
static SEQ: AtomicU64 = AtomicU64::new(0);
/// The first log segment, active until it is sealed.
const SEGMENT: &str = "log/00000000000000000000.seg";
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
    fn app(&self) -> Application<FileStore> {
        Application::new(FileStore::new(self.store()).unwrap())
    }
    fn snapshot(&self) -> Snapshot {
        self.app().snapshot().unwrap()
    }
    fn append_bytes(&self, bytes: &[u8]) {
        File::options()
            .append(true)
            .open(self.store().join(SEGMENT))
            .unwrap()
            .write_all(bytes)
            .unwrap();
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
/// Every request in this process is created at its start, or at its parent's
/// for a child worker, so retries match.
fn created() -> u64 {
    static CREATED: std::sync::LazyLock<u64> = std::sync::LazyLock::new(|| {
        std::env::var("RESCROW_TEST_CREATED")
            .map(|v| v.parse().unwrap())
            .unwrap_or_else(|_| rescrow_application::system_clock())
    });
    *CREATED
}
fn create() -> Command {
    Command::CreateResource(ResourceSpec {
        resource_id: "r".into(),
        lower: "0".into(),
        upper: Some("10".into()),
        initial: "5".into(),
        unit: "u".into(),
        scale: 0,
    })
}
/// The UUIDv7 a test names `name`, created at `millis`.
fn tx_at(name: &str, millis: u64) -> String {
    let hash = name.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    });
    rescrow_application::uuid_v7(millis, hash.into())
}
/// The UUIDv7 a test names `name`, created when the test process started.
fn tx(name: &str) -> String {
    tx_at(name, created())
}
/// The transaction a test named `name`.
fn named(snapshot: &Snapshot, name: &str) -> Transaction {
    let id = tx(name);
    snapshot
        .transactions
        .iter()
        .find(|t| t.request.transaction_id == id)
        .unwrap()
        .clone()
}
fn prepare_as(name: &str) -> Command {
    prepare_raw(&tx(name))
}
fn prepare_raw(id: &str) -> Command {
    Command::Prepare(Prepare {
        transaction_id: id.into(),
        operations: vec![Operation {
            operation_id: "o".into(),
            resource_id: "r".into(),
            delta: "-3".into(),
        }],
    })
}
fn prepare() -> Command {
    prepare_as("t")
}
fn commit() -> Command {
    Command::Commit(Commit {
        transaction_id: tx("t"),
        actuals: None,
    })
}
fn ready(temp: &Temp) -> FileStore {
    let app = temp.app();
    app.initialize().unwrap();
    app.execute(create()).unwrap();
    FileStore::new(temp.store()).unwrap()
}
#[test]
fn initialize_permissions_missing_files_and_stable_lock_inode() {
    let temp = Temp::new();
    let store = FileStore::new(temp.store()).unwrap();
    assert_eq!(store.load().unwrap_err(), StoreError::Missing);
    let app = temp.app();
    app.initialize().unwrap();
    let lock = fs::metadata(temp.store().join("state.lock")).unwrap().ino();
    assert_eq!(
        fs::metadata(temp.store()).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(temp.store().join("log"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    for name in ["live.json", SEGMENT, "state.lock"] {
        assert_eq!(
            fs::metadata(temp.store().join(name))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
    assert_eq!(app.initialize().unwrap_err().code, Code::AlreadyInitialized);
    app.execute(create()).unwrap();
    app.execute(prepare()).unwrap();
    assert_eq!(
        fs::metadata(temp.store().join("state.lock")).unwrap().ino(),
        lock
    );
    fs::remove_file(temp.store().join("live.json")).unwrap();
    fs::write(temp.store().join(".live-orphan.tmp"), b"not a checkpoint").unwrap();
    assert_eq!(store.load().unwrap_err(), StoreError::Missing);
    assert!(temp.store().join(".live-orphan.tmp").exists());
    assert_eq!(app.initialize().unwrap_err().code, Code::MissingStore);
    assert!(!temp.store().join("live.json").exists());
}
#[test]
fn stale_position_conflicts_and_invalid_live_state_fails_closed() {
    let temp = Temp::new();
    let store = ready(&temp);
    let old = store.load().unwrap();
    temp.app().execute(prepare()).unwrap();
    let stale = Entry {
        sequence: old.checkpoint.position + old.tail.len() as u64,
        recorded_at_ms: rescrow_application::system_clock(),
        record: Record::ResourceCreated(ResourceSpec {
            resource_id: "other".into(),
            lower: "0".into(),
            upper: None,
            initial: "0".into(),
            unit: "u".into(),
            scale: 0,
        }),
    };
    assert_eq!(
        store.append(std::slice::from_ref(&stale)).unwrap(),
        Publication::Conflict
    );
    let good = temp.snapshot();
    let live = fs::read(temp.store().join("live.json")).unwrap();
    for bytes in [b"{".to_vec(), b"{\"log_offset\":999999,\"checkpoint\":{\"schema_version\":2,\"position\":0,\"resources\":[],\"prepared\":[]}}".to_vec()] {
        fs::write(temp.store().join("live.json"), &bytes).unwrap();
        assert!(matches!(store.load(), Err(StoreError::Corrupt(_))));
        assert_eq!(fs::read(temp.store().join("live.json")).unwrap(), bytes);
    }
    let unsupported = String::from_utf8(live.clone())
        .unwrap()
        .replace("\"schema_version\":2", "\"schema_version\":99");
    fs::write(temp.store().join("live.json"), unsupported).unwrap();
    assert_eq!(
        temp.app().execute(Command::ListResources).unwrap_err().code,
        Code::UnsupportedSchema
    );
    File::options()
        .write(true)
        .open(temp.store().join("live.json"))
        .unwrap()
        .set_len(MAX_RECORD_BYTES + 1)
        .unwrap();
    assert_eq!(store.load().unwrap_err(), StoreError::TooLarge);
    fs::write(temp.store().join("live.json"), &live).unwrap();
    assert_eq!(temp.snapshot(), good);
    // A schema-1 store is recognized rather than treated as missing.
    fs::rename(
        temp.store().join("live.json"),
        temp.store().join("state.json"),
    )
    .unwrap();
    assert_eq!(store.load().unwrap_err(), StoreError::Unsupported(1));
    assert_eq!(
        temp.app().initialize().unwrap_err().code,
        Code::UnsupportedSchema
    );
}
#[test]
fn torn_appends_are_ignored_and_truncated_but_earlier_corruption_fails_closed() {
    let temp = Temp::new();
    ready(&temp);
    let before = temp.snapshot();
    // A partial record: header claims more bytes than were written.
    let mut torn = 100u32.to_le_bytes().to_vec();
    torn.extend_from_slice(&[0, 0, 0, 0, b'{', b'"']);
    temp.append_bytes(&torn);
    assert_eq!(temp.snapshot(), before);
    temp.app().execute(prepare()).unwrap();
    // A zero-filled extension, as after a crash before data reached the device.
    temp.append_bytes(&[0; 32]);
    assert_eq!(temp.snapshot().resources[0].reserved_down, "3");
    temp.app().execute(commit()).unwrap();
    let committed = temp.snapshot();
    assert_eq!(committed.resources[0].quantity, "2");
    assert_eq!(committed.revision, 3);
    // Corruption inside an acknowledged record followed by later records is not a torn append.
    let path = temp.store().join(SEGMENT);
    let mut bytes = fs::read(&path).unwrap();
    bytes[12] ^= 0x20;
    fs::write(&path, &bytes).unwrap();
    assert_eq!(
        temp.app().execute(Command::ListResources).unwrap_err().code,
        Code::CorruptState
    );
}
#[test]
fn checkpoints_bound_replay_and_agree_with_history() {
    let temp = Temp::new();
    ready(&temp);
    let app = Application::new(FileStore::new(temp.store()).unwrap()).with_checkpoint_interval(2);
    for index in 0..5 {
        let id = format!("t{index}");
        app.execute(prepare_as(&id)).unwrap();
        app.execute(Command::Abort {
            transaction_id: tx(&id),
        })
        .unwrap();
    }
    let loaded = FileStore::new(temp.store()).unwrap().load().unwrap();
    assert!(loaded.checkpoint.position >= 9);
    assert!(loaded.tail.len() < 2);
    let snapshot = temp.snapshot();
    assert_eq!(snapshot.revision, 11);
    assert_eq!(snapshot.transactions.len(), 5);
    // Replays of transactions older than the checkpoint still find their decision.
    assert_eq!(
        app.execute(Command::Abort {
            transaction_id: tx("t0")
        })
        .unwrap(),
        Output::Transaction(named(&snapshot, "t0"))
    );
    assert_eq!(temp.snapshot(), snapshot);
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
    // Readers take no lock, so a writer holding it never delays them.
    store.load().unwrap();
    let now = Instant::now();
    let entry = Entry {
        sequence: 1,
        recorded_at_ms: rescrow_application::system_clock(),
        record: Record::ResourceCreated(ResourceSpec {
            resource_id: "other".into(),
            lower: "0".into(),
            upper: None,
            initial: "0".into(),
            unit: "u".into(),
            scale: 0,
        }),
    };
    assert_eq!(
        store.append(std::slice::from_ref(&entry)).unwrap_err(),
        StoreError::Contention
    );
    assert!(now.elapsed() < Duration::from_secs(1));
    drop(lock);
    let good = temp.snapshot();
    for name in ["live.json", SEGMENT] {
        let path = temp.store().join(name);
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(store.load(), Err(StoreError::Io(_))));
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    }
    // A read-only log causes an actual pre-append I/O failure for ordinary users.
    // Root bypasses DAC, so the attempt may then succeed.
    let log = temp.store().join(SEGMENT);
    fs::set_permissions(&log, fs::Permissions::from_mode(0o400)).unwrap();
    let attempt = temp.app().execute(prepare());
    fs::set_permissions(&log, fs::Permissions::from_mode(0o600)).unwrap();
    if attempt.is_err() {
        assert_eq!(temp.snapshot(), good);
    } else {
        let status = fs::read_to_string("/proc/self/status").unwrap();
        assert!(
            status.lines().any(|l| l.starts_with("Uid:\t0\t")),
            "only root may bypass a read-only log"
        );
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
    let mut store = FileStore::new(path).unwrap().with_fault_hook(hook);
    if let Ok(bytes) = std::env::var("RESCROW_TEST_SEGMENT") {
        store = store.with_segment_size(bytes.parse().unwrap());
    }
    let mut app = Application::new(store);
    if std::env::var("RESCROW_TEST_CHECKPOINT").is_ok() {
        app = app.with_checkpoint_interval(1);
    }
    let result = match std::env::var("RESCROW_TEST_ACTION").as_deref() {
        Ok("init") => app.initialize().map(|()| Output::Initialized),
        Ok("migrate") => {
            let snapshot = FileStore::new(std::env::var("RESCROW_TEST_STORE").unwrap())
                .unwrap()
                .legacy()
                .unwrap();
            app.import(snapshot).map(|()| Output::Migrated)
        }
        _ => app.execute(prepare()),
    };
    let expected = std::env::var("RESCROW_TEST_EXPECT").unwrap();
    match expected.as_str() {
        "unknown" => assert_eq!(result.unwrap_err().category, Category::UnknownOutcome),
        "io" => assert_eq!(result.unwrap_err().code, Code::Io),
        "ok" => {
            result.unwrap();
        }
        _ => panic!("invalid expectation"),
    }
}
/// Stages after which an append may have taken effect.
fn appended(stage: Stage) -> bool {
    matches!(stage, Stage::AppendSynced | Stage::BeforeResponse)
}
fn child(temp: &Temp, stage: Stage, mode: &str, expect: &str) -> std::process::Child {
    let mut process = Process::new(std::env::current_exe().unwrap());
    process
        .args(["--exact", "child_worker", "--nocapture"])
        .env("RESCROW_TEST_STORE", temp.store())
        .env("RESCROW_TEST_STAGE", format!("{stage:?}"))
        .env("RESCROW_TEST_MODE", mode)
        .env("RESCROW_TEST_MARKER", temp.0.join("reached"))
        .env("RESCROW_TEST_EXPECT", expect)
        .env("RESCROW_TEST_CREATED", created().to_string())
        .stdout(std::process::Stdio::null());
    if expect == "ok" {
        process.env("RESCROW_TEST_CHECKPOINT", "1");
    }
    process.spawn().unwrap()
}
fn kill_at(temp: &Temp, stage: Stage, expect: &str) {
    kill_with(temp, stage, expect, child(temp, stage, "kill", expect));
}
fn kill_with(temp: &Temp, stage: Stage, _expect: &str, mut child: std::process::Child) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !temp.0.join("reached").exists() {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("child exited early: {status}");
        }
        if Instant::now() > deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("stage timeout at {stage:?}");
        }
        thread::sleep(Duration::from_millis(5));
    }
    child.kill().unwrap();
    child.wait().unwrap();
}
/// After a failure, the same and later commands complete exactly once.
fn finish(temp: &Temp) {
    let app = temp.app();
    app.execute(prepare()).unwrap();
    app.execute(commit()).unwrap();
    let committed = temp.snapshot();
    app.execute(commit()).unwrap();
    assert_eq!(temp.snapshot(), committed);
    assert_eq!(committed.resources[0].quantity, "2");
    assert_eq!(committed.revision, 3);
}
#[test]
fn kill_at_append_boundaries_exposes_old_or_new_log() {
    for stage in [
        Stage::BeforeAppend,
        Stage::DuringAppend,
        Stage::AppendSynced,
        Stage::BeforeResponse,
    ] {
        let temp = Temp::new();
        ready(&temp);
        let old = temp.snapshot();
        let expect = if appended(stage) { "unknown" } else { "io" };
        kill_at(&temp, stage, expect);
        // Loading also proves the OS released the killed owner's lock.
        let next = temp.snapshot();
        if appended(stage) {
            assert_eq!(next.transactions.len(), 1);
            assert_eq!(next.resources[0].reserved_down, "3");
        } else {
            assert_eq!(next, old);
        }
        finish(&temp);
    }
}
#[test]
fn injected_append_and_read_failures_preserve_replay_identity() {
    for stage in [
        Stage::BeforeAppend,
        Stage::DuringAppend,
        Stage::AppendSynced,
        Stage::BeforeResponse,
        Stage::ReadSync,
    ] {
        let temp = Temp::new();
        ready(&temp);
        let old = temp.snapshot();
        let expect = if appended(stage) || stage == Stage::ReadSync {
            "unknown"
        } else {
            "io"
        };
        assert!(
            child(&temp, stage, "error", expect)
                .wait()
                .unwrap()
                .success()
        );
        let next = temp.snapshot();
        if appended(stage) {
            assert_eq!(next.transactions.len(), 1);
        } else {
            assert_eq!(next, old);
        }
        finish(&temp);
    }
}
#[test]
fn checkpoint_failures_never_lose_or_repeat_appended_records() {
    for stage in [
        Stage::BeforeWrite,
        Stage::DuringWrite,
        Stage::FileSynced,
        Stage::Renamed,
        Stage::DirectorySync,
    ] {
        for mode in ["error", "kill"] {
            let temp = Temp::new();
            ready(&temp);
            if mode == "kill" {
                kill_at(&temp, stage, "ok");
            } else {
                assert!(child(&temp, stage, mode, "ok").wait().unwrap().success());
            }
            let next = temp.snapshot();
            assert_eq!(next.transactions.len(), 1, "{stage:?} {mode}");
            assert_eq!(next.revision, 2);
            finish(&temp);
        }
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
        let post = matches!(
            stage,
            Stage::Renamed | Stage::DirectorySync | Stage::BeforeResponse
        );
        let status = Process::new(std::env::current_exe().unwrap())
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
        let app = temp.app();
        if post {
            let snapshot = temp.snapshot();
            assert_eq!(snapshot.revision, 0);
            assert!(snapshot.resources.is_empty() && snapshot.transactions.is_empty());
            assert_eq!(app.initialize().unwrap_err().code, Code::AlreadyInitialized);
            assert_eq!(temp.snapshot(), snapshot);
        } else {
            assert_eq!(store.load().unwrap_err(), StoreError::Missing);
            assert_eq!(app.initialize().unwrap_err().code, Code::MissingStore);
        }
    }
}
/// Readers hammering the store once starved writers, which wait for the lock
/// only while no reader holds it. Readers now take no lock.
#[test]
fn concurrent_readers_do_not_starve_writers() {
    let temp = Temp::new();
    ready(&temp);
    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let readers: Vec<_> = (0..16)
        .map(|_| {
            let (path, done) = (temp.store(), done.clone());
            std::thread::spawn(move || {
                let app = Application::new(FileStore::new(path).unwrap());
                while !done.load(Ordering::Relaxed) {
                    app.resource("r").unwrap();
                }
            })
        })
        .collect();
    let writers: Vec<_> = (0..4)
        .map(|w| {
            let path = temp.store();
            std::thread::spawn(move || {
                let app = Application::new(
                    FileStore::new(path)
                        .unwrap()
                        .with_lock_timeout(Duration::from_millis(500)),
                );
                for n in 0..10 {
                    // Four concurrent holds of one fit the resource's five.
                    let id = tx(&format!("w{w}-{n}"));
                    let Command::Prepare(mut request) = prepare_raw(&id) else {
                        unreachable!()
                    };
                    request.operations[0].delta = "-1".into();
                    app.execute(Command::Prepare(request)).unwrap();
                    app.execute(Command::Abort { transaction_id: id }).unwrap();
                }
            })
        })
        .collect();
    for writer in writers {
        writer.join().unwrap();
    }
    done.store(true, Ordering::Relaxed);
    for reader in readers {
        reader.join().unwrap();
    }
    let snapshot = temp.snapshot();
    assert_eq!(snapshot.transactions.len(), 40);
    assert_eq!(snapshot.resources[0].quantity, "5");
}
/// The locked fallback of a read that found apparent damage still excludes writers.
#[test]
fn readers_share_the_lock_and_exclude_writers() {
    let temp = Temp::new();
    let store = ready(&temp).with_lock_timeout(Duration::from_millis(30));
    let lock = File::options()
        .read(true)
        .write(true)
        .open(temp.store().join("state.lock"))
        .unwrap();
    lock.lock_shared().unwrap();
    let current = store.load().unwrap();
    let entry = Entry {
        sequence: current.checkpoint.position + current.tail.len() as u64,
        recorded_at_ms: rescrow_application::system_clock(),
        record: Record::ResourceCreated(ResourceSpec {
            resource_id: "other".into(),
            lower: "0".into(),
            upper: None,
            initial: "0".into(),
            unit: "u".into(),
            scale: 0,
        }),
    };
    assert_eq!(
        store.append(std::slice::from_ref(&entry)).unwrap_err(),
        StoreError::Contention
    );
    drop(lock);
    assert_eq!(store.load().unwrap(), current);
}

/// A tiny segment size seals after every record.
fn segmented(temp: &Temp) -> Application<FileStore> {
    Application::new(FileStore::new(temp.store()).unwrap().with_segment_size(1))
        .with_checkpoint_interval(2)
}
fn indexes(temp: &Temp) -> Vec<PathBuf> {
    let mut paths: Vec<_> = fs::read_dir(temp.store().join("log"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "idx"))
        .collect();
    paths.sort();
    paths
}
#[test]
fn sealed_segments_keep_old_decisions_findable_through_indexes() {
    let temp = Temp::new();
    ready(&temp);
    let app = segmented(&temp);
    for index in 0..4 {
        let id = format!("t{index}");
        app.execute(prepare_as(&id)).unwrap();
        app.execute(commit_as(&id, "0")).unwrap();
    }
    let snapshot = temp.snapshot();
    assert_eq!(snapshot.revision, 9);
    assert_eq!(snapshot.resources[0].quantity, "5");
    // Every segment except the active one is sealed.
    let segments = fs::read_dir(temp.store().join("log"))
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .path()
                .extension()
                .is_some_and(|e| e == "seg")
        })
        .count();
    assert_eq!(indexes(&temp).len(), segments - 1);
    // A decision far behind the checkpoint is found through a sealed index.
    let store = FileStore::new(temp.store()).unwrap();
    let found = store.find(&tx("t0"), 0).unwrap().unwrap();
    assert_eq!(found.sequence, 2);
    assert!(store.find(&tx("missing"), 0).unwrap().is_none());
    assert_eq!(
        app.execute(commit_as("t0", "0")).unwrap(),
        Output::Transaction(named(&snapshot, "t0"))
    );
    assert_eq!(
        app.execute(commit_as("t0", "-1")).unwrap_err().code,
        Code::Conflict
    );
    // Reopening with the default size continues the active segment.
    finish_as(&temp, "u");
}
#[test]
fn missing_or_corrupt_indexes_and_changed_sealed_segments_fail_closed() {
    for damage in ["remove", "flip", "extend"] {
        let temp = Temp::new();
        ready(&temp);
        let app = segmented(&temp);
        for index in 0..3 {
            let id = format!("t{index}");
            app.execute(prepare_as(&id)).unwrap();
            app.execute(commit_as(&id, "-1")).unwrap();
        }
        // One record per segment: create, prepare t0, commit t0, ...
        let index = indexes(&temp)[2].clone();
        match damage {
            "remove" => fs::remove_file(&index).unwrap(),
            "flip" => {
                let mut bytes = fs::read(&index).unwrap();
                bytes[10] ^= 0x01;
                fs::write(&index, bytes).unwrap();
            }
            _ => {
                let segment = index.with_extension("seg");
                File::options()
                    .append(true)
                    .open(segment)
                    .unwrap()
                    .write_all(&[0; 8])
                    .unwrap();
            }
        }
        assert_eq!(
            app.execute(Command::ListTransactions).unwrap_err().code,
            Code::CorruptState,
            "{damage}"
        );
        if damage != "extend" {
            assert_eq!(
                app.execute(commit_as("t0", "-1")).unwrap_err().code,
                Code::CorruptState,
                "{damage}"
            );
        }
    }
}
#[test]
fn interrupted_sealing_resumes_without_losing_or_repeating_records() {
    for mode in ["error", "kill"] {
        let temp = Temp::new();
        ready(&temp);
        let old = temp.snapshot();
        let expect = if mode == "kill" { "kill" } else { "io" };
        let mut process = Process::new(std::env::current_exe().unwrap());
        process
            .args(["--exact", "child_worker", "--nocapture"])
            .env("RESCROW_TEST_STORE", temp.store())
            .env("RESCROW_TEST_STAGE", format!("{:?}", Stage::Sealed))
            .env("RESCROW_TEST_MODE", mode)
            .env("RESCROW_TEST_MARKER", temp.0.join("reached"))
            .env(
                "RESCROW_TEST_EXPECT",
                if mode == "kill" { "io" } else { expect },
            )
            .env("RESCROW_TEST_SEGMENT", "1")
            .stdout(std::process::Stdio::null());
        if mode == "kill" {
            kill_with(&temp, Stage::Sealed, expect, process.spawn().unwrap());
        } else {
            assert!(process.status().unwrap().success());
        }
        // The segment is sealed but its successor does not exist yet.
        assert_eq!(indexes(&temp).len(), 1);
        assert_eq!(temp.snapshot(), old);
        finish(&temp);
    }
}
fn commit_as(name: &str, delta: &str) -> Command {
    commit_raw(&tx(name), delta)
}
fn commit_raw(id: &str, delta: &str) -> Command {
    Command::Commit(Commit {
        transaction_id: id.into(),
        actuals: Some(vec![Actual {
            operation_id: "o".into(),
            delta: delta.into(),
        }]),
    })
}
fn finish_as(temp: &Temp, id: &str) {
    let app = temp.app();
    let before = temp.snapshot();
    app.execute(prepare_as(id)).unwrap();
    app.execute(commit_as(id, "-1")).unwrap();
    let after = temp.snapshot();
    assert_eq!(after.revision, before.revision + 2);
    app.execute(commit_as(id, "-1")).unwrap();
    assert_eq!(temp.snapshot(), after);
}

/// A schema-1 store: owner-only directory, lock file and `state.json`.
fn legacy_store(temp: &Temp) -> Snapshot {
    use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
    let memory = rescrow_adapters::memory::MemoryStore::default();
    let app = Application::new(memory);
    app.initialize().unwrap();
    app.execute(create()).unwrap();
    app.execute(prepare()).unwrap();
    app.execute(prepare_as("refused")).unwrap_err();
    let mut snapshot = app.snapshot().unwrap();
    snapshot.schema_version = 1;
    snapshot.revision = 7;
    fs::DirBuilder::new()
        .mode(0o700)
        .create(temp.store())
        .unwrap();
    for (name, bytes) in [
        ("state.lock", Vec::new()),
        ("state.json", serde_json::to_vec(&snapshot).unwrap()),
    ] {
        File::options()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(temp.store().join(name))
            .unwrap()
            .write_all(&bytes)
            .unwrap();
    }
    snapshot
}
fn migrate(temp: &Temp) -> Result<(), rescrow_application::Error> {
    let snapshot = FileStore::new(temp.store())
        .unwrap()
        .legacy()
        .map_err(rescrow_application::Error::from)?;
    temp.app().import(snapshot)
}
/// The imported ledger matches its source apart from the log's own revision.
fn assert_migrated(temp: &Temp, source: &Snapshot) {
    let imported = temp.snapshot();
    assert_eq!(
        Snapshot {
            schema_version: 1,
            revision: source.revision,
            ..imported
        },
        *source
    );
    assert!(!temp.store().join("state.json").exists());
    assert!(temp.store().join("state.json.migrated").exists());
}
#[test]
fn migration_converts_a_schema_1_store_and_retires_its_snapshot() {
    let temp = Temp::new();
    let source = legacy_store(&temp);
    let store = FileStore::new(temp.store()).unwrap();
    assert_eq!(store.load().unwrap_err(), StoreError::Unsupported(1));
    assert_eq!(
        temp.app().initialize().unwrap_err().code,
        Code::UnsupportedSchema
    );
    migrate(&temp).unwrap();
    assert_migrated(&temp, &source);
    assert_eq!(migrate(&temp).unwrap_err().code, Code::AlreadyInitialized);
    // Old decisions replay and live holds settle normally.
    let app = temp.app();
    assert_eq!(
        app.execute(prepare_as("refused")).unwrap_err().code,
        Code::Capacity
    );
    app.execute(commit()).unwrap();
    assert_eq!(temp.snapshot().resources[0].quantity, "2");
}
#[test]
fn interrupted_migration_is_redone_or_completed() {
    for stage in [
        Stage::BeforeWrite,
        Stage::FileSynced,
        Stage::Renamed,
        Stage::BeforeResponse,
    ] {
        let temp = Temp::new();
        let source = legacy_store(&temp);
        let mut process = Process::new(std::env::current_exe().unwrap());
        process
            .args(["--exact", "child_worker", "--nocapture"])
            .env("RESCROW_TEST_STORE", temp.store())
            .env("RESCROW_TEST_ACTION", "migrate")
            .env("RESCROW_TEST_STAGE", format!("{stage:?}"))
            .env("RESCROW_TEST_MODE", "kill")
            .env("RESCROW_TEST_MARKER", temp.0.join("reached"))
            .env("RESCROW_TEST_EXPECT", "io")
            .stdout(std::process::Stdio::null());
        kill_with(&temp, stage, "kill", process.spawn().unwrap());
        let published = matches!(stage, Stage::Renamed | Stage::BeforeResponse);
        // Until live.json is published the snapshot stays authoritative;
        // afterwards the log is, even before the snapshot is retired.
        if published {
            assert_eq!(temp.snapshot().resources, source.resources);
            assert_eq!(migrate(&temp).unwrap_err().code, Code::AlreadyInitialized);
        } else {
            assert_eq!(
                FileStore::new(temp.store()).unwrap().load().unwrap_err(),
                StoreError::Unsupported(1)
            );
            migrate(&temp).unwrap();
        }
        assert_migrated(&temp, &source);
    }
}
/// Regression bounds, not a benchmark: one mutation's written bytes and a point
/// read's replayed records do not grow with retained terminal history.
#[test]
fn writes_and_point_reads_do_not_grow_with_history() {
    let temp = Temp::new();
    ready(&temp);
    let app = temp.app();
    let settle = |range: std::ops::Range<usize>| {
        for index in range {
            let id = format!("h{index:04}");
            app.execute(prepare_as(&id)).unwrap();
            app.execute(commit_as(&id, "0")).unwrap();
        }
    };
    let live = || fs::metadata(temp.store().join("live.json")).unwrap().len();
    let log = || fs::metadata(temp.store().join(SEGMENT)).unwrap().len();
    settle(0..40);
    let (live_small, log_small) = (live(), log());
    let before = log();
    app.execute(prepare_as("small")).unwrap();
    let write_small = log() - before;
    app.execute(Command::Abort {
        transaction_id: tx("small"),
    })
    .unwrap();
    settle(40..400);
    let before = log();
    app.execute(prepare_as("large")).unwrap();
    let write_large = log() - before;
    assert!(log() > 8 * log_small, "history should have grown");
    // Live state holds one resource and one or two holds either way; only
    // its log position and offset gain digits.
    assert!(live() <= live_small + 300, "{} vs {live_small}", live());
    assert!(
        write_large <= write_small + 8,
        "{write_large} vs {write_small}"
    );
    let loaded = FileStore::new(temp.store()).unwrap().load().unwrap();
    assert!(loaded.tail.len() < 64, "{}", loaded.tail.len());
    assert_eq!(
        FileStore::new(temp.store())
            .unwrap()
            .find(&tx("h0000"), 0)
            .unwrap()
            .unwrap()
            .sequence,
        2
    );
}
/// A fresh prepare searches only records within its clock-skew bound, so a
/// damaged index of old history goes unread; a retry of an old request, and
/// inspection of all history, still read it and fail closed.
#[test]
fn new_prepares_skip_segments_older_than_their_skew_bound() {
    let temp = Temp::new();
    let old = created() - 2 * 60 * 60 * 1000;
    let past = Application::new(FileStore::new(temp.store()).unwrap().with_segment_size(1))
        .with_checkpoint_interval(2)
        .with_clock(move || old);
    past.initialize().unwrap();
    past.execute(create()).unwrap();
    for index in 0..3 {
        let id = tx_at(&format!("t{index}"), old);
        past.execute(prepare_raw(&id)).unwrap();
        past.execute(commit_raw(&id, "0")).unwrap();
    }
    let index = indexes(&temp)[2].clone();
    let mut bytes = fs::read(&index).unwrap();
    bytes[10] ^= 0x01;
    fs::write(&index, bytes).unwrap();
    let app = segmented(&temp);
    app.execute(prepare_as("fresh")).unwrap();
    app.execute(commit_as("fresh", "0")).unwrap();
    assert_eq!(
        app.execute(prepare_raw(&tx_at("t0", old)))
            .unwrap_err()
            .code,
        Code::CorruptState
    );
    assert_eq!(
        app.execute(Command::ShowTransaction {
            transaction_id: tx_at("t0", old)
        })
        .unwrap_err()
        .code,
        Code::CorruptState
    );
}
