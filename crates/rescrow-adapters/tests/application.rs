use rescrow_adapters::memory::{Fault, MemoryStore};
use rescrow_application::{
    Application, Category, Code, Loaded, LogStore, Publication, StoreError, dto::*,
};
use std::sync::atomic::{AtomicU64, Ordering};
/// Every request in this process is created at its start, so retries match.
fn created() -> u64 {
    static CREATED: std::sync::LazyLock<u64> =
        std::sync::LazyLock::new(rescrow_application::system_clock);
    *CREATED
}
/// The UUIDv7 a test names `name`, created at `millis`.
fn id_at(name: &str, millis: u64) -> String {
    let hash = name.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    });
    rescrow_application::uuid_v7(millis, hash.into())
}
/// The UUIDv7 a test names `name`, created when the test process started.
fn id(name: &str) -> String {
    id_at(name, created())
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
fn prepare(name: &str, delta: &str) -> Command {
    prepare_raw(&id(name), delta)
}
fn prepare_raw(transaction_id: &str, delta: &str) -> Command {
    Command::Prepare(Prepare {
        transaction_id: transaction_id.into(),
        operations: vec![Operation {
            operation_id: "o".into(),
            resource_id: "r".into(),
            delta: delta.into(),
        }],
    })
}
fn snapshot(store: &MemoryStore) -> Snapshot {
    Application::new(store.clone()).snapshot().unwrap()
}
fn ready() -> (MemoryStore, Application<MemoryStore>) {
    let store = MemoryStore::default();
    let app = Application::new(store.clone());
    app.initialize().unwrap();
    app.execute(create()).unwrap();
    (store, app)
}
#[test]
fn direct_api_publishes_rejections_and_replays_do_not_advance_revision() {
    let (store, app) = ready();
    app.execute(prepare("t", "-3")).unwrap();
    let before = snapshot(&store);
    app.execute(prepare("t", "-3")).unwrap();
    app.execute(create()).unwrap();
    app.execute(Command::ListResources).unwrap();
    assert_eq!(snapshot(&store), before);
    let err = app.execute(prepare("rejected", "-3")).unwrap_err();
    assert_eq!(err.code, Code::Capacity);
    let rejected = snapshot(&store);
    assert_eq!(rejected.revision, before.revision + 1);
    assert_eq!(rejected.resources, before.resources);
    assert_eq!(rejected.transactions.len(), 2);
    app.execute(Command::Abort {
        transaction_id: id("t"),
    })
    .unwrap();
    assert_eq!(
        app.execute(prepare("rejected", "-3")).unwrap_err().code,
        Code::Capacity
    );
    assert_eq!(
        app.execute(Command::Commit(Commit {
            transaction_id: id("rejected"),
            actuals: None
        }))
        .unwrap_err()
        .code,
        Code::Conflict
    );
    let revision = snapshot(&store).revision;
    app.execute(Command::Abort {
        transaction_id: id("t"),
    })
    .unwrap();
    assert_eq!(snapshot(&store).revision, revision);
}
#[test]
fn bounded_retries_prepublication_failures_and_lost_response() {
    let (store, app) = ready();
    let old = snapshot(&store);
    store.inject(Fault::BeforePublication);
    assert_eq!(app.execute(prepare("t", "-3")).unwrap_err().code, Code::Io);
    assert_eq!(snapshot(&store), old);
    store.inject(Fault::Conflict);
    store.inject(Fault::Conflict);
    let evaluations = store.evaluations();
    app.execute(prepare("t", "-3")).unwrap();
    assert_eq!(store.evaluations() - evaluations, 3);
    store.inject(Fault::LostResponse);
    let commit = Command::Commit(Commit {
        transaction_id: id("t"),
        actuals: Some(vec![Actual {
            operation_id: "o".into(),
            delta: "-2".into(),
        }]),
    });
    assert_eq!(
        app.execute(commit.clone()).unwrap_err().category,
        Category::UnknownOutcome
    );
    let published = snapshot(&store);
    app.execute(commit).unwrap();
    assert_eq!(snapshot(&store), published);
    assert_eq!(published.resources[0].quantity, "3");
    for _ in 0..32 {
        store.inject(Fault::Conflict);
    }
    assert_eq!(
        app.execute(prepare("u", "-1")).unwrap_err().code,
        Code::Contention
    );
}
#[test]
fn lost_prepare_rejection_and_abort_responses_keep_identity() {
    for (delta, expected) in [("-3", None), ("-6", Some(Code::Capacity))] {
        let (store, app) = ready();
        store.inject(Fault::LostResponse);
        assert_eq!(
            app.execute(prepare("t", delta)).unwrap_err().category,
            Category::UnknownOutcome
        );
        let before = snapshot(&store);
        let retry = app.execute(prepare("t", delta));
        match &expected {
            Some(code) => assert_eq!(&retry.unwrap_err().code, code),
            None => {
                retry.unwrap();
            }
        }
        assert_eq!(snapshot(&store), before);
        if expected.is_none() {
            store.inject(Fault::LostResponse);
            assert_eq!(
                app.execute(Command::Abort {
                    transaction_id: id("t")
                })
                .unwrap_err()
                .category,
                Category::UnknownOutcome
            );
            let before = snapshot(&store);
            app.execute(Command::Abort {
                transaction_id: id("t"),
            })
            .unwrap();
            assert_eq!(snapshot(&store), before);
        }
    }
}
#[test]
fn corrupt_storage_bad_input_and_revision_overflow_fail_closed() {
    let (store, app) = ready();
    // A tail record that no longer matches its transition.
    let (checkpoint, mut entries) = store.raw();
    let Record::ResourceCreated(spec) = &mut entries[0].record else {
        panic!("expected resource creation");
    };
    spec.initial = "11".into();
    store.set_raw(checkpoint.unwrap(), entries);
    assert_eq!(
        app.execute(Command::ListResources).unwrap_err().code,
        Code::CorruptState
    );
    assert_eq!(
        app.execute(prepare_raw("bad id", "-1"))
            .unwrap_err()
            .category,
        Category::Input
    );
    // A checkpoint whose holds disagree with its prepared transactions.
    let (store, app) = ready();
    app.execute(prepare("t", "-1")).unwrap();
    let (_, entries) = store.raw();
    let mut checkpoint = checkpoint_of(&store);
    checkpoint.prepared.clear();
    store.set_raw(checkpoint, entries);
    assert_eq!(
        app.execute(prepare("u", "-1")).unwrap_err().code,
        Code::CorruptState
    );
    // A checkpoint that disagrees with history is found by a full scan.
    let (store, app) = ready();
    let (_, entries) = store.raw();
    let mut checkpoint = checkpoint_of(&store);
    checkpoint.resources[0].quantity = "6".into();
    checkpoint.resources[0].available_down = "6".into();
    checkpoint.resources[0].available_up = "4".into();
    store.set_raw(checkpoint, entries);
    app.execute(Command::ShowResource {
        resource_id: "r".into(),
    })
    .unwrap();
    assert_eq!(
        app.execute(Command::ListResources).unwrap_err().code,
        Code::CorruptState
    );
    let (store, app) = ready();
    let (checkpoint, entries) = store.raw();
    let mut checkpoint = checkpoint.unwrap();
    checkpoint.schema_version = 99;
    store.set_raw(checkpoint, entries);
    assert_eq!(
        app.execute(Command::ListResources).unwrap_err().code,
        Code::UnsupportedSchema
    );
    let (store, app) = ready();
    let mut checkpoint = checkpoint_of(&store);
    checkpoint.position = u64::MAX;
    store.set_raw(checkpoint.clone(), vec![]);
    assert_eq!(
        app.execute(prepare("t", "-1")).unwrap_err().code,
        Code::RevisionExhausted
    );
    assert_eq!(store.raw(), (Some(checkpoint), vec![]));
    let store = MemoryStore::default();
    let app = Application::new(store);
    assert_eq!(
        app.execute(Command::ListResources).unwrap_err().code,
        Code::MissingStore
    );
}
/// The live state at the end of the log, as a checkpoint.
fn checkpoint_of(store: &MemoryStore) -> Checkpoint {
    let snapshot = snapshot(store);
    Checkpoint {
        schema_version: rescrow_application::SCHEMA_VERSION,
        position: snapshot.revision,
        recorded_at_ms: store.raw().1.last().map_or(0, |e| e.recorded_at_ms),
        resources: snapshot.resources,
        prepared: snapshot
            .transactions
            .into_iter()
            .filter(|tx| tx.state == State::Prepared {})
            .collect(),
    }
}
// A conflict that actually changes capacity proves evaluation is repeated against a
// fresh validated snapshot, rather than resubmitting a calculation made earlier.
struct Competing {
    inner: MemoryStore,
    first: std::cell::Cell<bool>,
}
impl LogStore for Competing {
    fn initialize(&self, c: &Checkpoint) -> Result<(), StoreError> {
        self.inner.initialize(c)
    }
    fn import(&self, c: &Checkpoint, entries: &[Entry]) -> Result<(), StoreError> {
        self.inner.import(c, entries)
    }
    fn load(&self) -> Result<Loaded, StoreError> {
        self.inner.load()
    }
    fn append(&self, entries: &[Entry]) -> Result<Publication, StoreError> {
        if self.first.replace(false) {
            Application::new(self.inner.clone())
                .execute(prepare("winner", "-5"))
                .unwrap();
            Ok(Publication::Conflict)
        } else {
            self.inner.append(entries)
        }
    }
    fn checkpoint(&self, c: &Checkpoint) -> Result<(), StoreError> {
        self.inner.checkpoint(c)
    }
    fn find(&self, id: &str, since_ms: u64) -> Result<Option<Entry>, StoreError> {
        self.inner.find(id, since_ms)
    }
    fn history(&self) -> Result<Vec<Entry>, StoreError> {
        self.inner.history()
    }
}
#[test]
fn conflict_reloads_and_recomputes_admission() {
    let (store, _) = ready();
    let app = Application::new(Competing {
        inner: store.clone(),
        first: std::cell::Cell::new(true),
    });
    assert_eq!(
        app.execute(prepare("loser", "-1")).unwrap_err().code,
        Code::Capacity
    );
    let snapshot = snapshot(&store);
    assert_eq!(snapshot.resources[0].reserved_down, "5");
    assert_eq!(snapshot.transactions.len(), 2);
    assert_eq!(snapshot.revision, 3);
}

#[test]
fn missing_resource_rejection_remains_terminal_after_resource_creation() {
    let store = MemoryStore::default();
    let app = Application::new(store.clone());
    app.initialize().unwrap();
    assert_eq!(
        app.execute(prepare("missing", "-1")).unwrap_err().code,
        Code::NotFound
    );
    let rejected = snapshot(&store);
    assert_eq!(rejected.transactions.len(), 1);
    assert!(rejected.resources.is_empty());
    app.execute(create()).unwrap();
    let before = snapshot(&store);
    assert_eq!(
        app.execute(prepare("missing", "-1")).unwrap_err().code,
        Code::NotFound
    );
    assert_eq!(snapshot(&store), before);
    assert_eq!(
        app.execute(Command::Commit(Commit {
            transaction_id: id("missing"),
            actuals: None
        }))
        .unwrap_err()
        .code,
        Code::Conflict
    );
}

#[test]
fn typed_api_returns_specific_values_and_freezes_capacity_diagnostics() {
    let (store, app) = ready();
    app.prepare(Prepare {
        transaction_id: id("held"),
        operations: vec![Operation {
            operation_id: "o".into(),
            resource_id: "r".into(),
            delta: "-3".into(),
        }],
    })
    .unwrap();
    let request = Prepare {
        transaction_id: id("rejected"),
        operations: vec![Operation {
            operation_id: "a".into(),
            resource_id: "r".into(),
            delta: "-4".into(),
        }],
    };
    let error = app.prepare(request.clone()).unwrap_err();
    assert_eq!(
        error.transaction_id.as_deref(),
        Some(id("rejected").as_str())
    );
    assert_eq!(
        error.capacity.as_deref(),
        Some(&CapacityFailure {
            resource_id: "r".into(),
            requested_delta: "-4".into(),
            available: "2".into()
        })
    );
    let serialized = serde_json::to_string(&error).unwrap();
    app.abort(&id("held")).unwrap();
    assert_eq!(
        serde_json::to_string(&app.prepare(request).unwrap_err()).unwrap(),
        serialized
    );
    assert!(matches!(
        app.transaction(&id("rejected")).unwrap().state,
        State::Aborted {
            capacity: Some(_),
            ..
        }
    ));
    assert_eq!(app.resource("r").unwrap().quantity, "5");
    assert_eq!(app.resources().unwrap().len(), 1);
    assert_eq!(app.transactions().unwrap().len(), 2);
    // Valid-looking records are still checked against their transitions.
    let (checkpoint, mut entries) = store.raw();
    let Record::Transaction(tx) = &mut entries.last_mut().unwrap().record else {
        panic!("expected a transaction record");
    };
    tx.state = State::Aborted {
        cause: AbortReason::Rejected(Rejection::Capacity),
        capacity: Some(CapacityFailure {
            resource_id: "other".into(),
            requested_delta: "-4".into(),
            available: "2".into(),
        }),
    };
    store.set_raw(checkpoint.unwrap(), entries);
    assert_eq!(app.transactions().unwrap_err().code, Code::CorruptState);
}

/// A schema-1 snapshot covering every transaction state, including a legacy
/// capacity rejection without details.
fn legacy_snapshot() -> Snapshot {
    let (store, app) = ready();
    app.execute(prepare("held", "-2")).unwrap();
    app.execute(prepare("used", "-1")).unwrap();
    app.execute(Command::Commit(Commit {
        transaction_id: id("used"),
        actuals: None,
    }))
    .unwrap();
    app.execute(prepare("released", "1")).unwrap();
    app.abort(&id("released")).unwrap();
    app.execute(prepare("refused", "-9")).unwrap_err();
    app.execute(prepare("legacy", "-9")).unwrap_err();
    app.execute(Command::Prepare(Prepare {
        transaction_id: id("nowhere"),
        operations: vec![Operation {
            operation_id: "o".into(),
            resource_id: "missing".into(),
            delta: "-1".into(),
        }],
    }))
    .unwrap_err();
    let mut snapshot = snapshot(&store);
    snapshot.schema_version = 1;
    snapshot.revision = 42;
    let legacy = snapshot
        .transactions
        .iter_mut()
        .find(|tx| tx.request.transaction_id == id("legacy"))
        .unwrap();
    let State::Aborted { capacity, .. } = &mut legacy.state else {
        panic!("expected a rejection");
    };
    *capacity = None;
    snapshot
}
#[test]
fn import_rebuilds_a_schema_1_snapshot_and_continues_from_it() {
    let source = legacy_snapshot();
    let store = MemoryStore::default();
    let app = Application::new(store.clone());
    app.import(source.clone()).unwrap();
    let imported = app.snapshot().unwrap();
    assert_eq!(
        Snapshot {
            revision: source.revision,
            schema_version: source.schema_version,
            ..imported.clone()
        },
        source
    );
    // The import is one checkpoint at the end of its entries.
    let (checkpoint, entries) = store.raw();
    assert_eq!(checkpoint.unwrap().position, entries.len() as u64);
    assert_eq!(imported.revision, entries.len() as u64);
    // Old decisions replay, and live holds settle normally.
    assert_eq!(
        app.abort(&id("released")).unwrap().request.transaction_id,
        id("released")
    );
    assert_eq!(
        app.execute(prepare("refused", "-9")).unwrap_err().code,
        Code::Capacity
    );
    app.execute(Command::Commit(Commit {
        transaction_id: id("held"),
        actuals: None,
    }))
    .unwrap();
    assert_eq!(app.resource("r").unwrap().quantity, "2");
    assert_eq!(app.snapshot().unwrap().revision, imported.revision + 1);
    assert_eq!(
        app.import(source).unwrap_err().code,
        Code::AlreadyInitialized
    );
}
#[test]
fn import_validates_the_snapshot_before_creating_anything() {
    let mut corrupt = legacy_snapshot();
    corrupt.resources[0].quantity = "3".into();
    let mut unsupported = legacy_snapshot();
    unsupported.schema_version = 3;
    for (snapshot, code) in [
        (corrupt, Code::CorruptState),
        (unsupported, Code::UnsupportedSchema),
    ] {
        let store = MemoryStore::default();
        let app = Application::new(store.clone());
        assert_eq!(app.import(snapshot).unwrap_err().code, code);
        assert_eq!(store.raw(), (None, vec![]));
    }
}

const HOUR: u64 = 60 * 60 * 1000;
const MINUTE: u64 = 60 * 1000;
/// An arbitrary start time, in milliseconds since the Unix epoch.
const T: u64 = 1_800_000_000_000;
/// A store and an Application whose clock the test sets.
fn clocked() -> (
    MemoryStore,
    Application<MemoryStore>,
    std::sync::Arc<AtomicU64>,
) {
    let clock = std::sync::Arc::new(AtomicU64::new(T));
    let store = MemoryStore::default();
    let reading = clock.clone();
    let app = Application::new(store.clone()).with_clock(move || reading.load(Ordering::Relaxed));
    app.initialize().unwrap();
    app.execute(create()).unwrap();
    (store, app, clock)
}
fn prepare_at(name: &str, created_at_ms: u64) -> Command {
    prepare_raw(&id_at(name, created_at_ms), "-1")
}
fn commit_at(name: &str, created_at_ms: u64) -> Command {
    Command::Commit(Commit {
        transaction_id: id_at(name, created_at_ms),
        actuals: None,
    })
}
#[test]
fn retries_replay_inside_the_window_and_expire_after_it() {
    let (store, app, clock) = clocked();
    app.execute(prepare_at("done", T)).unwrap();
    let committed = app.execute(commit_at("done", T)).unwrap();
    app.execute(prepare_at("held", T)).unwrap();
    clock.store(T + 23 * HOUR, Ordering::Relaxed);
    let revision = snapshot(&store).revision;
    assert_eq!(app.execute(prepare_at("done", T)).unwrap(), committed);
    assert_eq!(app.execute(commit_at("done", T)).unwrap(), committed);
    assert_eq!(snapshot(&store).revision, revision);
    clock.store(T + 25 * HOUR, Ordering::Relaxed);
    let expired = app.execute(prepare_at("done", T)).unwrap_err();
    assert_eq!(
        (expired.category, expired.code),
        (Category::Input, Code::Expired)
    );
    assert_eq!(
        app.execute(commit_at("done", T)).unwrap_err().code,
        Code::NotFound
    );
    // Inspection still reads all of history.
    assert_eq!(
        app.execute(Command::ShowTransaction {
            transaction_id: id_at("done", T)
        })
        .unwrap(),
        committed
    );
    // A transaction still holding capacity is live, so its retries never expire.
    let Output::Transaction(held) = app.execute(prepare_at("held", T)).unwrap() else {
        panic!()
    };
    assert_eq!(held.state, State::Prepared {});
    assert_eq!(snapshot(&store).revision, revision);
    app.execute(commit_at("held", T)).unwrap();
}
#[test]
fn new_ids_must_be_uuid_v7_created_near_the_clock() {
    let (store, app, _) = clocked();
    let ahead = id_at("ahead", T + 5 * MINUTE + 1);
    let error = app.execute(prepare_raw(&ahead, "-1")).unwrap_err();
    assert_eq!(error.code, Code::InvalidInput);
    assert_eq!(error.transaction_id.as_deref(), Some(ahead.as_str()));
    let v4 = id_at("v4", T).replacen("-7", "-4", 1);
    let upper = id_at("upper", T).to_uppercase();
    let mut variant = id_at("variant", T);
    variant.replace_range(19..20, "c");
    for bad in ["plain", &v4, &upper, &variant] {
        assert_eq!(
            app.execute(prepare_raw(bad, "-1")).unwrap_err().code,
            Code::InvalidInput,
            "{bad}"
        );
    }
    assert!(snapshot(&store).transactions.is_empty());
    app.execute(prepare_at("ahead", T + 5 * MINUTE)).unwrap();
}
#[test]
fn uuid_v7_round_trips_its_timestamp() {
    for millis in [0, 1, T, (1 << 48) - 1] {
        for random in [0, 1, u128::MAX] {
            let id = rescrow_application::uuid_v7(millis, random);
            assert_eq!(
                rescrow_application::uuid_v7_millis(&id),
                Some(millis),
                "{id}"
            );
        }
    }
}
#[test]
fn recorded_times_never_go_backwards() {
    let (store, app, clock) = clocked();
    clock.store(T - HOUR, Ordering::Relaxed);
    app.execute(prepare_at("late", T - HOUR)).unwrap();
    let (checkpoint, mut entries) = store.raw();
    assert!(entries.iter().all(|e| e.recorded_at_ms == T));
    entries.last_mut().unwrap().recorded_at_ms = T - 1;
    store.set_raw(checkpoint.unwrap(), entries);
    assert_eq!(
        app.execute(Command::ListResources).unwrap_err().code,
        Code::CorruptState
    );
}
