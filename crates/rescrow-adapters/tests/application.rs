use rescrow_adapters::memory::{Fault, MemoryStore};
use rescrow_application::{
    Application, Category, Code, Loaded, LogStore, Publication, StoreError, dto::*,
};
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
    Command::Prepare(Prepare {
        transaction_id: name.into(),
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
        transaction_id: "t".into(),
    })
    .unwrap();
    assert_eq!(
        app.execute(prepare("rejected", "-3")).unwrap_err().code,
        Code::Capacity
    );
    assert_eq!(
        app.execute(Command::Commit(Commit {
            transaction_id: "rejected".into(),
            actuals: None
        }))
        .unwrap_err()
        .code,
        Code::Conflict
    );
    let revision = snapshot(&store).revision;
    app.execute(Command::Abort {
        transaction_id: "t".into(),
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
        transaction_id: "t".into(),
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
                    transaction_id: "t".into()
                })
                .unwrap_err()
                .category,
                Category::UnknownOutcome
            );
            let before = snapshot(&store);
            app.execute(Command::Abort {
                transaction_id: "t".into(),
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
        app.execute(prepare("bad id", "-1")).unwrap_err().category,
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
    fn load(&self) -> Result<Loaded, StoreError> {
        self.inner.load()
    }
    fn append(&self, entry: &Entry) -> Result<Publication, StoreError> {
        if self.first.replace(false) {
            Application::new(self.inner.clone())
                .execute(prepare("winner", "-5"))
                .unwrap();
            Ok(Publication::Conflict)
        } else {
            self.inner.append(entry)
        }
    }
    fn checkpoint(&self, c: &Checkpoint) -> Result<(), StoreError> {
        self.inner.checkpoint(c)
    }
    fn find(&self, id: &str) -> Result<Option<Entry>, StoreError> {
        self.inner.find(id)
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
            transaction_id: "missing".into(),
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
        transaction_id: "held".into(),
        operations: vec![Operation {
            operation_id: "o".into(),
            resource_id: "r".into(),
            delta: "-3".into(),
        }],
    })
    .unwrap();
    let request = Prepare {
        transaction_id: "rejected".into(),
        operations: vec![Operation {
            operation_id: "a".into(),
            resource_id: "r".into(),
            delta: "-4".into(),
        }],
    };
    let error = app.prepare(request.clone()).unwrap_err();
    assert_eq!(error.transaction_id.as_deref(), Some("rejected"));
    assert_eq!(
        error.capacity.as_deref(),
        Some(&CapacityFailure {
            resource_id: "r".into(),
            requested_delta: "-4".into(),
            available: "2".into()
        })
    );
    let serialized = serde_json::to_string(&error).unwrap();
    app.abort("held").unwrap();
    assert_eq!(
        serde_json::to_string(&app.prepare(request).unwrap_err()).unwrap(),
        serialized
    );
    assert!(matches!(
        app.transaction("rejected").unwrap().state,
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
