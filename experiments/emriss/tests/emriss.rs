#[path = "../../../crates/rescrow-adapters/tests/support/concurrent_workload.rs"]
mod concurrent_workload;
#[path = "../../../crates/rescrow-cli/tests/support/history.rs"]
mod history;
#[path = "../../../crates/rescrow-adapters/tests/support/escrow_model.rs"]
mod model;

use emriss::storage::MemoryObjects;
use emriss_atom::MemoryRefs;
use futures::executor::block_on;
use model::{Command, Failure, Model, Op, Spec, tx_id};
use proptest::prelude::*;
use rescrow_application::{
    Category, Code, Error, dto,
    records::{RecordApplication, RecordStore},
};
use rescrow_emriss::EmrissStore;
use std::sync::{
    Barrier,
    atomic::{AtomicU64, Ordering},
};

/// Force two commands to evaluate the same root before either can publish.
#[derive(Clone)]
struct RacingStore {
    inner: EmrissStore,
    gate: std::sync::Arc<Barrier>,
    attempts: std::sync::Arc<AtomicU64>,
}
impl RecordStore for RacingStore {
    type View = rescrow_emriss::EmrissView;
    async fn initialize(
        &self,
        snapshot: &rescrow_application::ValidatedSnapshot,
    ) -> Result<(), rescrow_application::StoreError> {
        self.inner.initialize(snapshot).await
    }
    async fn load(&self) -> Result<Self::View, rescrow_application::StoreError> {
        self.inner.load().await
    }
    async fn publish(
        &self,
        expected: &Self::View,
        changes: &rescrow_application::records::Changes,
    ) -> Result<rescrow_application::Publication, rescrow_application::StoreError> {
        if self.attempts.fetch_add(1, Ordering::SeqCst) < 2 {
            self.gate.wait();
        }
        self.inner.publish(expected, changes).await
    }
}

#[test]
fn real_cas_conflict_reloads_and_recomputes_capacity() {
    let (objects, refs) = (MemoryObjects::default(), MemoryRefs::new());
    let inner = make_store(&objects, &refs);
    let mut model = Model::default();
    model
        .apply(&Command::Create(
            0,
            Spec {
                lower: 0,
                upper: Some(10),
                initial: 5,
                unit: 0,
                scale: 0,
            },
        ))
        .unwrap();
    block_on(RecordApplication::new(inner.clone()).import(model.snapshot(1))).unwrap();
    let store = RacingStore {
        inner: inner.clone(),
        gate: std::sync::Arc::new(Barrier::new(2)),
        attempts: Default::default(),
    };
    let results = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..2)
            .map(|id| {
                let store = store.clone();
                scope.spawn(move || {
                    block_on(
                        RecordApplication::new(store).execute(
                            Command::Prepare(
                                id,
                                vec![Op {
                                    id: 0,
                                    resource: 0,
                                    delta: -4,
                                }],
                            )
                            .dto(),
                        ),
                    )
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results.iter().find_map(|r| r.as_ref().err()).unwrap().code,
        Code::Capacity
    );
    assert_eq!(store.attempts.load(Ordering::SeqCst), 3);
    let snapshot = block_on(RecordApplication::new(inner).audit())
        .unwrap()
        .to_dto();
    assert_eq!(snapshot.revision, 3);
    assert_eq!(snapshot.resources[0].reserved_down, "4");
    assert_eq!(snapshot.transactions.len(), 2);
}

fn make_store(objects: &MemoryObjects, refs: &MemoryRefs) -> EmrissStore {
    EmrissStore::new(objects.clone(), refs.clone(), "memory", "escrow").unwrap()
}
fn assert_result(
    actual: Result<dto::Output, Error>,
    expected: Result<(), Failure>,
    model: &Model,
    command: &Command,
    revision: u64,
) {
    match expected {
        Ok(()) => assert_eq!(
            actual.unwrap(),
            model.output(command, revision),
            "{command:?}"
        ),
        Err(failure) => {
            let error = actual.unwrap_err();
            let (code, category) = match failure {
                Failure::MalformedPrepare => (Code::InvalidInput, Category::Input),
                Failure::InvalidActuals => (Code::InvalidInput, Category::Domain),
                Failure::Missing => (Code::NotFound, Category::Domain),
                Failure::Conflict => (Code::Conflict, Category::Domain),
                Failure::Capacity { .. } => (Code::Capacity, Category::Domain),
            };
            assert_eq!(
                (&error.code, &error.category),
                (&code, &category),
                "{command:?}"
            );
            assert_eq!(error.capacity.as_deref(), failure.capacity().as_ref());
            assert_eq!(error.transaction_id, command.transaction_id().map(tx_id));
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]
    #[test]
    fn incremental_histories_match_model(steps in prop::collection::vec((0_u8..11, 0_u16..7, -8_i64..=8, 0_u8..7), 1..=48)) {
        block_on(async {
            let (objects, refs) = (MemoryObjects::default(), MemoryRefs::new());
            let mut model = Model::default();
            let mut revision = 0;
            let initial = [Command::Create(0, Spec { lower: -2, upper: Some(10), initial: 5, unit: 0, scale: 0 }), Command::Create(1, Spec { lower: 0, upper: None, initial: 6, unit: 1, scale: 0 })];
            RecordApplication::new(make_store(&objects, &refs)).initialize().await.unwrap();
            for command in initial.into_iter().chain(steps.into_iter().map(|(action, id, delta, mode)| {
                match action {
                    0..=2 => Command::Prepare(id, vec![Op { id: 0, resource: 0, delta }, Op { id: 1, resource: if mode == 6 { 3 } else { 1 }, delta: -delta }]),
                    3 => Command::Commit(id, None),
                    4 => Command::Commit(id, Some(vec![(0, delta), (1, 0)])),
                    5 => Command::Abort(id),
                    6 => Command::Resource(mode % 3),
                    7 => Command::Transaction(id),
                    8 => Command::Resources,
                    9 => Command::Transactions,
                    _ => Command::Create(0, Spec { lower: -2, upper: Some(10), initial: if mode % 2 == 0 { 5 } else { 6 }, unit: 0, scale: 0 }),
                }
            })) {
                let old = model.clone();
                let expected = model.apply(&command);
                revision += u64::from(old != model);
                // Independent store instances reopen byte objects and the same ref.
                let app = RecordApplication::new(make_store(&objects, &refs));
                let actual = app.execute(command.dto()).await;
                assert_result(actual, expected, &model, &command, revision);
                assert_eq!(app.audit().await.unwrap().to_dto(), model.snapshot(revision));
            }
        });
    }
}

fn response(result: Result<dto::Output, Error>) -> history::Response {
    match &result {
        Ok(output) => history::Response {
            exit: Some(0),
            stdout: serde_json::to_string(output).unwrap(),
            stderr: String::new(),
            result: Some(result),
        },
        Err(error) => history::Response {
            exit: Some(match error.code {
                Code::InvalidInput => 2,
                Code::Capacity | Code::Overflow => 3,
                Code::Conflict | Code::NotFound => 4,
                _ => 6,
            }),
            stdout: String::new(),
            stderr: serde_json::to_string(error).unwrap(),
            result: Some(result),
        },
    }
}

#[test]
fn concurrent_independent_instances_match_legal_model_orderings() {
    for case in 0..16 {
        let (objects, refs) = (MemoryObjects::default(), MemoryRefs::new());
        let (setup, clients) = concurrent_workload::workload(case);
        let mut model = Model::default();
        for command in &setup {
            model.apply(command).unwrap();
        }
        block_on(
            RecordApplication::new(make_store(&objects, &refs))
                .import(model.snapshot(setup.len() as u64)),
        )
        .unwrap();
        let barrier = Barrier::new(clients.len());
        let clock = AtomicU64::new(0);
        let calls = std::thread::scope(|scope| {
            let handles: Vec<_> = clients
                .into_iter()
                .map(|commands| {
                    let (barrier, clock) = (&barrier, &clock);
                    let app = RecordApplication::new(make_store(&objects, &refs));
                    scope.spawn(move || {
                        commands
                            .into_iter()
                            .enumerate()
                            .map(|(i, command)| {
                                let invocation = clock.fetch_add(1, Ordering::SeqCst);
                                if i == 0 {
                                    barrier.wait();
                                }
                                let result = block_on(app.execute(command.dto()));
                                let completion = clock.fetch_add(1, Ordering::SeqCst);
                                history::Call {
                                    command,
                                    invocation,
                                    completion,
                                    response: response(result),
                                }
                            })
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            handles
                .into_iter()
                .flat_map(|h| h.join().unwrap())
                .collect()
        });
        let history = history::History {
            case,
            setup,
            calls,
            final_snapshot: block_on(RecordApplication::new(make_store(&objects, &refs)).audit())
                .unwrap()
                .to_dto(),
        };
        if let Err(error) = history::check(&history) {
            let directory = std::env::var_os("RESCROW_TEST_ROOT")
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| {
                    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                        .join("../../target/emriss-histories")
                });
            std::fs::create_dir_all(&directory).unwrap();
            let path = directory.join(format!("history-{}-{case}.json", std::process::id()));
            std::fs::write(&path, serde_json::to_vec_pretty(&history).unwrap()).unwrap();
            panic!("{error}; saved {}", path.display());
        }
    }
}

#[test]
fn import_is_validated_and_existing_roots_are_never_replaced() {
    block_on(async {
        let (objects, refs) = (MemoryObjects::default(), MemoryRefs::new());
        let app = RecordApplication::new(make_store(&objects, &refs));
        assert_eq!(
            app.execute(dto::Command::ListResources)
                .await
                .unwrap_err()
                .code,
            Code::MissingStore
        );
        let mut model = Model::default();
        model
            .apply(&Command::Create(
                0,
                Spec {
                    lower: 0,
                    upper: Some(10),
                    initial: 5,
                    unit: 0,
                    scale: 0,
                },
            ))
            .unwrap();
        let mut invalid = model.snapshot(1);
        invalid.resources[0].quantity = "4".into();
        assert_eq!(
            app.import(invalid).await.unwrap_err().code,
            Code::CorruptState
        );
        app.import(model.snapshot(1)).await.unwrap();
        assert_eq!(
            app.initialize().await.unwrap_err().code,
            Code::AlreadyInitialized
        );
        assert_eq!(app.audit().await.unwrap().to_dto(), model.snapshot(1));
        let mut exhausted = model.snapshot(u64::MAX);
        exhausted.resources[0].spec.resource_id = "r0".into();
        let other = RecordApplication::new(
            EmrissStore::new(objects, MemoryRefs::new(), "memory", "exhausted").unwrap(),
        );
        other.import(exhausted).await.unwrap();
        assert_eq!(
            other
                .execute(
                    Command::Prepare(
                        0,
                        vec![Op {
                            id: 0,
                            resource: 0,
                            delta: -1
                        }]
                    )
                    .dto()
                )
                .await
                .unwrap_err()
                .code,
            Code::RevisionExhausted
        );
    });
}

#[test]
fn a_single_edit_does_not_rewrite_retained_history() {
    block_on(async {
        let (objects, refs) = (MemoryObjects::without_append(), MemoryRefs::new());
        let store = make_store(&objects, &refs);
        let app = RecordApplication::new(store.clone());
        let mut model = Model::default();
        model
            .apply(&Command::Create(
                0,
                Spec {
                    lower: 0,
                    upper: Some(10),
                    initial: 5,
                    unit: 0,
                    scale: 0,
                },
            ))
            .unwrap();
        for id in 0..2000 {
            model
                .apply(&Command::Prepare(
                    id,
                    vec![Op {
                        id: 0,
                        resource: 0,
                        delta: -1,
                    }],
                ))
                .unwrap();
            model
                .apply(&Command::Commit(id, Some(vec![(0, 0)])))
                .unwrap();
        }
        app.import(model.snapshot(4001)).await.unwrap();
        let full_bytes = store.stats().packed_bytes;
        let before = store.stats();
        app.execute(
            Command::Prepare(
                3000,
                vec![Op {
                    id: 0,
                    resource: 0,
                    delta: -1,
                }],
            )
            .dto(),
        )
        .await
        .unwrap();
        let after = store.stats();
        assert!(
            after.packed_bytes - before.packed_bytes < full_bytes / 4,
            "edit rewrote too much: before={before:?}, after={after:?}"
        );
        assert_eq!(after.head_writes - before.head_writes, 1);
        let before = store.stats();
        app.execute(
            Command::Prepare(
                3000,
                vec![Op {
                    id: 0,
                    resource: 0,
                    delta: -1,
                }],
            )
            .dto(),
        )
        .await
        .unwrap();
        assert_eq!(store.stats().head_writes, before.head_writes);
        assert_eq!(store.stats().packed_bytes, before.packed_bytes);
        // A cold point read fetches a path rather than scanning old transactions.
        let cold = make_store(&objects, &refs);
        RecordApplication::new(cold.clone())
            .execute(Command::Resource(0).dto())
            .await
            .unwrap();
        assert!(cold.stats().range_reads < 30, "{:?}", cold.stats());
    });
}
