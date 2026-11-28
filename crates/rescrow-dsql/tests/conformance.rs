//! Same port implementation on loopback PostgreSQL or real peered DSQL.
//! Ignored by default: an explicit target is mandatory, never silently skipped.
use rescrow_application::{
    Application, StoreError,
    authority::*,
    capabilities::{DurabilityProfile, Readiness},
    dto,
};
use rescrow_dsql::{Config, DsqlStore};
use rescrow_test_support::{Config as SuiteConfig, concurrent, generated, qualification};
use std::{
    path::PathBuf,
    sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
    },
};

fn store() -> DsqlStore {
    let authority = format!("qualification-{}", rescrow_application::new_id().unwrap());
    let store = connect(&authority, false);
    store.install_schema().unwrap();
    store
}
fn connect(authority: &str, peer: bool) -> DsqlStore {
    if let Ok(path) = std::env::var("RESCROW_DSQL_CONFIG") {
        let primary: Config =
            rescrow_application::canonical::decode(&std::fs::read(&path).unwrap()).unwrap();
        let path = if peer {
            std::env::var("RESCROW_DSQL_PEER_CONFIG").expect(
                "real qualification requires RESCROW_DSQL_PEER_CONFIG for the second active Region",
            )
        } else {
            path
        };
        let mut config: Config =
            rescrow_application::canonical::decode(&std::fs::read(path).unwrap()).unwrap();
        if peer {
            assert_ne!(
                primary.cluster_arn, config.cluster_arn,
                "peer qualification must use distinct active endpoints"
            );
            assert_ne!(
                primary.cluster_arn.split(':').nth(3),
                config.cluster_arn.split(':').nth(3),
                "peer qualification must cross Regions"
            );
        }
        config.authority = authority.into();
        DsqlStore::connect(config).unwrap()
    } else {
        DsqlStore::postgres_simulation(
            &std::env::var("RESCROW_POSTGRES_SIMULATION_URL")
                .expect("set RESCROW_DSQL_CONFIG or RESCROW_POSTGRES_SIMULATION_URL"),
            authority,
        )
        .unwrap()
    }
}
fn create_commit(store: &DsqlStore, name: &str, nonce: u128, extra: &[RecordKey]) -> Commit {
    let mut keys = extra.to_vec();
    keys.push(RecordKey::resource(name).unwrap());
    let tenant = TenantId::local();
    let read = store.read(&tenant, &keys, 0).unwrap();
    let spec: rescrow_domain::ResourceSpec = dto::ResourceSpec {
        resource_id: name.into(),
        lower: "0".into(),
        upper: Some("10".into()),
        initial: "10".into(),
        unit: "u".into(),
        scale: 0,
    }
    .try_into()
    .unwrap();
    let changes = rescrow_domain::transition::create(spec, None).unwrap();
    Commit::from_changes(
        tenant,
        TransitionId::new(rescrow_application::uuid_v7(10, nonce)).unwrap(),
        &read,
        &[(10, changes)],
    )
    .unwrap()
}

#[test]
#[ignore = "requires an explicit PostgreSQL simulation or real DSQL target"]
fn generated_histories() {
    let defaults = SuiteConfig::default();
    let config = SuiteConfig {
        cases: if std::env::var_os("PROPTEST_CASES").is_some() {
            defaults.cases
        } else {
            16
        },
        ..defaults
    };
    generated::run(store, config);
}

#[test]
#[ignore = "requires an explicit PostgreSQL simulation or real DSQL target"]
fn concurrent_histories() {
    let failures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/dsql-histories");
    concurrent::run(store, &failures);
}

/// Deliberately route successive port calls over both independently opened
/// peers. A read and its publication may reach different active Regions.
#[derive(Clone)]
struct Peered {
    stores: [DsqlStore; 2],
    next: Arc<AtomicUsize>,
}
impl Peered {
    fn selected(&self) -> &DsqlStore {
        &self.stores[self.next.fetch_add(1, Ordering::Relaxed) % 2]
    }
}
impl TransactionalStore for Peered {
    fn identity(&self) -> Result<String, StoreError> {
        self.selected().identity()
    }
    fn initialize(&self) -> Result<(), StoreError> {
        self.stores[0].initialize()
    }
    fn read(&self, tenant: &TenantId, keys: &[RecordKey], since: u64) -> Result<Read, StoreError> {
        self.selected().read(tenant, keys, since)
    }
    fn scan(&self, tenant: &TenantId) -> Result<Inspection, StoreError> {
        self.selected().scan(tenant)
    }
    fn commit(&self, commit: &Commit) -> Result<CommitOutcome, StoreError> {
        self.selected().commit(commit)
    }
    fn resolve(&self, tenant: &TenantId, id: &TransitionId) -> Result<Resolution, StoreError> {
        self.selected().resolve(tenant, id)
    }
    fn history(&self, tenant: &TenantId) -> Result<Vec<HistoryRecord>, StoreError> {
        self.selected().history(tenant)
    }
}

#[test]
#[ignore = "requires an explicit PostgreSQL simulation or both real DSQL active peers"]
fn peered_concurrent_histories() {
    let failures =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/dsql-peered-histories");
    concurrent::run(
        || {
            let authority = format!("peered-history-{}", rescrow_application::new_id().unwrap());
            let left = connect(&authority, false);
            left.install_schema().unwrap();
            let right = connect(&authority, true);
            Peered {
                stores: [left, right],
                next: Arc::new(AtomicUsize::new(0)),
            }
        },
        &failures,
    );
}

#[test]
#[ignore = "requires an explicit PostgreSQL simulation or real DSQL target"]
fn tenancy_and_actual_profile() {
    qualification::tenancy(store());
    let store = store();
    assert!(matches!(store.identity(), Err(StoreError::Missing)));
    store.initialize().unwrap();
    let identity = store.identity().unwrap();
    assert!(matches!(store.initialize(), Err(StoreError::Exists)));
    assert_eq!(identity, store.identity().unwrap());
    let capabilities = store.readiness().unwrap();
    if std::env::var_os("RESCROW_DSQL_CONFIG").is_some() {
        capabilities
            .require(DurabilityProfile::EnterpriseMr)
            .unwrap();
    } else {
        assert_eq!(
            capabilities.durability_profile,
            DurabilityProfile::Simulation
        );
        assert!(capabilities.require(DurabilityProfile::Regional).is_err());
    }
}

#[test]
#[ignore = "requires an explicit PostgreSQL simulation or real DSQL target"]
fn disjoint_conditions_fences_and_exact_publication_replay() {
    let store = store();
    store.initialize().unwrap();
    let a = create_commit(&store, "a", 1, &[]);
    let b = create_commit(&store, "b", 2, &[]);
    assert!(matches!(
        store.commit(&a).unwrap(),
        CommitOutcome::Applied(_)
    ));
    assert!(matches!(
        store.commit(&b).unwrap(),
        CommitOutcome::Applied(_)
    ));
    assert!(matches!(
        store.commit(&a).unwrap(),
        CommitOutcome::Applied(_)
    ));
    let fenced = create_commit(&store, "fenced", 3, &[]);
    assert_eq!(
        store
            .resolve(fenced.tenant(), &fenced.history().transition_id)
            .unwrap(),
        Resolution::NotApplied
    );
    assert_eq!(store.commit(&fenced).unwrap(), CommitOutcome::Conflict);
    let resolved = store
        .resolve(a.tenant(), &a.history().transition_id)
        .unwrap();
    assert!(matches!(resolved,Resolution::Applied {history,..} if history == *a.history()));
    assert_eq!(
        Application::new(store).snapshot().unwrap().resources.len(),
        2
    );
}

#[test]
#[ignore = "requires an explicit PostgreSQL simulation or real DSQL target"]
fn absent_cross_dependencies_prevent_write_skew_and_keep_causal_history() {
    let store = store();
    store.initialize().unwrap();
    // Each creates a different key while depending on the other's absence.
    let a = create_commit(&store, "a", 4, &[RecordKey::resource("b").unwrap()]);
    let b = create_commit(&store, "b", 5, &[RecordKey::resource("a").unwrap()]);
    let barrier = Arc::new(Barrier::new(2));
    let results = std::thread::scope(|scope| {
        let handles: Vec<_> = [a, b]
            .into_iter()
            .map(|commit| {
                let store = store.clone();
                let barrier = barrier.clone();
                scope.spawn(move || {
                    barrier.wait();
                    store.commit(&commit).unwrap()
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(
        results
            .iter()
            .filter(|v| matches!(v, CommitOutcome::Applied(_)))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|v| matches!(v, CommitOutcome::Conflict))
            .count(),
        1
    );
    let snapshot = Application::new(store).snapshot().unwrap();
    assert_eq!(snapshot.resources.len(), 1);
}

#[test]
#[ignore = "requires an explicit PostgreSQL simulation or real DSQL target"]
fn guards_do_not_change_logical_versions_and_uuid_sort_is_not_causal_order() {
    let store = store();
    store.initialize().unwrap();
    let a = create_commit(&store, "a", 900, &[]);
    store.commit(&a).unwrap();
    let tenant = TenantId::local();
    let keys = [RecordKey::resource("a").unwrap()];
    let before = store.read(&tenant, &keys, 0).unwrap();
    // Same millisecond, lower UUID nonce, but causally after a.
    let b = create_commit(&store, "b", 1, &keys);
    store.commit(&b).unwrap();
    let after = store.read(&tenant, &keys, 0).unwrap();
    assert_eq!(before, after);
    let history = store.history(&tenant).unwrap();
    assert_eq!(history[0], *a.history());
    assert_eq!(history[1], *b.history());
    assert_eq!(
        Application::new(store).snapshot().unwrap().resources.len(),
        2
    );
}

#[test]
#[ignore = "requires an explicit PostgreSQL simulation or both real DSQL active peers"]
fn independent_connections_share_identity_and_observe_peer_publications() {
    let authority = format!(
        "peer-qualification-{}",
        rescrow_application::new_id().unwrap()
    );
    let left = connect(&authority, false);
    left.install_schema().unwrap();
    left.initialize().unwrap();
    let right = connect(&authority, true);
    assert_eq!(left.identity().unwrap(), right.identity().unwrap());
    let a = create_commit(&left, "a", 1, &[]);
    assert!(matches!(
        left.commit(&a).unwrap(),
        CommitOutcome::Applied(_)
    ));
    assert_eq!(
        right
            .resolve(a.tenant(), &a.history().transition_id)
            .unwrap(),
        left.resolve(a.tenant(), &a.history().transition_id)
            .unwrap()
    );
    let b = create_commit(&right, "b", 2, &[RecordKey::resource("a").unwrap()]);
    assert!(matches!(
        right.commit(&b).unwrap(),
        CommitOutcome::Applied(_)
    ));
    assert_eq!(
        Application::new(left.clone()).snapshot().unwrap(),
        Application::new(right).snapshot().unwrap()
    );
    let old_generation = left.identity().unwrap();
    drop(left);
    let reopened = connect(&authority, false);
    assert_eq!(old_generation, reopened.identity().unwrap());
    assert_eq!(
        Application::new(reopened)
            .snapshot()
            .unwrap()
            .resources
            .len(),
        2
    );
}

#[test]
#[ignore = "requires an explicit PostgreSQL simulation or real DSQL target"]
fn oversized_dependency_sets_publish_neither_values_nor_history() {
    let store = store();
    store.initialize().unwrap();
    for (resource_guards, reservation_guards) in [(rescrow_dsql::MAX_PARTICIPANTS, 0), (0, 2999)] {
        let mut records = std::collections::BTreeMap::new();
        records.insert(RecordKey::resource("target").unwrap(), None);
        for n in 0..resource_guards {
            records.insert(RecordKey::resource(&format!("guard-{n}")).unwrap(), None);
        }
        for n in 0..reservation_guards {
            records.insert(
                Scope::local().reservation(&format!("guard-{n}")).unwrap(),
                None,
            );
        }
        let observed = Read {
            records,
            recorded_at_ms: 0,
            fence: None,
        };
        let spec: rescrow_domain::ResourceSpec = dto::ResourceSpec {
            resource_id: "target".into(),
            lower: "0".into(),
            upper: Some("10".into()),
            initial: "10".into(),
            unit: "u".into(),
            scale: 0,
        }
        .try_into()
        .unwrap();
        let commit = Commit::from_changes(
            TenantId::local(),
            TransitionId::new(rescrow_application::uuid_v7(10, 1)).unwrap(),
            &observed,
            &[(10, rescrow_domain::transition::create(spec, None).unwrap())],
        )
        .unwrap();
        assert!(matches!(store.commit(&commit), Err(StoreError::TooLarge)));
        assert!(store.history(&TenantId::local()).unwrap().is_empty());
        assert!(
            Application::new(store.clone())
                .snapshot()
                .unwrap()
                .resources
                .is_empty()
        );
    }
}
