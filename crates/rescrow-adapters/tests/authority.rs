use rescrow_adapters::authority_memory::MemoryAuthority;
use rescrow_application::{
    authority::*,
    capabilities::{DurabilityProfile, Readiness},
    dto, uuid_v7,
};
use rescrow_domain::{self as d, transition};
use rescrow_test_support::fault::{Fault, FaultStore};
use std::collections::BTreeMap;

fn id(s: &str) -> d::Id {
    d::Id::new(s.into()).unwrap()
}
fn publication_id(n: u128) -> TransitionId {
    TransitionId::new(uuid_v7(10, n)).unwrap()
}
fn spec(name: &str) -> d::ResourceSpec {
    dto::ResourceSpec {
        resource_id: name.into(),
        lower: "0".into(),
        upper: Some("10".into()),
        initial: "10".into(),
        unit: "u".into(),
        scale: 0,
    }
    .try_into()
    .unwrap()
}
fn create<S: TransactionalStore>(s: &S, tenant: &TenantId, name: &str, n: u128) -> Commit {
    let key = RecordKey::resource(name).unwrap();
    let observed = s.read(tenant, &[key], 0).unwrap();
    Commit::from_changes(
        tenant.clone(),
        publication_id(n),
        &observed,
        &[(10, transition::create(spec(name), None).unwrap())],
    )
    .unwrap()
}
fn resources(observed: &ReadSet) -> BTreeMap<d::Id, d::Resource> {
    observed
        .values()
        .flatten()
        .filter_map(|v| match &v.value {
            Value::Resource(r) => Some((id(&r.spec.resource_id), r.clone().try_into().unwrap())),
            _ => None,
        })
        .collect()
}
fn prepare<S: TransactionalStore>(
    store: &S,
    tenant: &TenantId,
    namespace: &str,
    n: u128,
) -> Commit {
    let scope = Scope::new(tenant.clone(), namespace).unwrap();
    let txkey = scope.transaction("same").unwrap();
    let RecordKey::Transaction(txid) = &txkey else {
        unreachable!()
    };
    let request = d::Request::new(
        txid.clone(),
        vec![d::Operation::new(id("op"), id("r"), -4).unwrap()],
    )
    .unwrap();
    let observed = store
        .read(tenant, &[RecordKey::resource("r").unwrap(), txkey], 0)
        .unwrap();
    let changes = transition::prepare(request, None, &resources(&observed.records), 10).unwrap();
    Commit::from_changes(
        tenant.clone(),
        publication_id(n),
        &observed,
        &[(10, changes)],
    )
    .unwrap()
}

#[test]
fn disjoint_stale_reads_do_not_conflict_but_shared_records_do() {
    let store = MemoryAuthority::default();
    store.initialize().unwrap();
    let tenant = TenantId::local();
    let a = create(&store, &tenant, "a", 1);
    let b = create(&store, &tenant, "b", 2);
    let stale = create(&store, &tenant, "a", 3);
    assert!(matches!(
        store.commit(&a).unwrap(),
        CommitOutcome::Applied(_)
    ));
    assert!(matches!(
        store.commit(&b).unwrap(),
        CommitOutcome::Applied(_)
    ));
    assert_eq!(store.commit(&stale).unwrap(), CommitOutcome::Conflict);
    assert_eq!(
        store.resolve(&tenant, &publication_id(3)).unwrap(),
        Resolution::NotApplied
    );
    // Publication retries return their original success, even after later writes.
    assert!(matches!(
        store.commit(&a).unwrap(),
        CommitOutcome::Applied(_)
    ));
    assert_eq!(store.history(&tenant).unwrap().len(), 2);
}

#[test]
fn tenant_and_namespace_keys_isolate_replay_and_share_only_tenant_resources() {
    let store = MemoryAuthority::default();
    store.initialize().unwrap();
    let a = TenantId::new("a").unwrap();
    let b = TenantId::new("b").unwrap();
    for t in [&a, &b] {
        store.commit(&create(&store, t, "r", 1)).unwrap();
        store.commit(&prepare(&store, t, "one", 2)).unwrap();
    }
    // Another namespace reserves a second hold against a's same resource.
    store.commit(&prepare(&store, &a, "two", 3)).unwrap();
    let a_records = store.scan(&a).unwrap();
    let b_records = store.scan(&b).unwrap();
    assert_eq!(
        resources(&a_records.read.records)[&id("r")]
            .availability()
            .available_down(),
        2
    );
    assert_eq!(
        resources(&b_records.read.records)[&id("r")]
            .availability()
            .available_down(),
        6
    );
    let key = Scope::new(a.clone(), "two")
        .unwrap()
        .transaction("same")
        .unwrap();
    assert!(
        store
            .read(&b, &[key], 0)
            .unwrap()
            .records
            .values()
            .all(Option::is_none)
    );
    assert_eq!(
        store.resolve(&b, &publication_id(3)).unwrap(),
        Resolution::NotApplied
    );
    let unknown = TenantId::new("never-created").unwrap();
    assert!(store.scan(&unknown).unwrap().read.records.is_empty());
    assert!(store.history(&unknown).unwrap().is_empty());
}

#[test]
fn fault_wrapper_preserves_atomicity_and_resolves_lost_response_by_history() {
    let store = FaultStore::new(MemoryAuthority::default());
    store.initialize().unwrap();
    let tenant = TenantId::local();
    let commit = create(&store, &tenant, "r", 1);
    store.inject(Fault::Conflict);
    assert_eq!(store.commit(&commit).unwrap(), CommitOutcome::Conflict);
    assert!(store.scan(&tenant).unwrap().read.records.is_empty());
    store.inject(Fault::BeforePublication);
    assert!(matches!(
        store.commit(&commit),
        Err(rescrow_application::StoreError::Io(_))
    ));
    assert_eq!(
        store.resolve(&tenant, &publication_id(1)).unwrap(),
        Resolution::NotApplied
    );
    let commit = create(&store, &tenant, "r", 2);
    store.inject(Fault::LostResponse);
    assert!(matches!(
        store.commit(&commit),
        Err(rescrow_application::StoreError::Unknown(_))
    ));
    assert_eq!(
        store.resolve(&tenant, &publication_id(2)).unwrap(),
        Resolution::Applied {
            history: commit.history().clone(),
            receipt: match store.commit(&commit).unwrap() {
                CommitOutcome::Applied(r) => r,
                _ => unreachable!(),
            }
        }
    );
    assert_eq!(store.scan(&tenant).unwrap().read.records.len(), 1);
    assert_eq!(store.history(&tenant).unwrap().len(), 1);
    assert!(matches!(
        store.commit(&commit).unwrap(),
        CommitOutcome::Applied(_)
    ));
}

#[test]
fn atomic_batch_has_one_history_identity_and_no_partial_records() {
    let store = MemoryAuthority::default();
    store.initialize().unwrap();
    let tenant = TenantId::local();
    let observed = store
        .read(
            &tenant,
            &[
                RecordKey::resource("a").unwrap(),
                RecordKey::resource("b").unwrap(),
            ],
            0,
        )
        .unwrap();
    let commit = Commit::from_changes(
        tenant.clone(),
        publication_id(1),
        &observed,
        &[
            (10, transition::create(spec("a"), None).unwrap()),
            (10, transition::create(spec("b"), None).unwrap()),
        ],
    )
    .unwrap();
    assert!(matches!(
        store.commit(&commit).unwrap(),
        CommitOutcome::Applied(_)
    ));
    let history = store.history(&tenant).unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].records.len(), 2);
    assert_eq!(store.scan(&tenant).unwrap().read.records.len(), 2);
    let changed_identity = Commit::from_changes(
        tenant.clone(),
        publication_id(1),
        &observed,
        &[(10, transition::create(spec("a"), None).unwrap())],
    )
    .unwrap();
    assert!(store.commit(&changed_identity).is_err());
    assert_eq!(store.history(&tenant).unwrap(), history);
}

#[test]
fn readiness_refuses_durability_downgrade() {
    let store = MemoryAuthority::default();
    assert!(store.readiness().is_err());
    store.initialize().unwrap();
    let capabilities = store.readiness().unwrap();
    capabilities.require(DurabilityProfile::Simulation).unwrap();
    for profile in [
        DurabilityProfile::Local,
        DurabilityProfile::Regional,
        DurabilityProfile::EnterpriseMr,
    ] {
        let error = capabilities.require(profile).unwrap_err();
        assert!(
            error
                .message
                .contains("cannot serve required durability profile")
        );
    }
    let mut fake = capabilities;
    fake.durability_profile = DurabilityProfile::EnterpriseMr;
    assert!(fake.require(DurabilityProfile::EnterpriseMr).is_err());
}

#[test]
fn application_preserves_replays_lapses_and_namespace_boundaries() {
    use rescrow_application::transactional::Application;
    let store = FaultStore::new(MemoryAuthority::default());
    store.initialize().unwrap();
    let tenant = TenantId::new("tenant").unwrap();
    let app = |namespace: &str, now| {
        Application::new(store.clone())
            .with_scope(Scope::new(tenant.clone(), namespace).unwrap())
            .with_clock(move || now)
    };
    let a = app("a", 1000);
    let b = app("b", 1000);
    a.create_resource(dto::ResourceSpec::from(&spec("r")))
        .unwrap();
    let txid = uuid_v7(1000, 42);
    let request = dto::Prepare {
        transaction_id: txid.clone(),
        namespace: None,
        operations: vec![dto::Operation {
            operation_id: "op".into(),
            resource_id: "r".into(),
            delta: "-4".into(),
        }],
        hold_ms: Some(10),
    };
    store.inject(Fault::LostResponse);
    let prepared_a = a.prepare(request.clone()).unwrap();
    assert_eq!(prepared_a.request.namespace.as_deref(), Some("a"));
    let prepared_b = b.prepare(request.clone()).unwrap();
    assert_eq!(prepared_b.request.namespace.as_deref(), Some("b"));
    assert_eq!(a.resource("r").unwrap().available_down, "2");
    assert_eq!(a.prepare(request.clone()).unwrap(), prepared_a);
    assert_eq!(a.transactions().unwrap().len(), 1);
    assert_eq!(b.transactions().unwrap().len(), 1);
    a.commit(dto::Commit {
        transaction_id: txid.clone(),
        actuals: Some(vec![dto::Actual {
            operation_id: "op".into(),
            delta: "-2".into(),
        }]),
    })
    .unwrap();
    assert_eq!(b.transaction(&txid).unwrap(), prepared_b);
    let later = app("b", 1010);
    assert_eq!(
        later
            .commit(dto::Commit {
                transaction_id: txid.clone(),
                actuals: None
            })
            .unwrap_err()
            .code,
        rescrow_application::Code::Conflict
    );
    assert!(matches!(
        later.transaction(&txid).unwrap().state,
        dto::State::Aborted {
            cause: dto::AbortReason::Lapsed,
            ..
        }
    ));
    assert_eq!(later.resource("r").unwrap().quantity, "8");
    assert_eq!(later.resource("r").unwrap().reserved_down, "0");
    later.snapshot().unwrap();
    let other = Application::new(store.clone())
        .with_scope(Scope::new(TenantId::new("other").unwrap(), "a").unwrap());
    assert_eq!(
        other.resource("r").unwrap_err().code,
        rescrow_application::Code::NotFound
    );
    assert_eq!(
        other.transaction(&txid).unwrap_err().code,
        rescrow_application::Code::NotFound
    );
    assert!(other.snapshot().unwrap().resources.is_empty());
    let mut bad = request;
    bad.namespace = Some("a".into());
    assert_eq!(
        b.prepare(bad).unwrap_err().category,
        rescrow_application::Category::Input
    );
}

#[test]
fn ready_application_reports_and_enforces_observed_profile() {
    use rescrow_application::transactional::{Application, ReadyApplication};
    let store = MemoryAuthority::default();
    store.initialize().unwrap();
    assert!(
        ReadyApplication::start(
            Application::new(store.clone()),
            DurabilityProfile::EnterpriseMr
        )
        .is_err()
    );
    let ready =
        ReadyApplication::start(Application::new(store), DurabilityProfile::Simulation).unwrap();
    let response = ready
        .execute(dto::Command::ListResources, DurabilityProfile::Simulation)
        .unwrap();
    assert_eq!(response.durability_profile, DurabilityProfile::Simulation);
    assert_eq!(
        serde_json::to_value(response).unwrap()["durability_profile"],
        "simulation"
    );
    assert!(
        ready
            .execute(dto::Command::ListResources, DurabilityProfile::Local)
            .is_err()
    );
}

#[test]
fn unresolved_publication_is_not_retried_and_absent_resolution_fences_delayed_writes() {
    use rescrow_application::{Application, Code};
    let store = FaultStore::new(MemoryAuthority::default());
    let app = Application::new(store.clone()).with_clock(|| 1000);
    app.initialize().unwrap();
    app.create_resource(dto::ResourceSpec::from(&spec("r")))
        .unwrap();
    let request = dto::Prepare {
        transaction_id: uuid_v7(1000, 12),
        namespace: None,
        hold_ms: None,
        operations: vec![dto::Operation {
            operation_id: "op".into(),
            resource_id: "r".into(),
            delta: "-4".into(),
        }],
    };
    let before = store.evaluations();
    store.inject(Fault::LostResponseUnresolved);
    assert_eq!(
        app.prepare(request.clone()).unwrap_err().code,
        Code::UnknownOutcome
    );
    assert_eq!(store.evaluations(), before + 1);
    Application::new(store.clone())
        .with_clock(|| 1000)
        .prepare(request)
        .unwrap();
    assert_eq!(store.evaluations(), before + 1);
    assert_eq!(app.resource("r").unwrap().reserved_down, "4");
    let delayed = create(&store, &TenantId::local(), "delayed", 77);
    assert_eq!(
        store
            .resolve(&TenantId::local(), &publication_id(77))
            .unwrap(),
        Resolution::NotApplied
    );
    assert_eq!(store.commit(&delayed).unwrap(), CommitOutcome::Conflict);
    assert_eq!(app.resource("delayed").unwrap_err().code, Code::NotFound);
}
