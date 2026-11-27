use rescrow_adapters::authority_memory::MemoryAuthority;
use rescrow_application::{
    Application, Code,
    access::{AccessContext, Action, Caller, Grant, PendingLimits, Policy},
    authority::{RecordKey, TenantId, TransactionalStore, Value},
    dto, uuid_v7,
};
use std::sync::{Arc, Barrier};
const NOW: u64 = 1_800_000_000_000;
fn context(tenant: &str, namespace: &str, credential: &str, scopes: Vec<Action>) -> AccessContext {
    Caller {
        tenant: tenant.into(),
        namespace: Some(namespace.into()),
        issuer: "https://issuer.example".into(),
        subject: "workload".into(),
        credential_id: credential.into(),
        expires_at_s: NOW / 1000 + 3600,
        policy: Policy {
            scopes,
            resources: vec![Grant {
                resource_prefix: "r".into(),
                debit: true,
                credit: false,
                max_amount: "10".into(),
            }],
            max_hold_ms: None,
        },
    }
    .try_into()
    .unwrap()
}
fn all(tenant: &str, namespace: &str, credential: &str) -> AccessContext {
    context(
        tenant,
        namespace,
        credential,
        vec![
            Action::Create,
            Action::Prepare,
            Action::Commit,
            Action::Abort,
            Action::Read,
        ],
    )
}
fn create(id: &str) -> dto::Command {
    dto::Command::CreateResource(dto::ResourceSpec {
        resource_id: id.into(),
        lower: "0".into(),
        upper: Some("10".into()),
        initial: "10".into(),
        unit: "u".into(),
        scale: 0,
    })
}
fn prepare(n: u128, resource: &str) -> dto::Command {
    dto::Command::Prepare(dto::Prepare {
        transaction_id: uuid_v7(NOW, n),
        namespace: None,
        hold_ms: None,
        operations: vec![dto::Operation {
            operation_id: "op".into(),
            resource_id: resource.into(),
            delta: "-2".into(),
        }],
        context: Default::default(),
        authorization: None,
    })
}
#[test]
fn durable_tenant_and_credential_limits_do_not_count_replays_or_replace_facts() {
    let store = MemoryAuthority::default();
    let app = Application::new(store.clone())
        .with_clock(|| NOW)
        .with_pending_limits(PendingLimits {
            tenant: 2,
            credential: 1,
        })
        .unwrap();
    app.initialize().unwrap();
    let a = all("a", "workflow", "credential-a");
    let b = all("b", "workflow", "credential-a");
    for c in [&a, &b] {
        app.execute_authorized(c, create("r1")).unwrap();
        app.execute_authorized(c, prepare(1, "r1")).unwrap();
    }
    assert_eq!(
        app.execute_authorized(&a, prepare(2, "r1"))
            .unwrap_err()
            .code,
        Code::Throttled
    );
    let renewed = all("a", "workflow", "credential-b");
    assert!(
        app.execute_authorized(&renewed, prepare(1, "r1"))
            .unwrap()
            .replayed
    );
    let tx = app
        .scoped(a.scope().clone())
        .transaction(&uuid_v7(NOW, 1))
        .unwrap();
    assert_eq!(
        tx.request.authorization.unwrap().credential_id,
        a.facts().credential_id()
    );
    app.execute_authorized(&renewed, prepare(2, "r1")).unwrap();
    app.execute_authorized(
        &renewed,
        dto::Command::Abort {
            transaction_id: uuid_v7(NOW, 1),
        },
    )
    .unwrap();
    app.execute_authorized(&a, prepare(3, "r1")).unwrap();
    assert_eq!(
        app.scoped(b.scope().clone())
            .resource("r1")
            .unwrap()
            .reserved_down,
        "2"
    );
    app.scoped(a.scope().clone()).snapshot().unwrap();
    let read = store
        .read(&TenantId::new("a").unwrap(), &[RecordKey::Pending], 0)
        .unwrap();
    let Some(Value::Pending(counter)) =
        read.records[&RecordKey::Pending].as_ref().map(|v| &v.value)
    else {
        panic!("missing quota")
    };
    assert_eq!(counter.total, 2);
    let encoded =
        serde_json::to_string(&app.scoped(a.scope().clone()).snapshot().unwrap()).unwrap();
    assert!(!encoded.contains("https://issuer.example"));
    assert!(!encoded.contains("credential-a"));
}
#[test]
fn separate_instances_cannot_exceed_a_shared_quota_on_disjoint_resources() {
    let store = MemoryAuthority::default();
    store.initialize().unwrap();
    let caller = all("tenant", "workflow", "credential");
    let app = || {
        Application::new(store.clone())
            .with_clock(|| NOW)
            .with_pending_limits(PendingLimits {
                tenant: 1,
                credential: 1,
            })
            .unwrap()
    };
    for id in ["r1", "r2"] {
        app().execute_authorized(&caller, create(id)).unwrap();
    }
    let barrier = Arc::new(Barrier::new(2));
    let results = std::thread::scope(|scope| {
        let jobs: Vec<_> = ["r1", "r2"]
            .into_iter()
            .enumerate()
            .map(|(i, resource)| {
                let barrier = barrier.clone();
                let app = app();
                let caller = caller.clone();
                scope.spawn(move || {
                    barrier.wait();
                    app.execute_authorized(&caller, prepare(i as u128 + 1, resource))
                })
            })
            .collect();
        jobs.into_iter()
            .map(|j| j.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| r.as_ref().is_err_and(|e| e.code == Code::Throttled))
            .count(),
        1
    );
    assert_eq!(
        app()
            .scoped(caller.scope().clone())
            .snapshot()
            .unwrap()
            .transactions
            .len(),
        1
    );
}
#[test]
fn every_command_checks_current_permissions_and_namespace() {
    let store = MemoryAuthority::default();
    let app = Application::new(store).with_clock(|| NOW);
    app.initialize().unwrap();
    let admin = all("tenant", "workflow", "admin");
    app.execute_authorized(&admin, create("r")).unwrap();
    app.execute_authorized(&admin, prepare(1, "r")).unwrap();
    for action in [
        Action::Create,
        Action::Prepare,
        Action::Commit,
        Action::Abort,
        Action::Read,
    ] {
        let scopes = vec![
            Action::Create,
            Action::Prepare,
            Action::Commit,
            Action::Abort,
            Action::Read,
        ]
        .into_iter()
        .filter(|a| *a != action)
        .collect();
        let denied = context("tenant", "workflow", "admin", scopes);
        let command = match action {
            Action::Create => create("r2"),
            Action::Prepare => prepare(1, "r"),
            Action::Commit => dto::Command::Commit(dto::Commit {
                transaction_id: uuid_v7(NOW, 1),
                actuals: None,
            }),
            Action::Abort => dto::Command::Abort {
                transaction_id: uuid_v7(NOW, 1),
            },
            Action::Read => dto::Command::ShowTransaction {
                transaction_id: uuid_v7(NOW, 1),
            },
        };
        assert_eq!(
            app.execute_authorized(&denied, command).unwrap_err().code,
            Code::Forbidden
        );
    }
    let other = all("other", "workflow", "admin");
    assert_eq!(
        app.execute_authorized(
            &other,
            dto::Command::ShowTransaction {
                transaction_id: uuid_v7(NOW, 1)
            }
        )
        .unwrap_err()
        .code,
        Code::NotFound
    );
    let namespace = all("tenant", "other", "admin");
    assert_eq!(
        app.execute_authorized(
            &namespace,
            dto::Command::ShowTransaction {
                transaction_id: uuid_v7(NOW, 1)
            }
        )
        .unwrap_err()
        .code,
        Code::NotFound
    );
    assert_eq!(
        app.scoped(admin.scope().clone())
            .resource("r")
            .unwrap()
            .reserved_down,
        "2"
    );
}

#[test]
fn file_reopen_preserves_authority_identity_facts_and_atomic_quota_release() {
    use rescrow_adapters::file::FileStore;
    let root = std::env::temp_dir().join(format!(
        "rescrow-authorized-{}",
        rescrow_application::new_id().unwrap()
    ));
    let caller: AccessContext = Caller {
        tenant: "local".into(),
        namespace: None,
        issuer: "https://issuer.example".into(),
        subject: "workload".into(),
        credential_id: "credential".into(),
        expires_at_s: NOW / 1000 + 3600,
        policy: Policy {
            scopes: vec![
                Action::Create,
                Action::Prepare,
                Action::Commit,
                Action::Abort,
                Action::Read,
            ],
            resources: vec![Grant {
                resource_prefix: "r".into(),
                debit: true,
                credit: false,
                max_amount: "10".into(),
            }],
            max_hold_ms: None,
        },
    }
    .try_into()
    .unwrap();
    let app = || {
        Application::new(FileStore::new(&root).unwrap())
            .with_clock(|| NOW)
            .with_pending_limits(PendingLimits {
                tenant: 1,
                credential: 1,
            })
            .unwrap()
    };
    app().initialize().unwrap();
    let generation = TransactionalStore::identity(&FileStore::new(&root).unwrap()).unwrap();
    app().execute_authorized(&caller, create("r")).unwrap();
    app().execute_authorized(&caller, prepare(1, "r")).unwrap();
    assert!(
        app()
            .execute_authorized(&caller, prepare(1, "r"))
            .unwrap()
            .replayed
    );
    assert_eq!(
        app()
            .execute_authorized(&caller, prepare(2, "r"))
            .unwrap_err()
            .code,
        Code::Throttled
    );
    app()
        .execute_authorized(
            &caller,
            dto::Command::Commit(dto::Commit {
                transaction_id: uuid_v7(NOW, 1),
                actuals: None,
            }),
        )
        .unwrap();
    app().execute_authorized(&caller, prepare(2, "r")).unwrap();
    app()
        .execute_authorized(
            &caller,
            dto::Command::Abort {
                transaction_id: uuid_v7(NOW, 2),
            },
        )
        .unwrap();
    let mut command = prepare(3, "r");
    if let dto::Command::Prepare(request) = &mut command {
        request.hold_ms = Some(10);
    }
    app().execute_authorized(&caller, command).unwrap();
    let later = app().with_clock(|| NOW + 11);
    later.execute_authorized(&caller, prepare(4, "r")).unwrap();
    let snapshot = later.snapshot().unwrap();
    assert_eq!(snapshot.resources[0].quantity, "8");
    assert_eq!(snapshot.resources[0].reserved_down, "2");
    assert_eq!(
        TransactionalStore::identity(&FileStore::new(&root).unwrap()).unwrap(),
        generation
    );
    let raw = std::fs::read_to_string(root.join("live.json")).unwrap();
    assert!(!raw.contains("https://issuer.example"));
    assert!(!raw.contains("Bearer"));
    std::fs::remove_dir_all(root).unwrap();
}
