//! Capability-gated backend qualification. A declaration must have executable
//! evidence: declaring a feature without supplying its hook fails the suite.
use rescrow_application::{
    Application,
    authority::{Scope, TenantId, TransactionalStore},
    capabilities::Readiness,
    dto, uuid_v7,
};
pub struct Hooks<'a> {
    pub cross_process: Option<&'a dyn Fn()>,
    pub crash_reopen: Option<&'a dyn Fn()>,
}
pub fn run(store: &impl Readiness, hooks: Hooks<'_>) {
    let capabilities = store.readiness().unwrap();
    capabilities
        .require(capabilities.durability_profile)
        .unwrap();
    if capabilities.cross_process {
        hooks
            .cross_process
            .expect("cross-process qualification hook required")();
    }
    if capabilities.reopen_after_crash {
        hooks
            .crash_reopen
            .expect("crash/reopen qualification hook required")();
    }
}
/// Identical resource/reservation IDs in two tenants, and independent namespaces
/// sharing one tenant's capacity, through the same public Application API.
pub fn tenancy<S: TransactionalStore + Readiness + Clone>(store: S) {
    store.initialize().unwrap();
    if !store.readiness().unwrap().multi_tenant {
        return;
    }
    let app = |tenant, namespace| {
        Application::new(store.clone())
            .with_scope(Scope::new(TenantId::new(tenant).unwrap(), namespace).unwrap())
            .with_clock(crate::model::created)
    };
    let a = app("a", "one");
    let b = app("b", "one");
    let a2 = app("a", "two");
    let spec = dto::ResourceSpec {
        resource_id: "r".into(),
        lower: "0".into(),
        upper: Some("10".into()),
        initial: "10".into(),
        unit: "u".into(),
        scale: 0,
    };
    let id = uuid_v7(crate::model::created(), 123);
    let request = dto::Prepare {
        context: Default::default(),
        authorization: None,
        reservation_id: id.clone(),
        namespace: None,
        hold_ms: None,
        operations: vec![dto::Operation {
            operation_id: "op".into(),
            resource_id: "r".into(),
            delta: "-4".into(),
        }],
    };
    for scoped in [&a, &b] {
        scoped.create_resource(spec.clone()).unwrap();
        scoped.prepare(request.clone()).unwrap();
    }
    a2.prepare(request.clone()).unwrap();
    assert_eq!(a.resource("r").unwrap().reserved_down, "8");
    assert_eq!(b.resource("r").unwrap().reserved_down, "4");
    a.commit(dto::Commit {
        reservation_id: id.clone(),
        actuals: Some(vec![dto::Actual {
            operation_id: "op".into(),
            delta: "-2".into(),
        }]),
    })
    .unwrap();
    assert!(matches!(
        b.reservation(&id).unwrap().state,
        dto::State::Prepared {}
    ));
    a2.abort(&id).unwrap();
    assert_eq!(a.resource("r").unwrap().quantity, "8");
    assert_eq!(b.resource("r").unwrap().quantity, "10");
    assert_eq!(a.reservations().unwrap().len(), 1);
    assert_eq!(a2.reservations().unwrap().len(), 1);
    assert_eq!(b.reservations().unwrap().len(), 1);
    assert!(app("absent", "one").resources().unwrap().is_empty());
    assert_eq!(
        app("absent", "one").reservation(&id).unwrap_err().code,
        rescrow_application::Code::NotFound
    );
    a.snapshot().unwrap();
    b.snapshot().unwrap();
}
