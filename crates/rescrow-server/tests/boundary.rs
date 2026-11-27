use rescrow_adapters::authority_memory::MemoryAuthority;
use rescrow_application::{
    StoreError, authority::*, capabilities::*, dto, transactional::Application,
};
use rescrow_server::Server;
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

#[derive(Clone)]
struct Gated {
    inner: MemoryAuthority,
    entered: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
    gate: Arc<(Mutex<bool>, Condvar)>,
    reads: Arc<AtomicUsize>,
    panic_after: Arc<AtomicBool>,
}
impl TransactionalStore for Gated {
    fn initialize(&self) -> Result<(), StoreError> {
        self.inner.initialize()
    }
    fn read(&self, t: &TenantId, keys: &[RecordKey], since: u64) -> Result<Read, StoreError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.read(t, keys, since)
    }
    fn scan(&self, t: &TenantId) -> Result<Inspection, StoreError> {
        self.inner.scan(t)
    }
    fn commit(&self, c: &Commit) -> Result<CommitOutcome, StoreError> {
        let entered = self.entered.lock().unwrap().take();
        if let Some(entered) = entered {
            let _ = entered.send(());
            let (lock, cv) = &*self.gate;
            let _ = cv
                .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(2), |open| !*open)
                .unwrap();
        }
        let result = self.inner.commit(c);
        assert!(
            !self.panic_after.swap(false, Ordering::SeqCst),
            "injected panic after publication"
        );
        result
    }
    fn resolve(&self, t: &TenantId, id: &TransitionId) -> Result<Resolution, StoreError> {
        self.inner.resolve(t, id)
    }
    fn history(&self, t: &TenantId) -> Result<Vec<HistoryRecord>, StoreError> {
        self.inner.history(t)
    }
}
impl Readiness for Gated {
    fn readiness(&self) -> Result<Capabilities, StoreError> {
        self.inner.readiness()
    }
}
fn create(name: &str) -> dto::Command {
    dto::Command::CreateResource(dto::ResourceSpec {
        resource_id: name.into(),
        lower: "0".into(),
        upper: None,
        initial: "10".into(),
        unit: "u".into(),
        scale: 0,
    })
}
#[tokio::test(flavor = "current_thread")]
async fn blocked_io_and_cancelled_requests_do_not_block_workers_or_release_capacity_early() {
    let (entered, ready) = tokio::sync::oneshot::channel();
    let store = Gated {
        inner: MemoryAuthority::default(),
        entered: Arc::new(Mutex::new(Some(entered))),
        gate: Arc::new((Mutex::new(false), Condvar::new())),
        reads: Arc::default(),
        panic_after: Arc::default(),
    };
    store.initialize().unwrap();
    let server = Server::start(
        Application::new(store.clone()),
        DurabilityProfile::Simulation,
        1,
    )
    .unwrap();
    let begin = Instant::now();
    let first = {
        let server = server.clone();
        tokio::spawn(async move {
            server
                .execute(create("first"), DurabilityProfile::Simulation)
                .await
        })
    };
    ready.await.unwrap();
    // The current-thread runtime can still drive timers while storage blocks.
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(begin.elapsed() < Duration::from_secs(1));
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let reads = store.reads.load(Ordering::SeqCst);
    let second = {
        let server = server.clone();
        tokio::spawn(async move {
            server
                .execute(create("second"), DurabilityProfile::Simulation)
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(store.reads.load(Ordering::SeqCst), reads);
    assert!(!second.is_finished());
    let (lock, cv) = &*store.gate;
    *lock.lock().unwrap() = true;
    cv.notify_all();
    second.await.unwrap().unwrap();
    let response = server
        .execute(dto::Command::ListResources, DurabilityProfile::Simulation)
        .await
        .unwrap();
    assert_eq!(response.durability_profile, DurabilityProfile::Simulation);
    let dto::Output::Resources(resources) = response.output else {
        unreachable!()
    };
    assert_eq!(resources.len(), 2);
    assert!(
        server
            .execute(create("rejected"), DurabilityProfile::Local)
            .await
            .is_err()
    );
    assert_eq!(
        store.scan(&TenantId::local()).unwrap().read.records.len(),
        2
    );
}

#[tokio::test]
async fn an_unknown_task_delivery_keeps_the_original_transaction_identity() {
    use rescrow_application::{Code, system_clock, uuid_v7};
    let store = Gated {
        inner: MemoryAuthority::default(),
        entered: Arc::new(Mutex::new(None)),
        gate: Arc::new((Mutex::new(true), Condvar::new())),
        reads: Arc::default(),
        panic_after: Arc::default(),
    };
    let app = Application::new(store.clone());
    app.initialize().unwrap();
    app.execute(create("r")).unwrap();
    let server = Server::start(app, DurabilityProfile::Simulation, 1).unwrap();
    let id = uuid_v7(system_clock(), 77);
    let command = dto::Command::Prepare(dto::Prepare {
        transaction_id: id.clone(),
        namespace: None,
        hold_ms: None,
        operations: vec![dto::Operation {
            operation_id: "op".into(),
            resource_id: "r".into(),
            delta: "-1".into(),
        }],
    });
    store.panic_after.store(true, Ordering::SeqCst);
    let error = server
        .execute(command.clone(), DurabilityProfile::Simulation)
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::UnknownOutcome);
    assert_eq!(error.transaction_id, Some(id));
    server
        .execute(command, DurabilityProfile::Simulation)
        .await
        .unwrap();
    assert_eq!(
        Application::new(store).resource("r").unwrap().reserved_down,
        "1"
    );
}
