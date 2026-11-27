//! A long-lived service: group commit, lock-free reads, and races with other writers.
use rescrow_adapters::memory::{Fault, MemoryStore};
use rescrow_application::{
    Application, Category, Code, Loaded, LogStore, Publication, StoreError, dto::*,
    service::Service, system_clock, uuid_v7,
};
use std::{
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

/// A memory store whose appends take a while, so writes queue behind each other.
#[derive(Clone, Default)]
struct Slow {
    inner: MemoryStore,
    delay: Arc<AtomicBool>,
}
const DELAY: Duration = Duration::from_millis(50);
impl LogStore for Slow {
    fn initialize(&self, c: &Checkpoint) -> Result<(), StoreError> {
        self.inner.initialize(c)
    }
    fn load(&self) -> Result<Loaded, StoreError> {
        self.inner.load()
    }
    /// Appends, then waits, like a slow synchronization.
    fn append(&self, entries: &[Entry]) -> Result<Publication, StoreError> {
        let result = self.inner.append(entries);
        if self.delay.load(Ordering::Relaxed) {
            thread::sleep(DELAY);
        }
        result
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
fn create(initial: &str) -> Command {
    Command::CreateResource(ResourceSpec {
        resource_id: "r".into(),
        lower: "0".into(),
        upper: None,
        initial: initial.into(),
        unit: "u".into(),
        scale: 0,
    })
}
fn prepare(id: &str) -> Command {
    Command::Prepare(Prepare {
        context: Default::default(),
        authorization: None,
        namespace: None,
        transaction_id: id.into(),
        operations: vec![Operation {
            operation_id: "o".into(),
            resource_id: "r".into(),
            delta: "-1".into(),
        }],
        hold_ms: None,
    })
}
fn held(id: &str, hold_ms: u64) -> Command {
    let Command::Prepare(mut request) = prepare(id) else {
        unreachable!()
    };
    request.hold_ms = Some(hold_ms);
    Command::Prepare(request)
}
fn show<S: LogStore + Send + Sync + 'static>(service: &Service<S>) -> (String, String) {
    quantity(
        service
            .execute(Command::ShowResource {
                resource_id: "r".into(),
            })
            .unwrap(),
    )
}
fn lapsed(output: Output) -> bool {
    matches!(
        output,
        Output::Transaction(Transaction {
            state: State::Aborted {
                cause: AbortReason::Lapsed,
                ..
            },
            ..
        })
    )
}
/// Entries in the store's history that lapse `id`.
fn lapses(store: &MemoryStore, id: &str) -> usize {
    store
        .history()
        .unwrap()
        .into_iter()
        .filter(|e| match &e.record {
            Record::Transaction(tx) => {
                tx.request.transaction_id == id
                    && matches!(
                        tx.state,
                        State::Aborted {
                            cause: AbortReason::Lapsed,
                            ..
                        }
                    )
            }
            _ => false,
        })
        .count()
}
/// A service and a clock it reads, which moves only when the test moves it.
fn clocked(store: &MemoryStore, now: u64) -> (Service<MemoryStore>, Arc<AtomicU64>) {
    let app = Application::new(store.clone());
    app.initialize().unwrap();
    app.execute(create("5")).unwrap();
    let clock = Arc::new(AtomicU64::new(now));
    let read = clock.clone();
    let app = Application::new(store.clone()).with_clock(move || read.load(Ordering::Relaxed));
    (Service::start(app).unwrap(), clock)
}
fn id(n: u128) -> String {
    uuid_v7(system_clock(), n)
}
fn ready<S: LogStore + Clone + Send + Sync + 'static>(store: &S, initial: &str) -> Service<S> {
    let app = Application::new(store.clone());
    app.initialize().unwrap();
    app.execute(create(initial)).unwrap();
    Service::start(Application::new(store.clone())).unwrap()
}
fn quantity(output: Output) -> (String, String) {
    let Output::Resource(r) = output else {
        panic!("expected a resource")
    };
    (r.quantity, r.reserved_down)
}

#[test]
fn concurrent_writes_commit_together_without_over_reserving() {
    let store = Slow::default();
    let service = Arc::new(ready(&store, "5"));
    store.delay.store(true, Ordering::Relaxed);
    let start = Arc::new(Barrier::new(40));
    let results: Vec<_> = (0..40)
        .map(|n| {
            let (service, start) = (service.clone(), start.clone());
            thread::spawn(move || {
                start.wait();
                service.execute(prepare(&id(n)))
            })
        })
        .collect::<Vec<_>>()
        .into_iter()
        .map(|t| t.join().unwrap())
        .collect();
    let admitted = results.iter().filter(|r| r.is_ok()).count();
    assert_eq!(admitted, 5);
    assert!(
        results
            .iter()
            .filter_map(|r| r.as_ref().err())
            .all(|e| e.code == Code::Capacity)
    );
    // Writes that waited for one append were appended together by the next.
    let stats = service.stats();
    let (batches, writes) = (
        stats.batches.load(Ordering::Relaxed),
        stats.writes.load(Ordering::Relaxed),
    );
    assert_eq!(writes, 40);
    assert!(batches < 10, "{batches} batches for {writes} writes");
    // Every rejection is recorded, and the in-memory state matches history.
    let snapshot = Application::new(store.clone()).snapshot().unwrap();
    assert_eq!(snapshot.transactions.len(), 40);
    assert_eq!(snapshot.resources[0].reserved_down, "5");
    assert_eq!(
        service.execute(Command::ListResources).unwrap(),
        Output::Resources(snapshot.resources)
    );
}
#[test]
fn reads_do_not_wait_for_a_slow_append() {
    let store = Slow::default();
    let service = Arc::new(ready(&store, "5"));
    store.delay.store(true, Ordering::Relaxed);
    let writing = {
        let service = service.clone();
        thread::spawn(move || service.execute(prepare(&id(0))).unwrap())
    };
    thread::sleep(DELAY / 5);
    let start = Instant::now();
    let read = service.execute(Command::ShowResource {
        resource_id: "r".into(),
    });
    assert!(start.elapsed() < DELAY / 5, "{:?}", start.elapsed());
    // The read sees the last durable state, not the write still appending.
    assert_eq!(quantity(read.unwrap()), ("5".into(), "0".into()));
    writing.join().unwrap();
    let read = service.execute(Command::ShowResource {
        resource_id: "r".into(),
    });
    assert_eq!(quantity(read.unwrap()), ("5".into(), "1".into()));
}
#[test]
fn another_writer_is_detected_and_reloaded() {
    let store = MemoryStore::default();
    let service = ready(&store, "2");
    let other = Application::new(store.clone());
    let theirs = id(1);
    other.execute(prepare(&theirs)).unwrap();
    // Reads lag the other writer until this service next appends.
    let show = || {
        quantity(
            service
                .execute(Command::ShowResource {
                    resource_id: "r".into(),
                })
                .unwrap(),
        )
    };
    assert_eq!(show(), ("2".into(), "0".into()));
    let evaluations = store.evaluations();
    service.execute(prepare(&id(2))).unwrap();
    // The first append lost the race; the retry was evaluated with their hold.
    assert_eq!(store.evaluations() - evaluations, 2);
    assert_eq!(show(), ("2".into(), "2".into()));
    assert_eq!(
        service.execute(prepare(&id(3))).unwrap_err().code,
        Code::Capacity
    );
    // A transaction the other writer prepared is visible to settle.
    service
        .execute(Command::Abort {
            transaction_id: theirs,
        })
        .unwrap();
    assert_eq!(show(), ("2".into(), "1".into()));
}
#[test]
fn an_unknown_outcome_reaches_every_write_in_its_batch_and_reloads() {
    let store = Slow::default();
    let service = Arc::new(ready(&store, "10"));
    store.delay.store(true, Ordering::Relaxed);
    // Hold the writer in a first append, so the next writes queue together.
    let first = {
        let service = service.clone();
        thread::spawn(move || service.execute(prepare(&id(0))).unwrap())
    };
    thread::sleep(DELAY / 5);
    store.inner.inject(Fault::LostResponseUnresolved);
    let ids: Vec<_> = (1..=4).map(id).collect();
    let queued: Vec<_> = ids
        .iter()
        .map(|id| {
            let (service, id) = (service.clone(), id.clone());
            thread::spawn(move || service.execute(prepare(&id)))
        })
        .collect();
    first.join().unwrap();
    let outcomes: Vec<_> = queued.into_iter().map(|t| t.join().unwrap()).collect();
    let unknown = outcomes
        .iter()
        .filter(|r| {
            r.as_ref()
                .is_err_and(|e| e.category == Category::UnknownOutcome)
        })
        .count();
    assert!(unknown >= 1);
    // Retrying the same requests settles them, with the state reloaded.
    store.delay.store(false, Ordering::Relaxed);
    for id in &ids {
        service.execute(prepare(id)).unwrap();
    }
    let snapshot = Application::new(store.clone()).snapshot().unwrap();
    assert_eq!(snapshot.transactions.len(), 5);
    assert_eq!(snapshot.resources[0].reserved_down, "5");
}

#[test]
fn an_idle_service_lapses_a_hold_at_its_deadline() {
    let store = MemoryStore::default();
    let service = ready(&store, "5");
    let tx = id(0);
    service.execute(held(&tx, 50)).unwrap();
    assert_eq!(show(&service), ("5".into(), "1".into()));
    // Nothing else is written; reads alone see the capacity come back.
    let start = Instant::now();
    while show(&service).1 != "0" {
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "the hold never lapsed"
        );
        thread::sleep(Duration::from_millis(5));
    }
    let show_tx = Command::ShowTransaction {
        transaction_id: tx.clone(),
    };
    assert!(lapsed(service.execute(show_tx.clone()).unwrap()));
    // The lapse is a durable decision, which replay accepts.
    assert!(lapsed(
        Application::new(store.clone()).execute(show_tx).unwrap()
    ));
    assert_eq!(lapses(&store, &tx), 1);
}
#[test]
fn a_hold_another_writer_lapsed_is_not_lapsed_again() {
    let store = MemoryStore::default();
    let now = system_clock();
    let (service, clock) = clocked(&store, now);
    let tx = id(0);
    service.execute(held(&tx, 10)).unwrap();
    // Another process, whose clock has passed the deadline, writes the
    // resource and so lapses the hold. The service hasn't seen it.
    let other = Application::new(store.clone()).with_clock(move || now + 1000);
    other.execute(prepare(&id(1))).unwrap();
    assert_eq!(lapses(&store, &tx), 1);
    // Now the service's clock passes the deadline too. Its timer, or this
    // lapse, loses the race to append, reloads, and finds nothing due.
    clock.store(now + 1000, Ordering::Relaxed);
    assert_eq!(service.lapse().unwrap(), vec![]);
    assert_eq!(lapses(&store, &tx), 1);
    assert_eq!(show(&service), ("5".into(), "1".into()));
}
#[test]
fn a_failed_lapse_is_retried_after_a_pause() {
    let store = MemoryStore::default();
    let now = system_clock();
    let (service, clock) = clocked(&store, now);
    let tx = id(0);
    service.execute(held(&tx, 10)).unwrap();
    let appends = store.evaluations();
    store.inject(Fault::BeforePublication);
    clock.store(now + 100, Ordering::Relaxed);
    let start = Instant::now();
    while lapses(&store, &tx) == 0 {
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "the hold never lapsed"
        );
        thread::sleep(Duration::from_millis(5));
    }
    // One failed append, then one that lapsed it a second later, not a spin.
    assert_eq!(store.evaluations() - appends, 2);
    assert!(start.elapsed() >= Duration::from_millis(900));
    assert_eq!(show(&service), ("5".into(), "0".into()));
}
