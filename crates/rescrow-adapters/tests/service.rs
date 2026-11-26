//! A long-lived service: group commit, lock-free reads, and races with other writers.
use rescrow_adapters::memory::{Fault, MemoryStore};
use rescrow_application::{
    Application, Category, Code, Loaded, LogStore, Publication, StoreError, dto::*,
    service::Service, system_clock, uuid_v7,
};
use std::{
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, Ordering},
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
        transaction_id: id.into(),
        operations: vec![Operation {
            operation_id: "o".into(),
            resource_id: "r".into(),
            delta: "-1".into(),
        }],
        hold_ms: None,
    })
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
    store.inner.inject(Fault::LostResponse);
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
