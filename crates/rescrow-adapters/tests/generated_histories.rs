//! Sequential model checking, not OS-schedule replay or a throughput benchmark.
//! Commands are generated independently of the SUT. Selection of existing IDs and
//! actuals consults only the reference model, keeping histories useful while shrinking.
//! The store's clock is the model's, and advances only between commands.
#[path = "support/escrow_model.rs"]
mod model;
use model::{Command, Failure, Model, Op, Spec, State, tx_id};
use proptest::{
    prelude::*,
    test_runner::{FileFailurePersistence, TestCaseError},
};
use rescrow_adapters::memory::{Fault, MemoryStore};
use rescrow_application::{Application, Category, Code, Error, dto, service::Service};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

type TestResult = Result<(), TestCaseError>;
#[derive(Clone, Debug)]
enum Target {
    Any(u8),
    Prepared(u8),
    Terminal(u8),
    Missing(u8),
}
#[derive(Clone, Debug)]
enum Actuals {
    Full,
    Zero,
    Fraction(u8),
    Replay,
    Overuse,
    Reverse,
    Missing,
    Duplicate,
    Unknown,
}
#[derive(Clone, Debug)]
enum Action {
    Create(u8, Spec),
    Recreate(u8),
    Prepare {
        fresh: bool,
        slot: u8,
        operations: Vec<Op>,
        hold: Option<u64>,
    },
    Replay {
        target: Target,
        reverse: bool,
        change: bool,
        rehold: bool,
    },
    /// Advance the clock by this many milliseconds.
    Wait(u64),
    Commit(Target, Actuals),
    Abort(Target),
    InspectResource(u8),
    InspectTransaction(Target),
    InspectAll(bool),
}
#[derive(Clone, Copy, Debug)]
enum Effect {
    Clean,
    Conflicts,
    BeforePublication,
    LostResponse,
}
#[derive(Clone, Debug)]
struct Step {
    action: Action,
    effect: Effect,
    reload: bool,
}

fn spec() -> impl Strategy<Value = Spec> {
    (
        -6_i64..=6,
        0_i64..=12,
        any::<bool>(),
        any::<u8>(),
        0_u8..2,
        0_u8..3,
    )
        .prop_map(|(lower, width, bounded, offset, unit, scale)| Spec {
            lower,
            upper: bounded.then_some(lower + width),
            initial: lower + i64::from(offset) % (width + 1),
            unit,
            scale,
        })
}
fn target() -> impl Strategy<Value = Target> {
    prop_oneof![2=>(0_u8..32).prop_map(Target::Any),4=>(0_u8..32).prop_map(Target::Prepared),2=>(0_u8..32).prop_map(Target::Terminal),1=>(0_u8..8).prop_map(Target::Missing)]
}
fn operations() -> impl Strategy<Value = Vec<Op>> {
    let delta = prop_oneof![-8_i64..=-1, 1_i64..=8];
    prop_oneof![
        6=>prop::collection::btree_map(0_u8..4,delta,1..=3).prop_map(|entries|entries.into_iter().map(|(resource,delta)|Op {id:resource,resource,delta}).collect()),
        2=>prop::collection::vec((0_u8..4,0_u8..4,-8_i64..=8),0..=4).prop_map(|ops|ops.into_iter().map(|(id,resource,delta)|Op {id,resource,delta}).collect()),
    ]
}
/// Mostly short holds that lapse within a history, sometimes none, and
/// occasionally an invalid one.
fn hold() -> impl Strategy<Value = Option<u64>> {
    prop_oneof![
        4=>Just(None),
        6=>(1_u64..=40).prop_map(Some),
        1=>prop_oneof![Just(0), Just(30 * 24 * 60 * 60 * 1000 + 1)].prop_map(Some),
    ]
}
fn step() -> impl Strategy<Value = Step> {
    let actuals = prop_oneof![3=>Just(Actuals::Full),2=>Just(Actuals::Zero),3=>(0_u8..=3).prop_map(Actuals::Fraction),3=>Just(Actuals::Replay),1=>Just(Actuals::Overuse),1=>Just(Actuals::Reverse),1=>Just(Actuals::Missing),1=>Just(Actuals::Duplicate),1=>Just(Actuals::Unknown)];
    let action = prop_oneof![
        2=>(0_u8..4,spec()).prop_map(|(id,spec)|Action::Create(id,spec)),
        1=>(0_u8..4).prop_map(Action::Recreate),
        7=>(any::<bool>(),0_u8..8,operations(),hold()).prop_map(|(fresh,slot,operations,hold)|Action::Prepare {fresh,slot,operations,hold}),
        4=>(target(),any::<bool>(),any::<bool>(),prop::bool::weighted(0.2)).prop_map(|(target,reverse,change,rehold)|Action::Replay {target,reverse,change,rehold}),
        3=>(0_u64..=20).prop_map(Action::Wait),
        6=>(target(),actuals).prop_map(|(t,a)|Action::Commit(t,a)),
        4=>target().prop_map(Action::Abort),
        1=>(0_u8..4).prop_map(Action::InspectResource),
        1=>target().prop_map(Action::InspectTransaction),
        1=>any::<bool>().prop_map(Action::InspectAll),
    ];
    (action,prop_oneof![6=>Just(Effect::Clean),2=>Just(Effect::Conflicts),1=>Just(Effect::BeforePublication),1=>Just(Effect::LostResponse)],any::<bool>()).prop_map(|(action,effect,reload)|Step {action,effect,reload})
}
impl Model {
    fn missing(&self, slot: u8) -> u16 {
        let mut id = 4000 + u16::from(slot);
        while self.transactions.contains_key(&id) {
            id += 1;
        }
        id
    }
    fn select(&self, target: &Target) -> u16 {
        let (index, filter): (u8, fn(&State) -> bool) = match target {
            Target::Any(i) => (*i, |_| true),
            Target::Prepared(i) => (*i, |s| *s == State::Prepared),
            Target::Terminal(i) => (*i, |s| *s != State::Prepared),
            Target::Missing(i) => return self.missing(*i),
        };
        let candidates: Vec<_> = self
            .transactions
            .iter()
            .filter(|(_, tx)| filter(&tx.state))
            .map(|(id, _)| *id)
            .collect();
        if candidates.is_empty() {
            self.missing(index)
        } else {
            candidates[usize::from(index) % candidates.len()]
        }
    }
    /// The command an action issues, or `None` for one that only waits.
    fn command(&self, action: &Action, index: usize) -> Option<Command> {
        Some(match action {
            Action::Wait(_) => return None,
            Action::Create(id, spec) => Command::Create(*id, spec.clone()),
            Action::Recreate(id) => Command::Create(
                *id,
                self.resources.get(id).cloned().unwrap_or(Spec {
                    lower: 0,
                    upper: Some(10),
                    initial: 5,
                    unit: 0,
                    scale: 0,
                }),
            ),
            Action::Prepare {
                fresh,
                slot,
                operations,
                hold,
            } => Command::Prepare(
                if *fresh {
                    1000 + index as u16
                } else {
                    u16::from(*slot)
                },
                operations.clone(),
                *hold,
            ),
            Action::Replay {
                target,
                reverse,
                change,
                rehold,
            } => {
                let id = self.select(target);
                let (mut operations, mut hold) = self
                    .transactions
                    .get(&id)
                    .map(|tx| (tx.operations.clone(), tx.hold))
                    .unwrap_or_else(|| {
                        (
                            vec![Op {
                                id: 0,
                                resource: 0,
                                delta: 1,
                            }],
                            None,
                        )
                    });
                if *rehold {
                    hold = match hold {
                        None => Some(10),
                        Some(_) => None,
                    };
                }
                if *change {
                    operations[0].delta = if operations[0].delta == 1 { 2 } else { 1 };
                }
                if *reverse {
                    operations.reverse();
                }
                Command::Prepare(id, operations, hold)
            }
            Action::Commit(target, mode) => {
                let id = self.select(target);
                let Some(tx) = self.transactions.get(&id) else {
                    return Some(Command::Commit(id, None));
                };
                if matches!(mode, Actuals::Full) {
                    return Some(Command::Commit(id, None));
                }
                let mut actuals: Vec<_> = tx.operations.iter().map(|o| (o.id, o.delta)).collect();
                match mode {
                    Actuals::Zero => actuals.iter_mut().for_each(|(_, d)| *d = 0),
                    Actuals::Fraction(n) => actuals
                        .iter_mut()
                        .for_each(|(_, d)| *d = *d * i64::from(*n) / 3),
                    Actuals::Replay => {
                        if let State::Committed(old) = &tx.state {
                            actuals = old.clone();
                        }
                    }
                    Actuals::Overuse => actuals[0].1 += actuals[0].1.signum(),
                    Actuals::Reverse => actuals[0].1 = -actuals[0].1,
                    Actuals::Missing => {
                        actuals.pop();
                    }
                    Actuals::Duplicate => actuals.push(actuals[0]),
                    Actuals::Unknown => actuals[0].0 = 99,
                    Actuals::Full => unreachable!(),
                }
                Command::Commit(id, Some(actuals))
            }
            Action::Abort(target) => Command::Abort(self.select(target)),
            Action::InspectResource(id) => Command::Resource(*id),
            Action::InspectTransaction(t) => Command::Transaction(self.select(t)),
            Action::InspectAll(resources) => {
                if *resources {
                    Command::Resources
                } else {
                    Command::Transactions
                }
            }
        })
    }
}

struct Harness {
    model: Model,
    store: MemoryStore,
    /// The store's clock, kept equal to the model's.
    clock: Arc<AtomicU64>,
    /// One long-lived service for every command, or a new Application for each.
    service: Option<Service<MemoryStore>>,
}
impl Harness {
    fn new(service: bool) -> Self {
        let store = MemoryStore::default();
        Application::new(store.clone()).initialize().unwrap();
        // Requests are created when the process starts, so they are admitted.
        let now = model::created();
        let mut harness = Self {
            model: Model {
                now,
                ..Model::default()
            },
            store,
            clock: Arc::new(AtomicU64::new(now)),
            service: None,
        };
        if service {
            harness.service = Some(Service::start(harness.app()).unwrap());
        }
        harness
    }
    fn execute(&self, command: dto::Command) -> Result<dto::Output, Error> {
        match &self.service {
            Some(service) => service.execute(command),
            None => self.app().execute(command),
        }
    }
    /// A frequent checkpoint interval exercises replay from many positions.
    fn app(&self) -> Application<MemoryStore> {
        let clock = self.clock.clone();
        Application::new(self.store.clone())
            .with_checkpoint_interval(3)
            .with_clock(move || clock.load(Ordering::Relaxed))
    }
    /// Advance the clock. A service lapses every hold this makes due, at a
    /// time of its own choosing; lapsing them now keeps the history exact.
    /// Its timer may get there first, which leaves the same state, and finds
    /// nothing more afterwards, because the clock moves only here.
    fn wait(&mut self, ms: u64) -> TestResult {
        self.model.now += ms;
        self.clock.store(self.model.now, Ordering::Relaxed);
        let Some(service) = &self.service else {
            return Ok(());
        };
        let lapsed = service
            .lapse()
            .map_err(|e| TestCaseError::fail(format!("unexpected lapse error: {e}")))?;
        let expected: Vec<_> = self.model.lapse_all().into_iter().map(tx_id).collect();
        for tx in lapsed {
            prop_assert!(expected.contains(&tx.request.transaction_id), "{:?}", tx);
        }
        prop_assert_eq!(self.snapshot()?, self.model.snapshot(self.model.position));
        Ok(())
    }
    fn snapshot(&self) -> Result<dto::Snapshot, TestCaseError> {
        self.app()
            .snapshot()
            .map_err(|e| TestCaseError::fail(format!("unexpected load error: {e}")))
    }
    fn apply(&mut self, command: Command, effect: Effect, reload: bool) -> TestResult {
        let mut next = self.model.clone();
        let expected = next.apply(&command);
        let changed = next != self.model;
        let evaluations = self.store.evaluations();
        // Inject at a publication point predicted by the model. Never leave a
        // queued write fault behind after a malformed request, read, or replay.
        if changed {
            match effect {
                Effect::Clean => {}
                Effect::Conflicts => {
                    self.store.inject(Fault::Conflict);
                    self.store.inject(Fault::Conflict);
                }
                Effect::BeforePublication => self.store.inject(Fault::BeforePublication),
                Effect::LostResponse => self.store.inject(Fault::LostResponse),
            }
        }
        let mut actual = self.execute(command.dto());
        if changed && matches!(effect, Effect::BeforePublication | Effect::LostResponse) {
            let error = actual
                .as_ref()
                .expect_err("injected effect failure must be surfaced");
            let (code, category) = if matches!(effect, Effect::BeforePublication) {
                (Code::Io, Category::Storage)
            } else {
                (Code::UnknownOutcome, Category::UnknownOutcome)
            };
            prop_assert_eq!(
                (&error.code, &error.category),
                (&code, &category),
                "{:?}",
                command
            );
            prop_assert_eq!(
                error.transaction_id.clone(),
                command.transaction_id().map(tx_id)
            );
            if matches!(effect, Effect::BeforePublication) {
                prop_assert_eq!(self.snapshot()?, self.model.snapshot(self.model.position));
            } else {
                prop_assert_eq!(self.snapshot()?, next.snapshot(next.position));
            }
            // A new host retries the identical command, including rejected admissions.
            actual = self.execute(command.dto());
        }
        // Lapses are entries too, appended ahead of the command's own.
        let revision = next.position;
        match expected {
            Ok(()) => prop_assert_eq!(
                actual.map_err(|e| TestCaseError::fail(format!(
                    "unexpected error: {e}; {command:?}"
                )))?,
                next.output(&command, revision),
                "{:?}",
                command
            ),
            Err(failure) => check_error(
                actual.err().ok_or_else(|| {
                    TestCaseError::fail(format!(
                        "expected {failure:?}, got success for {command:?}"
                    ))
                })?,
                &failure,
                &command,
            )?,
        }
        let attempts = if !changed {
            0
        } else {
            match effect {
                Effect::Clean | Effect::LostResponse => 1,
                Effect::BeforePublication => 2,
                Effect::Conflicts => 3,
            }
        };
        prop_assert_eq!(
            self.store.evaluations() - evaluations,
            attempts,
            "{:?}",
            command
        );
        let observed = self.snapshot()?;
        let expected_snapshot = next.snapshot(revision);
        prop_assert_eq!(&observed, &expected_snapshot, "{:?}", command);
        if reload {
            // Real typed JSON round trip and inbound validation, without claiming
            // that resetting a memory adapter tests disk/process durability.
            let (checkpoint, entries) = self.store.raw();
            let encoded = serde_json::to_vec(&(checkpoint.unwrap(), entries)).unwrap();
            let (checkpoint, entries) = serde_json::from_slice(&encoded).unwrap();
            self.store.set_raw(checkpoint, entries);
            prop_assert_eq!(self.snapshot()?, expected_snapshot);
        }
        self.model = next;
        Ok(())
    }
}
fn check_error(error: Error, failure: &Failure, command: &Command) -> TestResult {
    let (code, category) = match failure {
        Failure::MalformedPrepare => (Code::InvalidInput, Category::Input),
        Failure::InvalidActuals => (Code::InvalidInput, Category::Domain),
        Failure::Missing => (Code::NotFound, Category::Domain),
        Failure::Conflict => (Code::Conflict, Category::Domain),
        Failure::Capacity { .. } => (Code::Capacity, Category::Domain),
    };
    prop_assert_eq!(
        (error.code, error.category),
        (code, category),
        "{:?}",
        command
    );
    prop_assert_eq!(error.transaction_id, command.transaction_id().map(tx_id));
    prop_assert_eq!(error.capacity.as_deref().cloned(), failure.capacity());
    Ok(())
}

fn history(service: bool, initial: Vec<Spec>, steps: Vec<Step>) -> TestResult {
    let mut h = Harness::new(service);
    for (id, spec) in initial.into_iter().enumerate() {
        h.apply(Command::Create(id as u8, spec), Effect::Clean, false)?;
    }
    for (index, step) in steps.into_iter().enumerate() {
        match h.model.command(&step.action, index) {
            Some(command) => h.apply(command, step.effect, step.reload)?,
            None => {
                if let Action::Wait(ms) = step.action {
                    h.wait(ms)?;
                }
            }
        }
    }
    h.apply(Command::Resources, Effect::Clean, true)?;
    h.apply(Command::Transactions, Effect::Clean, false)
}
proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: Some(Box::new(FileFailurePersistence::Direct(concat!(
            env!("CARGO_MANIFEST_DIR"), "/proptest-regressions/generated_histories.txt"
        )))),
        ..ProptestConfig::default()
    })]
    #[test]
    fn generated_histories_match_reference_model(
        initial in prop::collection::vec(spec(), 2..=3),
        steps in prop::collection::vec(step(), 1..=128),
    ) {
        history(false, initial, steps)?;
    }
    #[test]
    fn generated_histories_through_a_service_match_reference_model(
        initial in prop::collection::vec(spec(), 2..=3),
        steps in prop::collection::vec(step(), 1..=128),
    ) {
        history(true, initial, steps)?;
    }
    #[test]
    fn generated_lifecycles_preserve_identity_and_partial_usage(
        lower in -100_i64..=100,
        width in 4_i64..=100,
        negative in any::<bool>(),
        participants in 1_u8..=3,
        fraction in 0_u8..=3,
        abort in any::<bool>(),
        reload in any::<bool>(),
    ) {
        let mut h = Harness::new(false);
        for resource in 0..participants {
            h.apply(Command::Create(resource, Spec {
                lower,
                upper: Some(lower + width),
                initial: lower + width / 2,
                unit: 0,
                scale: 0,
            }), Effect::Clean, reload)?;
        }
        let delta = (width / 2) * if negative { -1 } else { 1 };
        let ops: Vec<_> = (0..participants)
            .map(|resource| Op { id: resource, resource, delta })
            .collect();
        h.apply(Command::Prepare(0, ops.clone(), None), Effect::LostResponse, reload)?;
        h.apply(Command::Prepare(0, ops.iter().rev().cloned().collect(), None), Effect::Clean, reload)?;
        let decision = if abort {
            Command::Abort(0)
        } else {
            Command::Commit(0, Some(ops.iter()
                .map(|o| (o.id, o.delta * i64::from(fraction) / 3))
                .collect()))
        };
        h.apply(decision.clone(), Effect::Conflicts, reload)?;
        h.apply(decision, Effect::Clean, reload)?;
        h.apply(if abort { Command::Commit(0, None) } else { Command::Abort(0) }, Effect::Clean, reload)?;
        let mut changed = ops;
        changed[0].delta = -changed[0].delta;
        h.apply(Command::Prepare(0, changed, None), Effect::Clean, reload)?;
    }
}
