//! Synchronous use cases over an append-only log, with a validated transfer
//! boundary, independent of adapters.
pub mod dto;
pub mod service;
use dto::{Command, Output, Snapshot};
use rescrow_domain::{self as d, transition};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Category {
    Input,
    Domain,
    StoredState,
    Storage,
    UnknownOutcome,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Code {
    InvalidInput,
    NotFound,
    Conflict,
    Capacity,
    Overflow,
    CorruptState,
    UnsupportedSchema,
    MissingStore,
    AlreadyInitialized,
    Contention,
    Io,
    TooLarge,
    RevisionExhausted,
    /// A prepare older than the retry window. It may or may not have been
    /// admitted earlier; that outcome can no longer be replayed.
    Expired,
    UnknownOutcome,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Error {
    pub category: Category,
    pub code: Code,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transaction_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity: Option<Box<dto::CapacityFailure>>,
}
impl Error {
    pub(crate) fn with_transaction(mut self, id: &str) -> Self {
        self.transaction_id = Some(id.into());
        self
    }
    fn rejection(reason: d::Rejection) -> Self {
        match reason {
            d::Rejection::Capacity(Some(failure)) => Self::domain(d::Error::Capacity(failure)),
            d::Rejection::Capacity(None) => Self {
                category: Category::Domain,
                code: Code::Capacity,
                message: "insufficient unreserved quantity or upward room (legacy rejection without details)".into(),
                transaction_id: None,
                capacity: None,
            },
            d::Rejection::Overflow => Self::domain(d::Error::Overflow),
            d::Rejection::NotFound => Self::domain(d::Error::NotFound),
        }
    }
    pub fn input(message: impl Into<String>) -> Self {
        Self {
            category: Category::Input,
            code: Code::InvalidInput,
            message: message.into(),
            transaction_id: None,
            capacity: None,
        }
    }
    fn domain(e: d::Error) -> Self {
        let code = match e {
            d::Error::Capacity(_) => Code::Capacity,
            d::Error::Overflow => Code::Overflow,
            d::Error::NotFound => Code::NotFound,
            d::Error::Conflict => Code::Conflict,
            _ => Code::InvalidInput,
        };
        Self {
            category: Category::Domain,
            code,
            message: e.to_string(),
            transaction_id: None,
            capacity: match &e {
                d::Error::Capacity(failure) => Some(Box::new(failure.into())),
                _ => None,
            },
        }
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)?;
        if let Some(id) = &self.transaction_id {
            write!(f, " [transaction {id}]")?;
        }
        Ok(())
    }
}
impl std::error::Error for Error {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreError {
    Missing,
    Exists,
    Contention,
    Corrupt(String),
    Unsupported(u32),
    Io(String),
    TooLarge,
    /// Publication may have happened. Never interpret this as a rollback.
    Unknown(String),
}
impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Missing => {
                f.write_str("store or live state is missing; ordinary commands never initialize it")
            }
            Self::Exists => f.write_str("store is already initialized"),
            Self::Contention => {
                f.write_str("bounded lock or revision retry limit reached; retry the same request")
            }
            Self::Corrupt(detail) => write!(f, "invalid stored state: {detail}"),
            Self::Unsupported(version) => {
                write!(f, "unsupported schema version {version}")
            }
            Self::Io(detail) => write!(f, "storage I/O failed: {detail}"),
            Self::TooLarge => f.write_str("a stored record exceeds the adapter's size limit"),
            Self::Unknown(detail) => write!(
                f,
                "publication outcome uncertain: {detail}; inspect/retry the same identity and request"
            ),
        }
    }
}
impl std::error::Error for StoreError {}
impl From<StoreError> for Error {
    fn from(e: StoreError) -> Self {
        let (category, code) = match &e {
            StoreError::Missing => (Category::Storage, Code::MissingStore),
            StoreError::Exists => (Category::Storage, Code::AlreadyInitialized),
            StoreError::Contention => (Category::Storage, Code::Contention),
            StoreError::Corrupt(_) => (Category::StoredState, Code::CorruptState),
            StoreError::Unsupported(_) => (Category::StoredState, Code::UnsupportedSchema),
            StoreError::Io(_) => (Category::Storage, Code::Io),
            StoreError::TooLarge => (Category::Storage, Code::TooLarge),
            StoreError::Unknown(_) => (Category::UnknownOutcome, Code::UnknownOutcome),
        };
        Self {
            category,
            code,
            message: e.to_string(),
            transaction_id: None,
            capacity: None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Publication {
    Applied,
    Conflict,
}

/// Schema version of checkpoints written by this Application.
pub const SCHEMA_VERSION: u32 = 2;

/// Decoded storage contents. The Application validates every part: the
/// checkpoint through `TryFrom`, and each tail entry by replaying its transition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Loaded {
    pub checkpoint: dto::Checkpoint,
    /// Entries after the checkpoint, in order.
    pub tail: Vec<dto::Entry>,
}

/// An append-only log of state changes plus a checkpoint of live state.
///
/// A successful append is the commit point; it must be durable before
/// `Applied` is returned, and any error after the append may have taken
/// effect must be `Unknown`, never `Io`. A load acknowledges durability of
/// what it returns, including an append whose response was lost.
/// Checkpoints are derived and never remove entries. Initialization never
/// replaces state.
pub trait LogStore {
    fn initialize(&self, empty: &dto::Checkpoint) -> Result<(), StoreError>;
    /// Create an authority holding exactly these entries, with a checkpoint at
    /// their end. Like initialization, never replaces existing live state.
    fn import(
        &self,
        checkpoint: &dto::Checkpoint,
        entries: &[dto::Entry],
    ) -> Result<(), StoreError>;
    fn load(&self) -> Result<Loaded, StoreError>;
    /// Append consecutive entries only if the log holds exactly
    /// `entries[0].sequence` entries. Entries are never empty. A failure may
    /// leave a prefix of them appended, which is the same as only the first
    /// writes having happened; report `Unknown` whenever that is possible.
    fn append(&self, entries: &[dto::Entry]) -> Result<Publication, StoreError>;
    /// Replace the checkpoint when this one is at a later position; otherwise do nothing.
    fn checkpoint(&self, checkpoint: &dto::Checkpoint) -> Result<(), StoreError>;
    /// The most recent terminal entry for a transaction among those recorded
    /// at or after `since_ms`. Older entries need not be read.
    fn find(&self, transaction_id: &str, since_ms: u64) -> Result<Option<dto::Entry>, StoreError>;
    /// Every entry from position zero, in order.
    fn history(&self) -> Result<Vec<dto::Entry>, StoreError>;
}

/// Validate a schema-1 whole-ledger snapshot, the format before the log.
pub fn validated_snapshot(snapshot: Snapshot) -> Result<d::Ledger, Error> {
    if snapshot.schema_version != 1 {
        return Err(StoreError::Unsupported(snapshot.schema_version).into());
    }
    d::Ledger::try_from(snapshot).map_err(corrupt)
}

/// Validated live state at a log position, plus terminal transactions found
/// in the tail after the checkpoint, each with its recording time.
#[derive(Clone)]
pub(crate) struct View {
    position: u64,
    /// When the last entry was recorded.
    recorded_at_ms: u64,
    live: d::Live,
    terminal: BTreeMap<d::Id, (u64, d::Transaction)>,
    tail: usize,
    /// The time a new entry would be recorded: the clock, but never earlier
    /// than the last entry.
    now_ms: u64,
}
impl View {
    /// The state after appending one entry of `changes`, recorded at `now_ms`.
    fn advance(&mut self, changes: &transition::Changes) -> Result<(), Error> {
        self.position = self.position.checked_add(1).ok_or_else(|| Error {
            category: Category::Storage,
            code: Code::RevisionExhausted,
            message: "log position exhausted".into(),
            transaction_id: None,
            capacity: None,
        })?;
        self.live = self.live.apply(changes);
        self.recorded_at_ms = self.now_ms;
        self.tail += 1;
        if let Some(tx) = changes.transaction()
            && *tx.state() != d::State::Prepared
        {
            self.terminal
                .insert(tx.request().id().clone(), (self.now_ms, tx.clone()));
        }
        Ok(())
    }
    /// A checkpoint now covers every entry. Terminal transactions it dropped
    /// are found through the store instead.
    fn checkpointed(&mut self) {
        self.tail = 0;
        self.terminal.clear();
    }
    pub(crate) fn live(&self) -> &d::Live {
        &self.live
    }
}
pub(crate) fn resource_in(view: &View, id: &str) -> Result<dto::Resource, Error> {
    let id = d::Id::new(id.into()).map_err(input)?;
    view.live
        .resources()
        .get(&id)
        .map(Into::into)
        .ok_or_else(|| Error::domain(d::Error::NotFound))
}

/// A validated write command.
pub(crate) enum Write {
    Create(d::ResourceSpec),
    Prepare(d::Request),
    Commit {
        id: d::Id,
        actuals: Option<Vec<(d::Id, i128)>>,
    },
    Abort(d::Id),
}
impl Write {
    pub(crate) fn create(spec: dto::ResourceSpec) -> Result<Self, Error> {
        Ok(Self::Create(spec.try_into().map_err(input)?))
    }
    pub(crate) fn prepare(request: dto::Prepare) -> Result<Self, Error> {
        Ok(Self::Prepare(request.try_into().map_err(input)?))
    }
    pub(crate) fn commit(command: dto::Commit) -> Result<Self, Error> {
        let id = d::Id::new(command.transaction_id).map_err(input)?;
        let actuals = command
            .actuals
            .map(|entries| {
                entries
                    .into_iter()
                    .map(|a| Ok((d::Id::new(a.operation_id)?, dto::decimal(&a.delta)?)))
                    .collect::<Result<Vec<_>, d::Error>>()
            })
            .transpose()
            .map_err(input)?;
        Ok(Self::Commit { id, actuals })
    }
    pub(crate) fn abort(id: &str) -> Result<Self, Error> {
        Ok(Self::Abort(d::Id::new(id.into()).map_err(input)?))
    }
}
/// A write's changes, and its result once they are durable.
type Evaluation = (transition::Changes, Result<Output, Error>);
fn transaction_output(output: Output) -> dto::Transaction {
    match output {
        Output::Transaction(tx) => tx,
        _ => unreachable!("transaction commands output transactions"),
    }
}

/// How long a prepare can be retried and replayed, and how far a client's
/// clock may lead or lag the Application's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryWindow {
    pub max_age: Duration,
    pub skew: Duration,
}
impl Default for RetryWindow {
    fn default() -> Self {
        Self {
            max_age: Duration::from_secs(24 * 60 * 60),
            skew: Duration::from_secs(5 * 60),
        }
    }
}

type Clock = Box<dyn Fn() -> u64 + Send + Sync>;
/// Milliseconds since the Unix epoch, from the system clock.
pub fn system_clock() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Synchronous typed use cases shared by CLI and embedded Rust callers.
pub struct Application<S> {
    store: S,
    attempts: usize,
    checkpoint_interval: usize,
    window: RetryWindow,
    clock: Clock,
}
impl<S: LogStore> Application<S> {
    pub fn new(store: S) -> Self {
        Self {
            store,
            attempts: 32,
            checkpoint_interval: 64,
            window: RetryWindow::default(),
            clock: Box::new(system_clock),
        }
    }
    /// Write a checkpoint once at least this many entries follow the current one.
    pub fn with_checkpoint_interval(mut self, entries: usize) -> Self {
        self.checkpoint_interval = entries.max(1);
        self
    }
    pub fn with_retry_window(mut self, window: RetryWindow) -> Self {
        self.window = window;
        self
    }
    /// Replace the source of milliseconds since the Unix epoch.
    pub fn with_clock(mut self, clock: impl Fn() -> u64 + Send + Sync + 'static) -> Self {
        self.clock = Box::new(clock);
        self
    }
    pub fn initialize(&self) -> Result<(), Error> {
        self.store
            .initialize(&checkpoint_dto(&d::Live::default(), 0, 0))?;
        Ok(())
    }
    /// Create a log-structured authority from a schema-1 snapshot. The snapshot
    /// is fully validated first, and the new log is then rebuilt and compared
    /// with it. Transaction history keeps its decisions but not the old revision.
    pub fn import(&self, snapshot: Snapshot) -> Result<(), Error> {
        let ledger = validated_snapshot(snapshot)?;
        let (checkpoint, entries) = imported(&ledger, (self.clock)());
        self.store.import(&checkpoint, &entries)?;
        if self.snapshot()? != Snapshot::from_ledger(&ledger, checkpoint.position) {
            return Err(corrupt("imported log does not reproduce its snapshot"));
        }
        Ok(())
    }
    /// Transport dispatcher; Rust callers may use the specific methods below.
    pub fn execute(&self, command: Command) -> Result<Output, Error> {
        match command {
            Command::CreateResource(v) => self.create_resource(v).map(Output::Resource),
            Command::Prepare(v) => self.prepare(v).map(Output::Transaction),
            Command::Commit(v) => self.commit(v).map(Output::Transaction),
            Command::Abort { transaction_id } => {
                self.abort(&transaction_id).map(Output::Transaction)
            }
            Command::ShowResource { resource_id } => {
                self.resource(&resource_id).map(Output::Resource)
            }
            Command::ListResources => self.resources().map(Output::Resources),
            Command::ShowTransaction { transaction_id } => {
                self.transaction(&transaction_id).map(Output::Transaction)
            }
            Command::ListTransactions => self.transactions().map(Output::Transactions),
        }
    }
    pub fn create_resource(&self, spec: dto::ResourceSpec) -> Result<dto::Resource, Error> {
        match self.write(&Write::create(spec)?)? {
            Output::Resource(resource) => Ok(resource),
            _ => unreachable!("resource creation outputs a resource"),
        }
    }
    /// Admit a frozen request; business rejections are persisted before returning an error.
    pub fn prepare(&self, request: dto::Prepare) -> Result<dto::Transaction, Error> {
        let id = request.transaction_id.clone();
        Write::prepare(request)
            .and_then(|w| self.write(&w))
            .map(transaction_output)
            .map_err(|e| e.with_transaction(&id))
    }
    /// Settle full or explicit actual usage; terminal retries must select the same actuals.
    pub fn commit(&self, command: dto::Commit) -> Result<dto::Transaction, Error> {
        let id = command.transaction_id.clone();
        Write::commit(command)
            .and_then(|w| self.write(&w))
            .map(transaction_output)
            .map_err(|e| e.with_transaction(&id))
    }
    /// Release a prepared hold. An existing committed decision conflicts.
    pub fn abort(&self, id: &str) -> Result<dto::Transaction, Error> {
        Write::abort(id)
            .and_then(|w| self.write(&w))
            .map(transaction_output)
            .map_err(|e| e.with_transaction(id))
    }
    pub fn resource(&self, id: &str) -> Result<dto::Resource, Error> {
        resource_in(&self.view()?, id)
    }
    pub fn resources(&self) -> Result<Vec<dto::Resource>, Error> {
        Ok(self.snapshot()?.resources)
    }
    pub fn transaction(&self, id: &str) -> Result<dto::Transaction, Error> {
        self.view()
            .and_then(|view| self.transaction_in(&view, id))
            .map_err(|e| e.with_transaction(id))
    }
    /// A transaction in `view`, or a terminal one anywhere in history.
    pub(crate) fn transaction_in(&self, view: &View, id: &str) -> Result<dto::Transaction, Error> {
        let transaction = d::Id::new(id.into()).map_err(input)?;
        self.lookup(view, &transaction, 0)?
            .as_ref()
            .map(Into::into)
            .ok_or_else(|| Error::domain(d::Error::NotFound))
            .map_err(|e| e.with_transaction(id))
    }
    pub fn transactions(&self) -> Result<Vec<dto::Transaction>, Error> {
        Ok(self.snapshot()?.transactions)
    }
    /// Validate the whole ledger and return it, with `revision` set to the log
    /// position. Reads every entry; balances are reconstructed from history.
    pub fn snapshot(&self) -> Result<Snapshot, Error> {
        let view = self.view()?;
        let mut history = self.store.history()?;
        if (history.len() as u64) < view.position {
            return Err(corrupt("log is shorter than its checkpoint"));
        }
        history.truncate(view.position as usize);
        let mut specs = BTreeMap::new();
        let mut transactions: BTreeMap<d::Id, d::Transaction> = BTreeMap::new();
        let mut recorded_at_ms = 0;
        for (position, entry) in history.into_iter().enumerate() {
            if entry.sequence != position as u64 {
                return Err(corrupt("log sequence gap"));
            }
            if entry.recorded_at_ms < recorded_at_ms {
                return Err(corrupt("log entry recorded before its predecessor"));
            }
            recorded_at_ms = entry.recorded_at_ms;
            match entry.record {
                dto::Record::ResourceCreated(spec) => {
                    let spec = d::ResourceSpec::try_from(spec).map_err(corrupt)?;
                    if specs.insert(spec.id().clone(), spec).is_some() {
                        return Err(corrupt("resource created twice"));
                    }
                }
                dto::Record::Transaction(tx) => {
                    let tx = d::Transaction::try_from(tx).map_err(corrupt)?;
                    let previous = transactions.get(tx.request().id());
                    let valid = matches!(
                        (previous.map(d::Transaction::state), tx.state()),
                        (
                            None,
                            d::State::Prepared | d::State::Aborted(d::AbortReason::Rejected(_))
                        ) | (
                            Some(d::State::Prepared),
                            d::State::Committed(_) | d::State::Aborted(d::AbortReason::Requested)
                        )
                    );
                    if !valid || previous.is_some_and(|p| p.request() != tx.request()) {
                        return Err(corrupt("transaction record out of order"));
                    }
                    transactions.insert(tx.request().id().clone(), tx);
                }
            }
        }
        let live = &view.live;
        let created_match = specs.len() == live.resources().len()
            && live
                .resources()
                .iter()
                .all(|(id, r)| specs.get(id) == Some(r.spec()));
        let prepared_match = transactions
            .iter()
            .filter(|(_, tx)| *tx.state() == d::State::Prepared)
            .eq(live.prepared().iter());
        if !created_match || !prepared_match || recorded_at_ms != view.recorded_at_ms {
            return Err(corrupt("checkpoint disagrees with history"));
        }
        let ledger = d::Ledger::restore(
            live.resources().values().cloned().collect(),
            transactions.into_values().collect(),
        )
        .map_err(corrupt)?;
        Ok(Snapshot::from_ledger(&ledger, view.position))
    }
    pub(crate) fn view(&self) -> Result<View, Error> {
        let loaded = self.store.load()?;
        let (position, recorded_at_ms, live) = live_from(loaded.checkpoint)?;
        let mut view = View {
            position,
            recorded_at_ms,
            live,
            terminal: BTreeMap::new(),
            tail: loaded.tail.len(),
            now_ms: 0,
        };
        for entry in loaded.tail {
            if entry.sequence != view.position {
                return Err(corrupt("log sequence gap"));
            }
            if entry.recorded_at_ms < view.recorded_at_ms {
                return Err(corrupt("log entry recorded before its predecessor"));
            }
            view.recorded_at_ms = entry.recorded_at_ms;
            replay(&mut view, entry.record)?;
            view.position = view
                .position
                .checked_add(1)
                .ok_or_else(|| corrupt("log position overflow"))?;
        }
        view.now_ms = (self.clock)().max(view.recorded_at_ms);
        Ok(view)
    }
    /// Admit a prepare not found in live state: its ID must be a UUIDv7 whose
    /// time is in the retry window. Every record of a request created at `c`
    /// is recorded no earlier than `c - skew`, so that bounds the search for it.
    fn admit(&self, view: &View, request: &d::Request) -> Result<u64, Error> {
        let created = uuid_v7_millis(request.id().as_str()).ok_or_else(|| {
            Error::input("a new transaction ID must be a UUIDv7 in canonical lowercase form")
        })?;
        let skew = millis(self.window.skew);
        if created < view.now_ms.saturating_sub(millis(self.window.max_age)) {
            return Err(Error {
                category: Category::Input,
                code: Code::Expired,
                message: format!(
                    "transaction ID was created at {created} ms, older than the {:?} retry \
                     window; an earlier attempt may have been admitted, but it can no longer \
                     be replayed",
                    self.window.max_age
                ),
                transaction_id: None,
                capacity: None,
            });
        }
        if created > view.now_ms.saturating_add(skew) {
            return Err(Error::input(format!(
                "transaction ID was created at {created} ms, more than {:?} ahead of the \
                 store's clock",
                self.window.skew
            )));
        }
        Ok(created.saturating_sub(skew))
    }
    /// The earliest recording time a retried decision is searched from.
    fn decided_since(&self, view: &View) -> u64 {
        view.now_ms
            .saturating_sub(millis(self.window.max_age))
            .saturating_sub(millis(self.window.skew))
    }
    /// A prepared transaction, or the latest terminal one recorded at or after `since_ms`.
    pub(crate) fn lookup(
        &self,
        view: &View,
        id: &d::Id,
        since_ms: u64,
    ) -> Result<Option<d::Transaction>, Error> {
        if let Some(tx) = view.live.prepared().get(id) {
            return Ok(Some(tx.clone()));
        }
        if let Some((recorded, tx)) = view.terminal.get(id) {
            return Ok((*recorded >= since_ms).then(|| tx.clone()));
        }
        let Some(entry) = self.store.find(id.as_str(), since_ms)? else {
            return Ok(None);
        };
        let dto::Record::Transaction(tx) = entry.record else {
            return Err(corrupt("terminal lookup returned a resource record"));
        };
        let tx = d::Transaction::try_from(tx).map_err(corrupt)?;
        if tx.request().id() != id
            || *tx.state() == d::State::Prepared
            || entry.recorded_at_ms < since_ms
        {
            return Err(corrupt("terminal lookup returned the wrong transaction"));
        }
        Ok(Some(tx))
    }
    /// One write, evaluated against freshly loaded state and appended alone.
    fn write(&self, write: &Write) -> Result<Output, Error> {
        let (_, mut outputs) = self.commit_batch(self.view()?, &[write]);
        outputs.pop().expect("one output per write")
    }
    /// Evaluate writes in order, each against the state the previous ones
    /// leave, and append all their records together. A lost race reloads and
    /// evaluates them again, so every output describes durable state. Returns
    /// the state after the batch, or `None` if it is unknown.
    pub(crate) fn commit_batch(
        &self,
        mut view: View,
        writes: &[&Write],
    ) -> (Option<View>, Vec<Result<Output, Error>>) {
        for _ in 0..self.attempts {
            view.now_ms = (self.clock)().max(view.recorded_at_ms);
            // A lost race reloads, so evaluation can advance this state in place.
            let mut entries = Vec::new();
            let mut outputs = Vec::with_capacity(writes.len());
            for write in writes {
                outputs.push(self.evaluate(&view, write).and_then(|(changes, output)| {
                    if !changes.is_empty() {
                        let sequence = view.position;
                        view.advance(&changes)?;
                        entries.push(dto::Entry {
                            sequence,
                            recorded_at_ms: view.now_ms,
                            record: record(&changes),
                        });
                    }
                    output
                }));
            }
            if entries.is_empty() {
                return (Some(view), outputs);
            }
            match self.store.append(&entries) {
                Ok(Publication::Applied) => {
                    if view.tail >= self.checkpoint_interval {
                        // Checkpoints only shorten later replay. A failure leaves a
                        // longer tail for the next writer, never a wrong answer.
                        let checkpoint = checkpoint_dto(&view.live, view.position, view.now_ms);
                        if self.store.checkpoint(&checkpoint).is_ok() {
                            view.checkpointed();
                        }
                    }
                    return (Some(view), outputs);
                }
                Ok(Publication::Conflict) => match self.view() {
                    Ok(fresh) => view = fresh,
                    Err(e) => return (None, writes.iter().map(|_| Err(e.clone())).collect()),
                },
                Err(e) => {
                    let e = Error::from(e);
                    return (None, writes.iter().map(|_| Err(e.clone())).collect());
                }
            }
        }
        let e = Error::from(StoreError::Contention);
        (Some(view), writes.iter().map(|_| Err(e.clone())).collect())
    }
    /// The changes a write makes to `view` and its result. Rejections the
    /// domain records are changes with an error result.
    fn evaluate(&self, view: &View, write: &Write) -> Result<Evaluation, Error> {
        match write {
            Write::Create(spec) => {
                let changes =
                    transition::create(spec.clone(), view.live.resources().get(spec.id()))
                        .map_err(Error::domain)?;
                let next = view.live.apply(&changes);
                let resource = (&next.resources()[spec.id()]).into();
                Ok((changes, Ok(Output::Resource(resource))))
            }
            Write::Prepare(request) => {
                let existing = match view.live.prepared().get(request.id()) {
                    Some(prepared) => Some(prepared.clone()),
                    None => self.lookup(view, request.id(), self.admit(view, request)?)?,
                };
                let changes =
                    transition::prepare(request.clone(), existing.as_ref(), view.live.resources())
                        .map_err(Error::domain)?;
                let tx = changes
                    .transaction()
                    .or(existing.as_ref())
                    .ok_or(Error::domain(d::Error::InvalidLedger))?;
                let output = match tx.rejection() {
                    Some(reason) => Err(Error::rejection(reason)),
                    None => Ok(Output::Transaction(tx.into())),
                };
                Ok((changes, output))
            }
            Write::Commit { id, actuals } => {
                let tx = self
                    .lookup(view, id, self.decided_since(view))?
                    .ok_or(Error::domain(d::Error::NotFound))?;
                let changes = transition::commit(&tx, actuals.clone(), view.live.resources())
                    .map_err(Error::domain)?;
                let output = changes.transaction().unwrap_or(&tx).into();
                Ok((changes, Ok(Output::Transaction(output))))
            }
            Write::Abort(id) => {
                let tx = self
                    .lookup(view, id, self.decided_since(view))?
                    .ok_or(Error::domain(d::Error::NotFound))?;
                let changes =
                    transition::abort(&tx, view.live.resources()).map_err(Error::domain)?;
                let output = changes.transaction().unwrap_or(&tx).into();
                Ok((changes, Ok(Output::Transaction(output))))
            }
        }
    }
}

/// Re-run the recorded transition and require that it reproduces the record.
fn replay(view: &mut View, record: dto::Record) -> Result<(), Error> {
    let changes = match record {
        dto::Record::ResourceCreated(spec) => {
            let spec = d::ResourceSpec::try_from(spec).map_err(corrupt)?;
            let changes = transition::create(spec.clone(), view.live.resources().get(spec.id()))
                .map_err(corrupt)?;
            if changes.is_empty() {
                return Err(corrupt("resource created twice"));
            }
            changes
        }
        dto::Record::Transaction(tx) => {
            let tx = d::Transaction::try_from(tx).map_err(corrupt)?;
            let id = tx.request().id();
            let resources = view.live.resources();
            let prepared = view.live.prepared().get(id);
            let known = prepared.is_some() || view.terminal.contains_key(id);
            let changes = match (tx.state(), prepared) {
                (d::State::Prepared | d::State::Aborted(d::AbortReason::Rejected(_)), _)
                    if !known =>
                {
                    transition::prepare(tx.request().clone(), None, resources)
                }
                (d::State::Committed(actuals), Some(prepared)) => transition::commit(
                    prepared,
                    Some(
                        actuals
                            .entries()
                            .iter()
                            .map(|(id, delta)| (id.clone(), *delta))
                            .collect(),
                    ),
                    resources,
                ),
                (d::State::Aborted(d::AbortReason::Requested), Some(prepared)) => {
                    transition::abort(prepared, resources)
                }
                _ => return Err(corrupt("transaction record out of order")),
            }
            .map_err(corrupt)?;
            if changes.transaction() != Some(&tx) {
                return Err(corrupt("record does not match its transition"));
            }
            if *tx.state() != d::State::Prepared {
                view.terminal
                    .insert(id.clone(), (view.recorded_at_ms, tx.clone()));
            }
            changes
        }
    };
    view.live = view.live.apply(&changes);
    Ok(())
}
/// Entries that rebuild a validated ledger: each resource's creation, then each
/// transaction's preparation and decision. They precede the checkpoint, so they
/// are validated as whole history rather than replayed in order.
fn imported(ledger: &d::Ledger, now_ms: u64) -> (dto::Checkpoint, Vec<dto::Entry>) {
    let mut records: Vec<dto::Record> = ledger
        .resources()
        .values()
        .map(|r| dto::Record::ResourceCreated(r.spec().into()))
        .collect();
    for tx in ledger.transactions().values() {
        match tx.state() {
            d::State::Prepared | d::State::Aborted(d::AbortReason::Rejected(_)) => {}
            d::State::Committed(_) | d::State::Aborted(d::AbortReason::Requested) => {
                let mut prepared = dto::Transaction::from(tx);
                prepared.state = dto::State::Prepared {};
                records.push(dto::Record::Transaction(prepared));
            }
        }
        records.push(dto::Record::Transaction(tx.into()));
    }
    let entries: Vec<_> = records
        .into_iter()
        .zip(0..)
        .map(|(record, sequence)| dto::Entry {
            sequence,
            recorded_at_ms: now_ms,
            record,
        })
        .collect();
    let checkpoint = checkpoint_dto(&ledger.live(), entries.len() as u64, now_ms);
    (checkpoint, entries)
}
fn record(changes: &transition::Changes) -> dto::Record {
    match changes.transaction() {
        Some(tx) => dto::Record::Transaction(tx.into()),
        None => dto::Record::ResourceCreated(changes.resources()[0].spec().into()),
    }
}
fn live_from(checkpoint: dto::Checkpoint) -> Result<(u64, u64, d::Live), Error> {
    if checkpoint.schema_version != SCHEMA_VERSION {
        return Err(StoreError::Unsupported(checkpoint.schema_version).into());
    }
    let live = d::Live::restore(
        checkpoint
            .resources
            .into_iter()
            .map(d::Resource::try_from)
            .collect::<Result<_, _>>()
            .map_err(corrupt)?,
        checkpoint
            .prepared
            .into_iter()
            .map(d::Transaction::try_from)
            .collect::<Result<_, _>>()
            .map_err(corrupt)?,
    )
    .map_err(corrupt)?;
    Ok((checkpoint.position, checkpoint.recorded_at_ms, live))
}
fn checkpoint_dto(live: &d::Live, position: u64, recorded_at_ms: u64) -> dto::Checkpoint {
    dto::Checkpoint {
        schema_version: SCHEMA_VERSION,
        position,
        recorded_at_ms,
        resources: live.resources().values().map(Into::into).collect(),
        prepared: live.prepared().values().map(Into::into).collect(),
    }
}
fn corrupt(detail: impl std::fmt::Display) -> Error {
    StoreError::Corrupt(detail.to_string()).into()
}
/// The millisecond timestamp of a UUIDv7 in canonical lowercase form
/// (`xxxxxxxx-xxxx-7xxx-[89ab]xxx-xxxxxxxxxxxx`), or `None` for anything else.
pub fn uuid_v7_millis(id: &str) -> Option<u64> {
    let bytes = id.as_bytes();
    let canonical = bytes.len() == 36
        && bytes.iter().enumerate().all(|(i, b)| match i {
            8 | 13 | 18 | 23 => *b == b'-',
            _ => b.is_ascii_digit() || (b'a'..=b'f').contains(b),
        });
    if !canonical || bytes[14] != b'7' || !b"89ab".contains(&bytes[19]) {
        return None;
    }
    u64::from_str_radix(&format!("{}{}", &id[..8], &id[9..13]), 16).ok()
}
/// A UUIDv7 in canonical lowercase form whose timestamp is `millis` (its low
/// 48 bits) and whose 74 remaining bits are the low bits of `random`. The
/// caller supplies the randomness, so two IDs created in the same millisecond
/// differ only if their `random` values do.
pub fn uuid_v7(millis: u64, random: u128) -> String {
    let millis = u128::from(millis) & ((1 << 48) - 1);
    let a = (random >> 62) & 0xfff;
    let b = random & ((1 << 62) - 1);
    let v = millis << 80 | 0x7 << 76 | a << 64 | 0b10 << 62 | b;
    let h = format!("{v:032x}");
    format!(
        "{}-{}-{}-{}-{}",
        &h[..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..]
    )
}
fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
fn input(error: d::Error) -> Error {
    Error::input(error.to_string())
}
