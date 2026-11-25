//! Synchronous use cases over an append-only log, with a validated transfer
//! boundary, independent of adapters.
pub mod dto;
use dto::{Command, Output, Snapshot};
use rescrow_domain::{self as d, transition};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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
    fn with_transaction(mut self, id: &str) -> Self {
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
    /// Append at `entry.sequence` only if the log holds exactly that many entries.
    fn append(&self, entry: &dto::Entry) -> Result<Publication, StoreError>;
    /// Replace the checkpoint when this one is at a later position; otherwise do nothing.
    fn checkpoint(&self, checkpoint: &dto::Checkpoint) -> Result<(), StoreError>;
    /// The most recent terminal entry for a transaction, anywhere in the log.
    fn find(&self, transaction_id: &str) -> Result<Option<dto::Entry>, StoreError>;
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
/// in the tail after the checkpoint.
struct View {
    position: u64,
    live: d::Live,
    terminal: BTreeMap<d::Id, d::Transaction>,
    tail: usize,
}

/// Synchronous typed use cases shared by CLI and embedded Rust callers.
pub struct Application<S> {
    store: S,
    attempts: usize,
    checkpoint_interval: usize,
}
impl<S: LogStore> Application<S> {
    pub fn new(store: S) -> Self {
        Self {
            store,
            attempts: 32,
            checkpoint_interval: 64,
        }
    }
    /// Write a checkpoint once at least this many entries follow the current one.
    pub fn with_checkpoint_interval(mut self, entries: usize) -> Self {
        self.checkpoint_interval = entries.max(1);
        self
    }
    pub fn initialize(&self) -> Result<(), Error> {
        self.store
            .initialize(&checkpoint_dto(&d::Live::default(), 0))?;
        Ok(())
    }
    /// Create a log-structured authority from a schema-1 snapshot. The snapshot
    /// is fully validated first, and the new log is then rebuilt and compared
    /// with it. Transaction history keeps its decisions but not the old revision.
    pub fn import(&self, snapshot: Snapshot) -> Result<(), Error> {
        let ledger = validated_snapshot(snapshot)?;
        let (checkpoint, entries) = imported(&ledger);
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
        let spec: d::ResourceSpec = spec.try_into().map_err(input)?;
        self.mutate(|view| {
            let changes = transition::create(spec.clone(), view.live.resources().get(spec.id()))
                .map_err(Error::domain)?;
            let next = view.live.apply(&changes);
            Ok((changes, Ok((&next.resources()[spec.id()]).into())))
        })
    }
    /// Admit a frozen request; business rejections are persisted before returning an error.
    pub fn prepare(&self, request: dto::Prepare) -> Result<dto::Transaction, Error> {
        let id = request.transaction_id.clone();
        let result = (|| {
            let request: d::Request = request.try_into().map_err(input)?;
            self.mutate(|view| {
                let existing = self.lookup(view, request.id())?;
                let changes =
                    transition::prepare(request.clone(), existing.as_ref(), view.live.resources())
                        .map_err(Error::domain)?;
                let tx = changes
                    .transaction()
                    .or(existing.as_ref())
                    .ok_or(Error::domain(d::Error::InvalidLedger))?;
                let output = match tx.rejection() {
                    Some(reason) => Err(Error::rejection(reason)),
                    None => Ok(tx.into()),
                };
                Ok((changes, output))
            })
        })();
        result.map_err(|e| e.with_transaction(&id))
    }
    /// Settle full or explicit actual usage; terminal retries must select the same actuals.
    pub fn commit(&self, command: dto::Commit) -> Result<dto::Transaction, Error> {
        let id = command.transaction_id;
        let result = (|| {
            let transaction = d::Id::new(id.clone()).map_err(input)?;
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
            self.mutate(|view| {
                let tx = self
                    .lookup(view, &transaction)?
                    .ok_or(Error::domain(d::Error::NotFound))?;
                let changes = transition::commit(&tx, actuals.clone(), view.live.resources())
                    .map_err(Error::domain)?;
                let output = changes.transaction().unwrap_or(&tx).into();
                Ok((changes, Ok(output)))
            })
        })();
        result.map_err(|e| e.with_transaction(&id))
    }
    /// Release a prepared hold. An existing committed decision conflicts.
    pub fn abort(&self, id: &str) -> Result<dto::Transaction, Error> {
        let result = (|| {
            let transaction = d::Id::new(id.into()).map_err(input)?;
            self.mutate(|view| {
                let tx = self
                    .lookup(view, &transaction)?
                    .ok_or(Error::domain(d::Error::NotFound))?;
                let changes =
                    transition::abort(&tx, view.live.resources()).map_err(Error::domain)?;
                let output = changes.transaction().unwrap_or(&tx).into();
                Ok((changes, Ok(output)))
            })
        })();
        result.map_err(|e| e.with_transaction(id))
    }
    pub fn resource(&self, id: &str) -> Result<dto::Resource, Error> {
        let id = d::Id::new(id.into()).map_err(input)?;
        self.view()?
            .live
            .resources()
            .get(&id)
            .map(Into::into)
            .ok_or_else(|| Error::domain(d::Error::NotFound))
    }
    pub fn resources(&self) -> Result<Vec<dto::Resource>, Error> {
        Ok(self.snapshot()?.resources)
    }
    pub fn transaction(&self, id: &str) -> Result<dto::Transaction, Error> {
        let result = (|| {
            let transaction = d::Id::new(id.into()).map_err(input)?;
            let view = self.view()?;
            self.lookup(&view, &transaction)?
                .as_ref()
                .map(Into::into)
                .ok_or_else(|| Error::domain(d::Error::NotFound))
        })();
        result.map_err(|e| e.with_transaction(id))
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
        for (position, entry) in history.into_iter().enumerate() {
            if entry.sequence != position as u64 {
                return Err(corrupt("log sequence gap"));
            }
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
        if !created_match || !prepared_match {
            return Err(corrupt("checkpoint disagrees with history"));
        }
        let ledger = d::Ledger::restore(
            live.resources().values().cloned().collect(),
            transactions.into_values().collect(),
        )
        .map_err(corrupt)?;
        Ok(Snapshot::from_ledger(&ledger, view.position))
    }
    fn view(&self) -> Result<View, Error> {
        let loaded = self.store.load()?;
        let (position, live) = live_from(loaded.checkpoint)?;
        let mut view = View {
            position,
            live,
            terminal: BTreeMap::new(),
            tail: loaded.tail.len(),
        };
        for entry in loaded.tail {
            if entry.sequence != view.position {
                return Err(corrupt("log sequence gap"));
            }
            replay(&mut view, entry.record)?;
            view.position = view
                .position
                .checked_add(1)
                .ok_or_else(|| corrupt("log position overflow"))?;
        }
        Ok(view)
    }
    fn lookup(&self, view: &View, id: &d::Id) -> Result<Option<d::Transaction>, Error> {
        if let Some(tx) = view.live.prepared().get(id).or(view.terminal.get(id)) {
            return Ok(Some(tx.clone()));
        }
        let Some(entry) = self.store.find(id.as_str())? else {
            return Ok(None);
        };
        let dto::Record::Transaction(tx) = entry.record else {
            return Err(corrupt("terminal lookup returned a resource record"));
        };
        let tx = d::Transaction::try_from(tx).map_err(corrupt)?;
        if tx.request().id() != id || *tx.state() == d::State::Prepared {
            return Err(corrupt("terminal lookup returned the wrong transaction"));
        }
        Ok(Some(tx))
    }
    fn mutate<R>(
        &self,
        evaluate: impl Fn(&View) -> Result<(transition::Changes, Result<R, Error>), Error>,
    ) -> Result<R, Error> {
        for _ in 0..self.attempts {
            let view = self.view()?;
            let (changes, output) = evaluate(&view)?;
            if changes.is_empty() {
                return output;
            }
            let next = view.position.checked_add(1).ok_or_else(|| Error {
                category: Category::Storage,
                code: Code::RevisionExhausted,
                message: "log position exhausted".into(),
                transaction_id: None,
                capacity: None,
            })?;
            let entry = dto::Entry {
                sequence: view.position,
                record: record(&changes),
            };
            match self.store.append(&entry)? {
                Publication::Applied => {
                    if view.tail + 1 >= self.checkpoint_interval {
                        // Checkpoints only shorten later replay. A failure leaves a
                        // longer tail for the next writer, never a wrong answer.
                        let _ = self
                            .store
                            .checkpoint(&checkpoint_dto(&view.live.apply(&changes), next));
                    }
                    return output;
                }
                Publication::Conflict => continue,
            }
        }
        Err(StoreError::Contention.into())
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
                view.terminal.insert(id.clone(), tx.clone());
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
fn imported(ledger: &d::Ledger) -> (dto::Checkpoint, Vec<dto::Entry>) {
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
        .map(|(record, sequence)| dto::Entry { sequence, record })
        .collect();
    let checkpoint = checkpoint_dto(&ledger.live(), entries.len() as u64);
    (checkpoint, entries)
}
fn record(changes: &transition::Changes) -> dto::Record {
    match changes.transaction() {
        Some(tx) => dto::Record::Transaction(tx.into()),
        None => dto::Record::ResourceCreated(changes.resources()[0].spec().into()),
    }
}
fn live_from(checkpoint: dto::Checkpoint) -> Result<(u64, d::Live), Error> {
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
    Ok((checkpoint.position, live))
}
fn checkpoint_dto(live: &d::Live, position: u64) -> dto::Checkpoint {
    dto::Checkpoint {
        schema_version: SCHEMA_VERSION,
        position,
        resources: live.resources().values().map(Into::into).collect(),
        prepared: live.prepared().values().map(Into::into).collect(),
    }
}
fn corrupt(detail: impl std::fmt::Display) -> Error {
    StoreError::Corrupt(detail.to_string()).into()
}
fn input(error: d::Error) -> Error {
    Error::input(error.to_string())
}
