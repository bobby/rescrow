//! Typed synchronous use cases over a transactional authority, with a validated
//! transfer boundary independent of adapters.
pub mod authority;
pub mod capabilities;
pub mod dto;
mod log_authority;
pub mod service;
pub mod transactional;
use dto::{Output, Snapshot};
use rescrow_domain::{self as d, transition};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
pub use transactional::Application;

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
            d::Rejection::Capacity(failure) => Self::domain(d::Error::Capacity(failure)),
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
    RevisionExhausted,
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
            Self::RevisionExhausted => f.write_str("log position exhausted"),
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
            StoreError::RevisionExhausted => (Category::Storage, Code::RevisionExhausted),
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
pub const SCHEMA_VERSION: u32 = 3;

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
    fn load(&self) -> Result<Loaded, StoreError>;
    /// Append consecutive entries only if the log holds exactly
    /// `entries[0].sequence` entries. Entries are never empty. The entire batch is one
    /// atomic publication, including its history identity. Report `Unknown`
    /// whenever the complete batch may have been published.
    fn append(&self, entries: &[dto::Entry]) -> Result<Publication, StoreError>;
    /// Replace the checkpoint when this one is at a later position; otherwise do nothing.
    fn checkpoint(&self, checkpoint: &dto::Checkpoint) -> Result<(), StoreError>;
    /// The most recent terminal entry for a transaction among those recorded
    /// at or after `since_ms`. Older entries need not be read.
    fn find(&self, transaction_id: &str, since_ms: u64) -> Result<Option<dto::Entry>, StoreError>;
    /// Every entry from position zero, in order.
    fn history(&self) -> Result<Vec<dto::Entry>, StoreError>;
    /// All entries of one atomic publication, or absence. Implementations may
    /// bound the search by the publication UUID's recording timestamp.
    fn publication(&self, identity: &str) -> Result<Option<Vec<dto::Entry>>, StoreError> {
        let entries: Vec<_> = self
            .history()?
            .into_iter()
            .filter(|e| e.publication_id.as_deref() == Some(identity))
            .collect();
        Ok((!entries.is_empty()).then_some(entries))
    }

    /// Actual backend capabilities, verified by `load` at readiness.
    fn capabilities(&self) -> capabilities::Capabilities {
        capabilities::Capabilities::simulation()
    }
}

/// Validate a whole-ledger snapshot, as `Application::snapshot` returns it.
pub fn validated_snapshot(snapshot: Snapshot) -> Result<d::Ledger, Error> {
    if snapshot.schema_version != 1 {
        return Err(StoreError::Unsupported(snapshot.schema_version).into());
    }
    d::Ledger::try_from(snapshot).map_err(corrupt)
}

/// Validated live state at a log position, plus terminal transactions found
/// in the tail after the checkpoint, each with its recording time.
#[derive(Clone)]
struct LogView {
    position: u64,
    /// When the last entry was recorded.
    recorded_at_ms: u64,
    live: d::Live,
    terminal: BTreeMap<d::Id, (u64, d::Transaction)>,
    tail: usize,
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
    /// Lapse every overdue hold, on any resource.
    Lapse,
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

/// Milliseconds since the Unix epoch, from the system clock.
pub fn system_clock() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Pure validation/replay of the local log representation. This is an adapter
/// codec helper, not a second Application or business write path.
struct LogReader<S> {
    store: S,
}
impl<S: LogStore> LogReader<S> {
    fn new(store: S) -> Self {
        Self { store }
    }
    pub(crate) fn snapshot_at(&self, view: LogView) -> Result<Snapshot, Error> {
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
                            d::State::Committed(_)
                                | d::State::Aborted(
                                    d::AbortReason::Requested | d::AbortReason::Lapsed
                                )
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
    pub(crate) fn view(&self) -> Result<LogView, StoreError> {
        let loaded = self.store.load()?;
        if loaded.checkpoint.schema_version != SCHEMA_VERSION {
            return Err(StoreError::Unsupported(loaded.checkpoint.schema_version));
        }
        let (position, recorded_at_ms, live) =
            live_from(loaded.checkpoint).map_err(log_authority::storage)?;
        let mut view = LogView {
            position,
            recorded_at_ms,
            live,
            terminal: BTreeMap::new(),
            tail: loaded.tail.len(),
        };
        for entry in loaded.tail {
            if entry.sequence != view.position {
                return Err(StoreError::Corrupt("log sequence gap".into()));
            }
            if entry.recorded_at_ms < view.recorded_at_ms {
                return Err(StoreError::Corrupt(
                    "log entry recorded before its predecessor".into(),
                ));
            }
            view.recorded_at_ms = entry.recorded_at_ms;
            replay(&mut view, entry.record).map_err(log_authority::storage)?;
            view.position = view
                .position
                .checked_add(1)
                .ok_or(StoreError::RevisionExhausted)?;
        }
        Ok(view)
    }
}

/// Re-run the recorded transition and require that it reproduces the record.
fn replay(view: &mut LogView, record: dto::Record) -> Result<(), Error> {
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
                    transition::prepare(tx.request().clone(), None, resources, view.recorded_at_ms)
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
                (d::State::Aborted(d::AbortReason::Lapsed), Some(prepared)) => {
                    transition::lapse(prepared, resources, view.recorded_at_ms)
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

impl<S: LogStore + ?Sized> LogStore for &S {
    fn initialize(&self, empty: &dto::Checkpoint) -> Result<(), StoreError> {
        (**self).initialize(empty)
    }
    fn load(&self) -> Result<Loaded, StoreError> {
        (**self).load()
    }
    fn append(&self, entries: &[dto::Entry]) -> Result<Publication, StoreError> {
        (**self).append(entries)
    }
    fn checkpoint(&self, checkpoint: &dto::Checkpoint) -> Result<(), StoreError> {
        (**self).checkpoint(checkpoint)
    }
    fn find(&self, id: &str, since: u64) -> Result<Option<dto::Entry>, StoreError> {
        (**self).find(id, since)
    }
    fn history(&self) -> Result<Vec<dto::Entry>, StoreError> {
        (**self).history()
    }
    fn publication(&self, identity: &str) -> Result<Option<Vec<dto::Entry>>, StoreError> {
        (**self).publication(identity)
    }
    fn capabilities(&self) -> capabilities::Capabilities {
        (**self).capabilities()
    }
}
