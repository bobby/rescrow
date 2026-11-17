//! Synchronous use cases and validated transfer boundary, independent of adapters.
pub mod dto;
use dto::{Command, Output, Snapshot};
use rescrow_domain as d;
use serde::{Deserialize, Serialize};

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
}
impl Error {
    pub fn input(message: impl Into<String>) -> Self {
        Self {
            category: Category::Input,
            code: Code::InvalidInput,
            message: message.into(),
        }
    }
    fn domain(e: d::Error) -> Self {
        let code = match e {
            d::Error::Capacity => Code::Capacity,
            d::Error::Overflow => Code::Overflow,
            d::Error::NotFound => Code::NotFound,
            d::Error::Conflict => Code::Conflict,
            _ => Code::InvalidInput,
        };
        Self {
            category: Category::Domain,
            code,
            message: e.to_string(),
        }
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
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
                write!(f, "unsupported snapshot schema version {version}")
            }
            Self::Io(detail) => write!(f, "storage I/O failed: {detail}"),
            Self::TooLarge => f.write_str("snapshot exceeds the adapter's size limit"),
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
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Publication {
    Applied,
    Conflict,
}

/// A load acknowledges durability, including a prior publication with a lost response.
/// CAS must compare under the same coordination boundary as publication. Errors after
/// publication must be Unknown, never Io. Initialization must never replace live state.
pub trait SnapshotStore {
    fn initialize(&self, empty: &Snapshot) -> Result<(), StoreError>;
    fn load(&self) -> Result<Snapshot, StoreError>;
    fn replace(
        &self,
        expected_revision: u64,
        candidate: &Snapshot,
    ) -> Result<Publication, StoreError>;
}

pub fn validated_snapshot(snapshot: Snapshot) -> Result<d::Ledger, Error> {
    if snapshot.schema_version != 1 {
        return Err(StoreError::Unsupported(snapshot.schema_version).into());
    }
    d::Ledger::try_from(snapshot).map_err(|e| StoreError::Corrupt(e.to_string()).into())
}

enum Validated {
    Create(d::ResourceSpec),
    Prepare(d::Request),
    Commit(d::Id, Option<Vec<(d::Id, i128)>>),
    Abort(d::Id),
    Resource(d::Id),
    Resources,
    Transaction(d::Id),
    Transactions,
}
impl TryFrom<Command> for Validated {
    type Error = d::Error;
    fn try_from(c: Command) -> Result<Self, Self::Error> {
        Ok(match c {
            Command::CreateResource(v) => Self::Create(v.try_into()?),
            Command::Prepare(v) => Self::Prepare(v.try_into()?),
            Command::Commit(v) => Self::Commit(
                d::Id::new(v.transaction_id)?,
                v.actuals
                    .map(|a| {
                        a.into_iter()
                            .map(|a| Ok((d::Id::new(a.operation_id)?, dto::decimal(&a.delta)?)))
                            .collect::<Result<_, d::Error>>()
                    })
                    .transpose()?,
            ),
            Command::Abort { transaction_id } => Self::Abort(d::Id::new(transaction_id)?),
            Command::ShowResource { resource_id } => Self::Resource(d::Id::new(resource_id)?),
            Command::ListResources => Self::Resources,
            Command::ShowTransaction { transaction_id } => {
                Self::Transaction(d::Id::new(transaction_id)?)
            }
            Command::ListTransactions => Self::Transactions,
        })
    }
}

/// A host supplies one snapshot adapter; no daemon or asynchronous runtime is required.
pub struct Application<S> {
    store: S,
    attempts: usize,
}
impl<S: SnapshotStore> Application<S> {
    pub fn new(store: S) -> Self {
        Self {
            store,
            attempts: 32,
        }
    }
    pub fn initialize(&self) -> Result<Output, Error> {
        self.store
            .initialize(&Snapshot::from_ledger(&d::Ledger::default(), 0))?;
        Ok(Output::Initialized)
    }
    pub fn execute(&self, command: Command) -> Result<Output, Error> {
        let command = Validated::try_from(command).map_err(|e| Error::input(e.to_string()))?;
        for _ in 0..self.attempts {
            let snapshot = self.store.load()?;
            let revision = snapshot.revision;
            let ledger = validated_snapshot(snapshot)?;
            let (next, output) = match &command {
                Validated::Resource(id) => {
                    return ledger
                        .resources()
                        .get(id)
                        .map(|r| Output::Resource(r.into()))
                        .ok_or_else(|| Error::domain(d::Error::NotFound));
                }
                Validated::Resources => {
                    return Ok(Output::Resources(
                        ledger.resources().values().map(Into::into).collect(),
                    ));
                }
                Validated::Transaction(id) => {
                    return ledger
                        .transactions()
                        .get(id)
                        .map(|t| Output::Transaction(t.into()))
                        .ok_or_else(|| Error::domain(d::Error::NotFound));
                }
                Validated::Transactions => {
                    return Ok(Output::Transactions(
                        ledger.transactions().values().map(Into::into).collect(),
                    ));
                }
                Validated::Create(spec) => {
                    let next = ledger.create(spec.clone()).map_err(Error::domain)?;
                    let result = Output::Resource((&next.resources()[spec.id()]).into());
                    (next, Ok(result))
                }
                Validated::Prepare(request) => {
                    let next = ledger.prepare(request.clone()).map_err(Error::domain)?;
                    let tx = &next.transactions()[request.id()];
                    let result = match tx.rejection() {
                        Some(d::Rejection::Capacity) => Err(Error::domain(d::Error::Capacity)),
                        Some(d::Rejection::Overflow) => Err(Error::domain(d::Error::Overflow)),
                        Some(d::Rejection::NotFound) => Err(Error::domain(d::Error::NotFound)),
                        None => Ok(Output::Transaction(tx.into())),
                    };
                    (next, result)
                }
                Validated::Commit(id, actuals) => {
                    let next = ledger.commit(id, actuals.clone()).map_err(Error::domain)?;
                    let result = Output::Transaction((&next.transactions()[id]).into());
                    (next, Ok(result))
                }
                Validated::Abort(id) => {
                    let next = ledger.abort(id).map_err(Error::domain)?;
                    let result = Output::Transaction((&next.transactions()[id]).into());
                    (next, Ok(result))
                }
            };
            if next == ledger {
                return output;
            }
            let revision_next = revision.checked_add(1).ok_or_else(|| Error {
                category: Category::Storage,
                code: Code::RevisionExhausted,
                message: "snapshot revision exhausted".into(),
            })?;
            let candidate = Snapshot::from_ledger(&next, revision_next);
            match self.store.replace(revision, &candidate)? {
                Publication::Applied => return output,
                Publication::Conflict => continue,
            }
        }
        Err(StoreError::Contention.into())
    }
}
