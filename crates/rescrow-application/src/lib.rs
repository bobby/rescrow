//! Synchronous use cases and validated transfer boundary, independent of adapters.
pub mod dto;
use dto::{Command, Output, Snapshot};
use rescrow_domain as d;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

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
                write!(f, "unsupported snapshot schema version {version}")
            }
            Self::Io(detail) => write!(f, "storage I/O failed: {detail}"),
            Self::TooLarge => f.write_str(
                "snapshot or its reserved settlement space exceeds the adapter's size limit",
            ),
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

/// A validated immutable ledger with storage metadata. Adapters validate each
/// decoded image once, before returning it. Sharing this value never permits mutation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidatedSnapshot {
    revision: u64,
    ledger: Arc<d::Ledger>,
}
impl ValidatedSnapshot {
    pub fn from_ledger(ledger: d::Ledger, revision: u64) -> Self {
        Self {
            revision,
            ledger: Arc::new(ledger),
        }
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn ledger(&self) -> &d::Ledger {
        &self.ledger
    }
    pub fn to_dto(&self) -> Snapshot {
        Snapshot::from_ledger(&self.ledger, self.revision)
    }
}
impl TryFrom<Snapshot> for ValidatedSnapshot {
    type Error = StoreError;
    fn try_from(snapshot: Snapshot) -> Result<Self, Self::Error> {
        if snapshot.schema_version != 1 {
            return Err(StoreError::Unsupported(snapshot.schema_version));
        }
        let revision = snapshot.revision;
        let ledger =
            d::Ledger::try_from(snapshot).map_err(|e| StoreError::Corrupt(e.to_string()))?;
        Ok(Self::from_ledger(ledger, revision))
    }
}

/// A load acknowledges durability, including a prior publication with a lost response.
/// CAS compares under the publication lock. Every decoded image crosses TryFrom
/// once; candidates already carry the Application's construction proof. Errors
/// after publication must be Unknown, never Io. Initialization never replaces state.
pub trait SnapshotStore {
    fn initialize(&self, empty: &ValidatedSnapshot) -> Result<(), StoreError>;
    fn load(&self) -> Result<ValidatedSnapshot, StoreError>;
    fn replace(
        &self,
        expected_revision: u64,
        candidate: &ValidatedSnapshot,
    ) -> Result<Publication, StoreError>;
}

pub fn validated_snapshot(snapshot: Snapshot) -> Result<d::Ledger, Error> {
    let validated = ValidatedSnapshot::try_from(snapshot)?;
    Ok(Arc::unwrap_or_clone(validated.ledger))
}

/// Synchronous typed use cases shared by CLI and embedded Rust callers.
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
    pub fn initialize(&self) -> Result<(), Error> {
        self.store
            .initialize(&ValidatedSnapshot::from_ledger(d::Ledger::default(), 0))?;
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
        self.mutate(
            |ledger| ledger.create(spec.clone()),
            |next| Ok((&next.resources()[spec.id()]).into()),
        )
    }
    /// Admit a frozen request; business rejections are persisted before returning an error.
    pub fn prepare(&self, request: dto::Prepare) -> Result<dto::Transaction, Error> {
        let id = request.transaction_id.clone();
        let result = (|| {
            let request: d::Request = request.try_into().map_err(input)?;
            self.mutate(
                |ledger| ledger.prepare(request.clone()),
                |next| {
                    let tx = &next.transactions()[request.id()];
                    match tx.rejection() {
                        Some(reason) => Err(Error::rejection(reason)),
                        None => Ok(tx.into()),
                    }
                },
            )
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
            self.mutate(
                |ledger| ledger.commit(&transaction, actuals.clone()),
                |next| Ok((&next.transactions()[&transaction]).into()),
            )
        })();
        result.map_err(|e| e.with_transaction(&id))
    }
    /// Release a prepared hold. An existing committed decision conflicts.
    pub fn abort(&self, id: &str) -> Result<dto::Transaction, Error> {
        let result = (|| {
            let transaction = d::Id::new(id.into()).map_err(input)?;
            self.mutate(
                |ledger| ledger.abort(&transaction),
                |next| Ok((&next.transactions()[&transaction]).into()),
            )
        })();
        result.map_err(|e| e.with_transaction(id))
    }
    pub fn resource(&self, id: &str) -> Result<dto::Resource, Error> {
        let id = d::Id::new(id.into()).map_err(input)?;
        self.store
            .load()?
            .ledger()
            .resources()
            .get(&id)
            .map(Into::into)
            .ok_or_else(|| Error::domain(d::Error::NotFound))
    }
    pub fn resources(&self) -> Result<Vec<dto::Resource>, Error> {
        Ok(self
            .store
            .load()?
            .ledger()
            .resources()
            .values()
            .map(Into::into)
            .collect())
    }
    pub fn transaction(&self, id: &str) -> Result<dto::Transaction, Error> {
        let result = (|| {
            let transaction = d::Id::new(id.into()).map_err(input)?;
            self.store
                .load()?
                .ledger()
                .transactions()
                .get(&transaction)
                .map(Into::into)
                .ok_or_else(|| Error::domain(d::Error::NotFound))
        })();
        result.map_err(|e| e.with_transaction(id))
    }
    pub fn transactions(&self) -> Result<Vec<dto::Transaction>, Error> {
        Ok(self
            .store
            .load()?
            .ledger()
            .transactions()
            .values()
            .map(Into::into)
            .collect())
    }
    fn mutate<R>(
        &self,
        change: impl Fn(&d::Ledger) -> Result<d::Ledger, d::Error>,
        result: impl Fn(&d::Ledger) -> Result<R, Error>,
    ) -> Result<R, Error> {
        for _ in 0..self.attempts {
            let snapshot = self.store.load()?;
            let ledger = snapshot.ledger();
            let next = change(ledger).map_err(Error::domain)?;
            let output = result(&next);
            if next == *ledger {
                return output;
            }
            let revision = snapshot.revision().checked_add(1).ok_or_else(|| Error {
                category: Category::Storage,
                code: Code::RevisionExhausted,
                message: "snapshot revision exhausted".into(),
                transaction_id: None,
                capacity: None,
            })?;
            let candidate = ValidatedSnapshot::from_ledger(next, revision);
            match self.store.replace(snapshot.revision(), &candidate)? {
                Publication::Applied => return output,
                Publication::Conflict => continue,
            }
        }
        Err(StoreError::Contention.into())
    }
}
fn input(error: d::Error) -> Error {
    Error::input(error.to_string())
}
