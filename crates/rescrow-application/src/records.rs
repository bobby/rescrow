//! Incremental Application over an immutable read view and one atomic root.
//! Record conversion proves local invariants; controlled transitions preserve
//! cross-record invariants. Import and explicit audit validate the full ledger.
use crate::{Error, Publication, StoreError, ValidatedSnapshot, dto};
use rescrow_domain::{self as d, transition};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, future::Future};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Key {
    Resource(String),
    Transaction(String),
}

/// Versioned storage transfer contract. The adapter owns its byte encoding.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "record",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Record {
    Root { schema_version: u32, revision: u64 },
    Resource(dto::Resource),
    Transaction(dto::Transaction),
}

/// Domain-proven replacements and the checked next Application revision.
/// Adapters can inspect but cannot manufacture a batch.
#[derive(Clone, Debug)]
pub struct Changes {
    revision: u64,
    records: Vec<Record>,
}
impl Changes {
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn records(&self) -> &[Record] {
        &self.records
    }
    fn new(revision: u64, changes: &transition::Changes) -> Result<Self, Error> {
        let revision = revision.checked_add(1).ok_or_else(|| Error {
            category: crate::Category::Storage,
            code: crate::Code::RevisionExhausted,
            message: "record revision exhausted".into(),
            transaction_id: None,
            capacity: None,
        })?;
        let mut records: Vec<_> = changes
            .resources()
            .iter()
            .map(|r| Record::Resource(r.into()))
            .collect();
        if let Some(tx) = changes.transaction() {
            records.push(Record::Transaction(tx.into()));
        }
        Ok(Self { revision, records })
    }
}

/// Every get/scan on a view observes the same immutable root. The adapter
/// validates physical keys against embedded IDs, including during scans.
pub trait ReadView: Send + Sync {
    fn revision(&self) -> u64;
    fn get(&self, key: &Key) -> impl Future<Output = Result<Option<Record>, StoreError>> + Send;
    fn scan(&self) -> impl Future<Output = Result<Vec<Record>, StoreError>> + Send;
}

/// Publication flushes all referenced dependencies before CAS of one root.
/// A conflict must change nothing authoritative; an ambiguous publication must
/// return Unknown. Initialize accepts only a fully validated import and never
/// replaces an existing root. Ordinary loads never initialize missing state.
pub trait RecordStore: Send + Sync {
    type View: ReadView;
    fn initialize(
        &self,
        snapshot: &ValidatedSnapshot,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
    fn load(&self) -> impl Future<Output = Result<Self::View, StoreError>> + Send;
    fn publish(
        &self,
        expected: &Self::View,
        changes: &Changes,
    ) -> impl Future<Output = Result<Publication, StoreError>> + Send;
}

pub struct RecordApplication<S> {
    store: S,
}
impl<S: RecordStore> RecordApplication<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }
    pub async fn initialize(&self) -> Result<(), Error> {
        self.store
            .initialize(&ValidatedSnapshot::from_ledger(d::Ledger::default(), 0))
            .await
            .map_err(Into::into)
    }
    /// Imports are full validation boundaries, never unchecked lazy adoption.
    pub async fn import(&self, snapshot: dto::Snapshot) -> Result<(), Error> {
        self.store
            .initialize(&snapshot.try_into()?)
            .await
            .map_err(Into::into)
    }
    /// Explicit O(dataset) verification. Point operations do not silently scan
    /// historical transactions to reconstruct every resource's quantity.
    pub async fn audit(&self) -> Result<ValidatedSnapshot, Error> {
        let view = self.store.load().await?;
        snapshot(&view).await?.try_into().map_err(Into::into)
    }
    pub async fn execute(&self, command: dto::Command) -> Result<dto::Output, Error> {
        let tx_id = match &command {
            dto::Command::Prepare(v) => Some(v.transaction_id.clone()),
            dto::Command::Commit(v) => Some(v.transaction_id.clone()),
            dto::Command::Abort { transaction_id }
            | dto::Command::ShowTransaction { transaction_id } => Some(transaction_id.clone()),
            _ => None,
        };
        let result = self.execute_inner(command).await;
        result.map_err(|mut e| {
            e.transaction_id = tx_id;
            e
        })
    }
    async fn execute_inner(&self, command: dto::Command) -> Result<dto::Output, Error> {
        let command = Command::try_from(command)?;
        for _ in 0..32 {
            let view = self.store.load().await?;
            let (changes, output) = evaluate(&view, &command).await?;
            if changes.is_empty() {
                return output;
            }
            let changes = Changes::new(view.revision(), &changes)?;
            match self.store.publish(&view, &changes).await? {
                Publication::Applied => return output,
                Publication::Conflict => continue,
            }
        }
        Err(StoreError::Contention.into())
    }
}

enum Command {
    Create(d::ResourceSpec),
    Prepare(d::Request),
    Commit(d::Id, Option<Vec<(d::Id, i128)>>),
    Abort(d::Id),
    Resource(d::Id),
    Transaction(d::Id),
    Resources,
    Transactions,
}
impl TryFrom<dto::Command> for Command {
    type Error = Error;
    fn try_from(command: dto::Command) -> Result<Self, Error> {
        use dto::Command as C;
        Ok(match command {
            C::CreateResource(v) => Self::Create(v.try_into().map_err(crate::input)?),
            C::Prepare(v) => Self::Prepare(v.try_into().map_err(crate::input)?),
            C::Commit(v) => Self::Commit(
                d::Id::new(v.transaction_id).map_err(crate::input)?,
                v.actuals
                    .map(|values| {
                        values
                            .into_iter()
                            .map(|a| Ok((d::Id::new(a.operation_id)?, dto::decimal(&a.delta)?)))
                            .collect::<Result<Vec<_>, d::Error>>()
                    })
                    .transpose()
                    .map_err(crate::input)?,
            ),
            C::Abort { transaction_id } => {
                Self::Abort(d::Id::new(transaction_id).map_err(crate::input)?)
            }
            C::ShowResource { resource_id } => {
                Self::Resource(d::Id::new(resource_id).map_err(crate::input)?)
            }
            C::ShowTransaction { transaction_id } => {
                Self::Transaction(d::Id::new(transaction_id).map_err(crate::input)?)
            }
            C::ListResources => Self::Resources,
            C::ListTransactions => Self::Transactions,
        })
    }
}

fn corrupt(error: impl std::fmt::Display) -> Error {
    StoreError::Corrupt(error.to_string()).into()
}

async fn resource(view: &impl ReadView, id: &d::Id) -> Result<Option<d::Resource>, Error> {
    match view.get(&Key::Resource(id.as_str().into())).await? {
        None => Ok(None),
        Some(Record::Resource(value)) if value.spec.resource_id == id.as_str() => {
            let resource: d::Resource = value.try_into().map_err(corrupt)?;
            // Validate accessed hold relationships without reading terminal history.
            for (tx_id, delta) in resource.holds() {
                let tx = transaction(view, tx_id)
                    .await?
                    .ok_or_else(|| corrupt("orphan resource hold"))?;
                if *tx.state() != d::State::Prepared
                    || !tx
                        .request()
                        .operations()
                        .iter()
                        .any(|op| op.resource() == id && op.delta() == *delta)
                {
                    return Err(corrupt("hold disagrees with prepared transaction"));
                }
            }
            Ok(Some(resource))
        }
        Some(_) => Err(corrupt("resource record/key mismatch")),
    }
}
async fn transaction(view: &impl ReadView, id: &d::Id) -> Result<Option<d::Transaction>, Error> {
    match view.get(&Key::Transaction(id.as_str().into())).await? {
        None => Ok(None),
        Some(Record::Transaction(value)) if value.request.transaction_id == id.as_str() => {
            value.try_into().map(Some).map_err(corrupt)
        }
        Some(_) => Err(corrupt("transaction record/key mismatch")),
    }
}
async fn participants(
    view: &impl ReadView,
    request: &d::Request,
) -> Result<BTreeMap<d::Id, d::Resource>, Error> {
    let mut resources = BTreeMap::new();
    for op in request.operations() {
        if let Some(value) = resource(view, op.resource()).await? {
            resources.insert(op.resource().clone(), value);
        }
    }
    Ok(resources)
}

type Evaluation = (transition::Changes, Result<dto::Output, Error>);
async fn evaluate(view: &impl ReadView, command: &Command) -> Result<Evaluation, Error> {
    use Command as C;
    let unchanged = transition::Changes::default();
    match command {
        C::Create(spec) => {
            let existing = resource(view, spec.id()).await?;
            let changes =
                transition::create(spec.clone(), existing.as_ref()).map_err(Error::domain)?;
            let value = changes
                .resources()
                .first()
                .or(existing.as_ref())
                .ok_or_else(|| corrupt("missing creation result"))?;
            let output = dto::Output::Resource(value.into());
            Ok((changes, Ok(output)))
        }
        C::Prepare(request) => {
            let existing = transaction(view, request.id()).await?;
            let resources =
                participants(view, existing.as_ref().map_or(request, |tx| tx.request())).await?;
            if let Some(tx) = &existing {
                transition::validate_participants(tx, &resources).map_err(corrupt)?;
            }
            let changes = transition::prepare(request.clone(), existing.as_ref(), &resources)
                .map_err(transition_error)?;
            let tx = changes
                .transaction()
                .or(existing.as_ref())
                .ok_or_else(|| corrupt("missing preparation result"))?;
            let output = match tx.rejection() {
                Some(reason) => Err(Error::rejection(reason)),
                None => Ok(dto::Output::Transaction(tx.into())),
            };
            Ok((changes, output))
        }
        C::Commit(id, _) | C::Abort(id) | C::Transaction(id) => {
            let tx = transaction(view, id)
                .await?
                .ok_or_else(|| Error::domain(d::Error::NotFound))?;
            let resources = participants(view, tx.request()).await?;
            transition::validate_participants(&tx, &resources).map_err(corrupt)?;
            let changes = match command {
                C::Commit(_, actuals) => transition::commit(&tx, actuals.clone(), &resources),
                C::Abort(_) => transition::abort(&tx, &resources),
                _ => Ok(unchanged),
            }
            .map_err(transition_error)?;
            let output = dto::Output::Transaction(changes.transaction().unwrap_or(&tx).into());
            Ok((changes, Ok(output)))
        }
        C::Resource(id) => {
            let value = resource(view, id)
                .await?
                .ok_or_else(|| Error::domain(d::Error::NotFound))?;
            Ok((unchanged, Ok(dto::Output::Resource((&value).into()))))
        }
        C::Resources | C::Transactions => {
            // Listing is explicitly a full read; validate all relationships as
            // well as each record before exposing the resulting transfer values.
            let validated: ValidatedSnapshot = snapshot(view).await?.try_into()?;
            let dto = validated.to_dto();
            let output = if matches!(command, C::Resources) {
                dto::Output::Resources(dto.resources)
            } else {
                dto::Output::Transactions(dto.transactions)
            };
            Ok((unchanged, Ok(output)))
        }
    }
}
fn transition_error(error: d::Error) -> Error {
    if error == d::Error::InvalidLedger {
        corrupt(error)
    } else {
        Error::domain(error)
    }
}
async fn snapshot(view: &impl ReadView) -> Result<dto::Snapshot, Error> {
    let mut resources = Vec::new();
    let mut transactions = Vec::new();
    for record in view.scan().await? {
        match record {
            Record::Resource(value) => resources.push(value),
            Record::Transaction(value) => transactions.push(value),
            Record::Root { .. } => {
                return Err(corrupt("root metadata returned as a business record"));
            }
        }
    }
    Ok(dto::Snapshot {
        schema_version: 1,
        revision: view.revision(),
        resources,
        transactions,
    })
}
