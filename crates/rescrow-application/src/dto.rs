//! Version 1 transfer contracts. Decimal quantities are strings, never JSON numbers.
use rescrow_domain as d;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceSpec {
    pub resource_id: String,
    pub lower: String,
    #[serde(deserialize_with = "required_option")]
    pub upper: Option<String>,
    pub initial: String,
    pub unit: String,
    pub scale: u32,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Operation {
    pub operation_id: String,
    pub resource_id: String,
    pub delta: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Prepare {
    /// A UUIDv7, in canonical lowercase form, for a new prepare. Its timestamp
    /// is when the client first created the request; every retry repeats it.
    pub transaction_id: String,
    pub operations: Vec<Operation>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Actual {
    pub operation_id: String,
    pub delta: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Commit {
    pub transaction_id: String,
    pub actuals: Option<Vec<Actual>>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "command",
    content = "input",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Command {
    CreateResource(ResourceSpec),
    Prepare(Prepare),
    Commit(Commit),
    Abort { transaction_id: String },
    ShowResource { resource_id: String },
    ListResources,
    ShowTransaction { transaction_id: String },
    ListTransactions,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Hold {
    pub transaction_id: String,
    pub delta: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Resource {
    pub spec: ResourceSpec,
    pub quantity: String,
    pub holds: Vec<Hold>,
    pub reserved_down: String,
    pub reserved_up: String,
    pub available_down: String,
    /// Finite representable room, including i128/offset limits for unbounded resources.
    pub available_up: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Rejection {
    Capacity,
    Overflow,
    NotFound,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "reason",
    content = "rejection",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum AbortReason {
    Requested,
    Rejected(Rejection),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum State {
    Prepared {},
    Committed {
        actuals: Vec<Actual>,
    },
    Aborted {
        cause: AbortReason,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        capacity: Option<CapacityFailure>,
    },
}
/// Frozen admission facts, shared by persisted rejections and structured errors.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapacityFailure {
    pub resource_id: String,
    pub requested_delta: String,
    pub available: String,
}
impl TryFrom<CapacityFailure> for d::CapacityFailure {
    type Error = d::Error;
    fn try_from(v: CapacityFailure) -> Result<Self, Self::Error> {
        Self::new(
            id(v.resource_id)?,
            decimal(&v.requested_delta)?,
            decimal(&v.available)?,
        )
    }
}
impl From<&d::CapacityFailure> for CapacityFailure {
    fn from(v: &d::CapacityFailure) -> Self {
        Self {
            resource_id: v.resource().as_str().into(),
            requested_delta: v.requested_delta().to_string(),
            available: v.available().to_string(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Transaction {
    pub request: Prepare,
    pub state: State,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub schema_version: u32,
    pub revision: u64,
    pub resources: Vec<Resource>,
    pub transactions: Vec<Transaction>,
}
impl Snapshot {
    pub fn from_ledger(ledger: &d::Ledger, revision: u64) -> Self {
        Self {
            schema_version: 1,
            revision,
            resources: ledger.resources().values().map(Resource::from).collect(),
            transactions: ledger
                .transactions()
                .values()
                .map(Transaction::from)
                .collect(),
        }
    }
}
/// Live state as of a log position: resources and transactions still holding capacity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Checkpoint {
    pub schema_version: u32,
    /// Number of log entries this checkpoint reflects.
    pub position: u64,
    /// When the last of those entries was recorded, or zero if there are none.
    pub recorded_at_ms: u64,
    pub resources: Vec<Resource>,
    pub prepared: Vec<Transaction>,
}
/// The outcome of one state change, as appended to the log.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "record",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Record {
    ResourceCreated(ResourceSpec),
    Transaction(Transaction),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Entry {
    /// Zero-based log position; also the position a conditional append expects.
    pub sequence: u64,
    /// When the Application recorded it, in milliseconds since the Unix epoch.
    /// Never earlier than the entry before it.
    pub recorded_at_ms: u64,
    pub record: Record,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "result",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Output {
    Initialized,
    Resource(Resource),
    Resources(Vec<Resource>),
    Transaction(Transaction),
    Transactions(Vec<Transaction>),
}

fn required_option<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<String>, D::Error> {
    Option::<String>::deserialize(deserializer)
}

pub fn decimal(value: &str) -> Result<i128, d::Error> {
    let parsed = value.parse::<i128>().map_err(|error| match error.kind() {
        std::num::IntErrorKind::PosOverflow | std::num::IntErrorKind::NegOverflow => {
            d::Error::Overflow
        }
        _ => d::Error::InvalidQuantity,
    })?;
    if parsed.to_string() != value {
        return Err(d::Error::InvalidQuantity);
    }
    Ok(parsed)
}
fn id(value: String) -> Result<d::Id, d::Error> {
    d::Id::new(value)
}
impl TryFrom<ResourceSpec> for d::ResourceSpec {
    type Error = d::Error;
    fn try_from(v: ResourceSpec) -> Result<Self, Self::Error> {
        Self::new(
            id(v.resource_id)?,
            decimal(&v.lower)?,
            v.upper.as_deref().map(decimal).transpose()?,
            decimal(&v.initial)?,
            v.unit,
            v.scale,
        )
    }
}
impl From<&d::ResourceSpec> for ResourceSpec {
    fn from(v: &d::ResourceSpec) -> Self {
        Self {
            resource_id: v.id().as_str().into(),
            lower: v.lower().to_string(),
            upper: v.upper().map(|u| u.to_string()),
            initial: v.initial().to_string(),
            unit: v.unit().into(),
            scale: v.scale(),
        }
    }
}
impl TryFrom<Operation> for d::Operation {
    type Error = d::Error;
    fn try_from(v: Operation) -> Result<Self, Self::Error> {
        Self::new(id(v.operation_id)?, id(v.resource_id)?, decimal(&v.delta)?)
    }
}
impl From<&d::Operation> for Operation {
    fn from(v: &d::Operation) -> Self {
        Self {
            operation_id: v.id().as_str().into(),
            resource_id: v.resource().as_str().into(),
            delta: v.delta().to_string(),
        }
    }
}
impl TryFrom<Prepare> for d::Request {
    type Error = d::Error;
    fn try_from(v: Prepare) -> Result<Self, Self::Error> {
        Self::new(
            id(v.transaction_id)?,
            v.operations
                .into_iter()
                .map(d::Operation::try_from)
                .collect::<Result<_, _>>()?,
        )
    }
}
impl From<&d::Request> for Prepare {
    fn from(v: &d::Request) -> Self {
        Self {
            transaction_id: v.id().as_str().into(),
            operations: v.operations().iter().map(Operation::from).collect(),
        }
    }
}
impl TryFrom<Resource> for d::Resource {
    type Error = d::Error;
    fn try_from(v: Resource) -> Result<Self, Self::Error> {
        let r = Self::restore(
            v.spec.try_into()?,
            decimal(&v.quantity)?,
            v.holds
                .into_iter()
                .map(|h| Ok((id(h.transaction_id)?, decimal(&h.delta)?)))
                .collect::<Result<_, d::Error>>()?,
        )?;
        let a = r.availability();
        if a.available_down() != decimal(&v.available_down)?
            || a.available_up() != decimal(&v.available_up)?
            || a.reserved_down() != decimal(&v.reserved_down)?
            || a.reserved_up() != decimal(&v.reserved_up)?
        {
            return Err(d::Error::InvalidLedger);
        }
        Ok(r)
    }
}
impl From<&d::Resource> for Resource {
    fn from(v: &d::Resource) -> Self {
        // All constructors and transitions preserve this invariant. No fallible encoding here.
        let a = v.availability();
        Self {
            spec: v.spec().into(),
            quantity: v.quantity().to_string(),
            holds: v
                .holds()
                .iter()
                .map(|(id, delta)| Hold {
                    transaction_id: id.as_str().into(),
                    delta: delta.to_string(),
                })
                .collect(),
            reserved_down: a.reserved_down().to_string(),
            reserved_up: a.reserved_up().to_string(),
            available_down: a.available_down().to_string(),
            available_up: a.available_up().to_string(),
        }
    }
}
impl TryFrom<Transaction> for d::Transaction {
    type Error = d::Error;
    fn try_from(v: Transaction) -> Result<Self, Self::Error> {
        let request: d::Request = v.request.try_into()?;
        let state = match v.state {
            State::Prepared {} => d::State::Prepared,
            State::Committed { actuals } => d::State::Committed(
                request.actuals(Some(
                    actuals
                        .into_iter()
                        .map(|a| Ok((id(a.operation_id)?, decimal(&a.delta)?)))
                        .collect::<Result<_, d::Error>>()?,
                ))?,
            ),
            State::Aborted { cause, capacity } => {
                if capacity.is_some() && cause != AbortReason::Rejected(Rejection::Capacity) {
                    return Err(d::Error::InvalidLedger);
                }
                d::State::Aborted(match cause {
                    AbortReason::Requested => d::AbortReason::Requested,
                    AbortReason::Rejected(r) => d::AbortReason::Rejected(match r {
                        Rejection::Capacity => d::Rejection::Capacity(
                            capacity.ok_or(d::Error::InvalidLedger)?.try_into()?,
                        ),
                        Rejection::Overflow => d::Rejection::Overflow,
                        Rejection::NotFound => d::Rejection::NotFound,
                    }),
                })
            }
        };
        Self::restore(request, state)
    }
}
impl From<&d::Transaction> for Transaction {
    fn from(v: &d::Transaction) -> Self {
        let state = match v.state() {
            d::State::Prepared => State::Prepared {},
            d::State::Committed(a) => State::Committed {
                actuals: a
                    .entries()
                    .iter()
                    .map(|(id, delta)| Actual {
                        operation_id: id.as_str().into(),
                        delta: delta.to_string(),
                    })
                    .collect(),
            },
            d::State::Aborted(cause) => State::Aborted {
                capacity: match cause {
                    d::AbortReason::Rejected(d::Rejection::Capacity(failure)) => {
                        Some(failure.into())
                    }
                    _ => None,
                },
                cause: match cause {
                    d::AbortReason::Requested => AbortReason::Requested,
                    d::AbortReason::Rejected(r) => AbortReason::Rejected(match r {
                        d::Rejection::Capacity(_) => Rejection::Capacity,
                        d::Rejection::Overflow => Rejection::Overflow,
                        d::Rejection::NotFound => Rejection::NotFound,
                    }),
                },
            },
        };
        Self {
            request: v.request().into(),
            state,
        }
    }
}
impl TryFrom<Snapshot> for d::Ledger {
    type Error = d::Error;
    fn try_from(v: Snapshot) -> Result<Self, Self::Error> {
        if v.schema_version != 1 {
            return Err(d::Error::InvalidLedger);
        }
        Self::restore(
            v.resources
                .into_iter()
                .map(d::Resource::try_from)
                .collect::<Result<_, _>>()?,
            v.transactions
                .into_iter()
                .map(d::Transaction::try_from)
                .collect::<Result<_, _>>()?,
        )
    }
}
