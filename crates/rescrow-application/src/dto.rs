//! Version 1 transfer contracts. Decimal quantities are strings, never JSON numbers.
use rescrow_domain as d;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceSpec {
    pub resource_id: String,
    pub lower: String,
    #[serde(deserialize_with = "required")]
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
    pub reservation_id: String,
    /// An authorized idempotency namespace; absent for the implicit local namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    pub operations: Vec<Operation>,
    /// How long the hold may last before it lapses, from 1 ms to 30 days, or
    /// `null` for no automatic expiry. Part of the request: a retry repeats it.
    #[serde(deserialize_with = "required")]
    pub hold_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub context: std::collections::BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorization: Option<AuthorizationFacts>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationFacts {
    pub principal_id: String,
    pub credential_id: String,
    pub policy_digest: String,
}
impl TryFrom<AuthorizationFacts> for d::access::Facts {
    type Error = d::Error;
    fn try_from(v: AuthorizationFacts) -> Result<Self, Self::Error> {
        Self::new(v.principal_id, v.credential_id, v.policy_digest)
    }
}
impl From<&d::access::Facts> for AuthorizationFacts {
    fn from(v: &d::access::Facts) -> Self {
        Self {
            principal_id: v.principal_id().into(),
            credential_id: v.credential_id().into(),
            policy_digest: v.policy_digest().into(),
        }
    }
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
    pub reservation_id: String,
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
    Abort { reservation_id: String },
    ShowResource { resource_id: String },
    ListResources,
    ShowReservation { reservation_id: String },
    ListReservations,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Hold {
    pub reservation_id: String,
    pub delta: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
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
    /// Still prepared when its deadline passed.
    Lapsed,
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
pub struct Reservation {
    pub request: Prepare,
    /// When the hold may lapse, fixed at admission; `null` if it never does.
    #[serde(deserialize_with = "required")]
    pub expires_at_ms: Option<u64>,
    pub state: State,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub schema_version: u32,
    pub revision: u64,
    pub resources: Vec<Resource>,
    pub reservations: Vec<Reservation>,
}
impl Snapshot {
    pub fn from_ledger(ledger: &d::Ledger, revision: u64) -> Self {
        Self {
            schema_version: 1,
            revision,
            resources: ledger.resources().values().map(Resource::from).collect(),
            reservations: ledger
                .reservations()
                .values()
                .map(Reservation::from)
                .collect(),
        }
    }
}
/// Live state as of a log position: resources and reservations still holding capacity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Checkpoint {
    pub schema_version: u32,
    // The invalid empty representation permits an explicit unsupported-schema
    // error for older envelopes; schema-4 validation always requires a UUID.
    #[serde(default)]
    pub authority_generation: String,
    /// Number of log entries this checkpoint reflects.
    pub position: u64,
    /// When the last of those entries was recorded, or zero if there are none.
    pub recorded_at_ms: u64,
    pub resources: Vec<Resource>,
    pub prepared: Vec<Reservation>,
}
/// Versioned pending-hold accounting for one tenant. A vector preserves
/// duplicate detection at the inbound boundary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Pending {
    pub total: u64,
    pub credentials: Vec<CredentialPending>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialPending {
    pub credential_id: String,
    pub count: u64,
}
impl TryFrom<Pending> for d::access::Pending {
    type Error = d::Error;
    fn try_from(v: Pending) -> Result<Self, Self::Error> {
        let pending = Self::restore(
            v.credentials
                .into_iter()
                .map(|c| (c.credential_id, c.count))
                .collect(),
        )?;
        if pending.total() != v.total {
            return Err(d::Error::InvalidLedger);
        }
        Ok(pending)
    }
}
impl From<&d::access::Pending> for Pending {
    fn from(v: &d::access::Pending) -> Self {
        Self {
            total: v.total(),
            credentials: v
                .entries()
                .iter()
                .map(|(id, count)| CredentialPending {
                    credential_id: id.clone(),
                    count: *count,
                })
                .collect(),
        }
    }
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
    Reservation(Reservation),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Entry {
    /// Atomic publication identity; absent only in legacy/test log entries.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication_id: Option<String>,
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
    Reservation(Reservation),
    Reservations(Vec<Reservation>),
}

/// An optional value whose field must still be present, as `null` if absent.
pub(crate) fn required<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> Result<Option<T>, D::Error> {
    Option::<T>::deserialize(deserializer)
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
        let request = Self::new(
            reservation_id(v.namespace, v.reservation_id)?,
            v.operations
                .into_iter()
                .map(d::Operation::try_from)
                .collect::<Result<_, _>>()?,
        )?
        .with_hold(v.hold_ms)?;
        Ok(request
            .with_context(d::access::Context::new(v.context.into_iter().collect())?)
            .with_authorization(
                v.authorization
                    .map(d::access::Facts::try_from)
                    .transpose()?,
            ))
    }
}
impl From<&d::Request> for Prepare {
    fn from(v: &d::Request) -> Self {
        Self {
            reservation_id: v.id().as_str().into(),
            namespace: v.id().namespace().map(str::to_owned),
            operations: v.operations().iter().map(Operation::from).collect(),
            hold_ms: v.hold_ms(),
            context: v.context().entries().clone(),
            authorization: v.authorization().map(Into::into),
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
                .map(|h| {
                    Ok((
                        reservation_id(h.namespace, h.reservation_id)?,
                        decimal(&h.delta)?,
                    ))
                })
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
                    reservation_id: id.as_str().into(),
                    namespace: id.namespace().map(str::to_owned),
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
impl TryFrom<Reservation> for d::Reservation {
    type Error = d::Error;
    fn try_from(v: Reservation) -> Result<Self, Self::Error> {
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
                    AbortReason::Lapsed => d::AbortReason::Lapsed,
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
        Self::restore(request, v.expires_at_ms, state)
    }
}
impl From<&d::Reservation> for Reservation {
    fn from(v: &d::Reservation) -> Self {
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
                    d::AbortReason::Lapsed => AbortReason::Lapsed,
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
            expires_at_ms: v.expires_at_ms(),
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
            v.reservations
                .into_iter()
                .map(d::Reservation::try_from)
                .collect::<Result<_, _>>()?,
        )
    }
}

fn reservation_id(namespace: Option<String>, value: String) -> Result<d::Id, d::Error> {
    let value = id(value)?;
    match namespace {
        None => Ok(value),
        Some(namespace) => d::Id::qualified(id(namespace)?, value),
    }
}
