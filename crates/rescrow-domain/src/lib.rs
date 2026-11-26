//! Pure, immutable local escrow rules. No persistence, serialization, or ambient facts.
#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};

pub mod transition;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    InvalidId,
    InvalidMetadata,
    InvalidQuantity,
    InvalidBounds,
    InvalidOperations,
    InvalidActuals,
    InvalidHold,
    Overflow,
    Capacity(CapacityFailure),
    NotFound,
    Conflict,
    InvalidLedger,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Capacity(failure) => return write!(f, "resource {} requested delta {} but only {} capacity is available", failure.resource.as_str(), failure.requested_delta, failure.available),
            Self::InvalidId => "IDs must contain 1..128 ASCII letters, digits, dots, underscores, or hyphens",
            Self::InvalidQuantity => "quantities must be canonical signed decimal integers",
            Self::InvalidMetadata => "unit must be printable and nonempty (at most 64 bytes); scale must be 0..38",
            Self::InvalidBounds => "quantity must be within the fixed resource bounds",
            Self::InvalidOperations => "operations must be nonempty, nonzero, and unique by operation and resource ID",
            Self::InvalidActuals => "actuals must name every operation once and stay within its reserved direction and magnitude",
            Self::InvalidHold => "a hold must last from 1 ms to 30 days",
            Self::Overflow => "quantity, magnitude, offset, or reservation total exceeds signed 128-bit range",
            Self::NotFound => "referenced resource or transaction does not exist",
            Self::Conflict => "request conflicts with an existing identity or terminal decision",
            Self::InvalidLedger => "resource balances, holds, and transaction records disagree",
        })
    }
}
impl std::error::Error for Error {}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Id(String);
impl Id {
    pub fn new(value: String) -> Result<Self, Error> {
        if value.is_empty()
            || value.len() > 128
            || !value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
        {
            return Err(Error::InvalidId);
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResourceSpec {
    id: Id,
    lower: i128,
    upper: Option<i128>,
    initial: i128,
    unit: String,
    scale: u32,
}
impl ResourceSpec {
    pub fn new(
        id: Id,
        lower: i128,
        upper: Option<i128>,
        initial: i128,
        unit: String,
        scale: u32,
    ) -> Result<Self, Error> {
        if unit.is_empty()
            || unit.len() > 64
            || unit.chars().any(char::is_control)
            || unit.trim() != unit
            || scale > 38
        {
            return Err(Error::InvalidMetadata);
        }
        if initial < lower || upper.is_some_and(|u| u < initial) {
            return Err(Error::InvalidBounds);
        }
        initial.checked_sub(lower).ok_or(Error::Overflow)?;
        if let Some(u) = upper {
            u.checked_sub(lower).ok_or(Error::Overflow)?;
        }
        Ok(Self {
            id,
            lower,
            upper,
            initial,
            unit,
            scale,
        })
    }
    pub fn id(&self) -> &Id {
        &self.id
    }
    pub fn lower(&self) -> i128 {
        self.lower
    }
    pub fn upper(&self) -> Option<i128> {
        self.upper
    }
    pub fn initial(&self) -> i128 {
        self.initial
    }
    pub fn unit(&self) -> &str {
        &self.unit
    }
    pub fn scale(&self) -> u32 {
        self.scale
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Operation {
    id: Id,
    resource: Id,
    delta: i128,
}
impl Operation {
    pub fn new(id: Id, resource: Id, delta: i128) -> Result<Self, Error> {
        if delta == 0 {
            return Err(Error::InvalidOperations);
        }
        delta.checked_abs().ok_or(Error::Overflow)?;
        Ok(Self {
            id,
            resource,
            delta,
        })
    }
    pub fn id(&self) -> &Id {
        &self.id
    }
    pub fn resource(&self) -> &Id {
        &self.resource
    }
    pub fn delta(&self) -> i128 {
        self.delta
    }
}

/// The longest hold a request may ask for, in milliseconds: 30 days.
pub const MAX_HOLD_MS: u64 = 30 * 24 * 60 * 60 * 1000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Request {
    id: Id,
    operations: Vec<Operation>,
    hold_ms: Option<u64>,
}
impl Request {
    pub fn new(id: Id, mut operations: Vec<Operation>) -> Result<Self, Error> {
        let mut resources = BTreeSet::new();
        let mut ids = BTreeSet::new();
        if operations.is_empty() {
            return Err(Error::InvalidOperations);
        }
        for op in &operations {
            if !resources.insert(op.resource.clone()) || !ids.insert(op.id.clone()) {
                return Err(Error::InvalidOperations);
            }
        }
        operations.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(Self {
            id,
            operations,
            hold_ms: None,
        })
    }
    /// Ask for the hold to lapse this long after admission, if it is still
    /// prepared then. Without one, a hold lasts until it is committed or aborted.
    pub fn with_hold(self, hold_ms: Option<u64>) -> Result<Self, Error> {
        if hold_ms.is_some_and(|ms| ms == 0 || ms > MAX_HOLD_MS) {
            return Err(Error::InvalidHold);
        }
        Ok(Self { hold_ms, ..self })
    }
    pub fn id(&self) -> &Id {
        &self.id
    }
    pub fn operations(&self) -> &[Operation] {
        &self.operations
    }
    pub fn hold_ms(&self) -> Option<u64> {
        self.hold_ms
    }
    pub fn actuals(&self, supplied: Option<Vec<(Id, i128)>>) -> Result<Actuals, Error> {
        let mut values = BTreeMap::new();
        match supplied {
            None => {
                for op in &self.operations {
                    values.insert(op.id.clone(), op.delta);
                }
            }
            Some(entries) => {
                for (id, delta) in entries {
                    if values.insert(id, delta).is_some() {
                        return Err(Error::InvalidActuals);
                    }
                }
            }
        }
        if values.len() != self.operations.len() {
            return Err(Error::InvalidActuals);
        }
        for op in &self.operations {
            let a = *values.get(&op.id).ok_or(Error::InvalidActuals)?;
            if (a != 0 && a.signum() != op.delta.signum())
                || a.checked_abs().ok_or(Error::InvalidActuals)? > op.delta.abs()
            {
                return Err(Error::InvalidActuals);
            }
        }
        Ok(Actuals(values))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Actuals(BTreeMap<Id, i128>);
impl Actuals {
    pub fn entries(&self) -> &BTreeMap<Id, i128> {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Rejection {
    Capacity(CapacityFailure),
    Overflow,
    NotFound,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AbortReason {
    Requested,
    Rejected(Rejection),
    /// Still prepared when its hold's deadline passed.
    Lapsed,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum State {
    Prepared,
    Committed(Actuals),
    Aborted(AbortReason),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Transaction {
    request: Request,
    /// When the hold may lapse, fixed at admission from the request's hold.
    expires_at_ms: Option<u64>,
    state: State,
}
impl Transaction {
    pub fn restore(
        request: Request,
        expires_at_ms: Option<u64>,
        state: State,
    ) -> Result<Self, Error> {
        if expires_at_ms.is_some() != request.hold_ms.is_some() {
            return Err(Error::InvalidLedger);
        }
        if let State::Committed(actuals) = &state {
            request.actuals(Some(
                actuals.0.iter().map(|(k, v)| (k.clone(), *v)).collect(),
            ))?;
        }
        if let State::Aborted(AbortReason::Rejected(Rejection::Capacity(failure))) = &state
            && !request
                .operations
                .iter()
                .any(|op| op.resource == failure.resource && op.delta == failure.requested_delta)
        {
            return Err(Error::InvalidLedger);
        }
        Ok(Self {
            request,
            expires_at_ms,
            state,
        })
    }
    pub fn request(&self) -> &Request {
        &self.request
    }
    pub fn expires_at_ms(&self) -> Option<u64> {
        self.expires_at_ms
    }
    /// Prepared, with a deadline at or before `now_ms`.
    pub fn is_due(&self, now_ms: u64) -> bool {
        self.state == State::Prepared && self.expires_at_ms.is_some_and(|at| at <= now_ms)
    }
    pub fn state(&self) -> &State {
        &self.state
    }
    pub fn rejection(&self) -> Option<Rejection> {
        match &self.state {
            State::Aborted(AbortReason::Rejected(r)) => Some(r.clone()),
            _ => None,
        }
    }
}

/// Available capacity and held quantities from one resource value.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Availability {
    available_down: i128,
    available_up: i128,
    reserved_down: i128,
    reserved_up: i128,
}
impl Availability {
    pub fn available_down(&self) -> i128 {
        self.available_down
    }
    pub fn available_up(&self) -> i128 {
        self.available_up
    }
    pub fn reserved_down(&self) -> i128 {
        self.reserved_down
    }
    pub fn reserved_up(&self) -> i128 {
        self.reserved_up
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapacityFailure {
    resource: Id,
    requested_delta: i128,
    available: i128,
}
impl CapacityFailure {
    pub fn new(resource: Id, requested_delta: i128, available: i128) -> Result<Self, Error> {
        let magnitude = requested_delta.checked_abs().ok_or(Error::InvalidLedger)?;
        if available < 0 || magnitude <= available {
            return Err(Error::InvalidLedger);
        }
        Ok(Self {
            resource,
            requested_delta,
            available,
        })
    }
    pub fn resource(&self) -> &Id {
        &self.resource
    }
    pub fn requested_delta(&self) -> i128 {
        self.requested_delta
    }
    pub fn available(&self) -> i128 {
        self.available
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Resource {
    spec: ResourceSpec,
    quantity: i128,
    holds: BTreeMap<Id, i128>,
    availability: Availability,
}
impl Resource {
    pub fn restore(
        spec: ResourceSpec,
        quantity: i128,
        entries: Vec<(Id, i128)>,
    ) -> Result<Self, Error> {
        let mut holds = BTreeMap::new();
        for (id, delta) in entries {
            if delta == 0 || delta == i128::MIN || holds.insert(id, delta).is_some() {
                return Err(Error::InvalidLedger);
            }
        }
        let mut r = Self {
            spec,
            quantity,
            holds,
            availability: Availability::default(),
        };
        r.availability = r.calculate_availability()?;
        Ok(r)
    }
    pub fn spec(&self) -> &ResourceSpec {
        &self.spec
    }
    pub fn quantity(&self) -> i128 {
        self.quantity
    }
    pub fn holds(&self) -> &BTreeMap<Id, i128> {
        &self.holds
    }
    /// Downward capacity, upward capacity, downward holds, upward holds.
    pub fn availability(&self) -> Availability {
        self.availability
    }
    /// Downward capacity, upward capacity, downward holds, upward holds.
    /// Even unbounded resources respect representable i128 offset/quantity limits.
    fn calculate_availability(&self) -> Result<Availability, Error> {
        let offset = self
            .quantity
            .checked_sub(self.spec.lower)
            .ok_or(Error::Overflow)?;
        if offset < 0 {
            return Err(Error::InvalidBounds);
        }
        // Unbounded resources are still capped by quantity and offset representation.
        let ceiling = self
            .spec
            .upper
            .unwrap_or(self.spec.lower.saturating_add(i128::MAX));
        let mut down = 0_i128;
        let mut up = 0_i128;
        for delta in self.holds.values() {
            if *delta < 0 {
                down = down
                    .checked_add(delta.checked_abs().ok_or(Error::Overflow)?)
                    .ok_or(Error::Overflow)?;
            } else {
                up = up.checked_add(*delta).ok_or(Error::Overflow)?;
            }
        }
        let room = ceiling.checked_sub(self.quantity).ok_or(Error::Overflow)?;
        if down > offset || up > room {
            return Err(Error::InvalidBounds);
        }
        Ok(Availability {
            available_down: offset - down,
            available_up: room - up,
            reserved_down: down,
            reserved_up: up,
        })
    }
    /// Reusable participant rule: opposite-direction pending changes never supply capacity.
    pub fn reserve(&self, transaction: &Id, delta: i128) -> Result<Self, Error> {
        if delta == 0 || delta == i128::MIN {
            return Err(Error::InvalidOperations);
        }
        let mut next = self.clone();
        if next.holds.insert(transaction.clone(), delta).is_some() {
            return Err(Error::Conflict);
        }
        next.availability = match next.calculate_availability() {
            Err(Error::InvalidBounds) => {
                let available = if delta < 0 {
                    self.availability.available_down
                } else {
                    self.availability.available_up
                };
                return Err(Error::Capacity(CapacityFailure::new(
                    self.spec.id.clone(),
                    delta,
                    available,
                )?));
            }
            other => other?,
        };
        Ok(next)
    }
    fn settle(&self, transaction: &Id, actual: i128) -> Result<Self, Error> {
        let mut next = self.clone();
        let reserved = next.holds.remove(transaction).ok_or(Error::InvalidLedger)?;
        if (actual != 0 && actual.signum() != reserved.signum())
            || actual.checked_abs().ok_or(Error::InvalidActuals)? > reserved.abs()
        {
            return Err(Error::InvalidActuals);
        }
        next.quantity = next.quantity.checked_add(actual).ok_or(Error::Overflow)?;
        next.availability = next.calculate_availability()?;
        Ok(next)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Ledger {
    resources: BTreeMap<Id, Resource>,
    transactions: BTreeMap<Id, Transaction>,
}
impl Ledger {
    pub fn resources(&self) -> &BTreeMap<Id, Resource> {
        &self.resources
    }
    pub fn transactions(&self) -> &BTreeMap<Id, Transaction> {
        &self.transactions
    }
    pub fn create(&self, spec: ResourceSpec) -> Result<Self, Error> {
        let changes = transition::create(spec.clone(), self.resources.get(spec.id()))?;
        Ok(self.apply(changes))
    }
    pub fn prepare(&self, request: Request, now_ms: u64) -> Result<Self, Error> {
        let changes = transition::prepare(
            request.clone(),
            self.transactions.get(request.id()),
            &self.resources,
            now_ms,
        )?;
        Ok(self.apply(changes))
    }
    pub fn commit(&self, id: &Id, supplied: Option<Vec<(Id, i128)>>) -> Result<Self, Error> {
        let tx = self.transactions.get(id).ok_or(Error::NotFound)?;
        Ok(self.apply(transition::commit(tx, supplied, &self.resources)?))
    }
    pub fn abort(&self, id: &Id) -> Result<Self, Error> {
        let tx = self.transactions.get(id).ok_or(Error::NotFound)?;
        Ok(self.apply(transition::abort(tx, &self.resources)?))
    }
    fn apply(&self, changes: transition::Changes) -> Self {
        let mut next = self.clone();
        for resource in changes.resources() {
            next.resources
                .insert(resource.spec().id().clone(), resource.clone());
        }
        if let Some(tx) = changes.transaction() {
            next.transactions
                .insert(tx.request().id().clone(), tx.clone());
        }
        next
    }
    /// Reconstruct trusted state only after validating every cross-record invariant.
    pub fn restore(
        resources: Vec<Resource>,
        transactions: Vec<Transaction>,
    ) -> Result<Self, Error> {
        let mut ledger = Self::default();
        for r in resources {
            if ledger.resources.insert(r.spec.id.clone(), r).is_some() {
                return Err(Error::InvalidLedger);
            }
        }
        for tx in transactions {
            if ledger
                .transactions
                .insert(tx.request.id.clone(), tx)
                .is_some()
            {
                return Err(Error::InvalidLedger);
            }
        }
        let mut expected: BTreeMap<Id, BTreeMap<Id, i128>> = ledger
            .resources
            .keys()
            .map(|id| (id.clone(), BTreeMap::new()))
            .collect();
        let mut quantities: BTreeMap<Id, Vec<i128>> = ledger
            .resources
            .iter()
            .map(|(id, r)| (id.clone(), vec![r.spec.initial]))
            .collect();
        for (id, tx) in &ledger.transactions {
            for op in &tx.request.operations {
                // Only a retained missing-resource rejection may reference an
                // absent resource. It cannot carry any hold or committed effect.
                if tx.state == State::Aborted(AbortReason::Rejected(Rejection::NotFound)) {
                    continue;
                }
                let holds = expected.get_mut(&op.resource).ok_or(Error::InvalidLedger)?;
                match &tx.state {
                    State::Prepared => {
                        holds.insert(id.clone(), op.delta);
                    }
                    State::Committed(a) => {
                        quantities
                            .get_mut(&op.resource)
                            .ok_or(Error::InvalidLedger)?
                            .push(a.0[&op.id]);
                    }
                    State::Aborted(_) => {}
                }
            }
        }
        for (id, r) in &ledger.resources {
            if r.holds != expected[id] || r.quantity != checked_sum(&quantities[id])? {
                return Err(Error::InvalidLedger);
            }
            r.calculate_availability()?;
        }
        Ok(ledger)
    }
}

// Sum opposite signs first so a valid final total doesn't depend on transaction ID order.
fn checked_sum(values: &[i128]) -> Result<i128, Error> {
    let mut positive = values.iter().copied().filter(|v| *v >= 0);
    let mut negative = values.iter().copied().filter(|v| *v < 0);
    let mut sum = 0_i128;
    loop {
        let value = if sum >= 0 {
            negative.next().or_else(|| positive.next())
        } else {
            positive.next().or_else(|| negative.next())
        };
        match value {
            Some(v) => sum = sum.checked_add(v).ok_or(Error::Overflow)?,
            None => return Ok(sum),
        }
    }
}

/// Current resources and the transactions still holding capacity. Terminal
/// history lives outside this value, so its size depends only on resources and
/// open reservations.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Live {
    resources: BTreeMap<Id, Resource>,
    prepared: BTreeMap<Id, Transaction>,
}
impl Live {
    pub fn resources(&self) -> &BTreeMap<Id, Resource> {
        &self.resources
    }
    pub fn prepared(&self) -> &BTreeMap<Id, Transaction> {
        &self.prepared
    }
    /// Prepared transactions holding any of `resources` whose deadline has
    /// passed by `now_ms`, in ID order.
    pub fn due<'a>(
        &self,
        resources: impl IntoIterator<Item = &'a Id>,
        now_ms: u64,
    ) -> Vec<&Transaction> {
        let ids: BTreeSet<&Id> = resources
            .into_iter()
            .filter_map(|id| self.resources.get(id))
            .flat_map(|r| r.holds.keys())
            .collect();
        ids.into_iter()
            .filter_map(|id| self.prepared.get(id))
            .filter(|tx| tx.is_due(now_ms))
            .collect()
    }
    /// Validate that holds and prepared transactions describe each other exactly.
    /// Balances are not reconstructed from history; a full Ledger restore does that.
    pub fn restore(resources: Vec<Resource>, prepared: Vec<Transaction>) -> Result<Self, Error> {
        let mut live = Self::default();
        for r in resources {
            if live.resources.insert(r.spec.id.clone(), r).is_some() {
                return Err(Error::InvalidLedger);
            }
        }
        let mut expected: BTreeMap<&Id, BTreeMap<Id, i128>> = live
            .resources
            .keys()
            .map(|id| (id, BTreeMap::new()))
            .collect();
        for tx in &prepared {
            if tx.state != State::Prepared {
                return Err(Error::InvalidLedger);
            }
            for op in &tx.request.operations {
                expected
                    .get_mut(&op.resource)
                    .ok_or(Error::InvalidLedger)?
                    .insert(tx.request.id.clone(), op.delta);
            }
        }
        for (id, r) in &live.resources {
            if r.holds != expected[id] {
                return Err(Error::InvalidLedger);
            }
        }
        for tx in prepared {
            if live.prepared.insert(tx.request.id.clone(), tx).is_some() {
                return Err(Error::InvalidLedger);
            }
        }
        Ok(live)
    }
    /// Make a transition's replacements visible together.
    pub fn apply(&self, changes: &transition::Changes) -> Self {
        let mut next = self.clone();
        for resource in changes.resources() {
            next.resources
                .insert(resource.spec.id.clone(), resource.clone());
        }
        if let Some(tx) = changes.transaction() {
            if tx.state == State::Prepared {
                next.prepared.insert(tx.request.id.clone(), tx.clone());
            } else {
                next.prepared.remove(&tx.request.id);
            }
        }
        next
    }
}
impl Ledger {
    /// The live part of a validated ledger.
    pub fn live(&self) -> Live {
        Live {
            resources: self.resources.clone(),
            prepared: self
                .transactions
                .iter()
                .filter(|(_, tx)| tx.state == State::Prepared)
                .map(|(id, tx)| (id.clone(), tx.clone()))
                .collect(),
        }
    }
}
