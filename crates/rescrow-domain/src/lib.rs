//! Pure, immutable local escrow rules. No persistence, serialization, or ambient facts.
#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    InvalidId,
    InvalidMetadata,
    InvalidQuantity,
    InvalidBounds,
    InvalidOperations,
    InvalidActuals,
    Overflow,
    Capacity,
    NotFound,
    Conflict,
    InvalidLedger,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidId => "IDs must contain 1..128 ASCII letters, digits, dots, underscores, or hyphens",
            Self::InvalidQuantity => "quantities must be canonical signed decimal integers",
            Self::InvalidMetadata => "unit must be printable and nonempty (at most 64 bytes); scale must be 0..38",
            Self::InvalidBounds => "quantity must be within the fixed resource bounds",
            Self::InvalidOperations => "operations must be nonempty, nonzero, and unique by operation and resource ID",
            Self::InvalidActuals => "actuals must name every operation once and stay within its reserved direction and magnitude",
            Self::Overflow => "quantity, magnitude, offset, or reservation total exceeds signed 128-bit range",
            Self::Capacity => "insufficient unreserved quantity or upward room",
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Request {
    id: Id,
    operations: Vec<Operation>,
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
        Ok(Self { id, operations })
    }
    pub fn id(&self) -> &Id {
        &self.id
    }
    pub fn operations(&self) -> &[Operation] {
        &self.operations
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
    Capacity,
    Overflow,
    NotFound,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AbortReason {
    Requested,
    Rejected(Rejection),
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
    state: State,
}
impl Transaction {
    pub fn restore(request: Request, state: State) -> Result<Self, Error> {
        if let State::Committed(actuals) = &state {
            request.actuals(Some(
                actuals.0.iter().map(|(k, v)| (k.clone(), *v)).collect(),
            ))?;
        }
        Ok(Self { request, state })
    }
    pub fn request(&self) -> &Request {
        &self.request
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Resource {
    spec: ResourceSpec,
    quantity: i128,
    holds: BTreeMap<Id, i128>,
    availability: (i128, i128, i128, i128),
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
            availability: (0, 0, 0, 0),
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
    pub fn availability(&self) -> (i128, i128, i128, i128) {
        self.availability
    }
    /// Downward capacity, upward capacity, downward holds, upward holds.
    /// Even unbounded resources respect representable i128 offset/quantity limits.
    fn calculate_availability(&self) -> Result<(i128, i128, i128, i128), Error> {
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
            return Err(Error::Capacity);
        }
        Ok((offset - down, room - up, down, up))
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
        next.availability = next.calculate_availability()?;
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
        if let Some(existing) = self.resources.get(&spec.id) {
            return if existing.spec == spec {
                Ok(self.clone())
            } else {
                Err(Error::Conflict)
            };
        }
        let mut next = self.clone();
        next.resources.insert(
            spec.id.clone(),
            Resource::restore(spec.clone(), spec.initial, vec![])?,
        );
        Ok(next)
    }
    pub fn prepare(&self, request: Request) -> Result<Self, Error> {
        if let Some(tx) = self.transactions.get(&request.id) {
            return if tx.request == request {
                Ok(self.clone())
            } else {
                Err(Error::Conflict)
            };
        }
        let mut next = self.clone();
        let mut state = State::Prepared;
        // Missing current resources are a business rejection of a valid request,
        // so its identity must remain rejected even if the resource is later created.
        if request
            .operations
            .iter()
            .any(|o| !self.resources.contains_key(&o.resource))
        {
            state = State::Aborted(AbortReason::Rejected(Rejection::NotFound));
        }
        for op in &request.operations {
            if state != State::Prepared {
                break;
            }
            match self.resources[&op.resource].reserve(&request.id, op.delta) {
                Ok(resource) => {
                    next.resources.insert(op.resource.clone(), resource);
                }
                Err(e) => {
                    let reason = match e {
                        Error::Capacity => Rejection::Capacity,
                        Error::Overflow => Rejection::Overflow,
                        other => return Err(other),
                    };
                    next = self.clone();
                    state = State::Aborted(AbortReason::Rejected(reason));
                    break;
                }
            }
        }
        next.transactions
            .insert(request.id.clone(), Transaction { request, state });
        Ok(next)
    }
    pub fn commit(&self, id: &Id, supplied: Option<Vec<(Id, i128)>>) -> Result<Self, Error> {
        let tx = self.transactions.get(id).ok_or(Error::NotFound)?;
        let actuals = tx.request.actuals(supplied)?;
        match &tx.state {
            State::Committed(old) if *old == actuals => return Ok(self.clone()),
            State::Committed(_) | State::Aborted(_) => return Err(Error::Conflict),
            State::Prepared => {}
        }
        self.finish(id, State::Committed(actuals))
    }
    pub fn abort(&self, id: &Id) -> Result<Self, Error> {
        match self.transactions.get(id).ok_or(Error::NotFound)?.state {
            State::Aborted(_) => Ok(self.clone()),
            State::Committed(_) => Err(Error::Conflict),
            State::Prepared => self.finish(id, State::Aborted(AbortReason::Requested)),
        }
    }
    fn finish(&self, id: &Id, state: State) -> Result<Self, Error> {
        let mut next = self.clone();
        for op in &self.transactions[id].request.operations {
            let actual = match &state {
                State::Committed(a) => a.0[&op.id],
                _ => 0,
            };
            let resource = self.resources[&op.resource].settle(id, actual)?;
            next.resources.insert(op.resource.clone(), resource);
        }
        next.transactions
            .get_mut(id)
            .ok_or(Error::InvalidLedger)?
            .state = state;
        Ok(next)
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
