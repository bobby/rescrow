//! Independent small-integer specification. Materialized quantities and pending
//! extrema are derived from accepted transaction history, never cached or updated
//! using the production transition functions. DTOs are used only for observation.
use rescrow_application::dto;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Spec {
    pub lower: i64,
    pub upper: Option<i64>,
    pub initial: i64,
    pub unit: u8,
    pub scale: u8,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Op {
    pub id: u8,
    pub resource: u8,
    pub delta: i64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Command {
    Create(u8, Spec),
    Prepare(u16, Vec<Op>),
    Commit(u16, Option<Vec<(u8, i64)>>),
    Abort(u16),
    Resource(u8),
    Transaction(u16),
    Resources,
    Transactions,
}
impl Command {
    pub fn transaction_id(&self) -> Option<u16> {
        match self {
            Self::Prepare(id, _)
            | Self::Commit(id, _)
            | Self::Abort(id)
            | Self::Transaction(id) => Some(*id),
            _ => None,
        }
    }
    pub fn dto(&self) -> dto::Command {
        match self {
            Self::Create(id, spec) => dto::Command::CreateResource(spec.dto(*id)),
            Self::Prepare(id, ops) => dto::Command::Prepare(dto::Prepare {
                transaction_id: tx_id(*id),
                operations: ops
                    .iter()
                    .map(|o| dto::Operation {
                        operation_id: op_id(o.id),
                        resource_id: resource_id(o.resource),
                        delta: o.delta.to_string(),
                    })
                    .collect(),
            }),
            Self::Commit(id, actuals) => dto::Command::Commit(dto::Commit {
                transaction_id: tx_id(*id),
                actuals: actuals.as_ref().map(|a| {
                    a.iter()
                        .map(|(id, d)| dto::Actual {
                            operation_id: op_id(*id),
                            delta: d.to_string(),
                        })
                        .collect()
                }),
            }),
            Self::Abort(id) => dto::Command::Abort {
                transaction_id: tx_id(*id),
            },
            Self::Resource(id) => dto::Command::ShowResource {
                resource_id: resource_id(*id),
            },
            Self::Transaction(id) => dto::Command::ShowTransaction {
                transaction_id: tx_id(*id),
            },
            Self::Resources => dto::Command::ListResources,
            Self::Transactions => dto::Command::ListTransactions,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Failure {
    MalformedPrepare,
    InvalidActuals,
    Missing,
    Conflict,
    Capacity {
        resource: u8,
        delta: i64,
        available: i64,
    },
}
impl Failure {
    pub fn capacity(&self) -> Option<dto::CapacityFailure> {
        match self {
            Self::Capacity {
                resource,
                delta,
                available,
            } => Some(dto::CapacityFailure {
                resource_id: resource_id(*resource),
                requested_delta: delta.to_string(),
                available: available.to_string(),
            }),
            _ => None,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum State {
    Prepared,
    Committed(Vec<(u8, i64)>),
    Aborted(Option<Failure>),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Transaction {
    pub operations: Vec<Op>,
    pub state: State,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Model {
    pub resources: BTreeMap<u8, Spec>,
    pub transactions: BTreeMap<u16, Transaction>,
}
impl Model {
    pub fn quantity(&self, resource: u8) -> i64 {
        self.resources[&resource].initial
            + self
                .transactions
                .values()
                .map(|tx| match &tx.state {
                    State::Committed(actuals) => tx
                        .operations
                        .iter()
                        .filter(|op| op.resource == resource)
                        .map(|op| actuals.iter().find(|(id, _)| *id == op.id).unwrap().1)
                        .sum(),
                    _ => 0,
                })
                .sum::<i64>()
    }
    /// Minimum and maximum possible quantities over every subset of pending commits.
    fn extrema(&self, resource: u8) -> (i64, i64) {
        let q = self.quantity(resource);
        self.transactions
            .values()
            .filter(|tx| tx.state == State::Prepared)
            .flat_map(|tx| &tx.operations)
            .filter(|op| op.resource == resource)
            .fold((q, q), |(low, high), op| {
                (low + op.delta.min(0), high + op.delta.max(0))
            })
    }
    pub fn apply(&mut self, command: &Command) -> Result<(), Failure> {
        match command {
            Command::Create(id, spec) => match self.resources.get(id) {
                Some(old) if old != spec => Err(Failure::Conflict),
                Some(_) => Ok(()),
                None => {
                    self.resources.insert(*id, spec.clone());
                    Ok(())
                }
            },
            Command::Prepare(id, operations) => {
                if operations.is_empty()
                    || operations.iter().enumerate().any(|(i, op)| {
                        op.delta == 0
                            || operations[..i]
                                .iter()
                                .any(|p| p.id == op.id || p.resource == op.resource)
                    })
                {
                    return Err(Failure::MalformedPrepare);
                }
                let mut operations = operations.clone();
                operations.sort_by_key(|op| op.id);
                if let Some(old) = self.transactions.get(id) {
                    if old.operations != operations {
                        return Err(Failure::Conflict);
                    }
                    return match &old.state {
                        State::Aborted(Some(failure)) => Err(failure.clone()),
                        _ => Ok(()),
                    };
                }
                let failure = if operations
                    .iter()
                    .any(|o| !self.resources.contains_key(&o.resource))
                {
                    Some(Failure::Missing)
                } else {
                    operations.iter().find_map(|op| {
                        let spec = &self.resources[&op.resource];
                        let (low, high) = self.extrema(op.resource);
                        if low + op.delta.min(0) < spec.lower {
                            Some(Failure::Capacity {
                                resource: op.resource,
                                delta: op.delta,
                                available: low - spec.lower,
                            })
                        } else if spec
                            .upper
                            .is_some_and(|upper| high + op.delta.max(0) > upper)
                        {
                            Some(Failure::Capacity {
                                resource: op.resource,
                                delta: op.delta,
                                available: spec.upper.unwrap() - high,
                            })
                        } else {
                            None
                        }
                    })
                };
                self.transactions.insert(
                    *id,
                    Transaction {
                        operations,
                        state: match &failure {
                            Some(f) => State::Aborted(Some(f.clone())),
                            None => State::Prepared,
                        },
                    },
                );
                match failure {
                    Some(f) => Err(f),
                    None => Ok(()),
                }
            }
            Command::Commit(id, supplied) => {
                let tx = self.transactions.get(id).ok_or(Failure::Missing)?;
                let mut actuals = supplied
                    .clone()
                    .unwrap_or_else(|| tx.operations.iter().map(|o| (o.id, o.delta)).collect());
                if actuals.len() != tx.operations.len()
                    || tx.operations.iter().any(|op| {
                        let values: Vec<_> =
                            actuals.iter().filter(|(id, _)| *id == op.id).collect();
                        values.len() != 1 || {
                            let a = values[0].1;
                            a != 0 && (a.signum() != op.delta.signum() || a.abs() > op.delta.abs())
                        }
                    })
                {
                    return Err(Failure::InvalidActuals);
                }
                actuals.sort_by_key(|(id, _)| *id);
                match &tx.state {
                    State::Committed(old) if *old == actuals => Ok(()),
                    State::Committed(_) | State::Aborted(_) => Err(Failure::Conflict),
                    State::Prepared => {
                        self.transactions.get_mut(id).unwrap().state = State::Committed(actuals);
                        Ok(())
                    }
                }
            }
            Command::Abort(id) => match &self.transactions.get(id).ok_or(Failure::Missing)?.state {
                State::Committed(_) => Err(Failure::Conflict),
                State::Aborted(_) => Ok(()),
                State::Prepared => {
                    self.transactions.get_mut(id).unwrap().state = State::Aborted(None);
                    Ok(())
                }
            },
            Command::Resource(id) => self
                .resources
                .contains_key(id)
                .then_some(())
                .ok_or(Failure::Missing),
            Command::Transaction(id) => self
                .transactions
                .contains_key(id)
                .then_some(())
                .ok_or(Failure::Missing),
            Command::Resources | Command::Transactions => Ok(()),
        }
    }
    pub fn snapshot(&self, revision: u64) -> dto::Snapshot {
        let mut transactions: Vec<_> = self
            .transactions
            .iter()
            .map(|(id, tx)| tx.dto(*id))
            .collect();
        transactions.sort_by(|a, b| a.request.transaction_id.cmp(&b.request.transaction_id));
        dto::Snapshot {
            schema_version: 1,
            revision,
            resources: self
                .resources
                .iter()
                .map(|(id, spec)| {
                    let q = self.quantity(*id);
                    let (low, high) = self.extrema(*id);
                    let mut holds: Vec<_> =
                        self.transactions
                            .iter()
                            .filter(|(_, tx)| tx.state == State::Prepared)
                            .flat_map(|(tid, tx)| {
                                tx.operations.iter().filter(|op| op.resource == *id).map(
                                    move |op| dto::Hold {
                                        transaction_id: tx_id(*tid),
                                        delta: op.delta.to_string(),
                                    },
                                )
                            })
                            .collect();
                    holds.sort_by(|a, b| a.transaction_id.cmp(&b.transaction_id));
                    // Unbounded resources still have a finite numeric ceiling. All model
                    // quantities are small, so this expression cannot overflow i128.
                    let up = match spec.upper {
                        Some(upper) => i128::from(upper - high),
                        None => i128::MAX - i128::from(high - spec.lower.min(0)),
                    };
                    dto::Resource {
                        spec: spec.dto(*id),
                        quantity: q.to_string(),
                        holds,
                        reserved_down: (q - low).to_string(),
                        reserved_up: (high - q).to_string(),
                        available_down: (low - spec.lower).to_string(),
                        available_up: up.to_string(),
                    }
                })
                .collect(),
            transactions,
        }
    }
    pub fn output(&self, command: &Command, revision: u64) -> dto::Output {
        match command {
            Command::Create(id, _) | Command::Resource(id) => dto::Output::Resource(
                self.snapshot(revision)
                    .resources
                    .into_iter()
                    .find(|r| r.spec.resource_id == resource_id(*id))
                    .unwrap(),
            ),
            Command::Prepare(id, _)
            | Command::Commit(id, _)
            | Command::Abort(id)
            | Command::Transaction(id) => dto::Output::Transaction(self.transactions[id].dto(*id)),
            Command::Resources => dto::Output::Resources(self.snapshot(revision).resources),
            Command::Transactions => {
                dto::Output::Transactions(self.snapshot(revision).transactions)
            }
        }
    }
}
impl Spec {
    fn dto(&self, id: u8) -> dto::ResourceSpec {
        dto::ResourceSpec {
            resource_id: resource_id(id),
            lower: self.lower.to_string(),
            upper: self.upper.map(|u| u.to_string()),
            initial: self.initial.to_string(),
            unit: format!("unit{}", self.unit),
            scale: u32::from(self.scale),
        }
    }
}
impl Transaction {
    fn dto(&self, id: u16) -> dto::Transaction {
        let dto::Command::Prepare(request) = Command::Prepare(id, self.operations.clone()).dto()
        else {
            unreachable!()
        };
        let state = match &self.state {
            State::Prepared => dto::State::Prepared {},
            State::Committed(actuals) => dto::State::Committed {
                actuals: actuals
                    .iter()
                    .map(|(id, delta)| dto::Actual {
                        operation_id: op_id(*id),
                        delta: delta.to_string(),
                    })
                    .collect(),
            },
            State::Aborted(failure) => dto::State::Aborted {
                cause: match failure {
                    None => dto::AbortReason::Requested,
                    Some(Failure::Missing) => dto::AbortReason::Rejected(dto::Rejection::NotFound),
                    Some(Failure::Capacity { .. }) => {
                        dto::AbortReason::Rejected(dto::Rejection::Capacity)
                    }
                    other => panic!("invalid model rejection: {other:?}"),
                },
                capacity: failure.as_ref().and_then(Failure::capacity),
            },
        };
        dto::Transaction { request, state }
    }
}
pub fn tx_id(id: u16) -> String {
    format!("t{id}")
}
fn resource_id(id: u8) -> String {
    format!("r{id}")
}
fn op_id(id: u8) -> String {
    format!("o{id}")
}
