//! Pure transitions over a transaction's participants, independent of ledger size.
use super::*;

/// Replacements that must become visible together. Only domain transitions can
/// construct these; an empty change is a read-only replay.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Changes {
    resources: Vec<Resource>,
    transaction: Option<Transaction>,
}
impl Changes {
    pub fn resources(&self) -> &[Resource] {
        &self.resources
    }
    pub fn transaction(&self) -> Option<&Transaction> {
        self.transaction.as_ref()
    }
    pub fn is_empty(&self) -> bool {
        self.resources.is_empty() && self.transaction.is_none()
    }
}

pub fn create(spec: ResourceSpec, existing: Option<&Resource>) -> Result<Changes, Error> {
    if let Some(resource) = existing {
        return if resource.spec == spec {
            Ok(Changes::default())
        } else {
            Err(Error::Conflict)
        };
    }
    Ok(Changes {
        resources: vec![Resource::restore(spec.clone(), spec.initial, vec![])?],
        transaction: None,
    })
}

/// Check the loaded transaction's relationships to its declared participants.
/// This does not reconstruct materialized balances from all historical commits.
pub fn validate_participants(
    tx: &Transaction,
    resources: &BTreeMap<Id, Resource>,
) -> Result<(), Error> {
    let missing_allowed = tx.state == State::Aborted(AbortReason::Rejected(Rejection::NotFound));
    for op in &tx.request.operations {
        match resources.get(&op.resource) {
            None if missing_allowed => (),
            None => return Err(Error::InvalidLedger),
            Some(resource) => {
                let expected = (tx.state == State::Prepared).then_some(&op.delta);
                if resource.spec.id != op.resource || resource.holds.get(&tx.request.id) != expected
                {
                    return Err(Error::InvalidLedger);
                }
            }
        }
    }
    Ok(())
}

pub fn prepare(
    request: Request,
    existing: Option<&Transaction>,
    resources: &BTreeMap<Id, Resource>,
) -> Result<Changes, Error> {
    if let Some(tx) = existing {
        if tx.request != request {
            return Err(Error::Conflict);
        }
        validate_participants(tx, resources)?;
        return Ok(Changes::default());
    }
    for op in &request.operations {
        if let Some(resource) = resources.get(&op.resource)
            && (resource.spec.id != op.resource || resource.holds.contains_key(&request.id))
        {
            return Err(Error::InvalidLedger);
        }
    }
    let mut replacements = Vec::new();
    let mut state = State::Prepared;
    if request
        .operations
        .iter()
        .any(|op| !resources.contains_key(&op.resource))
    {
        state = State::Aborted(AbortReason::Rejected(Rejection::NotFound));
    } else {
        for op in &request.operations {
            match resources[&op.resource].reserve(&request.id, op.delta) {
                Ok(resource) => replacements.push(resource),
                Err(error) => {
                    let reason = match error {
                        Error::Capacity(failure) => Rejection::Capacity(Some(failure)),
                        Error::Overflow => Rejection::Overflow,
                        other => return Err(other),
                    };
                    state = State::Aborted(AbortReason::Rejected(reason));
                    replacements.clear();
                    break;
                }
            }
        }
    }
    Ok(Changes {
        resources: replacements,
        transaction: Some(Transaction { request, state }),
    })
}

pub fn commit(
    tx: &Transaction,
    supplied: Option<Vec<(Id, i128)>>,
    resources: &BTreeMap<Id, Resource>,
) -> Result<Changes, Error> {
    let actuals = tx.request.actuals(supplied)?;
    validate_participants(tx, resources)?;
    match &tx.state {
        State::Committed(old) if *old == actuals => Ok(Changes::default()),
        State::Committed(_) | State::Aborted(_) => Err(Error::Conflict),
        State::Prepared => finish(tx, State::Committed(actuals), resources),
    }
}

pub fn abort(tx: &Transaction, resources: &BTreeMap<Id, Resource>) -> Result<Changes, Error> {
    validate_participants(tx, resources)?;
    match tx.state {
        State::Committed(_) => Err(Error::Conflict),
        State::Aborted(_) => Ok(Changes::default()),
        State::Prepared => finish(tx, State::Aborted(AbortReason::Requested), resources),
    }
}

fn finish(
    tx: &Transaction,
    state: State,
    resources: &BTreeMap<Id, Resource>,
) -> Result<Changes, Error> {
    let replacements = tx
        .request
        .operations
        .iter()
        .map(|op| {
            let actual = match &state {
                State::Committed(a) => a.0[&op.id],
                _ => 0,
            };
            resources
                .get(&op.resource)
                .ok_or(Error::InvalidLedger)?
                .settle(&tx.request.id, actual)
        })
        .collect::<Result<_, _>>()?;
    Ok(Changes {
        resources: replacements,
        transaction: Some(Transaction {
            request: tx.request.clone(),
            state,
        }),
    })
}
