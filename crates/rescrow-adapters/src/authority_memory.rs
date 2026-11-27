//! Record-scoped in-memory authority. The mutex is the atomic publication
//! primitive, not a global optimistic version: disjoint read sets never race.
use rescrow_application::{
    StoreError,
    authority::{
        Commit, CommitOutcome, HistoryRecord, Inspection, Read, ReadSet, Receipt, RecordKey,
        Resolution, TenantId, TransactionalStore, TransitionId, Version, Versioned,
    },
    capabilities::{Capabilities, Readiness},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct Tenant {
    records: ReadSet,
    publications: BTreeMap<TransitionId, Commit>,
    order: Vec<TransitionId>,
    fenced: BTreeSet<TransitionId>,
}
#[derive(Default)]
struct State {
    generation: Option<String>,
    tenants: BTreeMap<TenantId, Tenant>,
}
#[derive(Clone, Default)]
pub struct MemoryAuthority(Arc<Mutex<State>>);
impl TransactionalStore for MemoryAuthority {
    fn identity(&self) -> Result<String, StoreError> {
        self.0
            .lock()
            .unwrap()
            .generation
            .clone()
            .ok_or(StoreError::Missing)
    }
    fn initialize(&self) -> Result<(), StoreError> {
        let mut state = self.0.lock().unwrap();
        if state.generation.is_some() {
            return Err(StoreError::Exists);
        }
        state.generation =
            Some(rescrow_application::new_id().map_err(|e| StoreError::Io(e.message))?);
        Ok(())
    }
    fn read(
        &self,
        tenant: &TenantId,
        keys: &[RecordKey],
        terminal_since_ms: u64,
    ) -> Result<Read, StoreError> {
        let state = self.0.lock().unwrap();
        if state.generation.is_none() {
            return Err(StoreError::Missing);
        }
        let records = state.tenants.get(tenant).map(|t| &t.records);
        let records: ReadSet = keys
            .iter()
            .cloned()
            .map(|key| {
                let record = records.and_then(|rs| rs.get(&key)).cloned().flatten();
                let record = record.filter(|v| match &v.value {
                    rescrow_application::authority::Value::Reservation(tx) => {
                        matches!(tx.state, rescrow_application::dto::State::Prepared {})
                            || v.recorded_at_ms >= terminal_since_ms
                    }
                    _ => true,
                });
                (key, record)
            })
            .collect();
        let recorded_at_ms = records
            .values()
            .flatten()
            .map(|v| v.recorded_at_ms)
            .max()
            .unwrap_or(0);
        Ok(Read {
            records,
            recorded_at_ms,
            fence: None,
        })
    }
    fn scan(&self, tenant: &TenantId) -> Result<Inspection, StoreError> {
        let state = self.0.lock().unwrap();
        if state.generation.is_none() {
            return Err(StoreError::Missing);
        }
        let tenant = state.tenants.get(tenant);
        let records = tenant.map(|t| t.records.clone()).unwrap_or_default();
        let recorded_at_ms = records
            .values()
            .flatten()
            .map(|v| v.recorded_at_ms)
            .max()
            .unwrap_or(0);
        let history = tenant
            .map(|t| {
                t.order
                    .iter()
                    .map(|id| t.publications[id].history().clone())
                    .collect()
            })
            .unwrap_or_default();
        Ok(Inspection {
            read: Read {
                records,
                recorded_at_ms,
                fence: None,
            },
            history,
        })
    }

    fn commit(&self, commit: &Commit) -> Result<CommitOutcome, StoreError> {
        let mut state = self.0.lock().unwrap();
        if state.generation.is_none() {
            return Err(StoreError::Missing);
        }
        if commit.history().records.is_empty() || commit.replacements().is_empty() {
            return Err(StoreError::Corrupt("empty publication".into()));
        }
        let tenant = state.tenants.entry(commit.tenant().clone()).or_default();
        if let Some(earlier) = tenant.publications.get(&commit.history().transition_id) {
            // Publication identities are immutable, including their conditions
            // and replacements. Store the complete request below, not just a
            // business ID that could refer to a later decision.
            return if earlier == commit {
                Ok(CommitOutcome::Applied(receipt(commit)))
            } else {
                Err(StoreError::Corrupt(
                    "transition identity reused with different history".into(),
                ))
            };
        }
        if tenant.fenced.contains(&commit.history().transition_id) {
            return Ok(CommitOutcome::Conflict);
        }
        for (key, expected) in commit.conditions() {
            let actual = tenant
                .records
                .get(key)
                .and_then(|v| v.as_ref())
                .map(|v| &v.version);
            if actual != expected.as_ref() {
                return Ok(CommitOutcome::Conflict);
            }
        }
        let time = commit.history().records.last().unwrap().recorded_at_ms;
        if commit
            .history()
            .records
            .windows(2)
            .any(|w| w[0].recorded_at_ms > w[1].recorded_at_ms)
        {
            return Err(StoreError::Corrupt("batch recording times decrease".into()));
        }
        let earliest = commit.history().records[0].recorded_at_ms;
        for (key, value) in commit.replacements() {
            if !commit.conditions().contains_key(key) {
                return Err(StoreError::Corrupt(
                    "replacement has no version condition".into(),
                ));
            }
            value.validate(key)?;
        }
        if commit.conditions().keys().any(|key| {
            tenant
                .records
                .get(key)
                .and_then(|v| v.as_ref())
                .is_some_and(|v| v.recorded_at_ms > earliest)
        }) {
            return Err(StoreError::Corrupt(
                "recording time precedes a read dependency".into(),
            ));
        }
        let tenant = state.tenants.get_mut(commit.tenant()).unwrap();
        for (key, value) in commit.replacements() {
            tenant.records.insert(
                key.clone(),
                Some(Versioned {
                    version: Version(commit.history().transition_id.as_str().as_bytes().to_vec()),
                    recorded_at_ms: time,
                    value: value.clone(),
                }),
            );
        }
        tenant.order.push(commit.history().transition_id.clone());
        tenant
            .publications
            .insert(commit.history().transition_id.clone(), commit.clone());
        Ok(CommitOutcome::Applied(receipt(commit)))
    }
    fn resolve(
        &self,
        tenant: &TenantId,
        transition: &TransitionId,
    ) -> Result<Resolution, StoreError> {
        let mut state = self.0.lock().unwrap();
        if state.generation.is_none() {
            return Err(StoreError::Missing);
        }
        let tenant = state.tenants.entry(tenant.clone()).or_default();
        if let Some(commit) = tenant.publications.get(transition) {
            Ok(Resolution::Applied {
                history: commit.history().clone(),
                receipt: receipt(commit),
            })
        } else {
            tenant.fenced.insert(transition.clone());
            Ok(Resolution::NotApplied)
        }
    }

    fn history(&self, tenant: &TenantId) -> Result<Vec<HistoryRecord>, StoreError> {
        let state = self.0.lock().unwrap();
        if state.generation.is_none() {
            return Err(StoreError::Missing);
        }
        Ok(state
            .tenants
            .get(tenant)
            .map(|t| {
                t.order
                    .iter()
                    .map(|id| t.publications[id].history().clone())
                    .collect()
            })
            .unwrap_or_default())
    }
}
impl Readiness for MemoryAuthority {
    fn readiness(&self) -> Result<Capabilities, StoreError> {
        if self.0.lock().unwrap().generation.is_none() {
            return Err(StoreError::Missing);
        }
        Ok(Capabilities {
            multi_tenant: true,
            ..Capabilities::simulation()
        })
    }
}

fn receipt(commit: &Commit) -> Receipt {
    let version = Version(commit.history().transition_id.as_str().as_bytes().to_vec());
    Receipt {
        fence: None,
        versions: commit
            .replacements()
            .keys()
            .cloned()
            .map(|k| (k, version.clone()))
            .collect(),
    }
}
