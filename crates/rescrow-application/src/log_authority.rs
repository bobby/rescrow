//! The serialized local log implements the same authority port as record
//! backends. Its coarse fence is explicit; it never leaks into domain rules.
use crate::{
    LogReader, LogStore, Publication, StoreError,
    authority::*,
    capabilities::{Capabilities, Readiness},
    dto, uuid_v7,
};
use std::collections::BTreeMap;

fn version(position: u64) -> Version {
    Version(position.to_be_bytes().to_vec())
}
pub(crate) fn storage(error: crate::Error) -> StoreError {
    match error.code {
        crate::Code::MissingStore => StoreError::Missing,
        crate::Code::AlreadyInitialized => StoreError::Exists,
        crate::Code::Contention => StoreError::Contention,
        crate::Code::Io => StoreError::Io(error.message),
        crate::Code::UnknownOutcome => StoreError::Unknown(error.message),
        crate::Code::TooLarge => StoreError::TooLarge,
        crate::Code::UnsupportedSchema => StoreError::Unsupported(crate::SCHEMA_VERSION),
        crate::Code::RevisionExhausted => StoreError::RevisionExhausted,
        _ => StoreError::Corrupt(error.to_string()),
    }
}
fn local(tenant: &TenantId) -> bool {
    *tenant == TenantId::local()
}
fn empty() -> Read {
    Read {
        records: BTreeMap::new(),
        recorded_at_ms: 0,
        fence: None,
    }
}
fn key_supported(key: &RecordKey) -> bool {
    match key {
        RecordKey::Resource(id) | RecordKey::Transaction(id) => id.namespace().is_none(),
    }
}
fn receipt(history: &HistoryRecord, position: u64) -> Result<Receipt, StoreError> {
    let mut versions = BTreeMap::new();
    let mut put = |key| {
        versions.insert(key, version(position));
    };
    for entry in &history.records {
        match &entry.record {
            dto::Record::ResourceCreated(spec) => {
                put(RecordKey::resource(&spec.resource_id).map_err(storage)?)
            }
            dto::Record::Transaction(tx) => {
                let tx: rescrow_domain::Transaction = tx
                    .clone()
                    .try_into()
                    .map_err(|e: rescrow_domain::Error| StoreError::Corrupt(e.to_string()))?;
                put(RecordKey::Transaction(tx.request().id().clone()));
                if !matches!(
                    tx.state(),
                    rescrow_domain::State::Aborted(rescrow_domain::AbortReason::Rejected(_))
                ) {
                    for op in tx.request().operations() {
                        put(RecordKey::Resource(op.resource().clone()));
                    }
                }
            }
        }
    }
    Ok(Receipt {
        versions,
        fence: Some(version(position)),
    })
}
fn histories(entries: Vec<dto::Entry>) -> Result<Vec<(HistoryRecord, u64)>, StoreError> {
    let mut result: Vec<(HistoryRecord, u64)> = Vec::new();
    for entry in entries {
        let identity = TransitionId::new(
            entry
                .publication_id
                .unwrap_or_else(|| uuid_v7(entry.recorded_at_ms, u128::from(entry.sequence))),
        )?;
        let end = entry
            .sequence
            .checked_add(1)
            .ok_or(StoreError::RevisionExhausted)?;
        let record = TimedRecord {
            recorded_at_ms: entry.recorded_at_ms,
            record: entry.record,
        };
        match result.last_mut() {
            Some((history, position)) if history.transition_id == identity => {
                history.records.push(record);
                *position = end;
            }
            _ => {
                if crate::uuid_v7_millis(identity.as_str()) != Some(record.recorded_at_ms) {
                    return Err(StoreError::Corrupt(
                        "publication time differs from its identity".into(),
                    ));
                }
                result.push((
                    HistoryRecord {
                        transition_id: identity,
                        records: vec![record],
                    },
                    end,
                ));
            }
        }
    }
    Ok(result)
}
impl<S: LogStore> TransactionalStore for S {
    fn initialize(&self) -> Result<(), StoreError> {
        LogStore::initialize(
            self,
            &crate::checkpoint_dto(&rescrow_domain::Live::default(), 0, 0),
        )
    }
    fn read(
        &self,
        tenant: &TenantId,
        keys: &[RecordKey],
        terminal_since_ms: u64,
    ) -> Result<Read, StoreError> {
        if !local(tenant) {
            return Ok(Read {
                records: keys.iter().cloned().map(|key| (key, None)).collect(),
                ..empty()
            });
        }
        let reader = LogReader::new(self);
        let view = reader.view()?;
        let mut records = BTreeMap::new();
        for key in keys {
            let (value, recorded_at_ms) = match key {
                _ if !key_supported(key) => (None, view.recorded_at_ms),
                RecordKey::Resource(id) => (
                    view.live
                        .resources()
                        .get(id)
                        .map(|r| Value::Resource(r.into())),
                    view.recorded_at_ms,
                ),
                RecordKey::Transaction(id) => {
                    if let Some(tx) = view.live.prepared().get(id) {
                        (Some(Value::Transaction(tx.into())), view.recorded_at_ms)
                    } else if let Some((time, tx)) = view.terminal.get(id) {
                        (Some(Value::Transaction(tx.into())), *time)
                    } else {
                        match LogStore::find(self, id.as_str(), terminal_since_ms)?
                            .filter(|e| e.sequence < view.position)
                        {
                            Some(entry) => match entry.record {
                                dto::Record::Transaction(tx) => {
                                    (Some(Value::Transaction(tx)), entry.recorded_at_ms)
                                }
                                _ => {
                                    return Err(StoreError::Corrupt(
                                        "terminal lookup returned a resource".into(),
                                    ));
                                }
                            },
                            None => (None, view.recorded_at_ms),
                        }
                    }
                }
            };
            records.insert(
                key.clone(),
                value.map(|value| Versioned {
                    version: version(view.position),
                    recorded_at_ms,
                    value,
                }),
            );
        }
        Ok(Read {
            records,
            recorded_at_ms: view.recorded_at_ms,
            fence: Some(version(view.position)),
        })
    }
    fn scan(&self, tenant: &TenantId) -> Result<Inspection, StoreError> {
        if !local(tenant) {
            return Ok(Inspection {
                read: empty(),
                history: vec![],
            });
        }
        let reader = LogReader::new(self);
        let view = reader.view()?;
        let mut entries = LogStore::history(self)?;
        if (entries.len() as u64) < view.position {
            return Err(StoreError::Corrupt("log shorter than checkpoint".into()));
        }
        entries.truncate(usize::try_from(view.position).map_err(|_| StoreError::TooLarge)?);
        // The legacy reader verifies the checkpoint and all history. Its own
        // consistent cut may be newer; obtain that same cut from a second read
        // if a writer raced us, rather than combine two different snapshots.
        let snapshot = reader.snapshot_at(view.clone()).map_err(storage)?;
        let history = histories(entries)?;
        let mut records = BTreeMap::new();
        for r in snapshot.resources {
            let key = RecordKey::resource(&r.spec.resource_id)
                .map_err(|e| StoreError::Corrupt(e.to_string()))?;
            records.insert(
                key,
                Some(Versioned {
                    version: version(view.position),
                    recorded_at_ms: view.recorded_at_ms,
                    value: Value::Resource(r),
                }),
            );
        }
        for tx in snapshot.transactions {
            let domain: rescrow_domain::Transaction = tx
                .clone()
                .try_into()
                .map_err(|e: rescrow_domain::Error| StoreError::Corrupt(e.to_string()))?;
            let key = RecordKey::Transaction(domain.request().id().clone());
            let time = history
                .iter()
                .flat_map(|(h, _)| &h.records)
                .rev()
                .find_map(|r| match &r.record {
                    dto::Record::Transaction(t)
                        if t.request.transaction_id == tx.request.transaction_id =>
                    {
                        Some(r.recorded_at_ms)
                    }
                    _ => None,
                })
                .unwrap_or(view.recorded_at_ms);
            records.insert(
                key,
                Some(Versioned {
                    version: version(view.position),
                    recorded_at_ms: time,
                    value: Value::Transaction(tx),
                }),
            );
        }
        Ok(Inspection {
            read: Read {
                records,
                recorded_at_ms: view.recorded_at_ms,
                fence: Some(version(view.position)),
            },
            history: history.into_iter().map(|(h, _)| h).collect(),
        })
    }
    fn commit(&self, commit: &Commit) -> Result<CommitOutcome, StoreError> {
        if !local(commit.tenant()) || commit.conditions().keys().any(|k| !key_supported(k)) {
            return Err(StoreError::Corrupt(
                "local log authority supports only its implicit tenant and namespace".into(),
            ));
        }
        let expected = commit
            .fence()
            .and_then(|v| v.0.as_slice().try_into().ok())
            .map(u64::from_be_bytes)
            .ok_or_else(|| {
                StoreError::Corrupt("local publication is missing its coarse fence".into())
            })?;
        let end = expected
            .checked_add(commit.history().records.len() as u64)
            .ok_or(StoreError::RevisionExhausted)?;
        let entries: Vec<_> = commit
            .history()
            .records
            .iter()
            .enumerate()
            .map(|(i, r)| dto::Entry {
                publication_id: Some(commit.history().transition_id.as_str().into()),
                sequence: expected + i as u64,
                recorded_at_ms: r.recorded_at_ms,
                record: r.record.clone(),
            })
            .collect();
        match LogStore::append(self, &entries)? {
            Publication::Applied => Ok(CommitOutcome::Applied(receipt(commit.history(), end)?)),
            Publication::Conflict => Ok(CommitOutcome::Conflict),
        }
    }
    fn resolve(
        &self,
        tenant: &TenantId,
        identity: &TransitionId,
    ) -> Result<Resolution, StoreError> {
        if !local(tenant) {
            return Ok(Resolution::Unknown);
        }
        if let Some(entries) = LogStore::publication(self, identity.as_str())? {
            let mut publications = histories(entries)?;
            if publications.len() != 1 || &publications[0].0.transition_id != identity {
                return Err(StoreError::Corrupt(
                    "publication lookup returned the wrong identity".into(),
                ));
            }
            let (history, position) = publications.pop().unwrap();
            return Ok(Resolution::Applied {
                receipt: receipt(&history, position)?,
                history,
            });
        }
        // An unlocked log read cannot fence an in-flight publisher. Absence is
        // inconclusive; do not turn it into a known rollback.
        Ok(Resolution::Unknown)
    }
    fn history(&self, tenant: &TenantId) -> Result<Vec<HistoryRecord>, StoreError> {
        if !local(tenant) {
            return Ok(vec![]);
        }
        histories(LogStore::history(self)?).map(|h| h.into_iter().map(|(h, _)| h).collect())
    }
    fn maintain(&self, minimum_entries: usize) -> Result<(), StoreError> {
        let reader = LogReader::new(self);
        let view = reader.view()?;
        if view.tail >= minimum_entries {
            LogStore::checkpoint(
                self,
                &crate::checkpoint_dto(&view.live, view.position, view.recorded_at_ms),
            )?;
        }
        Ok(())
    }
}
impl<S: LogStore> Readiness for S {
    fn readiness(&self) -> Result<Capabilities, StoreError> {
        LogReader::new(self).view()?;
        Ok(self.capabilities())
    }
}
