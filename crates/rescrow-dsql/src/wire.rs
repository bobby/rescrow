//! Storage transfer types; domain validation remains in the Application.
use rescrow_application::{StoreError, authority::*, canonical, dto};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn corrupt(e: impl std::fmt::Display) -> StoreError {
    StoreError::Corrupt(e.to_string())
}
pub(crate) fn encode<T: Serialize>(value: &T) -> Result<String, StoreError> {
    serde_json::to_string(value).map_err(corrupt)
}
pub(crate) fn decode<T: serde::de::DeserializeOwned>(value: &str) -> Result<T, StoreError> {
    canonical::decode(value.as_bytes()).map_err(corrupt)
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Key {
    pub kind: String,
    pub namespace: String,
    pub id: String,
}
impl From<&RecordKey> for Key {
    fn from(key: &RecordKey) -> Self {
        match key {
            RecordKey::Resource(id) => Self {
                kind: "resource".into(),
                namespace: String::new(),
                id: id.as_str().into(),
            },
            RecordKey::Reservation(id) => Self {
                kind: "reservation".into(),
                namespace: id.namespace().unwrap_or("").into(),
                id: id.as_str().into(),
            },
            RecordKey::Pending => Self {
                kind: "pending".into(),
                namespace: String::new(),
                id: String::new(),
            },
        }
    }
}
impl Key {
    pub fn record_key(&self) -> Result<RecordKey, StoreError> {
        let key = match self.kind.as_str() {
            "resource" if self.namespace.is_empty() => {
                RecordKey::resource(&self.id).map_err(corrupt)?
            }
            "reservation" => {
                let scope = if self.namespace.is_empty() {
                    Scope::local()
                } else {
                    Scope::new(TenantId::local(), &self.namespace).map_err(corrupt)?
                };
                scope.reservation(&self.id).map_err(corrupt)?
            }
            "pending" if self.namespace.is_empty() && self.id.is_empty() => RecordKey::Pending,
            _ => return Err(corrupt("invalid structured record key")),
        };
        if Self::from(&key) != *self {
            return Err(corrupt("noncanonical record key"));
        }
        Ok(key)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(crate) enum StoredValue {
    Resource(dto::Resource),
    Reservation(dto::Reservation),
    Pending(dto::Pending),
}
impl From<&Value> for StoredValue {
    fn from(v: &Value) -> Self {
        match v {
            Value::Resource(v) => Self::Resource(v.clone()),
            Value::Reservation(v) => Self::Reservation(v.clone()),
            Value::Pending(v) => Self::Pending(v.clone()),
        }
    }
}
impl StoredValue {
    pub fn validated(self, key: &RecordKey) -> Result<Value, StoreError> {
        let value = match self {
            Self::Resource(v) => Value::Resource(v),
            Self::Reservation(v) => Value::Reservation(v),
            Self::Pending(v) => Value::Pending(v),
        };
        value.validate(key)?;
        Ok(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    recorded_at_ms: u64,
    record: dto::Record,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Publication {
    tenant: TenantId,
    id: TransitionId,
    conditions: Vec<(Key, Option<String>)>,
    replacements: Vec<(Key, StoredValue)>,
    records: Vec<Entry>,
}
impl Publication {
    pub fn from_commit(commit: &Commit) -> Result<Self, StoreError> {
        if commit.fence().is_some() {
            return Err(corrupt("DSQL does not accept a coarse fence"));
        }
        let result = Self {
            tenant: commit.tenant().clone(),
            id: commit.history().transition_id.clone(),
            conditions: commit
                .conditions()
                .iter()
                .map(|(k, v)| {
                    let version = v
                        .as_ref()
                        .map(|v| String::from_utf8(v.0.clone()).map_err(corrupt))
                        .transpose()?;
                    Ok((Key::from(k), version))
                })
                .collect::<Result<_, StoreError>>()?,
            replacements: commit
                .replacements()
                .iter()
                .map(|(k, v)| (Key::from(k), StoredValue::from(v)))
                .collect(),
            records: commit
                .history()
                .records
                .iter()
                .map(|v| Entry {
                    recorded_at_ms: v.recorded_at_ms,
                    record: v.record.clone(),
                })
                .collect(),
        };
        result.validate(commit.tenant(), &commit.history().transition_id)?;
        Ok(result)
    }
    pub fn validate(&self, tenant: &TenantId, id: &TransitionId) -> Result<(), StoreError> {
        if &self.tenant != tenant
            || &self.id != id
            || self.records.is_empty()
            || self.replacements.is_empty()
        {
            return Err(corrupt("invalid publication identity or empty publication"));
        }
        if rescrow_application::uuid_v7_millis(id.as_str()) != Some(self.records[0].recorded_at_ms)
            || self
                .records
                .windows(2)
                .any(|v| v[0].recorded_at_ms > v[1].recorded_at_ms)
        {
            return Err(corrupt("invalid publication recording times"));
        }
        let mut conditions = BTreeSet::new();
        for (key, version) in &self.conditions {
            key.record_key()?;
            if !conditions.insert(key) {
                return Err(corrupt("duplicate condition"));
            }
            if let Some(version) = version {
                TransitionId::new(version)?;
            }
        }
        let mut replacements = BTreeSet::new();
        for (key, value) in &self.replacements {
            value.clone().validated(&key.record_key()?)?;
            if !conditions.contains(key) || !replacements.insert(key) {
                return Err(corrupt("duplicate or unconditioned replacement"));
            }
        }
        for entry in &self.records {
            match &entry.record {
                dto::Record::ResourceCreated(v) => {
                    rescrow_domain::ResourceSpec::try_from(v.clone()).map_err(corrupt)?;
                }
                dto::Record::Reservation(v) => {
                    rescrow_domain::Reservation::try_from(v.clone()).map_err(corrupt)?;
                }
            }
        }
        Ok(())
    }
    pub fn history(&self) -> HistoryRecord {
        HistoryRecord {
            transition_id: self.id.clone(),
            records: self
                .records
                .iter()
                .map(|e| TimedRecord {
                    recorded_at_ms: e.recorded_at_ms,
                    record: e.record.clone(),
                })
                .collect(),
        }
    }
    pub fn receipt(&self) -> Result<Receipt, StoreError> {
        Ok(Receipt {
            fence: None,
            versions: self
                .replacements
                .iter()
                .map(|(k, _)| {
                    Ok((
                        k.record_key()?,
                        Version(self.id.as_str().as_bytes().to_vec()),
                    ))
                })
                .collect::<Result<BTreeMap<_, _>, StoreError>>()?,
        })
    }
}
