//! Transactional authority contract. Keys are structured, reads carry opaque
//! versions, and a publication conditions exactly the records it observed.
//!
//! A backend must atomically publish every replacement and the immutable history
//! record, or none of them. A batch is one such publication, including lapses.
//! `Unknown` is resolved by transition identity, never by submitting a second
//! publication. A missing history record proves `NotApplied` only after the
//! backend has fenced the original operation so it cannot arrive later.
use crate::{StoreError, dto};
use rescrow_domain as d;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A tenant is an Application identity, not an escrow quantity or policy.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct TenantId(d::Id);
impl TenantId {
    pub fn new(value: impl Into<String>) -> Result<Self, crate::Error> {
        d::Id::new(value.into())
            .map(Self)
            .map_err(|e| crate::Error::input(e.to_string()))
    }
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
    pub fn local() -> Self {
        Self(d::Id::new("local".into()).expect("constant valid ID"))
    }
}
impl TryFrom<String> for TenantId {
    type Error = crate::Error;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl From<TenantId> for String {
    fn from(value: TenantId) -> Self {
        value.0.as_str().into()
    }
}

/// Set by the composition root from authenticated/authorized context. It is
/// never taken from a resource ID, request body, or caller-controlled path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Scope {
    tenant: TenantId,
    namespace: Option<d::Id>,
}
impl Scope {
    pub fn new(tenant: TenantId, namespace: &str) -> Result<Self, crate::Error> {
        Ok(Self {
            tenant,
            namespace: Some(
                d::Id::new(namespace.into()).map_err(|e| crate::Error::input(e.to_string()))?,
            ),
        })
    }
    pub fn local() -> Self {
        Self {
            tenant: TenantId::local(),
            namespace: None,
        }
    }
    pub fn tenant(&self) -> &TenantId {
        &self.tenant
    }
    pub fn namespace(&self) -> Option<&str> {
        self.namespace.as_ref().map(d::Id::as_str)
    }
    pub fn reservation(&self, id: &str) -> Result<RecordKey, crate::Error> {
        let id = d::Id::new(id.into()).map_err(|e| crate::Error::input(e.to_string()))?;
        Ok(RecordKey::Reservation(match &self.namespace {
            None => id,
            Some(namespace) => d::Id::qualified(namespace.clone(), id)
                .map_err(|e| crate::Error::input(e.to_string()))?,
        }))
    }
}

/// Always passed together with a validated `TenantId`. Backends encode tuples
/// (or bind separate SQL columns); joining raw strings is not a key codec.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RecordKey {
    Resource(d::Id),
    /// A domain identity carries the authorized namespace and opaque ID as
    /// separate validated values. The same ID in two namespaces is distinct.
    Reservation(d::Id),
    Pending,
}
impl RecordKey {
    pub fn resource(id: &str) -> Result<Self, crate::Error> {
        d::Id::new(id.into())
            .map(Self::Resource)
            .map_err(|e| crate::Error::input(e.to_string()))
    }
}

/// Backend-owned concurrency token. The Application only tests equality.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Version(pub Vec<u8>);
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    Resource(dto::Resource),
    Reservation(dto::Reservation),
    Pending(dto::Pending),
}
impl Value {
    /// Validate every decoded record before exposing it to domain operations,
    /// including its agreement with the requested key.
    pub fn validate(&self, key: &RecordKey) -> Result<(), StoreError> {
        let valid = match (key, self) {
            (RecordKey::Resource(id), Self::Resource(value)) => {
                let r = d::Resource::try_from(value.clone()).map_err(invalid)?;
                r.spec().id() == id && id.namespace().is_none()
            }
            (RecordKey::Reservation(id), Self::Reservation(value)) => {
                let tx = d::Reservation::try_from(value.clone()).map_err(invalid)?;
                tx.request().id() == id
            }
            (RecordKey::Pending, Self::Pending(value)) => {
                d::access::Pending::try_from(value.clone()).map_err(invalid)?;
                true
            }
            _ => false,
        };
        if valid {
            Ok(())
        } else {
            Err(invalid("record key disagrees with its value"))
        }
    }
}
fn invalid(error: impl std::fmt::Display) -> StoreError {
    StoreError::Corrupt(error.to_string())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Versioned {
    pub version: Version,
    /// Nondecreasing along this record's versions. Unrelated resources need
    /// no global timestamp counter. The Application takes the maximum over its
    /// read set and clock before evaluating expiry; versions fence that time.
    pub recorded_at_ms: u64,
    pub value: Value,
}
/// Every requested key must appear, including absent keys. All values in a read
/// are from one consistent cut. Subsequent reads may observe a newer cut.
pub type ReadSet = BTreeMap<RecordKey, Option<Versioned>>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Read {
    pub records: ReadSet,
    /// An adapter may require a time floor (the file log does). A record
    /// authority uses only the maximum time of these records, not a contended
    /// global timestamp. A new transition must not precede this floor.
    pub recorded_at_ms: u64,
    /// Optional coarse fence for a serialized local authority. Record backends
    /// leave this absent and condition only individual records.
    pub fence: Option<Version>,
}
/// One consistent inspection cut. History and records must describe the same
/// cut; independently fetching them can race a writer and falsely look corrupt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Inspection {
    pub read: Read,
    pub history: Vec<HistoryRecord>,
}

/// Unique per publication, generated once outside retry loops. Never reused
/// for changed conditions or changed transitions, even after a conflict.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct TransitionId(String);
impl TransitionId {
    pub fn new(value: impl Into<String>) -> Result<Self, StoreError> {
        let value = value.into();
        if crate::uuid_v7_millis(&value).is_none() {
            return Err(invalid("transition identity must be a canonical UUIDv7"));
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for TransitionId {
    type Error = StoreError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl From<TransitionId> for String {
    fn from(value: TransitionId) -> Self {
        value.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimedRecord {
    pub recorded_at_ms: u64,
    pub record: dto::Record,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryRecord {
    pub transition_id: TransitionId,
    pub records: Vec<TimedRecord>,
}
/// Constructed by the Application from validated domain transitions. Adapters
/// do not reevaluate escrow rules. They check conditions and publish atomically.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Commit {
    tenant: TenantId,
    history: HistoryRecord,
    conditions: BTreeMap<RecordKey, Option<Version>>,
    replacements: BTreeMap<RecordKey, Value>,
    fence: Option<Version>,
}
impl Commit {
    /// Map domain changes to one conditional publication. The caller evaluates
    /// all changes in order against `observed`, and includes every record read
    /// in that evaluation, including absent identities. No adapter-specific
    /// version or global log position enters the Domain.
    pub fn from_changes(
        tenant: TenantId,
        transition_id: TransitionId,
        observed: &Read,
        changes: &[(u64, d::transition::Changes)],
    ) -> Result<Self, StoreError> {
        let mut records = Vec::new();
        let mut replacements = BTreeMap::new();
        let mut time = observed
            .records
            .values()
            .flatten()
            .map(|v| v.recorded_at_ms)
            .max()
            .unwrap_or(0)
            .max(observed.recorded_at_ms);
        for (key, versioned) in &observed.records {
            if let Some(versioned) = versioned {
                versioned.value.validate(key)?;
            }
        }
        for (recorded_at_ms, changes) in changes {
            if changes.is_empty() {
                continue;
            }
            if *recorded_at_ms < time {
                return Err(invalid("recording time precedes a dependency"));
            }
            time = *recorded_at_ms;
            for r in changes.resources() {
                replacements.insert(
                    RecordKey::Resource(r.spec().id().clone()),
                    Value::Resource(r.into()),
                );
            }
            let record = match changes.reservation() {
                Some(tx) => {
                    replacements.insert(
                        RecordKey::Reservation(tx.request().id().clone()),
                        Value::Reservation(tx.into()),
                    );
                    dto::Record::Reservation(tx.into())
                }
                None => dto::Record::ResourceCreated(changes.resources()[0].spec().into()),
            };
            records.push(TimedRecord {
                recorded_at_ms: time,
                record,
            });
        }
        // Quota is derived from these same reservation transitions, never a
        // second publication or a process-local admission counter.
        let mut pending = None;
        let mut reservations: BTreeMap<d::Id, d::Reservation> = BTreeMap::new();
        for (_, change) in changes {
            if let Some(tx) = change.reservation() {
                if tx.request().authorization().is_some() {
                    if pending.is_none() {
                        pending = Some(match observed.records.get(&RecordKey::Pending) {
                            Some(Some(Versioned {
                                value: Value::Pending(v),
                                ..
                            })) => d::access::Pending::try_from(v.clone()).map_err(invalid)?,
                            Some(None) => d::access::Pending::default(),
                            _ => {
                                return Err(invalid(
                                    "authenticated transition has no pending-counter read",
                                ));
                            }
                        });
                    }
                    let previous = match reservations.get(tx.request().id()) {
                        Some(previous) => Some(previous.clone()),
                        None => match observed
                            .records
                            .get(&RecordKey::Reservation(tx.request().id().clone()))
                            .and_then(|v| v.as_ref())
                        {
                            Some(Versioned {
                                value: Value::Reservation(v),
                                ..
                            }) => Some(d::Reservation::try_from(v.clone()).map_err(invalid)?),
                            _ => None,
                        },
                    };
                    pending = Some(
                        pending
                            .as_ref()
                            .unwrap()
                            .apply(previous.as_ref(), tx)
                            .map_err(invalid)?,
                    );
                }
                reservations.insert(tx.request().id().clone(), tx.clone());
            }
        }
        if let Some(pending) = pending {
            replacements.insert(RecordKey::Pending, Value::Pending((&pending).into()));
        }
        if records.is_empty() {
            return Err(invalid("empty publication"));
        }
        if crate::uuid_v7_millis(transition_id.as_str()) != Some(records[0].recorded_at_ms) {
            return Err(invalid(
                "publication UUID timestamp differs from its first recording time",
            ));
        }
        if replacements
            .keys()
            .any(|key| !observed.records.contains_key(key))
        {
            return Err(invalid("replacement has no read condition"));
        }
        Ok(Self {
            tenant,
            history: HistoryRecord {
                transition_id,
                records,
            },
            conditions: observed
                .records
                .iter()
                .map(|(k, v)| (k.clone(), v.as_ref().map(|v| v.version.clone())))
                .collect(),
            replacements,
            fence: observed.fence.clone(),
        })
    }
    pub fn fence(&self) -> Option<&Version> {
        self.fence.as_ref()
    }
    pub fn tenant(&self) -> &TenantId {
        &self.tenant
    }
    pub fn history(&self) -> &HistoryRecord {
        &self.history
    }
    pub fn conditions(&self) -> &BTreeMap<RecordKey, Option<Version>> {
        &self.conditions
    }
    pub fn replacements(&self) -> &BTreeMap<RecordKey, Value> {
        &self.replacements
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Receipt {
    pub versions: BTreeMap<RecordKey, Version>,
    pub fence: Option<Version>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommitOutcome {
    Applied(Receipt),
    Conflict,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Resolution {
    /// The original exact publication is durable. Callers must compare it with
    /// their expected history before using their already-computed response.
    Applied {
        history: HistoryRecord,
        receipt: Receipt,
    },
    /// The original publication is fenced and cannot subsequently take effect.
    NotApplied,
    /// Still in flight, unreachable, or otherwise unresolved.
    Unknown,
}

/// One port for server and local authorities. All reads and history lookups are
/// tenant scoped, including absent tenants. No unscoped existence lookup exists.
///
/// `scan` is for inspection and single-writer service caches, never a condition
/// on all tenants. Histories are append-only; `history` returns a complete
/// consistent cut for this tenant (ordering of unrelated records is adapter
/// specific, but causal predecessors must come first). `read` and `resolve`
/// acknowledge durability of observed writes. Terminal lookups may omit records
/// older than `terminal_since_ms`; prepared reservations are never omitted.
pub trait TransactionalStore {
    /// Stable identity of this actual authority, shared by every frontend and
    /// retained across reopen. Distinct empty authorities must not share it.
    fn identity(&self) -> Result<String, StoreError>;
    fn initialize(&self) -> Result<(), StoreError>;
    fn read(
        &self,
        tenant: &TenantId,
        keys: &[RecordKey],
        terminal_since_ms: u64,
    ) -> Result<Read, StoreError>;
    fn scan(&self, tenant: &TenantId) -> Result<Inspection, StoreError>;
    fn commit(&self, commit: &Commit) -> Result<CommitOutcome, StoreError>;
    fn resolve(
        &self,
        tenant: &TenantId,
        transition: &TransitionId,
    ) -> Result<Resolution, StoreError>;
    fn history(&self, tenant: &TenantId) -> Result<Vec<HistoryRecord>, StoreError>;
    /// Advisory derived-state maintenance after a publication. It cannot
    /// change authoritative records or turn an acknowledged write into failure.
    fn maintain(&self, _minimum_entries: usize) -> Result<(), StoreError> {
        Ok(())
    }
}
