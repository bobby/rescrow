//! Experimental structurally shared ledger, published through one Emriss atom.
//! Backends determine durability; memory objects/refs are only a prototype.
use ::emriss::{
    Map, SchemaId, StoreOptions, Value,
    cbor::{CborEncoder, CborRef, Kind},
    storage::ObjectStorage,
};
use emriss_atom::{AtomOptions, Atoms, Meta, RefName, RefStore, State};
use rescrow_application::{
    Publication, StoreError, ValidatedSnapshot,
    records::{Changes, Key, ReadView, Record, RecordStore},
};
use std::ops::Bound::Unbounded;

/// Adapter-owned codec wrapper; neither the Domain nor Application imports
/// Emriss. The CBOR record shape follows the existing typed Serde contract.
#[derive(Clone, Debug, PartialEq, Eq)]
struct StoredRecord(Record);
impl Value for StoredRecord {
    fn schema_id() -> SchemaId {
        *b"rescrow-record01"
    }
    fn encode(&self, out: &mut CborEncoder) -> ::emriss::Result<()> {
        encode(&serde_json::to_value(&self.0).map_err(format_error)?, out)
    }
    fn decode(item: CborRef<'_>) -> ::emriss::Result<Self> {
        serde_json::from_value(decode(item)?)
            .map(Self)
            .map_err(format_error)
    }
}
fn format_error(error: impl std::fmt::Display) -> ::emriss::Error {
    ::emriss::Error::InvalidFormat(error.to_string())
}
fn encode(value: &serde_json::Value, out: &mut CborEncoder) -> ::emriss::Result<()> {
    use serde_json::Value as V;
    match value {
        V::Null => {
            out.null();
        }
        V::Bool(v) => {
            out.bool(*v);
        }
        V::String(v) => {
            out.text(v);
        }
        V::Number(v) => {
            if let Some(n) = v.as_u64() {
                out.u64(n);
            } else if let Some(n) = v.as_i64() {
                out.i64(n);
            } else {
                return Err(format_error("floating-point record fields are unsupported"));
            }
        }
        V::Array(values) => {
            out.array(|a| {
                for v in values {
                    a.item(|out| encode(v, out))?;
                }
                Ok(())
            })?;
        }
        V::Object(fields) => {
            out.map(|m| {
                for (k, v) in fields {
                    m.field(k, |out| encode(v, out))?;
                }
                Ok(())
            })?;
        }
    }
    Ok(())
}
fn decode(item: CborRef<'_>) -> ::emriss::Result<serde_json::Value> {
    use serde_json::Value as V;
    Ok(match item.kind() {
        Kind::Null => V::Null,
        Kind::Bool => V::Bool(item.as_bool()?),
        Kind::Text => V::String(item.as_text()?.into()),
        Kind::Integer => {
            let n = item.as_i128()?;
            if n >= 0 {
                V::from(u64::try_from(n).map_err(format_error)?)
            } else {
                V::from(i64::try_from(n).map_err(format_error)?)
            }
        }
        Kind::Array => V::Array(
            item.array()?
                .map(|v| decode(v?))
                .collect::<::emriss::Result<_>>()?,
        ),
        Kind::Map => {
            let mut fields = serde_json::Map::new();
            for entry in item.map()? {
                let (k, v) = entry?;
                fields.insert(k.as_text()?.into(), decode(v)?);
            }
            V::Object(fields)
        }
        _ => return Err(format_error("unsupported record encoding")),
    })
}

type LedgerMap = Map<String, StoredRecord>;
const ROOT: &str = "root";
fn key(record: &Record) -> String {
    match record {
        Record::Root { .. } => ROOT.into(),
        Record::Resource(value) => format!("resource/{}", value.spec.resource_id),
        Record::Transaction(value) => format!("transaction/{}", value.request.transaction_id),
    }
}
fn lookup(key: &Key) -> String {
    match key {
        Key::Resource(id) => format!("resource/{id}"),
        Key::Transaction(id) => format!("transaction/{id}"),
    }
}
fn validate_key(physical: &str, record: &Record) -> Result<(), StoreError> {
    if physical == key(record) {
        Ok(())
    } else {
        Err(StoreError::Corrupt("record/key mismatch".into()))
    }
}
fn read_error(error: ::emriss::Error) -> StoreError {
    match error {
        ::emriss::Error::OutcomeUnknown => StoreError::Unknown(error.to_string()),
        ::emriss::Error::Storage(_) => StoreError::Io(error.to_string()),
        _ => StoreError::Corrupt(error.to_string()),
    }
}
fn edit_error(error: ::emriss::Error) -> StoreError {
    match error {
        ::emriss::Error::InvalidInput(_) => StoreError::TooLarge,
        _ => read_error(error),
    }
}
fn atom_error(error: emriss_atom::Error, publishing: bool) -> StoreError {
    use emriss_atom::Error as E;
    if publishing {
        // Errors may arise while completing the reflog *after* the ref CAS.
        // Preserve uncertainty even when that later error reports corruption.
        return match error {
            E::RefExists(_) => StoreError::Exists,
            E::Contended { .. } | E::Leased { .. } => StoreError::Contention,
            _ => StoreError::Unknown(error.to_string()),
        };
    }
    match error {
        E::RefNotFound(_) => StoreError::Missing,
        E::RefExists(_) => StoreError::Exists,
        E::Contended { .. } | E::Leased { .. } => StoreError::Contention,
        E::OutcomeUnknown | E::NotDurable(_) => StoreError::Unknown(error.to_string()),
        E::Corrupt(_) | E::StoreRolledBack { .. } | E::TypeMismatch { .. } => {
            StoreError::Corrupt(error.to_string())
        }
        E::Emriss(error) => read_error(error),
        _ => StoreError::Io(error.to_string()),
    }
}

/// Owns the authority's namespace. No method exposes a mutable atom handle or
/// accepts arbitrary roots. Initialize/import crosses full Application validation.
#[derive(Clone)]
pub struct EmrissStore {
    atoms: Atoms,
    name: RefName,
}
impl EmrissStore {
    /// `refs` must be dedicated/scoped to this authority: the atom name is
    /// `ledger`. Object namespaces do not automatically scope a RefStore.
    pub fn new<O: ObjectStorage + Clone, R: RefStore>(
        objects: O,
        refs: R,
        bucket: &str,
        namespace: &str,
    ) -> Result<Self, StoreError> {
        let atoms = Atoms::new(
            objects,
            bucket,
            namespace,
            StoreOptions::default(),
            refs,
            AtomOptions::default(),
        )
        .map_err(|e| atom_error(e, false))?;
        Ok(Self {
            atoms,
            name: RefName::new("ledger").map_err(|e| atom_error(e, false))?,
        })
    }
    /// Collection I/O counters for experiments; excludes atom commit/ref traffic.
    pub fn stats(&self) -> ::emriss::StorageStats {
        self.atoms.store().stats()
    }
}

/// Private construction binds a read view to an immutable value and commit.
pub struct EmrissView {
    state: State<LedgerMap>,
    revision: u64,
}
impl ReadView for EmrissView {
    fn revision(&self) -> u64 {
        self.revision
    }
    async fn get(&self, id: &Key) -> Result<Option<Record>, StoreError> {
        let key = lookup(id);
        let record = self.state.value.get(&key).await.map_err(read_error)?;
        if let Some(record) = &record {
            validate_key(&key, &record.0)?;
        }
        Ok(record.map(|r| r.0))
    }
    async fn scan(&self) -> Result<Vec<Record>, StoreError> {
        let mut cursor = self.state.value.range(Unbounded, Unbounded);
        let mut records = Vec::new();
        while let Some((k, r)) = cursor.next().await.map_err(read_error)? {
            validate_key(&k, &r.0)?;
            if k != ROOT {
                records.push(r.0);
            }
        }
        Ok(records)
    }
}
impl RecordStore for EmrissStore {
    type View = EmrissView;
    async fn initialize(&self, snapshot: &ValidatedSnapshot) -> Result<(), StoreError> {
        let snapshot = snapshot.to_dto();
        let mut map: LedgerMap = self.atoms.store().map();
        let records = std::iter::once(Record::Root {
            schema_version: 1,
            revision: snapshot.revision,
        })
        .chain(snapshot.resources.into_iter().map(Record::Resource))
        .chain(snapshot.transactions.into_iter().map(Record::Transaction));
        for record in records {
            map = map
                .assoc(key(&record), StoredRecord(record))
                .await
                .map_err(edit_error)?;
        }
        self.atoms
            .create(&self.name, map, Meta::default())
            .await
            .map_err(|e| atom_error(e, true))?;
        Ok(())
    }
    async fn load(&self) -> Result<Self::View, StoreError> {
        let atom = self
            .atoms
            .open::<LedgerMap>(&self.name)
            .await
            .map_err(|e| atom_error(e, false))?;
        let state = atom.deref().await.map_err(|e| atom_error(e, false))?;
        match state.value.get(&ROOT.into()).await.map_err(read_error)? {
            Some(StoredRecord(Record::Root {
                schema_version: 1,
                revision,
            })) => Ok(EmrissView { state, revision }),
            Some(StoredRecord(Record::Root { schema_version, .. })) => {
                Err(StoreError::Unsupported(schema_version))
            }
            _ => Err(StoreError::Corrupt("missing root metadata".into())),
        }
    }
    async fn publish(
        &self,
        expected: &Self::View,
        changes: &Changes,
    ) -> Result<Publication, StoreError> {
        if !expected.state.value.belongs_to(self.atoms.store()) {
            return Err(StoreError::Corrupt(
                "read view belongs to another store instance".into(),
            ));
        }
        if expected.revision.checked_add(1) != Some(changes.revision()) {
            return Err(StoreError::Corrupt(
                "candidate revision is not the successor".into(),
            ));
        }
        let mut map = expected.state.value.clone();
        for record in changes.records() {
            map = map
                .assoc(key(record), StoredRecord(record.clone()))
                .await
                .map_err(edit_error)?;
        }
        let root = Record::Root {
            schema_version: 1,
            revision: changes.revision(),
        };
        map = map
            .assoc(ROOT.into(), StoredRecord(root))
            .await
            .map_err(edit_error)?;
        let atom = self
            .atoms
            .open::<LedgerMap>(&self.name)
            .await
            .map_err(|e| atom_error(e, false))?;
        let applied = atom
            .compare_and_set(&expected.state.commit, map, Meta::default())
            .await
            .map_err(|e| atom_error(e, true))?;
        Ok(if applied {
            Publication::Applied
        } else {
            Publication::Conflict
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::emriss::storage::MemoryObjects;
    use emriss_atom::{AtomFault, MemoryRefs};
    use futures::executor::block_on;
    use rescrow_application::{Category, Code, dto, records::RecordApplication};

    fn resource() -> dto::Command {
        dto::Command::CreateResource(dto::ResourceSpec {
            resource_id: "r".into(),
            lower: "0".into(),
            upper: Some("10".into()),
            initial: "5".into(),
            unit: "credits".into(),
            scale: 0,
        })
    }
    fn prepare(id: &str, delta: &str) -> dto::Command {
        dto::Command::Prepare(dto::Prepare {
            transaction_id: id.into(),
            operations: vec![dto::Operation {
                operation_id: "o".into(),
                resource_id: "r".into(),
                delta: delta.into(),
            }],
        })
    }
    fn store(objects: &MemoryObjects, refs: &MemoryRefs) -> EmrissStore {
        EmrissStore::new(objects.clone(), refs.clone(), "memory", "escrow").unwrap()
    }
    #[test]
    fn flush_and_publication_failures_resolve_by_original_identity() {
        block_on(async {
            for fault in [
                AtomFault::BeforeCommit,
                AtomFault::AfterCommit,
                AtomFault::CompareAndSetLost,
                AtomFault::AfterCompareAndSet,
            ] {
                for decision in 0..4 {
                    let (objects, refs) = (MemoryObjects::default(), MemoryRefs::new());
                    let backend = store(&objects, &refs);
                    let app = RecordApplication::new(backend.clone());
                    app.initialize().await.unwrap();
                    app.execute(resource()).await.unwrap();
                    if decision == 1 || decision == 2 {
                        app.execute(prepare("t", "-3")).await.unwrap();
                    }
                    let command = match decision {
                        0 => prepare("t", "-3"),
                        1 => dto::Command::Commit(dto::Commit {
                            transaction_id: "t".into(),
                            actuals: Some(vec![dto::Actual {
                                operation_id: "o".into(),
                                delta: "-1".into(),
                            }]),
                        }),
                        2 => dto::Command::Abort {
                            transaction_id: "t".into(),
                        },
                        _ => prepare("t", "-8"),
                    };
                    backend.atoms.fail_next(fault);
                    let first = app.execute(command.clone()).await;
                    if let Err(error) = &first {
                        assert!(
                            error.category == Category::UnknownOutcome
                                || (decision == 3 && error.code == Code::Capacity),
                            "{fault:?}: {error:?}"
                        );
                        assert_eq!(error.transaction_id.as_deref(), Some("t"));
                    }
                    // Reopen all collection/atom state from the shared byte store.
                    let reopened = RecordApplication::new(store(&objects, &refs));
                    let outcome = reopened.execute(command.clone()).await;
                    if decision == 3 {
                        assert_eq!(outcome.unwrap_err().code, Code::Capacity);
                    } else {
                        outcome.unwrap();
                    }
                    let before = reopened.audit().await.unwrap().to_dto();
                    let _ = reopened.execute(command).await;
                    assert_eq!(reopened.audit().await.unwrap().to_dto(), before);
                    assert_eq!(
                        before.resources[0].quantity,
                        if decision == 1 { "4" } else { "5" }
                    );
                    assert_eq!(
                        before.resources[0].reserved_down,
                        if decision == 0 { "3" } else { "0" }
                    );
                    assert_eq!(before.transactions.len(), 1);
                }
            }
        });
    }
    #[test]
    fn codec_round_trip_preserves_full_width_numbers_and_rejects_unknown_fields() {
        let records = [
            Record::Root {
                schema_version: 1,
                revision: u64::MAX,
            },
            Record::Resource(dto::Resource {
                spec: dto::ResourceSpec {
                    resource_id: "r".into(),
                    lower: i128::MIN.to_string(),
                    upper: None,
                    initial: i128::MIN.to_string(),
                    unit: "credits".into(),
                    scale: 38,
                },
                quantity: i128::MIN.to_string(),
                holds: vec![],
                reserved_down: "0".into(),
                reserved_up: "0".into(),
                available_down: "0".into(),
                available_up: i128::MAX.to_string(),
            }),
        ];
        for record in records {
            let value = StoredRecord(record);
            let mut encoder = CborEncoder::new();
            value.encode(&mut encoder).unwrap();
            assert_eq!(
                StoredRecord::decode(CborRef::new(encoder.as_bytes()).unwrap()).unwrap(),
                value
            );
        }
        let mut encoder = CborEncoder::new();
        encode(&serde_json::json!({"record":"root", "value":{"schema_version":1,"revision":0,"extra":true}}), &mut encoder).unwrap();
        assert!(StoredRecord::decode(CborRef::new(encoder.as_bytes()).unwrap()).is_err());
    }
    #[test]
    fn full_audit_detects_balance_drift_that_a_lazy_point_read_cannot() {
        block_on(async {
            let (objects, refs) = (MemoryObjects::default(), MemoryRefs::new());
            let backend = store(&objects, &refs);
            let app = RecordApplication::new(backend.clone());
            app.initialize().await.unwrap();
            app.execute(resource()).await.unwrap();
            // Test-only bypass of the private authority. This record is locally
            // valid but cannot have resulted from controlled domain transitions.
            let atom = backend
                .atoms
                .open::<LedgerMap>(&backend.name)
                .await
                .unwrap();
            let map = atom.deref().await.unwrap().value;
            let StoredRecord(Record::Resource(mut value)) =
                map.get(&"resource/r".into()).await.unwrap().unwrap()
            else {
                panic!()
            };
            value.quantity = "4".into();
            value.available_down = "4".into();
            value.available_up = "6".into();
            let map = map
                .assoc("resource/r".into(), StoredRecord(Record::Resource(value)))
                .await
                .unwrap();
            atom.reset(map, Meta::default()).await.unwrap();
            let dto::Output::Resource(value) = app
                .execute(dto::Command::ShowResource {
                    resource_id: "r".into(),
                })
                .await
                .unwrap()
            else {
                panic!()
            };
            assert_eq!(value.quantity, "4");
            assert_eq!(app.audit().await.unwrap_err().code, Code::CorruptState);
        });
    }
    #[test]
    fn corrupt_records_keys_and_relationships_fail_at_the_boundary() {
        block_on(async {
            for corruption in 0..5 {
                let (objects, refs) = (MemoryObjects::default(), MemoryRefs::new());
                let backend = store(&objects, &refs);
                let app = RecordApplication::new(backend.clone());
                app.initialize().await.unwrap();
                app.execute(resource()).await.unwrap();
                app.execute(prepare("t", "-3")).await.unwrap();
                let atom = backend
                    .atoms
                    .open::<LedgerMap>(&backend.name)
                    .await
                    .unwrap();
                let mut map = atom.deref().await.unwrap().value;
                let StoredRecord(Record::Resource(mut r)) =
                    map.get(&"resource/r".into()).await.unwrap().unwrap()
                else {
                    panic!()
                };
                match corruption {
                    0 => {
                        r.quantity = "01".into();
                    }
                    1 => {
                        r.spec.resource_id = "different".into();
                    }
                    2 => {
                        map = map.dissoc(&"transaction/t".into()).await.unwrap();
                    }
                    3 => {
                        map = map
                            .assoc(
                                ROOT.into(),
                                StoredRecord(Record::Root {
                                    schema_version: 99,
                                    revision: 2,
                                }),
                            )
                            .await
                            .unwrap();
                    }
                    _ => {
                        let StoredRecord(Record::Transaction(mut tx)) =
                            map.get(&"transaction/t".into()).await.unwrap().unwrap()
                        else {
                            panic!()
                        };
                        tx.state = dto::State::Aborted {
                            cause: dto::AbortReason::Requested,
                            capacity: None,
                        };
                        map = map
                            .assoc(
                                "transaction/t".into(),
                                StoredRecord(Record::Transaction(tx)),
                            )
                            .await
                            .unwrap();
                    }
                }
                map = map
                    .assoc("resource/r".into(), StoredRecord(Record::Resource(r)))
                    .await
                    .unwrap();
                atom.reset(map, Meta::default()).await.unwrap();
                let expected = if corruption == 3 {
                    Code::UnsupportedSchema
                } else {
                    Code::CorruptState
                };
                assert_eq!(
                    app.execute(dto::Command::ShowResource {
                        resource_id: "r".into()
                    })
                    .await
                    .unwrap_err()
                    .code,
                    expected
                );
                assert_eq!(app.audit().await.unwrap_err().code, expected);
            }
        });
    }
}
