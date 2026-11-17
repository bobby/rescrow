//! Deterministic snapshot port for embedding tests, with single-shot effect faults.
use rescrow_application::{Publication, SnapshotStore, StoreError, dto::Snapshot};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};
#[derive(Clone, Debug)]
pub enum Fault {
    Conflict,
    BeforePublication,
    LostResponse,
}
#[derive(Default)]
struct State {
    snapshot: Option<Snapshot>,
    faults: VecDeque<Fault>,
    evaluations: usize,
}
#[derive(Clone, Default)]
pub struct MemoryStore(Arc<Mutex<State>>);
impl MemoryStore {
    pub fn inject(&self, fault: Fault) {
        self.0.lock().unwrap().faults.push_back(fault);
    }
    /// Deliberately bypass validation for corruption tests.
    pub fn set_raw(&self, snapshot: Snapshot) {
        self.0.lock().unwrap().snapshot = Some(snapshot);
    }
    pub fn evaluations(&self) -> usize {
        self.0.lock().unwrap().evaluations
    }
}
impl SnapshotStore for MemoryStore {
    fn initialize(&self, empty: &Snapshot) -> Result<(), StoreError> {
        if empty.schema_version != 1
            || empty.revision != 0
            || !empty.resources.is_empty()
            || !empty.transactions.is_empty()
        {
            return Err(StoreError::Corrupt(
                "initial snapshot must be empty at revision zero".into(),
            ));
        }
        let mut state = self.0.lock().unwrap();
        if state.snapshot.is_some() {
            return Err(StoreError::Exists);
        }
        state.snapshot = Some(empty.clone());
        Ok(())
    }
    fn load(&self) -> Result<Snapshot, StoreError> {
        self.0
            .lock()
            .unwrap()
            .snapshot
            .clone()
            .ok_or(StoreError::Missing)
    }
    fn replace(&self, expected: u64, candidate: &Snapshot) -> Result<Publication, StoreError> {
        let mut state = self.0.lock().unwrap();
        state.evaluations += 1;
        if state.snapshot.as_ref().ok_or(StoreError::Missing)?.revision != expected {
            return Ok(Publication::Conflict);
        }
        if expected.checked_add(1) != Some(candidate.revision) {
            return Err(StoreError::Corrupt("invalid revision".into()));
        }
        candidate
            .validate()
            .map_err(|e| StoreError::Corrupt(e.to_string()))?;
        let fault = state.faults.pop_front();
        match fault {
            Some(Fault::Conflict) => return Ok(Publication::Conflict),
            Some(Fault::BeforePublication) => {
                return Err(StoreError::Io("injected pre-publication failure".into()));
            }
            _ => {}
        }
        state.snapshot = Some(candidate.clone());
        if matches!(fault, Some(Fault::LostResponse)) {
            return Err(StoreError::Unknown("injected lost response".into()));
        }
        Ok(Publication::Applied)
    }
}
