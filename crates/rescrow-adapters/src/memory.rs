//! Deterministic log store for embedding tests, with single-shot effect faults.
//! It stores transfer values as decoded; the Application validates every load.
use rescrow_application::{
    Loaded, LogStore, Publication, StoreError,
    dto::{Checkpoint, Entry, Record, State as TxState},
};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};
#[derive(Clone, Debug)]
pub enum Fault {
    Conflict,
    BeforePublication,
    LostResponse,
    LostResponseUnresolved,
}
#[derive(Default)]
struct State {
    checkpoint: Option<Checkpoint>,
    entries: Vec<Entry>,
    faults: VecDeque<Fault>,
    evaluations: usize,
    unresolved: bool,
}
#[derive(Clone, Default)]
pub struct MemoryStore(Arc<Mutex<State>>);
impl MemoryStore {
    pub fn inject(&self, fault: Fault) {
        self.0.lock().unwrap().faults.push_back(fault);
    }
    /// The stored checkpoint and entries, for corruption tests.
    pub fn raw(&self) -> (Option<Checkpoint>, Vec<Entry>) {
        let state = self.0.lock().unwrap();
        (state.checkpoint.clone(), state.entries.clone())
    }
    /// Deliberately bypass validation for corruption tests.
    pub fn set_raw(&self, checkpoint: Checkpoint, entries: Vec<Entry>) {
        let mut state = self.0.lock().unwrap();
        state.checkpoint = Some(checkpoint);
        state.entries = entries;
    }
    /// Number of conditional appends attempted.
    pub fn evaluations(&self) -> usize {
        self.0.lock().unwrap().evaluations
    }
}
impl LogStore for MemoryStore {
    fn initialize(&self, empty: &Checkpoint) -> Result<(), StoreError> {
        if empty.schema_version != rescrow_application::SCHEMA_VERSION {
            return Err(StoreError::Unsupported(empty.schema_version));
        }
        if rescrow_application::uuid_v7_millis(&empty.authority_generation).is_none() {
            return Err(StoreError::Corrupt(
                "initial authority generation must be a UUIDv7".into(),
            ));
        }
        if empty.position != 0
            || empty.recorded_at_ms != 0
            || !empty.resources.is_empty()
            || !empty.prepared.is_empty()
        {
            return Err(StoreError::Corrupt(
                "initial checkpoint must be empty at position zero".into(),
            ));
        }
        let mut state = self.0.lock().unwrap();
        if state.checkpoint.is_some() {
            return Err(StoreError::Exists);
        }
        state.checkpoint = Some(empty.clone());
        Ok(())
    }
    fn load(&self) -> Result<Loaded, StoreError> {
        let state = self.0.lock().unwrap();
        let checkpoint = state.checkpoint.clone().ok_or(StoreError::Missing)?;
        let tail = usize::try_from(checkpoint.position)
            .ok()
            .and_then(|position| state.entries.get(position..))
            .map(<[Entry]>::to_vec)
            .unwrap_or_default();
        Ok(Loaded { checkpoint, tail })
    }
    fn append(&self, entries: &[Entry]) -> Result<Publication, StoreError> {
        let mut state = self.0.lock().unwrap();
        state.evaluations += 1;
        let checkpoint = state.checkpoint.as_ref().ok_or(StoreError::Missing)?;
        let length = (state.entries.len() as u64).max(checkpoint.position);
        if let Some(identity) = &entries[0].publication_id {
            let existing: Vec<_> = state
                .entries
                .iter()
                .filter(|e| e.publication_id.as_ref() == Some(identity))
                .cloned()
                .collect();
            if !existing.is_empty() {
                return if existing == entries {
                    Ok(Publication::Applied)
                } else {
                    Err(StoreError::Corrupt("publication identity reused".into()))
                };
            }
        }
        if entries[0].sequence != length {
            return Ok(Publication::Conflict);
        }
        let fault = state.faults.pop_front();
        match fault {
            Some(Fault::Conflict) => return Ok(Publication::Conflict),
            Some(Fault::BeforePublication) => {
                return Err(StoreError::Io("injected pre-publication failure".into()));
            }
            _ => {}
        }
        state.entries.extend_from_slice(entries);
        if matches!(fault, Some(Fault::LostResponseUnresolved)) {
            state.unresolved = true;
        }
        if matches!(
            fault,
            Some(Fault::LostResponse | Fault::LostResponseUnresolved)
        ) {
            return Err(StoreError::Unknown("injected lost response".into()));
        }
        Ok(Publication::Applied)
    }
    fn checkpoint(&self, checkpoint: &Checkpoint) -> Result<(), StoreError> {
        let mut state = self.0.lock().unwrap();
        let current = state.checkpoint.as_ref().ok_or(StoreError::Missing)?;
        if checkpoint.position > current.position
            && checkpoint.position <= state.entries.len() as u64
        {
            state.checkpoint = Some(checkpoint.clone());
        }
        Ok(())
    }
    fn find(&self, reservation_id: &str, since_ms: u64) -> Result<Option<Entry>, StoreError> {
        let state = self.0.lock().unwrap();
        Ok(state
            .entries
            .iter()
            .rev()
            .take_while(|entry| entry.recorded_at_ms >= since_ms)
            .find(|entry| match &entry.record {
                Record::Reservation(tx) => {
                    tx.request.reservation_id == reservation_id
                        && !matches!(tx.state, TxState::Prepared {})
                }
                Record::ResourceCreated(_) => false,
            })
            .cloned())
    }
    fn history(&self) -> Result<Vec<Entry>, StoreError> {
        let mut state = self.0.lock().unwrap();
        if std::mem::take(&mut state.unresolved) {
            return Err(StoreError::Unknown(
                "injected unavailable resolution read".into(),
            ));
        }
        Ok(state.entries.clone())
    }
}
