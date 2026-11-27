//! Single-shot effect faults around any transactional adapter. Faults do not
//! invent domain state or modify publication requests.
use rescrow_application::{
    StoreError,
    authority::*,
    capabilities::{Capabilities, Readiness},
};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

#[derive(Clone, Copy, Debug)]
pub enum Fault {
    Conflict,
    BeforePublication,
    LostResponse,
    LostResponseUnresolved,
}
pub struct FaultStore<S> {
    inner: Arc<S>,
    faults: Arc<Mutex<VecDeque<Fault>>>,
    unresolved: Arc<std::sync::atomic::AtomicBool>,
    attempts: Arc<std::sync::atomic::AtomicUsize>,
}
impl<S> Clone for FaultStore<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            faults: self.faults.clone(),
            unresolved: self.unresolved.clone(),
            attempts: self.attempts.clone(),
        }
    }
}
impl<S> FaultStore<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner: Arc::new(inner),
            faults: Arc::default(),
            unresolved: Arc::default(),
            attempts: Arc::default(),
        }
    }
    pub fn evaluations(&self) -> usize {
        self.attempts.load(std::sync::atomic::Ordering::SeqCst)
    }
    pub fn inner(&self) -> &S {
        &self.inner
    }
    pub fn inject(&self, fault: Fault) {
        self.faults.lock().unwrap().push_back(fault);
    }
}
impl<S: TransactionalStore> TransactionalStore for FaultStore<S> {
    fn identity(&self) -> Result<String, StoreError> {
        self.inner.identity()
    }
    fn initialize(&self) -> Result<(), StoreError> {
        self.inner.initialize()
    }
    fn read(
        &self,
        tenant: &TenantId,
        keys: &[RecordKey],
        terminal_since_ms: u64,
    ) -> Result<Read, StoreError> {
        self.inner.read(tenant, keys, terminal_since_ms)
    }
    fn scan(&self, tenant: &TenantId) -> Result<Inspection, StoreError> {
        self.inner.scan(tenant)
    }
    fn commit(&self, commit: &Commit) -> Result<CommitOutcome, StoreError> {
        self.attempts
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let fault = self.faults.lock().unwrap().pop_front();
        match fault {
            Some(Fault::Conflict) => return Ok(CommitOutcome::Conflict),
            Some(Fault::BeforePublication) => {
                return Err(StoreError::Io("injected pre-publication failure".into()));
            }
            _ => (),
        }
        let outcome = self.inner.commit(commit)?;
        if matches!(fault, Some(Fault::LostResponseUnresolved))
            && matches!(outcome, CommitOutcome::Applied(_))
        {
            self.unresolved
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
        if matches!(
            fault,
            Some(Fault::LostResponse | Fault::LostResponseUnresolved)
        ) && matches!(outcome, CommitOutcome::Applied(_))
        {
            Err(StoreError::Unknown(
                "injected lost publication response".into(),
            ))
        } else {
            Ok(outcome)
        }
    }
    fn resolve(
        &self,
        tenant: &TenantId,
        transition: &TransitionId,
    ) -> Result<Resolution, StoreError> {
        if self
            .unresolved
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Ok(Resolution::Unknown);
        }
        self.inner.resolve(tenant, transition)
    }
    fn maintain(&self, minimum_entries: usize) -> Result<(), StoreError> {
        self.inner.maintain(minimum_entries)
    }
    fn history(&self, tenant: &TenantId) -> Result<Vec<HistoryRecord>, StoreError> {
        self.inner.history(tenant)
    }
}
impl<S: Readiness> Readiness for FaultStore<S> {
    fn readiness(&self) -> Result<Capabilities, StoreError> {
        self.inner.readiness()
    }
}
