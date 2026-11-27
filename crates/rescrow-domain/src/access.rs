//! Pure resource grants, bounded admission facts, and pending-hold accounting.
//! Cryptographic credential verification and tenant routing belong outside this crate.
use crate::{Error, Id};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Action {
    Create,
    Prepare,
    Commit,
    Abort,
    Read,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Grant {
    prefix: String,
    debit: bool,
    credit: bool,
    max_amount: i128,
}
impl Grant {
    /// `*` grants every resource; otherwise this is a literal ASCII ID prefix.
    pub fn new(prefix: String, debit: bool, credit: bool, max_amount: i128) -> Result<Self, Error> {
        if prefix != "*" {
            Id::new(prefix.clone())?;
        }
        if max_amount <= 0 {
            return Err(Error::InvalidAuthorization);
        }
        Ok(Self {
            prefix,
            debit,
            credit,
            max_amount,
        })
    }
    pub fn prefix(&self) -> &str {
        &self.prefix
    }
    pub fn debit(&self) -> bool {
        self.debit
    }
    pub fn credit(&self) -> bool {
        self.credit
    }
    pub fn max_amount(&self) -> i128 {
        self.max_amount
    }
    pub fn matches(&self, id: &Id) -> bool {
        self.prefix == "*" || id.as_str().starts_with(&self.prefix)
    }
    pub fn permits(&self, id: &Id, delta: i128) -> bool {
        self.matches(id)
            && delta.checked_abs().is_some_and(|n| n <= self.max_amount)
            && (delta == 0 || (delta < 0 && self.debit) || (delta > 0 && self.credit))
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Policy {
    actions: BTreeSet<Action>,
    grants: Vec<Grant>,
    max_hold_ms: Option<u64>,
}
impl Policy {
    pub fn new(
        actions: Vec<Action>,
        grants: Vec<Grant>,
        max_hold_ms: Option<u64>,
    ) -> Result<Self, Error> {
        let count = actions.len();
        let actions: BTreeSet<_> = actions.into_iter().collect();
        if count == 0
            || count != actions.len()
            || grants.is_empty()
            || grants.len() > 64
            || max_hold_ms.is_some_and(|ms| ms == 0 || ms > crate::MAX_HOLD_MS)
        {
            return Err(Error::InvalidAuthorization);
        }
        Ok(Self {
            actions,
            grants,
            max_hold_ms,
        })
    }
    pub fn actions(&self) -> &BTreeSet<Action> {
        &self.actions
    }
    pub fn grants(&self) -> &[Grant] {
        &self.grants
    }
    pub fn max_hold_ms(&self) -> Option<u64> {
        self.max_hold_ms
    }
    pub fn allows(&self, action: Action) -> bool {
        self.actions.contains(&action)
    }
    pub fn can_read(&self, id: &Id) -> bool {
        self.grants.iter().any(|g| g.matches(id))
    }
    pub fn permits(&self, id: &Id, delta: i128) -> bool {
        self.grants.iter().any(|g| g.permits(id, delta))
    }
    pub fn permits_hold(&self, hold_ms: Option<u64>) -> bool {
        self.max_hold_ms
            .is_none_or(|max| hold_ms.is_some_and(|ms| ms <= max))
    }
}

/// Small immutable admission evidence. Approved resource/direction/amount scope
/// is the request manifest itself. These fingerprints identify the principal,
/// stable credential, and exact admitted policy without retaining bearer tokens.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Facts {
    principal_id: String,
    credential_id: String,
    policy_digest: String,
}
fn fingerprint(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
impl Facts {
    pub fn new(
        principal_id: String,
        credential_id: String,
        policy_digest: String,
    ) -> Result<Self, Error> {
        if ![&principal_id, &credential_id, &policy_digest]
            .into_iter()
            .all(|s| fingerprint(s))
        {
            return Err(Error::InvalidAuthorization);
        }
        Ok(Self {
            principal_id,
            credential_id,
            policy_digest,
        })
    }
    pub fn principal_id(&self) -> &str {
        &self.principal_id
    }
    pub fn credential_id(&self) -> &str {
        &self.credential_id
    }
    pub fn policy_digest(&self) -> &str {
        &self.policy_digest
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Context(BTreeMap<String, String>);
impl Context {
    pub const MAX_BYTES: usize = 4096;
    pub fn new(entries: Vec<(String, String)>) -> Result<Self, Error> {
        if entries.len() > 32 {
            return Err(Error::InvalidContext);
        }
        let mut values = BTreeMap::new();
        let mut bytes = 0usize;
        for (key, value) in entries {
            if key.is_empty()
                || key.len() > 64
                || !key
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
                || value.len() > 1024
            {
                return Err(Error::InvalidContext);
            }
            bytes = bytes
                .checked_add(key.len() + value.len())
                .ok_or(Error::InvalidContext)?;
            if bytes > Self::MAX_BYTES || values.insert(key, value).is_some() {
                return Err(Error::InvalidContext);
            }
        }
        Ok(Self(values))
    }
    pub fn entries(&self) -> &BTreeMap<String, String> {
        &self.0
    }
}

/// One versioned tenant record counts prepared authenticated transactions. It
/// participates in the same publication as holds, so separate servers cannot
/// pass a quota check concurrently using stale counts.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Pending {
    by_credential: BTreeMap<String, u64>,
    total: u64,
}
impl Pending {
    pub fn restore(entries: Vec<(String, u64)>) -> Result<Self, Error> {
        let mut next = Self::default();
        for (id, count) in entries {
            if !fingerprint(&id) || count == 0 || next.by_credential.insert(id, count).is_some() {
                return Err(Error::InvalidLedger);
            }
            next.total = next.total.checked_add(count).ok_or(Error::Overflow)?;
        }
        Ok(next)
    }
    pub fn from_prepared<'a>(
        transactions: impl IntoIterator<Item = &'a crate::Transaction>,
    ) -> Result<Self, Error> {
        let mut pending = Self::default();
        for tx in transactions {
            if *tx.state() == crate::State::Prepared
                && let Some(facts) = tx.request().authorization()
            {
                pending = pending.reserve(facts.credential_id(), u64::MAX, u64::MAX)?;
            }
        }
        Ok(pending)
    }
    pub fn apply(
        &self,
        before: Option<&crate::Transaction>,
        after: &crate::Transaction,
    ) -> Result<Self, Error> {
        if before.is_some_and(|tx| tx.request() != after.request()) {
            return Err(Error::InvalidLedger);
        }
        let was_prepared = before.is_some_and(|tx| *tx.state() == crate::State::Prepared);
        let is_prepared = *after.state() == crate::State::Prepared;
        let Some(facts) = after.request().authorization() else {
            return Ok(self.clone());
        };
        match (was_prepared, is_prepared) {
            (false, true) => self.reserve(facts.credential_id(), u64::MAX, u64::MAX),
            (true, false) => self.release(facts.credential_id()),
            _ => Ok(self.clone()),
        }
    }
    pub fn entries(&self) -> &BTreeMap<String, u64> {
        &self.by_credential
    }
    pub fn total(&self) -> u64 {
        self.total
    }
    pub fn for_credential(&self, id: &str) -> u64 {
        self.by_credential.get(id).copied().unwrap_or(0)
    }
    pub fn reserve(
        &self,
        id: &str,
        tenant_limit: u64,
        credential_limit: u64,
    ) -> Result<Self, Error> {
        if !fingerprint(id) {
            return Err(Error::InvalidAuthorization);
        }
        let count = self.for_credential(id);
        if self.total >= tenant_limit || count >= credential_limit {
            return Err(Error::AdmissionLimit);
        }
        let mut next = self.clone();
        next.total = next.total.checked_add(1).ok_or(Error::Overflow)?;
        next.by_credential
            .insert(id.into(), count.checked_add(1).ok_or(Error::Overflow)?);
        Ok(next)
    }
    pub fn release(&self, id: &str) -> Result<Self, Error> {
        let count = self.by_credential.get(id).ok_or(Error::InvalidLedger)?;
        let mut next = self.clone();
        next.total = next.total.checked_sub(1).ok_or(Error::InvalidLedger)?;
        if *count == 1 {
            next.by_credential.remove(id);
        } else {
            next.by_credential.insert(id.into(), count - 1);
        }
        Ok(next)
    }
}
