//! Process-local SQL work and JSON payload counters, not physical storage
//! requests, network bytes, or billing DPUs.
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct Measurements {
    /// Explicit SELECT/INSERT/UPDATE attempts, including failures and retries.
    /// Excludes DDL, transaction control and driver pool-health statements.
    pub sql_statement_attempts: u64,
    pub encoded_payload_read_bytes: u64,
    pub encoded_payload_write_attempt_bytes: u64,
    pub publication_attempts: u64,
    /// Includes exact publication replays; not a count of new decisions.
    pub applied_responses: u64,
    pub conflicts: u64,
    pub unknown_publications: u64,
}
#[derive(Default)]
pub(crate) struct Counters {
    pub sql_statement_attempts: AtomicU64,
    pub encoded_payload_read_bytes: AtomicU64,
    pub encoded_payload_write_attempt_bytes: AtomicU64,
    pub publication_attempts: AtomicU64,
    pub applied_responses: AtomicU64,
    pub conflicts: AtomicU64,
    pub unknown_publications: AtomicU64,
}
impl Counters {
    pub fn statement(&self, write_bytes: usize) {
        self.sql_statement_attempts.fetch_add(1, Ordering::Relaxed);
        self.encoded_payload_write_attempt_bytes
            .fetch_add(write_bytes as u64, Ordering::Relaxed);
    }
    pub fn read(&self, bytes: usize) {
        self.encoded_payload_read_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }
    pub fn snapshot(&self) -> Measurements {
        Measurements {
            sql_statement_attempts: self.sql_statement_attempts.load(Ordering::Relaxed),
            encoded_payload_read_bytes: self.encoded_payload_read_bytes.load(Ordering::Relaxed),
            encoded_payload_write_attempt_bytes: self
                .encoded_payload_write_attempt_bytes
                .load(Ordering::Relaxed),
            publication_attempts: self.publication_attempts.load(Ordering::Relaxed),
            applied_responses: self.applied_responses.load(Ordering::Relaxed),
            conflicts: self.conflicts.load(Ordering::Relaxed),
            unknown_publications: self.unknown_publications.load(Ordering::Relaxed),
        }
    }
}
