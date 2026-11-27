//! HTTP v1 transfer contract. These are data, independent of Axum/Tower or a
//! particular client. The Domain is never serialized directly.
use crate::{Category, Code, Error, capabilities::DurabilityProfile, dto};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Prepare {
    pub operations: Vec<dto::Operation>,
    #[serde(default)]
    pub hold_ms: Option<u64>,
    #[serde(default)]
    pub context: BTreeMap<String, String>,
    /// Optional assertion only; the credential still chooses the namespace.
    #[serde(default)]
    pub namespace: Option<String>,
}
impl Prepare {
    pub fn into_command(self, id: String) -> dto::Prepare {
        dto::Prepare {
            reservation_id: id,
            namespace: self.namespace,
            operations: self.operations,
            hold_ms: self.hold_ms,
            context: self.context,
            authorization: None,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Commit {
    #[serde(default)]
    pub actuals: Option<Vec<dto::Actual>>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceCreate {
    pub lower: String,
    #[serde(deserialize_with = "dto::required")]
    pub upper: Option<String>,
    pub initial: String,
    pub unit: String,
    pub scale: u32,
}
impl ResourceCreate {
    pub fn into_spec(self, id: String) -> dto::ResourceSpec {
        dto::ResourceSpec {
            resource_id: id,
            lower: self.lower,
            upper: self.upper,
            initial: self.initial,
            unit: self.unit,
            scale: self.scale,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Metadata {
    pub request_id: String,
    pub serving_region: String,
    pub deployment_cell: String,
    pub authority_generation: String,
    pub durability_profile: DurabilityProfile,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Envelope<T> {
    pub data: T,
    pub metadata: Metadata,
    pub links: BTreeMap<String, String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Consistency {
    Sampled,
    Authoritative,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Observation {
    pub consistency: Consistency,
    pub observed_from_ms: u64,
    pub observed_until_ms: u64,
    pub quantity: String,
    pub holds: Vec<dto::Hold>,
    pub reserved_down: String,
    pub reserved_up: String,
    pub available_down: String,
    pub available_up: String,
    pub finalization_pending: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Resource {
    pub spec: dto::ResourceSpec,
    pub observation: Observation,
}
impl Resource {
    /// Validate an untrusted server response before exposing it as a ledger DTO.
    pub fn validated(self) -> Result<dto::Resource, Error> {
        if self.observation.observed_from_ms > self.observation.observed_until_ms
            || self.observation.finalization_pending
        {
            return Err(Error::input("invalid resource observation"));
        }
        let value = self.into_dto();
        let _: rescrow_domain::Resource = value
            .clone()
            .try_into()
            .map_err(|e: rescrow_domain::Error| Error::input(e.to_string()))?;
        Ok(value)
    }
    pub fn observed(v: dto::Resource, from_ms: u64, until_ms: u64) -> Self {
        Self {
            spec: v.spec,
            observation: Observation {
                consistency: Consistency::Sampled,
                observed_from_ms: from_ms,
                observed_until_ms: until_ms,
                quantity: v.quantity,
                holds: v.holds,
                reserved_down: v.reserved_down,
                reserved_up: v.reserved_up,
                available_down: v.available_down,
                available_up: v.available_up,
                finalization_pending: false,
            },
        }
    }
    pub fn into_dto(self) -> dto::Resource {
        let o = self.observation;
        dto::Resource {
            spec: self.spec,
            quantity: o.quantity,
            holds: o.holds,
            reserved_down: o.reserved_down,
            reserved_up: o.reserved_up,
            available_down: o.available_down,
            available_up: o.available_up,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reservation {
    pub reservation: dto::Reservation,
    pub request_digest: String,
    pub actuals_digest: Option<String>,
    pub consistency: Consistency,
    pub materialization: Materialization,
    pub processing: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Materialization {
    Complete,
}
impl Reservation {
    pub fn validated(self) -> Result<dto::Reservation, Error> {
        let _: rescrow_domain::Reservation = self
            .reservation
            .clone()
            .try_into()
            .map_err(|e: rescrow_domain::Error| Error::input(e.to_string()))?;
        let canonical = Self::from_dto(self.reservation.clone())?;
        if canonical.request_digest != self.request_digest
            || canonical.actuals_digest != self.actuals_digest
            || (self.processing && !matches!(self.reservation.state, dto::State::Prepared {}))
        {
            return Err(Error::input(
                "invalid reservation response digest or processing state",
            ));
        }
        Ok(self.reservation)
    }
    pub fn from_dto(reservation: dto::Reservation) -> Result<Self, Error> {
        Ok(Self {
            request_digest: crate::canonical::prepare_digest(&reservation.request)?,
            actuals_digest: crate::canonical::actuals_digest(&reservation)?,
            reservation,
            consistency: Consistency::Authoritative,
            materialization: Materialization::Complete,
            processing: false,
        })
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Problem {
    #[serde(rename = "type")]
    pub problem_type: String,
    pub code: Code,
    pub category: Category,
    pub status: u16,
    pub detail: String,
    pub request_id: String,
    pub reservation_id: Option<String>,
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity: Option<Box<dto::CapacityFailure>>,
}
/// Exhaustive mapping: new Application codes require an explicit wire contract.
pub fn status(code: &Code) -> (u16, bool) {
    match code {
        Code::Unauthenticated => (401, false),
        Code::Forbidden => (403, false),
        Code::NotFound => (404, false),
        Code::MethodNotAllowed => (405, false),
        Code::UnsupportedMediaType => (415, false),
        Code::Conflict | Code::Capacity | Code::AlreadyInitialized => (409, false),
        Code::InvalidInput
        | Code::Overflow
        | Code::TooLarge
        | Code::LimitExceeded
        | Code::Expired => (422, false),
        Code::Throttled => (429, true),
        Code::Contention | Code::Io | Code::UnknownOutcome | Code::DeadlineExceeded => (503, true),
        Code::CorruptState
        | Code::UnsupportedSchema
        | Code::MissingStore
        | Code::RevisionExhausted => (503, false),
    }
}
impl Problem {
    pub fn from_error(error: Error, request_id: String) -> Self {
        let (status, retryable) = status(&error.code);
        let code = serde_json::to_value(&error.code).expect("error codes are serializable");
        let code_name = code.as_str().expect("error codes serialize to strings");
        let mut detail=match error.code {
            Code::Io=>"authority unavailable; retry only the identical request".to_string(),
            Code::CorruptState=>"authority state failed validation; operator attention is required".to_string(),
            Code::UnsupportedSchema=>"authority schema is unsupported; operator attention is required".to_string(),
            Code::MissingStore=>"selected authority is not initialized or is unavailable".to_string(),
            Code::UnknownOutcome|Code::DeadlineExceeded=>"outcome is unconfirmed and may have been applied; inspect or retry only the same reservation identity and request".to_string(),
            _=>error.message,
        };
        if detail.len() > 1024 {
            let mut end = 1024;
            while !detail.is_char_boundary(end) {
                end -= 1;
            }
            detail.truncate(end);
            detail.push('…');
        }
        Self {
            problem_type: format!("urn:rescrow:problem:{code_name}"),
            category: error.category,
            code: error.code,
            status,
            detail,
            request_id,
            reservation_id: error.reservation_id,
            retryable,
            capacity: error.capacity,
        }
    }
    pub fn into_error(self) -> Error {
        Error {
            category: self.category,
            code: self.code,
            message: self.detail,
            reservation_id: self.reservation_id,
            capacity: self.capacity,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LaneCapacity {
    pub concurrency: u32,
    pub queue: u32,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantLane {
    pub concurrency: u32,
    pub queue: u32,
    pub requests_per_second: u32,
    pub burst: u32,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    pub body_bytes: usize,
    pub operation_count: usize,
    pub context_bytes: usize,
    pub numeric_magnitude: String,
    pub request_timeout_ms: u64,
    pub queue_timeout_ms: u64,
    pub max_tenants: usize,
    pub page_size: usize,
    pub pending: crate::access::PendingLimits,
    pub admission: LaneCapacity,
    pub decisions: LaneCapacity,
    pub recovery: LaneCapacity,
    pub tenant_admission: TenantLane,
    pub tenant_decisions: TenantLane,
    pub tenant_recovery: TenantLane,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            body_bytes: 1024 * 1024,
            operation_count: 64,
            context_bytes: 4096,
            numeric_magnitude: (1u128 << 127).to_string(),
            request_timeout_ms: 5000,
            queue_timeout_ms: 100,
            max_tenants: 1024,
            page_size: 100,
            pending: crate::access::PendingLimits::default(),
            admission: LaneCapacity {
                concurrency: 32,
                queue: 128,
            },
            decisions: LaneCapacity {
                concurrency: 16,
                queue: 128,
            },
            recovery: LaneCapacity {
                concurrency: 16,
                queue: 128,
            },
            tenant_admission: TenantLane {
                concurrency: 8,
                queue: 16,
                requests_per_second: 100,
                burst: 100,
            },
            tenant_decisions: TenantLane {
                concurrency: 8,
                queue: 32,
                requests_per_second: 200,
                burst: 200,
            },
            tenant_recovery: TenantLane {
                concurrency: 8,
                queue: 32,
                requests_per_second: 200,
                burst: 200,
            },
        }
    }
}
impl Limits {
    pub fn validate(&self) -> Result<(), Error> {
        self.pending.validate()?;
        let magnitude = self.numeric_magnitude.parse::<u128>().map_err(|_| {
            Error::input(
                "numeric_magnitude must be a canonical positive integer no greater than 2^127",
            )
        })?;
        if magnitude == 0
            || magnitude > (1u128 << 127)
            || magnitude.to_string() != self.numeric_magnitude
        {
            return Err(Error::input("invalid numeric magnitude limit"));
        }
        if self.body_bytes == 0
            || self.body_bytes > 16 * 1024 * 1024
            || self.operation_count == 0
            || self.operation_count > 1024
            || self.context_bytes == 0
            || self.context_bytes > rescrow_domain::access::Context::MAX_BYTES
            || self.request_timeout_ms == 0
            || self.request_timeout_ms > 300_000
            || self.queue_timeout_ms > self.request_timeout_ms
            || self.max_tenants == 0
            || self.max_tenants > 10_000
            || self.page_size == 0
            || self.page_size > 1000
        {
            return Err(Error::input("request limits are outside supported bounds"));
        }
        for capacity in [self.admission, self.decisions, self.recovery] {
            if capacity.concurrency > 4096 || capacity.queue > 100_000 {
                return Err(Error::input(
                    "global lane capacity is outside supported bounds",
                ));
            }
        }
        if self.decisions.concurrency == 0 || self.recovery.concurrency == 0 {
            return Err(Error::input(
                "decisions and recovery need reserved concurrency",
            ));
        }
        for lane in [
            self.tenant_admission,
            self.tenant_decisions,
            self.tenant_recovery,
        ] {
            if lane.concurrency == 0
                || lane.concurrency > 4096
                || lane.queue > 100_000
                || lane.requests_per_second == 0
                || lane.requests_per_second > 1_000_000
                || lane.burst == 0
                || lane.burst > 1_000_000
            {
                return Err(Error::input(
                    "tenant lane limits are outside supported bounds",
                ));
            }
        }
        Ok(())
    }
    fn number(&self, value: &str) -> Result<(), Error> {
        let value = dto::decimal(value).map_err(|e| Error::input(e.to_string()))?;
        let magnitude = self
            .numeric_magnitude
            .parse::<u128>()
            .map_err(|_| Error::input("invalid numeric magnitude configuration"))?;
        if value.unsigned_abs() > magnitude {
            Err(Error::new(
                Category::Input,
                Code::LimitExceeded,
                "numeric magnitude exceeds the configured limit",
            ))
        } else {
            Ok(())
        }
    }
    pub fn prepare(&self, request: &dto::Prepare) -> Result<(), Error> {
        if request.operations.len() > self.operation_count {
            return Err(Error::new(
                Category::Input,
                Code::LimitExceeded,
                "operation count exceeds the configured participant limit",
            ));
        }
        let context_bytes = request.context.iter().try_fold(0usize, |n, (k, v)| {
            n.checked_add(k.len() + v.len())
                .ok_or_else(|| Error::input("context length overflow"))
        })?;
        if context_bytes > self.context_bytes {
            return Err(Error::new(
                Category::Input,
                Code::LimitExceeded,
                "context exceeds the configured byte limit",
            ));
        }
        for op in &request.operations {
            self.number(&op.delta)?;
        }
        let _: rescrow_domain::Request = request
            .clone()
            .try_into()
            .map_err(|e: rescrow_domain::Error| Error::input(e.to_string()))?;
        Ok(())
    }
    pub fn resource(&self, spec: &dto::ResourceSpec) -> Result<(), Error> {
        self.number(&spec.lower)?;
        self.number(&spec.initial)?;
        if let Some(upper) = &spec.upper {
            self.number(upper)?;
        }
        let _: rescrow_domain::ResourceSpec = spec
            .clone()
            .try_into()
            .map_err(|e: rescrow_domain::Error| Error::input(e.to_string()))?;
        Ok(())
    }
    pub fn actuals(&self, actuals: Option<&[dto::Actual]>) -> Result<(), Error> {
        if let Some(actuals) = actuals {
            if actuals.len() > self.operation_count {
                return Err(Error::new(
                    Category::Input,
                    Code::LimitExceeded,
                    "actual count exceeds the configured participant limit",
                ));
            }
            for actual in actuals {
                self.number(&actual.delta)?;
            }
        }
        Ok(())
    }
}

/// Preserve the Domain's legal dot-only IDs without URL dot-segment
/// normalization. `~` is outside the Domain alphabet, making these unambiguous.
pub fn path_id(id: &str) -> Result<String, Error> {
    crate::authority::RecordKey::resource(id)?;
    Ok(match id {
        "." => "~Lg",
        ".." => "~Li4",
        _ => id,
    }
    .into())
}
pub fn decode_path_id(id: &str) -> Result<String, Error> {
    let value = match id {
        "~Lg" => ".",
        "~Li4" => "..",
        "." | ".." => {
            return Err(Error::input(
                "dot-only IDs require the escaped path representation",
            ));
        }
        _ => id,
    };
    crate::authority::RecordKey::resource(value)?;
    Ok(value.into())
}
