//! Verified startup capabilities, kept outside the pure escrow Domain.
use crate::{Error, StoreError, dto};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DurabilityProfile {
    Simulation,
    Local,
    Regional,
    EnterpriseMr,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Consistency {
    Linearizable,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureDomain {
    Process,
    Device,
    AvailabilityZone,
    Region,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionBehavior {
    RejectWithoutAuthority,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    pub durability_profile: DurabilityProfile,
    pub consistency: Consistency,
    pub failure_domain: FailureDomain,
    pub active_regions: BTreeSet<String>,
    pub witness_region: Option<String>,
    pub max_record_bytes: Option<u64>,
    pub max_participants: Option<usize>,
    pub partition_behavior: PartitionBehavior,
    pub multi_tenant: bool,
    pub cross_process: bool,
    pub reopen_after_crash: bool,
}
impl Capabilities {
    pub fn simulation() -> Self {
        Self {
            durability_profile: DurabilityProfile::Simulation,
            consistency: Consistency::Linearizable,
            failure_domain: FailureDomain::Process,
            active_regions: BTreeSet::new(),
            witness_region: None,
            max_record_bytes: None,
            max_participants: None,
            partition_behavior: PartitionBehavior::RejectWithoutAuthority,
            multi_tenant: false,
            cross_process: false,
            reopen_after_crash: false,
        }
    }
    /// Validate the observed topology as well as the configured profile. A
    /// stronger profile may satisfy a smaller one; simulation never satisfies
    /// any durability requirement.
    pub fn require(&self, required: DurabilityProfile) -> Result<(), Error> {
        use DurabilityProfile::*;
        let valid = self.max_record_bytes != Some(0)
            && self.max_participants != Some(0)
            && match self.durability_profile {
                Simulation => self.failure_domain == FailureDomain::Process,
                Local => self.failure_domain == FailureDomain::Device,
                Regional => {
                    self.failure_domain == FailureDomain::AvailabilityZone
                        && self.active_regions.len() == 1
                        && self.witness_region.is_none()
                }
                EnterpriseMr => {
                    self.failure_domain == FailureDomain::Region
                        && self.active_regions.len() >= 2
                        && self
                            .witness_region
                            .as_ref()
                            .is_some_and(|w| !self.active_regions.contains(w))
                }
            };
        let meets = match required {
            Simulation => true,
            Local => self.durability_profile != Simulation,
            Regional => matches!(self.durability_profile, Regional | EnterpriseMr),
            EnterpriseMr => self.durability_profile == EnterpriseMr,
        };
        if valid && meets {
            Ok(())
        } else {
            Err(Error::input(format!(
                "authority capabilities {:?} cannot serve required durability profile {required:?}; verify actual infrastructure and topology",
                self
            )))
        }
    }
}

/// Implementations inspect the live infrastructure, not just deployment
/// configuration. A future DSQL implementation must inspect peering/witness;
/// merely returning requested configuration is not a readiness check.
pub trait Readiness {
    fn readiness(&self) -> Result<Capabilities, StoreError>;
}

/// Server response metadata. Local CLI outputs keep their existing shape.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Response {
    pub durability_profile: DurabilityProfile,
    pub output: dto::Output,
}
