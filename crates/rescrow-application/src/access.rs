//! Credential-independent authorization input and validation. Token signatures
//! are verified by an outer identity adapter before constructing this context.
use crate::{
    Category, Code, Error,
    authority::{Scope, TenantId},
    canonical, dto,
};
use rescrow_domain::{self as d, access as a};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Action {
    #[serde(rename = "resource:create")]
    Create,
    #[serde(rename = "transaction:prepare")]
    Prepare,
    #[serde(rename = "transaction:commit")]
    Commit,
    #[serde(rename = "transaction:abort")]
    Abort,
    #[serde(rename = "read")]
    Read,
}
impl From<Action> for a::Action {
    fn from(v: Action) -> Self {
        match v {
            Action::Create => Self::Create,
            Action::Prepare => Self::Prepare,
            Action::Commit => Self::Commit,
            Action::Abort => Self::Abort,
            Action::Read => Self::Read,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Grant {
    pub resource_prefix: String,
    pub debit: bool,
    pub credit: bool,
    pub max_amount: String,
}
impl TryFrom<Grant> for a::Grant {
    type Error = d::Error;
    fn try_from(v: Grant) -> Result<Self, Self::Error> {
        Self::new(
            v.resource_prefix,
            v.debit,
            v.credit,
            dto::decimal(&v.max_amount)?,
        )
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    pub scopes: Vec<Action>,
    pub resources: Vec<Grant>,
    pub max_hold_ms: Option<u64>,
}
impl TryFrom<Policy> for a::Policy {
    type Error = d::Error;
    fn try_from(v: Policy) -> Result<Self, Self::Error> {
        Self::new(
            v.scopes.into_iter().map(Into::into).collect(),
            v.resources
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            v.max_hold_ms,
        )
    }
}

/// Only verified identity adapters construct this transfer value. It is not an
/// HTTP request body and is never deserialized from a caller-supplied tenant path.
#[derive(Clone, Debug)]
pub struct Caller {
    pub tenant: String,
    pub namespace: Option<String>,
    pub issuer: String,
    pub subject: String,
    /// Stable across token refresh, unlike a per-token `jti`.
    pub credential_id: String,
    pub expires_at_s: u64,
    pub policy: Policy,
}
#[derive(Clone, Debug)]
pub struct AccessContext {
    scope: Scope,
    policy: a::Policy,
    facts: a::Facts,
    expires_at_s: u64,
}
fn identity(s: &str, max: usize) -> bool {
    !s.is_empty() && s.len() <= max && !s.chars().any(char::is_control) && s.trim() == s
}
fn fingerprint(domain: &str, value: &impl Serialize) -> String {
    let digest = canonical::digest(
        domain,
        &serde_json::to_vec(value).expect("identity DTO is serializable"),
    );
    digest["sha256:".len()..].into()
}
impl TryFrom<Caller> for AccessContext {
    type Error = Error;
    fn try_from(v: Caller) -> Result<Self, Self::Error> {
        if !identity(&v.issuer, 512)
            || !identity(&v.subject, 256)
            || !identity(&v.credential_id, 128)
            || v.expires_at_s == 0
        {
            return Err(Error::new(
                Category::Authentication,
                Code::Unauthenticated,
                "invalid workload identity facts",
            ));
        }
        let tenant = TenantId::new(v.tenant)?;
        let scope = match v.namespace.as_deref() {
            Some(namespace) => Scope::new(tenant, namespace)?,
            None if tenant == TenantId::local() => Scope::local(),
            None => {
                return Err(Error::input(
                    "non-local identities require an authorized namespace",
                ));
            }
        };
        let policy: a::Policy = v
            .policy
            .clone()
            .try_into()
            .map_err(|e: d::Error| Error::input(e.to_string()))?;
        let principal_id = fingerprint("rescrow.principal.v1", &[&v.issuer, &v.subject]);
        let credential_id = fingerprint(
            "rescrow.credential.v1",
            &[&v.issuer, &v.subject, &v.credential_id],
        );
        let policy_digest = fingerprint("rescrow.authorization.v1", &v.policy);
        let facts = a::Facts::new(principal_id, credential_id, policy_digest)
            .map_err(|e| Error::input(e.to_string()))?;
        Ok(Self {
            scope,
            policy,
            facts,
            expires_at_s: v.expires_at_s,
        })
    }
}
impl AccessContext {
    pub fn scope(&self) -> &Scope {
        &self.scope
    }
    pub fn facts(&self) -> &a::Facts {
        &self.facts
    }
    pub fn policy(&self) -> &a::Policy {
        &self.policy
    }
    pub fn validate_at(&self, now_ms: u64) -> Result<(), Error> {
        if now_ms / 1000 >= self.expires_at_s {
            Err(Error::new(
                Category::Authentication,
                Code::Unauthenticated,
                "workload token has expired",
            ))
        } else {
            Ok(())
        }
    }
    pub fn require(&self, action: Action) -> Result<(), Error> {
        if self.policy.allows(action.into()) {
            Ok(())
        } else {
            Err(Error::new(
                Category::Authorization,
                Code::Forbidden,
                "credential does not grant this operation",
            ))
        }
    }
    pub fn resource(&self, action: Action, id: &str, delta: Option<&str>) -> Result<(), Error> {
        self.require(action)?;
        let id = d::Id::new(id.into()).map_err(|e| Error::input(e.to_string()))?;
        let allowed = match delta {
            Some(delta) => self.policy.permits(
                &id,
                dto::decimal(delta).map_err(|e| Error::input(e.to_string()))?,
            ),
            None => self.policy.can_read(&id),
        };
        if allowed {
            Ok(())
        } else {
            Err(Error::new(
                Category::Authorization,
                if action == Action::Read {
                    Code::NotFound
                } else {
                    Code::Forbidden
                },
                "resource is outside the credential's authorized scope",
            ))
        }
    }
    pub(crate) fn bind_prepare(&self, mut request: dto::Prepare) -> Result<dto::Prepare, Error> {
        if request
            .namespace
            .as_deref()
            .is_some_and(|n| Some(n) != self.scope.namespace())
        {
            return Err(Error::new(
                Category::Authorization,
                Code::Forbidden,
                "namespace differs from credential context",
            ));
        }
        if request.authorization.is_some() {
            return Err(Error::input(
                "admission authorization facts cannot be supplied by the request",
            ));
        }
        self.request(Action::Prepare, &request)?;
        request.authorization = Some((&self.facts).into());
        Ok(request)
    }
    pub fn request(&self, action: Action, request: &dto::Prepare) -> Result<(), Error> {
        self.require(action)?;
        if action == Action::Prepare && !self.policy.permits_hold(request.hold_ms) {
            return Err(Error::new(
                Category::Authorization,
                Code::Forbidden,
                "hold lifetime is outside the credential's authorized scope",
            ));
        }
        for op in &request.operations {
            self.resource(
                action,
                &op.resource_id,
                if action == Action::Read {
                    None
                } else {
                    Some(&op.delta)
                },
            )?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingLimits {
    pub tenant: u64,
    pub credential: u64,
}
impl PendingLimits {
    pub fn validate(self) -> Result<Self, Error> {
        if self.tenant == 0 || self.credential == 0 || self.credential > self.tenant {
            return Err(Error::input(
                "pending limits must be positive and credential must not exceed tenant",
            ));
        }
        Ok(self)
    }
}
impl Default for PendingLimits {
    fn default() -> Self {
        Self {
            tenant: 4096,
            credential: 256,
        }
    }
}
