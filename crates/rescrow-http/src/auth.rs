//! Cached asymmetric workload JWT verification. No token-selected URL is
//! fetched, and no bearer token is retained in an Application admission record.
use axum::http::request::Parts;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use rescrow_application::{
    Category, Code, Error,
    access::{AccessContext, Action, Caller, Grant, Policy},
    canonical,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Pluggable identity boundary. Called on a bounded blocking worker. An mTLS
/// implementation can inspect verified connection extensions in `Parts`; raw
/// identity headers alone are never proof of an authenticated peer.
pub trait Authenticator: Send + Sync + 'static {
    fn authenticate(&self, parts: &Parts) -> Result<AccessContext, Error>;
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PinnedAlgorithm {
    RS256,
    ES256,
    EdDSA,
}
impl PinnedAlgorithm {
    fn algorithm(self) -> Algorithm {
        match self {
            Self::RS256 => Algorithm::RS256,
            Self::ES256 => Algorithm::ES256,
            Self::EdDSA => Algorithm::EdDSA,
        }
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationKey {
    pub kid: String,
    pub algorithm: PinnedAlgorithm,
    pub public_key_pem: String,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Issuer {
    pub issuer: String,
    pub audience: String,
    pub keys: Vec<VerificationKey>,
}
struct TrustedKey {
    kid: String,
    algorithm: Algorithm,
    key: DecodingKey,
    validation: Validation,
}
pub struct JwtAuthenticator {
    keys: Vec<TrustedKey>,
}
#[derive(Deserialize)]
struct Claims {
    iss: String,
    sub: String,
    exp: u64,
    tenant: String,
    namespace: Option<String>,
    credential_id: String,
    scopes: Vec<Action>,
    resources: Vec<Grant>,
    max_hold_ms: Option<u64>,
}
fn invalid() -> Error {
    Error::new(
        Category::Authentication,
        Code::Unauthenticated,
        "a valid workload bearer token is required",
    )
}
fn bounded(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.len() <= max
        && !value.chars().any(char::is_control)
        && value.trim() == value
}
impl JwtAuthenticator {
    pub const MAX_TOKEN_BYTES: usize = 16 * 1024;
    pub fn new(issuers: Vec<Issuer>) -> Result<Self, Error> {
        if issuers.is_empty() || issuers.len() > 16 {
            return Err(Error::input("configure 1..16 trusted JWT issuers"));
        }
        let provider = jsonwebtoken::crypto::CryptoProvider::try_get_default()
            .ok_or_else(|| Error::input("exactly one JWT crypto provider must be selected"))?;
        let mut names = BTreeSet::new();
        let mut keys = Vec::new();
        for issuer in issuers {
            if !bounded(&issuer.issuer, 512)
                || !bounded(&issuer.audience, 256)
                || issuer.keys.is_empty()
                || issuer.keys.len() > 32
                || !names.insert(issuer.issuer.clone())
            {
                return Err(Error::input(
                    "invalid or duplicate trusted issuer configuration",
                ));
            }
            let mut kids = BTreeSet::new();
            for config in issuer.keys {
                if !bounded(&config.kid, 128)
                    || !kids.insert(config.kid.clone())
                    || config.public_key_pem.len() > 16 * 1024
                {
                    return Err(Error::input(
                        "invalid or duplicate verification key configuration",
                    ));
                }
                // Only public PEM containers are accepted; signing keys do not
                // belong in the HTTP verifier configuration.
                if !config.public_key_pem.contains("PUBLIC KEY-----")
                    || config.public_key_pem.contains("PRIVATE KEY")
                {
                    return Err(Error::input("JWT verification requires a public key"));
                }
                let algorithm = config.algorithm.algorithm();
                let key = match config.algorithm {
                    PinnedAlgorithm::RS256 => {
                        DecodingKey::from_rsa_pem(config.public_key_pem.as_bytes())
                    }
                    PinnedAlgorithm::ES256 => {
                        DecodingKey::from_ec_pem(config.public_key_pem.as_bytes())
                    }
                    PinnedAlgorithm::EdDSA => {
                        DecodingKey::from_ed_pem(config.public_key_pem.as_bytes())
                    }
                }
                .map_err(|_| Error::input("invalid public verification key"))?;
                (provider.verifier_factory)(&algorithm, &key).map_err(|_| {
                    Error::input("public key does not support its pinned algorithm")
                })?;
                let mut validation = Validation::new(algorithm);
                validation.leeway = 0;
                validation.validate_nbf = true;
                validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
                validation.set_issuer(std::slice::from_ref(&issuer.issuer));
                validation.set_audience(std::slice::from_ref(&issuer.audience));
                keys.push(TrustedKey {
                    kid: config.kid,
                    algorithm,
                    key,
                    validation,
                });
                if keys.len() > 64 {
                    return Err(Error::input("at most 64 verification keys are supported"));
                }
            }
        }
        Ok(Self { keys })
    }
    pub fn verify(&self, token: &str) -> Result<AccessContext, Error> {
        if token.len() > Self::MAX_TOKEN_BYTES || token.bytes().any(|b| b.is_ascii_whitespace()) {
            return Err(invalid());
        }
        let pieces: Vec<_> = token.split('.').collect();
        if pieces.len() != 3 {
            return Err(invalid());
        }
        let header = URL_SAFE_NO_PAD.decode(pieces[0]).map_err(|_| invalid())?;
        let payload = URL_SAFE_NO_PAD.decode(pieces[1]).map_err(|_| invalid())?;
        let header: serde_json::Value = canonical::decode(&header).map_err(|_| invalid())?;
        let _: serde_json::Value = canonical::decode(&payload).map_err(|_| invalid())?;
        if !header.is_object()
            || ["crit", "enc", "zip", "cty", "b64"]
                .iter()
                .any(|key| header.get(key).is_some())
        {
            return Err(invalid());
        }
        if header
            .get("typ")
            .is_some_and(|v| v != "JWT" && v != "at+jwt")
        {
            return Err(invalid());
        }
        let parsed = jsonwebtoken::decode_header(token).map_err(|_| invalid())?;
        let kid = parsed.kid.as_deref().ok_or_else(invalid)?;
        if !bounded(kid, 128) {
            return Err(invalid());
        }
        for key in &self.keys {
            if key.kid != kid || key.algorithm != parsed.alg {
                continue;
            }
            if let Ok(data) = jsonwebtoken::decode::<Claims>(token, &key.key, &key.validation) {
                let claims = data.claims;
                let caller = Caller {
                    tenant: claims.tenant,
                    namespace: claims.namespace,
                    issuer: claims.iss,
                    subject: claims.sub,
                    credential_id: claims.credential_id,
                    expires_at_s: claims.exp,
                    policy: Policy {
                        scopes: claims.scopes,
                        resources: claims.resources,
                        max_hold_ms: claims.max_hold_ms,
                    },
                };
                let context: AccessContext = caller.try_into().map_err(|_| invalid())?;
                context
                    .validate_at(rescrow_application::system_clock())
                    .map_err(|_| invalid())?;
                return Ok(context);
            }
        }
        Err(invalid())
    }
}
impl Authenticator for JwtAuthenticator {
    fn authenticate(&self, parts: &Parts) -> Result<AccessContext, Error> {
        let mut values = parts
            .headers
            .get_all(axum::http::header::AUTHORIZATION)
            .iter();
        let value = values.next().ok_or_else(invalid)?;
        if values.next().is_some() {
            return Err(invalid());
        }
        let value = value.to_str().map_err(|_| invalid())?;
        let (scheme, token) = value.split_once(' ').ok_or_else(invalid)?;
        if !scheme.eq_ignore_ascii_case("Bearer") {
            return Err(invalid());
        }
        self.verify(token)
    }
}
