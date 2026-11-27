//! HTTP v1 client. A business request owns its identity before any network I/O.
//! Explicit alternate endpoints must share a pinned authority generation.
use reqwest::{Method, Url, header};
use rescrow_application::{
    Category, Code, Error,
    api::{self, Envelope, Page},
    canonical,
    capabilities::DurabilityProfile,
    dto,
};
use serde::{Serialize, de::DeserializeOwned};
use std::{sync::Mutex, time::Duration};
use tokio::time::Instant;

pub struct Config {
    pub endpoints: Vec<String>,
    pub bearer: String,
    pub required_profile: DurabilityProfile,
    pub authority_generation: Option<String>,
    pub attempt_timeout: Duration,
    pub overall_timeout: Duration,
    pub max_attempts: u32,
}
impl Config {
    pub fn new(endpoint: String, bearer: String) -> Self {
        Self {
            endpoints: vec![endpoint],
            bearer,
            required_profile: DurabilityProfile::Simulation,
            authority_generation: None,
            attempt_timeout: Duration::from_secs(5),
            overall_timeout: Duration::from_secs(30),
            max_attempts: 8,
        }
    }
}
/// Serialize/store this value before dispatch if work must survive client restart.
#[derive(Clone, Debug, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Preparation {
    pub id: String,
    pub request: api::Prepare,
}
impl Preparation {
    pub fn new(request: api::Prepare) -> Result<Self, Error> {
        let id = rescrow_application::new_id()?;
        canonical::prepare_digest(&request.clone().into_command(id.clone()))?;
        Ok(Self { id, request })
    }
}
pub struct Client {
    http: reqwest::Client,
    endpoints: Vec<Url>,
    config: Config,
    generation: Mutex<Option<String>>,
}
fn unknown(id: Option<&str>) -> Error {
    let mut e = Error::new(
        Category::UnknownOutcome,
        Code::DeadlineExceeded,
        "remote outcome is unconfirmed; inspect or retry only the identical identity and request",
    );
    e.transaction_id = id.map(str::to_owned);
    e
}
impl Client {
    pub fn new(config: Config) -> Result<Self, Error> {
        if config.endpoints.is_empty()
            || config.endpoints.len() > 8
            || config.bearer.is_empty()
            || config.bearer.len() > 16 * 1024
            || config.bearer.chars().any(char::is_control)
            || config.attempt_timeout.is_zero()
            || config.overall_timeout.is_zero()
            || config.attempt_timeout > config.overall_timeout
            || config.overall_timeout > Duration::from_secs(300)
            || config.max_attempts == 0
            || config.max_attempts > 32
        {
            return Err(Error::input(
                "invalid client endpoints, credentials or retry bounds",
            ));
        }
        if config
            .authority_generation
            .as_ref()
            .is_some_and(|g| rescrow_application::uuid_v7_millis(g).is_none())
            || (config.endpoints.len() > 1 && config.authority_generation.is_none())
        {
            return Err(Error::input(
                "alternate endpoints require the authority's canonical generation UUID",
            ));
        }
        let endpoints = config
            .endpoints
            .iter()
            .map(|value| {
                let url = Url::parse(value).map_err(|_| Error::input("invalid endpoint URL"))?;
                let loopback = url.host_str().is_some_and(|h| {
                    h == "localhost"
                        || h.trim_matches(['[', ']'])
                            .parse::<std::net::IpAddr>()
                            .is_ok_and(|ip| ip.is_loopback())
                });
                if !(url.scheme() == "https" || (url.scheme() == "http" && loopback))
                    || !url.username().is_empty()
                    || url.password().is_some()
                    || url.query().is_some()
                    || url.fragment().is_some()
                    || url.path() != "/"
                    || url.host_str().is_none()
                {
                    return Err(Error::input(
                        "endpoint must be an HTTPS origin, or loopback HTTP origin",
                    ));
                }
                Ok(url)
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let roots =
            rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let tls = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|_| Error::input("TLS protocol configuration failed"))?
        .with_root_certificates(roots)
        .with_no_client_auth();
        let http = reqwest::Client::builder()
            .tls_backend_preconfigured(tls)
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()
            .map_err(|_| Error::input("HTTP client configuration failed"))?;
        let generation = Mutex::new(config.authority_generation.clone());
        Ok(Self {
            http,
            endpoints,
            config,
            generation,
        })
    }
    pub fn authority_generation(&self) -> Option<String> {
        self.generation.lock().unwrap().clone()
    }
    async fn request<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<Vec<u8>>,
        id: Option<&str>,
        key: bool,
        deadline: Instant,
    ) -> Result<Envelope<T>, Error> {
        let profile =
            serde_json::to_value(self.config.required_profile).expect("profile serializes");
        let mut last = unknown(id);
        for attempt in 0..self.config.max_attempts {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            let endpoint = &self.endpoints[attempt as usize % self.endpoints.len()];
            let url = endpoint
                .join(path)
                .map_err(|_| Error::input("invalid request path"))?;
            let mut request = self
                .http
                .request(method.clone(), url)
                .bearer_auth(&self.config.bearer)
                .header("require-durability-profile", profile.as_str().unwrap())
                .timeout(self.config.attempt_timeout.min(remaining));
            if let Some(generation) = self.authority_generation() {
                request = request.header("require-authority-generation", generation);
            }
            if let Some(body) = &body {
                request = request
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(body.clone());
            }
            if key {
                request = request.header(
                    "idempotency-key",
                    id.ok_or_else(|| Error::input("prepare identity is missing"))?,
                );
            }
            let result = async {
                let mut response = request.send().await.map_err(|_| unknown(id))?;
                let status = response.status();
                let retry_after = response
                    .headers()
                    .get(header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(0);
                let generation = response
                    .headers()
                    .get("authority-generation")
                    .and_then(|v| v.to_str().ok())
                    .filter(|v| rescrow_application::uuid_v7_millis(v).is_some())
                    .ok_or_else(|| unknown(id))?
                    .to_owned();
                {
                    let mut expected = self.generation.lock().unwrap();
                    if expected.as_ref().is_some_and(|v| v != &generation) {
                        let mut e = Error::new(
                            Category::Input,
                            Code::Conflict,
                            "endpoint belongs to a different authority generation",
                        );
                        e.transaction_id = id.map(str::to_owned);
                        return Err(e);
                    }
                    *expected = Some(generation.clone());
                }
                // Bound untrusted response bodies while streaming, including chunked replies.
                let mut bytes = Vec::new();
                while let Some(chunk) = response.chunk().await.map_err(|_| unknown(id))? {
                    if bytes.len().saturating_add(chunk.len()) > 16 * 1024 * 1024 {
                        return Err(unknown(id));
                    }
                    bytes.extend_from_slice(&chunk);
                }
                if status.is_success() {
                    if !matches!(status.as_u16(), 200..=202) {
                        return Err(unknown(id));
                    }
                    let envelope: Envelope<T> =
                        canonical::decode(&bytes).map_err(|_| unknown(id))?;
                    if envelope.metadata.authority_generation != generation
                        || !envelope
                            .metadata
                            .durability_profile
                            .satisfies(self.config.required_profile)
                    {
                        return Err(unknown(id));
                    }
                    Ok((Some(envelope), status.as_u16() == 202, retry_after))
                } else {
                    let problem: api::Problem =
                        canonical::decode(&bytes).map_err(|_| unknown(id))?;
                    if problem.status != status.as_u16()
                        || api::status(&problem.code) != (problem.status, problem.retryable)
                        || problem
                            .transaction_id
                            .as_deref()
                            .is_some_and(|v| id.is_some_and(|id| id != v))
                    {
                        return Err(unknown(id));
                    }
                    let retry = problem.retryable && matches!(status.as_u16(), 429 | 503);
                    last = problem.into_error();
                    if !retry {
                        return Err(last.clone());
                    }
                    Ok((None, true, retry_after))
                }
            };
            let delay = match tokio::time::timeout_at(deadline, result).await {
                Ok(Ok((Some(value), false, _))) => return Ok(value),
                Ok(Ok((_, _, after))) => Duration::from_secs(after),
                Ok(Err(e)) if e.category != Category::UnknownOutcome => return Err(e),
                _ => {
                    last = unknown(id);
                    Duration::ZERO
                }
            };
            if attempt + 1 == self.config.max_attempts {
                break;
            }
            let mut random = [0; 8];
            // Jitter entropy failure must not replace an ambiguous outcome with
            // a different error or lose its business identity.
            let _ = getrandom::fill(&mut random);
            let jitter = u64::from_le_bytes(random) % (50u64 << attempt.min(4));
            let delay = delay.max(Duration::from_millis(50 + jitter));
            if delay >= deadline.saturating_duration_since(Instant::now()) {
                break;
            }
            tokio::time::sleep(delay).await;
        }
        if last.transaction_id.is_none() {
            last.transaction_id = id.map(str::to_owned);
        }
        Err(last)
    }
    pub async fn execute(&self, command: dto::Command) -> Result<dto::Output, Error> {
        let deadline = Instant::now() + self.config.overall_timeout;
        let expected_prepare = match &command {
            dto::Command::Prepare(request) => Some(canonical::prepare_digest(request)?),
            _ => None,
        };
        let (method, path, body, id, key, resource, list) = match command {
            dto::Command::CreateResource(v) => (
                Method::PUT,
                format!("/v1/resources/{}", api::path_id(&v.resource_id)?),
                Some(encode(&api::ResourceCreate {
                    lower: v.lower,
                    upper: v.upper,
                    initial: v.initial,
                    unit: v.unit,
                    scale: v.scale,
                })?),
                None,
                false,
                true,
                false,
            ),
            dto::Command::ShowResource { resource_id } => (
                Method::GET,
                format!("/v1/resources/{}", api::path_id(&resource_id)?),
                None,
                None,
                false,
                true,
                false,
            ),
            dto::Command::ListResources => (
                Method::GET,
                "/v1/resources".into(),
                None,
                None,
                false,
                true,
                true,
            ),
            dto::Command::Prepare(v) => {
                if v.authorization.is_some()
                    || rescrow_application::uuid_v7_millis(&v.transaction_id).is_none()
                {
                    return Err(Error::input(
                        "prepare requires a caller-created UUIDv7 and no client authorization facts",
                    ));
                }
                let id = v.transaction_id;
                (
                    Method::POST,
                    "/v1/transactions".into(),
                    Some(encode(&api::Prepare {
                        operations: v.operations,
                        hold_ms: v.hold_ms,
                        context: v.context,
                        namespace: v.namespace,
                    })?),
                    Some(id),
                    true,
                    false,
                    false,
                )
            }
            dto::Command::Commit(v) => (
                Method::POST,
                format!(
                    "/v1/transactions/{}/commit",
                    api::path_id(&v.transaction_id)?
                ),
                Some(encode(&api::Commit { actuals: v.actuals })?),
                Some(v.transaction_id),
                false,
                false,
                false,
            ),
            dto::Command::Abort { transaction_id } => (
                Method::POST,
                format!("/v1/transactions/{}/abort", api::path_id(&transaction_id)?),
                Some(b"{}".to_vec()),
                Some(transaction_id),
                false,
                false,
                false,
            ),
            dto::Command::ShowTransaction { transaction_id } => (
                Method::GET,
                format!("/v1/transactions/{}", api::path_id(&transaction_id)?),
                None,
                Some(transaction_id),
                false,
                false,
                false,
            ),
            dto::Command::ListTransactions => (
                Method::GET,
                "/v1/transactions".into(),
                None,
                None,
                false,
                false,
                true,
            ),
        };
        if list {
            let mut resources = Vec::new();
            let mut transactions = Vec::new();
            let mut next = Some(path.clone());
            let mut previous = None;
            while let Some(current) = next.take() {
                let cursor = if resource {
                    let result: Envelope<Page<api::Resource>> = self
                        .request(method.clone(), &current, None, None, false, deadline)
                        .await?;
                    resources.extend(
                        result
                            .data
                            .items
                            .into_iter()
                            .map(api::Resource::validated)
                            .collect::<Result<Vec<_>, _>>()
                            .map_err(|_| unknown(None))?,
                    );
                    result.data.next_cursor
                } else {
                    let result: Envelope<Page<api::Transaction>> = self
                        .request(method.clone(), &current, None, None, false, deadline)
                        .await?;
                    transactions.extend(
                        result
                            .data
                            .items
                            .into_iter()
                            .map(api::Transaction::validated)
                            .collect::<Result<Vec<_>, _>>()
                            .map_err(|_| unknown(None))?,
                    );
                    result.data.next_cursor
                };
                if let Some(cursor) = cursor {
                    if cursor.len() > 2048
                        || previous.as_ref() == Some(&cursor)
                        || !cursor
                            .bytes()
                            .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c))
                    {
                        return Err(unknown(None));
                    }
                    next = Some(format!("{path}?cursor={cursor}"));
                    previous = Some(cursor);
                }
            }
            Ok(if resource {
                dto::Output::Resources(resources)
            } else {
                dto::Output::Transactions(transactions)
            })
        } else if resource {
            let result: Envelope<api::Resource> = self
                .request(method, &path, body, id.as_deref(), key, deadline)
                .await?;
            if api::path_id(&result.data.spec.resource_id).ok().as_deref()
                != path.rsplit('/').next()
            {
                return Err(unknown(None));
            }
            Ok(dto::Output::Resource(
                result.data.validated().map_err(|_| unknown(None))?,
            ))
        } else {
            let result: Envelope<api::Transaction> = self
                .request(method, &path, body, id.as_deref(), key, deadline)
                .await?;
            if expected_prepare
                .as_ref()
                .is_some_and(|digest| digest != &result.data.request_digest)
            {
                return Err(unknown(id.as_deref()));
            }
            let tx = result
                .data
                .validated()
                .map_err(|_| unknown(id.as_deref()))?;
            if id.as_deref() != Some(tx.request.transaction_id.as_str()) {
                return Err(unknown(id.as_deref()));
            }
            Ok(dto::Output::Transaction(tx))
        }
    }
    pub async fn prepare(&self, preparation: &Preparation) -> Result<dto::Transaction, Error> {
        match self
            .execute(dto::Command::Prepare(
                preparation
                    .request
                    .clone()
                    .into_command(preparation.id.clone()),
            ))
            .await?
        {
            dto::Output::Transaction(value) => Ok(value),
            _ => unreachable!(),
        }
    }
}
fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, Error> {
    serde_json::to_vec(value).map_err(|_| Error::input("request encoding failed"))
}
