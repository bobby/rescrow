#![allow(dead_code)]
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use rescrow_adapters::authority_memory::MemoryAuthority;
use rescrow_application::{
    Application,
    api::{self, Limits},
    authority::TransactionalStore,
    capabilities::DurabilityProfile,
};
use rescrow_http::{
    Api,
    auth::{Issuer, JwtAuthenticator, PinnedAlgorithm, VerificationKey},
    config::Config,
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};

pub struct Harness {
    pub endpoint: String,
    pub client: reqwest::Client,
    pub store: MemoryAuthority,
    pub generation: String,
    pub server: rescrow_server::Server<MemoryAuthority>,
    pub stop: Option<tokio::sync::oneshot::Sender<()>>,
    pub task: tokio::task::JoinHandle<std::io::Result<()>>,
}
pub fn issuer() -> Issuer {
    Issuer {
        issuer: "https://issuer.example".into(),
        audience: "rescrow".into(),
        keys: vec![VerificationKey {
            kid: "test".into(),
            algorithm: PinnedAlgorithm::EdDSA,
            public_key_pem: include_str!("../fixtures/test-verification-key.pem").into(),
        }],
    }
}
pub fn token(tenant: &str, namespace: Option<&str>, scopes: &[&str]) -> String {
    token_claims(
        json!({"iss":"https://issuer.example","aud":"rescrow","sub":"agent","exp":jsonwebtoken::get_current_timestamp()+3600,"tenant":tenant,"namespace":namespace,"credential_id":"agent-credential","scopes":scopes,"resources":[{"resource_prefix":"*","debit":true,"credit":true,"max_amount":i128::MAX.to_string()}],"max_hold_ms":null}),
    )
}
pub fn token_claims(claims: Value) -> String {
    let mut header = Header::new(Algorithm::EdDSA);
    header.kid = Some("test".into());
    jsonwebtoken::encode(
        &header,
        &claims,
        &EncodingKey::from_ed_pem(include_bytes!("../fixtures/test-signing-key.pem")).unwrap(),
    )
    .unwrap()
}
pub fn admin(tenant: &str) -> String {
    token(
        tenant,
        Some("workflow"),
        &[
            "resource:create",
            "transaction:prepare",
            "transaction:commit",
            "transaction:abort",
            "read",
        ],
    )
}
pub fn config(limits: Limits) -> Config {
    Config {
        serving_region: "local".into(),
        deployment_cell: "test".into(),
        required_profile: DurabilityProfile::Simulation,
        cursor_key: "a5".repeat(32),
        limits,
    }
}
impl Harness {
    pub async fn start(limits: Limits) -> Self {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let store = MemoryAuthority::default();
        store.initialize().unwrap();
        let api = Api::new(
            Application::new(store.clone()),
            config(limits),
            Arc::new(JwtAuthenticator::new(vec![issuer()]).unwrap()),
        )
        .unwrap();
        let server = api.server().clone();
        let generation = store.identity().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (stop, shutdown) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(api.serve(listener, async {
            let _ = shutdown.await;
        }));
        Self {
            endpoint,
            client: reqwest::Client::new(),
            store,
            generation,
            server,
            stop: Some(stop),
            task,
        }
    }
    pub fn put(&self, path: &str, token: &str, body: Value) -> reqwest::RequestBuilder {
        self.client
            .put(format!("{}{path}", self.endpoint))
            .bearer_auth(token)
            .json(&body)
    }
    pub fn post(&self, path: &str, token: &str, body: Value) -> reqwest::RequestBuilder {
        self.client
            .post(format!("{}{path}", self.endpoint))
            .bearer_auth(token)
            .json(&body)
    }
    pub fn get(&self, path: &str, token: &str) -> reqwest::RequestBuilder {
        self.client
            .get(format!("{}{path}", self.endpoint))
            .bearer_auth(token)
    }
    pub async fn create(&self, id: &str, token: &str) {
        let id = api::path_id(id).unwrap();
        let response = self
            .put(&format!("/v1/resources/{id}"), token, spec())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 201, "{}", response.text().await.unwrap());
    }
    pub async fn finish(mut self) {
        let _ = self.stop.take().unwrap().send(());
        self.task.await.unwrap().unwrap();
    }
}
pub fn spec() -> Value {
    json!({"lower":"0","upper":"100","initial":"60","unit":"credits","scale":0})
}
pub fn prepare(delta: &str) -> Value {
    json!({"operations":[{"operation_id":"op","resource_id":"credits","delta":delta}],"hold_ms":null,"context":BTreeMap::<String,String>::new()})
}
pub fn new_id() -> String {
    rescrow_application::new_id().unwrap()
}
pub async fn problem(
    response: reqwest::Response,
    status: u16,
    code: rescrow_application::Code,
) -> api::Problem {
    assert_eq!(response.status().as_u16(), status);
    assert_eq!(
        response.headers()["content-type"],
        "application/problem+json"
    );
    let request_id = response.headers()["x-request-id"]
        .to_str()
        .unwrap()
        .to_string();
    let problem: api::Problem = response.json().await.unwrap();
    assert_eq!(problem.code, code);
    assert_eq!(problem.request_id, request_id);
    assert_eq!(problem.status, status);
    problem
}
