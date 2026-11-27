mod support;
use axum::{
    Json,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::IntoResponse,
};
use rescrow_application::{
    Application, Category, Code, Error,
    api::{Limits, Problem},
    dto,
};
use rescrow_client::{Client, Config};
use rescrow_http::{Api, auth::JwtAuthenticator};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use support::*;
#[tokio::test]
async fn lost_http_response_retries_identical_key_and_bytes_without_reserving_twice() {
    let h = Harness::start(Limits::default()).await;
    let token = admin("tenant");
    h.create("credits", &token).await;
    let api = Api::new(
        Application::new(h.store.clone()),
        config(Limits::default()),
        Arc::new(JwtAuthenticator::new(vec![issuer()]).unwrap()),
    )
    .unwrap();
    let records = Arc::new(Mutex::new(Vec::new()));
    let once = Arc::new(AtomicBool::new(true));
    let router = api.router().layer(middleware::from_fn({
        let records = records.clone();
        move |request: Request<Body>, next: Next| {
            let records = records.clone();
            let once = once.clone();
            async move {
                let (parts, body) = request.into_parts();
                let bytes = to_bytes(body, 1024 * 1024).await.unwrap();
                let key = parts
                    .headers
                    .get("idempotency-key")
                    .map(|v| v.to_str().unwrap().to_owned());
                records.lock().unwrap().push((key.clone(), bytes.to_vec()));
                let response = next
                    .run(Request::from_parts(parts, Body::from(bytes)))
                    .await;
                if once.swap(false, Ordering::SeqCst) {
                    assert_eq!(response.status(), 201);
                    let mut error = Error::new(
                        Category::UnknownOutcome,
                        Code::UnknownOutcome,
                        "injected lost response",
                    );
                    error.reservation_id = key;
                    let mut lost = (
                        StatusCode::SERVICE_UNAVAILABLE,
                        Json(Problem::from_error(error, "lost-response".into())),
                    )
                        .into_response();
                    *lost.headers_mut() = response.headers().clone();
                    lost.headers_mut()
                        .insert("content-type", "application/problem+json".parse().unwrap());
                    lost
                } else {
                    response
                }
            }
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (stop, shutdown) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = shutdown.await;
            })
            .await
            .unwrap();
    });
    let mut config = Config::new(endpoint, token);
    config.authority_generation = Some(h.generation.clone());
    config.overall_timeout = Duration::from_secs(3);
    config.attempt_timeout = Duration::from_secs(1);
    let client = Client::new(config).unwrap();
    let id = new_id();
    let request:dto::Prepare=serde_json::from_value(serde_json::json!({"reservation_id":id,"operations":[{"operation_id":"op","resource_id":"credits","delta":"-40"}],"hold_ms":null})).unwrap();
    client
        .execute(dto::Command::Prepare(request))
        .await
        .unwrap();
    {
        let records = records.lock().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0], records[1]);
        assert_eq!(records[0].0.as_deref(), Some(id.as_str()));
    }
    let output = client
        .execute(dto::Command::ShowResource {
            resource_id: "credits".into(),
        })
        .await
        .unwrap();
    let dto::Output::Resource(r) = output else {
        panic!()
    };
    assert_eq!(r.reserved_down, "40");
    stop.send(()).unwrap();
    task.await.unwrap();
    h.finish().await;
}
#[tokio::test]
async fn explicit_fallback_requires_the_same_actual_authority() {
    let h = Harness::start(Limits::default()).await;
    let token = admin("tenant");
    h.create("credits", &token).await;
    let unavailable = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let dead = format!("http://{}", unavailable.local_addr().unwrap());
    drop(unavailable);
    let mut config = Config::new(dead, token.clone());
    config.endpoints.push(h.endpoint.clone());
    assert!(Client::new(config).is_err());
    let mut config = Config::new("http://127.0.0.1:1".into(), token.clone());
    config.endpoints.push(h.endpoint.clone());
    config.authority_generation = Some(h.generation.clone());
    config.overall_timeout = Duration::from_secs(3);
    config.attempt_timeout = Duration::from_secs(1);
    Client::new(config)
        .unwrap()
        .execute(dto::Command::ShowResource {
            resource_id: "credits".into(),
        })
        .await
        .unwrap();
    let other = Harness::start(Limits::default()).await;
    let mut config = Config::new(other.endpoint.clone(), token);
    config.authority_generation = Some(h.generation.clone());
    let client = Client::new(config).unwrap();
    let error = client
        .execute(dto::Command::CreateResource(dto::ResourceSpec {
            resource_id: "credits".into(),
            lower: "0".into(),
            upper: Some("100".into()),
            initial: "60".into(),
            unit: "credits".into(),
            scale: 0,
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::Conflict);
    assert!(
        Application::new(other.store.clone())
            .scoped(
                rescrow_application::authority::Scope::new(
                    rescrow_application::authority::TenantId::new("tenant").unwrap(),
                    "workflow"
                )
                .unwrap()
            )
            .resources()
            .unwrap()
            .is_empty()
    );
    other.finish().await;
    h.finish().await;
}
