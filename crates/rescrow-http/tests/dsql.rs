#![cfg(feature = "dsql")]
mod support;
use axum::{
    Json,
    body::Body,
    extract::Request,
    http::StatusCode,
    middleware::{self, Next},
    response::IntoResponse,
};
use rescrow_application::{
    Application, Category, Code, Error,
    api::{Limits, Problem},
    authority::TransactionalStore,
    dto,
};
use rescrow_client::{Client, Config};
use rescrow_dsql::DsqlStore;
use rescrow_http::{Api, auth::JwtAuthenticator};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use support::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an explicit PostgreSQL simulation or real DSQL target"]
async fn authenticated_http_lost_prepare_response_replays_one_durable_hold() {
    let store = tokio::task::spawn_blocking(|| {
        let authority = format!(
            "http-qualification-{}",
            rescrow_application::new_id().unwrap()
        );
        let store = if let Ok(path) = std::env::var("RESCROW_DSQL_CONFIG") {
            let mut config: rescrow_dsql::Config =
                rescrow_application::canonical::decode(&std::fs::read(path).unwrap()).unwrap();
            config.authority = authority;
            DsqlStore::connect(config).unwrap()
        } else {
            DsqlStore::postgres_simulation(
                &std::env::var("RESCROW_POSTGRES_SIMULATION_URL")
                    .expect("explicit test target required"),
                &authority,
            )
            .unwrap()
        };
        store.install_schema().unwrap();
        store.initialize().unwrap();
        store
    })
    .await
    .unwrap();
    let (api, generation) = tokio::task::spawn_blocking(move || {
        let generation = store.identity().unwrap();
        let mut config = config(Limits::default());
        if std::env::var_os("RESCROW_DSQL_CONFIG").is_some() {
            config.required_profile =
                rescrow_application::capabilities::DurabilityProfile::EnterpriseMr;
        }
        let mut invalid = config.clone();
        invalid.limits.operation_count = rescrow_dsql::MAX_PARTICIPANTS + 1;
        assert!(
            Api::new(
                Application::new(store.clone()),
                invalid,
                Arc::new(JwtAuthenticator::new(vec![issuer()]).unwrap()),
            )
            .is_err()
        );
        (
            Api::new(
                Application::new(store),
                config,
                Arc::new(JwtAuthenticator::new(vec![issuer()]).unwrap()),
            )
            .unwrap(),
            generation,
        )
    })
    .await
    .unwrap();
    let once = Arc::new(AtomicBool::new(true));
    let router = api.router().layer(middleware::from_fn(
        move |request: Request<Body>, next: Next| {
            let once = once.clone();
            async move {
                let prepare = request.uri().path() == "/v1/reservations"
                    && request.method() == axum::http::Method::POST;
                let id = request
                    .headers()
                    .get("idempotency-key")
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_owned);
                let response = next.run(request).await;
                if prepare && once.swap(false, Ordering::SeqCst) {
                    assert_eq!(response.status(), StatusCode::CREATED);
                    let mut error = Error::new(
                        Category::UnknownOutcome,
                        Code::UnknownOutcome,
                        "injected loss after durable publication",
                    );
                    error.reservation_id = id;
                    let mut lost = (
                        StatusCode::SERVICE_UNAVAILABLE,
                        Json(Problem::from_error(error, "dsql-lost-response".into())),
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
        },
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = Config::new(
        format!("http://{}", listener.local_addr().unwrap()),
        admin("tenant"),
    );
    config.authority_generation = Some(generation);
    if std::env::var_os("RESCROW_DSQL_CONFIG").is_some() {
        config.required_profile =
            rescrow_application::capabilities::DurabilityProfile::EnterpriseMr;
    }
    let client = Client::new(config).unwrap();
    let (stop, shutdown) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = shutdown.await;
            })
            .await
            .unwrap();
    });
    client
        .execute(dto::Command::CreateResource(dto::ResourceSpec {
            resource_id: "credits".into(),
            lower: "0".into(),
            upper: Some("100".into()),
            initial: "60".into(),
            unit: "u".into(),
            scale: 0,
        }))
        .await
        .unwrap();
    let id = rescrow_application::new_id().unwrap();
    let prepare:dto::Prepare=serde_json::from_value(serde_json::json!({"reservation_id":id,"operations":[{"operation_id":"op","resource_id":"credits","delta":"-40"}],"hold_ms":null})).unwrap();
    client
        .execute(dto::Command::Prepare(prepare))
        .await
        .unwrap();
    let dto::Output::Resource(resource) = client
        .execute(dto::Command::ShowResource {
            resource_id: "credits".into(),
        })
        .await
        .unwrap()
    else {
        panic!("wrong resource output")
    };
    assert_eq!(resource.reserved_down, "40");
    assert_eq!(client.measurements().retries, 1);
    let actuals = Some(vec![dto::Actual {
        operation_id: "op".into(),
        delta: "-25".into(),
    }]);
    let commit = dto::Command::Commit(dto::Commit {
        reservation_id: id,
        actuals,
    });
    assert_eq!(
        client.execute(commit.clone()).await.unwrap(),
        client.execute(commit).await.unwrap()
    );
    let dto::Output::Resource(resource) = client
        .execute(dto::Command::ShowResource {
            resource_id: "credits".into(),
        })
        .await
        .unwrap()
    else {
        panic!("wrong resource output")
    };
    assert_eq!(resource.quantity, "35");
    assert_eq!(resource.reserved_down, "0");
    stop.send(()).unwrap();
    task.await.unwrap();
    api.server().drain().await;
}
