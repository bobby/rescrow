mod support;
use rescrow_application::{
    Application, Code,
    api::{self, Envelope, Limits, Page},
};
use serde_json::json;
use support::*;
#[tokio::test]
async fn socket_workflow_metadata_partial_decision_replay_and_isolation() {
    let h = Harness::start(Limits::default()).await;
    let token = admin("tenant-a");
    h.create("credits", &token).await;
    let original = h
        .put("/admin/v1/resources/credits", &token, spec())
        .send()
        .await
        .unwrap();
    assert_eq!(original.status(), 200);
    let id = new_id();
    let request = prepare("-40");
    let first = h
        .post("/v1/reservations", &token, request.clone())
        .header("idempotency-key", &id)
        .header("x-request-id", "test-request")
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), 201);
    assert_eq!(
        first.headers()["location"],
        format!("/v1/reservations/{id}")
    );
    assert_eq!(first.headers()["authority-generation"], h.generation);
    assert_eq!(first.headers()["durability-profile"], "simulation");
    let first: Envelope<api::Reservation> = first.json().await.unwrap();
    assert_eq!(first.metadata.request_id, "test-request");
    assert!(!first.data.processing);
    assert_eq!(first.data.request_digest.len(), 71);
    let replay = h
        .post("/v1/reservations", &token, request.clone())
        .header("idempotency-key", &id)
        .send()
        .await
        .unwrap();
    assert_eq!(replay.status(), 200);
    let replay: Envelope<api::Reservation> = replay.json().await.unwrap();
    assert_eq!(replay.data, first.data);
    let conflict = h
        .post("/v1/reservations", &token, prepare("-39"))
        .header("idempotency-key", &id)
        .send()
        .await
        .unwrap();
    problem(conflict, 409, Code::Conflict).await;
    let commit = h
        .post(
            &format!("/v1/reservations/{id}/commit"),
            &token,
            json!({"actuals":[{"operation_id":"op","delta":"-25"}]}),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(commit.status(), 200);
    let committed: Envelope<api::Reservation> = commit.json().await.unwrap();
    assert!(committed.data.actuals_digest.is_some());
    let replay = h
        .post(
            &format!("/v1/reservations/{id}/commit"),
            &token,
            json!({"actuals":[{"operation_id":"op","delta":"-25"}]}),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(replay.status(), 200);
    problem(
        h.post(
            &format!("/v1/reservations/{id}/commit"),
            &token,
            json!({"actuals":[{"operation_id":"op","delta":"-24"}]}),
        )
        .send()
        .await
        .unwrap(),
        409,
        Code::Conflict,
    )
    .await;
    problem(
        h.post(&format!("/v1/reservations/{id}/abort"), &token, json!({}))
            .send()
            .await
            .unwrap(),
        409,
        Code::Conflict,
    )
    .await;
    let resource: Envelope<api::Resource> = h
        .get("/v1/resources/credits", &token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resource.data.observation.quantity, "35");
    assert!(resource.data.observation.holds.is_empty());
    assert_eq!(
        resource.data.observation.consistency,
        api::Consistency::Sampled
    );
    let other = admin("tenant-b");
    problem(
        h.get("/v1/resources/credits", &other).send().await.unwrap(),
        404,
        Code::NotFound,
    )
    .await;
    h.create("credits", &other).await;
    let resource: Envelope<api::Resource> = h
        .get("/v1/resources/credits", &other)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resource.data.observation.quantity, "60");
    problem(
        h.get(&format!("/v1/reservations/{id}"), &other)
            .send()
            .await
            .unwrap(),
        404,
        Code::NotFound,
    )
    .await;
    let list: Envelope<Page<api::Reservation>> = h
        .get("/v1/reservations", &token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(list.data.items.len(), 1);
    assert_eq!(
        Application::new(h.store.clone())
            .scoped(
                rescrow_application::authority::Scope::new(
                    rescrow_application::authority::TenantId::new("tenant-a").unwrap(),
                    "workflow"
                )
                .unwrap()
            )
            .resource("credits")
            .unwrap()
            .quantity,
        "35"
    );
    h.finish().await;
}
#[tokio::test]
async fn malformed_duplicate_expired_and_mismatched_authority_requests_have_problems() {
    let h = Harness::start(Limits::default()).await;
    let token = admin("tenant");
    h.create("credits", &token).await;
    let request = prepare("-1");
    let response = h
        .client
        .get(format!("{}/v1/resources/credits", h.endpoint))
        .send()
        .await
        .unwrap();
    problem(response, 401, Code::Unauthenticated).await;
    problem(
        h.post("/v1/reservations", &token, request.clone())
            .send()
            .await
            .unwrap(),
        422,
        Code::InvalidInput,
    )
    .await;
    let id = new_id();
    let response = h
        .client
        .post(format!("{}/v1/reservations", h.endpoint))
        .bearer_auth(&token)
        .header("content-type", "application/json")
        .header("idempotency-key", &id)
        .body(r#"{"operations":[],"operations":[]}"#)
        .send()
        .await
        .unwrap();
    problem(response, 422, Code::InvalidInput).await;
    let expired =
        rescrow_application::uuid_v7(rescrow_application::system_clock() - 25 * 60 * 60 * 1000, 7);
    let response = h
        .post("/v1/reservations", &token, request.clone())
        .header("idempotency-key", expired)
        .send()
        .await
        .unwrap();
    problem(response, 422, Code::Expired).await;
    let response = h
        .post("/v1/reservations", &token, request.clone())
        .header("idempotency-key", &id)
        .header("require-authority-generation", new_id())
        .send()
        .await
        .unwrap();
    problem(response, 409, Code::Conflict).await;
    let response = h
        .post("/v1/reservations", &token, request)
        .header("idempotency-key", &id)
        .header("require-durability-profile", "local")
        .send()
        .await
        .unwrap();
    problem(response, 422, Code::InvalidInput).await;
    let response = h
        .client
        .delete(format!("{}/v1/resources/credits", h.endpoint))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    problem(response, 405, Code::MethodNotAllowed).await;
    let response = h.get("/v1/missing", &token).send().await.unwrap();
    problem(response, 404, Code::NotFound).await;
    h.finish().await;
}
#[tokio::test]
async fn concurrent_identical_preparations_report_exactly_one_creation() {
    let h = Harness::start(Limits::default()).await;
    let token = admin("tenant");
    h.create("credits", &token).await;
    let id = new_id();
    let a = h
        .post("/v1/reservations", &token, prepare("-40"))
        .header("idempotency-key", &id)
        .send();
    let b = h
        .post("/v1/reservations", &token, prepare("-40"))
        .header("idempotency-key", &id)
        .send();
    let (a, b) = tokio::join!(a, b);
    let mut statuses = [a.unwrap().status().as_u16(), b.unwrap().status().as_u16()];
    statuses.sort();
    assert_eq!(statuses, [200, 201]);
    let response = h.get("/v1/resources/credits", &token).send().await.unwrap();
    let data: Envelope<api::Resource> = response.json().await.unwrap();
    assert_eq!(data.data.observation.reserved_down, "40");
    let response = h
        .post(&format!("/v1/reservations/{id}/abort"), &token, json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let response = h
        .post(&format!("/v1/reservations/{id}/abort"), &token, json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    h.finish().await;
}

#[tokio::test]
async fn every_endpoint_rechecks_scopes_and_resource_grants() {
    let h = Harness::start(Limits::default()).await;
    let admin = admin("tenant");
    h.create("credits", &admin).await;
    let id = new_id();
    assert_eq!(
        h.post("/v1/reservations", &admin, prepare("-10"))
            .header("idempotency-key", &id)
            .send()
            .await
            .unwrap()
            .status(),
        201
    );
    let endpoints = [
        (
            "PUT",
            "/admin/v1/resources/other".to_owned(),
            "resource:create",
            spec(),
        ),
        (
            "POST",
            "/v1/reservations".into(),
            "reservation:prepare",
            prepare("-10"),
        ),
        (
            "POST",
            format!("/v1/reservations/{id}/commit"),
            "reservation:commit",
            json!({}),
        ),
        (
            "POST",
            format!("/v1/reservations/{id}/abort"),
            "reservation:abort",
            json!({}),
        ),
        ("GET", "/v1/resources".into(), "read", json!({})),
        ("GET", "/v1/resources/credits".into(), "read", json!({})),
        ("GET", "/v1/reservations".into(), "read", json!({})),
        ("GET", format!("/v1/reservations/{id}"), "read", json!({})),
    ];
    let scopes = [
        "resource:create",
        "reservation:prepare",
        "reservation:commit",
        "reservation:abort",
        "read",
    ];
    for (method, path, scope, body) in endpoints {
        for authenticated in [false, true] {
            let mut request = h
                .client
                .request(method.parse().unwrap(), format!("{}{path}", h.endpoint));
            if authenticated {
                let allowed: Vec<_> = scopes.iter().copied().filter(|s| *s != scope).collect();
                request = request.bearer_auth(token("tenant", Some("workflow"), &allowed));
            }
            if method != "GET" {
                request = request.json(&body).header("idempotency-key", &id);
            }
            problem(
                request.send().await.unwrap(),
                if authenticated { 403 } else { 401 },
                if authenticated {
                    Code::Forbidden
                } else {
                    Code::Unauthenticated
                },
            )
            .await;
        }
    }
    let narrow = token_claims(
        json!({"iss":"https://issuer.example","aud":"rescrow","sub":"agent","exp":jsonwebtoken::get_current_timestamp()+3600,"tenant":"tenant","namespace":"workflow","credential_id":"agent-credential","scopes":scopes,"resources":[{"resource_prefix":"credits","debit":true,"credit":false,"max_amount":"9"}]}),
    );
    problem(
        h.post("/v1/reservations", &narrow, prepare("-10"))
            .header("idempotency-key", &id)
            .send()
            .await
            .unwrap(),
        403,
        Code::Forbidden,
    )
    .await;
    problem(
        h.post("/v1/reservations", &narrow, prepare("1"))
            .header("idempotency-key", new_id())
            .send()
            .await
            .unwrap(),
        403,
        Code::Forbidden,
    )
    .await;
    problem(
        h.put("/admin/v1/resources/other", &narrow, spec())
            .send()
            .await
            .unwrap(),
        403,
        Code::Forbidden,
    )
    .await;
    problem(
        h.get("/v1/resources/other", &narrow).send().await.unwrap(),
        404,
        Code::NotFound,
    )
    .await;
    let other_namespace = token("tenant", Some("other"), &scopes);
    problem(
        h.get(&format!("/v1/reservations/{id}"), &other_namespace)
            .send()
            .await
            .unwrap(),
        404,
        Code::NotFound,
    )
    .await;
    let mut foreign = prepare("-1");
    foreign["namespace"] = json!("other");
    problem(
        h.post("/v1/reservations", &admin, foreign)
            .header("idempotency-key", new_id())
            .send()
            .await
            .unwrap(),
        403,
        Code::Forbidden,
    )
    .await;
    h.finish().await;
}

#[tokio::test]
async fn pagination_is_bound_to_filters_tenant_and_policy_and_dot_ids_are_addressable() {
    let h = Harness::start(Limits::default()).await;
    let token = admin("tenant");
    for id in ["r1", "r2", "r3", ".", ".."] {
        h.create(id, &token).await;
    }
    for (id, path) in [(".", "~Lg"), ("..", "~Li4")] {
        let value: Envelope<api::Resource> = h
            .get(&format!("/v1/resources/{path}"), &token)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(value.data.spec.resource_id, id);
        assert_eq!(value.links["self"], format!("/v1/resources/{path}"));
    }
    let first: Envelope<Page<api::Resource>> = h
        .get("/v1/resources?prefix=r&limit=2", &token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(first.data.items.len(), 2);
    let cursor = first.data.next_cursor.unwrap();
    let last: Envelope<Page<api::Resource>> = h
        .get(&first.links["next"], &token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(last.data.items[0].spec.resource_id, "r3");
    assert!(last.data.next_cursor.is_none());
    for path in [
        format!("/v1/resources?cursor={cursor}"),
        format!("/v1/resources?prefix=r&cursor={cursor}x"),
        "/v1/resources?limit=0".into(),
        "/v1/resources?limit=1&limit=2".into(),
    ] {
        assert_eq!(h.get(&path, &token).send().await.unwrap().status(), 422);
    }
    problem(
        h.get(
            &format!("/v1/resources?prefix=r&cursor={cursor}"),
            &admin("other"),
        )
        .send()
        .await
        .unwrap(),
        422,
        Code::InvalidInput,
    )
    .await;
    h.finish().await;
}

#[tokio::test]
async fn configured_limits_shed_admission_while_decisions_and_recovery_progress() {
    let limits = Limits {
        body_bytes: 512,
        operation_count: 1,
        context_bytes: 4,
        numeric_magnitude: "100".into(),
        pending: rescrow_application::access::PendingLimits {
            tenant: 1,
            credential: 1,
        },
        ..Limits::default()
    };
    let h = Harness::start(limits).await;
    let token = admin("tenant");
    h.create("credits", &token).await;
    let mut value = prepare("-1");
    value["context"] = json!({"k":"1234"});
    problem(
        h.post("/v1/reservations", &token, value)
            .header("idempotency-key", new_id())
            .send()
            .await
            .unwrap(),
        422,
        Code::LimitExceeded,
    )
    .await;
    let mut value = prepare("-1");
    value["operations"]
        .as_array_mut()
        .unwrap()
        .push(json!({"operation_id":"b","resource_id":"b","delta":"-1"}));
    problem(
        h.post("/v1/reservations", &token, value)
            .header("idempotency-key", new_id())
            .send()
            .await
            .unwrap(),
        422,
        Code::LimitExceeded,
    )
    .await;
    problem(
        h.post("/v1/reservations", &token, prepare("-101"))
            .header("idempotency-key", new_id())
            .send()
            .await
            .unwrap(),
        422,
        Code::LimitExceeded,
    )
    .await;
    let id = new_id();
    let mut boundary = prepare("-1");
    boundary["context"] = json!({"k":"123"});
    assert_eq!(
        h.post("/v1/reservations", &token, boundary)
            .header("idempotency-key", &id)
            .send()
            .await
            .unwrap()
            .status(),
        201
    );
    let blocked = new_id();
    let response = h
        .post("/v1/reservations", &token, prepare("-1"))
        .header("idempotency-key", &blocked)
        .send()
        .await
        .unwrap();
    assert_eq!(response.headers()["retry-after"], "1");
    problem(response, 429, Code::Throttled).await;
    assert_eq!(
        h.get(&format!("/v1/reservations/{id}"), &token)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        h.post(&format!("/v1/reservations/{id}/abort"), &token, json!({}))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        h.post("/v1/reservations", &token, prepare("-1"))
            .header("idempotency-key", &blocked)
            .send()
            .await
            .unwrap()
            .status(),
        201
    );
    let response = h
        .client
        .post(format!("{}/v1/reservations", h.endpoint))
        .bearer_auth(&token)
        .header("content-type", "application/json")
        .header("idempotency-key", new_id())
        .body(" ".repeat(513))
        .send()
        .await
        .unwrap();
    problem(response, 422, Code::LimitExceeded).await;
    h.finish().await;
    let mut limits = Limits::default();
    limits.tenant_admission.burst = 1;
    limits.tenant_admission.requests_per_second = 1;
    let h = Harness::start(limits).await;
    let token = admin("tenant");
    h.create("credits", &token).await;
    problem(
        h.post("/v1/reservations", &token, prepare("-1"))
            .header("idempotency-key", new_id())
            .send()
            .await
            .unwrap(),
        429,
        Code::Throttled,
    )
    .await;
    assert_eq!(
        h.get("/v1/resources/credits", &token)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    h.finish().await;
}

#[tokio::test]
async fn reservation_names_and_admin_creation_replace_the_previous_interface() {
    let h = Harness::start(Limits::default()).await;
    let token = admin("tenant");
    let created = h
        .put("/admin/v1/resources/credits", &token, spec())
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    assert_eq!(created.headers()["location"], "/v1/resources/credits");
    let created: Envelope<api::Resource> = created.json().await.unwrap();
    assert_eq!(created.data.spec.resource_id, "credits");
    assert_eq!(created.links["self"], "/v1/resources/credits");
    assert_eq!(
        h.get(&created.links["self"], &token)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    problem(
        h.put("/v1/resources/credits", &token, spec())
            .send()
            .await
            .unwrap(),
        405,
        Code::MethodNotAllowed,
    )
    .await;
    let reader = support::token("tenant", Some("workflow"), &["read"]);
    problem(
        h.put("/admin/v1/resources/forbidden", &reader, spec())
            .send()
            .await
            .unwrap(),
        403,
        Code::Forbidden,
    )
    .await;
    let id = new_id();
    for (method, path) in [
        ("POST", "/v1/transactions".to_string()),
        ("GET", "/v1/transactions".to_string()),
        ("GET", format!("/v1/transactions/{id}")),
        ("POST", format!("/v1/transactions/{id}/commit")),
        ("POST", format!("/v1/transactions/{id}/abort")),
    ] {
        let mut request = h
            .client
            .request(method.parse().unwrap(), format!("{}{path}", h.endpoint))
            .bearer_auth(&token);
        if method == "POST" {
            request = request.header("idempotency-key", &id).json(&prepare("-1"));
        }
        problem(request.send().await.unwrap(), 404, Code::NotFound).await;
    }
    // Retired scope names are invalid claims, never aliases for the new scopes.
    let old_scope = support::token("tenant", Some("workflow"), &["transaction:prepare"]);
    problem(
        h.post("/v1/reservations", &old_scope, prepare("-1"))
            .header("idempotency-key", &id)
            .send()
            .await
            .unwrap(),
        401,
        Code::Unauthenticated,
    )
    .await;
    // Body identifiers are never accepted in place of the idempotency header.
    let mut old_field = prepare("-1");
    old_field["transaction_id"] = json!(&id);
    problem(
        h.post("/v1/reservations", &token, old_field)
            .header("idempotency-key", &id)
            .send()
            .await
            .unwrap(),
        422,
        Code::InvalidInput,
    )
    .await;
    problem(
        h.get(&format!("/v1/reservations/{id}"), &token)
            .send()
            .await
            .unwrap(),
        404,
        Code::NotFound,
    )
    .await;
    let response = h
        .post("/v1/reservations", &token, prepare("-1"))
        .header("idempotency-key", &id)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 201);
    assert_eq!(
        response.headers()["location"],
        format!("/v1/reservations/{id}")
    );
    let value: serde_json::Value = response.json().await.unwrap();
    assert_eq!(
        value["data"]["reservation"]["request"]["reservation_id"],
        id
    );
    assert!(value["data"].get("transaction").is_none());
    assert!(
        value["data"]["reservation"]["request"]
            .get("transaction_id")
            .is_none()
    );
    let response = h
        .post("/v1/reservations", &token, prepare("-100"))
        .header("idempotency-key", new_id())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 409);
    let value: serde_json::Value = response.json().await.unwrap();
    assert!(value["reservation_id"].is_string());
    assert!(value.get("transaction_id").is_none());
    h.finish().await;
}
