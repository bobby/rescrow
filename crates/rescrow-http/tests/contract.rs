mod support;
use rescrow_application::{
    Category, Code, Error,
    api::{Limits, Problem},
};
use serde_json::{Value, json};
use support::*;

async fn capture(
    cases: &mut Vec<Value>,
    operation: &str,
    request: Option<Value>,
    response: reqwest::Response,
) {
    let status = response.status().as_u16();
    let headers = response
        .headers()
        .iter()
        .map(|(k, v)| (k.to_string(), json!(v.to_str().unwrap())))
        .collect::<serde_json::Map<String, Value>>();
    cases.push(json!({"operation":operation,"request":request,"status":status,"headers":headers,"response":response.json::<Value>().await.unwrap()}));
}
#[tokio::test]
async fn actual_socket_contract_and_exhaustive_problem_table() {
    let document: Value =
        serde_json::from_str(include_str!("../../../openapi/rescrow-v1.json")).unwrap();
    for (code, table) in document["x-problem-codes"].as_object().unwrap() {
        let code: Code = serde_json::from_value(json!(code)).unwrap();
        let problem = Problem::from_error(
            Error::new(Category::Input, code, "test"),
            "contract-test".into(),
        );
        assert_eq!(json!(problem.status), table["status"]);
        assert_eq!(json!(problem.retryable), table["retryable"]);
        assert_eq!(json!(problem.problem_type), table["type"]);
    }
    let h = Harness::start(Limits::default()).await;
    let token = admin("contract");
    let mut cases = Vec::new();
    capture(
        &mut cases,
        "create_resource",
        Some(spec()),
        h.put("/admin/v1/resources/credits", &token, spec())
            .send()
            .await
            .unwrap(),
    )
    .await;
    capture(
        &mut cases,
        "create_resource",
        Some(spec()),
        h.put("/admin/v1/resources/credits", &token, spec())
            .send()
            .await
            .unwrap(),
    )
    .await;
    capture(
        &mut cases,
        "get_resource",
        None,
        h.get("/v1/resources/credits", &token).send().await.unwrap(),
    )
    .await;
    capture(
        &mut cases,
        "list_resources",
        None,
        h.get("/v1/resources?limit=1", &token).send().await.unwrap(),
    )
    .await;
    let id = new_id();
    let body = prepare("-40");
    for _ in 0..2 {
        capture(
            &mut cases,
            "prepare",
            Some(body.clone()),
            h.post("/v1/reservations", &token, body.clone())
                .header("idempotency-key", &id)
                .send()
                .await
                .unwrap(),
        )
        .await;
    }
    capture(
        &mut cases,
        "get_reservation",
        None,
        h.get(&format!("/v1/reservations/{id}"), &token)
            .send()
            .await
            .unwrap(),
    )
    .await;
    let actuals = json!({"actuals":[{"operation_id":"op","delta":"-25"}]});
    capture(
        &mut cases,
        "commit",
        Some(actuals.clone()),
        h.post(&format!("/v1/reservations/{id}/commit"), &token, actuals)
            .send()
            .await
            .unwrap(),
    )
    .await;
    capture(
        &mut cases,
        "abort",
        Some(json!({})),
        h.post(&format!("/v1/reservations/{id}/abort"), &token, json!({}))
            .send()
            .await
            .unwrap(),
    )
    .await;
    capture(
        &mut cases,
        "prepare",
        Some(prepare("-40")),
        h.post("/v1/reservations", &token, prepare("-40"))
            .header("idempotency-key", new_id())
            .send()
            .await
            .unwrap(),
    )
    .await;
    let id = new_id();
    capture(
        &mut cases,
        "prepare",
        Some(prepare("-1")),
        h.post("/v1/reservations", &token, prepare("-1"))
            .header("idempotency-key", &id)
            .send()
            .await
            .unwrap(),
    )
    .await;
    capture(
        &mut cases,
        "abort",
        Some(json!({})),
        h.post(&format!("/v1/reservations/{id}/abort"), &token, json!({}))
            .send()
            .await
            .unwrap(),
    )
    .await;
    capture(
        &mut cases,
        "list_reservations",
        None,
        h.get("/v1/reservations?state=aborted&limit=1", &token)
            .send()
            .await
            .unwrap(),
    )
    .await;
    capture(
        &mut cases,
        "get_resource",
        None,
        h.client
            .get(format!("{}/v1/resources/credits", h.endpoint))
            .send()
            .await
            .unwrap(),
    )
    .await;
    if let Ok(path) = std::env::var("RESCROW_CONTRACT_CAPTURE") {
        std::fs::write(path, serde_json::to_vec_pretty(&cases).unwrap()).unwrap();
    }
    h.finish().await;
}
