use rescrow_application::{api, canonical, dto, uuid_v7};
#[test]
fn duplicate_json_keys_are_rejected_at_every_depth() {
    for input in [
        r#"{"operations":[],"operations":[]}"#,
        r#"{"operations":[],"context":{"workflow":"a","workflow":"b"}}"#,
        r#"{"operations":[{"operation_id":"a","operation_id":"b","resource_id":"r","delta":"-1"}]}"#,
    ] {
        assert!(
            canonical::decode::<api::Prepare>(input.as_bytes())
                .unwrap_err()
                .message
                .contains("duplicate JSON")
        );
    }
}
#[test]
fn canonical_request_vectors_match_an_independent_sha256_encoding() {
    let vectors: serde_json::Value = serde_json::from_str(include_str!(
        "../../../openapi/fixtures/request-digests.json"
    ))
    .unwrap();
    for vector in vectors["prepare"].as_array().unwrap() {
        let body: api::Prepare = serde_json::from_value(vector["request"].clone()).unwrap();
        let mut request = body.into_command(uuid_v7(1000, 1));
        assert_eq!(
            String::from_utf8(canonical::prepare_bytes(&request).unwrap()).unwrap(),
            vector["canonical"]
        );
        assert_eq!(
            canonical::prepare_digest(&request).unwrap(),
            vector["digest"]
        );
        request.operations.reverse();
        request.namespace = Some("another-namespace".into());
        request.reservation_id = uuid_v7(1000, 2);
        assert_eq!(
            canonical::prepare_digest(&request).unwrap(),
            vector["digest"]
        );
    }
    let actuals: Vec<dto::Actual> =
        serde_json::from_value(vectors["actuals"][0]["actuals"].clone()).unwrap();
    let tx = dto::Reservation {
        request: dto::Prepare {
            reservation_id: uuid_v7(1000, 1),
            namespace: None,
            hold_ms: None,
            context: Default::default(),
            authorization: None,
            operations: vec![
                dto::Operation {
                    operation_id: "a".into(),
                    resource_id: "a".into(),
                    delta: "-25".into(),
                },
                dto::Operation {
                    operation_id: "z".into(),
                    resource_id: "z".into(),
                    delta: "5".into(),
                },
            ],
        },
        expires_at_ms: None,
        state: dto::State::Committed { actuals },
    };
    assert_eq!(
        canonical::actuals_digest(&tx).unwrap().unwrap(),
        vectors["actuals"][0]["digest"]
    );
}

#[test]
fn configured_http_limits_reject_invalid_bounds_and_preserve_signed_minimum() {
    use rescrow_application::{Code, api::Limits, dto::ResourceSpec};
    let limits = Limits::default();
    limits.validate().unwrap();
    let spec = ResourceSpec {
        resource_id: "r".into(),
        lower: i128::MIN.to_string(),
        upper: None,
        initial: i128::MIN.to_string(),
        unit: "u".into(),
        scale: 0,
    };
    limits.resource(&spec).unwrap();
    let mut reduced = limits.clone();
    reduced.numeric_magnitude = i128::MAX.to_string();
    assert_eq!(
        reduced.resource(&spec).unwrap_err().code,
        Code::LimitExceeded
    );
    for modify in [
        |l: &mut Limits| l.operation_count = 1025,
        |l: &mut Limits| l.context_bytes = 4097,
        |l: &mut Limits| l.body_bytes = 16 * 1024 * 1024 + 1,
        |l: &mut Limits| l.decisions.concurrency = 0,
        |l: &mut Limits| l.recovery.concurrency = 0,
        |l: &mut Limits| l.tenant_admission.burst = 0,
        |l: &mut Limits| l.pending.credential = l.pending.tenant + 1,
    ] {
        let mut invalid = limits.clone();
        modify(&mut invalid);
        assert!(invalid.validate().is_err());
    }
}
