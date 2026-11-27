use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use rescrow_application::{
    Code,
    access::{Action, Grant, Policy},
};
use rescrow_http::auth::{
    Authenticator, Issuer, JwtAuthenticator, PinnedAlgorithm, VerificationKey,
};
use serde_json::json;
fn verifier() -> JwtAuthenticator {
    JwtAuthenticator::new(vec![Issuer {
        issuer: "https://issuer.example".into(),
        audience: "rescrow".into(),
        keys: vec![VerificationKey {
            kid: "test".into(),
            algorithm: PinnedAlgorithm::EdDSA,
            public_key_pem: include_str!("fixtures/test-verification-key.pem").into(),
        }],
    }])
    .unwrap()
}
fn claims() -> serde_json::Value {
    json!({"iss":"https://issuer.example","aud":"rescrow","sub":"agent","exp":jsonwebtoken::get_current_timestamp()+3600,"tenant":"tenant-a","namespace":"workflow","credential_id":"stable-agent-credential","scopes":["transaction:prepare","read"],"resources":[{"resource_prefix":"budget.","debit":true,"credit":false,"max_amount":"100"}],"max_hold_ms":3600000})
}
fn token(value: serde_json::Value) -> String {
    let mut header = Header::new(Algorithm::EdDSA);
    header.kid = Some("test".into());
    jsonwebtoken::encode(
        &header,
        &value,
        &EncodingKey::from_ed_pem(include_bytes!("fixtures/test-signing-key.pem")).unwrap(),
    )
    .unwrap()
}
#[test]
fn verifies_pinned_workload_identity_and_ignores_no_security_constraints() {
    let verifier = verifier();
    let credential = token(claims());
    let context = verifier.verify(&credential).unwrap();
    assert_eq!(context.scope().tenant().as_str(), "tenant-a");
    assert_eq!(context.scope().namespace(), Some("workflow"));
    context
        .resource(Action::Prepare, "budget.agent", Some("-100"))
        .unwrap();
    assert_eq!(
        context
            .resource(Action::Prepare, "budget.agent", Some("1"))
            .unwrap_err()
            .code,
        Code::Forbidden
    );
    for (field, value) in [
        ("iss", json!("https://another-issuer.example")),
        ("aud", json!("other-service")),
        ("exp", json!(jsonwebtoken::get_current_timestamp() - 1)),
        ("nbf", json!(jsonwebtoken::get_current_timestamp() + 100)),
        ("tenant", json!("../escape")),
        ("namespace", json!("../escape")),
        ("scopes", json!(["unknown:scope"])),
    ] {
        let mut changed = claims();
        changed[field] = value;
        assert_eq!(
            verifier.verify(&token(changed)).unwrap_err().code,
            Code::Unauthenticated,
            "{field}"
        );
    }
    let mut absent = claims();
    absent.as_object_mut().unwrap().remove("tenant");
    assert!(verifier.verify(&token(absent)).is_err());
    let mut parts = axum::http::Request::builder()
        .header("authorization", format!("Bearer {credential}"))
        .body(())
        .unwrap()
        .into_parts()
        .0;
    verifier.authenticate(&parts).unwrap();
    parts.headers.append(
        "authorization",
        format!("Bearer {credential}").parse().unwrap(),
    );
    assert!(verifier.authenticate(&parts).is_err());
    let _:Policy=serde_json::from_value(json!({"scopes":["read"],"resources":[{"resource_prefix":"budget.","debit":false,"credit":false,"max_amount":"1"}],"max_hold_ms":null})).unwrap();
    let _ = Grant {
        resource_prefix: "*".into(),
        debit: true,
        credit: true,
        max_amount: "100".into(),
    };
}
#[test]
fn rejects_algorithm_confusion_duplicate_keys_and_untrusted_header_extensions() {
    let verifier = verifier();
    let mut header = Header::new(Algorithm::HS256);
    header.kid = Some("test".into());
    let confused = jsonwebtoken::encode(
        &header,
        &claims(),
        &EncodingKey::from_secret(include_bytes!("fixtures/test-verification-key.pem")).unwrap(),
    )
    .unwrap();
    assert!(verifier.verify(&confused).is_err());
    let sign_raw = |header: &str, payload: &str| {
        let message = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(header),
            URL_SAFE_NO_PAD.encode(payload)
        );
        let signature = jsonwebtoken::crypto::sign(
            message.as_bytes(),
            &EncodingKey::from_ed_pem(include_bytes!("fixtures/test-signing-key.pem")).unwrap(),
            Algorithm::EdDSA,
        )
        .unwrap();
        format!("{message}.{signature}")
    };
    let payload = claims().to_string();
    for header in [
        r#"{"alg":"EdDSA","alg":"HS256","kid":"test"}"#,
        r#"{"alg":"EdDSA","kid":"test","crit":["exp"]}"#,
        r#"{"alg":"EdDSA","kid":"test","cty":"JWT"}"#,
        r#"{"alg":"EdDSA","kid":"unknown"}"#,
    ] {
        assert!(verifier.verify(&sign_raw(header, &payload)).is_err());
    }
    let duplicate_payload = payload.replacen('{', "{\"tenant\":\"other\",", 1);
    assert!(
        verifier
            .verify(&sign_raw(
                r#"{"alg":"EdDSA","kid":"test"}"#,
                &duplicate_payload
            ))
            .is_err()
    );
    assert!(
        verifier
            .verify(&"x".repeat(JwtAuthenticator::MAX_TOKEN_BYTES + 1))
            .is_err()
    );
}
