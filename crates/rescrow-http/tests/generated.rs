mod support;
use rescrow_application::{api::Limits, dto};
use std::{path::PathBuf, process::Command};
use support::*;
#[tokio::test]
async fn generated_clients_smoke_against_real_server() {
    if std::env::var_os("RESCROW_LANGUAGE_SMOKE").is_none() {
        return;
    }
    let h = Harness::start(Limits::default()).await;
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    for (program, file) in [
        ("python3", "clients/python/smoke.py"),
        ("node", "clients/typescript/smoke.ts"),
    ] {
        let mut command = Command::new(program);
        command
            .arg(root.join(file))
            .env("RESCROW_ENDPOINT", &h.endpoint)
            .env("RESCROW_TOKEN", admin("generated"))
            .env("RESCROW_GENERATION", &h.generation);
        let output = tokio::task::spawn_blocking(move || command.output().unwrap())
            .await
            .unwrap();
        assert!(
            output.status.success(),
            "{program}: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    h.finish().await;
}
#[tokio::test]
async fn remote_cli_uses_same_business_commands_and_json_contract() {
    let Some(binary) = std::env::var_os("RESCROW_REMOTE_CLI") else {
        return;
    };
    let h = Harness::start(Limits::default()).await;
    let temp = std::env::temp_dir().join(format!("rescrow-remote-{}", new_id()));
    std::fs::create_dir(&temp).unwrap();
    let ops = temp.join("ops.json");
    std::fs::write(
        &ops,
        r#"[{"operation_id":"op","resource_id":"credits","delta":"-40"}]"#,
    )
    .unwrap();
    let actuals = temp.join("actuals.json");
    std::fs::write(&actuals, r#"[{"operation_id":"op","delta":"-25"}]"#).unwrap();
    let id = new_id();
    let commands = vec![
        vec![
            "resource",
            "create",
            "credits",
            "--initial",
            "60",
            "--upper",
            "100",
            "--unit",
            "credits",
        ],
        vec![
            "resource",
            "create",
            "credits",
            "--initial",
            "60",
            "--upper",
            "100",
            "--unit",
            "credits",
        ],
        vec![
            "transaction",
            "prepare",
            &id,
            "--operations",
            ops.to_str().unwrap(),
        ],
        vec![
            "transaction",
            "prepare",
            &id,
            "--operations",
            ops.to_str().unwrap(),
        ],
        vec![
            "transaction",
            "commit",
            &id,
            "--actuals",
            actuals.to_str().unwrap(),
        ],
        vec![
            "transaction",
            "commit",
            &id,
            "--actuals",
            actuals.to_str().unwrap(),
        ],
        vec!["transaction", "show", &id],
        vec!["transaction", "list"],
        vec!["resource", "list"],
        vec!["resource", "show", "credits"],
    ];
    let mut last = None;
    for args in commands {
        let mut command = Command::new(&binary);
        command
            .arg("--endpoint")
            .arg(&h.endpoint)
            .arg("--authority-generation")
            .arg(&h.generation)
            .arg("--json")
            .args(args)
            .env("RESCROW_TOKEN", admin("cli"));
        let output = tokio::task::spawn_blocking(move || command.output().unwrap())
            .await
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stderr.is_empty());
        last = Some(serde_json::from_slice::<dto::Output>(&output.stdout).unwrap());
    }
    let dto::Output::Resource(resource) = last.unwrap() else {
        panic!()
    };
    assert_eq!(resource.quantity, "35");
    let id = new_id();
    let mut command = Command::new(binary);
    command
        .arg("--endpoint")
        .arg(&h.endpoint)
        .arg("--json")
        .args([
            "transaction",
            "prepare",
            &id,
            "--operations",
            ops.to_str().unwrap(),
        ])
        .env("RESCROW_TOKEN", admin("cli"));
    let output = tokio::task::spawn_blocking(move || command.output().unwrap())
        .await
        .unwrap();
    assert_eq!(output.status.code(), Some(3));
    let error: rescrow_application::Error = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error.code, rescrow_application::Code::Capacity);
    assert_eq!(error.transaction_id.as_deref(), Some(id.as_str()));
    std::fs::remove_dir_all(temp).unwrap();
    h.finish().await;
}
