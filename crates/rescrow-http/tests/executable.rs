#![cfg(unix)]
mod support;
use rescrow_adapters::file::FileStore;
use rescrow_application::{
    Application, api::Limits, authority::TransactionalStore, capabilities::DurabilityProfile,
};
use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};
use support::*;
struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
#[tokio::test]
async fn executable_selects_existing_file_or_disposable_memory_and_handles_sigterm() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    for (file, tls) in [(true, false), (false, false), (false, true)] {
        let root = std::env::temp_dir().join(format!("rescrow-http-bin-{}", new_id()));
        std::fs::create_dir(&root).unwrap();
        let mut cfg = config(Limits::default());
        let generation = if file {
            cfg.required_profile = DurabilityProfile::Local;
            let store = FileStore::new(root.join("store")).unwrap();
            Application::new(store.clone()).initialize().unwrap();
            Some(TransactionalStore::identity(&store).unwrap())
        } else {
            None
        };
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let backend = if file {
            serde_json::json!({"type":"file","path":root.join("store")})
        } else {
            serde_json::json!({"type":"memory"})
        };
        let cert = root.join("cert.pem");
        let key = root.join("key.pem");
        let tls_config = if tls {
            let output = Command::new("openssl")
                .args([
                    "req",
                    "-x509",
                    "-newkey",
                    "rsa:2048",
                    "-nodes",
                    "-days",
                    "1",
                    "-subj",
                    "/CN=localhost",
                    "-addext",
                    "subjectAltName=IP:127.0.0.1",
                    "-addext",
                    "basicConstraints=critical,CA:FALSE",
                    "-out",
                ])
                .arg(&cert)
                .arg("-keyout")
                .arg(&key)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            serde_json::json!({"certificate":cert,"private_key":key})
        } else {
            serde_json::Value::Null
        };
        let path = root.join("config.json");
        std::fs::write(&path,serde_json::to_vec(&serde_json::json!({"listen":address.to_string(),"backend":backend,"api":cfg,"issuers":[issuer()],"tls":tls_config})).unwrap()).unwrap();
        let mut child = Process(
            Command::new(env!("CARGO_BIN_EXE_rescrow-http"))
                .arg("--config")
                .arg(path)
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap(),
        );
        let token = token(
            "local",
            None,
            &[
                "resource:create",
                "reservation:prepare",
                "reservation:commit",
                "reservation:abort",
                "read",
            ],
        );
        let client = if tls {
            reqwest::Client::builder()
                .add_root_certificate(
                    reqwest::Certificate::from_pem(&std::fs::read(&cert).unwrap()).unwrap(),
                )
                .build()
                .unwrap()
        } else {
            reqwest::Client::new()
        };
        let scheme = if tls { "https" } else { "http" };
        let url = format!("{scheme}://{address}/v1/resources/credits");
        let mut ready = false;
        let mut last_error = None;
        for _ in 0..100 {
            match client.get(&url).bearer_auth(&token).send().await {
                Ok(_) => {
                    ready = true;
                    break;
                }
                Err(e) => last_error = Some(e),
            }
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "server exited before readiness"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(ready, "last connection error: {last_error:?}");
        let response = client
            .put(url.replacen("/v1/resources/", "/admin/v1/resources/", 1))
            .bearer_auth(&token)
            .json(&spec())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 201);
        if let Some(generation) = generation {
            assert_eq!(response.headers()["authority-generation"], generation);
        }
        let value: rescrow_application::api::Envelope<rescrow_application::api::Resource> = client
            .get(&url)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(value.data.observation.quantity, "60");
        if file {
            problem(
                client
                    .get(&url)
                    .bearer_auth(admin("other"))
                    .send()
                    .await
                    .unwrap(),
                404,
                rescrow_application::Code::NotFound,
            )
            .await;
        }
        assert!(
            Command::new("kill")
                .args(["-TERM", &child.0.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        let mut exited = false;
        for _ in 0..100 {
            if let Some(status) = child.0.try_wait().unwrap() {
                assert!(status.success());
                exited = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(exited, "SIGTERM did not drain and exit");
        drop(child);
        std::fs::remove_dir_all(root).unwrap();
    }
}
