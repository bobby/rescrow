mod support;
use rescrow_adapters::authority_memory::MemoryAuthority;
use rescrow_application::{
    Application, Code, StoreError,
    api::{Envelope, Limits, Reservation},
    authority::*,
    capabilities::*,
};
use rescrow_http::{Api, auth::JwtAuthenticator};
use std::{
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use support::*;
#[derive(Clone)]
struct Delayed {
    inner: MemoryAuthority,
    armed: Arc<AtomicBool>,
    after: bool,
    entered: Arc<tokio::sync::Notify>,
    gate: Arc<(Mutex<bool>, Condvar)>,
}
impl Delayed {
    fn release(&self) {
        *self.gate.0.lock().unwrap() = true;
        self.gate.1.notify_all();
    }
}
impl TransactionalStore for Delayed {
    fn identity(&self) -> Result<String, StoreError> {
        self.inner.identity()
    }
    fn initialize(&self) -> Result<(), StoreError> {
        self.inner.initialize()
    }
    fn read(&self, t: &TenantId, k: &[RecordKey], s: u64) -> Result<Read, StoreError> {
        self.inner.read(t, k, s)
    }
    fn scan(&self, t: &TenantId) -> Result<Inspection, StoreError> {
        self.inner.scan(t)
    }
    fn commit(&self, c: &Commit) -> Result<CommitOutcome, StoreError> {
        if !self.armed.swap(false, Ordering::SeqCst) {
            return self.inner.commit(c);
        }
        let result = if self.after {
            Some(self.inner.commit(c))
        } else {
            None
        };
        self.entered.notify_one();
        let _guard = self
            .gate
            .1
            .wait_timeout_while(
                self.gate.0.lock().unwrap(),
                Duration::from_secs(5),
                |open| !*open,
            )
            .unwrap();
        result.unwrap_or_else(|| self.inner.commit(c))
    }
    fn resolve(&self, t: &TenantId, id: &TransitionId) -> Result<Resolution, StoreError> {
        self.inner.resolve(t, id)
    }
    fn history(&self, t: &TenantId) -> Result<Vec<HistoryRecord>, StoreError> {
        self.inner.history(t)
    }
}
impl Readiness for Delayed {
    fn readiness(&self) -> Result<Capabilities, StoreError> {
        self.inner.readiness()
    }
}
#[tokio::test]
async fn processing_requires_durable_proof_and_shutdown_waits_for_cancelled_real_writes() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    for after in [true, false] {
        let store = Delayed {
            inner: MemoryAuthority::default(),
            armed: Arc::default(),
            after,
            entered: Arc::default(),
            gate: Arc::new((Mutex::new(false), Condvar::new())),
        };
        store.initialize().unwrap();
        let limits = Limits {
            request_timeout_ms: 600,
            ..Limits::default()
        };
        let api = Api::new(
            Application::new(store.clone()),
            config(limits),
            Arc::new(JwtAuthenticator::new(vec![issuer()]).unwrap()),
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (stop, shutdown) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(api.serve(listener, async {
            let _ = shutdown.await;
        }));
        let client = reqwest::Client::new();
        let token = admin("tenant");
        assert_eq!(
            client
                .put(format!("{endpoint}/admin/v1/resources/credits"))
                .bearer_auth(&token)
                .json(&spec())
                .send()
                .await
                .unwrap()
                .status(),
            201
        );
        store.armed.store(true, Ordering::SeqCst);
        let id = new_id();
        let response = client
            .post(format!("{endpoint}/v1/reservations"))
            .bearer_auth(&token)
            .header("idempotency-key", &id)
            .json(&prepare("-10"))
            .send()
            .await
            .unwrap();
        store.entered.notified().await;
        if after {
            assert_eq!(response.status(), 202);
            assert_eq!(response.headers()["retry-after"], "1");
            assert_eq!(
                response.headers()["location"],
                format!("/v1/reservations/{id}")
            );
            let value: Envelope<Reservation> = response.json().await.unwrap();
            assert!(value.data.processing);
            assert_eq!(
                value.data.materialization,
                rescrow_application::api::Materialization::Complete
            );
        } else {
            let value = problem(response, 503, Code::DeadlineExceeded).await;
            assert_eq!(value.reservation_id.as_deref(), Some(id.as_str()));
            assert!(value.retryable);
        }
        stop.send(()).unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(
            !task.is_finished(),
            "shutdown returned while real blocking work was still active"
        );
        store.release();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let app = Application::new(store.inner)
            .scoped(Scope::new(TenantId::new("tenant").unwrap(), "workflow").unwrap());
        assert_eq!(app.resource("credits").unwrap().reserved_down, "10");
        assert_eq!(app.reservations().unwrap().len(), 1);
    }
}
