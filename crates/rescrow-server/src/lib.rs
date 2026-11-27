//! Bounded async entrypoint for synchronous Application calls. Tokio is a
//! dependency of this composition only; the local CLI never compiles it.
use rescrow_application::{
    Error, StoreError,
    authority::TransactionalStore,
    capabilities::{DurabilityProfile, Readiness, Response},
    dto::Command,
    transactional::{Application, ReadyApplication},
};
use std::sync::Arc;
use tokio::sync::Semaphore;

pub struct Server<S> {
    app: Arc<ReadyApplication<S>>,
    permits: Arc<Semaphore>,
}
impl<S> Clone for Server<S> {
    fn clone(&self) -> Self {
        Self {
            app: self.app.clone(),
            permits: self.permits.clone(),
        }
    }
}
impl<S: TransactionalStore + Readiness + Send + Sync + 'static> Server<S> {
    /// Probe live infrastructure and verify the required profile before serving.
    /// Call this during startup, outside an async worker's request handling.
    pub fn start(
        app: Application<S>,
        required: DurabilityProfile,
        max_in_flight: usize,
    ) -> Result<Self, Error> {
        if max_in_flight == 0 || max_in_flight > Semaphore::MAX_PERMITS {
            return Err(Error::input(
                "max_in_flight must fit a nonzero semaphore capacity",
            ));
        }
        Ok(Self {
            app: Arc::new(ReadyApplication::start(app, required)?),
            permits: Arc::new(Semaphore::new(max_in_flight)),
        })
    }
    /// Capacity is acquired asynchronously before dispatching to the blocking
    /// pool. The permit stays with the operation after request cancellation;
    /// cancellation cannot free an admitted write's concurrency slot early.
    pub async fn execute(
        &self,
        command: Command,
        required: DurabilityProfile,
    ) -> Result<Response, Error> {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| StoreError::Io(e.to_string()))?;
        let transaction_id = match &command {
            Command::Prepare(v) => Some(v.transaction_id.clone()),
            Command::Commit(v) => Some(v.transaction_id.clone()),
            Command::Abort { transaction_id } | Command::ShowTransaction { transaction_id } => {
                Some(transaction_id.clone())
            }
            _ => None,
        };
        let app = self.app.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            app.execute(command, required)
        })
        .await
        .unwrap_or_else(|e| {
            Err(StoreError::Unknown(format!(
                "application task stopped before delivering its outcome: {e}"
            ))
            .into())
        })
        .map_err(|mut error| {
            if error.transaction_id.is_none() {
                error.transaction_id = transaction_id;
            }
            error
        })
    }
}
