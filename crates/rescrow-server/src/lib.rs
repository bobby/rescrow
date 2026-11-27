//! Bounded async entrypoint for synchronous Application calls. Tokio is a
//! dependency of this composition only; the local CLI never compiles it.
pub mod admission;
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
    control: Arc<admission::Control>,
}
impl<S> Clone for Server<S> {
    fn clone(&self) -> Self {
        Self {
            app: self.app.clone(),
            permits: self.permits.clone(),
            control: self.control.clone(),
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
            control: admission::Control::new(rescrow_application::api::Limits::default())?,
        })
    }
    pub fn start_with_limits(
        app: Application<S>,
        required: DurabilityProfile,
        limits: rescrow_application::api::Limits,
    ) -> Result<Self, Error> {
        limits.validate()?;
        let max_in_flight = (limits.admission.concurrency
            + limits.decisions.concurrency
            + limits.recovery.concurrency) as usize;
        let app = app.with_pending_limits(limits.pending)?;
        Ok(Self {
            app: Arc::new(ReadyApplication::start(app, required)?),
            permits: Arc::new(Semaphore::new(max_in_flight)),
            control: admission::Control::new(limits)?,
        })
    }
    pub fn authority_generation(&self) -> &str {
        self.app.authority_generation()
    }
    pub fn control(&self) -> &Arc<admission::Control> {
        &self.control
    }
    pub fn capabilities(&self) -> &rescrow_application::capabilities::Capabilities {
        self.app.capabilities()
    }
    pub async fn reserve(
        &self,
        context: &rescrow_application::access::AccessContext,
        lane: admission::Lane,
    ) -> Result<admission::Lease, Error> {
        self.control.acquire(context.scope().tenant(), lane).await
    }
    pub async fn execute_authorized(
        &self,
        context: rescrow_application::access::AccessContext,
        command: Command,
        required: DurabilityProfile,
        lease: admission::Lease,
    ) -> Result<rescrow_application::transactional::Execution, Error> {
        self.control.validate(
            &lease,
            context.scope().tenant(),
            admission::Lane::command(&command),
        )?;
        let work = self.control.register()?;
        let transaction_id = command_id(&command);
        let app = self.app.clone();
        tokio::task::spawn_blocking(move || {
            let _lease = lease;
            let _work = work;
            app.execute_authorized(&context, command, required)
        })
        .await
        .unwrap_or_else(|e| {
            Err(StoreError::Unknown(format!(
                "application task stopped before delivering its outcome: {e}"
            ))
            .into())
        })
        .map_err(|mut e| {
            if e.transaction_id.is_none() {
                e.transaction_id = transaction_id;
            }
            e
        })
    }
    pub async fn inspect_preparation(
        &self,
        context: rescrow_application::access::AccessContext,
        request: rescrow_application::dto::Prepare,
        required: DurabilityProfile,
        lease: admission::Lease,
    ) -> Result<Option<rescrow_application::dto::Transaction>, Error> {
        self.control
            .validate(&lease, context.scope().tenant(), admission::Lane::Recovery)?;
        let work = self.control.register()?;
        let id = request.transaction_id.clone();
        let app = self.app.clone();
        tokio::task::spawn_blocking(move || {
            let _lease = lease;
            let _work = work;
            app.inspect_preparation(&context, request, required)
        })
        .await
        .unwrap_or_else(|e| {
            Err(StoreError::Unknown(format!("preparation inspection stopped: {e}")).into())
        })
        .map_err(|mut e| {
            if e.transaction_id.is_none() {
                e.transaction_id = Some(id);
            }
            e
        })
    }
    pub async fn drain(&self) {
        self.control.drain().await;
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
        let work = self.control.register()?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let _work = work;
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

fn command_id(command: &Command) -> Option<String> {
    match command {
        Command::Prepare(v) => Some(v.transaction_id.clone()),
        Command::Commit(v) => Some(v.transaction_id.clone()),
        Command::Abort { transaction_id } | Command::ShowTransaction { transaction_id } => {
            Some(transaction_id.clone())
        }
        _ => None,
    }
}
