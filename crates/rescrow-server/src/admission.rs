//! Bounded traffic classes. Pending-budget quotas are durable Application state;
//! these rate/concurrency controls are explicitly per serving process.
use rescrow_application::{
    Category, Code, Error,
    api::{LaneCapacity, Limits, TenantLane},
    authority::TenantId,
    dto::Command,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lane {
    Admission,
    Decision,
    Recovery,
}
impl Lane {
    fn index(self) -> usize {
        match self {
            Self::Admission => 0,
            Self::Decision => 1,
            Self::Recovery => 2,
        }
    }
    pub fn command(command: &Command) -> Self {
        match command {
            Command::CreateResource(_) | Command::Prepare(_) => Self::Admission,
            Command::Commit(_) | Command::Abort { .. } => Self::Decision,
            _ => Self::Recovery,
        }
    }
}
struct Gate {
    active: Arc<Semaphore>,
    queue: Arc<Semaphore>,
    capacity: u32,
}
impl Gate {
    fn new(c: LaneCapacity) -> Self {
        Self {
            active: Arc::new(Semaphore::new(c.concurrency as usize)),
            queue: Arc::new(Semaphore::new(c.queue as usize)),
            capacity: c.concurrency,
        }
    }
}
struct Bucket {
    credits: u128,
    last: Instant,
    rate: u32,
    burst: u32,
}
impl Bucket {
    fn new(c: TenantLane) -> Self {
        Self {
            credits: u128::from(c.burst) * 1_000_000_000,
            last: Instant::now(),
            rate: c.requests_per_second,
            burst: c.burst,
        }
    }
    fn take(&mut self) -> bool {
        let now = Instant::now();
        self.credits = self
            .credits
            .saturating_add(
                now.duration_since(self.last)
                    .as_nanos()
                    .saturating_mul(u128::from(self.rate)),
            )
            .min(u128::from(self.burst) * 1_000_000_000);
        self.last = now;
        if self.credits < 1_000_000_000 {
            false
        } else {
            self.credits -= 1_000_000_000;
            true
        }
    }
}
struct TenantGate {
    gate: Gate,
    bucket: Mutex<Bucket>,
}
impl TenantGate {
    fn new(c: TenantLane) -> Self {
        Self {
            gate: Gate::new(LaneCapacity {
                concurrency: c.concurrency,
                queue: c.queue,
            }),
            bucket: Mutex::new(Bucket::new(c)),
        }
    }
}
struct Tenant {
    lanes: [TenantGate; 3],
}
struct State {
    closing: bool,
    active: usize,
    tenants: BTreeMap<TenantId, Arc<Tenant>>,
}
pub struct Control {
    limits: Limits,
    global: [Gate; 3],
    state: Mutex<State>,
    work: watch::Sender<usize>,
}
fn throttled(message: &str) -> Error {
    Error::new(Category::Admission, Code::Throttled, message)
}
fn closing() -> Error {
    Error::new(
        Category::Storage,
        Code::Io,
        "server is draining; retry the identical request",
    )
}
impl Control {
    pub fn new(limits: Limits) -> Result<Arc<Self>, Error> {
        limits.validate()?;
        let global = [
            Gate::new(limits.admission),
            Gate::new(limits.decisions),
            Gate::new(limits.recovery),
        ];
        let (work, _) = watch::channel(0);
        Ok(Arc::new(Self {
            limits,
            global,
            state: Mutex::new(State {
                closing: false,
                active: 0,
                tenants: BTreeMap::new(),
            }),
            work,
        }))
    }
    pub fn limits(&self) -> &Limits {
        &self.limits
    }
    fn tenant(&self, id: &TenantId) -> Result<Arc<Tenant>, Error> {
        let mut state = self.state.lock().unwrap();
        if state.closing {
            return Err(closing());
        }
        if let Some(tenant) = state.tenants.get(id) {
            return Ok(tenant.clone());
        }
        if state.tenants.len() >= self.limits.max_tenants {
            return Err(throttled("tenant traffic partition limit reached"));
        }
        let tenant = Arc::new(Tenant {
            lanes: [
                TenantGate::new(self.limits.tenant_admission),
                TenantGate::new(self.limits.tenant_decisions),
                TenantGate::new(self.limits.tenant_recovery),
            ],
        });
        state.tenants.insert(id.clone(), tenant.clone());
        Ok(tenant)
    }
    pub async fn acquire(self: &Arc<Self>, id: &TenantId, lane: Lane) -> Result<Lease, Error> {
        let global = &self.global[lane.index()];
        if global.capacity == 0 {
            return Err(throttled("new admissions are disabled"));
        }
        let tenant = self.tenant(id)?;
        let tenant_gate = &tenant.lanes[lane.index()];
        if !tenant_gate.bucket.lock().unwrap().take() {
            return Err(throttled(
                "tenant request rate exceeded; retry after one second",
            ));
        }
        if let Ok(local) = tenant_gate.gate.active.clone().try_acquire_owned()
            && let Ok(global) = global.active.clone().try_acquire_owned()
        {
            return Ok(Lease {
                control: self.clone(),
                tenant: id.clone(),
                lane,
                _local: local,
                _global: global,
            });
        }
        let _global_queue = global
            .queue
            .clone()
            .try_acquire_owned()
            .map_err(|_| throttled("traffic class queue is full"))?;
        let _tenant_queue = tenant_gate
            .gate
            .queue
            .clone()
            .try_acquire_owned()
            .map_err(|_| throttled("tenant traffic queue is full"))?;
        let permits =
            tokio::time::timeout(Duration::from_millis(self.limits.queue_timeout_ms), async {
                let local = tenant_gate
                    .gate
                    .active
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| closing())?;
                let global = global
                    .active
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| closing())?;
                Ok::<_, Error>((local, global))
            })
            .await
            .map_err(|_| throttled("bounded traffic queue wait expired"))??;
        Ok(Lease {
            control: self.clone(),
            tenant: id.clone(),
            lane,
            _local: permits.0,
            _global: permits.1,
        })
    }
    pub fn validate(&self, lease: &Lease, id: &TenantId, lane: Lane) -> Result<(), Error> {
        if !std::ptr::eq(self, Arc::as_ptr(&lease.control))
            || &lease.tenant != id
            || lease.lane != lane
        {
            return Err(Error::input(
                "traffic lease does not match this command's authority, tenant and class",
            ));
        }
        Ok(())
    }
    pub(crate) fn register(self: &Arc<Self>) -> Result<Work, Error> {
        let mut state = self.state.lock().unwrap();
        if state.closing {
            return Err(closing());
        }
        state.active = state
            .active
            .checked_add(1)
            .ok_or_else(|| throttled("active work counter exhausted"))?;
        self.work.send_replace(state.active);
        Ok(Work {
            control: self.clone(),
        })
    }
    pub fn close(&self) {
        self.state.lock().unwrap().closing = true;
    }
    /// Stops admission before observing zero, so no queued request can start a
    /// late mutation after draining completes. Actual blocking work owns `Work`.
    pub async fn drain(&self) {
        self.close();
        let mut work = self.work.subscribe();
        let _ = work.wait_for(|active| *active == 0).await;
    }
}
pub struct Lease {
    control: Arc<Control>,
    tenant: TenantId,
    lane: Lane,
    _local: OwnedSemaphorePermit,
    _global: OwnedSemaphorePermit,
}
pub(crate) struct Work {
    control: Arc<Control>,
}
impl Drop for Work {
    fn drop(&mut self) {
        let mut state = self.control.state.lock().unwrap();
        state.active = state
            .active
            .checked_sub(1)
            .expect("work is registered exactly once");
        self.control.work.send_replace(state.active);
    }
}
