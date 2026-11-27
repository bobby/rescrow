//! Use cases over record-scoped atomic authority publications.
use crate::{
    Category, Code, Error, RetryWindow, StoreError, Write,
    authority::*,
    capabilities::{self, DurabilityProfile, Readiness},
    corrupt,
    dto::{self, Command, Output},
    input, millis, system_clock, uuid_v7, uuid_v7_millis,
};
use rescrow_domain::{self as d, transition};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

/// Publication-aware outcome, computed during conditional evaluation so races
/// cannot cause two HTTP requests to both claim they created one identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Execution {
    pub output: Output,
    pub replayed: bool,
}
type Evaluation = (transition::Changes, Result<Output, Error>);
#[derive(Clone)]
pub(crate) struct View {
    read: Read,
    live: d::Live,
    reservations: BTreeMap<d::Id, (u64, d::Reservation)>,
    now_ms: u64,
    complete: bool,
    terminal_since_ms: u64,
    pending: d::access::Pending,
}
impl View {
    fn from_read(read: Read, now: u64) -> Result<Self, Error> {
        let mut resources = Vec::new();
        let mut prepared = Vec::new();
        let mut reservations = BTreeMap::new();
        let mut now_ms = now.max(read.recorded_at_ms);
        let mut pending = d::access::Pending::default();
        for (key, value) in &read.records {
            if let Some(value) = value {
                value.value.validate(key)?;
                now_ms = now_ms.max(value.recorded_at_ms);
                match &value.value {
                    Value::Pending(value) => pending = value.clone().try_into().map_err(corrupt)?,
                    Value::Resource(r) => resources.push(r.clone().try_into().map_err(corrupt)?),
                    Value::Reservation(tx) => {
                        let tx: d::Reservation = tx.clone().try_into().map_err(corrupt)?;
                        if *tx.state() == d::State::Prepared {
                            prepared.push(tx.clone());
                        }
                        reservations.insert(tx.request().id().clone(), (value.recorded_at_ms, tx));
                    }
                }
            }
        }
        let known = d::access::Pending::from_prepared(&prepared).map_err(corrupt)?;
        if known
            .entries()
            .iter()
            .any(|(id, count)| pending.for_credential(id) < *count)
        {
            return Err(corrupt(
                "pending counter omits loaded prepared reservations",
            ));
        }
        let live = d::Live::restore(resources, prepared).map_err(corrupt)?;
        for (_, tx) in reservations.values() {
            transition::validate_participants(tx, live.resources()).map_err(corrupt)?;
        }
        Ok(Self {
            read,
            live,
            reservations,
            now_ms,
            complete: false,
            terminal_since_ms: 0,
            pending,
        })
    }
    pub(crate) fn live(&self) -> &d::Live {
        &self.live
    }
    fn advance(&mut self, changes: &transition::Changes) -> Result<(), Error> {
        let pending = match changes.reservation() {
            Some(tx) => self
                .pending
                .apply(
                    self.reservations.get(tx.request().id()).map(|(_, tx)| tx),
                    tx,
                )
                .map_err(corrupt)?,
            None => self.pending.clone(),
        };
        self.live = self.live.apply(changes);
        self.pending = pending;
        if let Some(tx) = changes.reservation() {
            self.reservations
                .insert(tx.request().id().clone(), (self.now_ms, tx.clone()));
        }
        Ok(())
    }
    fn received(&mut self, commit: &Commit, receipt: Receipt) -> Result<(), Error> {
        if !receipt.versions.keys().eq(commit.replacements().keys()) {
            return Err(corrupt(
                "publication receipt does not cover its replacements",
            ));
        }
        for (key, value) in commit.replacements() {
            self.read.records.insert(
                key.clone(),
                Some(Versioned {
                    version: receipt.versions[key].clone(),
                    recorded_at_ms: self.now_ms,
                    value: value.clone(),
                }),
            );
        }
        self.read.fence = receipt.fence;
        self.read.recorded_at_ms = self.read.recorded_at_ms.max(self.now_ms);
        Ok(())
    }
}

pub struct Application<S> {
    store: Arc<S>,
    scope: Scope,
    attempts: usize,
    window: RetryWindow,
    pub(crate) clock: Arc<dyn Fn() -> u64 + Send + Sync>,
    pending_limits: crate::access::PendingLimits,
    checkpoint_interval: usize,
}
impl<S: TransactionalStore> Application<S> {
    pub fn new(store: S) -> Self {
        Self {
            store: Arc::new(store),
            scope: Scope::local(),
            attempts: 32,
            window: RetryWindow::default(),
            clock: Arc::new(system_clock),
            pending_limits: crate::access::PendingLimits::default(),
            checkpoint_interval: 64,
        }
    }
    pub fn with_pending_limits(
        mut self,
        limits: crate::access::PendingLimits,
    ) -> Result<Self, Error> {
        self.pending_limits = limits.validate()?;
        Ok(self)
    }
    pub fn scoped(&self, scope: Scope) -> Self {
        Self {
            store: self.store.clone(),
            scope,
            attempts: self.attempts,
            window: self.window,
            clock: self.clock.clone(),
            checkpoint_interval: self.checkpoint_interval,
            pending_limits: self.pending_limits,
        }
    }
    pub fn with_checkpoint_interval(mut self, entries: usize) -> Self {
        self.checkpoint_interval = entries.max(1);
        self
    }
    pub(crate) fn view(&self) -> Result<View, Error> {
        let inspection = self.store.scan(self.scope.tenant())?;
        let mut view = View::from_read(inspection.read, (self.clock)())?;
        view.complete = true;
        Ok(view)
    }
    pub(crate) fn reservation_in(&self, view: &View, id: &str) -> Result<dto::Reservation, Error> {
        let scoped = self.scoped_id(id)?;
        self.lookup(view, &scoped, 0)?
            .as_ref()
            .map(Into::into)
            .ok_or_else(|| Error::domain(d::Error::NotFound))
            .map_err(|e| e.with_reservation(id))
    }
    pub(crate) fn qualify(&self, write: Write) -> Result<Write, Error> {
        Ok(match write {
            Write::Prepare(request) => {
                if request
                    .id()
                    .namespace()
                    .is_some_and(|n| Some(n) != self.scope.namespace())
                {
                    return Err(Error::input(
                        "request namespace differs from authorized context",
                    ));
                }
                let request = d::Request::new(
                    self.scoped_id(request.id().as_str())?,
                    request.operations().to_vec(),
                )
                .map_err(input)?
                .with_hold(request.hold_ms())
                .map_err(input)?
                .with_context(request.context().clone())
                .with_authorization(request.authorization().cloned());
                Write::Prepare(request)
            }
            Write::Commit { id, actuals } => Write::Commit {
                id: self.scoped_id(id.as_str())?,
                actuals,
            },
            Write::Abort(id) => Write::Abort(self.scoped_id(id.as_str())?),
            other => other,
        })
    }
    pub fn with_scope(mut self, scope: Scope) -> Self {
        self.scope = scope;
        self
    }
    pub fn with_clock(mut self, clock: impl Fn() -> u64 + Send + Sync + 'static) -> Self {
        self.clock = Arc::new(clock);
        self
    }
    pub fn with_retry_window(mut self, window: RetryWindow) -> Self {
        self.window = window;
        self
    }
    pub fn initialize(&self) -> Result<(), Error> {
        self.store.initialize().map_err(Into::into)
    }
    pub fn execute(&self, command: Command) -> Result<Output, Error> {
        match command {
            Command::CreateResource(v) => self.create_resource(v).map(Output::Resource),
            Command::Prepare(v) => self.prepare(v).map(Output::Reservation),
            Command::Commit(v) => self.commit(v).map(Output::Reservation),
            Command::Abort { reservation_id } => {
                self.abort(&reservation_id).map(Output::Reservation)
            }
            Command::ShowResource { resource_id } => {
                self.resource(&resource_id).map(Output::Resource)
            }
            Command::ListResources => self.resources().map(Output::Resources),
            Command::ShowReservation { reservation_id } => {
                self.reservation(&reservation_id).map(Output::Reservation)
            }
            Command::ListReservations => self.reservations().map(Output::Reservations),
        }
    }
    pub fn execute_detailed(&self, command: Command) -> Result<Execution, Error> {
        let reservation_id = match &command {
            Command::Prepare(v) => Some(v.reservation_id.clone()),
            Command::Commit(v) => Some(v.reservation_id.clone()),
            Command::Abort { reservation_id } => Some(reservation_id.clone()),
            _ => None,
        };
        let write = match command {
            Command::CreateResource(v) => Write::create(v),
            Command::Prepare(v) => Write::prepare(v),
            Command::Commit(v) => Write::commit(v),
            Command::Abort { reservation_id } => Write::abort(&reservation_id),
            read => {
                return self.execute(read).map(|output| Execution {
                    output,
                    replayed: false,
                });
            }
        };
        write
            .and_then(|w| self.qualify(w))
            .and_then(|w| self.write_detailed(&w))
            .map_err(|mut e| {
                if reservation_id.is_some() {
                    e.reservation_id = reservation_id;
                }
                e
            })
    }
    /// Read-only resolution for an in-flight HTTP prepare. `None` is not a
    /// rollback proof. Only a matching durable original request can be reported
    /// as processing; this never admits work or extends a deadline.
    pub fn inspect_preparation(
        &self,
        context: &crate::access::AccessContext,
        request: dto::Prepare,
    ) -> Result<Option<dto::Reservation>, Error> {
        let id = request.reservation_id.clone();
        (|| {
            context.validate_at((self.clock)())?;
            let app = self.scoped(context.scope().clone());
            let request = context.bind_prepare(request)?;
            let write = app.qualify(Write::prepare(request)?)?;
            let Write::Prepare(request) = &write else {
                unreachable!()
            };
            let since = uuid_v7_millis(request.id().as_str())
                .unwrap_or(0)
                .saturating_sub(millis(self.window.skew));
            let view = app.load_keys(Self::keys(&write), since)?;
            let existing = match view.live.prepared().get(request.id()) {
                Some(tx) => Some(tx.clone()),
                None => app.lookup(&view, request.id(), app.admit(&view, request)?)?,
            };
            let Some(tx) = existing else { return Ok(None) };
            let changes = transition::prepare(
                request.clone(),
                Some(&tx),
                view.live.resources(),
                view.now_ms,
            )
            .map_err(Error::domain)?;
            if !changes.is_empty() {
                return Err(corrupt("preparation inspection produced a mutation"));
            }
            if let Some(reason) = tx.rejection() {
                return Err(Error::rejection(reason));
            }
            Ok(Some((&tx).into()))
        })()
        .map_err(|e: Error| e.with_reservation(&id))
    }
    pub fn execute_authorized(
        &self,
        context: &crate::access::AccessContext,
        command: Command,
    ) -> Result<Execution, Error> {
        let id = match &command {
            Command::Prepare(v) => Some(v.reservation_id.clone()),
            Command::Commit(v) => Some(v.reservation_id.clone()),
            Command::Abort { reservation_id } | Command::ShowReservation { reservation_id } => {
                Some(reservation_id.clone())
            }
            _ => None,
        };
        self.execute_authorized_inner(context, command)
            .map_err(|mut e| {
                if e.reservation_id.is_none() {
                    e.reservation_id = id;
                }
                e
            })
    }
    fn execute_authorized_inner(
        &self,
        context: &crate::access::AccessContext,
        command: Command,
    ) -> Result<Execution, Error> {
        use crate::access::Action;
        context.validate_at((self.clock)())?;
        let app = self.scoped(context.scope().clone());
        let command = match command {
            Command::CreateResource(spec) => {
                context.resource(Action::Create, &spec.resource_id, None)?;
                Command::CreateResource(spec)
            }
            Command::Prepare(request) => Command::Prepare(context.bind_prepare(request)?),
            Command::Commit(commit) => {
                context.require(Action::Commit)?;
                let tx = app.reservation(&commit.reservation_id)?;
                context.request(Action::Commit, &tx.request)?;
                Command::Commit(commit)
            }
            Command::Abort { reservation_id } => {
                context.require(Action::Abort)?;
                let tx = app.reservation(&reservation_id)?;
                context.request(Action::Abort, &tx.request)?;
                Command::Abort { reservation_id }
            }
            Command::ShowResource { resource_id } => {
                context.resource(Action::Read, &resource_id, None)?;
                Command::ShowResource { resource_id }
            }
            Command::ShowReservation { reservation_id } => {
                context.require(Action::Read)?;
                let tx = app.reservation(&reservation_id)?;
                context.request(Action::Read, &tx.request)?;
                return Ok(Execution {
                    output: Output::Reservation(tx),
                    replayed: false,
                });
            }
            Command::ListResources => {
                context.require(Action::Read)?;
                let mut visible = Vec::new();
                for resource in app.resources()? {
                    let id = d::Id::new(resource.spec.resource_id.clone()).map_err(corrupt)?;
                    if context.policy().can_read(&id) {
                        visible.push(resource);
                    }
                }
                return Ok(Execution {
                    output: Output::Resources(visible),
                    replayed: false,
                });
            }
            Command::ListReservations => {
                context.require(Action::Read)?;
                let mut visible = Vec::new();
                for tx in app.reservations()? {
                    if context.request(Action::Read, &tx.request).is_ok() {
                        visible.push(tx);
                    }
                }
                return Ok(Execution {
                    output: Output::Reservations(visible),
                    replayed: false,
                });
            }
        };
        app.execute_detailed(command)
    }
    pub fn create_resource(&self, spec: dto::ResourceSpec) -> Result<dto::Resource, Error> {
        match self.write(&Write::create(spec)?)? {
            Output::Resource(r) => Ok(r),
            _ => unreachable!(),
        }
    }
    pub fn prepare(&self, mut request: dto::Prepare) -> Result<dto::Reservation, Error> {
        let id = request.reservation_id.clone();
        let result = (|| {
            if request
                .namespace
                .as_deref()
                .is_some_and(|n| Some(n) != self.scope.namespace())
            {
                return Err(Error::input(
                    "request namespace differs from authorized context",
                ));
            }
            request.namespace = self.scope.namespace().map(str::to_owned);
            let write = Write::prepare(request)?;
            self.write(&write).map(crate::reservation_output)
        })();
        result.map_err(|e| e.with_reservation(&id))
    }
    fn scoped_id(&self, id: &str) -> Result<d::Id, Error> {
        match self.scope.reservation(id)? {
            RecordKey::Reservation(id) => Ok(id),
            _ => unreachable!(),
        }
    }
    pub fn commit(&self, command: dto::Commit) -> Result<dto::Reservation, Error> {
        let id = command.reservation_id.clone();
        (|| {
            let Write::Commit { actuals, .. } = Write::commit(command)? else {
                unreachable!()
            };
            self.write(&Write::Commit {
                id: self.scoped_id(&id)?,
                actuals,
            })
            .map(crate::reservation_output)
        })()
        .map_err(|e: Error| e.with_reservation(&id))
    }
    pub fn abort(&self, id: &str) -> Result<dto::Reservation, Error> {
        self.scoped_id(id)
            .and_then(|id| self.write(&Write::Abort(id)))
            .map(crate::reservation_output)
            .map_err(|e| e.with_reservation(id))
    }
    pub fn resource(&self, id: &str) -> Result<dto::Resource, Error> {
        let key = RecordKey::resource(id)?;
        let view = self.load_keys(BTreeSet::from([key.clone()]), 0)?;
        match &view.read.records[&key] {
            Some(Versioned {
                value: Value::Resource(r),
                ..
            }) => Ok(r.clone()),
            _ => Err(Error::domain(d::Error::NotFound)),
        }
    }
    pub fn resources(&self) -> Result<Vec<dto::Resource>, Error> {
        Ok(self.snapshot()?.resources)
    }
    pub fn reservation(&self, id: &str) -> Result<dto::Reservation, Error> {
        (|| {
            let txid = self.scoped_id(id)?;
            let view = self.load_keys(BTreeSet::from([RecordKey::Reservation(txid.clone())]), 0)?;
            self.lookup(&view, &txid, 0)?
                .as_ref()
                .map(Into::into)
                .ok_or_else(|| Error::domain(d::Error::NotFound))
        })()
        .map_err(|e: Error| e.with_reservation(id))
    }
    pub fn reservations(&self) -> Result<Vec<dto::Reservation>, Error> {
        Ok(self
            .snapshot()?
            .reservations
            .into_iter()
            .filter(|tx| tx.request.namespace.as_deref() == self.scope.namespace())
            .collect())
    }
    /// Tenant-wide integrity inspection. A composition root must authorize
    /// this administrative operation separately from namespace point lookups.
    pub fn snapshot(&self) -> Result<dto::Snapshot, Error> {
        let inspection = self.store.scan(self.scope.tenant())?;
        let view = View::from_read(inspection.read, (self.clock)())?;
        let mut live = d::Live::default();
        let mut reservations = BTreeMap::<d::Id, d::Reservation>::new();
        let mut revision = 0u64;
        let mut identities = BTreeSet::new();
        let mut times = BTreeMap::<RecordKey, u64>::new();
        for history in inspection.history {
            if !identities.insert(history.transition_id) || history.records.is_empty() {
                return Err(corrupt("duplicate or empty history publication"));
            }
            for entry in history.records {
                let now = entry.recorded_at_ms;
                let changes = match entry.record {
                    dto::Record::ResourceCreated(spec) => {
                        let spec: d::ResourceSpec = spec.try_into().map_err(corrupt)?;
                        transition::create(spec.clone(), live.resources().get(spec.id()))
                            .map_err(corrupt)?
                    }
                    dto::Record::Reservation(tx) => {
                        let tx: d::Reservation = tx.try_into().map_err(corrupt)?;
                        let prior = reservations.get(tx.request().id());
                        let changes = match (tx.state(), prior) {
                            (
                                d::State::Prepared | d::State::Aborted(d::AbortReason::Rejected(_)),
                                None,
                            ) => transition::prepare(
                                tx.request().clone(),
                                None,
                                live.resources(),
                                now,
                            ),
                            (d::State::Committed(a), Some(prior)) => transition::commit(
                                prior,
                                Some(a.entries().iter().map(|(i, d)| (i.clone(), *d)).collect()),
                                live.resources(),
                            ),
                            (d::State::Aborted(d::AbortReason::Requested), Some(prior)) => {
                                transition::abort(prior, live.resources())
                            }
                            (d::State::Aborted(d::AbortReason::Lapsed), Some(prior)) => {
                                transition::lapse(prior, live.resources(), now)
                            }
                            _ => return Err(corrupt("reservation history out of order")),
                        }
                        .map_err(corrupt)?;
                        if changes.reservation() != Some(&tx) {
                            return Err(corrupt("history does not reproduce its transition"));
                        }
                        reservations.insert(tx.request().id().clone(), tx);
                        changes
                    }
                };
                if changes.is_empty() {
                    return Err(corrupt("history contains a replay"));
                }
                let keys = changes
                    .resources()
                    .iter()
                    .map(|r| RecordKey::Resource(r.spec().id().clone()))
                    .chain(
                        changes
                            .reservation()
                            .map(|tx| RecordKey::Reservation(tx.request().id().clone())),
                    );
                for key in keys {
                    if times.get(&key).is_some_and(|time| *time > now) {
                        return Err(corrupt("record time decreased"));
                    }
                    times.insert(key, now);
                }
                live = live.apply(&changes);
                revision = revision
                    .checked_add(1)
                    .ok_or_else(|| corrupt("history count overflow"))?;
            }
        }
        if live != view.live
            || reservations.len() != view.reservations.len()
            || reservations
                .iter()
                .any(|(id, tx)| view.reservations.get(id).map(|(_, tx)| tx) != Some(tx))
        {
            return Err(corrupt("records disagree with history"));
        }
        if view.pending
            != d::access::Pending::from_prepared(reservations.values()).map_err(corrupt)?
        {
            return Err(corrupt(
                "pending counter disagrees with reservation history",
            ));
        }
        let ledger = d::Ledger::restore(
            live.resources().values().cloned().collect(),
            reservations.into_values().collect(),
        )
        .map_err(corrupt)?;
        Ok(dto::Snapshot::from_ledger(&ledger, revision))
    }
    fn load_keys(
        &self,
        mut keys: BTreeSet<RecordKey>,
        terminal_since_ms: u64,
    ) -> Result<View, Error> {
        for _ in 0..self.attempts {
            let requested: Vec<_> = keys.iter().cloned().collect();
            let read = self
                .store
                .read(self.scope.tenant(), &requested, terminal_since_ms)?;
            if !read.records.keys().eq(keys.iter()) {
                return Err(corrupt("read omitted or added requested keys"));
            }
            let old_len = keys.len();
            for (key, value) in &read.records {
                if let Some(value) = value {
                    value.value.validate(key)?;
                    match &value.value {
                        Value::Pending(_) => (),
                        Value::Resource(r) => {
                            let r: d::Resource = r.clone().try_into().map_err(corrupt)?;
                            keys.extend(r.holds().keys().cloned().map(RecordKey::Reservation));
                        }
                        Value::Reservation(tx) => {
                            let tx: d::Reservation = tx.clone().try_into().map_err(corrupt)?;
                            if tx.request().authorization().is_some() {
                                keys.insert(RecordKey::Pending);
                            }
                            keys.extend(
                                tx.request()
                                    .operations()
                                    .iter()
                                    .map(|op| RecordKey::Resource(op.resource().clone())),
                            );
                        }
                    }
                }
            }
            if keys.len() == old_len {
                let mut view = View::from_read(read, (self.clock)())?;
                view.terminal_since_ms = terminal_since_ms;
                return Ok(view);
            }
        }
        Err(StoreError::Contention.into())
    }
    fn keys(write: &Write) -> BTreeSet<RecordKey> {
        match write {
            Write::Create(spec) => BTreeSet::from([RecordKey::Resource(spec.id().clone())]),
            Write::Prepare(request) => request
                .operations()
                .iter()
                .map(|op| RecordKey::Resource(op.resource().clone()))
                .chain([RecordKey::Reservation(request.id().clone())])
                .chain(request.authorization().map(|_| RecordKey::Pending))
                .collect(),
            Write::Commit { id, .. } | Write::Abort(id) => {
                BTreeSet::from([RecordKey::Reservation(id.clone())])
            }
            Write::Lapse => BTreeSet::new(),
        }
    }
    fn write(&self, write: &Write) -> Result<Output, Error> {
        self.write_detailed(write).map(|e| e.output)
    }
    fn write_detailed(&self, write: &Write) -> Result<Execution, Error> {
        let since = match write {
            Write::Prepare(request) => uuid_v7_millis(request.id().as_str())
                .unwrap_or(0)
                .saturating_sub(millis(self.window.skew)),
            Write::Commit { .. } | Write::Abort(_) => (self.clock)()
                .saturating_sub(millis(self.window.max_age))
                .saturating_sub(millis(self.window.skew)),
            _ => 0,
        };
        let view = self.load_keys(Self::keys(write), since)?;
        let (_, mut outputs) = self.commit_batch_detailed(view, &[write]);
        outputs.pop().expect("one output per write")
    }
    fn lookup(
        &self,
        view: &View,
        id: &d::Id,
        since_ms: u64,
    ) -> Result<Option<d::Reservation>, Error> {
        Ok(view
            .reservations
            .get(id)
            .filter(|(recorded, tx)| *tx.state() == d::State::Prepared || *recorded >= since_ms)
            .map(|(_, tx)| tx.clone()))
    }
    fn admit(&self, view: &View, request: &d::Request) -> Result<u64, Error> {
        let created = uuid_v7_millis(request.id().as_str()).ok_or_else(|| {
            Error::input("a new reservation ID must be a UUIDv7 in canonical lowercase form")
        })?;
        let skew = millis(self.window.skew);
        if created < view.now_ms.saturating_sub(millis(self.window.max_age)) {
            return Err(Error {
                category: Category::Input,
                code: Code::Expired,
                message: format!(
                    "reservation ID was created at {created} ms, older than the {:?} retry \
                     window; an earlier attempt may have been admitted, but it can no longer \
                     be replayed",
                    self.window.max_age
                ),
                reservation_id: None,
                capacity: None,
            });
        }
        if created > view.now_ms.saturating_add(skew) {
            return Err(Error::input(format!(
                "reservation ID was created at {created} ms, more than {:?} ahead of the \
                 store's clock",
                self.window.skew
            )));
        }
        Ok(created.saturating_sub(skew))
    }
    /// The earliest recording time a retried decision is searched from.
    fn decided_since(&self, view: &View) -> u64 {
        view.now_ms
            .saturating_sub(millis(self.window.max_age))
            .saturating_sub(millis(self.window.skew))
    }
    fn evaluate(&self, view: &View, write: &Write) -> Result<Evaluation, Error> {
        match write {
            Write::Create(spec) => {
                let changes =
                    transition::create(spec.clone(), view.live.resources().get(spec.id()))
                        .map_err(Error::domain)?;
                let next = view.live.apply(&changes);
                let resource = (&next.resources()[spec.id()]).into();
                Ok((changes, Ok(Output::Resource(resource))))
            }
            Write::Prepare(request) => {
                let existing = match view.live.prepared().get(request.id()) {
                    Some(prepared) => Some(prepared.clone()),
                    None => self.lookup(view, request.id(), self.admit(view, request)?)?,
                };
                if existing.is_none()
                    && let Some(facts) = request.authorization()
                {
                    view.pending
                        .reserve(
                            facts.credential_id(),
                            self.pending_limits.tenant,
                            self.pending_limits.credential,
                        )
                        .map_err(|e| {
                            Error::new(Category::Admission, Code::Throttled, e.to_string())
                        })?;
                }
                let changes = transition::prepare(
                    request.clone(),
                    existing.as_ref(),
                    view.live.resources(),
                    view.now_ms,
                )
                .map_err(Error::domain)?;
                let tx = changes
                    .reservation()
                    .or(existing.as_ref())
                    .ok_or(Error::domain(d::Error::InvalidLedger))?;
                let output = match tx.rejection() {
                    Some(reason) => Err(Error::rejection(reason)),
                    None => Ok(Output::Reservation(tx.into())),
                };
                Ok((changes, output))
            }
            Write::Commit { id, actuals } => {
                let tx = self
                    .lookup(view, id, self.decided_since(view))?
                    .ok_or(Error::domain(d::Error::NotFound))?;
                let changes = transition::commit(&tx, actuals.clone(), view.live.resources())
                    .map_err(Error::domain)?;
                let output = changes.reservation().unwrap_or(&tx).into();
                Ok((changes, Ok(Output::Reservation(output))))
            }
            Write::Abort(id) => {
                let tx = self
                    .lookup(view, id, self.decided_since(view))?
                    .ok_or(Error::domain(d::Error::NotFound))?;
                let changes =
                    transition::abort(&tx, view.live.resources()).map_err(Error::domain)?;
                let output = changes.reservation().unwrap_or(&tx).into();
                Ok((changes, Ok(Output::Reservation(output))))
            }
            // Its lapses are all it changes, and `lapse_due` records them.
            Write::Lapse => Ok((
                transition::Changes::default(),
                Ok(Output::Reservations(vec![])),
            )),
        }
    }
}

impl<S: TransactionalStore> Application<S> {
    pub(crate) fn commit_batch(
        &self,
        base: View,
        writes: &[&Write],
    ) -> (Option<View>, Vec<Result<Output, Error>>) {
        let (view, outputs) = self.commit_batch_detailed(base, writes);
        (
            view,
            outputs.into_iter().map(|v| v.map(|e| e.output)).collect(),
        )
    }
    fn commit_batch_detailed(
        &self,
        mut base: View,
        writes: &[&Write],
    ) -> (Option<View>, Vec<Result<Execution, Error>>) {
        let fail = |error: Error| writes.iter().map(|_| Err(error.clone())).collect();
        if base.complete {
            for key in writes.iter().flat_map(|w| Self::keys(w)) {
                base.read.records.entry(key).or_insert(None);
            }
        } else if writes
            .iter()
            .flat_map(|w| Self::keys(w))
            .any(|k| !base.read.records.contains_key(&k))
        {
            let keys = base
                .read
                .records
                .keys()
                .cloned()
                .chain(writes.iter().flat_map(|w| Self::keys(w)))
                .collect();
            match self.load_keys(keys, base.terminal_since_ms) {
                Ok(view) => base = view,
                Err(e) => return (None, fail(e)),
            }
        }

        for _ in 0..self.attempts {
            let mut view = base.clone();
            view.now_ms = view.now_ms.max((self.clock)());
            let mut changes = Vec::new();
            let mut outputs = Vec::with_capacity(writes.len());
            let mut dependencies = BTreeSet::new();
            for write in writes {
                dependencies.extend(Self::keys(write));
                match write {
                    Write::Commit { id, .. } | Write::Abort(id) => {
                        if let Some((_, tx)) = view.reservations.get(id) {
                            dependencies.extend(
                                tx.request()
                                    .operations()
                                    .iter()
                                    .map(|op| RecordKey::Resource(op.resource().clone())),
                            );
                        }
                    }
                    Write::Lapse => dependencies.extend(
                        view.live
                            .resources()
                            .keys()
                            .cloned()
                            .map(RecordKey::Resource),
                    ),
                    _ => (),
                }

                outputs.push(lapse_due(&mut view, write, &mut changes).and_then(
                    |lapsed| match write {
                        Write::Lapse => Ok(Execution {
                            replayed: lapsed.is_empty(),
                            output: Output::Reservations(lapsed.iter().map(Into::into).collect()),
                        }),
                        _ => self.evaluate(&view, write).and_then(|(next, output)| {
                            let replayed = next.is_empty();
                            push(&mut view, &next, &mut changes)?;
                            output.map(|output| Execution { output, replayed })
                        }),
                    },
                ));
            }
            if changes.is_empty() {
                return (Some(view), outputs);
            }
            for (_, change) in &changes {
                if change
                    .reservation()
                    .is_some_and(|tx| tx.request().authorization().is_some())
                {
                    dependencies.insert(RecordKey::Pending);
                }
                dependencies.extend(
                    change
                        .resources()
                        .iter()
                        .map(|r| RecordKey::Resource(r.spec().id().clone())),
                );
                dependencies.extend(
                    change
                        .reservation()
                        .map(|tx| RecordKey::Reservation(tx.request().id().clone())),
                );
            }
            // Hydration validates the connected hold graph. Only the resources
            // and identities used by these transitions are concurrency
            // dependencies; an unrelated participant's quantity is not one.
            let observed = Read {
                records: base
                    .read
                    .records
                    .iter()
                    .filter(|(k, _)| dependencies.contains(*k))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                recorded_at_ms: base.read.recorded_at_ms,
                fence: base.read.fence.clone(),
            };
            let commit = (|| {
                let mut random = [0; 16];
                getrandom::fill(&mut random).map_err(|e| {
                    StoreError::Io(format!("transition identity generation failed: {e}"))
                })?;
                let identity =
                    TransitionId::new(uuid_v7(view.now_ms, u128::from_le_bytes(random)))?;
                Commit::from_changes(self.scope.tenant().clone(), identity, &observed, &changes)
            })();
            let commit = match commit {
                Ok(c) => c,
                Err(e) => return (Some(base), fail(e.into())),
            };
            let outcome = match self.store.commit(&commit) {
                Err(error @ StoreError::Unknown(_)) => match self
                    .store
                    .resolve(self.scope.tenant(), &commit.history().transition_id)
                {
                    Ok(Resolution::Applied { history, receipt })
                        if history == *commit.history() =>
                    {
                        Ok(CommitOutcome::Applied(receipt))
                    }
                    Ok(Resolution::Applied { .. }) => Err(StoreError::Corrupt(
                        "resolved publication has different history".into(),
                    )),
                    Ok(Resolution::NotApplied) => Ok(CommitOutcome::Conflict),
                    Ok(Resolution::Unknown) | Err(_) => Err(error),
                },
                other => other,
            };
            match outcome {
                Ok(CommitOutcome::Applied(receipt)) => match view.received(&commit, receipt) {
                    Ok(()) => {
                        let _ = self.store.maintain(self.checkpoint_interval);
                        return (Some(view), outputs);
                    }
                    Err(e) => {
                        return (
                            None,
                            fail(
                                StoreError::Unknown(format!(
                                    "applied publication has invalid receipt: {e}"
                                ))
                                .into(),
                            ),
                        );
                    }
                },
                Ok(CommitOutcome::Conflict) => {
                    let keys = base
                        .read
                        .records
                        .keys()
                        .cloned()
                        .chain(writes.iter().flat_map(|w| Self::keys(w)))
                        .collect();
                    match self.load_keys(keys, base.terminal_since_ms) {
                        Ok(fresh) => base = fresh,
                        Err(e) => return (None, fail(e)),
                    }
                }
                Err(error) => return (None, fail(error.into())),
            }
        }
        (Some(base), fail(StoreError::Contention.into()))
    }
}
fn push(
    view: &mut View,
    changes: &transition::Changes,
    entries: &mut Vec<(u64, transition::Changes)>,
) -> Result<(), Error> {
    if !changes.is_empty() {
        view.advance(changes)?;
        entries.push((view.now_ms, changes.clone()));
    }
    Ok(())
}
fn lapse_due(
    view: &mut View,
    write: &Write,
    entries: &mut Vec<(u64, transition::Changes)>,
) -> Result<Vec<d::Reservation>, Error> {
    let (live, now) = (&view.live, view.now_ms);
    let on = |request: &d::Request| {
        live.due(request.operations().iter().map(d::Operation::resource), now)
    };
    let due: Vec<_> = match write {
        Write::Create(_) => vec![],
        Write::Lapse => live.due(live.resources().keys(), now),
        Write::Prepare(request) => on(request),
        Write::Commit { id, .. } | Write::Abort(id) => live
            .prepared()
            .get(id)
            .map(|tx| on(tx.request()))
            .unwrap_or_default(),
    }
    .into_iter()
    .cloned()
    .collect();
    let mut lapsed = Vec::new();
    for tx in due {
        let changes = transition::lapse(&tx, view.live.resources(), now).map_err(Error::domain)?;
        push(view, &changes, entries)?;
        lapsed.extend(changes.reservation().cloned());
    }
    Ok(lapsed)
}

/// Startup is fail-closed: the adapter probes the actual authority before any
/// command is admitted. Per-request profile requirements are checked again.
pub struct ReadyApplication<S> {
    app: Application<S>,
    capabilities: capabilities::Capabilities,
    authority_generation: String,
}
impl<S: TransactionalStore + Readiness> ReadyApplication<S> {
    pub fn start(app: Application<S>, required: DurabilityProfile) -> Result<Self, Error> {
        let capabilities = app.store.readiness()?;
        capabilities.require(required)?;
        let authority_generation = app.store.identity()?;
        if crate::uuid_v7_millis(&authority_generation).is_none() {
            return Err(corrupt("authority identity is not a canonical UUIDv7"));
        }
        Ok(Self {
            app,
            capabilities,
            authority_generation,
        })
    }
    pub fn execute_authorized(
        &self,
        context: &crate::access::AccessContext,
        command: Command,
        required: DurabilityProfile,
    ) -> Result<Execution, Error> {
        self.capabilities.require(required)?;
        if !self.capabilities.multi_tenant && *context.scope() != Scope::local() {
            return Err(Error::new(
                Category::Authorization,
                Code::Forbidden,
                "authority supports only its implicit local tenant and namespace",
            ));
        }
        self.app.execute_authorized(context, command)
    }
    pub fn inspect_preparation(
        &self,
        context: &crate::access::AccessContext,
        request: dto::Prepare,
        required: DurabilityProfile,
    ) -> Result<Option<dto::Reservation>, Error> {
        self.capabilities.require(required)?;
        if !self.capabilities.multi_tenant && *context.scope() != Scope::local() {
            return Err(Error::new(
                Category::Authorization,
                Code::Forbidden,
                "authority supports only its implicit local tenant and namespace",
            ));
        }
        self.app.inspect_preparation(context, request)
    }
    pub fn authority_generation(&self) -> &str {
        &self.authority_generation
    }
    pub fn capabilities(&self) -> &capabilities::Capabilities {
        &self.capabilities
    }
    pub fn execute(
        &self,
        command: Command,
        required: DurabilityProfile,
    ) -> Result<capabilities::Response, Error> {
        self.capabilities.require(required)?;
        self.app
            .execute(command)
            .map(|output| capabilities::Response {
                durability_profile: self.capabilities.durability_profile,
                output,
            })
    }
}

pub(crate) fn resource_in(view: &View, id: &str) -> Result<dto::Resource, Error> {
    let id = d::Id::new(id.into()).map_err(input)?;
    view.live
        .resources()
        .get(&id)
        .map(Into::into)
        .ok_or_else(|| Error::domain(d::Error::NotFound))
}
