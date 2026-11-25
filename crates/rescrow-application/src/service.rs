//! A long-lived Application for a server or embedded process.
//!
//! Reads come from the last durable state, held in memory and swapped
//! atomically, so they take no lock and do no I/O. One writer thread takes
//! every write waiting for it, evaluates them in order against that state, and
//! appends all their records together with one synchronization (group
//! commit). Each write's caller is answered once its record is durable.
//!
//! The service's state includes only its own writes and those it reloaded
//! after losing a race. If other processes also write the store, they stay
//! safe, because appends are still conditional, but reads here can lag their
//! writes until this service next writes.
use crate::{
    Application, Error, LogStore, StoreError, View, Write, dto::Command, dto::Output, resource_in,
};
use arc_swap::ArcSwap;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    thread,
};

/// The most writes committed together.
const MAX_BATCH: usize = 256;

struct Job {
    write: Write,
    reply: mpsc::SyncSender<Result<Output, Error>>,
}

/// Counts of appended batches and the writes they carried.
#[derive(Debug, Default)]
pub struct Stats {
    pub batches: AtomicU64,
    pub writes: AtomicU64,
}

pub struct Service<S> {
    app: Arc<Application<S>>,
    current: Arc<ArcSwap<View>>,
    jobs: Option<mpsc::Sender<Job>>,
    writer: Option<thread::JoinHandle<()>>,
    stats: Arc<Stats>,
}
impl<S: LogStore + Send + Sync + 'static> Service<S> {
    /// Load the store and start the writer thread.
    pub fn start(app: Application<S>) -> Result<Self, Error> {
        let view = app.view()?;
        let app = Arc::new(app);
        let current = Arc::new(ArcSwap::from_pointee(view.clone()));
        let stats = Arc::<Stats>::default();
        let (jobs, queue) = mpsc::channel();
        let writer = {
            let (app, current, stats) = (app.clone(), current.clone(), stats.clone());
            thread::Builder::new()
                .name("rescrow-writer".into())
                .spawn(move || run(&app, &current, &stats, view, &queue))
                .map_err(|e| StoreError::Io(e.to_string()))?
        };
        Ok(Self {
            app,
            current,
            jobs: Some(jobs),
            writer: Some(writer),
            stats,
        })
    }
    /// The same commands and results as [`Application::execute`], except that
    /// `ListResources` reads the validated live state rather than replaying
    /// all of history.
    pub fn execute(&self, command: Command) -> Result<Output, Error> {
        match command {
            Command::CreateResource(spec) => self.write(Write::create(spec)),
            Command::Prepare(request) => {
                let id = request.transaction_id.clone();
                self.write(Write::prepare(request))
                    .map_err(|e| e.with_transaction(&id))
            }
            Command::Commit(command) => {
                let id = command.transaction_id.clone();
                self.write(Write::commit(command))
                    .map_err(|e| e.with_transaction(&id))
            }
            Command::Abort { transaction_id } => self
                .write(Write::abort(&transaction_id))
                .map_err(|e| e.with_transaction(&transaction_id)),
            Command::ShowResource { resource_id } => {
                resource_in(&self.current.load(), &resource_id).map(Output::Resource)
            }
            Command::ListResources => Ok(Output::Resources(
                self.current
                    .load()
                    .live()
                    .resources()
                    .values()
                    .map(Into::into)
                    .collect(),
            )),
            Command::ShowTransaction { transaction_id } => self
                .app
                .transaction_in(&self.current.load(), &transaction_id)
                .map(Output::Transaction),
            Command::ListTransactions => self.app.transactions().map(Output::Transactions),
        }
    }
    pub fn stats(&self) -> &Stats {
        &self.stats
    }
    fn write(&self, write: Result<Write, Error>) -> Result<Output, Error> {
        let (reply, response) = mpsc::sync_channel(1);
        let job = Job {
            write: write?,
            reply,
        };
        let stopped = || Error::from(StoreError::Io("the service's writer has stopped".into()));
        self.jobs
            .as_ref()
            .ok_or_else(stopped)?
            .send(job)
            .map_err(|_| stopped())?;
        response.recv().map_err(|_| stopped())?
    }
}
impl<S> Drop for Service<S> {
    fn drop(&mut self) {
        self.jobs.take();
        if let Some(writer) = self.writer.take() {
            let _ = writer.join();
        }
    }
}
/// The writer thread. It owns the latest durable state, or none after an
/// unknown outcome, when the next batch reloads it.
fn run<S: LogStore>(
    app: &Application<S>,
    current: &ArcSwap<View>,
    stats: &Stats,
    view: View,
    queue: &mpsc::Receiver<Job>,
) {
    let mut state = Some(view);
    while let Ok(first) = queue.recv() {
        let mut jobs = vec![first];
        jobs.extend(queue.try_iter().take(MAX_BATCH - 1));
        let writes: Vec<&Write> = jobs.iter().map(|job| &job.write).collect();
        let (next, outputs) = match state.take().map_or_else(|| app.view(), Ok) {
            Ok(view) => app.commit_batch(view, &writes),
            Err(e) => (None, writes.iter().map(|_| Err(e.clone())).collect()),
        };
        if let Some(next) = &next {
            current.store(Arc::new(next.clone()));
        }
        state = next;
        stats.batches.fetch_add(1, Ordering::Relaxed);
        stats.writes.fetch_add(jobs.len() as u64, Ordering::Relaxed);
        for (job, output) in jobs.into_iter().zip(outputs) {
            let _ = job.reply.send(output);
        }
    }
}
