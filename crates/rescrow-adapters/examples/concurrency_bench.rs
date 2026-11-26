//! Concurrent writers and readers against one file store.
//!
//! Each writer thread does prepare/abort pairs; each reader thread reads a
//! resource until the writers finish. In `clients` mode every thread opens the
//! store itself, so its file locks contend exactly as separate processes'
//! would. In `service` mode the threads share one long-lived `Service`.
//!
//! cargo run --release -p rescrow-adapters --example concurrency_bench -- target/bench [writers readers [clients|service]]
use rescrow_adapters::file::FileStore;
use rescrow_application::{
    Application, Code, Error, dto::*, service::Service, system_clock, uuid_v7,
};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

/// Prepare/abort pairs per writer.
const PAIRS: usize = 25;
/// Each reader's pause between reads, in both modes. A reader that never
/// pauses or blocks keeps a core busy, and with more threads than cores it
/// can starve the service's single writer thread of CPU; requests arriving at
/// a server leave such gaps.
const READ_PAUSE: Duration = Duration::from_micros(100);

fn app(path: &Path) -> Application<FileStore> {
    Application::new(FileStore::new(path).unwrap())
}
fn prepare(id: &str) -> Command {
    Command::Prepare(Prepare {
        transaction_id: id.into(),
        operations: vec![Operation {
            operation_id: "o".into(),
            resource_id: "r".into(),
            delta: "-1".into(),
        }],
    })
}
/// How the threads reach the store.
#[derive(Clone)]
enum Client {
    /// Each thread opens its own store, like separate processes.
    Own(PathBuf),
    Shared(Arc<Service<FileStore>>),
}
impl Client {
    fn connect(&self) -> Box<dyn Fn(Command) -> Result<Output, Error>> {
        match self {
            Self::Own(path) => {
                let app = app(path);
                Box::new(move |c| app.execute(c))
            }
            Self::Shared(service) => {
                let service = service.clone();
                Box::new(move |c| service.execute(c))
            }
        }
    }
}
type Execute = dyn Fn(Command) -> Result<Output, Error>;
/// Retries contention, as a client following the contract would, and counts it.
fn write(execute: &Execute, command: Command, contended: &AtomicU64) -> Duration {
    let start = Instant::now();
    loop {
        match execute(command.clone()) {
            Ok(_) => return start.elapsed(),
            Err(e) if e.code == Code::Contention => {
                contended.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => panic!("{e}"),
        }
    }
}
fn percentile(sorted: &[Duration], p: usize) -> f64 {
    sorted[(sorted.len() - 1) * p / 100].as_secs_f64() * 1e3
}
fn run(path: &Path, writers: usize, readers: usize, service: bool) {
    let setup = app(path);
    setup.initialize().unwrap();
    setup
        .execute(Command::CreateResource(ResourceSpec {
            resource_id: "r".into(),
            lower: "0".into(),
            upper: None,
            initial: "1000000".into(),
            unit: "units".into(),
            scale: 0,
        }))
        .unwrap();
    let client = if service {
        Client::Shared(Arc::new(Service::start(app(path)).unwrap()))
    } else {
        Client::Own(path.to_owned())
    };
    let contended = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let reads = Arc::new(AtomicU64::new(0));
    let reading: Vec<_> = (0..readers)
        .map(|_| {
            let (client, done, reads) = (client.clone(), done.clone(), reads.clone());
            thread::spawn(move || {
                let execute = client.connect();
                while !done.load(Ordering::Relaxed) {
                    execute(Command::ShowResource {
                        resource_id: "r".into(),
                    })
                    .unwrap();
                    reads.fetch_add(1, Ordering::Relaxed);
                    thread::sleep(READ_PAUSE);
                }
            })
        })
        .collect();
    let start = Instant::now();
    let writing: Vec<_> = (0..writers)
        .map(|w| {
            let (client, contended) = (client.clone(), contended.clone());
            thread::spawn(move || {
                let execute = client.connect();
                let mut latencies = Vec::new();
                for pair in 0..PAIRS {
                    let id = uuid_v7(system_clock(), (w * PAIRS + pair) as u128);
                    latencies.push(write(&*execute, prepare(&id), &contended));
                    let abort = Command::Abort { transaction_id: id };
                    latencies.push(write(&*execute, abort, &contended));
                }
                latencies
            })
        })
        .collect();
    let mut latencies: Vec<Duration> = writing
        .into_iter()
        .flat_map(|t| t.join().unwrap())
        .collect();
    let elapsed = start.elapsed().as_secs_f64();
    done.store(true, Ordering::Relaxed);
    for t in reading {
        t.join().unwrap();
    }
    drop(client);
    latencies.sort_unstable();
    let snapshot = setup.snapshot().unwrap();
    assert_eq!(snapshot.resources[0].quantity, "1000000");
    assert!(snapshot.resources[0].holds.is_empty());
    println!(
        "{},{writers},{readers},{:.0},{:.1},{:.1},{:.1},{},{:.0}",
        if service { "service" } else { "clients" },
        latencies.len() as f64 / elapsed,
        percentile(&latencies, 50),
        percentile(&latencies, 99),
        percentile(&latencies, 100),
        contended.load(Ordering::Relaxed),
        reads.load(Ordering::Relaxed) as f64 / elapsed,
    );
}
fn main() {
    let parent = PathBuf::from(
        std::env::args_os()
            .nth(1)
            .expect("supply an existing benchmark parent directory"),
    );
    // Optionally a single configuration: writers, readers, and mode.
    let args: Vec<String> = std::env::args().skip(2).collect();
    let count = |a: &String| a.parse::<usize>().expect("writers and readers are counts");
    let service = args.get(2).is_some_and(|mode| mode == "service");
    let configs = match &args[..] {
        [writers, readers, ..] => vec![(count(writers), count(readers))],
        _ => vec![(1, 0), (8, 0), (8, 8), (16, 16)],
    };
    println!("mode,writers,readers,writes_per_s,p50_ms,p99_ms,max_ms,contended,reads_per_s");
    for (writers, readers) in configs {
        let path = parent.join(format!("rescrow-concurrency-{}", std::process::id()));
        run(&path, writers, readers, service);
        fs::remove_dir_all(path).unwrap();
    }
}
