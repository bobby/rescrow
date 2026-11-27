//! Small local benchmark of how point operations scale with retained history.
//! History is built through the Application, so seeding pays real appends. Each
//! seeded transaction is created ten minutes after the last, so the retained
//! history lies outside a new request's clock-skew bound, as it would in use.
//! cargo run --release -p rescrow-adapters --example history_bench -- target/bench
use rescrow_adapters::file::FileStore;
use rescrow_application::{Application, dto::*, system_clock, uuid_v7};
use std::{
    fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

const SPACING_MS: u64 = 10 * 60 * 1000;

fn median(mut samples: Vec<u128>) -> u128 {
    samples.sort_unstable();
    samples[samples.len() / 2]
}
fn prepare(id: &str) -> Command {
    Command::Prepare(Prepare {
        namespace: None,
        transaction_id: id.into(),
        operations: vec![Operation {
            operation_id: "o".into(),
            resource_id: "r".into(),
            delta: "-1".into(),
        }],
        hold_ms: None,
    })
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let parent = PathBuf::from(
        std::env::args_os()
            .nth(1)
            .ok_or("supply an existing benchmark parent directory")?,
    );
    println!("history,live_bytes,log_bytes,inspect_us,replay_us,prepare_abort_us");
    for count in [0, 1_000, 10_000] {
        let path = parent.join(format!("rescrow-bench-{}-{count}", std::process::id()));
        let now = system_clock();
        let seed_clock = Arc::new(AtomicU64::new(now - (count + 1) * SPACING_MS));
        let clock = seed_clock.clone();
        let seeder = Application::new(FileStore::new(&path)?)
            .with_clock(move || clock.load(Ordering::Relaxed));
        seeder.initialize()?;
        let spec = ResourceSpec {
            resource_id: "r".into(),
            lower: "0".into(),
            upper: Some("10".into()),
            initial: "5".into(),
            unit: "units".into(),
            scale: 0,
        };
        seeder.execute(Command::CreateResource(spec.clone()))?;
        for index in 0..count {
            let created = seed_clock.fetch_add(SPACING_MS, Ordering::Relaxed);
            let id = uuid_v7(created, index.into());
            seeder.execute(prepare(&id))?;
            seeder.execute(Command::Commit(Commit {
                transaction_id: id,
                actuals: Some(vec![Actual {
                    operation_id: "o".into(),
                    delta: "0".into(),
                }]),
            }))?;
        }
        let app = Application::new(FileStore::new(&path)?);
        let replay = Command::CreateResource(spec);
        let live_bytes = fs::metadata(path.join("live.json"))?.len();
        let log_bytes = fs::read_dir(path.join("log"))?
            .map(|entry| Ok(entry?.metadata()?.len()))
            .sum::<std::io::Result<u64>>()?;
        let mut inspect = vec![];
        let mut replays = vec![];
        let mut mutations = vec![];
        for index in 0..9 {
            let start = Instant::now();
            std::hint::black_box(app.execute(Command::ShowResource {
                resource_id: "r".into(),
            })?);
            let read = start.elapsed().as_micros();
            let start = Instant::now();
            std::hint::black_box(app.execute(replay.clone())?);
            let retry = start.elapsed().as_micros();
            let id = uuid_v7(system_clock(), u128::from(count + index));
            let start = Instant::now();
            app.execute(prepare(&id))?;
            app.execute(Command::Abort { transaction_id: id })?;
            if index > 1 {
                inspect.push(read);
                replays.push(retry);
                mutations.push(start.elapsed().as_micros());
            }
        }
        println!(
            "{count},{live_bytes},{log_bytes},{},{},{}",
            median(inspect),
            median(replays),
            median(mutations)
        );
        fs::remove_dir_all(path)?;
    }
    Ok(())
}
