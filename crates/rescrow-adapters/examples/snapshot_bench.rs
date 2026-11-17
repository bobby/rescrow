//! Small local benchmark; synthetic retained history, real locking/sync/publication.
//! cargo run --release -p rescrow-adapters --example snapshot_bench -- target/bench
use rescrow_adapters::file::FileStore;
use rescrow_application::{Application, SnapshotStore, dto::*};
use std::{fs, path::PathBuf, time::Instant};

fn median(mut samples: Vec<u128>) -> u128 {
    samples.sort_unstable();
    samples[samples.len() / 2]
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let parent = PathBuf::from(
        std::env::args_os()
            .nth(1)
            .ok_or("supply an existing benchmark parent directory")?,
    );
    println!("history,snapshot_bytes,inspect_us,replay_us,prepare_abort_us");
    for count in [0, 1_000, 10_000] {
        let path = parent.join(format!("rescrow-bench-{}-{count}", std::process::id()));
        let store = FileStore::new(&path)?;
        store.initialize(
            &Snapshot {
                schema_version: 1,
                revision: 0,
                resources: vec![],
                transactions: vec![],
            }
            .try_into()?,
        )?;
        let app = Application::new(FileStore::new(&path)?);
        app.execute(Command::CreateResource(ResourceSpec {
            resource_id: "r".into(),
            lower: "0".into(),
            upper: Some("10".into()),
            initial: "5".into(),
            unit: "units".into(),
            scale: 0,
        }))?;
        let mut seed = store.load()?.to_dto();
        let before = seed.revision;
        seed.revision += 1;
        for index in 0..count {
            seed.transactions.push(Transaction {
                request: Prepare {
                    transaction_id: format!("history-{index:06}"),
                    operations: vec![Operation {
                        operation_id: "o".into(),
                        resource_id: "r".into(),
                        delta: "-1".into(),
                    }],
                },
                state: State::Committed {
                    actuals: vec![Actual {
                        operation_id: "o".into(),
                        delta: "0".into(),
                    }],
                },
            });
        }
        store.replace(before, &seed.clone().try_into()?)?;
        let replay = Command::CreateResource(seed.resources[0].spec.clone());
        let bytes = fs::metadata(path.join("state.json"))?.len();
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
            let id = format!("sample-{index}");
            let start = Instant::now();
            app.execute(Command::Prepare(Prepare {
                transaction_id: id.clone(),
                operations: vec![Operation {
                    operation_id: "o".into(),
                    resource_id: "r".into(),
                    delta: "-1".into(),
                }],
            }))?;
            app.execute(Command::Abort { transaction_id: id })?;
            if index > 1 {
                inspect.push(read);
                replays.push(retry);
                mutations.push(start.elapsed().as_micros());
            }
        }
        println!(
            "{count},{bytes},{},{},{}",
            median(inspect),
            median(replays),
            median(mutations)
        );
        fs::remove_dir_all(path)?;
    }
    Ok(())
}
