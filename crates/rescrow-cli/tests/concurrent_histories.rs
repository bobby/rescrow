//! CLI process histories, checked against the same independent specification as
//! the sequential Application properties. Unix runs are smoke coverage; the
//! documented file-store qualification profile remains Linux.
#![cfg(unix)]

#[path = "support/history.rs"]
mod history;
#[path = "../../rescrow-adapters/tests/support/escrow_model.rs"]
mod model;

use history::{Call, History, Response, check};
use model::{Command, Model, Op, Spec};
use rescrow_application::{Error, dto};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command as Process, Output},
    sync::{
        Barrier,
        atomic::{AtomicU64, Ordering},
    },
    thread,
};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Case(PathBuf);
impl Case {
    fn new(case: u8) -> Self {
        let parent = std::env::var_os("RESCROW_TEST_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/concurrent-histories")
            });
        fs::create_dir_all(&parent).unwrap();
        let root = parent.join(format!(
            "history-{}-{case}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        Self(root)
    }
    fn process(&self) -> Process {
        let mut process = Process::new(env!("CARGO_BIN_EXE_rescrow"));
        process
            .arg("--store")
            .arg(self.0.join("store"))
            .arg("--json");
        process
    }
    fn input(&self, name: &str, value: &impl serde::Serialize) -> PathBuf {
        let path = self.0.join(format!("{name}.json"));
        fs::write(&path, serde_json::to_vec(value).unwrap()).unwrap();
        path
    }
    fn command(&self, command: &Command, name: &str) -> Process {
        let mut process = self.process();
        match command.dto() {
            dto::Command::CreateResource(spec) => {
                process.args([
                    "resource",
                    "create",
                    &spec.resource_id,
                    "--lower",
                    &spec.lower,
                    "--initial",
                    &spec.initial,
                    "--unit",
                    &spec.unit,
                    "--scale",
                    &spec.scale.to_string(),
                ]);
                if let Some(upper) = spec.upper {
                    process.args(["--upper", &upper]);
                }
            }
            dto::Command::Prepare(request) => {
                process
                    .args([
                        "transaction",
                        "prepare",
                        &request.transaction_id,
                        "--operations",
                    ])
                    .arg(self.input(name, &request.operations));
            }
            dto::Command::Commit(request) => {
                process.args(["transaction", "commit", &request.transaction_id]);
                if let Some(actuals) = request.actuals {
                    process.arg("--actuals").arg(self.input(name, &actuals));
                }
            }
            dto::Command::Abort { transaction_id } => {
                process.args(["transaction", "abort", &transaction_id]);
            }
            dto::Command::ShowResource { resource_id } => {
                process.args(["resource", "show", &resource_id]);
            }
            dto::Command::ListResources => {
                process.args(["resource", "list"]);
            }
            dto::Command::ShowTransaction { transaction_id } => {
                process.args(["transaction", "show", &transaction_id]);
            }
            dto::Command::ListTransactions => {
                process.args(["transaction", "list"]);
            }
        }
        process
    }
}
impl Drop for Case {
    fn drop(&mut self) {
        // Preserve inputs and the live ledger even on a harness/setup panic.
        if !thread::panicking() {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
}

fn response(output: Output) -> Response {
    let result = if output.status.success() {
        serde_json::from_slice::<dto::Output>(&output.stdout)
            .ok()
            .map(Ok)
    } else {
        serde_json::from_slice::<Error>(&output.stderr)
            .ok()
            .map(Err)
    };
    Response {
        exit: output.status.code(),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        result,
    }
}

fn operations(delta: i64) -> Vec<Op> {
    vec![
        Op {
            id: 0,
            resource: 0,
            delta,
        },
        Op {
            id: 1,
            resource: 1,
            delta: -delta,
        },
    ]
}

fn workload(case: u8) -> (Vec<Command>, Vec<Vec<Command>>) {
    let sign = if case & 1 == 0 { -1 } else { 1 };
    let setup = vec![
        Command::Create(
            0,
            Spec {
                lower: 0,
                upper: Some(10),
                initial: 5,
                unit: 0,
                scale: 0,
            },
        ),
        Command::Create(
            1,
            Spec {
                lower: 0,
                upper: if case & 8 == 0 { Some(10) } else { None },
                initial: 5,
                unit: 1,
                scale: 0,
            },
        ),
        Command::Prepare(0, operations(sign * 3)),
        Command::Prepare(1, operations(-sign * 2)),
    ];
    let fresh = operations(sign * (1 + i64::from(case % 3)));
    let mut retry = fresh.clone();
    retry.reverse();
    if case & 2 != 0 {
        retry[0].delta += sign;
    }
    let partial = Some(vec![(0, sign), (1, 0)]);
    let read = match case % 3 {
        0 => Command::Transaction(0),
        1 => Command::Transactions,
        _ => Command::Resource(0),
    };
    let clients = vec![
        vec![
            Command::Prepare(2, fresh),
            Command::Commit(2, partial.clone()),
            Command::Prepare(2, retry),
            Command::Commit(2, partial.clone()),
        ],
        vec![
            Command::Commit(0, partial),
            Command::Abort(1),
            Command::Prepare(0, operations(sign * 3).into_iter().rev().collect()),
            read,
        ],
        vec![
            Command::Abort(0),
            Command::Prepare(3, operations(-sign * 5)),
            Command::Commit(
                3,
                if case & 4 == 0 {
                    None
                } else {
                    Some(vec![(0, 0), (1, 0)])
                },
            ),
            Command::Resources,
        ],
    ];
    (setup, clients)
}

fn run(case: u8) {
    let directory = Case::new(case);
    assert!(
        directory
            .process()
            .arg("init")
            .output()
            .unwrap()
            .status
            .success()
    );
    let (setup, clients) = workload(case);
    directory.input("setup", &setup);
    let mut model = Model::default();
    let mut revision = 0;
    for (i, command) in setup.iter().enumerate() {
        let old = model.clone();
        model.apply(command).unwrap();
        revision += u64::from(old != model);
        let actual = response(
            directory
                .command(command, &format!("setup-{i}"))
                .output()
                .unwrap(),
        );
        assert_eq!(actual.exit, Some(0), "{actual:?}");
        assert!(actual.stderr.is_empty(), "{actual:?}");
        assert_eq!(actual.result, Some(Ok(model.output(command, revision))));
    }
    // Prepare all input files before recording any invocation. The first call
    // of each client enters the barrier after invocation and before spawning,
    // ensuring overlapping call intervals without presuming an OS schedule.
    let clients: Vec<_> = clients
        .into_iter()
        .enumerate()
        .map(|(client, commands)| {
            commands
                .into_iter()
                .enumerate()
                .map(|(i, command)| {
                    let process = directory.command(&command, &format!("client-{client}-{i}"));
                    (command, process)
                })
                .collect::<Vec<_>>()
        })
        .collect();
    let barrier = Barrier::new(clients.len());
    let clock = AtomicU64::new(0);
    let calls = thread::scope(|scope| {
        let handles: Vec<_> = clients
            .into_iter()
            .map(|commands| {
                let (barrier, clock) = (&barrier, &clock);
                scope.spawn(move || {
                    commands
                        .into_iter()
                        .enumerate()
                        .map(|(i, (command, mut process))| {
                            let invocation = clock.fetch_add(1, Ordering::SeqCst);
                            if i == 0 {
                                barrier.wait();
                            }
                            let output = process.output().unwrap();
                            let completion = clock.fetch_add(1, Ordering::SeqCst);
                            Call {
                                command,
                                invocation,
                                completion,
                                response: response(output),
                            }
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|handle| handle.join().unwrap())
            .collect()
    });
    // Retain the observations even if validating the final ledger fails.
    directory.input("calls", &calls);
    let history = History {
        case,
        setup,
        calls,
        final_snapshot: rescrow_application::Application::new(
            rescrow_adapters::file::FileStore::new(directory.0.join("store")).unwrap(),
        )
        .snapshot()
        .unwrap(),
    };
    if let Err(error) = check(&history) {
        let path = directory.input("history", &history);
        panic!(
            "case {case}: {error}; recorded history: {}; inputs and store retained in {}",
            path.display(),
            directory.0.display()
        );
    }
}

#[test]
fn mixed_cli_histories_match_independent_model() {
    if let Some(path) = std::env::var_os("RESCROW_HISTORY_REPLAY") {
        let path = Path::new(&path);
        let history: History = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        check(&history).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
    } else if let Ok(case) = std::env::var("RESCROW_HISTORY_CASE") {
        let case: u8 = case.parse().expect("RESCROW_HISTORY_CASE must be 0..=15");
        assert!(case < 16, "RESCROW_HISTORY_CASE must be 0..=15");
        run(case);
    } else {
        for case in 0..16 {
            run(case);
        }
    }
}
