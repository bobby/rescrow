use clap::{Parser, Subcommand};
use rescrow_adapters::file::{FileStore, MAX_SNAPSHOT_BYTES};
use rescrow_application::{Application, Category, Code, Error, dto::*};
use std::{
    fs::File,
    io::{self, Read, Write},
    path::{Path, PathBuf},
    process::ExitCode,
};

#[derive(Parser)]
#[command(name = "rescrow", version, about = "Cooperative local escrow budgets")]
struct Cli {
    #[arg(long)]
    store: PathBuf,
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Top,
}
#[derive(Subcommand)]
enum Top {
    /// Initialize an empty store. Never overwrites an existing state.
    Init,
    Resource {
        #[command(subcommand)]
        command: ResourceCommand,
    },
    Transaction {
        #[command(subcommand)]
        command: TransactionCommand,
    },
}
#[derive(Subcommand)]
enum ResourceCommand {
    Create {
        id: String,
        #[arg(long, default_value = "0", allow_hyphen_values = true)]
        lower: String,
        #[arg(long, allow_hyphen_values = true)]
        upper: Option<String>,
        #[arg(long, allow_hyphen_values = true)]
        initial: String,
        #[arg(long)]
        unit: String,
        #[arg(long, default_value_t = 0)]
        scale: u32,
    },
    Show {
        id: String,
    },
    List,
}
#[derive(Subcommand)]
enum TransactionCommand {
    Prepare {
        id: String,
        #[arg(long)]
        operations: PathBuf,
    },
    Show {
        id: String,
    },
    List,
    Commit {
        id: String,
        #[arg(long)]
        actuals: Option<PathBuf>,
    },
    Abort {
        id: String,
    },
}
fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(&cli) {
        Ok(output) => {
            let rendered = if cli.json {
                serde_json::to_string(&output).map_err(io::Error::other)
            } else {
                Ok(human(&output))
            };
            match rendered.and_then(|v| writeln!(io::stdout().lock(), "{v}")) {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => {
                    eprintln!("output failed: {e}; inspect/retry the same transaction identity");
                    ExitCode::from(7)
                }
            }
        }
        Err(error) => {
            if cli.json {
                eprintln!(
                    "{}",
                    serde_json::to_string(&error)
                        .expect("error contains only serializable scalars")
                );
            } else {
                eprintln!("{error}");
                if error.category == Category::UnknownOutcome {
                    eprintln!(
                        "Publication may have succeeded; inspect/retry the same transaction ID and request."
                    );
                }
            }
            ExitCode::from(exit_code(&error))
        }
    }
}
fn run(cli: &Cli) -> Result<Output, Error> {
    // Convert files to transfer values before attempting any storage effects.
    let command = match &cli.command {
        Top::Init => None,
        Top::Resource { command } => Some(match command {
            ResourceCommand::Create {
                id,
                lower,
                upper,
                initial,
                unit,
                scale,
            } => Command::CreateResource(ResourceSpec {
                resource_id: id.clone(),
                lower: lower.clone(),
                upper: upper.clone(),
                initial: initial.clone(),
                unit: unit.clone(),
                scale: *scale,
            }),
            ResourceCommand::Show { id } => Command::ShowResource {
                resource_id: id.clone(),
            },
            ResourceCommand::List => Command::ListResources,
        }),
        Top::Transaction { command } => Some(match command {
            TransactionCommand::Prepare { id, operations } => Command::Prepare(Prepare {
                transaction_id: id.clone(),
                operations: read_json(operations)?,
            }),
            TransactionCommand::Show { id } => Command::ShowTransaction {
                transaction_id: id.clone(),
            },
            TransactionCommand::List => Command::ListTransactions,
            TransactionCommand::Commit { id, actuals } => Command::Commit(Commit {
                transaction_id: id.clone(),
                actuals: actuals.as_deref().map(read_json).transpose()?,
            }),
            TransactionCommand::Abort { id } => Command::Abort {
                transaction_id: id.clone(),
            },
        }),
    };
    let app = Application::new(FileStore::new(&cli.store)?);
    match command {
        Some(c) => app.execute(c),
        None => app.initialize(),
    }
}
fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, Error> {
    let file = File::open(path).map_err(|e| Error::input(format!("{}: {e}", path.display())))?;
    let mut bytes = Vec::new();
    file.take(MAX_SNAPSHOT_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| Error::input(e.to_string()))?;
    if bytes.len() as u64 > MAX_SNAPSHOT_BYTES {
        return Err(Error::input("input JSON exceeds 16 MiB"));
    }
    serde_json::from_slice(&bytes).map_err(|e| Error::input(format!("{}: {e}", path.display())))
}
fn human(output: &Output) -> String {
    match output {
        Output::Initialized => "Initialized empty local store.".into(),
        Output::Resource(r) => resource_text(r),
        Output::Resources(rs) => {
            if rs.is_empty() {
                "No resources.".into()
            } else {
                rs.iter().map(resource_text).collect::<Vec<_>>().join("\n")
            }
        }
        Output::Transaction(t) => transaction_text(t),
        Output::Transactions(ts) => {
            if ts.is_empty() {
                "No transactions.".into()
            } else {
                ts.iter()
                    .map(transaction_text)
                    .collect::<Vec<_>>()
                    .join("\n")
            }
        }
    }
}
fn resource_text(r: &Resource) -> String {
    format!(
        "{}: quantity={} {} (scale {}), bounds=[{}, {}], reserved down={} up={}, available down={} up={} (numeric room)",
        r.spec.resource_id,
        r.quantity,
        r.spec.unit,
        r.spec.scale,
        r.spec.lower,
        r.spec.upper.as_deref().unwrap_or("unbounded"),
        r.reserved_down,
        r.reserved_up,
        r.available_down,
        r.available_up
    )
}
fn transaction_text(t: &Transaction) -> String {
    let state = match &t.state {
        State::Prepared {} => "prepared".into(),
        State::Committed { actuals } => format!(
            "committed ({})",
            actuals
                .iter()
                .map(|a| format!("{}={}", a.operation_id, a.delta))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        State::Aborted { cause } => format!("aborted ({cause:?})"),
    };
    format!(
        "{}: {state}; operations: {}",
        t.request.transaction_id,
        t.request
            .operations
            .iter()
            .map(|o| format!("{}:{}={}", o.operation_id, o.resource_id, o.delta))
            .collect::<Vec<_>>()
            .join(", ")
    )
}
fn exit_code(error: &Error) -> u8 {
    match error.code {
        Code::InvalidInput => 2,
        Code::Capacity | Code::Overflow => {
            if error.category == Category::Input {
                2
            } else {
                3
            }
        }
        Code::Conflict | Code::NotFound | Code::AlreadyInitialized => 4,
        Code::Contention => 5,
        Code::UnknownOutcome => 7,
        _ => 6,
    }
}
