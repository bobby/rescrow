use clap::{Parser, Subcommand};
use rescrow_adapters::file::{FileStore, MAX_RECORD_BYTES};
use rescrow_application::{Application, Category, Code, Error, dto::*};
use std::{
    fs::File,
    io::{self, Read, Write},
    path::{Path, PathBuf},
    process::ExitCode,
};
#[cfg(feature = "remote")]
mod remote;

#[derive(Parser)]
#[command(name = "rescrow", version, about = "Cooperative local escrow budgets")]
struct Cli {
    /// The store directory; required by every command except `new-id`.
    #[arg(long)]
    #[cfg_attr(feature = "remote", arg(conflicts_with = "endpoint"))]
    store: Option<PathBuf>,
    /// Explicit HTTP origins; alternate endpoints require --authority-generation.
    #[cfg(feature = "remote")]
    #[arg(long, action = clap::ArgAction::Append)]
    endpoint: Vec<String>,
    /// Read the bearer token from a file (otherwise RESCROW_TOKEN).
    #[cfg(feature = "remote")]
    #[arg(long, requires = "endpoint")]
    token_file: Option<PathBuf>,
    #[cfg(feature = "remote")]
    #[arg(long, requires = "endpoint")]
    authority_generation: Option<String>,
    #[cfg(feature = "remote")]
    #[arg(long, requires = "endpoint", value_parser = ["simulation", "local", "regional", "enterprise-mr"])]
    require_profile: Option<String>,
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Top,
}
#[derive(Subcommand)]
enum Top {
    /// Print a fresh UUIDv7 for a new transaction. Create one per business
    /// request and repeat it on every retry of that request.
    NewId,
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
        /// A UUIDv7, generated once when the request is first made; a retry
        /// repeats it.
        id: String,
        #[arg(long)]
        operations: PathBuf,
        /// Let the hold lapse if it is still prepared this long after
        /// admission, such as 500ms, 90s, 15m, 1h or 7d (at most 30d).
        /// Without it, the hold lasts until committed or aborted.
        #[arg(long, value_parser = duration_ms)]
        hold: Option<u64>,
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
    if let Top::NewId = cli.command {
        return match new_id().and_then(|id| {
            let id = if cli.json { format!("{id:?}") } else { id };
            writeln!(io::stdout().lock(), "{id}")
        }) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("could not create an ID: {e}");
                ExitCode::from(6)
            }
        };
    }
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
        Top::NewId | Top::Init => None,
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
            TransactionCommand::Prepare {
                id,
                operations,
                hold,
            } => Command::Prepare(Prepare {
                context: Default::default(),
                authorization: None,
                namespace: None,
                transaction_id: id.clone(),
                operations: read_json(operations)?,
                hold_ms: *hold,
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
    #[cfg(feature = "remote")]
    if !cli.endpoint.is_empty() {
        return remote::run(cli, command.ok_or_else(|| Error::input("remote initialization is owned by server configuration; initialize a file store locally before serving it"))?);
    }
    let store = cli
        .store
        .as_ref()
        .ok_or_else(|| Error::input("--store is required"))?;
    let app = Application::new(FileStore::new(store)?);
    match (command, &cli.command) {
        (Some(c), _) => app.execute(c),
        (None, _) => app.initialize().map(|()| Output::Initialized),
    }
}
/// Milliseconds in a duration such as `500ms`, `90s`, `15m`, `1h` or `7d`.
/// The Application checks its range.
fn duration_ms(value: &str) -> Result<u64, String> {
    let split = value
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(value.len());
    let (count, unit) = value.split_at(split);
    let scale = match unit {
        "ms" => 1,
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => return Err("expected a whole number followed by ms, s, m, h or d".into()),
    };
    count
        .parse::<u64>()
        .ok()
        .and_then(|n| n.checked_mul(scale))
        .ok_or_else(|| "expected a whole number followed by ms, s, m, h or d".into())
}
/// A UUIDv7 at the current time, its random bits from the OS.
fn new_id() -> io::Result<String> {
    let mut random = [0; 16];
    File::open("/dev/urandom")?.read_exact(&mut random)?;
    Ok(rescrow_application::uuid_v7(
        rescrow_application::system_clock(),
        u128::from_le_bytes(random),
    ))
}
fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, Error> {
    let file = File::open(path).map_err(|e| Error::input(format!("{}: {e}", path.display())))?;
    let mut bytes = Vec::new();
    file.take(MAX_RECORD_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| Error::input(e.to_string()))?;
    if bytes.len() as u64 > MAX_RECORD_BYTES {
        return Err(Error::input("input JSON exceeds 16 MiB"));
    }
    rescrow_application::canonical::decode(&bytes)
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
        State::Aborted { cause, .. } => format!("aborted ({cause:?})"),
    };
    let expires = t
        .expires_at_ms
        .map(|at| format!("; expires at {at} ms"))
        .unwrap_or_default();
    format!(
        "{}: {state}{expires}; operations: {}",
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
        Code::InvalidInput
        | Code::LimitExceeded
        | Code::MethodNotAllowed
        | Code::UnsupportedMediaType
        | Code::Unauthenticated
        | Code::Forbidden => 2,
        Code::Capacity | Code::Overflow => {
            if error.category == Category::Input {
                2
            } else {
                3
            }
        }
        Code::Conflict | Code::NotFound | Code::AlreadyInitialized | Code::Expired => 4,
        Code::Contention | Code::Throttled => 5,
        Code::UnknownOutcome | Code::DeadlineExceeded => 7,
        _ => 6,
    }
}
