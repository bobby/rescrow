use clap::{Parser, Subcommand};
use rescrow_application::{authority::TransactionalStore, capabilities::Readiness};
use rescrow_dsql::{Config, DsqlStore};
use std::{io::Read, path::PathBuf};

#[derive(Parser)]
#[command(about = "Explicit DSQL schema installation, authority initialization, and live status")]
struct Args {
    /// DSQL connector JSON (no secret keys; uses the AWS credential chain).
    #[arg(
        long,
        required_unless_present = "postgres_simulation",
        conflicts_with = "postgres_simulation"
    )]
    config: Option<PathBuf>,
    /// Development only, literal loopback PostgreSQL URL. Never qualifies DSQL.
    #[arg(long, requires = "authority")]
    postgres_simulation: Option<String>,
    #[arg(long, requires = "postgres_simulation")]
    authority: Option<String>,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    InstallSchema,
    Initialize,
    Status,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let store = if let Some(url) = args.postgres_simulation {
        DsqlStore::postgres_simulation(&url, args.authority.as_deref().ok_or("missing authority")?)?
    } else {
        let mut bytes = Vec::new();
        std::fs::File::open(args.config.ok_or("missing configuration")?)?
            .take(1024 * 1024 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > 1024 * 1024 {
            return Err("configuration exceeds 1 MiB".into());
        }
        let config: Config = rescrow_application::canonical::decode(&bytes)?;
        DsqlStore::connect(config)?
    };
    match args.command {
        Command::InstallSchema => store.install_schema()?,
        Command::Initialize => store.initialize()?,
        Command::Status => println!(
            "{}",
            serde_json::to_string_pretty(
                &serde_json::json!({"authority_generation":store.identity()?,"capabilities":store.readiness()?})
            )?
        ),
    }
    Ok(())
}
