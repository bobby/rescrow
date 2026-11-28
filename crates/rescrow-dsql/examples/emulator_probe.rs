//! Native AWS Emulator compatibility gate, not an authority or performance test.
//! Only fixed probe SQL is sent; production storage continues using bound queries.
use clap::Parser;
use serde::Serialize;
use sqlx::{
    Connection, PgConnection,
    postgres::{PgConnectOptions, PgSslMode},
};
use std::{collections::BTreeMap, path::PathBuf, time::Duration};

#[derive(Parser)]
struct Args {
    #[arg(long, value_parser = clap::value_parser!(u16).range(1..))]
    port: u16,
    #[arg(long)]
    ca_file: PathBuf,
}

#[derive(Serialize)]
struct Check {
    passed: bool,
    sqlstate: Option<String>,
    diagnostic: Option<String>,
}
fn check(result: Result<(), sqlx::Error>) -> Check {
    match result {
        Ok(()) => Check {
            passed: true,
            sqlstate: None,
            diagnostic: None,
        },
        Err(sqlx::Error::Database(error)) => Check {
            passed: false,
            sqlstate: error.code().map(|code| code.into_owned()),
            // These are fixed, isolated probe statements, with no customer data.
            diagnostic: Some(error.message().chars().take(512).collect()),
        },
        Err(_) => Check {
            passed: false,
            sqlstate: None,
            diagnostic: Some("connection, TLS, or protocol failure".into()),
        },
    }
}

async fn probe(args: Args, password: String) -> BTreeMap<&'static str, Check> {
    let options = PgConnectOptions::new()
        .host("127.0.0.1")
        .port(args.port)
        .username("admin")
        .database("postgres")
        .password(&password)
        .ssl_mode(PgSslMode::VerifyFull)
        .ssl_root_cert(args.ca_file);
    let mut checks = BTreeMap::new();
    let mut connection = match PgConnection::connect_with(&options).await {
        Ok(connection) => {
            checks.insert("tls_authentication", check(Ok(())));
            connection
        }
        Err(error) => {
            checks.insert("tls_authentication", check(Err(error)));
            return checks;
        }
    };
    checks.insert(
        "simple_query",
        check(
            sqlx::raw_sql("SELECT 1 AS probe")
                .fetch_one(&mut connection)
                .await
                .map(|_| ()),
        ),
    );
    checks.insert(
        "public_schema",
        check(
            sqlx::raw_sql(
                "CREATE TABLE IF NOT EXISTS public.rescrow_emulator_probe (
                    id VARCHAR(128) PRIMARY KEY, version INTEGER NOT NULL)",
            )
            .execute(&mut connection)
            .await
            .map(|_| ()),
        ),
    );
    // Check command acceptance only, not isolation/concurrency semantics.
    let result = {
        let transaction = connection
            .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ")
            .await;
        match transaction {
            Ok(transaction) => transaction.rollback().await,
            Err(error) => Err(error),
        }
    };
    checks.insert("repeatable_read_commands", check(result));
    // The adapter uses this SQLx parameter-binding protocol for all stored data.
    // Run last: an incomplete emulator may not resynchronize after rejecting Parse.
    checks.insert(
        "bound_query",
        check(
            sqlx::query_scalar::<_, i32>("SELECT $1::INTEGER")
                .bind(1_i32)
                .fetch_one(&mut connection)
                .await
                .and_then(|value| {
                    if value == 1 {
                        Ok(())
                    } else {
                        Err(sqlx::Error::Protocol("incorrect bound result".into()))
                    }
                }),
        ),
    );
    checks
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let password = std::env::var("PGPASSWORD").map_err(|_| "set an emulator-only PGPASSWORD")?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let checks = runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(20), probe(args, password)).await
    });
    let checks = checks.map_err(|_| "emulator compatibility probe timed out")?;
    let compatible = checks.len() == 5 && checks.values().all(|check| check.passed);
    println!(
        "{}",
        serde_json::to_string(&serde_json::json!({
            "adapter_prerequisites_passed": compatible,
            "checks": checks,
            "reservation_semantics_qualified": false,
        }))?
    );
    Ok(())
}
