//! Run with: cargo run -p rescrow-adapters --example embedded -- /path/to/store
use rescrow_adapters::file::FileStore;
use rescrow_application::{Application, Code, dto::*};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args_os()
        .nth(1)
        .ok_or("supply a store directory (its parent must exist)")?;
    let app = Application::new(FileStore::new(path)?);
    match app.initialize() {
        Ok(_) => {}
        Err(e) if e.code == Code::AlreadyInitialized => {}
        Err(e) => return Err(e.into()),
    }
    app.create_resource(ResourceSpec {
        resource_id: "example".into(),
        lower: "0".into(),
        upper: Some("100".into()),
        initial: "60".into(),
        unit: "credits".into(),
        scale: 0,
    })?;
    // A client creates the ID once per request and repeats it on every retry.
    // Real clients fill its random bits from a random source; this example
    // stands in the process ID and clock nanoseconds.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .subsec_nanos();
    let work = rescrow_application::uuid_v7(
        rescrow_application::system_clock(),
        u128::from(std::process::id()) << 32 | u128::from(nanos),
    );
    app.prepare(Prepare {
        context: Default::default(),
        authorization: None,
        namespace: None,
        reservation_id: work.clone(),
        operations: vec![Operation {
            operation_id: "spend".into(),
            resource_id: "example".into(),
            delta: "-40".into(),
        }],
        hold_ms: None,
    })?;
    app.commit(Commit {
        reservation_id: work,
        actuals: Some(vec![Actual {
            operation_id: "spend".into(),
            delta: "-25".into(),
        }]),
    })?;
    let result = app.resource("example")?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
