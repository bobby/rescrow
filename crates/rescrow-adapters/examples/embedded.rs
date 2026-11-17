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
    app.execute(Command::CreateResource(ResourceSpec {
        resource_id: "example".into(),
        lower: "0".into(),
        upper: Some("100".into()),
        initial: "60".into(),
        unit: "credits".into(),
        scale: 0,
    }))?;
    app.execute(Command::Prepare(Prepare {
        transaction_id: "example-work".into(),
        operations: vec![Operation {
            operation_id: "spend".into(),
            resource_id: "example".into(),
            delta: "-40".into(),
        }],
    }))?;
    app.execute(Command::Commit(Commit {
        transaction_id: "example-work".into(),
        actuals: Some(vec![Actual {
            operation_id: "spend".into(),
            delta: "-25".into(),
        }]),
    }))?;
    let result = app.execute(Command::ShowResource {
        resource_id: "example".into(),
    })?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
