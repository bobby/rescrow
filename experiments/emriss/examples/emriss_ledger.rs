//! In-memory prototype; no durability claim. Reopens through independent handles.
use emriss::storage::MemoryObjects;
use emriss_atom::MemoryRefs;
use rescrow_application::{dto::*, records::RecordApplication};
use rescrow_emriss::EmrissStore;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    futures::executor::block_on(async {
        let objects = MemoryObjects::without_append();
        let refs = MemoryRefs::new();
        let store = EmrissStore::new(objects.clone(), refs.clone(), "memory", "example")?;
        let app = RecordApplication::new(store.clone());
        app.initialize().await?;
        for (id, initial, upper, unit) in [
            ("credits", "60", Some("100"), "credits"),
            ("tokens", "8", None, "tokens"),
        ] {
            app.execute(Command::CreateResource(ResourceSpec {
                resource_id: id.into(),
                lower: "0".into(),
                upper: upper.map(Into::into),
                initial: initial.into(),
                unit: unit.into(),
                scale: 0,
            }))
            .await?;
        }
        let operations = serde_json::from_str(include_str!("../../../examples/operations.json"))?;
        app.execute(Command::Prepare(Prepare {
            transaction_id: "work".into(),
            operations,
        }))
        .await?;
        let actuals = serde_json::from_str(include_str!("../../../examples/actuals.json"))?;
        let commit = Command::Commit(Commit {
            transaction_id: "work".into(),
            actuals: Some(actuals),
        });
        app.execute(commit.clone()).await?;
        let reopened =
            RecordApplication::new(EmrissStore::new(objects, refs, "memory", "example")?);
        reopened.execute(commit).await?;
        let snapshot = reopened.audit().await?.to_dto();
        assert_eq!(snapshot.revision, 4);
        assert_eq!(snapshot.resources[0].quantity, "35");
        println!("{}", serde_json::to_string_pretty(&snapshot)?);
        Ok(())
    })
}
