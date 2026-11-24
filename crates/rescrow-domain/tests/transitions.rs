use rescrow_domain::{transition, *};
use std::collections::BTreeMap;

fn id(value: &str) -> Id {
    Id::new(value.into()).unwrap()
}
fn resource(name: &str) -> Resource {
    Resource::restore(
        ResourceSpec::new(id(name), 0, Some(10), 5, "credits".into(), 0).unwrap(),
        5,
        vec![],
    )
    .unwrap()
}
fn request() -> Request {
    Request::new(
        id("t"),
        vec![
            Operation::new(id("a"), id("a"), -3).unwrap(),
            Operation::new(id("b"), id("b"), 3).unwrap(),
        ],
    )
    .unwrap()
}
#[test]
fn replacements_include_only_participants_and_leave_inputs_unchanged() {
    let before: BTreeMap<_, _> = ["a", "b", "unrelated"]
        .into_iter()
        .map(|name| (id(name), resource(name)))
        .collect();
    let changes = transition::prepare(request(), None, &before).unwrap();
    assert_eq!(changes.resources().len(), 2);
    assert!(
        changes
            .resources()
            .iter()
            .all(|r| r.spec().id() != &id("unrelated"))
    );
    assert!(before.values().all(|r| r.holds().is_empty()));
    let held = changes
        .resources()
        .iter()
        .map(|r| (r.spec().id().clone(), r.clone()))
        .collect();
    let tx = changes.transaction().unwrap();
    assert!(
        transition::prepare(request(), Some(tx), &held)
            .unwrap()
            .is_empty()
    );
    let settled = transition::commit(tx, Some(vec![(id("a"), -1), (id("b"), 0)]), &held).unwrap();
    assert_eq!(settled.resources()[0].quantity(), 4);
    assert_eq!(settled.resources()[1].quantity(), 5);
    assert!(settled.resources().iter().all(|r| r.holds().is_empty()));
}
#[test]
fn individually_valid_records_do_not_bypass_participant_validation() {
    let tx = Transaction::restore(request(), State::Prepared).unwrap();
    let resources = [(id("a"), resource("a")), (id("b"), resource("b"))]
        .into_iter()
        .collect();
    assert_eq!(
        transition::commit(&tx, None, &resources).unwrap_err(),
        Error::InvalidLedger
    );
    assert_eq!(
        transition::abort(&tx, &resources).unwrap_err(),
        Error::InvalidLedger
    );
    assert_eq!(
        transition::prepare(request(), Some(&tx), &resources).unwrap_err(),
        Error::InvalidLedger
    );
}
