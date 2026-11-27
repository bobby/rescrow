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
    let changes = transition::prepare(request(), None, &before, 0).unwrap();
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
    let tx = changes.reservation().unwrap();
    assert!(
        transition::prepare(request(), Some(tx), &held, 0)
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
    let tx = Reservation::restore(request(), None, State::Prepared).unwrap();
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
        transition::prepare(request(), Some(&tx), &resources, 0).unwrap_err(),
        Error::InvalidLedger
    );
}
fn held(hold_ms: Option<u64>) -> Request {
    request().with_hold(hold_ms).unwrap()
}
fn by_id(resources: &[Resource]) -> BTreeMap<Id, Resource> {
    resources
        .iter()
        .map(|r| (r.spec().id().clone(), r.clone()))
        .collect()
}
#[test]
fn holds_last_from_one_millisecond_to_thirty_days() {
    for invalid in [0, MAX_HOLD_MS + 1] {
        assert_eq!(
            request().with_hold(Some(invalid)).unwrap_err(),
            Error::InvalidHold
        );
    }
    assert_eq!(held(Some(MAX_HOLD_MS)).hold_ms(), Some(MAX_HOLD_MS));
    assert_eq!(held(None).hold_ms(), None);
    // The hold is part of the request, so a retry with another one conflicts.
    assert_ne!(held(Some(1)), held(Some(2)));
    let before = by_id(&[resource("a"), resource("b")]);
    assert_eq!(
        transition::prepare(held(Some(1)), None, &before, u64::MAX).unwrap_err(),
        Error::Overflow
    );
}
#[test]
fn a_hold_lapses_only_once_its_deadline_passes_and_releases_both_directions() {
    let before = by_id(&[resource("a"), resource("b"), resource("unrelated")]);
    let prepared = transition::prepare(held(Some(100)), None, &before, 1_000).unwrap();
    let tx = prepared.reservation().unwrap().clone();
    assert_eq!(tx.expires_at_ms(), Some(1_100));
    let resources = by_id(prepared.resources());
    let live = Live::restore(resources.values().cloned().collect(), vec![tx.clone()]).unwrap();
    // A replay keeps the deadline fixed at admission.
    assert!(
        transition::prepare(held(Some(100)), Some(&tx), &resources, 1_050)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        transition::prepare(held(Some(200)), Some(&tx), &resources, 1_050).unwrap_err(),
        Error::Conflict
    );
    assert!(!tx.is_due(1_099));
    assert!(live.due([&id("a")], 1_099).is_empty());
    assert_eq!(
        transition::lapse(&tx, &resources, 1_099).unwrap_err(),
        Error::Conflict
    );
    assert!(live.due([&id("unrelated")], 1_100).is_empty());
    assert_eq!(live.due([&id("a"), &id("b")], 1_100), vec![&tx]);
    let lapsed = transition::lapse(&tx, &resources, 1_100).unwrap();
    let after = lapsed.reservation().unwrap();
    assert_eq!(*after.state(), State::Aborted(AbortReason::Lapsed));
    assert_eq!(after.expires_at_ms(), Some(1_100));
    // Both directions are released, and no quantity changes.
    assert!(lapsed.resources().iter().all(|r| r.holds().is_empty()));
    assert!(lapsed.resources().iter().all(|r| r.quantity() == 5));
    let released = by_id(lapsed.resources());
    // A lapsed reservation cannot commit; aborting it again is a replay.
    assert_eq!(
        transition::commit(after, None, &released).unwrap_err(),
        Error::Conflict
    );
    assert!(transition::abort(after, &released).unwrap().is_empty());
    assert_eq!(
        transition::lapse(after, &released, 2_000).unwrap_err(),
        Error::Conflict
    );
}
#[test]
fn holds_without_a_deadline_never_lapse() {
    let before = by_id(&[resource("a"), resource("b")]);
    let prepared = transition::prepare(held(None), None, &before, 0).unwrap();
    let tx = prepared.reservation().unwrap().clone();
    assert_eq!(tx.expires_at_ms(), None);
    let resources = by_id(prepared.resources());
    let live = Live::restore(resources.values().cloned().collect(), vec![tx.clone()]).unwrap();
    assert!(live.due([&id("a"), &id("b")], u64::MAX).is_empty());
    assert_eq!(
        transition::lapse(&tx, &resources, u64::MAX).unwrap_err(),
        Error::Conflict
    );
    // A deadline must agree with the request's hold.
    assert_eq!(
        Reservation::restore(held(None), Some(5), State::Prepared).unwrap_err(),
        Error::InvalidLedger
    );
    assert_eq!(
        Reservation::restore(held(Some(5)), None, State::Prepared).unwrap_err(),
        Error::InvalidLedger
    );
}

#[test]
fn identical_reservation_ids_in_different_namespaces_share_capacity_safely() {
    use rescrow_domain as d;
    use std::collections::BTreeMap;
    let plain = |s: &str| d::Id::new(s.into()).unwrap();
    let spec = d::ResourceSpec::new(plain("r"), 0, Some(10), 10, "units".into(), 0).unwrap();
    let resource = d::Resource::restore(spec, 10, vec![]).unwrap();
    let mut resources = BTreeMap::from([(plain("r"), resource)]);
    let request = |namespace| {
        d::Request::new(
            d::Id::qualified(plain(namespace), plain("same")).unwrap(),
            vec![d::Operation::new(plain("op"), plain("r"), -4).unwrap()],
        )
        .unwrap()
    };
    let a = request("a");
    let b = request("b");
    assert_ne!(a.id(), b.id());
    let first = d::transition::prepare(a, None, &resources, 0).unwrap();
    resources.insert(plain("r"), first.resources()[0].clone());
    let second = d::transition::prepare(b, None, &resources, 0).unwrap();
    resources.insert(plain("r"), second.resources()[0].clone());
    assert_eq!(resources[&plain("r")].holds().len(), 2);
    assert_eq!(resources[&plain("r")].availability().available_down(), 2);
    let commit = d::transition::commit(first.reservation().unwrap(), None, &resources).unwrap();
    resources.insert(plain("r"), commit.resources()[0].clone());
    let abort = d::transition::abort(second.reservation().unwrap(), &resources).unwrap();
    assert_eq!(abort.resources()[0].quantity(), 6);
    assert!(abort.resources()[0].holds().is_empty());
}
