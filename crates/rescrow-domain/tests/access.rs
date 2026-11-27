use rescrow_domain::{self as d, access::*};
fn fingerprint(c: char) -> String {
    std::iter::repeat_n(c, 64).collect()
}
#[test]
fn context_is_bounded_and_duplicate_keys_are_not_discarded() {
    assert!(
        Context::new(vec![
            ("workflow".into(), "one".into()),
            ("workflow".into(), "two".into())
        ])
        .is_err()
    );
    assert!(Context::new(vec![("../key".into(), "v".into())]).is_err());
    assert!(Context::new(vec![("key".into(), "é".repeat(513))]).is_err());
    let entries = (0..4).map(|n| (n.to_string(), "x".repeat(1023))).collect();
    assert!(Context::new(entries).is_ok());
    let entries = (0..4).map(|n| (n.to_string(), "x".repeat(1024))).collect();
    assert!(Context::new(entries).is_err());
}
#[test]
fn grants_intersect_actions_directions_amounts_and_lifetimes() {
    let grant = Grant::new("budget.".into(), true, false, 40).unwrap();
    let policy = Policy::new(vec![Action::Prepare, Action::Read], vec![grant], Some(1000)).unwrap();
    let budget = d::Id::new("budget.agent".into()).unwrap();
    assert!(policy.allows(Action::Prepare));
    assert!(!policy.allows(Action::Create));
    assert!(policy.permits(&budget, -40));
    assert!(!policy.permits(&budget, -41));
    assert!(!policy.permits(&budget, 1));
    assert!(!policy.permits(&d::Id::new("other".into()).unwrap(), -1));
    assert!(!policy.permits(&budget, i128::MIN));
    assert!(policy.permits_hold(Some(1000)));
    assert!(!policy.permits_hold(None));
    assert!(!policy.permits_hold(Some(1001)));
}
#[test]
fn pending_holds_enforce_both_limits_and_release_exactly_once() {
    let (a, b) = (fingerprint('a'), fingerprint('b'));
    let empty = Pending::default();
    let one = empty.reserve(&a, 2, 1).unwrap();
    assert_eq!(one.reserve(&a, 2, 1), Err(d::Error::AdmissionLimit));
    let two = one.reserve(&b, 2, 1).unwrap();
    assert_eq!(two.total(), 2);
    assert_eq!(
        two.reserve(&fingerprint('c'), 2, 1),
        Err(d::Error::AdmissionLimit)
    );
    let released = two.release(&a).unwrap();
    assert_eq!(released.for_credential(&a), 0);
    assert_eq!(released.release(&a), Err(d::Error::InvalidLedger));
    assert_eq!(released.release(&b).unwrap(), empty);
    assert!(Pending::restore(vec![(a.clone(), 1), (a, 1)]).is_err());
    assert!(Pending::restore(vec![(b, 0)]).is_err());
}
#[test]
fn credential_rotation_replays_original_facts_but_context_changes_conflict() {
    use std::collections::BTreeMap;
    let id = |s: &str| d::Id::new(s.into()).unwrap();
    let resource = d::Resource::restore(
        d::ResourceSpec::new(id("r"), 0, None, 10, "u".into(), 0).unwrap(),
        10,
        vec![],
    )
    .unwrap();
    let resources = BTreeMap::from([(id("r"), resource)]);
    let facts = |credential| {
        Facts::new(fingerprint('a'), fingerprint(credential), fingerprint('c')).unwrap()
    };
    let request = d::Request::new(
        id("t"),
        vec![d::Operation::new(id("op"), id("r"), -2).unwrap()],
    )
    .unwrap()
    .with_authorization(Some(facts('b')));
    let first = d::transition::prepare(request.clone(), None, &resources, 0).unwrap();
    let prepared = first.reservation().unwrap();
    let resources = BTreeMap::from([(id("r"), first.resources()[0].clone())]);
    let renewed = request.clone().with_authorization(Some(facts('d')));
    assert!(request.same_intent(&renewed));
    assert_ne!(request, renewed);
    assert!(
        d::transition::prepare(renewed, Some(prepared), &resources, 0)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        prepared.request().authorization().unwrap().credential_id(),
        fingerprint('b')
    );
    let changed =
        request.with_context(Context::new(vec![("workflow".into(), "different".into())]).unwrap());
    assert_eq!(
        d::transition::prepare(changed, Some(prepared), &resources, 0),
        Err(d::Error::Conflict)
    );
}
