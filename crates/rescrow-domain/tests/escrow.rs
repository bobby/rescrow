use rescrow_domain::*;
fn id(s: &str) -> Id {
    Id::new(s.into()).unwrap()
}
fn resource(name: &str, lower: i128, upper: Option<i128>, initial: i128) -> ResourceSpec {
    ResourceSpec::new(id(name), lower, upper, initial, "units".into(), 0).unwrap()
}
fn req(name: &str, ops: &[(&str, &str, i128)]) -> Request {
    Request::new(
        id(name),
        ops.iter()
            .map(|(o, r, d)| Operation::new(id(o), id(r), *d).unwrap())
            .collect(),
    )
    .unwrap()
}
fn base() -> Ledger {
    Ledger::default()
        .create(resource("r", 0, Some(10), 5))
        .unwrap()
}
#[test]
fn construction_and_numeric_edges() {
    for value in ["", "space id", "é", "a/b"] {
        assert_eq!(Id::new(value.into()), Err(Error::InvalidId));
    }
    assert!(Id::new("a".repeat(129)).is_err());
    assert!(ResourceSpec::new(id("r"), 4, Some(3), 4, "u".into(), 0).is_err());
    assert!(ResourceSpec::new(id("r"), i128::MIN, Some(i128::MAX), 0, "u".into(), 0).is_err());
    assert!(ResourceSpec::new(id("r"), 0, None, 0, "".into(), 0).is_err());
    assert!(ResourceSpec::new(id("r"), 0, None, 0, "u".into(), 39).is_err());
    assert_eq!(
        Operation::new(id("o"), id("r"), i128::MIN),
        Err(Error::Overflow)
    );
    assert!(Operation::new(id("o"), id("r"), 0).is_err());
    assert!(Request::new(id("t"), vec![]).is_err());
    assert!(
        Request::new(
            id("t"),
            vec![Operation::new(id("o"), id("r"), 1).unwrap(); 2]
        )
        .is_err()
    );
    let min = Ledger::default()
        .create(resource("min", i128::MIN, Some(-1), i128::MIN))
        .unwrap();
    let min = min
        .prepare(req("t", &[("o", "min", i128::MAX)]), 0)
        .unwrap()
        .commit(&id("t"), None)
        .unwrap();
    assert_eq!(min.resources()[&id("min")].quantity(), -1);
    Ledger::restore(
        min.resources().values().cloned().collect(),
        min.reservations().values().cloned().collect(),
    )
    .unwrap();
    let max = Ledger::default()
        .create(resource("max", 0, None, i128::MAX))
        .unwrap();
    let rejected = max.prepare(req("t", &[("o", "max", 1)]), 0).unwrap();
    assert!(rejected.reservations()[&id("t")].rejection().is_some());
}
#[test]
fn directions_do_not_fund_each_other_and_participants_are_atomic() {
    let original = base().create(resource("s", 0, Some(10), 1)).unwrap();
    let held = original
        .prepare(req("credit", &[("a", "r", 5)]), 0)
        .unwrap();
    let rejected = held.prepare(req("debit", &[("a", "r", -6)]), 0).unwrap();
    assert_eq!(rejected.resources(), held.resources());
    assert_eq!(original.resources()[&id("r")].holds().len(), 0);
    let held = original
        .prepare(req("debit", &[("a", "r", -5)]), 0)
        .unwrap();
    assert_eq!(
        held.prepare(req("credit", &[("a", "r", 6)]), 0)
            .unwrap()
            .resources(),
        held.resources()
    );
    let rejected = original
        .prepare(req("multi", &[("a", "r", -2), ("b", "s", -2)]), 0)
        .unwrap();
    assert_eq!(rejected.resources(), original.resources());
    assert_eq!(
        rejected.reservations()[&id("multi")].rejection(),
        Some(Rejection::Capacity(
            CapacityFailure::new(id("s"), -2, 1).unwrap()
        ))
    );
    let missing = original
        .prepare(req("missing", &[("a", "unknown", 1)]), 0)
        .unwrap();
    assert_eq!(missing.resources(), original.resources());
    assert_eq!(
        missing.reservations()[&id("missing")].rejection(),
        Some(Rejection::NotFound)
    );
    let created = missing.create(resource("unknown", 0, Some(10), 0)).unwrap();
    assert_eq!(
        created
            .prepare(req("missing", &[("a", "unknown", 1)]), 0)
            .unwrap(),
        created
    );
    Ledger::restore(
        missing.resources().values().cloned().collect(),
        missing.reservations().values().cloned().collect(),
    )
    .unwrap();
}
#[test]
fn replays_and_all_terminal_conflicts() {
    let request = req("t", &[("b", "r", -3), ("a", "s", 2)]);
    let original = base().create(resource("s", 0, Some(10), 1)).unwrap();
    let prepared = original.prepare(request.clone(), 0).unwrap();
    assert_eq!(
        prepared
            .prepare(req("t", &[("a", "s", 2), ("b", "r", -3)]), 0)
            .unwrap(),
        prepared
    );
    assert_eq!(
        prepared.prepare(req("t", &[("b", "r", -2)]), 0),
        Err(Error::Conflict)
    );
    for actuals in [
        vec![(id("a"), 1)],
        vec![(id("a"), 1), (id("a"), 0)],
        vec![(id("a"), 3), (id("b"), 0)],
        vec![(id("a"), -1), (id("b"), 0)],
        vec![(id("a"), 0), (id("b"), i128::MIN)],
    ] {
        assert_eq!(
            prepared.commit(&id("t"), Some(actuals)),
            Err(Error::InvalidActuals)
        );
    }
    let actual = vec![(id("a"), 0), (id("b"), -2)];
    let committed = prepared.commit(&id("t"), Some(actual.clone())).unwrap();
    assert_eq!(committed.resources()[&id("r")].quantity(), 3);
    assert_eq!(committed.resources()[&id("s")].quantity(), 1);
    assert!(committed.resources().values().all(|r| r.holds().is_empty()));
    assert_eq!(committed.commit(&id("t"), Some(actual)).unwrap(), committed);
    assert_eq!(committed.commit(&id("t"), None), Err(Error::Conflict));
    assert_eq!(committed.abort(&id("t")), Err(Error::Conflict));
    assert_eq!(
        committed.prepare(req("t", &[("b", "r", -2)]), 0),
        Err(Error::Conflict)
    );
    assert_eq!(
        committed.create(resource("r", 0, Some(10), 5)).unwrap(),
        committed
    );
    assert_eq!(committed.prepare(request.clone(), 0).unwrap(), committed);
    let aborted = prepared.abort(&id("t")).unwrap();
    assert_eq!(aborted.abort(&id("t")).unwrap(), aborted);
    assert_eq!(aborted.commit(&id("t"), None), Err(Error::Conflict));
    assert_eq!(aborted.prepare(request, 0).unwrap(), aborted);
    assert_eq!(aborted.resources(), original.resources());
    assert_eq!(
        aborted.prepare(req("t", &[("b", "r", -2)]), 0),
        Err(Error::Conflict)
    );
    let rejected = original
        .prepare(req("reject", &[("o", "r", -6)]), 0)
        .unwrap();
    let funded = rejected
        .prepare(req("fund", &[("o", "r", 5)]), 0)
        .unwrap()
        .commit(&id("fund"), None)
        .unwrap();
    assert_eq!(
        funded.prepare(req("reject", &[("o", "r", -6)]), 0).unwrap(),
        funded
    );
    assert_eq!(funded.commit(&id("reject"), None), Err(Error::Conflict));
    assert_eq!(funded.abort(&id("reject")).unwrap(), funded);
    assert_eq!(
        funded.prepare(req("reject", &[("o", "r", -5)]), 0),
        Err(Error::Conflict)
    );
    assert_eq!(
        original.create(resource("r", 0, Some(10), 5)).unwrap(),
        original
    );
    assert_eq!(
        original.create(resource("r", 0, Some(10), 6)),
        Err(Error::Conflict)
    );
    assert_eq!(original.commit(&id("none"), None), Err(Error::NotFound));
    assert_eq!(original.abort(&id("none")), Err(Error::NotFound));
}
#[test]
fn enumerate_reservations_outcomes_and_decision_orders() {
    // Independent reference: only already materialized quantity supplies downward
    // capacity and only existing room supplies upward capacity. Enumerate every
    // full/partial/zero/abort choice and both terminal orders.
    for q in 0..=4_i128 {
        for a in [-3_i128, -2, -1, 1, 2, 3] {
            for b in [-3_i128, -2, -1, 1, 2, 3] {
                let start = Ledger::default()
                    .create(resource("r", 0, Some(4), q))
                    .unwrap();
                let la = start.prepare(req("a", &[("o", "r", a)]), 0).unwrap();
                let admissible_a = a.min(0).abs() <= q && a.max(0) <= 4 - q;
                assert_eq!(
                    la.reservations()[&id("a")].rejection().is_none(),
                    admissible_a
                );
                if !admissible_a {
                    continue;
                }
                let lab = la.prepare(req("b", &[("o", "r", b)]), 0).unwrap();
                let admissible_b =
                    a.min(0).abs() + b.min(0).abs() <= q && a.max(0) + b.max(0) <= 4 - q;
                assert_eq!(
                    lab.reservations()[&id("b")].rejection().is_none(),
                    admissible_b
                );
                if !admissible_b {
                    assert_eq!(la.resources(), lab.resources());
                    continue;
                }
                let choices = |d: i128| vec![None, Some(0), Some(d.signum()), Some(d)];
                for aa in choices(a) {
                    for bb in choices(b) {
                        let settle = |l: Ledger, t: &str, actual: Option<i128>| match actual {
                            Some(d) => l.commit(&id(t), Some(vec![(id("o"), d)])).unwrap(),
                            None => l.abort(&id(t)).unwrap(),
                        };
                        let ab = settle(settle(lab.clone(), "a", aa), "b", bb);
                        let ba = settle(settle(lab.clone(), "b", bb), "a", aa);
                        assert_eq!(ab, ba);
                        let expected = q + aa.unwrap_or(0) + bb.unwrap_or(0);
                        assert_eq!(ab.resources()[&id("r")].quantity(), expected);
                        assert!((0..=4).contains(&expected));
                        Ledger::restore(
                            ab.resources().values().cloned().collect(),
                            ab.reservations().values().cloned().collect(),
                        )
                        .unwrap();
                    }
                }
            }
        }
    }
}

#[test]
fn overflowing_totals_are_retained_rejections_without_partial_holds() {
    for (initial, delta, extra) in [(0, i128::MAX, 1), (i128::MAX, -i128::MAX, -1)] {
        let start = Ledger::default()
            .create(resource("r", 0, Some(i128::MAX), initial))
            .unwrap();
        let held = start
            .prepare(req("first", &[("o", "r", delta)]), 0)
            .unwrap();
        let rejected = held
            .prepare(req("overflow", &[("o", "r", extra)]), 0)
            .unwrap();
        assert_eq!(rejected.resources(), held.resources());
        assert_eq!(
            rejected.reservations()[&id("overflow")].rejection(),
            Some(Rejection::Overflow)
        );
        let released = rejected.abort(&id("first")).unwrap();
        assert_eq!(
            released
                .prepare(req("overflow", &[("o", "r", extra)]), 0)
                .unwrap(),
            released
        );
        Ledger::restore(
            released.resources().values().cloned().collect(),
            released.reservations().values().cloned().collect(),
        )
        .unwrap();
    }
}
