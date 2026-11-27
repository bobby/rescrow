use rescrow_application::{Code, dto::*, validated_snapshot};
use rescrow_domain as d;
fn sample() -> Snapshot {
    let spec = ResourceSpec {
        resource_id: "r".into(),
        lower: "0".into(),
        upper: Some("10".into()),
        initial: "5".into(),
        unit: "units".into(),
        scale: 0,
    };
    let ledger = d::Ledger::default()
        .create(spec.try_into().unwrap())
        .unwrap();
    let request: d::Request = Prepare {
        namespace: None,
        transaction_id: "t".into(),
        operations: vec![Operation {
            operation_id: "o".into(),
            resource_id: "r".into(),
            delta: "-3".into(),
        }],
        hold_ms: None,
    }
    .try_into()
    .unwrap();
    Snapshot::from_ledger(&ledger.prepare(request, 0).unwrap(), 2)
}
#[test]
fn canonical_decimal_and_typed_json() {
    assert_eq!(decimal("not-an-integer"), Err(d::Error::InvalidQuantity));
    assert_eq!(
        decimal("170141183460469231731687303715884105728"),
        Err(d::Error::Overflow)
    );
    for s in [
        "+1",
        "01",
        "-0",
        "1.0",
        "1e3",
        " 1",
        "",
        "170141183460469231731687303715884105728",
    ] {
        assert!(decimal(s).is_err(), "{s}");
    }
    for s in [
        "0",
        "-1",
        "170141183460469231731687303715884105727",
        "-170141183460469231731687303715884105728",
    ] {
        assert_eq!(decimal(s).unwrap().to_string(), s);
    }
    // A hold is optional but its field is not: `null` means no deadline.
    assert!(
        serde_json::from_str::<Command>(
            r#"{"command":"prepare","input":{"transaction_id":"t","operations":[]}}"#
        )
        .is_err()
    );
    let cmd: Command = serde_json::from_str(r#"{"command":"prepare","input":{"transaction_id":"t","operations":[{"operation_id":"o","resource_id":"r","delta":"-9007199254740993"}],"hold_ms":null}}"#).unwrap();
    let Command::Prepare(p) = cmd else { panic!() };
    assert_eq!(
        d::Request::try_from(p).unwrap().operations()[0].delta(),
        -9007199254740993
    );
    assert!(
        serde_json::from_str::<Operation>(r#"{"operation_id":"o","resource_id":"r","delta":3}"#)
            .is_err()
    );
    for json in [
        r#"{"status":"future"}"#,
        r#"{"status":"committed"}"#,
        r#"{"status":"prepared","actuals":[]}"#,
        r#"{"status":"aborted"}"#,
    ] {
        assert!(serde_json::from_str::<State>(json).is_err());
    }
}
#[test]
fn round_trips_every_lifecycle() {
    let prepared = validated_snapshot(sample()).unwrap();
    for ledger in [
        prepared.clone(),
        prepared
            .commit(&d::Id::new("t".into()).unwrap(), None)
            .unwrap(),
        prepared.abort(&d::Id::new("t".into()).unwrap()).unwrap(),
    ] {
        let dto = Snapshot::from_ledger(&ledger, 42);
        let decoded: Snapshot = serde_json::from_slice(&serde_json::to_vec(&dto).unwrap()).unwrap();
        assert_eq!(validated_snapshot(decoded).unwrap(), ledger);
    }
    let rejected = prepared
        .prepare(
            Prepare {
                namespace: None,
                transaction_id: "reject".into(),
                operations: vec![Operation {
                    operation_id: "o".into(),
                    resource_id: "r".into(),
                    delta: "-6".into(),
                }],
                hold_ms: None,
            }
            .try_into()
            .unwrap(),
            0,
        )
        .unwrap();
    assert_eq!(
        validated_snapshot(Snapshot::from_ledger(&rejected, 3)).unwrap(),
        rejected
    );
}
#[test]
fn reject_impossible_records_without_dropping_entries() {
    let mutate: Vec<fn(&mut Snapshot)> = vec![
        |s| s.resources.push(s.resources[0].clone()),
        |s| s.transactions.push(s.transactions[0].clone()),
        |s| {
            let h = s.resources[0].holds[0].clone();
            s.resources[0].holds.push(h);
        },
        |s| s.resources[0].quantity = "6".into(),
        |s| s.resources[0].reserved_down = "2".into(),
        |s| s.resources[0].available_down = "7".into(),
        |s| s.resources[0].holds[0].delta = "-4".into(),
        |s| s.resources[0].holds[0].transaction_id = "missing".into(),
        |s| s.transactions[0].request.operations[0].resource_id = "missing".into(),
        |s| {
            let o = s.transactions[0].request.operations[0].clone();
            s.transactions[0].request.operations.push(o);
        },
        |s| {
            s.transactions[0].state = State::Aborted {
                cause: AbortReason::Requested,
                capacity: None,
            }
        },
        |s| {
            s.transactions[0].state = State::Committed {
                actuals: vec![Actual {
                    operation_id: "o".into(),
                    delta: "-4".into(),
                }],
            }
        },
        |s| s.resources.clear(),
        |s| s.transactions.clear(),
        |s| s.resources[0].spec.initial = "6".into(),
        |s| s.resources[0].spec.lower = "6".into(),
        |s| s.resources[0].spec.upper = Some("4".into()),
    ];
    for change in mutate {
        let mut s = sample();
        change(&mut s);
        assert_eq!(validated_snapshot(s).unwrap_err().code, Code::CorruptState);
    }
    let mut s = sample();
    s.schema_version = 2;
    assert_eq!(
        validated_snapshot(s).unwrap_err().code,
        Code::UnsupportedSchema
    );
}

#[test]
fn documented_json_examples_use_the_validated_contract() {
    let operations: Vec<Operation> =
        serde_json::from_str(include_str!("../../../examples/operations.json")).unwrap();
    let request: d::Request = Prepare {
        namespace: None,
        transaction_id: "work".into(),
        operations,
        hold_ms: None,
    }
    .try_into()
    .unwrap();
    let actuals: Vec<Actual> =
        serde_json::from_str(include_str!("../../../examples/actuals.json")).unwrap();
    request
        .actuals(Some(
            actuals
                .into_iter()
                .map(|a| {
                    (
                        d::Id::new(a.operation_id).unwrap(),
                        decimal(&a.delta).unwrap(),
                    )
                })
                .collect(),
        ))
        .unwrap();
    let credit: Vec<Operation> =
        serde_json::from_str(include_str!("../../../examples/credit.json")).unwrap();
    let _: d::Request = Prepare {
        namespace: None,
        transaction_id: "credit".into(),
        operations: credit,
        hold_ms: None,
    }
    .try_into()
    .unwrap();
    assert!(
        serde_json::from_str::<ResourceSpec>(
            r#"{"resource_id":"r","lower":"0","initial":"1","unit":"u","scale":0}"#
        )
        .is_err()
    );
    assert!(
        serde_json::from_str::<AbortReason>(r#"{"reason":"requested","rejection":"capacity"}"#)
            .is_err()
    );
}

#[test]
fn capacity_details_are_required_and_validated() {
    let prepared = validated_snapshot(sample()).unwrap();
    let rejected = prepared
        .prepare(
            Prepare {
                namespace: None,
                transaction_id: "reject".into(),
                operations: vec![Operation {
                    operation_id: "o".into(),
                    resource_id: "r".into(),
                    delta: "-6".into(),
                }],
                hold_ms: None,
            }
            .try_into()
            .unwrap(),
            0,
        )
        .unwrap();
    let dto = Snapshot::from_ledger(&rejected, 4);
    for change in [0, 1, 2, 3, 4] {
        let mut invalid = dto.clone();
        let State::Aborted { cause, capacity } = &mut invalid.transactions[0].state else {
            panic!()
        };
        match change {
            0 => capacity.as_mut().unwrap().available = "6".into(),
            1 => capacity.as_mut().unwrap().requested_delta = "-7".into(),
            2 => capacity.as_mut().unwrap().resource_id = "other".into(),
            3 => *capacity = None,
            _ => *cause = AbortReason::Requested,
        }
        assert_eq!(
            validated_snapshot(invalid).unwrap_err().code,
            Code::CorruptState
        );
    }
}

#[test]
fn qualified_identities_round_trip_without_string_joining() {
    let decode = |namespace: &str| {
        let request: d::Request = Prepare {
            transaction_id: "same".into(),
            namespace: Some(namespace.into()),
            operations: vec![Operation {
                operation_id: "op".into(),
                resource_id: "r".into(),
                delta: "-1".into(),
            }],
            hold_ms: None,
        }
        .try_into()
        .unwrap();
        request
    };
    let a = decode("a");
    let b = decode("b");
    assert_ne!(a.id(), b.id());
    assert_eq!(a.id().as_str(), "same");
    assert_eq!(Prepare::from(&a).namespace.as_deref(), Some("a"));
    assert_eq!(d::Request::try_from(Prepare::from(&a)).unwrap(), a);
    let spec =
        d::ResourceSpec::new(d::Id::new("r".into()).unwrap(), 0, None, 5, "u".into(), 0).unwrap();
    let resource =
        d::Resource::restore(spec, 5, vec![(a.id().clone(), -1), (b.id().clone(), -1)]).unwrap();
    let transfer = Resource::from(&resource);
    assert_eq!(transfer.holds.len(), 2);
    assert_eq!(d::Resource::try_from(transfer).unwrap(), resource);
    let invalid = Prepare {
        namespace: Some("../escape".into()),
        ..Prepare::from(&a)
    };
    assert!(d::Request::try_from(invalid).is_err());
}
