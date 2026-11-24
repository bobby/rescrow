//! Shared bounded workloads for CLI and Emriss history checks.
use super::model::{Command, Op, Spec};

fn operations(delta: i64) -> Vec<Op> {
    vec![
        Op {
            id: 0,
            resource: 0,
            delta,
        },
        Op {
            id: 1,
            resource: 1,
            delta: -delta,
        },
    ]
}

pub fn workload(case: u8) -> (Vec<Command>, Vec<Vec<Command>>) {
    let sign = if case & 1 == 0 { -1 } else { 1 };
    let setup = vec![
        Command::Create(
            0,
            Spec {
                lower: 0,
                upper: Some(10),
                initial: 5,
                unit: 0,
                scale: 0,
            },
        ),
        Command::Create(
            1,
            Spec {
                lower: 0,
                upper: if case & 8 == 0 { Some(10) } else { None },
                initial: 5,
                unit: 1,
                scale: 0,
            },
        ),
        Command::Prepare(0, operations(sign * 3)),
        Command::Prepare(1, operations(-sign * 2)),
    ];
    let fresh = operations(sign * (1 + i64::from(case % 3)));
    let mut retry = fresh.clone();
    retry.reverse();
    if case & 2 != 0 {
        retry[0].delta += sign;
    }
    let partial = Some(vec![(0, sign), (1, 0)]);
    let read = match case % 3 {
        0 => Command::Transaction(0),
        1 => Command::Transactions,
        _ => Command::Resource(0),
    };
    let clients = vec![
        vec![
            Command::Prepare(2, fresh),
            Command::Commit(2, partial.clone()),
            Command::Prepare(2, retry),
            Command::Commit(2, partial.clone()),
        ],
        vec![
            Command::Commit(0, partial),
            Command::Abort(1),
            Command::Prepare(0, operations(sign * 3).into_iter().rev().collect()),
            read,
        ],
        vec![
            Command::Abort(0),
            Command::Prepare(3, operations(-sign * 5)),
            Command::Commit(
                3,
                if case & 4 == 0 {
                    None
                } else {
                    Some(vec![(0, 0), (1, 0)])
                },
            ),
            Command::Resources,
        ],
    ];
    (setup, clients)
}
