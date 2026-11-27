//! Concurrent clients checked against the independent model and real-time order.
use crate::{
    history::{Call, History, Response, check},
    model::{self, Command, Op, Spec},
};
use rescrow_application::{Application, authority::TransactionalStore};
use std::{
    path::Path,
    sync::{
        Barrier,
        atomic::{AtomicU64, Ordering},
    },
    thread,
};
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
        Command::Prepare(0, operations(sign * 3), None),
        Command::Prepare(1, operations(-sign * 2), None),
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
            Command::Prepare(2, fresh, None),
            Command::Commit(2, partial.clone()),
            Command::Prepare(2, retry, None),
            Command::Commit(2, partial.clone()),
        ],
        vec![
            Command::Commit(0, partial),
            Command::Abort(1),
            Command::Prepare(0, operations(sign * 3).into_iter().rev().collect(), None),
            read,
        ],
        vec![
            Command::Abort(0),
            Command::Prepare(3, operations(-sign * 5), None),
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

/// Bounded linearizability histories. Each client uses a fresh Application on
/// the same authority; the checker searches orders rather than assuming one.
pub fn run<S: TransactionalStore + Clone + Send + Sync + 'static>(
    factory: impl Fn() -> S,
    failure_root: &Path,
) {
    for case in 0..16 {
        let store = factory();
        let app = Application::new(store.clone()).with_clock(model::created);
        app.initialize().unwrap();
        let (setup, clients) = workload(case);
        for command in &setup {
            app.execute(command.dto()).unwrap();
        }
        let events = AtomicU64::new(0);
        let barrier = Barrier::new(clients.len());
        let calls = thread::scope(|scope| {
            let handles: Vec<_> = clients
                .into_iter()
                .map(|commands| {
                    let (events, barrier, store) = (&events, &barrier, store.clone());
                    scope.spawn(move || {
                        commands
                            .into_iter()
                            .enumerate()
                            .map(|(i, command)| {
                                let invocation = events.fetch_add(1, Ordering::SeqCst);
                                if i == 0 {
                                    barrier.wait();
                                }
                                let result = Application::new(store.clone())
                                    .with_clock(model::created)
                                    .execute(command.dto());
                                let completion = events.fetch_add(1, Ordering::SeqCst);
                                Call {
                                    command,
                                    invocation,
                                    completion,
                                    response: Response::application(result),
                                }
                            })
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            handles
                .into_iter()
                .flat_map(|h| h.join().unwrap())
                .collect()
        });
        let history = History {
            case,
            setup,
            calls,
            final_snapshot: app.snapshot().unwrap(),
        };
        if let Err(error) = check(&history) {
            std::fs::create_dir_all(failure_root).unwrap();
            let path = failure_root.join(format!("history-{case}.json"));
            std::fs::write(&path, serde_json::to_vec_pretty(&history).unwrap()).unwrap();
            panic!("{error}; observations retained at {}", path.display());
        }
    }
}
