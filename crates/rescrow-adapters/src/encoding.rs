//! Encoding policy stays outside the Domain. The bound reserves space for every
//! outstanding transaction's largest terminal payload, plus all mutable numeric
//! fields at their maximum widths. Removing holds only makes the bound smaller.
use rescrow_application::{StoreError, ValidatedSnapshot, dto::*};
use serde::Serialize;
use std::io::{self, Write};

pub(crate) struct Encoded {
    pub bytes: Vec<u8>,
    pub settlement_bytes: u64,
}
pub(crate) fn encode(snapshot: &ValidatedSnapshot) -> Result<Encoded, StoreError> {
    let dto = snapshot.to_dto();
    let bytes = serde_json::to_vec(&dto).map_err(|e| StoreError::Corrupt(e.to_string()))?;
    let mut required = bytes.len() as u64;
    // u64 revisions can grow even when a mutation leaves the encoded ledger size unchanged.
    required += 20 - dto.revision.to_string().len() as u64;
    for resource in &dto.resources {
        // Signed quantity: up to 40 ASCII bytes. Capacity and totals: nonnegative i128.
        required += 40 - resource.quantity.len() as u64;
        for value in [
            &resource.reserved_down,
            &resource.reserved_up,
            &resource.available_down,
            &resource.available_up,
        ] {
            required += 39 - value.len() as u64;
        }
    }
    let aborted = encoded_len(&State::Aborted {
        cause: AbortReason::Requested,
        capacity: None,
    })?;
    for tx in &dto.transactions {
        if !matches!(tx.state, State::Prepared {}) {
            continue;
        }
        // Every admissible actual has at most its reservation's decimal width;
        // zero is shorter or equal. IDs and the original request never change.
        let committed = State::Committed {
            actuals: tx
                .request
                .operations
                .iter()
                .map(|op| Actual {
                    operation_id: op.operation_id.clone(),
                    delta: op.delta.clone(),
                })
                .collect(),
        };
        let current = encoded_len(&tx.state)?;
        required = required
            .checked_add(encoded_len(&committed)?.max(aborted).max(current) - current)
            .ok_or(StoreError::TooLarge)?;
    }
    Ok(Encoded {
        bytes,
        settlement_bytes: required,
    })
}
fn encoded_len(value: &impl Serialize) -> Result<u64, StoreError> {
    struct Counter(u64);
    impl Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len() as u64)
                .ok_or_else(|| io::Error::other("encoded length overflow"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, value).map_err(|_| StoreError::TooLarge)?;
    Ok(counter.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{file::FileStore, memory::MemoryStore};
    use rescrow_application::{Application, Code, SnapshotStore};
    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };
    static NEXT: AtomicU64 = AtomicU64::new(0);
    struct Temp(PathBuf);
    impl Temp {
        fn new() -> Self {
            let parent = std::env::var_os("RESCROW_TEST_ROOT")
                .map(PathBuf::from)
                .unwrap_or_else(std::env::temp_dir);
            let path = parent.join(format!(
                "rescrow-space-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
        fn store(&self) -> PathBuf {
            self.0.join("store")
        }
    }
    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn spec(id: &str) -> ResourceSpec {
        ResourceSpec {
            resource_id: id.into(),
            lower: "-5".into(),
            upper: Some("5".into()),
            initial: "0".into(),
            unit: "units".into(),
            scale: 0,
        }
    }
    fn request(id: &str, delta: i128) -> Prepare {
        Prepare {
            transaction_id: id.into(),
            operations: vec![Operation {
                operation_id: format!("{id}{}", "o".repeat(127))
                    .chars()
                    .take(128)
                    .collect(),
                resource_id: "r".into(),
                delta: delta.to_string(),
            }],
        }
    }
    fn seeded() -> (MemoryStore, Application<MemoryStore>) {
        let store = MemoryStore::default();
        let app = Application::new(store.clone());
        app.initialize().unwrap();
        app.create_resource(spec("r")).unwrap();
        (store, app)
    }
    #[test]
    fn reject_prepare_that_fits_now_but_cannot_fit_its_commit() {
        let (memory, model) = seeded();
        let initial = encode(&memory.load().unwrap()).unwrap();
        model.prepare(request("a", -3)).unwrap();
        let prepared = encode(&memory.load().unwrap()).unwrap();
        model
            .commit(Commit {
                transaction_id: "a".into(),
                actuals: None,
            })
            .unwrap();
        let committed = encode(&memory.load().unwrap()).unwrap();
        assert!(committed.bytes.len() > prepared.bytes.len());
        let limit = ((prepared.bytes.len() + committed.bytes.len()) / 2) as u64;
        assert!(initial.settlement_bytes <= limit);
        assert!((prepared.bytes.len() as u64) < limit);
        assert!(prepared.settlement_bytes > limit);
        let temp = Temp::new();
        let store = FileStore::new(temp.store())
            .unwrap()
            .with_size_limit(limit)
            .unwrap();
        let app = Application::new(
            FileStore::new(temp.store())
                .unwrap()
                .with_size_limit(limit)
                .unwrap(),
        );
        app.initialize().unwrap();
        app.create_resource(spec("r")).unwrap();
        let old = store.load().unwrap();
        assert_eq!(
            app.prepare(request("a", -3)).unwrap_err().code,
            Code::TooLarge
        );
        assert_eq!(store.load().unwrap(), old);
    }
    #[test]
    fn every_commit_abort_order_fits_and_other_writes_cannot_spend_the_headroom() {
        // Opposing holds, long operation IDs, small transaction IDs, negative
        // quantities, zero/partial/full actuals, aborts, and a revision digit rollover.
        for first in ["a", "b"] {
            for a in [None, Some(0), Some(-1), Some(-3)] {
                for b in [None, Some(0), Some(1), Some(3)] {
                    let (memory, model) = seeded();
                    model.prepare(request("a", -3)).unwrap();
                    model.prepare(request("b", 3)).unwrap();
                    let bound = encode(&memory.load().unwrap()).unwrap().settlement_bytes;
                    let temp = Temp::new();
                    let store = FileStore::new(temp.store())
                        .unwrap()
                        .with_size_limit(bound)
                        .unwrap();
                    let app = Application::new(
                        FileStore::new(temp.store())
                            .unwrap()
                            .with_size_limit(bound)
                            .unwrap(),
                    );
                    app.initialize().unwrap();
                    app.create_resource(spec("r")).unwrap();
                    app.prepare(request("a", -3)).unwrap();
                    app.prepare(request("b", 3)).unwrap();
                    // Exercise 9 -> 10 on the same valid ledger. Revision is storage metadata.
                    let mut image = store.load().unwrap().to_dto();
                    image.revision = 9;
                    fs::write(
                        temp.store().join("state.json"),
                        serde_json::to_vec(&image).unwrap(),
                    )
                    .unwrap();
                    let old = store.load().unwrap();
                    assert_eq!(
                        app.create_resource(spec("another")).unwrap_err().code,
                        Code::TooLarge
                    );
                    assert_eq!(
                        app.prepare(request("reject", -5)).unwrap_err().code,
                        Code::TooLarge
                    );
                    assert_eq!(store.load().unwrap(), old);
                    let decisions = if first == "a" {
                        [("a", a), ("b", b)]
                    } else {
                        [("b", b), ("a", a)]
                    };
                    let mut previous = encode(&old).unwrap().settlement_bytes;
                    for (id, actual) in decisions {
                        match actual {
                            Some(delta) => {
                                app.commit(Commit {
                                    transaction_id: id.into(),
                                    actuals: Some(vec![Actual {
                                        operation_id: request(id, 1).operations[0]
                                            .operation_id
                                            .clone(),
                                        delta: delta.to_string(),
                                    }]),
                                })
                                .unwrap();
                            }
                            None => {
                                app.abort(id).unwrap();
                            }
                        }
                        let next = store.load().unwrap();
                        let size = encode(&next).unwrap();
                        assert!(size.settlement_bytes <= previous);
                        assert!(size.bytes.len() as u64 <= bound);
                        previous = size.settlement_bytes;
                    }
                    assert_eq!(
                        app.resource("r").unwrap().quantity,
                        (a.unwrap_or(0) + b.unwrap_or(0)).to_string()
                    );
                }
            }
        }
    }
    #[test]
    fn terminal_size_bound_covers_extreme_quantities_and_multiple_participants() {
        let store = MemoryStore::default();
        let app = Application::new(store.clone());
        app.initialize().unwrap();
        for (id, lower, upper, initial) in [
            ("low", i128::MIN, -1, i128::MIN),
            ("high", 0, i128::MAX, i128::MAX),
        ] {
            app.create_resource(ResourceSpec {
                resource_id: id.into(),
                lower: lower.to_string(),
                upper: Some(upper.to_string()),
                initial: initial.to_string(),
                unit: "units".into(),
                scale: 0,
            })
            .unwrap();
        }
        app.prepare(Prepare {
            transaction_id: "t".into(),
            operations: vec![
                Operation {
                    operation_id: "low".into(),
                    resource_id: "low".into(),
                    delta: i128::MAX.to_string(),
                },
                Operation {
                    operation_id: "high".into(),
                    resource_id: "high".into(),
                    delta: (-i128::MAX).to_string(),
                },
            ],
        })
        .unwrap();
        let mut prepared = store.load().unwrap().to_dto();
        prepared.revision = u64::MAX - 1;
        for actual in [None, Some(0), Some(i128::MAX / 2), Some(i128::MAX)] {
            store.set_raw(prepared.clone());
            let bound = encode(&store.load().unwrap()).unwrap().settlement_bytes;
            match actual {
                Some(delta) => {
                    app.commit(Commit {
                        transaction_id: "t".into(),
                        actuals: Some(vec![
                            Actual {
                                operation_id: "low".into(),
                                delta: delta.to_string(),
                            },
                            Actual {
                                operation_id: "high".into(),
                                delta: (-delta).to_string(),
                            },
                        ]),
                    })
                    .unwrap();
                }
                None => {
                    app.abort("t").unwrap();
                }
            }
            let terminal = encode(&store.load().unwrap()).unwrap();
            assert!(terminal.settlement_bytes <= bound);
            assert!(terminal.bytes.len() as u64 <= bound);
        }
    }
}
