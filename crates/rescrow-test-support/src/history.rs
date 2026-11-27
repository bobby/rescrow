//! Bounded linearizability search using only the independent history model.
use super::model::{Command, Failure, Model, tx_id};
use rescrow_application::{Category, Code, Error, dto};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Response {
    pub exit: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub result: Option<Result<dto::Output, Error>>,
}
impl Response {
    pub fn application(result: Result<dto::Output, Error>) -> Self {
        let (exit, stdout, stderr) = match &result {
            Ok(output) => (0, serde_json::to_string(output).unwrap(), String::new()),
            Err(error) => {
                let exit = match error.code {
                    Code::InvalidInput => 2,
                    Code::Capacity | Code::Overflow => 3,
                    Code::NotFound | Code::Conflict | Code::Expired | Code::AlreadyInitialized => 4,
                    Code::Contention => 5,
                    Code::UnknownOutcome => 7,
                    _ => 6,
                };
                (exit, String::new(), serde_json::to_string(error).unwrap())
            }
        };
        Self {
            exit: Some(exit),
            stdout,
            stderr,
            result: Some(result),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Call {
    pub command: Command,
    // Events come from one monotonic counter, shared by the client threads.
    pub invocation: u64,
    pub completion: u64,
    pub response: Response,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct History {
    pub case: u8,
    pub setup: Vec<Command>,
    pub calls: Vec<Call>,
    pub final_snapshot: dto::Snapshot,
}

fn error_contract(failure: &Failure) -> (Code, Category, i32) {
    match failure {
        Failure::MalformedPrepare => (Code::InvalidInput, Category::Input, 2),
        Failure::InvalidActuals => (Code::InvalidInput, Category::Domain, 2),
        Failure::Missing => (Code::NotFound, Category::Domain, 4),
        Failure::Conflict => (Code::Conflict, Category::Domain, 4),
        Failure::Capacity { .. } => (Code::Capacity, Category::Domain, 3),
    }
}

fn matches(call: &Call, model: &Model, revision: u64, result: Result<(), Failure>) -> bool {
    let response = &call.response;
    match (result, &response.result) {
        (Ok(()), Some(Ok(output))) => {
            response.exit == Some(0)
                && response.stderr.is_empty()
                && serde_json::from_str::<dto::Output>(&response.stdout)
                    .ok()
                    .as_ref()
                    == Some(output)
                && *output == model.output(&call.command, revision)
        }
        (Err(failure), Some(Err(error))) => {
            let (code, category, exit) = error_contract(&failure);
            response.exit == Some(exit)
                && response.stdout.is_empty()
                && serde_json::from_str::<Error>(&response.stderr)
                    .ok()
                    .as_ref()
                    == Some(error)
                && error.code == code
                && error.category == category
                && !error.message.is_empty()
                && error.transaction_id == call.command.transaction_id().map(tx_id)
                && error.capacity.as_deref() == failure.capacity().as_ref()
        }
        _ => false,
    }
}

/// Return a witness ordering, or distinguish an impossible history from an
/// exhausted search. A budget limit must never count as a passing history.
pub fn check(history: &History) -> Result<Vec<usize>, String> {
    if history.calls.len() > 16 {
        return Err("history exceeds the 16-call search limit".into());
    }
    let mut events = HashSet::new();
    for call in &history.calls {
        if call.invocation >= call.completion
            || !events.insert(call.invocation)
            || !events.insert(call.completion)
        {
            return Err("invalid or duplicated invocation/completion event".into());
        }
    }
    let mut model = Model::default();
    let mut revision = 0;
    for command in &history.setup {
        let old = model.clone();
        model
            .apply(command)
            .map_err(|e| format!("invalid setup: {e:?}"))?;
        revision += u64::from(model != old);
    }
    let predecessors = history
        .calls
        .iter()
        .map(|call| {
            history
                .calls
                .iter()
                .enumerate()
                .fold(0, |mask, (i, other)| {
                    mask | if other.completion < call.invocation {
                        1 << i
                    } else {
                        0
                    }
                })
        })
        .collect();
    let mut search = Search {
        history,
        predecessors,
        failed: HashSet::new(),
        visited: 0,
    };
    search.visit(model, revision, 0)?.ok_or_else(|| {
        format!(
            "no legal sequential ordering ({} states searched)",
            search.visited
        )
    })
}

struct Search<'a> {
    history: &'a History,
    predecessors: Vec<u64>,
    failed: HashSet<(u64, String)>,
    visited: usize,
}
impl Search<'_> {
    fn visit(
        &mut self,
        model: Model,
        revision: u64,
        done: u64,
    ) -> Result<Option<Vec<usize>>, String> {
        if done == (1 << self.history.calls.len()) - 1 {
            return Ok((model.snapshot(revision) == self.history.final_snapshot).then(Vec::new));
        }
        let key = (
            done,
            serde_json::to_string(&model.snapshot(revision)).unwrap(),
        );
        if self.failed.contains(&key) {
            return Ok(None);
        }
        self.visited += 1;
        if self.visited > 100_000 {
            return Err("search exhausted its 100000-state budget".into());
        }
        for i in 0..self.history.calls.len() {
            if done & (1 << i) != 0 || self.predecessors[i] & !done != 0 {
                continue;
            }
            let call = &self.history.calls[i];
            let mut next = model.clone();
            let result = next.apply(&call.command);
            let next_revision = revision + u64::from(next != model);
            if matches(call, &next, next_revision, result)
                && let Some(mut order) = self.visit(next, next_revision, done | (1 << i))?
            {
                order.insert(0, i);
                return Ok(Some(order));
            }
        }
        self.failed.insert(key);
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Op, Spec};

    fn setup() -> Vec<Command> {
        vec![Command::Create(
            0,
            Spec {
                lower: 0,
                upper: Some(10),
                initial: 5,
                unit: 0,
                scale: 0,
            },
        )]
    }
    // Construct observations from a chosen serial execution, then vary intervals
    // and results to challenge the checker independently of process scheduling.
    fn serial(commands: Vec<Command>) -> History {
        let setup = setup();
        let mut model = Model::default();
        model.apply(&setup[0]).unwrap();
        let mut revision = 1;
        let mut calls = Vec::new();
        for (i, command) in commands.into_iter().enumerate() {
            let old = model.clone();
            let result = model.apply(&command);
            revision += u64::from(old != model);
            let response = match result {
                Ok(()) => {
                    let output = model.output(&command, revision);
                    Response {
                        exit: Some(0),
                        stdout: serde_json::to_string(&output).unwrap(),
                        stderr: String::new(),
                        result: Some(Ok(output)),
                    }
                }
                Err(failure) => {
                    let (code, category, exit) = error_contract(&failure);
                    let error = Error {
                        code,
                        category,
                        message: "model rejection".into(),
                        transaction_id: command.transaction_id().map(tx_id),
                        capacity: failure.capacity().map(Box::new),
                    };
                    Response {
                        exit: Some(exit),
                        stdout: String::new(),
                        stderr: serde_json::to_string(&error).unwrap(),
                        result: Some(Err(error)),
                    }
                }
            };
            calls.push(Call {
                command,
                invocation: (i * 2) as u64,
                completion: (i * 2 + 1) as u64,
                response,
            });
        }
        History {
            case: 0,
            setup,
            calls,
            final_snapshot: model.snapshot(revision),
        }
    }
    fn prepare(id: u16, delta: i64) -> Command {
        Command::Prepare(
            id,
            vec![Op {
                id: 0,
                resource: 0,
                delta,
            }],
            None,
        )
    }
    #[test]
    fn backtracks_across_overlapping_calls_but_preserves_real_time() {
        let mut h = serial(vec![prepare(0, -2), Command::Resource(0), prepare(1, -2)]);
        h.calls.swap(0, 2);
        h.calls.swap(1, 2);
        // Both prepares initially match their individual responses. Trying t1
        // first leads to a dead end: the read saw only t0's hold.
        for (i, call) in h.calls.iter_mut().enumerate() {
            call.invocation = i as u64;
            call.completion = (i + 3) as u64;
        }
        assert_eq!(check(&h).unwrap(), vec![1, 2, 0]);
        h.calls[2].invocation = 0;
        h.calls[2].completion = 1;
        h.calls[1].invocation = 2;
        h.calls[1].completion = 5;
        h.calls[0].invocation = 3;
        h.calls[0].completion = 4;
        assert!(check(&h).unwrap_err().contains("no legal"));
    }
    #[test]
    fn saved_histories_round_trip_and_invalid_observations_fail_closed() {
        let h = serial(vec![prepare(0, -2), Command::Resources]);
        let encoded = serde_json::to_vec(&h).unwrap();
        let mut decoded: History = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(check(&h).unwrap(), check(&decoded).unwrap());
        decoded.calls[0].response.result = None;
        assert!(check(&decoded).is_err());
        let mut h = h;
        h.calls[1].invocation = h.calls[0].completion;
        assert!(check(&h).unwrap_err().contains("event"));
    }
    #[test]
    fn rejects_impossible_reads_duplicate_effects_and_revision_changes() {
        let mut h = serial(vec![
            prepare(0, -3),
            Command::Commit(0, None),
            Command::Commit(0, None),
            Command::Resources,
        ]);
        assert!(check(&h).is_ok());
        h.final_snapshot.resources[0].quantity = "-1".into();
        assert!(check(&h).is_err());
        let mut h = serial(vec![Command::Resource(0)]);
        if let Some(Ok(dto::Output::Resource(resource))) = &mut h.calls[0].response.result {
            resource.quantity = "4".into();
        }
        h.calls[0].response.stdout = serde_json::to_string(
            h.calls[0]
                .response
                .result
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap(),
        )
        .unwrap();
        assert!(check(&h).is_err());
        let mut h = serial(vec![prepare(0, -3), prepare(0, -3)]);
        h.final_snapshot.revision += 1;
        assert!(check(&h).is_err());
    }
    #[test]
    fn checks_frozen_rejections_typed_errors_and_exit_status() {
        let mut h = serial(vec![prepare(0, -6), prepare(1, -2), prepare(0, -6)]);
        assert!(check(&h).is_ok());
        h.calls[2].response.exit = Some(0);
        assert!(check(&h).is_err());
        h.calls[2].response.exit = Some(3);
        if let Some(Err(error)) = &mut h.calls[2].response.result {
            error.capacity.as_mut().unwrap().available = "3".into();
        }
        h.calls[2].response.stderr = serde_json::to_string(
            h.calls[2]
                .response
                .result
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap_err(),
        )
        .unwrap();
        assert!(check(&h).is_err());
    }
}
