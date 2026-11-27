#![cfg(target_os = "linux")]
mod support;
use rescrow_application::dto::*;
use std::{
    cell::RefCell,
    fs,
    path::PathBuf,
    process::{Child, Command as Process, Output as ProcessOutput},
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
fn created() -> u64 {
    static CREATED: std::sync::LazyLock<u64> =
        std::sync::LazyLock::new(rescrow_application::system_clock);
    *CREATED
}
/// The UUIDv7 a test names `name`, created when the test process started, so
/// re-running a prepare is a retry.
fn tx(name: &str) -> String {
    let hash = name.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    });
    rescrow_application::uuid_v7(created(), hash.into())
}
struct Case {
    root: PathBuf,
    http_binary: Option<PathBuf>,
    remote: RefCell<Option<support::RemoteServer>>,
}
impl Case {
    fn new() -> Self {
        let root = std::env::var_os("RESCROW_TEST_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir)
            .join(format!(
                "rescrow-cli-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        fs::create_dir(&root).unwrap();
        let http_binary = std::env::var_os("RESCROW_CLI_TEST_SERVER").map(PathBuf::from);
        assert!(
            http_binary.is_none() || cfg!(feature = "remote"),
            "HTTP scenarios require the CLI remote feature"
        );
        Self {
            root,
            http_binary,
            remote: RefCell::new(None),
        }
    }
    fn process(&self, args: &[&str]) -> Process {
        self.command(args, true)
    }
    fn command(&self, args: &[&str], json: bool) -> Process {
        let mut p = Process::new(env!("CARGO_BIN_EXE_rescrow"));
        // File initialization/readiness checks belong to local composition.
        // Every business operation on the initialized store uses HTTP when
        // this same scenario suite is run with RESCROW_CLI_TEST_SERVER.
        if let Some(binary) = &self.http_binary
            && args != ["init"]
            && self.root.join("store/live.json").exists()
        {
            let mut remote = self.remote.borrow_mut();
            let server =
                remote.get_or_insert_with(|| support::RemoteServer::start(binary, &self.root));
            p.arg("--endpoint")
                .arg(&server.endpoint)
                .arg("--token-file")
                .arg(self.root.join(".rescrow-http-dev/token"))
                .arg("--authority-generation")
                .arg(&server.generation)
                .arg("--require-profile")
                .arg("local");
        } else {
            p.arg("--store").arg(self.root.join("store"));
        }
        if json {
            p.arg("--json");
        }
        p.args(args);
        p
    }
    fn run(&self, args: &[&str]) -> ProcessOutput {
        self.process(args).output().unwrap()
    }
    fn ok(&self, args: &[&str]) -> Output {
        let output = self.run(args);
        assert!(
            output.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stderr.is_empty());
        serde_json::from_slice(&output.stdout).unwrap()
    }
    fn spawn(&self, args: &[&str]) -> Child {
        self.process(args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap()
    }
    /// Waits for a spawned command. Bounded lock contention (exit 5) means "retry
    /// the same request", so this does, as a client following the contract would.
    fn settle(&self, child: Child, args: &[&str]) -> ProcessOutput {
        let mut output = child.wait_with_output().unwrap();
        for _ in 0..10 {
            if output.status.code() != Some(5) {
                break;
            }
            output = self.run(args);
        }
        output
    }
    fn file(&self, name: &str, contents: &str) -> String {
        let path = self.root.join(name);
        fs::write(&path, contents).unwrap();
        path.to_str().unwrap().into()
    }
    fn ready(&self, initial: &str) {
        self.ok(&["init"]);
        self.ok(&[
            "resource",
            "create",
            "r",
            "--initial",
            initial,
            "--upper",
            "100",
            "--unit",
            "u",
        ]);
    }
    /// The whole validated ledger, read through the same file store the CLI uses.
    fn snapshot(&self) -> Snapshot {
        rescrow_application::Application::new(
            rescrow_adapters::file::FileStore::new(self.root.join("store")).unwrap(),
        )
        .snapshot()
        .unwrap()
    }
}
impl Drop for Case {
    fn drop(&mut self) {
        drop(self.remote.get_mut().take());
        let _ = fs::remove_dir_all(&self.root);
    }
}
#[test]
fn separate_process_walkthrough_partial_commit_abort_and_replay() {
    let c = Case::new();
    c.ready("60");
    c.ok(&[
        "resource",
        "create",
        "s",
        "--initial",
        "8",
        "--unit",
        "tokens",
    ]);
    let ops=c.file("ops.json",r#"[{"operation_id":"a","resource_id":"r","delta":"-40"},{"operation_id":"b","resource_id":"s","delta":"-3"}]"#);
    c.ok(&["transaction", "prepare", &tx("work"), "--operations", &ops]);
    let other = c.file(
        "other.json",
        r#"[{"operation_id":"a","resource_id":"r","delta":"30"}]"#,
    );
    c.ok(&[
        "transaction",
        "prepare",
        &tx("other"),
        "--operations",
        &other,
    ]);
    let Output::Resource(r) = c.ok(&["resource", "show", "r"]) else {
        panic!()
    };
    assert_eq!(
        (&r.quantity, &r.available_down, &r.available_up),
        (&"60".into(), &"20".into(), &"10".into())
    );
    let actual = c.file(
        "actual.json",
        r#"[{"operation_id":"a","delta":"-25"},{"operation_id":"b","delta":"0"}]"#,
    );
    c.ok(&["transaction", "commit", &tx("work"), "--actuals", &actual]);
    let before = c.snapshot();
    c.ok(&["transaction", "commit", &tx("work"), "--actuals", &actual]);
    c.ok(&["transaction", "prepare", &tx("work"), "--operations", &ops]);
    assert_eq!(c.snapshot(), before);
    c.ok(&["transaction", "abort", &tx("other")]);
    let Output::Resource(r) = c.ok(&["resource", "show", "r"]) else {
        panic!()
    };
    assert_eq!(r.quantity, "35");
    assert!(r.holds.is_empty());
    let Output::Resource(s) = c.ok(&["resource", "show", "s"]) else {
        panic!()
    };
    assert_eq!(s.quantity, "8");
    c.ok(&["resource", "list"]);
    c.ok(&["transaction", "list"]);
    c.ok(&["transaction", "show", &tx("work")]);
    assert_eq!(
        c.run(&["transaction", "abort", &tx("work")]).status.code(),
        Some(4)
    );
    assert_eq!(
        c.run(&["transaction", "commit", &tx("other")])
            .status
            .code(),
        Some(4)
    );
}
#[test]
fn independent_processes_cannot_over_reserve_scarce_capacity() {
    let c = Case::new();
    c.ready("5");
    let ops = c.file(
        "ops.json",
        r#"[{"operation_id":"o","resource_id":"r","delta":"-1"}]"#,
    );
    let ids: Vec<_> = (0..20).map(|n| tx(&format!("t{n}"))).collect();
    let requests: Vec<_> = ids
        .iter()
        .map(|id| ["transaction", "prepare", id, "--operations", &ops])
        .collect();
    let children: Vec<_> = requests.iter().map(|args| c.spawn(args)).collect();
    let mut successes = 0;
    for (child, args) in children.into_iter().zip(&requests) {
        let result = c.settle(child, args);
        match result.status.code() {
            Some(0) => successes += 1,
            Some(3) => {}
            other => panic!(
                "unexpected {other:?}: {}",
                String::from_utf8_lossy(&result.stderr)
            ),
        }
    }
    assert_eq!(successes, 5);
    let snapshot = c.snapshot();
    assert_eq!(snapshot.transactions.len(), 20);
    assert_eq!(snapshot.resources[0].reserved_down, "5");
    assert_eq!(snapshot.resources[0].quantity, "5");
}
#[test]
fn identical_prepare_and_commit_replays_publish_once() {
    let c = Case::new();
    c.ready("5");
    let ops = c.file(
        "ops.json",
        r#"[{"operation_id":"o","resource_id":"r","delta":"-3"}]"#,
    );
    for args in [
        vec!["transaction", "prepare", &tx("same"), "--operations", &ops],
        vec!["transaction", "commit", &tx("same")],
    ] {
        let before = c.snapshot().revision;
        let children: Vec<_> = (0..12).map(|_| c.spawn(&args)).collect();
        for child in children {
            let out = c.settle(child, &args);
            assert!(
                out.status.success(),
                "{}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
        assert_eq!(c.snapshot().revision, before + 1);
    }
    assert_eq!(c.snapshot().resources[0].quantity, "2");
}
#[test]
fn commit_abort_race_has_exactly_one_terminal_decision() {
    let c = Case::new();
    c.ready("50");
    let ops = c.file(
        "ops.json",
        r#"[{"operation_id":"o","resource_id":"r","delta":"-1"}]"#,
    );
    let mut commits = 0;
    for n in 0..12 {
        let id = tx(&format!("t{n}"));
        c.ok(&["transaction", "prepare", &id, "--operations", &ops]);
        let (commit_args, abort_args) = (
            ["transaction", "commit", id.as_str()],
            ["transaction", "abort", id.as_str()],
        );
        let commit = c.spawn(&commit_args);
        let abort = c.spawn(&abort_args);
        let committed = c.settle(commit, &commit_args);
        let aborted = c.settle(abort, &abort_args);
        assert_ne!(committed.status.success(), aborted.status.success());
        if committed.status.success() {
            commits += 1;
            assert_eq!(aborted.status.code(), Some(4));
        } else {
            assert_eq!(committed.status.code(), Some(4));
        }
        let before = c.snapshot();
        if committed.status.success() {
            c.ok(&["transaction", "commit", &id]);
        } else {
            c.ok(&["transaction", "abort", &id]);
        }
        assert_eq!(c.snapshot(), before);
    }
    assert_eq!(
        c.snapshot().resources[0].quantity,
        (50 - commits).to_string()
    );
    assert!(c.snapshot().resources[0].holds.is_empty());
}
#[test]
fn new_id_prints_distinct_current_uuid_v7s() {
    let id = || {
        let out = Process::new(env!("CARGO_BIN_EXE_rescrow"))
            .arg("new-id")
            .output()
            .unwrap();
        assert!(out.status.success());
        String::from_utf8(out.stdout).unwrap().trim_end().to_owned()
    };
    let (before, first, second) = (rescrow_application::system_clock(), id(), id());
    assert_ne!(first, second);
    let millis = rescrow_application::uuid_v7_millis(&first).unwrap();
    assert!((before..=rescrow_application::system_clock()).contains(&millis));
    let c = Case::new();
    c.ready("5");
    let ops = c.file(
        "ops.json",
        r#"[{"operation_id":"o","resource_id":"r","delta":"-1"}]"#,
    );
    c.ok(&["transaction", "prepare", &first, "--operations", &ops]);
}
#[test]
fn machine_errors_invalid_inputs_and_exact_large_signed_values() {
    let c = Case::new();
    let missing = c.run(&["resource", "list"]);
    assert!(missing.stdout.is_empty());
    assert_eq!(missing.status.code(), Some(6));
    let error: rescrow_application::Error = serde_json::from_slice(&missing.stderr).unwrap();
    assert_eq!(error.code, rescrow_application::Code::MissingStore);
    c.ok(&["init"]);
    c.ok(&[
        "resource",
        "create",
        "large",
        "--lower",
        "-9007199254740994",
        "--initial",
        "-9007199254740993",
        "--unit",
        "units",
    ]);
    let Output::Resource(r) = c.ok(&["resource", "show", "large"]) else {
        panic!()
    };
    assert_eq!(r.quantity, "-9007199254740993");
    let invalid = c.file(
        "bad.json",
        r#"[{"operation_id":"o","resource_id":"large","delta":-1}]"#,
    );
    let before = c.snapshot();
    let output = c.run(&[
        "transaction",
        "prepare",
        &tx("bad"),
        "--operations",
        &invalid,
    ]);
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert_eq!(c.snapshot(), before);
    let ops = c.file(
        "good.json",
        r#"[{"operation_id":"o","resource_id":"large","delta":"-2"}]"#,
    );
    assert_eq!(
        c.run(&[
            "transaction",
            "prepare",
            &tx("reject"),
            "--operations",
            &ops
        ])
        .status
        .code(),
        Some(3)
    );
    assert_eq!(c.snapshot().transactions.len(), 1);
    assert_eq!(c.run(&["init"]).status.code(), Some(4));
    let human = c
        .command(&["resource", "show", "large"], false)
        .output()
        .unwrap();
    assert!(human.status.success());
    assert!(
        String::from_utf8(human.stdout)
            .unwrap()
            .contains("quantity=-9007199254740993")
    );
}
#[test]
fn an_abandoned_hold_lapses_when_a_later_prepare_needs_its_capacity() {
    let c = Case::new();
    c.ready("10");
    let all = c.file(
        "all.json",
        r#"[{"operation_id":"o","resource_id":"r","delta":"-10"}]"#,
    );
    let prepare = |name: &str, extra: &[&str]| {
        let id = tx(name);
        let mut args = vec!["transaction", "prepare", &id, "--operations", &all];
        args.extend_from_slice(extra);
        c.run(&args)
    };
    let held = prepare("abandoned", &["--hold", "1s"]);
    assert!(held.status.success());
    let Output::Transaction(held) = serde_json::from_slice(&held.stdout).unwrap() else {
        panic!("expected a transaction")
    };
    assert_eq!(held.request.hold_ms, Some(1_000));
    assert!(held.expires_at_ms.is_some());
    // While the hold lasts, its capacity is unavailable.
    assert_eq!(prepare("early", &[]).status.code(), Some(3));
    std::thread::sleep(std::time::Duration::from_millis(1_100));
    assert!(prepare("later", &[]).status.success());
    let Output::Transaction(lapsed) = c.ok(&["transaction", "show", &tx("abandoned")]) else {
        panic!("expected a transaction")
    };
    assert_eq!(
        lapsed.state,
        State::Aborted {
            cause: AbortReason::Lapsed,
            capacity: None,
        }
    );
    assert_eq!(
        c.run(&["transaction", "commit", &tx("abandoned")])
            .status
            .code(),
        Some(4)
    );
    // Durations need a unit and must fit the 30-day limit.
    for invalid in ["10", "5x", "31d", "0s"] {
        assert_eq!(
            prepare("invalid", &["--hold", invalid]).status.code(),
            Some(2),
            "{invalid}"
        );
    }
}
