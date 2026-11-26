#![cfg(target_os = "linux")]
use rescrow_application::dto::*;
use std::{
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
        Self { root }
    }
    fn process(&self, args: &[&str]) -> Process {
        let mut p = Process::new(env!("CARGO_BIN_EXE_rescrow"));
        p.arg("--store")
            .arg(self.root.join("store"))
            .arg("--json")
            .args(args);
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
    let human = Process::new(env!("CARGO_BIN_EXE_rescrow"))
        .arg("--store")
        .arg(c.root.join("store"))
        .args(["resource", "show", "large"])
        .output()
        .unwrap();
    assert!(human.status.success());
    assert!(
        String::from_utf8(human.stdout)
            .unwrap()
            .contains("quantity=-9007199254740993")
    );
}
