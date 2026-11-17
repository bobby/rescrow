#![cfg(target_os = "linux")]
use rescrow_application::dto::*;
use std::{
    fs,
    path::PathBuf,
    process::{Child, Command as Process, Output as ProcessOutput},
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
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
    fn snapshot(&self) -> Snapshot {
        serde_json::from_slice(&fs::read(self.root.join("store/state.json")).unwrap()).unwrap()
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
    c.ok(&["transaction", "prepare", "work", "--operations", &ops]);
    let other = c.file(
        "other.json",
        r#"[{"operation_id":"a","resource_id":"r","delta":"30"}]"#,
    );
    c.ok(&["transaction", "prepare", "other", "--operations", &other]);
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
    c.ok(&["transaction", "commit", "work", "--actuals", &actual]);
    let before = c.snapshot();
    c.ok(&["transaction", "commit", "work", "--actuals", &actual]);
    c.ok(&["transaction", "prepare", "work", "--operations", &ops]);
    assert_eq!(c.snapshot(), before);
    c.ok(&["transaction", "abort", "other"]);
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
    c.ok(&["transaction", "show", "work"]);
    assert_eq!(
        c.run(&["transaction", "abort", "work"]).status.code(),
        Some(4)
    );
    assert_eq!(
        c.run(&["transaction", "commit", "other"]).status.code(),
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
    let mut children = Vec::new();
    for n in 0..20 {
        children.push(c.spawn(&[
            "transaction",
            "prepare",
            &format!("t{n}"),
            "--operations",
            &ops,
        ]));
    }
    let mut successes = 0;
    for child in children {
        let result = child.wait_with_output().unwrap();
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
        vec!["transaction", "prepare", "same", "--operations", &ops],
        vec!["transaction", "commit", "same"],
    ] {
        let before = c.snapshot().revision;
        let children: Vec<_> = (0..12).map(|_| c.spawn(&args)).collect();
        for child in children {
            let out = child.wait_with_output().unwrap();
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
        let tx = format!("t{n}");
        c.ok(&["transaction", "prepare", &tx, "--operations", &ops]);
        let commit = c.spawn(&["transaction", "commit", &tx]);
        let abort = c.spawn(&["transaction", "abort", &tx]);
        let committed = commit.wait_with_output().unwrap();
        let aborted = abort.wait_with_output().unwrap();
        assert_ne!(committed.status.success(), aborted.status.success());
        if committed.status.success() {
            commits += 1;
            assert_eq!(aborted.status.code(), Some(4));
        } else {
            assert_eq!(committed.status.code(), Some(4));
        }
        let before = c.snapshot();
        if committed.status.success() {
            c.ok(&["transaction", "commit", &tx]);
        } else {
            c.ok(&["transaction", "abort", &tx]);
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
    let output = c.run(&["transaction", "prepare", "bad", "--operations", &invalid]);
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert_eq!(c.snapshot(), before);
    let ops = c.file(
        "good.json",
        r#"[{"operation_id":"o","resource_id":"large","delta":"-2"}]"#,
    );
    assert_eq!(
        c.run(&["transaction", "prepare", "reject", "--operations", &ops])
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
