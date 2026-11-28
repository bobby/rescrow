"""Synthetic fixtures test evidence validation; they are not AWS measurements."""

import copy
import contextlib
import io
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace

SPEC = importlib.util.spec_from_file_location(
    "dsql_report", Path(__file__).with_name("dsql-report.py")
)
REPORT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REPORT)
LEFT = "arn:aws:dsql:us-east-1:123456789012:cluster/aaaaaaaaaaaaaaaaaaaaaaaaaa"
RIGHT = "arn:aws:dsql:us-west-2:123456789012:cluster/bbbbbbbbbbbbbbbbbbbbbbbbbb"
GENERATION = "01900000-0000-7000-8000-000000000001"
RUN = "01900000-0000-7000-8000-000000000002"
RESERVATION = "01900000-0000-7000-8000-000000000003"
START = 1795800000000  # Fixed, minute-aligned fixture time.


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.root = Path(self.scratch.name)

    def save(self, name, value):
        path = self.root / name
        path.write_text(json.dumps(value))
        return path

    def summary(self):
        return {
            "run_id": RUN,
            "authority_generation": GENERATION,
            "measurement_started_at_ms": START + 2000,
            "measurement_finished_at_ms": START + 12000,
            "billing_window_started_at_ms": START,
            "billing_window_finished_at_ms": START + 60000,
            "billing_clock_guard_ms": 2000,
            "outcomes": {"committed": 1},
            "completed_reservations": 1,
            "unknown_outcomes_including_warmup": 0,
            "complete": True,
        }

    def run_files(self):
        self.save(
            "configuration.json",
            {
                "run_id": RUN,
                "build_profile": "debug",
                "arguments": {
                    "clients": 1,
                    "iterations": 1,
                    "warmup": 0,
                    "profile": "simulation",
                },
            },
        )
        self.save("summary.json", self.summary())
        self.save(
            "plans.jsonl",
            {
                "worker": 0,
                "iteration": 0,
                "warmup": False,
                "prepare": {"reservation_id": RESERVATION},
            },
        )
        self.save(
            "events.jsonl",
            {
                "worker": 0,
                "iteration": 0,
                "warmup": False,
                "reservation_id": RESERVATION,
                "prepare_us": 100,
                "commit_us": 200,
                "lifecycle_us": 301,
                "outcome": "committed",
                "error": None,
            },
        )

    def topology(self):
        paths = []
        for name, arn in [("left.json", LEFT), ("right.json", RIGHT)]:
            paths.append(
                self.save(
                    name,
                    {
                        "cluster_arn": arn,
                        "cluster_arns": [LEFT, RIGHT],
                        "authority_generation": GENERATION,
                        "capabilities": {
                            "durability_profile": "enterprise-mr",
                            "failure_domain": "region",
                            "active_regions": ["us-east-1", "us-west-2"],
                            "witness_region": "us-east-2",
                        },
                    },
                )
            )
        return paths

    def log(self, times=None):
        before = {key: 0 for key in REPORT.COUNTERS}
        after = {key: 10 for key in REPORT.COUNTERS}
        times = times or [START + 200, START + 1200, START + 12800, START + 13800]
        lines = [
            f'{REPORT.iso(START - 5000)} INFO serving authority_generation="{GENERATION}"'
        ]
        for time, counters in zip(times, [before, before, after, after]):
            lines.append(
                f"{REPORT.iso(time)} INFO rescrow_http::dsql: DSQL cumulative measurements measurements={json.dumps(counters)}"
            )
        path = self.root / "server.log"
        path.write_text("\n".join(lines))
        return path

    def prices(self, sums=(100, 200)):
        entries = []
        for index, (arn, dpu) in enumerate(zip((LEFT, RIGHT), sums)):
            region, request = REPORT.metric_request(arn, START, START + 60000)
            filename = f"dpu-{index}.json"
            self.save(
                filename,
                {
                    "cluster_arn": arn,
                    "region": region,
                    "request": request,
                    "response": {
                        "Label": "TotalDPU",
                        "Datapoints": [{"Timestamp": REPORT.iso(START), "Sum": dpu}],
                    },
                },
            )
            entries.append(
                {
                    "metrics_file": filename,
                    "price_usd_per_million_dpu": str(index + 2),
                    "pricing_source": "https://example.invalid/synthetic-unit-test-price",
                    "price_checked_at": REPORT.iso(START),
                }
            )
        return self.save("prices.json", {"peers": entries})

    def test_run_cross_checks_identities_counts_unknowns_and_completion(self):
        self.run_files()
        REPORT.validate_run(self.root)
        summary = self.summary()
        summary["completed_reservations"] = 2
        self.save("summary.json", summary)
        with self.assertRaisesRegex(ValueError, "summary disagrees"):
            REPORT.validate_run(self.root)
        summary = self.summary()
        summary["complete"] = False
        self.save("summary.json", summary)
        with self.assertRaisesRegex(ValueError, "completion flag"):
            REPORT.validate_run(self.root)

    def test_event_duplicates_and_invented_success_cannot_pass(self):
        self.run_files()
        path = self.root / "events.jsonl"
        path.write_text(path.read_text() + "\n" + path.read_text())
        with self.assertRaisesRegex(ValueError, "duplicate event"):
            REPORT.validate_run(self.root)
        self.run_files()
        event = REPORT.load(path)
        event["outcome"] = "invented_success"
        self.save("events.jsonl", event)
        with self.assertRaisesRegex(ValueError, "unknown event outcome"):
            REPORT.validate_run(self.root)

    def test_only_two_matching_distinct_peers_are_accepted(self):
        paths = self.topology()
        self.assertEqual(set(REPORT.topology(paths, GENERATION)), {LEFT, RIGHT})
        with self.assertRaisesRegex(ValueError, "duplicate topology"):
            REPORT.topology([paths[0], paths[0]], GENERATION)
        other = REPORT.load(paths[1])
        other["authority_generation"] = RUN
        self.save("right.json", other)
        with self.assertRaisesRegex(ValueError, "generation differs"):
            REPORT.topology(paths, GENERATION)
        paths = self.topology()
        other = REPORT.load(paths[1])
        other["cluster_arns"] = [LEFT.replace("a" * 26, "c" * 26), RIGHT]
        self.save("right.json", other)
        with self.assertRaisesRegex(ValueError, "topologies or accounts disagree"):
            REPORT.topology(paths, GENERATION)

    def test_counter_windows_require_idle_boundaries_and_no_restarts(self):
        summary = self.summary()
        path = self.log()
        delta = REPORT.adapter_log(path, summary)
        self.assertEqual(delta["counters"]["sql_statement_attempts"], 10)
        text = path.read_text()
        path.write_text(
            text.replace(
                '"sql_statement_attempts": 0', '"sql_statement_attempts": 20', 1
            )
        )
        with self.assertRaisesRegex(ValueError, "counter reset"):
            REPORT.adapter_log(path, summary)
        path = self.log([START - 18000, START - 8000, START + 12800, START + 13800])
        with self.assertRaisesRegex(ValueError, "too far"):
            REPORT.adapter_log(path, summary)

    def test_both_peers_are_summed_once_using_their_respective_prices(self):
        peers = REPORT.topology(self.topology(), GENERATION)
        result = REPORT.cost(self.prices(), self.summary(), peers)
        self.assertEqual(result["estimated_dpu_cost_usd"], "0.0008")
        self.assertEqual(
            result["estimated_dpu_usd_per_million_completed_reservations"], "800.0000"
        )
        summary = self.summary()
        summary["complete"] = False
        self.assertIsNone(
            REPORT.cost(self.root / "prices.json", summary, peers)[
                "estimated_dpu_usd_per_million_completed_reservations"
            ]
        )

    def test_missing_datapoints_wrong_windows_and_double_counted_peers_fail(self):
        peers = REPORT.topology(self.topology(), GENERATION)
        path = self.prices()
        metric = REPORT.load(self.root / "dpu-0.json")
        metric["response"]["Datapoints"] = []
        self.save("dpu-0.json", metric)
        with self.assertRaisesRegex(ValueError, "missing DPU datapoints"):
            REPORT.cost(path, self.summary(), peers)
        self.prices()
        metric = REPORT.load(self.root / "dpu-0.json")
        metric["request"]["Statistics"] = ["Average"]
        self.save("dpu-0.json", metric)
        with self.assertRaisesRegex(ValueError, "wrong metric"):
            REPORT.cost(path, self.summary(), peers)
        self.prices()
        manifest = REPORT.load(path)
        manifest["peers"][1] = copy.deepcopy(manifest["peers"][0])
        self.save("prices.json", manifest)
        with self.assertRaisesRegex(ValueError, "duplicated"):
            REPORT.cost(path, self.summary(), peers)

    def test_invalid_numeric_and_duplicate_json_inputs_are_rejected(self):
        for value in ("NaN", "Infinity", "-1"):
            with self.assertRaises(ValueError):
                REPORT.decimal(value)
        with self.assertRaisesRegex(ValueError, "duplicate JSON"):
            REPORT.parse('{"Sum": 1, "Sum": 0}')
        with self.assertRaises(ValueError):
            REPORT.integer(True)
        with self.assertRaisesRegex(ValueError, "minute aligned"):
            REPORT.metric_request(LEFT, START + 1, START + 60000)

    def test_cost_rejects_unguarded_or_different_billing_windows(self):
        peers = REPORT.topology(self.topology(), GENERATION)
        prices = self.prices()
        summary = self.summary()
        summary["billing_clock_guard_ms"] = 0
        with self.assertRaisesRegex(ValueError, "clock guard"):
            REPORT.cost(prices, summary, peers)
        summary = self.summary()
        summary["measurement_started_at_ms"] = START
        with self.assertRaisesRegex(ValueError, "missing guards"):
            REPORT.cost(prices, summary, peers)
        metric = REPORT.load(self.root / "dpu-0.json")
        metric["request"]["EndTime"] = REPORT.iso(START + 120000)
        self.save("dpu-0.json", metric)
        with self.assertRaisesRegex(ValueError, "time window"):
            REPORT.cost(prices, self.summary(), peers)

    def test_busy_counter_boundary_cannot_be_attributed_to_the_run(self):
        path = self.log()
        parts = path.read_text().rsplit('"sql_statement_attempts": 10', 1)
        path.write_text('"sql_statement_attempts": 11'.join(parts))
        with self.assertRaisesRegex(ValueError, "other traffic"):
            REPORT.adapter_log(path, self.summary())

    def test_partial_report_stays_unqualified_and_copied_logs_cannot_double_counts(
        self,
    ):
        self.run_files()
        args = SimpleNamespace(
            run_directory=self.root,
            topology=[],
            server_log=[],
            dpu_manifest=None,
            output=self.root / "evidence.json",
        )
        with contextlib.redirect_stdout(io.StringIO()):
            REPORT.report(args)
        evidence = REPORT.load(args.output)
        self.assertFalse(evidence["measurement_evidence_complete"])
        self.assertIn("pending", evidence["enterprise_qualification"])
        self.assertEqual(len(evidence["sources"]), 4)
        path = self.log()
        copied = self.root / "copy.log"
        copied.write_text(path.read_text())
        args.server_log = [path, copied]
        args.output = self.root / "duplicate.json"
        with self.assertRaisesRegex(ValueError, "duplicate server log content"):
            REPORT.report(args)

    def test_collection_keeps_the_exact_query_and_never_runs_a_shell(self):
        args = SimpleNamespace(
            cluster_arn=LEFT,
            start_ms=START,
            end_ms=START + 60000,
            profile="qualification",
            output=self.root / "collected.json",
        )
        response = {"Label": "TotalDPU", "Datapoints": []}
        with patch.object(
            REPORT.subprocess,
            "run",
            return_value=SimpleNamespace(returncode=0, stdout=json.dumps(response)),
        ) as execute:
            REPORT.collect(args)
        command = execute.call_args.args[0]
        self.assertEqual(command[:3], ["aws", "--region", "us-east-1"])
        self.assertNotIn("shell", execute.call_args.kwargs)
        record = REPORT.load(args.output)
        self.assertEqual(
            record["request"]["Dimensions"], [{"Name": "ClusterId", "Value": "a" * 26}]
        )
        self.assertEqual(record["response"], response)
        with self.assertRaisesRegex(ValueError, "already exists"):
            REPORT.collect(args)

    def test_failed_collection_does_not_publish_an_artifact_or_diagnostics(self):
        args = SimpleNamespace(
            cluster_arn=LEFT,
            start_ms=START,
            end_ms=START + 60000,
            profile=None,
            output=self.root / "failed.json",
        )
        with patch.object(
            REPORT.subprocess,
            "run",
            return_value=SimpleNamespace(returncode=1, stderr="private diagnostics"),
        ):
            with self.assertRaisesRegex(ValueError, "check AWS login/profile") as error:
                REPORT.collect(args)
        self.assertNotIn("private", str(error.exception))
        self.assertFalse(args.output.exists())


if __name__ == "__main__":
    unittest.main()
