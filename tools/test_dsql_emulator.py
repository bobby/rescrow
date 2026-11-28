"""Prevent emulator qualification tooling from using ambient AWS access or leaking tokens."""
import argparse
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("dsql_emulator", Path(__file__).with_name("dsql-emulator.py"))
EMULATOR = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(EMULATOR)


class EmulatorIsolation(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.config = self.root / "config"
        self.ca = self.root / "ca.pem"
        for path in (self.config, self.ca, self.root / "emulator_credentials"):
            path.write_text("fixture")
        self.meta = {"message": "emulator server information",
                     "endpoint_url": "https://127.0.0.1:12345",
                     "config_file": str(self.config), "ca_file": str(self.ca)}
        self.log = self.root / "startup.log"
        self.log.write_text(json.dumps({"fields": self.meta}) + "\n")

    def args(self):
        binary = self.root / "binary"
        binary.write_text("fixture")
        return argparse.Namespace(startup_log=self.log, sql_port=15442,
                                  emulator=binary, aws=binary, probe=binary,
                                  output=self.root / "result")

    def test_requires_literal_https_loopback(self):
        for endpoint in ("https://dsql.us-east-1.amazonaws.com", "http://127.0.0.1:443",
                         "https://127.0.0.1.example.com:443", "https://localhost:443",
                         "https://user@127.0.0.1:443", "https://127.0.0.1:443/path",
                         "https://127.0.0.1:443?next=aws"):
            with self.subTest(endpoint=endpoint):
                self.log.write_text(json.dumps({"fields": {**self.meta, "endpoint_url": endpoint}}))
                with self.assertRaises(ValueError):
                    EMULATOR.metadata(self.log)

    def test_discards_ambient_credentials_and_service_overrides(self):
        with patch.dict(os.environ, {
            "AWS_PROFILE": "real-account", "AWS_ACCESS_KEY_ID": "real-key",
            "AWS_SECRET_ACCESS_KEY": "real-secret", "AWS_SESSION_TOKEN": "real-session",
            "AWS_ENDPOINT_URL_DSQL": "https://real-dsql.example",
            "AWS_ENDPOINT_URL_STS": "https://real-sts.example",
            "AWS_CONTAINER_CREDENTIALS_FULL_URI": "http://metadata.example",
            "PGHOST": "real-db.example", "PGPASSWORD": "real-password",
        }):
            env = EMULATOR.local_environment(self.meta)
        for key in ("AWS_PROFILE", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
                    "AWS_ENDPOINT_URL_DSQL", "AWS_ENDPOINT_URL_STS",
                    "AWS_CONTAINER_CREDENTIALS_FULL_URI", "PGHOST", "PGPASSWORD"):
            self.assertNotIn(key, env)
        self.assertEqual(env["AWS_SHARED_CREDENTIALS_FILE"], str(self.root / "emulator_credentials"))
        self.assertEqual(env["AWS_ENDPOINT_URL"], self.meta["endpoint_url"])
        self.assertEqual(env["AWS_EC2_METADATA_DISABLED"], "true")

    def test_wrong_account_stops_before_create_cluster(self):
        with patch.object(EMULATOR, "command", return_value='{"Account":"123456789012"}') as call:
            with self.assertRaisesRegex(ValueError, "refusing to create"):
                EMULATOR.run(self.args())
        self.assertEqual(call.call_count, 1)
        self.assertIn("get-caller-identity", call.call_args.args[0])

    def test_incompatible_driver_preserves_report_without_token(self):
        args = self.args()
        token = "signed-emulator-token-not-for-artifacts"
        cluster = {"identifier": "fixture", "status": "ACTIVE", "endpoint": "127.0.0.1",
                   "arn": "arn:aws:dsql:us-east-1:000000000000:cluster/fixture"}
        data = {"adapter_prerequisites_passed": False,
                "checks": {"bound_query": {"passed": False, "sqlstate": "0A000"}},
                "reservation_semantics_qualified": False}
        calls = []

        def command(argv, env):
            calls.append(argv)
            self.assertNotIn(token, argv)
            if "get-caller-identity" in argv:
                return '{"Account":"000000000000"}'
            if "create-cluster" in argv or "get-cluster" in argv:
                return json.dumps(cluster)
            if "generate-db-connect-admin-auth-token" in argv:
                return token
            if "--port" in argv:
                self.assertEqual(env["PGPASSWORD"], token)
                return json.dumps(data)
            return "fixture version"

        with patch.object(EMULATOR, "command", side_effect=command), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(EMULATOR.run(args), 2)
        report = json.loads((args.output / "compatibility.json").read_text())
        self.assertFalse(report["real_multi_region_qualified"])
        self.assertFalse(report["reservation_histories_run"])
        self.assertFalse(report["authenticated_http_benchmark_run"])
        for path in args.output.iterdir():
            self.assertNotIn(token, path.read_text())
        for argv in calls:
            if "sts" in argv or "dsql" in argv:
                self.assertEqual(argv[argv.index("--endpoint-url") + 1], self.meta["endpoint_url"])


if __name__ == "__main__":
    unittest.main()
