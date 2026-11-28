#!/usr/bin/env python3
"""Probe an already-running official AWS Emulator; never select real AWS targets."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from urllib.parse import urlsplit


def metadata(path):
    for line in path.read_text().splitlines():
        try:
            fields = json.loads(line).get("fields", {})
        except (ValueError, AttributeError):
            continue
        if fields.get("message") == "emulator server information":
            endpoint = urlsplit(fields["endpoint_url"])
            if (endpoint.scheme != "https" or endpoint.hostname != "127.0.0.1"
                    or not endpoint.port or endpoint.username or endpoint.password
                    or endpoint.path not in ("", "/") or endpoint.query or endpoint.fragment):
                raise ValueError("requires an HTTPS endpoint on literal 127.0.0.1")
            return fields
    raise ValueError("startup log has no emulator server information")


def local_environment(meta):
    # Ignore all inherited service-specific endpoints, profiles, credentials,
    # signing settings and SQL destinations. Use only this run's generated files.
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(("AWS_", "PG"))}
    config = Path(meta["config_file"])
    credentials = config.parent / "emulator_credentials"
    for path in (config, credentials, Path(meta["ca_file"])):
        if not path.is_file():
            raise ValueError("emulator credential/config/CA file is absent; restart it")
    env.update(
        AWS_ENDPOINT_URL=meta["endpoint_url"],
        AWS_CONFIG_FILE=str(config), AWS_SHARED_CREDENTIALS_FILE=str(credentials),
        AWS_CA_BUNDLE=meta["ca_file"], SSL_CERT_FILE=meta["ca_file"],
        AWS_REGION="us-east-1", AWS_DEFAULT_REGION="us-east-1",
        AWS_EC2_METADATA_DISABLED="true", AWS_DISABLE_HOST_PREFIX_INJECTION="true",
        AWS_PAGER="", AWS_CLI_AUTO_PROMPT="off",
    )
    return env


def command(argv, env, *, timeout=30):
    result = subprocess.run(argv, env=env, capture_output=True, text=True,
                            timeout=timeout, check=False)
    if result.returncode:
        # SDK/CLI errors can contain signed URLs or connection credentials.
        raise ValueError(f"{Path(argv[0]).name} failed (exit {result.returncode}); output withheld")
    return result.stdout.strip()


def save(path, value):
    with path.open("x", encoding="utf-8") as output:
        json.dump(value, output, indent=2, sort_keys=True)
        output.write("\n")


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def run(args):
    os.umask(0o077)
    meta = metadata(args.startup_log)
    env = local_environment(meta)
    args.output.mkdir(mode=0o700, parents=True, exist_ok=False)
    cli = [str(args.aws.resolve()), "--endpoint-url", meta["endpoint_url"],
           "--region", "us-east-1", "--no-cli-pager"]

    def aws(*arguments):
        return command([*cli, *arguments], env)

    # Check identity before creating anything. Explicit CLI endpoint overrides
    # even configuration-file endpoints; the credential chain is emulator-only.
    identity = json.loads(aws("sts", "get-caller-identity"))
    if identity.get("Account") != "000000000000":
        raise ValueError("STS did not identify the emulator account; refusing to create a cluster")
    cluster = json.loads(aws("dsql", "create-cluster", "--no-deletion-protection"))
    save(args.output / "created-cluster.json", cluster)
    deadline = time.monotonic() + 20
    while cluster.get("status") != "ACTIVE":
        if time.monotonic() >= deadline:
            raise ValueError("emulated cluster did not become ACTIVE")
        time.sleep(0.1)
        cluster = json.loads(aws("dsql", "get-cluster", "--identifier", cluster["identifier"]))
    # Always exercise GetCluster, even if CreateCluster already reports ACTIVE.
    cluster = json.loads(aws("dsql", "get-cluster", "--identifier", cluster["identifier"]))
    save(args.output / "active-cluster.json", cluster)
    if (cluster.get("endpoint") != "127.0.0.1" or cluster.get("status") != "ACTIVE"
            or not cluster.get("arn", "").startswith("arn:aws:dsql:us-east-1:000000000000:cluster/")):
        raise ValueError("cluster is not an active loopback emulator resource")
    # Keep generated credentials and signed tokens out of argv and artifacts.
    token = aws("dsql", "generate-db-connect-admin-auth-token", "--hostname", "127.0.0.1")
    probe_env = {**env, "PGPASSWORD": token}
    data = json.loads(command([str(args.probe.resolve()), "--port", str(args.sql_port),
                               "--ca-file", meta["ca_file"]], probe_env))
    report = {
        "kind": "aws-emulator-dsql-compatibility",
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "emulator_version": command([str(args.emulator.resolve()), "--version"], env),
        "emulator_sha256": digest(args.emulator),
        "probe_sha256": digest(args.probe),
        "sources_sha256": {str(path.relative_to(Path(__file__).resolve().parent.parent)): digest(path)
                           for path in (Path(__file__).resolve(),
                                        Path(__file__).resolve().parent.parent / "Cargo.lock",
                                        Path(__file__).resolve().parent.parent / "crates/rescrow-dsql/examples/emulator_probe.rs")},
        "aws_cli_version": command([str(args.aws.resolve()), "--version"], env),
        "control_plane": {"account": identity["Account"], "create_cluster": "passed",
                          "get_cluster": "passed", "cluster": cluster},
        "data_plane": data,
        "reservation_histories_run": False,
        "authenticated_http_benchmark_run": False,
        "real_multi_region_qualified": False,
        "note": "Passing prerequisites would permit adapter integration work, not qualification.",
    }
    save(args.output / "compatibility.json", report)
    passed = data["adapter_prerequisites_passed"]
    print(f"AWS Emulator DSQL prerequisites {'passed' if passed else 'FAILED'}: {args.output / 'compatibility.json'}")
    return 0 if passed else 2


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--startup-log", type=Path, required=True)
    parser.add_argument("--sql-port", type=int, required=True)
    parser.add_argument("--emulator", type=Path, required=True)
    parser.add_argument("--aws", type=Path, required=True)
    parser.add_argument("--probe", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.sql_port <= 65535:
        parser.error("--sql-port must be between 1 and 65535")
    try:
        return run(args)
    except (ValueError, KeyError, OSError, subprocess.TimeoutExpired) as error:
        # TimeoutExpired includes argv but never the environment / token.
        print(f"emulator check failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
