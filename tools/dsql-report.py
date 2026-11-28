#!/usr/bin/env python3
"""Assemble measured HTTP/adapter/DPU evidence. Never declares enterprise qualification.

Uses only the standard library. collect-dpu performs one read-only AWS CLI call;
report validates local artifacts and leaves missing evidence explicit.
"""
import argparse
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import uuid

COUNTERS = {
    "sql_statement_attempts",
    "encoded_payload_read_bytes",
    "encoded_payload_write_attempt_bytes",
    "publication_attempts",
    "applied_responses",
    "conflicts",
    "unknown_publications",
}
ARN = re.compile(r"arn:aws:dsql:([a-z0-9-]{1,20}):([0-9]{12}):cluster/([a-z0-9]{26})\Z")
ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate JSON field")
        result[key] = value
    return result


def parse(text):
    return json.loads(text, object_pairs_hook=unique_object)


def load(path):
    return parse(Path(path).read_text())


def integer(value):
    require(type(value) is int and value >= 0, "expected nonnegative integer")
    return value


def decimal(value):
    try:
        result = Decimal(str(value))
    except InvalidOperation:
        raise ValueError("expected finite nonnegative decimal") from None
    require(result.is_finite() and result >= 0, "expected finite nonnegative decimal")
    return result


def timestamp(value):
    require(isinstance(value, str), "expected timestamp string")
    date = datetime.fromisoformat(value.replace("Z", "+00:00"))
    require(date.tzinfo is not None, "timestamp must include a timezone")
    return int(date.timestamp() * 1000)


def iso(value):
    return datetime.fromtimestamp(value / 1000, timezone.utc).isoformat(
        timespec="milliseconds"
    )


def generation(value):
    require(isinstance(value, str), "expected UUIDv7 string")
    parsed = uuid.UUID(value)
    require(str(parsed) == value and parsed.version == 7, "expected canonical UUIDv7")
    return value


def source(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as file:
        for block in iter(lambda: file.read(65536), b""):
            digest.update(block)
    return {"file": Path(path).name, "sha256": digest.hexdigest()}


def write_new(path, value):
    encoded = json.dumps(value, indent=2, allow_nan=False) + "\n"
    with Path(path).open("x") as file:
        file.write(encoded)


def metric_request(arn, start, end):
    match = ARN.fullmatch(arn)
    require(match is not None, "invalid AWS DSQL cluster ARN")
    require(
        type(start) is int and type(end) is int and 0 <= start < end,
        "invalid billing interval",
    )
    require(
        start % 60000 == end % 60000 == 0 and end - start <= 86400000,
        "billing interval must be minute aligned and at most 24 hours",
    )
    return match[1], {
        "Namespace": "AWS/AuroraDSQL",
        "MetricName": "TotalDPU",
        "Dimensions": [{"Name": "ClusterId", "Value": match[3]}],
        "StartTime": iso(start),
        "EndTime": iso(end),
        "Period": 60,
        "Statistics": ["Sum"],
    }


def collect(args):
    # Check the destination before requesting data; create it exclusively only
    # after successful collection and serialization.
    require(not args.output.exists(), "output already exists")
    region, query = metric_request(args.cluster_arn, args.start_ms, args.end_ms)
    command = ["aws", "--region", region, "--output", "json", "--no-cli-pager"]
    if args.profile:
        command.extend(["--profile", args.profile])
    command.extend(
        ["cloudwatch", "get-metric-statistics", "--cli-input-json", json.dumps(query)]
    )
    result = subprocess.run(command, capture_output=True, text=True, timeout=60)
    require(
        result.returncode == 0,
        "CloudWatch collection failed; check AWS login/profile and permissions",
    )
    write_new(
        args.output,
        {
            "cluster_arn": args.cluster_arn,
            "region": region,
            "request": query,
            "response": parse(result.stdout),
            "collected_at": datetime.now(timezone.utc).isoformat(),
        },
    )


def validate_run(root):
    config = load(root / "configuration.json")
    summary = load(root / "summary.json")
    require(generation(config["run_id"]) == summary["run_id"], "run IDs disagree")
    generation(summary["authority_generation"])
    args = config["arguments"]
    clients, iterations, warmup = (
        integer(args[key]) for key in ("clients", "iterations", "warmup")
    )
    expected = clients * (iterations + warmup)
    require(0 < expected <= 1000000, "invalid plan count")
    planned, coordinates = {}, set()
    with (root / "plans.jsonl").open() as file:
        for line in file:
            plan = parse(line)
            identity = generation(plan["prepare"]["reservation_id"])
            require(identity not in planned, "duplicate planned reservation identity")
            worker, iteration = integer(plan["worker"]), integer(plan["iteration"])
            require(
                worker < clients and iteration < iterations + warmup,
                "plan coordinates outside workload",
            )
            require(
                plan["warmup"] is (iteration < warmup), "plan warmup flag disagrees"
            )
            require(
                (worker, iteration) not in coordinates, "duplicate plan coordinates"
            )
            coordinates.add((worker, iteration))
            planned[identity] = (worker, iteration, plan["warmup"])
    require(len(planned) == expected, "plan count disagrees with configuration")
    observed, outcomes = set(), Counter()
    unknown, failures, measured, committed = 0, 0, 0, 0
    with (root / "events.jsonl").open() as file:
        for line in file:
            event = parse(line)
            require(type(event["warmup"]) is bool, "event warmup must be boolean")
            integer(event["worker"])
            integer(event["iteration"])
            allowed = {
                "committed",
                "unknown_prepare",
                "unknown_commit",
                "failed_prepare",
                "failed_commit",
                "unexpected_response",
                "rejected_prepare",
                "admission_rejected_prepare",
            }
            require(event["outcome"] in allowed, "unknown event outcome")
            prepare = integer(event["prepare_us"])
            commit = (
                integer(event["commit_us"]) if event["commit_us"] is not None else 0
            )
            require(
                integer(event["lifecycle_us"]) >= prepare + commit, "latencies disagree"
            )
            if event["outcome"] in {"committed", "unknown_commit", "failed_commit"}:
                require(
                    event["commit_us"] is not None, "settlement has no commit latency"
                )
            identity = event["reservation_id"]
            require(
                identity in planned and identity not in observed,
                "missing/duplicate event identity",
            )
            require(
                planned[identity]
                == (event["worker"], event["iteration"], event["warmup"]),
                "event disagrees with plan",
            )
            observed.add(identity)
            unknown += event["outcome"].startswith("unknown_")
            failures += event["outcome"] in (
                "failed_prepare",
                "failed_commit",
                "unexpected_response",
            )
            if not event["warmup"]:
                measured += 1
                committed += event["outcome"] == "committed"
                outcomes[event["outcome"]] += 1
    require(
        len(observed) == expected and measured == clients * iterations,
        "incomplete event log",
    )
    require(
        dict(outcomes) == summary["outcomes"]
        and committed == summary["completed_reservations"],
        "summary disagrees with events",
    )
    require(
        unknown == summary["unknown_outcomes_including_warmup"],
        "summary omits unknown outcomes",
    )
    integer(summary["unknown_outcomes_including_warmup"])
    integer(summary["completed_reservations"])
    for count in summary["outcomes"].values():
        integer(count)
    require(
        summary["complete"] is (unknown == 0 and failures == 0),
        "incorrect completion flag",
    )
    start, end = (
        integer(summary[key])
        for key in ("measurement_started_at_ms", "measurement_finished_at_ms")
    )
    require(start < end, "invalid measurement interval")
    return config, summary


def topology(paths, expected_generation):
    peers, common = {}, None
    for path in paths:
        item = load(path)
        arn = item["cluster_arn"]
        match = ARN.fullmatch(arn or "")
        require(
            match is not None and arn not in peers, "invalid or duplicate topology peer"
        )
        require(
            item["authority_generation"] == expected_generation,
            "topology generation differs from HTTP authority",
        )
        cap = item["capabilities"]
        require(
            cap["durability_profile"] == "enterprise-mr"
            and cap["failure_domain"] == "region",
            "topology is not multi-Region",
        )
        active = cap["active_regions"]
        witness = cap["witness_region"]
        members = item["cluster_arns"]
        require(
            len(members) == len(set(members)) == 2 and arn in members,
            "invalid cluster membership",
        )
        member_arns = [ARN.fullmatch(member) for member in members]
        require(
            all(
                member is not None and member[1] in active and member[2] == match[2]
                for member in member_arns
            ),
            "cluster membership disagrees with Regions/account",
        )
        require(
            len(active) == len(set(active)) == 2
            and match[1] in active
            and witness not in active
            and witness,
            "invalid active/witness topology",
        )
        description = (tuple(sorted(active)), witness, match[2], tuple(sorted(members)))
        require(
            common is None or common == description,
            "peer topologies or accounts disagree",
        )
        common = description
        peers[arn] = match[1]
    require(
        len(peers) == 2 and len(set(peers.values())) == 2,
        "provide status from two distinct active peers",
    )
    require(
        set(peers) == set(common[3]),
        "status targets do not match observed peer membership",
    )
    return peers


def adapter_log(path, summary):
    generations, samples = set(), []
    for raw in Path(path).read_text().splitlines():
        line = ANSI.sub("", raw)
        for value in re.findall(r'authority_generation="([^"]+)"', line):
            generations.add(value)
        if (
            "DSQL cumulative measurements" not in line
            and "DSQL final measurements" not in line
        ):
            continue
        marker = line.find("measurements=")
        require(marker >= 0, "adapter log has no measurement object")
        counter, _ = json.JSONDecoder(object_pairs_hook=unique_object).raw_decode(
            line[marker + len("measurements=") :]
        )
        require(set(counter) == COUNTERS, "unknown adapter counter schema")
        for value in counter.values():
            integer(value)
        samples.append((timestamp(line.split()[0]), counter))
    require(
        generations == {summary["authority_generation"]},
        "server log generation differs from run",
    )
    require(
        all(
            a[0] <= b[0] and all(a[1][key] <= b[1][key] for key in COUNTERS)
            for a, b in zip(samples, samples[1:])
        ),
        "adapter clock/counter reset; use one file per process",
    )
    start, end = (
        summary["measurement_started_at_ms"],
        summary["measurement_finished_at_ms"],
    )
    before = [item for item in samples if item[0] <= start]
    after = [item for item in samples if item[0] >= end]
    require(
        len(before) >= 2 and len(after) >= 2,
        "need two quiescent snapshots on each side of measurement",
    )
    require(
        before[-1][1] == before[-2][1] and after[0][1] == after[1][1],
        "counter boundaries include other traffic or incomplete drain",
    )
    require(
        500 <= before[-1][0] - before[-2][0] and 500 <= after[1][0] - after[0][0],
        "quiescent snapshots too close together",
    )
    require(
        start - before[-1][0] <= 5000 and after[0][0] - end <= 5000,
        "counter boundaries too far from measurement",
    )
    delta = {key: after[0][1][key] - before[-1][1][key] for key in COUNTERS}
    return {"before_ms": before[-1][0], "after_ms": after[0][0], "counters": delta}


def cost(manifest_path, summary, peers):
    manifest = load(manifest_path)
    start, end = summary.get("billing_window_started_at_ms"), summary.get(
        "billing_window_finished_at_ms"
    )
    require(start is not None and end is not None, "run needs --align-billing-window")
    require(
        summary.get("billing_clock_guard_ms") == 2000,
        "billing window needs the two-second clock guard",
    )
    require(
        start
        <= summary["measurement_started_at_ms"]
        < summary["measurement_finished_at_ms"]
        <= end,
        "billing window does not enclose measurement",
    )
    require(
        2000 <= summary["measurement_started_at_ms"] - start < 5000
        and 2000 <= end - summary["measurement_finished_at_ms"] < 62000,
        "billing window has missing guards or includes excess time",
    )
    records, total, sources = {}, Decimal(0), [source(manifest_path)]
    for entry in manifest["peers"]:
        path = manifest_path.parent / entry["metrics_file"]
        metric = load(path)
        arn = metric["cluster_arn"]
        require(
            arn in peers and arn not in records,
            "DPU cluster differs from topology or is duplicated",
        )
        region, query = metric_request(arn, start, end)
        require(
            metric["region"] == region and metric["request"] == query,
            "DPU request has wrong metric, cluster, period or time window",
        )
        require(
            metric["response"]["Label"] == "TotalDPU",
            "DPU response has wrong metric label",
        )
        points = {}
        for point in metric["response"]["Datapoints"]:
            time = timestamp(point["Timestamp"])
            require(
                start <= time < end and time % 60000 == 0 and time not in points,
                "duplicate or misaligned DPU datapoint",
            )
            points[time] = decimal(point["Sum"])
        require(
            len(points) == (end - start) // 60000,
            "missing DPU datapoints; do not infer zero from absent data",
        )
        price = decimal(entry["price_usd_per_million_dpu"])
        require(
            price > 0, "provide a positive regional list price; credits are excluded"
        )
        require(
            entry["pricing_source"].startswith("https://"),
            "pricing source must be an HTTPS URL",
        )
        timestamp(entry["price_checked_at"])
        dpu = sum(points.values(), Decimal(0))
        estimate = dpu * price / Decimal(1000000)
        total += estimate
        records[arn] = {
            "region": region,
            "total_dpu": str(dpu),
            "price_usd_per_million_dpu": str(price),
            "estimated_dpu_cost_usd": str(estimate),
            "pricing_source": entry["pricing_source"],
            "price_checked_at": entry["price_checked_at"],
        }
        sources.append(source(path))
    require(set(records) == set(peers), "cost needs both active peers")
    completed = summary["completed_reservations"]
    return {
        "peers": records,
        "estimated_dpu_cost_usd": str(total),
        "estimated_dpu_usd_per_million_completed_reservations": (
            str(total * 1000000 / completed)
            if completed and summary["complete"]
            else None
        ),
        "exclusions": [
            "storage",
            "network",
            "HTTP service compute",
            "observability",
            "taxes/discounts/credits",
        ],
        "sources": sources,
    }


def report(args):
    root = args.run_directory
    config, summary = validate_run(root)
    missing = []
    if not summary["complete"]:
        missing.append("resolved and settled workload")
    if not summary["completed_reservations"]:
        missing.append("completed reservations")
    if config["build_profile"] != "release":
        missing.append("release build")
    if not re.fullmatch(
        r"[0-9a-f]{40}", config.get("build_revision") or ""
    ) or not config.get("build_rustc"):
        missing.append("embedded source revision and Rust toolchain")
    if config["arguments"]["profile"] != "enterprise-mr":
        missing.append("enterprise-mr HTTP requirement")
    peers = (
        topology(args.topology, summary["authority_generation"])
        if args.topology
        else {}
    )
    if not peers:
        missing.append("observed topology from both peers")
    require(
        len(set(path.resolve() for path in args.server_log)) == len(args.server_log),
        "duplicate server log",
    )
    require(
        len({source(path)["sha256"] for path in args.server_log})
        == len(args.server_log),
        "duplicate server log content",
    )
    logs = [adapter_log(path, summary) for path in args.server_log]
    if not logs:
        missing.append("quiescent adapter counters for all serving processes")
    totals = {key: sum(log["counters"][key] for log in logs) for key in COUNTERS}
    dpu = None
    if args.dpu_manifest:
        require(peers, "DPU cost requires topology evidence")
        require(
            config["arguments"].get("align_billing_window") is True,
            "DPU cost needs an explicitly aligned benchmark run",
        )
        dpu = cost(args.dpu_manifest, summary, peers)
    else:
        missing.append("measured DPU cost from both peers")
    completed = summary["completed_reservations"]
    files = [
        root / name
        for name in (
            "configuration.json",
            "summary.json",
            "plans.jsonl",
            "events.jsonl",
        )
    ]
    files += args.topology + args.server_log
    result = {
        "run_id": summary["run_id"],
        "authority_generation": summary["authority_generation"],
        "measurement_evidence_complete": not missing,
        "missing_evidence": missing,
        "enterprise_qualification": "pending correctness and regional-drill evidence; this report alone cannot qualify enterprise service",
        "http": summary,
        "source_revision": config.get("build_revision"),
        "rustc": config.get("build_rustc"),
        "adapter": {
            "process_windows": logs,
            "totals": totals if logs else None,
            "sql_statement_attempts_per_completed_reservation": (
                totals["sql_statement_attempts"] / completed
                if logs and completed
                else None
            ),
            "publication_conflict_rate": (
                totals["conflicts"] / totals["publication_attempts"]
                if totals["publication_attempts"]
                else None
            ),
        },
        "dpu_estimate": dpu,
        "sources": [source(path) for path in files],
        "attribution_assumptions": [
            "Dedicated test cluster and serving processes; no other traffic in the billing/counter windows.",
            "All serving process logs supplied separately; client/servers synchronized within the two-second billing clock guard.",
            "Input prices are operator-verified regional prices; DPU estimate is not an invoice.",
        ],
    }
    write_new(args.output or root / "evidence.json", result)
    print(
        json.dumps(
            {
                "run_id": result["run_id"],
                "measurement_evidence_complete": not missing,
                "missing_evidence": missing,
            }
        )
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    fetch = sub.add_parser(
        "collect-dpu", help="Read TotalDPU from CloudWatch using AWS CLI v2"
    )
    fetch.add_argument("--cluster-arn", required=True)
    fetch.add_argument("--start-ms", type=int, required=True)
    fetch.add_argument("--end-ms", type=int, required=True)
    fetch.add_argument("--profile")
    fetch.add_argument("--output", type=Path, required=True)
    fetch.set_defaults(function=collect)
    local = sub.add_parser("report", help="Validate and join local benchmark evidence")
    local.add_argument("run_directory", type=Path)
    local.add_argument("--server-log", type=Path, action="append", default=[])
    local.add_argument("--topology", type=Path, action="append", default=[])
    local.add_argument("--dpu-manifest", type=Path)
    local.add_argument("--output", type=Path)
    local.set_defaults(function=report)
    args = parser.parse_args()
    try:
        args.function(args)
    except (
        ValueError,
        KeyError,
        TypeError,
        ArithmeticError,
        OSError,
        subprocess.TimeoutExpired,
    ) as error:
        # Do not dump input objects, subprocess stderr or credential diagnostics.
        parser.exit(1, f"Evidence collection failed: {error}\n")


if __name__ == "__main__":
    main()
