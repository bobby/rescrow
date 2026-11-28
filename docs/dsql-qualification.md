# DSQL adapter qualification

Status: experimental implementation; real DSQL qualification and the enterprise
performance decision are pending. PostgreSQL simulation is development evidence.
It does not establish DSQL concurrency, replication, cost, or regional recovery.

The acceptance requirements are [#46](https://github.com/bobby/rescrow/issues/46),
[#47](https://github.com/bobby/rescrow/issues/47), and [DESIGN §9.3](../DESIGN.md#93-provisional-performance-gates).
The HTTP driver pulls reusable work forward from
[#34](https://github.com/bobby/rescrow/issues/34); it does not complete that issue's
PostgreSQL and SlateDB baselines. Routing, N-1 capacity and repeated regional-loss
qualification in #48–#50 remain necessary before production enterprise claims.

## Storage protocol

`rescrow-dsql` implements the Application's synchronous transactional port on a
dedicated Tokio runtime. Async hosts call it through the server's blocking
boundary. AWS dependencies stay out of the default CLI graph. The official
[AWS SQLx connector](https://github.com/awslabs/aurora-dsql-connectors/tree/main/rust/sqlx)
provides IAM tokens, refresh and verified TLS. Its automatic OCC retry feature
is disabled: the Application reloads and reevaluates after conflicts.

The schema has authorities, records, and immutable publications. An authority's
generation is created once and retained across frontends and reconnects. Tenant,
record kind, namespace and ID occupy separate primary-key columns. The maximum
key length stays below 1 KiB because all identity components are bounded ASCII.

Every publication rewrites a marker on **every read dependency**, retaining its
logical version when its business value is unchanged. An absent dependency
becomes a physical tombstone in that transaction. GET never creates tombstones.
This makes both positive and negative dependencies participate in DSQL's write
conflict detection under Repeatable Read; two publications cannot independently
accept contradictory observations. Replacements, markers and history commit
together. Unrelated resources have no shared optimistic counter, except the
existing authenticated tenant outstanding-hold quota.

Each marker identifies the last publication that touched the dependency. History
stores those predecessor IDs and inspection orders the resulting DAG. UUIDv7
sort order alone cannot order publications made in the same millisecond.
There is no global history sequence or timestamp allocator.

A publication inserts its immutable ID into the publication table. A resolver
either returns the exact durable publication or inserts a permanent NULL fence
under that same key. The original publication and the fence cannot both commit.
Absent history by itself never proves rollback. A transport failure or deadline
around COMMIT remains unknown; resolution never blindly resubmits it.

The adapter enforces 128 resource dependencies, at most 2,999 condition rows plus
one history row, a 900 KiB encoded payload-column ceiling and an 8 MiB conservative
mutated-data budget. Existing payloads rewritten by markers count too. Operations
have a configurable 1–120 second deadline and connections retire after 50 minutes.
These sit below the current [DSQL limits](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/CHAP_quotas.html):
1 MiB per non-indexed column, 2 MiB per row, 3,000 mutated rows, 10 MiB per
transaction, 5 minutes per transaction, and 60 minutes per connection.
Large lapse batches can hit an adapter limit before the nominal participant cap;
they fail atomically rather than splitting a publication.

## Configuration and explicit initialization

Use an existing peered cluster or agree on an AWS account/profile, two active
Regions, witness Region and spend cap before provisioning a disposable cluster.
Configure credentials through the normal AWS credential chain, never in JSON or
source control. The runtime identity needs IAM database connection permission and
`dsql:GetCluster` for both peers. Database grants must permit reading authorities,
reading/writing records and inserting/reading publications. Schema installation
and authority initialization use a separately authorized administrative identity.

An adapter configuration file has this shape; replace the sample ARN:

```json
{
  "cluster_arn": "arn:aws:dsql:us-east-1:123456789012:cluster/aaaaaaaaaaaaaaaaaaaaaaaaaa",
  "authority": "qualification",
  "user": "admin",
  "profile": "rescrow-qualification",
  "max_connections": 16,
  "operation_timeout_seconds": 30
}
```

Create a second file for the other active cluster, keeping the authority name the
same. Status reads both live AWS descriptions, requires active reciprocal peers
and a distinct common witness, and verifies the derived SQL endpoint identity.
A single-Region cluster or requested profile label cannot pass this check.

```sh
cargo build -p rescrow-dsql -p rescrow-http --features rescrow-http/dsql --locked
target/debug/rescrow-dsql --config /path/to/active-a.json install-schema
target/debug/rescrow-dsql --config /path/to/active-a.json initialize
target/debug/rescrow-dsql --config /path/to/active-a.json status
target/debug/rescrow-dsql --config /path/to/active-b.json status
```

Each DDL statement executes separately, outside DML. Repeating an interrupted
installation is safe. Ordinary HTTP startup never installs, initializes, resets
or migrates. Initialization never replaces an existing generation.

In the existing [HTTP composition](../API.md), select
`"backend": {"type":"dsql", "config": {...}}` and set
`api.required_profile` to `"enterprise-mr"`. Build the executable with its `dsql`
feature. Retain workload JWT verification, HTTPS for remote listeners, and the
usual request limits. Both serving Regions must report the same generation.
The implementation's capability metadata describes observed infrastructure;
the experimental status still applies until the evidence below exists.

## Development and real qualification

The loopback simulator uses exactly the same schema and port implementation, but
reports only `simulation`, with no cross-process/crash durability claim. Start a
dedicated disposable PostgreSQL container; do not reuse another project's data.

```sh
docker run --detach --name rescrow-dsql-dev --publish 127.0.0.1:55432:5432 \
  --env POSTGRES_HOST_AUTH_METHOD=trust --tmpfs /var/lib/postgresql:rw postgres:18
RESCROW_POSTGRES_SIMULATION_URL=postgres://postgres@127.0.0.1:55432/postgres \
  cargo test -p rescrow-dsql --locked -- --include-ignored --test-threads=1
RESCROW_POSTGRES_SIMULATION_URL=postgres://postgres@127.0.0.1:55432/postgres \
  cargo test -p rescrow-http --features dsql --test dsql --locked -- --ignored
docker rm --force rescrow-dsql-dev
```

Real runs use both active peers and fail if a target is missing. Tests create fresh
authority names in shared tables and retain their records for inspection. They
do not clean arbitrary cluster data or provision AWS resources.

```sh
RESCROW_DSQL_CONFIG=/path/to/active-a.json \
RESCROW_DSQL_PEER_CONFIG=/path/to/active-b.json \
PROPTEST_CASES=256 PROPTEST_RNG_SEED=42 \
  cargo test -p rescrow-dsql --test conformance --locked -- --ignored --test-threads=1
RESCROW_DSQL_CONFIG=/path/to/active-a.json \
  cargo test -p rescrow-http --features dsql --test dsql --locked -- --ignored
```

Retain command output, exact revision/toolchain, timestamps, cluster descriptions,
endpoint/Region mappings and the actual generation. Existing tests cover model
histories, concurrent linearizability, tenant/namespace isolation, causal ordering,
absence write skew, immutable publication resolution, and a lost HTTP response
after durable preparation. Peer qualification opens independent connections,
reads a peer publication, writes through the peer and reopens the authority.
The PostgreSQL-only transport test withholds the server's completed COMMIT
response, then verifies an unknown result resolves to exactly one publication.
Additional real DSQL transport-loss, stale-publisher, process termination and impairment
experiments must accompany these tests before the acceptance gate passes.

## Authenticated HTTP measurements

Build the load generator in release mode:

```sh
cargo build --release -p rescrow-client --example http_bench --locked
target/release/examples/http_bench \
  --endpoint https://active-a.example \
  --token-file /path/to/short-lived-workload-token \
  --profile enterprise-mr --authority-generation ACTUAL-GENERATION-UUID \
  --clients 8 --iterations 1000 --warmup 100 \
  --workload independent --opposing --output /path/to/new-result-directory
```

Use dedicated test-tenant credentials that permit resource creation, preparation,
commit and reads. Tokens are read from files and never included in output. Every
run creates uniquely prefixed resources. `plans.jsonl` is synchronized before
network dispatch and retains all original reservation IDs and actuals.
`configuration.json`, `events.jsonl` and `summary.json` preserve reproducible
workloads and per-call evidence. Unknown outcomes or failed settlements mark the
run incomplete; reconcile the original identities, never invent replacements.

Run independent, hot, and skewed workloads; vary `--hold-ms`,
`--settle-delay-ms`, `--actual-percent`, `--opposing`, `--capacity 10` for near
exhaustion, concurrency, and pool size. A single-tenant independent run includes
the existing shared quota record. Report that contention instead of calling it
independent storage scaling. The driver is closed-loop and does not establish an
open-loop arrival SLO. Preserve server configuration: default per-tenant rates
and concurrency can cap observed throughput before storage does.

Measure from each active Region to its local HTTP endpoint, to the remote
endpoint, and with both Regions writing. Pin the generation for multiple
`--endpoint` arguments. Latencies include client retries. Report successful and
failed calls separately; a fast rejection is not a completed reservation.
Measure batching separately when a batching path has been implemented and
verified; this HTTP path does not currently batch cross-request publications.

HTTP attempt/retry counters and body bytes are measured by the production Rust
client. They exclude headers, TLS, storage traffic and internal SQL retries.
The DSQL executable logs cumulative adapter counters once per second and after
drain: explicit SQL statement attempts, encoded payload bytes, publication
attempts, conflicts and unknown responses. Difference all serving processes'
counters over the measurement window. Statement attempts exclude transaction
control, pool health and DDL; payload bytes exclude keys, headers and TLS. These
are application SQL work measurements, not physical storage requests or bytes.
Physical storage traffic and DPU consumption need separate database measurements;
missing fields stay NULL. Do not rename an application counter to imply a
physical storage metric.

Collect `TotalDPU` with the Sum statistic from **both** active clusters over an
isolated, aligned measurement window. It already includes replication components;
do not add `MultiRegionWriteDPU` again. Use prices for each actual Region and
retain the pricing retrieval date. AWS documents the
[DPU components](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/billing-metering.html),
[per-peer CloudWatch metrics](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/cloudwatch-monitoring.html),
and [regional pricing](https://aws.amazon.com/rds/aurora/dsql/pricing/).
Report DPU-only estimates separately from billed cost, storage, network, service
compute and observability. SQL statement count is not a DPU estimate.

## Decision record and remaining evidence

The decision remains **pending real measurements**. Keep DSQL as the selected
prototype candidate, without claiming the enterprise gates have passed. Attach:

| Evidence | Acceptance |
| --- | --- |
| Real adapter and authenticated HTTP histories | Escrow invariants, exact replay and atomic publication pass on both peers. |
| Lost commit acknowledgement and delayed original publisher | Same identity resolves exactly; a durable NotApplied fence prevents later publication. |
| Independent and hot workloads | p50/p95/p99, completed reservations/s, rejection/OCC rates, retry and byte amplification, sustainable load and limits. |
| Both active Regions plus witness | Observed reciprocal topology and one actual authority generation. |
| Within/across-Region latency | Compare prepare p99 ≤1 s and commit p99 ≤500 ms at stated sustainable load. |
| Regional impairment and restored traffic | Preserve acknowledged state; measure recovery and retry amplification, then test failback. |
| Cost | Isolated, time-aligned measurements from both peers, price inputs, explicit denominator and exclusions. |
| Batching/hot-budget scaling | Measure a verified implementation; do not extrapolate unsharded results to the §9.3 rights-shard target. |

Record measured bottlenecks and the resulting decision in DESIGN. A failed early
unsharded throughput run does not disprove the rights-sharded target. If DSQL
misses the gates after justified improvements, record why and reconsider MRSC
with its separate-key protocol. Full RPO-zero/60-second RTO and N-1 capacity
claims require repeated real drills at contracted load, including authorization
and recovery work, beyond a connectivity blackhole smoke test.
