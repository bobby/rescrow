# AWS Emulator DSQL compatibility

Actual AWS use is deferred at the user's request. Local development now includes
the official [AWS Emulator](https://github.com/aws/aws-emulator), without a real
AWS profile or account. Emulator evidence does not qualify DSQL replication,
durability, security, performance or cost.

## Observed result

On 2026-11-27 (2026-11-28 UTC), the signed Linux amd64 release **1.0.665**,
commit `461e190cfbed2a60f922688bafee74a0fd88eec8`, produced these results with
AWS CLI 2.42.5 and the repository's SQLx 0.9.1. The
[machine-readable report](evidence/aws-emulator-1.0.665-dsql.json) includes binary
and source hashes and the emulated cluster description.

| Check | Result |
| --- | --- |
| Signed STS identity | Account `000000000000`, the emulator account. |
| DSQL CreateCluster and GetCluster | Passed; emulated cluster reported ACTIVE. |
| Native SQL TLS and generated admin auth token | Passed with certificate and hostname verification. |
| SQLx simple query | Passed. |
| Repeatable Read BEGIN / ROLLBACK | Commands accepted; isolation semantics were not tested. |
| Schema-qualified DDL | Failed: `42601`, `unknown database public`. |
| SQLx bound query | Failed: `0A000`, `this endpoint serves the simple query protocol; message type 'P' is not supported`. |

The emulator's [pinned wire-protocol source](https://github.com/aws/aws-emulator/blob/461e190cfbed2a60f922688bafee74a0fd88eec8/services/common/src/sql_wire_planes/pg_wire.rs#L26-L30)
explicitly rejects the extended query protocol. The adapter uses SQLx bound
queries for stored data, so this release cannot run the adapter's reservation
histories or authenticated HTTP benchmark. ACTIVE metadata and accepted
transaction commands do not establish reservation semantics.

The production adapter remains unchanged. Keep the existing PostgreSQL tests as
separate simulation evidence and retry this compatibility gate with future AWS
Emulator releases. A passing gate permits adapter integration work; it does not
qualify any storage guarantees. Real multi-Region qualification and its
performance/cost decision remain deferred, rather than failed on this evidence.

## Reproduce

Install or extract the official AWS Emulator Linux package using
[AWS's installation and signature-verification instructions](https://docs.aws.amazon.com/emulator/latest/userguide/getting-started-linux.html).
Record `aws-emulator --version`; releases change frequently. This measurement used
the package signature verified against the documented publishing key fingerprint
`98BC F336 A3BE 53B7 34E9 67C5 2E5B F49B D940 F624`.
AWS CLI v2 and this repository's Rust toolchain are also required.

Start a fresh emulator in one terminal. Port 15432 must be free; the single-port
listener range makes the first cluster's native SQL port deterministic. The
emulator's control API gets a separate, automatically selected port.

```sh
umask 077
mkdir -p target/aws-emulator
EMULATOR_LISTENER_PORT_RANGE=15432-15432 \
  aws-emulator --bind 127.0.0.1 --port 0 --auth on \
  --telemetry off --idle-timeout 300 \
  > target/aws-emulator/startup.log 2> target/aws-emulator/notice.log
```

In another terminal, wait for the `emulator server information` line in the log,
then run:

```sh
cargo build -p rescrow-dsql --example emulator_probe --locked
python3 tools/dsql-emulator.py \
  --startup-log target/aws-emulator/startup.log --sql-port 15432 \
  --emulator "$(command -v aws-emulator)" --aws "$(command -v aws)" \
  --probe target/debug/examples/emulator_probe \
  --output target/aws-emulator/result
```

The tool requires a literal loopback HTTPS endpoint, ignores inherited AWS
credentials/profiles and service endpoint overrides, and uses only the
emulator's generated credential/config/CA files. Every API request pins the local
endpoint explicitly, and STS must identify the emulator account before cluster
creation. TLS verification remains enabled. The generated DB token stays in a
subprocess environment; it is not written into command arguments or artifacts.
No changes to the user's AWS configuration are required.

Exit **0** means all five data-plane prerequisites passed, **2** means the probe
completed but prerequisites failed, and **1** means setup or execution failed.
Artifacts are created exclusively; use a new output directory for another run.
Each run creates one new emulated cluster. Restart the emulator before rerunning
with the same single-port listener range.

Stop the emulator with Ctrl+C after the check. Its resources are disposable;
clean shutdown removes its generated credential files. AWS documents that the
emulator [is not validated for load testing and cannot establish cloud security or multi-Region behavior](https://docs.aws.amazon.com/emulator/latest/userguide/what-is-service.html#what-is-service-limitations).
Do not feed emulated CloudWatch data into a real DPU cost report.
