# Rescrow

Rescrow is a Rust implementation of local durable escrow transactions for resource budgets. Reserve a bounded quantity, coordinate work, and settle actual usage while other agents and workflows continue using the remaining capacity.

The first milestone implements a pure Domain, a synchronous Application API with validated transfer types, and a file-backed local CLI. [PLAN.md](PLAN.md) records the scope and completion evidence. [DESIGN.md](DESIGN.md) describes the broader proposed distributed protocol, security model, and enterprise qualification targets. Enterprise performance and durability remain unqualified. A small local benchmark is described below; it is not a throughput or latency guarantee.

## Build and run locally

Requires Linux and Rust/Cargo 1.96.0, pinned in `rust-toolchain.toml`. No HTTP server, cloud account, or async runtime is required.

```sh
cargo build -p rescrow-cli --locked
# Optional installation:
cargo install --path crates/rescrow-cli --locked
```

Select a store explicitly with `--store` **before** the command. Its parent directory must already exist. Initialization creates the store with owner-only permissions; an existing directory must also have owner-only permissions. Each command below is a separate process. Use a fresh `.rescrow` directory for this example; `init` never overwrites existing state.

```sh
target/debug/rescrow --store .rescrow init
target/debug/rescrow --store .rescrow resource create credits --initial 60 --upper 100 --unit credits
target/debug/rescrow --store .rescrow resource create tokens --initial 8 --unit tokens

target/debug/rescrow --store .rescrow transaction prepare work --operations examples/operations.json
target/debug/rescrow --store .rescrow transaction prepare replenishment --operations examples/credit.json
target/debug/rescrow --store .rescrow resource show credits
# quantity=60, reserved down=40 up=30, available down=20 up=10

target/debug/rescrow --store .rescrow transaction commit work --actuals examples/actuals.json
target/debug/rescrow --store .rescrow transaction abort replenishment
target/debug/rescrow --store .rescrow --json resource list
# credits=35, tokens=8; both resources have no holds

# Identical retries return the existing outcome and do not consume again:
target/debug/rescrow --store .rescrow transaction prepare work --operations examples/operations.json
target/debug/rescrow --store .rescrow transaction commit work --actuals examples/actuals.json
target/debug/rescrow --store .rescrow transaction show work
```

`resource list`, `transaction list`, and `transaction show <id>` inspect one consistent ledger. `transaction commit <id>` without `--actuals` commits the full reservation. `resource create` accepts `--lower` (default `0`), optional `--upper` (otherwise unbounded), required `--initial` and `--unit`, and `--scale` (default `0`, maximum `38`). Scale describes the integer's decimal places; no conversion or floating-point arithmetic occurs. Bounds and metadata are immutable.

Operation files contain a nonempty array of `{ "operation_id", "resource_id", "delta" }` records, with distinct operation IDs and at most one operation per resource. Actual files contain `{ "operation_id", "delta" }` records for **every** original operation. Deltas and quantities are canonical signed decimal strings (`"0"`, `"-25"`, `"100"`): no leading zeros, plus sign, whitespace, exponent, or negative zero. Reservations must be nonzero; actual zero releases unused capacity. Actuals must have the reserved direction and no greater magnitude. The signed minimum is a valid quantity/bound when its offsets fit, but cannot be a reservation because its absolute magnitude is unrepresentable.

Transaction IDs are caller-supplied permanent replay identities. Valid IDs use 1–128 ASCII letters, digits, dots, underscores, or hyphens. Operation order is normalized by ID; changing parameters under an existing transaction ID conflicts. Identical resource creation is a no-op even after its quantity changes. Pending credits cannot fund debits, and pending debits cannot supply upward room. A capacity, total-overflow, or missing-resource rejection is durably recorded as an aborted transaction before its business error is returned. It cannot later reserve under that ID. Malformed requests fail before admission. A missing-resource prepare retains its rejected original request even if that resource is later created. Terminal identities are retained indefinitely. Repeated terminal commands must agree with the existing decision and actuals.

Use `--json` for typed Application results on stdout and structured errors on stderr. Quantities remain strings, including values beyond JavaScript's exact integer range. Success has a `result` tag and a `value` payload where applicable. Errors have stable `category` and `code` fields plus a diagnostic `message`. Transaction operations also include `transaction_id`. New capacity rejections include `capacity.resource_id`, `capacity.requested_delta`, and `capacity.available`, with quantities encoded as strings. These are frozen admission facts: replay returns the original diagnostic even after available capacity changes. Clap usage/help errors are textual. No prompts or hidden process state are involved.

| Exit | Meaning |
| --- | --- |
| 0 | Success. |
| 2 | Invalid input or CLI usage. |
| 3 | Domain capacity/overflow rejection (an admitted prepare rejection is retained). |
| 4 | Identity/terminal conflict, missing referenced object (an admitted prepare rejection is retained), or already initialized. |
| 5 | Bounded lock contention or exhausted CAS retries; retry the same request. |
| 6 | Storage failure, missing/corrupt/unsupported/oversized state, or revision exhaustion. |
| 7 | Unknown publication outcome or failure delivering output; inspect/retry the same identity and request. |

An unknown outcome is never a known rollback. A new transaction ID can reserve again, so preserve the original ID and actuals across retries. After uncertain initialization, inspect the store; retrying `init` validates and synchronizes existing state before reporting `already_initialized`. A directory with an existing lock file but no live snapshot fails closed even for `init`; choose a fresh directory for a new store. A failed initialization before publication may leave such a directory.

## Rust callers and boundaries

The four crates have explicit dependencies:

```text
rescrow-cli -> rescrow-adapters -> rescrow-application -> rescrow-domain
           -> rescrow-application
```

The Domain has no dependencies or serialization. The Application owns `dto::Command`, `dto::Output`, structured errors, versioned snapshots, conversions, and the `SnapshotStore` port. Rust callers can use `create_resource`, `resource`, `resources`, `prepare`, `commit`, `abort`, `transaction`, and `transactions`, each returning its specific DTO type. `initialize` returns `Result<(), Error>`. `execute` dispatches the CLI command enum through those same methods. Domain availability is a named `Availability` value with `available_down`, `available_up`, `reserved_down`, and `reserved_up` getters.

The snapshot port now exchanges `ValidatedSnapshot` values. Adapters convert each decoded `dto::Snapshot` through `TryFrom` once; the result contains a private, shared immutable ledger plus its revision. Candidates built from domain transitions already have their construction proof, so publication needs no domain → DTO → domain validation round trip. `to_dto` provides the storage representation. Custom adapter implementations must update their port signatures to this type. Outbound conversions remain total; reads and CAS checks still validate every newly loaded image and perform the required synchronization. An adapter supplies revisioned load and conditional publication; the Application reevaluates from fresh state on conflicts, up to 32 attempts. Reads and identical replays do not advance the revision.

[The runnable embedded example](crates/rescrow-adapters/examples/embedded.rs) creates a resource, reserves, partially commits, and inspects it through the Rust API. It can share a store with independent CLI invocations:

```sh
cargo run -p rescrow-adapters --example embedded -- .rescrow
# Running it again returns the same balance (35 example credits).
```

Schema-1 snapshots remain readable. Capacity details are optional only to represent legacy rejected records that did not retain those facts; new admissions always retain them. The conversion validates any supplied detail against the original operation and rejects details attached to other abort causes. Older binaries with strict DTO decoding may reject new records containing this added field.

`rescrow-adapters::memory::MemoryStore` supplies deterministic tests with conflict, pre-publication, and lost-response injection; it makes no durability promise. File-store fault hooks are explicit Rust constructor options used by qualification tests, never activated by CLI environment variables.

### Emriss prototype

The standalone package [`experiments/emriss`](experiments/emriss/Cargo.toml) provides a structurally shared record store using Emriss and `emriss-atom` from the private sibling checkout at `../../evidentsystems/emriss` (tested at revision `3c5183edb50afce0d94d4b5756961c59afda4b79`). It has its own Cargo workspace and lockfile so ordinary Rescrow builds and CI neither resolve nor retrieve that private dependency. Changes to the sibling checkout require revalidation; its source revision is not pinned by a path dependency. It is an embedded experimental path alongside the existing file-backed CLI, not a replacement for the qualified local profile. The default CLI build does not compile Emriss, an async runtime, or cloud SDKs.

The dependency boundaries remain inward. `rescrow-domain::transition` implements pure creation, preparation, commit and abort over the affected resources and transaction, returning private-construction replacement batches. The existing whole-ledger API delegates to these same rules. `rescrow-application::records::RecordApplication` orchestrates asynchronous reads, validates transfer values using the existing `TryFrom` conversions, checks accessed hold/transaction relationships, and retries publication conflicts up to 32 times. Outbound `From` conversions remain total. The Application's `RecordStore` and immutable `ReadView` interfaces contain no Emriss types.

The adapter stores Application-owned `records::Record` values (root schema/revision, resource DTOs and transaction DTOs) in one Emriss map. An adapter-local codec wrapper maps the existing Serde record shape to canonical CBOR maps, arrays, strings and integer metadata; quantities retain canonical decimal strings. This uses `serde_json::Value` as an in-memory representation bridge, not JSON text embedded in stored CBOR. Stable schema ID `rescrow-record01` identifies the prototype record family. Keys and embedded record IDs must agree. Encoding/decoding does not construct Domain values or perform business validation; that remains the Application's conversion boundary.

Every command loads one immutable map root. It reads the target transaction, its participant resources and those resources' active hold identities; it does not load all retained terminal transactions. Changes edit only affected paths, flush their dependencies, and CAS-publish one atom commit. Conflicts cause fresh evaluation; failed or uncertain publication never becomes a successful business response. Read-only operations and identical retries do not publish. One root still serializes writes. A resource's holds and a transaction's operations are currently eager record fields, so work scales with the affected records and their active holds/participants, rather than being constant in their size.

`initialize()` creates an empty authority. `import(snapshot)` fully validates a schema-1 snapshot before creating an authority, and never replaces an existing one. `audit()` and list commands traverse and validate the entire ledger, including materialized balances versus retained transaction history. Point operations validate accessed records/relationships without that historical reconstruction. Thus an unrelated corrupt record or a locally well-formed but historically incorrect balance may remain undetected until accessed or audited. Page digests establish byte integrity, not domain correctness. The namespace and root must be exclusively managed by these validated transitions; arbitrary Emriss roots cannot be adopted through this API.

```sh
cargo run --manifest-path experiments/emriss/Cargo.toml --example emriss_ledger --locked
cargo test --manifest-path experiments/emriss/Cargo.toml --locked
cargo fmt --manifest-path experiments/emriss/Cargo.toml -- --check
cargo clippy --manifest-path experiments/emriss/Cargo.toml --all-targets --locked -- -D warnings
```

The example uses `MemoryObjects` and `MemoryRefs`, reopens through independent handles, settles a multi-resource reservation, and checks an identical retry. These backends lose state with their last owner. `EmrissStore::new` accepts caller-supplied object/ref backends; the ref store must be dedicated or scoped to this authority's `ledger` atom. No durable local Emriss filesystem adapter or live cloud qualification is included. All referenced pages, commits and the mutable ref would need the selected deployment's durability before acknowledgement; a durable ref alone is insufficient. The Emriss format remains experimental, with no migration or reclamation provided here. Its per-record/page limits also differ from the file adapter's reserved settlement-space policy: this prototype does not yet qualify settlement headroom at those limits. It must not be used to claim the existing file profile or the proposed enterprise guarantees.

The prototype tests compare generated histories against the independent model, exercise the same 16 concurrent workloads as the CLI through independent Emriss instances, force an actual competing CAS, inject failures before/after commit publication and response loss, reject corrupt records and hold relationships, and validate imports/revision exhaustion. A 2,000-terminal-transaction check verifies that a single edit writes less than a quarter of the original packed history, an identical retry writes no collection pages, and a cold point read fetches fewer than 30 ranges. These are regression bounds over memory-backed object storage, not latency or durable-backend benchmarks. Concurrent failures save observation histories under `target/emriss-histories` by default, or under `RESCROW_TEST_ROOT` when selected; the replay command in the concurrent histories section can recheck their typed observations (transport fields are synthesized for embedded calls).

## Local persistence and limits

The selected canonical store directory contains `state.json`, a stable `state.lock`, and possibly orphan `.state-*.tmp` candidates. Every load takes an OS shared lock. Publication takes an exclusive lock, reloads and compares the revision, writes a uniquely created temporary file in the same directory, synchronizes it, renames it over `state.json`, and synchronizes the directory before success. The parent directory is also synchronized at initialization. Reads and replays synchronize the live file and directory to complete a previous uncertain publication. Files use mode `600`, directories `700`; existing broader permissions are rejected. Lock acquisition times out after two seconds, independently of the bounded Application CAS retry policy.

The [Rust rename contract](https://doc.rust-lang.org/std/fs/fn.rename.html) supplies same-filesystem replacement; Linux requires [directory synchronization](https://man7.org/linux/man-pages/man2/fsync.2.html) to persist the directory entry. The adapter never truncates the live state or promotes an orphan candidate. Missing, invalid, oversized (over 16 MiB), or unsupported live snapshots fail closed. There is no automatic backup selection, reset, or rollback after uncertain publication. Every publication also reserves settlement space: the encoded snapshot must fit with every outstanding transaction's largest committed/aborted state, all mutable resource numbers at their maximum decimal widths, and a 20-digit revision. Holds disappearing during settlement only reduce the bound. This check applies to resource creation and admitted rejection records as well as preparations, so later commands cannot spend earlier holds' settlement space. A write can return `too_large` before the live file reaches 16 MiB. Embedded callers may select a smaller limit with `FileStore::with_size_limit`; cooperating callers should select the same limit for a store. The CLI uses 16 MiB. The guarantee covers size growth for snapshots published under this policy, not disk-full errors or revision exhaustion. Older snapshots remain readable, but an existing image that already lacks headroom cannot acquire this guarantee retroactively; writes still fail closed if the bound does not fit. CLI JSON input files also have a 16 MiB ceiling. Even an unbounded resource has finite representable upward room because quantities, offsets, and totals must fit `i128`.

All participants, holds, replay records, and final effects are published in one ledger snapshot. There are no partially published participants or separate materialization steps to recover. Whole-file validation/rewrites grow with resources and permanently retained transaction history; publications serialize through one lock. This is one logical shard per resource and one cooperating local user per store. It does not enforce budgets against anyone who can directly rewrite the authoritative store. Network filesystems, manual live editing, hard-link/path manipulation, and device loss are outside the local contract.

Reservations never expire automatically. Claims, external execution, trusted metering/refunds, hierarchy, sharding, signed audit history, HTTP, SQLite, and cloud backends remain deferred. The distributed protocol and enterprise RPO/RTO targets in DESIGN.md remain proposed and unqualified.

## Verification

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build -p rescrow-cli --locked
cargo tree --workspace --locked

# Select an existing directory on a local persistent filesystem for process tests:
mkdir -p target/qualification
RESCROW_TEST_ROOT="$PWD/target/qualification" cargo test --workspace --locked
```

The suite enumerates small positive/negative reservation sets and partial/zero/abort decisions against independent capacity/balance formulas, tests conversion corruption and replay conflicts, and injects stale revisions and effect failures. Independent CLI processes contend for scarce capacity and race commit/abort and identical retries. File tests kill child publishers before/during writes, after file sync, after rename, and before response delivery, then reload, retry, and check released OS locks. Other tests exercise permission failures, bounded lock contention, unsupported/truncated/oversized state, and injected directory-sync failures.

These checks have been exercised on this development host's Linux tmpfs and Btrfs filesystems. They establish behavior across process termination/restart and simulated I/O failures. They do **not** qualify physical power loss, machine reboot, arbitrary devices/controllers, or regional failover. The local benchmark below measures only this development setup.

### Generated histories

Proptest is a development-only dependency of `rescrow-adapters`; it adds no dependency to the Domain or installed CLI. [generated_histories.rs](crates/rescrow-adapters/tests/generated_histories.rs) checks the Application against an independent small-integer model in [escrow_model.rs](crates/rescrow-adapters/tests/support/escrow_model.rs). The model derives materialized quantities and pending minimum/maximum quantities from transaction history, rather than maintaining the production ledger's quantity/hold structures or calling production transition functions.

The history property generates up to 128 commands over finite and unbounded resources: creation, multi-resource preparation, normalized replays, changed requests, full/partial/zero commits, aborts, reads, malformed operation/actual sets, and missing references. After each command it compares the full stored snapshot, returned result/error, frozen capacity diagnostics, and revision behavior with the model. Selected publication points inject CAS conflicts, known pre-publication failures, or applied writes with lost responses; retries use the identical command through a new Application instance. Selected steps round-trip typed JSON and reload through the inbound validation boundary.

A separate generated lifecycle property guarantees successful multi-resource preparations, partial/zero/full settlement or abort, terminal retries, and terminal conflicts. Each property runs 256 cases by default and shrinks failures into smaller inputs. Run them independently or increase the case count with a reproducible seed:

```sh
cargo test -p rescrow-adapters --test generated_histories --locked
PROPTEST_CASES=2048 PROPTEST_RNG_SEED=42 cargo test -p rescrow-adapters --test generated_histories --locked
```

Failure seeds are persisted in `crates/rescrow-adapters/proptest-regressions/generated_histories.txt` and replayed on subsequent runs. Commit files from genuine failures with their fixes. Seed replay depends on the strategies and dependency version; a seed is not an OS schedule. These tests cover sequential histories, small-integer business rules, serialization, and simulated publication faults. Existing numeric-edge, conversion, filesystem-interruption, and concurrent CLI tests remain necessary; the generated model does not qualify process concurrency or device durability.

### Concurrent histories

[`concurrent_histories.rs`](crates/rescrow-cli/tests/concurrent_histories.rs) runs 16 bounded scenarios, each with three concurrent clients issuing four separate CLI processes in sequence against one store. The scenarios vary reservation direction and magnitude, finite/unbounded resources, normalized/changed prepare retries, and full/zero settlement. They mix multi-resource prepares, partial commits and identical commit retries, commit/abort races, capacity rejections, and resource/transaction reads. The first call intervals overlap through a client barrier; subsequent calls follow each client's observed completion, without imposing a publication order.

Each call records invocation/completion events from one shared monotonic counter, its model command, typed result/error, exit status, and stdout/stderr. The checker reuses the independent transaction-history model above and searches for a sequential ordering matching every observation while preserving completion-before-invocation order. It also checks the final complete snapshot, including holds, retained rejections, and revision. The search memoizes failed states and fails explicitly on its limits of 16 calls or 100,000 explored states. Checker regression tests challenge backtracking, real-time constraints, impossible reads, duplicate effects, replay revisions, frozen capacity diagnostics, and saved-history decoding.

```sh
cargo test -p rescrow-cli --test concurrent_histories --locked
# Repeat one workload; the OS schedule can differ on every run:
RESCROW_HISTORY_CASE=7 cargo test -p rescrow-cli --test concurrent_histories --locked
# Recheck an exact recorded observation history without starting new processes:
RESCROW_HISTORY_REPLAY=/path/to/history.json cargo test -p rescrow-cli --test concurrent_histories --locked
```

Failures preserve `history.json`, input files, and the live store under `target/concurrent-histories/history-*` by default, or under `RESCROW_TEST_ROOT` when selected. Separate `setup.json` and `calls.json` preserve observations even if the final snapshot cannot be decoded. Successful cases remove their directories. Use failing commands and observations to construct regression tests with their fixes. Replay checks recorded evidence and does not reproduce the OS schedule; an impossible recorded history remains impossible after an implementation fix. These bounded scenarios do not shrink automatically or exhaust all concurrent schedules. They run on Unix hosts for smoke coverage; Linux CI exercises the supported local profile. Process concurrency checks do not qualify physical power-loss or device durability.

## Local benchmark

`crates/rescrow-adapters/examples/snapshot_bench.rs` seeds 0, 1,000, and 10,000 retained zero-usage commits, then measures resource inspection, identical resource-create replay, and a prepare/abort pair. It uses the real file adapter, locks, and synchronization. The output reports seeded snapshot bytes and median microseconds over seven samples after two warm-up cycles; each mutation cycle retains another terminal transaction.

```sh
mkdir -p target/bench
cargo run --release --locked -p rescrow-adapters --example snapshot_bench -- target/bench
# For isolating CPU/encoding overhead from persistent-device latency:
cargo run --release --locked -p rescrow-adapters --example snapshot_bench -- /tmp
```

Development measurement on 2026-11-17, Rust 1.96.0 release builds, Linux tmpfs: alternating two runs of baseline `f20e048` and two of this implementation, using the same harness (adapted only for the snapshot port). Seeded sizes were 262, 191,261, and 1,910,261 bytes. The table gives the range of each run's median at 10,000 retained transactions:

| Operation | Baseline | Updated |
| --- | --- | --- |
| Resource inspection | 124.7–138.5 ms | 56.7–72.0 ms |
| Identical create replay | 120.7–258.5 ms | 79.0–88.4 ms |
| Prepare + abort | 857.7–1,124.7 ms | 548.1–757.1 ms |

Whole-harness user+system CPU time, including fixture construction and all three sizes, was 5.40–5.51 seconds before and 4.03–4.05 seconds after. Scheduler contention and persistent-filesystem synchronization made wall-clock results noisy; these small samples are evidence of reduced repeated-conversion work on this host, not an SLO, throughput qualification, or a power-loss test. In particular, tmpfs does not measure persistent-device durability latency. Snapshot validation and rewrites still scale with retained history.

## Guiding values

Simplicity is a product and engineering value. Following Rich Hickey's [Simple Made Easy](https://www.infoq.com/presentations/Simple-Made-Easy/), keep independent concerns from becoming intertwined. Pursue elegant designs whose parts and relationships are clear. Every abstraction, layer, dependency, and service should earn its place by resolving a concrete concern. Prefer direct functions, explicit values, and removing superseded machinery.

We follow the techniques in Scott Wlaschin's [Domain Modeling Made Functional](https://pragprog.com/titles/swdddf/domain-modeling-made-functional/), adapted to Rust:

1. **Domain:** a purely functional library of valid domain types and the functions/methods that implement business rules. Private construction and state-specific types make illegal local states unrepresentable.
2. **Application:** a usable Rust API that coordinates domain operations, input/output, and side effects. It owns transfer types for exchanging and storing data. Inbound `TryFrom` conversions validate transfer values into domain values; outbound `From` conversions are infallible. This is the anti-corruption boundary.
3. **Entrypoints and adapters:** CLI, HTTP, embedded composition, and infrastructure implementations invoke or support the Application. Dependencies point inward. Domain logic is shared across deployment shapes.

State-dependent and concurrent invariants still need explicit domain checks and conditional persistence; types alone cannot prove that a stored version is current. See [the architecture](DESIGN.md#11-rust-implementation-structure) for responsibilities and conversion rules.

## Deployment spectrum

The same Domain and Application are intended to support:

- An embedded Rust caller or local CLI coordinating concurrent agents against one durable local store.
- An optional local service for continuous recovery or a stronger local access boundary.
- Regional HTTP deployments with explicit backend durability guarantees.
- Enterprise services spanning multiple US Regions, with qualified regional-loss RPO zero and RTO at most 60 seconds.

Local use is first-class and requires no cloud account or HTTP server. The first implementation uses a versioned JSON file, OS locking, and synchronized atomic replacement so independent CLI invocations share one local authority. Local qualification covers process interruption/restart and simulated I/O failures on the tested filesystems; physical power-loss/device qualification remains outstanding. SQLite remains a possible later adapter if the simple file store becomes insufficient. Enterprise deployments add globally consistent authority, regional redundancy, identity, isolation, and independent audit evidence. The reference enterprise authority is DynamoDB MRSC; S3 Standard provides checkpoints/retained evidence, and Express can provide disposable acceleration. These are proposed designs and acceptance targets requiring validation.

All shapes preserve escrow bounds, durable decisions, retry identity, and safe settlement/refund rules. A backend must meet the selected profile; selecting local operation cannot silently weaken an enterprise namespace.

## Contributing

The following instructions apply to all contributors, human and agent, throughout this repository. Read [DESIGN.md](DESIGN.md) for the current technical proposal. Agent contributors must also read [AGENTS.md](AGENTS.md) for agent-specific instructions. Keep shared contributor guidance here and reserve AGENTS.md for instructions specific to agents.

The local snapshot milestone is implemented; the broader distributed and enterprise design remains proposed. Do not imply that deferred APIs, commands, guarantees, benchmarks, or crates already exist. Keep the public explanation, architecture, and implementation consistent as the project develops.

### Simplicity and design judgment

Treat simplicity in Rich Hickey's sense as a governing value: keep independent concerns from becoming intertwined. Pursue elegance in design and implementation. Prefer explicit domain concepts, values, and direct functions. Add an abstraction, dependency, layer, crate, or service only for a concrete requirement or demonstrated separation of concerns, and explain the coupling it removes. Consolidate or remove superseded mechanisms. A smaller line count, a familiar framework, or a generic interface is not itself evidence of simplicity.

The three architectural responsibilities below are intentional. They do not justify generic repository frameworks, mediator buses, universal effect interpreters, or a separate crate for every concept. Start with cohesive modules and split packages when a real dependency/build/distribution boundary requires it.

### Architectural boundaries

Follow the techniques of *Domain Modeling Made Functional*, adapted to Rust. Dependencies point inward: entrypoints/adapters may depend on the Application and Domain; the Application may depend on the Domain; the Domain depends on neither outer layer.

- **Domain:** purely functional types and functions/methods implementing business logic. Use private fields, validated constructors, newtypes, and enums with state-specific payloads to make illegal local states unrepresentable. No I/O, ambient time/randomness, logging, serialization derives that bypass validation, runtime/storage/framework types, or cloud SDK dependencies. Supply time and identity facts explicitly. No unsafe code in the Domain.
- **Application:** public Rust use cases, transfer types, anti-corruption conversions, side-effect orchestration, and the small interfaces its effects require. Own publication order, CAS retries and unknown outcomes, durable recovery, and input/output. Keep backend version tokens and transport/storage encoding here rather than in the Domain. Do not import concrete adapters or CLI/HTTP frameworks.
- **Outer composition:** CLI, HTTP, embedded hosts, and infrastructure adapters. Adapters implement Application-owned interfaces; composition roots select and wire them. Handlers parse transport, establish identity, invoke Application methods, and render results. Do not duplicate or bypass domain rules here.

Pure domain functions validate rules involving current state. Conditional persistence validates concurrency. Types do not prove that a valid value is still current or that a remote side effect occurred. Preserve all three responsibilities explicitly.

### Transfer-type conversions

Implement fallible `TryFrom<TransferType> for DomainType` and infallible `From<DomainType> for TransferType` (or a borrowed equivalent). Keep concrete conversion implementations in the Application, which owns the transfer types. The Domain must not import DTOs.

Validate every inbound source, including stored records, recovery tickets, replayed history, and migrations. Deserialization alone does not establish domain validity. Reject invalid values with typed errors; never use panics, truncation, silent defaults, or unchecked construction to cross this boundary. Outbound `From` must be total. Encoding, I/O, and incompatible schema migration can fail separately. Conversions must not hide network/storage access.

Reuse transfer contracts where they actually match. Introduce additional DTOs only for a real representation, version, or trust-boundary difference. Do not serialize domain types directly to make an adapter convenient.

### Deployment shapes and guarantees

Support local on-device/CLI use and embedded Rust use through the same Application as regional and enterprise HTTP deployments. Local use must not require AWS credentials, an HTTP server, a remote identity provider, or compilation of cloud SDKs. Coordinate concurrent local processes through one transactional local authority; separate in-memory instances do not share a budget. A daemon is optional, and recovery needs a running process.

Keep profile capabilities explicit. Local/device durability and enterprise multi-region guarantees differ; enterprise namespaces must never silently downgrade. Do not weaken reservation bounds, stable replay identity, claim/expiration safety, or refund conservation to simplify a deployment. Preserve enterprise RPO/RTO and security requirements when changing enterprise composition. Do not promise enforcement against an agent that can directly rewrite its authoritative store.

### Verification and documentation

Check representational invariants and transitions in pure Domain tests; check conversion failures/round trips, publication ordering, retries, recovery, and effect failures at the Application boundary; check actual consistency/durability semantics in adapter conformance tests. Test independent local CLI processes sharing one store and qualify regional failover against real enterprise infrastructure before claiming support.

Choose checks appropriate to the change. Do not invent a build/test command before a toolchain exists or write tests that merely mirror low-impact documentation edits. Preserve the distinction between design targets and measured results. Update related README/design examples when contracts change, and describe both the effect of an architectural change and the complexity it adds or removes.
