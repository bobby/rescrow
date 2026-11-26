# Rescrow

Rescrow is a Rust implementation of local durable escrow transactions for resource budgets. Reserve a bounded quantity, coordinate work, and settle actual usage while other agents and workflows continue using the remaining capacity.

The first milestone implements a pure Domain, a synchronous Application API with validated transfer types, and a file-backed local CLI. [DESIGN.md](DESIGN.md) describes the broader proposed distributed protocol, security model, and enterprise qualification targets. Enterprise performance and durability remain unqualified. A small local benchmark is described below; it is not a throughput or latency guarantee.

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

# Create each transaction's ID once, when the request is first made:
work=$(target/debug/rescrow new-id)
replenishment=$(target/debug/rescrow new-id)
target/debug/rescrow --store .rescrow transaction prepare "$work" --operations examples/operations.json
target/debug/rescrow --store .rescrow transaction prepare "$replenishment" --operations examples/credit.json
target/debug/rescrow --store .rescrow resource show credits
# quantity=60, reserved down=40 up=30, available down=20 up=10

target/debug/rescrow --store .rescrow transaction commit "$work" --actuals examples/actuals.json
target/debug/rescrow --store .rescrow transaction abort "$replenishment"
target/debug/rescrow --store .rescrow --json resource list
# credits=35, tokens=8; both resources have no holds

# Identical retries return the existing outcome and do not consume again:
target/debug/rescrow --store .rescrow transaction prepare "$work" --operations examples/operations.json
target/debug/rescrow --store .rescrow transaction commit "$work" --actuals examples/actuals.json
target/debug/rescrow --store .rescrow transaction show "$work"
```

`resource list`, `transaction list`, and `transaction show <id>` inspect one consistent ledger. `transaction commit <id>` without `--actuals` commits the full reservation. `resource create` accepts `--lower` (default `0`), optional `--upper` (otherwise unbounded), required `--initial` and `--unit`, and `--scale` (default `0`, maximum `38`). Scale describes the integer's decimal places; no conversion or floating-point arithmetic occurs. Bounds and metadata are immutable.

Operation files contain a nonempty array of `{ "operation_id", "resource_id", "delta" }` records, with distinct operation IDs and at most one operation per resource. Actual files contain `{ "operation_id", "delta" }` records for **every** original operation. Deltas and quantities are canonical signed decimal strings (`"0"`, `"-25"`, `"100"`): no leading zeros, plus sign, whitespace, exponent, or negative zero. Reservations must be nonzero; actual zero releases unused capacity. Actuals must have the reserved direction and no greater magnitude. The signed minimum is a valid quantity/bound when its offsets fit, but cannot be a reservation because its absolute magnitude is unrepresentable.

`transaction prepare --hold <duration>` lets the hold lapse if it is still prepared that long after admission, for example `--hold 500ms`, `90s`, `15m`, `1h` or `7d`, from 1 ms to 30 days. Without it, a hold lasts until it is committed or aborted. The hold is part of the request, so a retry repeats it, and the store fixes its deadline once, at admission, as `expires_at_ms`; a retry never extends it. A lapse is a durable abort with cause `lapsed`, never a silent removal:

- There is no sweeper. A write that could reserve or release capacity on a resource first lapses every overdue hold there, appending each lapse ahead of its own record in the same synchronized append. Capacity therefore returns exactly when a write needs it, including to the prepare that finds it.
- The store's clock settles the race between a decision and its deadline. A commit or abort evaluated before the deadline wins; after it, the hold has already lapsed, so a commit conflicts (exit 4) and an abort returns the lapsed transaction.
- Reads never write. `resource show` can report a hold past its deadline until a write touches that resource; `transaction show` reports `expires_at_ms`, so a caller can tell.
- The deadline is a lower bound on when capacity returns, not a promise that it returns then. There is no grace period: processes on one machine share a clock, and recorded times never go backward.

A lapsing hold is not an authorization to spend: work that cannot be safely abandoned at its deadline should not ask for one.

Transaction IDs are caller-supplied replay identities. A new prepare's ID must be a [UUIDv7](https://www.rfc-editor.org/rfc/rfc9562#name-uuid-version-7) in canonical lowercase form, created when the client first makes the request and repeated unchanged on every retry; `rescrow new-id` prints one, and any RFC 9562 generator works. Its timestamp bounds the retry window:

- A retry within 24 hours of the ID's timestamp returns the original outcome. A prepared transaction keeps replaying, and can be committed or aborted, at any age.
- A new prepare whose ID is older than 24 hours fails with `expired`. One more than 5 minutes ahead of the store's clock is invalid input. Neither is recorded.
- A commit or abort of a transaction decided more than 24 hours ago reports `not_found`; `transaction show` and `transaction list` still read all history.

An ID therefore never names two transactions: inside the window the original is found, and after it the ID has expired. Operation order is normalized by ID; changing parameters under an existing transaction ID conflicts. Identical resource creation is a no-op even after its quantity changes. Pending credits cannot fund debits, and pending debits cannot supply upward room. A capacity, total-overflow, or missing-resource rejection is durably recorded as an aborted transaction before its business error is returned. It cannot later reserve under that ID. Malformed requests fail before admission. A missing-resource prepare retains its rejected original request even if that resource is later created. Repeated terminal commands must agree with the existing decision and actuals.

Use `--json` for typed Application results on stdout and structured errors on stderr. Quantities remain strings, including values beyond JavaScript's exact integer range. Success has a `result` tag and a `value` payload where applicable. Errors have stable `category` and `code` fields plus a diagnostic `message`. Transaction operations also include `transaction_id`. New capacity rejections include `capacity.resource_id`, `capacity.requested_delta`, and `capacity.available`, with quantities encoded as strings. These are frozen admission facts: replay returns the original diagnostic even after available capacity changes. Clap usage/help errors are textual. No prompts or hidden process state are involved.

| Exit | Meaning |
| --- | --- |
| 0 | Success. |
| 2 | Invalid input or CLI usage. |
| 3 | Domain capacity/overflow rejection (an admitted prepare rejection is retained). |
| 4 | Identity/terminal conflict, missing referenced object (an admitted prepare rejection is retained), expired transaction ID, or already initialized. |
| 5 | Bounded lock contention or exhausted CAS retries; retry the same request. |
| 6 | Storage failure, missing/corrupt/unsupported/oversized state, or revision exhaustion. |
| 7 | Unknown publication outcome or failure delivering output; inspect/retry the same identity and request. |

An unknown outcome is never a known rollback. A new transaction ID can reserve again, so preserve the original ID and actuals across retries, and finish retrying within the 24-hour window. After uncertain initialization, inspect the store; retrying `init` validates and synchronizes existing state before reporting `already_initialized`. A directory with an existing lock file but no live snapshot fails closed even for `init`; choose a fresh directory for a new store. A failed initialization before publication may leave such a directory.

## Rust callers and boundaries

The four crates have explicit dependencies:

```text
rescrow-cli -> rescrow-adapters -> rescrow-application -> rescrow-domain
           -> rescrow-application
```

The Domain has no dependencies or serialization. The Application owns `dto::Command`, `dto::Output`, structured errors, checkpoint and log-entry records, conversions, and the `LogStore` port. Rust callers can use `create_resource`, `resource`, `resources`, `prepare`, `commit`, `abort`, `transaction`, and `transactions`, each returning its specific DTO type. `initialize` returns `Result<(), Error>`. `execute` dispatches the CLI command enum through those same methods. Domain availability is a named `Availability` value with `available_down`, `available_up`, `reserved_down`, and `reserved_up` getters.

A `LogStore` keeps a checkpoint of live state (resources and prepared transactions) and an append-only log of entries, each one record of a resource creation or transaction state change at a sequence number and the time it was recorded. An adapter supplies:

- `load`: the checkpoint plus the entries after it.
- `append`: a conditional append at the next sequence number, which is the commit point; a lost race reports a conflict.
- `checkpoint`: an advisory replacement of the checkpoint, written every 64 entries by default (`with_checkpoint_interval`).
- `find`: the latest entry for a transaction recorded at or after a given time.
- `history`: every entry, for inspection and full validation.

The Application validates every record through the transfer-type boundary, checks each entry against the transition it records, and reevaluates from fresh state on conflicts, up to 32 attempts. Point commands replay only the entries after the checkpoint; listing commands and `snapshot` rebuild and check the whole ledger from history. Reads and identical replays append nothing. Retry lookups are bounded by the window above (`with_retry_window`), and the Application's clock is replaceable for tests (`with_clock`). `uuid_v7` builds an ID from a timestamp and caller-supplied random bits.

`service::Service` wraps an Application for a long-lived process, such as a server. Reads come from the last durable state, held in memory behind an [`ArcSwap`](https://docs.rs/arc-swap), so they take no lock and do no I/O; listing transactions still reads history. One writer thread takes every write waiting for it, evaluates them in order, and appends their records together with one synchronization (group commit), answering each caller once its record is durable. An append that fails part-way can leave a prefix of a batch, which is the same as only its first writes succeeding, so every write in that batch reports an unknown outcome and the next batch reloads. Other processes may still write the store: appends stay conditional, so the service detects the race and reloads, but its reads can lag their writes until it next writes. `ListResources` reads the validated live state instead of replaying history.

[The runnable embedded example](crates/rescrow-adapters/examples/embedded.rs) creates a resource, reserves, partially commits, and inspects it through the Rust API. It can share a store with independent CLI invocations:

```sh
cargo run -p rescrow-adapters --example embedded -- .rescrow
# Each run is a new request with a new ID: a second run asks for 40 of the 35
# remaining credits and is rejected for capacity.
```

A capacity rejection always records its details, validated against the original operation; details attached to other abort causes are rejected. The project is pre-release, so the stored format can change without a migration path: re-create a store written by an older build.

`rescrow-adapters::memory::MemoryStore` supplies deterministic tests with conflict, pre-publication, and lost-response injection; it makes no durability promise. File-store fault hooks are explicit Rust constructor options used by qualification tests, never activated by CLI environment variables.

## Local persistence and limits

The selected canonical store directory contains `live.json`, a `log/` directory of numbered segments, a stable `state.lock`, and possibly orphan temporary candidates. Reads take no lock. Segments only grow and every other file is replaced by atomic rename, so a read sees a prefix of the log ending at its last complete record; it synchronizes what it read before returning it, so it never reports a write that a crash could still undo. A read that finds anything invalid may have raced a writer removing a torn append, so it is repeated under the OS shared lock, which excludes writers, before failing closed. Writes take the lock exclusively, reload, and append. Each log record is length- and checksum-framed; an append writes its records at the end of the active segment and synchronizes it once before success. That synchronization also covers every earlier record in the segment, so a writer needs no other synchronization unless it starts a segment. Bytes after the last valid record are an unacknowledged torn append, removed by the next writer. When the active segment passes 4 MiB (`with_segment_size`), it is sealed by writing an immutable index of its finished transactions, sorted by ID, and a new segment starts. `live.json` holds the checkpoint and the log position it covers. It is replaced by writing a uniquely created temporary file in the same directory, synchronizing it, renaming it over the old one, and synchronizing the directory; a lost checkpoint only means a longer replay. Files use mode `600`, directories `700`; existing broader permissions are rejected. Writers' lock acquisition times out after two seconds, independently of the bounded Application retry policy.

The [Rust rename contract](https://doc.rust-lang.org/std/fs/fn.rename.html) supplies same-filesystem replacement; Linux requires [directory synchronization](https://man7.org/linux/man-pages/man2/fsync.2.html) to persist the directory entry. Missing, invalid, oversized, or unsupported state fails closed, as does a sealed segment that changed after sealing or an index that disagrees with it. There is no automatic backup selection, reset, or rollback after uncertain publication. `live.json` and each record are limited to 16 MiB (`FileStore::with_size_limit`); a write beyond it returns `too_large`. CLI JSON input files have the same ceiling. Even an unbounded resource has finite representable upward room because quantities, offsets, and totals must fit `i128`.

A retry lookup walks segments newest-first and stops at the first segment recorded entirely before its time bound, reading only each sealed segment's index and first record along the way. A write appends one record and a point read replays at most one checkpoint interval, however long history grows; `live.json` grows with resources and prepared transactions, not with settled ones. Listing commands still read all history. Writes serialize through one lock; reads never wait for it. This is one logical shard per resource and one cooperating local user per store. It does not enforce budgets against anyone who can directly rewrite the authoritative store. Network filesystems, manual live editing, hard-link/path manipulation, and device loss are outside the local contract.

History is retained indefinitely for inspection, although no retry reads entries older than the window. Holds lapse only when asked to, and only when a write touches their resources. Execution claims, hold renewal, a background sweeper, external execution, trusted metering/refunds, hierarchy, sharding, signed audit history, HTTP, SQLite, and cloud backends remain deferred. The distributed protocol and enterprise RPO/RTO targets in DESIGN.md remain proposed and unqualified.

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

The history property generates up to 128 commands over finite and unbounded resources: creation, multi-resource preparation with and without holds, clock advances past their deadlines, normalized replays, changed requests and holds, full/partial/zero commits, aborts, reads, malformed operation/actual sets, and missing references. After each command it compares the full stored snapshot, returned result/error, frozen capacity diagnostics, and revision behavior with the model. Selected publication points inject CAS conflicts, known pre-publication failures, or applied writes with lost responses; retries use the identical command through a new Application instance. Selected steps round-trip typed JSON and reload through the inbound validation boundary.

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

`crates/rescrow-adapters/examples/history_bench.rs` seeds 0, 1,000, and 10,000 retained zero-usage commits through the Application, created ten minutes apart so the retained history lies outside a new request's skew bound, as it would in use. It then measures resource inspection, identical resource-create replay, and a prepare/abort pair with fresh IDs. It uses the real file adapter, locks, and synchronization. The output reports `live.json` and log bytes and median microseconds over seven samples after two warm-up cycles.

```sh
mkdir -p target/bench
cargo run --release --locked -p rescrow-adapters --example history_bench -- target/bench
```

Development measurement on 2026-11-25, Rust 1.96.0 release build, one run in a Linux container (OrbStack on macOS) writing to the container's own filesystem:

| History | `live.json` | Log | Inspection | Create replay | Prepare + abort |
| --- | --- | --- | --- | --- | --- |
| 0 | 119 B | 184 B | 22.1 ms | 0.20 ms | 105.8 ms |
| 1,000 | 571 B | 575 KB | 2.3 ms | 0.20 ms | 14.7 ms |
| 10,000 | 577 B | 6.4 MB | 2.1 ms | 0.16 ms | 20.0 ms |

The snapshot store this replaced measured 56.7–72.0 ms inspection and 548.1–757.1 ms prepare + abort at 10,000 transactions on a different host and filesystem (Linux tmpfs), so the two are not directly comparable; the comparison that matters is that the cost no longer grows with retained history. The 0-history row includes first-run costs. These small samples are not an SLO, throughput qualification, or a power-loss test.

### Concurrency

`crates/rescrow-adapters/examples/concurrency_bench.rs` runs writer threads doing 25 prepare/abort pairs each, alongside reader threads reading a resource until the writers finish. In `clients` mode every thread opens the store itself, so file locks contend exactly as separate CLI processes' would. In `service` mode the threads share one `Service`. Writers retry `contention` as the contract says and the benchmark counts it.

```sh
cargo run --release --locked -p rescrow-adapters --example concurrency_bench -- target/bench 8 8 clients
cargo run --release --locked -p rescrow-adapters --example concurrency_bench -- target/bench 8 8 service
```

Development measurement on 2026-11-25, Rust 1.96.0 release builds, a 12-CPU Linux container (OrbStack on macOS), two runs of each. First, readers that read continuously, before and after readers stopped taking the lock:

| Writers + readers | Shared-lock reads: writes/s, p99 | Lock-free reads: writes/s, p99 |
| --- | --- | --- |
| 1 + 0 | 493, 5 ms (one run of 15) | 375–476, 3–5 ms |
| 8 + 0 | 197–235, 582–592 ms | 243–415, 173–231 ms |
| 8 + 8 | did not finish in 240 s | 232–267, 183–212 ms; 14,000–16,000 reads/s |
| 16 + 16 | did not finish in 240 s | 138–141, 675–886 ms; 21,000 reads/s |

With shared-lock reads, a writer could take the lock only at an instant when no reader held it, so continuous readers starved writers. Readers pausing 100 µs between reads, as requests arriving at a server would, then compare separate clients with one service:

| Writers + readers | Clients: writes/s, p99 | Service: writes/s, p99 |
| --- | --- | --- |
| 1 + 0 | 280–375, 3–5 ms | 286–799, 2–6 ms |
| 8 + 0 | 288–302, 211–319 ms | 670–1,256, 11–57 ms |
| 8 + 8 | 438–442, 120–129 ms | 893–1,255, 12–20 ms |
| 16 + 16 | 306–316, 369–392 ms; 2–3 contention retries | 1,174–1,182, 25–49 ms |

Separate clients still serialize on the lock and evaluate optimistically, so with many writers one can lose the race repeatedly; 16 writers occasionally exhausted the 32 attempts and retried. The service evaluates every waiting write in one batch with one synchronization, so throughput grows with concurrency instead. Its reads are served from memory: continuous readers did 11–15 million reads/s, against about 20,000 for clients. But continuous readers without pauses, more threads than cores, starved the service's writer thread of CPU (56–775 writes/s); a server should leave its writer a core. Run-to-run variation on this shared host was large, so these ranges show direction, not capacity.

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
