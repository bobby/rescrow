# First implementation: file-backed local CLI

Status: implemented local milestone. See README.md for commands, reproduction steps, and qualification limits.

Build the smallest useful vertical slice of [the design](DESIGN.md): a pure Domain, an Application with validated transfer types, and independent CLI invocations sharing a local file store. File persistence replaces the earlier in-memory-only scope. Memory storage remains useful for deterministic tests.

## Milestone scope

Deliver resource creation/inspection, multi-resource preparation, full or partial commit, abort, transaction inspection, and replay-safe retries. Support bounded signed integer changes, including both downward quantity and upward-room reservations. Use one logical shard per resource and one cooperating local user per store.

The initial Application publishes the entire ledger in one atomic snapshot replacement. Preparation changes all participants together; commit/abort materialize their effects in that same publication. There are no partially published participants to recover. This is deliberately a stronger local publication primitive than separate key writes; it does not implement or qualify the distributed preparation/recovery protocol in DESIGN.md.

Defer execution claims, expiry, external tool execution, trusted metering/refunds, hierarchical policy, cross-domain grants, sharding/rebalancing, signed audit history, HTTP, SQLite, and cloud backends. Do not expose placeholder operations for them. Reservations have no automatic expiration. This is a cooperative local escrow exerciser; enterprise and external-execution guarantees remain future qualification work.

## 1. Establish the workspace and dependency boundaries

Start with four crates:

```text
crates/
  rescrow-domain/       valid types and pure transitions
  rescrow-application/  use cases, transfer types/conversions, snapshot-store port
  rescrow-adapters/     file-store implementation and test support
  rescrow-cli/          argument parsing, composition, output
```

Dependencies point inward. The Domain starts with the standard library only. The Application owns the persistence interface and never imports a concrete adapter. The CLI composes the file adapter with the Application. Keep persistence and parsing dependencies out of the Domain.

Use a synchronous implementation; this milestone needs no async runtime. The installed Rust/Cargo toolchain is 1.96.0; verify it and pin an appropriate toolchain during implementation. Use [Serde](https://serde.rs/) on transfer types, [serde_json](https://docs.rs/serde_json/latest/serde_json/) for the snapshot and JSON output, and [Clap](https://docs.rs/clap/latest/clap/) for CLI parsing. Add dependencies only as their concrete use appears. Add workspace checks, a lockfile, and ignore build output and the example local store.

Exit: the workspace builds, the Domain has no serialization/runtime/infrastructure dependency, and each outer crate depends only in the permitted direction.

## 2. Implement the pure escrow model

Introduce validated IDs, checked `i128` quantities/deltas, resource metadata/bounds, operations, reservations, and a ledger of resources and transactions. Keep fields private. Distinguish prepared, committed, and aborted transactions with enums carrying the data required for that state. Keep backend revisions and file paths outside the Domain.

Implement pure operations for:

- Creating a resource with fixed lower bound, optional upper bound, initial quantity, unit, and scale.
- Preparing a fixed, nonempty operation list, with unique operation IDs and at most one operation per resource.
- Reserving negative and positive changes independently; pending credits cannot fund debits and pending debits cannot create upward room.
- Committing full usage or explicit actual deltas for every operation, in the same direction and no greater than the reserved magnitude; zero releases the unused reservation.
- Aborting a prepared transaction and releasing its reservations.
- Returning resource availability and transaction status from a consistent ledger value.

Validate bounds, offsets, totals, overflow, and minimum signed-value edge cases before producing a new state. Compute all participant changes before constructing a successful next ledger, so failure cannot leave half a reservation. Keep per-resource rules reusable when a later Application publishes participants separately.

Use caller-supplied transaction IDs as this local prototype's replay identity. Store the normalized original request with each transaction. The same ID and request return the existing outcome/current state; changed parameters conflict. Normalization uses validated values and stable operation ordering, without adding a cryptographic digest service. Duplicate identical resource creation is a no-op; conflicting creation parameters fail without replacing the existing resource.

After a syntactically/domain-valid prepare request is admitted, a business rejection such as insufficient capacity records an aborted transaction with its original request and reason, while leaving resource quantities and holds unchanged. Retrying that ID cannot later turn the rejected request into a new reservation. Malformed input is rejected before admission. Terminal IDs remain retained. Identical commit/abort retries do not apply effects twice; incompatible terminal decisions or changed actual deltas conflict.

Exit: pure tests cover both directions, partial use, all-or-nothing multi-resource admission, replay semantics, and every terminal-state conflict. Rejections preserve the relevant invariants and do not mutate inputs.

## 3. Define transfer types and the anti-corruption boundary

The Application owns command/result DTOs and a versioned snapshot DTO. Quantities cross boundaries as canonical decimal strings. Use typed variants for commands/results and structured errors, with stable machine-readable error codes. Reuse those transfer contracts between Rust callers and CLI JSON where they match.

Implement `TryFrom<TransferType> for DomainType` for inbound commands and records, and `From<DomainType> for TransferType` or borrowed equivalents for outbound values. Validate stored snapshots through the same boundary as external input. Check unique IDs, referenced resources, transaction/reservation agreement, recomputable totals, state payloads, and the ledger's bounds. Reject malformed/unknown lifecycle variants and unsupported schema versions. Do not silently discard duplicate collection entries or default missing domain facts.

Conversions are pure. They establish representation/construction invariants; domain functions check current-state rules. Separate input errors, domain rejections, corrupt/unsupported stored state, and storage failures. Outbound conversion is infallible; encoding and writing remain separate fallible operations.

Exit: valid domain values round-trip through supported record DTOs, invalid DTOs cannot create domain states, and JSON examples exercise typed decoding rather than unchecked map access.

## 4. Implement the Application over a snapshot port

Expose Rust methods for resource creation/inspection, preparation, commit, abort, and transaction inspection. Start with one small Application-owned snapshot interface: load a validated revisioned snapshot and conditionally replace it against an expected revision. Adapters invoke the Application-owned `TryFrom` boundary once for each newly decoded image; the private immutable `ValidatedSnapshot` carries that proof into Application evaluation and candidate publication. Initialization explicitly creates an empty store; ordinary commands do not interpret a missing file as empty state.

For a mutation, convert the command, load/validate the snapshot, evaluate pure domain logic, convert the result, and publish it conditionally. On a revision conflict, reload, validate, and recompute within a bounded retry policy. Do not reuse calculations from a stale ledger. Persist an admitted terminal rejection before returning its business error. Reads use a single validated snapshot. Read-only or replay-only operations do not advance its revision unnecessarily.

The revision is Application/storage metadata and advances monotonically with checked arithmetic. An applied write with a lost response is resolved through the stored command identity. A post-publication durability error has an unknown outcome; it cannot be reported as a known rollback. Keep the distinction visible to CLI callers so they retry the same ID.

Use a small in-memory/fault-injecting test implementation of this port. Do not introduce a generic repository framework, clock service, authentication service, or effect interpreter for capabilities absent from this milestone.

Exit: direct Rust API tests cover conflicts, repeated evaluation, stored-state validation, lost responses, terminal replays, and preservation of the old state on pre-publication failure.

## 5. Add the file snapshot adapter

Use an explicitly selected store directory:

```text
<store>/
  state.json           versioned ledger and replay identities
  state.lock           stable coordination file; never replace or remove it
  .state-<unique>.tmp   temporary candidate during publication
```

Initially qualify local Linux filesystems. All invocations use the same canonical store directory. Use owner-only directory/file permissions where supported. This is cooperative coordination: an agent with direct write access can bypass the store. Network filesystems, manual live editing, and storage-path aliases/hard-link manipulation are outside the initial contract.

Use OS file locking, not lockfile existence or a PID to determine ownership. Rust provides [file locking and synchronization](https://doc.rust-lang.org/std/fs/struct.File.html#method.lock). Use shared locks for loading and an exclusive lock for conditional replacement. Lock acquisition is bounded and reports contention separately from corruption or I/O failure. The lock inode stays stable while `state.json` is replaced.

For conditional publication:

1. Hold the exclusive lock, reload the current snapshot envelope, and compare the expected revision. A mismatch returns a conflict for the Application to reevaluate.
2. Encode the validated candidate into a uniquely created temporary file in the same directory, with explicit permissions and bounded record size, including reserved space for all outstanding settlements and mutable numeric-field growth. Every publication must preserve this space; preparation, resource creation, and rejection records cannot consume it.
3. Finish buffered writes and synchronize the temporary file.
4. Rename it over `state.json` on the same filesystem; never truncate the live file in place. See Rust's [rename contract](https://doc.rust-lang.org/std/fs/fn.rename.html).
5. Synchronize the containing directory before acknowledging success. Linux documents why [directory synchronization is necessary](https://man7.org/linux/man-pages/man2/fsync.2.html).
6. Release the lock through scoped ownership, including error paths.

Initialization uses the same publication discipline under the lock and never overwrites an existing store. Account for durability of newly created directory entries. Replays and reads must complete any necessary synchronization before confirming a state whose previous publication outcome was uncertain; observing a renamed file alone does not establish durable acknowledgement.

Orphan temporary files are never promoted automatically. A missing, corrupt, oversized, or unsupported live snapshot fails closed rather than resetting budgets or selecting an older image. Do not roll back a replacement after an uncertain error. Preserve the current state and tell callers to inspect/retry the same identity. Document whole-file rewrite cost, serialized publication, and the tested filesystem/device failure model. No append log, snapshot compaction, background recovery worker, or backup-selection mechanism is needed in this milestone.

Exit: separate processes coordinate correctly, acknowledged writes survive the tested restart cases, interruptions expose a complete old or new snapshot, and ambiguous post-rename failures preserve replay identity. Do not equate process-kill tests with physical power-loss qualification.

## 6. Add a stateless command-line entrypoint

Planned commands, all using `--store <directory>`:

| Command | Purpose |
| --- | --- |
| `init` | Explicitly initialize an empty local store. |
| `resource create` | Create a resource with its fixed metadata and bounds. |
| `resource show` / `resource list` | Inspect quantities, bounds, reservations, and available capacity. |
| `transaction prepare` | Prepare an operation list under a caller-supplied transaction ID. |
| `transaction show` / `transaction list` | Inspect current and terminal outcomes. |
| `transaction commit` | Commit full or explicitly partial actual usage. |
| `transaction abort` | Release a prepared reservation without applying consumption. |

Use ordinary flags for simple commands and operation/actual-delta JSON files for multi-resource inputs, avoiding a separate custom operation syntax. Add `--json` for machine-readable results, clear human-readable defaults, documented exit codes, and errors on stderr. Keep stdout usable by scripts. Prompts, a REPL, daemon operation, and a reset command are unnecessary. Every invocation loads its state from disk and exits; no hidden process memory carries a workflow forward.

The CLI creates Application command DTOs, calls the Application, and renders its results. It contains no escrow calculations or direct edits of stored ledger fields. Initial values and every integer quantity round-trip without JSON floating-point conversion.

Exit: a documented sequence of separate CLI processes creates resources, reserves capacity, inspects it, partially commits, aborts another hold, and verifies final balances. Replaying the same transaction cannot consume twice.

## 7. Qualify the vertical slice and document its limits

Test independently meaningful properties:

- Enumerate small sets of positive/negative reservations and commit/abort outcomes; all valid combinations respect bounds and abort/commit ordering preserves the result.
- Reject over-reservation even when an opposite-direction pending operation would make the optimistic balance look sufficient.
- Reject a multi-resource prepare without leaving a hold on its earlier participants.
- Reject malformed DTOs, impossible stored state, missing resources, duplicate IDs, overuse, direction changes, overflow, and incompatible repeated commands.
- Run independent CLI processes concurrently against a scarce budget and verify that successful reservations do not exceed capacity.
- Race commit against abort, and identical replays against one another; one terminal outcome wins and effects occur once.
- Interrupt publication before write completion, after file synchronization, after rename, and before response delivery. Verify complete snapshots, released OS locks, and retry behavior.
- Exercise file permission errors, bounded lock contention, stale revisions, invalid schema versions, truncated JSON, and directory-sync failures without resetting state.

Once a workspace exists, run formatting, linting, unit/property tests, and CLI integration tests appropriate to the changes. Check the dependency graph and demonstrate a local build without HTTP/cloud dependencies. Update README with implemented commands and reproducible examples, clearly separating this milestone's support from enterprise design targets.

Completion means that an independent Rust caller and separate CLI invocations can exercise the same pure escrow rules through validated transfer types, against one safe local snapshot publication scheme. The sections above preserve the implementation requirements; completion evidence is listed below.


## Completion evidence

- **1 — Workspace:** four crates, inward dependencies, a dependency-free Domain, pinned Rust 1.96.0, Cargo.lock, build ignores, and CI formatting/lint/test/build checks.
- **2 — Domain:** validated private types and immutable transitions in `rescrow-domain`; `tests/escrow.rs` covers bounds/numeric edges, independent reservation directions, atomic admission, partial use, replays, terminal conflicts, and exhaustive small outcome/order combinations.
- **3 — Transfers:** Application-owned typed commands/results and schema-1 snapshots with canonical decimal strings. `tests/conversions.rs` covers typed JSON, lifecycle round trips, invalid construction, duplicates, balances, references, totals, and reservation agreement.
- **4 — Application:** synchronous typed Application methods and a validated `SnapshotStore` port; adapter `tests/application.rs` exercises direct Rust calls, real competing-state conflicts, bounded retries, rejected admissions, lost responses, corrupt state, revision exhaustion, and pre-publication failures.
- **5 — Files:** settlement-space admission checks with mixed-order/partial/extreme-value regression tests, plus Linux owner-only files/directories, bounded shared/exclusive OS locks, stable lock inode, checked revisions, synchronized same-directory replacement, fail-closed loads, and synchronization before replay acknowledgement. Adapter `tests/file.rs` tests process kills at publication checkpoints and injected failures, including directory sync and response loss.
- **6 — CLI:** all listed commands, ordinary flags and typed JSON operation/actual files, JSON/human output, stable errors and documented exits. CLI `tests/cli.rs` covers separate-process workflows, scarce-budget admission, identical concurrent replays, and commit/abort races. `examples/*.json` and the embedded Rust example provide runnable inputs/callers.
- **7 — Qualification:** formatting, strict Clippy, workspace tests, local-only build, dependency graph checks, and Proptest histories against an independent transaction-history model with shrinking/persisted failure seeds. A subsequent concurrent-history harness reuses that model to check mixed CLI results and final snapshots against sequential orderings respecting real-time precedence; failures retain observations and store files for investigation. Process tests exercised Linux tmpfs and local Btrfs; these are process-restart/injected-failure results, not power-loss, device-loss, or enterprise qualification. README documents the publication scheme, costs, limits, and deferred work.


The local follow-up also replaces tuple availability with named fields, retains structured capacity-rejection facts for stable replay diagnostics, and removes repeated full-ledger conversion through validated immutable snapshots. The benchmark example and README record development measurements and their limits. Existing schema-1 capacity rejections remain readable without fabricated historical details; the Rust snapshot-port signatures change to `ValidatedSnapshot`.

# Next milestone: log-structured local store

Status: proposed. Nothing below is implemented.

The snapshot store rewrites and revalidates the whole ledger, including permanently retained terminal transactions, on every publication. Following the escrow paper's storage design (DESIGN.md section 2), separate small live state from append-only history. Live state holds resources, their holds and `PREPARED` transactions. History is an append-only log whose durable append is the commit point, with checkpoints written lazily. A write's cost should depend on its record and the outstanding reservations, not on retained history. Domain rules, CLI commands and outputs, and the existing history checks stay unchanged.

## Layout

```text
<store>/
  state.lock          unchanged stable coordination file
  live.json           checkpoint: resources, holds, PREPARED transactions,
                      and the log position it reflects
  log/<n>.seg         append-only framed records (length, checksum, sequence)
  log/<n>.idx         immutable sorted terminal-transaction index, written
                      once when segment <n> is sealed
```

Record kinds: `ResourceCreated`, `Prepared`, `Committed` (with actual deltas), and `Aborted` (requested or admitted rejection, including its retained capacity facts). Sealed segments and indexes are immutable write-once units. They correspond to create-if-absent objects in later object-storage adapters, whose tail would be immutable batch objects behind a conditionally replaced head rather than an appended file.

## Steps

1. **Affected-record transitions.** Restore `rescrow-domain::transition` and its tests from the parked `prototype-emriss-ledger` branch, without Emriss code. The whole-ledger API delegates to it, and log replay applies the same pure transitions.
2. **Port.** Replace `SnapshotStore` with an Application-owned port: load a checkpoint and the records after it, conditionally append one record against an expected log position (`Applied`, `Conflict`, or unknown outcome), write a checkpoint for a position, and find a terminal transaction by ID. Log records get transfer types with fallible `TryFrom` validation, including replayed records. The Application replays and evaluates; the adapter frames, synchronizes, locks and indexes bytes.
3. **Commit point.** Under the exclusive lock, append one record and synchronize the segment before acknowledging. This applies to preparation too, which is always durable (the paper's `recover=YES`). Sequence numbers play the role of the paper's timestamps: recovery applies exactly the records after the checkpoint's position. After a failed synchronization, report an unknown outcome, never retry synchronization on that handle, and let the next load decide.
4. **Torn and corrupt records.** An invalid frame at the end of the newest segment was never acknowledged. Loads ignore it, and the next writer truncates it under the exclusive lock before appending. An invalid frame anywhere else, a sequence gap, or a record that fails validation fails closed.
5. **Checkpoints.** When the tail exceeds a record or byte threshold, the writer publishes a new `live.json` through the existing temporary-file, synchronize, rename, and directory-synchronize discipline. No background worker is required. A crash before rename leaves the previous checkpoint and a longer tail.
6. **Segments and the terminal index.** Roll a segment at a size threshold. Sealing writes its sorted index of terminal transaction IDs, request digests and record offsets as a synchronized, create-if-absent file. Replay and conflict lookups use live state, then the in-memory active-segment tail, then sealed indexes newest-first. Indexes are derived and rebuildable from their segment; a missing or invalid index fails closed until rebuilt. Segments are retained indefinitely so terminal IDs remain reserved. Index merging is deferred.
7. **Full scans.** `list` commands and an explicit audit replay all segments sequentially, validating checksums, sequences, transitions and checkpointed balances against full replay. Only these operations pay history-proportional cost.
8. **Migration.** An explicit step converts a schema-1 `state.json` into a checkpoint plus a log containing its terminal transactions, preserving replay identities and capacity facts. Ordinary commands fail closed on an unmigrated store rather than upgrading it silently.
9. **Removals.** Remove whole-ledger publication and the snapshot settlement-space policy. Commit and abort shrink live state and append one bounded record, so a per-record size cap replaces reserved settlement headroom. Disk-full remains outside the contract.
10. **Memory store.** The deterministic memory implementation implements the same port, with conflict, pre-append and lost-response injection.

## Qualification

- Unchanged acceptance gates: generated histories against the independent model and the 16 concurrent CLI workloads.
- Process kills and injected failures before append, after write but before synchronization, after synchronization but before response, during checkpoint publication, and during index sealing.
- A torn final record, corruption mid-log and in an index, a sequence gap, and failed synchronization.
- Migration round trips for every schema-1 fixture, including legacy capacity rejections.
- A regression bound showing that one mutation's written bytes and a point read's work do not grow with retained terminal history.

Update DESIGN.md section 6.4's local-disk paragraph and README's local persistence section with the layout, costs, limits, and migration. State that this remains one serialized local authority per store, qualified for process restart rather than power or device loss.
