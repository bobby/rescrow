# HTTP API v1

[OpenAPI 3.1](openapi/rescrow-v1.json) is the versioned wire contract. The examples in [the captured socket fixtures](openapi/fixtures/http.json) are real memory-server responses; CI validates fresh responses against the same schemas. [Digest vectors](openapi/fixtures/request-digests.json) are independently reproducible. Distributed backends, execution claims, signed receipts, events and enterprise qualification remain later milestones.

## Run a local service

The default CLI remains synchronous and local. HTTP and remote clients are optional builds:

```sh
cargo build -p rescrow-cli --features remote -p rescrow-http --locked
# Start with a fresh store; omit this command if it is already initialized.
target/debug/rescrow --store .rescrow init
# Requires Python 3 and OpenSSL. Creates fresh owner-only development credentials.
python3 examples/dev-identity.py
target/debug/rescrow-http --config .rescrow-http-dev/config.json
```

In another shell:

```sh
target/debug/rescrow --endpoint http://127.0.0.1:8080 \
  --token-file .rescrow-http-dev/token --require-profile local \
  --json resource create credits --initial 60 --upper 100 --unit credits
target/debug/rescrow --endpoint http://127.0.0.1:8080 \
  --token-file .rescrow-http-dev/token --require-profile local \
  --json resource create tokens --initial 8 --unit tokens
work=$(target/debug/rescrow new-id)
target/debug/rescrow --endpoint http://127.0.0.1:8080 \
  --token-file .rescrow-http-dev/token --require-profile local \
  --json transaction prepare "$work" --operations examples/operations.json
```

All resource/transaction commands and their typed CLI output shapes are shared with local mode. Authentication facts and the credential namespace appear in remote transaction DTOs. Remote `init` is rejected: initialization belongs to server composition. `RESCROW_TOKEN` is an alternative to `--token-file`; there is no token argument on the command line. `new-id` stays local. The development helper never overwrites existing credentials; its token expires after one hour. Use your workload issuer's public keys and restricted grants for a deployment.

Configuration selects `backend: {"type":"file","path":".rescrow"}` or explicit disposable `{"type":"memory"}`. The file backend opens an existing initialized authority and supports only tenant `local` with no namespace. Memory supports isolated tenants and namespaces, initializes once at startup, and reports `simulation`. Its state and generation change on restart. Neither serves regional or enterprise durability. The file format is schema 4; older formats fail closed and have no migration in this pre-release project.

Plain HTTP binds only loopback addresses. Other listeners require `tls: {"certificate":"fullchain.pem","private_key":"key.pem"}`; `tls: null` selects loopback HTTP. The executable accepts SIGINT/SIGTERM, stops new storage dispatch, lets requests finish, and drains actual blocking operations even when a request timed out or disconnected. Infrastructure readiness and the selected profile are checked before binding. The `authority_generation` UUID comes from the initialized store, survives file reopen/checkpoints, and cannot be supplied as a serving label.

## Identity and permissions

Each request supplies one `Authorization: Bearer …` header. Configure `issuers` with an exact issuer, audience and one or more `{kid,algorithm,public_key_pem}` entries. Supported pinned algorithms are RS256, ES256 and EdDSA; symmetric algorithms, expired tokens, future `nbf`, nested/encrypted/critical JWT extensions and duplicate claim/header keys are rejected. Public keys are loaded at startup; token-selected URLs are never fetched. Restart with an updated trusted key set for rotation. `Authenticator` is pluggable and runs in a bounded blocking lane; another implementation can inspect verified peer extensions for mTLS.

Verified custom claims are `tenant`, `namespace`, stable `credential_id`, `scopes`, `resources`, and optional `max_hold_ms`. Non-local tenants require a namespace. `resources` contains 1–64 grants of `{resource_prefix,debit,credit,max_amount}`; `*` matches every resource, otherwise the prefix is literal. Maximum amounts are positive decimal i128 strings. Scopes are `resource:create`, `transaction:prepare`, `transaction:commit`, `transaction:abort`, and `read`. Creation requires the explicit admin scope. Reads filter lists to visible resources and manifests. Every replay rechecks the current credential, including the original manifest for decisions. A resource outside read scope and objects in another tenant/namespace return 404. Request context cannot grant permissions. An optional prepare `namespace` only asserts equality with the credential's namespace.

Admitted requests persist three bounded SHA-256 fingerprints: principal, stable credential and policy. Bearer tokens, subjects and issuer URLs are not stored. Credential IDs must stay stable across ordinary token renewal for outstanding-hold quotas to follow the credential. Current grants govern access, including access by a rotated credential to a shared authorized namespace. There is no vendor SDK or live identity-provider dependency during requests.

## Representations and endpoints

| Method/path | Scope | Success |
| --- | --- | --- |
| `PUT /v1/resources/{id}` | `resource:create` | 201 new; 200 identical immutable specification |
| `GET /v1/resources/{id}` | `read` | 200 sampled resource |
| `GET /v1/resources` | `read` | 200 visible resources |
| `POST /v1/transactions` | `transaction:prepare` | 201 new; 200 replay; 202 confirmed preparation while original response is pending |
| `GET /v1/transactions/{id}` | `read` | 200 authoritative transaction status |
| `GET /v1/transactions` | `read` | 200 visible transactions in the credential namespace |
| `POST /v1/transactions/{id}/commit` | `transaction:commit` | 200, including identical actuals replay |
| `POST /v1/transactions/{id}/abort` | `transaction:abort` | 200, including identical abort replay |

Quantities and deltas are canonical signed i128 decimal **strings**. Units are trimmed, nonempty, control-free strings of at most 64 UTF-8 bytes. Timestamps are UTC epoch milliseconds; RFC 3339 is not duplicated on the wire. A resource separates immutable `spec` from sampled `observation`, including the observation interval, quantity, holds, totals and availability. Admission always requires a preparation; displayed availability is not a reservation. Reads do not lapse overdue holds. These atomic adapters have `materialization: complete` and `finalization_pending: false`.

A success envelope has `data`, `metadata` and `links`. Metadata contains `request_id`, `serving_region`, `deployment_cell`, `authority_generation` and actual `durability_profile`. The last two and `X-Request-ID` are also response headers. `self`, decision and pagination links are relative to the configured origin. `Location` identifies the returned object; 202 also includes integer-seconds `Retry-After`. Dot-only Domain IDs use `~Lg` for `.` and `~Li4` for `..` in paths to survive URL normalization.

Lists accept `limit`, literal ID `prefix`, and opaque `cursor`; transactions also accept `state=prepared|committed|aborted`. IDs sort lexicographically. `data.next_cursor` and `links.next` provide continuation. Signed cursors expire after 15 minutes and bind the authority, tenant, namespace, policy, endpoint and filters; they cannot be reused across those contexts. Keep filters when paging. Pages are keyset samples, with no snapshot across requests. The current authorities inspect retained records before slicing; pagination bounds responses rather than storage scan cost.

## Replay, deadlines and fallback

`Idempotency-Key` is one canonical lowercase UUIDv7, created once before the first attempt. Preserve it together with intent and actuals across process/workflow retries. The server chooses tenant and namespace from verified identity. New IDs older than 24 hours or more than 5 minutes in the future are rejected. Prepared transactions replay at any age; terminal creation/decision replay follows the README's bounded window. Never generate a replacement identity to recover an uncertain outcome.

The prepare digest is `sha256:` plus lowercase hex of SHA-256 over `rescrow.prepare.v1`, NUL, then compact canonical UTF-8 JSON. Its recursively sorted object keys are `context`, `hold_ms`, `operations`; operations sort by operation ID, each with `delta`, `operation_id`, `resource_id`. Omitted context/hold become `{}`/`null`. Decimal spelling is validated; array order and JSON whitespace do not change intent. No Unicode normalization occurs. Identity, namespace, headers and authorization facts are excluded. Actuals encode as a recursively sorted object `{ "actuals": [...] }`, sort by operation ID, and use the separate `rescrow.actuals.v1` tag. Omitting actuals means full reserved usage, which hashes identically to explicit full actuals. Duplicate JSON keys at any depth fail before admission.

A new durable preparation returns 201; a race between identical prepares produces one 201 and authoritative 200 replays. A capacity/missing-resource/overflow rejection is recorded before the error is returned and replays the frozen diagnostic. Conflicting intent or actuals returns 409. After a request timeout, 202 is possible only after a separate read confirms the exact durable matching prepare. Without that evidence, the server returns 503 with the known ID and an explicitly unconfirmed outcome. Cancellation never rolls back a published write.

Rust `Preparation::new` creates an identity once, before I/O; serialize it for durable workflow use. `Client::execute` supports the shared DTO commands. The Rust, Python and TypeScript transports retain identical serialized bytes/key, enforce attempt and overall deadlines, use bounded jitter and `Retry-After`, and reject redirects. They retry only transport ambiguity, 202, or retryable 429/503. GET is sufficient for status polling. Default bounds are 5 seconds per attempt, 30 seconds overall, eight attempts. TLS certificate verification is enabled; plaintext origins must be loopback. Generated language clients expose one page at a time; the CLI/Rust command API follows cursors within the overall deadline.

Alternate endpoints are an explicit ordered list. Multiple endpoints require the operator-supplied authority generation, sent as `Require-Authority-Generation` on **every** attempt. The server rejects a mismatch before mutation, and clients check returned generations and minimum durability profiles. A list of independent memory stores cannot act as fallback. One endpoint can pin its first observed generation; supply a known generation when continuity must be enforced from the first request. This mechanism supports future regional compositions; it does not qualify regional availability today.

## Problems

Problems use `application/problem+json` with required `type`, `code`, `category`, `status`, `detail`, `request_id`, nullable `transaction_id`, and `retryable`; capacity rejection also carries its frozen `capacity` facts. Type is `urn:rescrow:problem:<code>` for every row below. Unknown outcomes may have applied: retryability only authorizes retrying the identical request. Storage paths and internal exception details are not exposed.

| Code | HTTP | Retryable |
| --- | --- | --- |
| `unauthenticated` | 401 | false |
| `forbidden` | 403 | false |
| `not_found` | 404 | false |
| `method_not_allowed` | 405 | false |
| `unsupported_media_type` | 415 | false |
| `conflict` | 409 | false |
| `capacity` | 409 | false |
| `already_initialized` | 409 | false |
| `invalid_input` | 422 | false |
| `overflow` | 422 | false |
| `too_large` | 422 | false |
| `limit_exceeded` | 422 | false |
| `expired` | 422 | false |
| `throttled` | 429 | true |
| `contention` | 503 | true |
| `io` | 503 | true |
| `unknown_outcome` | 503 | true |
| `deadline_exceeded` | 503 | true |
| `corrupt_state` | 503 | false |
| `unsupported_schema` | 503 | false |
| `missing_store` | 503 | false |
| `revision_exhausted` | 503 | false |

401 includes `WWW-Authenticate: Bearer`. Retryable problems include integer-seconds `Retry-After`. Known transaction identities also get a status `Location`. Invalid correlation IDs receive a safe replacement correlation value. The Application retries CAS contention at most 32 times; HTTP bounds how long a caller waits without abandoning an actual write.

## Limits and admission

`api.limits` is optional; omitted fields retain defaults. Limits apply to transport requests before storage dispatch, while outstanding-hold accounting is atomic authoritative state.

| Setting | Default / supported ceiling |
| --- | --- |
| `body_bytes` | 1 MiB / 16 MiB |
| `operation_count` | 64 / 1024, also applies to explicit actuals |
| `numeric_magnitude` | decimal `170141183460469231731687303715884105728` (2^127); Domain reservation rules still exclude signed minimum |
| `context_bytes` | 4096 / 4096; max 32 keys, key 1–64 ASCII bytes, value at most 1024 UTF-8 bytes |
| `pending.tenant`, `pending.credential` | 4096 / 256 outstanding prepared transactions; configurable positive limits, credential ≤ tenant |
| `request_timeout_ms`, `queue_timeout_ms` | 5000 / 100; request ceiling 300000 |
| `page_size`, `max_tenants` | 100 / 1024; ceilings 1000 / 10000 |
| Global `admission` concurrency / queue | 32 / 128 |
| Global `decisions`, `recovery` concurrency / queue | 16 / 128 each |
| `tenant_admission` concurrency / queue / requests_per_second / burst | 8 / 16 / 100 / 100 |
| `tenant_decisions`, `tenant_recovery` concurrency / queue / requests_per_second / burst | 8 / 32 / 200 / 200 each |

Traffic rates, concurrency and queues are per serving process. The bounded tenant registry is retained until restart; reaching `max_tenants` sheds unregistered tenants. Admission, decisions and recovery have separate storage and authentication capacity. Saturation, rate exhaustion, full queues and queue timeouts return 429. Setting global admission concurrency to zero disables new writes in that class while preserving decision/recovery capacity. Positive decision/recovery concurrency is required. Authentication runs off async workers; the permit survives request cancellation.

Outstanding-hold counts are tenant-wide across server instances sharing an authority and count a transaction once per admitting credential. Replay never increments them. Commit, abort and lapse release them in the same atomic publication as the transaction/resources. This adds one tenant counter dependency to authenticated transitions; unrelated authenticated admissions can therefore contend on that quota record. Local unauthenticated embedded/CLI use has no such counter. Expired holds still count until a write lapses them; there is no independent background sweeper in this milestone.

## Generate and verify clients

```sh
python3 tools/generate-clients.py
npm ci --prefix clients/typescript --ignore-scripts
npm run check --prefix clients/typescript
python3 -m venv .venv-contract
.venv-contract/bin/pip install -r tools/requirements-contract.txt
.venv-contract/bin/python tools/check-contract.py
cargo build -p rescrow-cli --features remote --locked
RESCROW_LANGUAGE_SMOKE=1 \
RESCROW_REMOTE_CLI="$PWD/target/debug/rescrow" \
RESCROW_CONTRACT_CAPTURE=/tmp/rescrow-http-contract.json \
cargo test --workspace --all-features --locked
RESCROW_CLI_TEST_SERVER="$PWD/target/debug/rescrow-http" \
cargo test -p rescrow-cli --features remote --test cli --locked
.venv-contract/bin/python tools/check-contract.py /tmp/rescrow-http-contract.json
```

Language smokes require Python 3.11+ and Node 24+; they execute against the real memory HTTP harness. The generator uses only Python's standard library and reads the checked-in OpenAPI schemas/operations; transport implementations are reviewed separately. The [Python](clients/python/smoke.py) and [TypeScript](clients/typescript/smoke.ts) smokes also show stable-key retry usage. Generated clients require no runtime package installation. CI also runs the seven existing CLI scenarios against the file-backed HTTP executable, covering multi-resource partial commit/abort/replay, competing independent processes, identical retries, commit/abort races, new IDs, exact large signed values and machine errors, and hold expiration. File initialization and uninitialized-store checks remain local composition; business commands use HTTP. CI checks regeneration, TypeScript types, real socket conformance, remote CLI commands, lost responses, authority mismatch, scopes, quotas, lane saturation and shutdown with cancelled writes, together with the existing local safety suite.
