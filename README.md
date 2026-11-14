# Rescrow

Rescrow is a proposed Rust implementation of durable escrow transactions for resource budgets. Reserve a bounded quantity, authorize work, and settle actual usage while other agents and workflows continue using the remaining capacity.

The repository currently contains design documentation. There is no implementation, installable CLI, or measured performance yet. [DESIGN.md](DESIGN.md) specifies the protocol, deployment profiles, security model, and qualification work.

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

Local use is first-class and requires no cloud account or HTTP server. The reference local store is SQLite with transactional cross-process coordination. Local durability covers its tested device/process failure model. Enterprise deployments add globally consistent authority, regional redundancy, identity, isolation, and independent audit evidence. The reference enterprise authority is DynamoDB MRSC; S3 Standard provides checkpoints/retained evidence, and Express can provide disposable acceleration. These are proposed designs and acceptance targets requiring validation.

All shapes preserve escrow bounds, durable decisions, retry identity, and safe settlement/refund rules. A backend must meet the selected profile; selecting local operation cannot silently weaken an enterprise namespace.

## Contributing

The following instructions apply to all contributors, human and agent, throughout this repository. Read [DESIGN.md](DESIGN.md) for the current technical proposal. Agent contributors must also read [AGENTS.md](AGENTS.md) for agent-specific instructions. Keep shared contributor guidance here and reserve AGENTS.md for instructions specific to agents.

The repository is currently documentation-only; do not imply that proposed APIs, commands, guarantees, benchmarks, or crates already exist. Keep the public explanation, architecture, and implementation consistent as the project develops.

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
