# Agent contributors

Before working in this repository, read and follow
[README.md](README.md), especially
[Contributing](README.md#contributing). It contains the contributor
instructions shared by humans and agents.

Keep shared contributor guidance in README.md. Use this file only for instructions specific to agent contributors.

## Simplicity

We value simplicity in the sense of Rich Hickey's talk ["Simple Made
Easy"](https://www.infoq.com/presentations/Simple-Made-Easy/), i.e.
preferring to keep concerns disentangled or "de-complected". We also
value elegance, in the sense that all else being equal we should prefer
less code, less state (especially durable state), fewer constructs,
fewer abstractions, etc.

Specifically, we prefer to use immutable data and functional
programming where possible. Use clean, well-understood primitives and
algorithms when dealing with complex issues like concurrency and
distributed systems.

## Code Style and Architecture

We prefer to work in the style of Scott Wlaschin's book [Domain Modeling Made Functional](https://pragprog.com/titles/swdddf/domain-modeling-made-functional/):

* The business domain logic is implemented in a simple, functional
  style in terms of domain types whose role is to make it impossible
  to represent illegal domain states. In EvidentSource they live in
  `evidentsource/domain`.
* Application, interface, transmission, side-effect, and storage
  concerns:
  * Are implemented in terms of "transfer types", such as those in
    `evidentsource/api/transfer`. Domain types are converted
    infallibly into transfer types, but conversions from transfer
    types into domain types are fallible, with descriptive
    validation, as an anti-corruption layer.
  * May depend on the domain logic, but the domain logic can't look
    outward.
