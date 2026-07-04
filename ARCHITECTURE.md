# Vyuh Architecture

Vyuh is an Axum-based Rust web framework for building typed JSON APIs. The core
architecture is handler-first: application code defines routes, commands,
signals, tasks, and OpenAPI metadata through typed functions and bundle
registration rather than through a separate configuration layer. Handlers can
be registered with direct APIs or macros.

## Workspace Layout

- `vyuh/` contains the runtime framework crate.
- `vyuh-macros/` contains procedural macros used by the runtime crate.
- `docs/book/src/` contains the canonical documentation source for subsystem
  and product docs. `docs/book/` contains mdBook configuration and generated
  book support.
- `vyuh/web/` contains package-owned shared web assets, landing-page source,
  and built-in console templates.
- `llms.txt` is the compact documentation routing entrypoint for LLMs and
  coding agents.
- `vyuh/examples/<subsystem>/` contains grouped runnable examples.
- `migrations/` contains project-level migration examples.

## Runtime Crate

The `vyuh` crate is organized around these subsystems:

- `site` builds and serves a `Site`, wires bundles into Axum, initializes
  services, logging, emitters, commands, and database access.
- `conf` defines `SiteConf`, environment loading, and runtime configuration
  validation.
- `bundles` is the composition layer for routes, commands, signals, emitters,
  services, migrations, schema contributors, docs, and assets.
- `routes` defines route metadata, method handling, middleware helpers, and
  built-in route behavior.
- `callables` provides the type-erased invocation model used by routes,
  commands, signals, emitters, and tasks.
- `db` provides backend-selected SQLx aliases, source-first typed query
  builders, dialect-specific SQL rendering and validation, typed function and
  expression extension hooks, sessions, placeholder handling, mock sessions,
  and database errors.
- `auth` and `roles` provide JWT authentication and bit-mask role support.
- `validation` and `validators` provide typed validation primitives and
  extractor integration.
- `signals`, `emitters`, and `channels` provide in-process fanout, scheduled
  or external event sources, and signal-backed client-facing live delivery.
- `tasks` provides typed background task registration and backend-selected task
  execution.
- `commands` provides typed command registration and command dispatch through a
  built `Site`.
- `apidocs` and `schema` generate OpenAPI and schema output from registered
  operations and types.
- `assets`, `templates`, and `embed` provide embedded assets, server-side
  templates, private bundle resources, and the shared web asset surface used by
  the built-in console.
- `collectors` provides URL metadata, asset collection, and selected page
  collection built on normal bundles and routes.
- `db::migrations` provides backend-aware crate-owned migration registration,
  Gaman schema integration, and migration command support. Postgres is the
  richest backend, and SQLite is supported for Gaman's native safe subset.
  Migration files live in a crate-level `migrations/` directory, not under
  assets.
- `logging` configures structured tracing output.

## Macro Crate

The `vyuh-macros` crate exposes derive and attribute macros that keep user code
compact while feeding metadata into the runtime:

- Route, command, signal, emitter, task, cron, periodic, and asset macros
  generate bundle parts.
- `Record`, `Filterable`, `Validate`, and role/schema macros
  generate database, validation, schema, and auth integration code.
- Macro implementation should keep parsing, validation, diagnostics, and code
  generation separated.

## Backend Model

No database backend feature is enabled by default. In that lightweight mode,
Vyuh uses SQLite-compatible SQLx aliases and a shared in-memory SQLite default
database URL, while tasks use `MemoryTaskStore`.

Production applications should enable exactly one database backend feature:

- `postgres` is the recommended production backend for high-concurrency task
  workers and Postgres-only capabilities.
- `mysql` enables MySQL SQLx types and MySQL-backed task storage.
- `sqlite` enables SQLite SQLx types and SQLite-backed task storage.
- Postgres-only capabilities such as LISTEN/NOTIFY and `RETURNING *` helpers
  must stay behind Postgres cfg boundaries.

Backend selection belongs in `vyuh/src/db/commons.rs`, where `Database`,
`Arguments`, `Row`, `QueryResult`, and `Pool` are aliased.

## Signal And Channel Model

Signals are the only application event publish path. `site.signals().emit(T)`
queues fire-and-forget in-process handler fanout and also offers the same typed
payload to channels. Delayed event production is intentionally not part of the
signal client; scheduled sources belong in emitters, and durable delayed work
belongs in tasks.

Channels are consumers of typed signal payloads, not a separate topic bus.
Routes attach a `Subscriber` to a `Channels::user(UserKey)` stream and declare
accepted payload types with `deliver::<T>()` or `deliver_if::<T>(...)`.
Delivery policy is user-scoped: re-registering a `UserKey` replaces that user's
older rules, while multiple channel sessions for the user share one retained
queue and hold independent cursors.

The channel backend owns per-user policies, fixed-length per-user retained
queues, per-channel cursor/session state, atomic attach with replay, live
wakeup, and close/find operations. Predicates run before serialization; accepted
payloads are serialized once and delivered through a shared envelope across
WebSocket, SSE, and polling. Internal indexing uses Rust type identity, while
the client-facing event type uses the payload schema name.

## Request Flow

1. A bundle registers routes, commands, emitters, services, schema contributors,
   templates, assets, signals, and optional crate-owned migration sources.
2. `Site::build` validates `SiteConf` and bundle metadata.
3. `SiteBuilder` creates the database pool, router, template engine,
   authenticator, command registry, channel backend, signal engine, emitter
   engine, services, and task engine. Database-backed builds use the selected
   backend task store; lightweight builds use `MemoryTaskStore`.
4. When console is enabled, Vyuh injects its internal `vyuh/web` asset dir before
   template loading so console HTML and public assets ship with the runtime
   crate.
5. Axum routes receive `Site` as state and handlers use typed extractors.
6. Handlers call query builders or services and return typed responses.
7. OpenAPI and schema metadata are produced from registered operations and
   type metadata.

## Extension Rules

- Prefer adding behavior through bundles and typed subsystem registries.
- Keep backend-specific behavior isolated behind backend cfgs.
- Keep `mod.rs` files as module wiring and re-export surfaces.
- Keep public APIs fallible and explicit.
- Add tests for non-trivial behavior at the subsystem boundary.
