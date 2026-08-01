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
  built-in route behavior. It re-exports Mool's `Page<T>` as the canonical
  database-backed paginated JSON result while keeping ordinary resources and
  bounded lists unwrapped. A built site exposes immutable `Routes` indexes for
  named reversal and method-aware URL-to-operation resolution.
- `callables` provides the type-erased invocation model used by routes,
  commands, signals, emitters, and tasks. Canonical UUID-backed `OperationId`
  values identify registered metadata independently of OpenAPI string IDs.
- `db` provides backend-selected SQLx aliases, source-first typed query
  builders, dialect-specific SQL rendering and validation, typed function and
  expression extension hooks, sessions, placeholder handling, mock sessions,
  and database errors.
- `auth` provides complete token and opaque-key providers. One token provider
  owns access and optional refresh behavior through `TokenKind`, normalizes JWT,
  Django signing, PASETO, BRANCA, and custom codecs into `AuthToken`, and exposes
  only `AuthUser` to protected handlers. `roles` provides bit-mask authorization
  after audience validation. Bundles own an explicit `Audience` descriptor or
  receive the site's bounded default audience during site construction;
  omission never grants unrestricted access.
- `routes::ClientIp` resolves a single forwarded client address when supplied,
  with TCP peer-address fallback for applications and framework routes.
- `validation` and `validators` provide typed validation primitives and
  extractor integration.
- `signals`, `emitters`, and `channels` provide in-process fanout, scheduled
  or external event sources, and signal-backed client-facing live delivery.
- `tasks` provides typed background task registration and backend-selected task
  execution.
- `observability` owns configured liveness/readiness probes and bounded-label
  Prometheus HTTP metrics. Deployment policy is supplied through `SiteConf`,
  not inferred from a host environment.
- `commands` provides typed command registration and command dispatch through a
  built `Site`.
- `apidocs` and `schema` generate OpenAPI and schema output from registered
  operations and types.
- `assets`, `templates`, and `embed` provide overlaid embedded assets,
  server-side templates, private bundle resources, desired-schema assets, and
  the shared web asset surface used by the built-in console.
- `email` is an optional SMTP facade built on Lettre. It renders bundle-owned
  templates through the site template engine, produces text alternatives for
  HTML mail, and keeps SMTP transport details outside application APIs.
- `utils` provides small framework-neutral helpers for common web application
  tasks. Subsystem-specific helpers stay in their owning modules.
- `collectors` provides URL metadata, asset collection, and selected page
  collection built on normal bundles and routes.
- `db` is a facade over the standalone Mool database toolkit. Vyuh re-exports
  Mool's pools, sessions, records, models, typed queries, filters, relations,
  raw SQL, and migration types as `vyuh::db`; its database derives emit against
  that facade. Schemars remains an application-owned direct dependency.
- `db::migrations` provides backend-aware crate-owned migration registration,
  Gaman schema integration, and migration command support. Postgres is the
  richest backend, and SQLite is supported for Gaman's native safe subset.
  Migration files live in a crate-level `migrations/` directory, not under
  assets.
- Persistent task stores use Mool models, typed query scopes, transactions, and
  backend lock extensions. Vyuh owns task lifecycle and claim policy; Mool owns
  database execution. Task schemas are migration-owned and are never created or
  altered during site startup.
- Vyuh-owned database integrations, such as PostgreSQL LISTEN/NOTIFY emitters,
  are layered over the Mool pool through extension traits and remain native to
  Vyuh.
- `logging` configures structured tracing output.

## Macro Crate

The `vyuh-macros` crate exposes derive and attribute macros that keep user code
compact while feeding metadata into the runtime:

- Route, command, signal, emitter, task, cron, periodic, and asset macros
  generate bundle parts.
- `embed_asset!` delegates directory discovery and force-mode expansion to Rust
  Silos' shared macro implementation while emitting Vyuh's asset facade types.
- `Record`, `Filterable`, `Validate`, and role/schema macros
  generate database, validation, schema, and auth integration code.
- Macro implementation should keep parsing, validation, diagnostics, and code
  generation separated.

## Backend Model

No database backend feature is enabled by default. In that lightweight mode,
Vyuh has no active SQL dialect or database pool, while tasks use
`MemoryTaskStore`.

Production applications should enable exactly one database backend feature:

- `postgres` is the clustered production backend for high-concurrency task
  workers and Postgres-only capabilities.
- `sqlite` is supported for local and single-process durable deployments.
- `mysql` compiles but is experimental until its migration and concurrent-task
  evidence reaches the PostgreSQL/SQLite release bar.
- Postgres-only capabilities such as LISTEN/NOTIFY and `RETURNING *` helpers
  must stay behind Postgres cfg boundaries.

Backend aliases and migration/query types are re-exported from Mool through
`vyuh::db`; Vyuh does not own a second database abstraction.

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
   backend task store; lightweight builds use `MemoryTaskStore`. When enabled,
   observability endpoints are mounted before the site router is finalized and
   request metrics are applied as a site-wide middleware.
4. The authenticator resolves key sources off request threads and builds
   immutable credential-provider, selector, metric, and typed login-method
   registries plus the site-secret key ring. Application-owned routes select
   identity proof with `.via(...)`, while password, Basic, MFA, and optional
   OIDC methods delegate successful proof to the selected credential provider.
   Runtime `.using(...)` and `.via(...)` calls retain descriptors without
   failure; terminal operations resolve them against the immutable registries.
   Unselected login, refresh, and logout target the default provider, while
   request extraction alone falls through absent access credentials. Refresh
   validates and rotates a credential only inside its selected provider. Cookie
   credentials use provider-managed delivery, CSRF validation, and logout
   response attachments. Optional lifecycle storage supplies replay protection
   and revocation without changing handler signatures. Each framework provider
   implements one private asynchronous runtime contract for authenticate,
   login, refresh, logout, capabilities, and OpenAPI metadata. Server-side
   session storage is not implemented, but a stateful provider can fit that
   contract without changing handler or registration APIs.
5. When console is enabled, Vyuh injects its internal `vyuh/web` asset dir before
   private template and schema asset loading. Later asset directories override
   earlier matching paths; schema assets are parsed into the migration registry
   without modifying the database. Console authentication uses signed stateless
   access credentials and IP-bound token cookies; only its status snapshot is cached
   in process memory.
6. `Site::run` executes one inert command unless it selects `serve`; `serve` and
   direct server startup bind the listener, then start task, emitter, and service
   workers before accepting HTTP work.
7. Axum routes receive `Site` as state and handlers use typed extractors.
   Vyuh-registered routes also receive their `OperationId` as a request
   extension; task, signal, and command invocation contexts carry the same
   identity for their own operation.
8. Handlers call query builders or services and return typed responses. Normal
   JSON resources remain direct, paginated queries return Mool's `Page<T>`, and
   framework-generated JSON failures normalize into `ErrorReport`; raw responses
   remain an explicit application escape hatch.
9. OpenAPI and schema metadata are produced from registered operations and
   type metadata.

## Extension Rules

- Prefer adding behavior through bundles and typed subsystem registries.
- Keep backend-specific behavior isolated behind backend cfgs.
- Keep `mod.rs` files as module wiring and re-export surfaces.
- Keep public APIs fallible and explicit.
- Add tests for non-trivial behavior at the subsystem boundary.
