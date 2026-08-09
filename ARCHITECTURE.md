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
  only `AuthUser` to protected handlers. Typed `TokenClaims` adapters map
  externally shaped authenticated JSON into that envelope without bypassing the
  provider's common validation. `roles` provides bit-mask authorization
  after audience validation. Bundles own an explicit `Audience` descriptor or
  receive the site's bounded default audience during site construction;
  omission never grants unrestricted access.
- `routes::ClientIp` resolves a single forwarded client address when supplied,
  with TCP peer-address fallback for applications and framework routes.
- `validation` and `validators` provide typed validation primitives and
  extractor integration.
- `signals`, `emitters`, and `channels` provide in-process fanout, scheduled
  or external event sources, and signal-backed client-facing live delivery.
  Cron and periodic emitters may submit deterministic input through the shared
  task runtime using a cursor-backed transaction; this is a narrow durable
  enqueue path, not a second scheduler or workflow runtime.
- `tasks` provides typed input, value-less durable background task registration,
  immediate transactional submission, named concurrency lanes, batched claims
  and commits, lane-owned retry/backoff and idempotency-retention policy, local
  runner and store-wide database rate limits, adaptive polling, lease renewal,
  and explicit continuation lifecycle control. Tasks statically declare their
  lane and optional key rule; reusable bundles may contribute complete lane
  defaults, while site configuration resolves or strictly rejects missing lanes.
- `observability` owns configured liveness/readiness probes and bounded-label
  Prometheus HTTP metrics. Task readiness is derived from per-site task runtime
  state updated by initialization and existing scheduler ticks, never by an
  additional task-store probe. Deployment policy is supplied through `SiteConf`,
  not inferred from a host environment.
- `commands` provides typed command registration and command dispatch through a
  built `Site`.
- `apidocs` and `schema` generate OpenAPI and schema output from registered
  operations and types.
- `mcp` is an optional, tool-only Streamable HTTP subsystem over explicitly
  registered semantic callables and eligible typed JSON routes. Bundle-owned
  `McpToolRegistry` entries retain stable schemas, authorization metadata, and
  either a direct callable or route target; `McpEngine` separately owns one or
  more independently configured service endpoints. Direct tools execute through
  `McpToolContext`, while route-backed tools reconstruct only a static-method
  JSON request and dispatch through the built site router. The engine owns
  protocol framing, role-filtered discovery, protected-resource metadata,
  external JWT/JWKS verification, and a swappable public-document cache. It
  converts validated subjects into normal `AuthUser` values but never forwards
  bearer credentials or owns OAuth login and consent. Modern MCP requests are
  stateless and validate mirrored routing headers against body metadata, while
  prior initialization-based revisions are accepted without framework session
  state.
- `assets`, `templates`, and `embed` provide overlaid embedded assets,
  a validated site-wide static URL, server-side templates, private bundle
  resources, desired-schema assets, and the shared web asset surface used by
  the built-in console.
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
- Persistent task stores use Mool models, typed query scopes, batched writes,
  transactions, and backend lock extensions. Vyuh owns lane scheduling,
  leases, idempotency ownership, durable token buckets, and database-relative
  wake deadlines; Mool owns database execution. A store-wide policy fingerprint
  prevents incompatible workers from claiming concurrently. Task schemas are
  migration-owned and are never created or altered during site startup. Store
  internals are framework-owned; the ordinary site facade exposes only typed
  submission, resume, reassignment, and read-only inspection.
- Vyuh-owned database integrations, such as PostgreSQL LISTEN/NOTIFY emitters,
  are layered over the Mool pool through extension traits and remain native to
  Vyuh.
- `logging` configures structured tracing output. Console log inspection reads
  only configured JSON file sinks through bounded per-site reverse readers;
  optional `mail_admins` sinks enqueue bounded error reports for the existing
  site-owned SMTP delivery runtime and use lossy per-sink delivery throttles.
  Neither facility creates a second log store or process-global log cache.

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
`MemoryTaskStore`. That store is development-only when tasks are registered;
production construction requires a durable database backend.

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
   backend task store; development builds may use `MemoryTaskStore`. When enabled,
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
5. Vyuh validates one site-wide static URL and mounts all bundle `public/**`
   assets at its local path. Relative URLs use the browser host; absolute URLs
   support a fixed CDN origin. Templates and the console resolve every asset
   through that same immutable runtime.
6. When console is enabled, Vyuh injects its internal `vyuh/web` asset dir before
   private template and schema asset loading. Later asset directories override
   earlier matching paths; schema assets are parsed into the migration registry
   without modifying the database. Console uses the application's unchanged
   immutable authenticator registry and its public console-only audience.
   `ConsoleAccess` is a synchronous application policy over an optional normal
   `AuthUser`; no console provider, credential exchange, login route, cookie, or
   process-global state exists. Debug development access is visibly marked, while
   enabled non-debug consoles require a policy. Console URL values are immutable
   and its bounded status cache is mutable state owned by that one built site.
7. `Site::run` executes one inert command unless it selects `serve`; `serve` and
   direct server startup bind the listener, then start task, emitter, and service
   workers before accepting HTTP work. One task dispatcher rotates configured
   lanes fairly, fills bounded per-lane queues through batched lane claims,
   renews active leases, and commits handler outcomes through one common batch.
   Submission bypasses the runner and commits immediately. Saturated lanes use
   the short poll interval; future readiness, lease, and rate deadlines use
   database time; otherwise the runner uses the bounded fallback interval.
8. Axum routes receive `Site` as state and handlers use typed extractors.
   Vyuh-registered routes also receive their `OperationId` as a request
   extension; task, signal, and command invocation contexts carry the same
   identity for their own operation. Operation bundle origin is assigned once at
   registration and never rewritten by bundle composition.
9. Handlers call query builders or services and return typed responses. Normal
   JSON resources remain direct, paginated queries return Mool's `Page<T>`, and
   framework-generated JSON failures normalize into `ErrorReport`; raw responses
   remain an explicit application escape hatch.
10. When enabled, each `with_mcp` declaration claims the unclaimed MCP registry
    entries in its bundle subtree. Site construction finalizes all service paths,
    rejects unclaimed entries and endpoint/resource collisions, and builds a
    deterministic catalog per service. Protected endpoints validate their own
    canonical resource audience and map the subject to `AuthUser`. Discovery
    filters tools using the same required `permit!` masks used again at call
    time. Direct targets receive `McpToolContext`; route targets receive the
    semantic object unchanged as an internal JSON body through the existing
    router. Neither path receives the external bearer credential. Only public
    OAuth metadata and JWKS may cross the service-owned cache boundary.
11. OpenAPI and schema metadata are produced from registered operations and
    type metadata.

## Extension Rules

- Prefer adding behavior through bundles and typed subsystem registries.
- Keep backend-specific behavior isolated behind backend cfgs.
- Keep `mod.rs` files as module wiring and re-export surfaces.
- Keep public APIs fallible and explicit.
- Add tests for non-trivial behavior at the subsystem boundary.
