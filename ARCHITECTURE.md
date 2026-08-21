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
  provider's common validation. Exact string scopes and `Permit<ScopeRule>`
  provide extractor-driven application authorization
  after audience validation. `BundleConf` contributes an explicit `Audience`
  descriptor or bundles receive the site's bounded default audience during site
  construction; omission never grants unrestricted access. Bundle provider
  contributions are validated then merged into the one central registry, while
  shared providers and auth lifecycle remain `SiteConf` concerns. Provider selector ownership is
  indexed by local audience, allowing the same Bearer source for disjoint OAuth
  resources without token sniffing or verifier fallthrough.
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
  and commits, optional process-local `Data<Batch<T>>` handler invocation,
  storage-only workflow lineage/kind metadata, lane-owned retry/backoff and
  idempotency-retention policy, local
  runner and store-wide database rate limits, adaptive polling, lease renewal,
  opt-in durable lane ownership, and explicit continuation lifecycle control.
  Ordinary lanes retain the direct task claim path. An owned lane coordinates
  through a separate primary-key lease row, keeps one fenced owner across task
  execution, and runs lifecycle hooks as independent async futures outside task
  concurrency. Its lease renewal joins the same centrally paced store turn and
  wakes only for normal polling, useful work, or the half-lease safety deadline;
  it does not create a per-lane renewal loop. Tasks statically declare their
  lane and optional key rule; reusable bundles may contribute complete lane
  defaults, while site configuration resolves or strictly rejects missing
  lanes.
  Local handler batching groups matching rows already held by a lane queue and
  never adds a store query or waiting policy. It is independent of durable lane
  ownership: each row retains its own attempt, rate permit, lease, fenced
  commit, and inspection history while one batch future consumes one local
  handler-concurrency slot.
- `cache` provides an immutable per-site registry of asynchronous named cache
  providers. Its typed handles own JSON serialization, canonical provider and
  namespace key scoping, and bounded metrics; providers own byte storage, TTL,
  atomic integer, and scoped-clear semantics. The default memory provider is
  bounded and local to one built site. OAuth verifier state remains a separate,
  per-site protocol concern and never consumes an application cache provider.
- `observability` owns configured liveness/readiness probes and bounded-label
  Prometheus HTTP metrics. Task readiness is derived from per-site task runtime
  state updated by initialization and existing scheduler ticks, never by an
  additional task-store probe. Deployment policy is supplied through `SiteConf`,
  not inferred from a host environment.
- `commands` provides typed command registration and command dispatch through a
  built `Site`.
- `apidocs` and `schema` generate OpenAPI and schema output from registered
  operations and types.
- `mcp` is an optional Streamable HTTP subsystem over explicitly registered
  semantic callables and static resources. Bundle-owned `McpToolRegistry` and
  `McpResourceRegistry` entries retain stable schemas, direct callable targets,
  MCP annotations, and immutable registered text content; `McpEngine`
  separately owns finalized bundle-subtree service endpoints. Tools execute
  through `McpToolContext`, without a route adapter, while resources are served
  only by the MCP protocol. The engine owns protocol framing, scope-filtered
  discovery, resource listing and reads, and optional
  protected-resource metadata; it authenticates through the central audience
  selector just like a normal route. Generic optional `auth::oauth`
  delegates external JWT/JWKS validation and bounded key rotation to a private
  Huskarl runtime built eagerly for the site; MCP only publishes protected-resource
  metadata when that provider exposes it. It never
  forwards credentials or owns OAuth login and consent. Modern MCP requests are
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
- `Record`, `ManagedRecord`, `Model`, `Filterable`, `SortKey`, `SqlEnum`, and
  `embed_migrations!` delegate Mool macro expansion through `vyuh::db`, keeping
  Vyuh applications on one database runtime identity without a direct Mool
  dependency. Validation and schema macros generate their corresponding
  integration code.
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

Task lane ownership uses leased rows on every backend. PostgreSQL advisory
locks are deliberately excluded: session locks would pin pool connections and
transaction locks would require a database transaction across handler or hook
execution. Owner tokens fence renewals, commits, lifecycle transitions, and
release after lease takeover; no transaction remains open during application
execution.

## Signal And Channel Model

Signals are the only application event publish path. `site.signals().emit(T)`
queues fire-and-forget in-process handler fanout and also offers the same typed
payload to channels. Delayed event production is intentionally not part of the
signal client; scheduled sources belong in emitters, and durable delayed work
belongs in tasks.

Channels are consumers of typed signal payloads, not a separate topic bus.
Routes attach a `Subscriber` to a `Channels::user(UserKey).channel(ChannelKey)`
stream and declare accepted payload types with `deliver::<T>()` or
`deliver_if::<T>(...)`. `(UserKey, ChannelKey)` is the sole logical identity:
it owns policy, bounded process-local replay, and debounce. Re-registering a
logical channel replaces only its policy; its active sessions continue using
the replacement. Physical session IDs are runtime-private and receiver drop
removes only that session. `Beacon` is the declarative authenticated route
form: site construction derives its private `ChannelKey` from the final route
name, normalized path, and `GET`, and only explicit Beacon operation markers
receive such keys. Endpoints neither replace each other nor share retention.
Its optional trailing debounce is likewise user-, channel-, and signal-type-
scoped. Direct keys are application-owned stable names and are bounded per user
to prevent unbounded request-derived channel state.

`SubscriptionRuntime` owns logical-channel policies, type indexes, predicates,
debounce, local replay logs, and session wakeup. It has no public session
close/find APIs; transport response lifetime owns physical cleanup.
`SubscriptionLog` is a bounded process-local reconnect convenience, not a
durable event history. Predicates run before serialization; accepted payloads
are serialized once and delivered through a shared envelope across WebSocket,
SSE, and polling. One site-owned scheduler handles bounded,
generation-guarded trailing Beacon deadlines without request-time locks.

`ChannelFanout` is a crate-private future boundary for ephemeral shared-node
delivery. Its raw envelope uses a UUIDv7 delivery id and a deterministic signal
key derived from the complete Rust type identity. Incoming envelopes are decoded
and evaluated only by the receiving node's active `SubscriptionRuntime`; they
never enter `SignalEngine`, so domain handlers cannot be duplicated. There is
no shared backend or cross-node replay today. A future shared mode is live-only:
local replay and debounce remain node-local, duplicate delivery across attached
nodes is possible, and an explicit application namespace is required at site
build. Redis, if added, uses Pub/Sub rather than Streams for this mechanism.

## Request Flow

1. A bundle registers routes, commands, emitters, services, schema contributors,
   templates, assets, signals, and optional crate-owned migration sources.
2. `Site::build` validates `SiteConf` and bundle metadata.
3. `SiteBuilder` creates the database pool, router, template engine,
   authenticator, cache registry, command registry, channel backend, signal engine, emitter
   engine, services, and task engine. Database-backed builds use the selected
   backend task store; development builds may use `MemoryTaskStore`. When enabled,
   observability endpoints are mounted before the site router is finalized and
   request metrics are applied as a site-wide middleware.
4. The authenticator resolves key sources off request threads and builds
   immutable credential-provider, selector, metric, and typed login-method
   registries plus the site-secret key ring. OAuth resource, external ID-token,
   and federated discovery/JWKS state is initialized before the site starts.
   Application-owned routes select identity proof with `.via(...)`, while
   password, Basic, MFA, and optional federated methods delegate successful
   proof to the selected credential provider.
   Runtime `.using(...)` and `.via(...)` calls retain descriptors without
   failure; terminal operations resolve them against the immutable registries.
   Credential issuance, method login, refresh, and logout require an explicit
   `.using(PROVIDER)` selection, while request extraction alone falls through
   absent access credentials. `AuthConf::default()` installs no provider;
   `AuthConf::development()` deliberately installs the framework JWT provider.
   Access
   dispatch first resolves the route audience, then considers only providers
   whose static audience coverage includes it. A selector may repeat across
   disjoint audience sets, while overlapping or unrestricted same-selector
   providers fail site construction. Multiple eligible credentials still fail
   before cryptographic work. Refresh preserves the credential's complete
   audience set and rotates it only inside its selected provider. MFA,
   federated, OTP, and stateful magic-link continuations require atomic
   one-time application stores before the site can start. Cookie
   credentials use provider-managed delivery, CSRF validation, and logout
   response attachments. Optional lifecycle storage supplies replay protection
   and revocation without changing handler signatures. Each framework provider
   implements one private asynchronous runtime contract for authenticate,
   login, refresh, logout, capabilities, and OpenAPI metadata. Server-side
   session storage is not implemented, but a stateful provider can fit that
   contract without changing handler or registration APIs.
   The cache registry validates configured provider names and default selection
   once, then exposes lock-free provider selection through `site.cache()`;
   individual providers remain responsible for their own asynchronous I/O.
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
   Every Vyuh HTTP operation is registered once at a slashless internal path.
   The outer `SiteService` removes one terminal slash before Axum matching,
   preserves `OriginalUri`, and dispatches once. Static per-route guards redirect
   slashful declarations or reject the alternate form for `trim = false`; no
   route-aware slash index, alias, scan, fallback re-entry, or lock exists.
   Vyuh-registered routes also receive their `OperationId` as a request
   extension; task, signal, and command invocation contexts carry the same
   identity for their own operation. Operation bundle origin is assigned once at
   registration and never rewritten by bundle composition.
9. Handlers call query builders or services and return typed responses. Normal
   JSON resources remain direct, paginated queries return Mool's `Page<T>`, and
   framework-generated JSON failures normalize into `ErrorReport`; raw responses
   remain an explicit application escape hatch.
10. When enabled, each `BundleConf::mcp` declaration selects tools and static
    resources in its final bundle subtree, with the nearest nested declaration
    owning each registration. Site construction finalizes all service paths,
    rejects unclaimed entries, audience mismatches, duplicate resource URIs,
    cross-service UI attachments, and endpoint/resource collisions, then builds
    deterministic tool and resource catalogs per service. Protected endpoints
    validate their configured audience and map the subject to `AuthUser`.
    Discovery filters tools using the same normalized scope rules used again at
    call time; resources remain protected by that same endpoint boundary.
    Direct targets receive `McpToolContext`; the MCP engine does not adapt or
    dispatch HTTP routes. Tools and resources never receive the external bearer
    credential. Only public
    OAuth metadata and JWKS remain inside an eligible provider's per-site
    Huskarl verifier and never cross the application cache boundary.
11. OpenAPI and schema metadata are produced from registered operations and
    type metadata.

## Extension Rules

- Prefer adding behavior through bundles and typed subsystem registries.
- Keep backend-specific behavior isolated behind backend cfgs.
- Keep `mod.rs` files as module wiring and re-export surfaces.
- Keep public APIs fallible and explicit.
- Add tests for non-trivial behavior at the subsystem boundary.
