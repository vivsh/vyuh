# Vyuh Architecture

Vyuh is an Axum-based Rust web framework for building typed JSON APIs. The core
architecture is handler-first: application code defines routes, commands,
signals, tasks, and OpenAPI metadata through typed functions and bundle
registration rather than through a separate configuration layer. Handlers can
be registered with direct APIs or macros.

## Task pipeline and ownership

The task runtime uses independent claim, commit, and lane-coordination
transactions. Cancellation intentionally does not interrupt running handlers.
Poll-paced recovery reconciles uncertain claim and commit acknowledgements.
Clustered heartbeats retire stale nodes; ordinary claims reclaim their abandoned
running tasks. Standalone startup returns prior running claims under the
deployment-enforced singleton contract. There is no runtime fingerprint
enforcement or version-based admission.

Upgrading from the lease-owned runtime is a coordinated breaking change:
stop all workers and writers, back up, and apply the ledger-tracked task reset
and node-schema migrations before starting the new runtime. The reset deletes
all old tasks and idempotency reservations, not schedule cursors, rate balances,
or lane lifecycle state. Individual task lease columns and the old fingerprint
storage are removed. Fresh installations use the desired schema directly.

The runner owns lane-local input queues and one shared FIFO output buffer.
Global and lane execution permits travel with a completion until the collector
has buffered every accepted outcome. A separate database semaphore bounds
requests through application of their responses. Claims and flushes can overlap
without a combined store turn. An operation-local UUID correlates each immutable
request with its acknowledgement; durable ownership is the task's nullable
UUID `node_id`, fenced with its lifetime attempt and running status.

One bounded local broadcast channel replaces the wake notifier. Dispatcher clones
send `Wake` after ingress and `Cancel(TaskId)` after acknowledged terminal cancellation;
each runner subscribes. Its receiver replaces the existing wake branch in the
seven-branch select (DB results, handler completions, hook completions, death
deadline, controls, shutdown, scheduling deadline). Claimed inputs arrive through
DB results, not a separate input channel. Notifications never bypass poll gates.
Cancellation IDs are deduplicated in operation-local scratch and remove only
queued snapshots. They create no outputs or uncommitted pressure: the store has
already finalized cancellation. There is no retained cancellation set or queue
index. Execution permits and running invocations are untouched.
Lag, no subscriber, remote producers, or a claim response arriving after the
notification may miss this local optimization. The terminal store record remains
authoritative and rejects subsequent outputs.

`commit_tasks(&[TaskCommit])` is the one store entrypoint for regular output
flushing and public cancellation. Outcome entries carry node UUID, lifetime
attempt, lane and optional lane token; Cancel entries carry only a task ID.
Acceptance checks differ, but result encoding, cleanup, idempotency retention,
bulk writes and atomic parent/join delivery are shared. Cancellation targets any
nonterminal status without execution ownership, commits Failed and the marker,
and clears ownership immediately. Missing/terminal targets are no-ops. Duplicate
IDs within one low-level batch are rejected before mutation. Requests borrow the
existing immutable buffer; no receipt or parallel task state is introduced.

Cancellation now holds the existing commit transaction's task/parent row locks
until commit or rollback, including idempotency bookkeeping where applicable.
PostgreSQL/MySQL use the existing FOR UPDATE reads; SQLite uses its transaction
semantics; memory uses its existing atomic staging under one mutex. There is no
intermediate intent-only write. Ordinary outputs retain their owner-filtered
read and write fields; cancellation batches additionally persist the flag.

The store alone changes task lifecycle fields. Each output flush atomically
writes outcomes and workflow changes; each claim independently establishes
ownership. Poll gates still control execution of newly committed children and
resumed parents. Lane-owner leases and tokens remain centrally coordinated.
Transient database errors retain requests for poll-paced retry or reconciliation;
permanent database and internal protocol errors stop the runner. Graceful draining stops claims and queued dispatch, collects and persists
executing work, and waits for every admitted database response. A final single
update returns this incarnation's residual running claims to pending; queue
snapshots are discarded only after acknowledgement. Failed or uncertain give-back
is retried at the polling interval until acknowledged or the death watchdog expires.

All runners register their fresh incarnation UUID in `vyuh_task_nodes`
before claiming. The store owns `started_at` and `heartbeat_at`, both using
database time. In clustered mode, a centrally scheduled heartbeat updates that row every 12
seconds by default, independently of task polling. It shares the existing DB
semaphore and response application path, with priority at the next available
slot. It neither adds task-path queries nor changes lane coordination.
`TaskMode::Standalone` registers once at startup but sends no heartbeats. The
deployment must guarantee one runner per task store, including during restarts.
Producer-only submission does not register nodes or reclaim tasks in either mode.
After worker setup, standalone startup uses one update to return all running rows,
then registers its new incarnation before claiming. Registration failure prevents
startup in either mode; it does not trigger automatic re-registration.
Clustered startup does not reclaim other incarnation UUIDs. Both reset scopes
clear node ownership and advance readiness using store time, preserving
attempts, checkpoints, resume input, results, cancellation intent, and join metadata.
Normal claims still enforce retry budgets and cancellation. Returned queued tasks
retain the attempt already charged at claim. No task hot-path query is added;
lane locks retain their separate token/expiry lifecycle.

After a successful self-heartbeat, one separate autocommit DELETE retires peers
strictly older than six heartbeat intervals (72 seconds by default). The delete
rechecks age in its predicate; all clustered runners must use the same interval.
The self-update lock is released before peer deletion. A nullable, indexed
`vyuh_tasks.node_id` FK uses `ON DELETE SET NULL` and default `ON UPDATE NO ACTION`.
Retirement clears ownership only; normal claims select ownerless Pending or
Running rows and retain ordinary cancellation, retry budgets, and workflow rules.
Reclaimed joins reuse already-persisted resume input. No recovery scan or new
task-path statement is introduced. FK checks and cascades do incur engine locks
and index maintenance; deletion can wait on task transactions and scale with the
number of abandoned claims. Existing task/lane explicit lock order is unchanged.

A heartbeat that matches no node puts the runner in terminal `Retired` mode:
abort invocation/hook futures, discard unacknowledged local work and DB futures,
and exit that runner without new flushes, give-back, or re-registration. Other
application services remain running with unhealthy task health; site readiness
still respects its explicit task-readiness policy. Already-issued
SQL and external effects may still complete; fencing protects durable results,
not exactly-once effects. Transient heartbeat errors retry through the same
central DB admission as other failed operations. Heartbeats continue during graceful draining; future deadlines
alone do not prevent completion. Shutdown retains node rows until a clustered
peer retires them. Standalone performs no periodic cleanup. Stalled DB operations
and lane gates can delay recovery; no bounded takeover time is promised.

### Database acknowledgement recovery

The runner retains one immutable request per operation ID. Its in-flight phase
owns retry eligibility and optional prepared response evidence; there is no
receipt table, mutable task cache, or recovery worker. Claim/coordination stores
retain their prepared TaskPoll when COMMIT acknowledgement fails. Output requests
already retain task IDs, lifetime attempts, outcomes and lane tokens.

Recovery starts no earlier than one poll interval after failure and uses the
existing DB semaphore. Current reads reconcile exact IDs; same-node/same-attempt
Running outputs may be applied through the existing fenced transition engine.
Advanced or missing executions are resolved as obsolete, not asserted to be
successful historical commits. Claim recovery never charges another attempt or
rate permit. Repeated recovery errors preserve original identities and bounded
distinct wake hints.

Only recovery adds current locking reads: lane rows before sorted task IDs on
PostgreSQL/MySQL, without SKIP LOCKED. SQLite reserves its existing writer before
the first recovery read using a zero-row UPDATE; no task data changes in that
statement. Valid recovered lane leases are renewed under their existing row lock.
Lost lane ownership relinquishes still-node-owned claims in bounded store batches
before local work is discarded. Normal successful claim/flush SQL is unchanged.

A runner-local death deadline is armed while DB operations are executing or
unresolved and reset by any acknowledged runner DB operation. Its duration is six
heartbeat intervals; it is disarmed when no operation remains. Idle standalone
runners therefore need no liveness query. Successful heartbeats preserve liveness
but do not clear unresolved-operation health failures. The deadline remains
selectable while every DB permit is occupied. Expiry stops the task runner,
aborts local work and drops unacknowledged requests without give-back or
re-registration. This is not a per-query timeout or a proof of server rollback.

### Task safety terminology

These are existing store guarantees, not additional schedulers or deployment
compatibility checks:

- **Ownership fencing:** an output is accepted only for the task's current
  `node_id`, claimed lifetime attempt, and `Running` status, with a valid
  lane-owner token when lane ownership applies. A stale execution cannot
  overwrite a newer result, create children, or deliver a parent result.
  Fencing rejects stale writes; it does not itself detect dead nodes or recover
  abandoned tasks.
- **Cancellation precedence:** cancellation and regular outputs share atomic
  commit serialization. If cancellation commits first, the task is terminally
  failed and any later output is stale, including retry, checkpoint and child
  creation. If terminal completion commits first, cancellation is a no-op.
  Historical intent flags remain honored by ordinary claim/recovery paths.
  This protects stored state, not external effects or interruption of code.
- **Atomic workflow commits:** related durable transitions succeed or roll
  back in the same database transaction. Parent checkpoint/suspension and child
  insertion are atomic; a terminal child's result and eligible parent delivery
  are atomic. For `all`, child results and parent counter/readiness updates are
  atomic; ordered results are materialized separately in the parent's claim
  transaction. Children and resumed parents execute only through a later
  eligible claim, never recursively during a flush.

The store owns these mutations; runner and handler task records are immutable
snapshots. Removing runtime fingerprints does not remove these guarantees, and
future contract versions do not replace them. They do not imply exactly-once
external effects. Database contact, node retirement and lane polling may delay recovery.

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
- Private `diagnostics` projects typed assembly errors into public
  `BuildDiagnostic` values on demand. Errors own their sources; the site and
  registries retain no diagnostic state. Configuration preserves subsystem
  causes, while service initialization attaches the existing concrete type
  only on failure. Terminal presentation sanitizes and bounds causes without
  changing HTTP errors, validation sequencing, or runtime initialization.
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
- `services` constructs site-owned dependencies before runtime startup. A sealed
  output constraint accepts owned `ServiceInstance<T>` values and fallible
  `Result<ServiceInstance<T>, ServiceError>` factories through the same callable
  conversion. Initialization retains unique ownership before exposing facades;
  factory errors propagate through `SiteError` and drop assembly-owned resources
  without starting registered workers. No separate construction runtime exists.
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
- `tasks` provides typed input, durable background task registration,
  immediate transactional submission, named concurrency lanes, batched claims
  and commits, optional process-local `Data<Batch<T>>` handler invocation,
  store-owned parent/root lineage, registration-derived Work/Flow kind, lane-owned retry/backoff and
  idempotency-retention policy, local
  runner and store-wide database rate limits, adaptive polling,
  opt-in durable lane ownership, and explicit continuation lifecycle control.
  Ordinary lanes retain the direct task claim path. An owned lane coordinates
  through a separate primary-key lease row, keeps one fenced owner across task
  execution, and runs lifecycle hooks as independent async futures outside task
  concurrency. Its lease renewal uses centrally admitted lane coordination and
  wakes only for normal polling, useful work, or the half-lease safety deadline;
  it does not create a per-lane renewal loop. Tasks statically declare their
  lane and optional key rule; reusable bundles may contribute complete lane
  defaults, while site configuration resolves or strictly rejects missing
  lanes.
  PostgreSQL's partial readiness index orders ownerless Pending and Running
  tasks by lane, readiness, creation time, and ID. Status belongs only in its
  predicate so selecting both statuses does not break the claim's index ordering.
  Ordinary lane refill capacity subtracts running invocations, queued tasks, and
  all completed-but-uncommitted task outcomes, regardless of outcome kind.
  This uses existing lane accounting without scanning the commit queue. Commit
  acknowledgements release refill capacity on a later poll; handler concurrency
  remains invocation-based. Owned lanes already drain a cohort before refilling.
  Local handler batching groups matching rows already held by a lane queue and
  never adds a store query or waiting policy. It is independent of durable lane
  ownership: each row retains its own attempt, rate permit, node ownership, fenced
  commit, and inspection history while one batch future consumes one local
  handler-concurrency slot.
  Flow handlers construct `FlowState::spawn` requests without accessing the site.
  The returned request owns its typed child payload, serialized checkpoint, and
  options only until internal preparation resolves it through the executing
  site's registry. It becomes the existing store-facing `TaskOutcome::Spawn`;
  no unresolved payload reaches the store, and no public task-facade spawn
  operation exists. Ordinary outcomes do not perform registry preparation.
  Work registers through `work`/`work_batch` and Flow through `flow`; task remains
  the shared submission, storage, and runtime concept. These registration names
  do not change persisted handler identities.
  Work uses task-specific async callable registration and `WorkState<T>` or direct
  serializable outputs. Flow uses one-time synchronous factories, `FlowConf`, and
  immutable definitions implementing `Flow::advance`. Site construction consumes
  factory registrations, deduplicates dispatcher construction in temporary
  type-keyed scratch, and retains only prepared definitions. Manual definitions
  require no optional dependency. Factories may receive `PartialSite` (database
  handle only); execution receives input, read-only task identity and continuation.
  Execution metadata comes from `Flow::Input`, not factory arguments. Factory
  errors and panics fail construction before workers start.
  Both kinds receive continuations and support suspension; only Flow can sleep
  or spawn children (one child or an `all` group). Runtime site/service and
  operation identity extraction remain unavailable to Flow.
  Task-only return conversion serializes before type erasure through the existing
  single callable future. WorkError chooses retry/failure, FlowError permits
  only failure, and TaskRuntimeError describes infrastructure/API failures.
  Inferred sealed return categories avoid macro type detection. Typed output and
  conversion scratch are invocation-local, never parallel durable task state. The registered handler variant owns its
  classification; immutable store configuration contains name/kind pairs.
  Both kinds use the same spawned invocation and scheduler. Kind validation
  occurs before application execution and capability validation occurs after
  store fencing, with cancellation taking precedence. Neither adds SQL or a
  separate recovery policy. Sync signatures restrict supported capabilities,
  not arbitrary Rust effects or blocking operations.
  The non-default `pravah` feature re-exports the selected dependency and prepares
  builders/compiled graphs through one adapter. A prepared graph owns only the
  immutable graph, shared dispatcher, and instruction budget. Each invocation
  owns a temporary VM; snapshots live exclusively in task continuations. Dispatchers
  select registered Work payloads without performing effects or transforming
  results. Agent Work returns the complete AgentResponse, preserving partial
  history/compaction acknowledgements even on an inner AgentError. Terminal Work
  failure maps to an AgentResponse with the fixed `vyuh_task_failure` AgentError.
  The dispatcher's history policy initializes fresh executions only; restored
  snapshots own their saved policy. History I/O runs in Work through an explicitly
  configured executor, never inside synchronous advancement. Suspension failures terminate the
  parent; successful domain values remain unchanged. Pure instruction-budget
  exhaustion returns ordinary zero-delay sleep. No VM cache, implicit executor,
  per-instruction persistence, new table or scheduling loop is introduced.
  Registration validates configuration without computing deployment fingerprints.
  Protocol upgrades are ledger-owned and never reinterpret existing checkpoints.
  Spawn outcomes checkpoint a suspended parent and prepare one child; terminal
  child outcomes deliver Serde `Result` values through the existing resume input.
  Task updates, child inserts, and narrow parent updates use bounded Mool bulk
  writes in the same flush transaction. Claims are separate transactions;
  transient wake lanes respect each lane's existing poll eligibility. Public
  resume remains independent. Per-step retries reset at successful checkpoints,
  while lifetime invocation counts remain intact. `FlowState::all` uses the same
  transition engine. The store owns ordered `waiting_children` and a completion
  counter; memory storage owns these in its private wait map. Neither is part of
  runner/handler snapshots. Fenced first-terminal child transitions decrement
  counters without membership or sibling-result reads. Satisfied parents become
  pending when the terminal-child flush commits. Their next claim fetches terminal results in
  shared bounded current-read chunks, persists one ordered resume envelope, and
  clears the wait in that claim transaction. Reclaims reuse durable resume input.
  Ordinary tasks issue no additional SQL; join writes use narrow bulk patches.
  Fan-out outcomes use weighted FIFO flush capacity and existing uncommitted
  accounting; no fan-out-specific queue or scheduling loop is introduced.
  `max_all_children` is validated at input and outcome acceptance, not a concurrency limit.
  No separate workflow scheduler or output archive is introduced. Each task retains
  its latest JSON result in `last_result`; checkpoint commits clear it. Results
  and resume inputs share a fixed 32,768-byte envelope limit. Terminal child
  delivery reuses the persisted result bytes. Result envelopes require a
  coordinated schema/data upgrade; mixed old/new
  workers or writers are unsupported.
  Cancellation uses the shared commit operation to terminally fail an active task
  and deliver its child result atomically. Runner/handler snapshots remain
  immutable. `cancel` returns unit acknowledgement of terminal persistence or an
  idempotent no-op, not interrupted execution. Cancellation bypasses lane claim
  gates; resulting parent execution still obeys ordinary polling and admission.
  Local queued tasks can skip execution via the shared control channel. Both
  single and shared running invocations continue; their cancelled outputs become
  stale. A parent can resume before the child's external work has stopped. A hung
  handler can delay lane idle and draining, but no longer delays cancellation
  finalization or parent delivery. Lane-owner loss still aborts invocations.
  Execution permits end at collection; uncommitted pressure ends at acknowledgement.
  Cancellation protects the durable terminal result, not cessation of effects.
- `cache` provides an immutable per-site registry of asynchronous named cache
  providers. Its typed handles own JSON serialization, canonical provider and
  namespace key scoping, and bounded metrics; providers own byte storage, TTL,
  atomic integer, and scoped-clear semantics. The default memory provider is
  bounded and local to one built site. OAuth verifier state remains a separate,
  per-site protocol concern and never consumes an application cache provider.
- `observability` owns configured liveness/readiness probes and bounded-label
  Prometheus HTTP metrics. Task readiness is derived from per-site task runtime
  state updated by initialization and runner database acknowledgements, never by an
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
  node ownership, lane leases, idempotency ownership, durable token buckets, and database-relative
  wake deadlines; Mool owns database execution. There is no store-wide
  deployment fingerprint or policy-row lock. Task schemas are
  migration-owned and are never created or altered during site startup. Store
  internals are framework-owned; the ordinary site facade exposes only typed
  submission, cancellation, resume, reassignment, and read-only inspection.
  Store configuration is supplied during inert site construction, without task SQL,
  and shared as an immutable `Arc<TaskStoreConf>` without a configuration lock or
  uninitialized state. Producers use it directly: there is no lazy task-store `OnceCell`.
  `SiteBuilder::start_runtime` invokes `TaskRunner::initialize` to validate retained
  tasks and prepare rate/lane rows, reclaim standalone claims, and register the
  runner incarnation before claims in both modes.
  The separate site-runtime start guard remains. Input validation, startup
  missing-handler/lane detection, and task/lane fencing remain; there is no
  cross-worker configuration compatibility check. One ordinary PostgreSQL lane
  without a global rate or lane lock claims through one data statement: a bounded
  materialized selection locks candidates, an UPDATE establishes ownership, and
  RETURNING supplies updated snapshots. Payloads are selected once. The same
  statement supplies database time, future-readiness hints, and a narrow
  underfilled-page existence check so contention does not trigger long fallback
  polling. Cancelled, exhausted, invalid-counter, and join-materialization batches
  instead return the locked selection to the shared Rust transition machinery.
  Transaction control, periodic maintenance, and exceptional workflow writes are
  additional statements, not part of a universal one-query guarantee.
  SQLite retains combined selection/timing reads; coordinated lanes reserve rate
  permits before task locks. Mool handles projections, sequential queries, and
  bulk writes. The PostgreSQL atomic claim uses a bound raw statement because
  Mool's typed API does not express data-modifying CTEs with metadata-only rows.
  Claims select ownerless
  ready Pending or abandoned Running rows. Flushes validate each independent entry
  once and borrow a task-ID-to-command map for row selection, fenced acceptance,
  workflow preparation, and obsolete-output reporting. The map is operation-local,
  not another owner of buffered outcomes; recovery prepares its own map from its
  retained immutable request. Fenced outcome reads retain deterministic identity
  order and atomic workflow mutations. There are no mixed store
  turns or task renewal reads. A clone-shared counter spaces bounded physical
  idempotency cleanup; logical expiry and lane lifecycle decisions remain
  store-owned and are not deferred by that counter.
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

- Route, signal, work, work-batch, flow, cron, periodic, and asset macros
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
   coordinates durable lane leases, and flushes one common outcome buffer.
   Input queues remain isolated by lane for admission, rates, concurrency, and
   ownership. The shared outcome queue batches completed work across lanes;
   completion writes are not subject to handler rate permits.
   Database futures do not block execution or completion collection. Claims and
   flushes have independent pacing and may overlap up to `db_concurrency`.
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
