# Production deployment

Vyuh 0.3 supports PostgreSQL and SQLite production deployments. PostgreSQL is
the clustered deployment target. SQLite is supported for a durable, local,
single-process deployment; it is not a multi-node task queue. MySQL compiles,
but remains experimental until it has equivalent migration and concurrent-task
coverage.

## Start from the production profile

Use the opt-in profile so existing applications keep their development
behavior until they choose a hardened baseline:

```rust
use vyuh::{SiteConf, db::DbConf};

let conf = SiteConf::production()
    .database(DbConf::from_url(
        "postgres://vyuh:secret@db.example.internal/app?max=20&min=2",
    )?);
# Ok::<_, vyuh::db::DbError>(conf)
```

The profile enables tracing, response compression, request timeouts, body
limits, security headers, health probes, and Prometheus metrics. It keeps CORS
disabled: allowed origins are application policy, so install an explicit Tower
CORS layer with the precise origins, methods, and headers your application
accepts.

Production validation rejects permissive CORS, an enabled console without a
secure cookie, and auth cookies that are not `Secure` and `HttpOnly`. The
console is disabled by default. If it is enabled intentionally, expose it only
through an authenticated administrative network path and HTTPS.

## Probes and metrics

The profile exposes these routes by default:

| Route | Purpose |
| --- | --- |
| `/healthz` | Liveness: the process has started serving requests. |
| `/readyz` | Readiness: startup completed, task readiness policy passes, and, with a database feature, a bounded `SELECT 1` succeeds. |
| `/metrics` | Prometheus text exposition. |

Set `ObservabilityConf` if these paths collide with an application route or
need to be mounted behind a proxy prefix. Keep `/metrics` and probe endpoints
on an internal network or proxy allowlist; they disclose operational state.

HTTP metrics use only method, registered route template, and status class as
labels. They never include raw paths, user identifiers, request bodies, or
secrets. The exposed metrics cover totals, duration histogram buckets,
in-flight requests, server errors, and recovered panics.

## Database and migrations

Mool is Vyuh's database and migration boundary. Applications retain
`vyuh::db` imports for 0.3 compatibility; Vyuh consumes Mool's public facade
and does not require applications to depend directly on Gaman.

Vyuh's 0.3 release gate therefore requires the selected Mool release to provide
backendless compilation, the model/query/mock APIs used by enabled-backend
tests, the public migration runner facade, and runtime SQLx access. The runner
must also yield `Send` command futures when serialized through Vyuh's
`tokio::sync::Mutex`; SQLx test macros remain a Vyuh development dependency.

Apply migrations before deploying or starting workers. Task tables are a
crate-owned schema contribution in the same Mool/Gaman migration registry as
application schemas. `Site::build` never creates, alters, or baselines a task
table. A safe release order is:

1. Build and inspect the intended migration plan in a staging copy.
2. Apply the migration through the deployment job.
3. Verify schema/status through the migration command.
4. Start the application workers only after the migration succeeds. Roll workers
   only when that upgrade explicitly supports overlapping versions.

The node-owned task runtime requires an [offline, destructive upgrade](tasks.md#upgrading-existing-task-stores)
from the old lease-owned schema. Stop workers and writers first; the reset deletes
old tasks and their results. Ordinary schema diff generation is not a substitute
for that coordinated reset.

For an existing task table, inspect the live schema first. Use the migration
engine's baseline/fake operation only after an operator has reviewed and
confirmed equivalence. Generate and apply normal migrations for differences;
never silently stamp a production schema during startup.

## Cluster responsibilities

Task workers use fresh node identities and fenced claims. In the default
`TaskMode::Clustered`, heartbeats retire stale nodes and ordinary claims recover
their abandoned tasks. All peers must use the same heartbeat interval.
`TaskMode::Standalone` skips periodic heartbeat work but requires deployment-enforced
exclusivity, including during restarts; select it explicitly for a single-runner
SQLite deployment. Backend selection does not select the task mode.

Task handlers must still be idempotent: an isolated or crashed worker may have
performed an external effect before its output committed. Cancellation prevents
later outputs from changing the cancelled row but never interrupts a running
handler. Vyuh does not promise exactly-once effects. See [task recovery and health](tasks.md#runtime-health)
for watchdog, readiness, and shutdown behavior.

Vyuh does not provide a general HTTP rate limiter. Enforce HTTP rate limits at a
reverse proxy or API gateway, or install application middleware backed by a
shared store when a cluster-wide limit is required. Terminate TLS at the
application or trusted proxy, forward only validated proxy headers, and set
host/firewall policy outside the framework.

Signals, channels, and periodic emitters are in-process runtime facilities.
They do not become a distributed event bus merely by running multiple Vyuh
replicas. Task-targeted cron/periodic emitters are different: their schedule cursor
and task insertion commit atomically, with coalesced catch-up after restart.
Use a shared external transport when event delivery must cross processes.

## 0.3 compatibility note

Vyuh 0.3 keeps the `vyuh::db` import facade and the current task lifecycle API.
The notable operational changes are that task persistence now uses Mool-native
queries, task migrations are external and pre-deploy, and `SiteConf::production`
is the explicit hardened profile. This is a pre-1.0 release: APIs remain
subject to documented change rather than a 1.0 stability promise.
