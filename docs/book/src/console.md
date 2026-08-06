# Console

Vyuh console is a built-in operational UI and JSON API for inspection. It is
enabled by default in debug builds at `/console`, disabled by default in release
builds, protected by an application-owned access policy, and read-only in this
pass.

Use it for inspecting registered operations, task records, runtime status,
OpenAPI for application routes, and redacted runtime configuration. Do not use
it as an application admin framework or a command/task execution surface.

## Mental Model

- Console is a built-in operational app mounted at `ConsoleConf.path` when
  `ConsoleConf.enabled` is true.
- `ConsoleConf::default()` enables the console in debug builds and disables it
  in release builds.
- Console uses normal application access credentials carrying the public
  `CONSOLE_AUDIENCE`; it never installs private providers or browser cookies.
- A synchronous application-owned `ConsoleAccess` policy decides whether the
  authenticated user, or deliberately no user, may inspect Console.
- The HTML UI is server-rendered with Minijinja and progressively enhanced with
  HTMX. JSON APIs remain available under `/api`.

## Configuration

Configuration lives on `SiteConf`:

```rust
use vyuh::prelude::*;
use vyuh::{
    Site,
    auth::AuthUser,
    console::{CONSOLE_AUDIENCE, ConsoleAccess, ConsoleConf},
};

const ADMIN_MASK: u64 = 1;

struct AdminConsole;

impl ConsoleAccess for AdminConsole {
    fn allows(&self, _site: &Site, user: Option<&AuthUser>) -> bool {
        user.is_some_and(|user| user.roles & ADMIN_MASK != 0)
    }
}

let conf = SiteConf::default().console(
    ConsoleConf::default()
        .enabled(true)
        .access(AdminConsole)
        .path("/console"),
);
```

Defaults:

| Field | Default |
| --- | --- |
| `enabled` | `cfg!(debug_assertions)` |
| `path` | `/console` |
| `access` | debug development mode; required in non-debug builds |
| `page_size_default` | `50` |
| `page_size_max` | `250` |
| `status_cache_ttl_seconds` | `5` |

Issue a regular application credential with the console audience whenever the
authenticated user is allowed to inspect Console:

```rust
site.auth().login(user, &[API, CONSOLE_AUDIENCE]).await?;
```

The configured provider controls delivery, rotation, validation, CSRF, and
logout exactly as it does for application routes. Console never mints, exchanges,
or clears credentials. Applications own sign-in and sign-out routes.

In debug builds an enabled Console without `.access(...)` deliberately runs in
development mode. It accepts no credential, displays a persistent warning, and
must not be exposed publicly. In non-debug builds an enabled Console without a
policy fails site construction.

## Endpoints

All endpoints are mounted under `ConsoleConf.path`.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/` | canonical status overview page |
| `GET` | `/overview` | status overview page |
| `GET` | `/runtime` | formatted site, process, and system runtime page |
| `GET` | `/operations` | operation listing page with in-page inspector |
| `GET` | `/operations/{id}` | operation detail page |
| `GET` | `/tasks` | task listing page with filters and in-page inspector |
| `GET` | `/tasks/{id}` | task detail page |
| `GET` | `/schedules` | durable task schedule listing with in-page inspector |
| `GET` | `/openapi` | OpenAPI page for non-console routes |
| `GET` | `/conf` | redacted runtime configuration page |
| `GET` | `/api/session` | inspect the current authenticated console identity |
| `GET` | `/api/operations` | list/search operation metadata |
| `GET` | `/api/operations/{id}` | inspect one operation |
| `GET` | `/api/tasks` | list task records |
| `GET` | `/api/tasks/{id}` | inspect one task record |
| `GET` | `/api/schedules` | list task-targeted schedule definitions and cursors |
| `GET` | `/api/logs` | query bounded configured file logs |
| `GET` | `/api/status` | combined site, process, system, and safe task-runtime status |
| `GET` | `/api/openapi` | OpenAPI JSON for non-console routes |
| `GET` | `/api/conf` | redacted runtime configuration JSON |

There are no mutating endpoints in v1. Console cannot run commands, retry or
cancel tasks, fire signals, or control services.

## Assets And Templates

Console pages use the package-owned `vyuh/web/` assets:

```text
/static/css/vyuh.css
/static/css/vyuh.<hash>.css
/static/console/img/vyuh-logo-transparent.png
/static/console/js/console.js
```

Console assets use the site's shared `SiteConf::static_url(...)` configuration.
Changing `ConsoleConf.path` never changes asset URLs: a console at
`/dynrs/console` still uses `/static/**` by default, or the configured CDN URL.
The console's CSS, JavaScript, favicon, and logo are bundle-owned assets. HTMX
and ReDoc remain intentionally hosted by their upstream CDNs for now, so they
are the only console frontend dependencies outside `static_url`.

Console links are resolved from the finalized site route registry during site
construction. A missing framework console route is therefore a build error,
not a silently synthesized fallback URL.

The HTML templates live under `vyuh/web/templates/console/**` and are loaded
through the same bundle asset template path used by application templates.
Applications do not need to copy console assets to enable the built-in console.

## Operations

`/api/operations` is the single operation listing endpoint. Use query
parameters for filtering:

```text
/console/api/operations?kind=route&q=user&hidden=false&limit=50
```

Supported filters:

- `kind`: `route`, `command`, `task`, `service`, `signal`, `cron`,
  `periodic`, `pgnotify`, or `api_doc`.
- `q`: text search across name, summary, description, and path.
- `tag`: operation tag.
- `owner`: operation owner.
- `hidden`: `true` or `false`.
- `limit` and `cursor`: offset-style pagination.

The response includes operation metadata derived from the same bundle operation
model used by routes, OpenAPI, commands, tasks, signals, emitters, and services.
The HTML operations page uses the same filters and keeps selected operation
request/response details in a right-side inspector.

## OpenAPI

`/api/openapi` generates an OpenAPI JSON document from visible route operations
outside the console bundle. `/openapi` renders the same JSON in the console UI.

Console routes and hidden documentation marker operations are excluded. This
keeps the console OpenAPI view focused on the application surface even though
the console itself is mounted into the same site.

## Tasks

`/api/tasks` lists task records without claiming or modifying them:

```text
/console/api/tasks?status=pending&lane=email&created_from=2026-06-01&created_to=2026-06-30&page=1&per_page=50
```

Supported filters:

- `status`: `pending`, `running`, `suspended`, `succeeded`, or `failed`.
- `name`: registered task name.
- `lane`: named task execution lane.
- `idempotency_key`: task-handler-scoped idempotency key.
- `created_from`: inclusive task creation date in `YYYY-MM-DD` format.
- `created_to`: inclusive task creation date in `YYYY-MM-DD` format.
- `q`: text search across name, lane, idempotency key, and last error.
- `page` and `per_page`: one-indexed canonical pagination.

`/api/tasks/{id}` returns the safe task detail shape for one task ID, including
status, attempts, lane, timing, idempotency key, last error, and JSON
input/state/resume fields when they parse as JSON.
The HTML task page exposes search, status, name, lane, idempotency-key, and date-range
filters and shows selected task details without leaving the list.

## Schedules

The **Schedules** page shows task-targeted cron and periodic emitters. It is a
read-only view of the immutable schedule definition and its durable
`last_submitted_at` cursor:

```text
/console/api/schedules?source=cron&lane=exports&page=1&per_page=25
```

It supports `source` (`cron` or `periodic`), `task`, `lane`, text `q`,
`awaiting_first_run=true`, and one-indexed `page`/`per_page` filters. Each entry shows its target task,
effective lane, first-start policy, last durable submission, and a computed
next occurrence. An `Immediately` schedule without a cursor shows its pending
first submission instead of a later normal slot.

The next occurrence is advisory; it is calculated from the immutable schedule
definition and cursor, not a second mutable scheduler state. The page does not
show signal-only emitters, task execution history, or mutation controls. A
restart coalesces missed task intervals into one catch-up submission, as
described in [Emitters](emitters.md).

## Logs

The **Logs** page reads only configured `LogSink::File` JSON logs. It does not
read stdout or stderr, which are not durable local sources. Add an ordinary
rotating file rule when console log inspection is wanted:

```rust
LogSink::File {
    dir: "logs".into(),
    rotation: Rotation::Daily,
}
```

The page filters by file rule, level, target prefix, inclusive UTC date range,
and text in messages, event fields, and spans. It reads newest entries first by
seeking backward through fixed-size file blocks. Each request has fixed file,
line, output, and scan budgets; broad searches can return partial results with
a visible truncation notice. Use narrower dates or continue with the opaque
older-results cursor rather than expecting an unbounded historical scan.

Log files may contain application-sensitive values. Console access is required,
responses use `Cache-Control: no-store`, and values are escaped before HTML
rendering, but applications should still avoid writing credentials or secrets
to logs.

## Status

`/api/status` returns one redaction-safe object. `/runtime` renders the same
status data as grouped operational sections with formatted CPU, memory, process,
system, and site runtime details.

The status object includes:

- site fields: Vyuh version, package name, host, port, project directory,
  timezone, database backend, uptime, enabled compile-time features, operation
  count, command count, and service count;
- process fields: PID, executable path, current directory, argv, memory, virtual
  memory, CPU usage, and platform-supported thread/open-file counts;
- system fields: hostname, OS, kernel, architecture, CPU, load average, memory,
  swap, and boot time.

Console never exposes env vars, secrets, JWT keys, API keys, cookies, full
database URLs, or raw configuration.

It also reports the task runtime's safe health state, consecutive store-failure
count, last successful scheduler tick, and failure class. Native task error
chains remain in structured logs and are never retained by the console.

Status is cached in-process for `ConsoleConf.status_cache_ttl_seconds`, default
5 seconds. Requests inside that window return the previous snapshot instead of
refreshing system/process information again.

## Config

`/api/conf` returns a redaction-safe configuration DTO. `/conf` renders the same
DTO as a console page.

The config shape is operational, not a raw `SiteConf` serialization. It includes
site host/port, project directory, timezone, selected database backend, console
settings, task and emitter limits, upload limits, channel limits, HTTP
middleware flags, and logging sink mode/path.

Sensitive values are omitted or redacted. Console config does not expose env
vars, secret values, JWT key material, API key values, cookie values, or full
database URLs.

## Current Limitations

- Console is read-only.
- Console access uses the application's credential lifetime and logout policy.
  The framework does not provide a console-specific token or sign-out flow.
- Pagination uses offset cursors in this pass.
- Task listing is inspection-only and does not affect task leasing or retries.
