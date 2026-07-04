# Console

Vyuh console is a built-in operational UI and JSON API for inspection. It is
enabled by default in debug builds at `/console`, disabled by default in release
builds, isolated from application auth, and read-only in this pass.

Use it for inspecting registered operations, task records, runtime status,
OpenAPI for application routes, and redacted runtime configuration. Do not use
it as an application admin framework or a command/task execution surface.

## Mental Model

- Console is a built-in operational app mounted at `ConsoleConf.path` when
  `ConsoleConf.enabled` is true.
- `ConsoleConf::default()` enables the console in debug builds and disables it
  in release builds.
- Console roles are separate from application roles.
- Console auth uses a console-only cookie/session. Normal app JWTs do not grant
  console access.
- The HTML UI is server-rendered with Minijinja and progressively enhanced with
  HTMX. JSON APIs remain available under `/api`.

## Configuration

Configuration lives on `SiteConf`:

```rust
use vyuh::prelude::*;
use vyuh::console::ConsoleConf;

let conf = SiteConf::default().console(
    ConsoleConf::default()
        .enabled(true)
        .path("/console"),
);
```

Defaults:

| Field | Default |
| --- | --- |
| `enabled` | `cfg!(debug_assertions)` |
| `path` | `/console` |
| `bootstrap_token_ttl_seconds` | `300` |
| `session_ttl_seconds` | `28800` |
| `print_bootstrap_url` | `LocalOnly` |
| `cookie_name` | `vyuh_console` |
| `page_size_default` | `50` |
| `page_size_max` | `250` |
| `status_cache_ttl_seconds` | `5` |

With `LocalOnly`, Vyuh prints a short-lived bootstrap URL only when the
configured host is `localhost`, `127.0.0.1`, or `::1`:

```text
Vyuh console enabled:
http://localhost:8080/console/login?token=...
Token expires in 300 seconds.
```

Bootstrap tokens are in-memory and are not persisted. Consuming a bootstrap
token creates a console session cookie and redirects to the console root. The
session cookie lasts 8 hours by default.

In debug builds on `localhost`, `127.0.0.1`, or `::1`, the console also allows
direct access without a bootstrap token. In release builds, enable the console
explicitly and keep the bootstrap/session flow or another guarded access policy
in place.

## Roles

Console roles live under `vyuh::console`:

```rust
use vyuh::console::ConsoleRole;
```

The roles are `Viewer`, `Operator`, and `Admin`. In this read-only pass,
`Viewer` can access all console APIs. `Operator` and `Admin` are reserved for
future guarded operations.

These roles do not affect `AuthUser`, `permit!(...)`, API keys, or application
authorization.

## Endpoints

All endpoints are mounted under `ConsoleConf.path`.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/` | canonical status overview page |
| `GET` | `/login?token=...` | consume bootstrap token, set console cookie, and redirect to `/` |
| `GET` | `/login-page` | show console login guidance |
| `GET` | `/overview` | status overview page |
| `GET` | `/runtime` | formatted site, process, and system runtime page |
| `GET` | `/operations` | operation listing page with in-page inspector |
| `GET` | `/operations/{id}` | operation detail page |
| `GET` | `/tasks` | task listing page with filters and in-page inspector |
| `GET` | `/tasks/{id}` | task detail page |
| `GET` | `/openapi` | OpenAPI page for non-console routes |
| `GET` | `/conf` | redacted runtime configuration page |
| `POST` | `/api/logout` | clear console cookie |
| `GET` | `/api/session` | inspect current console session |
| `GET` | `/api/operations` | list/search operation metadata |
| `GET` | `/api/operations/{id}` | inspect one operation |
| `GET` | `/api/tasks` | list task records |
| `GET` | `/api/tasks/{id}` | inspect one task record |
| `GET` | `/api/status` | combined site, process, and system status |
| `GET` | `/api/openapi` | OpenAPI JSON for non-console routes |
| `GET` | `/api/conf` | redacted runtime configuration JSON |

There are no mutating endpoints in v1. Console cannot run commands, retry or
cancel tasks, fire signals, or control services.

## Assets And Templates

Console pages use the package-owned `vyuh/web/` assets:

```text
/assets/css/vyuh.css
/assets/css/vyuh.<hash>.css
/assets/img/vyuh-logo-transparent.png
/assets/js/console.js
```

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
/console/api/tasks?status=pending&priority_min=10&created_from=2026-06-01&created_to=2026-06-30&limit=50
```

Supported filters:

- `status`: `pending`, `running`, `suspended`, `succeeded`, or `failed`.
- `name`: registered task name.
- `priority_min`: minimum task priority.
- `identity`: task identity.
- `created_from`: inclusive task creation date in `YYYY-MM-DD` format.
- `created_to`: inclusive task creation date in `YYYY-MM-DD` format.
- `q`: text search across name, identity, and last error.
- `limit` and `cursor`: offset-style pagination.

`/api/tasks/{id}` returns the safe task detail shape for one task ID, including
status, attempts, priority, timing, identity, last error, and JSON
payload/state/resume/output/result fields when they parse as JSON.
The HTML task page exposes search, status, name, identity, and date-range
filters and shows selected task details without leaving the list.

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
- Console sessions are in-memory, process-local, and expire after
  `ConsoleConf.session_ttl_seconds`.
- Pagination uses offset cursors in this pass.
- Task listing is inspection-only and does not affect task leasing or retries.
