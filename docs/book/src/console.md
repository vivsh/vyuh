# Console

Vyuh console is a built-in operational UI and JSON API for inspection. It is
enabled by default in debug builds at `/console`, disabled by default in release
builds, isolated from application credentials by a console-only audience and
cookie, and read-only in this pass.

Use it for inspecting registered operations, task records, runtime status,
OpenAPI for application routes, and redacted runtime configuration. Do not use
it as an application admin framework or a command/task execution surface.

## Mental Model

- Console is a built-in operational app mounted at `ConsoleConf.path` when
  `ConsoleConf.enabled` is true.
- `ConsoleConf::default()` enables the console in debug builds and disables it
  in release builds.
- Console auth is two private JWT providers built through the same Vyuh
  authentication runtime and provider-registration path as application providers.
- Console credentials use a console-only audience and cookie selector. Normal
  app credentials do not grant console access, and console credentials cannot
  satisfy application audiences.
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
| `cookie_name` | `vyuh_console` |
| `secure_cookie` | `false` |
| `page_size_default` | `50` |
| `page_size_max` | `250` |
| `status_cache_ttl_seconds` | `5` |

Run `vyuh console-token` to print a short-lived login credential, then open the
console login page. The credential is accepted only at `GET`/`POST /login` and
is exchanged for the normal console cookie. Browser requests for console HTML
pages redirect there on `401`; console JSON APIs keep their normal JSON `401`.
The login credential is stateless and can be reused until it expires.

Applications can create the same short-lived credential when they need to hand
off a user to the console:

```rust
let login = site.console().login_token(user).await?;
let token = login.credentials().access();
```

The login page accepts that value in its form or as `?token=...` for a
short-lived handoff URL. Query credentials can appear in browser history, proxy
logs, and referrers, so use the form when that exposure is unacceptable.

Applications may instead decide who may access the console through their own
authentication flow, then write the standard login response directly:

```rust
async fn open_console(
    site: Site,
    user: AuthUser,
    client_ip: ClientIp,
) -> Result<Response, Error> {
    let login = site.console().login(user, client_ip).await?;
    let mut response = Redirect::to("/console").into_response();
    login.write(&mut response);
    Ok(response)
}
```

The private login provider uses the site-secret JWT key ring, an
`x-vyuh-console-login` header, and a 90-second access token. The private browser
provider uses an HTTP-only cookie valid for 90 minutes and the resolved client
IP as credential binding. It binds to a single valid `X-Forwarded-For` address
when supplied, otherwise to the TCP peer address. Neither provider has a refresh
credential.

Login also creates a readable CSRF cookie. Unsafe console API requests must copy
that value into `X-CSRF-Token`; the bundled console JavaScript does this
automatically.

## Endpoints

All endpoints are mounted under `ConsoleConf.path`.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET`, `POST` | `/login` | render or submit the console token login exchange |
| `GET` | `/` | canonical status overview page |
| `GET` | `/overview` | status overview page |
| `GET` | `/runtime` | formatted site, process, and system runtime page |
| `GET` | `/operations` | operation listing page with in-page inspector |
| `GET` | `/operations/{id}` | operation detail page |
| `GET` | `/tasks` | task listing page with filters and in-page inspector |
| `GET` | `/tasks/{id}` | task detail page |
| `GET` | `/openapi` | OpenAPI page for non-console routes |
| `GET` | `/conf` | redacted runtime configuration page |
| `POST` | `/api/logout` | clear console cookie |
| `GET` | `/api/session` | inspect the current authenticated console identity |
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
- Console authentication is stateless. Login tokens can be reused during their
  90-second lifetime; single-use tokens require durable replay state.
- Pagination uses offset cursors in this pass.
- Task listing is inspection-only and does not affect task leasing or retries.
