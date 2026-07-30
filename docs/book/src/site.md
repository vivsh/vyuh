# Site

`Site` is the built Vyuh application. It owns configuration, the router,
database pool, authenticator, template engine, task dispatcher, signal and
emitter engines, services, commands, logging, and shutdown coordination.

Most applications interact with `Site` in two places:

- At startup, through `Site::build`, `Site::run`, `Site::serve`, or
  `Site::test`.
- Inside handlers and workers, where `Site` or subsystem handles can be
  extracted when framework access is needed.

## Overview

The main public pieces are:

- `SiteConf` for application configuration.
- `Site::run(conf, bundle)` as the standard application entrypoint.
- `Site::build(conf, bundle)` for inert site assembly.
- `Site::serve(conf, bundle)` for advanced direct HTTP serving.
- `Site::test(conf, bundle, pool)` for inert tests with an explicit SQLx pool.
- `site.start()` for serving an already-built site.
- `Site` accessors such as `db()`, `tasks()`, `templates()`, `service()`,
  `auth()`, `signals()`, `routes()`, and `operations()`.
- `vyuh::testing::router(&site)` for tests or Axum interop.
- `SiteConf::http(...)` for global HTTP middleware and slash behavior.
- `SiteConf::templates(...)` for Minijinja environment behavior.

`Site` is cheap to clone. Clones share the same underlying application state.

## Configuration

Start from `SiteConf::default()` and set only what the application needs:

```rust
use vyuh::prelude::*;
use vyuh::db::DbConf;
use vyuh::console::ConsoleConf;
use vyuh::file_storage::UploadConf;
use vyuh::middlewares::{HttpConf, TraceConf};
use vyuh::templates::{TemplateConf, TemplateDateFormats};

let conf = SiteConf::default()
    .host("127.0.0.1")
    .port(8080)
    .project_dir(".")
    .database(DbConf::from_url("sqlite://app.db?max=5")?)
    .secret_key("replace-with-a-long-random-secret")
    .templates(TemplateConf {
        date_formats: TemplateDateFormats {
            date: "%d %b %Y".into(),
            time: "%H:%M".into(),
            datetime: "%d %b %Y, %H:%M".into(),
        },
        ..TemplateConf::default()
    })
    .http(HttpConf {
        trace: TraceConf { enabled: true },
        ..HttpConf::default()
    })
    .uploads(UploadConf {
        dir: "media/uploads".into(),
        base_url: Some("/media/uploads".into()),
        ..UploadConf::default()
    })
    .console(ConsoleConf::default())
    .timezone("UTC");
```

`project_dir` is the base for relative media, upload, reload, auth key, and log
paths. Static files and templates belong to bundles through asset dirs.
`SiteConf::validate()` checks required fields and path readability before the
site is built.

With no database backend feature enabled, `SiteConf::default()` uses a shared
in-memory SQLite database URL and tasks use `MemoryTaskStore`. This is intended
for quick starts, docs, local experiments, and tests. Production applications
should enable exactly one backend feature (`postgres`, `mysql`, or `sqlite`) and
configure a durable database.

For global HTTP behavior, see [Middlewares](middlewares.md). For Minijinja
environment behavior and formatting helpers, see [Templates](templates.md).
For upload storage, see [Uploads](uploads.md).
For optional operational inspection, see [Console](console.md).

Environment helpers are available when configuration should come from the
process environment:

```rust
let conf = vyuh::SiteConf::from_env_with_files()?;
```

`from_env_with_files()` loads `.env`, then `.env.test`, `.env.dev`, or
`.env.prod` depending on the build mode. Environment variables currently patch
common deployment fields such as `DATABASE_URL`, `SECRET_KEY`, `HOST`, `PORT`,
`TZ`, and `LOG_INIT`.

## Lifecycle

Vyuh keeps lifecycle on `Site`:

| Method | Purpose |
| --- | --- |
| `Site::build` | assemble an inert site without runtime workers or HTTP |
| `Site::run` | standard command-aware entrypoint; no args defaults to `serve` |
| `Site::serve` | advanced direct path that starts runtime workers and HTTP |
| `Site::test` | assemble an inert site with an explicit SQLx pool |
| `site.start` | start runtime workers and serve an already-built site |

Use `Site::run` for ordinary application binaries:

```rust
use vyuh::prelude::*;

#[tokio::main]
async fn main() -> Result<(), vyuh::SiteError> {
    let bundle = bundles::bundle! {
        // routes, services, tasks, signals, assets, commands
    };

    vyuh::Site::run(SiteConf::from_env_with_files()?, bundle).await
}
```

Use `Site::serve` when a binary should ignore commands and only serve HTTP:

```rust
use vyuh::prelude::*;

Site::serve(SiteConf::from_env_with_files()?, app_bundle()).await?;
```

Use `Site::build` when the caller needs the site before serving, for example to
inspect configuration, run setup code, or pass the built site to another
runtime:

```rust
let site = vyuh::Site::build(conf, bundle).await?;
site.start().await?;
```

When arguments are supplied, `Site::run` executes the requested command:

```rust
#[tokio::main]
async fn main() -> Result<(), vyuh::SiteError> {
    vyuh::Site::run(vyuh::SiteConf::from_env_with_files()?, app_bundle()).await
}
```

During build, Vyuh validates configuration and bundles, builds the router,
creates the database pool, loads templates, constructs service facades,
registers OpenAPI endpoints, and prepares task stores. It does not start task
workers, emitters, PgNotify listeners, or service workers.

The runtime starts only through `serve` or `site.start()`. Vyuh binds the HTTP
listener first, then starts background engines, then begins serving requests.
One-shot commands therefore retain database and service access without
generating scheduled signals or consuming durable work.

## Using Site In Handlers

Handlers can extract `Site` directly:

```rust
use vyuh::prelude::*;

#[bundles::route(path = "/health")]
async fn health(site: Site) -> Json<String> {
    Json(site.timezone().to_string())
}
```

Prefer subsystem handles for subsystem-specific work:

```rust
let db = site.db();
let templates = site.templates();
let tasks = site.tasks();
let auth = site.auth();
let routes = site.routes();
let operations = site.operations();
let counter = site.service::<CounterService>()?;
```

Task submission should go through `site.tasks().submit(...)` or
`site.tasks().submit_with(...)`. Template rendering should usually go through
`site.templates().render(...)` or the `Templates` route extractor.

## Error Rendering

Route parse errors, validation errors, auth failures, database errors, template
errors, and application `vyuh::Error` values are normalized into `ErrorReport`
before they are rendered. In automatic mode, JSON requests receive JSON errors
and ordinary browser/form requests receive HTML errors. See [Errors](errors.md)
for the application/subsystem/rendered error model.

Applications can replace error rendering with `SiteConf::errors(...)`:

```rust
use vyuh::prelude::*;
use vyuh::errors::ErrorConf;

let conf = SiteConf::default().errors(
    ErrorConf::default().handler(|ctx, report| async move {
        (
            report.status,
            [("content-type", "application/json")],
            serde_json::json!({
                "path": ctx.path,
                "code": report.code,
                "detail": report.detail,
            })
            .to_string(),
        )
            .into_response()
    }),
);
```

The handler is async and receives request context plus the normalized report, so
applications can render templates, add headers, or choose a different content
type.

## Routing And Reverse URLs

Raw Axum router access is intentionally not part of the normal application
lifecycle. Use `Site::serve` or `site.start()` for serving. Use
`vyuh::testing::router(&site)` only for tests or interop that truly needs an Axum
`Router`.

Named routes are reversed through the site route facade:

```rust
let url = site.routes().reverse_url(
    "user_detail",
    &[("id", "42")],
);
```

`reverse_url` returns `None` when the route name or required parameters do not
match a registered route. Resolve a URL to its method-specific runtime
operation through the same facade:

```rust
let id = site.routes().resolve_url(
    HttpMethod::GET,
    "/users/42?tab=profile",
);

let operation = id.and_then(|id| site.operations().find(id));
```

`site.operations().list()` iterates over all operation metadata without
allocating. Hidden framework operations are included and can be filtered with
`Operation::hidden`.

## Testing

Use `Site::test` when a test should build the real site with a caller-provided
SQLx pool:

```rust
#[sqlx::test]
async fn route_works(pool: vyuh::db::Pool) -> Result<(), vyuh::SiteError> {
    let site = vyuh::Site::test(vyuh::SiteConf::default(), app_bundle(), pool).await?;
    let app = vyuh::testing::router(&site);
    Ok(())
}
```

For route-level tests, use `vyuh::testing::test_site` to provision a complete
`TestSite`, or build a site and send requests through
`vyuh::testing::TestSite::new(site)` or `vyuh::testing::router(&site)`. Use
`.log_init(false)` in tests when test output should stay quiet.

`TestSite` is inert by default. Tests that deliberately cover task workers,
emitters, PgNotify listeners, or service workers opt in explicitly:

```rust,ignore
let site = vyuh::testing::TestSite::new(site);
site.start_runtime().await?;
```

`test_site` owns Mool's isolated database and applies registered migrations:

```rust,ignore
let site = vyuh::testing::test_site(conf, app_bundle()).await?;
site.get("/api/posts").send().await.assert_ok();
site.teardown().await?;
```

For a database shared by multiple test sites, retain Mool's `TestDatabase` in
the fixture and use `TestSite::from_pool`; shut down every site before calling
the fixture's `teardown`.

For an owned integration fixture, `#[vyuh::test]` is thin syntax sugar over
`TestSite`. Its body always receives `&TestSite`, and the generated wrapper
shuts down the site and tears down the Mool database even when the body fails:

```rust,ignore
#[vyuh::test(conf = test_conf, bundle = app_bundle)]
async fn public_posts_hide_drafts(
    site: &vyuh::testing::TestSite,
) -> Result<(), TestError> {
    site.get("/api/posts").send().await.assert_ok();
    Ok(())
}
```

With no arguments the macro uses `SiteConf::default()` and an empty
`Bundle::default()`; it never discovers application bundles automatically. Use
`migrations = false` for schema-negative or migration-command tests. Shared
database fixtures continue to use `TestSite::from_pool` directly.

## Shutdown

`Site` owns a shared shutdown notifier. Long-lived service workers and other
background loops should observe `site.shutdown_notifier()` and exit when it is
notified.

```rust
let shutdown = site.shutdown_notifier();
tokio::select! {
    _ = shutdown.notified() => {}
    _ = do_work() => {}
}
```

`Site::serve` and `site.start()` install bounded graceful server shutdown. The
first `Ctrl+C` starts graceful shutdown and prints a message explaining that a
second `Ctrl+C` will force shutdown. `SIGTERM`, touch-reload, and
`site.shutdown()` also start graceful shutdown. If active requests do not drain
within `conf.http.shutdown.grace_period_ms`, Vyuh forces server shutdown and
returns from `Site::serve` or `site.start()`.

Channel transports are shutdown-aware: SSE streams end, WebSockets close, and
long-poll requests return promptly when shutdown starts. Long-lived service
workers should still select on `site.shutdown_notifier()` so they can stop
their own work cleanly before the grace period expires.

`shutdown_and_wait()` can be used by tests or embedding code that needs to
notify active background tasks and abort remaining join handles.

## Failure Modes

- Invalid configuration returns `SiteError::ConfError`.
- Database pool setup returns `SiteError::DatabaseError`.
- Bundle validation and duplicate registration errors return `SiteError::BundleError`.
- Template loading errors return `SiteError::TemplateError`.
- Service construction errors return `SiteError::ServiceError`.
- Task store migration errors return `SiteError::TaskMigrationError`.
- Server bind or runtime errors return `SiteError::IOError` or
  `SiteError::ServeError`.

## Current Limitations

- `Site` is an in-process application handle, not a distributed coordinator.
- Background engines are tied to the process that serves the site.
- `Site::test` uses the supplied pool but does not replace application-level
  schema setup; tests still need the schema their routes and services expect.
