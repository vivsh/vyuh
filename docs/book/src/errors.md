# Errors

Vyuh uses different error shapes for different jobs. Application handlers should
usually return `vyuh::Error`. Subsystems still keep their own error types for
framework machinery, and rendered output is transport-specific.

## Mental Model

| Layer | Type | Use |
| --- | --- | --- |
| Application error | `vyuh::Error` | normal handler failure from routes, commands, tasks, signals, and emitters |
| Subsystem error | `CommandError`, `TaskError`, `SignalError`, `EmitterError`, `SiteError` | parsing, registration, storage, dispatch, startup, and other framework machinery |
| Render input | `ErrorView` | transport-neutral error data passed to JSON, HTML, and command renderers |
| HTTP JSON body | `ErrorReport` | default JSON response body for routes and middleware |
| CLI output | command renderer | human-readable stderr output for command failures |

The important boundary is this: users return `Error` from handler logic, while
Vyuh converts that error into `ErrorView` before rendering. `ErrorReport` is
the default HTTP JSON body, not the universal error abstraction.

## Rendering Pipeline

All rendered errors follow the same normalization path:

```text
vyuh::Error / ValidationReport / subsystem error
        -> ErrorView
        -> JSON renderer / HTML renderer / command renderer
```

`ErrorView` is the shared renderer input:

```rust
pub struct ErrorView {
    pub status: StatusCode,
    pub source: ErrorSourceKind,
    pub kind: ErrorKind,
    pub code: Cow<'static, str>,
    pub message: Cow<'static, str>,
    pub errors: Option<serde_json::Value>,
    pub validation: Option<ValidationReport>,
}
```

Use `ErrorView` when deciding what message to show. Use `ErrorReport` only when
you want Vyuh's default HTTP JSON envelope.

`ErrorReport` may include an optional `debug` object in debug-oriented
configuration/builds. That object is for development diagnostics such as
context, causes, details, or backtrace text. Production error responses should
leave it absent.

## Application Errors

Use `vyuh::Error` for ordinary handler failures:

```rust
use vyuh::prelude::*;

async fn show_user(Data(input): Data<UserLookup>) -> Result<Json<UserOut>, Error> {
    let user = find_user(input.id)
        .await
        .ok_or_else(|| Error::not_found("user not found"))?;
    Ok(Json(user))
}
```

Convenience constructors cover common cases:

```rust
Error::bad_request("invalid input")
Error::not_found("record not found")
Error::invalid("business rule failed")
Error::conflict("version conflict")
Error::unavailable("upstream unavailable")
Error::other(err)
Error::wrap(ErrorKind::Unavailable, err)
```

Use `with_context(...)` to add operator-facing detail while preserving the
machine-readable kind.

## Subsystem Errors

Subsystem errors describe framework mechanics:

- `CommandError`: unknown commands, unknown flags, help rendering, unsupported
  command schemas, command argument parsing, and command rendering.
- `TaskError`: task store, lease, migration, serialization, and retry machinery.
- `SignalError` and `EmitterError`: registration, dispatch, source setup, and
  source execution machinery.
- `SiteError`: configuration, build, startup, and shutdown lifecycle failures.

Application code inside those handlers should still return `Error`:

```rust
use vyuh::prelude::*;

async fn command(Data(args): Data<ReindexArgs>) -> Result<(), vyuh::Error> {
    reindex(args.full).await.map_err(Error::other)
}
```

## Validation

Validation failures are structured reports. `Valid<E>` runs `Validate` after
the inner data wrapper parses or extracts successfully:

```rust
use vyuh::prelude::*;

async fn create(Valid(Data(input)): Valid<Data<CreateUser>>) -> Result<(), Error> {
    Ok(())
}
```

Routes convert validation failures into a `422` `ErrorView`, then render the
view as JSON, HTML, or any custom HTTP response. Commands render the same
field-oriented report as CLI output:

```text
Validation failed for command 'create-user':

  --email
    Enter a valid email address.

Use 'create-user --help' for usage.
```

See [Validation](validation.md) for the full `code`, `message`, and `params`
error object.

## HTTP Errors

Routes, middleware, extractors, auth, database, template, validation, and
application failures normalize into `ErrorView` before a response is rendered.
Multipart upload parse, MIME screening, and size-limit failures use the same
pipeline.
The default JSON renderer turns that view into `ErrorReport`.

Upload-specific status codes follow the same model: malformed multipart returns
`400`, unsupported declared or sniffed file type returns `415`, oversized
uploads return `413`, and upload validation returns `422`.

Applications can customize JSON and HTML rendering separately:

```rust
use vyuh::prelude::*;
use vyuh::errors::{ErrorConf, HttpErrorRenderMode};

let conf = SiteConf::default().errors(
    ErrorConf::default()
        .json(|ctx, view| async move {
            (
                view.status,
                Json(serde_json::json!({
                    "code": view.code,
                    "message": "Custom JSON message",
                    "path": ctx.path,
                    "errors": view.errors,
                })),
            )
                .into_response()
        })
        .html(|ctx, view| async move {
            (
                view.status,
                Html(format!("<h1>{}</h1><p>{}</p>", view.status, view.message)),
            )
                .into_response()
        })
        .http_mode(HttpErrorRenderMode::Auto),
);
```

`HttpErrorRenderMode::Auto` uses JSON when the request is clearly JSON: the
request `Content-Type` is `application/json` or `*+json`, or `Accept` explicitly
asks for a JSON media type. Otherwise it renders HTML, including normal browser
requests, forms, multipart uploads, missing `Accept`, and `Accept: */*`.

Use `Json` or `Html` to force one renderer for all HTTP errors. A custom
`handler(...)` still replaces HTTP error rendering entirely.

Renderer inputs are request-aware. JSON and HTML renderers receive
`ErrorRequestContext`, which includes method, URI, path, and headers:

```rust
use vyuh::prelude::*;
use vyuh::errors::ErrorConf;

ErrorConf::default().json(|ctx, view| async move {
    (
        view.status,
        Json(serde_json::json!({
            "code": view.code,
            "message": view.message,
            "path": ctx.path,
        })),
    )
        .into_response()
})
```

The lower-level `handler(|ctx, report| ...)` hook remains available when an
application wants to replace the final HTTP response from the default
`ErrorReport` directly.

## Command Errors

`Site::run` builds the site, runs a command when one is supplied, and renders
command failures for a terminal. With no command arguments it runs the built-in
`serve` command:

- help output goes to stdout and succeeds;
- unknown command, unknown flag, missing argument, parse failure, validation
  failure, and handler failure return a terminal diagnostic and non-zero status;
- handler `Error` values retain their complete causal chain for terminal output.

Command parsing and help generation remain `CommandError` concerns. Handler
logic should return `Error`. The terminal renderer preserves that error's
context and causes, including native Mool, Gaman, and SQLx diagnostics. HTTP
responses continue to use the normal redacted error view.

Customize command output separately from HTTP rendering:

```rust
use vyuh::prelude::*;
use vyuh::errors::ErrorConf;

let conf = SiteConf::default().errors(
    ErrorConf::default().command(|ctx, view| {
        if view.validation.is_some() {
            format!("{} failed validation. Run '{} --help'.", ctx.command, ctx.command)
        } else {
            format!("{} failed: {}", ctx.command, view.message)
        }
    }),
);
```

Command renderers receive `ErrorCommandContext`, which includes the command name
and raw command arguments. `view.message` contains the complete terminal error
chain for handler failures. Renderers return the diagnostic carried by the
command failure; the application entrypoint decides how to write it.

Commands do not render `ErrorReport`; command output is terminal text.

## Task Errors And Retry

Task retry is explicit. A task handler returning `Err(Error)` marks the task as
failed terminally. Vyuh does not infer retry behavior from `ErrorKind`.

Return `TaskState::retry(...)` when work should be retried:

```rust
use vyuh::prelude::*;

async fn send_email(Data(job): Data<EmailJob>) -> Result<TaskState<String>, Error> {
    match deliver(&job).await {
        Ok(()) => Ok(TaskState::complete("sent".to_string())?),
        Err(err) if err.is_transient() => Ok(TaskState::retry(
            Some(std::time::Duration::from_secs(60)),
            err.to_string(),
        )),
        Err(err) => Err(Error::unavailable(err.to_string())),
    }
}
```

This keeps retry policy visible in task code instead of hiding it in a broad
error classification.

## ErrorKind Mapping

`ErrorKind` carries the broad class of failure. HTTP routes map it to status
codes when converting into `ErrorReport`:

| Kind | HTTP Status |
| --- | --- |
| `BadRequest` | `400` |
| `Unauthorized` | `401` |
| `Forbidden` | `403` |
| `NotFound` | `404` |
| `Conflict` | `409` |
| `Integrity` | `409` |
| `Invalid` | `422` |
| `RateLimited` | `429` |
| `Unavailable` | `503` |
| `Other` | `500` |

Commands do not render `ErrorReport`; they render terminal text. Tasks do not
retry from `ErrorKind`; they use `TaskState::retry(...)`.
