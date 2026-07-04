# Response

Vyuh handlers return ordinary Rust values that implement `IntoResponse`.
Prefer the response types re-exported from `vyuh::routes` so response behavior
and OpenAPI metadata stay close to the handler signature.

## Mental Model

- Request wrappers parse input.
- Response wrappers describe output.
- `Data<T>` and `Json<T>` return JSON when `T: Serialize`.
- `Created<T>` and `Accepted<T>` return status-specific JSON.
- `Html<T>` returns HTML.
- `StatusCode`, `NoContent`, redirects, and raw `Response` cover lower-level
  cases.
- OpenAPI response metadata comes from the handler return type unless a route
  patch overrides it.

## JSON

Use `Data<T>` when the response is application data shared with other Vyuh
subsystems:

```rust
use vyuh::prelude::*;

#[derive(Serialize, JsonSchema)]
struct NoteOut {
    id: u64,
    title: String,
}

async fn show_data() -> Data<NoteOut> {
    Data::new(NoteOut {
        id: 1,
        title: "Vyuh".into(),
    })
}
```

Use `Json<T>` for JSON responses:

```rust
use vyuh::prelude::*;

#[derive(Serialize, JsonSchema)]
struct NoteOut {
    id: u64,
    title: String,
}

async fn show() -> Json<NoteOut> {
    Json(NoteOut {
        id: 1,
        title: "Vyuh".into(),
    })
}
```

When `T: JsonSchema`, OpenAPI documents the response body as
`application/json`.

Use `JsonStr` only when the body is already serialized JSON:

```rust
use vyuh::prelude::*;
use vyuh::routes::JsonStr;

async fn raw_json() -> JsonStr {
    JsonStr::from(r#"{"ok":true}"#)
}
```

`JsonStr` does not validate or serialize the string.

Use status-specific JSON wrappers when the status is part of the contract:

```rust
use vyuh::prelude::*;

#[derive(Serialize, JsonSchema)]
struct JobOut {
    id: String,
}

async fn create_job() -> Accepted<JobOut> {
    Accepted(JobOut { id: "job_1".into() })
}
```

`Created<T>` returns `201 application/json`; `Accepted<T>` returns
`202 application/json`.

## HTML

Use `Html<T>` for HTML responses:

```rust
use vyuh::prelude::*;

async fn page() -> Html<&'static str> {
    Html("<h1>Dashboard</h1>")
}
```

For server-side templates, prefer `Templates::html(...)`:

```rust
use vyuh::prelude::*;
use vyuh::templates::{TemplateError, Templates};

async fn dashboard(templates: Templates) -> Result<Html<String>, TemplateError> {
    templates.html("dashboard.html", &serde_json::json!({ "title": "Dashboard" }))
}
```

HTML return metadata is also used by slash policy `Auto` to distinguish page
routes from API routes.

## Status And Empty Responses

Return `StatusCode` when the status is the whole response:

```rust
use vyuh::prelude::*;

async fn accepted() -> StatusCode {
    StatusCode::ACCEPTED
}
```

Use `NoContent` or `()` for empty success responses:

```rust
use vyuh::prelude::*;

async fn delete_note() -> NoContent {
    NoContent
}
```

## Redirects And Headers

Use `TemporaryRedirect` or `PermanentRedirect` when the handler's response is
the redirect itself:

```rust
use vyuh::prelude::*;

async fn old_path() -> PermanentRedirect {
    PermanentRedirect::to("/new-path")
}

async fn temporary() -> TemporaryRedirect {
    redirect::to("/new-path")
}
```

These typed wrappers are reflected in OpenAPI as redirects with a `Location`
header. `redirect::to(...)` is a shortcut for `TemporaryRedirect`;
`redirect::permanent(...)` is a shortcut for `PermanentRedirect`. Use Axum's
`Redirect::to(...)` directly for form POST flows that should return
`303 See Other`; add a route response override when that endpoint is part of a
public spec.

Use `AppendHeaders` or tuple responses when a handler needs custom headers:

```rust
use vyuh::prelude::*;

async fn with_headers() -> (AppendHeaders<[(&'static str, &'static str); 1]>, Json<&'static str>) {
    (AppendHeaders([("cache-control", "no-store")]), Json("ok"))
}
```

## Errors

Handlers can return `Result<T, vyuh::Error>` for ordinary application
failures:

```rust
use vyuh::prelude::*;

async fn show() -> Result<Json<String>, Error> {
    Err(Error::not_found("item not found"))
}
```

Framework errors such as auth, database, template, validation, and extractor
errors, plus application `vyuh::Error` values, normalize into `ErrorReport`
before they are rendered. The site error handler can replace the final body,
status, headers, and content type. Validation `ErrorReport` bodies include
field-oriented `code`, `message`, and `params` entries. See [Errors](errors.md),
[Site](site.md), and [Validation](validation.md).

## Raw Responses

Use `Response` when a route needs full control:

```rust
use vyuh::prelude::*;
use vyuh::routes::Response;

async fn raw() -> Response {
    (StatusCode::CREATED, "created").into_response()
}
```

Raw responses are an escape hatch. Vyuh cannot infer precise OpenAPI response
metadata from an opaque `Response`, so document it with route OpenAPI overrides
when the endpoint is part of a public API.

## OpenAPI

Vyuh infers the primary response from the return type:

| Return type | OpenAPI metadata |
| --- | --- |
| `Data<T>` | JSON response body when `T: JsonSchema` |
| `Json<T>` | JSON response body when `T: JsonSchema` |
| `Created<T>` | `201` JSON response body when `T: JsonSchema` |
| `Accepted<T>` | `202` JSON response body when `T: JsonSchema` |
| `Html<String>` | `text/html` response |
| `TemporaryRedirect` | `307` redirect with `Location` header |
| `PermanentRedirect` | `308` redirect with `Location` header |
| `FileResponse` or `StreamResponse` | binary response body |
| `StatusCode` | empty response; use overrides for exact status docs |
| `NoContent` or `()` | empty success response |
| `Response` | unknown unless patched |

Use [OpenAPI](openapi.md) response overrides for non-`200` success responses,
additional error responses, custom descriptions, or raw responses.

## Examples

The snippets in this chapter show JSON response wrappers, HTML and template
responses, redirects, status-only responses, and OpenAPI response metadata.

## Failure Modes

- Serialization failure for `Data<T>` or `Json<T>` becomes an application
  error.
- Template rendering failure returns `TemplateError` and flows through the
  error pipeline.
- Raw `Response` values are allowed, but Vyuh cannot infer precise OpenAPI
  metadata from them.

## Current Limitations

- Response metadata is inferred from the primary return type unless explicitly
  patched.
- Raw responses require manual OpenAPI metadata for public APIs.
- Content negotiation is application-owned; wrappers choose their content type
  directly.
