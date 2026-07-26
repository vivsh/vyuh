# Routes

Routes are Vyuh's HTTP boundary. A route connects an async handler to a path,
one or more HTTP methods, a stable operation name, and Bundle metadata used by
reverse routing and OpenAPI.

Route macros are convenience syntax over direct registration APIs. The macro
path and the direct API path create the same kind of `BundlePart`.

Use routes for HTTP APIs, pages, webhooks, and browser-facing endpoints. Do not
use routes for maintenance scripts, durable background work, or site-lifetime
workers; use commands, tasks, or services for those.

## Overview

The route pipeline has three parts:

- A handler function defines runtime behavior and typed input/output metadata.
- `RouteConf` defines the route name, path, accepted methods, and optional
  slash behavior.
- A `Bundle` collects routes and composes them with prefixes, middleware, tags,
  and other subsystem registrations.

The `vyuh::routes` module provides Vyuh-owned request wrappers such as
`Json<T>`, `Query<T>`, `Path<T>`, `Form<T>`, and `MultipartForm<T>`. These wrappers are the
recommended route API; Axum remains an internal implementation detail unless an
application explicitly imports Axum types as an escape hatch. See
[Request](request.md) for wrapper behavior and [Validation](validation.md)
for `Valid<E>`.

## Macro Sugar And Direct API

The ergonomic path is the route macro:

```rust
#[bundles::route(path = "/notes")]
async fn list_notes() -> Json<Vec<Note>> {
    Json(Vec::new())
}
```

The equivalent direct API is `bundles::route(handler, RouteConf)` inside
`bundles::bundle(...)`:

```rust
let bundle = bundles::bundle([bundles::route(
    list_notes,
    RouteConf {
        name: Cow::Borrowed("list_notes"),
        path: Cow::Borrowed("/notes"),
        methods: Methods::GET,
        slash: None,
    },
)]);
```

Use the macro for ordinary static routes. Use the direct API when routes are
generated, feature-gated, or assembled conditionally.

## Route Registration

`RouteConf` has four fields:

- `name`: logical route name used by `reverse()`, operation IDs, and diagnostic
  metadata. Macro routes default to the function name.
- `path`: Axum-style absolute path, such as `/notes` or `/notes/{id}`.
- `methods`: a `Methods` filter. Macro routes default to `GET`.
- `slash`: optional route-level trailing-slash behavior.

Paths must start with `/`, must not be empty, and must not contain `//`. Bundle
prefixes follow the same rule and also must not end in `/`.

Multiple HTTP methods can be registered on one handler by repeating
`method = "..."`.

Trailing-slash behavior defaults to the site's `HttpConf`. Override it on a
route when a specific page or API endpoint needs canonical behavior:

```rust
#[bundles::route(path = "/docs/", slash = "redirect_append")]
async fn docs() -> Html<&'static str> {
    Html("docs")
}
```

See [Middlewares](middlewares.md) for `SlashPolicy`, site defaults, bundle
overrides, and API vs HTML behavior.

`Methods` supports `GET`, `POST`, `PUT`, `PATCH`, `DELETE`, `HEAD`, `OPTIONS`,
`TRACE`, and `CONNECT`. `CONNECT` routes can be served, but OpenAPI 3 does not
represent them as operations.

## Handlers

Route handlers are normal async Axum handlers with additional Vyuh metadata
derived from the signature.

Common inputs:

- `Site` is runtime state and is not emitted as an OpenAPI parameter.
- `Path<T>` parses path parameters and contributes path parameter metadata.
- `Query<T>` parses query parameters and contributes query parameter metadata.
- `Json<T>` parses a JSON request body and contributes JSON request-body
  metadata.
- `Form<T>` parses a form request body and contributes form request-body
  metadata.
- `MultipartForm<T>` parses file uploads and contributes `multipart/form-data`
  metadata. See [Uploads](uploads.md).
- `Valid<E>` wraps a request extractor and runs `Validate` after parsing.
- `AuthUser`, `permit!(Role, Variant)`, and `ApiKey` contribute security
  metadata.

Common outputs:

- `Json<T>` becomes an `application/json` response.
- `Html<String>` becomes a `text/html` response.
- `StatusCode` and `()` become empty responses.
- Raw `Response` is allowed but has unknown response metadata unless patched.

For the full response API, see [Response](response.md).

Doc comments become operation text. The first paragraph is the summary;
remaining paragraphs become the description.

## Parsing And Validation

Request wrappers parse only by default:

```rust
use schemars::JsonSchema;
use vyuh::prelude::*;

async fn create(Json(input): Json<CreateUser>) {
    // JSON was parsed, but Validate was not run.
}
```

Validation is an explicit route-boundary choice:

```rust
use schemars::JsonSchema;
use vyuh::prelude::*;

#[derive(Deserialize, JsonSchema, Validate)]
struct CreateUser {
    #[validate(email)]
    email: String,

    #[validate(min_length = 3)]
    name: String,
}

async fn create(Valid(Json(input)): Valid<Json<CreateUser>>) {
    // JSON was parsed, then CreateUser::validate() was run.
}
```

Parse failures return `400` through `ErrorReport`. Validation failures return
`422` through `ErrorReport` with field-oriented `code`, `message`, and `params`
entries.

For the full request API, see [Request](request.md). For
validation rules, nested validation, runtime-only rules, and OpenAPI behavior,
see [Validation](validation.md). For application errors and HTTP rendering, see
[Errors](errors.md).

## Bundles

Routes are registered as `BundlePart` values. Macro routes and direct
`bundles::route(handler, RouteConf)` registration produce the same kind of
bundle part.

Route names must be unique across a composed bundle. A bundle also rejects two
routes with the same path and overlapping HTTP methods.

Reverse routing resolves a registered route name to its final path. Path
parameters are percent-encoded. Missing path arguments return `None`; extra
arguments are ignored.

See [Bundles](bundles.md) for `BundlePart`, `bundle!`, cross-module bundle
organization, validation, composition behavior, and the general patch API.

## Middleware Metadata

Middleware can add runtime behavior and OpenAPI metadata when it implements
`routes::Middleware` and returns a `LayerSpec`. Plain Tower layers can be
wrapped with `routes::layer_from(layer)` when they should not affect OpenAPI
metadata.

Site-wide transport middleware such as request IDs, panic catching, tracing,
compression, CORS, timeouts, body limits, security headers, and slash policy is
configured through `SiteConf::http(...)`; see [Middlewares](middlewares.md).

## Examples

Route registration is demonstrated by the snippets in this chapter and by the
current runnable examples:

```sh
cargo run -p vyuh --example chatroom
cargo run -p vyuh --example console
```

- `chatroom`: HTML route plus channel transport routes.
- `console`: routes, tasks, signals, emitters, commands, OpenAPI, and console
  inspection in one application.

## Failure Modes

Route failures are reported during site build:

- Invalid route paths or prefixes.
- Empty route names.
- Duplicate route names.
- Duplicate path plus overlapping methods.

The macro catches invalid static path and method values at compile time. The
direct API uses the same runtime bundle validation path.

## Best Practices

- Give public routes stable names when callers use `reverse()`.
- Keep route doc comments user-facing because OpenAPI uses them.
- Use direct registration for generated routes, conditional routes, or
  feature-gated route lists.
- Apply `with_prefix` at bundle composition boundaries.

## Current Limitations

- Route registration is explicit; Vyuh does not auto-discover handlers.
- Raw Axum router access is reserved for tests and interop.
- OpenAPI metadata is inferred from handler types unless patched.
