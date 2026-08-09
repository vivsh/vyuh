# OpenAPI

OpenAPI is generated from Vyuh route metadata. Route configuration, handler
signatures, doc comments, middleware metadata, and explicit patches are combined
into an OpenAPI 3 spec at site build time.

OpenAPI is a first-class subsystem. It is commonly used with routes, but it has
its own configuration, schema conversion, response metadata, and override APIs.

## Overview

OpenAPI generation uses these inputs:

- `RouteConf` supplies the path, route name, and HTTP methods.
- Handler arguments supply path, query, body, and ignored state metadata.
- Handler return types supply response body, content type, and default status
  metadata.
- Doc comments supply operation summary and description.
- `PatchOp` overrides names, descriptions, argument metadata, response metadata,
  status codes, and extra responses.
- `AuthUser`, `Permit<ScopeRule>`, and validation wrappers contribute security and
  error response metadata.
- `OpenApiConf` controls the generated spec endpoint and API metadata.

Vyuh emits OpenAPI 3.0.3 by default for broad tooling compatibility. Use
`.openapi_version(bundles::OpenApiVersion::V31)` when a project wants an
OpenAPI 3.1 document and its client/documentation tooling supports it.

Vyuh's UUID-backed `OperationId` is the canonical runtime identity used by
`site.operations()` and handler extraction. It is separate from OpenAPI's
configurable string `operationId`; multi-method OpenAPI suffixes never create
additional runtime operations.

`Permit<R>` contributes deterministic `x-vyuh-scopes` metadata with an `all`
or `any` mode. Application scopes stay separate from OAuth authorization-server
scope requirements in the standard security scheme.

## Registration

OpenAPI is registered on a `Bundle` with `with_openapi`. By default, Vyuh
serves only the JSON spec:

```rust
let bundle = routes.with_openapi(
    bundles::OpenApiConf::default()
        .title("Notes API")
        .version("0.1.0"),
);
```

Use `.spec(...)` to place the JSON spec under the same prefix as the API it
describes:

```rust
let bundle = routes.with_openapi(
    bundles::OpenApiConf::default()
        .title("Notes API")
        .version("0.1.0")
        .openapi_version(bundles::OpenApiVersion::V30)
        .description("Notes service API")
        .spec("/api/openapi.json"),
);
```

Add `.viewer(...)` when the site should also serve an HTML documentation UI.
Swagger UI is the default viewer:

```rust
let bundle = routes.with_openapi(
    bundles::OpenApiConf::default()
        .spec("/api/openapi.json")
        .viewer("/api/docs"),
);
```

Use `.viewer_with(...)` to choose another built-in viewer:

```rust
let bundle = routes.with_openapi(
    bundles::OpenApiConf::default()
        .spec("/api/openapi.json")
        .viewer_with("/api/docs", bundles::DocViewer::Redoc),
);
```

The JSON spec is generated during site build. Schema conversion or serialization
errors fail startup instead of failing on the first documentation request.

## Order Sensitivity

`with_openapi` snapshots the route operations that are already registered in the
bundle. Routes added or merged after `with_openapi` will not appear in that
generated spec.

Register OpenAPI after all route registration and bundle merge steps for the API
surface the spec should describe. Prefixes and metadata applied to already
captured routes still affect the final generated paths and operation metadata.

See [Bundles](bundles.md) for bundle composition rules and the general
order-sensitive behavior of bundle-level APIs.

## Schemas

Request and response schemas come from `JsonSchema` types used in extractors and
returns:

```rust
use schemars::JsonSchema;
use vyuh::prelude::*;

#[derive(Deserialize, JsonSchema)]
struct CreateNote {
    title: String,
}

#[derive(Serialize, JsonSchema)]
struct Note {
    id: i64,
    title: String,
}
```

`Json<CreateNote>` is emitted as an `application/json` request body.
`Json<Note>` is emitted as an `application/json` response body. Shared schemas
are emitted into OpenAPI components when schemars produces reusable definitions.
For database-backed pagination, `Json<routes::Page<Note>>` emits Mool's
`items`, `total`, `page`, `per_page`, and `total_pages` response schema.

## Validation Metadata

Validation metadata is opt-in at the route boundary. Deriving `Validate` on a
type does not automatically add validation constraints to every route that uses
that type.

Plain wrappers document parse shape only:

```rust
use schemars::JsonSchema;
use vyuh::prelude::*;

async fn create(Json(input): Json<CreateNote>) {
    // OpenAPI uses the plain JsonSchema for CreateNote.
}
```

`Valid<E>` documents supported validation constraints and runs runtime
validation:

```rust
use schemars::JsonSchema;

#[derive(Deserialize, JsonSchema, Validate)]
struct CreateNote {
    #[validate(min_length = 3)]
    title: String,
}

async fn create(Valid(Json(input)): Valid<Json<CreateNote>>) {
    // OpenAPI includes minLength for title, and runtime validation returns 422.
}
```

Vyuh emits only constraints that can be represented accurately in OpenAPI, such
as string length, numeric ranges, formats, patterns, collection sizes, enum
values, and explicit custom validator hints. Runtime-only validators such as
`custom` remain enforcement logic only unless they opt in with
`custom_schema = "name"`, which emits `x-vyuh-validators` vendor metadata for
clients.

See [Validation](validation.md) for the full validation model.

## Response Metadata

Vyuh infers the primary response from the handler return type. `PatchOp` can
override the inferred response status and description through the direct API:

```rust
bundles::route(create_note, conf).patch(
    PatchOp::new()
        .ret()
        .status(201)
        .doc("Created note")
        .done(),
)
```

The same response override can be written on the route macro:

```rust
#[bundles::route(
    path = "/notes",
    method = "POST",
    returns(status = 201, description = "Created note")
)]
async fn create_note(Json(input): Json<CreateNote>) -> Json<Note> {
    Json(Note {
        id: 1,
        title: input.title,
    })
}
```

Additional responses are appended with `PatchOp::append()`:

```rust
PatchOp::new()
    .append()
    .status(409)
    .typed::<Json<ApiError>>()
    .doc("Title already exists")
    .done()
```

Equivalent macro syntax uses `returns(ty = "...")` for appended response
metadata:

```rust
#[bundles::route(
    path = "/notes",
    method = "POST",
    returns(status = 201, description = "Created note"),
    returns(ty = "Json<ApiError>", status = 409, description = "Title already exists")
)]
async fn create_note(Json(input): Json<CreateNote>) -> Json<Note> {
    Json(Note {
        id: 1,
        title: input.title,
    })
}
```

This is useful for documented error responses, alternate success responses, and
handlers returning raw `Response`.

Common response wrappers document their status and content type directly:

```rust
use schemars::JsonSchema;
use vyuh::prelude::*;

#[derive(Serialize, JsonSchema)]
struct Note {
    id: i64,
}

async fn create_note() -> Created<Note> {
    Created(Note { id: 1 })
}

async fn accepted_note() -> Accepted<Note> {
    Accepted(Note { id: 1 })
}

async fn moved() -> PermanentRedirect {
    PermanentRedirect::to("/notes")
}

async fn shortcut_redirect() -> TemporaryRedirect {
    redirect::to("/notes")
}
```

`Created<T>` is documented as `201 application/json`, and `Accepted<T>` as
`202 application/json`. `NoContent` and `()` are documented as `204`.
`TemporaryRedirect`, `PermanentRedirect`, `redirect::to(...)`, and
`redirect::permanent(...)` are documented as redirects with a `Location` header.
`FileResponse` and `StreamResponse` are documented as binary responses.

Raw `Response` remains allowed, but Vyuh cannot infer its real status, content
type, headers, or schema. Use response overrides when raw responses are part of
the public API.

Multipart upload routes document typed upload contracts. `MultipartForm<T>`
emits `multipart/form-data`; file fields are binary; repeated file fields are
arrays of binary items; and `MultipartData` rules such as allowed content types,
extensions, max bytes, sniffing, and multiple files are exposed through
OpenAPI encoding metadata and `x-vyuh-upload-*` extensions.

## Error Responses

Vyuh documents common framework errors from handler metadata:

- request inputs imply `400 Bad Request`;
- `Valid<T>` inputs imply `422 Unprocessable Entity`;
- auth inputs such as `AuthUser` and `Permit<ScopeRule>` imply safe `401`, `403`,
  `500`, and provider-unavailable `503` responses.

These responses are explicit operation metadata contributed by the argument
wrappers, not generator guesswork. They use Vyuh's standard `ErrorReport` body.
`500` is not added to unrelated public operations. Authentication extractors
include it because framework or provider configuration can fail independently
of an invalid credential.

## Security

Public endpoints need no annotation. If a handler has no auth extractor, the
operation has no OpenAPI security requirement.

Authenticated endpoints are inferred from handler arguments:

```rust
use vyuh::prelude::*;
use vyuh::auth::AuthUser;

async fn me(user: AuthUser) -> Json<String> {
    Json(user.key)
}
```

`AuthUser` contributes one OpenAPI alternative for each configured provider,
with a scheme derived from that provider's access-credential location. JWT,
PASETO, BRANCA, and custom token codecs contribute their `bearerFormat`; opaque
`AuthKey` providers use OpenAPI `apiKey`. `AuthUser` routes also carry their
effective bundle audience as `x-vyuh-audience` metadata. Unsafe operations that
may authenticate through a cookie carry `x-vyuh-csrf-header` metadata.
When a route omits `.with_audience(...)`, this extension contains the site's
configured default audience; strict explicit-audience mode rejects that route
instead.

Login methods are registered once through `AuthConf::method`. Application-owned
route bundles do not repeat the selected method as OpenAPI-only metadata.
`BasicCredentials` contributes an HTTP Basic input scheme, and
`LoginResponse<T>` contributes the selected response-body schema. Applications
can use the ordinary response override APIs when they want to document a
specific MFA challenge or OIDC redirect route more narrowly.

## Argument Overrides

Argument names and descriptions are usually extracted from the handler. `PatchOp`
can adjust argument metadata by position through the direct API:

```rust
PatchOp::new()
    .arg(0)
    .name("id")
    .doc("Note id")
    .done()
```

The same override can be written on the route macro:

```rust
#[bundles::route(
    path = "/notes/{id}",
    arg(pos = 0, name = "id", ty = "i64", description = "Note id")
)]
async fn get_note(Path(id): Path<i64>) -> Json<Note> {
    Json(Note {
        id,
        title: "example".to_string(),
    })
}
```

The patch applies only to metadata. Runtime extraction still follows the handler
signature.

## Middleware Metadata

Middleware that implements `routes::Middleware` can return a `LayerSpec`. Layer
parts contribute OpenAPI parameters to every operation in the layered bundle.
`routes::layer_from(layer)` applies a Tower layer without OpenAPI metadata.

## Examples

OpenAPI is demonstrated by the snippets in this chapter and by the console
example, which exposes OpenAPI output for the registered application routes:

```sh
cargo run -p vyuh --example console
```

The standalone route and response snippets above show spec registration,
response overrides, documented error responses, and custom error schemas.

## Failure Modes

OpenAPI failures are reported during site build:

- Unsupported schema conversion.
- JSON serialization failure for the generated spec.
- Hidden OpenAPI routes colliding with existing route paths.

`CONNECT` routes can be served by Vyuh but are not represented as OpenAPI
operations because OpenAPI 3 does not model `CONNECT`.

## Best Practices

- Keep handler doc comments user-facing.
- Use `PatchOp` for non-200 success statuses and documented error responses.
- Prefer concrete request and response structs that derive `JsonSchema`.
- Keep spec endpoints under the same prefix as the API they describe.
