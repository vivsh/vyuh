# Request

Vyuh request data wrappers are the normal way to receive route input. They keep
Axum extractor details behind Vyuh's API boundary and make parsing, errors, and
OpenAPI metadata consistent across routes.

Vyuh wrappers are intentionally thin. They delegate parsing behavior to Axum
internally, then convert failures into Vyuh's `ErrorReport` shape. Wrapping the
extractors gives Vyuh consistent errors, first-class OpenAPI integration, and a
stable public API even if the internal Axum integration changes.

All parse failures become `ErrorReport` values and are rendered through the
site's configured error handler.

## Mental Model

- Request wrappers parse incoming data.
- Request wrappers never validate.
- `Valid<E>` adds validation.
- Parse failures return `400`.
- Validation failures return `422`.

The route lifecycle is:

```text
Request -> Wrapper Parse -> Valid (optional) -> Handler -> Response
```

Use these from `vyuh::routes`:

| Wrapper | Source | Parse failure | OpenAPI behavior |
| --- | --- | --- | --- |
| `Data<T>` | JSON body | `400` | JSON request body or response body |
| `Json<T>` | JSON body | `400` | JSON request body or response body |
| `Query<T>` | query string | `400` | query parameters |
| `Path<T>` | path captures | `400` | path parameters |
| `Form<T>` | URL-encoded form body | `400` | form request body |
| `MultipartForm<T>` | multipart form body | `400`, `413`, `415` | multipart form request body |
| `BodyBytes` | raw body bytes | `400` | opaque binary request body |

Wrappers parse only, even when the DTO derives `Validate`. Validation runs only
when a wrapper is wrapped in `Valid<E>`. See [Validation](validation.md).

Use `Data<T>` when the input is application data rather than explicitly
HTTP-shaped JSON. It behaves like `Json<T>` for routes, but the same wrapper is
also used by commands, tasks, signals, and emitters.

```rust
use schemars::JsonSchema;
use vyuh::prelude::*;

#[derive(Deserialize)]
struct CreateNote {
    title: String,
}

async fn create(Data(input): Data<CreateNote>) {
    // input is Arc<CreateNote>; field access works through Deref.
}
```

## JSON

`Json<T>` parses an `application/json` request body:

```rust
use vyuh::prelude::*;

#[derive(Deserialize)]
struct CreateNote {
    title: String,
}

async fn create(Json(input): Json<CreateNote>) {
    // input is parsed, but not validated.
}
```

`Json<T>` can also be returned from handlers when `T: Serialize`:

```rust
use vyuh::prelude::*;

#[derive(Serialize)]
struct NoteOut {
    id: u64,
}

async fn show() -> Json<NoteOut> {
    Json(NoteOut { id: 1 })
}
```

Invalid JSON or a body that cannot deserialize into `T` returns `400`.

## Query

`Query<T>` parses query strings:

```rust
use vyuh::prelude::*;

#[derive(Deserialize)]
struct SearchParams {
    q: String,
    page: Option<u32>,
}

async fn search(Query(params): Query<SearchParams>) {
    // /search?q=vyuh&page=2
}
```

Malformed query strings or failed deserialization return `400`.

Unknown query parameters follow Serde deserialization behavior. They are
ignored for ordinary structs, and rejected when the target type opts into
`#[serde(deny_unknown_fields)]`.

## Path

`Path<T>` parses path captures. Use a struct when names matter:

```rust
use vyuh::prelude::*;

#[derive(Deserialize)]
struct UserPath {
    id: uuid::Uuid,
}

#[vyuh::bundles::route(path = "/users/{id}")]
async fn user_detail(Path(path): Path<UserPath>) {
    let id = path.id;
}
```

Use a tuple for positional captures:

```rust
use vyuh::prelude::*;

#[vyuh::bundles::route(path = "/orgs/{org}/users/{id}")]
async fn user_in_org(Path((org, id)): Path<(String, u64)>) {
    // org and id are parsed from the path.
}
```

Path parse failures return `400`.

## Form

`Form<T>` parses `application/x-www-form-urlencoded` request bodies:

```rust
use vyuh::prelude::*;

#[derive(Deserialize)]
struct LoginForm {
    email: String,
    password: String,
}

async fn login(Form(form): Form<LoginForm>) {
    // form.email, form.password
}
```

Form parse failures return `400`.

## Multipart

`MultipartForm<T>` parses `multipart/form-data` upload requests. It is route
only because multipart is an HTTP request shape with file streaming, temporary
files, content types, and request limits.

```rust
use vyuh::prelude::*;
use vyuh::routes::{MultipartForm, UploadedFile};
use vyuh::MultipartData;
use schemars::JsonSchema;

#[derive(JsonSchema, MultipartData)]
struct AvatarUpload {
    #[upload(content_types = ["image/png"], extensions = ["png"], sniff = "image")]
    avatar: UploadedFile,
}

async fn upload(MultipartForm(input): MultipartForm<AvatarUpload>) {
    // input.avatar is parsed and screened, but not validated by Validate.
}
```

Use `MultipartMap` and `MultipartSpec` directly when macro-less dynamic upload
handling is a better fit. MIME screening uses the `infer` crate to check a
bounded prefix of uploaded bytes, so invalid files can be rejected before Vyuh
accepts or stores the full upload. Large files stream to memory or temporary
files according to `SiteConf::uploads(...)`.

Save accepted files through `site.file_storage()`. Uploaded/runtime files are
separate from bundled assets and `collect_assets`.

See [Uploads](uploads.md) for typed uploads, macro-less uploads, large-file
behavior, MIME sniffing, and `LocalStorage`.

## Raw Body

`BodyBytes` reads the request body as bytes. It is useful for webhooks because
signature verification usually needs the exact raw payload:

```rust
use vyuh::prelude::*;

async fn webhook(BodyBytes(bytes): BodyBytes) {
    // Verify the provider signature against the raw bytes before decoding.
}
```

Use it for custom protocols, signed payloads, or cases where deserialization is
not appropriate. `BodyBytes` is intentionally excluded from the validation
system because it represents raw request data.

`BodyBytes` does not generate a JSON schema. In OpenAPI it is documented as an
opaque binary request body.

## Ownership Helpers

Wrappers support pattern matching, `Deref`, `AsRef`, and `into_inner()`:

```rust
use vyuh::prelude::*;

async fn create(json: Json<CreateNote>) {
    let input = json.into_inner();
}
```

`Valid<E>` supports the same ownership pattern around the wrapped extractor.

## Validation

Use `Valid<E>` when parsed input should be validated:

```rust
use vyuh::prelude::*;

async fn create(Valid(Json(input)): Valid<Json<CreateNote>>) {
    // input is parsed and validated.
}

async fn create_data(Valid(Data(input)): Valid<Data<CreateNote>>) {
    // same validation model for application data.
}
```

Parse failures and validation failures are different:

- A parse failure means Vyuh could not read the request into the wrapper type.
- A validation failure means parsing succeeded, but `Validate` rejected the
  parsed value. Validation errors return `422` with field entries containing
  `code`, `message`, and `params`.

Plain wrappers never publish validation constraints to OpenAPI. Validation
metadata is published only when `Valid<E>` is used. The full model is described
in [Validation](validation.md).

## OpenAPI

Wrappers contribute request metadata:

- `Json<T>` becomes a JSON request body.
- `Data<T>` becomes a JSON request body.
- `Query<T>` becomes query parameters.
- `Path<T>` becomes path parameters.
- `Form<T>` becomes a form request body.
- `MultipartForm<T>` becomes a `multipart/form-data` request body.
- `BodyBytes` becomes an opaque binary request body.

Given a DTO that derives `Validate`:

```rust
use schemars::JsonSchema;

#[derive(Deserialize, JsonSchema, Validate)]
struct CreateNote {
    #[validate(min_length = 3)]
    title: String,
}
```

`Json<CreateNote>` documents only the parse shape:

```yaml
requestBody:
  content:
    application/json:
      schema:
        $ref: '#/components/schemas/CreateNote'
```

`Valid<Json<CreateNote>>` documents the parse shape plus supported validation
constraints:

```yaml
requestBody:
  content:
    application/json:
      schema:
        type: object
        properties:
          title:
            type: string
            minLength: 3
```

See [Validation](validation.md) for supported constraints, structured
validation errors, runtime-only rules, and explicit `custom_schema` hints.

## Axum Escape Hatch

Vyuh wrappers are the recommended API. Direct Axum extractors remain possible
through explicit Axum imports, or through `vyuh::routes::axum_extractors` when a
route needs behavior Vyuh does not wrap yet.

## Failure Modes

- Parse and deserialization failures return `400`.
- Validation failures return `422` only when `Valid<E>` is used.
- Multipart type, size, and sniffing failures use the upload error mapping
  described in [Uploads](uploads.md).

## Current Limitations

- Request wrappers are route-only.
- Direct Axum extractors are available, but Vyuh cannot infer custom metadata
  unless the wrapper or route patch provides it.
- Validation metadata is intentionally opt-in through `Valid<E>`.
