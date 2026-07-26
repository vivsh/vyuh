# Validation

Vyuh validation is explicit at the route boundary. Parsing and validation are
separate steps:

- `Json<T>`, `Query<T>`, `Path<T>`, and `Form<T>` parse request data.
- `Valid<E>` opts a parsed extractor into runtime validation.
- `#[derive(Validate)]` implements the validation rules.
- `ValidationSchema` exposes accurately representable rules to OpenAPI.

This keeps API behavior visible in the handler signature:

```rust
use schemars::JsonSchema;
use vyuh::prelude::*;

async fn parse_only(Json(input): Json<CreateUser>) {
    // parsed only
}

async fn parse_and_validate(Valid(Json(input)): Valid<Json<CreateUser>>) {
    // parsed and validated
}
```

## Mental Model

- Request wrappers parse.
- `Valid<E>` validates.
- `Validate` decides runtime correctness.
- `ValidationSchema` describes supported constraints for OpenAPI.
- Unsupported or business-specific rules never appear in OpenAPI unless they
  opt in with explicit `custom_schema` vendor metadata.

## Defining Rules

Derive `Validate` on request DTOs and add `#[validate(...)]` attributes:

```rust
use schemars::JsonSchema;
use vyuh::prelude::*;

#[derive(Deserialize, JsonSchema, Validate)]
struct CreateUser {
    #[validate(email)]
    email: String,

    #[validate(min_length = 3, max_length = 80)]
    name: String,

    #[validate(min = 18)]
    age: u8,
}
```

Deriving `Validate` alone does not change route behavior. A route validates
only when the argument uses `Valid<E>`.

Supported derive rules:

| Category | Rules |
| --- | --- |
| Strings | `min_length`, `max_length`, `exact_length`, `pattern` |
| Formats | `email`, `url`, `uuid`, `phone_e164`, `ipv4`, `ipv6`, `date`, `datetime` |
| Numbers | `min`, `max`, `exclusive_min`, `exclusive_max`, `multiple_of` |
| Collections | `min_items`, `max_items`, `unique_items` |
| Choices | `enum_values(...)` |
| Nested data | `delegate` |
| Custom logic | `custom = "path"`, optional `custom_schema = "name"` |

`min_length`, `max_length`, and `exact_length` count Unicode characters. They
match JSON Schema `minLength` and `maxLength` semantics, not UTF-8 byte length.

## Using Valid

`Valid<E>` is generic over request wrappers:

```rust
use vyuh::prelude::*;

async fn create(Valid(Json(input)): Valid<Json<CreateUser>>) {}
async fn search(Valid(Query(input)): Valid<Query<SearchParams>>) {}
async fn update(Valid(Path(input)): Valid<Path<UserPath>>) {}
async fn login(Valid(Form(input)): Valid<Form<LoginForm>>) {}
```

The wrapped type must implement `Validate`. If it does not, Rust reports a
compile-time trait-bound error at the route signature.

`Valid<E>` supports `Deref` and `into_inner()`:

```rust
use vyuh::prelude::*;

async fn create(valid: Valid<Json<CreateUser>>) {
    let Json(input) = valid.into_inner();
}
```

## Standalone Validation

Validation is not tied to HTTP. Any value whose type implements `Validate` can
be checked directly from tests, services, commands, tasks, or ordinary
application code:

```rust
use vyuh::prelude::*;

#[derive(Validate)]
struct CreateUser {
    #[validate(email)]
    email: String,

    #[validate(min_length = 3)]
    name: String,
}

let input = CreateUser {
    email: "bad-email".into(),
    name: "Al".into(),
};

let report = input.validate().expect_err("input should be invalid");
assert!(report.has_error("email"));
assert!(report.has_error("name"));
```

Use `validate()` directly when a test needs to assert the validation rules
without building a site or sending an HTTP request. `Valid<E>` is the route
extractor layer; `Validate` is the underlying runtime rule implementation.

For field-oriented assertions, inspect the `ValidationReport`:

```rust
let flat = report.to_field_map_flat();
assert_eq!(flat["name"][0], "Must be at least 3 characters");
```

For structured assertions, use the nested helpers:

```rust
let report = input.validate().expect_err("invalid");
let errors = report.to_nested_errors();
assert_eq!(errors["name"][0]["code"], "min_length");
assert_eq!(errors["name"][0]["params"]["min"], "3");
```

Use `to_nested_errors()` when tests should assert the same `code`, `message`,
and `params` shape returned by `ErrorReport`. Use `to_nested_messages()` or
`to_field_map_flat()` for simpler message-only assertions.

## Errors

Vyuh preserves the distinction between parsing and validation:

- Parse or deserialization failure returns `400`.
- Validation failure returns `422`.

Validation failures are field-oriented and flow through `ErrorReport` and the
site-wide error handler:

```json
{
  "source": "validation",
  "code": "validation_error",
  "detail": "Validation failed.",
  "errors": {
    "email": [
      {
        "code": "email",
        "message": "Enter a valid email address.",
        "params": {}
      }
    ],
    "non_field_errors": [
      {
        "code": "invalid",
        "message": "The submitted data is invalid.",
        "params": {}
      }
    ],
    "name": [
      {
        "code": "min_length",
        "message": "Ensure this field has at least 3 characters.",
        "params": {
          "min": "3"
        }
      }
    ]
  }
}
```

For the larger error model, including `vyuh::Error`, command rendering, and task
retry behavior, see [Errors](errors.md).

Applications can replace the rendered response with `SiteConf::errors(...)`.
The validation report remains the normalized transport object before rendering.
Root-level errors are emitted under `non_field_errors`.

## Nested Validation

Use `delegate` when a field's type has its own validation rules:

```rust
use schemars::JsonSchema;

#[derive(Deserialize, JsonSchema, Validate)]
struct Address {
    #[validate(min_length = 2)]
    city: String,
}

#[derive(Deserialize, JsonSchema, Validate)]
struct Signup {
    #[validate(delegate)]
    address: Address,
}
```

Nested errors preserve field paths so clients can bind failures to form fields
or JSON paths.

## Runtime-Only Rules

Some validation belongs only at runtime:

- custom functions,
- database checks,
- authorization checks,
- cross-record or business rules,
- rules that cannot be represented exactly in JSON Schema.

Runtime validation is authoritative. OpenAPI is documentation only, so Vyuh
emits schema constraints only when they can be represented accurately.

Custom validators are runtime-only by default:

```rust
#[validate(custom = "validate_slug")]
slug: String,
```

A custom validator returns `Result<(), ValidationError>`:

```rust
use vyuh::prelude::*;

fn validate_slug(value: &String) -> Result<(), ValidationError> {
    if value.chars().all(|ch| ch.is_ascii_lowercase() || ch == '-') {
        Ok(())
    } else {
        Err(ValidationError::new("slug", "Enter a valid slug.")
            .with_param("allowed", "lowercase letters, digits, and dashes"))
    }
}
```

Expose a custom validator name to OpenAPI clients only when the frontend should
know about it:

```rust
#[validate(custom = "validate_slug", custom_schema = "slug")]
slug: String,
```

This emits Vyuh vendor metadata:

```json
{
  "x-vyuh-validators": ["slug"]
}
```

`custom_schema` is not JSON Schema. It is a client hint for application-specific
rules, and it is emitted only when the route uses `Valid<E>`. It requires
`custom`, accepts a string literal, and does not affect runtime validation.

## OpenAPI

OpenAPI validation metadata appears only for `Valid<E>`:

```rust
use vyuh::prelude::*;

async fn parse_only(Json(input): Json<CreateUser>) {}
async fn validated(Valid(Json(input)): Valid<Json<CreateUser>>) {}
```

The first route documents the request shape only. The second route documents
the request shape plus supported constraints such as string length, numeric
minimum and maximum, patterns, formats, collection limits, enum values, and
explicit custom hints from `custom_schema`.

`ValidationSchema` is the low-level trait generated by `#[derive(Validate)]`.
Most applications do not need to implement it manually; it exists so advanced
types can expose schema constraints in the same way as derive-generated types.

## Current Limitations

- Validation runs only where code explicitly calls `validate()` or uses
  `Valid<E>`.
- OpenAPI contains only constraints that Vyuh can represent accurately, plus
  explicit `custom_schema` vendor hints.
- Custom validators are ordinary Rust functions; Vyuh does not add a runtime
  validator registry in this pass.
