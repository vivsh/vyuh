# Vyuh

[![Crates.io](https://img.shields.io/crates/v/vyuh)](https://crates.io/crates/vyuh)
[![docs.rs](https://img.shields.io/docsrs/vyuh)](https://docs.rs/vyuh)
[![License](https://img.shields.io/crates/l/vyuh)](LICENSE)

_Vyuh_ (व्यूह, _vyoo-huh_) means "formation" or "arrangement".

Vyuh is a handler-first Rust web framework for building typed APIs and
application runtimes on top of Axum and SQLx.

It is built for applications that need more than routing. Routes, OpenAPI,
durable tasks, live channels, scheduled emitters, commands, services, and
operational introspection all live in one coherent model instead of feeling
like unrelated add-ons.

Vyuh keeps that model explicit. Handler signatures stay meaningful. Validation
is opt-in. Auth is opt-in. Retry is explicit. Bundles compose features without
hiding how the application is wired.

Website: [vivsh.github.io/vyuh](https://vivsh.github.io/vyuh/)
Docs: [vivsh.github.io/vyuh/docs](https://vivsh.github.io/vyuh/docs/)

Vyuh is usable today, but it is not API-stable yet. Expect breaking changes
before `1.0`.

## Highlights

- Typed handlers across subsystems: `Data<T>` is used consistently across
  routes, commands, tasks, signals, emitters, and live channel delivery.
- OpenAPI from real application code: request data, responses, validation, and
  auth metadata come from handler shapes and route metadata, with no per-route
  schema wiring.
- OpenAPI and console are effectively free: enable them once, and they follow
  the same bundle tree, prefixes, and nesting as the rest of the application.
- Durable tasks as continuations: value-less tasks can complete, sleep, suspend,
  resume, and request retry, with named isolation lanes owning retry/backoff
  policy, batched persistence, idempotency retention, and optional local or
  store-wide start-rate limits.
- Built-in live delivery: channels let clients subscribe to typed signal
  payloads over SSE, WebSocket, or long polling.
- Read-only operations console: inspect routes, operations, config, tasks, and
  runtime status from the same application.
- Bundle-based composition: keep routes, assets, tasks, services, signals, and
  docs together as one feature unit.
- Crate-level feature packaging: a bundle can be exported from another Rust
  crate and bring its handlers, services, assets, and metadata with it.

## How It Works

Two ideas drive the framework.

`Handler uniformity`

The same typed wrapper, `Data<T>`, appears across the major execution paths.
That keeps the framework mentally small.

- Routes parse request data into handler input.
- Commands receive typed input.
- Tasks receive typed input and perform durable value-less work with optional
  `TaskState` lifecycle control.
- Signals and emitters exchange typed data. Channels can expose emitted
  signal payloads to clients.

Uniformity here is practical, not forced. Services remain separate because they
represent site-lifetime components, not handler data. Validation stays explicit
through `Valid<E>`. Auth stays explicit through auth extractors.

`Bundles as composition`

A bundle is Vyuh's feature unit. A feature can own its routes, tasks, services,
signals, emitters, assets, templates, and OpenAPI declaration together, then be
merged or prefixed into a larger application.

That keeps application structure visible. Instead of scattering feature wiring
across several registries and config surfaces, Vyuh keeps the moving parts near
the code that defines them.

Bundles are also the packaging boundary. A crate can export a `Bundle`, and an
application can import it like any other Rust dependency. That imported feature
can still participate in routing, OpenAPI, console inspection, assets,
templates, and the rest of the site.

From that model, the framework gives you typed APIs, generated OpenAPI, durable
tasks, channels, emitters, commands, services, and the built-in console without
forcing each subsystem into a different programming style.

## Getting Started

Vyuh requires Rust 1.97 or newer.

Add the crate with one backend feature for production work:

```toml
vyuh = { version = "0.2", features = ["postgres"] }
schemars = "1"
```

For local experiments or documentation examples, Vyuh can run without a backend
feature with no live database pool and in-memory tasks.

Start with one route, one cron emitter, and one OpenAPI declaration:

```rust
use schemars::JsonSchema;
use vyuh::prelude::*;

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, Validate)]
struct Signup {
    #[validate(email)]
    email: String,

    #[validate(min_length = 3, max_length = 80)]
    name: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
struct UserCreated {
    id: i64,
    email: String,
    name: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
struct SystemPulse {
    source: String,
}

#[bundles::route(path = "/users", method = "POST")]
async fn signup(Valid(Data(input)): Valid<Data<Signup>>) -> Result<Data<UserCreated>, Error> {
    Ok(Data::new(UserCreated {
        id: 1,
        email: input.email.clone(),
        name: input.name.clone(),
    }))
}

#[bundles::cron(expr = "0 */5 * * * *")]
async fn heartbeat() -> Data<SystemPulse> {
    Data::new(SystemPulse {
        source: "signup-service".into(),
    })
}

#[tokio::main]
async fn main() -> Result<(), SiteError> {
    let app = bundles::bundle! {
        signup,
        heartbeat,
    }
    .with_conf(bundles::conf().openapi(
        bundles::OpenApiConf::default()
            .title("Vyuh Example")
            .version("0.1.0")
            .description("A route, a cron emitter, and generated OpenAPI.")
            .spec("/openapi.json")
            .viewer("/docs"),
    ));

    Site::run(SiteConf::from_env_with_files()?, app).await
}
```

The route handles HTTP input. The cron handler runs on a schedule. Both are
ordinary async functions, both return typed `Data<T>`, and both are registered
through the same bundle.

`Valid<Data<Signup>>` parses and validates request data at the handler
boundary. `Data<UserCreated>` becomes the JSON response body and response
schema. `with_conf(bundles::conf().openapi(...))` exposes the generated spec and docs page without
adding per-route OpenAPI code. `Site::run(...)` is the standard application
entrypoint.

Tasks, commands, signals, channels, and services follow the same bundle-driven
model. See the getting started book for the full walkthrough.

As the application grows, keep adding features as bundles instead of widening
one central setup file.

## Common Paths

- Build an API: routes, request wrappers, response wrappers, validation,
  errors, and OpenAPI.
- Add durable background work: tasks plus a database-backed task store.
- Add live updates: signals, emitters, and channels.
- Add operations tooling: site-aware commands and the optional console.
- Compose features cleanly: define one bundle per domain area and merge them at
  the top level.

## Documentation

- [Website](https://vivsh.github.io/vyuh/)
- [Full docs](https://vivsh.github.io/vyuh/docs/)
- [Source docs](docs/book/src/SUMMARY.md)
- [Site and lifecycle](docs/book/src/site.md)
- [Bundles](docs/book/src/bundles.md)
- [OpenAPI](docs/book/src/openapi.md)
- [Tasks](docs/book/src/tasks.md)
- [Channels](docs/book/src/channels.md)
- [Console](docs/book/src/console.md)

## Backend Support

Vyuh supports Postgres, MySQL, and SQLite through Mool, with Postgres as the
preferred production backend where concurrency and notification features matter
most.

Enable exactly one backend feature in production:

```toml
vyuh = { version = "0.2", features = ["postgres"] }
vyuh = { version = "0.2", features = ["mysql"] }
vyuh = { version = "0.2", features = ["sqlite"] }
```

With no backend feature enabled, Vyuh uses SQLite-compatible aliases and an
in-memory task store. That mode is useful for quick starts, docs, and tests,
not for durable production workloads.

## Current Caveats

- Vyuh is not API-stable yet.
- Services are in-process and not durable.
- Tasks are durable single-task continuations, not workflow orchestration.
- `MemoryTaskStore` is the no-backend default and is not durable.
- Some features remain intentionally Postgres-only, such as `LISTEN`/`NOTIFY`
  and selected SQL helpers.

## License

Vyuh is licensed under the [MIT License](LICENSE).
