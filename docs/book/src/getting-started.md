# Getting Started

This chapter walks through a small Vyuh application that touches the framework
surfaces most projects care about first: routes, provider authentication, a task, a command,
a cron emitter, a signal handler, and OpenAPI.

It is not a production-ready application, and it is not trying to hide that.
The goal is simpler: show how Vyuh keeps HTTP, background work, scheduled work,
and operations in one model without making them feel like separate systems.

## Data Types

Start with ordinary Rust types.

`Data<T>` is the main wrapper Vyuh moves through handlers. Add `Validate` when
input should be checked at the boundary. Add `JsonSchema` when a type should
appear in generated OpenAPI.

```rust
{{#include ../../../vyuh/examples/getting_started.rs:data_types}}
```

These types are intentionally plain. Vyuh does not ask you to declare a second
schema layer just to move data through the framework.

## Routes

Routes are ordinary async functions with typed inputs and typed outputs. The
important thing to notice is not the macro. It is the function signature.

```rust
{{#include ../../../vyuh/examples/getting_started.rs:routes}}
```

`signup` is a validated JSON route. `login` selects the configured password
method with `.via(PASSWORD)` and returns the normal JWT credentials; `me`
extracts the resulting `AuthUser`.

That combination is already most of what a real API does: parse input, validate
it, authenticate some endpoints, and issue tokens without dropping into
untyped request plumbing.

## Task, Cron, And Command

Tasks, cron emitters, signals, and commands are different runtime paths, but
Vyuh does not make them feel alien. They use the same handler style and the
same bundle composition model.

```rust
{{#include ../../../vyuh/examples/getting_started.rs:runtime_paths}}
```

This is where Vyuh starts to feel different. The route, task, cron emitter,
signal handler, and command are not the same feature, but they are close enough
in shape that you can move between them without switching mental models.

- The route handles HTTP input.
- The task handles durable background input.
- The cron handler runs on a schedule and emits typed data.
- The signal handler receives that emitted data in-process.
- The command handles CLI input.

All of them are ordinary async functions over typed data. That is the
uniformity argument in practice, not as a slogan.

## Auth And OpenAPI

Auth stays explicit. If a handler does not extract auth, Vyuh does no auth work
for it. For a first application, the default token provider is the path with
the least ceremony: configure one `PasswordLogin`, return a `LoginResponse`,
and extract `AuthUser` where a route should require an access token.

OpenAPI works the same way Vyuh usually works: attach it once at the bundle,
and let it follow the routes that bundle already owns. That means prefixes,
nesting, and route metadata stay aligned without a parallel documentation tree.

```rust
{{#include ../../../vyuh/examples/getting_started.rs:api_bundle}}
```

OpenAPI and the docs viewer need no per-route schema file, no separate route
table, and no duplicate metadata layer. They come from the handlers and bundle
declaration you already had to write anyway. Authenticated routes also belong
to a bundle audience, which becomes part of their generated metadata.

## Command Bundle

Commands are registered directly and then merged like any other bundle part.
That matters because commands are operational code, but they still belong to
the same application.

```rust
{{#include ../../../vyuh/examples/getting_started.rs:command_bundle}}
```

The result is modest but useful: CLI work stays close to the feature that owns
it instead of drifting into a separate operational codebase.

## Main Function

Put the bundle and site configuration together in `main`. This is where the
different surfaces stop being examples and become one application.

```rust
{{#include ../../../vyuh/examples/getting_started.rs:main}}
```

The bundle is where the feature surface comes together: routes, a task, a cron
emitter, a signal handler, command registration, provider-protected handlers, and
OpenAPI. `main` stays small because the feature wiring already lives with the
feature.

## What To Notice

- One bundle holds the feature surface instead of scattering setup across
  unrelated registries.
- `Data<T>` keeps the route, task, signal, command, and cron shapes
  recognizably close to each other.
- Validation is explicit through `Valid<Data<T>>`, not inferred from derives
  alone.
- Provider auth is explicit through `AuthUser` and a bundle audience, while
  setup stays small with `AuthConf::default()`.
- OpenAPI is attached once and follows the bundle tree automatically.

From here, the next useful pages are [Bundles](bundles.md), [Routes](routes.md),
[OpenAPI](openapi.md), [Tasks](tasks.md), and [Auth](auth.md).
