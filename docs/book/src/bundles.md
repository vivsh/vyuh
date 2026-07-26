# Bundles

Bundles are Vyuh's composition API. A `Bundle` collects routes, signals,
emitters, tasks, services, commands, asset directories, URL info providers,
migrations, schema contributors, and OpenAPI configuration into one value that can be mounted,
merged, prefixed, layered, validated, and passed to site startup.

Every registerable item enters a bundle as a `BundlePart`. Macros create
`BundlePart` values for ergonomic static registration; the direct APIs create
the same parts explicitly.

## Overview

A bundle has three jobs:

- collect runtime registrations for each subsystem,
- collect operation metadata for docs, diagnostic surfaces, and reverse routing,
- accumulate registration errors until `Bundle::validate()` or site build.

Invalid paths, duplicate route names, duplicate route method/path collisions,
duplicate migrations, and subsystem registration errors are stored on the
bundle. `Site` construction validates the final composed bundle before startup.

## BundlePart

`BundlePart` is the common registration unit for framework features. Route,
signal, emitter, task, service, command, asset, URL info, migration, and schema helpers
all return a `BundlePart`.

The direct constructor is `bundles::bundle([...])`:

```rust
let bundle = bundles::bundle([
    bundles::route(
        list_notes,
        RouteConf {
            name: Cow::Borrowed("list_notes"),
            path: Cow::Borrowed("/notes"),
            methods: Methods::GET,
            slash: None,
        },
    ),
    bundles::signal::<NoteChanged, _, _>(
        index_note_change,
        signals::SignalConf::default(),
    ),
]);
```

Some bundle parts expose operation metadata. `BundlePart::patch(PatchOp)`
amends that metadata before the part is registered into a bundle.

## Patch API

`BundlePart::patch(PatchOp)` is the general metadata override API. It can adjust
operation names, descriptions, argument metadata, return metadata, status codes,
and additional responses when the part has operation metadata.

```rust
let route = bundles::route(create_note, conf).patch(
    PatchOp::new()
        .ret()
        .status(201)
        .doc("Created note")
        .done(),
);
```

Patches affect metadata only. They do not change runtime extraction, routing, or
handler behavior. Routes and OpenAPI are the main v0 use case for patching;
other subsystem docs mention patching only when there is a feature-specific
use case.

## Macro Sugar

Feature macros such as `#[bundles::route]` and `#[bundles::signal]` generate a
hidden bundle-part helper next to the annotated item. `bundle!` calls those
helpers and passes the resulting parts to `bundles::bundle([...])`.

```rust
#[bundles::route(path = "/notes")]
async fn list_notes() -> Json<Vec<Note>> {
    Json(Vec::new())
}

#[bundles::signal]
async fn index_note_change(Data(event): Data<NoteChanged>) {
    tracing::info!("note {} changed", event.id);
}

let bundle = bundles::bundle! {
    list_notes,
    index_note_change,
};
```

The macro path does not add a unique runtime capability. Use direct
registration when bundle parts are generated, conditional, feature-gated, or
assembled from tables.

`bundle!` is Vyuh's only custom macro syntax. Attribute macros and derives use
ordinary structured metadata such as `key = "value"` and `item(...)`; direct
APIs remain the canonical escape hatch when macro syntax is not appropriate.

## Module Organization

`bundle!` members must be macro-registered items visible in the module where
`bundle!` is invoked. The macro expands each member to an unqualified helper
call named like `__bundle_part_<item>()`.

For cross-module organization, let each module expose a bundle function and
compose those bundles in the parent module:

```rust
mod notes {
    use vyuh::prelude::*;

    #[bundles::route(path = "/notes")]
    async fn list_notes() -> Json<Vec<Note>> {
        Json(Vec::new())
    }

    pub fn bundle() -> bundles::Bundle {
        bundles::bundle! {
            list_notes,
        }
    }
}

mod ops {
    pub fn bundle() -> vyuh::bundles::Bundle {
        vyuh::bundles::Bundle::new()
    }
}

let app = notes::bundle().merge(ops::bundle());
```

Do not rely on `bundle!` to reach into another module's generated helper. Merge
the other module's `Bundle` instead.

## Bundles From Other Crates

Bundles are not limited to local modules. A Rust crate can export a function
that returns a `Bundle`, and an application can import and merge that bundle
like any other dependency:

```rust
let app = bundles::bundle! {
    local_health_check,
}
.merge(blog_feature::bundle())
.merge(admin_feature::bundle());
```

This is useful when a feature should ship with more than routes. An imported
bundle can bring its handlers, services, templates, assets, URL info, OpenAPI
metadata, commands, signals, tasks, emitters, schema contributors, and migration
source registration together. After it is merged, it remains part of the same
site: prefixes, OpenAPI generation, console inspection, middleware layers, and
validation all see it as normal bundle content.

Use this boundary for reusable application capabilities: a blog, chat system,
auth module, internal admin area, or shared operational tooling. The exporting
crate should expose a small `bundle()` function and keep feature-owned assets
and templates inside the crate's bundle resources.

Migrations are registered through bundles but are owned by the crate, not by an
individual bundle. A crate can expose several bundles, but it should register at
most one migration source for its flat top-level `migrations/` directory. Child
crate migrations use a virtual Gaman namespace:

```rust
use vyuh::prelude::*;

static MIGRATIONS: db::EmbeddedMigrations =
    db::embed_migrations!("migrations");

#[bundles::schema(namespace = "auth")]
fn schema() -> Result<db::Schema, db::SchemaLoadError> {
    db::Schema::builder(db::Dialect::Postgres)
        .table::<User>()
        .build()
}

#[bundles::migrations]
fn migrations() -> db::MigrationSource {
    db::crate_migration("auth", &MIGRATIONS)
}

pub fn bundle() -> bundles::Bundle {
    bundles::bundle! {
        schema,
        migrations,
    }
}
```

The root application uses `db::root_migration(&MIGRATIONS)` and an unnamespaced
`#[bundles::schema]`. Duplicate root migration sources or duplicate crate
namespaces are bundle validation errors.

## Composition

`merge` combines two bundles, including routes, operations, services, signals,
emitters, tasks, commands, URL info, migrations, schema contributors, assets, and doc
configuration.

```rust
let api = notes::bundle()
    .merge(users::bundle())
    .with_prefix("/v1")
    .with_tags(["api", "v1"]);
```

`with_prefix` prefixes route paths and operation metadata. Prefixes must start
with `/`, must not be `/`, and must not end with `/`.

`with_tags` adds tags to all current operations in the bundle. `layer` applies
middleware to all routes in the bundle; middleware that exposes metadata also
updates operations for documentation.

`reverse(name, args)` resolves a named route to its final path. Missing path
arguments return `None`; extra arguments are ignored; substituted values are
percent-encoded.

`iter_operations()` exposes the collected operation metadata. Callers that show
operations should filter hidden entries.

## OpenAPI Order

`with_openapi` snapshots route operations already registered in the bundle.
Routes added or merged after `with_openapi` do not appear in that generated
spec.

Call `with_openapi` after route registration and merge steps for the API surface
being documented:

```rust
let api = notes::bundle()
    .merge(users::bundle())
    .with_prefix("/v1")
    .with_openapi(
        bundles::OpenApiConf::default()
            .title("Notes API")
            .spec("/openapi.json"),
    );
```

Prefixes and metadata applied to already captured operations still affect final
paths and operation metadata.

## Examples

Bundles are exercised by the current runnable examples:

```sh
cargo run -p vyuh --example chatroom
cargo run -p vyuh --example console
cargo run -p vyuh --example signals_emitters
cargo run -p vyuh --features sqlite --example tasks
```

- `chatroom`: routes, static HTML, and channels in one bundle.
- `console`: routes, commands, tasks, signals, emitters, OpenAPI, and console
  configuration in one bundle.
- `signals_emitters`: signal fanout plus cron, periodic, direct, and
  Postgres-gated PgNotify emitter registration.
- `tasks`: durable task handlers; it requires the `sqlite` feature.

## Best Practices

- Use `bundle!` for ordinary static, same-module macro registrations.
- Use `bundles::bundle([...])` for generated or conditional bundle parts.
- Return `Bundle` values from feature modules and compose them with `merge`.
- Apply `with_prefix`, `with_tags`, and `layer` at clear composition boundaries.
- Call `with_openapi` after the routes for that spec have been registered and
  merged.

## Failure Modes

- Invalid route paths, prefixes, slash rules, and operation metadata are
  collected on the bundle and reported during validation or site build.
- Duplicate route names or method/path pairs fail site build.
- Duplicate subsystem registrations, such as services or commands with the same
  identity, fail before the site starts.
- Duplicate root migration sources or duplicate migration namespaces fail
  before the site starts.
- OpenAPI captures only routes registered before `with_openapi`.

## Current Limitations

- `bundle!` only works with macro-registered items visible in the current
  module.
- Bundle composition is order-sensitive for APIs that snapshot metadata.
- Macros are convenience only; direct registration is the canonical escape hatch
  for generated or conditional registrations.
