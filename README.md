# Vyuh

[![Crates.io](https://img.shields.io/crates/v/vyuh)](https://crates.io/crates/vyuh)
[![docs.rs](https://img.shields.io/docsrs/vyuh)](https://docs.rs/vyuh)
[![License](https://img.shields.io/crates/l/vyuh)](LICENSE)

**Build the application, not just its HTTP endpoints.**

Vyuh is a Rust application framework on Axum and SQLx. Typed APIs,
authentication, durable workflows, live updates, and MCP tools fit into one
composable application model.

Organize by feature: a bundle brings its handlers, services, templates, assets,
and schema registrations together. Merge bundles, mount them under prefixes, or
ship them as reusable Rust crates.

[Book](https://vivsh.github.io/vyuh/docs/) ·
[API reference](https://docs.rs/vyuh) ·
[Examples](vyuh/examples)

**Rust 1.92+ · Pre-1.0: breaking changes are still expected.**

## Why Vyuh?

- **One API contract.** Typed inputs, responses, validation, and auth metadata
  drive [OpenAPI](docs/book/src/openapi.md). Documentation follows your handlers
  and bundle structure, with explicit overrides where needed.

- **Auth that fits your application.** Password, MFA, passwordless, and optional
  federated login feed a common `AuthUser`. Typed permits and audiences express
  access boundaries. You retain ownership of accounts, login routes, storage,
  and domain-specific authorization. [Auth →](docs/book/src/auth.md)

- **Persistence that composes.** Mool provides typed queries, bulk operations,
  and transactions; Gaman powers generated, reviewable migrations. Reusable
  crates ship their own migration histories, composed in dependency order.
  Apply reviewed migrations before deployment—not silently at startup.
  [Database →](docs/book/src/db.md) · [Migrations →](docs/book/src/migrations.md)

- **Workflows that survive restarts.** Async Work handles effects, retries, and
  external suspension. Synchronous Flow adds checkpoints, sleep, child spawning,
  and ordered all-settled joins. Lanes, rates, batching, and leased lane ownership
  control execution. Durable cron/periodic scheduling atomically records schedule
  progress and task creation, coalescing missed runs.
  [Tasks →](docs/book/src/tasks.md) · [Schedules →](docs/book/src/emitters.md)

- **Live applications and AI-facing tools.** Typed, scoped subscriptions deliver
  updates over WebSocket, SSE, or long polling with bounded process-local replay.
  Optional MCP exposes explicitly registered tools and static resources through
  the same auth model, with UI attachments for compatible clients.
  [Channels →](docs/book/src/channels.md) · [MCP →](docs/book/src/mcp.md)

For browser-facing applications, Vyuh also includes MiniJinja templates, embedded
assets, typed uploads, local file storage, optional SMTP mail, named caches, and
static page export. Explore the [web platform](docs/book/src/SUMMARY.md#web-platform).

## What the code looks like

An authenticated, validated endpoint from the [blog example](vyuh/examples/blog.rs):

```rust
#[bundles::route(path = "/api/users", method = "POST")]
async fn create_user(
    site: Site,
    user: AdminUser,
    Valid(Json(input)): Valid<Json<UserInput>>,
) -> Result<Json<UserOut>, Error> {
    let mut db = site.db();
    let _admin = user.into_user();
    let user = insert_user(
        &mut db,
        &input.username,
        &input.display_name,
        &input.password,
        input.is_admin,
    )
    .await?;
    Ok(Json(UserOut::from(user)))
}
```

`AdminUser` is a `Permit<AdminAccess>`; `Valid` enforces input validation.
The application's `insert_user` hashes the password and persists the user.
Models, policies, helpers, and configuration live in the linked example.

Macros are sugar over [direct registration](docs/book/src/routes.md#macro-sugar-and-direct-api).
The same [bundle](docs/book/src/bundles.md) can own this endpoint, management
commands, background work, and resources.

## Built to be operated

Inspect routes, operations, tasks, and runtime status in the **read-only console**.
Use structured logs, readiness probes, and Prometheus metrics. Run typed
management commands against the same configured application. Test HTTP handlers
without binding a port, with auth helpers and isolated database fixtures.

[Testing and lifecycle](docs/book/src/site.md) ·
[Commands](docs/book/src/commands.md) ·
[Console](docs/book/src/console.md) ·
[Deployment](docs/book/src/production.md)

Database-backed tasks atomically commit checkpoints with child creation, and
child results with parent readiness. Execution resumes through later claims.
It is **at least once**: external effects need idempotency, and cancellation
cannot undo them. Signals, live replay, and service workers remain process-local;
they do not inherit task durability.

## Try it

The blog includes login, a browser UI, post/comment APIs, uploads, migrations,
and OpenAPI. With Rust 1.92+, local PostgreSQL, and permission to create a
database:

```sh
git clone https://github.com/vivsh/vyuh.git
cd vyuh

createdb vyuh_blog
export DATABASE_URL=postgres://localhost/vyuh_blog
export VYUH_SECRET_KEY="$(openssl rand -hex 32)"

cargo run -p vyuh --features postgres,migrations --example blog -- migrate
cargo run -p vyuh --features postgres,migrations --example blog -- users:create-admin \
  --username admin --display-name Admin --password change-me
cargo run -p vyuh --features postgres,migrations --example blog -- serve
```

Adjust database credentials as needed. Open [the blog](http://127.0.0.1:8080/),
sign in, and explore the protected API docs at `/docs`.
This is local development only: replace example credentials and configure access
before deployment. The debug console permits anonymous access by default.

For your own application, follow [Getting Started](docs/book/src/getting-started.md).

## Backend support

Choose one backend: **PostgreSQL** for clustered deployments, **SQLite** for
durable local single-process applications, or **MySQL** for experimental support.
Backend-specific capabilities remain explicit. Without a backend, development
builds use non-durable in-memory task storage.

[Contributing](CONTRIBUTING.md) · [MIT License](LICENSE)
