# Commands

Vyuh commands are site-aware CLI entrypoints. Use them for administration,
diagnostics, maintenance, one-off data repair, and local operational tools that
should run against the same configured site as the web server.

Built-in commands are available in every command-aware application:

- `serve` starts the HTTP server.
- `help` lists commands and shows command-specific help.
- `health` checks whether the site can be built.
- `config` prints redaction-safe runtime configuration.
- `collect_assets` copies bundled public assets, optionally filtered by glob.
- `collect_pages` renders selected static URLs and copies public assets.

When the `migrations` feature is enabled, Vyuh also registers migration
commands:

- `make_migration` writes a new root application migration in debug builds.
- `show_migrations` lists root and crate migrations with applied status.
- `migrate` applies or inspects the composed migration graph.
- `sql_migrate` prints SQL for all migrations or one selected migration.
- `verify_db` compares the live database with migration replay state.
- `inspect_db` prints the live database schema.

Applications register additional commands through bundles.

Commands execute against the fully constructed `Site`. They share the same
configuration, database pools, services, templates, assets, logging, task queue,
and dependency injection as HTTP routes.

Commands are not durable background work. Use [Tasks](tasks.md) for retryable
work that must survive restarts, and [Services](services.md) for site-lifetime
background loops.

Use commands for one-shot operational actions that need the configured `Site`.
Do not use commands for user-facing HTTP endpoints, retryable background jobs,
or long-running supervised workers.

## Mental Model

```text
HTTP Request   -> Route
CLI Invocation -> Command
Durable Work   -> Task
Site Lifetime  -> Service
```

| Subsystem | Trigger         | Lifetime            | Use For                                       | Not For                 |
| --------- | --------------- | ------------------- | --------------------------------------------- | ----------------------- |
| Routes    | HTTP request    | one request         | APIs, pages, webhooks                         | maintenance scripts     |
| Commands  | CLI invocation  | one process command | admin, repair, reindex, diagnostics           | durable background jobs |
| Tasks     | task submission | persisted work unit | retryable async work, sleeps, external resume | interactive CLI tools   |
| Services  | site startup    | site lifetime       | shared clients, caches, in-process loops      | one-off operations      |

## Overview

The main public pieces are:

- `bundles::command(handler, CommandConf)` for registration.
- `CommandConf::new(name)` for naming the command.
- `Data<T>` for typed command arguments.
- `Site::run(conf, bundle)` for command-aware application entrypoints.

Service constructors have completed and service workers have been spawned before
the command handler runs.

## Typical Commands

Commands are a natural home for operational tools such as database
verification, indexing, imports, exports, and maintenance.

Typical commands include:

- `verify_sql`
- `migrate`
- `make_migration`
- `seed`
- `search:reindex`
- `cache:warm`
- `users:repair`
- `import`
- `export`
- `send-test-email`

Migration generation is root-only. Child crate migrations are consumed through
virtual namespaces such as `auth/0001_initial`, but the composed application
does not generate dependency crate migrations. Generate those while developing
the dependency crate itself.

## Registration

Define a typed argument struct and register the command as a bundle part:

```rust
use vyuh::prelude::*;
use vyuh::commands::CommandConf;

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
struct ReindexArgs {
    #[serde(default)]
    dry_run: bool,
    #[serde(default)]
    force: bool,
}

async fn reindex(Data(args): Data<ReindexArgs>) -> Result<(), Error> {
    if args.dry_run {
        println!("Search reindex would run.");
        return Ok(());
    }

    println!("Search reindex requested. force={}", args.force);
    Ok(())
}

let bundle = bundles::bundle([bundles::command(
    reindex,
    CommandConf::new("search:reindex").description("Rebuild the search index."),
)]);
```

Command names must be unique. The reserved `help` command is provided by Vyuh.
Flat names are the primary API today, but namespaced names such as
`user:create`, `search:reindex`, and `db:repair` are a good convention for
larger applications.

## Running

Use `Site::run` as the normal command-aware application entrypoint. With no
command arguments it runs the built-in `serve` command:

```rust
#[tokio::main]
async fn main() -> Result<(), vyuh::SiteError> {
    vyuh::Site::run(vyuh::SiteConf::from_env_with_files()?, app_bundle()).await
}
```

Then run commands by name through the application binary:

```sh
cargo run -- search:reindex --dry-run
cargo run -- help
cargo run -- help search:reindex
cargo run -- search:reindex --help
cargo run -- search:reindex -h
```

`config` prints the same redaction-safe configuration shape used by the console
config view. In debug builds, `config --raw` prints raw `SiteConf` for local
diagnostics. Raw config output is disabled in release builds.

`Site::run` returns an error when site build or command execution fails. It does
not call `std::process::exit`. With a normal `#[tokio::main] async fn main() ->
Result<_, _>`:

| Exit Code | Meaning |
| --------- | ------- |
| `0` | command succeeded |
| `1` | site build failed |
| `1` | command failed |

Commands should print normal output to stdout. Diagnostics should go to stderr
or be returned as errors for the application entrypoint to render.

Each command invocation runs one command in that process. Vyuh does not take a
global command lock, so separate processes may run commands concurrently. Use
database locks, transactions, advisory locks, or application-level coordination
when an operation must be exclusive.

## Arguments

Command arguments come from the data type's `JsonSchema`. Keep command argument
structs simple and object-shaped:

- strings: `--name Vyuh` or `--name=Vyuh`
- integers and numbers: `--limit 10`
- booleans: `--verbose`, `--verbose true`, or `--no-verbose`
- arrays: repeated values after one flag, such as `--tag api web admin`
- repeated arrays: `--tag api web --tag admin`
- optional fields: omitted when absent
- required fields: reported as missing when not supplied

Unsupported schema shapes fail during site build instead of silently producing a
command with no arguments.

Rust field names are exposed as kebab-case flags. A field named `dry_run` is
shown as `--dry-run`; Vyuh also accepts the snake_case alias `--dry_run`.

Array values may be split across repeated flags:

```sh
search:reindex --tag api web --tag admin
```

Empty arrays are not represented with a flag; omit an optional array field when
there are no values. Empty strings are accepted when the shell passes an empty
argument, for example `--name ""`. Scalar and boolean flags may appear only
once; repeated scalar and boolean flags are reported as duplicate flags.

`Data<T>` stores an `Arc<T>` so the same wrapper can be shared across
subsystems. It supports pattern matching, `Deref`, `AsRef`, and `into_inner()`:

```rust
use vyuh::prelude::*;

async fn reindex(Data(args): Data<ReindexArgs>) -> Result<(), Error> {
    println!("Search reindex requested. force={}", args.force);
    Ok(())
}
```

## Site-Aware Commands

Extract `Site` when a command needs subsystem access:

```rust
use vyuh::prelude::*;
use vyuh::commands::CommandConf;
use vyuh::db::{DBSession, Statement};

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
struct VerifySqlArgs {
    #[serde(default)]
    pending_only: bool,
}

async fn verify_sql(site: Site, Data(args): Data<VerifySqlArgs>) -> Result<(), Error> {
    let mut db = site.db();
    let sql = if args.pending_only {
        "SELECT COUNT(*) FROM migrations WHERE applied_at IS NULL"
    } else {
        "SELECT COUNT(*) FROM migrations"
    };

    let count: i64 = db
        .fetch_scalar(Statement::from_str(sql))
        .await
        .map_err(Error::other)?;

    println!("Verified migration table. matching_rows={count}");
    Ok(())
}

let bundle = bundles::bundle([bundles::command(
    verify_sql,
    CommandConf::new("verify_sql").description("Verify migration table access."),
)]);
```

The full site is available because commands run after site build. Service
constructors are different: they run while the site is still being assembled.

Commands may enqueue tasks and this is often a good pattern:

```rust
use vyuh::prelude::*;

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct RebuildIndex {
    full: bool,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
struct RebuildArgs {
    #[serde(default)]
    full: bool,
}

async fn rebuild(site: Site, Data(args): Data<RebuildArgs>) -> Result<(), Error> {
    site.tasks()
        .submit_with(RebuildIndex { full: args.full }, Default::default())
        .await
        .map_err(Error::other)?;
    Ok(())
}
```

Use this when the command should trigger durable work and return quickly. Do the
work directly in the command only when it is naturally short-lived and
operationally interactive.

Commands do not automatically run inside a database transaction. Use the normal
database/session/transaction APIs explicitly when an operation needs atomicity.

## Validation

Wrap command data in `Valid<Data<T>>` when CLI arguments should be validated
with the same rules used by routes:

```rust
use vyuh::prelude::*;

#[derive(Deserialize, Serialize, JsonSchema, Validate)]
struct CreateUser {
    #[validate(email)]
    email: String,
    #[validate(min_length = 3)]
    name: String,
}

async fn create_user(Valid(Data(args)): Valid<Data<CreateUser>>) -> Result<(), Error> {
    println!("creating {}", args.email);
    Ok(())
}
```

Argument parsing errors are command errors. Validation failures keep their
field-oriented structure and are rendered as CLI output:

```text
Validation failed for command 'create-user':

  --email
    Enter a valid email address.

Use 'create-user --help' for usage.
```

See [Validation](validation.md) for validation rules and [Errors](errors.md)
for the application/subsystem error boundary.

## Help And Errors

`help` lists registered commands. `help <command>`, `<command> --help`, and
`<command> -h` show the flags derived from the command argument schema and field
descriptions when available. Help and unknown-command reporting use command
metadata only; they do not open the database pool, start services, run emitters,
or start the HTTP server.

Commands and flags are shown in deterministic alphabetical order.

`CommandConf::description(...)` overrides the handler doc-comment summary in
help output:

```rust
bundles::command(
    reindex,
    CommandConf::new("search:reindex").description("Rebuild the search index."),
)
```

Command errors are explicit:

- unknown commands mention `help`;
- unknown flags include the command and flag name;
- duplicate scalar and boolean flags are rejected;
- missing required arguments name the flag;
- parse errors include the flag, supplied value, and expected type;
- validation failures render field-oriented CLI output;
- handler `vyuh::Error` values render compact application messages;
- duplicate command names and reserved names fail site build.

`CommandError` is for command machinery. Application command handlers should
return `vyuh::Error`.

## Router Boundary

Commands do not need raw router access. Use `Site::serve` for server-only
binaries or the built-in `serve` command through `Site::run`, and use
`vyuh::testing::router(&site)` only for tests or interop that truly needs an
Axum `Router`.

## Examples

Command handlers in this page show the supported patterns:

- typed command arguments with `Data<T>`;
- site-aware commands that extract `Site`;
- commands that enqueue durable tasks and return quickly.

## Current Limitations

- Commands are in-process and scoped to one built site.
- Commands are not durable, retried, scheduled, or supervised.
- Argument parsing intentionally supports a small predictable flag syntax.
- Commands should stay short-lived; long-running background behavior belongs in
  services or tasks.
- Macro sugar for commands is deferred; direct registration is the supported
  API in this pass.
