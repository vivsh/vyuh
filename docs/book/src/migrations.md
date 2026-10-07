# Migrations

Vyuh treats migrations as part of a crate's public implementation. Every crate
owns a single migration history regardless of how many bundles it exports. When
applications compose crates, their migration histories are combined into one
migration graph.

The mental model is:

1. Every crate owns one migration history.
2. Bundles register that history with the application.
3. The application composes all registered histories into one migration graph.
4. Only the owning crate generates new migrations.
5. Applications apply migrations; they do not generate dependency crate
   migrations.

Migrations are powered by Gaman. Postgres is the broadest backend; SQLite is
supported for the native subset that Gaman can represent safely. They are
discovered through bundle registration so applications only include the crates
they actually use. Ownership, however, remains at the crate level rather than
the individual bundle level.

A crate's migration history is flat:

```text
migrations/
  0001_initial.yaml
  0002_add_sessions.yaml
```

Migrations do not live under `assets/`; they are database history. Embedded
`assets/schema/**` files may define desired schema state, but they do not replace
committed migrations and never run automatically at site startup.

Migration files are embedded into the executable at compile time with
`db::embed_migrations!`, so release deployments do not need to ship loose YAML
files beside the binary or declare Mool as a direct dependency.

## Root Migrations

Root application migrations are unnamespaced:

```rust
use vyuh::prelude::*;

static MIGRATIONS: db::EmbeddedMigrations =
    db::embed_migrations!("migrations");

#[bundles::migrations]
fn migrations() -> db::MigrationSource {
    db::root_migration(&MIGRATIONS)
}
```

Root schema contributors omit the namespace:

```rust
use vyuh::prelude::*;

#[bundles::schema]
fn schema() -> Result<db::Schema, db::SchemaLoadError> {
    db::Schema::builder(db::Dialect::Postgres)
        .table::<Account>()
        .table::<Project>()
        .build()
}
```

## Crate Migrations

Reusable crates register their migration history under a virtual Gaman
namespace:

```rust
use vyuh::prelude::*;

static MIGRATIONS: db::EmbeddedMigrations =
    db::embed_migrations!("migrations");

#[bundles::migrations]
fn migrations() -> db::MigrationSource {
    db::crate_migration("auth", &MIGRATIONS)
}
```

The files in that crate remain local names such as `0001_initial.yaml`. In the
composed app, Gaman sees them as `auth/0001_initial`, which avoids collisions
with the root app and other crates.

Schema contributors from reusable crates should use the same namespace:

```rust
use vyuh::prelude::*;

#[bundles::schema(namespace = "auth")]
fn schema() -> Result<db::Schema, db::SchemaLoadError> {
    db::Schema::builder(db::Dialect::Postgres)
        .table::<User>()
        .table::<Session>()
        .build()
}
```

## Composition

During site build, Vyuh collects every registered migration source and schema
contributor into one composed migration graph. Root migrations remain
unnamespaced while reusable crates keep their virtual namespaces:

```text
app
├── root migrations
│   ├── 0001_initial
│   └── 0002_projects
│
├── auth crate
│   ├── auth/0001_initial
│   └── auth/0002_sessions
│
└── blog crate
    ├── blog/0001_initial
    └── blog/0002_comments

↓

one composed migration graph
```

## Commands

Enable migrations with the database backend you want to operate:

```sh
cargo run --features postgres,migrations -- show_migrations
cargo run --features sqlite,migrations -- show_migrations
```

Generation is intentionally root-only:

```sh
cargo run --features postgres,migrations -- make_migration add_projects
cargo run --features postgres,migrations -- make_migration placeholder --empty
cargo run --features postgres,migrations -- make_migration --check
cargo run --features postgres,migrations -- make_migration --dry-run
cargo run --features postgres,migrations -- make_migration add_projects --non-interactive
cargo run --features postgres,migrations -- make_migration merge_heads --merge
```

When Gaman needs a decision—for example, whether a removed column was
renamed—`make_migration` prompts on an interactive terminal and reruns the
request with the selected decisions. Use `--non-interactive` in CI or other
non-terminal automation; it never reads standard input and instead reports the
pending clarification and its available choices.

The composed app consumes child crate migrations but does not generate them.
To generate migrations for a reusable crate, run that crate's own migration
setup while developing that crate. This keeps ownership obvious: dependency
crates ship migration history; the root app generates root migrations.

Execution commands operate on the composed migration graph:

```sh
cargo run --features postgres,migrations -- show_migrations
cargo run --features postgres,migrations -- migrate
cargo run --features postgres,migrations -- migrate --plan
cargo run --features postgres,migrations -- migrate --check
cargo run --features postgres,migrations -- migrate --fake
cargo run --features postgres,migrations -- migrate --target auth/0002_sessions
cargo run --features postgres,migrations -- sql_migrate
cargo run --features postgres,migrations -- sql_migrate auth/0002_sessions
cargo run --features postgres,migrations -- sql_migrate auth/0002_sessions --backwards
cargo run --features postgres,migrations -- verify_db
cargo run --features postgres,migrations -- inspect_db
```

No migration runs implicitly during `serve`, `Site::start`, DB pool creation,
or bundle construction. Release binaries can apply and inspect embedded
migrations, but they do not generate new migration files.

## Durable Task Schema Changes

When an application registers task handlers, Vyuh contributes the current
task-runtime schema to migration planning. Task tables are never created or
changed by worker startup. Generate the normal application migration, inspect
its SQL for the selected backend, and commit it with the application.

Current tasks retain JSON results in `last_result`, resume values in
`resume_input`, lineage in `parent_id`/`root_id`, Work/Flow classification, per-step
attempt counts, cancellation state, and private `all`-join metadata. Execution
ownership is nullable UUID `node_id`, referencing `vyuh_task_nodes` with
`ON DELETE SET NULL`. Individual task leases and deployment fingerprints are gone.

For existing deployments, use the backend-specific
[task-protocol migration templates](https://github.com/vivsh/vyuh/tree/main/vyuh/migrations/task_protocol)
for the breaking node-owned pipeline upgrade:

1. Stop **all old workers and writers**, including submit/resume-only processes,
   and back up the database. Mixed-version operation is unsupported.
2. Explicitly accept the destructive reset: **all old tasks, results, checkpoints,
   joins and idempotency reservations are deleted**. Schedule cursors, rate
   balances and lane lifecycle records remain. Review application-added foreign
   keys, triggers and indexes; the templates do not use `CASCADE` to remove them.
3. Integrate `0013_reset_tasks` followed by `0014_node_pipeline` into the
   application's current Gaman history. Adapt dependencies before first use;
   do not modify applied identities or apply a second generated schema change
   that adds the same columns and constraints.
4. Apply the migrations through the ledger. Data changes and their ledger entries
   must commit atomically; MySQL/MariaDB DDL is separate and non-atomic. Keep lane,
   rate, retry, and idempotency configuration unchanged through the upgrade.
   SQLite requires the complete predecessor schema for its table rebuild with
   foreign keys enabled. If MySQL DDL fails, inspect partial changes before retrying.
5. Deploy the new runtime, then restart workers and writers. Startup registers
   nodes and validates configuration; it does not migrate or verify fingerprints.

Historical templates 0001–0012 remain immutable for existing ledgers; they are
not compatibility code in the new runtime. Fresh installations generate only
the desired schema. The reset has no automatic downgrade: restoring a backup
is the only way to recover deleted tasks. No schema changes are applied at startup.

See [Tasks](tasks.md) for current handler APIs and runtime guarantees.

## SQLite

SQLite migrations use the same bundle registration and command surface:

```sh
cargo run --features sqlite,migrations -- make_migration add_tables
cargo run --features sqlite,migrations -- migrate --plan
cargo run --features sqlite,migrations -- migrate
```

Gaman reports unsupported SQLite features as migration errors instead of Vyuh
trying to emulate them. In practice, avoid relying on database schemas,
extensions, enums, stored functions, function-backed triggers, and unsafe table
rebuilds when targeting SQLite.
