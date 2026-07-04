# Database

Vyuh's database subsystem is a thin SQLx-backed layer around database pools,
sessions, query builders, typed row scanning, typed value binding, and database
error mapping.

Vyuh has no default database backend feature. With no backend feature enabled,
it uses SQLite-compatible SQLx aliases and a shared in-memory SQLite default
database URL. This is useful for quick starts, docs, local experiments, and
tests.

Production applications should enable exactly one backend feature. MySQL and
SQLite are supported by the core query-builder and session APIs where SQLx can
express the same behavior. Postgres-only features such as `LISTEN`/`NOTIFY` and
row locking are gated by the `postgres` feature. Durable task storage is
available for Postgres, MySQL, and SQLite, with Postgres recommended for
multi-worker deployments.

## Overview

The main public pieces are:

- `DbConf` and `DbPool` for SQLx pool setup.
- `DBSession` for code that can run against either a pool or transaction.
- `db::from(&Model::table())` for source-only typed queries.
- `db::query(sql)` for raw SQL with Vyuh named placeholders.
- `Statement` for direct SQL with native driver placeholders.
- `Record` for reusable row/value shapes used with builders.
- `Model` for table-backed records with primary-key and schema metadata.
- `DbError` for database error normalization into framework errors and HTTP
  responses.

Typed queries intentionally stay close to SQL while avoiding stringly query
composition. A source provides typed columns, records describe row/value shapes,
and terminal methods execute one SQL statement. Use raw SQL when a query is more
naturally expressed by hand.

## Direct SQLx Access

Vyuh does not replace SQLx. It keeps SQLx as the database foundation and exposes
the underlying pool when direct SQLx is the better tool.

Use `DbPool::as_sqlx()` to reach the active SQLx pool:

```rust
use sqlx::Row as _;
use vyuh::db::DbPool;

# async fn load_count(pool: &DbPool) -> Result<i64, vyuh::db::DbError> {
let row = sqlx::query("SELECT COUNT(*) AS total FROM notes")
    .fetch_one(pool.as_sqlx())
    .await?;
let total: i64 = row.try_get("total")?;
# Ok(total)
# }
```

Use direct SQLx for complex joins, backend-specific SQL, SQLx macros, streaming,
custom JSON aggregation, and queries where the builder would hide more than it
helps. Use Vyuh typed queries when you want source-bound columns, typed
`Record` structs, and code that can run against `DBSession`
implementations such as `DbPool`, transactions, or mocks.

## Backend Features

No backend feature is enabled by default:

```toml
[dependencies]
vyuh = { version = "0.2" }
```

In this lightweight mode, `DbConf::default()` uses a shared in-memory SQLite URL
and tasks use `MemoryTaskStore`. Do not use this mode when the application needs
durable task storage or production database behavior.

Production applications should choose exactly one backend feature:

```toml
[dependencies]
vyuh = { version = "0.2", features = ["postgres"] }
```

Available backend features are:

- `postgres` - enables Postgres SQLx types and Postgres-only helpers.
- `mysql` - enables MySQL SQLx types for the common query/session surface.
- `sqlite` - enables SQLite SQLx types for the common query/session surface.

Compile-time checks reject builds with multiple backend features.

## Configuration

`DbConf` can be built directly, loaded from `DATABASE_URL`, or parsed from a URL
with pool settings in the query string:

```rust
use vyuh::db::{DbConf, DbPool};

# async fn build_pool() -> Result<(), vyuh::db::DbError> {
let conf = DbConf::from_url("postgres://localhost/app?max=20&min=2&lazy=true")?;
let pool = DbPool::from_conf(&conf).await?;
# Ok(())
# }
```

The supported URL options are:

- `max` - maximum pool connections.
- `min` - minimum pool connections.
- `lazy` - whether SQLx should connect lazily.

`Site::db()` returns the site-scoped `DbPool`.

## Models And Derives

Database derives are sugar over direct trait implementations:

- `#[derive(Record)]` implements `db::Record` and `sqlx::FromRow`.
- `#[derive(Model)]` implements the common full-row case: `Record`, table
  identity, primary-key metadata, and migration schema metadata.

Use the derives for ordinary structs and direct trait implementations when a
type needs custom column ordering, nested scanning, or binding behavior.

```rust
use vyuh::prelude::*;

#[derive(Debug, Clone, db::Model)]
#[table(name = "notes")]
struct Note {
    #[column(primary_key)]
    id: i64,
    title: String,
    done: bool,
}
```

Table names are inferred from the struct name as snake_case by default
(`AuditLog` -> `audit_log`). Field names become column names. Optional
Gaman-compatible attributes can override the inferred metadata:

```rust
use vyuh::prelude::*;

#[derive(Debug, Clone, db::Model)]
#[table(name = "auth_users", schema = "auth")]
struct User {
    #[column(primary_key)]
    id: i64,
    #[column(name = "email_address", type = "citext")]
    email: String,
    #[column(nullable)]
    nickname: Option<String>,
    #[column(default = "now()")]
    created_at: chrono::DateTime<chrono::Utc>,
}
```

## Typed Queries

Typed queries keep query construction and row shapes separate:

- A **source** is something valid in SQL `FROM`: a table, CTE, or subquery.
- A **record** is a Rust row/value shape used for scanning or binding values.
- A **projection** is a record used as terminal selected output.
- A **bindable record** is a record used as insert or update values.

`db::from(...)` accepts sources only. Records and projections are not sources,
so they cannot be passed to `db::from(...)` and do not expose query-construction
helpers. Columns and `pick(...)` exist only on source handles.

Typed reads start from a source and end with a terminal method. `Model::table()`
returns a table source, and that source exposes its columns directly:

```rust
use vyuh::prelude::*;

# async fn load<S: db::DBSession>(session: &mut S) -> Result<(), db::DbError> {
# #[derive(Debug, Clone, db::Model)]
# #[table(name = "notes")]
# struct Note { id: i64, title: String, done: bool }
let table = Note::table();

let notes = db::from(&table)
    .filter(table.done.eq(db::val(false)))
    .order_by(table.id.asc())
    .all::<Note>()
    .exec(session)
    .await?;
# Ok(())
# }
```

The query reads row/value metadata from the terminal `Record`. The source table
and terminal projection can differ: `db::from(Post::table()).all::<PostWithAuthor>()`
starts from `posts` and selects the `PostWithAuthor` row shape. Filters and
ordering are built from source-owned typed columns.
Planner errors, bind errors, and placeholder errors are returned by the terminal
async call.

Read terminals:

- `all` for all rows.
- `one` for exactly one row.
- `first` for zero-or-one row.
- `slice(offset, count)` for a single limited query.

Write terminals build executables that return affected rows when run:

```rust
use vyuh::prelude::*;

# #[derive(Debug, Clone, db::Model)]
# #[table(name = "notes")]
# struct Note { id: i64, title: String, done: bool }
#[derive(Debug, Clone, db::Record)]
#[table(name = "notes")]
struct NotePatch {
    done: bool,
}

# async fn save<S: db::DBSession>(session: &mut S, id: i64) -> Result<(), db::DbError> {
let table = Note::table();

db::from(&table)
    .filter(table.id.eq(db::var("id")))
    .bind("id", id)
    .update(&NotePatch { done: true })
    .exec(session)
    .await?;
# Ok(())
# }
```

Other write terminals are:

- `insert(&record)`
- `delete()`
- `batch_insert(&records)`
- `batch_upsert(&records, [conflict_column])`

Each terminal returns an executable. Call `.plan(dialect)` to inspect the SQL or
`.exec(session).await` to run it.

Most typed-query structs are intentionally hidden from the normal API. Let Rust
infer source, column, and executable types from `db::from(...)`, source columns,
and terminal methods. Application code should not name query-scope, executable,
or generated column support types directly.

Borrowed executables are the default. Use `.into_owned()` only when the
operation must be stored beyond the lifetime of its row payload; batch owned
conversion copies every row.

Write CTEs are different from normal write execution. A CTE source cannot borrow
the row payload from a local stack frame, so data-modifying CTE support must be
built from owned executable state: owned rows plus owned bind values that can be
planned and rendered later. Vyuh therefore does not expose write CTE conversion
until that owned path is implemented end-to-end.

Use `returning::<T>()` before a write terminal when the active database supports
`RETURNING`. Vyuh renders this for Postgres and SQLite and rejects unsupported
dialects clearly:

```rust
use vyuh::prelude::*;

# #[derive(Debug, Clone, db::Model)]
# #[table(name = "notes")]
# struct Note { id: i64, title: String, done: bool }
# #[derive(Debug, Clone, db::Record)]
# #[table(name = "notes")]
# struct NewNote { title: String, done: bool }
# async fn save<S: db::DBSession>(session: &mut S) -> Result<(), db::DbError> {
let note = db::from(Note::table())
    .returning::<Note>()
    .insert(&NewNote {
        title: "Release notes".to_string(),
        done: false,
    })
    .exec(session)
    .await?;
# let _: Note = note;
# Ok(())
# }
```

Use `plan(dialect)` when you need offline SQL planning without executing:

```rust
use vyuh::prelude::*;

# #[derive(Debug, Clone, db::Model)]
# #[table(name = "notes")]
# struct Note { id: i64, title: String, done: bool }
let table = Note::table();
let plan = db::from(&table)
    .filter(table.done.eq(db::val(false)))
    .all::<Note>()
    .plan(db::typed::Dialect::Postgres)?;
# let _ = plan;
# Ok::<(), db::QueryError>(())
```

Use `db::meta(&source)` when you need source introspection for diagnostics,
tooling, or tests. Metadata is intentionally separate from query construction:

```rust
use vyuh::prelude::*;

# #[derive(Debug, Clone, db::Model)]
# #[table(name = "notes")]
# struct Note { id: i64, title: String, done: bool }
let table = Note::table();
let meta = db::meta(&table);

assert_eq!(meta.name(), "notes");
assert_eq!(meta.schema(), None);
# Ok::<(), db::QueryError>(())
```

## Implicit Joins

Joined row shapes use flattened root fields and reference metadata. Query code
still starts from the root model table; joins are generated from the terminal
`Record` shape:

```rust
use vyuh::prelude::*;

#[derive(Debug, Clone, db::Model)]
#[table(name = "posts")]
struct Post {
    id: i64,
    user_id: i64,
    title: String,
}

#[derive(Debug, Clone, db::Model)]
#[table(name = "users")]
struct User {
    id: i64,
    email: String,
}

#[derive(Debug, Clone, db::Record)]
struct PostWithAuthor {
    #[column(flatten)]
    post: Post,
    #[column(reference(from = "user_id", to = "id"))]
    author: User,
}
```

Selecting `PostWithAuthor` generates a join from `posts` to `users`:

```rust
use vyuh::prelude::*;

# async fn load<S: db::DBSession>(session: &mut S) -> Result<(), db::DbError> {
# #[derive(Debug, Clone, db::Model)]
# #[table(name = "posts")]
# struct Post { id: i64, user_id: i64, title: String }
# #[derive(Debug, Clone, db::Model)]
# #[table(name = "users")]
# struct User { id: i64, email: String }
# #[derive(Debug, Clone, db::Record)]
# struct PostWithAuthor {
#     #[column(flatten)]
#     post: Post,
#     #[column(reference(from = "user_id", to = "id"))]
#     author: User,
# }
let table = Post::table();

let rows = db::from(&table)
    .filter(table.id.gte(db::val(10_i64)))
    .all::<PostWithAuthor>()
    .exec(session)
    .await?;
# let _: Vec<PostWithAuthor> = rows;
# Ok(())
# }
```

`Option<T>` reference fields use `LEFT JOIN`; required reference fields use
`INNER JOIN`. V1 supports one-column references. Use raw SQL for many-to-many
joins, lateral joins, reporting queries, and custom `ON` expressions.
Reference semantics are intentionally separate from source semantics: the
terminal projection can request references, but query construction still uses
the source handle's columns.

## Explicit Projections

Every record projection describes exactly what the query selects. Keep list,
detail, admin, and export shapes as small `Record` structs instead of relying
on implicit field loading:

```rust
use vyuh::prelude::*;

#[derive(Debug, Clone, db::Model)]
#[table(name = "posts")]
struct Post {
    id: i64,
    author_id: i64,
    title: String,
    body: String,
}

#[derive(Debug, Clone, db::Record)]
struct PostListRow {
    #[column(flatten)]
    post: Post,
}

#[derive(Debug, Clone, db::Record)]
struct PostDetailRow {
    #[column(flatten)]
    post: Post,

    #[column(reference(from = "author_id", to = "id"))]
    author: User,
}

# #[derive(Debug, Clone, db::Model)]
# #[table(name = "users")]
# struct User { id: i64, email: String }
```

Choose the terminal projection that matches the page or API response:

```rust
use vyuh::prelude::*;

# async fn load<S: db::DBSession>(session: &mut S) -> Result<(), db::DbError> {
# #[derive(Debug, Clone, db::Model)]
# #[table(name = "users")]
# struct User { id: i64, email: String }
# #[derive(Debug, Clone, db::Model)]
# #[table(name = "posts")]
# struct Post {
#     id: i64,
#     author_id: i64,
#     title: String,
#     body: String,
# }
# #[derive(Debug, Clone, db::Record)]
# struct PostListRow {
#     #[column(flatten)]
#     post: Post,
# }
# #[derive(Debug, Clone, db::Record)]
# struct PostDetailRow {
#     #[column(flatten)]
#     post: Post,
#     #[column(reference(from = "author_id", to = "id"))]
#     author: User,
# }
let table = Post::table();

let list = db::from(&table)
    .all::<PostListRow>()
    .exec(session)
    .await?;

let detail = db::from(&table)
    .all::<PostDetailRow>()
    .exec(session)
    .await?;
# let _: Vec<PostListRow> = list;
# let _: Vec<PostDetailRow> = detail;
# Ok(())
# }
```

Reference fields determine joins. Required references default to `INNER JOIN`;
canonical `Option<T>` references default to `LEFT JOIN`. Use `join = "inner"` or
`join = "left"` only when the default is not the desired SQL shape.

Projection structs are terminal output shapes. They are not sources and do not
own query columns:

```compile_fail
# use vyuh::prelude::*;
# #[derive(Debug, Clone, db::Model)]
# #[table(name = "posts")]
# struct Post { id: i64, title: String }
#[derive(Debug, Clone, db::Record)]
struct PostListRow {
    #[column(flatten)]
    post: Post,
}

let _ = PostListRow::cols();
let _ = db::from(PostListRow);
let posts = Post::table();
let _ = posts.cols_for::<PostListRow>();
```

## CTEs And Subqueries

Typed select scopes can become CTE or subquery sources. Pick a projected column
explicitly when using a source in an `IN (...)` predicate:

```rust
use vyuh::prelude::*;

# async fn load<S: db::DBSession>(session: &mut S) -> Result<(), db::DbError> {
# #[derive(Debug, Clone, db::Model)]
# #[table(name = "posts")]
# struct Post { id: i64, title: String }
# #[derive(Debug, Clone, db::Model)]
# #[table(name = "comments")]
# struct Comment { id: i64, post_id: i64, flagged: bool }
# #[derive(Debug, Clone, db::Record)]
# #[table(name = "comments")]
# struct CommentPostId { post_id: i64 }
let comments = Comment::table();

let active = db::from(&comments)
    .filter(comments.flagged.eq(db::val(false)))
    .select_expr("post_id", &comments.post_id)
    .all::<CommentPostId>()
    .subquery()?;

let posts = Post::table();

let rows = db::from(&posts)
    .filter(posts.id.in_(active.pick(&active.post_id)))
    .all::<Post>()
    .exec(session)
    .await?;
# let _: Vec<Post> = rows;
# Ok(())
# }
```

CTEs use the same projection model with `.all::<Row>().cte()?` and
`.with(&cte)`.

Current CTE/subquery boundaries are intentional:

- Select executables can become CTEs or subqueries.
- Non-returning writes cannot become sources.
- Returning `insert`, `update`, and batch writes may become write CTEs later,
  but only through an owned executable representation. The CTE must own its bind
  material because it is rendered as part of another statement.
- Returning `delete` remains executable-only for now. It is observable through
  `.plan(dialect)` but is not a reusable source.
- No write operation becomes a plain subquery. Data-modifying writes are
  statement-level CTEs in dialects that support them, not derived table
  expressions.

## Dialects And Extensions

Typed queries render through an explicit dialect layer. Postgres is the most
complete target; SQLite and MySQL support the common query surface and reject
unsupported features clearly. Vyuh does not silently rewrite semantics such as
`ILIKE` for SQLite.

Use `db::func(...)` for reusable database functions:

```rust
use std::borrow::Cow;
use vyuh::prelude::*;

#[derive(Clone)]
struct Unaccent;

impl db::DbFunction<String> for Unaccent {
    fn name(&self, _dialect: db::typed::Dialect) -> Result<Cow<'static, str>, db::QueryError> {
        Ok(Cow::Borrowed("unaccent"))
    }

    fn validate(&self, dialect: db::typed::Dialect, _arity: usize) -> Result<(), db::QueryError> {
        if dialect == db::typed::Dialect::Postgres {
            return Ok(());
        }
        Err(db::QueryError::BindError(
            "unaccent is only supported for postgres".to_string(),
        ))
    }
}

# #[derive(Debug, Clone, db::Model)]
# #[table(name = "posts")]
# struct Post { id: i64, title: String }
# fn build() -> Result<(), db::QueryError> {
let posts = Post::table();

let plan = db::from(&posts)
    .filter(db::func(Unaccent, (&posts.title,)).eq(db::val("release".to_string())))
    .all::<Post>()
    .plan(db::typed::Dialect::Postgres)?;
# let _ = plan;
# Ok(())
# }
```

For SQL that is not a simple function call, implement `db::DbExpression<T>`.
Custom expressions can declare child expressions with `db::FunctionArgs::new`
so normal bind collection, source validation, and dialect validation still run.
These are the intended extension points; internal source, column, projection,
and executable structs are macro-support details.
Use raw SQL through `db::query(...)` when the expression stops being a small,
reusable typed extension.

Dialect-specific helpers live under explicit dialect namespaces. For example,
Postgres-only helpers belong under `db::typed::postgres::*` and still render as
ordinary typed expressions:

```rust
use vyuh::prelude::*;

# #[derive(Debug, Clone, db::Model)]
# #[table(name = "posts")]
# struct Post { id: i64, title: String }
let posts = Post::table();
let plan = db::from(&posts)
    .filter(db::typed::postgres::unaccent(&posts.title).eq(db::val("release".to_string())))
    .all::<Post>()
    .plan(db::typed::Dialect::Postgres)?;
# let _ = plan;
# Ok::<(), db::QueryError>(())
```

## Records Without The Derive

`#[derive(Record)]` and `#[derive(Model)]` are thin: they delegate all metadata
to a pure-Rust builder. `Record` only requires `record_schema()`, and `Model`
only requires `model_schema()`, `PrimaryKey`, and `primary_key()`. Everything
else is a default method that reads the schema. You can therefore describe a
record by hand with the same fluent API the macros use:

```rust
use vyuh::prelude::*;

struct Note {
    id: i64,
    title: String,
}

impl db::Record for Note {
    fn record_schema() -> db::RecordSchema<Self> {
        db::RecordSchema::new("notes")
            .column("id")
            .column("title")
            .bind_columns(vec!["id".to_string(), "title".to_string()])
    }

    fn record_scan_ordered(
        row: &db::Row,
        start_idx: &mut usize,
    ) -> Result<Self, sqlx::Error> {
        use sqlx::Row as _;
        let note = Note {
            id: row.try_get(*start_idx)?,
            title: row.try_get(*start_idx + 1)?,
        };
        *start_idx += 2;
        Ok(note)
    }
}
```

Row scanning and value binding stay type-directed (Rust needs the concrete field
types to encode and decode), so the derive generates those bodies for you. The
builder also exposes `.reference(...)`, `.root(...)`, and `.schema(...)` for
joined shapes.

## Raw SQL

Use `db::query(sql)` when hand-written SQL is clearer than a builder but named
placeholders and typed fetch methods are still useful:

```rust
use vyuh::prelude::*;

# async fn count<S: db::DBSession>(session: &mut S) -> Result<i64, db::DbError> {
let total = db::query("SELECT COUNT(*) FROM notes WHERE done = :done")
    .bind("done", false)
    .scalar::<i64>(session)
    .await?;
# Ok(total)
# }
```

Named placeholders are resolved to the active backend's placeholder syntax at
execution time. Missing placeholders return a `QueryError`.

## Direct Statements

Use `Statement` when hand-written SQL is clearer than a builder but you still
want to execute through the `DBSession` abstraction:

```rust
use vyuh::prelude::*;

# async fn count<S: db::DBSession>(session: &mut S) -> Result<i64, db::DbError> {
let total: i64 = session
    .fetch_scalar(db::Statement::from_str("SELECT COUNT(*) FROM notes WHERE done = $1").bind(false))
    .await?;
# Ok(total)
# }
```

`Statement` is intentionally low-level. Placeholder syntax in raw SQL is the
database driver's syntax, not Vyuh's named-placeholder syntax.

## Sessions And Transactions

Query code should usually accept `impl DBSession`. That lets the same function
run against a `DbPool`, a transaction, or the mock DB session used in tests.

```rust
use vyuh::prelude::*;

# #[derive(Debug, db::Record)]
# #[table(name = "todos")]
# struct NewTodo { title: String }
# #[derive(Debug, db::Model)]
# #[table(name = "todos")]
# struct Todo { id: i64, title: String }
async fn create_todo<S: db::DBSession>(session: &mut S, title: String) -> Result<u64, db::DbError> {
    db::from(&Todo::table())
        .insert(&NewTodo { title })
        .exec(session)
        .await
}
```

Transactions are started from `DbPool::begin()` and implement `DBSession`.

## Mock Sessions

`vyuh::db::mock::MockDBSession` records SQL and returns planned responses. It is
useful for testing query construction without a live database.

```rust
use vyuh::prelude::*;
use vyuh::db::mock::MockDBSession;

# async fn test_query() -> Result<(), db::DbError> {
let mut db = MockDBSession::new();
db.plan_execute_ok("INSERT INTO notes", 1);

# #[derive(Debug, Clone, db::Model)]
# #[table(name = "notes")]
# struct Note { id: i64, title: String, done: bool }
#[derive(Debug, Clone, db::Record)]
#[table(name = "notes")]
struct NewNote { title: String, done: bool }

let rows = db::from(&Note::table())
    .insert(&NewNote { title: "Ship it".to_string(), done: false })
    .exec(&mut db)
    .await?;
assert_eq!(rows, 1);
# Ok(())
# }
```

## Examples

The snippets in this chapter cover typed `Record` reads and writes, generated
joins, CTE/subquery sources, raw queries, direct SQLx access through `DbPool`,
and transactions.

## Failure Modes

- Columns from unknown sources or undeclared references return a `QueryError`.
- Missing row data for `insert` or `update` returns a bind error.
- Empty bulk inserts are rejected.
- Missing named placeholder values return a placeholder error.
- SQLx row-not-found errors map to `DbError::DoesNotExist`.
- SQLx database constraint errors map to `DbError::Integrity`.
- Backend-specific helpers return `DbError::Unsupported` when unavailable.

## Current Limitations

- DB derives do not form a full ORM; complex joins and relationship loading
  remain explicit SQL/query-builder work.
- Raw `Statement` SQL uses native SQLx placeholder syntax.
- Postgres-only helpers are intentionally not emulated on MySQL or SQLite.
