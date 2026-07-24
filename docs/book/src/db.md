# Database

Vyuh's database subsystem is a compatibility facade over Mool's database and
migration APIs. Mool owns pools, sessions, query builders, typed row scanning,
typed value binding, database error mapping, and migration execution.

Vyuh exposes this functionality through `vyuh::db`. The underlying database
toolkit lives in the standalone Mool crate, but Vyuh applications should keep
using the framework facade unless they are intentionally depending on Mool
outside a Vyuh app.

Vyuh has no default database backend feature. In this backendless mode it has
no live SQL dialect or pool and tasks use `MemoryTaskStore`. This is useful for
quick starts, docs, local experiments, and tests that do not need database
behavior.

Production applications should enable exactly one backend feature. PostgreSQL
and SQLite are Vyuh 0.3 production targets; PostgreSQL is the clustered target
and SQLite is local/single-process only. MySQL compiles but is experimental.
Postgres-only features such as `LISTEN`/`NOTIFY` and row locking are gated by
the `postgres` feature.

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

In this lightweight mode, `DbConf::default()` intentionally has no database
URL and `DbPool` is disabled. Tasks use `MemoryTaskStore`. Do not use this mode
when the application needs durable task storage or production database behavior.

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

### Filters

`Filterable` is the typed-query-native way to reuse WHERE predicates from
request/query DTOs. A filter is bound to one model and only appends predicates
to the current query scope. It does not order, paginate, load relations, execute
queries, or produce extra SQL statements.

```rust
use vyuh::prelude::*;

# #[derive(Debug, Clone, db::Model)]
# #[table(name = "posts")]
# struct Post {
#     id: i64,
#     title: String,
#     published: bool,
#     #[column(type = "timestamptz")]
#     created_at: chrono::DateTime<chrono::Utc>,
# }
#[derive(Debug, Clone, db::Filterable)]
#[filter(model = Post)]
struct PostFilter {
    #[filter(op = "eq")]
    published: Option<bool>,

    #[filter(op = "ilike", column = "title")]
    q: Option<String>,

    #[filter(op = "gte", column = "created_at")]
    created_after: Option<chrono::DateTime<chrono::Utc>>,

    #[filter(op = "in", column = "id")]
    ids: Vec<i64>,
}

# async fn load<S: db::DBSession>(
#     session: &mut S,
#     filter: PostFilter,
# ) -> Result<Vec<Post>, db::DbError> {
let posts = Post::table();
let rows = db::from(&posts)
    .filter_with(&filter)
    .all::<Post>()
    .exec(session)
    .await?;
# Ok(rows)
# }
```

`Option<T>` fields emit no predicate when `None`. `Vec<T>` and
`Option<Vec<T>>` fields with `#[filter(op = "in")]` emit no predicate when the list is
empty. The `column = "field"` target is checked against the model's generated
typed columns, so `column = "missing"` fails during compilation.

`Filterable` remains intentionally narrow. Relation-aware filters, ordering,
and pagination belong in separate query-spec APIs rather than this WHERE-only
trait.

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
let id_var = db::var::<i64>().named("id");

db::from(&table)
    .filter(table.id.eq(&id_var))
    .bind(&id_var, id)
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

`update(...)` and `delete()` intentionally require at least one `filter(...)`.
Vyuh does not render unfiltered update/delete statements through typed queries;
use raw SQL only when a whole-table mutation is deliberate and reviewed.

Each terminal returns an executable. Call `.plan(dialect)` to inspect the SQL or
`.exec(session).await` to run it.

Simple writes pass a record directly. When a write needs computed values, call
`set(...)` after the write terminal:

```rust
use vyuh::prelude::*;

# #[derive(Debug, Clone, db::Model)]
# #[table(name = "notes")]
# struct Note { id: i64, title: String, done: bool, updated_at: chrono::DateTime<chrono::Utc> }
#[derive(Debug, Clone, db::Record)]
#[table(name = "notes")]
struct NotePatch {
    title: String,
}

# async fn save<S: db::DBSession>(session: &mut S, id: i64) -> Result<(), db::DbError> {
let notes = Note::table();
let id_var = db::var::<i64>().named("id");
let patch = NotePatch {
    title: "Updated title".to_string(),
};

db::from(&notes)
    .filter(notes.id.eq(&id_var))
    .bind(&id_var, id)
    .update(&patch)
    .set(&notes.updated_at, db::funcs::now())
    .exec(session)
    .await?;
# Ok(())
# }
```

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
`RETURNING`. Add `set(...)` after `returning::<T>()` when returned fields need
computed expressions. Vyuh renders this for Postgres and SQLite and rejects
unsupported dialects clearly:

```rust
use vyuh::{db::backend::ReturningExt, prelude::*};

# #[derive(Debug, Clone, db::Model)]
# #[table(name = "notes")]
# struct Note { id: i64, title: String, done: bool }
# #[derive(Debug, Clone, db::Record)]
# #[table(name = "notes")]
# struct NewNote { title: String, done: bool }
# async fn save<S: db::DBSession>(session: &mut S) -> Result<(), db::DbError> {
let id_hint = db::var::<i64>().named("id_hint");
let title = db::var::<String>().named("title");
let out = db::out::<Note>();
let note = db::from(Note::table())
    .returning::<Note>()
    .set(&out.id, &id_hint)
    .set(&out.title, &title)
    .insert(&NewNote {
        title: "Release notes".to_string(),
        done: false,
    })
    .bind(&id_hint, 1_i64)
    .bind(&title, "Release notes".to_string())
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
    .plan(db::queries::Dialect::Postgres)?;
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
    #[column(reference(on(from = "user_id", to = "id")))]
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
#     #[column(reference(on(from = "user_id", to = "id")))]
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
`INNER JOIN`. Multiple `on(from, to)` pairs are joined with `AND` for composite
equality joins:

```rust
#[derive(Debug, Clone, db::Record)]
struct TenantPostWithAuthor {
    #[column(flatten)]
    post: Post,

    #[column(reference(
        on(from = "author_id", to = "id"),
        on(from = "tenant_id", to = "tenant_id")
    ))]
    author: User,
}
```

Attribute references intentionally stay equality-only. Use raw SQL for arbitrary
`ON` predicates until trait-backed custom join predicates are wired into
projection rendering.
Reference semantics are separate from source semantics: the terminal projection
can request joins, but query construction still uses the source handle's
columns.

### Backrefs And Relation Filters

Forward FK metadata can declare a reverse marker:

```rust
struct UserPosts;

#[derive(Debug, Clone, db::Model)]
struct Post {
    id: i64,

    #[column(references = "users.id", backref = UserPosts)]
    author_id: i64,

    title: String,
}
```

The marker implements `Backref`, either by macro-generated code later or by hand
when the relation is more deliberate. Backrefs are not query sources. They are
typed relation paths used inside predicates:

```rust
use vyuh::db::backend::TextSearchExt;

let users = User::table();
let posts = db::backref::<UserPosts>(&users);

let rows = db::from(&users)
    .filter(posts.any(|post| post.title.ilike(db::val("%vyuh%".to_string()))))
    .all::<User>()
    .exec(session)
    .await?;
```

`any(...)` renders a correlated `EXISTS` inside the same SQL statement.
`none()` means no related rows at all; use `.any(...).not()` for “none matching
this predicate”.

Many-to-many relations are explicit and use two joins: parent-to-through and
through-to-target. They also render as correlated `EXISTS` predicates and remain
separate from `db::from(...)` sources.

Relation handles also expose aggregate expressions:

```rust
let users = User::table();
let posts = db::backref::<UserPosts>(&users);

let active = db::from(&users)
    .filter(posts.count().gt(db::val(0_i64)))
    .all::<User>();
```

### Explicit Prefetch

Prefetch is explicit and separate from normal query execution. It is another
executable, so it issues its own SQL statement rather than hiding work inside a
read terminal.

```rust
let users = db::from(&User::table())
    .all::<User>()
    .exec(session)
    .await?;

let users = db::prefetch::<UserPosts>(users)
    .exec(session)
    .await?;
```

`#[column(prefetch = UserPosts)]` fields may appear on models or records. They
are application-side relation state: ignored for migrations, binding, scanning
columns, returning, and typed source columns. Normal scans initialize `Vec<T>`
prefetch fields as empty; explicit prefetch populates them. V1 supports many
backrefs with `Vec<T>` fields.

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

    #[column(reference(on(from = "author_id", to = "id")))]
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
#     #[column(reference(on(from = "author_id", to = "id")))]
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

Derived output fields are assigned after the read terminal with `db::out::<T>()`
and flat `set(...)` calls. Assignment targets are passed by reference for both
output columns and source columns.
The terminal projection still decides the output record:

```rust
use vyuh::prelude::*;

# async fn load<S: db::DBSession>(session: &mut S) -> Result<(), db::DbError> {
# #[derive(Debug, Clone, db::Model)]
# #[table(name = "comments")]
# struct Comment { id: i64, post_id: i64 }
#[derive(Debug, Clone, db::Record)]
#[table(name = "comments")]
struct CommentCount {
    post_id: i64,
    comment_count: i64,
}

let comments = Comment::table();
let out = db::out::<CommentCount>();

let rows = db::from(&comments)
    .group_by(&comments.post_id)
    .all::<CommentCount>()
    .set(&out.post_id, &comments.post_id)
    .set(&out.comment_count, db::funcs::count(&comments.id))
    .exec(session)
    .await?;
# let _: Vec<CommentCount> = rows;
# Ok(())
# }
```

## Aggregate Functions

Portable aggregate helpers live under `db::funcs`:

```rust
db::funcs::count(&comments.id)
db::funcs::count_all()
db::funcs::sum(&comments.id)
db::funcs::avg(&comments.id)
db::funcs::min(&comments.id)
db::funcs::max(&comments.id)
```

They can be used in grouped projections, scalar reads, relation aggregates, and
window expressions with `.over(...)`.

## Conditional Expressions

Portable conditional helpers also live under `db::funcs`.
`coalesce(expr, fallback)` keeps the same expression type, while
`case().when(...).else_(...)` builds a typed SQL `CASE` expression:

```rust
use vyuh::prelude::*;

# async fn load<S: db::DBSession>(session: &mut S) -> Result<(), db::DbError> {
# #[derive(Debug, Clone, db::Model)]
# #[table(name = "posts")]
# struct Post { id: i64, title: String, published: bool }
#[derive(Debug, Clone, db::Record)]
#[table(name = "posts")]
struct PostLabel {
    id: i64,
    label: String,
}

let posts = Post::table();
let out = db::out::<PostLabel>();

let rows = db::from(&posts)
    .all::<PostLabel>()
    .set(&out.id, db::funcs::coalesce(&posts.id, db::val(0_i64)))
    .set(
        &out.label,
        db::funcs::case()
            .when(posts.published.eq(db::val(true)), db::val("Published".to_string()))
            .else_(db::val("Draft".to_string())),
    )
    .exec(session)
    .await?;
# let _: Vec<PostLabel> = rows;
# Ok(())
# }
```

## JSON Columns

JSON storage is declared on normal Rust fields. The field keeps its Rust type
for scanning and binding, while typed queries expose the SQL expression as
`db::types::Json`:

```rust
use serde::{Deserialize, Serialize};
use vyuh::prelude::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PostMeta {
    status: String,
}

#[derive(Debug, Clone, db::Model)]
#[table(name = "posts")]
struct Post {
    id: i64,
    title: String,
    #[column(type = "jsonb")]
    meta: PostMeta,
}
```

JSON query helpers live under `db::funcs::json`:

```rust
use vyuh::prelude::*;

# #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
# struct PostMeta { status: String }
# #[derive(Debug, Clone, db::Model)]
# #[table(name = "posts")]
# struct Post { id: i64, title: String, #[column(type = "jsonb")] meta: PostMeta }
# fn build() {
let posts = Post::table();

let _query = db::from(&posts)
    .filter(db::funcs::json::text(&posts.meta, "status").eq(db::val("published".to_string())))
    .all::<Post>();
# }
```

The portable JSON helpers are `get`, `text`, `exists`, `json_type`, and
`array_length`. PostgreSQL JSONB-only helpers live under
`db::funcs::json::postgres`, such as `contains`.

```rust
# use vyuh::prelude::*;
# #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
# struct PostMeta { status: String }
# #[derive(Debug, Clone, db::Model)]
# #[table(name = "posts")]
# struct Post { id: i64, title: String, #[column(type = "jsonb")] meta: PostMeta }
# fn build() {
let posts = Post::table();

let _query = db::from(&posts)
    .filter(db::funcs::json::postgres::contains(
        &posts.meta,
        db::funcs::json::value(serde_json::json!({ "status": "published" })),
    ))
    .all::<Post>();
# }
```

JSON functions only accept JSON-marked expressions. A regular `String` column
does not compile with `db::funcs::json::*`, which keeps JSON operators from
leaking onto unrelated SQL types.

## Array Columns

SQL arrays use normal Rust `Vec<T>` fields. No array annotation is required for
typed-query columns:

```rust
use vyuh::prelude::*;

#[derive(Debug, Clone, db::Model)]
#[table(name = "posts")]
struct Post {
    id: i64,
    title: String,
    tags: Vec<String>,
    scores: Option<Vec<i64>>,
}
```

The generated query columns are SQL array expressions:

```rust
use vyuh::prelude::*;

# #[derive(Debug, Clone, db::Model)]
# #[table(name = "posts")]
# struct Post { id: i64, title: String, tags: Vec<String>, scores: Option<Vec<i64>> }
# fn build() {
let posts = Post::table();

let _query = db::from(&posts)
    .filter(db::funcs::array::contains(
        &posts.tags,
        db::funcs::array::value(vec!["rust".to_string()]),
    ))
    .filter(db::funcs::array::overlaps(
        &posts.tags,
        db::funcs::array::value(vec!["vyuh".to_string(), "rust".to_string()]),
    ))
    .all::<Post>();
# }
```

Array helpers live under `db::funcs::array`. The initial portable surface is
Postgres-native: `contains`, `contained_by`, `overlaps`, `is_empty`, `length`,
`cardinality`, `position`, `any`, `all`, and `value`. SQLite and MySQL planning
return clear errors for these helpers because they do not have the same native
SQL array semantics.

## Window Functions

Window functions are typed expressions for derived read projections. They render
on Postgres, modern SQLite, and MySQL 8+. Vyuh does not currently inspect the
server version, so older SQLite/MySQL runtimes may still reject the SQL.

```rust
use vyuh::prelude::*;

# async fn load<S: db::DBSession>(session: &mut S) -> Result<(), db::DbError> {
# #[derive(Debug, Clone, db::Model)]
# #[table(name = "posts")]
# struct Post { id: i64, author_id: i64, created_at: chrono::DateTime<chrono::Utc> }
#[derive(Debug, Clone, db::Record)]
#[table(name = "posts")]
struct PostRank {
    id: i64,
    rank: i64,
}

let posts = Post::table();
let out = db::out::<PostRank>();

let rows = db::from(&posts)
    .all::<PostRank>()
    .set(&out.id, &posts.id)
    .set(
        &out.rank,
        db::funcs::row_number().over(
            db::funcs::window()
                .partition_by(&posts.author_id)
                .order_by(posts.created_at.desc()),
        ),
    )
    .exec(session)
    .await?;
# let _: Vec<PostRank> = rows;
# Ok(())
# }
```

Supported portable helpers cover the common analytics cases:

- Ranking: `row_number`, `rank`, `dense_rank`, `percent_rank`, `cume_dist`,
  and `ntile`.
- Offset and value: `lag`, `lag_by`, `lag_or`, `lead`, `lead_by`, `lead_or`,
  `first_value`, `last_value`, and `nth_value`.
- Aggregates: `count`, `sum`, `avg`, `min`, and `max` with `.over(...)`.
- Frames: `rows_between`, `range_between`, `unbounded_preceding`,
  `preceding`, `current_row`, `following`, and `unbounded_following`.

Frames use typed expressions for offsets:

```rust
# use vyuh::prelude::*;
# #[derive(Debug, Clone, db::Model)]
# #[table(name = "posts")]
# struct Post { id: i64, author_id: i64, created_at: chrono::DateTime<chrono::Utc> }
# #[derive(Debug, Clone, db::Record)]
# #[table(name = "posts")]
# struct PostStats { id: i64, running_id: i64 }
let posts = Post::table();
let out = db::out::<PostStats>();

let _query = db::from(&posts)
    .all::<PostStats>()
    .set(
        out.running_id,
        db::funcs::sum(&posts.id).over(
            db::funcs::window()
                .partition_by(&posts.author_id)
                .order_by(posts.created_at.asc())
                .rows_between(db::funcs::unbounded_preceding(), db::funcs::current_row()),
        ),
    );
```

Window expressions are accepted in selected output and read ordering. SQL does
not allow them in `WHERE`, `GROUP BY`, `HAVING`, or mutation assignments. Filter
on a window value by projecting it into a subquery or CTE first:

```rust
# use vyuh::prelude::*;
# #[derive(Debug, Clone, db::Model)]
# #[table(name = "posts")]
# struct Post { id: i64, created_at: chrono::DateTime<chrono::Utc> }
# #[derive(Debug, Clone, db::Record)]
# #[table(name = "posts")]
# struct PostRank { id: i64, rank: i64 }
let posts = Post::table();
let out = db::out::<PostRank>();

let ranked = db::from(&posts)
    .all::<PostRank>()
    .set(&out.id, &posts.id)
    .set(
        &out.rank,
        db::funcs::row_number().over(db::funcs::window().order_by(posts.created_at.desc())),
    )
    .subquery()?;

let _top = db::from(&ranked)
    .filter(ranked.rank.eq(db::val(1_i64)))
    .all::<PostRank>();
# Ok::<(), db::DbError>(())
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
let out = db::out::<CommentPostId>();

let active = db::from(&comments)
    .filter(comments.flagged.eq(db::val(false)))
    .all::<CommentPostId>()
    .set(&out.post_id, &comments.post_id)
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

Tables can be picked directly when the unfiltered source is enough:

```rust
use vyuh::prelude::*;

# async fn load<S: db::DBSession>(session: &mut S) -> Result<(), db::DbError> {
# #[derive(Debug, Clone, db::Model)]
# #[table(name = "posts")]
# struct Post { id: i64, author_id: i64, title: String }
# #[derive(Debug, Clone, db::Model)]
# #[table(name = "users")]
# struct User { id: i64, active: bool }
let posts = Post::table();
let users = User::table();

let rows = db::from(&posts)
    .filter(posts.author_id.in_(users.pick(&users.id)))
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

## Set Operations

Typed read executables with the same projection type can be combined with
`union(...)`, `union_all(...)`, and `except(...)`:

```rust
use vyuh::prelude::*;

# async fn load<S: db::DBSession>(session: &mut S) -> Result<(), db::DbError> {
# #[derive(Debug, Clone, db::Model)]
# #[table(name = "posts")]
# struct Post { id: i64, title: String, published: bool }
let posts = Post::table();

let published = db::from(&posts)
    .filter(posts.published.eq(db::val(true)))
    .all::<Post>();

let drafts = db::from(&posts)
    .filter(posts.published.eq(db::val(false)))
    .all::<Post>();

let rows = published
    .union_all(drafts)
    .exec(session)
    .await?;
# let _: Vec<Post> = rows;
# Ok(())
# }
```

Set operations remain one executable and one SQL statement. In this pass, set
operands must be plain read executables: local operand CTEs and operand
`order_by(...)` are rejected because their grammar differs across backends.
Whole-set ordering and pagination are intentionally deferred.

## Dialects And Extensions

Typed queries render through an explicit dialect layer. Postgres is the most
complete target; SQLite and MySQL support the common query surface and reject
unsupported features clearly. Vyuh does not silently rewrite semantics such as
`ILIKE` for SQLite.

Use `db::funcs::func(...)` for reusable database functions:

```rust
use std::borrow::Cow;
use vyuh::prelude::*;

#[derive(Clone)]
struct Unaccent;

impl db::DbFunction<String> for Unaccent {
    fn name(&self, _dialect: db::queries::Dialect) -> Result<Cow<'static, str>, db::QueryError> {
        Ok(Cow::Borrowed("unaccent"))
    }

    fn validate(&self, dialect: db::queries::Dialect, _arity: usize) -> Result<(), db::QueryError> {
        if dialect == db::queries::Dialect::Postgres {
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
    .filter(db::funcs::func(Unaccent, (&posts.title,)).eq(db::val("release".to_string())))
    .all::<Post>()
    .plan(db::queries::Dialect::Postgres)?;
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
Postgres-only helpers belong under `db::funcs::postgres::*` and still render as
ordinary typed expressions:

```rust
use vyuh::prelude::*;

# #[derive(Debug, Clone, db::Model)]
# #[table(name = "posts")]
# struct Post { id: i64, title: String }
let posts = Post::table();
let plan = db::from(&posts)
    .filter(db::funcs::postgres::unaccent(&posts.title).eq(db::val("release".to_string())))
    .all::<Post>()
    .plan(db::queries::Dialect::Postgres)?;
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

- Typed queries are not a raw-SQL replacement. Use `db::query(...)` for
  reporting SQL, lateral joins, vendor hints, and other advanced grammar not
  represented by typed expressions.
- Set operations currently combine plain read executables only. Whole-set
  ordering/pagination and operand-local CTEs are deferred.
- Prefetch is explicit and narrow in v1: many-side loading only, no nested
  prefetch, and no implicit relation loading.
- Raw `Statement` SQL uses native SQLx placeholder syntax.
- Postgres-only helpers are intentionally not emulated on MySQL or SQLite; the
  dialect layer fails clearly when a feature is not supported.
