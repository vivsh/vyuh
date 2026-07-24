#![cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]

use std::borrow::Cow;
use std::hash::Hash;

use vyuh::prelude::*;

#[derive(Debug)]
struct PostRow;

#[allow(dead_code)]
#[derive(Clone)]
struct PostRowCols {
    id: db::queries::__private::ProjectedColumn<i64>,
    title: db::queries::__private::ProjectedColumn<String>,
    comment_count: db::queries::__private::ProjectedColumn<i64>,
}

#[allow(dead_code)]
#[derive(Clone)]
struct PostRowOut {
    id: db::queries::__private::OutputColumn<i64>,
    title: db::queries::__private::OutputColumn<String>,
    comment_count: db::queries::__private::OutputColumn<i64>,
}

impl db::Record for PostRow {
    fn record_schema() -> db::RecordSchema<Self> {
        db::RecordSchema::new("posts").root("post").columns(
            ["post.id", "post.title", "comment_count"]
                .into_iter()
                .map(str::to_string)
                .collect(),
        )
    }

    fn record_scan_ordered(_row: &db::Row, _start_idx: &mut usize) -> Result<Self, sqlx::Error> {
        Ok(Self)
    }

    fn record_scan_unordered(_row: &db::Row) -> Result<Self, sqlx::Error> {
        Ok(Self)
    }
}

impl db::queries::__private::Projectable for PostRow {
    type Columns = PostRowCols;

    fn projected_columns(source: db::queries::__private::ProjectionSource) -> Self::Columns {
        PostRowCols {
            id: source.col("id"),
            title: source.col("title"),
            comment_count: source.col("comment_count"),
        }
    }
}

impl db::queries::__private::HasOutputCols for PostRow {
    type OutputColumns = PostRowOut;

    fn output_columns(source: db::queries::__private::OutputSource) -> Self::OutputColumns {
        PostRowOut {
            id: source.col("id"),
            title: source.col("title"),
            comment_count: source.col("comment_count"),
        }
    }
}

#[derive(Debug)]
struct PostWithAuthor;

#[allow(dead_code)]
#[derive(Clone)]
struct PostWithAuthorOut {
    id: db::queries::__private::OutputColumn<i64>,
    title: db::queries::__private::OutputColumn<String>,
    display_name: db::queries::__private::OutputColumn<String>,
    comment_count: db::queries::__private::OutputColumn<i64>,
}

impl db::Record for PostWithAuthor {
    fn record_schema() -> db::RecordSchema<Self> {
        db::RecordSchema::new("posts")
            .root("post")
            .references(vec![db::ReferenceMeta {
                logical_name: "author",
                table_name: "users",
                table_schema: None,
                columns: &[db::JoinColumn {
                    from: "post.author_id",
                    to: "id",
                }],
                join_type: db::JoinType::Inner,
            }])
            .columns(
                [
                    "post.id",
                    "post.title",
                    "author.display_name",
                    "comment_count",
                ]
                .into_iter()
                .map(str::to_string)
                .collect(),
            )
    }

    fn record_scan_ordered(_row: &db::Row, _start_idx: &mut usize) -> Result<Self, sqlx::Error> {
        Ok(Self)
    }

    fn record_scan_unordered(_row: &db::Row) -> Result<Self, sqlx::Error> {
        Ok(Self)
    }
}

impl db::queries::__private::HasOutputCols for PostWithAuthor {
    type OutputColumns = PostWithAuthorOut;

    fn output_columns(source: db::queries::__private::OutputSource) -> Self::OutputColumns {
        PostWithAuthorOut {
            id: source.col("post.id"),
            title: source.col("post.title"),
            display_name: source.col("author.display_name"),
            comment_count: source.col("comment_count"),
        }
    }
}

#[derive(Debug)]
struct PostIdRow;

#[derive(Clone)]
struct PostIdCols {
    id: db::queries::__private::ProjectedColumn<i64>,
}

#[allow(dead_code)]
#[derive(Clone)]
struct PostIdOut {
    id: db::queries::__private::OutputColumn<i64>,
}

impl db::Record for PostIdRow {
    fn record_schema() -> db::RecordSchema<Self> {
        db::RecordSchema::new("posts")
            .root("post")
            .columns(vec!["post.id".to_string()])
    }

    fn record_scan_ordered(_row: &db::Row, _start_idx: &mut usize) -> Result<Self, sqlx::Error> {
        Ok(Self)
    }

    fn record_scan_unordered(_row: &db::Row) -> Result<Self, sqlx::Error> {
        Ok(Self)
    }
}

impl db::queries::__private::Projectable for PostIdRow {
    type Columns = PostIdCols;

    fn projected_columns(source: db::queries::__private::ProjectionSource) -> Self::Columns {
        PostIdCols {
            id: source.col("id"),
        }
    }
}

impl db::queries::__private::HasOutputCols for PostIdRow {
    type OutputColumns = PostIdOut;

    fn output_columns(source: db::queries::__private::OutputSource) -> Self::OutputColumns {
        PostIdOut {
            id: source.col("post.id"),
        }
    }
}

#[derive(Debug)]
struct CommentPostIdRow;

#[derive(Clone)]
struct CommentPostIdCols {
    post_id: db::queries::__private::ProjectedColumn<i64>,
}

#[allow(dead_code)]
#[derive(Clone)]
struct CommentPostIdOut {
    post_id: db::queries::__private::OutputColumn<i64>,
}

impl db::Record for CommentPostIdRow {
    fn record_schema() -> db::RecordSchema<Self> {
        db::RecordSchema::new("comments")
            .root("comment")
            .columns(vec!["comment.post_id".to_string()])
    }

    fn record_scan_ordered(_row: &db::Row, _start_idx: &mut usize) -> Result<Self, sqlx::Error> {
        Ok(Self)
    }

    fn record_scan_unordered(_row: &db::Row) -> Result<Self, sqlx::Error> {
        Ok(Self)
    }
}

impl db::queries::__private::Projectable for CommentPostIdRow {
    type Columns = CommentPostIdCols;

    fn projected_columns(source: db::queries::__private::ProjectionSource) -> Self::Columns {
        CommentPostIdCols {
            post_id: source.col("post_id"),
        }
    }
}

impl db::queries::__private::HasOutputCols for CommentPostIdRow {
    type OutputColumns = CommentPostIdOut;

    fn output_columns(source: db::queries::__private::OutputSource) -> Self::OutputColumns {
        CommentPostIdOut {
            post_id: source.col("comment.post_id"),
        }
    }
}

#[derive(Debug)]
struct CommentCountRow;

#[allow(dead_code)]
#[derive(Clone)]
struct CommentCountCols {
    post_id: db::queries::__private::ProjectedColumn<i64>,
    comment_count: db::queries::__private::ProjectedColumn<i64>,
}

#[allow(dead_code)]
#[derive(Clone)]
struct CommentCountOut {
    post_id: db::queries::__private::OutputColumn<i64>,
    comment_count: db::queries::__private::OutputColumn<i64>,
}

impl db::Record for CommentCountRow {
    fn record_schema() -> db::RecordSchema<Self> {
        db::RecordSchema::new("comments").root("comment").columns(
            ["post_id", "comment_count"]
                .into_iter()
                .map(str::to_string)
                .collect(),
        )
    }

    fn record_scan_ordered(_row: &db::Row, _start_idx: &mut usize) -> Result<Self, sqlx::Error> {
        Ok(Self)
    }

    fn record_scan_unordered(_row: &db::Row) -> Result<Self, sqlx::Error> {
        Ok(Self)
    }
}

impl db::queries::__private::Projectable for CommentCountRow {
    type Columns = CommentCountCols;

    fn projected_columns(source: db::queries::__private::ProjectionSource) -> Self::Columns {
        CommentCountCols {
            post_id: source.col("post_id"),
            comment_count: source.col("comment_count"),
        }
    }
}

impl db::queries::__private::HasOutputCols for CommentCountRow {
    type OutputColumns = CommentCountOut;

    fn output_columns(source: db::queries::__private::OutputSource) -> Self::OutputColumns {
        CommentCountOut {
            post_id: source.col("post_id"),
            comment_count: source.col("comment_count"),
        }
    }
}

#[derive(Debug)]
struct PostWithCounts;

impl db::Record for PostWithCounts {
    fn record_schema() -> db::RecordSchema<Self> {
        db::RecordSchema::new("posts")
            .root("post")
            .references(vec![db::ReferenceMeta {
                logical_name: "counts",
                table_name: "comment_counts",
                table_schema: None,
                columns: &[db::JoinColumn {
                    from: "post.id",
                    to: "post_id",
                }],
                join_type: db::JoinType::Left,
            }])
            .columns(
                ["post.id", "post.title", "counts.comment_count"]
                    .into_iter()
                    .map(str::to_string)
                    .collect(),
            )
    }

    fn record_scan_ordered(_row: &db::Row, _start_idx: &mut usize) -> Result<Self, sqlx::Error> {
        Ok(Self)
    }

    fn record_scan_unordered(_row: &db::Row) -> Result<Self, sqlx::Error> {
        Ok(Self)
    }
}

#[derive(Clone)]
struct NewPost {
    title: String,
    view_count: i64,
}

impl db::Record for NewPost {
    fn record_schema() -> db::RecordSchema<Self> {
        db::RecordSchema::new("posts").bind_columns(
            ["title", "view_count"]
                .into_iter()
                .map(str::to_string)
                .collect(),
        )
    }

    fn record_bind_values(&self, args: &mut db::Arguments<'static>) -> Result<(), sqlx::Error> {
        use sqlx::Arguments as _;

        args.add(self.title.clone())
            .map_err(|err| sqlx::Error::Protocol(err.to_string()))?;
        args.add(self.view_count)
            .map_err(|err| sqlx::Error::Protocol(err.to_string()))?;
        Ok(())
    }
}

#[derive(Clone)]
struct PostWithId {
    id: i64,
    title: String,
}

impl db::Record for PostWithId {
    fn record_schema() -> db::RecordSchema<Self> {
        db::RecordSchema::new("posts")
            .bind_columns(["id", "title"].into_iter().map(str::to_string).collect())
    }

    fn record_bind_values(&self, args: &mut db::Arguments<'static>) -> Result<(), sqlx::Error> {
        use sqlx::Arguments as _;

        args.add(self.id)
            .map_err(|err| sqlx::Error::Protocol(err.to_string()))?;
        args.add(self.title.clone())
            .map_err(|err| sqlx::Error::Protocol(err.to_string()))?;
        Ok(())
    }
}

#[derive(Clone)]
struct PostKey {
    id: i64,
}

impl db::Record for PostKey {
    fn record_schema() -> db::RecordSchema<Self> {
        db::RecordSchema::new("posts").bind_columns(vec!["id".to_string()])
    }

    fn record_bind_values(&self, args: &mut db::Arguments<'static>) -> Result<(), sqlx::Error> {
        use sqlx::Arguments as _;

        args.add(self.id)
            .map_err(|err| sqlx::Error::Protocol(err.to_string()))?;
        Ok(())
    }
}

#[derive(Clone)]
struct UserRow {
    name: String,
}

impl db::Record for UserRow {
    fn record_schema() -> db::RecordSchema<Self> {
        db::RecordSchema::new("users").bind_columns(vec!["name".to_string()])
    }

    fn record_bind_values(&self, args: &mut db::Arguments<'static>) -> Result<(), sqlx::Error> {
        use sqlx::Arguments as _;

        args.add(self.name.clone())
            .map_err(|err| sqlx::Error::Protocol(err.to_string()))?;
        Ok(())
    }
}

#[derive(Debug, Clone, db::Model)]
#[table(name = "typed_users")]
struct TypedUser {
    id: i64,
    display_name: String,
    active: bool,
}

#[derive(Debug, Clone, db::Model)]
#[table(name = "typed_posts")]
struct TypedPost {
    id: i64,
    author_id: i64,
    title: String,
    published: bool,
    #[column(type = "timestamptz")]
    created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, db::Filterable)]
#[filter(model = TypedPost)]
struct TypedPostFilter {
    #[filter(op = "eq")]
    published: Option<bool>,
    #[filter(op = "ilike", column = "title")]
    q: Option<String>,
    #[filter(op = "gte", column = "created_at")]
    created_after: Option<chrono::DateTime<chrono::Utc>>,
    #[filter(op = "in", column = "id")]
    ids: Vec<i64>,
    #[filter(op = "in", column = "id")]
    optional_ids: Option<Vec<i64>>,
}

#[derive(Debug, Clone, db::Filterable)]
#[filter(model = TypedUser)]
struct TypedUserFilter {
    #[filter(op = "eq")]
    active: Option<bool>,
}

#[derive(Debug, Clone, db::Record)]
struct TypedPostWithAuthor {
    #[column(flatten)]
    post: TypedPost,
    #[column(reference(on(from = "author_id", to = "id")))]
    author: TypedUser,
}

#[derive(Debug, Clone, db::Model)]
#[table(name = "typed_comments")]
struct TypedComment {
    id: i64,
    post_id: i64,
    flagged: bool,
}

#[derive(Debug, Clone, db::Record)]
#[table(name = "typed_comments")]
struct TypedCommentPostId {
    post_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TypedPostMeta {
    status: String,
}

#[derive(Debug, Clone, db::Model)]
#[table(name = "typed_json_posts")]
struct TypedJsonPost {
    id: i64,
    #[column(type = "jsonb")]
    meta: TypedPostMeta,
}

#[cfg(feature = "postgres")]
#[derive(Debug, Clone, db::Model)]
#[table(name = "typed_array_posts")]
struct TypedArrayPost {
    id: i64,
    tags: Vec<String>,
    optional_scores: Option<Vec<i64>>,
}

#[derive(Debug, Clone, db::Record)]
#[table(name = "typed_posts")]
struct TypedPostPatch {
    title: String,
}

#[derive(Debug, Clone, db::Record)]
#[table(name = "typed_posts")]
struct TypedPostSummary {
    id: i64,
    title: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone, db::Record)]
#[table(name = "typed_posts")]
struct TypedPostRank {
    id: i64,
    row_number: i64,
    rank: i64,
    dense_rank: i64,
    percent_rank: f64,
    cume_dist: f64,
    bucket: i64,
}

#[allow(dead_code)]
#[derive(Debug, Clone, db::Record)]
#[table(name = "typed_posts")]
struct TypedPostStats {
    author_id: i64,
    post_count: i64,
    running_id: i64,
    average_id: f64,
    previous_title: String,
    next_title: String,
    first_title: String,
    last_title: String,
    nth_title: String,
}

#[derive(Debug, Clone, db::Model)]
#[table(name = "typed_weird_names")]
struct TypedWeirdNames {
    id: i64,
    cols: String,
    pick: String,
    schema: String,
    name: String,
}

#[derive(Debug, Clone, db::Record)]
#[table(name = "users", schema = "auth")]
struct AuthUser {
    id: i64,
}

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

#[derive(Clone)]
struct LowerExpr {
    title: db::queries::__private::Column<String>,
}

impl db::DbExpression<String> for LowerExpr {
    fn args(&self) -> db::FunctionArgs {
        db::FunctionArgs::new((&self.title,))
    }

    fn render(&self, ctx: &mut db::ExprRenderCtx<'_>) -> Result<String, db::QueryError> {
        Ok(format!("LOWER({})", ctx.arg(0)?))
    }
}

/// Verifies that typed query handles are cloneable hash keys without requiring `Copy`.
#[test]
fn typed_query_handles_are_cloneable_hashable_keys() {
    fn assert_handle<T: Clone + Eq + Hash>() {}

    assert_handle::<db::queries::__private::ModelTable<TypedPost>>();
    assert_handle::<db::queries::__private::Column<i64>>();
    assert_handle::<db::queries::Var<String>>();
}

/// Verifies that source columns support the public query shape with terminal projections.
#[test]
fn generated_handles_support_public_select_shape() {
    use db::queries::{Dialect, from, val};

    let post_table = TypedPost::table();
    let plan = from(&post_table)
        .filter(post_table.published.eq(val(true)))
        .order_by(post_table.created_at.desc())
        .all::<TypedPostWithAuthor>()
        .plan(Dialect::Postgres)
        .expect("generated handles should render");

    assert_eq!(
        plan.sql,
        "SELECT post.id, post.author_id, post.title, post.published, post.created_at, author.id, author.display_name, author.active FROM typed_posts post JOIN typed_users author ON author.id = post.author_id WHERE (post.published = $1) ORDER BY post.created_at DESC"
    );
}

/// Verifies that typed `Filterable` appends optional, scalar, and list predicates.
#[test]
fn typed_filterable_appends_where_predicates() {
    use db::queries::{Dialect, from};

    let post_table = TypedPost::table();
    let filter = TypedPostFilter {
        published: Some(true),
        q: Some("%vyuh%".to_string()),
        created_after: None,
        ids: vec![1, 2],
        optional_ids: None,
    };
    let plan = from(&post_table)
        .filter_with(&filter)
        .all::<TypedPost>()
        .plan(Dialect::Postgres)
        .expect("typed filter should render");

    assert_eq!(
        plan.sql,
        "SELECT typed_post.id, typed_post.author_id, typed_post.title, typed_post.published, typed_post.created_at FROM typed_posts typed_post WHERE (typed_post.published = $1) AND (typed_post.title ILIKE $2) AND typed_post.id IN ($3, $4)"
    );
}

/// Verifies that typed filters skip empty optional and `IN` values.
#[test]
fn typed_filterable_skips_empty_values() {
    use db::queries::{Dialect, from};

    let post_table = TypedPost::table();
    let filter = TypedPostFilter {
        published: None,
        q: None,
        created_after: None,
        ids: Vec::new(),
        optional_ids: None,
    };
    let plan = from(&post_table)
        .filter_with(&filter)
        .all::<TypedPost>()
        .plan(Dialect::Postgres)
        .expect("empty filter should render");

    assert_eq!(
        plan.sql,
        "SELECT typed_post.id, typed_post.author_id, typed_post.title, typed_post.published, typed_post.created_at FROM typed_posts typed_post"
    );
}

/// Verifies that regular predicates and typed filters compose with `AND`.
#[test]
fn typed_filterable_composes_with_existing_filters() {
    use db::queries::{Dialect, from, val};

    let post_table = TypedPost::table();
    let filter = TypedPostFilter {
        published: Some(true),
        q: None,
        created_after: None,
        ids: Vec::new(),
        optional_ids: None,
    };
    let plan = from(&post_table)
        .filter(post_table.author_id.eq(val(10_i64)))
        .filter_with(&filter)
        .all::<TypedPost>()
        .plan(Dialect::Postgres)
        .expect("composed filter should render");

    assert_eq!(
        plan.sql,
        "SELECT typed_post.id, typed_post.author_id, typed_post.title, typed_post.published, typed_post.created_at FROM typed_posts typed_post WHERE (typed_post.author_id = $1) AND (typed_post.published = $2)"
    );
}

/// Verifies that optional list filters emit typed `IN` predicates when present.
#[test]
fn typed_filterable_supports_optional_in_lists() {
    use db::queries::{Dialect, from};

    let post_table = TypedPost::table();
    let filter = TypedPostFilter {
        published: None,
        q: None,
        created_after: None,
        ids: Vec::new(),
        optional_ids: Some(vec![10, 20]),
    };
    let plan = from(&post_table)
        .filter_with(&filter)
        .all::<TypedPost>()
        .plan(Dialect::Postgres)
        .expect("optional IN filter should render");

    assert_eq!(
        plan.sql,
        "SELECT typed_post.id, typed_post.author_id, typed_post.title, typed_post.published, typed_post.created_at FROM typed_posts typed_post WHERE typed_post.id IN ($1, $2)"
    );
}

/// Verifies that applying a model-bound filter to the wrong root table fails.
#[test]
fn typed_filterable_rejects_wrong_model_source() {
    use db::queries::{Dialect, from};

    let post_table = TypedPost::table();
    let filter = TypedUserFilter { active: Some(true) };
    let error = from(&post_table)
        .filter_with(&filter)
        .all::<TypedPost>()
        .plan(Dialect::Postgres)
        .expect_err("wrong model filter should fail");

    let message = error.to_string();
    assert!(message.contains("belongs to 'typed_users'"), "{message}");
}

/// Verifies that read executable variants accept flat typed output assignments.
#[test]
fn read_executables_support_flat_output_assignments() {
    use db::queries::{Dialect, from};

    let post = TypedPost::table();
    let out = db::out::<TypedPostSummary>();
    let one = from(&post)
        .one::<TypedPostSummary>()
        .set(&out.id, &post.id)
        .set(&out.title, &post.title)
        .plan(Dialect::Postgres)
        .expect("one set should render");
    let first = from(&post)
        .first::<TypedPostSummary>()
        .set(&out.id, &post.id)
        .set(&out.title, &post.title)
        .plan(Dialect::Postgres)
        .expect("first set should render");
    let slice = from(&post)
        .slice::<TypedPostSummary>(10, 5)
        .set(out.id, &post.id)
        .set(out.title, &post.title)
        .plan(Dialect::Postgres)
        .expect("slice set should render");

    assert!(one.sql.starts_with("SELECT typed_post.id AS id"));
    assert_eq!(one.sql, first.sql);
    assert!(slice.sql.ends_with("LIMIT 5 OFFSET 10"));
}

/// Verifies that source deref handles allow columns that collide with compatibility names.
#[test]
fn source_deref_supports_reserved_column_names() {
    use db::queries::{Dialect, from, val};

    let table = TypedWeirdNames::table();
    let plan = from(&table)
        .filter(table.cols.eq(val("columns".to_string())))
        .filter(table.pick.eq(val("pick".to_string())))
        .filter(table.schema.eq(val("schema".to_string())))
        .filter(table.name.eq(val("name".to_string())))
        .all::<TypedWeirdNames>()
        .plan(Dialect::Postgres)
        .expect("reserved-name columns should render through deref");

    assert_eq!(
        plan.sql,
        "SELECT typed_weird_name.id, typed_weird_name.cols, typed_weird_name.pick, typed_weird_name.schema, typed_weird_name.name FROM typed_weird_names typed_weird_name WHERE (typed_weird_name.cols = $1) AND (typed_weird_name.pick = $2) AND (typed_weird_name.schema = $3) AND (typed_weird_name.name = $4)"
    );
}

/// Verifies that db::meta reports redaction-free source metadata for all sources.
#[test]
fn db_meta_reports_table_cte_and_subquery_sources() {
    use db::queries::{from, val};

    let post = TypedPost::table();
    let table_meta = db::meta(&post);
    assert_eq!(table_meta.kind(), db::SourceKind::Table);
    assert_eq!(table_meta.name(), "typed_posts");
    assert_eq!(table_meta.schema(), None);
    assert_eq!(table_meta.qualified_name(), "typed_posts");
    assert!(table_meta.writable_columns().contains(&"title".to_string()));

    let comment = TypedComment::table();
    let active = from(&comment)
        .filter(comment.flagged.eq(val(false)))
        .all::<TypedCommentPostId>()
        .set(db::out::<TypedCommentPostId>().post_id, &comment.post_id)
        .subquery()
        .expect("subquery should build");
    let subquery_meta = db::meta(&active);
    assert_eq!(subquery_meta.kind(), db::SourceKind::Subquery);
    assert_eq!(subquery_meta.name(), "subquery_typedcommentpostid");
    assert_eq!(subquery_meta.output_columns(), ["post_id"]);

    let stale = from(&post)
        .filter(post.published.eq(val(false)))
        .all::<TypedPostSummary>()
        .cte()
        .expect("cte should build");
    let cte_meta = db::meta(&stale);
    assert_eq!(cte_meta.kind(), db::SourceKind::Cte);
    assert_eq!(cte_meta.name(), "cte_typedpostsummary");
    assert_eq!(cte_meta.output_columns(), ["id", "title"]);
}

/// Verifies that common SELECT rendering is backend-neutral except placeholders.
#[test]
fn common_select_renders_across_dialects() {
    use db::queries::{Dialect, from, val};

    let post_table = TypedPost::table();
    let query = from(&post_table).filter(post_table.published.eq(val(true)));

    let postgres = query
        .clone()
        .all::<TypedPost>()
        .plan(Dialect::Postgres)
        .expect("postgres select should render");
    let sqlite = query
        .clone()
        .all::<TypedPost>()
        .plan(Dialect::Sqlite)
        .expect("sqlite select should render");
    let mysql = query
        .all::<TypedPost>()
        .plan(Dialect::Mysql)
        .expect("mysql select should render");

    assert!(postgres.sql.ends_with("WHERE (typed_post.published = $1)"));
    assert!(sqlite.sql.ends_with("WHERE (typed_post.published = ?)"));
    assert!(mysql.sql.ends_with("WHERE (typed_post.published = ?)"));
}

/// Verifies that typed set operations render as one observable SQL statement.
#[test]
fn set_operations_render_union_union_all_and_except() {
    use db::queries::{Dialect, from, val};

    let post_table = TypedPost::table();
    let published = from(&post_table)
        .filter(post_table.published.eq(val(true)))
        .all::<TypedPost>();
    let draft = from(&post_table)
        .filter(post_table.published.eq(val(false)))
        .all::<TypedPost>();

    let union = published
        .clone()
        .union(draft.clone())
        .plan(Dialect::Postgres)
        .expect("UNION should render");
    let union_all = published
        .clone()
        .union_all(draft.clone())
        .plan(Dialect::Postgres)
        .expect("UNION ALL should render");
    let except = published
        .except(draft)
        .plan(Dialect::Postgres)
        .expect("EXCEPT should render");

    assert!(union.sql.contains(" UNION "));
    assert!(union_all.sql.contains(" UNION ALL "));
    assert!(except.sql.contains(" EXCEPT "));
    assert_eq!(union.dynamic_bind_count, 2);
    assert_eq!(union_all.dynamic_bind_count, 2);
    assert_eq!(except.dynamic_bind_count, 2);
}

/// Verifies that set operations share parameter planning across both operands.
#[test]
fn set_operations_reuse_handle_vars_across_operands() {
    use db::queries::{Dialect, from, var};

    let post_table = TypedPost::table();
    let title = var::<String>().named("title");
    let left = from(&post_table)
        .filter(post_table.title.eq(&title))
        .all::<TypedPost>();
    let right = from(&post_table)
        .filter(post_table.title.ne(&title))
        .all::<TypedPost>();

    let plan = left
        .union_all(right)
        .bind(&title, "vyuh".to_string())
        .plan(Dialect::Postgres)
        .expect("set operation should render");

    assert!(plan.sql.contains("(typed_post.title = $1)"));
    assert!(plan.sql.contains("(typed_post.title != $1)"));
    assert_eq!(plan.dynamic_bind_count, 1);
    assert_eq!(plan.params["title"].occurrences, [1, 1]);
}

/// Verifies that dialect validation rejects PostgreSQL-only ILIKE on SQLite.
#[test]
fn sqlite_rejects_ilike() {
    use db::queries::{Dialect, from, var};

    let post_table = TypedPost::table();
    let term = var::<String>().named("term");
    let error = from(&post_table)
        .filter(post_table.title.ilike(&term))
        .all::<TypedPost>()
        .plan(Dialect::Sqlite)
        .expect_err("sqlite should reject ILIKE");

    assert!(error.to_string().contains("ILIKE"));
}

/// Verifies that MySQL returning writes fail during planning with a dialect error.
#[test]
fn mysql_rejects_returning() {
    use db::queries::{Dialect, from};

    let post_table = TypedPost::table();
    let patch = TypedPostPatch {
        title: "Draft".to_string(),
    };
    let error = from(&post_table)
        .returning::<TypedPostSummary>()
        .insert(&patch)
        .plan(Dialect::Mysql)
        .expect_err("mysql should reject RETURNING");

    assert!(error.to_string().contains("RETURNING"));
}

/// Verifies that custom functions render through the dialect-aware function hook.
#[test]
fn custom_function_renders_with_dialect_name() {
    use db::queries::{Dialect, from, val};

    let post_table = TypedPost::table();
    let plan = from(&post_table)
        .filter(db::funcs::func(Unaccent, (&post_table.title,)).eq(val("hello".to_string())))
        .all::<TypedPost>()
        .plan(Dialect::Postgres)
        .expect("custom function should render");

    assert!(plan.sql.contains("unaccent(typed_post.title)"));
}

/// Verifies that custom function validation can reject unsupported dialects.
#[test]
fn custom_function_can_reject_dialect() {
    use db::queries::{Dialect, from, val};

    let post_table = TypedPost::table();
    let error = from(&post_table)
        .filter(db::funcs::func(Unaccent, (&post_table.title,)).eq(val("hello".to_string())))
        .all::<TypedPost>()
        .plan(Dialect::Sqlite)
        .expect_err("custom function should reject sqlite");

    assert!(error.to_string().contains("unaccent"));
}

/// Verifies that PostgreSQL helper functions are isolated under the dialect namespace.
#[test]
fn postgres_helper_renders_and_rejects_other_dialects() {
    use db::queries::{Dialect, from, val};

    let post_table = TypedPost::table();
    let plan = from(&post_table)
        .filter(db::funcs::postgres::unaccent(&post_table.title).eq(val("hello".to_string())))
        .all::<TypedPost>()
        .plan(Dialect::Postgres)
        .expect("postgres helper should render");

    assert!(plan.sql.contains("unaccent(typed_post.title)"));

    let error = from(&post_table)
        .filter(db::funcs::postgres::unaccent(&post_table.title).eq(val("hello".to_string())))
        .all::<TypedPost>()
        .plan(Dialect::Sqlite)
        .expect_err("postgres helper should reject sqlite");

    assert!(error.to_string().contains("unaccent"));
    assert!(error.to_string().contains("sqlite"));
}

/// Verifies JSON storage fields generate JSON marker query columns.
#[test]
fn json_storage_fields_generate_marker_columns() {
    let posts = TypedJsonPost::table();
    let _: &db::queries::__private::Column<db::types::Json> = &posts.meta;
}

/// Verifies JSON helpers render from the JSON submodule.
#[test]
fn json_helpers_render_from_json_submodule() {
    let posts = TypedJsonPost::table();
    let plan = db::from(&posts)
        .filter(db::funcs::json::text(&posts.meta, "status").eq(db::val("published".to_string())))
        .filter(db::funcs::json::exists(&posts.meta, "featured"))
        .all::<TypedJsonPost>()
        .plan(db::queries::Dialect::Postgres)
        .expect("json query should render");

    assert!(plan.sql.contains("typed_json_post.meta #>> '{status}'"));
    assert!(plan.sql.contains("typed_json_post.meta #> '{featured}'"));
}

/// Verifies PostgreSQL JSON helpers stay under the JSON submodule.
#[test]
fn postgres_json_helpers_render_from_json_submodule() {
    let posts = TypedJsonPost::table();
    let plan = db::from(&posts)
        .filter(db::funcs::json::postgres::contains(
            &posts.meta,
            db::funcs::json::value(serde_json::json!({ "status": "published" })),
        ))
        .all::<TypedJsonPost>()
        .plan(db::queries::Dialect::Postgres)
        .expect("jsonb contains should render");

    assert!(plan.sql.contains("(typed_json_post.meta @> $1)"));
}

/// Verifies array storage fields generate array marker query columns.
#[cfg(feature = "postgres")]
#[test]
fn array_storage_fields_generate_marker_columns() {
    let posts = TypedArrayPost::table();
    let _: &db::queries::__private::Column<db::types::Array<String>> = &posts.tags;
    let _: &db::queries::__private::Column<db::types::Array<i64>> = &posts.optional_scores;

    let table = <TypedArrayPost as db::IntoTable>::into_table(&db::Dialect::Postgres);
    assert!(
        table
            .columns
            .iter()
            .any(|column| column.name == "tags" && !column.nullable)
    );
    assert!(
        table
            .columns
            .iter()
            .any(|column| column.name == "optional_scores" && column.nullable)
    );
}

/// Verifies array helpers render from the array submodule.
#[test]
fn array_helpers_render_from_array_submodule() {
    use db::queries::{__private::table, Dialect, from, val};

    let posts = table("posts");
    let tags = posts.col::<db::types::Array<String>>("tags");
    let other_tags = posts.col::<db::types::Array<String>>("other_tags");
    let scores = posts.col::<db::types::Array<i64>>("scores");

    let plan = from(&posts)
        .filter(db::funcs::array::contains(&tags, &other_tags))
        .filter(db::funcs::array::contained_by(&tags, &other_tags))
        .filter(db::funcs::array::overlaps(&tags, &other_tags))
        .filter(db::funcs::array::any(&tags, val("rust".to_string())))
        .filter(db::funcs::array::all(&tags, val("rust".to_string())))
        .filter(db::funcs::array::is_empty(&scores).not())
        .filter(db::funcs::array::length(&scores).gt(val(0_i64)))
        .filter(db::funcs::array::cardinality(&scores).gt(val(0_i64)))
        .filter(db::funcs::array::position(&tags, val("rust".to_string())).gt(val(0_i64)))
        .all::<PostRow>()
        .plan(Dialect::Postgres)
        .expect("array query should render");

    assert!(plan.sql.contains("post.tags @> post.other_tags"));
    assert!(plan.sql.contains("post.tags <@ post.other_tags"));
    assert!(plan.sql.contains("post.tags && post.other_tags"));
    assert!(plan.sql.contains("$1 = ANY(post.tags)"));
    assert!(plan.sql.contains("$2 = ALL(post.tags)"));
    assert!(plan.sql.contains("cardinality(post.scores) = 0"));
    assert!(plan.sql.contains("array_length(post.scores, 1)"));
    assert!(plan.sql.contains("array_position(post.tags, $5)"));
}

/// Verifies array helpers reject dialects without native SQL arrays.
#[test]
fn array_helpers_reject_non_postgres_dialects() {
    use db::queries::{__private::table, Dialect, from};

    let posts = table("posts");
    let tags = posts.col::<db::types::Array<String>>("tags");
    let error = from(&posts)
        .filter(db::funcs::array::is_empty(&tags))
        .all::<PostRow>()
        .plan(Dialect::Sqlite)
        .expect_err("sqlite should reject array helpers");

    assert!(error.to_string().contains("array"));
    assert!(error.to_string().contains("sqlite"));
}

/// Verifies that custom expressions can render typed SQL using child expressions.
#[test]
fn custom_expression_renders_with_arguments() {
    use db::queries::{Dialect, from, val};

    let post_table = TypedPost::table();
    let plan = from(&post_table)
        .filter(
            db::funcs::custom(LowerExpr {
                title: post_table.title.clone(),
            })
            .eq(val("draft".to_string())),
        )
        .all::<TypedPost>()
        .plan(Dialect::Postgres)
        .expect("custom expression should render");

    assert!(plan.sql.contains("LOWER(typed_post.title)"));
}

/// Verifies that portable conditional expressions render as typed output assignments.
#[test]
fn conditional_expressions_render_in_output_assignments() {
    use db::queries::{Dialect, from, val};

    let post = TypedPost::table();
    let out = db::out::<TypedPostSummary>();
    let plan = from(&post)
        .all::<TypedPostSummary>()
        .set(&out.id, db::funcs::coalesce(&post.id, val(0_i64)))
        .set(
            &out.title,
            db::funcs::case()
                .when(post.published.eq(val(true)), val("published".to_string()))
                .else_(val("draft".to_string())),
        )
        .plan(Dialect::Postgres)
        .expect("conditional expressions should render");

    assert!(plan.sql.contains("COALESCE(typed_post.id, $1) AS id"));
    assert!(
        plan.sql
            .contains("CASE WHEN (typed_post.published = $2) THEN $3 ELSE $4 END AS title")
    );
}

/// Verifies that derive-generated projected columns support subquery `pick(...)`.
#[test]
fn generated_handles_support_public_subquery_pick_shape() {
    use db::queries::{Dialect, from, val};

    let comment_table = TypedComment::table();
    let post_table = TypedPost::table();
    let active = from(&comment_table)
        .filter(comment_table.flagged.eq(val(false)))
        .all::<TypedCommentPostId>()
        .set(
            db::out::<TypedCommentPostId>().post_id,
            &comment_table.post_id,
        )
        .subquery()
        .expect("subquery should build");

    let plan = from(&post_table)
        .filter(post_table.id.in_(active.pick(&active.post_id)))
        .all::<TypedPost>()
        .plan(Dialect::Postgres)
        .expect("subquery should render");

    assert_eq!(
        plan.sql,
        "SELECT typed_post.id, typed_post.author_id, typed_post.title, typed_post.published, typed_post.created_at FROM typed_posts typed_post WHERE typed_post.id IN (SELECT subquery_typedcommentpostid.post_id FROM (SELECT typed_comment.post_id AS post_id FROM typed_comments typed_comment WHERE (typed_comment.flagged = $1)) subquery_typedcommentpostid)"
    );
}

/// Verifies that table sources can be picked directly for one-column IN predicates.
#[test]
fn table_pick_renders_one_column_source_predicate() {
    use db::queries::{Dialect, from};

    let post_table = TypedPost::table();
    let user_table = TypedUser::table();
    let plan = from(&post_table)
        .filter(post_table.author_id.in_(user_table.pick(&user_table.id)))
        .all::<TypedPost>()
        .plan(Dialect::Postgres)
        .expect("table pick should render");

    assert_eq!(
        plan.sql,
        "SELECT typed_post.id, typed_post.author_id, typed_post.title, typed_post.published, typed_post.created_at FROM typed_posts typed_post WHERE typed_post.author_id IN (SELECT id FROM typed_users)"
    );
}

/// Verifies that Record patch rows work with model-provided table handles.
#[test]
fn bindable_patch_rows_work_with_model_table_handles() {
    use db::queries::{Dialect, from, var};

    let post_table = TypedPost::table();
    let patch = TypedPostPatch {
        title: "Updated".to_string(),
    };
    let id = var::<i64>().named("id");

    let insert = from(&post_table)
        .insert(&patch)
        .plan(Dialect::Postgres)
        .expect("insert should render");
    assert_eq!(insert.sql, "INSERT INTO typed_posts (title) VALUES ($1)");

    let update = from(&post_table)
        .filter(post_table.id.eq(&id))
        .update(&patch)
        .plan(Dialect::Postgres)
        .expect("update should render");
    assert_eq!(
        update.sql,
        "UPDATE typed_posts SET title = $1 WHERE (id = $2)"
    );
}

/// Verifies that write values can mix record fields with expression overrides.
#[test]
fn write_values_support_expression_assignments_and_record_overrides() {
    use db::queries::{Dialect, from, val, var};

    let post_table = TypedPost::table();
    let patch = TypedPostPatch {
        title: "Stored".to_string(),
    };

    let generated = TypedPostPatch {
        title: "Ignored".to_string(),
    };
    let expression_override = from(&post_table)
        .insert(&generated)
        .set(&post_table.title, val("Generated".to_string()))
        .plan(Dialect::Postgres)
        .expect("insert override should render");
    assert_eq!(
        expression_override.sql,
        "INSERT INTO typed_posts (title) VALUES ($1)"
    );
    assert_eq!(expression_override.prebound_count, 0);
    assert_eq!(expression_override.dynamic_bind_count, 1);

    let mixed = from(&post_table)
        .filter(post_table.id.eq(val(1_i64)))
        .update(&patch)
        .set(&post_table.title, val("Override".to_string()))
        .plan(Dialect::Postgres)
        .expect("mixed update should render");
    assert_eq!(
        mixed.sql,
        "UPDATE typed_posts SET title = $1 WHERE (id = $2)"
    );
    assert_eq!(mixed.prebound_count, 0);
    assert_eq!(mixed.dynamic_bind_count, 2);

    let title = var::<String>().named("title");
    let bound = from(&post_table)
        .insert(&patch)
        .set(&post_table.title, &title)
        .bind(&title, "Bound".to_string())
        .plan(Dialect::Postgres)
        .expect("insert override should support executable binds");
    assert_eq!(bound.sql, "INSERT INTO typed_posts (title) VALUES ($1)");
    assert_eq!(bound.params["title"].position, 1);
}

/// Verifies that duplicate explicit write assignments fail during planning.
#[test]
fn write_values_reject_duplicate_explicit_assignments() {
    use db::queries::{Dialect, from, val};

    let post_table = TypedPost::table();
    let error = from(&post_table)
        .filter(post_table.id.eq(val(1_i64)))
        .update(&TypedPostPatch {
            title: "Stored".to_string(),
        })
        .set(&post_table.title, val("first".to_string()))
        .set(&post_table.title, val("second".to_string()))
        .plan(Dialect::Postgres)
        .expect_err("duplicate explicit assignment should fail");

    assert!(error.to_string().contains("duplicate assignment"));
}

/// Verifies that common ranking window functions render across supported dialects.
#[test]
fn window_ranking_functions_render_across_dialects() {
    use db::queries::{Dialect, from, val};

    let post = TypedPost::table();
    let base_window = || {
        db::funcs::window()
            .partition_by(&post.author_id)
            .order_by(post.created_at.desc())
    };
    let out = db::out::<TypedPostRank>();
    let query = from(&post)
        .all::<TypedPostRank>()
        .set(out.row_number, db::funcs::row_number().over(base_window()))
        .set(out.rank, db::funcs::rank().over(base_window()))
        .set(out.dense_rank, db::funcs::dense_rank().over(base_window()))
        .set(
            out.percent_rank,
            db::funcs::percent_rank().over(base_window()),
        )
        .set(out.cume_dist, db::funcs::cume_dist().over(base_window()))
        .set(out.bucket, db::funcs::ntile(val(4_i64)).over(base_window()));

    let postgres = query
        .clone()
        .plan(Dialect::Postgres)
        .expect("postgres window query should render");
    let sqlite = query
        .clone()
        .plan(Dialect::Sqlite)
        .expect("sqlite window query should render");
    let mysql = query
        .plan(Dialect::Mysql)
        .expect("mysql window query should render");

    assert!(postgres.sql.contains("ROW_NUMBER() OVER"));
    assert!(postgres.sql.contains("NTILE($1) OVER"));
    assert!(sqlite.sql.contains("NTILE(?) OVER"));
    assert!(mysql.sql.contains("NTILE(?) OVER"));
}

/// Verifies that aggregate and value window functions render with frames.
#[test]
fn window_aggregate_and_value_functions_render() {
    use db::queries::{Dialect, from, val, var};

    let post = TypedPost::table();
    let offset = var::<i64>().named("offset");
    let base_window = || {
        db::funcs::window()
            .partition_by(&post.author_id)
            .order_by(post.created_at.desc())
    };
    let out = db::out::<TypedPostStats>();
    let plan = from(&post)
        .all::<TypedPostStats>()
        .set(
            out.post_count,
            db::funcs::count(&post.id).over(db::funcs::window().partition_by(&post.author_id)),
        )
        .set(
            out.running_id,
            db::funcs::sum(&post.id).over(
                db::funcs::window()
                    .partition_by(&post.author_id)
                    .order_by(post.created_at.asc())
                    .rows_between(db::funcs::unbounded_preceding(), db::funcs::current_row()),
            ),
        )
        .set(
            out.average_id,
            db::funcs::avg(&post.id).over(db::funcs::window().partition_by(&post.author_id)),
        )
        .set(
            out.previous_title,
            db::funcs::lag_or(&post.title, &offset, val("missing".to_string()))
                .over(db::funcs::window().order_by(post.created_at.asc())),
        )
        .set(
            out.next_title,
            db::funcs::lead_by(&post.title, val(1_i64))
                .over(db::funcs::window().order_by(post.created_at.asc())),
        )
        .set(
            out.first_title,
            db::funcs::first_value(&post.title).over(base_window()),
        )
        .set(
            out.last_title,
            db::funcs::last_value(&post.title).over(
                db::funcs::window()
                    .order_by(post.created_at.asc())
                    .range_between(
                        db::funcs::unbounded_preceding(),
                        db::funcs::unbounded_following(),
                    ),
            ),
        )
        .set(
            out.nth_title,
            db::funcs::nth_value(&post.title, val(2_i64)).over(base_window()),
        )
        .plan(Dialect::Postgres)
        .expect("window stats should render");

    assert!(plan.sql.contains("COUNT(typed_post.id) OVER"));
    assert!(
        plan.sql
            .contains("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW")
    );
    assert!(
        plan.sql
            .contains("RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING")
    );
    assert!(plan.sql.contains("LAG(typed_post.title, $1, $2) OVER"));
    assert!(plan.sql.contains("LEAD(typed_post.title, $3) OVER"));
    assert!(plan.sql.contains("NTH_VALUE(typed_post.title, $4) OVER"));
    assert_eq!(plan.params["offset"].position, 1);
}

/// Verifies that window expressions are rejected outside read output/order contexts.
#[test]
fn window_functions_are_rejected_in_invalid_contexts() {
    use db::queries::{Dialect, from, val};

    let post = TypedPost::table();
    let windowed =
        || db::funcs::row_number().over(db::funcs::window().order_by(post.created_at.desc()));

    let filter = from(&post)
        .filter(windowed().eq(val(1_i64)))
        .all::<TypedPost>()
        .plan(Dialect::Postgres)
        .expect_err("WHERE window should fail");
    assert!(filter.to_string().contains("WHERE"));

    let group = from(&post)
        .group_by(windowed())
        .all::<TypedPost>()
        .plan(Dialect::Postgres)
        .expect_err("GROUP BY window should fail");
    assert!(group.to_string().contains("GROUP BY"));

    let write = from(&post)
        .filter(post.id.eq(val(1_i64)))
        .update(&TypedPostPatch {
            title: "Stored".to_string(),
        })
        .set(&post.id, windowed())
        .plan(Dialect::Postgres)
        .expect_err("write window should fail");
    assert!(write.to_string().contains("mutation"));
}

/// Verifies that only window-capable functions can use OVER.
#[test]
fn over_rejects_non_window_functions() {
    use db::queries::{Dialect, from};

    let post = TypedPost::table();
    let error = from(&post)
        .all::<TypedPost>()
        .set(
            db::out::<TypedPost>().created_at,
            db::funcs::now().over(db::funcs::window()),
        )
        .plan(Dialect::Postgres)
        .expect_err("non-window function should fail");

    assert!(error.to_string().contains("window-capable"));
}

/// Verifies that SELECT rendering uses only object columns and implicit Record references.
#[test]
fn all_plan_renders_implicit_join_from_scannable_references() {
    use db::queries::{
        __private::{reference, table},
        Dialect, from, funcs, val, var,
    };

    let post = table("posts");
    let author = reference("author");
    let author_id = author.col::<i64>("id");
    let phone = var::<String>().named("phone");
    let plan = from(&post)
        .filter(post.col::<String>("phone").ilike(&phone))
        .filter(author.col::<bool>("active").eq(val(true)))
        .group_by(post.col::<i64>("id"))
        .having(funcs::count(&author_id).gt(val(0_i64)))
        .order_by(
            post.col::<chrono::DateTime<chrono::Utc>>("created_at")
                .desc(),
        )
        .all::<PostWithAuthor>()
        .set(
            db::out::<PostWithAuthor>().comment_count,
            funcs::count(&author_id),
        )
        .plan(Dialect::Postgres)
        .expect("select should render");

    assert_eq!(
        plan.sql,
        "SELECT post.id, post.title, author.display_name, COUNT(author.id) AS comment_count FROM posts post JOIN users author ON author.id = post.author_id WHERE (post.phone ILIKE $1) AND (author.active = $2) GROUP BY post.id HAVING (COUNT(author.id) > $3) ORDER BY post.created_at DESC"
    );
    assert_eq!(plan.params["phone"].position, 1);
    assert_eq!(plan.params["phone"].source, db::queries::ParamSource::Var);
}

/// Verifies that slice planning is terminal-shaped and adds LIMIT/OFFSET.
#[test]
fn slice_plan_renders_limit_and_offset() {
    use db::queries::{__private::table, Dialect, from};

    let post = table("posts");
    let plan = from(&post)
        .slice::<PostRow>(20, 10)
        .plan(Dialect::Postgres)
        .expect("slice should render");

    assert_eq!(
        plan.sql,
        "SELECT post.id, post.title, post.comment_count FROM posts post LIMIT 10 OFFSET 20"
    );
}

/// Verifies that repeated vars reuse Postgres placeholders but keep occurrence metadata.
#[test]
fn repeated_vars_reuse_postgres_placeholder() {
    use db::queries::{__private::table, Dialect, from, var};

    let post = table("posts");
    let title = post.col::<String>("title");
    let term = var::<String>().named("term");
    let plan = from(&post)
        .filter(title.like(&term).or(title.ilike(&term)))
        .all::<PostRow>()
        .plan(Dialect::Postgres)
        .expect("select should render");

    assert_eq!(
        plan.sql,
        "SELECT post.id, post.title, post.comment_count FROM posts post WHERE ((post.title LIKE $1) OR (post.title ILIKE $1))"
    );
    assert_eq!(plan.params["term"].occurrences, vec![1, 1]);
    assert_eq!(plan.dynamic_bind_count, 1);
}

/// Verifies that repeated vars in `?` dialects create separate bind occurrences.
#[test]
fn repeated_vars_bind_each_positional_occurrence() {
    use db::queries::{__private::table, Dialect, from, var};

    let post = table("posts");
    let title = post.col::<String>("title");
    let term = var::<String>().named("term");
    let plan = from(&post)
        .filter(title.like(&term).or(title.like(&term)))
        .all::<PostRow>()
        .plan(Dialect::Sqlite)
        .expect("select should render");

    assert_eq!(
        plan.sql,
        "SELECT post.id, post.title, post.comment_count FROM posts post WHERE ((post.title LIKE ?) OR (post.title LIKE ?))"
    );
    assert_eq!(plan.params["term"].occurrences, vec![1, 2]);
    assert_eq!(plan.dynamic_bind_count, 2);
}

/// Verifies that cloned anonymous vars preserve identity.
#[test]
fn cloned_anonymous_var_reuses_placeholder() {
    use db::queries::{__private::table, Dialect, from, var};

    let post = table("posts");
    let title = post.col::<String>("title");
    let term = var::<String>();
    let same = term.clone();
    let plan = from(&post)
        .filter(title.like(&term).or(title.like(&same)))
        .all::<PostRow>()
        .plan(Dialect::Postgres)
        .expect("anonymous var should render");

    assert_eq!(
        plan.sql,
        "SELECT post.id, post.title, post.comment_count FROM posts post WHERE ((post.title LIKE $1) OR (post.title LIKE $1))"
    );
    assert_eq!(plan.params["__var_1"].occurrences, vec![1, 1]);
}

/// Verifies that separately-created anonymous vars are distinct.
#[test]
fn distinct_anonymous_vars_do_not_collide() {
    use db::queries::{__private::table, Dialect, from, var};

    let post = table("posts");
    let title = post.col::<String>("title");
    let left = var::<String>();
    let right = var::<String>();
    let plan = from(&post)
        .filter(title.like(&left).or(title.like(&right)))
        .all::<PostRow>()
        .plan(Dialect::Postgres)
        .expect("anonymous vars should render");

    assert_eq!(
        plan.sql,
        "SELECT post.id, post.title, post.comment_count FROM posts post WHERE ((post.title LIKE $1) OR (post.title LIKE $2))"
    );
    assert_eq!(plan.params["__var_1"].position, 1);
    assert_eq!(plan.params["__var_2"].position, 2);
}

/// Verifies that duplicate immediate values remain distinct generated parameters.
#[test]
fn duplicate_values_create_distinct_binds() {
    use db::queries::{Dialect, from, val};

    let post_table = TypedPost::table();
    let plan = from(&post_table)
        .filter(
            post_table
                .title
                .eq(val("draft".to_string()))
                .or(post_table.title.eq(val("draft".to_string()))),
        )
        .all::<TypedPost>()
        .plan(Dialect::Postgres)
        .expect("select should render");

    assert_eq!(
        plan.sql,
        "SELECT typed_post.id, typed_post.author_id, typed_post.title, typed_post.published, typed_post.created_at FROM typed_posts typed_post WHERE ((typed_post.title = $1) OR (typed_post.title = $2))"
    );
    assert_eq!(plan.dynamic_bind_count, 2);
}

/// Verifies that a CTE can be used as the root source for a typed select.
#[test]
fn cte_root_renders_with_clause() {
    use db::queries::{__private::table, Dialect, from, val, var};

    let post = table("posts");
    let term = var::<String>().named("term");
    let recent = from(&post)
        .filter(post.col::<bool>("published").eq(val(true)))
        .all::<PostRow>()
        .cte_as("recent_posts")
        .expect("cte should build");

    let plan = from(&recent)
        .with(&recent)
        .filter(recent.title.like(&term))
        .all::<PostRow>()
        .plan(Dialect::Postgres)
        .expect("cte query should render");

    assert_eq!(
        plan.sql,
        "WITH recent_posts AS (SELECT post.id, post.title, post.comment_count FROM posts post WHERE (post.published = $1)) SELECT recent_posts.id, recent_posts.title, recent_posts.comment_count FROM recent_posts WHERE (recent_posts.title LIKE $2)"
    );
}

/// Verifies that repeated vars reuse Postgres placeholders across CTE and parent scopes.
#[test]
fn cte_and_parent_reuse_postgres_vars() {
    use db::queries::{__private::table, Dialect, from, var};

    let post = table("posts");
    let term = var::<String>().named("term");
    let recent = from(&post)
        .filter(post.col::<String>("title").like(&term))
        .all::<PostRow>()
        .cte_as("recent_posts")
        .expect("cte should build");

    let plan = from(&recent)
        .with(&recent)
        .filter(recent.title.ilike(&term))
        .all::<PostRow>()
        .plan(Dialect::Postgres)
        .expect("cte query should render");

    assert_eq!(
        plan.sql,
        "WITH recent_posts AS (SELECT post.id, post.title, post.comment_count FROM posts post WHERE (post.title LIKE $1)) SELECT recent_posts.id, recent_posts.title, recent_posts.comment_count FROM recent_posts WHERE (recent_posts.title ILIKE $1)"
    );
    assert_eq!(plan.params["term"].occurrences, vec![1, 1]);
}

/// Verifies that SQLite binds each CTE and parent var occurrence positionally.
#[test]
fn cte_and_parent_repeat_sqlite_vars() {
    use db::queries::{__private::table, Dialect, from, var};

    let post = table("posts");
    let term = var::<String>().named("term");
    let recent = from(&post)
        .filter(post.col::<String>("title").like(&term))
        .all::<PostRow>()
        .cte_as("recent_posts")
        .expect("cte should build");

    let plan = from(&recent)
        .with(&recent)
        .filter(recent.title.like(&term))
        .all::<PostRow>()
        .plan(Dialect::Sqlite)
        .expect("cte query should render");

    assert_eq!(
        plan.sql,
        "WITH recent_posts AS (SELECT post.id, post.title, post.comment_count FROM posts post WHERE (post.title LIKE ?)) SELECT recent_posts.id, recent_posts.title, recent_posts.comment_count FROM recent_posts WHERE (recent_posts.title LIKE ?)"
    );
    assert_eq!(plan.params["term"].occurrences, vec![1, 2]);
}

/// Verifies that CTEs can feed implicit Record references without explicit joins.
#[test]
fn cte_can_feed_scannable_reference() {
    use db::queries::{
        __private::{reference, table},
        Dialect, from, funcs, val,
    };

    let comment = table("comments");
    let counts_ref = reference("counts");
    let out = db::out::<CommentCountRow>();
    let counts = from(&comment)
        .group_by(comment.col::<i64>("post_id"))
        .all::<CommentCountRow>()
        .set(out.post_id, comment.col::<i64>("post_id"))
        .set(out.comment_count, funcs::count(comment.col::<i64>("id")))
        .cte_as("comment_counts")
        .expect("cte should build");

    let plan = from(table("posts"))
        .with(&counts)
        .filter(counts_ref.col::<i64>("comment_count").gte(val(5_i64)))
        .all::<PostWithCounts>()
        .plan(Dialect::Postgres)
        .expect("cte reference query should render");

    assert_eq!(
        plan.sql,
        "WITH comment_counts AS (SELECT comment.post_id AS post_id, COUNT(comment.id) AS comment_count FROM comments comment GROUP BY comment.post_id) SELECT post.id, post.title, counts.comment_count FROM posts post LEFT JOIN comment_counts counts ON counts.post_id = post.id WHERE (counts.comment_count >= $1)"
    );
}

/// Verifies that a typed subquery can be used inside an IN predicate.
#[test]
fn subquery_in_predicate_renders_one_column_select() {
    use db::queries::{__private::table, Dialect, from, val};

    let post = table("posts");
    let comment = table("comments");
    let active_post_ids = from(&comment)
        .filter(comment.col::<bool>("flagged").eq(val(false)))
        .all::<CommentPostIdRow>()
        .subquery_as("active_post_ids")
        .expect("subquery should build");

    let plan = from(&post)
        .filter(
            post.col::<i64>("id")
                .in_(active_post_ids.pick(&active_post_ids.post_id)),
        )
        .all::<PostRow>()
        .plan(Dialect::Postgres)
        .expect("subquery predicate should render");

    assert_eq!(
        plan.sql,
        "SELECT post.id, post.title, post.comment_count FROM posts post WHERE post.id IN (SELECT active_post_ids.post_id FROM (SELECT comment.post_id FROM comments comment WHERE (comment.flagged = $1)) active_post_ids)"
    );
}

/// Verifies that a CTE can feed a mutation through a one-column IN predicate.
#[test]
fn cte_can_feed_update_filter() {
    use db::queries::{__private::table, Dialect, from, var};

    let post = table("posts");
    let published = var::<bool>().named("published");
    let stale = from(&post)
        .filter(post.col::<bool>("published").eq(&published))
        .all::<PostIdRow>()
        .cte_as("stale_posts")
        .expect("cte should build");
    let patch = NewPost {
        title: "Archived".to_string(),
        view_count: 0,
    };

    let plan = from(&post)
        .with(&stale)
        .filter(post.col::<i64>("id").in_(stale.pick(&stale.id)))
        .update(&patch)
        .plan(Dialect::Postgres)
        .expect("update should render");

    assert_eq!(
        plan.sql,
        "WITH stale_posts AS (SELECT post.id FROM posts post WHERE (post.published = $3)) UPDATE posts SET title = $1, view_count = $2 WHERE id IN (SELECT id FROM stale_posts)"
    );
}

/// Verifies that write plans report pre-bound row values and dynamic bind counts separately.
#[test]
fn write_plans_report_prebound_and_total_binds() {
    use db::queries::{__private::table, Dialect, from, var};

    let post = table("posts");
    let id = var::<i64>().named("id");
    let patch = NewPost {
        title: "Updated".to_string(),
        view_count: 5,
    };

    let plan = from(&post)
        .filter(post.col::<i64>("id").eq(&id))
        .update(&patch)
        .plan(Dialect::Postgres)
        .expect("update should render");

    assert_eq!(plan.prebound_count, 2);
    assert_eq!(plan.dynamic_bind_count, 1);
    assert_eq!(plan.total_bind_count, 3);
}

/// Verifies that update and delete render valid root-column mutation SQL without aliases.
#[test]
fn mutation_plans_render_root_filters_without_undeclared_aliases() {
    use db::queries::{__private::table, Dialect, from, val, var};

    let post = table("posts");
    let id = var::<i64>().named("id");
    let patch = NewPost {
        title: "Updated".to_string(),
        view_count: 5,
    };

    let update = from(&post)
        .filter(post.col::<i64>("id").eq(&id))
        .update(&patch)
        .plan(Dialect::Postgres)
        .expect("update should render");
    assert_eq!(
        update.sql,
        "UPDATE posts SET title = $1, view_count = $2 WHERE (id = $3)"
    );

    let delete = from(&post)
        .filter(post.col::<i64>("view_count").gt(val(1_i64)))
        .delete()
        .plan(Dialect::Postgres)
        .expect("delete should render");
    assert_eq!(delete.sql, "DELETE FROM posts WHERE (view_count > $1)");
}

/// Verifies that unsafe update and delete statements require explicit filters.
#[test]
fn update_and_delete_require_filters() {
    use db::queries::{__private::table, Dialect, from};

    let post = table("posts");
    let patch = NewPost {
        title: "Updated".to_string(),
        view_count: 5,
    };

    let update = from(&post)
        .update(&patch)
        .plan(Dialect::Postgres)
        .expect_err("update without filter should fail");
    assert!(update.to_string().contains("require at least one filter"));

    let delete = from(&post)
        .delete()
        .plan(Dialect::Postgres)
        .expect_err("delete without filter should fail");
    assert!(delete.to_string().contains("require at least one filter"));
}

/// Verifies that batch insert and batch upsert render one SQL statement.
#[test]
fn batch_insert_and_upsert_render_one_statement() {
    use db::queries::{__private::table, Dialect, from};

    let post = table("posts");
    let id = post.col::<i64>("id");
    let rows = [
        NewPost {
            title: "First".to_string(),
            view_count: 1,
        },
        NewPost {
            title: "Second".to_string(),
            view_count: 2,
        },
    ];

    let insert = from(&post)
        .batch_insert(&rows)
        .plan(Dialect::Postgres)
        .expect("batch insert should render");
    assert_eq!(
        insert.sql,
        "INSERT INTO posts (title, view_count) VALUES ($1, $2), ($3, $4)"
    );

    let upsert = from(&post)
        .batch_upsert(&rows, [&id])
        .plan(Dialect::Postgres)
        .expect("batch upsert should render");
    assert_eq!(
        upsert.sql,
        "INSERT INTO posts (title, view_count) VALUES ($1, $2), ($3, $4) ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, view_count = EXCLUDED.view_count"
    );

    let sqlite = from(&post)
        .batch_upsert(&rows, [&id])
        .plan(Dialect::Sqlite)
        .expect("sqlite upsert should render");
    assert_eq!(
        sqlite.sql,
        "INSERT INTO posts (title, view_count) VALUES (?, ?), (?, ?) ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, view_count = EXCLUDED.view_count"
    );

    let mysql = from(&post)
        .batch_upsert(&rows, [&id])
        .plan(Dialect::Mysql)
        .expect("mysql upsert should render");
    assert_eq!(
        mysql.sql,
        "INSERT INTO posts (title, view_count) VALUES (?, ?), (?, ?) ON DUPLICATE KEY UPDATE title = VALUES(title), view_count = VALUES(view_count)"
    );
}

/// Verifies that borrowed write executables can be copied into owned operations.
#[test]
fn owned_write_executables_plan_after_payload_copy() {
    use db::queries::{__private::table, Dialect, from};

    let post = table("posts");
    let id = post.col::<i64>("id");
    let row = NewPost {
        title: "Owned".to_string(),
        view_count: 1,
    };
    let owned = from(&post).insert(&row).into_owned();
    let insert = owned
        .plan(Dialect::Postgres)
        .expect("owned insert should render");
    assert_eq!(
        insert.sql,
        "INSERT INTO posts (title, view_count) VALUES ($1, $2)"
    );

    let rows = [row.clone()];
    let owned_batch = from(&post).batch_upsert(&rows, [&id]).into_owned();
    let upsert = owned_batch
        .plan(Dialect::Postgres)
        .expect("owned batch upsert should render");
    assert!(upsert.sql.contains("ON CONFLICT (id) DO UPDATE"));
}

/// Verifies that write scopes can return a projection with `RETURNING`.
#[test]
fn returning_write_scope_renders_projection() {
    use db::queries::{Dialect, from, val};

    let post = TypedPost::table();
    let row = TypedPostPatch {
        title: "Updated".to_string(),
    };
    let insert_plan = from(&post)
        .returning::<TypedPostSummary>()
        .insert(&row)
        .plan(Dialect::Postgres)
        .expect("insert returning should render");
    assert_eq!(
        insert_plan.sql,
        "INSERT INTO typed_posts (title) VALUES ($1) RETURNING id, title"
    );

    let sqlite_insert = from(&post)
        .returning::<TypedPostSummary>()
        .insert(&row)
        .plan(Dialect::Sqlite)
        .expect("sqlite returning should render");
    assert_eq!(
        sqlite_insert.sql,
        "INSERT INTO typed_posts (title) VALUES (?) RETURNING id, title"
    );

    let update_plan = from(&post)
        .filter(post.id.eq(db::val(1_i64)))
        .returning::<TypedPostSummary>()
        .update(&row)
        .plan(Dialect::Postgres)
        .expect("update returning should render");
    assert_eq!(
        update_plan.sql,
        "UPDATE typed_posts SET title = $1 WHERE (id = $2) RETURNING id, title"
    );

    let delete_plan = from(&post)
        .filter(post.id.eq(db::val(1_i64)))
        .returning::<TypedPostSummary>()
        .delete()
        .plan(Dialect::Postgres)
        .expect("delete returning should render");
    assert_eq!(
        delete_plan.sql,
        "DELETE FROM typed_posts WHERE (id = $1) RETURNING id, title"
    );

    let computed = from(&post)
        .returning::<TypedPostSummary>()
        .set(db::out::<TypedPostSummary>().id, val(42_i64))
        .set(db::out::<TypedPostSummary>().title, &post.title)
        .insert(&row)
        .set(&post.title, val("Computed".to_string()))
        .plan(Dialect::Postgres)
        .expect("computed returning should render");
    assert_eq!(
        computed.sql,
        "INSERT INTO typed_posts (title) VALUES ($1) RETURNING $2 AS id, title AS title"
    );
}

/// Verifies that joined records are rejected as write-returning projections.
#[test]
fn returning_rejects_joined_projection() {
    use db::queries::{Dialect, from};

    let row = TypedPostPatch {
        title: "Updated".to_string(),
    };
    let err = from(TypedPost::table())
        .returning::<TypedPostWithAuthor>()
        .insert(&row)
        .plan(Dialect::Postgres)
        .expect_err("joined returning should fail");

    assert!(err.to_string().contains("joined reference"));
}

/// Verifies that upsert update sets never rewrite conflict columns.
#[test]
fn upsert_excludes_conflict_columns_from_update_set() {
    use db::queries::{__private::table, Dialect, from};

    let post = table("posts");
    let id = post.col::<i64>("id");
    let rows = [PostWithId {
        id: 1,
        title: "First".to_string(),
    }];

    let upsert = from(&post)
        .batch_upsert(&rows, [&id])
        .plan(Dialect::Postgres)
        .expect("batch upsert should render");

    assert_eq!(
        upsert.sql,
        "INSERT INTO posts (id, title) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title"
    );
}

/// Verifies that Postgres and SQLite use DO NOTHING when only conflict columns are bound.
#[test]
fn upsert_uses_do_nothing_without_update_columns() {
    use db::queries::{__private::table, Dialect, from};

    let post = table("posts");
    let id = post.col::<i64>("id");
    let rows = [PostKey { id: 1 }];

    let upsert = from(&post)
        .batch_upsert(&rows, [&id])
        .plan(Dialect::Postgres)
        .expect("batch upsert should render");

    assert_eq!(
        upsert.sql,
        "INSERT INTO posts (id) VALUES ($1) ON CONFLICT (id) DO NOTHING"
    );
}

/// Verifies that plan-time validation rejects stale or duplicated named binds.
#[test]
fn planning_rejects_unused_and_duplicate_binds() {
    use db::queries::{__private::table, Dialect, from, var};

    let post = table("posts");
    let id_col = post.col::<i64>("id");
    let used = var::<i64>().named("id");
    let unused_var = var::<i64>().named("unused");

    let unused = from(&post)
        .filter(id_col.eq(&used))
        .bind(&unused_var, 1_i64)
        .all::<PostRow>()
        .plan(Dialect::Postgres)
        .expect_err("unused bind should fail during planning");
    assert!(unused.to_string().contains("unused binding"));

    let id = var::<i64>().named("id");
    let duplicate = from(&post)
        .filter(post.col::<i64>("id").eq(&id))
        .bind(&id, 1_i64)
        .bind(&id, 2_i64)
        .all::<PostRow>()
        .plan(Dialect::Postgres)
        .expect_err("duplicate bind should fail during planning");
    assert!(duplicate.to_string().contains("duplicate binding"));

    let left = var::<i64>().named("same");
    let right = var::<i64>().named("same");
    let duplicate_name = from(&post)
        .filter(id_col.eq(&left).or(id_col.eq(&right)))
        .all::<PostRow>()
        .plan(Dialect::Postgres)
        .expect_err("duplicate var display names should fail");
    assert!(
        duplicate_name
            .to_string()
            .contains("duplicate placeholder name")
    );
}

/// Verifies that invalid bind names fail during query planning.
#[test]
fn planning_rejects_invalid_bind_names() {
    use db::queries::{Dialect, from, var};

    let reserved = var::<i64>().named("__typed_1");
    let invalid_reserved = from(TypedPost::table())
        .filter(TypedPost::table().id.eq(&reserved))
        .all::<TypedPost>()
        .plan(Dialect::Postgres)
        .expect_err("reserved bind names should fail");
    assert!(invalid_reserved.to_string().contains("invalid identifier"));

    let invalid = var::<i64>().named("123bad");
    let invalid_first = from(TypedPost::table())
        .filter(TypedPost::table().id.eq(&invalid))
        .all::<TypedPost>()
        .plan(Dialect::Postgres)
        .expect_err("invalid bind names should fail");
    assert!(invalid_first.to_string().contains("invalid identifier"));
}

/// Verifies that schema-qualified table handles are distinct owners.
#[test]
fn schema_qualified_table_owners_are_distinct() {
    use db::queries::{__private, Dialect, from, val};

    let public_users = __private::table_schema("public", "users");
    let auth_users = __private::table_schema("auth", "users");
    let public_id = public_users.col::<i64>("id");

    let error = from(&auth_users)
        .filter(public_id.eq(val(1_i64)))
        .all::<AuthUser>()
        .plan(Dialect::Postgres)
        .expect_err("schema-qualified owners should not match");

    assert!(error.to_string().contains("public.users"));
    assert!(error.to_string().contains("auth.users"));
}

/// Verifies that derived select targets must belong to the terminal row shape.
#[test]
fn select_targets_must_match_terminal_projection() {
    use db::queries::{__private::table, Dialect, from, funcs};

    let post = table("posts");
    let error = from(&post)
        .all::<PostRow>()
        .set(
            db::out::<CommentCountRow>().comment_count,
            funcs::count(post.col::<i64>("id")),
        )
        .plan(Dialect::Postgres)
        .expect_err("unknown projection should fail");

    assert!(error.to_string().contains("expected"));
}

/// Verifies that CTE definitions must be uniquely named and actually used.
#[test]
fn cte_validation_rejects_duplicate_and_unused_definitions() {
    use db::queries::{__private::table, Dialect, from};

    let post = table("posts");
    let first = from(&post)
        .all::<PostRow>()
        .cte_as("recent_posts")
        .expect("cte should build");
    let second = from(&post)
        .all::<PostRow>()
        .cte_as("recent_posts")
        .expect("cte should build");

    let duplicate = from(&first)
        .with(&first)
        .with(&second)
        .all::<PostRow>()
        .plan(Dialect::Postgres)
        .expect_err("duplicate CTE names should fail");
    assert!(duplicate.to_string().contains("duplicate CTE"));

    let unused = from(&post)
        .with(&first)
        .all::<PostRow>()
        .plan(Dialect::Postgres)
        .expect_err("unused CTE should fail");
    assert!(unused.to_string().contains("unused CTE"));
}

/// Verifies that CTE and subquery source names and columns are validated.
#[test]
fn cte_and_subquery_validation_rejects_invalid_sources() {
    use db::queries::{__private::table, Dialect, from};

    let post = table("posts");
    let invalid = from(&post)
        .all::<PostRow>()
        .cte_as("bad-name")
        .expect_err("invalid CTE name should fail");
    assert!(invalid.to_string().contains("invalid identifier"));

    let recent = from(&post)
        .all::<PostRow>()
        .cte_as("recent_posts")
        .expect("cte should build");
    let other = from(&post)
        .all::<PostIdRow>()
        .cte_as("other_posts")
        .expect("cte should build");
    let wrong_source = from(&post)
        .with(&recent)
        .filter(post.col::<i64>("id").in_(recent.pick(&other.id)))
        .all::<PostRow>()
        .plan(Dialect::Postgres)
        .expect_err("wrong CTE source column should fail");
    assert!(wrong_source.to_string().contains("picked column"));

    let not_registered = from(&recent)
        .all::<PostRow>()
        .plan(Dialect::Postgres)
        .expect_err("CTE root without with should fail");
    assert!(not_registered.to_string().contains("not registered"));
}

/// Verifies that invalid terminal combinations fail during planning.
#[test]
fn terminal_validation_rejects_invalid_combinations() {
    use db::queries::{
        __private::{reference, table},
        Dialect, from, val,
    };

    let post = table("posts");
    let author = reference("author");
    let patch = NewPost {
        title: "Updated".to_string(),
        view_count: 5,
    };

    let mutation_ref = from(&post)
        .filter(author.col::<bool>("active").eq(val(true)))
        .update(&patch)
        .plan(Dialect::Postgres)
        .expect_err("reference filters are not supported for mutations");
    assert!(mutation_ref.to_string().contains("reference column"));

    let typed_post = TypedPost::table();
    let mismatch = from(&typed_post)
        .insert(&UserRow {
            name: "Alice".to_string(),
        })
        .plan(Dialect::Postgres)
        .expect_err("invalid write columns should fail");
    assert!(mismatch.to_string().contains("not writable"));
}

// ---------------------------------------------------------------------------
// Projection shapes and reference join defaults.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, db::Model)]
#[table(name = "projection_profiles")]
struct ProjectionProfile {
    id: i64,
    name: String,
    bio: Option<String>,
}

#[derive(Debug, Clone, db::Model)]
#[table(name = "projection_users")]
struct ProjectionUser {
    id: i64,
    display_name: String,
}

#[derive(Debug, Clone, db::Model)]
#[table(name = "projection_users")]
struct ProjectionUserWithPrefetch {
    id: i64,
    display_name: String,
    #[column(prefetch = ProjectionUserPosts)]
    posts: Vec<ProjectionPost>,
}

#[derive(Debug, Clone, db::Model)]
#[table(name = "projection_posts")]
struct ProjectionPost {
    id: i64,
    author_id: i64,
    title: String,
}

struct ProjectionUserPosts;

impl db::Backref for ProjectionUserPosts {
    type From = ProjectionUser;
    type To = ProjectionPost;

    const NAME: &'static str = "ProjectionUserPosts";
    const CARDINALITY: db::RelationCardinality = db::RelationCardinality::Many;

    fn meta() -> db::ReferenceMeta {
        db::ReferenceMeta {
            logical_name: "posts",
            table_name: ProjectionPost::table_name(),
            table_schema: ProjectionPost::table_schema(),
            columns: &[db::JoinColumn {
                from: "id",
                to: "author_id",
            }],
            join_type: db::JoinType::Inner,
        }
    }
}

impl db::ManyBackref for ProjectionUserPosts {}

#[derive(Debug, Clone, db::Model)]
#[table(name = "projection_roles")]
struct ProjectionRole {
    id: i64,
    name: String,
}

#[derive(Debug, Clone, db::Model)]
#[table(name = "projection_user_roles")]
struct ProjectionUserRole {
    id: i64,
    user_id: i64,
    role_id: i64,
}

struct ProjectionUserRoles;

impl db::ManyToMany for ProjectionUserRoles {
    type From = ProjectionUser;
    type Through = ProjectionUserRole;
    type To = ProjectionRole;

    const NAME: &'static str = "ProjectionUserRoles";

    fn from_through() -> db::ReferenceMeta {
        db::ReferenceMeta {
            logical_name: "user_roles",
            table_name: ProjectionUserRole::table_name(),
            table_schema: ProjectionUserRole::table_schema(),
            columns: &[db::JoinColumn {
                from: "id",
                to: "user_id",
            }],
            join_type: db::JoinType::Inner,
        }
    }

    fn through_to() -> db::ReferenceMeta {
        db::ReferenceMeta {
            logical_name: "roles",
            table_name: ProjectionRole::table_name(),
            table_schema: ProjectionRole::table_schema(),
            columns: &[db::JoinColumn {
                from: "role_id",
                to: "id",
            }],
            join_type: db::JoinType::Inner,
        }
    }
}

#[derive(Debug, Clone, db::Model)]
#[table(name = "projection_tenant_users")]
struct ProjectionTenantUser {
    id: i64,
    tenant_id: i64,
    display_name: String,
}

#[derive(Debug, Clone, db::Model)]
#[table(name = "projection_tenant_posts")]
struct ProjectionTenantPost {
    id: i64,
    author_id: i64,
    tenant_id: i64,
    title: String,
}

#[derive(Debug, Clone, db::Record)]
struct ProjectionPostList {
    #[column(flatten)]
    post: ProjectionPost,
}

#[derive(Debug, Clone, db::Record)]
struct ProjectionPostWithAuthor {
    #[column(flatten)]
    post: ProjectionPost,
    #[column(reference(on(from = "author_id", to = "id")))]
    author: ProjectionUser,
}

#[derive(Debug, Clone, db::Record)]
struct ProjectionPostWithOptionalAuthor {
    #[column(flatten)]
    post: ProjectionPost,
    #[column(reference(on(from = "author_id", to = "id")))]
    author: Option<ProjectionUser>,
}

#[derive(Debug, Clone, db::Record)]
struct ProjectionPostWithInnerOptionalAuthor {
    #[column(flatten)]
    post: ProjectionPost,
    #[column(reference(join = "inner", on(from = "author_id", to = "id")))]
    author: Option<ProjectionUser>,
}

#[derive(Debug, Clone, db::Record)]
struct ProjectionPostWithLeftAuthor {
    #[column(flatten)]
    post: ProjectionPost,
    #[column(reference(join = "left", on(from = "author_id", to = "id")))]
    author: ProjectionUser,
}

#[derive(Debug, Clone, db::Record)]
struct ProjectionPostWithStdOptionalAuthor {
    #[column(flatten)]
    post: ProjectionPost,
    #[column(reference(on(from = "author_id", to = "id")))]
    author: std::option::Option<ProjectionUser>,
}

#[derive(Debug, Clone, db::Record)]
struct ProjectionPostWithCoreOptionalAuthor {
    #[column(flatten)]
    post: ProjectionPost,
    #[column(reference(on(from = "author_id", to = "id")))]
    author: core::option::Option<ProjectionUser>,
}

#[derive(Debug, Clone, db::Record)]
struct ProjectionTenantPostWithAuthor {
    #[column(flatten)]
    post: ProjectionTenantPost,
    #[column(reference(
        on(from = "author_id", to = "id"),
        on(from = "tenant_id", to = "tenant_id")
    ))]
    author: ProjectionTenantUser,
}

/// Verifies optional scalar fields are selected normally; they are not replaced with NULL.
#[test]
fn optional_scalar_field_is_selected() {
    use db::queries::{Dialect, from};

    let table = ProjectionProfile::table();
    let plan = from(&table)
        .all::<ProjectionProfile>()
        .plan(Dialect::Postgres)
        .expect("select should render");

    assert!(plan.sql.contains("projection_profile.bio"));
    assert!(!plan.sql.contains("NULL"));
}

/// Verifies a projection without a reference field emits no join.
#[test]
fn projection_without_reference_emits_no_join() {
    use db::queries::{Dialect, from};

    let table = ProjectionPost::table();
    let plan = from(&table)
        .all::<ProjectionPostList>()
        .plan(Dialect::Postgres)
        .expect("select should render");

    assert!(!plan.sql.contains("JOIN"));
    assert!(!plan.sql.contains("author.display_name"));
}

/// Verifies prefetch fields are application state, not selected or bound columns.
#[test]
fn prefetch_fields_are_not_table_columns() {
    use db::queries::{Dialect, from};

    let table = ProjectionUserWithPrefetch::table();
    let columns = ProjectionUserWithPrefetch::record_column_names();
    let bind_columns = ProjectionUserWithPrefetch::record_bind_column_names();
    let plan = from(&table)
        .all::<ProjectionUserWithPrefetch>()
        .plan(Dialect::Postgres)
        .expect("prefetch field model should render");

    assert_eq!(columns, vec!["id".to_string(), "display_name".to_string()]);
    assert_eq!(
        bind_columns,
        vec!["id".to_string(), "display_name".to_string()]
    );
    assert!(!plan.sql.contains("posts"));
}

/// Verifies non-optional reference fields default to inner joins.
#[test]
fn required_reference_defaults_to_inner_join() {
    use db::queries::{Dialect, from};

    let table = ProjectionPost::table();
    let plan = from(&table)
        .all::<ProjectionPostWithAuthor>()
        .plan(Dialect::Postgres)
        .expect("select should render");

    assert!(
        plan.sql
            .contains(" JOIN projection_users author ON author.id = post.author_id")
    );
    assert!(plan.sql.contains("author.display_name"));
}

/// Verifies `reference(on(...), on(...))` renders composite equality joins.
#[test]
fn composite_reference_on_pairs_render_and_join() {
    use db::queries::{Dialect, from};

    let table = ProjectionTenantPost::table();
    let plan = from(&table)
        .all::<ProjectionTenantPostWithAuthor>()
        .plan(Dialect::Postgres)
        .expect("composite reference should render");

    assert!(plan.sql.contains(
        "JOIN projection_tenant_users author ON author.id = post.author_id AND author.tenant_id = post.tenant_id"
    ));
}

/// Verifies relation backref filters render as correlated EXISTS predicates.
#[test]
fn backref_any_renders_correlated_exists() {
    use db::queries::{Dialect, from};

    let users = ProjectionUser::table();
    let posts = db::backref::<ProjectionUserPosts>(&users);
    let plan = from(&users)
        .filter(posts.any(|post| post.title.like(db::val("%vyuh%".to_string()))))
        .all::<ProjectionUser>()
        .plan(Dialect::Postgres)
        .expect("backref predicate should render");

    assert!(plan.sql.contains(
        "EXISTS (SELECT 1 FROM projection_posts posts WHERE posts.author_id = projection_user.id AND (posts.title LIKE $1))"
    ));
}

/// Verifies many-to-many filters render through the join table in one SQL statement.
#[test]
fn many_to_many_any_renders_joined_exists() {
    use db::queries::{Dialect, from};

    let users = ProjectionUser::table();
    let roles = db::many_to_many::<ProjectionUserRoles>(&users);
    let plan = from(&users)
        .filter(roles.any(|role| role.name.eq(db::val("admin".to_string()))))
        .all::<ProjectionUser>()
        .plan(Dialect::Postgres)
        .expect("many-to-many predicate should render");

    assert!(plan.sql.contains(
        "EXISTS (SELECT 1 FROM projection_user_roles user_roles JOIN projection_roles roles ON roles.id = user_roles.role_id WHERE user_roles.user_id = projection_user.id AND (roles.name = $1))"
    ));
}

/// Verifies relation aggregate helpers render correlated subquery expressions.
#[test]
fn backref_aggregate_renders_correlated_subquery() {
    use db::queries::{Dialect, from};

    let users = ProjectionUser::table();
    let posts = db::backref::<ProjectionUserPosts>(&users);
    let plan = from(&users)
        .scalar(posts.count())
        .plan(Dialect::Postgres)
        .expect("backref aggregate should render");

    assert!(plan.sql.contains(
        "SELECT (SELECT COUNT(*) FROM projection_posts posts WHERE posts.author_id = projection_user.id) FROM projection_users projection_user"
    ));
}

/// Verifies canonical `Option<T>` references default to left joins.
#[test]
fn option_reference_defaults_to_left_join() {
    use db::queries::{Dialect, from};

    let table = ProjectionPost::table();
    let plan = from(&table)
        .all::<ProjectionPostWithOptionalAuthor>()
        .plan(Dialect::Postgres)
        .expect("select should render");

    assert!(
        plan.sql
            .contains(" LEFT JOIN projection_users author ON author.id = post.author_id")
    );
}

/// Verifies explicit join overrides beat the optional-reference default.
#[test]
fn explicit_reference_join_overrides_default() {
    use db::queries::{Dialect, from};

    let table = ProjectionPost::table();
    let inner_plan = from(&table)
        .all::<ProjectionPostWithInnerOptionalAuthor>()
        .plan(Dialect::Postgres)
        .expect("inner override should render");
    let left_plan = from(&table)
        .all::<ProjectionPostWithLeftAuthor>()
        .plan(Dialect::Postgres)
        .expect("left override should render");

    assert!(
        inner_plan
            .sql
            .contains(" JOIN projection_users author ON author.id = post.author_id")
    );
    assert!(!inner_plan.sql.contains(" LEFT JOIN "));
    assert!(
        left_plan
            .sql
            .contains(" LEFT JOIN projection_users author ON author.id = post.author_id")
    );
}

/// Verifies fully-qualified standard Option paths are recognized for left joins.
#[test]
fn qualified_option_references_default_to_left_join() {
    use db::queries::{Dialect, from};

    let table = ProjectionPost::table();
    let std_plan = from(&table)
        .all::<ProjectionPostWithStdOptionalAuthor>()
        .plan(Dialect::Postgres)
        .expect("std option select should render");
    let core_plan = from(&table)
        .all::<ProjectionPostWithCoreOptionalAuthor>()
        .plan(Dialect::Postgres)
        .expect("core option select should render");

    assert!(std_plan.sql.contains(" LEFT JOIN projection_users author"));
    assert!(core_plan.sql.contains(" LEFT JOIN projection_users author"));
}

/// Verifies the `count` terminal renders `SELECT COUNT(*)`.
#[test]
fn count_terminal_renders_count_star() {
    use db::queries::{Dialect, from, val};

    let post_table = TypedPost::table();
    let plan = from(&post_table)
        .filter(post_table.published.eq(val(true)))
        .count()
        .plan(Dialect::Postgres)
        .expect("count should render");

    assert_eq!(
        plan.sql,
        "SELECT COUNT(*) FROM typed_posts typed_post WHERE (typed_post.published = $1)"
    );
}

/// Verifies the `exists` terminal wraps the filter in `SELECT EXISTS(...)`.
#[test]
fn exists_terminal_renders_exists_wrapper() {
    use db::queries::{Dialect, from, val};

    let post_table = TypedPost::table();
    let plan = from(&post_table)
        .filter(post_table.published.eq(val(true)))
        .exists()
        .plan(Dialect::Postgres)
        .expect("exists should render");

    assert_eq!(
        plan.sql,
        "SELECT EXISTS(SELECT 1 FROM typed_posts typed_post WHERE (typed_post.published = $1))"
    );
}

/// Verifies the `scalar` terminal renders an arbitrary expression projection.
#[test]
fn scalar_terminal_renders_expression() {
    use db::queries::{Dialect, from, funcs};

    let post_table = TypedPost::table();
    let plan = from(&post_table)
        .scalar(funcs::count(&post_table.id))
        .plan(Dialect::Postgres)
        .expect("scalar should render");

    assert_eq!(
        plan.sql,
        "SELECT COUNT(typed_post.id) FROM typed_posts typed_post"
    );
}
