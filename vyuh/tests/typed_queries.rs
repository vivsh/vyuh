use std::borrow::Cow;
use std::hash::Hash;

use vyuh::prelude::*;

#[derive(Debug)]
struct PostRow;

#[allow(dead_code)]
#[derive(Clone)]
struct PostRowCols {
    id: db::typed::__private::ProjectedColumn<i64>,
    title: db::typed::__private::ProjectedColumn<String>,
    comment_count: db::typed::__private::ProjectedColumn<i64>,
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

impl db::typed::__private::Projectable for PostRow {
    type Columns = PostRowCols;

    fn projected_columns(source: db::typed::__private::ProjectionSource) -> Self::Columns {
        PostRowCols {
            id: source.col("id"),
            title: source.col("title"),
            comment_count: source.col("comment_count"),
        }
    }
}

#[derive(Debug)]
struct PostWithAuthor;

impl db::Record for PostWithAuthor {
    fn record_schema() -> db::RecordSchema<Self> {
        db::RecordSchema::new("posts")
            .root("post")
            .references(vec![db::ReferenceMeta {
                logical_name: "author",
                table_name: "users",
                table_schema: None,
                from_column: "post.author_id",
                to_column: "id",
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

#[derive(Debug)]
struct PostIdRow;

#[derive(Clone)]
struct PostIdCols {
    id: db::typed::__private::ProjectedColumn<i64>,
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

impl db::typed::__private::Projectable for PostIdRow {
    type Columns = PostIdCols;

    fn projected_columns(source: db::typed::__private::ProjectionSource) -> Self::Columns {
        PostIdCols {
            id: source.col("id"),
        }
    }
}

#[derive(Debug)]
struct CommentPostIdRow;

#[derive(Clone)]
struct CommentPostIdCols {
    post_id: db::typed::__private::ProjectedColumn<i64>,
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

impl db::typed::__private::Projectable for CommentPostIdRow {
    type Columns = CommentPostIdCols;

    fn projected_columns(source: db::typed::__private::ProjectionSource) -> Self::Columns {
        CommentPostIdCols {
            post_id: source.col("post_id"),
        }
    }
}

#[derive(Debug)]
struct CommentCountRow;

#[allow(dead_code)]
#[derive(Clone)]
struct CommentCountCols {
    post_id: db::typed::__private::ProjectedColumn<i64>,
    comment_count: db::typed::__private::ProjectedColumn<i64>,
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

impl db::typed::__private::Projectable for CommentCountRow {
    type Columns = CommentCountCols;

    fn projected_columns(source: db::typed::__private::ProjectionSource) -> Self::Columns {
        CommentCountCols {
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
                from_column: "post.id",
                to_column: "post_id",
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

#[derive(Debug, Clone, db::Record)]
struct TypedPostWithAuthor {
    #[column(flatten)]
    post: TypedPost,
    #[column(reference(from = "author_id", to = "id"))]
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

#[derive(Clone)]
struct LowerExpr {
    title: db::typed::__private::Column<String>,
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

    assert_handle::<db::typed::__private::ModelTable<TypedPost>>();
    assert_handle::<db::typed::__private::Column<i64>>();
    assert_handle::<db::typed::Var<String>>();
}

/// Verifies that source columns support the public query shape with terminal projections.
#[test]
fn generated_handles_support_public_select_shape() {
    use db::typed::{Dialect, from, val};

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

/// Verifies that source deref handles allow columns that collide with compatibility names.
#[test]
fn source_deref_supports_reserved_column_names() {
    use db::typed::{Dialect, from, val};

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
    use db::typed::{from, val};

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
        .select_expr("post_id", &comment.post_id)
        .all::<TypedCommentPostId>()
        .subquery()
        .expect("subquery should build");
    let subquery_meta = db::meta(&active);
    assert_eq!(subquery_meta.kind(), db::SourceKind::Subquery);
    assert_eq!(subquery_meta.name(), "subquery_typedcommentpostid");
    assert_eq!(subquery_meta.output_columns(), ["post_id"]);

    let stale = from(&post)
        .filter(post.published.eq(val(false)))
        .select_expr("id", &post.id)
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
    use db::typed::{Dialect, from, val};

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

/// Verifies that dialect validation rejects PostgreSQL-only ILIKE on SQLite.
#[test]
fn sqlite_rejects_ilike() {
    use db::typed::{Dialect, from, var};

    let post_table = TypedPost::table();
    let error = from(&post_table)
        .filter(post_table.title.ilike(var("term")))
        .all::<TypedPost>()
        .plan(Dialect::Sqlite)
        .expect_err("sqlite should reject ILIKE");

    assert!(error.to_string().contains("ILIKE"));
}

/// Verifies that MySQL returning writes fail during planning with a dialect error.
#[test]
fn mysql_rejects_returning() {
    use db::typed::{Dialect, from};

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
    use db::typed::{Dialect, from, func, val};

    let post_table = TypedPost::table();
    let plan = from(&post_table)
        .filter(func(Unaccent, (&post_table.title,)).eq(val("hello".to_string())))
        .all::<TypedPost>()
        .plan(Dialect::Postgres)
        .expect("custom function should render");

    assert!(plan.sql.contains("unaccent(typed_post.title)"));
}

/// Verifies that custom function validation can reject unsupported dialects.
#[test]
fn custom_function_can_reject_dialect() {
    use db::typed::{Dialect, from, func, val};

    let post_table = TypedPost::table();
    let error = from(&post_table)
        .filter(func(Unaccent, (&post_table.title,)).eq(val("hello".to_string())))
        .all::<TypedPost>()
        .plan(Dialect::Sqlite)
        .expect_err("custom function should reject sqlite");

    assert!(error.to_string().contains("unaccent"));
}

/// Verifies that PostgreSQL helper functions are isolated under the dialect namespace.
#[test]
fn postgres_helper_renders_and_rejects_other_dialects() {
    use db::typed::{Dialect, from, val};

    let post_table = TypedPost::table();
    let plan = from(&post_table)
        .filter(db::typed::postgres::unaccent(&post_table.title).eq(val("hello".to_string())))
        .all::<TypedPost>()
        .plan(Dialect::Postgres)
        .expect("postgres helper should render");

    assert!(plan.sql.contains("unaccent(typed_post.title)"));

    let error = from(&post_table)
        .filter(db::typed::postgres::unaccent(&post_table.title).eq(val("hello".to_string())))
        .all::<TypedPost>()
        .plan(Dialect::Sqlite)
        .expect_err("postgres helper should reject sqlite");

    assert!(error.to_string().contains("unaccent"));
    assert!(error.to_string().contains("sqlite"));
}

/// Verifies that custom expressions can render typed SQL using child expressions.
#[test]
fn custom_expression_renders_with_arguments() {
    use db::typed::{Dialect, custom, from, val};

    let post_table = TypedPost::table();
    let plan = from(&post_table)
        .filter(
            custom(LowerExpr {
                title: post_table.title.clone(),
            })
            .eq(val("draft".to_string())),
        )
        .all::<TypedPost>()
        .plan(Dialect::Postgres)
        .expect("custom expression should render");

    assert!(plan.sql.contains("LOWER(typed_post.title)"));
}

/// Verifies that derive-generated projected columns support subquery `pick(...)`.
#[test]
fn generated_handles_support_public_subquery_pick_shape() {
    use db::typed::{Dialect, from, val};

    let comment_table = TypedComment::table();
    let post_table = TypedPost::table();
    let active = from(&comment_table)
        .filter(comment_table.flagged.eq(val(false)))
        .select_expr("post_id", &comment_table.post_id)
        .all::<TypedCommentPostId>()
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

/// Verifies that Record patch rows work with model-provided table handles.
#[test]
fn bindable_patch_rows_work_with_model_table_handles() {
    use db::typed::{Dialect, from, var};

    let post_table = TypedPost::table();
    let patch = TypedPostPatch {
        title: "Updated".to_string(),
    };

    let insert = from(&post_table)
        .insert(&patch)
        .plan(Dialect::Postgres)
        .expect("insert should render");
    assert_eq!(insert.sql, "INSERT INTO typed_posts (title) VALUES ($1)");

    let update = from(&post_table)
        .filter(post_table.id.eq(var("id")))
        .update(&patch)
        .plan(Dialect::Postgres)
        .expect("update should render");
    assert_eq!(
        update.sql,
        "UPDATE typed_posts SET title = $1 WHERE (id = $2)"
    );
}

/// Verifies that SELECT rendering uses only object columns and implicit Record references.
#[test]
fn all_plan_renders_implicit_join_from_scannable_references() {
    use db::typed::{
        __private::{reference, table},
        Dialect, count, from, val, var,
    };

    let post = table("posts");
    let author = reference("author");
    let author_id = author.col::<i64>("id");

    let plan = from(&post)
        .filter(post.col::<String>("phone").ilike(var("phone")))
        .filter(author.col::<bool>("active").eq(val(true)))
        .select_expr("comment_count", count(&author_id))
        .group_by(post.col::<i64>("id"))
        .having(count(&author_id).gt(val(0_i64)))
        .order_by(
            post.col::<chrono::DateTime<chrono::Utc>>("created_at")
                .desc(),
        )
        .all::<PostWithAuthor>()
        .plan(Dialect::Postgres)
        .expect("select should render");

    assert_eq!(
        plan.sql,
        "SELECT post.id, post.title, author.display_name, COUNT(author.id) AS comment_count FROM posts post JOIN users author ON author.id = post.author_id WHERE (post.phone ILIKE $1) AND (author.active = $2) GROUP BY post.id HAVING (COUNT(author.id) > $3) ORDER BY post.created_at DESC"
    );
    assert_eq!(plan.params["phone"].position, 1);
    assert_eq!(plan.params["phone"].source, db::typed::ParamSource::Var);
}

/// Verifies that slice planning is terminal-shaped and adds LIMIT/OFFSET.
#[test]
fn slice_plan_renders_limit_and_offset() {
    use db::typed::{__private::table, Dialect, from};

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
    use db::typed::{__private::table, Dialect, from, var};

    let post = table("posts");
    let title = post.col::<String>("title");
    let plan = from(&post)
        .filter(title.like(var("term")).or(title.ilike(var("term"))))
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
    use db::typed::{__private::table, Dialect, from, var};

    let post = table("posts");
    let title = post.col::<String>("title");
    let plan = from(&post)
        .filter(title.like(var("term")).or(title.like(var("term"))))
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

/// Verifies that duplicate immediate values remain distinct generated parameters.
#[test]
fn duplicate_values_create_distinct_binds() {
    use db::typed::{Dialect, from, val};

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
    use db::typed::{__private::table, Dialect, from, val, var};

    let post = table("posts");
    let recent = from(&post)
        .filter(post.col::<bool>("published").eq(val(true)))
        .all::<PostRow>()
        .cte_as("recent_posts")
        .expect("cte should build");

    let plan = from(&recent)
        .with(&recent)
        .filter(recent.title.like(var("term")))
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
    use db::typed::{__private::table, Dialect, from, var};

    let post = table("posts");
    let recent = from(&post)
        .filter(post.col::<String>("title").like(var("term")))
        .all::<PostRow>()
        .cte_as("recent_posts")
        .expect("cte should build");

    let plan = from(&recent)
        .with(&recent)
        .filter(recent.title.ilike(var("term")))
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
    use db::typed::{__private::table, Dialect, from, var};

    let post = table("posts");
    let recent = from(&post)
        .filter(post.col::<String>("title").like(var("term")))
        .all::<PostRow>()
        .cte_as("recent_posts")
        .expect("cte should build");

    let plan = from(&recent)
        .with(&recent)
        .filter(recent.title.like(var("term")))
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
    use db::typed::{
        __private::{reference, table},
        Dialect, count, from, val,
    };

    let comment = table("comments");
    let counts_ref = reference("counts");
    let counts = from(&comment)
        .select_expr("post_id", comment.col::<i64>("post_id"))
        .select_expr("comment_count", count(comment.col::<i64>("id")))
        .group_by(comment.col::<i64>("post_id"))
        .all::<CommentCountRow>()
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
    use db::typed::{__private::table, Dialect, from, val};

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
    use db::typed::{__private::table, Dialect, from, var};

    let post = table("posts");
    let stale = from(&post)
        .filter(post.col::<bool>("published").eq(var("published")))
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
    use db::typed::{__private::table, Dialect, from, var};

    let post = table("posts");
    let patch = NewPost {
        title: "Updated".to_string(),
        view_count: 5,
    };

    let plan = from(&post)
        .filter(post.col::<i64>("id").eq(var("id")))
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
    use db::typed::{__private::table, Dialect, from, val, var};

    let post = table("posts");
    let patch = NewPost {
        title: "Updated".to_string(),
        view_count: 5,
    };

    let update = from(&post)
        .filter(post.col::<i64>("id").eq(var("id")))
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

/// Verifies that batch insert and batch upsert render one SQL statement.
#[test]
fn batch_insert_and_upsert_render_one_statement() {
    use db::typed::{__private::table, Dialect, from};

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
    use db::typed::{__private::table, Dialect, from};

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
    use db::typed::{Dialect, from};

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
}

/// Verifies that joined records are rejected as write-returning projections.
#[test]
fn returning_rejects_joined_projection() {
    use db::typed::{Dialect, from};

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
    use db::typed::{__private::table, Dialect, from};

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
    use db::typed::{__private::table, Dialect, from};

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
    use db::typed::{__private::table, Dialect, from, var};

    let post = table("posts");
    let id = post.col::<i64>("id");

    let unused = from(&post)
        .filter(id.eq(var("id")))
        .bind("unused", 1_i64)
        .all::<PostRow>()
        .plan(Dialect::Postgres)
        .expect_err("unused bind should fail during planning");
    assert!(unused.to_string().contains("unused binding"));

    let duplicate = from(&post)
        .filter(post.col::<i64>("id").eq(var("id")))
        .bind("id", 1_i64)
        .bind("id", 2_i64)
        .all::<PostRow>()
        .plan(Dialect::Postgres)
        .expect_err("duplicate bind should fail during planning");
    assert!(duplicate.to_string().contains("duplicate binding"));
}

/// Verifies that invalid bind names fail during query planning.
#[test]
fn planning_rejects_invalid_bind_names() {
    use db::typed::{Dialect, from};

    let invalid_reserved = from(TypedPost::table())
        .bind("__typed_1", 1_i64)
        .all::<TypedPost>()
        .plan(Dialect::Postgres)
        .expect_err("reserved bind names should fail");
    assert!(invalid_reserved.to_string().contains("invalid identifier"));

    let invalid_first = from(TypedPost::table())
        .bind("123bad", 1_i64)
        .all::<TypedPost>()
        .plan(Dialect::Postgres)
        .expect_err("invalid bind names should fail");
    assert!(invalid_first.to_string().contains("invalid identifier"));
}

/// Verifies that schema-qualified table handles are distinct owners.
#[test]
fn schema_qualified_table_owners_are_distinct() {
    use db::typed::{__private, Dialect, from, val};

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

/// Verifies that derived select expressions must target fields in the terminal row shape.
#[test]
fn select_expr_must_match_scannable_projection() {
    use db::typed::{__private::table, Dialect, count, from};

    let post = table("posts");
    let error = from(&post)
        .select_expr("missing_count", count(post.col::<i64>("id")))
        .all::<PostRow>()
        .plan(Dialect::Postgres)
        .expect_err("unknown projection should fail");

    assert!(error.to_string().contains("invalid projection"));
}

/// Verifies that CTE definitions must be uniquely named and actually used.
#[test]
fn cte_validation_rejects_duplicate_and_unused_definitions() {
    use db::typed::{__private::table, Dialect, from};

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
    use db::typed::{__private::table, Dialect, from};

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
    use db::typed::{
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
#[table(name = "projection_posts")]
struct ProjectionPost {
    id: i64,
    author_id: i64,
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
    #[column(reference(from = "author_id", to = "id"))]
    author: ProjectionUser,
}

#[derive(Debug, Clone, db::Record)]
struct ProjectionPostWithOptionalAuthor {
    #[column(flatten)]
    post: ProjectionPost,
    #[column(reference(from = "author_id", to = "id"))]
    author: Option<ProjectionUser>,
}

#[derive(Debug, Clone, db::Record)]
struct ProjectionPostWithInnerOptionalAuthor {
    #[column(flatten)]
    post: ProjectionPost,
    #[column(reference(from = "author_id", to = "id", join = "inner"))]
    author: Option<ProjectionUser>,
}

#[derive(Debug, Clone, db::Record)]
struct ProjectionPostWithLeftAuthor {
    #[column(flatten)]
    post: ProjectionPost,
    #[column(reference(from = "author_id", to = "id", join = "left"))]
    author: ProjectionUser,
}

#[derive(Debug, Clone, db::Record)]
struct ProjectionPostWithStdOptionalAuthor {
    #[column(flatten)]
    post: ProjectionPost,
    #[column(reference(from = "author_id", to = "id"))]
    author: std::option::Option<ProjectionUser>,
}

#[derive(Debug, Clone, db::Record)]
struct ProjectionPostWithCoreOptionalAuthor {
    #[column(flatten)]
    post: ProjectionPost,
    #[column(reference(from = "author_id", to = "id"))]
    author: core::option::Option<ProjectionUser>,
}

/// Verifies optional scalar fields are selected normally; they are not replaced with NULL.
#[test]
fn optional_scalar_field_is_selected() {
    use db::typed::{Dialect, from};

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
    use db::typed::{Dialect, from};

    let table = ProjectionPost::table();
    let plan = from(&table)
        .all::<ProjectionPostList>()
        .plan(Dialect::Postgres)
        .expect("select should render");

    assert!(!plan.sql.contains("JOIN"));
    assert!(!plan.sql.contains("author.display_name"));
}

/// Verifies non-optional reference fields default to inner joins.
#[test]
fn required_reference_defaults_to_inner_join() {
    use db::typed::{Dialect, from};

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

/// Verifies canonical `Option<T>` references default to left joins.
#[test]
fn option_reference_defaults_to_left_join() {
    use db::typed::{Dialect, from};

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
    use db::typed::{Dialect, from};

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
    use db::typed::{Dialect, from};

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
    use db::typed::{Dialect, from, val};

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
    use db::typed::{Dialect, from, val};

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
    use db::typed::{Dialect, count, from};

    let post_table = TypedPost::table();
    let plan = from(&post_table)
        .scalar(count(&post_table.id))
        .plan(Dialect::Postgres)
        .expect("scalar should render");

    assert_eq!(
        plan.sql,
        "SELECT COUNT(typed_post.id) FROM typed_posts typed_post"
    );
}
