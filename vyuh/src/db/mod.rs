mod argvalue;
mod commons;
mod executor;
mod interfaces;
mod migrations;
mod placeholders;
pub mod queries;

pub mod mock;

pub use argvalue::ArgValue;
pub use commons::{Arguments, Database, Pool, QueryResult, Row};
pub use executor::*;
pub use interfaces::{JoinType, Model, ModelSchema, Record, RecordSchema, ReferenceMeta};
pub use migrations::{
    Column, ColumnRef, ColumnType, Constraint, Dialect, Index, IntoTable, Schema, SchemaBuilder,
    Table, TableBuilder,
};
#[cfg(feature = "migrations")]
pub use migrations::{
    EmbeddedMigrations, MigrationError, MigrationRegistry, MigrationSource, SchemaSource,
    crate_migration, crate_schema, embedded_migrations, root_migration, root_schema,
};
pub use queries::typed;
pub use queries::{
    DbExpression, DbFunction, Expr, ExprRenderCtx, FilterOp, FilterPredicate, FilterValue,
    Filterable, FunctionArgs, Page, ParamSource, ParamSpec, QueryError, QueryPlan, RawQuery,
    Statement,
};
pub use queries::{SourceKind, SourceMeta, count, custom, from, func, meta, now, val, var};
pub use sqlx::test as test_db;
pub use vyuh_macros::{Filterable, Model, Record};

/// Start a raw SQL query with named-bind support.
pub fn query(sql: &str) -> RawQuery {
    RawQuery::new(sql)
}
