//! Dialect-specific rendering and feature validation for typed queries.

use std::borrow::Cow;

use crate::db::placeholders::Dialect;

use super::super::QueryError;
use super::expr::ColumnRef;
use super::validate::validate_identifier;

mod common;
mod features;
mod mysql;
mod postgres;
mod sqlite;

pub(super) use features::DialectFeature;

static POSTGRES: postgres::PostgresSpec = postgres::PostgresSpec;
static SQLITE: sqlite::SqliteSpec = sqlite::SqliteSpec;
static MYSQL: mysql::MysqlSpec = mysql::MysqlSpec;

/// Internal dialect renderer used by the typed-query planner.
///
/// Common SQL is rendered by the shared renderer. This trait owns only syntax
/// and feature points that genuinely differ between supported backends.
pub(super) trait DialectRenderer: Send + Sync {
    fn dialect(&self) -> Dialect;

    fn placeholder(&self, position: usize) -> String;

    fn validate_feature(&self, feature: DialectFeature) -> Result<(), QueryError>;

    fn render_upsert(
        &self,
        conflict: &[ColumnRef],
        update_columns: &[&str],
    ) -> Result<String, QueryError>;

    fn render_returning(&self, columns: &[String]) -> Result<String, QueryError> {
        self.validate_feature(DialectFeature::Returning)?;
        if columns.is_empty() {
            return Err(QueryError::BindError(
                "RETURNING requires at least one column".to_string(),
            ));
        }
        Ok(format!(" RETURNING {}", common::render_columns(columns)?))
    }

    fn render_function(&self, name: Cow<'static, str>) -> Result<Cow<'static, str>, QueryError> {
        validate_identifier(&name)?;
        Ok(name)
    }
}

pub(super) fn renderer(dialect: Dialect) -> &'static dyn DialectRenderer {
    match dialect {
        Dialect::Postgres => &POSTGRES,
        Dialect::Sqlite => &SQLITE,
        Dialect::Mysql => &MYSQL,
    }
}

pub(super) fn validate_feature(
    dialect: Dialect,
    feature: DialectFeature,
) -> Result<(), QueryError> {
    renderer(dialect).validate_feature(feature)
}

pub(super) fn render_function(
    dialect: Dialect,
    name: Cow<'static, str>,
) -> Result<Cow<'static, str>, QueryError> {
    renderer(dialect).render_function(name)
}
