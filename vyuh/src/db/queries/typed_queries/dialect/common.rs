//! Shared SQL rendering helpers for typed-query dialects.

use crate::db::placeholders::Dialect;

use super::super::super::QueryError;
use super::super::expr::ColumnRef;
use super::super::validate::validate_identifier;

pub(super) fn unsupported(dialect: Dialect, feature: &str) -> QueryError {
    QueryError::BindError(format!(
        "{feature} is not supported for {}",
        dialect_name(dialect)
    ))
}

pub(super) fn dialect_name(dialect: Dialect) -> &'static str {
    match dialect {
        Dialect::Postgres => "postgres",
        Dialect::Sqlite => "sqlite",
        Dialect::Mysql => "mysql",
    }
}

pub(super) fn render_conflict(conflict: &[ColumnRef]) -> Result<String, QueryError> {
    let mut sql = String::new();
    for (idx, column) in conflict.iter().enumerate() {
        if idx > 0 {
            sql.push_str(", ");
        }
        validate_identifier(&column.name)?;
        sql.push_str(&column.name);
    }
    Ok(sql)
}

pub(super) fn render_excluded_update(update_columns: &[&str]) -> Result<String, QueryError> {
    let mut sql = String::new();
    for (idx, column) in update_columns.iter().enumerate() {
        if idx > 0 {
            sql.push_str(", ");
        }
        validate_identifier(column)?;
        sql.push_str(column);
        sql.push_str(" = EXCLUDED.");
        sql.push_str(column);
    }
    Ok(sql)
}

pub(super) fn render_columns(columns: &[String]) -> Result<String, QueryError> {
    let mut sql = String::new();
    for (idx, column) in columns.iter().enumerate() {
        if idx > 0 {
            sql.push_str(", ");
        }
        validate_identifier(column)?;
        sql.push_str(column);
    }
    Ok(sql)
}

pub(super) fn render_on_conflict(
    conflict: &[ColumnRef],
    update_columns: &[&str],
) -> Result<String, QueryError> {
    let conflict = render_conflict(conflict)?;
    if update_columns.is_empty() {
        return Ok(format!(" ON CONFLICT ({conflict}) DO NOTHING"));
    }
    Ok(format!(
        " ON CONFLICT ({conflict}) DO UPDATE SET {}",
        render_excluded_update(update_columns)?
    ))
}
