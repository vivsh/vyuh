//! PostgreSQL-specific typed SQL helpers.

use std::borrow::Cow;

use crate::db::placeholders::Dialect;

use super::super::super::QueryError;
use super::super::expr::{Expr, IntoExpr};
use super::super::extension::{DbFunction, func};

/// Creates a typed `unaccent(expr)` expression.
pub fn unaccent(expr: impl IntoExpr<String>) -> Expr<String> {
    func(Unaccent, (expr.into_expr(),))
}

#[derive(Clone, Copy, Debug)]
struct Unaccent;

impl DbFunction<String> for Unaccent {
    fn name(&self, _dialect: Dialect) -> Result<Cow<'static, str>, QueryError> {
        Ok(Cow::Borrowed("unaccent"))
    }

    fn validate(&self, dialect: Dialect, _arity: usize) -> Result<(), QueryError> {
        if dialect == Dialect::Postgres {
            return Ok(());
        }
        Err(QueryError::BindError(format!(
            "unaccent is not supported for {}",
            dialect_name(dialect)
        )))
    }
}

fn dialect_name(dialect: Dialect) -> &'static str {
    match dialect {
        Dialect::Postgres => "postgres",
        Dialect::Sqlite => "sqlite",
        Dialect::Mysql => "mysql",
    }
}
