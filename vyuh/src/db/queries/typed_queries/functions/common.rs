//! Portable typed SQL functions and expressions.

use std::borrow::Cow;

use crate::db::placeholders::Dialect;

use super::super::super::QueryError;
use super::super::expr::{Expr, IntoExpr};
use super::super::extension::{DbExpression, DbFunction, ExprRenderCtx, custom, func};

/// Creates a typed COUNT(...) expression.
pub fn count<T>(expr: impl IntoExpr<T>) -> Expr<i64> {
    func(Count, (expr.into_expr(),))
}

/// Creates a typed CURRENT_TIMESTAMP expression.
pub fn now() -> Expr<chrono::DateTime<chrono::Utc>> {
    custom(CurrentTimestamp)
}

#[derive(Clone)]
struct Count;

impl DbFunction<i64> for Count {
    fn name(&self, _dialect: Dialect) -> Result<Cow<'static, str>, QueryError> {
        Ok(Cow::Borrowed("COUNT"))
    }

    fn validate(&self, _dialect: Dialect, arity: usize) -> Result<(), QueryError> {
        if arity == 1 {
            return Ok(());
        }
        Err(QueryError::BindError(
            "COUNT requires exactly one argument".to_string(),
        ))
    }
}

#[derive(Clone)]
struct CurrentTimestamp;

impl DbExpression<chrono::DateTime<chrono::Utc>> for CurrentTimestamp {
    fn render(&self, _ctx: &mut ExprRenderCtx<'_>) -> Result<String, QueryError> {
        Ok("CURRENT_TIMESTAMP".to_string())
    }
}
