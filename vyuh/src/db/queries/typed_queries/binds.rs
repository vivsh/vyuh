//! Parameter planning and SQLx statement binding for typed queries.

use indexmap::IndexMap;
use std::collections::{HashMap, HashSet};

use crate::db::argvalue::ArgValue;
use crate::db::commons::Arguments;
use crate::db::interfaces::Record;

use super::super::{QueryError, Statement};
use super::expr::{ColumnRef, ExprNode};
use super::handles::Table;
use super::plan::{ParamSource, QueryPlan};
use super::source::Source;
use super::validate::{table_name, validate_identifier};

const GENERATED_PREFIX: &str = "__typed_";

pub(super) fn statement_from_plan(
    plan: QueryPlan,
    mut args: Arguments<'static>,
) -> Result<Statement, QueryError> {
    validate_unused_binds(&plan)?;
    for name in &plan.bind_order {
        let value = plan
            .values
            .get(name)
            .ok_or_else(|| QueryError::MissingBinding(name.clone()))?;
        value
            .bind_value(&mut args)
            .map_err(|err| QueryError::BindError(err.to_string()))?;
    }
    Ok(Statement::new(&plan.sql, args))
}

pub(super) fn finish_plan(plan: QueryPlan) -> Result<QueryPlan, QueryError> {
    validate_unused_binds(&plan)?;
    Ok(plan)
}

pub(super) fn validate_unused_binds(plan: &QueryPlan) -> Result<(), QueryError> {
    for name in plan.values.keys() {
        if name.starts_with(GENERATED_PREFIX) {
            continue;
        }
        match plan.params.get(name) {
            Some(spec) if spec.source == ParamSource::Var => {}
            Some(_) => {}
            None => return Err(QueryError::UnusedBinding(name.clone())),
        }
    }
    Ok(())
}

pub(super) fn collect_expr_binds(
    node: &ExprNode,
    values: &mut HashMap<String, ArgValue>,
) -> Result<(), QueryError> {
    match node {
        ExprNode::Column(_) | ExprNode::Value(_) => Ok(()),
        ExprNode::Binary { left, right, .. } | ExprNode::Bool { left, right, .. } => {
            collect_expr_binds(left, values)?;
            collect_expr_binds(right, values)
        }
        ExprNode::Unary { expr, .. } => collect_expr_binds(expr, values),
        ExprNode::Function { args, .. } | ExprNode::Custom { args, .. } => {
            for arg in args {
                collect_expr_binds(arg, values)?;
            }
            Ok(())
        }
        ExprNode::InSource { left, source } => {
            collect_expr_binds(left, values)?;
            collect_source_binds(&source.source, values)
        }
    }
}

pub(super) fn collect_source_binds(
    source: &Source,
    values: &mut HashMap<String, ArgValue>,
) -> Result<(), QueryError> {
    match source {
        Source::Subquery(subquery) => subquery.data.scope.collect_binds_into(values),
        Source::Cte(_) | Source::Table(_) => Ok(()),
    }
}

pub(super) fn collect_source_ctes(source: &Source, used: &mut HashSet<String>) {
    if let Source::Cte(cte) = source {
        used.insert(cte.data.name.to_string());
    }
}

pub(super) fn collect_expr_ctes(node: &ExprNode, used: &mut HashSet<String>) {
    match node {
        ExprNode::Column(_) | ExprNode::Value(_) => {}
        ExprNode::Binary { left, right, .. } | ExprNode::Bool { left, right, .. } => {
            collect_expr_ctes(left, used);
            collect_expr_ctes(right, used);
        }
        ExprNode::Unary { expr, .. } => collect_expr_ctes(expr, used),
        ExprNode::Function { args, .. } | ExprNode::Custom { args, .. } => {
            for arg in args {
                collect_expr_ctes(arg, used);
            }
        }
        ExprNode::InSource { left, source } => {
            collect_expr_ctes(left, used);
            collect_source_ctes(&source.source, used);
        }
    }
}

pub(super) fn validate_cte_usage(
    defined: &HashSet<String>,
    used: &HashSet<String>,
) -> Result<(), QueryError> {
    for name in defined {
        if !used.contains(name) {
            return Err(QueryError::BindError(format!("unused CTE '{}'", name)));
        }
    }
    for name in used {
        if !defined.contains(name) {
            return Err(QueryError::BindError(format!(
                "CTE '{}' is not registered",
                name
            )));
        }
    }
    Ok(())
}

pub(super) fn insert_bind(
    values: &mut HashMap<String, ArgValue>,
    name: &str,
    value: ArgValue,
) -> Result<(), QueryError> {
    if values.contains_key(name) {
        return Err(QueryError::BindError(format!(
            "duplicate binding for '{}'",
            name
        )));
    }
    values.insert(name.to_string(), value);
    Ok(())
}

pub(super) fn validate_select_exprs(
    exprs: &IndexMap<String, ExprNode>,
    columns: &[String],
) -> Result<(), QueryError> {
    for name in exprs.keys() {
        validate_identifier(name).map_err(|_| QueryError::InvalidProjection(name.to_string()))?;
        if !columns.iter().any(|column| column == name) {
            return Err(QueryError::InvalidProjection(name.to_string()));
        }
    }
    Ok(())
}

pub(super) fn bind_columns<T>() -> Result<Vec<String>, QueryError>
where
    T: Record,
{
    let columns = T::record_bind_column_names();
    if columns.is_empty() {
        return Err(QueryError::BindError("no bindable columns".to_string()));
    }
    for column in &columns {
        validate_identifier(column)?;
    }
    Ok(columns)
}

pub(super) fn validate_bind_columns(table: &Table, columns: &[String]) -> Result<(), QueryError> {
    let Some(known) = table.data.columns.as_ref() else {
        return Ok(());
    };
    for column in columns {
        if !known.iter().any(|known| known == column) {
            return Err(QueryError::BindError(format!(
                "column '{}' is not writable for {}",
                column,
                table_name(table.data.schema.as_deref(), table.data.name.as_ref())
            )));
        }
    }
    Ok(())
}

pub(super) fn upsert_update_columns<'a>(
    columns: &'a [String],
    conflict: &[ColumnRef],
) -> Result<Vec<&'a str>, QueryError> {
    let mut update_columns = Vec::with_capacity(columns.len());
    for column in columns {
        validate_identifier(column)?;
        if !conflict
            .iter()
            .any(|conflict| conflict.name.as_ref() == column)
        {
            update_columns.push(column.as_str());
        }
    }
    Ok(update_columns)
}

pub(super) fn bind_rows<T>(rows: &[T], col_count: usize) -> Result<Arguments<'static>, QueryError>
where
    T: Record,
{
    if rows.is_empty() {
        return Err(QueryError::BindError(
            "cannot insert empty list".to_string(),
        ));
    }
    let mut args = Arguments::default();
    for row in rows {
        bind_row(row, col_count, &mut args)?;
    }
    Ok(args)
}

pub(super) fn bind_row<T>(
    row: &T,
    col_count: usize,
    args: &mut Arguments<'static>,
) -> Result<(), QueryError>
where
    T: Record,
{
    use sqlx::Arguments as _;

    let before = args.len();
    row.record_bind_values(args)
        .map_err(|err| QueryError::BindError(err.to_string()))?;
    let bound = args.len().saturating_sub(before);
    if bound != col_count {
        return Err(QueryError::BindCountMismatch {
            expected: col_count,
            got: bound,
        });
    }
    Ok(())
}
