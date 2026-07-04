//! Validation and source-shape helpers for typed queries.

use std::any::type_name;
use std::collections::HashSet;

use crate::db::interfaces::{Record, ReferenceMeta};
use crate::db::placeholders::Dialect;

use super::super::QueryError;
use super::dialect::{self, DialectFeature};
use super::expr::{ColumnRef, ExprNode};
use super::handles::{ColumnOwner, Table};
use super::plan::{ParamSource, ParamSpec};
use super::render::SelectModel;
use super::source::{Source, SourceColumnRef};

const GENERATED_PREFIX: &str = "__typed_";

pub(super) fn validate_table_parts(
    source: &Table,
    schema: Option<&'static str>,
    table: &'static str,
) -> Result<(), QueryError> {
    if source.data.name.as_ref() != table || source.data.schema.as_deref() != schema {
        return Err(QueryError::TableMismatch {
            expected: table_name(schema, table),
            got: table_name(source.data.schema.as_deref(), source.data.name.as_ref()),
        });
    }
    Ok(())
}

pub(super) fn table_name(schema: Option<&str>, table: &str) -> String {
    match schema {
        Some(schema) => format!("{schema}.{table}"),
        None => table.to_string(),
    }
}

pub(super) fn source_key(source: &Source) -> (&'static str, Option<&str>, &str) {
    match source {
        Source::Table(table) => (
            "table",
            table.data.schema.as_deref(),
            table.data.name.as_ref(),
        ),
        Source::Cte(cte) => ("cte", None, cte.data.name.as_ref()),
        Source::Subquery(subquery) => ("subquery", None, subquery.data.name.as_ref()),
    }
}

pub(super) fn source_label(source: &Source) -> String {
    let (_, schema, name) = source_key(source);
    table_name(schema, name)
}

pub(super) fn source_alias(source: &Source, table_alias: &str) -> String {
    match source {
        Source::Table(_) => table_alias.to_string(),
        Source::Cte(cte) => cte.data.name.to_string(),
        Source::Subquery(subquery) => subquery.data.name.to_string(),
    }
}

pub(super) fn source_table(source: &Source) -> Result<&Table, QueryError> {
    match source {
        Source::Table(table) => Ok(table),
        Source::Cte(cte) => Err(QueryError::BindError(format!(
            "CTE '{}' cannot be used as a mutation target",
            cte.data.name
        ))),
        Source::Subquery(subquery) => Err(QueryError::BindError(format!(
            "subquery '{}' cannot be used as a mutation target",
            subquery.data.name
        ))),
    }
}

pub(super) fn validate_source_shape<T>(source: &Source) -> Result<(), QueryError>
where
    T: Record,
{
    match source {
        Source::Table(table) => {
            validate_table_parts(table, T::record_table_schema(), T::record_table_name())
        }
        Source::Cte(cte) => validate_output_columns::<T>(&cte.data.columns),
        Source::Subquery(subquery) => validate_output_columns::<T>(&subquery.data.columns),
    }
}

pub(super) fn validate_output_columns<T>(available: &[String]) -> Result<(), QueryError>
where
    T: Record,
{
    for column in output_columns(&T::record_column_names())? {
        if !available.iter().any(|available| available == &column) {
            return Err(QueryError::InvalidProjection(column));
        }
    }
    Ok(())
}

pub(super) fn output_columns(columns: &[String]) -> Result<Vec<String>, QueryError> {
    let mut output = Vec::with_capacity(columns.len());
    for column in columns {
        output.push(output_column(column)?);
    }
    Ok(output)
}

pub(super) fn output_column(column: &str) -> Result<String, QueryError> {
    if let Some((_, name)) = column.split_once('.') {
        validate_identifier(name)?;
        return Ok(name.to_string());
    }
    validate_identifier(column)?;
    Ok(column.to_string())
}

pub(super) fn validate_table_source(table: &Table, source: &Source) -> Result<(), QueryError> {
    match source {
        Source::Table(source) => validate_table_identity(table, source),
        _ => Err(QueryError::BindError(
            "table column does not belong to query source".to_string(),
        )),
    }
}

pub(super) fn validate_source_identity(name: &str, source: &Source) -> Result<(), QueryError> {
    if source_key(source).2 == name {
        return Ok(());
    }
    Err(QueryError::BindError(format!(
        "source column belongs to '{}', not '{}'",
        name,
        source_label(source)
    )))
}

pub(super) fn validate_table_identity(left: &Table, right: &Table) -> Result<(), QueryError> {
    if left != right {
        return Err(QueryError::BindError(format!(
            "column belongs to '{}', not '{}'",
            table_name(left.data.schema.as_deref(), left.data.name.as_ref()),
            table_name(right.data.schema.as_deref(), right.data.name.as_ref())
        )));
    }
    Ok(())
}

pub(super) fn is_model_root(reference: &str, model: &SelectModel) -> bool {
    reference == model.root_alias || reference == model.scan_root_alias
}

pub(super) fn is_logical_root(reference: &str, source: &Source, root_alias: Option<&str>) -> bool {
    if let Some(root_alias) = root_alias
        && reference == root_alias
    {
        return true;
    }
    reference == source_alias(source, &singular_alias(source_key(source).2))
}

pub(super) fn validate_source_column(column: &SourceColumnRef) -> Result<(), QueryError> {
    validate_identifier(&column.name)?;
    if let Some(owner) = &column.owner {
        validate_picked_source(&column.source, owner)?;
    }
    match &column.source {
        Source::Cte(cte) => validate_named_output(&cte.data.columns, &column.name),
        Source::Subquery(subquery) => validate_named_output(&subquery.data.columns, &column.name),
        Source::Table(_) => Err(QueryError::BindError(
            "table source is not valid inside IN subquery".to_string(),
        )),
    }
}

pub(super) fn validate_picked_source(expected: &Source, actual: &Source) -> Result<(), QueryError> {
    if expected == actual {
        return Ok(());
    }
    Err(QueryError::BindError(format!(
        "picked column belongs to '{}', not '{}'",
        source_label(actual),
        source_label(expected)
    )))
}

pub(super) fn validate_named_output(columns: &[String], name: &str) -> Result<(), QueryError> {
    if columns.iter().any(|column| column == name) {
        return Ok(());
    }
    Err(QueryError::InvalidProjection(name.to_string()))
}

pub(super) fn validate_expr_owners(
    node: &ExprNode,
    source: &Source,
    references: Option<&HashSet<&str>>,
    root_alias: Option<&str>,
    allow_references: bool,
) -> Result<(), QueryError> {
    match node {
        ExprNode::Column(column) => match &column.owner {
            ColumnOwner::Root(table) => validate_table_source(table, source),
            ColumnOwner::Source(name) => validate_source_identity(name, source),
            ColumnOwner::Reference(reference) if allow_references => {
                let Some(references) = references else {
                    return Err(QueryError::UnknownAlias(reference.to_string()));
                };
                if is_logical_root(reference.as_ref(), source, root_alias)
                    || references.contains(reference.as_ref())
                {
                    Ok(())
                } else {
                    Err(QueryError::UnknownAlias(reference.to_string()))
                }
            }
            ColumnOwner::Reference(reference) => Err(QueryError::BindError(format!(
                "mutation filters do not support reference column '{}'",
                reference
            ))),
        },
        ExprNode::Value(_) => Ok(()),
        ExprNode::Binary { left, right, .. } | ExprNode::Bool { left, right, .. } => {
            validate_expr_owners(left, source, references, root_alias, allow_references)?;
            validate_expr_owners(right, source, references, root_alias, allow_references)
        }
        ExprNode::Unary { expr, .. } => {
            validate_expr_owners(expr, source, references, root_alias, allow_references)
        }
        ExprNode::Function { args, .. } | ExprNode::Custom { args, .. } => {
            for arg in args {
                validate_expr_owners(arg, source, references, root_alias, allow_references)?;
            }
            Ok(())
        }
        ExprNode::InSource { left, source: rhs } => {
            validate_expr_owners(left, source, references, root_alias, allow_references)?;
            validate_source_column(rhs)
        }
    }
}

pub(super) fn validate_conflict_columns(
    columns: &[ColumnRef],
    source: &Table,
) -> Result<(), QueryError> {
    for column in columns {
        validate_identifier(&column.name)?;
        match &column.owner {
            ColumnOwner::Root(table) => validate_table_identity(&table, source)?,
            ColumnOwner::Source(source) => {
                return Err(QueryError::BindError(format!(
                    "upsert conflict column cannot use source '{}'",
                    source
                )));
            }
            ColumnOwner::Reference(reference) => {
                return Err(QueryError::BindError(format!(
                    "upsert conflict column cannot use reference '{}'",
                    reference
                )));
            }
        }
    }
    Ok(())
}

pub(super) fn validate_returning_supported(dialect: Dialect) -> Result<(), QueryError> {
    dialect::validate_feature(dialect, DialectFeature::Returning)
}

pub(super) fn validate_returning_projection(model: &SelectModel) -> Result<(), QueryError> {
    if let Some(reference) = model.references.keys().next() {
        return Err(QueryError::BindError(format!(
            "RETURNING does not support joined reference '{}'",
            reference
        )));
    }
    for column in &model.columns {
        output_column(column)?;
    }
    Ok(())
}

pub(super) fn validate_reference(reference: &ReferenceMeta) -> Result<(), QueryError> {
    validate_identifier(reference.logical_name)?;
    validate_identifier(reference.table_name)?;
    validate_identifier(reference.to_column)?;
    if reference.from_column.is_empty() {
        return Err(QueryError::MissingReference {
            reference: reference.logical_name,
            field: "from",
        });
    }
    Ok(())
}

pub(super) fn validate_param_compatible(
    name: &str,
    existing: &ParamSpec,
    rust_type: Option<&'static str>,
    source: ParamSource,
) -> Result<(), QueryError> {
    if existing.rust_type != rust_type || existing.source != source {
        return Err(QueryError::BindError(format!(
            "conflicting parameter '{}'",
            name
        )));
    }
    Ok(())
}

pub(super) fn validate_var_name(name: &str) -> Result<(), QueryError> {
    validate_identifier(name)?;
    if name.starts_with(GENERATED_PREFIX) {
        return Err(QueryError::InvalidIdentifier(name.to_string()));
    }
    Ok(())
}

pub(super) fn validate_identifier(value: &str) -> Result<(), QueryError> {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return Err(QueryError::InvalidIdentifier(value.to_string()));
    };
    if !(first.is_ascii_alphabetic() || first == '_')
        || !chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err(QueryError::InvalidIdentifier(value.to_string()));
    }
    Ok(())
}

pub(super) fn generated_source_name<T>(prefix: &str) -> String {
    let raw = type_name::<T>().rsplit("::").next().unwrap_or("row");
    let ident = to_identifier(raw);
    format!("{prefix}_{ident}")
}

pub(super) fn to_identifier(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            out.push(ch.to_ascii_lowercase());
        } else if !out.ends_with('_') {
            out.push('_');
        }
    }
    if out.is_empty() || out.starts_with(|ch: char| ch.is_ascii_digit()) {
        out.insert(0, '_');
    }
    out
}

pub(super) fn singular_alias(name: &str) -> String {
    name.strip_suffix('s').unwrap_or(name).to_string()
}
