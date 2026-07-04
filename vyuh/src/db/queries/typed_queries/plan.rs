//! Rendered SQL and parameter metadata produced from a typed query.
use indexmap::IndexMap;
use std::collections::HashMap;
use std::fmt;

use crate::db::argvalue::ArgValue;

/// Dialect-specific SQL and parameter metadata produced from a typed query AST.
#[derive(Clone)]
pub struct QueryPlan {
    /// SQL rendered for the selected dialect.
    pub sql: String,
    /// Named `var(...)` and generated `val(...)` parameters in encounter order.
    pub params: IndexMap<String, ParamSpec>,
    /// Rust type name expected by a row-scanning terminal, when known.
    pub result_type: Option<&'static str>,
    /// Number of row payload values already bound before dynamic query params.
    pub prebound_count: usize,
    /// Number of `var(...)` and `val(...)` placeholders bound after row values.
    pub dynamic_bind_count: usize,
    /// Total number of placeholders in the rendered statement.
    pub total_bind_count: usize,
    pub(super) values: HashMap<String, ArgValue>,
    pub(super) bind_order: Vec<String>,
}

impl fmt::Debug for QueryPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryPlan")
            .field("sql", &self.sql)
            .field("params", &self.params)
            .field("result_type", &self.result_type)
            .field("prebound_count", &self.prebound_count)
            .field("dynamic_bind_count", &self.dynamic_bind_count)
            .field("total_bind_count", &self.total_bind_count)
            .finish_non_exhaustive()
    }
}

/// Metadata for one logical planned SQL parameter.
#[derive(Debug, Clone)]
pub struct ParamSpec {
    /// Stable parameter name.
    pub name: String,
    /// One-based first placeholder position in the rendered SQL.
    pub position: usize,
    /// Every one-based placeholder position for this logical parameter.
    pub occurrences: Vec<usize>,
    /// Rust type name inferred from the expression context.
    pub rust_type: Option<&'static str>,
    /// Optional SQL type hint reserved for future code generation.
    pub sql_type: Option<String>,
    /// Whether this parameter came from `val(...)` or `var(...)`.
    pub source: ParamSource,
}

/// Describes whether a parameter came from `val(...)` or `var(...)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParamSource {
    Val,
    Var,
}
