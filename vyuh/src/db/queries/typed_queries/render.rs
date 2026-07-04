//! Dialect-aware SQL rendering, planning, and validation engine.
use indexmap::IndexMap;
use std::any::type_name;
use std::collections::HashMap;

use crate::db::argvalue::ArgValue;
use crate::db::interfaces::{JoinType, Record, ReferenceMeta};
use crate::db::placeholders::Dialect;

use super::super::QueryError;
use super::binds::upsert_update_columns;
use super::dialect::{self, DialectFeature, DialectRenderer};
use super::expr::{ColumnRef, ExprNode, ValueNode};
use super::extension::ExprRenderCtx;
use super::handles::{ColumnOwner, Table};
use super::plan::{ParamSource, ParamSpec, QueryPlan};
use super::scope::QueryScope;
use super::source::{CteSource, Source, SourceColumnRef};
use super::validate::{
    is_model_root, output_column, singular_alias, source_alias, source_key, source_table,
    validate_identifier, validate_param_compatible, validate_reference, validate_source_column,
    validate_source_identity, validate_source_shape, validate_table_source, validate_var_name,
};

const GENERATED_PREFIX: &str = "__typed_";

#[derive(Clone)]
pub(super) struct SelectModel {
    pub(super) source: Source,
    pub(super) root_alias: String,
    pub(super) scan_root_alias: String,
    pub(super) references: IndexMap<String, ReferenceMeta>,
    pub(super) columns: Vec<String>,
    pub(super) result_type: &'static str,
}

#[derive(Clone, Copy)]
pub(super) enum RenderMode<'a> {
    Select(&'a SelectModel),
    MutationRoot { source: &'a Source },
}

impl SelectModel {
    pub(super) fn new<T>(source: &Source) -> Result<Self, QueryError>
    where
        T: Record,
    {
        validate_source_shape::<T>(source)?;
        let schema = T::record_schema();
        let scan_root_alias = schema
            .root_name
            .map(str::to_string)
            .unwrap_or_else(|| singular_alias(schema.table_name));
        validate_identifier(&scan_root_alias)?;
        let root_alias = source_alias(source, &scan_root_alias);
        validate_identifier(&root_alias)?;
        let references = schema
            .references
            .into_iter()
            .map(|reference| {
                validate_reference(&reference)?;
                Ok((reference.logical_name.to_string(), reference))
            })
            .collect::<Result<IndexMap<_, _>, QueryError>>()?;
        Ok(Self {
            source: source.clone(),
            root_alias,
            scan_root_alias,
            references,
            columns: schema.column_names,
            result_type: type_name::<T>(),
        })
    }

    /// Builds a projection-free model over a source for aggregate terminals
    /// (`count`, `scalar`, `exists`). Only root-table columns are addressable;
    /// joined references require a row-shaped `Record` projection instead.
    pub(super) fn source_only(source: &Source) -> Result<Self, QueryError> {
        let scan_root_alias = singular_alias(source_key(source).2);
        validate_identifier(&scan_root_alias)?;
        let root_alias = source_alias(source, &scan_root_alias);
        validate_identifier(&root_alias)?;
        Ok(Self {
            source: source.clone(),
            root_alias,
            scan_root_alias,
            references: IndexMap::new(),
            columns: Vec::new(),
            result_type: "",
        })
    }
}

pub(super) struct Renderer {
    dialect: Dialect,
    dialect_renderer: &'static dyn DialectRenderer,
    params: IndexMap<String, PlannedParam>,
    bind_order: Vec<String>,
    value_counter: usize,
    prebound_count: usize,
}

struct PlannedParam {
    spec: ParamSpec,
    value: Option<ArgValue>,
}

impl Renderer {
    pub(super) fn new(dialect: Dialect) -> Self {
        Self {
            dialect,
            dialect_renderer: dialect::renderer(dialect),
            params: IndexMap::new(),
            bind_order: Vec::new(),
            value_counter: 0,
            prebound_count: 0,
        }
    }

    pub(super) fn with_prebound(dialect: Dialect, prebound_count: usize) -> Self {
        Self {
            prebound_count,
            ..Self::new(dialect)
        }
    }

    pub(super) fn plan(
        self,
        sql: String,
        result_type: Option<&'static str>,
        mut values: HashMap<String, ArgValue>,
    ) -> QueryPlan {
        let mut params = IndexMap::new();
        for (key, planned) in self.params {
            if let Some(value) = planned.value {
                values.insert(key.clone(), value);
            }
            params.insert(key, planned.spec);
        }
        let dynamic_bind_count = self.bind_order.len();
        let total_bind_count = self.prebound_count + dynamic_bind_count;
        QueryPlan {
            sql,
            params,
            result_type,
            prebound_count: self.prebound_count,
            dynamic_bind_count,
            total_bind_count,
            values,
            bind_order: self.bind_order,
        }
    }

    pub(super) fn render_select(
        &mut self,
        scope: &QueryScope,
        model: &SelectModel,
        slice: Option<(usize, usize)>,
    ) -> Result<String, QueryError> {
        let mut sql = String::new();
        self.render_with(scope, &mut sql)?;
        sql.push_str("SELECT ");
        self.render_projection(scope, model, &mut sql)?;
        self.render_from(model, &mut sql)?;
        self.render_filters(scope, RenderMode::Select(model), &mut sql)?;
        self.render_groups(scope, RenderMode::Select(model), &mut sql)?;
        self.render_having(scope, RenderMode::Select(model), &mut sql)?;
        self.render_orders(scope, RenderMode::Select(model), &mut sql)?;
        if let Some((offset, count)) = slice {
            sql.push_str(" LIMIT ");
            sql.push_str(&count.to_string());
            sql.push_str(" OFFSET ");
            sql.push_str(&offset.to_string());
        }
        Ok(sql)
    }

    pub(super) fn render_insert(
        &mut self,
        scope: &QueryScope,
        columns: &[String],
        rows: usize,
        upsert: bool,
        conflict: &[ColumnRef],
        returning: Option<&SelectModel>,
    ) -> Result<String, QueryError> {
        if rows == 0 {
            return Err(QueryError::BindError(
                "cannot insert empty list".to_string(),
            ));
        }
        let mut sql = String::new();
        self.render_with(scope, &mut sql)?;
        sql.push_str("INSERT INTO ");
        let table = source_table(&scope.source)?;
        sql.push_str(&self.render_table_name(table)?);
        sql.push_str(" (");
        sql.push_str(&columns.join(", "));
        sql.push_str(") VALUES ");
        self.render_values_grid(rows, columns.len(), &mut sql);
        if upsert {
            self.render_upsert(columns, conflict, &mut sql)?;
        }
        self.render_returning(returning, &mut sql)?;
        Ok(sql)
    }

    pub(super) fn render_update(
        &mut self,
        scope: &QueryScope,
        columns: &[String],
        returning: Option<&SelectModel>,
    ) -> Result<String, QueryError> {
        let mut sql = String::new();
        self.render_with(scope, &mut sql)?;
        sql.push_str("UPDATE ");
        let table = source_table(&scope.source)?;
        sql.push_str(&self.render_table_name(table)?);
        sql.push_str(" SET ");
        for (idx, column) in columns.iter().enumerate() {
            if idx > 0 {
                sql.push_str(", ");
            }
            validate_identifier(column)?;
            sql.push_str(column);
            sql.push_str(" = ");
            sql.push_str(&self.placeholder(idx + 1));
        }
        self.render_filters(
            scope,
            RenderMode::MutationRoot {
                source: &scope.source,
            },
            &mut sql,
        )?;
        self.render_returning(returning, &mut sql)?;
        Ok(sql)
    }

    pub(super) fn render_delete(
        &mut self,
        scope: &QueryScope,
        returning: Option<&SelectModel>,
    ) -> Result<String, QueryError> {
        let mut sql = String::new();
        self.render_with(scope, &mut sql)?;
        sql.push_str("DELETE FROM ");
        let table = source_table(&scope.source)?;
        sql.push_str(&self.render_table_name(table)?);
        self.render_filters(
            scope,
            RenderMode::MutationRoot {
                source: &scope.source,
            },
            &mut sql,
        )?;
        self.render_returning(returning, &mut sql)?;
        Ok(sql)
    }

    /// Renders `SELECT COUNT(*) FROM ... [WHERE ...] [GROUP BY ...] [HAVING ...]`.
    pub(super) fn render_count(
        &mut self,
        scope: &QueryScope,
        model: &SelectModel,
    ) -> Result<String, QueryError> {
        let mut sql = String::new();
        self.render_with(scope, &mut sql)?;
        sql.push_str("SELECT COUNT(*)");
        self.render_from(model, &mut sql)?;
        self.render_filters(scope, RenderMode::Select(model), &mut sql)?;
        self.render_groups(scope, RenderMode::Select(model), &mut sql)?;
        self.render_having(scope, RenderMode::Select(model), &mut sql)?;
        Ok(sql)
    }

    /// Renders `SELECT EXISTS(SELECT 1 FROM ... [WHERE ...])`.
    pub(super) fn render_exists(
        &mut self,
        scope: &QueryScope,
        model: &SelectModel,
    ) -> Result<String, QueryError> {
        let mut sql = String::new();
        self.render_with(scope, &mut sql)?;
        sql.push_str("SELECT EXISTS(SELECT 1");
        self.render_from(model, &mut sql)?;
        self.render_filters(scope, RenderMode::Select(model), &mut sql)?;
        sql.push(')');
        Ok(sql)
    }

    /// Renders `SELECT <expr> FROM ... [WHERE ...] [GROUP BY ...] [HAVING ...]`.
    pub(super) fn render_scalar(
        &mut self,
        scope: &QueryScope,
        model: &SelectModel,
        expr: &ExprNode,
    ) -> Result<String, QueryError> {
        let mut sql = String::new();
        self.render_with(scope, &mut sql)?;
        sql.push_str("SELECT ");
        let rendered = self.render_expr(expr, RenderMode::Select(model))?;
        sql.push_str(&rendered);
        self.render_from(model, &mut sql)?;
        self.render_filters(scope, RenderMode::Select(model), &mut sql)?;
        self.render_groups(scope, RenderMode::Select(model), &mut sql)?;
        self.render_having(scope, RenderMode::Select(model), &mut sql)?;
        Ok(sql)
    }

    fn render_projection(
        &mut self,
        scope: &QueryScope,
        model: &SelectModel,
        sql: &mut String,
    ) -> Result<(), QueryError> {
        if model.columns.is_empty() {
            return Err(QueryError::InvalidProjection(
                "typed projection has no selected columns".to_string(),
            ));
        }
        for (idx, column) in model.columns.iter().enumerate() {
            if idx > 0 {
                sql.push_str(", ");
            }
            if let Some(expr) = scope.select_exprs.get(column) {
                sql.push_str(&self.render_expr(expr, RenderMode::Select(model))?);
                sql.push_str(" AS ");
                sql.push_str(column);
            } else if column == "*" {
                sql.push('*');
            } else {
                sql.push_str(&self.resolve_model_column(column, model)?);
            }
        }
        Ok(())
    }

    fn render_returning(
        &self,
        model: Option<&SelectModel>,
        sql: &mut String,
    ) -> Result<(), QueryError> {
        let Some(model) = model else {
            return Ok(());
        };
        let columns = model
            .columns
            .iter()
            .map(|column| output_column(column))
            .collect::<Result<Vec<_>, _>>()?;
        sql.push_str(&self.dialect_renderer.render_returning(&columns)?);
        Ok(())
    }

    fn render_with(&mut self, scope: &QueryScope, sql: &mut String) -> Result<(), QueryError> {
        if scope.ctes.is_empty() {
            return Ok(());
        }
        if scope.ctes.iter().any(|cte| cte.data.recursive) {
            self.dialect_renderer
                .validate_feature(DialectFeature::RecursiveCte)?;
            sql.push_str("WITH RECURSIVE ");
        } else {
            sql.push_str("WITH ");
        }
        for (idx, cte) in scope.ctes.iter().enumerate() {
            if idx > 0 {
                sql.push_str(", ");
            }
            self.render_cte(cte, sql)?;
        }
        sql.push(' ');
        Ok(())
    }

    fn render_cte(&mut self, cte: &CteSource, sql: &mut String) -> Result<(), QueryError> {
        validate_identifier(&cte.data.name)?;
        sql.push_str(&cte.data.name);
        sql.push_str(" AS (");
        sql.push_str(&self.render_select(&cte.data.scope, &cte.data.model, None)?);
        sql.push(')');
        Ok(())
    }

    fn render_from(&mut self, model: &SelectModel, sql: &mut String) -> Result<(), QueryError> {
        sql.push_str(" FROM ");
        self.render_source(&model.source, &model.root_alias, sql)?;
        for reference in model.references.values() {
            let join = match reference.join_type {
                JoinType::Inner => " JOIN ",
                JoinType::Left => " LEFT JOIN ",
            };
            sql.push_str(join);
            if let Some(schema) = reference.table_schema {
                validate_identifier(schema)?;
                sql.push_str(schema);
                sql.push('.');
            }
            sql.push_str(reference.table_name);
            sql.push(' ');
            sql.push_str(reference.logical_name);
            sql.push_str(" ON ");
            sql.push_str(reference.logical_name);
            sql.push('.');
            sql.push_str(reference.to_column);
            sql.push_str(" = ");
            sql.push_str(&self.resolve_reference_from(reference, model)?);
        }
        Ok(())
    }

    fn render_source(
        &mut self,
        source: &Source,
        alias: &str,
        sql: &mut String,
    ) -> Result<(), QueryError> {
        match source {
            Source::Table(table) => {
                sql.push_str(&self.render_table_name(table)?);
                sql.push(' ');
                sql.push_str(alias);
            }
            Source::Cte(cte) => sql.push_str(&cte.data.name),
            Source::Subquery(subquery) => {
                sql.push('(');
                sql.push_str(&self.render_select(
                    &subquery.data.scope,
                    &subquery.data.model,
                    None,
                )?);
                sql.push_str(") ");
                sql.push_str(alias);
            }
        }
        Ok(())
    }

    fn render_filters(
        &mut self,
        scope: &QueryScope,
        mode: RenderMode<'_>,
        sql: &mut String,
    ) -> Result<(), QueryError> {
        if scope.filters.is_empty() {
            return Ok(());
        }
        sql.push_str(" WHERE ");
        for (idx, filter) in scope.filters.iter().enumerate() {
            if idx > 0 {
                sql.push_str(" AND ");
            }
            sql.push_str(&self.render_expr(&filter.node, mode)?);
        }
        Ok(())
    }

    fn render_groups(
        &mut self,
        scope: &QueryScope,
        mode: RenderMode<'_>,
        sql: &mut String,
    ) -> Result<(), QueryError> {
        if scope.groups.is_empty() {
            return Ok(());
        }
        sql.push_str(" GROUP BY ");
        for (idx, group) in scope.groups.iter().enumerate() {
            if idx > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&self.render_expr(group, mode)?);
        }
        Ok(())
    }

    fn render_having(
        &mut self,
        scope: &QueryScope,
        mode: RenderMode<'_>,
        sql: &mut String,
    ) -> Result<(), QueryError> {
        if scope.having.is_empty() {
            return Ok(());
        }
        sql.push_str(" HAVING ");
        for (idx, predicate) in scope.having.iter().enumerate() {
            if idx > 0 {
                sql.push_str(" AND ");
            }
            sql.push_str(&self.render_expr(&predicate.node, mode)?);
        }
        Ok(())
    }

    fn render_orders(
        &mut self,
        scope: &QueryScope,
        mode: RenderMode<'_>,
        sql: &mut String,
    ) -> Result<(), QueryError> {
        if scope.orders.is_empty() {
            return Ok(());
        }
        sql.push_str(" ORDER BY ");
        for (idx, order) in scope.orders.iter().enumerate() {
            if idx > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&self.render_expr(&order.expr, mode)?);
            if order.desc {
                sql.push_str(" DESC");
            } else {
                sql.push_str(" ASC");
            }
        }
        Ok(())
    }

    fn render_values_grid(&self, rows: usize, cols: usize, sql: &mut String) {
        for row in 0..rows {
            if row > 0 {
                sql.push_str(", ");
            }
            sql.push('(');
            for col in 0..cols {
                if col > 0 {
                    sql.push_str(", ");
                }
                let position = row * cols + col + 1;
                sql.push_str(&self.placeholder(position));
            }
            sql.push(')');
        }
    }

    fn render_upsert(
        &self,
        columns: &[String],
        conflict: &[ColumnRef],
        sql: &mut String,
    ) -> Result<(), QueryError> {
        let update_columns = upsert_update_columns(columns, conflict)?;
        self.dialect_renderer
            .validate_feature(DialectFeature::Upsert)?;
        sql.push_str(
            &self
                .dialect_renderer
                .render_upsert(conflict, &update_columns)?,
        );
        Ok(())
    }

    fn render_expr(&mut self, node: &ExprNode, mode: RenderMode<'_>) -> Result<String, QueryError> {
        match node {
            ExprNode::Column(column) => self.render_column(column, mode),
            ExprNode::Value(value) => self.render_value(value),
            ExprNode::Binary { left, op, right } => {
                self.validate_operator(op)?;
                Ok(format!(
                    "({} {} {})",
                    self.render_expr(left, mode)?,
                    op,
                    self.render_expr(right, mode)?
                ))
            }
            ExprNode::Unary { op, expr } => Ok(format!("{op} ({})", self.render_expr(expr, mode)?)),
            ExprNode::Bool { left, op, right } => Ok(format!(
                "({} {} {})",
                self.render_expr(left, mode)?,
                op,
                self.render_expr(right, mode)?
            )),
            ExprNode::Function { function, args } => {
                function.validate(self.dialect, args.len())?;
                let name = function.name(self.dialect)?;
                let rendered = args
                    .iter()
                    .map(|arg| self.render_expr(arg, mode))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(format!("{name}({})", rendered.join(", ")))
            }
            ExprNode::Custom { expression, args } => {
                expression.validate(self.dialect)?;
                let rendered = args
                    .iter()
                    .map(|arg| self.render_expr(arg, mode))
                    .collect::<Result<Vec<_>, _>>()?;
                let mut ctx = ExprRenderCtx::new(self.dialect, &rendered);
                expression.render(&mut ctx)
            }
            ExprNode::InSource { left, source } => Ok(format!(
                "{} IN ({})",
                self.render_expr(left, mode)?,
                self.render_source_column_query(source)?
            )),
        }
    }

    fn render_column(
        &self,
        column: &ColumnRef,
        mode: RenderMode<'_>,
    ) -> Result<String, QueryError> {
        validate_identifier(&column.name)?;
        match (mode, &column.owner) {
            (RenderMode::Select(model), ColumnOwner::Root(table)) => {
                validate_table_source(table, &model.source)?;
                Ok(format!("{}.{}", model.root_alias, column.name))
            }
            (RenderMode::Select(model), ColumnOwner::Source(source)) => {
                validate_source_identity(source, &model.source)?;
                Ok(format!("{}.{}", model.root_alias, column.name))
            }
            (RenderMode::Select(model), ColumnOwner::Reference(reference)) => {
                if is_model_root(reference, model) {
                    return Ok(format!("{}.{}", model.root_alias, column.name));
                }
                if !model.references.contains_key(reference.as_ref()) {
                    return Err(QueryError::UnknownAlias(reference.to_string()));
                }
                Ok(format!("{reference}.{}", column.name))
            }
            (RenderMode::MutationRoot { source }, ColumnOwner::Root(table)) => {
                validate_table_source(table, source)?;
                Ok(column.name.to_string())
            }
            (RenderMode::MutationRoot { .. }, ColumnOwner::Source(source)) => {
                Err(QueryError::BindError(format!(
                    "mutation filters do not support source column '{}'",
                    source
                )))
            }
            (RenderMode::MutationRoot { .. }, ColumnOwner::Reference(reference)) => {
                Err(QueryError::BindError(format!(
                    "mutation filters do not support reference column '{}'",
                    reference
                )))
            }
        }
    }

    fn render_source_column_query(
        &mut self,
        column: &SourceColumnRef,
    ) -> Result<String, QueryError> {
        validate_source_column(column)?;
        match &column.source {
            Source::Cte(cte) => Ok(format!("SELECT {} FROM {}", column.name, cte.data.name)),
            Source::Subquery(subquery) => Ok(format!(
                "SELECT {}.{} FROM ({}) {}",
                subquery.data.name,
                column.name,
                self.render_select(&subquery.data.scope, &subquery.data.model, None)?,
                subquery.data.name
            )),
            Source::Table(_) => Err(QueryError::BindError(
                "table columns cannot be used as subquery sources".to_string(),
            )),
        }
    }

    fn render_value(&mut self, value: &ValueNode) -> Result<String, QueryError> {
        match value {
            ValueNode::Val {
                name,
                rust_type,
                value,
            } => {
                let final_name = name.clone().unwrap_or_else(|| self.next_value_name());
                let position = self.push_param(
                    final_name,
                    Some(*rust_type),
                    ParamSource::Val,
                    Some(value.clone()),
                )?;
                Ok(self.placeholder(position))
            }
            ValueNode::Var { name, rust_type } => {
                validate_var_name(name)?;
                let position = self.push_param(name.clone(), *rust_type, ParamSource::Var, None)?;
                Ok(self.placeholder(position))
            }
        }
    }

    fn resolve_model_column(
        &self,
        column: &str,
        model: &SelectModel,
    ) -> Result<String, QueryError> {
        if let Some((owner, name)) = column.split_once('.') {
            validate_identifier(owner)?;
            validate_identifier(name)?;
            if owner == model.root_alias || owner == model.scan_root_alias {
                return Ok(format!("{}.{}", model.root_alias, name));
            }
            if model.references.contains_key(owner) {
                return Ok(format!("{owner}.{name}"));
            }
            return Err(QueryError::UnknownAlias(owner.to_string()));
        }
        validate_identifier(column)?;
        Ok(format!("{}.{}", model.root_alias, column))
    }

    fn resolve_reference_from(
        &self,
        reference: &ReferenceMeta,
        model: &SelectModel,
    ) -> Result<String, QueryError> {
        self.resolve_model_column(reference.from_column, model)
    }

    fn render_table_name(&self, table: &Table) -> Result<String, QueryError> {
        validate_identifier(&table.data.name)?;
        if let Some(schema) = &table.data.schema {
            validate_identifier(schema)?;
            return Ok(format!("{schema}.{}", table.data.name));
        }
        Ok(table.data.name.to_string())
    }

    fn validate_operator(&self, op: &str) -> Result<(), QueryError> {
        if op == "ILIKE" {
            self.dialect_renderer
                .validate_feature(DialectFeature::Ilike)?;
        }
        Ok(())
    }

    fn placeholder(&self, position: usize) -> String {
        self.dialect_renderer.placeholder(position)
    }

    fn push_param(
        &mut self,
        name: String,
        rust_type: Option<&'static str>,
        source: ParamSource,
        value: Option<ArgValue>,
    ) -> Result<usize, QueryError> {
        match self.dialect {
            Dialect::Postgres => self.push_postgres_param(name, rust_type, source, value),
            Dialect::Mysql | Dialect::Sqlite => {
                self.push_positional_param(name, rust_type, source, value)
            }
        }
    }

    fn push_postgres_param(
        &mut self,
        name: String,
        rust_type: Option<&'static str>,
        source: ParamSource,
        value: Option<ArgValue>,
    ) -> Result<usize, QueryError> {
        if let Some(existing) = self.params.get_mut(&name) {
            validate_param_compatible(&name, &existing.spec, rust_type, source)?;
            existing.spec.occurrences.push(existing.spec.position);
            return Ok(existing.spec.position);
        }
        let position = self.prebound_count + self.params.len() + 1;
        self.bind_order.push(name.clone());
        self.params.insert(
            name.clone(),
            PlannedParam {
                spec: ParamSpec {
                    name,
                    position,
                    occurrences: vec![position],
                    rust_type,
                    sql_type: None,
                    source,
                },
                value,
            },
        );
        Ok(position)
    }

    fn push_positional_param(
        &mut self,
        name: String,
        rust_type: Option<&'static str>,
        source: ParamSource,
        value: Option<ArgValue>,
    ) -> Result<usize, QueryError> {
        let position = self.prebound_count + self.bind_order.len() + 1;
        self.bind_order.push(name.clone());
        if let Some(existing) = self.params.get_mut(&name) {
            validate_param_compatible(&name, &existing.spec, rust_type, source)?;
            existing.spec.occurrences.push(position);
            return Ok(position);
        }
        self.params.insert(
            name.clone(),
            PlannedParam {
                spec: ParamSpec {
                    name,
                    position,
                    occurrences: vec![position],
                    rust_type,
                    sql_type: None,
                    source,
                },
                value,
            },
        );
        Ok(position)
    }

    fn next_value_name(&mut self) -> String {
        self.value_counter += 1;
        format!("{GENERATED_PREFIX}{}", self.value_counter)
    }
}
