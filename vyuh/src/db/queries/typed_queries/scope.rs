//! Query scope composition and planning helpers.
use indexmap::IndexMap;
use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::Arc;

use crate::db::argvalue::ArgValue;
use crate::db::commons::Arguments;
use crate::db::interfaces::{Record, ReferenceMeta};
use crate::db::placeholders::Dialect;

use super::super::QueryError;
use super::binds::{
    bind_columns, bind_row, bind_rows, collect_expr_binds, collect_expr_ctes, collect_source_ctes,
    finish_plan, insert_bind, validate_bind_columns, validate_cte_usage, validate_select_exprs,
};
use super::expr::{ExprNode, IntoExpr, OrderExpr, Predicate};
use super::plan::QueryPlan;
use super::render::{Renderer, SelectModel};
use super::source::{
    Cte, CteData, CteSource, SelectSource, Source, Subquery, SubqueryData, SubquerySource,
};
use super::traits::{IntoColumnRef, Projectable};
use super::validate::{
    generated_source_name, output_columns, source_table, validate_conflict_columns,
    validate_expr_owners, validate_identifier, validate_returning_projection,
    validate_returning_supported, validate_var_name,
};

/// Composable query scope rooted at one table.
#[derive(Clone)]
pub struct QueryScope {
    pub(super) source: Source,
    pub(super) ctes: Vec<CteSource>,
    pub(super) filters: Vec<Predicate>,
    pub(super) groups: Vec<ExprNode>,
    pub(super) having: Vec<Predicate>,
    pub(super) orders: Vec<OrderExpr>,
    pub(super) select_exprs: IndexMap<String, ExprNode>,
    pub(super) binds: HashMap<String, ArgValue>,
    pub(super) errors: Vec<QueryError>,
}

/// Write scope that returns rows through a `RETURNING` projection.
pub struct ReturningScope<R> {
    pub(super) scope: QueryScope,
    pub(super) _marker: PhantomData<fn() -> R>,
}

impl QueryScope {
    pub(super) fn new(source: Source) -> Self {
        Self {
            source,
            ctes: Vec::new(),
            filters: Vec::new(),
            groups: Vec::new(),
            having: Vec::new(),
            orders: Vec::new(),
            select_exprs: IndexMap::new(),
            binds: HashMap::new(),
            errors: Vec::new(),
        }
    }

    /// Adds a WHERE predicate.
    pub fn filter(mut self, predicate: Predicate) -> Self {
        self.filters.push(predicate);
        self
    }

    /// Adds a GROUP BY expression.
    pub fn group_by<T>(mut self, expr: impl IntoExpr<T>) -> Self {
        self.groups.push(expr.into_expr().node);
        self
    }

    /// Adds a HAVING predicate.
    pub fn having(mut self, predicate: Predicate) -> Self {
        self.having.push(predicate);
        self
    }

    /// Adds an ORDER BY expression.
    pub fn order_by(mut self, expr: OrderExpr) -> Self {
        self.orders.push(expr);
        self
    }

    /// Overrides or adds a SELECT expression by scan field name.
    pub fn select_expr<T>(mut self, name: &str, expr: impl IntoExpr<T>) -> Self {
        self.select_exprs
            .insert(name.to_string(), expr.into_expr().node);
        self
    }

    /// Binds a runtime value for a `var(...)`.
    pub fn bind<T>(mut self, name: &str, value: T) -> Self
    where
        T: Clone
            + for<'q> sqlx::Encode<'q, crate::db::commons::Database>
            + sqlx::Type<crate::db::commons::Database>
            + Send
            + Sync
            + 'static,
    {
        if let Err(error) = validate_var_name(name) {
            self.errors.push(error);
            return self;
        }
        if self.binds.contains_key(name) {
            self.errors.push(QueryError::BindError(format!(
                "duplicate binding for '{}'",
                name
            )));
        } else {
            self.binds.insert(name.to_string(), ArgValue::new(value));
        }
        self
    }

    /// Adds a CTE definition to this query scope.
    pub fn with<T>(mut self, cte: &Cte<T>) -> Self
    where
        T: Projectable,
    {
        self.ctes.push(cte.as_source());
        self
    }

    /// Adds a recursive CTE definition to this query scope.
    #[doc(hidden)]
    pub fn with_recursive<T>(mut self, cte: &Cte<T>) -> Self
    where
        T: Projectable,
    {
        let mut source = cte.as_source();
        source = source.recursive();
        self.ctes.push(source);
        self
    }

    pub(super) fn cte<T>(self) -> Result<Cte<T>, QueryError>
    where
        T: Record + Projectable,
    {
        self.cte_as::<T>(&generated_source_name::<T>("cte"))
    }

    pub(super) fn cte_as<T>(self, name: &str) -> Result<Cte<T>, QueryError>
    where
        T: Record + Projectable,
    {
        let source = self.select_source::<T>(name)?;
        let data = Arc::new(CteData {
            name: Arc::from(name),
            scope: self,
            model: source.model,
            columns: source.columns,
            recursive: false,
        });
        let cte_source = CteSource { data: data.clone() };
        let columns = T::projected_columns(super::source::ProjectionSource::new(Source::Cte(
            cte_source,
        )));
        Ok(Cte {
            data,
            columns,
            _marker: PhantomData,
        })
    }

    pub(super) fn subquery<T>(self) -> Result<Subquery<T>, QueryError>
    where
        T: Record + Projectable,
    {
        self.subquery_as::<T>(&generated_source_name::<T>("subquery"))
    }

    pub(super) fn subquery_as<T>(self, name: &str) -> Result<Subquery<T>, QueryError>
    where
        T: Record + Projectable,
    {
        let source = self.select_source::<T>(name)?;
        let data = Arc::new(SubqueryData {
            name: Arc::from(name),
            scope: self,
            model: source.model,
            columns: source.columns,
        });
        let subquery_source = SubquerySource { data: data.clone() };
        let columns = T::projected_columns(super::source::ProjectionSource::new(Source::Subquery(
            subquery_source,
        )));
        Ok(Subquery {
            data,
            columns,
            _marker: PhantomData,
        })
    }

    /// Uses a record projection as the `RETURNING` shape for write terminals.
    pub fn returning<R>(self) -> ReturningScope<R>
    where
        R: Record,
    {
        ReturningScope {
            scope: self,
            _marker: PhantomData,
        }
    }

    pub(super) fn plan_all<T>(&self, dialect: Dialect) -> Result<QueryPlan, QueryError>
    where
        T: Record,
    {
        self.plan_select::<T>(None, dialect)
    }

    pub(super) fn plan_first<T>(&self, dialect: Dialect) -> Result<QueryPlan, QueryError>
    where
        T: Record,
    {
        self.plan_select::<T>(Some((0, 1)), dialect)
    }

    pub(super) fn plan_slice<T>(
        &self,
        offset: usize,
        count: usize,
        dialect: Dialect,
    ) -> Result<QueryPlan, QueryError>
    where
        T: Record,
    {
        self.plan_select::<T>(Some((offset, count)), dialect)
    }

    pub(super) fn plan_insert<T>(&self, row: &T, dialect: Dialect) -> Result<QueryPlan, QueryError>
    where
        T: Record,
    {
        self.plan_insert_with_args(row, dialect)
            .map(|(plan, _)| plan)
    }

    pub(super) fn plan_update<T>(&self, row: &T, dialect: Dialect) -> Result<QueryPlan, QueryError>
    where
        T: Record,
    {
        self.plan_update_with_args(row, dialect)
            .map(|(plan, _)| plan)
    }

    pub(super) fn plan_delete(&self, dialect: Dialect) -> Result<QueryPlan, QueryError> {
        self.validate_scope_errors()?;
        self.validate_mutation_filters()?;
        let mut renderer = Renderer::new(dialect);
        let sql = renderer.render_delete(self, None)?;
        finish_plan(renderer.plan(sql, None, self.collect_binds()?))
    }

    pub(super) fn plan_count(&self, dialect: Dialect) -> Result<QueryPlan, QueryError> {
        self.validate_scope_errors()?;
        let model = SelectModel::source_only(&self.source)?;
        self.validate_aggregate(&model, None)?;
        let mut renderer = Renderer::new(dialect);
        let sql = renderer.render_count(self, &model)?;
        finish_plan(renderer.plan(sql, None, self.collect_binds()?))
    }

    pub(super) fn plan_exists(&self, dialect: Dialect) -> Result<QueryPlan, QueryError> {
        self.validate_scope_errors()?;
        let model = SelectModel::source_only(&self.source)?;
        self.validate_aggregate(&model, None)?;
        let mut renderer = Renderer::new(dialect);
        let sql = renderer.render_exists(self, &model)?;
        finish_plan(renderer.plan(sql, None, self.collect_binds()?))
    }

    pub(super) fn plan_scalar(
        &self,
        expr: ExprNode,
        dialect: Dialect,
    ) -> Result<QueryPlan, QueryError> {
        self.validate_scope_errors()?;
        let model = SelectModel::source_only(&self.source)?;
        self.validate_aggregate(&model, Some(&expr))?;
        let mut renderer = Renderer::new(dialect);
        let sql = renderer.render_scalar(self, &model, &expr)?;
        finish_plan(renderer.plan(sql, None, self.collect_binds()?))
    }

    pub(super) fn plan_batch_insert<T>(
        &self,
        rows: &[T],
        dialect: Dialect,
    ) -> Result<QueryPlan, QueryError>
    where
        T: Record,
    {
        self.plan_batch_insert_with_args(rows, dialect)
            .map(|(plan, _)| plan)
    }

    pub(super) fn plan_batch_upsert<T, I, C>(
        &self,
        rows: &[T],
        conflict: I,
        dialect: Dialect,
    ) -> Result<QueryPlan, QueryError>
    where
        T: Record,
        I: IntoIterator<Item = C>,
        C: IntoColumnRef,
    {
        self.plan_batch_upsert_with_args(rows, conflict, dialect)
            .map(|(plan, _)| plan)
    }

    fn plan_select<T>(
        &self,
        slice: Option<(usize, usize)>,
        dialect: Dialect,
    ) -> Result<QueryPlan, QueryError>
    where
        T: Record,
    {
        self.validate_scope_errors()?;
        let model = SelectModel::new::<T>(&self.source)?;
        self.validate_columns_for_select(&model)?;
        let mut renderer = Renderer::new(dialect);
        let sql = renderer.render_select(self, &model, slice)?;
        finish_plan(renderer.plan(sql, Some(model.result_type), self.collect_binds()?))
    }

    fn select_source<T>(&self, name: &str) -> Result<SelectSource, QueryError>
    where
        T: Record,
    {
        validate_identifier(name)?;
        self.validate_scope_errors()?;
        let model = SelectModel::new::<T>(&self.source)?;
        self.validate_columns_for_select(&model)?;
        Ok(SelectSource {
            columns: output_columns(&model.columns)?,
            model,
        })
    }

    pub(super) fn plan_insert_with_args<T>(
        &self,
        row: &T,
        dialect: Dialect,
    ) -> Result<(QueryPlan, Arguments<'static>), QueryError>
    where
        T: Record,
    {
        self.validate_scope_errors()?;
        self.validate_insert_scope::<T>()?;
        let columns = bind_columns::<T>()?;
        validate_bind_columns(source_table(&self.source)?, &columns)?;
        let mut args = Arguments::default();
        bind_row(row, columns.len(), &mut args)?;
        let mut renderer = Renderer::with_prebound(dialect, columns.len());
        let sql = renderer.render_insert(self, &columns, 1, false, &[], None)?;
        Ok((
            finish_plan(renderer.plan(sql, None, self.collect_binds()?))?,
            args,
        ))
    }

    pub(super) fn plan_update_with_args<T>(
        &self,
        row: &T,
        dialect: Dialect,
    ) -> Result<(QueryPlan, Arguments<'static>), QueryError>
    where
        T: Record,
    {
        self.validate_scope_errors()?;
        self.validate_update_scope::<T>()?;
        let columns = bind_columns::<T>()?;
        validate_bind_columns(source_table(&self.source)?, &columns)?;
        let mut args = Arguments::default();
        bind_row(row, columns.len(), &mut args)?;
        let mut renderer = Renderer::with_prebound(dialect, columns.len());
        let sql = renderer.render_update(self, &columns, None)?;
        Ok((
            finish_plan(renderer.plan(sql, None, self.collect_binds()?))?,
            args,
        ))
    }

    pub(super) fn plan_batch_insert_with_args<T>(
        &self,
        rows: &[T],
        dialect: Dialect,
    ) -> Result<(QueryPlan, Arguments<'static>), QueryError>
    where
        T: Record,
    {
        self.validate_scope_errors()?;
        self.validate_insert_scope::<T>()?;
        let columns = bind_columns::<T>()?;
        validate_bind_columns(source_table(&self.source)?, &columns)?;
        let args = bind_rows(rows, columns.len())?;
        let mut renderer = Renderer::with_prebound(dialect, rows.len() * columns.len());
        let sql = renderer.render_insert(self, &columns, rows.len(), false, &[], None)?;
        Ok((
            finish_plan(renderer.plan(sql, None, self.collect_binds()?))?,
            args,
        ))
    }

    pub(super) fn plan_batch_upsert_with_args<T, I, C>(
        &self,
        rows: &[T],
        conflict: I,
        dialect: Dialect,
    ) -> Result<(QueryPlan, Arguments<'static>), QueryError>
    where
        T: Record,
        I: IntoIterator<Item = C>,
        C: IntoColumnRef,
    {
        self.validate_scope_errors()?;
        self.validate_insert_scope::<T>()?;
        let columns = bind_columns::<T>()?;
        validate_bind_columns(source_table(&self.source)?, &columns)?;
        let conflict = conflict
            .into_iter()
            .map(IntoColumnRef::into_column_ref)
            .collect::<Vec<_>>();
        if conflict.is_empty() {
            return Err(QueryError::BindError(
                "batch_upsert requires conflict columns".to_string(),
            ));
        }
        validate_conflict_columns(&conflict, source_table(&self.source)?)?;
        let args = bind_rows(rows, columns.len())?;
        let mut renderer = Renderer::with_prebound(dialect, rows.len() * columns.len());
        let sql = renderer.render_insert(self, &columns, rows.len(), true, &conflict, None)?;
        Ok((
            finish_plan(renderer.plan(sql, None, self.collect_binds()?))?,
            args,
        ))
    }

    pub(super) fn plan_insert_returning<T>(
        &self,
        row: &T,
        dialect: Dialect,
        returning: &SelectModel,
    ) -> Result<(QueryPlan, Arguments<'static>), QueryError>
    where
        T: Record,
    {
        self.validate_scope_errors()?;
        self.validate_insert_scope::<T>()?;
        let columns = bind_columns::<T>()?;
        validate_bind_columns(source_table(&self.source)?, &columns)?;
        let mut args = Arguments::default();
        bind_row(row, columns.len(), &mut args)?;
        let mut renderer = Renderer::with_prebound(dialect, columns.len());
        let sql = renderer.render_insert(self, &columns, 1, false, &[], Some(returning))?;
        Ok((self.finish_returning(renderer, sql, returning)?, args))
    }

    pub(super) fn plan_update_returning<T>(
        &self,
        row: &T,
        dialect: Dialect,
        returning: &SelectModel,
    ) -> Result<(QueryPlan, Arguments<'static>), QueryError>
    where
        T: Record,
    {
        self.validate_scope_errors()?;
        self.validate_update_scope::<T>()?;
        let columns = bind_columns::<T>()?;
        validate_bind_columns(source_table(&self.source)?, &columns)?;
        let mut args = Arguments::default();
        bind_row(row, columns.len(), &mut args)?;
        let mut renderer = Renderer::with_prebound(dialect, columns.len());
        let sql = renderer.render_update(self, &columns, Some(returning))?;
        Ok((self.finish_returning(renderer, sql, returning)?, args))
    }

    pub(super) fn plan_delete_returning(
        &self,
        dialect: Dialect,
        returning: &SelectModel,
    ) -> Result<QueryPlan, QueryError> {
        self.validate_scope_errors()?;
        self.validate_mutation_filters()?;
        let mut renderer = Renderer::new(dialect);
        let sql = renderer.render_delete(self, Some(returning))?;
        self.finish_returning(renderer, sql, returning)
    }

    pub(super) fn plan_batch_insert_returning<T>(
        &self,
        rows: &[T],
        dialect: Dialect,
        returning: &SelectModel,
    ) -> Result<(QueryPlan, Arguments<'static>), QueryError>
    where
        T: Record,
    {
        self.validate_scope_errors()?;
        self.validate_insert_scope::<T>()?;
        let columns = bind_columns::<T>()?;
        validate_bind_columns(source_table(&self.source)?, &columns)?;
        let args = bind_rows(rows, columns.len())?;
        let mut renderer = Renderer::with_prebound(dialect, rows.len() * columns.len());
        let sql =
            renderer.render_insert(self, &columns, rows.len(), false, &[], Some(returning))?;
        Ok((self.finish_returning(renderer, sql, returning)?, args))
    }

    pub(super) fn plan_batch_upsert_returning<T, I, C>(
        &self,
        rows: &[T],
        conflict: I,
        dialect: Dialect,
        returning: &SelectModel,
    ) -> Result<(QueryPlan, Arguments<'static>), QueryError>
    where
        T: Record,
        I: IntoIterator<Item = C>,
        C: IntoColumnRef,
    {
        self.validate_scope_errors()?;
        self.validate_insert_scope::<T>()?;
        let columns = bind_columns::<T>()?;
        validate_bind_columns(source_table(&self.source)?, &columns)?;
        let conflict = conflict
            .into_iter()
            .map(IntoColumnRef::into_column_ref)
            .collect::<Vec<_>>();
        if conflict.is_empty() {
            return Err(QueryError::BindError(
                "batch_upsert requires conflict columns".to_string(),
            ));
        }
        validate_conflict_columns(&conflict, source_table(&self.source)?)?;
        let args = bind_rows(rows, columns.len())?;
        let mut renderer = Renderer::with_prebound(dialect, rows.len() * columns.len());
        let sql =
            renderer.render_insert(self, &columns, rows.len(), true, &conflict, Some(returning))?;
        Ok((self.finish_returning(renderer, sql, returning)?, args))
    }

    fn finish_returning(
        &self,
        renderer: Renderer,
        sql: String,
        returning: &SelectModel,
    ) -> Result<QueryPlan, QueryError> {
        finish_plan(renderer.plan(sql, Some(returning.result_type), self.collect_binds()?))
    }

    fn validate_columns_for_select(&self, model: &SelectModel) -> Result<(), QueryError> {
        validate_select_exprs(&self.select_exprs, &model.columns)?;
        self.validate_ctes(Some(&model.references))?;
        let mut refs = HashSet::new();
        for key in model.references.keys() {
            refs.insert(key.as_str());
        }
        for node in self.expression_nodes() {
            validate_expr_owners(
                node,
                &self.source,
                Some(&refs),
                Some(&model.scan_root_alias),
                true,
            )?;
        }
        Ok(())
    }

    fn validate_scope_errors(&self) -> Result<(), QueryError> {
        if let Some(error) = self.errors.first() {
            return Err(error.clone());
        }
        Ok(())
    }

    /// Validates aggregate terminals (`count`/`exists`/`scalar`) over a
    /// projection-free source model. Only root columns are addressable.
    fn validate_aggregate(
        &self,
        model: &SelectModel,
        extra: Option<&ExprNode>,
    ) -> Result<(), QueryError> {
        self.validate_ctes(Some(&model.references))?;
        let refs: HashSet<&str> = HashSet::new();
        for node in self.expression_nodes().chain(extra) {
            validate_expr_owners(
                node,
                &self.source,
                Some(&refs),
                Some(&model.scan_root_alias),
                true,
            )?;
        }
        Ok(())
    }

    fn validate_mutation_filters(&self) -> Result<(), QueryError> {
        self.validate_ctes(None)?;
        for node in self.expression_nodes() {
            validate_expr_owners(node, &self.source, None, None, false)?;
        }
        Ok(())
    }

    fn validate_ctes(
        &self,
        references: Option<&IndexMap<String, ReferenceMeta>>,
    ) -> Result<(), QueryError> {
        let defined = self.defined_ctes()?;
        let mut used = HashSet::new();
        collect_source_ctes(&self.source, &mut used);
        if let Some(references) = references {
            for reference in references.values() {
                if defined.contains(reference.table_name) {
                    used.insert(reference.table_name.to_string());
                }
            }
        }
        for node in self.expression_nodes() {
            collect_expr_ctes(node, &mut used);
        }
        validate_cte_usage(&defined, &used)
    }

    fn defined_ctes(&self) -> Result<HashSet<String>, QueryError> {
        let mut defined = HashSet::new();
        for cte in &self.ctes {
            let name = cte.data.name.to_string();
            if !defined.insert(name.clone()) {
                return Err(QueryError::BindError(format!("duplicate CTE '{}'", name)));
            }
        }
        Ok(defined)
    }

    fn expression_nodes(&self) -> impl Iterator<Item = &ExprNode> {
        self.filters
            .iter()
            .map(|predicate| &predicate.node)
            .chain(self.groups.iter())
            .chain(self.having.iter().map(|predicate| &predicate.node))
            .chain(self.orders.iter().map(|order| &order.expr))
            .chain(self.select_exprs.values())
    }

    fn collect_binds(&self) -> Result<HashMap<String, ArgValue>, QueryError> {
        let mut values = HashMap::new();
        self.collect_binds_into(&mut values)?;
        Ok(values)
    }

    pub(super) fn collect_binds_into(
        &self,
        values: &mut HashMap<String, ArgValue>,
    ) -> Result<(), QueryError> {
        for cte in &self.ctes {
            cte.data.scope.collect_binds_into(values)?;
        }
        for node in self.expression_nodes() {
            collect_expr_binds(node, values)?;
        }
        for (name, value) in &self.binds {
            insert_bind(values, name, value.clone())?;
        }
        Ok(())
    }

    fn validate_insert_scope<T>(&self) -> Result<(), QueryError>
    where
        T: Record,
    {
        if !self.filters.is_empty()
            || !self.groups.is_empty()
            || !self.having.is_empty()
            || !self.orders.is_empty()
            || !self.select_exprs.is_empty()
            || !self.ctes.is_empty()
        {
            return Err(QueryError::BindError(
                "insert does not support query-scope modifiers".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_update_scope<T>(&self) -> Result<(), QueryError>
    where
        T: Record,
    {
        if !self.groups.is_empty()
            || !self.having.is_empty()
            || !self.orders.is_empty()
            || !self.select_exprs.is_empty()
        {
            return Err(QueryError::BindError(
                "update does not support select-only modifiers".to_string(),
            ));
        }
        self.validate_mutation_filters()
    }
}

impl<R> ReturningScope<R>
where
    R: Record,
{
    fn model(&self, dialect: Dialect) -> Result<SelectModel, QueryError> {
        validate_returning_supported(dialect)?;
        let model = SelectModel::new::<R>(&self.scope.source)?;
        validate_returning_projection(&model)?;
        Ok(model)
    }

    pub(super) fn plan_insert<T>(
        &self,
        row: &T,
        dialect: Dialect,
    ) -> Result<(QueryPlan, Arguments<'static>), QueryError>
    where
        T: Record,
    {
        let model = self.model(dialect)?;
        self.scope.plan_insert_returning(row, dialect, &model)
    }

    pub(super) fn plan_update<T>(
        &self,
        row: &T,
        dialect: Dialect,
    ) -> Result<(QueryPlan, Arguments<'static>), QueryError>
    where
        T: Record,
    {
        let model = self.model(dialect)?;
        self.scope.plan_update_returning(row, dialect, &model)
    }

    pub(super) fn plan_delete(&self, dialect: Dialect) -> Result<QueryPlan, QueryError> {
        let model = self.model(dialect)?;
        self.scope.plan_delete_returning(dialect, &model)
    }

    pub(super) fn plan_batch_insert<T>(
        &self,
        rows: &[T],
        dialect: Dialect,
    ) -> Result<(QueryPlan, Arguments<'static>), QueryError>
    where
        T: Record,
    {
        let model = self.model(dialect)?;
        self.scope
            .plan_batch_insert_returning(rows, dialect, &model)
    }

    pub(super) fn plan_batch_upsert<T, I, C>(
        &self,
        rows: &[T],
        conflict: I,
        dialect: Dialect,
    ) -> Result<(QueryPlan, Arguments<'static>), QueryError>
    where
        T: Record,
        I: IntoIterator<Item = C>,
        C: IntoColumnRef,
    {
        let model = self.model(dialect)?;
        self.scope
            .plan_batch_upsert_returning(rows, conflict, dialect, &model)
    }
}
