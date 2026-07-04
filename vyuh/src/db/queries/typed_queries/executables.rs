//! Executable typed-query terminals.
use std::marker::PhantomData;

use crate::db::commons::{Arguments, Row};
use crate::db::executor::{DBSession, DbError};
use crate::db::interfaces::Record;
use crate::db::placeholders::Dialect;

use super::super::QueryError;
use super::binds::statement_from_plan;
use super::expr::ExprNode;
use super::plan::QueryPlan;
use super::scope::{QueryScope, ReturningScope};
use super::source::{Cte, Subquery};
use super::traits::Projectable;

/// Fetch-all executable produced by `all::<T>()`.
pub struct All<T> {
    pub(super) scope: QueryScope,
    pub(super) _marker: PhantomData<fn() -> T>,
}

/// Fetch-one executable produced by `one::<T>()`.
pub struct One<T> {
    pub(super) scope: QueryScope,
    pub(super) _marker: PhantomData<fn() -> T>,
}

/// Fetch-optional executable produced by `first::<T>()`.
pub struct First<T> {
    pub(super) scope: QueryScope,
    pub(super) _marker: PhantomData<fn() -> T>,
}

/// Limited fetch executable produced by `slice::<T>(...)`.
pub struct Slice<T> {
    pub(super) scope: QueryScope,
    pub(super) offset: usize,
    pub(super) count: usize,
    pub(super) _marker: PhantomData<fn() -> T>,
}

/// `COUNT(*)` executable.
pub struct Count {
    pub(super) scope: QueryScope,
}

/// `EXISTS(...)` executable.
pub struct Exists {
    pub(super) scope: QueryScope,
}

/// Scalar select executable.
pub struct Scalar<V> {
    pub(super) scope: QueryScope,
    pub(super) expr: ExprNode,
    pub(super) _marker: PhantomData<fn() -> V>,
}

/// Single-row insert executable.
pub struct Insert<'a, T> {
    pub(super) scope: QueryScope,
    pub(super) row: &'a T,
}

/// Single-row update executable.
pub struct Update<'a, T> {
    pub(super) scope: QueryScope,
    pub(super) row: &'a T,
}

/// Delete executable.
pub struct Delete {
    pub(super) scope: QueryScope,
}

/// Multi-row insert executable.
pub struct BatchInsert<'a, T> {
    pub(super) scope: QueryScope,
    pub(super) rows: &'a [T],
}

/// Multi-row upsert executable.
pub struct BatchUpsert<'a, T> {
    pub(super) scope: QueryScope,
    pub(super) rows: &'a [T],
    pub(super) conflict: Vec<super::expr::ColumnRef>,
}

/// Owned single-row insert executable.
pub struct OwnedInsert<T> {
    scope: QueryScope,
    row: T,
}

/// Owned single-row update executable.
pub struct OwnedUpdate<T> {
    scope: QueryScope,
    row: T,
}

/// Owned multi-row insert executable.
pub struct OwnedBatchInsert<T> {
    scope: QueryScope,
    rows: Vec<T>,
}

/// Owned multi-row upsert executable.
pub struct OwnedBatchUpsert<T> {
    scope: QueryScope,
    rows: Vec<T>,
    conflict: Vec<super::expr::ColumnRef>,
}

/// Returning insert executable.
pub struct ReturningInsert<'a, R, T> {
    pub(super) returning: ReturningScope<R>,
    pub(super) row: &'a T,
}

/// Returning update executable.
pub struct ReturningUpdate<'a, R, T> {
    pub(super) returning: ReturningScope<R>,
    pub(super) row: &'a T,
}

/// Returning delete executable.
pub struct ReturningDelete<R> {
    pub(super) returning: ReturningScope<R>,
}

/// Returning multi-row insert executable.
pub struct ReturningBatchInsert<'a, R, T> {
    pub(super) returning: ReturningScope<R>,
    pub(super) rows: &'a [T],
}

/// Returning multi-row upsert executable.
pub struct ReturningBatchUpsert<'a, R, T> {
    pub(super) returning: ReturningScope<R>,
    pub(super) rows: &'a [T],
    pub(super) conflict: Vec<super::expr::ColumnRef>,
}

impl<T> All<T>
where
    T: Record,
{
    /// Renders SQL and parameter metadata without executing the query.
    pub fn plan(&self, dialect: Dialect) -> Result<QueryPlan, QueryError> {
        self.scope.plan_all::<T>(dialect)
    }

    /// Converts this select executable into a CTE source.
    pub fn cte(self) -> Result<Cte<T>, QueryError>
    where
        T: Projectable,
    {
        self.scope.cte::<T>()
    }

    /// Converts this select executable into a named CTE source.
    #[doc(hidden)]
    pub fn cte_as(self, name: &str) -> Result<Cte<T>, QueryError>
    where
        T: Projectable,
    {
        self.scope.cte_as::<T>(name)
    }

    /// Converts this select executable into a subquery source.
    pub fn subquery(self) -> Result<Subquery<T>, QueryError>
    where
        T: Projectable,
    {
        self.scope.subquery::<T>()
    }

    /// Converts this select executable into a named subquery source.
    #[doc(hidden)]
    pub fn subquery_as(self, name: &str) -> Result<Subquery<T>, QueryError>
    where
        T: Projectable,
    {
        self.scope.subquery_as::<T>(name)
    }

    /// Executes this query against a database session.
    pub async fn exec<S>(self, session: &mut S) -> Result<Vec<T>, DbError>
    where
        T: for<'r> sqlx::FromRow<'r, Row> + Send + Unpin + 'static,
        S: DBSession,
    {
        let stmt = statement_from_plan(self.plan(Dialect::active())?, Arguments::default())?;
        session.fetch_all(stmt).await
    }
}

impl<T> One<T>
where
    T: Record,
{
    /// Renders SQL and parameter metadata without executing the query.
    pub fn plan(&self, dialect: Dialect) -> Result<QueryPlan, QueryError> {
        self.scope.plan_first::<T>(dialect)
    }

    /// Executes this query and requires exactly one row.
    pub async fn exec<S>(self, session: &mut S) -> Result<T, DbError>
    where
        T: for<'r> sqlx::FromRow<'r, Row> + Send + Unpin + 'static,
        S: DBSession,
    {
        let stmt = statement_from_plan(self.plan(Dialect::active())?, Arguments::default())?;
        session.fetch_one(stmt).await
    }
}

impl<T> First<T>
where
    T: Record,
{
    /// Renders SQL and parameter metadata without executing the query.
    pub fn plan(&self, dialect: Dialect) -> Result<QueryPlan, QueryError> {
        self.scope.plan_first::<T>(dialect)
    }

    /// Executes this query and returns the first row, if any.
    pub async fn exec<S>(self, session: &mut S) -> Result<Option<T>, DbError>
    where
        T: for<'r> sqlx::FromRow<'r, Row> + Send + Unpin + 'static,
        S: DBSession,
    {
        let stmt = statement_from_plan(self.plan(Dialect::active())?, Arguments::default())?;
        session.fetch_optional(stmt).await
    }
}

impl<T> Slice<T>
where
    T: Record,
{
    /// Renders SQL and parameter metadata without executing the query.
    pub fn plan(&self, dialect: Dialect) -> Result<QueryPlan, QueryError> {
        self.scope.plan_slice::<T>(self.offset, self.count, dialect)
    }

    /// Executes this limited query against a database session.
    pub async fn exec<S>(self, session: &mut S) -> Result<Vec<T>, DbError>
    where
        T: for<'r> sqlx::FromRow<'r, Row> + Send + Unpin + 'static,
        S: DBSession,
    {
        let stmt = statement_from_plan(self.plan(Dialect::active())?, Arguments::default())?;
        session.fetch_all(stmt).await
    }
}

impl Count {
    /// Renders SQL and parameter metadata without executing the query.
    pub fn plan(&self, dialect: Dialect) -> Result<QueryPlan, QueryError> {
        self.scope.plan_count(dialect)
    }

    /// Executes this count query.
    pub async fn exec<S>(self, session: &mut S) -> Result<i64, DbError>
    where
        S: DBSession,
    {
        let stmt = statement_from_plan(self.plan(Dialect::active())?, Arguments::default())?;
        session.fetch_scalar(stmt).await
    }
}

impl Exists {
    /// Renders SQL and parameter metadata without executing the query.
    pub fn plan(&self, dialect: Dialect) -> Result<QueryPlan, QueryError> {
        self.scope.plan_exists(dialect)
    }

    /// Executes this exists query.
    pub async fn exec<S>(self, session: &mut S) -> Result<bool, DbError>
    where
        S: DBSession,
    {
        let stmt = statement_from_plan(self.plan(Dialect::active())?, Arguments::default())?;
        session.fetch_scalar(stmt).await
    }
}

impl<V> Scalar<V> {
    /// Renders SQL and parameter metadata without executing the query.
    pub fn plan(&self, dialect: Dialect) -> Result<QueryPlan, QueryError> {
        self.scope.plan_scalar(self.expr.clone(), dialect)
    }

    /// Executes this scalar query.
    pub async fn exec<S>(self, session: &mut S) -> Result<V, DbError>
    where
        V: for<'d> sqlx::Decode<'d, crate::db::commons::Database>
            + sqlx::Type<crate::db::commons::Database>
            + Send
            + Unpin
            + 'static,
        S: DBSession,
    {
        let stmt = statement_from_plan(self.plan(Dialect::active())?, Arguments::default())?;
        session.fetch_scalar(stmt).await
    }
}

impl<T> Insert<'_, T>
where
    T: Record,
{
    /// Renders SQL and parameter metadata without executing the insert.
    pub fn plan(&self, dialect: Dialect) -> Result<QueryPlan, QueryError> {
        self.scope.plan_insert(self.row, dialect)
    }

    /// Copies the payload into an owned executable.
    pub fn into_owned(self) -> OwnedInsert<T>
    where
        T: Clone,
    {
        OwnedInsert {
            scope: self.scope,
            row: self.row.clone(),
        }
    }

    /// Executes this insert.
    pub async fn exec<S>(self, session: &mut S) -> Result<u64, DbError>
    where
        S: DBSession,
    {
        let (plan, args) = self
            .scope
            .plan_insert_with_args(self.row, Dialect::active())?;
        session.execute(statement_from_plan(plan, args)?).await
    }
}

impl<T> Update<'_, T>
where
    T: Record,
{
    /// Renders SQL and parameter metadata without executing the update.
    pub fn plan(&self, dialect: Dialect) -> Result<QueryPlan, QueryError> {
        self.scope.plan_update(self.row, dialect)
    }

    /// Copies the payload into an owned executable.
    pub fn into_owned(self) -> OwnedUpdate<T>
    where
        T: Clone,
    {
        OwnedUpdate {
            scope: self.scope,
            row: self.row.clone(),
        }
    }

    /// Executes this update.
    pub async fn exec<S>(self, session: &mut S) -> Result<u64, DbError>
    where
        S: DBSession,
    {
        let (plan, args) = self
            .scope
            .plan_update_with_args(self.row, Dialect::active())?;
        session.execute(statement_from_plan(plan, args)?).await
    }
}

impl Delete {
    /// Renders SQL and parameter metadata without executing the delete.
    pub fn plan(&self, dialect: Dialect) -> Result<QueryPlan, QueryError> {
        self.scope.plan_delete(dialect)
    }

    /// Owned conversion for API symmetry.
    pub fn into_owned(self) -> Self {
        self
    }

    /// Executes this delete.
    pub async fn exec<S>(self, session: &mut S) -> Result<u64, DbError>
    where
        S: DBSession,
    {
        let stmt = statement_from_plan(self.plan(Dialect::active())?, Arguments::default())?;
        session.execute(stmt).await
    }
}

impl<T> BatchInsert<'_, T>
where
    T: Record,
{
    /// Renders SQL and parameter metadata without executing the insert.
    pub fn plan(&self, dialect: Dialect) -> Result<QueryPlan, QueryError> {
        self.scope.plan_batch_insert(self.rows, dialect)
    }

    /// Copies all rows into an owned executable.
    pub fn into_owned(self) -> OwnedBatchInsert<T>
    where
        T: Clone,
    {
        OwnedBatchInsert {
            scope: self.scope,
            rows: self.rows.to_vec(),
        }
    }

    /// Executes this batch insert.
    pub async fn exec<S>(self, session: &mut S) -> Result<u64, DbError>
    where
        S: DBSession,
    {
        let (plan, args) = self
            .scope
            .plan_batch_insert_with_args(self.rows, Dialect::active())?;
        session.execute(statement_from_plan(plan, args)?).await
    }
}

impl<T> BatchUpsert<'_, T>
where
    T: Record,
{
    /// Renders SQL and parameter metadata without executing the upsert.
    pub fn plan(&self, dialect: Dialect) -> Result<QueryPlan, QueryError> {
        self.scope
            .plan_batch_upsert(self.rows, self.conflict.clone(), dialect)
    }

    /// Copies all rows into an owned executable.
    pub fn into_owned(self) -> OwnedBatchUpsert<T>
    where
        T: Clone,
    {
        OwnedBatchUpsert {
            scope: self.scope,
            rows: self.rows.to_vec(),
            conflict: self.conflict,
        }
    }

    /// Executes this batch upsert.
    pub async fn exec<S>(self, session: &mut S) -> Result<u64, DbError>
    where
        S: DBSession,
    {
        let (plan, args) =
            self.scope
                .plan_batch_upsert_with_args(self.rows, self.conflict, Dialect::active())?;
        session.execute(statement_from_plan(plan, args)?).await
    }
}

impl<T> OwnedInsert<T>
where
    T: Record,
{
    /// Renders SQL and parameter metadata without executing the insert.
    pub fn plan(&self, dialect: Dialect) -> Result<QueryPlan, QueryError> {
        self.scope.plan_insert(&self.row, dialect)
    }

    /// Executes this owned insert.
    pub async fn exec<S>(self, session: &mut S) -> Result<u64, DbError>
    where
        S: DBSession,
    {
        Insert {
            scope: self.scope,
            row: &self.row,
        }
        .exec(session)
        .await
    }
}

impl<T> OwnedUpdate<T>
where
    T: Record,
{
    /// Renders SQL and parameter metadata without executing the update.
    pub fn plan(&self, dialect: Dialect) -> Result<QueryPlan, QueryError> {
        self.scope.plan_update(&self.row, dialect)
    }

    /// Executes this owned update.
    pub async fn exec<S>(self, session: &mut S) -> Result<u64, DbError>
    where
        S: DBSession,
    {
        Update {
            scope: self.scope,
            row: &self.row,
        }
        .exec(session)
        .await
    }
}

impl<T> OwnedBatchInsert<T>
where
    T: Record,
{
    /// Renders SQL and parameter metadata without executing the insert.
    pub fn plan(&self, dialect: Dialect) -> Result<QueryPlan, QueryError> {
        self.scope.plan_batch_insert(&self.rows, dialect)
    }

    /// Executes this owned batch insert.
    pub async fn exec<S>(self, session: &mut S) -> Result<u64, DbError>
    where
        S: DBSession,
    {
        BatchInsert {
            scope: self.scope,
            rows: &self.rows,
        }
        .exec(session)
        .await
    }
}

impl<T> OwnedBatchUpsert<T>
where
    T: Record,
{
    /// Renders SQL and parameter metadata without executing the upsert.
    pub fn plan(&self, dialect: Dialect) -> Result<QueryPlan, QueryError> {
        self.scope
            .plan_batch_upsert(&self.rows, self.conflict.clone(), dialect)
    }

    /// Executes this owned batch upsert.
    pub async fn exec<S>(self, session: &mut S) -> Result<u64, DbError>
    where
        S: DBSession,
    {
        BatchUpsert {
            scope: self.scope,
            rows: &self.rows,
            conflict: self.conflict,
        }
        .exec(session)
        .await
    }
}

impl<R, T> ReturningInsert<'_, R, T>
where
    R: Record,
    T: Record,
{
    /// Renders SQL and parameter metadata without executing the insert.
    pub fn plan(&self, dialect: Dialect) -> Result<QueryPlan, QueryError> {
        self.returning
            .plan_insert(self.row, dialect)
            .map(|(plan, _)| plan)
    }

    /// Executes this returning insert.
    pub async fn exec<S>(self, session: &mut S) -> Result<R, DbError>
    where
        R: for<'r> sqlx::FromRow<'r, Row> + Send + Unpin + 'static,
        S: DBSession,
    {
        let (plan, args) = self.returning.plan_insert(self.row, Dialect::active())?;
        session.fetch_one(statement_from_plan(plan, args)?).await
    }
}

impl<R, T> ReturningUpdate<'_, R, T>
where
    R: Record,
    T: Record,
{
    /// Renders SQL and parameter metadata without executing the update.
    pub fn plan(&self, dialect: Dialect) -> Result<QueryPlan, QueryError> {
        self.returning
            .plan_update(self.row, dialect)
            .map(|(plan, _)| plan)
    }

    /// Executes this returning update.
    pub async fn exec<S>(self, session: &mut S) -> Result<Vec<R>, DbError>
    where
        R: for<'r> sqlx::FromRow<'r, Row> + Send + Unpin + 'static,
        S: DBSession,
    {
        let (plan, args) = self.returning.plan_update(self.row, Dialect::active())?;
        session.fetch_all(statement_from_plan(plan, args)?).await
    }
}

impl<R> ReturningDelete<R>
where
    R: Record,
{
    /// Renders SQL and parameter metadata without executing the delete.
    pub fn plan(&self, dialect: Dialect) -> Result<QueryPlan, QueryError> {
        self.returning.plan_delete(dialect)
    }

    /// Executes this returning delete.
    pub async fn exec<S>(self, session: &mut S) -> Result<Vec<R>, DbError>
    where
        R: for<'r> sqlx::FromRow<'r, Row> + Send + Unpin + 'static,
        S: DBSession,
    {
        let plan = self.returning.plan_delete(Dialect::active())?;
        session
            .fetch_all(statement_from_plan(plan, Arguments::default())?)
            .await
    }
}

impl<R, T> ReturningBatchInsert<'_, R, T>
where
    R: Record,
    T: Record,
{
    /// Renders SQL and parameter metadata without executing the insert.
    pub fn plan(&self, dialect: Dialect) -> Result<QueryPlan, QueryError> {
        self.returning
            .plan_batch_insert(self.rows, dialect)
            .map(|(plan, _)| plan)
    }

    /// Executes this returning batch insert.
    pub async fn exec<S>(self, session: &mut S) -> Result<Vec<R>, DbError>
    where
        R: for<'r> sqlx::FromRow<'r, Row> + Send + Unpin + 'static,
        S: DBSession,
    {
        let (plan, args) = self
            .returning
            .plan_batch_insert(self.rows, Dialect::active())?;
        session.fetch_all(statement_from_plan(plan, args)?).await
    }
}

impl<R, T> ReturningBatchUpsert<'_, R, T>
where
    R: Record,
    T: Record,
{
    /// Renders SQL and parameter metadata without executing the upsert.
    pub fn plan(&self, dialect: Dialect) -> Result<QueryPlan, QueryError> {
        self.returning
            .plan_batch_upsert(self.rows, self.conflict.clone(), dialect)
            .map(|(plan, _)| plan)
    }

    /// Executes this returning batch upsert.
    pub async fn exec<S>(self, session: &mut S) -> Result<Vec<R>, DbError>
    where
        R: for<'r> sqlx::FromRow<'r, Row> + Send + Unpin + 'static,
        S: DBSession,
    {
        let (plan, args) =
            self.returning
                .plan_batch_upsert(self.rows, self.conflict, Dialect::active())?;
        session.fetch_all(statement_from_plan(plan, args)?).await
    }
}
