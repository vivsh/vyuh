//! Shared task-store state, predicates, filters, and error translation.

use chrono::{DateTime, Duration as ChronoDuration, Utc};

use crate::{
    db,
    tasks::{TaskError, TaskFilter, TaskRecord, TaskStatus},
};

use super::model::{TaskIdempotencyRow, TaskRateRow, TaskRow, TaskRuntimeRow};

/// Mool-backed persistent task store selected by Vyuh's database feature.
#[derive(Clone)]
pub struct DbTaskStore {
    pub(super) pool: db::DbPool,
    pub(super) batch_size: usize,
    pub(super) lease_duration: std::time::Duration,
    pub(super) runtime_conf:
        std::sync::Arc<tokio::sync::RwLock<Option<crate::tasks::TaskStoreConf>>>,
}

impl DbTaskStore {
    /// Creates a task store over the selected Mool database pool.
    pub fn new(pool: db::Pool, batch_size: usize, lease_duration: std::time::Duration) -> Self {
        Self {
            pool: db::DbPool::from_pool(pool),
            batch_size: batch_size.max(1),
            lease_duration,
            runtime_conf: std::sync::Arc::new(tokio::sync::RwLock::new(None)),
        }
    }

    /// Returns the selected backend's typed task table handle.
    pub(super) fn table() -> db::queries::ModelTable<TaskRow> {
        <TaskRow as db::Model>::table()
    }

    /// Returns the typed idempotency ownership table.
    pub(super) fn idempotency_table() -> db::queries::ModelTable<TaskIdempotencyRow> {
        <TaskIdempotencyRow as db::Model>::table()
    }

    /// Returns the typed global rate-bucket table.
    pub(super) fn rate_table() -> db::queries::ModelTable<TaskRateRow> {
        <TaskRateRow as db::Model>::table()
    }

    /// Returns the singleton task-runtime policy table.
    pub(super) fn runtime_table() -> db::queries::ModelTable<TaskRuntimeRow> {
        <TaskRuntimeRow as db::Model>::table()
    }

    /// Selects pending work that is ready or running work whose lease expired.
    pub(super) fn due_predicate(
        table: &db::queries::ModelTable<TaskRow>,
        now: DateTime<Utc>,
    ) -> db::queries::Predicate {
        let pending = table.status.eq(db::val(TaskStatus::Pending.as_i16())).and(
            table
                .ready_at
                .is_null()
                .or(table.ready_at.lte(db::val(Some(now)))),
        );
        let expired = table.status.eq(db::val(TaskStatus::Running.as_i16())).and(
            table
                .leased_until
                .is_not_null()
                .and(table.leased_until.lte(db::val(Some(now)))),
        );
        pending.or(expired)
    }

    /// Calculates an absolute lease deadline while rejecting invalid durations.
    pub(super) fn lease_until(
        &self,
        row: &TaskRow,
        now: DateTime<Utc>,
    ) -> Result<DateTime<Utc>, TaskError> {
        let default_milliseconds =
            i64::try_from(self.lease_duration.as_millis()).map_err(|_| {
                TaskError::TaskExecutionError(
                    "task lease duration is outside the supported range".into(),
                )
            })?;
        let milliseconds = row.lease_duration_ms.unwrap_or(default_milliseconds);
        if milliseconds < 0 {
            return Err(TaskError::TaskExecutionError(
                "task lease duration cannot be negative".into(),
            ));
        }
        add_time(
            now,
            ChronoDuration::milliseconds(milliseconds),
            "task lease duration",
        )
    }

    /// Converts selected database rows without exposing persistence types.
    pub(super) fn into_records(rows: Vec<TaskRow>) -> Result<Vec<TaskRecord>, TaskError> {
        rows.into_iter().map(TaskRecord::try_from).collect()
    }
}

/// Adds a bounded duration without allowing Chrono arithmetic to overflow.
pub(super) fn add_time(
    now: DateTime<Utc>,
    duration: ChronoDuration,
    label: &str,
) -> Result<DateTime<Utc>, TaskError> {
    now.checked_add_signed(duration).ok_or_else(|| {
        TaskError::TaskExecutionError(format!("{label} is outside the supported range"))
    })
}

/// Applies every supported task-list filter to one typed query scope.
pub(super) fn apply_filter(
    mut scope: db::queries::QueryScope,
    table: &db::queries::ModelTable<TaskRow>,
    filter: &TaskFilter,
) -> db::queries::QueryScope {
    if let Some(status) = filter.status {
        scope = scope.filter(table.status.eq(db::val(status.as_i16())));
    }
    if let Some(name) = &filter.name {
        scope = scope.filter(table.name.eq(db::val(name.clone())));
    }
    if let Some(lane) = &filter.lane {
        scope = scope.filter(table.lane_name.eq(db::val(lane.clone())));
    }
    if let Some(key) = &filter.idempotency_key {
        scope = scope.filter(table.idempotency_key.eq(db::val(Some(key.clone()))));
    }
    if let Some(created_from) = filter.created_from {
        scope = scope.filter(table.created_at.gte(db::val(created_from)));
    }
    if let Some(created_to) = filter.created_to {
        scope = scope.filter(table.created_at.lte(db::val(created_to)));
    }
    if let Some(query) = &filter.query {
        scope = scope.filter(text_filter(table, query));
    }
    scope
}

/// Creates the portable case-folded text predicate for task search.
fn text_filter(table: &db::queries::ModelTable<TaskRow>, query: &str) -> db::queries::Predicate {
    let pattern = format!("%{}%", query.to_lowercase());
    case_fold_like(table.name.clone(), pattern.clone())
        .or(case_fold_like(table.lane_name.clone(), pattern.clone()))
        .or(case_fold_like_optional(
            table.idempotency_key.clone(),
            pattern.clone(),
        ))
        .or(case_fold_like_optional(table.last_error.clone(), pattern))
}

#[derive(Clone)]
struct CaseFoldLike {
    args: db::FunctionArgs,
}

impl db::DbExpression<bool> for CaseFoldLike {
    fn args(&self) -> db::FunctionArgs {
        self.args.clone()
    }

    fn render(&self, context: &mut db::ExprRenderCtx<'_>) -> Result<(), db::QueryError> {
        context.push_sql("LOWER(");
        context.push_arg(0)?;
        context.push_sql(") LIKE LOWER(");
        context.push_arg(1)?;
        context.push_sql(")");
        Ok(())
    }
}

/// Builds a portable case-folded text predicate for required text columns.
fn case_fold_like<T>(column: T, pattern: String) -> db::queries::Predicate
where
    T: db::queries::IntoExpr<String>,
{
    db::funcs::custom::<bool, _>(CaseFoldLike {
        args: db::FunctionArgs::new((column.into_expr(), db::val(pattern))),
    })
    .eq(db::val(true))
}

/// Builds a portable case-folded text predicate for nullable text columns.
fn case_fold_like_optional<T>(column: T, pattern: String) -> db::queries::Predicate
where
    T: db::queries::IntoExpr<Option<String>>,
{
    db::funcs::custom::<bool, _>(CaseFoldLike {
        args: db::FunctionArgs::new((column.into_expr(), db::val(Some(pattern)))),
    })
    .eq(db::val(true))
}
