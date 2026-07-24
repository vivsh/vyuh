//! Shared task-store state, predicates, filters, and error translation.

use chrono::{DateTime, Duration as ChronoDuration, Utc};

use crate::{
    db,
    tasks::{TaskError, TaskListFilter, TaskRecord, TaskStatus},
};

use super::model::TaskRow;

/// Mool-backed persistent task store selected by Vyuh's database feature.
#[derive(Clone)]
pub struct DbTaskStore {
    pub(super) pool: db::DbPool,
    pub(super) batch_size: usize,
    pub(super) lease_duration: std::time::Duration,
}

impl DbTaskStore {
    /// Creates a task store over the selected Mool database pool.
    pub fn new(pool: db::Pool, batch_size: usize, lease_duration: std::time::Duration) -> Self {
        Self {
            pool: db::DbPool::from_pool(pool),
            batch_size: batch_size.max(1),
            lease_duration,
        }
    }

    /// Returns the selected backend's typed task table handle.
    pub(super) fn table() -> db::queries::ModelTable<TaskRow> {
        <TaskRow as db::Model>::table()
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

    /// Restricts a mutation to the runner that currently owns a task lease.
    pub(super) fn owned_predicate(
        table: &db::queries::ModelTable<TaskRow>,
        task_id: uuid::Uuid,
        runner_id: &str,
    ) -> db::queries::Predicate {
        table
            .id
            .eq(db::val(task_id))
            .and(table.locked_by.eq(db::val(Some(runner_id.to_owned()))))
            .and(table.status.eq(db::val(TaskStatus::Running.as_i16())))
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

/// Resolves explicit retry timing before the retry compare-and-set update.
pub(super) fn retry_delay(
    requested: Option<std::time::Duration>,
    configured_ms: Option<i64>,
) -> Result<ChronoDuration, TaskError> {
    match requested {
        Some(duration) => ChronoDuration::from_std(duration).map_err(|_| {
            TaskError::TaskExecutionError(
                "task retry duration is outside the supported range".into(),
            )
        }),
        None => {
            let milliseconds = configured_ms.unwrap_or_default();
            if milliseconds < 0 {
                return Err(TaskError::TaskExecutionError(
                    "task retry duration cannot be negative".into(),
                ));
            }
            Ok(ChronoDuration::milliseconds(milliseconds))
        }
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

/// Records an ignored outcome after a lease was lost or reclaimed.
pub(super) fn log_stale_owner(rows: u64, task_id: uuid::Uuid, runner_id: &str) {
    if rows == 0 {
        tracing::warn!(
            "Task {} outcome ignored because runner {} no longer owns it",
            task_id,
            runner_id
        );
    }
}

/// Preserves the task identity conflict as a stable domain error.
pub(super) fn map_store_error(error: db::DbError) -> TaskError {
    match &error {
        db::DbError::Integrity {
            kind: db::IntegrityKind::Unique,
            constraint: Some(constraint),
            ..
        } if constraint.contains("identity") => TaskError::IdentityError,
        _ => TaskError::StoreError(error),
    }
}

/// Applies every supported task-list filter to one typed query scope.
pub(super) fn apply_filter(
    mut scope: db::queries::QueryScope,
    table: &db::queries::ModelTable<TaskRow>,
    filter: &TaskListFilter,
) -> db::queries::QueryScope {
    if let Some(status) = filter.status {
        scope = scope.filter(table.status.eq(db::val(status.as_i16())));
    }
    if let Some(name) = &filter.name {
        scope = scope.filter(table.name.eq(db::val(name.clone())));
    }
    if let Some(identity) = &filter.identity {
        scope = scope.filter(table.identity.eq(db::val(Some(identity.clone()))));
    }
    if let Some(priority) = filter.priority_min {
        scope = scope.filter(table.priority.gte(db::val(priority)));
    }
    if let Some(created_from) = filter.created_from {
        scope = scope.filter(table.created_at.gte(db::val(created_from)));
    }
    if let Some(created_to) = filter.created_to {
        scope = scope.filter(table.created_at.lte(db::val(created_to)));
    }
    if let Some(query) = &filter.q {
        scope = scope.filter(text_filter(table, query));
    }
    scope
}

/// Creates the portable case-folded text predicate for task search.
fn text_filter(table: &db::queries::ModelTable<TaskRow>, query: &str) -> db::queries::Predicate {
    let pattern = format!("%{}%", query.to_lowercase());
    case_fold_like(table.name.clone(), pattern.clone())
        .or(case_fold_like_optional(
            table.identity.clone(),
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
