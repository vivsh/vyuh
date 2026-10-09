//! Read projections, query predicates, filters, database clock, and bounded SQL helpers.

use super::store::DbTaskStore;
use chrono::{DateTime, Duration as ChronoDuration, Utc};

use crate::{
    db,
    tasks::{TaskFilter, TaskRecord, TaskRuntimeError, TaskStatus},
};

use super::model::TaskRow;

/// Internal bulk-operation ceiling; Mool additionally applies backend parameter limits.
pub(super) const WRITE_CHUNK_SIZE: usize = 10_000;

/// Keeps opportunistic housekeeping bounded independently of pipeline tuning.
pub(super) const MAINTENANCE_BATCH_SIZE: usize = 250;

/// Resolves finalized lane policy without any database access.
pub(super) fn configured_lane(
    conf: &crate::tasks::TaskStoreConf,
    lane: crate::tasks::TaskLane,
) -> Result<&crate::tasks::TaskLaneConf, TaskRuntimeError> {
    conf.lanes
        .iter()
        .find(|entry| entry.lane() == lane)
        .ok_or_else(|| TaskRuntimeError::UnknownLane(lane.to_string()))
}

/// Adds a bounded duration without allowing Chrono arithmetic to overflow.
pub(super) fn add_time(
    now: DateTime<Utc>,
    duration: ChronoDuration,
    label: &str,
) -> Result<DateTime<Utc>, TaskRuntimeError> {
    now.checked_add_signed(duration).ok_or_else(|| {
        TaskRuntimeError::TaskExecutionError(format!("{label} is outside the supported range"))
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
            pattern,
        ))
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
use super::model::{TaskClaimRow, TaskProbeRow};
use crate::db::Record as _;
/// A metadata row is deliberately distinct from a task, including on an empty result.
#[allow(clippy::large_enum_variant)] // Keep row decoding allocation-free beyond the owned fields.
pub(super) enum StoreRow {
    Clock(StoreClockRow),
    Snapshot(TaskRow),
    Candidate(TaskClaimRow),
    Probe(TaskProbeRow),
    Deadline(LaneDeadlineRow),
}

/// A deadline is query evidence, not a task snapshot or retained scheduler state.
#[derive(db::Record)]
pub(super) struct LaneDeadlineRow {
    pub(super) lane_name: String,
    pub(super) deadline: Option<DateTime<Utc>>,
}

impl<'r> db::sqlx::FromRow<'r, db::Row> for StoreRow {
    fn from_row(row: &'r db::Row) -> Result<Self, db::sqlx::Error> {
        use db::sqlx::Row as _;
        match row.try_get::<i32, _>("turn_kind")? {
            0 => Ok(Self::Clock(StoreClockRow::record_scan_ordered(
                row, &mut 1,
            )?)),
            1 => Ok(Self::Snapshot(TaskRow::record_scan_ordered(row, &mut 2)?)),
            3 => Ok(Self::Candidate(TaskClaimRow::record_scan_ordered(
                row, &mut 2,
            )?)),
            4 => Ok(Self::Deadline(LaneDeadlineRow::record_scan_unordered(row)?)),
            5 => Ok(Self::Probe(TaskProbeRow::record_scan_unordered(row)?)),
            _ => Err(db::sqlx::Error::Protocol(
                "invalid task turn row discriminator".into(),
            )),
        }
    }
}

impl DbTaskStore {
    /// Returns one deterministic page using the existing count/read query pair.
    pub(super) async fn list_tasks_impl(
        &self,
        filter: TaskFilter,
    ) -> Result<crate::routes::Page<TaskRecord>, TaskRuntimeError> {
        let table = Self::table();
        let mut pool = self.pool.clone();
        let page = apply_filter(db::from(&table), &table, &filter)
            .sort(table.created_at.desc())
            .sort(table.id.desc())
            .page::<TaskRow, _>(
                db::Pagination {
                    page_num: filter.page,
                    page_size: filter.per_page,
                },
                &mut pool,
            )
            .await?;
        Ok(crate::routes::Page::new(
            Self::into_records(page.items)?,
            page.total,
            page.page,
            page.per_page,
        ))
    }
}

/// Pads metadata in the mixed projection without loading or decoding task payloads.
pub(super) fn probe_columns(columns: &[String]) -> String {
    let probe_names = TaskProbeRow::record_column_names();
    columns
        .iter()
        .map(|name| {
            if probe_names.contains(name) {
                format!("probes.{name}")
            } else {
                "NULL".into()
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Seeks the earliest future ownerless task using the readiness index.
pub(super) fn deadline(lane: &str) -> String {
    format!("(SELECT ready_at FROM vyuh_tasks WHERE lane_name = {lane}
        AND status IN (0, 1) AND node_id IS NULL AND ready_at > clock.now ORDER BY ready_at LIMIT 1)")
}

/// Numbers shared parameters on the two backends that support the optimized claim read.
pub(super) fn parameter(index: usize) -> String {
    if cfg!(feature = "postgres") {
        format!("${index}")
    } else {
        format!("?{index}")
    }
}

/// Combines future work and token readiness without polling a blocked lane early.
pub(super) fn effective_lane_wake(
    rate_blocked: bool,
    rate_wake: Option<std::time::Duration>,
    task_wake: Option<std::time::Duration>,
    has_flows: bool,
) -> Option<std::time::Duration> {
    if has_flows {
        return rate_wake.into_iter().chain(task_wake).min();
    }
    if rate_blocked {
        return rate_wake;
    }
    task_wake.map(|task| rate_wake.map_or(task, |permit| permit.max(task)))
}

/// Operation-local clock evidence; no persisted runtime-policy row is consulted.
#[derive(db::Record)]
pub(super) struct StoreClockRow {
    pub(super) now: chrono::DateTime<Utc>,
}

/// Missing clock evidence indicates a malformed internal query projection.
pub(super) fn missing_clock() -> TaskRuntimeError {
    TaskRuntimeError::TaskExecutionError("task query omitted database clock".into())
}

/// Reads database time without locking or inspecting deployment metadata.
pub(super) async fn statement_now(
    transaction: &mut db::DbTransaction<'_>,
) -> Result<chrono::DateTime<Utc>, TaskRuntimeError> {
    use db::DbSession as _;
    Ok(transaction
        .fetch_scalar(db::Statement::raw("SELECT CURRENT_TIMESTAMP"))
        .await?)
}

impl DbTaskStore {
    /// Selects ready pending or abandoned running work without an execution owner.
    pub(super) fn due_predicate(
        table: &db::queries::ModelTable<TaskRow>,
        now: DateTime<Utc>,
    ) -> db::queries::Predicate {
        table
            .status
            .eq(db::val(TaskStatus::Pending.as_i16()))
            .or(table.status.eq(db::val(TaskStatus::Running.as_i16())))
            .and(table.node_id.is_null())
            .and(
                table
                    .ready_at
                    .is_null()
                    .or(table.ready_at.lte(db::val(Some(now)))),
            )
    }

    /// Converts selected database rows without exposing persistence types.
    pub(super) fn into_records(rows: Vec<TaskRow>) -> Result<Vec<TaskRecord>, TaskRuntimeError> {
        rows.into_iter().map(TaskRecord::try_from).collect()
    }
}
