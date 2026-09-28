//! Transaction-local policy and outcome reads without a payload transport envelope.

use chrono::{DateTime, Utc};

use super::{
    model::{TaskClaimRow, TaskLeaseRow, TaskRow},
    runtime::RuntimeTurnRow,
};
use crate::{
    db::{self, DbSession as _, Record as _},
    tasks::{TaskCommit, TaskLease, TaskRuntimeError},
};

/// A metadata row is deliberately distinct from a task, including on an empty result.
#[allow(clippy::large_enum_variant)] // Keep row decoding allocation-free beyond the owned fields.
pub(super) enum TurnRead {
    Runtime(RuntimeTurnRow),
    Snapshot(TaskRow),
    Renewal(TaskLeaseRow),
    Candidate(TaskClaimRow),
    Deadline(LaneDeadlineRow),
}

/// A deadline is query evidence, not a task snapshot or retained scheduler state.
#[derive(db::Record)]
pub(super) struct LaneDeadlineRow {
    pub(super) lane_name: String,
    pub(super) deadline: Option<DateTime<Utc>>,
}

/// Shares ordinary observations while keeping coordinated lanes on their established path.
pub(super) async fn observations(
    tx: &mut db::DbTransaction<'_>,
    runner: &str,
    commits: &[TaskCommit],
    leases: &[TaskLease],
    expected: &str,
    batch: usize,
) -> Result<(DateTime<Utc>, Option<Vec<TaskRow>>, Vec<TaskLeaseRow>), TaskRuntimeError> {
    if !commits.is_empty() {
        #[cfg(feature = "postgres")]
        if !leases.is_empty() {
            let (now, rows, leases) =
                super::mixed_read::read(tx, runner, commits, leases, expected, batch).await?;
            return Ok((now, Some(rows), leases));
        }
        let (now, rows) = outcomes(tx, runner, commits, expected, batch).await?;
        return Ok((now, Some(rows), Vec::new()));
    }
    if !leases.is_empty() {
        let (now, rows) = renewals(tx, leases, expected, batch).await?;
        return Ok((now, None, rows));
    }
    Ok((
        super::runtime::verify_runtime_policy(tx, expected).await?,
        None,
        Vec::new(),
    ))
}

impl<'r> db::sqlx::FromRow<'r, db::Row> for TurnRead {
    fn from_row(row: &'r db::Row) -> Result<Self, db::sqlx::Error> {
        use db::sqlx::Row as _;
        match row.try_get::<i32, _>("turn_kind")? {
            0 => Ok(Self::Runtime(RuntimeTurnRow::record_scan_ordered(
                row, &mut 1,
            )?)),
            1 => Ok(Self::Snapshot(TaskRow::record_scan_ordered(row, &mut 3)?)),
            2 => Ok(Self::Renewal(TaskLeaseRow::record_scan_ordered(
                row, &mut 3,
            )?)),
            3 => Ok(Self::Candidate(TaskClaimRow::record_scan_ordered(
                row, &mut 3,
            )?)),
            4 => Ok(Self::Deadline(LaneDeadlineRow::record_scan_unordered(row)?)),
            5 => Ok(Self::Renewal(TaskLeaseRow::record_scan_unordered(row)?)),
            _ => Err(db::sqlx::Error::Protocol(
                "invalid task turn row discriminator".into(),
            )),
        }
    }
}

/// Locks policy before owned tasks, even when every requested outcome is stale.
/// Each bounded chunk includes its policy evidence and uses ordinary row decoding.
pub(super) async fn outcomes(
    tx: &mut db::DbTransaction<'_>,
    runner: &str,
    commits: &[TaskCommit],
    expected: &str,
    batch_size: usize,
) -> Result<(DateTime<Utc>, Vec<TaskRow>), TaskRuntimeError> {
    let mut now = None;
    let mut tasks = Vec::with_capacity(commits.len());
    let mut ids = commits
        .iter()
        .map(|commit| commit.task_id.into_uuid())
        .collect::<Vec<_>>();
    ids.sort_unstable();
    if let Some((id, _)) = ids
        .iter()
        .zip(ids.iter().skip(1))
        .find(|(left, right)| left == right)
    {
        return Err(TaskRuntimeError::TaskExecutionError(format!(
            "task outcome batch contains duplicate task {id}"
        )));
    }
    for chunk in ids.chunks(batch_size.max(1).min(db::backend::PARAMETER_LIMIT - 3)) {
        let rows = tx
            .fetch_all::<TurnRead>(row_statement(Some(runner), chunk, expected))
            .await?;
        let mut verified = false;
        for row in rows {
            match row {
                TurnRead::Runtime(runtime) => {
                    runtime.verify(expected)?;
                    now.get_or_insert(runtime.now);
                    verified = true;
                }
                TurnRead::Snapshot(task) => tasks.push(task),
                _ => return Err(unexpected_projection()),
            }
        }
        if !verified {
            return Err(super::runtime::missing_policy());
        }
    }
    let now = match now {
        Some(now) => now,
        None => super::runtime::verify_runtime_policy(tx, expected).await?,
    };
    Ok((now, tasks))
}

/// Reads renewal evidence alongside policy, without retrieving any task payload bytes.
pub(super) async fn renewals(
    tx: &mut db::DbTransaction<'_>,
    leases: &[TaskLease],
    expected: &str,
    batch_size: usize,
) -> Result<(DateTime<Utc>, Vec<TaskLeaseRow>), TaskRuntimeError> {
    let mut now = None;
    let mut tasks = Vec::with_capacity(leases.len());
    let mut ids = leases
        .iter()
        .map(|lease| lease.task_id.into_uuid())
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids.dedup();
    for chunk in ids.chunks(batch_size.max(1).min(db::backend::PARAMETER_LIMIT - 3)) {
        let rows = tx
            .fetch_all::<TurnRead>(row_statement(None, chunk, expected))
            .await?;
        let mut verified = false;
        for row in rows {
            match row {
                TurnRead::Runtime(runtime) => {
                    runtime.verify(expected)?;
                    now.get_or_insert(runtime.now);
                    verified = true;
                }
                TurnRead::Renewal(task) => tasks.push(task),
                _ => return Err(unexpected_projection()),
            }
        }
        if !verified {
            return Err(super::runtime::missing_policy());
        }
    }
    let now = match now {
        Some(now) => now,
        None => super::runtime::verify_runtime_policy(tx, expected).await?,
    };
    Ok((now, tasks))
}

/// Reports an internal read-shape mismatch rather than silently dropping a task.
fn unexpected_projection() -> TaskRuntimeError {
    TaskRuntimeError::TaskExecutionError("unexpected task turn projection".into())
}

/// Combines shared policy fencing and deterministic outcome locks without selecting join data.
fn row_statement(runner: Option<&str>, ids: &[uuid::Uuid], expected: &str) -> db::Statement {
    let sql = row_sql(runner.is_some(), ids.len());
    let mut statement = db::Statement::raw(&sql)
        .bind(super::runtime::RUNTIME_ID)
        .bind(expected.to_owned());
    if let Some(runner) = runner {
        statement = statement.bind(runner.to_owned());
    }
    for id in ids {
        statement = statement.bind(*id);
    }
    statement
}

/// Renders only schema-owned column names; task inputs never enter SQL text.
fn row_projection(owned: bool) -> (String, String, String) {
    let columns = if owned {
        TaskRow::record_column_names()
    } else {
        TaskLeaseRow::record_column_names()
    };
    let task_columns = columns
        .iter()
        .map(|name| format!("t.{name}"))
        .collect::<Vec<_>>()
        .join(", ");
    let null_columns = columns
        .iter()
        .map(|name| format!("NULL AS {name}"))
        .collect::<Vec<_>>()
        .join(", ");

    (columns.join(", "), task_columns, null_columns)
}

/// Renders one locked snapshot statement for bounded task identities.
fn row_sql(owned: bool, count: usize) -> String {
    let (columns, task_columns, null_columns) = row_projection(owned);
    let first_id = if owned { 4 } else { 3 };
    let parameters = (0..count)
        .map(|offset| parameter(offset + first_id))
        .collect::<Vec<_>>()
        .join(", ");
    let ownership = owned
        .then(|| format!("AND t.locked_by = {} AND t.status = 1", parameter(3)))
        .unwrap_or_default();
    let kind = if owned { 1 } else { 2 };
    #[cfg(feature = "postgres")]
    let (shared, exclusive) = (" FOR SHARE", " FOR UPDATE");
    #[cfg(feature = "mysql")]
    let (shared, exclusive) = (" LOCK IN SHARE MODE", " FOR UPDATE");
    #[cfg(feature = "sqlite")]
    let (shared, exclusive) = ("", "");
    let sql = format!(
        "WITH runtime AS (SELECT policy_fingerprint, CURRENT_TIMESTAMP AS now \
         FROM vyuh_task_runtime WHERE id = {}{shared}), \
         owned AS (SELECT {task_columns} FROM vyuh_tasks t \
         WHERE EXISTS (SELECT 1 FROM runtime WHERE policy_fingerprint = {}) \
         {ownership} AND t.id IN ({parameters}) \
         ORDER BY t.id{exclusive}) \
         SELECT 0 AS turn_kind, policy_fingerprint, now, {null_columns} FROM runtime \
         UNION ALL SELECT {kind} AS turn_kind, NULL, NULL, {} FROM owned ORDER BY turn_kind",
        parameter(1),
        parameter(2),
        columns,
    );
    sql
}

/// Generates backend placeholders only; caller values are always bound parameters.
fn parameter(index: usize) -> String {
    if cfg!(feature = "postgres") {
        format!("${index}")
    } else {
        "?".into()
    }
}
