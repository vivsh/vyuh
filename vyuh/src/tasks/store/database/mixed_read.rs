//! PostgreSQL outcome/renewal observations, discarded at the end of the store turn.

use super::{
    claim_sql::parameter,
    model::{TaskLeaseRow, TaskRow},
    turn_read::TurnRead,
};

#[cfg(all(test, feature = "migrations"))]
#[path = "tests/mixed_read.rs"]
mod tests;
use crate::{
    db::{self, DbSession as _, Record as _},
    tasks::{TaskCommit, TaskLease, TaskRuntimeError},
};
use chrono::{DateTime, Utc};
use std::collections::BTreeMap;

/// Locks bounded requested identities in order, without retrieving renewal-only task payloads.
pub(super) async fn read(
    tx: &mut db::DbTransaction<'_>,
    runner: &str,
    commits: &[TaskCommit],
    leases: &[TaskLease],
    expected: &str,
    batch: usize,
) -> Result<(DateTime<Utc>, Vec<TaskRow>, Vec<TaskLeaseRow>), TaskRuntimeError> {
    let requests = requests(commits, leases)?;
    let (mut now, mut outcomes, mut renewals) = (None, Vec::new(), Vec::new());
    for chunk in requests.chunks(batch.clamp(1, (db::backend::PARAMETER_LIMIT - 3) / 3)) {
        let rows = tx
            .fetch_all::<TurnRead>(statement(runner, expected, chunk))
            .await?;
        let mut verified = false;
        for row in rows {
            match row {
                TurnRead::Runtime(runtime) => {
                    runtime.verify(expected)?;
                    now.get_or_insert(runtime.now);
                    verified = true;
                }
                TurnRead::Snapshot(row) => outcomes.push(row),
                TurnRead::Renewal(row) => renewals.push(row),
                _ => {
                    return Err(TaskRuntimeError::TaskExecutionError(
                        "unexpected mixed turn projection".into(),
                    ));
                }
            }
        }
        if !verified {
            return Err(super::runtime::missing_policy());
        }
    }
    Ok((
        now.ok_or_else(super::runtime::missing_policy)?,
        outcomes,
        renewals,
    ))
}

/// Deduplicates only renewals; duplicate outcomes remain a rejected operation.
fn requests(
    commits: &[TaskCommit],
    leases: &[TaskLease],
) -> Result<Vec<(uuid::Uuid, bool, bool)>, TaskRuntimeError> {
    let mut ids = BTreeMap::new();
    for commit in commits {
        if ids
            .insert(commit.task_id.into_uuid(), (true, false))
            .is_some()
        {
            return Err(TaskRuntimeError::TaskExecutionError(format!(
                "task outcome batch contains duplicate task {}",
                commit.task_id
            )));
        }
    }
    for lease in leases {
        ids.entry(lease.task_id.into_uuid())
            .or_insert((false, false))
            .1 = true;
    }
    Ok(ids
        .into_iter()
        .map(|(id, (outcome, renewal))| (id, outcome, renewal))
        .collect())
}

/// One row projection masks lifecycle values for requests that only need lease evidence.
fn projection() -> String {
    let lease_columns = TaskLeaseRow::record_column_names();
    TaskRow::record_column_names().iter().map(|name| {
        if lease_columns.contains(name) { format!("t.{name}") }
        else { format!("CASE WHEN wanted.outcome AND t.locked_by = {} AND t.status = 1 THEN t.{name} ELSE NULL END AS {name}", parameter(3)) }
    }).collect::<Vec<_>>().join(", ")
}

/// Preserves policy fencing even when every task is absent or belongs to another owner.
fn statement(runner: &str, expected: &str, requests: &[(uuid::Uuid, bool, bool)]) -> db::Statement {
    let mut statement = db::Statement::raw(&sql(requests.len()))
        .bind(super::runtime::RUNTIME_ID)
        .bind(expected.to_owned())
        .bind(runner.to_owned());
    for (id, outcome, renewal) in requests {
        statement = statement.bind(*id).bind(*outcome).bind(*renewal);
    }
    statement
}

/// Renders schema-owned names and numbered parameters, never caller-provided SQL fragments.
fn sql(count: usize) -> String {
    let values = values(count);
    let columns = TaskRow::record_column_names();
    let nulls = columns
        .iter()
        .map(|name| format!("NULL AS {name}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "WITH runtime AS MATERIALIZED (SELECT policy_fingerprint, CURRENT_TIMESTAMP AS now \
         FROM vyuh_task_runtime WHERE id = {} FOR SHARE), \
         wanted(id, outcome, renewal) AS (VALUES {values}), \
         locked AS MATERIALIZED (SELECT CASE WHEN wanted.outcome AND t.locked_by = {} AND t.status = 1 \
         THEN 1 ELSE 5 END AS turn_kind, {} FROM vyuh_tasks t JOIN wanted ON wanted.id = t.id \
         WHERE EXISTS (SELECT 1 FROM runtime WHERE policy_fingerprint = {}) \
         AND (wanted.renewal OR (wanted.outcome AND t.locked_by = {} AND t.status = 1)) \
         ORDER BY t.id FOR UPDATE OF t) \
         SELECT 0 AS turn_kind, policy_fingerprint, now, {nulls} FROM runtime \
         UNION ALL SELECT turn_kind, NULL, NULL, {} FROM locked ORDER BY turn_kind, id",
        parameter(1),
        parameter(3),
        projection(),
        parameter(2),
        parameter(3),
        columns.join(", ")
    )
}

/// Expands only placeholder positions; all request identities and flags remain bound values.
fn values(count: usize) -> String {
    (0..count)
        .map(|i| {
            format!(
                "({}, {}, {})",
                parameter(4 + i * 3),
                parameter(5 + i * 3),
                parameter(6 + i * 3)
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}
