//! Ordinary claim evidence in one bounded statement; mutations remain in the shared path.

use super::{claim_sql, model::TaskRow, turn_read::TurnRead};
use crate::{
    db::{self, DbSession as _},
    tasks::{LaneClaim, TaskRuntimeError, TaskStoreConf},
};
use chrono::{DateTime, Utc};

#[cfg(all(
    test,
    feature = "migrations",
    any(feature = "postgres", feature = "sqlite")
))]
#[path = "tests/claim_read.rs"]
mod tests;

/// Selects only ordinary lanes without backend-specific rate or ownership coordination.
pub(super) fn eligible<'a>(conf: &TaskStoreConf, claims: &'a [LaneClaim]) -> Option<&'a LaneClaim> {
    if cfg!(feature = "mysql") {
        return None;
    }
    let [claim] = claims else { return None };
    conf.lanes
        .iter()
        .find(|lane| lane.lane() == claim.lane)
        .filter(|lane| lane.lane_lock().is_none() && lane.global_rate().is_none())
        .map(|_| claim)
}

/// Reads policy first and distinguishes it from task projections, even on an empty lane.
pub(super) async fn read(
    tx: &mut db::DbTransaction<'_>,
    claim: &LaneClaim,
    expected: &str,
    batch: usize,
) -> Result<
    (
        DateTime<Utc>,
        Vec<TaskRow>,
        Vec<TaskRow>,
        Option<DateTime<Utc>>,
    ),
    TaskRuntimeError,
> {
    let rows = tx
        .fetch_all::<TurnRead>(statement(claim, expected, batch))
        .await?;
    let mut now = None;
    for row in &rows {
        if let TurnRead::Runtime(runtime) = row {
            runtime.verify(expected)?;
            now = Some(runtime.now);
        }
    }
    let (probe, selected, deadline) = decode(rows, claim)?;
    Ok((
        now.ok_or_else(super::runtime::missing_policy)?,
        probe,
        selected,
        deadline,
    ))
}

/// Reads candidates after outcome/renewal writes, retaining the already-verified turn clock.
pub(super) async fn read_after(
    tx: &mut db::DbTransaction<'_>,
    claim: &LaneClaim,
    now: DateTime<Utc>,
    batch: usize,
) -> Result<(Vec<TaskRow>, Vec<TaskRow>, Option<DateTime<Utc>>), TaskRuntimeError> {
    let clock = claim_sql::parameter(1);
    let clock = if cfg!(feature = "postgres") {
        format!("CAST({clock} AS TIMESTAMPTZ)")
    } else {
        clock
    };
    let runtime = format!("WITH runtime AS MATERIALIZED (SELECT {clock} AS now)");
    let sql = claim_sql::query(
        &runtime,
        &claim_sql::parameter(2),
        &claim_sql::parameter(3),
        "TRUE",
        false,
    );
    let statement = db::Statement::raw(&sql)
        .bind(now)
        .bind(claim.lane.to_string())
        .bind(claim.limit.min(batch) as i64);
    decode(tx.fetch_all::<TurnRead>(statement).await?, claim)
}

/// Decodes only the discriminated projection; metadata never becomes a fabricated task.
fn decode(
    rows: Vec<TurnRead>,
    claim: &LaneClaim,
) -> Result<(Vec<TaskRow>, Vec<TaskRow>, Option<DateTime<Utc>>), TaskRuntimeError> {
    let (mut probes, mut selected, mut deadline) = (Vec::new(), Vec::new(), None);
    for row in rows {
        match row {
            TurnRead::Runtime(_) => {}
            TurnRead::Snapshot(row) => probes.push(row),
            TurnRead::Candidate(row) => selected.push(row.into()),
            TurnRead::Deadline(row) if row.lane_name == claim.lane.as_str() => {
                deadline = row.deadline
            }
            _ => {
                return Err(TaskRuntimeError::TaskExecutionError(
                    "unexpected claim projection".into(),
                ));
            }
        }
    }
    Ok((probes, selected, deadline))
}

/// Combines policy locking with bounded claim evidence without weakening empty-turn fencing.
fn statement(claim: &LaneClaim, expected: &str, batch: usize) -> db::Statement {
    let clock = if cfg!(feature = "sqlite") {
        "strftime('%Y-%m-%dT%H:%M:%S+00:00', CURRENT_TIMESTAMP)"
    } else {
        "CURRENT_TIMESTAMP"
    };
    let lock = if cfg!(feature = "postgres") {
        " FOR SHARE"
    } else {
        ""
    };
    let runtime = format!(
        "WITH runtime AS MATERIALIZED (SELECT policy_fingerprint, {clock} AS now \
         FROM vyuh_task_runtime WHERE id = {}{lock})",
        claim_sql::parameter(1)
    );
    let guard = format!(
        "(SELECT policy_fingerprint FROM runtime) = {}",
        claim_sql::parameter(2)
    );
    let sql = claim_sql::query(
        &runtime,
        &claim_sql::parameter(3),
        &claim_sql::parameter(4),
        &guard,
        true,
    );
    db::Statement::raw(&sql)
        .bind(super::runtime::RUNTIME_ID)
        .bind(expected.to_owned())
        .bind(claim.lane.to_string())
        .bind(claim.limit.min(batch) as i64)
}

/// Exposes actual backend plans for the bounded claim statement without executing it.
#[cfg(all(test, any(feature = "postgres", feature = "sqlite")))]
pub(super) async fn explain(
    tx: &mut db::DbTransaction<'_>,
    claim: &LaneClaim,
    expected: &str,
) -> Result<Vec<String>, TaskRuntimeError> {
    let (sql, args) = statement(claim, expected, 32)
        .into_parts()
        .map_err(db::DbError::from)?;
    #[cfg(feature = "postgres")]
    let rows = tx
        .fetch_all::<(String,)>(db::Statement::new(&format!("EXPLAIN {sql}"), args))
        .await?
        .into_iter()
        .map(|(line,)| line)
        .collect();
    #[cfg(feature = "sqlite")]
    let rows = tx
        .fetch_all::<(i64, i64, i64, String)>(db::Statement::new(
            &format!("EXPLAIN QUERY PLAN {sql}"),
            args,
        ))
        .await?
        .into_iter()
        .map(|(_, _, _, line)| line)
        .collect();
    Ok(rows)
}
