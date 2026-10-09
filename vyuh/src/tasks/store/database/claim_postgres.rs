//! PostgreSQL ownership assignment and claim evidence in one bounded statement.
use super::store::DbTaskStore;

use super::{
    claim::ClaimTurn,
    model::{TaskClaimRow, TaskRow},
    queries::configured_lane,
};
use crate::db::{DbSession as _, Record as _};
use crate::{
    db,
    tasks::{LaneClaim, LanePoll, TaskId, TaskPoll, TaskRuntimeError, TaskStoreConf},
};
use chrono::{DateTime, Utc};

#[cfg(test)]
#[path = "tests/claim_postgres.rs"]
pub(super) mod tests;

/// Operation-local evidence distinguishes locked fallback rows from completed claim writes.
#[allow(clippy::large_enum_variant)]
enum PgClaimRow {
    Hints {
        now: DateTime<Utc>,
        next_ready_at: Option<DateTime<Utc>>,
        ready_remaining: bool,
        fast_path: bool,
    },
    Selected(TaskClaimRow),
    Claimed {
        task: TaskClaimRow,
        reclaimed: bool,
    },
}

impl<'r> db::sqlx::FromRow<'r, db::Row> for PgClaimRow {
    fn from_row(row: &'r db::Row) -> Result<Self, db::sqlx::Error> {
        use db::sqlx::Row as _;
        match row.try_get::<i32, _>("turn_kind")? {
            0 => Ok(Self::Hints {
                now: row.try_get("now")?,
                next_ready_at: row.try_get("next_ready_at")?,
                ready_remaining: row.try_get("ready_remaining")?,
                fast_path: row.try_get("fast_path")?,
            }),
            1 => Ok(Self::Selected(TaskClaimRow::record_scan_ordered(
                row, &mut 6,
            )?)),
            2 => Ok(Self::Claimed {
                task: TaskClaimRow::record_scan_ordered(row, &mut 6)?,
                reclaimed: row.try_get("reclaimed")?,
            }),
            _ => Err(db::sqlx::Error::Protocol(
                "invalid PostgreSQL claim projection".into(),
            )),
        }
    }
}

impl DbTaskStore {
    /// Claims ordinary rows in SQL or reuses the locked page for shared exceptional transitions.
    pub(super) async fn claim_postgres(
        &self,
        tx: &mut db::DbTransaction<'_>,
        node: &uuid::Uuid,
        claim: &LaneClaim,
        conf: &TaskStoreConf,
        deliveries: &mut Vec<(TaskId, String)>,
    ) -> Result<(DateTime<Utc>, TaskPoll), TaskRuntimeError> {
        let lane = configured_lane(conf, claim.lane)?;
        let limit = claim.limit.min(lane.claim_batch_size_value());
        let rows = tx
            .fetch_all::<PgClaimRow>(statement(
                node,
                claim,
                limit,
                lane.retry_policy().max_attempts(),
            ))
            .await?;
        let now = hints(&rows)?.0;
        let turn = ClaimTurn {
            runner_id: node,
            claim,
            rate: None,
            retry: lane.retry_policy(),
            conf,
            now,
        };
        self.apply_postgres(tx, turn, rows, deliveries).await
    }

    /// Preserves shared fallback transitions and finalizes joins before claim acknowledgement.
    async fn apply_postgres(
        &self,
        tx: &mut db::DbTransaction<'_>,
        turn: ClaimTurn<'_>,
        rows: Vec<PgClaimRow>,
        deliveries: &mut Vec<(TaskId, String)>,
    ) -> Result<(DateTime<Utc>, TaskPoll), TaskRuntimeError> {
        let (now, next, remaining, fast) = hints(&rows)?;
        let claim = turn.claim;
        let limit = claim
            .limit
            .min(configured_lane(turn.conf, claim.lane)?.claim_batch_size_value());
        let (selected, tasks, reclaimed) = decode(rows, fast)?;
        let saturated = limit > 0 && (selected.len() + tasks.len() == limit || remaining);
        let mut waits = Vec::new();
        let (tasks, reclaimed) = if fast {
            (tasks, reclaimed)
        } else {
            self.finish_claim(tx, &turn, selected, limit, deliveries, &mut waits)
                .await?
        };
        let mut poll = TaskPoll {
            lanes: vec![LanePoll {
                lane: claim.lane,
                tasks,
                reclaimed,
                saturated,
                next_wake_in: next.and_then(|at| (at - now).to_std().ok()),
                owner: None,
            }],
            wake_lanes: Vec::new(),
        };
        if self.maintenance_due() {
            super::submit::delete_expired_owners(tx, now, super::queries::MAINTENANCE_BATCH_SIZE)
                .await?;
        }
        super::workflow::materialize(tx, waits, &mut poll, super::queries::WRITE_CHUNK_SIZE)
            .await?;
        Ok((now, poll))
    }
}

/// Requires exactly one timing row, including for an empty selection.
fn hints(
    rows: &[PgClaimRow],
) -> Result<(DateTime<Utc>, Option<DateTime<Utc>>, bool, bool), TaskRuntimeError> {
    let mut hints = rows.iter().filter_map(|row| match row {
        PgClaimRow::Hints {
            now,
            next_ready_at,
            ready_remaining,
            fast_path,
        } => Some((*now, *next_ready_at, *ready_remaining, *fast_path)),
        _ => None,
    });
    let result = hints.next().ok_or_else(super::queries::missing_clock)?;
    if hints.next().is_some() {
        return Err(invalid());
    }
    Ok(result)
}

/// Never allows a partially applied ownership update to masquerade as a fallback batch.
fn decode(
    rows: Vec<PgClaimRow>,
    fast: bool,
) -> Result<(Vec<TaskRow>, Vec<crate::tasks::TaskRecord>, usize), TaskRuntimeError> {
    let (mut selected, mut tasks, mut reclaimed) = (Vec::new(), Vec::new(), 0);
    for row in rows {
        match row {
            PgClaimRow::Hints { .. } => {}
            PgClaimRow::Selected(row) if !fast => selected.push(TaskRow::from(row)),
            PgClaimRow::Claimed {
                task,
                reclaimed: recovered,
            } if fast => {
                tasks.push(TaskRow::from(task).try_into()?);
                reclaimed += usize::from(recovered);
            }
            _ => return Err(invalid()),
        }
    }
    Ok((selected, tasks, reclaimed))
}

fn invalid() -> TaskRuntimeError {
    TaskRuntimeError::TaskExecutionError("inconsistent PostgreSQL claim response".into())
}

/// Mool lacks a typed data-modifying CTE with a source-free metadata result; bind all inputs.
fn statement(
    node: &uuid::Uuid,
    claim: &LaneClaim,
    limit: usize,
    max_attempts: u32,
) -> db::Statement {
    let sql = format!(
        "WITH clock AS MATERIALIZED (SELECT NOW() AS now),
        selected AS MATERIALIZED ({}),
        guard AS MATERIALIZED (SELECT NOT EXISTS (SELECT 1 FROM selected s WHERE
            s.cancelled OR s.step_attempts < 0 OR s.step_attempts >= $4::bigint
            OR s.attempts = 2147483647 OR s.step_attempts = 2147483647
            OR s.waiting_children IS NOT NULL) AS fast_path),
        claimed AS ({}) {} ORDER BY turn_kind, ready_at, created_at, id",
        selection(),
        update(),
        projection()
    );
    db::Statement::raw(&sql)
        .bind(claim.lane.to_string())
        .bind(limit as i64)
        .bind(*node)
        .bind(i64::from(max_attempts))
}

/// Streams ordered identities into one bounded locking step; payloads are loaded once.
fn selection() -> String {
    let columns = TaskClaimRow::record_column_names()
        .iter()
        .map(|name| format!("t.{name}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT {columns} FROM vyuh_tasks t WHERE {}
        ORDER BY t.ready_at, t.created_at, t.id LIMIT $2 FOR UPDATE OF t SKIP LOCKED",
        due()
    )
}

fn due() -> &'static str {
    "t.lane_name = $1 AND t.node_id IS NULL AND t.status IN (0, 1)
     AND (t.ready_at IS NULL OR t.ready_at <= (SELECT now FROM clock))"
}

/// Locked selection owns eligibility; repeating its predicate can mis-plan the identity update.
fn update() -> String {
    let fields = TaskClaimRow::record_column_names()
        .iter()
        .map(|name| match name.as_str() {
            "status" | "node_id" | "attempts" | "step_attempts" | "updated_at" => {
                format!("t.{name}")
            }
            _ => format!("s.{name}"),
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "UPDATE vyuh_tasks t SET status = 1, node_id = $3, attempts = t.attempts + 1,
        step_attempts = t.step_attempts + 1, updated_at = (SELECT now FROM clock)
     FROM selected s WHERE t.id = s.id AND (SELECT fast_path FROM guard)
     RETURNING s.status = 1 AS reclaimed, {fields}"
    )
}

/// Reads changed fields from RETURNING, not the statement snapshot's old task versions.
fn projection() -> String {
    let names = TaskClaimRow::record_column_names();
    let nulls = names
        .iter()
        .map(|name| format!("NULL AS {name}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT CASE WHEN guard.fast_path AND EXISTS
            (SELECT id FROM selected EXCEPT SELECT id FROM claimed) THEN -1 ELSE 0 END AS turn_kind,
        clock.now, {} AS next_ready_at, {} AS ready_remaining,
        guard.fast_path, NULL::boolean AS reclaimed, {nulls} FROM clock CROSS JOIN guard
        UNION ALL SELECT 2, NULL, NULL, NULL, NULL, claimed.* FROM claimed
        UNION ALL SELECT 1, NULL, NULL, NULL, NULL, NULL, selected.* FROM selected
            WHERE NOT (SELECT fast_path FROM guard)",
        deadline(),
        remaining()
    )
}

fn deadline() -> String {
    super::queries::deadline("$1")
}

/// Under contention, a short page must not turn visible ready work into a long fallback wait.
fn remaining() -> String {
    "CASE WHEN $2 > 0 AND NOT EXISTS (SELECT 1 FROM selected OFFSET GREATEST($2 - 1, 0) LIMIT 1)
        THEN EXISTS (SELECT 1 FROM vyuh_tasks t WHERE t.lane_name = $1 AND t.node_id IS NULL
            AND t.status IN (0, 1) AND (t.ready_at IS NULL OR t.ready_at <= (SELECT now FROM clock))
            AND NOT EXISTS (SELECT 1 FROM selected s WHERE s.id = t.id))
        ELSE FALSE END"
        .into()
}
