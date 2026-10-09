//! Lane-scoped batch claiming, durable rate permits, and database-relative wake hints.

#[cfg(test)]
#[path = "tests/claim.rs"]
pub(super) mod tests;
use crate::tasks::store::StoreFailure;

use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::{
    db,
    tasks::{
        LaneClaim, LanePoll, TaskPoll, TaskRate, TaskRecord, TaskRetry, TaskRuntimeError,
        TaskStatus, TaskStoreConf,
    },
};

use super::queries::{
    MAINTENANCE_BATCH_SIZE, StoreRow, WRITE_CHUNK_SIZE, configured_lane, deadline,
    effective_lane_wake, parameter, probe_columns,
};
use super::{
    model::{RatePatch, TaskClaimRow, TaskProbeRow, TaskRateRow, TaskReadyRow, TaskRow},
    store::DbTaskStore,
};
use crate::db::{DbSession as _, Record as _};

use crate::tasks::rate::{TOKEN_SCALE, next_permit, refill};

/// Immutable inputs shared by one database lane-claim turn.
pub(super) struct ClaimTurn<'a> {
    pub(super) runner_id: &'a uuid::Uuid,
    pub(super) claim: &'a LaneClaim,
    pub(super) rate: Option<TaskRate>,
    pub(super) retry: TaskRetry,
    pub(super) conf: &'a crate::tasks::TaskStoreConf,
    pub(super) now: DateTime<Utc>,
}

impl DbTaskStore {
    /// Claims all requested lanes in one transaction and returns store-relative timing evidence.
    #[allow(dead_code)]
    pub(super) async fn claim_tasks_impl(
        &self,
        runner_id: &uuid::Uuid,
        claims: &[LaneClaim],
    ) -> Result<TaskPoll, StoreFailure<TaskPoll>> {
        let mut transaction = self.pool.begin().await?;
        let conf = &self.runtime_conf;
        let mut deliveries = Vec::new();
        let (now, mut poll, new_idle) = self
            .poll_claims(&mut transaction, runner_id, claims, conf, &mut deliveries)
            .await?;
        let wake = super::workflow::finalize_workflow(
            &mut transaction,
            &[],
            deliveries,
            &[],
            conf,
            now,
            WRITE_CHUNK_SIZE,
        )
        .await?;
        super::lane_owner::reconcile_workflow(&mut transaction, &mut poll, &wake, &new_idle, now)
            .await?;
        poll.wake_lanes = wake;
        match transaction.commit().await {
            Ok(()) => Ok(poll),
            Err(error) => Err(StoreFailure::CommitUnknown {
                error: error.into(),
                prepared: poll,
            }),
        }
    }

    /// Chooses the PostgreSQL atomic claim without changing coordinated-lane transactions.
    async fn poll_claims(
        &self,
        tx: &mut db::DbTransaction<'_>,
        node: &uuid::Uuid,
        claims: &[LaneClaim],
        conf: &TaskStoreConf,
        deliveries: &mut Vec<(crate::tasks::TaskId, String)>,
    ) -> Result<(DateTime<Utc>, TaskPoll, Vec<crate::tasks::TaskLane>), TaskRuntimeError> {
        #[cfg(feature = "postgres")]
        if let Some(claim) = eligible(conf, claims) {
            let (now, poll) = self
                .claim_postgres(tx, node, claim, conf, deliveries)
                .await?;
            return Ok((now, poll, Vec::new()));
        }
        let (now, preloaded) = if let Some(claim) = eligible(conf, claims) {
            let (now, probe, selected, deadline) = read(
                tx,
                claim,
                configured_lane(conf, claim.lane)?.claim_batch_size_value(),
            )
            .await?;
            (now, Some((probe, selected, deadline)))
        } else {
            (super::queries::statement_now(tx).await?, None)
        };
        let (poll, idle) = self
            .claim_tasks_tx(tx, node, claims, conf, now, deliveries, preloaded)
            .await?;
        Ok((now, poll, idle))
    }

    /// Claims one fair lane batch inside an already-authorized scheduler transaction.
    pub(super) async fn claim_tasks_tx(
        &self,
        transaction: &mut db::DbTransaction<'_>,
        runner_id: &uuid::Uuid,
        claims: &[LaneClaim],
        conf: &crate::tasks::TaskStoreConf,
        now: DateTime<Utc>,
        deliveries: &mut Vec<(crate::tasks::TaskId, String)>,
        mut preloaded: Option<(Vec<TaskProbeRow>, Vec<TaskRow>, Option<DateTime<Utc>>)>,
    ) -> Result<(TaskPoll, Vec<crate::tasks::TaskLane>), TaskRuntimeError> {
        let mut new_idle = Vec::new();
        let mut waits = Vec::new();
        let mut ordered = claims.iter().collect::<Vec<_>>();
        ordered.sort_unstable_by_key(|claim| claim.lane.as_str());
        let mut lanes = Vec::with_capacity(claims.len());
        for claim in ordered {
            let lane_conf = configured_lane(conf, claim.lane)?;
            let lane = if lane_conf.lane_lock().is_some() {
                self.claim_owned_lane(
                    transaction,
                    runner_id,
                    claim,
                    lane_conf,
                    conf,
                    now,
                    deliveries,
                    &mut new_idle,
                    &mut waits,
                )
                .await?
            } else {
                let turn = ClaimTurn {
                    runner_id,
                    claim,
                    rate: lane_conf.global_rate(),
                    retry: lane_conf.retry_policy(),
                    conf,
                    now,
                };
                if let Some((probe, selected, deadline)) = preloaded.take() {
                    self.claim_preloaded(
                        transaction,
                        turn,
                        probe,
                        selected,
                        deadline,
                        deliveries,
                        &mut waits,
                    )
                    .await?
                } else {
                    self.claim_lane(transaction, turn, deliveries, &mut waits)
                        .await?
                }
            };
            lanes.push(lane);
        }
        if self.maintenance_due() {
            super::submit::delete_expired_owners(transaction, now, MAINTENANCE_BATCH_SIZE).await?;
        }
        lanes.sort_by_key(|lane| {
            claims
                .iter()
                .position(|claim| claim.lane == lane.lane)
                .unwrap_or(usize::MAX)
        });
        let mut poll = TaskPoll {
            lanes,
            wake_lanes: Vec::new(),
        };
        super::workflow::materialize(transaction, waits, &mut poll, WRITE_CHUNK_SIZE).await?;
        Ok((poll, new_idle))
    }

    /// Claims one lane's bounded candidates and reserves its durable permits.
    pub(super) async fn claim_lane(
        &self,
        transaction: &mut db::DbTransaction<'_>,
        turn: ClaimTurn<'_>,
        deliveries: &mut Vec<(crate::tasks::TaskId, String)>,
        waits: &mut Vec<(crate::tasks::TaskId, String, i32)>,
    ) -> Result<LanePoll, TaskRuntimeError> {
        if eligible(turn.conf, std::slice::from_ref(turn.claim)).is_some() {
            let (probe, selected, deadline) = read_after(
                transaction,
                turn.claim,
                turn.now,
                configured_lane(turn.conf, turn.claim.lane)?.claim_batch_size_value(),
            )
            .await?;
            return self
                .claim_preloaded(
                    transaction,
                    turn,
                    probe,
                    selected,
                    deadline,
                    deliveries,
                    waits,
                )
                .await;
        }
        self.claim_sequential(transaction, turn, deliveries, waits)
            .await
    }

    /// Keeps the established sequence for coordinated lanes and unsupported SQL backends.
    async fn claim_sequential(
        &self,
        transaction: &mut db::DbTransaction<'_>,
        turn: ClaimTurn<'_>,
        deliveries: &mut Vec<(crate::tasks::TaskId, String)>,
        waits: &mut Vec<(crate::tasks::TaskId, String, i32)>,
    ) -> Result<LanePoll, TaskRuntimeError> {
        let limit = turn
            .claim
            .limit
            .min(configured_lane(turn.conf, turn.claim.lane)?.claim_batch_size_value());
        let probed =
            probe_candidates(transaction, turn.now, turn.claim.lane.as_str(), limit).await?;
        let saturated = limit > 0 && probed.len() == limit;
        let runnable_count = runnable_work(&probed, turn.retry)?;
        let (permits, rate_wake) = self
            .reserve_rate(
                transaction,
                turn.claim.lane,
                turn.rate,
                runnable_count,
                turn.now,
            )
            .await?;
        let selected = select_candidates(
            transaction,
            turn.now,
            turn.claim.lane.as_str(),
            limit,
            (turn.rate.is_some() && permits == 0).then_some(turn.retry.max_attempts() as i32),
        )
        .await?;
        let (tasks, reclaimed) = self
            .finish_claim(transaction, &turn, selected, permits, deliveries, waits)
            .await?;
        let rate_blocked = permits < runnable_count;
        let task_wake = next_task_deadline(transaction, turn.claim.lane.as_str(), turn.now).await?;
        Ok(LanePoll {
            lane: turn.claim.lane,
            tasks,
            reclaimed,
            saturated,
            next_wake_in: effective_lane_wake(
                rate_blocked,
                rate_wake,
                task_wake,
                turn.conf
                    .handlers
                    .iter()
                    .any(|(_, kind)| *kind == crate::tasks::TaskKind::Flow),
            ),
            owner: None,
        })
    }

    /// Applies the shared cancellation, recovery and ownership writes to locked candidates.
    pub(super) async fn finish_claim(
        &self,
        tx: &mut db::DbTransaction<'_>,
        turn: &ClaimTurn<'_>,
        selected: Vec<TaskRow>,
        permits: usize,
        deliveries: &mut Vec<(crate::tasks::TaskId, String)>,
        waits: &mut Vec<(crate::tasks::TaskId, String, i32)>,
    ) -> Result<(Vec<TaskRecord>, usize), TaskRuntimeError> {
        let (mut exhausted, mut candidates) = split_exhausted(selected, turn.retry)?;
        self.fail_exhausted(tx, &mut exhausted, turn.conf, turn.now)
            .await?;
        for row in &exhausted {
            super::workflow::queue_delivery(row, deliveries)?;
        }
        let mut available = permits;
        candidates.retain(|row| {
            if row.kind == 1 {
                return true;
            }
            if available == 0 {
                return false;
            }
            available -= 1;
            true
        });
        let reclaimed = candidates
            .iter()
            .filter(|row| row.status == TaskStatus::Running.as_i16())
            .count();
        let tasks = self
            .claim_candidates(tx, candidates, turn.runner_id, turn.now, waits)
            .await?;
        Ok((tasks, reclaimed))
    }

    /// Keeps scheduled readiness independent of execution ownership.
    async fn claim_preloaded(
        &self,
        tx: &mut db::DbTransaction<'_>,
        turn: ClaimTurn<'_>,
        probe: Vec<TaskProbeRow>,
        selected: Vec<TaskRow>,
        deadline: Option<DateTime<Utc>>,
        deliveries: &mut Vec<(crate::tasks::TaskId, String)>,
        waits: &mut Vec<(crate::tasks::TaskId, String, i32)>,
    ) -> Result<LanePoll, TaskRuntimeError> {
        let limit = turn
            .claim
            .limit
            .min(configured_lane(turn.conf, turn.claim.lane)?.claim_batch_size_value());
        let saturated = limit > 0 && probe.len() == limit;
        let permits = runnable_count(&probe, turn.retry)?;
        let (tasks, reclaimed) = self
            .finish_claim(tx, &turn, selected, permits, deliveries, waits)
            .await?;
        let next = deadline;
        Ok(LanePoll {
            lane: turn.claim.lane,
            tasks,
            reclaimed,
            saturated,
            next_wake_in: next.and_then(|value| (value - turn.now).to_std().ok()),
            owner: None,
        })
    }

    /// Terminates expired leases that already consumed their invocation budget.
    async fn fail_exhausted(
        &self,
        transaction: &mut db::DbTransaction<'_>,
        rows: &mut [TaskRow],
        conf: &crate::tasks::TaskStoreConf,
        now: DateTime<Utc>,
    ) -> Result<(), TaskRuntimeError> {
        if rows.is_empty() {
            return Ok(());
        }
        for row in rows.iter_mut() {
            super::commit::apply_outcome(
                row,
                &crate::tasks::TaskOutcome::fail("Maximum task attempts exhausted"),
                TaskRetry::default(),
                now,
            )?;
            row.node_id = None;
            row.updated_at = now;
        }
        super::commit::update_idempotency_batch(transaction, rows, conf, now).await?;
        super::commit::batch_update_rows(transaction, rows, WRITE_CHUNK_SIZE).await
    }

    /// Persists ownership for a locked candidate set in one bounded update.
    async fn claim_candidates(
        &self,
        transaction: &mut db::DbTransaction<'_>,
        mut candidates: Vec<TaskRow>,
        runner_id: &uuid::Uuid,
        now: DateTime<Utc>,
        waits: &mut Vec<(crate::tasks::TaskId, String, i32)>,
    ) -> Result<Vec<TaskRecord>, TaskRuntimeError> {
        for row in &mut candidates {
            row.attempts = row.attempts.checked_add(1).ok_or_else(|| {
                TaskRuntimeError::TaskExecutionError("task attempt count overflowed".into())
            })?;
            row.step_attempts = row.step_attempts.checked_add(1).ok_or_else(|| {
                TaskRuntimeError::TaskExecutionError("task step attempt count overflowed".into())
            })?;
            row.status = TaskStatus::Running.as_i16();
            row.node_id = Some(*runner_id);
            row.updated_at = now;
        }
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let table = Self::table();
        let changed = db::from(&table)
            .update_many(
                &candidates,
                (
                    &table.status,
                    &table.attempts,
                    &table.step_attempts,
                    &table.node_id,
                    &table.updated_at,
                ),
            )
            .batch_size(WRITE_CHUNK_SIZE)
            .exec(transaction)
            .await?;
        if changed != candidates.len() as u64 {
            return Err(TaskRuntimeError::TaskExecutionError(
                "task claim batch changed an unexpected number of rows".into(),
            ));
        }
        for row in &mut candidates {
            if let Some(membership) = row.waiting_children.take() {
                waits.push((
                    crate::tasks::TaskId::new(row.id),
                    membership,
                    row.remaining_completions,
                ));
            }
        }
        Self::into_records(candidates)
    }

    /// Reserves durable lane permits in the same transaction as task claims.
    async fn reserve_rate(
        &self,
        transaction: &mut db::DbTransaction<'_>,
        lane: crate::tasks::TaskLane,
        rate: Option<TaskRate>,
        requested: usize,
        now: DateTime<Utc>,
    ) -> Result<(usize, Option<Duration>), TaskRuntimeError> {
        if requested == 0 {
            return Ok((0, None));
        }
        let Some(rate) = rate else {
            return Ok((requested, None));
        };
        let mut row = load_rate_for_update(transaction, lane.as_str())
            .await?
            .ok_or_else(|| {
                TaskRuntimeError::InvalidConfig(format!(
                    "task rate state for lane '{}' is not initialized",
                    lane
                ))
            })?;
        refill(&mut row.tokens_micros, &mut row.updated_at, rate, now)?;
        let available = usize::try_from(row.tokens_micros / TOKEN_SCALE).unwrap_or(usize::MAX);
        let permits = requested.min(available);
        row.tokens_micros = row.tokens_micros.saturating_sub(
            i64::try_from(permits)
                .unwrap_or(i64::MAX)
                .saturating_mul(TOKEN_SCALE),
        );
        persist_rate(transaction, &row).await?;
        Ok((
            permits,
            next_permit(row.tokens_micros, rate, row.updated_at, now),
        ))
    }
}

/// Reads one bounded candidate page without taking ownership locks.
pub(super) async fn probe_candidates(
    transaction: &mut db::DbTransaction<'_>,
    now: DateTime<Utc>,
    lane: &str,
    limit: usize,
) -> Result<Vec<TaskProbeRow>, TaskRuntimeError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let table = DbTaskStore::table();
    db::from(&table)
        .filter(table.lane_name.eq(db::val(lane.to_string())))
        .filter(DbTaskStore::due_predicate(&table, now))
        .sort(table.ready_at.asc())
        .sort(table.created_at.asc())
        .sort(table.id.asc())
        .slice::<TaskProbeRow>(0, limit)
        .exec(transaction)
        .await
        .map_err(TaskRuntimeError::from)
}

fn runnable_count(rows: &[TaskProbeRow], retry: TaskRetry) -> Result<usize, TaskRuntimeError> {
    let mut count = 0;
    for row in rows {
        if !row.cancelled && !retry.exhausted(row.step_attempts)? {
            count += 1;
        }
    }
    Ok(count)
}

/// Only Work rows consume durable rate permits; Flow remains subject to recovery budgets.
fn runnable_work(rows: &[TaskProbeRow], retry: TaskRetry) -> Result<usize, TaskRuntimeError> {
    rows.iter()
        .filter(|row| row.kind == 0)
        .try_fold(0, |count, row| {
            Ok(count + usize::from(!row.cancelled && !retry.exhausted(row.step_attempts)?))
        })
}

fn split_exhausted(
    rows: Vec<TaskRow>,
    retry: TaskRetry,
) -> Result<(Vec<TaskRow>, Vec<TaskRow>), TaskRuntimeError> {
    let mut exhausted = Vec::new();
    let mut runnable = Vec::new();
    for row in rows {
        if row.cancelled || retry.exhausted(row.step_attempts)? {
            exhausted.push(row);
        } else {
            runnable.push(row);
        }
    }
    Ok((exhausted, runnable))
}

/// Writes one lane's reserved fixed-point balance while its row is locked.
async fn persist_rate(
    transaction: &mut db::DbTransaction<'_>,
    row: &TaskRateRow,
) -> Result<(), TaskRuntimeError> {
    let table = DbTaskStore::rate_table();
    let patch = RatePatch {
        tokens_micros: row.tokens_micros,
        updated_at: row.updated_at,
    };
    db::from(&table)
        .filter(table.id.eq(db::val(row.id)))
        .update(&patch)
        .exec(transaction)
        .await?;
    Ok(())
}

/// Finds the earliest future ownerless pending task for a polled lane.
pub(super) async fn next_task_deadline(
    transaction: &mut db::DbTransaction<'_>,
    lane: &str,
    now: DateTime<Utc>,
) -> Result<Option<Duration>, TaskRuntimeError> {
    let table = DbTaskStore::table();
    let deadline = db::from(&table)
        .filter(table.lane_name.eq(db::val(lane.to_owned())))
        .filter(
            table
                .status
                .eq(db::val(0_i16))
                .or(table.status.eq(db::val(1_i16))),
        )
        .filter(table.node_id.is_null())
        .filter(table.ready_at.gt(db::val(Some(now))))
        .sort(table.ready_at.asc())
        .first::<TaskReadyRow>()
        .exec(transaction)
        .await?;
    Ok(deadline
        .and_then(|row| row.ready_at)
        .and_then(|value| (value - now).to_std().ok()))
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
/// Locks one backend-supported candidate batch without waiting on peer workers.
async fn select_candidates(
    transaction: &mut db::DbTransaction<'_>,
    now: DateTime<Utc>,
    lane: &str,
    limit: usize,
    blocked: Option<i32>,
) -> Result<Vec<TaskRow>, TaskRuntimeError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    use crate::db::backend::{LockWaitExt as _, RowLockExt as _};
    let table = DbTaskStore::table();
    let query = db::from(&table)
        .filter(table.lane_name.eq(db::val(lane.to_string())))
        .filter(DbTaskStore::due_predicate(&table, now));
    let query = if let Some(maximum) = blocked {
        query.filter(
            table
                .kind
                .eq(db::val(1_i16))
                .or(table.cancelled.eq(db::val(true)))
                .or(table.step_attempts.gte(db::val(maximum))),
        )
    } else {
        query
    };
    Ok(query
        .sort(table.ready_at.asc())
        .sort(table.created_at.asc())
        .sort(table.id.asc())
        .for_update()
        .skip_locked()
        .slice::<super::model::TaskClaimRow>(0, limit)
        .exec(transaction)
        .await?
        .into_iter()
        .map(TaskRow::from)
        .collect())
}

#[cfg(feature = "sqlite")]
/// Selects one candidate batch inside SQLite's serial write transaction.
async fn select_candidates(
    transaction: &mut db::DbTransaction<'_>,
    now: DateTime<Utc>,
    lane: &str,
    limit: usize,
    blocked: Option<i32>,
) -> Result<Vec<TaskRow>, TaskRuntimeError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let table = DbTaskStore::table();
    let query = db::from(&table)
        .filter(table.lane_name.eq(db::val(lane.to_string())))
        .filter(DbTaskStore::due_predicate(&table, now));
    let query = if let Some(maximum) = blocked {
        query.filter(
            table
                .kind
                .eq(db::val(1_i16))
                .or(table.cancelled.eq(db::val(true)))
                .or(table.step_attempts.gte(db::val(maximum))),
        )
    } else {
        query
    };
    Ok(query
        .sort(table.ready_at.asc())
        .sort(table.created_at.asc())
        .sort(table.id.asc())
        .slice::<super::model::TaskClaimRow>(0, limit)
        .exec(transaction)
        .await?
        .into_iter()
        .map(TaskRow::from)
        .collect())
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
/// Locks one durable rate row before refilling and reserving permits.
async fn load_rate_for_update(
    transaction: &mut db::DbTransaction<'_>,
    lane: &str,
) -> Result<Option<TaskRateRow>, TaskRuntimeError> {
    use crate::db::backend::RowLockExt as _;
    let table = DbTaskStore::rate_table();
    Ok(db::from(&table)
        .filter(table.lane_name.eq(db::val(lane.to_string())))
        .for_update()
        .first::<TaskRateRow>()
        .exec(transaction)
        .await?)
}

#[cfg(feature = "sqlite")]
/// Loads one durable rate row inside SQLite's serial write transaction.
async fn load_rate_for_update(
    transaction: &mut db::DbTransaction<'_>,
    lane: &str,
) -> Result<Option<TaskRateRow>, TaskRuntimeError> {
    let table = DbTaskStore::rate_table();
    Ok(db::from(&table)
        .filter(table.lane_name.eq(db::val(lane.to_string())))
        .first::<TaskRateRow>()
        .exec(transaction)
        .await?)
}

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

/// Reads database time alongside task projections, even on an empty lane.
pub(super) async fn read(
    tx: &mut db::DbTransaction<'_>,
    claim: &LaneClaim,
    batch: usize,
) -> Result<
    (
        DateTime<Utc>,
        Vec<TaskProbeRow>,
        Vec<TaskRow>,
        Option<DateTime<Utc>>,
    ),
    TaskRuntimeError,
> {
    let rows = tx.fetch_all::<StoreRow>(statement(claim, batch)).await?;
    let mut now = None;
    for row in &rows {
        if let StoreRow::Clock(clock) = row {
            now = Some(clock.now);
        }
    }
    let (probe, selected, deadline) = decode(rows, claim)?;
    Ok((
        now.ok_or_else(super::queries::missing_clock)?,
        probe,
        selected,
        deadline,
    ))
}

/// Reads candidates using the operation's existing database clock.
pub(super) async fn read_after(
    tx: &mut db::DbTransaction<'_>,
    claim: &LaneClaim,
    now: DateTime<Utc>,
    batch: usize,
) -> Result<(Vec<TaskProbeRow>, Vec<TaskRow>, Option<DateTime<Utc>>), TaskRuntimeError> {
    let clock = parameter(1);
    let clock = if cfg!(feature = "postgres") {
        format!("CAST({clock} AS TIMESTAMPTZ)")
    } else {
        clock
    };
    let clock = format!("WITH clock AS MATERIALIZED (SELECT {clock} AS now)");
    let sql = query(&clock, &parameter(2), &parameter(3), false);
    let statement = db::Statement::raw(&sql)
        .bind(now)
        .bind(claim.lane.to_string())
        .bind(claim.limit.min(batch) as i64);
    decode(tx.fetch_all::<StoreRow>(statement).await?, claim)
}

/// Decodes only the discriminated projection; metadata never becomes a fabricated task.
fn decode(
    rows: Vec<StoreRow>,
    claim: &LaneClaim,
) -> Result<(Vec<TaskProbeRow>, Vec<TaskRow>, Option<DateTime<Utc>>), TaskRuntimeError> {
    let (mut probes, mut selected, mut deadline) = (Vec::new(), Vec::new(), None);
    for row in rows {
        match row {
            StoreRow::Clock(_) => {}
            StoreRow::Probe(row) => probes.push(row),
            StoreRow::Candidate(row) => selected.push(row.into()),
            StoreRow::Deadline(row) if row.lane_name == claim.lane.as_str() => {
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

/// Includes database time even when the lane has no ready tasks.
fn statement(claim: &LaneClaim, batch: usize) -> db::Statement {
    let clock = if cfg!(feature = "sqlite") {
        "strftime('%Y-%m-%dT%H:%M:%S+00:00', CURRENT_TIMESTAMP)"
    } else {
        "CURRENT_TIMESTAMP"
    };
    let clock = format!("WITH clock AS MATERIALIZED (SELECT {clock} AS now)");
    let sql = query(&clock, &parameter(1), &parameter(2), true);
    db::Statement::raw(&sql)
        .bind(claim.lane.to_string())
        .bind(claim.limit.min(batch) as i64)
}

/// Keeps the mixed-row clock wrapper raw: Mool cannot emit a source-free clock row.
/// Payloads are selected only by the locked branch; contention probes are metadata-only.
pub(super) fn query(clock: &str, lane: &str, limit: &str, include_clock: bool) -> String {
    let probe = if cfg!(feature = "sqlite") {
        format!(
            "SELECT {} FROM selected",
            TaskProbeRow::record_column_names().join(", ")
        )
    } else {
        candidates(&TaskProbeRow::record_column_names(), lane, limit, false)
    };
    let selected = candidates(&TaskClaimRow::record_column_names(), lane, limit, true);
    format!(
        "{clock}, probes AS MATERIALIZED ({probe}), \
         selected AS MATERIALIZED ({selected}) {} ORDER BY turn_kind, ready_at, created_at, id",
        projection(lane, include_clock)
    )
}

/// Keeps ordered bounded selection and the existing task-lock policy under contention.
fn candidates(columns: &[String], lane: &str, limit: &str, locked: bool) -> String {
    let columns = columns
        .iter()
        .map(|name| format!("t.{name}"))
        .collect::<Vec<_>>()
        .join(", ");
    let lock = if locked && cfg!(feature = "postgres") {
        " FOR UPDATE OF t SKIP LOCKED"
    } else {
        ""
    };
    format!("SELECT {columns} FROM vyuh_tasks t WHERE t.lane_name = {lane}
        AND t.status IN (0, 1) AND t.node_id IS NULL AND (t.ready_at IS NULL OR t.ready_at <= (SELECT now FROM clock))
        ORDER BY t.ready_at, t.created_at, t.id LIMIT {limit}{lock}")
}

/// Preserves separate row projections without transporting payloads inside JSON envelopes.
fn projection(lane: &str, include_clock: bool) -> String {
    let columns = TaskClaimRow::record_column_names();
    let probe_columns = probe_columns(&columns);
    let nulls = columns
        .iter()
        .map(|name| format!("NULL AS {name}"))
        .collect::<Vec<_>>()
        .join(", ");
    let deadline_columns = columns
        .iter()
        .map(|name| {
            if name == "lane_name" {
                lane.to_owned()
            } else {
                "NULL".into()
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let null_deadline = if cfg!(feature = "postgres") {
        "CAST(NULL AS TIMESTAMPTZ)"
    } else {
        "NULL"
    };
    let metadata = if include_clock {
        format!(
            "SELECT 0 AS turn_kind, now, {nulls}, {null_deadline} AS deadline FROM clock UNION ALL "
        )
    } else {
        String::new()
    };
    format!(
        "{metadata}SELECT 3 AS turn_kind, NULL AS now, \
         selected.*, {null_deadline} AS deadline FROM selected \
         UNION ALL SELECT 5, NULL, {probe_columns}, NULL FROM probes \
         UNION ALL SELECT 4, NULL, {deadline_columns}, {} FROM clock",
        deadline(lane)
    )
}
