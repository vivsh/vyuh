//! Laneed batch claiming, durable rate permits, and database-relative wake hints.

use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::{
    db,
    tasks::{
        LaneClaim, LanePoll, TaskPoll, TaskRate, TaskRecord, TaskRetry, TaskRuntimeError,
        TaskStatus,
    },
};

use super::{
    common::DbTaskStore,
    model::{RatePatch, TaskRateRow, TaskRow},
};

use crate::tasks::rate::{TOKEN_SCALE, next_permit, refill};

/// Immutable inputs shared by one database lane-claim turn.
pub(super) struct ClaimTurn<'a> {
    pub(super) runner_id: &'a str,
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
        runner_id: &str,
        claims: &[LaneClaim],
    ) -> Result<TaskPoll, TaskRuntimeError> {
        let mut transaction = self.pool.begin().await?;
        let (conf, fingerprint) = self.runtime_conf.read().await.clone().ok_or_else(|| {
            TaskRuntimeError::InvalidConfig("task runtime was not initialized".into())
        })?;
        let (now, preloaded) = if let Some(claim) = super::claim_read::eligible(&conf, claims) {
            let (now, probe, selected, deadline) =
                super::claim_read::read(&mut transaction, claim, &fingerprint, self.batch_size)
                    .await?;
            (now, Some((probe, selected, deadline)))
        } else {
            (
                super::runtime::verify_runtime_policy(&mut transaction, &fingerprint).await?,
                None,
            )
        };
        let mut deliveries = Vec::new();
        let (mut poll, new_idle) = self
            .claim_tasks_tx(
                &mut transaction,
                runner_id,
                claims,
                &conf,
                now,
                &mut deliveries,
                preloaded,
            )
            .await?;
        let wake = super::writes::finalize_workflow(
            &mut transaction,
            &[],
            deliveries,
            &[],
            &conf,
            now,
            self.batch_size,
        )
        .await?;
        super::lane_owner::reconcile_workflow(&mut transaction, &mut poll, &wake, &new_idle, now)
            .await?;
        transaction.commit().await?;
        Ok(poll)
    }

    /// Claims one fair lane batch inside an already-authorized scheduler transaction.
    pub(super) async fn claim_tasks_tx(
        &self,
        transaction: &mut db::DbTransaction<'_>,
        runner_id: &str,
        claims: &[LaneClaim],
        conf: &crate::tasks::TaskStoreConf,
        now: DateTime<Utc>,
        deliveries: &mut Vec<(crate::tasks::TaskId, String)>,
        mut preloaded: Option<(Vec<TaskRow>, Vec<TaskRow>, Option<DateTime<Utc>>)>,
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
            super::writes::delete_expired_owners(transaction, now, self.batch_size).await?;
        }
        lanes.sort_by_key(|lane| {
            claims
                .iter()
                .position(|claim| claim.lane == lane.lane)
                .unwrap_or(usize::MAX)
        });
        let mut poll = TaskPoll { lanes };
        super::all::materialize(transaction, waits, &mut poll, self.batch_size).await?;
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
        if super::claim_read::eligible(turn.conf, std::slice::from_ref(turn.claim)).is_some() {
            let (probe, selected, deadline) =
                super::claim_read::read_after(transaction, turn.claim, turn.now, self.batch_size)
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
        let limit = turn.claim.limit.min(self.batch_size);
        let probed =
            probe_candidates(transaction, turn.now, turn.claim.lane.as_str(), limit).await?;
        let saturated = limit > 0 && probed.len() == limit;
        let runnable_count = runnable_count(&probed, turn.retry)?;
        let (permits, rate_wake) = self
            .reserve_rate(
                transaction,
                turn.claim.lane,
                turn.rate,
                runnable_count,
                turn.now,
            )
            .await?;
        let selected =
            select_candidates(transaction, turn.now, turn.claim.lane.as_str(), limit).await?;
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
            next_wake_in: effective_lane_wake(rate_blocked, rate_wake, task_wake),
            owner: None,
        })
    }

    /// Applies the shared cancellation, recovery and ownership writes to locked candidates.
    async fn finish_claim(
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
            super::writes::queue_delivery(row, deliveries)?;
        }
        candidates.truncate(permits);
        let reclaimed = candidates
            .iter()
            .filter(|row| row.status == TaskStatus::Running.as_i16())
            .count();
        let tasks = self
            .claim_candidates(tx, candidates, turn.runner_id, turn.now, waits)
            .await?;
        Ok((tasks, reclaimed))
    }

    /// Future rows cannot be claimed; only newly written leases augment the pre-claim deadline.
    async fn claim_preloaded(
        &self,
        tx: &mut db::DbTransaction<'_>,
        turn: ClaimTurn<'_>,
        probe: Vec<TaskRow>,
        selected: Vec<TaskRow>,
        deadline: Option<DateTime<Utc>>,
        deliveries: &mut Vec<(crate::tasks::TaskId, String)>,
        waits: &mut Vec<(crate::tasks::TaskId, String, i32)>,
    ) -> Result<LanePoll, TaskRuntimeError> {
        let limit = turn.claim.limit.min(self.batch_size);
        let saturated = limit > 0 && probe.len() == limit;
        let permits = runnable_count(&probe, turn.retry)?;
        let (tasks, reclaimed) = self
            .finish_claim(tx, &turn, selected, permits, deliveries, waits)
            .await?;
        let next = tasks
            .iter()
            .filter_map(|task| task.leased_until)
            .filter(|value| *value > turn.now)
            .chain(deadline)
            .min();
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
            super::writes::apply_outcome(
                row,
                &crate::tasks::TaskOutcome::fail("Maximum task attempts exhausted"),
                TaskRetry::default(),
                now,
            )?;
            row.locked_by = None;
            row.leased_until = None;
            row.updated_at = now;
        }
        super::writes::update_idempotency_batch(transaction, rows, conf, now).await?;
        super::writes::batch_update_rows(transaction, rows, self.batch_size).await
    }

    /// Persists ownership for a locked candidate set in one bounded update.
    async fn claim_candidates(
        &self,
        transaction: &mut db::DbTransaction<'_>,
        mut candidates: Vec<TaskRow>,
        runner_id: &str,
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
            row.locked_by = Some(runner_id.into());
            row.leased_until = Some(self.lease_until(row.lease_duration_ms, now)?);
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
                    &table.locked_by,
                    &table.leased_until,
                    &table.updated_at,
                ),
            )
            .batch_size(self.batch_size)
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
) -> Result<Vec<TaskRow>, TaskRuntimeError> {
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
        .slice::<TaskRow>(0, limit)
        .exec(transaction)
        .await
        .map_err(TaskRuntimeError::from)
}

fn runnable_count(rows: &[TaskRow], retry: TaskRetry) -> Result<usize, TaskRuntimeError> {
    let mut count = 0;
    for row in rows {
        if !row.cancelled && !retry.exhausted(row.step_attempts)? {
            count += 1;
        }
    }
    Ok(count)
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

fn configured_lane(
    conf: &crate::tasks::TaskStoreConf,
    lane: crate::tasks::TaskLane,
) -> Result<&crate::tasks::TaskLaneConf, TaskRuntimeError> {
    conf.lanes
        .iter()
        .find(|entry| entry.lane() == lane)
        .ok_or_else(|| TaskRuntimeError::UnknownLane(lane.to_string()))
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

/// Finds the earliest future ready task or reclaimable lease for polled lanes.
pub(super) async fn next_task_deadline(
    transaction: &mut db::DbTransaction<'_>,
    lane: &str,
    now: DateTime<Utc>,
) -> Result<Option<Duration>, TaskRuntimeError> {
    use db::DbSession as _;
    #[cfg(feature = "postgres")]
    let parameters = ["$1", "$2", "$3", "$4"];
    #[cfg(not(feature = "postgres"))]
    let parameters = ["?", "?", "?", "?"];
    let [pending_lane, pending_now, running_lane, running_now] = parameters;
    // Each scalar subquery retains its ordered index seek; MIN sees only two rows.
    let sql = format!(
        "SELECT MIN(deadline) FROM (SELECT (SELECT ready_at FROM vyuh_tasks \
         WHERE lane_name = {pending_lane} AND status = 0 AND ready_at > {pending_now} \
         ORDER BY ready_at LIMIT 1) AS deadline UNION ALL \
         SELECT (SELECT leased_until FROM vyuh_tasks WHERE lane_name = {running_lane} \
         AND status = 1 AND leased_until > {running_now} ORDER BY leased_until LIMIT 1)) deadlines"
    );
    let deadline: Option<DateTime<Utc>> = transaction
        .fetch_scalar(
            db::Statement::raw(&sql)
                .bind(lane.to_owned())
                .bind(now)
                .bind(lane.to_owned())
                .bind(now),
        )
        .await?;
    Ok(deadline.and_then(|value| (value - now).to_std().ok()))
}

/// Combines future work and token readiness without polling a blocked lane early.
fn effective_lane_wake(
    rate_blocked: bool,
    rate_wake: Option<Duration>,
    task_wake: Option<Duration>,
) -> Option<Duration> {
    if rate_blocked {
        return rate_wake;
    }
    task_wake.map(|task| rate_wake.map_or(task, |permit| permit.max(task)))
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
/// Locks one backend-supported candidate batch without waiting on peer workers.
async fn select_candidates(
    transaction: &mut db::DbTransaction<'_>,
    now: DateTime<Utc>,
    lane: &str,
    limit: usize,
) -> Result<Vec<TaskRow>, TaskRuntimeError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    use crate::db::backend::{LockWaitExt as _, RowLockExt as _};
    let table = DbTaskStore::table();
    Ok(db::from(&table)
        .filter(table.lane_name.eq(db::val(lane.to_string())))
        .filter(DbTaskStore::due_predicate(&table, now))
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
) -> Result<Vec<TaskRow>, TaskRuntimeError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let table = DbTaskStore::table();
    Ok(db::from(&table)
        .filter(table.lane_name.eq(db::val(lane.to_string())))
        .filter(DbTaskStore::due_predicate(&table, now))
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies quantized refill accounting retains elapsed time below one micro-token.
    #[test]
    fn slow_rate_refill_preserves_fractional_elapsed_time() -> Result<(), TaskRuntimeError> {
        let rate = TaskRate::new(1, Duration::from_secs(365 * 24 * 60 * 60));
        let started = Utc::now();
        let now = started + chrono::Duration::seconds(1);
        let mut row = TaskRateRow {
            id: uuid::Uuid::now_v7(),
            lane_name: "slow".into(),
            policy_fingerprint: "policy".into(),
            tokens_micros: 0,
            updated_at: started,
        };
        refill(&mut row.tokens_micros, &mut row.updated_at, rate, now)?;
        assert_eq!(row.tokens_micros, 0);
        assert_eq!(row.updated_at, started);
        let wake = next_permit(row.tokens_micros, rate, row.updated_at, now).ok_or_else(|| {
            TaskRuntimeError::TaskExecutionError("missing permit deadline".into())
        })?;
        assert!(wake < rate.period());
        Ok(())
    }
}
