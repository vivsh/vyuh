//! Grouped batch claiming, durable rate permits, and database-relative wake hints.

use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::{
    db,
    tasks::{
        GroupClaim, GroupPoll, TaskError, TaskIdempotency, TaskPoll, TaskRate, TaskRecord,
        TaskRetry, TaskStatus,
    },
};

use super::{
    common::DbTaskStore,
    model::{RatePatch, TaskRateRow, TaskRow},
};

use crate::tasks::rate::{TOKEN_SCALE, next_permit, refill};

impl DbTaskStore {
    /// Claims all requested groups in one transaction and returns store-relative timing evidence.
    pub(super) async fn claim_tasks_impl(
        &self,
        runner_id: &str,
        claims: &[GroupClaim],
    ) -> Result<TaskPoll, TaskError> {
        let mut transaction = self.pool.begin().await?;
        let conf =
            self.runtime_conf.read().await.clone().ok_or_else(|| {
                TaskError::InvalidConfig("task runtime was not initialized".into())
            })?;
        super::runtime::verify_runtime_policy(&mut transaction, &conf).await?;
        let now = statement_now(&mut transaction).await?;
        let mut ordered = claims.iter().collect::<Vec<_>>();
        ordered.sort_unstable_by_key(|claim| claim.group.as_str());
        let mut groups = Vec::with_capacity(claims.len());
        for claim in ordered {
            let group_conf = configured_group(&conf, claim.group)?;
            let group = self
                .claim_group(
                    &mut transaction,
                    runner_id,
                    claim,
                    group_conf.global_rate(),
                    group_conf.retry_policy(),
                    conf.idempotency,
                    now,
                )
                .await?;
            groups.push(group);
        }
        super::writes::delete_expired_owners(&mut transaction, now, self.batch_size).await?;
        groups.sort_by_key(|group| {
            claims
                .iter()
                .position(|claim| claim.group == group.group)
                .unwrap_or(usize::MAX)
        });
        transaction.commit().await?;
        Ok(TaskPoll { groups })
    }

    /// Claims one group's bounded candidates and reserves its durable permits.
    async fn claim_group(
        &self,
        transaction: &mut db::DbTransaction<'_>,
        runner_id: &str,
        claim: &GroupClaim,
        rate: Option<TaskRate>,
        retry: TaskRetry,
        idempotency: TaskIdempotency,
        now: DateTime<Utc>,
    ) -> Result<GroupPoll, TaskError> {
        let limit = claim.limit.min(self.batch_size);
        let probed = probe_candidates(transaction, now, claim.group.as_str(), limit).await?;
        let saturated = limit > 0 && probed.len() == limit;
        let runnable_count = runnable_count(&probed, retry)?;
        let (permits, rate_wake) = self
            .reserve_rate(transaction, claim.group, rate, runnable_count, now)
            .await?;
        let selected = select_candidates(transaction, now, claim.group.as_str(), limit).await?;
        let (mut exhausted, mut candidates) = split_exhausted(selected, retry)?;
        self.fail_exhausted(transaction, &mut exhausted, idempotency, now)
            .await?;
        candidates.truncate(permits);
        let rate_blocked = permits < runnable_count;
        let reclaimed = candidates
            .iter()
            .filter(|row| row.status == TaskStatus::Running.as_i16())
            .count();
        let tasks = self
            .claim_candidates(transaction, candidates, runner_id, now)
            .await?;
        let task_wake = next_task_deadline(transaction, claim.group.as_str(), now).await?;
        Ok(GroupPoll {
            group: claim.group,
            tasks,
            reclaimed,
            saturated,
            next_wake_in: effective_group_wake(rate_blocked, rate_wake, task_wake),
        })
    }

    /// Terminates expired leases that already consumed their invocation budget.
    async fn fail_exhausted(
        &self,
        transaction: &mut db::DbTransaction<'_>,
        rows: &mut [TaskRow],
        idempotency: TaskIdempotency,
        now: DateTime<Utc>,
    ) -> Result<(), TaskError> {
        if rows.is_empty() {
            return Ok(());
        }
        for row in rows.iter_mut() {
            super::writes::finish(
                row,
                TaskStatus::Failed,
                Some("Maximum task attempts exhausted".into()),
                now,
            );
            row.locked_by = None;
            row.leased_until = None;
            row.updated_at = now;
        }
        super::writes::update_idempotency_batch(transaction, rows, idempotency, now).await?;
        super::writes::batch_update_rows(transaction, rows, self.batch_size).await
    }

    /// Persists ownership for a locked candidate set in one bounded update.
    async fn claim_candidates(
        &self,
        transaction: &mut db::DbTransaction<'_>,
        mut candidates: Vec<TaskRow>,
        runner_id: &str,
        now: DateTime<Utc>,
    ) -> Result<Vec<TaskRecord>, TaskError> {
        for row in &mut candidates {
            row.attempts = row.attempts.checked_add(1).ok_or_else(|| {
                TaskError::TaskExecutionError("task attempt count overflowed".into())
            })?;
            row.status = TaskStatus::Running.as_i16();
            row.locked_by = Some(runner_id.into());
            row.leased_until = Some(self.lease_until(row, now)?);
            row.updated_at = now;
        }
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let table = Self::table();
        let changed = db::from(&table)
            .batch_update(
                &candidates,
                (
                    &table.status,
                    &table.attempts,
                    &table.locked_by,
                    &table.leased_until,
                    &table.updated_at,
                ),
            )
            .batch_size(self.batch_size)
            .exec(transaction)
            .await?;
        if changed != candidates.len() as u64 {
            return Err(TaskError::TaskExecutionError(
                "task claim batch changed an unexpected number of rows".into(),
            ));
        }
        Self::into_records(candidates)
    }

    /// Reserves durable group permits in the same transaction as task claims.
    async fn reserve_rate(
        &self,
        transaction: &mut db::DbTransaction<'_>,
        group: crate::tasks::TaskGroup,
        rate: Option<TaskRate>,
        requested: usize,
        now: DateTime<Utc>,
    ) -> Result<(usize, Option<Duration>), TaskError> {
        if requested == 0 {
            return Ok((0, None));
        }
        let Some(rate) = rate else {
            return Ok((requested, None));
        };
        let mut row = load_rate_for_update(transaction, group.as_str())
            .await?
            .ok_or_else(|| {
                TaskError::InvalidConfig(format!(
                    "task rate state for group '{}' is not initialized",
                    group
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
async fn probe_candidates(
    transaction: &mut db::DbTransaction<'_>,
    now: DateTime<Utc>,
    group: &str,
    limit: usize,
) -> Result<Vec<TaskRow>, TaskError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let table = DbTaskStore::table();
    db::from(&table)
        .filter(table.group_name.eq(db::val(group.to_string())))
        .filter(DbTaskStore::due_predicate(&table, now))
        .order_by(table.ready_at.asc())
        .order_by(table.created_at.asc())
        .order_by(table.id.asc())
        .slice::<TaskRow>(0, limit)
        .exec(transaction)
        .await
        .map_err(TaskError::from)
}

fn runnable_count(rows: &[TaskRow], retry: TaskRetry) -> Result<usize, TaskError> {
    let mut count = 0;
    for row in rows {
        if !retry.exhausted(row.attempts)? {
            count += 1;
        }
    }
    Ok(count)
}

fn split_exhausted(
    rows: Vec<TaskRow>,
    retry: TaskRetry,
) -> Result<(Vec<TaskRow>, Vec<TaskRow>), TaskError> {
    let mut exhausted = Vec::new();
    let mut runnable = Vec::new();
    for row in rows {
        if retry.exhausted(row.attempts)? {
            exhausted.push(row);
        } else {
            runnable.push(row);
        }
    }
    Ok((exhausted, runnable))
}

fn configured_group(
    conf: &crate::tasks::TaskStoreConf,
    group: crate::tasks::TaskGroup,
) -> Result<&crate::tasks::TaskGroupConf, TaskError> {
    conf.groups
        .iter()
        .find(|entry| entry.group() == group)
        .ok_or_else(|| TaskError::UnknownGroup(group.to_string()))
}

/// Writes one group's reserved fixed-point balance while its row is locked.
async fn persist_rate(
    transaction: &mut db::DbTransaction<'_>,
    row: &TaskRateRow,
) -> Result<(), TaskError> {
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

async fn statement_now(
    transaction: &mut db::DbTransaction<'_>,
) -> Result<DateTime<Utc>, TaskError> {
    use db::DbSession as _;
    Ok(transaction
        .fetch_scalar(db::Statement::raw("SELECT CURRENT_TIMESTAMP"))
        .await?)
}

/// Finds the earliest future ready task or reclaimable lease for polled groups.
async fn next_task_deadline(
    transaction: &mut db::DbTransaction<'_>,
    group: &str,
    now: DateTime<Utc>,
) -> Result<Option<Duration>, TaskError> {
    let table = DbTaskStore::table();
    let pending = db::from(&table)
        .filter(table.group_name.eq(db::val(group.to_string())))
        .filter(table.status.eq(db::val(TaskStatus::Pending.as_i16())))
        .filter(table.ready_at.gt(db::val(Some(now))))
        .order_by(table.ready_at.asc())
        .first::<TaskRow>()
        .exec(&mut *transaction)
        .await?;
    let running = db::from(&table)
        .filter(table.group_name.eq(db::val(group.to_string())))
        .filter(table.status.eq(db::val(TaskStatus::Running.as_i16())))
        .filter(table.leased_until.gt(db::val(Some(now))))
        .order_by(table.leased_until.asc())
        .first::<TaskRow>()
        .exec(transaction)
        .await?;
    let deadline = [
        pending.and_then(|row| row.ready_at),
        running.and_then(|row| row.leased_until),
    ]
    .into_iter()
    .flatten()
    .min();
    Ok(deadline.and_then(|value| (value - now).to_std().ok()))
}

/// Combines future work and token readiness without polling a blocked lane early.
fn effective_group_wake(
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
    group: &str,
    limit: usize,
) -> Result<Vec<TaskRow>, TaskError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    use crate::db::backend::{LockWaitExt as _, RowLockExt as _};
    let table = DbTaskStore::table();
    Ok(db::from(&table)
        .filter(table.group_name.eq(db::val(group.to_string())))
        .filter(DbTaskStore::due_predicate(&table, now))
        .order_by(table.ready_at.asc())
        .order_by(table.created_at.asc())
        .order_by(table.id.asc())
        .for_update()
        .skip_locked()
        .slice::<TaskRow>(0, limit)
        .exec(transaction)
        .await?)
}

#[cfg(feature = "sqlite")]
/// Selects one candidate batch inside SQLite's serial write transaction.
async fn select_candidates(
    transaction: &mut db::DbTransaction<'_>,
    now: DateTime<Utc>,
    group: &str,
    limit: usize,
) -> Result<Vec<TaskRow>, TaskError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let table = DbTaskStore::table();
    Ok(db::from(&table)
        .filter(table.group_name.eq(db::val(group.to_string())))
        .filter(DbTaskStore::due_predicate(&table, now))
        .order_by(table.ready_at.asc())
        .order_by(table.created_at.asc())
        .order_by(table.id.asc())
        .slice::<TaskRow>(0, limit)
        .exec(transaction)
        .await?)
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
/// Locks one durable rate row before refilling and reserving permits.
async fn load_rate_for_update(
    transaction: &mut db::DbTransaction<'_>,
    group: &str,
) -> Result<Option<TaskRateRow>, TaskError> {
    use crate::db::backend::RowLockExt as _;
    let table = DbTaskStore::rate_table();
    Ok(db::from(&table)
        .filter(table.group_name.eq(db::val(group.to_string())))
        .for_update()
        .first::<TaskRateRow>()
        .exec(transaction)
        .await?)
}

#[cfg(feature = "sqlite")]
/// Loads one durable rate row inside SQLite's serial write transaction.
async fn load_rate_for_update(
    transaction: &mut db::DbTransaction<'_>,
    group: &str,
) -> Result<Option<TaskRateRow>, TaskError> {
    let table = DbTaskStore::rate_table();
    Ok(db::from(&table)
        .filter(table.group_name.eq(db::val(group.to_string())))
        .first::<TaskRateRow>()
        .exec(transaction)
        .await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies quantized refill accounting retains elapsed time below one micro-token.
    #[test]
    fn slow_rate_refill_preserves_fractional_elapsed_time() -> Result<(), TaskError> {
        let rate = TaskRate::new(1, Duration::from_secs(365 * 24 * 60 * 60));
        let started = Utc::now();
        let now = started + chrono::Duration::seconds(1);
        let mut row = TaskRateRow {
            id: uuid::Uuid::now_v7(),
            group_name: "slow".into(),
            policy_fingerprint: "policy".into(),
            tokens_micros: 0,
            updated_at: started,
        };
        refill(&mut row.tokens_micros, &mut row.updated_at, rate, now)?;
        assert_eq!(row.tokens_micros, 0);
        assert_eq!(row.updated_at, started);
        let wake = next_permit(row.tokens_micros, rate, row.updated_at, now)
            .ok_or_else(|| TaskError::TaskExecutionError("missing permit deadline".into()))?;
        assert!(wake < rate.period());
        Ok(())
    }
}
