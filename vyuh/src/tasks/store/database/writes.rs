//! Batched task submission, lifecycle commits, inspection, and reassignment.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::{
    db,
    tasks::{
        IdempotencyRetention, ScheduledTaskWrite, TaskCommit, TaskFilter, TaskId, TaskLease,
        TaskOutcome, TaskReceipt, TaskRecord, TaskRetry, TaskRuntimeError, TaskStatus, TaskWrite,
    },
};

use super::{
    common::{DbTaskStore, add_time, apply_filter},
    model::{
        IdempotencyExpiryPatch, ResumePatch, TaskIdempotencyRow, TaskRow, TaskSchedulePatch,
        TaskScheduleRow,
    },
};

impl DbTaskStore {
    /// Stores one ordered task batch with transactionally coordinated idempotency keys.
    pub(super) async fn store_tasks_impl(
        &self,
        writes: Vec<TaskWrite>,
    ) -> Result<Vec<TaskReceipt>, TaskRuntimeError> {
        if writes.is_empty() {
            return Ok(Vec::new());
        }
        validate_write_lanes(self, &writes).await?;
        let mut transaction = self.pool.begin().await?;
        verify_policy(self, &mut transaction).await?;
        let now = statement_now(&mut transaction).await?;
        let receipts = self.store_writes_tx(&mut transaction, writes, now).await?;
        transaction.commit().await?;
        Ok(receipts)
    }

    /// Stores a schedule cursor and one task intent in one durable transaction.
    pub(super) async fn store_scheduled_impl(
        &self,
        scheduled: ScheduledTaskWrite,
    ) -> Result<Option<TaskReceipt>, TaskRuntimeError> {
        validate_write_lanes(self, std::slice::from_ref(&scheduled.write)).await?;
        let mut transaction = self.pool.begin().await?;
        verify_policy(self, &mut transaction).await?;
        let now = statement_now(&mut transaction).await?;
        let row = schedule_row(&scheduled.name, scheduled.occurrence, now)?;
        insert_schedule_if_missing(&mut transaction, &row).await?;
        let cursor = load_schedule_for_update(&mut transaction, &scheduled.name).await?;
        let cursor = cursor.ok_or_else(|| {
            TaskRuntimeError::TaskExecutionError("scheduled task cursor was not stored".into())
        })?;
        if cursor.last_submitted_at >= scheduled.occurrence {
            transaction.commit().await?;
            return Ok(None);
        }
        let mut receipts = self
            .store_writes_tx(&mut transaction, vec![scheduled.write], now)
            .await?;
        let receipt = receipts.pop().ok_or_else(|| {
            TaskRuntimeError::TaskExecutionError(
                "scheduled task submission omitted a receipt".into(),
            )
        })?;
        let cursor = now.max(scheduled.occurrence);
        update_schedule_cursor(&mut transaction, &scheduled.name, cursor, now).await?;
        transaction.commit().await?;
        Ok(Some(receipt))
    }

    /// Reads known durable schedule cursors in one bounded query.
    pub(super) async fn schedule_snapshot_impl(
        &self,
        names: &[String],
    ) -> Result<crate::tasks::TaskScheduleSnapshot, TaskRuntimeError> {
        let mut transaction = self.pool.begin().await?;
        verify_policy(self, &mut transaction).await?;
        let now = statement_now(&mut transaction).await?;
        if names.is_empty() {
            transaction.commit().await?;
            return Ok(crate::tasks::TaskScheduleSnapshot {
                now,
                cursors: HashMap::new(),
            });
        }
        let table = Self::schedule_table();
        let rows = db::from(&table)
            .filter(table.name.in_values(names.to_vec()))
            .all::<TaskScheduleRow>()
            .exec(&mut transaction)
            .await?;
        transaction.commit().await?;
        Ok(crate::tasks::TaskScheduleSnapshot {
            now,
            cursors: rows
                .into_iter()
                .map(|row| (row.name, row.last_submitted_at))
                .collect(),
        })
    }

    /// Resolves idempotency and inserts task rows inside an existing transaction.
    async fn store_writes_tx(
        &self,
        transaction: &mut db::DbTransaction<'_>,
        writes: Vec<TaskWrite>,
        now: DateTime<Utc>,
    ) -> Result<Vec<TaskReceipt>, TaskRuntimeError> {
        let (prepared, key_rows) = prepare_writes(writes, now)?;
        upsert_key_owners(transaction, &key_rows, self.batch_size).await?;
        let owners = load_key_owners(transaction, &key_rows).await?;
        let owners = replace_expired_owners(transaction, owners, &key_rows, now).await?;
        let (rows, receipts) = resolve_writes(prepared, owners)?;
        if !rows.is_empty() {
            let table = Self::table();
            db::from(&table)
                .insert_many(&rows)
                .batch_size(self.batch_size)
                .exec(transaction)
                .await?;
        }
        Ok(receipts)
    }

    /// Commits one bounded outcome batch in a shared transaction.
    #[allow(dead_code)]
    pub(super) async fn commit_outcomes_impl(
        &self,
        runner_id: &str,
        commits: &[TaskCommit],
    ) -> Result<(), TaskRuntimeError> {
        if commits.is_empty() {
            return Ok(());
        }
        let mut transaction = self.pool.begin().await?;
        verify_policy(self, &mut transaction).await?;
        let now = statement_now(&mut transaction).await?;
        let conf = self.runtime_conf.read().await.clone().ok_or_else(|| {
            TaskRuntimeError::InvalidConfig("task runtime was not initialized".into())
        })?;
        let (children, deliveries, waits) = self
            .commit_outcomes_tx(&mut transaction, runner_id, commits, &conf, now)
            .await?;
        finalize_workflow(
            &mut transaction,
            &children,
            deliveries,
            &waits,
            &conf,
            now,
            self.batch_size,
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Commits owned outcomes inside an already-authorized scheduler transaction.
    pub(super) async fn commit_outcomes_tx(
        &self,
        transaction: &mut db::DbTransaction<'_>,
        runner_id: &str,
        commits: &[TaskCommit],
        conf: &crate::tasks::TaskStoreConf,
        now: DateTime<Utc>,
    ) -> Result<
        (
            Vec<TaskRow>,
            Vec<(TaskId, String)>,
            Vec<super::all::WaitWrite>,
        ),
        TaskRuntimeError,
    > {
        crate::tasks::store::all::validate_turn(commits, self.batch_size, conf.max_all_children)?;
        if commits.is_empty() {
            return Ok((Vec::new(), Vec::new(), Vec::new()));
        }
        let mut outcomes = collect_outcomes(commits)?;
        let allowed = fenced_commits(transaction, runner_id, commits, conf, now).await?;
        let ids = outcomes.keys().map(|id| id.into_uuid()).collect::<Vec<_>>();
        let mut rows = load_owned_batch(transaction, ids, runner_id).await?;
        rows.retain(|row| allowed.contains(&TaskId::new(row.id)));
        let (children, conflicts, waits) =
            workflow::prepare_children(transaction, &rows, &outcomes, conf, now, self.batch_size)
                .await?;
        let deliveries = apply_owned_outcomes(&mut rows, &mut outcomes, &conflicts, conf, now)?;
        warn_unowned_outcomes(&outcomes, runner_id);
        update_idempotency_batch(transaction, &mut rows, conf, now).await?;
        batch_update_rows(transaction, &rows, self.batch_size).await?;
        Ok((children, deliveries, waits))
    }

    /// Resumes a suspended task only while it remains suspended.
    pub(super) async fn resume_impl(
        &self,
        id: TaskId,
        input: String,
    ) -> Result<bool, TaskRuntimeError> {
        crate::tasks::result::validate_resume(&input)?;
        let mut transaction = self.pool.begin().await?;
        verify_policy(self, &mut transaction).await?;
        let now = statement_now(&mut transaction).await?;
        let table = Self::table();
        let patch = ResumePatch {
            status: TaskStatus::Pending.as_i16(),
            resume_input: Some(input),
            ready_at: Some(now),
            updated_at: now,
        };
        let changed = db::from(&table)
            .filter(table.id.eq(db::val(id.into_uuid())))
            .filter(table.status.eq(db::val(TaskStatus::Suspended.as_i16())))
            .filter(table.cancelled.eq(db::val(false)))
            .update(&patch)
            .exec(&mut transaction)
            .await?;
        transaction.commit().await?;
        Ok(changed > 0)
    }

    /// Extends leases still owned by this runner and reports lost ownership.
    #[allow(dead_code)]
    pub(super) async fn renew_leases_impl(
        &self,
        runner_id: &str,
        leases: &[TaskLease],
    ) -> Result<Vec<TaskId>, TaskRuntimeError> {
        if leases.is_empty() {
            return Ok(Vec::new());
        }
        let mut transaction = self.pool.begin().await?;
        verify_policy(self, &mut transaction).await?;
        let now = statement_now(&mut transaction).await?;
        let conf = self.runtime_conf.read().await.clone().ok_or_else(|| {
            TaskRuntimeError::InvalidConfig("task runtime was not initialized".into())
        })?;
        let mut deliveries = Vec::new();
        let (lost, _) = self
            .renew_leases_tx(
                &mut transaction,
                runner_id,
                leases,
                &conf,
                now,
                &mut deliveries,
            )
            .await?;
        finalize_workflow(
            &mut transaction,
            &[],
            deliveries,
            &[],
            &conf,
            now,
            self.batch_size,
        )
        .await?;
        transaction.commit().await?;
        Ok(lost)
    }

    /// Renews still-owned leases inside an already-authorized scheduler transaction.
    pub(super) async fn renew_leases_tx(
        &self,
        transaction: &mut db::DbTransaction<'_>,
        runner_id: &str,
        leases: &[TaskLease],
        conf: &crate::tasks::TaskStoreConf,
        now: DateTime<Utc>,
        deliveries: &mut Vec<(TaskId, String)>,
    ) -> Result<(Vec<TaskId>, Vec<TaskId>), TaskRuntimeError> {
        if leases.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        let allowed = fenced_leases(transaction, runner_id, leases, conf, now).await?;
        let ids = leases
            .iter()
            .filter(|lease| allowed.contains(&lease.task_id))
            .map(|lease| lease.task_id.into_uuid())
            .collect::<Vec<_>>();
        let mut rows = super::cancellation::load_renewals(transaction, ids).await?;
        let lanes = leases
            .iter()
            .map(|lease| (lease.task_id, lease.lane))
            .collect::<std::collections::HashMap<_, _>>();
        rows.retain(|row| {
            lanes
                .get(&TaskId::new(row.id))
                .is_some_and(|lane| lane.as_str() == row.lane_name)
        });
        let cancelled = super::cancellation::classify_renewals(&mut rows, runner_id);
        self.cancel_renewals(transaction, &mut rows, conf, now, deliveries)
            .await?;
        for row in &mut rows {
            row.leased_until = Some(self.lease_until(row, now)?);
            row.updated_at = now;
        }
        batch_renew(transaction, &rows, self.batch_size).await?;
        let task_ids = leases.iter().map(|lease| lease.task_id).collect::<Vec<_>>();
        Ok((lost_ids(&task_ids, &rows), cancelled))
    }

    /// Reassigns non-running work after verifying the source lane has drained.
    pub(super) async fn reassign_lane_impl(
        &self,
        from: &str,
        to: &str,
    ) -> Result<u64, TaskRuntimeError> {
        require_runtime_lane(self, to).await?;
        let mut transaction = self.pool.begin().await?;
        verify_policy(self, &mut transaction).await?;
        let mut rows = load_active_lane_for_update(&mut transaction, from).await?;
        if rows
            .iter()
            .any(|row| row.status == TaskStatus::Running.as_i16())
        {
            transaction.rollback().await?;
            return Err(TaskRuntimeError::LaneBusy(from.into()));
        }
        let now = statement_now(&mut transaction).await?;
        for row in &mut rows {
            row.lane_name = to.into();
            row.updated_at = now;
        }
        let table = Self::table();
        let changed = if rows.is_empty() {
            0
        } else {
            db::from(&table)
                .update_many(&rows, (&table.lane_name, &table.updated_at))
                .batch_size(self.batch_size)
                .exec(&mut transaction)
                .await?
        };
        transaction.commit().await?;
        Ok(changed)
    }

    /// Reads one task without exposing its persistence row.
    pub(super) async fn get_task_impl(
        &self,
        id: TaskId,
    ) -> Result<Option<TaskRecord>, TaskRuntimeError> {
        let table = Self::table();
        let mut pool = self.pool.clone();
        db::from(&table)
            .filter(table.id.eq(db::val(id.into_uuid())))
            .first::<TaskRow>()
            .exec(&mut pool)
            .await?
            .map(TaskRecord::try_from)
            .transpose()
    }
}

/// Verifies lane-owner tokens only for renewals belonging to opt-in lanes.
async fn fenced_leases(
    transaction: &mut db::DbTransaction<'_>,
    runner_id: &str,
    leases: &[TaskLease],
    conf: &crate::tasks::TaskStoreConf,
    now: DateTime<Utc>,
) -> Result<std::collections::HashSet<TaskId>, TaskRuntimeError> {
    let mut allowed = std::collections::HashSet::with_capacity(leases.len());
    let mut lanes = std::collections::BTreeMap::new();
    for lease in leases {
        if !locked_lane(conf, lease.lane.as_str()) {
            allowed.insert(lease.task_id);
            continue;
        }
        insert_lane_token(
            &mut lanes,
            lease.lane.as_str(),
            lease.owner_token.as_deref(),
        )?;
    }
    let valid_lanes = valid_locked_lanes(transaction, runner_id, lanes, now).await?;
    for lease in leases {
        if valid_lanes.contains(lease.lane.as_str()) {
            allowed.insert(lease.task_id);
        }
    }
    Ok(allowed)
}

fn locked_lane(conf: &crate::tasks::TaskStoreConf, lane_name: &str) -> bool {
    conf.lanes
        .iter()
        .find(|lane| lane.lane().as_str() == lane_name)
        .is_some_and(|lane| lane.lane_lock().is_some())
}

/// Locks one scheduler turn's owned lanes in a globally stable order.
pub(super) async fn lock_lane_rows(
    transaction: &mut db::DbTransaction<'_>,
    lanes: std::collections::BTreeSet<crate::tasks::TaskLane>,
) -> Result<(), TaskRuntimeError> {
    for lane in lanes {
        if load_lane_lock(transaction, lane.as_str()).await?.is_none() {
            return Err(TaskRuntimeError::InvalidConfig(format!(
                "task lane lock '{lane}' is not initialized"
            )));
        }
    }
    Ok(())
}

/// Verifies lane-owner tokens only for commits belonging to opt-in lanes.
async fn fenced_commits(
    transaction: &mut db::DbTransaction<'_>,
    runner_id: &str,
    commits: &[TaskCommit],
    conf: &crate::tasks::TaskStoreConf,
    now: DateTime<Utc>,
) -> Result<std::collections::HashSet<TaskId>, TaskRuntimeError> {
    let mut allowed = std::collections::HashSet::with_capacity(commits.len());
    let mut lanes = std::collections::BTreeMap::new();
    for commit in commits {
        if !locked_lane(conf, commit.lane.as_str()) {
            allowed.insert(commit.task_id);
            continue;
        }
        insert_lane_token(
            &mut lanes,
            commit.lane.as_str(),
            commit.owner_token.as_deref(),
        )?;
    }
    let valid_lanes = valid_locked_lanes(transaction, runner_id, lanes, now).await?;
    for commit in commits {
        if valid_lanes.contains(commit.lane.as_str()) {
            allowed.insert(commit.task_id);
        }
    }
    Ok(allowed)
}

fn insert_lane_token<'a>(
    lanes: &mut std::collections::BTreeMap<&'a str, Option<&'a str>>,
    lane: &'a str,
    token: Option<&'a str>,
) -> Result<(), TaskRuntimeError> {
    if let Some(existing) = lanes.insert(lane, token)
        && existing != token
    {
        return Err(TaskRuntimeError::TaskExecutionError(format!(
            "lane '{lane}' has conflicting owner tokens in one scheduler turn"
        )));
    }
    Ok(())
}

async fn valid_locked_lanes<'a>(
    transaction: &mut db::DbTransaction<'_>,
    runner_id: &str,
    lanes: std::collections::BTreeMap<&'a str, Option<&'a str>>,
    now: DateTime<Utc>,
) -> Result<std::collections::HashSet<&'a str>, TaskRuntimeError> {
    let mut valid = std::collections::HashSet::with_capacity(lanes.len());
    for (lane, token) in lanes {
        if locked_commit(transaction, runner_id, lane, token, now).await? {
            valid.insert(lane);
        }
    }
    Ok(valid)
}

async fn locked_commit(
    transaction: &mut db::DbTransaction<'_>,
    runner_id: &str,
    lane: &str,
    token: Option<&str>,
    now: DateTime<Utc>,
) -> Result<bool, TaskRuntimeError> {
    let row = load_lane_lock(transaction, lane).await?;
    Ok(row.is_some_and(|row| {
        row.owner_id.as_deref() == Some(runner_id)
            && row.owner_token.as_deref() == token
            && row.leased_until.is_some_and(|deadline| deadline > now)
    }))
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
async fn load_lane_lock(
    transaction: &mut db::DbTransaction<'_>,
    lane: &str,
) -> Result<Option<super::model::TaskLaneLockRow>, TaskRuntimeError> {
    use crate::db::backend::RowLockExt as _;
    let table = DbTaskStore::lane_lock_table();
    Ok(db::from(&table)
        .filter(table.lane_name.eq(db::val(lane.to_string())))
        .for_update()
        .first::<super::model::TaskLaneLockRow>()
        .exec(transaction)
        .await?)
}

#[cfg(feature = "sqlite")]
async fn load_lane_lock(
    transaction: &mut db::DbTransaction<'_>,
    lane: &str,
) -> Result<Option<super::model::TaskLaneLockRow>, TaskRuntimeError> {
    let table = DbTaskStore::lane_lock_table();
    Ok(db::from(&table)
        .filter(table.lane_name.eq(db::val(lane.to_string())))
        .first::<super::model::TaskLaneLockRow>()
        .exec(transaction)
        .await?)
}

#[path = "writes_submissions.rs"]
mod submissions;
pub(super) use submissions::delete_expired_owners;
use submissions::*;

#[cfg(any(feature = "postgres", feature = "mysql"))]
/// Locks every still-owned task in one outcome batch before mutation.
async fn load_owned_batch(
    transaction: &mut db::DbTransaction<'_>,
    task_ids: Vec<uuid::Uuid>,
    runner_id: &str,
) -> Result<Vec<TaskRow>, TaskRuntimeError> {
    use crate::db::backend::RowLockExt as _;
    let table = DbTaskStore::table();
    Ok(db::from(&table)
        .filter(table.id.in_values(task_ids))
        .filter(table.locked_by.eq(db::val(Some(runner_id.to_owned()))))
        .filter(table.status.eq(db::val(TaskStatus::Running.as_i16())))
        .sort(table.id.asc())
        .for_update()
        .all::<TaskRow>()
        .exec(transaction)
        .await?)
}

#[cfg(feature = "sqlite")]
/// Loads every still-owned task inside SQLite's serial write transaction.
async fn load_owned_batch(
    transaction: &mut db::DbTransaction<'_>,
    task_ids: Vec<uuid::Uuid>,
    runner_id: &str,
) -> Result<Vec<TaskRow>, TaskRuntimeError> {
    let table = DbTaskStore::table();
    Ok(db::from(&table)
        .filter(table.id.in_values(task_ids))
        .filter(table.locked_by.eq(db::val(Some(runner_id.to_owned()))))
        .filter(table.status.eq(db::val(TaskStatus::Running.as_i16())))
        .all::<TaskRow>()
        .exec(transaction)
        .await?)
}

/// Rejects duplicate task IDs before locking rows for one commit batch.
fn collect_outcomes(
    commits: &[TaskCommit],
) -> Result<HashMap<TaskId, &TaskOutcome>, TaskRuntimeError> {
    let mut outcomes = std::collections::HashMap::with_capacity(commits.len());
    for commit in commits {
        if outcomes.insert(commit.task_id, &commit.outcome).is_some() {
            return Err(TaskRuntimeError::TaskExecutionError(format!(
                "task outcome batch contains duplicate task {}",
                commit.task_id
            )));
        }
    }
    Ok(outcomes)
}

#[path = "writes_accept.rs"]
mod acceptance;
use acceptance::apply_owned_outcomes;

fn warn_unowned_outcomes(outcomes: &HashMap<TaskId, &TaskOutcome>, runner_id: &str) {
    for task_id in outcomes.keys() {
        tracing::warn!(%task_id, %runner_id,
            "task outcome ignored because its lease is no longer owned");
    }
}

/// Applies one lifecycle transition using statement time.
pub(super) fn apply_outcome(
    row: &mut TaskRow,
    outcome: &TaskOutcome,
    retry: TaskRetry,
    now: DateTime<Utc>,
) -> Result<(), TaskRuntimeError> {
    let cancellation = row
        .cancelled
        .then(crate::tasks::store::workflow::cancellation);
    let outcome = cancellation.as_ref().unwrap_or(outcome);
    let preserve_resume = matches!(outcome, TaskOutcome::Retry { .. });
    match outcome {
        TaskOutcome::Complete => finish(row, TaskStatus::Succeeded, None, now),
        TaskOutcome::CompleteWith { output } => complete_output(row, output, now),
        TaskOutcome::Suspend { state }
        | TaskOutcome::Spawn { state, .. }
        | TaskOutcome::All { state, .. } => {
            row.last_result = None;
            row.step_attempts = 0;
            row.status = TaskStatus::Suspended.as_i16();
            row.state = Some(state.clone());
            row.ready_at = None;
        }
        TaskOutcome::Sleep { state, delay } => {
            row.last_result = None;
            row.step_attempts = 0;
            row.status = TaskStatus::Pending.as_i16();
            row.state = Some(state.clone());
            row.ready_at = Some(add_time(
                now,
                chrono_duration(*delay)?,
                "task sleep duration",
            )?);
        }
        TaskOutcome::Retry { error } => apply_retry(row, retry, error.clone(), now)?,
        TaskOutcome::Fail { error } => finish(row, TaskStatus::Failed, Some(error.clone()), now),
    }
    if !preserve_resume {
        row.resume_input = None;
    }
    row.locked_by = None;
    row.leased_until = None;
    row.updated_at = now;
    Ok(())
}

/// Persists validated output or contains invalid low-level output as a terminal failure.
fn complete_output(row: &mut TaskRow, output: &str, now: DateTime<Utc>) {
    match crate::tasks::result::validate_output(output) {
        Ok(()) => {
            row.status = TaskStatus::Succeeded.as_i16();
            row.ready_at = None;
            row.completed_at = Some(now);
            row.last_result = Some(crate::tasks::result::success(output));
        }
        Err(error) => finish(row, TaskStatus::Failed, Some(error.to_string()), now),
    }
}

/// Schedules another attempt or terminates a task at its retry bound.
fn apply_retry(
    row: &mut TaskRow,
    retry: TaskRetry,
    error: String,
    now: DateTime<Utc>,
) -> Result<(), TaskRuntimeError> {
    if retry.exhausted(row.step_attempts)? {
        finish(row, TaskStatus::Failed, Some(error), now);
    } else {
        row.status = TaskStatus::Pending.as_i16();
        row.last_result = Some(crate::tasks::result::failure(TaskId::new(row.id), error));
        row.ready_at = Some(add_time(
            now,
            chrono_duration(retry.delay(row.step_attempts)?)?,
            "task retry duration",
        )?);
    }
    Ok(())
}

fn lane_retry(
    lanes: &[crate::tasks::TaskLaneConf],
    name: &str,
) -> Result<TaskRetry, TaskRuntimeError> {
    lanes
        .iter()
        .find(|lane| lane.lane().as_str() == name)
        .map(crate::tasks::TaskLaneConf::retry_policy)
        .ok_or_else(|| TaskRuntimeError::UnknownLane(name.into()))
}

pub(super) fn finish(
    row: &mut TaskRow,
    status: TaskStatus,
    error: Option<String>,
    now: DateTime<Utc>,
) {
    row.status = status.as_i16();
    row.last_result = Some(match error {
        Some(error) => crate::tasks::result::failure(TaskId::new(row.id), error),
        None => crate::tasks::result::UNIT_RESULT.into(),
    });
    row.ready_at = None;
    row.completed_at = Some(now);
}

/// Releases or archives every terminal key through one set-based mutation.
pub(super) async fn update_idempotency_batch(
    transaction: &mut db::DbTransaction<'_>,
    rows: &mut [TaskRow],
    conf: &crate::tasks::TaskStoreConf,
    now: DateTime<Utc>,
) -> Result<(), TaskRuntimeError> {
    let table = DbTaskStore::idempotency_table();
    let (active, retained) = retention_groups(rows, conf)?;
    if !active.is_empty() {
        db::from(&table)
            .filter(table.task_id.in_values(active))
            .delete()
            .exec(transaction)
            .await?;
    }
    for (duration, ids) in retained {
        let expires = add_time(now, chrono_duration(duration)?, "idempotency retention")?;
        let patch = IdempotencyExpiryPatch {
            expires_at: Some(expires),
            updated_at: now,
        };
        db::from(&table)
            .filter(table.task_id.in_values(ids))
            .update(&patch)
            .exec(transaction)
            .await?;
        apply_expiry(rows, conf, duration, expires)?;
    }
    Ok(())
}

type RetentionGroup = (Duration, Vec<uuid::Uuid>);
type RetentionGroups = (Vec<uuid::Uuid>, Vec<RetentionGroup>);

/// Groups terminal idempotent rows by their finalized per-handler lane policy.
fn retention_groups(
    rows: &mut [TaskRow],
    conf: &crate::tasks::TaskStoreConf,
) -> Result<RetentionGroups, TaskRuntimeError> {
    let mut active = Vec::new();
    let mut retained: Vec<RetentionGroup> = Vec::new();
    for row in rows.iter_mut() {
        if !is_terminal_idempotent(row)? {
            continue;
        }
        match retention_for(row, conf)? {
            IdempotencyRetention::ActiveOnly => {
                row.idempotency_expires_at = None;
                active.push(row.id);
            }
            IdempotencyRetention::RetainFor(duration) => {
                if let Some((_, ids)) = retained.iter_mut().find(|(value, _)| *value == duration) {
                    ids.push(row.id);
                } else {
                    retained.push((duration, vec![row.id]));
                }
            }
        }
    }
    Ok((active, retained))
}

/// Reflects one set-based retained-key write in the loaded task rows.
fn apply_expiry(
    rows: &mut [TaskRow],
    conf: &crate::tasks::TaskStoreConf,
    duration: Duration,
    expires: DateTime<Utc>,
) -> Result<(), TaskRuntimeError> {
    for row in rows {
        if is_terminal_idempotent(row)?
            && matches!(retention_for(row, conf)?, IdempotencyRetention::RetainFor(value) if value == duration)
        {
            row.idempotency_expires_at = Some(expires);
        }
    }
    Ok(())
}

/// Returns the retention inherited by one idempotent task row.
fn retention_for(
    row: &TaskRow,
    conf: &crate::tasks::TaskStoreConf,
) -> Result<IdempotencyRetention, TaskRuntimeError> {
    conf.idempotency_for(&row.name).ok_or_else(|| {
        TaskRuntimeError::InvalidConfig(format!("task '{}' has no idempotency policy", row.name))
    })
}

fn is_terminal_idempotent(row: &TaskRow) -> Result<bool, TaskRuntimeError> {
    Ok(row.idempotency_key.is_some()
        && matches!(
            TaskStatus::from_i16(row.status)?,
            TaskStatus::Succeeded | TaskStatus::Failed
        ))
}

/// Persists lifecycle fields; lifetime attempts are written exclusively when claiming.
pub(super) async fn batch_update_rows(
    transaction: &mut db::DbTransaction<'_>,
    rows: &[TaskRow],
    batch_size: usize,
) -> Result<(), TaskRuntimeError> {
    if rows.is_empty() {
        return Ok(());
    }
    let table = DbTaskStore::table();
    let changed = db::from(&table)
        .update_many(
            rows,
            (
                &table.status,
                &table.state,
                &table.resume_input,
                &table.step_attempts,
                &table.last_result,
                &table.ready_at,
                &table.completed_at,
                &table.locked_by,
                &table.leased_until,
                &table.updated_at,
                &table.idempotency_expires_at,
            ),
        )
        .batch_size(batch_size)
        .exec(&mut *transaction)
        .await?;
    if changed != rows.len() as u64 {
        return Err(TaskRuntimeError::TaskExecutionError(
            "task outcome batch changed an unexpected number of rows".into(),
        ));
    }
    let terminal_waits = rows
        .iter()
        .filter(|row| {
            matches!(row.status, 3 | 4)
                && (row.waiting_children.is_some() || row.remaining_completions > 0)
        })
        .map(|row| row.id)
        .collect::<Vec<_>>();
    super::all::clear(transaction, &terminal_waits, batch_size).await?;
    Ok(())
}

/// Persists renewed ownership deadlines without touching lifecycle fields.
async fn batch_renew(
    transaction: &mut db::DbTransaction<'_>,
    rows: &[TaskRow],
    batch_size: usize,
) -> Result<(), TaskRuntimeError> {
    if rows.is_empty() {
        return Ok(());
    }
    let table = DbTaskStore::table();
    db::from(&table)
        .update_many(rows, (&table.leased_until, &table.updated_at))
        .batch_size(batch_size)
        .exec(transaction)
        .await?;
    Ok(())
}

fn lost_ids(requested: &[TaskId], rows: &[TaskRow]) -> Vec<TaskId> {
    requested
        .iter()
        .copied()
        .filter(|id| !rows.iter().any(|row| row.id == id.into_uuid()))
        .collect()
}

fn chrono_duration(duration: Duration) -> Result<chrono::Duration, TaskRuntimeError> {
    chrono::Duration::from_std(duration).map_err(|_| {
        TaskRuntimeError::TaskExecutionError("task duration is outside the supported range".into())
    })
}

/// Creates the initial schedule row without treating its first occurrence as complete.
fn schedule_row(
    name: &str,
    occurrence: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<TaskScheduleRow, TaskRuntimeError> {
    let before = occurrence
        .checked_sub_signed(chrono::Duration::nanoseconds(1))
        .ok_or_else(|| {
            TaskRuntimeError::TaskExecutionError("scheduled occurrence is outside range".into())
        })?;
    Ok(TaskScheduleRow {
        name: name.into(),
        last_submitted_at: before,
        updated_at: now,
    })
}

/// Creates a cursor row without overwriting a concurrent worker's position.
async fn insert_schedule_if_missing(
    transaction: &mut db::DbTransaction<'_>,
    row: &TaskScheduleRow,
) -> Result<(), TaskRuntimeError> {
    let table = DbTaskStore::schedule_table();
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    {
        use crate::db::backend::IgnoreConflictsExt as _;
        db::from(&table)
            .insert_many(std::slice::from_ref(row))
            .ignore_conflicts_on(&table.name)
            .exec(transaction)
            .await?;
    }
    #[cfg(all(feature = "mysql", not(any(feature = "postgres", feature = "sqlite"))))]
    {
        use crate::db::backend::IgnoreErrorsExt as _;
        db::from(&table)
            .insert_many(std::slice::from_ref(row))
            .ignore_errors()
            .exec(transaction)
            .await?;
    }
    Ok(())
}

/// Advances one locked cursor after its task submission has been resolved.
async fn update_schedule_cursor(
    transaction: &mut db::DbTransaction<'_>,
    name: &str,
    occurrence: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<(), TaskRuntimeError> {
    let table = DbTaskStore::schedule_table();
    let patch = TaskSchedulePatch {
        last_submitted_at: occurrence,
        updated_at: now,
    };
    db::from(&table)
        .filter(table.name.eq(db::val(name.to_owned())))
        .update(&patch)
        .exec(transaction)
        .await?;
    Ok(())
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
/// Locks one schedule cursor before deciding whether an occurrence was covered.
async fn load_schedule_for_update(
    transaction: &mut db::DbTransaction<'_>,
    name: &str,
) -> Result<Option<TaskScheduleRow>, TaskRuntimeError> {
    use crate::db::backend::RowLockExt as _;
    let table = DbTaskStore::schedule_table();
    db::from(&table)
        .filter(table.name.eq(db::val(name.to_owned())))
        .for_update()
        .first::<TaskScheduleRow>()
        .exec(transaction)
        .await
        .map_err(TaskRuntimeError::from)
}

#[cfg(feature = "sqlite")]
/// Reads one schedule cursor inside SQLite's serial write transaction.
async fn load_schedule_for_update(
    transaction: &mut db::DbTransaction<'_>,
    name: &str,
) -> Result<Option<TaskScheduleRow>, TaskRuntimeError> {
    let table = DbTaskStore::schedule_table();
    db::from(&table)
        .filter(table.name.eq(db::val(name.to_owned())))
        .first::<TaskScheduleRow>()
        .exec(transaction)
        .await
        .map_err(TaskRuntimeError::from)
}

async fn statement_now(
    transaction: &mut db::DbTransaction<'_>,
) -> Result<DateTime<Utc>, TaskRuntimeError> {
    use db::DbSession as _;
    Ok(transaction
        .fetch_scalar(db::Statement::raw("SELECT CURRENT_TIMESTAMP"))
        .await?)
}

#[cfg(all(test, any(feature = "postgres", feature = "sqlite")))]
#[path = "tests/writes.rs"]
mod tests;
#[path = "workflow.rs"]
mod workflow;
pub(super) use workflow::{finalize_workflow, queue_delivery};
#[path = "inspection.rs"]
mod inspection;
