//! Batched task submission, lifecycle commits, inspection, and reassignment.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::{
    db,
    tasks::{
        TaskCommit, TaskError, TaskFilter, TaskId, TaskIdempotency, TaskOutcome, TaskReceipt,
        TaskRecord, TaskRetry, TaskStatus, TaskWrite,
    },
};

use super::{
    common::{DbTaskStore, add_time, apply_filter},
    model::{IdempotencyExpiryPatch, ResumePatch, TaskIdempotencyRow, TaskRow},
};

impl DbTaskStore {
    /// Stores one ordered task batch with transactionally coordinated idempotency keys.
    pub(super) async fn store_tasks_impl(
        &self,
        writes: Vec<TaskWrite>,
    ) -> Result<Vec<TaskReceipt>, TaskError> {
        if writes.is_empty() {
            return Ok(Vec::new());
        }
        validate_write_lanes(self, &writes).await?;
        let mut transaction = self.pool.begin().await?;
        verify_policy(self, &mut transaction).await?;
        let now = statement_now(&mut transaction).await?;
        let (prepared, key_rows) = prepare_writes(writes, now)?;
        upsert_key_owners(&mut transaction, &key_rows, self.batch_size).await?;
        let owners = load_key_owners(&mut transaction, &key_rows).await?;
        let owners = replace_expired_owners(&mut transaction, owners, &key_rows, now).await?;
        let (rows, receipts) = resolve_writes(prepared, owners)?;
        if !rows.is_empty() {
            let table = Self::table();
            db::from(&table)
                .batch_insert(&rows)
                .batch_size(self.batch_size)
                .exec(&mut transaction)
                .await?;
        }
        transaction.commit().await?;
        Ok(receipts)
    }

    /// Commits one bounded outcome batch in a shared transaction.
    #[allow(dead_code)]
    pub(super) async fn commit_outcomes_impl(
        &self,
        runner_id: &str,
        commits: &[TaskCommit],
    ) -> Result<(), TaskError> {
        if commits.is_empty() {
            return Ok(());
        }
        let mut transaction = self.pool.begin().await?;
        verify_policy(self, &mut transaction).await?;
        let now = statement_now(&mut transaction).await?;
        let conf =
            self.runtime_conf.read().await.clone().ok_or_else(|| {
                TaskError::InvalidConfig("task runtime was not initialized".into())
            })?;
        self.commit_outcomes_tx(&mut transaction, runner_id, commits, &conf, now)
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Commits owned outcomes inside an already-authorized scheduler transaction.
    pub(super) async fn commit_outcomes_tx(
        &self,
        mut transaction: &mut db::DbTransaction<'_>,
        runner_id: &str,
        commits: &[TaskCommit],
        conf: &crate::tasks::TaskStoreConf,
        now: DateTime<Utc>,
    ) -> Result<(), TaskError> {
        if commits.is_empty() {
            return Ok(());
        }
        let mut outcomes = collect_outcomes(commits)?;
        let ids = outcomes.keys().map(|id| id.into_uuid()).collect::<Vec<_>>();
        let mut rows = load_owned_batch(&mut transaction, ids, runner_id).await?;
        apply_owned_outcomes(&mut rows, &mut outcomes, &conf.lanes, now)?;
        warn_unowned_outcomes(&outcomes, runner_id);
        update_idempotency_batch(&mut transaction, &mut rows, conf.idempotency, now).await?;
        batch_update_rows(&mut transaction, &rows, self.batch_size).await?;
        Ok(())
    }

    /// Resumes a suspended task only while it remains suspended.
    pub(super) async fn resume_impl(&self, id: TaskId, input: String) -> Result<bool, TaskError> {
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
        task_ids: &[TaskId],
    ) -> Result<Vec<TaskId>, TaskError> {
        if task_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut transaction = self.pool.begin().await?;
        verify_policy(self, &mut transaction).await?;
        let now = statement_now(&mut transaction).await?;
        let lost = self
            .renew_leases_tx(&mut transaction, runner_id, task_ids, now)
            .await?;
        transaction.commit().await?;
        Ok(lost)
    }

    /// Renews still-owned leases inside an already-authorized scheduler transaction.
    pub(super) async fn renew_leases_tx(
        &self,
        mut transaction: &mut db::DbTransaction<'_>,
        runner_id: &str,
        task_ids: &[TaskId],
        now: DateTime<Utc>,
    ) -> Result<Vec<TaskId>, TaskError> {
        if task_ids.is_empty() {
            return Ok(Vec::new());
        }
        let ids = task_ids.iter().map(|id| id.into_uuid()).collect::<Vec<_>>();
        let mut rows = load_owned_batch(&mut transaction, ids, runner_id).await?;
        for row in &mut rows {
            row.leased_until = Some(self.lease_until(row, now)?);
            row.updated_at = now;
        }
        batch_renew(&mut transaction, &rows, self.batch_size).await?;
        Ok(lost_ids(task_ids, &rows))
    }

    /// Reassigns non-running work after verifying the source lane has drained.
    pub(super) async fn reassign_lane_impl(&self, from: &str, to: &str) -> Result<u64, TaskError> {
        require_runtime_lane(self, to).await?;
        let mut transaction = self.pool.begin().await?;
        verify_policy(self, &mut transaction).await?;
        let mut rows = load_active_lane_for_update(&mut transaction, from).await?;
        if rows
            .iter()
            .any(|row| row.status == TaskStatus::Running.as_i16())
        {
            transaction.rollback().await?;
            return Err(TaskError::LaneBusy(from.into()));
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
                .batch_update(&rows, (&table.lane_name, &table.updated_at))
                .batch_size(self.batch_size)
                .exec(&mut transaction)
                .await?
        };
        transaction.commit().await?;
        Ok(changed)
    }

    /// Returns one deterministic page after applying typed filters.
    pub(super) async fn list_tasks_impl(
        &self,
        filter: TaskFilter,
    ) -> Result<crate::routes::Page<TaskRecord>, TaskError> {
        let table = Self::table();
        let mut pool = self.pool.clone();
        let page = apply_filter(db::from(&table), &table, &filter)
            .order_by(table.created_at.desc())
            .order_by(table.id.desc())
            .page::<TaskRow, _>(filter.page, filter.per_page, &mut pool)
            .await?;
        let records = page
            .items
            .into_iter()
            .map(TaskRecord::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(crate::routes::Page::new(
            records,
            page.total,
            page.page,
            page.per_page,
        ))
    }

    /// Reads one task without exposing its persistence row.
    pub(super) async fn get_task_impl(&self, id: TaskId) -> Result<Option<TaskRecord>, TaskError> {
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

type OwnerKey = (String, String);

struct PreparedWrite {
    write: TaskWrite,
    owner_key: Option<OwnerKey>,
}

/// Normalizes store-relative timestamps and unique key candidates before mutation.
fn prepare_writes(
    writes: Vec<TaskWrite>,
    now: DateTime<Utc>,
) -> Result<(Vec<PreparedWrite>, Vec<TaskIdempotencyRow>), TaskError> {
    let mut prepared = Vec::with_capacity(writes.len());
    let mut key_rows = Vec::new();
    let mut unique = HashSet::new();
    for mut write in writes {
        normalize_write(&mut write, now)?;
        let owner_key = write
            .record
            .idempotency_key
            .as_ref()
            .map(|key| (write.record.name.clone(), key.clone()));
        if let Some(key) = &owner_key
            && unique.insert(key.clone())
        {
            key_rows.push(key_row(&write.record, key.1.clone(), now)?);
        }
        prepared.push(PreparedWrite { write, owner_key });
    }
    Ok((prepared, key_rows))
}

fn normalize_write(write: &mut TaskWrite, now: DateTime<Utc>) -> Result<(), TaskError> {
    write.record.created_at = now;
    write.record.updated_at = now;
    write.record.ready_at = Some(match write.initial_delay {
        Some(delay) => add_time(now, chrono_duration(delay)?, "task initial delay")?,
        None => now,
    });
    Ok(())
}

/// Rejects low-level writes that bypass the typed client's lane validation.
async fn validate_write_lanes(store: &DbTaskStore, writes: &[TaskWrite]) -> Result<(), TaskError> {
    let conf = store.runtime_conf.read().await;
    for write in writes {
        let lane = &write.record.lane;
        let configured = conf
            .as_ref()
            .is_some_and(|conf| conf.lanes.iter().any(|entry| entry.lane().as_str() == lane));
        if !configured {
            return Err(TaskError::UnknownLane(lane.clone()));
        }
        let handler = &write.record.name;
        if !conf
            .as_ref()
            .is_some_and(|conf| conf.handlers.iter().any(|name| name == handler))
        {
            return Err(TaskError::TaskNotFound(handler.clone()));
        }
    }
    Ok(())
}

/// Validates one persisted lane name against initialized durable policy.
async fn require_runtime_lane(store: &DbTaskStore, lane: &str) -> Result<(), TaskError> {
    let configured = store
        .runtime_conf
        .read()
        .await
        .as_ref()
        .is_some_and(|conf| conf.lanes.iter().any(|entry| entry.lane().as_str() == lane));
    configured
        .then_some(())
        .ok_or_else(|| TaskError::UnknownLane(lane.into()))
}

/// Holds a shared durable policy lock across one mutation transaction.
async fn verify_policy(
    store: &DbTaskStore,
    transaction: &mut db::DbTransaction<'_>,
) -> Result<(), TaskError> {
    let conf = store
        .runtime_conf
        .read()
        .await
        .clone()
        .ok_or_else(|| TaskError::InvalidConfig("task runtime was not initialized".into()))?;
    super::runtime::verify_runtime_policy(transaction, &conf).await
}

/// Removes one bounded maintenance batch of expired idempotency archives.
pub(super) async fn delete_expired_owners(
    transaction: &mut db::DbTransaction<'_>,
    now: DateTime<Utc>,
    limit: usize,
) -> Result<(), TaskError> {
    let table = DbTaskStore::idempotency_table();
    let expired = db::from(&table)
        .filter(table.expires_at.lte(db::val(Some(now))))
        .order_by(table.expires_at.asc())
        .slice::<TaskIdempotencyRow>(0, limit)
        .exec(&mut *transaction)
        .await?;
    let ids = expired.into_iter().map(|row| row.id).collect::<Vec<_>>();
    if ids.is_empty() {
        return Ok(());
    }
    db::from(&table)
        .filter(table.id.in_values(ids))
        .delete()
        .exec(transaction)
        .await?;
    Ok(())
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
/// Locks all active source-lane rows before checking whether work drained.
async fn load_active_lane_for_update(
    transaction: &mut db::DbTransaction<'_>,
    lane: &str,
) -> Result<Vec<TaskRow>, TaskError> {
    use crate::db::backend::RowLockExt as _;
    load_active_lane_scope(lane)
        .for_update()
        .all::<TaskRow>()
        .exec(transaction)
        .await
        .map_err(TaskError::from)
}

#[cfg(feature = "sqlite")]
/// Loads active source-lane rows inside SQLite's serial write transaction.
async fn load_active_lane_for_update(
    transaction: &mut db::DbTransaction<'_>,
    lane: &str,
) -> Result<Vec<TaskRow>, TaskError> {
    load_active_lane_scope(lane)
        .all::<TaskRow>()
        .exec(transaction)
        .await
        .map_err(TaskError::from)
}

/// Selects every non-terminal row that must move as one lane lifecycle unit.
fn load_active_lane_scope(lane: &str) -> db::queries::QueryScope {
    let table = DbTaskStore::table();
    let statuses = [
        TaskStatus::Pending.as_i16(),
        TaskStatus::Running.as_i16(),
        TaskStatus::Suspended.as_i16(),
    ];
    db::from(&table)
        .filter(table.lane_name.eq(db::val(lane.to_string())))
        .filter(table.status.in_values(statuses))
}

/// Resolves every prepared intent against the owner snapshot in input order.
fn resolve_writes(
    prepared: Vec<PreparedWrite>,
    owners: Vec<TaskIdempotencyRow>,
) -> Result<(Vec<TaskRow>, Vec<TaskReceipt>), TaskError> {
    let owners = owners
        .into_iter()
        .map(|owner| ((owner.task_name.clone(), owner.key_value.clone()), owner))
        .collect::<HashMap<_, _>>();
    let mut rows = Vec::with_capacity(prepared.len());
    let mut receipts = Vec::with_capacity(prepared.len());
    for prepared in prepared {
        let task_id = prepared.write.record.id;
        let receipt = match prepared.owner_key {
            None => TaskReceipt::Queued(task_id),
            Some(key) => resolve_owner(&prepared.write, owners.get(&key), task_id)?,
        };
        if matches!(receipt, TaskReceipt::Queued(_)) {
            rows.push(TaskRow::from(prepared.write.record));
        }
        receipts.push(receipt);
    }
    Ok((rows, receipts))
}

fn resolve_owner(
    write: &TaskWrite,
    owner: Option<&TaskIdempotencyRow>,
    task_id: TaskId,
) -> Result<TaskReceipt, TaskError> {
    let owner = owner
        .ok_or_else(|| TaskError::TaskExecutionError("idempotency owner was not stored".into()))?;
    if owner.task_id == task_id.into_uuid() {
        return Ok(TaskReceipt::Queued(task_id));
    }
    if write.record.idempotency_fingerprint.as_deref() == Some(owner.fingerprint.as_str()) {
        Ok(TaskReceipt::Existing(TaskId::new(owner.task_id)))
    } else if write.ignore_conflicts {
        Ok(TaskReceipt::Ignored(TaskId::new(owner.task_id)))
    } else {
        Err(TaskError::IdempotencyConflict(TaskId::new(owner.task_id)))
    }
}

/// Builds the handler-scoped owner row for one keyed task intent.
fn key_row(
    record: &TaskRecord,
    key: String,
    now: DateTime<Utc>,
) -> Result<TaskIdempotencyRow, TaskError> {
    let fingerprint = record.idempotency_fingerprint.clone().ok_or_else(|| {
        TaskError::TaskExecutionError("idempotent task is missing its fingerprint".into())
    })?;
    Ok(TaskIdempotencyRow {
        id: uuid::Uuid::now_v7(),
        task_name: record.name.clone(),
        key_value: key,
        fingerprint,
        task_id: record.id.into_uuid(),
        expires_at: None,
        created_at: now,
        updated_at: now,
    })
}

/// Claims every previously unused key in one bounded write operation.
async fn upsert_key_owners(
    transaction: &mut db::DbTransaction<'_>,
    rows: &[TaskIdempotencyRow],
    batch_size: usize,
) -> Result<(), TaskError> {
    if rows.is_empty() {
        return Ok(());
    }
    let table = DbTaskStore::idempotency_table();
    db::from(&table)
        .batch_upsert(rows, (&table.task_name, &table.key_value))
        .update_only(&table.updated_at)
        .batch_size(batch_size)
        .exec(transaction)
        .await?;
    Ok(())
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
/// Locks all potentially matching key owners in one deterministic query.
async fn load_key_owners(
    transaction: &mut db::DbTransaction<'_>,
    rows: &[TaskIdempotencyRow],
) -> Result<Vec<TaskIdempotencyRow>, TaskError> {
    use crate::db::backend::RowLockExt as _;
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    owner_scope(rows)?
        .order_by(DbTaskStore::idempotency_table().id.asc())
        .for_update()
        .all::<TaskIdempotencyRow>()
        .exec(transaction)
        .await
        .map_err(TaskError::from)
}

#[cfg(feature = "sqlite")]
/// Loads all key owners inside SQLite's serial write transaction.
async fn load_key_owners(
    transaction: &mut db::DbTransaction<'_>,
    rows: &[TaskIdempotencyRow],
) -> Result<Vec<TaskIdempotencyRow>, TaskError> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    owner_scope(rows)?
        .order_by(DbTaskStore::idempotency_table().id.asc())
        .all::<TaskIdempotencyRow>()
        .exec(transaction)
        .await
        .map_err(TaskError::from)
}

fn owner_scope(rows: &[TaskIdempotencyRow]) -> Result<db::queries::QueryScope, TaskError> {
    let table = DbTaskStore::idempotency_table();
    let predicate = rows
        .iter()
        .map(|row| {
            table
                .task_name
                .eq(db::val(row.task_name.clone()))
                .and(table.key_value.eq(db::val(row.key_value.clone())))
        })
        .reduce(db::queries::Predicate::or)
        .ok_or_else(|| TaskError::TaskExecutionError("idempotency owner set is empty".into()))?;
    Ok(db::from(&table).filter(predicate))
}

/// Replaces expired owners in one delete and insert pair while rows are locked.
async fn replace_expired_owners(
    transaction: &mut db::DbTransaction<'_>,
    mut owners: Vec<TaskIdempotencyRow>,
    candidates: &[TaskIdempotencyRow],
    now: DateTime<Utc>,
) -> Result<Vec<TaskIdempotencyRow>, TaskError> {
    let expired = expired_replacements(&owners, candidates, now);
    if expired.is_empty() {
        return Ok(owners);
    }
    let expired_ids = expired.iter().map(|(id, _)| *id).collect::<Vec<_>>();
    let replaced_ids = expired_ids.iter().copied().collect::<HashSet<_>>();
    let replacements = expired
        .into_iter()
        .map(|(_, replacement)| replacement)
        .collect::<Vec<_>>();
    let table = DbTaskStore::idempotency_table();
    db::from(&table)
        .filter(table.id.in_values(expired_ids))
        .delete()
        .exec(&mut *transaction)
        .await?;
    db::from(&table)
        .batch_insert(&replacements)
        .exec(transaction)
        .await?;
    owners.retain(|owner| !replaced_ids.contains(&owner.id));
    owners.extend(replacements);
    Ok(owners)
}

fn expired_replacements(
    owners: &[TaskIdempotencyRow],
    candidates: &[TaskIdempotencyRow],
    now: DateTime<Utc>,
) -> Vec<(uuid::Uuid, TaskIdempotencyRow)> {
    owners
        .iter()
        .filter(|owner| owner.expires_at.is_some_and(|expiry| expiry <= now))
        .filter_map(|owner| {
            candidates
                .iter()
                .find(|candidate| {
                    candidate.task_name == owner.task_name && candidate.key_value == owner.key_value
                })
                .cloned()
                .map(|replacement| (owner.id, replacement))
        })
        .collect()
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
/// Locks every still-owned task in one outcome batch before mutation.
async fn load_owned_batch(
    transaction: &mut db::DbTransaction<'_>,
    task_ids: Vec<uuid::Uuid>,
    runner_id: &str,
) -> Result<Vec<TaskRow>, TaskError> {
    use crate::db::backend::RowLockExt as _;
    let table = DbTaskStore::table();
    Ok(db::from(&table)
        .filter(table.id.in_values(task_ids))
        .filter(table.locked_by.eq(db::val(Some(runner_id.to_owned()))))
        .filter(table.status.eq(db::val(TaskStatus::Running.as_i16())))
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
) -> Result<Vec<TaskRow>, TaskError> {
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
) -> Result<std::collections::HashMap<TaskId, TaskOutcome>, TaskError> {
    let mut outcomes = std::collections::HashMap::with_capacity(commits.len());
    for commit in commits {
        if outcomes
            .insert(commit.task_id, commit.outcome.clone())
            .is_some()
        {
            return Err(TaskError::TaskExecutionError(format!(
                "task outcome batch contains duplicate task {}",
                commit.task_id
            )));
        }
    }
    Ok(outcomes)
}

/// Applies only outcomes whose rows remain owned by the committing runner.
fn apply_owned_outcomes(
    rows: &mut [TaskRow],
    outcomes: &mut std::collections::HashMap<TaskId, TaskOutcome>,
    lanes: &[crate::tasks::TaskLaneConf],
    now: DateTime<Utc>,
) -> Result<(), TaskError> {
    for row in rows {
        let outcome = outcomes.remove(&TaskId::new(row.id)).ok_or_else(|| {
            TaskError::TaskExecutionError(format!("task {} has no pending outcome", row.id))
        })?;
        let retry = lane_retry(lanes, &row.lane_name)?;
        apply_outcome(row, outcome, retry, now)?;
    }
    Ok(())
}

fn warn_unowned_outcomes(
    outcomes: &std::collections::HashMap<TaskId, TaskOutcome>,
    runner_id: &str,
) {
    for task_id in outcomes.keys() {
        tracing::warn!(%task_id, %runner_id,
            "task outcome ignored because its lease is no longer owned");
    }
}

/// Applies one payload-free lifecycle transition using statement time.
fn apply_outcome(
    row: &mut TaskRow,
    outcome: TaskOutcome,
    retry: TaskRetry,
    now: DateTime<Utc>,
) -> Result<(), TaskError> {
    let preserve_resume = matches!(outcome, TaskOutcome::Retry { .. });
    match outcome {
        TaskOutcome::Complete => finish(row, TaskStatus::Succeeded, None, now),
        TaskOutcome::Suspend { state } => {
            row.status = TaskStatus::Suspended.as_i16();
            row.state = Some(state);
            row.ready_at = None;
        }
        TaskOutcome::Sleep { state, delay } => {
            row.status = TaskStatus::Pending.as_i16();
            row.state = Some(state);
            row.ready_at = Some(add_time(
                now,
                chrono_duration(delay)?,
                "task sleep duration",
            )?);
        }
        TaskOutcome::Retry { error } => apply_retry(row, retry, error, now)?,
        TaskOutcome::Fail { error } => finish(row, TaskStatus::Failed, Some(error), now),
    }
    if !preserve_resume {
        row.resume_input = None;
    }
    row.locked_by = None;
    row.leased_until = None;
    row.updated_at = now;
    Ok(())
}

/// Schedules another attempt or terminates a task at its retry bound.
fn apply_retry(
    row: &mut TaskRow,
    retry: TaskRetry,
    error: String,
    now: DateTime<Utc>,
) -> Result<(), TaskError> {
    if retry.exhausted(row.attempts)? {
        finish(row, TaskStatus::Failed, Some(error), now);
    } else {
        row.status = TaskStatus::Pending.as_i16();
        row.last_error = Some(error);
        row.ready_at = Some(add_time(
            now,
            chrono_duration(retry.delay(row.attempts)?)?,
            "task retry duration",
        )?);
    }
    Ok(())
}

fn lane_retry(lanes: &[crate::tasks::TaskLaneConf], name: &str) -> Result<TaskRetry, TaskError> {
    lanes
        .iter()
        .find(|lane| lane.lane().as_str() == name)
        .map(crate::tasks::TaskLaneConf::retry_policy)
        .ok_or_else(|| TaskError::UnknownLane(name.into()))
}

pub(super) fn finish(
    row: &mut TaskRow,
    status: TaskStatus,
    error: Option<String>,
    now: DateTime<Utc>,
) {
    row.status = status.as_i16();
    row.last_error = error;
    row.ready_at = None;
    row.completed_at = Some(now);
}

/// Releases or archives every terminal key through one set-based mutation.
pub(super) async fn update_idempotency_batch(
    transaction: &mut db::DbTransaction<'_>,
    rows: &mut [TaskRow],
    policy: TaskIdempotency,
    now: DateTime<Utc>,
) -> Result<(), TaskError> {
    let ids = terminal_idempotent_ids(rows)?;
    if ids.is_empty() {
        return Ok(());
    }
    let table = DbTaskStore::idempotency_table();
    match policy {
        TaskIdempotency::ActiveOnly => {
            db::from(&table)
                .filter(table.task_id.in_values(ids))
                .delete()
                .exec(transaction)
                .await?;
        }
        TaskIdempotency::RetainFor(duration) => {
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
            for row in rows {
                if is_terminal_idempotent(row)? {
                    row.idempotency_expires_at = Some(expires);
                }
            }
        }
    }
    Ok(())
}

fn terminal_idempotent_ids(rows: &[TaskRow]) -> Result<Vec<uuid::Uuid>, TaskError> {
    rows.iter()
        .filter_map(|row| match is_terminal_idempotent(row) {
            Ok(true) => Some(Ok(row.id)),
            Ok(false) => None,
            Err(error) => Some(Err(error)),
        })
        .collect()
}

fn is_terminal_idempotent(row: &TaskRow) -> Result<bool, TaskError> {
    Ok(row.idempotency_key.is_some()
        && matches!(
            TaskStatus::from_i16(row.status)?,
            TaskStatus::Succeeded | TaskStatus::Failed
        ))
}

/// Persists all lifecycle fields through one bounded batch update.
pub(super) async fn batch_update_rows(
    transaction: &mut db::DbTransaction<'_>,
    rows: &[TaskRow],
    batch_size: usize,
) -> Result<(), TaskError> {
    if rows.is_empty() {
        return Ok(());
    }
    let table = DbTaskStore::table();
    let changed = db::from(&table)
        .batch_update(
            rows,
            (
                &table.status,
                &table.state,
                &table.resume_input,
                &table.attempts,
                &table.last_error,
                &table.ready_at,
                &table.completed_at,
                &table.locked_by,
                &table.leased_until,
                &table.updated_at,
                &table.idempotency_expires_at,
            ),
        )
        .batch_size(batch_size)
        .exec(transaction)
        .await?;
    if changed != rows.len() as u64 {
        return Err(TaskError::TaskExecutionError(
            "task outcome batch changed an unexpected number of rows".into(),
        ));
    }
    Ok(())
}

/// Persists renewed ownership deadlines without touching lifecycle fields.
async fn batch_renew(
    transaction: &mut db::DbTransaction<'_>,
    rows: &[TaskRow],
    batch_size: usize,
) -> Result<(), TaskError> {
    if rows.is_empty() {
        return Ok(());
    }
    let table = DbTaskStore::table();
    db::from(&table)
        .batch_update(rows, (&table.leased_until, &table.updated_at))
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

fn chrono_duration(duration: Duration) -> Result<chrono::Duration, TaskError> {
    chrono::Duration::from_std(duration).map_err(|_| {
        TaskError::TaskExecutionError("task duration is outside the supported range".into())
    })
}

async fn statement_now(
    transaction: &mut db::DbTransaction<'_>,
) -> Result<DateTime<Utc>, TaskError> {
    use db::DbSession as _;
    Ok(transaction
        .fetch_scalar(db::Statement::raw("SELECT CURRENT_TIMESTAMP"))
        .await?)
}
