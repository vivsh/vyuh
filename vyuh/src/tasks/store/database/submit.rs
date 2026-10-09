//! Batched submission preparation and idempotency arbitration.
use super::store::DbTaskStore;

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};

use crate::{
    db,
    tasks::{
        ScheduledTaskWrite, TaskId, TaskReceipt, TaskRecord, TaskRuntimeError, TaskStatus,
        TaskWrite,
    },
};

use super::{
    model::{TaskIdempotencyRow, TaskRow, TaskSchedulePatch, TaskScheduleRow},
    queries::add_time,
};

use super::commit::chrono_duration;
use super::queries::statement_now;

#[cfg(all(test, any(feature = "postgres", feature = "sqlite")))]
#[path = "tests/submit.rs"]
mod tests;

impl DbTaskStore {
    /// Executes an independent submission or control transaction.
    pub(super) async fn store_tasks_impl(
        &self,
        writes: Vec<TaskWrite>,
    ) -> Result<Vec<TaskReceipt>, TaskRuntimeError> {
        if writes.is_empty() {
            return Ok(Vec::new());
        }
        validate_write_lanes(self, &writes)?;
        let mut transaction = self.pool.begin().await?;
        let now = statement_now(&mut transaction).await?;
        let (receipts, _) = self
            .store_writes_tx(&mut transaction, writes, now, None)
            .await?;
        transaction.commit().await?;
        Ok(receipts)
    }
    /// Executes an independent submission or control transaction.
    pub(super) async fn store_scheduled_impl(
        &self,
        scheduled: ScheduledTaskWrite,
    ) -> Result<Option<TaskReceipt>, TaskRuntimeError> {
        validate_write_lanes(self, std::slice::from_ref(&scheduled.write))?;
        let mut transaction = self.pool.begin().await?;
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
        let (mut receipts, _) = self
            .store_writes_tx(&mut transaction, vec![scheduled.write], now, None)
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

    /// Shares insertion/idempotency work with ordinary submission, publishing only after commit.
    pub(super) async fn store_admitted_impl(
        &self,
        writes: Vec<TaskWrite>,
        node: &uuid::Uuid,
        limit: usize,
    ) -> Result<(Vec<TaskReceipt>, Vec<TaskRecord>), TaskRuntimeError> {
        validate_write_lanes(self, &writes)?;
        let mut transaction = self.pool.begin().await?;
        let now = statement_now(&mut transaction).await?;
        let result = self
            .store_writes_tx(&mut transaction, writes, now, Some((*node, limit)))
            .await?;
        transaction.commit().await?;
        Ok(result)
    }
    /// Executes an independent submission or control transaction.
    pub(super) async fn schedule_snapshot_impl(
        &self,
        names: &[String],
    ) -> Result<crate::tasks::TaskScheduleSnapshot, TaskRuntimeError> {
        let mut transaction = self.pool.begin().await?;
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
}

impl DbTaskStore {
    /// Resolves idempotency and inserts task rows inside an existing transaction.
    async fn store_writes_tx(
        &self,
        transaction: &mut db::DbTransaction<'_>,
        writes: Vec<TaskWrite>,
        now: DateTime<Utc>,
        delivery: Option<(uuid::Uuid, usize)>,
    ) -> Result<(Vec<TaskReceipt>, Vec<TaskRecord>), TaskRuntimeError> {
        let (prepared, key_rows) = prepare_writes(writes, now)?;
        upsert_key_owners(transaction, &key_rows, super::queries::WRITE_CHUNK_SIZE).await?;
        let owners = load_key_owners(transaction, &key_rows).await?;
        let owners = replace_expired_owners(transaction, owners, &key_rows, now).await?;
        let (mut rows, receipts) = resolve_writes(prepared, owners)?;
        let tasks = self.admit_rows(&mut rows, delivery, now)?;
        if !rows.is_empty() {
            #[cfg(feature = "postgres")]
            super::notify::insert(
                transaction,
                &rows,
                super::queries::WRITE_CHUNK_SIZE,
                delivery.map(|(node, _)| node),
            )
            .await?;
            #[cfg(not(feature = "postgres"))]
            {
                let table = Self::table();
                db::from(&table)
                    .insert_many(&rows)
                    .batch_size(super::queries::WRITE_CHUNK_SIZE)
                    .exec(transaction)
                    .await?;
            }
        }
        Ok((receipts, tasks))
    }

    /// Assigns only newly inserted, immediately eligible rows to reserved local capacity.
    pub(super) fn admit_rows(
        &self,
        rows: &mut [TaskRow],
        delivery: Option<(uuid::Uuid, usize)>,
        now: DateTime<Utc>,
    ) -> Result<Vec<TaskRecord>, TaskRuntimeError> {
        let Some((node, mut remaining)) = delivery else {
            return Ok(Vec::new());
        };
        let mut tasks = Vec::new();
        for row in rows {
            if remaining == 0 {
                break;
            }
            let mut record = TaskRecord::try_from(row.clone())?;
            if record.node_id.is_some()
                || (record.parent_id.is_some()
                    && !crate::tasks::store::transitions::sticky(&record, &self.runtime_conf))
                || !crate::tasks::store::transitions::direct(&record, &self.runtime_conf, now)
            {
                continue;
            }
            crate::tasks::store::transitions::admit(&mut record, node, now)?;
            *row = TaskRow::from(record.clone());
            tasks.push(record);
            remaining -= 1;
        }
        Ok(tasks)
    }
}

pub(super) type OwnerKey = (String, String);

pub(super) struct PreparedWrite {
    pub(super) write: TaskWrite,
    pub(super) owner_key: Option<OwnerKey>,
}

/// Normalizes store-relative timestamps and unique key candidates before mutation.
pub(super) fn prepare_writes(
    writes: Vec<TaskWrite>,
    now: DateTime<Utc>,
) -> Result<(Vec<PreparedWrite>, Vec<TaskIdempotencyRow>), TaskRuntimeError> {
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

pub(super) fn normalize_write(
    write: &mut TaskWrite,
    now: DateTime<Utc>,
) -> Result<(), TaskRuntimeError> {
    write.record.created_at = now;
    write.record.updated_at = now;
    write.record.ready_at = Some(match write.initial_delay {
        Some(delay) => add_time(now, chrono_duration(delay)?, "task initial delay")?,
        None => now,
    });
    Ok(())
}

/// Rejects low-level writes that bypass the typed client's lane validation.
pub(super) fn validate_write_lanes(
    store: &DbTaskStore,
    writes: &[TaskWrite],
) -> Result<(), TaskRuntimeError> {
    let conf = &store.runtime_conf;
    for write in writes {
        let lane = &write.record.lane;
        let configured = conf.lanes.iter().any(|entry| entry.lane().as_str() == lane);
        if !configured {
            return Err(TaskRuntimeError::UnknownLane(lane.clone()));
        }
        let handler = &write.record.name;
        if !conf.handlers.iter().any(|(name, _)| name == handler) {
            return Err(TaskRuntimeError::TaskNotFound(handler.clone()));
        }
    }
    Ok(())
}

/// Validates one persisted lane name against initialized durable policy.
pub(super) fn require_runtime_lane(
    store: &DbTaskStore,
    lane: &str,
) -> Result<(), TaskRuntimeError> {
    let configured = store
        .runtime_conf
        .lanes
        .iter()
        .any(|entry| entry.lane().as_str() == lane);
    configured
        .then_some(())
        .ok_or_else(|| TaskRuntimeError::UnknownLane(lane.into()))
}

/// Removes one bounded maintenance batch of expired idempotency archives.
pub(super) async fn delete_expired_owners(
    transaction: &mut db::DbTransaction<'_>,
    now: DateTime<Utc>,
    limit: usize,
) -> Result<(), TaskRuntimeError> {
    let table = DbTaskStore::idempotency_table();
    let expired = db::from(&table)
        .filter(table.expires_at.lte(db::val(Some(now))))
        .sort(table.expires_at.asc())
        .slice::<TaskIdempotencyRow>(0, limit)
        .exec(&mut *transaction)
        .await?;
    let ids = expired.into_iter().map(|row| row.id).collect::<Vec<_>>();
    delete_expired_owner_ids(transaction, ids, now).await
}

/// Rechecks expiry at deletion so a stale sweep cannot remove a renewed reservation.
pub(super) async fn delete_expired_owner_ids(
    transaction: &mut db::DbTransaction<'_>,
    ids: Vec<uuid::Uuid>,
    now: DateTime<Utc>,
) -> Result<(), TaskRuntimeError> {
    if ids.is_empty() {
        return Ok(());
    }
    let table = DbTaskStore::idempotency_table();
    db::from(&table)
        .filter(table.id.in_values(ids))
        .filter(table.expires_at.lte(db::val(Some(now))))
        .delete()
        .exec(transaction)
        .await?;
    Ok(())
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
/// Locks all active source-lane rows before checking whether work drained.
pub(super) async fn load_active_lane_for_update(
    transaction: &mut db::DbTransaction<'_>,
    lane: &str,
) -> Result<Vec<TaskRow>, TaskRuntimeError> {
    use crate::db::backend::RowLockExt as _;
    load_active_lane_scope(lane)
        .for_update()
        .all::<TaskRow>()
        .exec(transaction)
        .await
        .map_err(TaskRuntimeError::from)
}

#[cfg(feature = "sqlite")]
/// Loads active source-lane rows inside SQLite's serial write transaction.
pub(super) async fn load_active_lane_for_update(
    transaction: &mut db::DbTransaction<'_>,
    lane: &str,
) -> Result<Vec<TaskRow>, TaskRuntimeError> {
    load_active_lane_scope(lane)
        .all::<TaskRow>()
        .exec(transaction)
        .await
        .map_err(TaskRuntimeError::from)
}

/// Selects every non-terminal row that must move as one lane lifecycle unit.
pub(super) fn load_active_lane_scope(lane: &str) -> db::queries::QueryScope {
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
pub(super) fn resolve_writes(
    prepared: Vec<PreparedWrite>,
    owners: Vec<TaskIdempotencyRow>,
) -> Result<(Vec<TaskRow>, Vec<TaskReceipt>), TaskRuntimeError> {
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

pub(super) fn resolve_owner(
    write: &TaskWrite,
    owner: Option<&TaskIdempotencyRow>,
    task_id: TaskId,
) -> Result<TaskReceipt, TaskRuntimeError> {
    let owner = owner.ok_or_else(|| {
        TaskRuntimeError::TaskExecutionError("idempotency owner was not stored".into())
    })?;
    if owner.task_id == task_id.into_uuid() {
        return Ok(TaskReceipt::Queued(task_id));
    }
    if write.record.idempotency_fingerprint.as_deref() == Some(owner.fingerprint.as_str()) {
        Ok(TaskReceipt::Existing(TaskId::new(owner.task_id)))
    } else if write.ignore_conflicts {
        Ok(TaskReceipt::Ignored(TaskId::new(owner.task_id)))
    } else {
        Err(TaskRuntimeError::IdempotencyConflict(TaskId::new(
            owner.task_id,
        )))
    }
}

/// Builds the handler-scoped owner row for one keyed task intent.
pub(super) fn key_row(
    record: &TaskRecord,
    key: String,
    now: DateTime<Utc>,
) -> Result<TaskIdempotencyRow, TaskRuntimeError> {
    let fingerprint = record.idempotency_fingerprint.clone().ok_or_else(|| {
        TaskRuntimeError::TaskExecutionError("idempotent task is missing its fingerprint".into())
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
pub(super) async fn upsert_key_owners(
    transaction: &mut db::DbTransaction<'_>,
    rows: &[TaskIdempotencyRow],
    batch_size: usize,
) -> Result<(), TaskRuntimeError> {
    if rows.is_empty() {
        return Ok(());
    }
    let table = DbTaskStore::idempotency_table();
    db::from(&table)
        .upsert_many(rows, (&table.task_name, &table.key_value))
        .update_only(&table.updated_at)
        .batch_size(batch_size)
        .exec(transaction)
        .await?;
    Ok(())
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
/// Locks all potentially matching key owners in one deterministic query.
pub(super) async fn load_key_owners(
    transaction: &mut db::DbTransaction<'_>,
    rows: &[TaskIdempotencyRow],
) -> Result<Vec<TaskIdempotencyRow>, TaskRuntimeError> {
    use crate::db::backend::RowLockExt as _;
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    owner_scope(rows)?
        .sort(DbTaskStore::idempotency_table().id.asc())
        .for_update()
        .all::<TaskIdempotencyRow>()
        .exec(transaction)
        .await
        .map_err(TaskRuntimeError::from)
}

#[cfg(feature = "sqlite")]
/// Loads all key owners inside SQLite's serial write transaction.
pub(super) async fn load_key_owners(
    transaction: &mut db::DbTransaction<'_>,
    rows: &[TaskIdempotencyRow],
) -> Result<Vec<TaskIdempotencyRow>, TaskRuntimeError> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    owner_scope(rows)?
        .sort(DbTaskStore::idempotency_table().id.asc())
        .all::<TaskIdempotencyRow>()
        .exec(transaction)
        .await
        .map_err(TaskRuntimeError::from)
}

pub(super) fn owner_scope(
    rows: &[TaskIdempotencyRow],
) -> Result<db::queries::QueryScope, TaskRuntimeError> {
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
        .ok_or_else(|| {
            TaskRuntimeError::TaskExecutionError("idempotency owner set is empty".into())
        })?;
    Ok(db::from(&table).filter(predicate))
}

/// Replaces expired owners in one delete and insert pair while rows are locked.
pub(super) async fn replace_expired_owners(
    transaction: &mut db::DbTransaction<'_>,
    mut owners: Vec<TaskIdempotencyRow>,
    candidates: &[TaskIdempotencyRow],
    now: DateTime<Utc>,
) -> Result<Vec<TaskIdempotencyRow>, TaskRuntimeError> {
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
        .insert_many(&replacements)
        .exec(transaction)
        .await?;
    owners.retain(|owner| !replaced_ids.contains(&owner.id));
    owners.extend(replacements);
    Ok(owners)
}

pub(super) fn expired_replacements(
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
