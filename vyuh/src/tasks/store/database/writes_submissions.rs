//! Batched submission preparation and idempotency arbitration.

use super::*;

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
pub(super) async fn validate_write_lanes(
    store: &DbTaskStore,
    writes: &[TaskWrite],
) -> Result<(), TaskRuntimeError> {
    let conf = store.runtime_conf.read().await;
    for write in writes {
        let lane = &write.record.lane;
        let configured = conf
            .as_ref()
            .is_some_and(|(conf, _)| conf.lanes.iter().any(|entry| entry.lane().as_str() == lane));
        if !configured {
            return Err(TaskRuntimeError::UnknownLane(lane.clone()));
        }
        let handler = &write.record.name;
        if !conf
            .as_ref()
            .is_some_and(|(conf, _)| conf.handlers.iter().any(|(name, _)| name == handler))
        {
            return Err(TaskRuntimeError::TaskNotFound(handler.clone()));
        }
    }
    Ok(())
}

/// Validates one persisted lane name against initialized durable policy.
pub(super) async fn require_runtime_lane(
    store: &DbTaskStore,
    lane: &str,
) -> Result<(), TaskRuntimeError> {
    let configured = store
        .runtime_conf
        .read()
        .await
        .as_ref()
        .is_some_and(|(conf, _)| conf.lanes.iter().any(|entry| entry.lane().as_str() == lane));
    configured
        .then_some(())
        .ok_or_else(|| TaskRuntimeError::UnknownLane(lane.into()))
}

/// Holds a shared durable policy lock across one mutation transaction.
pub(super) async fn verify_policy(
    store: &DbTaskStore,
    transaction: &mut db::DbTransaction<'_>,
) -> Result<DateTime<Utc>, TaskRuntimeError> {
    let (_, fingerprint) = store.runtime_conf.read().await.clone().ok_or_else(|| {
        TaskRuntimeError::InvalidConfig("task runtime was not initialized".into())
    })?;
    super::super::runtime::verify_runtime_policy(transaction, &fingerprint).await
}

/// Removes one bounded maintenance batch of expired idempotency archives.
pub(in super::super) async fn delete_expired_owners(
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
pub(in super::super) async fn delete_expired_owner_ids(
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
