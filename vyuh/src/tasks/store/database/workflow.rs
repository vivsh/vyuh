//! Bounded workflow preparation and post-claim finalization in the store transaction.

use super::*;
use crate::db::Model as _;

/// A narrow, operation-local parent write; never loads or rewrites checkpoint data.
#[derive(Debug, Clone, db::Model)]
#[table(name = "vyuh_tasks")]
struct ParentWrite {
    #[column(primary_key)]
    id: uuid::Uuid,
    lane_name: String,
    status: i16,
    resume_input: Option<String>,
    ready_at: Option<DateTime<Utc>>,
    updated_at: DateTime<Utc>,
}

/// Reserves child keys in bulk; conflicts fail their parent without aborting siblings.
pub(super) async fn prepare_children(
    tx: &mut db::DbTransaction<'_>,
    rows: &[TaskRow],
    outcomes: &HashMap<TaskId, &TaskOutcome>,
    conf: &crate::tasks::TaskStoreConf,
    now: DateTime<Utc>,
    batch_size: usize,
) -> Result<(Vec<TaskRow>, HashSet<TaskId>), TaskRuntimeError> {
    if !outcomes
        .values()
        .any(|outcome| matches!(outcome, TaskOutcome::Spawn { .. }))
    {
        return Ok((Vec::new(), HashSet::new()));
    }
    let mut writes = Vec::new();
    for row in rows {
        if let Some(child) = prepare_child(row, outcomes, conf)? {
            writes.push(child);
        }
    }
    if writes.is_empty() {
        return Ok((Vec::new(), HashSet::new()));
    }
    let (prepared, keys) = prepare_writes(writes, now)?;
    upsert_key_owners(tx, &keys, batch_size).await?;
    let owners = load_key_owners(tx, &keys).await?;
    let owners = replace_expired_owners(tx, owners, &keys, now).await?;
    resolve_children(prepared, owners)
}

/// Validates one fenced spawn and derives lineage before reserving any child identity.
fn prepare_child(
    row: &TaskRow,
    outcomes: &HashMap<TaskId, &TaskOutcome>,
    conf: &crate::tasks::TaskStoreConf,
) -> Result<Option<crate::tasks::TaskWrite>, TaskRuntimeError> {
    if row.cancelled {
        return Ok(None);
    }
    let Some(outcome @ TaskOutcome::Spawn { child, .. }) =
        outcomes.get(&TaskId::new(row.id)).copied()
    else {
        return Ok(None);
    };
    let kind = crate::tasks::TaskKind::from_i16(row.kind)?;
    if crate::tasks::store::workflow::capability_error(kind, outcome, &conf.handlers).is_some() {
        return Ok(None);
    }
    if child.ignore_conflicts {
        return Err(TaskRuntimeError::InvalidOptions(
            "spawn cannot ignore conflicts".into(),
        ));
    }
    lane_retry(&conf.lanes, &child.record.lane)?;
    let mut child = child.clone();
    child.record.parent_id = Some(TaskId::new(row.id));
    child.record.root_id = Some(TaskId::new(row.root_id.unwrap_or(row.id)));
    child.ignore_conflicts = true;
    Ok(Some(child))
}

/// Accepts only newly allocated child identities, never idempotent existing receipts.
fn resolve_children(
    prepared: Vec<PreparedWrite>,
    owners: Vec<TaskIdempotencyRow>,
) -> Result<(Vec<TaskRow>, HashSet<TaskId>), TaskRuntimeError> {
    let owners = owners
        .into_iter()
        .map(|owner| ((owner.task_name.clone(), owner.key_value.clone()), owner))
        .collect::<HashMap<_, _>>();
    let mut children = Vec::new();
    let mut conflicts = HashSet::new();
    for prepared in prepared {
        let write = prepared.write;
        let receipt = match prepared.owner_key {
            None => TaskReceipt::Queued(write.record.id),
            Some(key) => resolve_owner(&write, owners.get(&key), write.record.id)?,
        };
        if matches!(receipt, TaskReceipt::Queued(_)) {
            children.push(TaskRow::from(write.record));
        } else if let Some(parent) = write.record.parent_id {
            conflicts.insert(parent);
        }
    }
    Ok((children, conflicts))
}

/// Encodes a terminal accepted child transition for delivery after claim selection.
pub(in super::super) fn queue_delivery(
    row: &TaskRow,
    deliveries: &mut Vec<(TaskId, String)>,
) -> Result<(), TaskRuntimeError> {
    if let Some(parent) = row.parent_id
        && let Some(result) = crate::tasks::store::workflow::terminal_result(
            TaskStatus::from_i16(row.status)?,
            row.last_result.as_deref(),
        )?
    {
        deliveries.push((TaskId::new(parent), result.to_owned()));
    }
    Ok(())
}

/// Inserts children and resumes suspended parents in the originating transaction.
pub(in super::super) async fn finalize_workflow(
    tx: &mut db::DbTransaction<'_>,
    children: &[TaskRow],
    deliveries: Vec<(TaskId, String)>,
    conf: &crate::tasks::TaskStoreConf,
    now: DateTime<Utc>,
    batch_size: usize,
) -> Result<Vec<crate::tasks::TaskLane>, TaskRuntimeError> {
    let mut lanes = children
        .iter()
        .map(|child| child.lane_name.clone())
        .collect::<HashSet<_>>();
    if !children.is_empty() {
        db::from(&DbTaskStore::table())
            .insert_many(children)
            .batch_size(batch_size)
            .exec(&mut *tx)
            .await?;
    }
    resume_parents(tx, deliveries, now, batch_size, &mut lanes).await?;
    Ok(conf
        .lanes
        .iter()
        .filter_map(|lane| lanes.contains(lane.lane().as_str()).then_some(lane.lane()))
        .collect())
}

/// Updates only eligible parents using deterministic locks and narrow batched writes.
async fn resume_parents(
    tx: &mut db::DbTransaction<'_>,
    deliveries: Vec<(TaskId, String)>,
    now: DateTime<Utc>,
    batch_size: usize,
    lanes: &mut HashSet<String>,
) -> Result<(), TaskRuntimeError> {
    let mut results = deliveries
        .into_iter()
        .collect::<std::collections::BTreeMap<_, _>>();
    let ids = results.keys().map(|id| id.into_uuid()).collect::<Vec<_>>();
    for chunk in ids.chunks(batch_size) {
        let mut parents = load_parents(tx, chunk).await?;
        for parent in &mut parents {
            parent.status = TaskStatus::Pending.as_i16();
            parent.resume_input = results.remove(&TaskId::new(parent.id));
            parent.ready_at = Some(now);
            parent.updated_at = now;
            lanes.insert(parent.lane_name.clone());
        }
        if !parents.is_empty() {
            let table = ParentWrite::table();
            db::from(&table)
                .update_many(
                    &parents,
                    (
                        &table.status,
                        &table.resume_input,
                        &table.ready_at,
                        &table.updated_at,
                    ),
                )
                .batch_size(batch_size)
                .exec(&mut *tx)
                .await?;
        }
    }
    Ok(())
}

/// Locks only eligible parents, selecting no continuation or original payload columns.
async fn load_parents(
    tx: &mut db::DbTransaction<'_>,
    ids: &[uuid::Uuid],
) -> Result<Vec<ParentWrite>, TaskRuntimeError> {
    let table = DbTaskStore::table();
    let query = db::from(&table)
        .filter(table.id.in_values(ids.to_vec()))
        .filter(table.status.eq(db::val(TaskStatus::Suspended.as_i16())))
        .filter(table.cancelled.eq(db::val(false)))
        .sort(table.id.asc());
    #[cfg(any(feature = "postgres", feature = "mysql"))]
    let query = {
        use crate::db::backend::RowLockExt as _;
        query.for_update()
    };
    Ok(query.all::<ParentWrite>().exec(tx).await?)
}
