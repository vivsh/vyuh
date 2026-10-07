//! Bounded workflow preparation and post-claim finalization in the store transaction.

use super::*;
use crate::db::Model as _;
use crate::tasks::TaskPoll;

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
    remaining_completions: i32,
}

/// Reserves child keys in bulk; conflicts fail their parent without aborting siblings.
pub(super) async fn prepare_children(
    tx: &mut db::DbTransaction<'_>,
    rows: &[TaskRow],
    commands: &HashMap<TaskId, &TaskCommit>,
    conf: &crate::tasks::TaskStoreConf,
    now: DateTime<Utc>,
    batch_size: usize,
) -> Result<
    (
        Vec<TaskRow>,
        HashSet<TaskId>,
        Vec<super::super::workflow::WaitWrite>,
    ),
    TaskRuntimeError,
> {
    if !commands.values().any(|commit| {
        matches!(
            commit.outcome(),
            Some(TaskOutcome::Spawn { .. } | TaskOutcome::All { .. })
        )
    }) {
        return Ok((Vec::new(), HashSet::new(), Vec::new()));
    }
    let (writes, mut conflicts, mut waits) = prepare_groups(rows, commands, conf, now)?;
    if writes.is_empty() {
        return Ok((Vec::new(), conflicts, waits));
    }
    let (prepared, mut keys) = prepare_writes(writes, now)?;
    let owners = if waits.is_empty() {
        upsert_key_owners(tx, &keys, batch_size).await?;
        let owners = load_key_owners(tx, &keys).await?;
        replace_expired_owners(tx, owners, &keys, now).await?
    } else {
        reserve_group_keys(tx, &mut keys, now, batch_size).await?
    };
    let (mut children, found) = resolve_children(prepared, owners)?;
    conflicts.extend(found);
    super::super::workflow::release_rejected(tx, &children, &conflicts, batch_size).await?;
    children.retain(|child| {
        !child
            .parent_id
            .is_some_and(|id| conflicts.contains(&TaskId::new(id)))
    });
    waits.retain(|wait| !conflicts.contains(&TaskId::new(wait.id)));
    Ok((children, conflicts, waits))
}

/// Derives group lineage and ordered membership without mutating durable key ownership.
#[expect(
    clippy::type_complexity,
    reason = "Invocation-local scratch, not a second lifecycle owner"
)]
fn prepare_groups(
    rows: &[TaskRow],
    commands: &HashMap<TaskId, &TaskCommit>,
    conf: &crate::tasks::TaskStoreConf,
    now: DateTime<Utc>,
) -> Result<
    (
        Vec<crate::tasks::TaskWrite>,
        HashSet<TaskId>,
        Vec<super::super::workflow::WaitWrite>,
    ),
    TaskRuntimeError,
> {
    let (mut writes, mut conflicts, mut waits) = (Vec::new(), HashSet::new(), Vec::new());
    for row in rows {
        if let Some(TaskOutcome::All { children, .. }) = commands
            .get(&TaskId::new(row.id))
            .and_then(|commit| commit.outcome())
        {
            if row.cancelled {
                continue;
            }
            if crate::tasks::TaskKind::from_i16(row.kind)? != crate::tasks::TaskKind::Flow
                || crate::tasks::store::workflow::validate_group(children, conf).is_err()
            {
                conflicts.insert(TaskId::new(row.id));
                continue;
            }
            let ids = children
                .iter()
                .map(|child| child.record.id)
                .collect::<Vec<_>>();
            waits.push(super::super::workflow::WaitWrite::new(
                row.id,
                &row.lane_name,
                &ids,
                now,
            )?);
            writes.extend(children.iter().map(|child| child_write(row, child)));
            continue;
        }
        if let Some(child) = prepare_child(row, commands, conf)? {
            writes.push(child);
        }
    }
    Ok((writes, conflicts, waits))
}

/// Reserves fan-out keys in stable handler/key order with bounded parameter counts.
async fn reserve_group_keys(
    tx: &mut db::DbTransaction<'_>,
    keys: &mut [TaskIdempotencyRow],
    now: DateTime<Utc>,
    batch: usize,
) -> Result<Vec<TaskIdempotencyRow>, TaskRuntimeError> {
    keys.sort_by(|a, b| (&a.task_name, &a.key_value).cmp(&(&b.task_name, &b.key_value)));
    let mut owners = Vec::with_capacity(keys.len());
    for chunk in keys.chunks(batch.clamp(1, 250)) {
        upsert_key_owners(tx, chunk, batch).await?;
        let loaded = load_key_owners(tx, chunk).await?;
        owners.extend(replace_expired_owners(tx, loaded, chunk, now).await?);
    }
    Ok(owners)
}

/// Validates one fenced spawn and derives lineage before reserving any child identity.
fn prepare_child(
    row: &TaskRow,
    commands: &HashMap<TaskId, &TaskCommit>,
    conf: &crate::tasks::TaskStoreConf,
) -> Result<Option<crate::tasks::TaskWrite>, TaskRuntimeError> {
    if row.cancelled {
        return Ok(None);
    }
    let Some(outcome @ TaskOutcome::Spawn { child, .. }) = commands
        .get(&TaskId::new(row.id))
        .and_then(|commit| commit.outcome())
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
    Ok(Some(child_write(row, child)))
}

fn child_write(row: &TaskRow, child: &crate::tasks::TaskWrite) -> crate::tasks::TaskWrite {
    let mut child = child.clone();
    child.record.parent_id = Some(TaskId::new(row.id));
    child.record.root_id = Some(TaskId::new(row.root_id.unwrap_or(row.id)));
    child.ignore_conflicts = true;
    child
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
    waits: &[super::super::workflow::WaitWrite],
    conf: &crate::tasks::TaskStoreConf,
    now: DateTime<Utc>,
    batch_size: usize,
) -> Result<Vec<crate::tasks::TaskLane>, TaskRuntimeError> {
    let mut lanes = children
        .iter()
        .map(|child| child.lane_name.clone())
        .collect::<HashSet<_>>();
    super::super::workflow::install(tx, waits, batch_size, &mut lanes).await?;
    if !children.is_empty() {
        db::from(DbTaskStore::table())
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
    let mut results = std::collections::BTreeMap::<TaskId, (usize, String)>::new();
    for (parent, result) in deliveries {
        let entry = results.entry(parent).or_insert((0, result));
        entry.0 += 1;
    }
    let ids = results.keys().map(|id| id.into_uuid()).collect::<Vec<_>>();
    for chunk in ids.chunks(batch_size) {
        let mut parents = load_parents(tx, chunk).await?;
        let joining = parents
            .iter()
            .filter(|parent| parent.remaining_completions > 0)
            .map(|parent| parent.id)
            .collect::<HashSet<_>>();
        let mut invalid = Vec::new();
        for parent in &mut parents {
            let Some((count, result)) = results.remove(&TaskId::new(parent.id)) else {
                continue;
            };
            deliver(parent, count, result, now, &mut invalid);
            if parent.status == TaskStatus::Pending.as_i16() {
                lanes.insert(parent.lane_name.clone());
            }
        }
        let joins = parents
            .extract_if(.., |parent| joining.contains(&parent.id))
            .collect::<Vec<_>>();
        persist_counters(tx, &joins, batch_size).await?;
        persist_scalar(tx, &parents, batch_size).await?;
        super::super::workflow::clear(tx, &invalid, batch_size).await?;
    }
    Ok(())
}

/// Calculates scalar or counter delivery using only the authoritative narrow parent row.
fn deliver(
    parent: &mut ParentWrite,
    count: usize,
    result: String,
    now: DateTime<Utc>,
    invalid: &mut Vec<uuid::Uuid>,
) {
    if parent.remaining_completions > 0 {
        match i32::try_from(count)
            .ok()
            .and_then(|count| parent.remaining_completions.checked_sub(count))
        {
            Some(remaining) if remaining >= 0 => {
                parent.remaining_completions = remaining;
                if remaining > 0 {
                    return;
                }
            }
            _ => {
                parent.resume_input = Some(crate::tasks::store::workflow::failure(
                    TaskId::new(parent.id),
                    "All completion counter underflow",
                ));
                invalid.push(parent.id);
                parent.remaining_completions = 0;
            }
        }
    } else {
        parent.resume_input = Some(result);
    }
    parent.status = TaskStatus::Pending.as_i16();
    parent.ready_at = Some(now);
    parent.updated_at = now;
}

/// Keeps ordinary single-child parent writes on their original four-column path.
async fn persist_scalar(
    tx: &mut db::DbTransaction<'_>,
    parents: &[ParentWrite],
    batch: usize,
) -> Result<(), TaskRuntimeError> {
    if parents.is_empty() {
        return Ok(());
    }
    let table = ParentWrite::table();
    db::from(&table)
        .update_many(
            parents,
            (
                &table.status,
                &table.resume_input,
                &table.ready_at,
                &table.updated_at,
            ),
        )
        .batch_size(batch)
        .exec(tx)
        .await?;
    Ok(())
}

/// Active waits write only counters; satisfied waits additionally advance readiness.
async fn persist_counters(
    tx: &mut db::DbTransaction<'_>,
    parents: &[ParentWrite],
    batch: usize,
) -> Result<(), TaskRuntimeError> {
    let table = ParentWrite::table();
    let (waiting, ready): (Vec<_>, Vec<_>) = parents
        .iter()
        .cloned()
        .partition(|parent| parent.remaining_completions > 0);
    if !waiting.is_empty() {
        db::from(&table)
            .update_many(&waiting, &table.remaining_completions)
            .batch_size(batch)
            .exec(&mut *tx)
            .await?;
    }
    if !ready.is_empty() {
        db::from(&table)
            .update_many(
                &ready,
                (
                    &table.remaining_completions,
                    &table.status,
                    &table.ready_at,
                    &table.updated_at,
                    &table.resume_input,
                ),
            )
            .batch_size(batch)
            .exec(tx)
            .await?;
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

/// Narrow authoritative wait installation, deferred until after claim selection.
#[derive(db::Model)]
#[table(name = "vyuh_tasks")]
pub(in super::super) struct WaitWrite {
    #[column(primary_key)]
    pub(in super::super) id: uuid::Uuid,
    waiting_children: Option<String>,
    remaining_completions: i32,
    status: i16,
    ready_at: Option<DateTime<Utc>>,
    lane_name: String,
}

impl WaitWrite {
    pub(in super::super) fn new(
        id: uuid::Uuid,
        lane: &str,
        children: &[TaskId],
        now: DateTime<Utc>,
    ) -> Result<Self, TaskRuntimeError> {
        Ok(Self {
            id,
            waiting_children: Some(serde_json::to_string(children)?),
            lane_name: lane.into(),
            remaining_completions: children.len() as i32,
            status: if children.is_empty() {
                TaskStatus::Pending
            } else {
                TaskStatus::Suspended
            }
            .as_i16(),
            ready_at: children.is_empty().then_some(now),
        })
    }
}

/// Claim materialization never rewrites application checkpoints or child results.
#[derive(db::Model)]
#[table(name = "vyuh_tasks")]
struct JoinInput {
    #[column(primary_key)]
    id: uuid::Uuid,
    resume_input: Option<String>,
    waiting_children: Option<String>,
    remaining_completions: i32,
}

#[derive(db::Model)]
#[table(name = "vyuh_tasks")]
struct ChildResult {
    #[column(primary_key)]
    id: uuid::Uuid,
    status: i16,
    last_result: Option<String>,
}

#[derive(db::Record)]
struct ClearWait {
    waiting_children: Option<String>,
    remaining_completions: i32,
}

/// Releases only keys belonging to newly allocated children of rejected groups.
pub(in super::super) async fn release_rejected(
    tx: &mut db::DbTransaction<'_>,
    children: &[TaskRow],
    rejected: &HashSet<TaskId>,
    batch: usize,
) -> Result<(), TaskRuntimeError> {
    let ids = children
        .iter()
        .filter(|row| {
            row.idempotency_key.is_some()
                && row
                    .parent_id
                    .is_some_and(|id| rejected.contains(&TaskId::new(id)))
        })
        .map(|row| row.id)
        .collect::<Vec<_>>();
    let table = DbTaskStore::idempotency_table();
    for chunk in ids.chunks(batch.clamp(1, 250)) {
        db::from(&table)
            .filter(table.task_id.in_values(chunk.to_vec()))
            .delete()
            .exec(&mut *tx)
            .await?;
    }
    Ok(())
}

/// Installs only accepted waits; empty groups become ready after this turn's claims.
pub(in super::super) async fn install(
    tx: &mut db::DbTransaction<'_>,
    waits: &[WaitWrite],
    batch: usize,
    lanes: &mut HashSet<String>,
) -> Result<(), TaskRuntimeError> {
    if waits.is_empty() {
        return Ok(());
    }
    let table = WaitWrite::table();
    db::from(&table)
        .update_many(
            waits,
            (
                &table.waiting_children,
                &table.remaining_completions,
                &table.status,
                &table.ready_at,
            ),
        )
        .batch_size(batch)
        .exec(&mut *tx)
        .await?;
    lanes.extend(
        waits
            .iter()
            .filter(|wait| wait.remaining_completions == 0)
            .map(|wait| wait.lane_name.clone()),
    );
    Ok(())
}

/// Clears malformed or terminal waits without reading their ordered membership.
pub(in super::super) async fn clear(
    tx: &mut db::DbTransaction<'_>,
    ids: &[uuid::Uuid],
    batch: usize,
) -> Result<(), TaskRuntimeError> {
    let table = JoinInput::table();
    let patch = ClearWait {
        waiting_children: None,
        remaining_completions: 0,
    };
    for chunk in ids.chunks(batch.clamp(1, 250)) {
        db::from(&table)
            .filter(table.id.in_values(chunk.to_vec()))
            .update(&patch)
            .exec(&mut *tx)
            .await?;
    }
    Ok(())
}

/// Materializes all selected lanes together; ordinary claims never enter a query here.
pub(in super::super) async fn materialize(
    tx: &mut db::DbTransaction<'_>,
    waits: Vec<(TaskId, String, i32)>,
    poll: &mut TaskPoll,
    batch: usize,
) -> Result<(), TaskRuntimeError> {
    if waits.is_empty() {
        return Ok(());
    }
    let mut patches = Vec::with_capacity(waits.len());
    let mut members = Vec::new();
    let mut errors = HashMap::new();
    for (id, json, remaining) in waits {
        let index = patches.len();
        patches.push(JoinInput {
            id: id.into_uuid(),
            resume_input: Some("{\"Ok\":[".into()),
            waiting_children: None,
            remaining_completions: 0,
        });
        match serde_json::from_str::<Vec<TaskId>>(&json) {
            Ok(ids) if remaining == 0 => {
                members.extend(ids.into_iter().map(|child| (index, child)))
            }
            _ => {
                errors.insert(index, "All membership or counter is invalid");
            }
        }
    }
    collect(tx, &members, &mut patches, &mut errors, batch).await?;
    for (index, patch) in patches.iter_mut().enumerate() {
        if let Some(error) = errors.get(&index) {
            patch.resume_input = Some(crate::tasks::store::workflow::failure(
                TaskId::new(patch.id),
                error,
            ));
        } else if let Some(buffer) = &mut patch.resume_input {
            buffer.push_str("]}");
        }
    }
    persist_inputs(tx, patches, poll, batch).await
}

/// Bounds loaded child bytes to a query chunk, packing IDs across parent boundaries.
async fn collect(
    tx: &mut db::DbTransaction<'_>,
    members: &[(usize, TaskId)],
    patches: &mut [JoinInput],
    errors: &mut HashMap<usize, &'static str>,
    batch: usize,
) -> Result<(), TaskRuntimeError> {
    for chunk in members.chunks(batch.clamp(1, 250)) {
        let ids = chunk
            .iter()
            .filter(|(index, _)| !errors.contains_key(index))
            .map(|(_, id)| id.into_uuid())
            .collect::<Vec<_>>();
        if ids.is_empty() {
            continue;
        }
        let rows = load_results(tx, ids).await?;
        let mut rows = rows
            .into_iter()
            .map(|row| (row.id, row))
            .collect::<HashMap<_, _>>();
        for (index, id) in chunk {
            if errors.contains_key(index) {
                continue;
            }
            let Some(buffer) = patches
                .get_mut(*index)
                .and_then(|patch| patch.resume_input.as_mut())
            else {
                continue;
            };
            let child = rows.remove(&id.into_uuid());
            let result = child.as_ref().and_then(|row| row.last_result.as_deref());
            if let Err(error) =
                crate::tasks::store::workflow::append_result(buffer, result, child.is_some())
            {
                buffer.clear();
                errors.insert(*index, error);
            }
        }
    }
    Ok(())
}

/// Current reads avoid repeatable-read snapshots and never lock nonterminal members.
async fn load_results(
    tx: &mut db::DbTransaction<'_>,
    ids: Vec<uuid::Uuid>,
) -> Result<Vec<ChildResult>, TaskRuntimeError> {
    let table = DbTaskStore::table();
    let query = db::from(&table)
        .filter(table.id.in_values(ids))
        .filter(table.status.in_values(vec![
            TaskStatus::Succeeded.as_i16(),
            TaskStatus::Failed.as_i16(),
        ]))
        .sort(table.id.asc());
    #[cfg(any(feature = "postgres", feature = "mysql"))]
    let query = {
        use crate::db::backend::RowLockExt as _;
        query.for_update()
    };
    Ok(query.all::<ChildResult>().exec(tx).await?)
}

/// Persists inputs and only then updates operation-local snapshots returned by the store.
async fn persist_inputs(
    tx: &mut db::DbTransaction<'_>,
    patches: Vec<JoinInput>,
    poll: &mut TaskPoll,
    batch: usize,
) -> Result<(), TaskRuntimeError> {
    let table = JoinInput::table();
    db::from(&table)
        .update_many(
            &patches,
            (
                &table.resume_input,
                &table.waiting_children,
                &table.remaining_completions,
            ),
        )
        .batch_size(batch)
        .exec(tx)
        .await?;
    let mut inputs = patches
        .into_iter()
        .map(|patch| (TaskId::new(patch.id), patch.resume_input))
        .collect::<HashMap<_, _>>();
    for task in poll.lanes.iter_mut().flat_map(|lane| &mut lane.tasks) {
        if let Some(input) = inputs.remove(&task.id) {
            task.resume_input = input;
        }
    }
    Ok(())
}
