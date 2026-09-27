//! Join-only bulk patches and bounded claim-time result collection.

use super::{common::DbTaskStore, model::TaskRow};
use crate::{
    db::{self, Model as _},
    tasks::{TaskId, TaskPoll, TaskRuntimeError, TaskStatus},
};
use chrono::{DateTime, Utc};
use std::collections::{HashMap, HashSet};

/// Narrow authoritative wait installation, deferred until after claim selection.
#[derive(db::Model)]
#[table(name = "vyuh_tasks")]
pub(super) struct WaitWrite {
    #[column(primary_key)]
    pub(super) id: uuid::Uuid,
    waiting_children: Option<String>,
    remaining_completions: i32,
    status: i16,
    ready_at: Option<DateTime<Utc>>,
    lane_name: String,
}

impl WaitWrite {
    pub(super) fn new(
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
pub(super) async fn release_rejected(
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
pub(super) async fn install(
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
pub(super) async fn clear(
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
pub(super) async fn materialize(
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
            patch.resume_input = Some(crate::tasks::store::all::failure(
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
                crate::tasks::store::all::append_result(buffer, result, child.is_some())
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
