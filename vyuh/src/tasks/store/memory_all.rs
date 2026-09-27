//! Authoritative memory-store join state and operation-local preparation.

use super::*;

#[derive(Clone)]
pub(super) struct TaskWait {
    pub(super) children: Vec<TaskId>,
    pub(super) remaining_completions: i32,
}

/// Records only accepted group identities in submission order before authoritative installation.
pub(super) fn prepared_wait(parent: TaskId, children: &[TaskRecord]) -> TaskWait {
    let children = children
        .iter()
        .filter(|child| child.parent_id == Some(parent))
        .map(|child| child.id)
        .collect::<Vec<_>>();
    TaskWait {
        remaining_completions: children.len() as i32,
        children,
    }
}

/// Stages a whole group, undoing its inserts on any identity conflict.
pub(super) fn prepare(
    state: &MemoryState,
    parent: &TaskRecord,
    checkpoint: String,
    group: Vec<TaskWrite>,
    children: &mut Vec<TaskRecord>,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<TaskOutcome, TaskRuntimeError> {
    let conf = state
        .conf
        .as_ref()
        .ok_or_else(|| TaskRuntimeError::InvalidConfig("Store not initialized".into()))?;
    if let Err(error) = super::super::all::validate_group(&group, conf) {
        return Ok(TaskOutcome::fail(error.to_string()));
    }
    let start = children.len();
    for mut child in group {
        child.record.parent_id = Some(parent.id);
        child.record.root_id = Some(parent.root_id.unwrap_or(parent.id));
        if !matches!(
            stage_write(&state.tasks, children, child, now),
            Ok(TaskReceipt::Queued(_))
        ) {
            children.truncate(start);
            return Ok(TaskOutcome::fail(
                "All child preparation or identity conflict",
            ));
        }
    }
    Ok(TaskOutcome::Suspend { state: checkpoint })
}

/// Installs new waits only after all fallible turn operations have succeeded.
pub(super) fn install(
    state: &mut MemoryState,
    waits: Vec<(TaskId, TaskWait)>,
    now: chrono::DateTime<chrono::Utc>,
) -> Vec<TaskLane> {
    let mut wake = Vec::new();
    for (id, wait) in waits {
        if wait.children.is_empty()
            && let Some(parent) = state.tasks.iter_mut().find(|task| task.id == id)
        {
            parent.status = TaskStatus::Pending;
            parent.ready_at = Some(now);
            if let Some(lane) = state.conf.as_ref().and_then(|conf| {
                conf.lanes
                    .iter()
                    .find(|lane| lane.lane().as_str() == parent.lane)
            }) {
                wake.push(lane.lane());
            }
        }
        state.waits.insert(id, wait);
    }
    wake
}

/// Materializes only actually claimed parents, persisting input before returning snapshots.
pub(super) fn materialize(state: &mut MemoryState, poll: &mut TaskPoll) {
    for snapshot in poll.lanes.iter_mut().flat_map(|lane| &mut lane.tasks) {
        let Some(wait) = state.waits.remove(&snapshot.id) else {
            continue;
        };
        let input = aggregate(state, snapshot.id, &wait);
        if let Some(parent) = state.tasks.iter_mut().find(|task| task.id == snapshot.id) {
            parent.resume_input = Some(input);
            *snapshot = parent.clone();
        }
    }
}

/// Borrows terminal member bytes in submission order without copying all results.
fn aggregate(state: &MemoryState, parent: TaskId, wait: &TaskWait) -> String {
    if wait.remaining_completions != 0 {
        return super::super::all::failure(parent, "All claimed before completion");
    }
    let mut buffer = String::from("{\"Ok\":[");
    for id in &wait.children {
        let child = state.tasks.iter().find(|task| task.id == *id);
        let result = child.and_then(|task| task.last_result.as_deref());
        let terminal = child
            .is_some_and(|task| matches!(task.status, TaskStatus::Succeeded | TaskStatus::Failed));
        if let Err(error) = super::super::all::append_result(&mut buffer, result, terminal) {
            return super::super::all::failure(parent, error);
        }
    }
    buffer.push_str("]}");
    buffer
}

/// Removes metadata only for touched parents whose finalization made it unnecessary.
pub(super) fn clear_terminal(state: &mut MemoryState, touched: &[TaskRecord]) {
    for task in touched {
        if state.waits.contains_key(&task.id)
            && state.tasks.iter().any(|current| {
                current.id == task.id
                    && matches!(current.status, TaskStatus::Succeeded | TaskStatus::Failed)
            })
        {
            state.waits.remove(&task.id);
        }
    }
}
