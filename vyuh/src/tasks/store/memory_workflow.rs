//! Operation-local staging for the memory store's atomic workflow transitions.

use super::*;

/// Rolls back touched rows and bounded lane state if any phase of a turn fails.
pub(super) fn atomic_operation(
    state: &mut MemoryState,
    runner: Option<&uuid::Uuid>,
    claims: &[LaneClaim],
    commits: &[TaskCommit],
    batch_size: usize,
    lease: Duration,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(TaskPoll, CommitResult), TaskRuntimeError> {
    let maximum = state
        .conf
        .as_ref()
        .map_or(256, |conf| conf.max_all_children);
    super::super::workflow::validate_turn(commits, batch_size, maximum)?;
    let rates = state.rates.clone();
    let locks = state.lane_locks.clone();
    let mut undo = Vec::new();
    let result = run_turn(
        state, runner, claims, commits, batch_size, lease, now, &mut undo,
    );
    if result.is_err() {
        for original in undo.into_iter().rev() {
            if let Some(task) = state.tasks.iter_mut().find(|task| task.id == original.id) {
                *task = original;
            }
        }
        state.rates = rates;
        state.lane_locks = locks;
    }
    result
}

/// Keeps generated work out of claim selection, finalizing only after fallible work succeeds.
fn run_turn(
    state: &mut MemoryState,
    runner: Option<&uuid::Uuid>,
    claims: &[LaneClaim],
    commits: &[TaskCommit],
    batch_size: usize,
    lease: Duration,
    now: chrono::DateTime<chrono::Utc>,
    undo: &mut Vec<TaskRecord>,
) -> Result<(TaskPoll, CommitResult), TaskRuntimeError> {
    let (children, mut deliveries, waits) = commit_outcomes_state(state, commits, now, undo)?;
    let phases = state
        .lane_locks
        .iter()
        .map(|(name, row)| (name.clone(), row.phase))
        .collect::<HashMap<_, _>>();
    let mut poll = if let Some(runner) = runner {
        claim_tasks_state(
            state,
            runner,
            claims,
            batch_size,
            lease,
            now,
            &mut deliveries,
            undo,
        )?
    } else {
        TaskPoll {
            lanes: Vec::new(),
            wake_lanes: Vec::new(),
        }
    };
    workflow::materialize(state, &mut poll);
    let mut wake_lanes = workflow::install(state, waits, now);
    wake_lanes.extend(finalize_workflow(state, children, deliveries, now));
    workflow::clear_terminal(state, undo);
    reconcile_poll(state, &mut poll, &wake_lanes, &phases, now);
    let rejected = commits
        .iter()
        .filter(|commit| !undo.iter().any(|task| task.id == commit.task_id()))
        .map(|commit| commit.task_id())
        .collect();
    poll.wake_lanes = wake_lanes.clone();
    Ok((
        poll,
        CommitResult {
            wake_lanes,
            rejected,
        },
    ))
}

/// Stages accepted task updates and children before mutating any existing task.
pub(super) fn stage_outcomes(
    state: &MemoryState,
    commits: &[TaskCommit],
    now: chrono::DateTime<chrono::Utc>,
) -> Result<
    (
        Vec<(usize, TaskRecord)>,
        Vec<TaskRecord>,
        Vec<(TaskId, String)>,
        Vec<(TaskId, workflow::TaskWait)>,
    ),
    TaskRuntimeError,
> {
    let mut updates = Vec::with_capacity(commits.len());
    let mut children = Vec::new();
    let mut deliveries = Vec::new();
    let mut waits = Vec::new();
    let mut seen = std::collections::HashSet::with_capacity(commits.len());
    for commit in commits {
        if !seen.insert(commit.task_id()) {
            return Err(TaskRuntimeError::InvalidOptions(
                "duplicate task outcome".into(),
            ));
        }
        if !memory_commit_allowed(state, commit, now)? {
            continue;
        }
        let Some((index, task)) = owned_task(state, commit)? else {
            continue;
        };
        let task = staged_task(state, task, commit, now, &mut children, &mut deliveries)?;
        if matches!(commit.outcome(), Some(TaskOutcome::All { .. }))
            && task.status == TaskStatus::Suspended
        {
            waits.push((task.id, workflow::prepared_wait(task.id, &children)));
        }
        updates.push((index, task));
    }
    Ok((updates, children, deliveries, waits))
}

/// Borrows only the authoritative row still owned by the outcome's runner and lane.
fn owned_task<'a>(
    state: &'a MemoryState,
    commit: &TaskCommit,
) -> Result<Option<(usize, &'a TaskRecord)>, TaskRuntimeError> {
    let owned = state.tasks.iter().enumerate().find(|(_, task)| {
        task.id == commit.task_id() && commit.accepts(task.status, task.node_id, task.attempts)
    });
    if let Some((_, task)) = owned
        && commit.lane().is_some_and(|lane| task.lane != lane.as_str())
    {
        return Err(TaskRuntimeError::UnknownLane(task.lane.clone()));
    }
    Ok(owned)
}

/// Calculates an accepted row's checkpoint and delivery without mutating stored tasks.
fn staged_task(
    state: &MemoryState,
    task: &TaskRecord,
    commit: &TaskCommit,
    now: chrono::DateTime<chrono::Utc>,
    children: &mut Vec<TaskRecord>,
    deliveries: &mut Vec<(TaskId, String)>,
) -> Result<TaskRecord, TaskRuntimeError> {
    let conf = state
        .conf
        .as_ref()
        .ok_or_else(|| TaskRuntimeError::InvalidConfig("task store is not initialized".into()))?;
    let mut task = task.clone();
    task.cancelled |= commit.is_cancel();
    let outcome = commit
        .outcome()
        .cloned()
        .unwrap_or_else(super::super::workflow::cancellation);
    let outcome = if task.cancelled {
        super::super::workflow::cancellation()
    } else if let Some(error) =
        super::super::workflow::capability_error(task.kind, &outcome, &conf.handlers)
    {
        TaskOutcome::fail(error)
    } else {
        prepare_spawn(state, &task, outcome, children, now)?
    };
    let retry = conf
        .lanes
        .iter()
        .find(|lane| lane.lane().as_str() == task.lane)
        .map(|lane| lane.retry_policy())
        .ok_or_else(|| TaskRuntimeError::UnknownLane(task.lane.clone()))?;
    apply_outcome(&mut task, outcome, retry, now)?;
    finalize_idempotency(&mut task, conf, now)?;
    queue_delivery(&task, deliveries)?;
    Ok(task)
}

/// Resolves child identity conflicts without adopting an independent task.
fn prepare_spawn(
    state: &MemoryState,
    parent: &TaskRecord,
    outcome: TaskOutcome,
    children: &mut Vec<TaskRecord>,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<TaskOutcome, TaskRuntimeError> {
    if let TaskOutcome::All {
        state: checkpoint,
        children: group,
    } = outcome
    {
        return workflow::prepare(state, parent, checkpoint, group, children, now);
    }
    let TaskOutcome::Spawn {
        state: checkpoint,
        mut child,
    } = outcome
    else {
        return Ok(outcome);
    };
    validate_write_lanes(state, std::slice::from_ref(&child))?;
    if child.ignore_conflicts {
        return Err(TaskRuntimeError::InvalidOptions(
            "spawn cannot ignore conflicts".into(),
        ));
    }
    child.record.parent_id = Some(parent.id);
    child.record.root_id = Some(parent.root_id.unwrap_or(parent.id));
    match stage_write(&state.tasks, children, child, now) {
        Ok(TaskReceipt::Queued(_)) => Ok(TaskOutcome::Suspend { state: checkpoint }),
        Ok(_)
        | Err(TaskRuntimeError::IdempotencyConflict(_) | TaskRuntimeError::AlreadyExists(_)) => Ok(
            TaskOutcome::fail("Child task identity conflicts with an existing task"),
        ),
        Err(error) => Err(error),
    }
}

/// Captures a terminal child result while its accepted transition is still available.
pub(super) fn queue_delivery(
    task: &TaskRecord,
    deliveries: &mut Vec<(TaskId, String)>,
) -> Result<(), TaskRuntimeError> {
    if let Some(parent) = task.parent_id
        && let Some(result) =
            super::super::workflow::terminal_result(task.status, task.last_result.as_deref())?
    {
        deliveries.push((parent, result.to_owned()));
    }
    Ok(())
}

/// Applies prevalidated workflow writes only after this turn's claim selection.
pub(super) fn finalize_workflow(
    state: &mut MemoryState,
    children: Vec<TaskRecord>,
    deliveries: Vec<(TaskId, String)>,
    now: chrono::DateTime<chrono::Utc>,
) -> Vec<TaskLane> {
    let mut names = std::collections::HashSet::new();
    for child in &children {
        names.insert(child.lane.clone());
    }
    state.tasks.extend(children);
    for (id, (count, result)) in group_deliveries(deliveries) {
        if let Some(parent) = state
            .tasks
            .iter_mut()
            .find(|task| task.id == id && !task.cancelled && task.status == TaskStatus::Suspended)
        {
            if let Some(wait) = state.waits.get_mut(&id) {
                let remaining = wait.remaining_completions.checked_sub(count);
                if remaining.is_none_or(|remaining| remaining < 0) {
                    parent.resume_input = Some(super::super::workflow::failure(
                        id,
                        "All completion counter underflow",
                    ));
                    state.waits.remove(&id);
                } else {
                    wait.remaining_completions = remaining.unwrap_or(0);
                    if wait.remaining_completions > 0 {
                        continue;
                    }
                }
            } else {
                parent.resume_input = Some(result);
            }
            parent.status = TaskStatus::Pending;
            parent.ready_at = Some(now);
            parent.updated_at = now;
            names.insert(parent.lane.clone());
        }
    }
    state
        .conf
        .as_ref()
        .into_iter()
        .flat_map(|conf| &conf.lanes)
        .filter_map(|lane| names.contains(lane.lane().as_str()).then_some(lane.lane()))
        .collect()
}

fn group_deliveries(
    deliveries: Vec<(TaskId, String)>,
) -> std::collections::BTreeMap<TaskId, (i32, String)> {
    let mut grouped = std::collections::BTreeMap::new();
    for (id, result) in deliveries {
        grouped.entry(id).or_insert((0, result)).0 += 1;
    }
    grouped
}

/// Cancels only a newly proposed idle hook when finalization added ready work.
pub(super) fn reconcile_poll(
    state: &mut MemoryState,
    poll: &mut TaskPoll,
    lanes: &[TaskLane],
    phases: &HashMap<String, LaneOwnerPhase>,
    now: chrono::DateTime<chrono::Utc>,
) {
    for lane in &mut poll.lanes {
        if !lanes.contains(&lane.lane) {
            continue;
        }
        lane.next_wake_in = Some(Duration::ZERO);
        if !state
            .tasks
            .iter()
            .any(|task| task.lane == lane.lane.as_str() && is_due(task, now))
        {
            continue;
        }
        if let Some(owner) = state.lane_locks.get_mut(lane.lane.as_str()) {
            owner.empty_since = None;
            if owner.phase == LaneOwnerPhase::Idling
                && phases.get(lane.lane.as_str()) != Some(&LaneOwnerPhase::Idling)
            {
                memory_activate(owner);
                lane.owner = Some(memory_owner_poll(owner, None));
            }
        }
    }
}

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
    if let Err(error) = super::super::workflow::validate_group(&group, conf) {
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
        return super::super::workflow::failure(parent, "All claimed before completion");
    }
    let mut buffer = String::from("{\"Ok\":[");
    for id in &wait.children {
        let child = state.tasks.iter().find(|task| task.id == *id);
        let result = child.and_then(|task| task.last_result.as_deref());
        let terminal = child
            .is_some_and(|task| matches!(task.status, TaskStatus::Succeeded | TaskStatus::Failed));
        if let Err(error) = super::super::workflow::append_result(&mut buffer, result, terminal) {
            return super::super::workflow::failure(parent, error);
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
