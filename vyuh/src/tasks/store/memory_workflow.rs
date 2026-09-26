//! Operation-local staging for the memory store's atomic workflow transitions.

use super::*;

/// Rolls back touched rows and bounded lane state if any phase of a turn fails.
pub(super) fn atomic_turn(
    state: &mut MemoryState,
    runner: &str,
    claims: &[LaneClaim],
    commits: &[TaskCommit],
    renewals: &[TaskLease],
    batch_size: usize,
    lease: Duration,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<TaskTick, TaskError> {
    let rates = state.rates.clone();
    let locks = state.lane_locks.clone();
    let mut undo = Vec::new();
    let result = run_turn(
        state, runner, claims, commits, renewals, batch_size, lease, now, &mut undo,
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
    runner: &str,
    claims: &[LaneClaim],
    commits: &[TaskCommit],
    renewals: &[TaskLease],
    batch_size: usize,
    lease: Duration,
    now: chrono::DateTime<chrono::Utc>,
    undo: &mut Vec<TaskRecord>,
) -> Result<TaskTick, TaskError> {
    let (children, mut deliveries) = commit_outcomes_state(state, runner, commits, now, undo)?;
    let phases = state
        .lane_locks
        .iter()
        .map(|(name, row)| (name.clone(), row.phase))
        .collect::<HashMap<_, _>>();
    let lost = renew_leases_state(state, runner, renewals, lease, now, undo)?;
    let mut poll = claim_tasks_state(
        state,
        runner,
        claims,
        batch_size,
        lease,
        now,
        &mut deliveries,
        undo,
    )?;
    let wake_lanes = finalize_workflow(state, children, deliveries, now);
    reconcile_poll(state, &mut poll, &wake_lanes, &phases, now);
    Ok(TaskTick {
        poll,
        lost,
        wake_lanes,
    })
}

/// Stages accepted task updates and children before mutating any existing task.
pub(super) fn stage_outcomes(
    state: &MemoryState,
    runner: &str,
    commits: &[TaskCommit],
    now: chrono::DateTime<chrono::Utc>,
) -> Result<
    (
        Vec<(usize, TaskRecord)>,
        Vec<TaskRecord>,
        Vec<(TaskId, String)>,
    ),
    TaskError,
> {
    let mut updates = Vec::with_capacity(commits.len());
    let mut children = Vec::new();
    let mut deliveries = Vec::new();
    let mut seen = std::collections::HashSet::with_capacity(commits.len());
    for commit in commits {
        if !seen.insert(commit.task_id) {
            return Err(TaskError::InvalidOptions("duplicate task outcome".into()));
        }
        let retry = configured_retry(state, commit.lane)?;
        if !memory_commit_allowed(state, runner, commit, now)? {
            continue;
        }
        let Some((index, task)) = state.tasks.iter().enumerate().find(|(_, task)| {
            task.id == commit.task_id
                && task.status == TaskStatus::Running
                && task.locked_by.as_deref() == Some(runner)
        }) else {
            continue;
        };
        if task.lane != commit.lane.as_str() {
            return Err(TaskError::UnknownLane(task.lane.clone()));
        }
        let task = staged_task(
            state,
            task,
            commit.outcome.clone(),
            retry,
            now,
            &mut children,
            &mut deliveries,
        )?;
        updates.push((index, task));
    }
    Ok((updates, children, deliveries))
}

/// Calculates an accepted row's checkpoint and delivery without mutating stored tasks.
fn staged_task(
    state: &MemoryState,
    task: &TaskRecord,
    outcome: TaskOutcome,
    retry: TaskRetry,
    now: chrono::DateTime<chrono::Utc>,
    children: &mut Vec<TaskRecord>,
    deliveries: &mut Vec<(TaskId, String)>,
) -> Result<TaskRecord, TaskError> {
    let conf = state
        .conf
        .as_ref()
        .ok_or_else(|| TaskError::InvalidConfig("task store is not initialized".into()))?;
    let mut task = task.clone();
    let outcome = prepare_spawn(state, &task, outcome, children, now)?;
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
) -> Result<TaskOutcome, TaskError> {
    let TaskOutcome::Spawn {
        state: checkpoint,
        mut child,
    } = outcome
    else {
        return Ok(outcome);
    };
    validate_write_lanes(state, std::slice::from_ref(&child))?;
    if child.ignore_conflicts {
        return Err(TaskError::InvalidOptions(
            "spawn cannot ignore conflicts".into(),
        ));
    }
    child.record.parent_id = Some(parent.id);
    child.record.root_id = Some(parent.root_id.unwrap_or(parent.id));
    match stage_write(&state.tasks, children, child, now) {
        Ok(TaskReceipt::Queued(_)) => Ok(TaskOutcome::Suspend { state: checkpoint }),
        Ok(_) | Err(TaskError::IdempotencyConflict(_) | TaskError::AlreadyExists(_)) => Ok(
            TaskOutcome::fail("Child task identity conflicts with an existing task"),
        ),
        Err(error) => Err(error),
    }
}

/// Captures a terminal child result while its accepted transition is still available.
pub(super) fn queue_delivery(
    task: &TaskRecord,
    deliveries: &mut Vec<(TaskId, String)>,
) -> Result<(), TaskError> {
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
    for (id, result) in deliveries {
        if let Some(parent) = state
            .tasks
            .iter_mut()
            .find(|task| task.id == id && task.status == TaskStatus::Suspended)
        {
            parent.status = TaskStatus::Pending;
            parent.resume_input = Some(result);
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
