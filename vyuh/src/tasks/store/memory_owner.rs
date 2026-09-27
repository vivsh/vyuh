//! Memory-store lane ownership transitions.

use super::*;

/// Coordinates one process-local lane owner using the durable-store state machine.
pub(super) fn claim_owned_state(
    state: &mut MemoryState,
    runner_id: &str,
    claim: &LaneClaim,
    lane: &crate::tasks::TaskLaneConf,
    lease_duration: Duration,
    now: chrono::DateTime<chrono::Utc>,
    deliveries: &mut Vec<(TaskId, String)>,
    undo: &mut Vec<TaskRecord>,
) -> Result<LanePoll, TaskRuntimeError> {
    #[cfg(test)]
    {
        state.lane_lock_turns = state.lane_lock_turns.saturating_add(1);
    }
    let name = claim.lane.to_string();
    let mut owner = state.lane_locks.remove(&name).ok_or_else(|| {
        TaskRuntimeError::InvalidConfig(format!(
            "task lane lock '{}' is not initialized",
            claim.lane
        ))
    })?;
    let had_owner = owner.owner_token.is_some();
    let acquiring = claim
        .owner
        .as_ref()
        .is_some_and(|request| request.token.is_none());
    let mut result = claim_owned_inner(
        state,
        &mut owner,
        runner_id,
        claim,
        lane,
        lease_duration,
        now,
        deliveries,
        undo,
    );
    let took_over = acquiring
        && had_owner
        && result.as_ref().is_ok_and(|poll| {
            poll.owner
                .as_ref()
                .is_some_and(|owner| owner.token.is_some())
        });
    if took_over
        && let Ok(poll) = &mut result
        && let Some(owner) = &mut poll.owner
    {
        owner.takeover = true;
    }
    state.lane_locks.insert(name, owner);
    result
}

pub(super) fn claim_owned_inner(
    state: &mut MemoryState,
    owner: &mut MemoryLaneLock,
    runner_id: &str,
    claim: &LaneClaim,
    lane: &crate::tasks::TaskLaneConf,
    lease_duration: Duration,
    now: chrono::DateTime<chrono::Utc>,
    deliveries: &mut Vec<(TaskId, String)>,
    undo: &mut Vec<TaskRecord>,
) -> Result<LanePoll, TaskRuntimeError> {
    if !memory_owner(owner, runner_id, claim, lease_duration, now)? {
        let wake = owner
            .leased_until
            .and_then(|deadline| (deadline - now).to_std().ok())
            .or_else(|| task_deadline(&state.tasks, claim.lane.as_str(), now));
        return memory_wait_poll(claim.lane, owner, wake);
    }
    if let Some(hook) = claim
        .owner
        .as_ref()
        .and_then(|request| request.hook.as_ref())
    {
        apply_memory_hook(state, owner, hook, claim.lane, lane, now)?;
    }
    if let Some(action) = memory_action(owner.phase) {
        return memory_poll_action(claim.lane, owner, action, None);
    }
    memory_phase_poll(
        state,
        owner,
        runner_id,
        claim,
        lane,
        lease_duration,
        now,
        deliveries,
        undo,
    )
}

pub(super) fn memory_owner(
    owner: &mut MemoryLaneLock,
    runner_id: &str,
    claim: &LaneClaim,
    duration: Duration,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<bool, TaskRuntimeError> {
    let requested = claim
        .owner
        .as_ref()
        .and_then(|request| request.token.as_deref());
    let current = requested.is_some_and(|token| {
        owner.owner_id.as_deref() == Some(runner_id)
            && owner.owner_token.as_deref() == Some(token)
            && memory_owner_live(owner, now)
    });
    if current {
        owner.leased_until = checked_deadline(now, duration)?;
        return Ok(true);
    }
    if requested.is_some() || memory_owner_live(owner, now) {
        return Ok(false);
    }
    owner.owner_id = Some(runner_id.into());
    owner.owner_token = Some(uuid::Uuid::now_v7().to_string());
    owner.leased_until = checked_deadline(now, duration)?;
    Ok(true)
}

pub(super) fn memory_phase_poll(
    state: &mut MemoryState,
    owner: &mut MemoryLaneLock,
    runner_id: &str,
    claim: &LaneClaim,
    lane: &crate::tasks::TaskLaneConf,
    lease_duration: Duration,
    now: chrono::DateTime<chrono::Utc>,
    deliveries: &mut Vec<(TaskId, String)>,
    undo: &mut Vec<TaskRecord>,
) -> Result<LanePoll, TaskRuntimeError> {
    let quiescent = claim
        .owner
        .as_ref()
        .is_some_and(|request| request.quiescent);
    if matches!(
        owner.phase,
        LaneOwnerPhase::Active | LaneOwnerPhase::IdleFailed
    ) && !quiescent
    {
        return memory_poll(claim.lane, owner, None);
    }
    let completed_work = claim
        .owner
        .as_ref()
        .is_some_and(|request| request.completed_work);
    if owner.phase == LaneOwnerPhase::IdleFailed && completed_work {
        memory_activate(owner);
    }
    let candidates = memory_candidates(state, claim.lane, lane, now);
    if candidates.is_empty() {
        owner.flushing = false;
        return memory_empty(state, owner, claim, lane, now);
    }
    owner.empty_since = None;
    if matches!(
        owner.phase,
        LaneOwnerPhase::Idle | LaneOwnerPhase::BusyFailed
    ) {
        return memory_start_busy(owner, claim.lane, lane, now);
    }
    let turn = MemoryOwnerTurn {
        runner_id,
        claim,
        lane,
        lease_duration,
        now,
    };
    memory_flush_poll(state, owner, candidates, &turn, deliveries, undo)
}

/// Selects one ordered, bounded candidate window without claiming its tasks.
pub(super) fn memory_candidates(
    state: &MemoryState,
    lane_name: TaskLane,
    lane: &crate::tasks::TaskLaneConf,
    now: chrono::DateTime<chrono::Utc>,
) -> Vec<usize> {
    let size = lane
        .lane_lock()
        .map_or(1, crate::tasks::TaskLaneLock::batch_size);
    let mut candidates = due_indices(&state.tasks, lane_name.as_str(), now);
    candidates.sort_by_key(|index| state.tasks.get(*index).map(|task| readiness(task, now)));
    candidates.truncate(size);
    candidates
}

/// Continues an open cohort or waits for its threshold before claiming.
pub(super) fn memory_flush_poll(
    state: &mut MemoryState,
    owner: &mut MemoryLaneLock,
    candidates: Vec<usize>,
    turn: &MemoryOwnerTurn<'_>,
    deliveries: &mut Vec<(TaskId, String)>,
    undo: &mut Vec<TaskRecord>,
) -> Result<LanePoll, TaskRuntimeError> {
    if !owner.flushing && !memory_flush(&state.tasks, &candidates, turn.lane, turn.now)? {
        return memory_poll(
            turn.claim.lane,
            owner,
            memory_flush_wake(&state.tasks, &candidates, turn.lane, turn.now)?,
        );
    }
    owner.flushing = true;
    let allow_claim = turn
        .claim
        .owner
        .as_ref()
        .is_some_and(|request| request.allow_claim);
    if !allow_claim {
        return memory_poll(turn.claim.lane, owner, Some(Duration::ZERO));
    }
    memory_claim(
        state,
        owner,
        turn.runner_id,
        turn.claim,
        turn.lane,
        turn.lease_duration,
        turn.now,
        deliveries,
        undo,
    )
}

pub(super) fn memory_empty(
    state: &MemoryState,
    owner: &mut MemoryLaneLock,
    claim: &LaneClaim,
    lane: &crate::tasks::TaskLaneConf,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<LanePoll, TaskRuntimeError> {
    if matches!(
        owner.phase,
        LaneOwnerPhase::Idle | LaneOwnerPhase::IdleFailed | LaneOwnerPhase::BusyFailed
    ) {
        let phase = owner.phase;
        memory_release(owner, phase);
        return memory_poll(
            claim.lane,
            owner,
            task_deadline(&state.tasks, claim.lane.as_str(), now),
        );
    }
    let quiescent = claim
        .owner
        .as_ref()
        .is_some_and(|request| request.quiescent);
    if !quiescent {
        return memory_poll(claim.lane, owner, None);
    }
    let delay = lane
        .lane_lock()
        .map_or(Duration::ZERO, crate::tasks::TaskLaneLock::idle_duration);
    let started = *owner.empty_since.get_or_insert(now);
    let elapsed = (now - started).to_std().unwrap_or(Duration::ZERO);
    if elapsed < delay {
        return memory_poll(claim.lane, owner, Some(delay.saturating_sub(elapsed)));
    }
    memory_start_idle(owner, claim.lane, lane)
}

pub(super) fn memory_start_idle(
    owner: &mut MemoryLaneLock,
    lane_name: TaskLane,
    lane: &crate::tasks::TaskLaneConf,
) -> Result<LanePoll, TaskRuntimeError> {
    if lane.lane_lock().and_then(|lock| lock.idle_hook()).is_some() {
        memory_transition(owner, LaneOwnerPhase::Idling)?;
        return memory_poll_action(lane_name, owner, LaneHookAction::Idle, None);
    }
    memory_release(owner, LaneOwnerPhase::Idle);
    memory_poll(lane_name, owner, None)
}

pub(super) fn memory_start_busy(
    owner: &mut MemoryLaneLock,
    lane_name: TaskLane,
    lane: &crate::tasks::TaskLaneConf,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<LanePoll, TaskRuntimeError> {
    if owner.hook_retry_at.is_some_and(|retry| retry > now) {
        let wake = owner
            .hook_retry_at
            .and_then(|retry| (retry - now).to_std().ok());
        memory_release(owner, LaneOwnerPhase::BusyFailed);
        return memory_poll(lane_name, owner, wake);
    }
    if lane.lane_lock().and_then(|lock| lock.busy_hook()).is_some() {
        memory_transition(owner, LaneOwnerPhase::Busying)?;
        return memory_poll_action(lane_name, owner, LaneHookAction::Busy, None);
    }
    memory_activate(owner);
    memory_poll(lane_name, owner, Some(Duration::ZERO))
}

pub(super) fn memory_claim(
    state: &mut MemoryState,
    owner: &MemoryLaneLock,
    runner_id: &str,
    claim: &LaneClaim,
    lane: &crate::tasks::TaskLaneConf,
    lease_duration: Duration,
    now: chrono::DateTime<chrono::Utc>,
    deliveries: &mut Vec<(TaskId, String)>,
    undo: &mut Vec<TaskRecord>,
) -> Result<LanePoll, TaskRuntimeError> {
    let size = lane
        .lane_lock()
        .map_or(1, crate::tasks::TaskLaneLock::batch_size);
    let retry = lane.retry_policy();
    let conf = state
        .conf
        .clone()
        .ok_or_else(|| TaskRuntimeError::InvalidConfig("task store is not initialized".into()))?;
    fail_exhausted(
        &mut state.tasks,
        claim.lane.as_str(),
        size,
        now,
        &conf,
        retry,
        deliveries,
        undo,
    )?;
    let candidates = due_count(state, claim.lane, now).min(size);
    let (permits, rate_wake) =
        reserve_permits(state, claim.lane, candidates, lane.global_rate(), now)?;
    let reservation = ClaimReservation {
        claim: LaneClaim {
            lane: claim.lane,
            limit: size,
            owner: None,
        },
        permits,
        rate_wake,
        candidates,
    };
    let mut poll = claim_lane_state(state, runner_id, reservation, lease_duration, now, undo)?;
    poll.owner = Some(memory_owner_poll(owner, None));
    Ok(poll)
}

pub(super) fn apply_memory_hook(
    state: &MemoryState,
    owner: &mut MemoryLaneLock,
    hook: &LaneHookResult,
    lane_name: TaskLane,
    lane: &crate::tasks::TaskLaneConf,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(), TaskRuntimeError> {
    if owner.generation != hook.generation || memory_action(owner.phase) != Some(hook.action) {
        return Ok(());
    }
    match (hook.action, &hook.result) {
        (LaneHookAction::Idle, Ok(())) => finish_memory_idle(state, owner, lane_name, lane, now)?,
        (LaneHookAction::Idle, Err(error)) => memory_fail_idle(owner, error),
        (LaneHookAction::Busy, Ok(())) => memory_activate(owner),
        (LaneHookAction::Busy, Err(error)) => memory_fail_busy(state, owner, error, now)?,
    }
    Ok(())
}

pub(super) fn finish_memory_idle(
    state: &MemoryState,
    owner: &mut MemoryLaneLock,
    lane_name: TaskLane,
    lane: &crate::tasks::TaskLaneConf,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(), TaskRuntimeError> {
    if due_indices(&state.tasks, lane_name.as_str(), now).is_empty() {
        memory_release(owner, LaneOwnerPhase::Idle);
    } else if lane.lane_lock().and_then(|lock| lock.busy_hook()).is_some() {
        memory_transition(owner, LaneOwnerPhase::Busying)?;
    } else {
        memory_activate(owner);
    }
    Ok(())
}

pub(super) fn memory_fail_idle(owner: &mut MemoryLaneLock, error: &str) {
    owner.last_hook_error = Some(error.into());
    memory_release(owner, LaneOwnerPhase::IdleFailed);
}

pub(super) fn memory_fail_busy(
    state: &MemoryState,
    owner: &mut MemoryLaneLock,
    error: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(), TaskRuntimeError> {
    let delay = state
        .conf
        .as_ref()
        .map_or(Duration::from_secs(1), |conf| conf.poll_interval);
    owner.last_hook_error = Some(error.into());
    owner.hook_retry_at = checked_deadline(now, delay)?;
    memory_release(owner, LaneOwnerPhase::BusyFailed);
    Ok(())
}

pub(super) fn memory_flush(
    tasks: &[TaskRecord],
    candidates: &[usize],
    lane: &crate::tasks::TaskLaneConf,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<bool, TaskRuntimeError> {
    let lock = lane
        .lane_lock()
        .ok_or_else(|| TaskRuntimeError::InvalidConfig("locked lane lost its policy".into()))?;
    Ok(candidates.len() >= lock.batch_size()
        || memory_flush_wake(tasks, candidates, lane, now)? == Some(Duration::ZERO))
}

pub(super) fn memory_flush_wake(
    tasks: &[TaskRecord],
    candidates: &[usize],
    lane: &crate::tasks::TaskLaneConf,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Option<Duration>, TaskRuntimeError> {
    let Some(deadline) = lane.lane_lock().and_then(|lock| lock.batch_deadline()) else {
        return Ok(None);
    };
    let oldest = candidates
        .first()
        .and_then(|index| tasks.get(*index))
        .map(|task| readiness(task, now).0);
    let Some(oldest) = oldest else {
        return Ok(None);
    };
    let due = oldest
        .checked_add_signed(chrono_duration(deadline)?)
        .ok_or_else(|| {
            TaskRuntimeError::InvalidConfig("lane lock deadline exceeds timestamp range".into())
        })?;
    Ok(Some((due - now).to_std().unwrap_or(Duration::ZERO)))
}

pub(super) fn memory_owner_live(
    owner: &MemoryLaneLock,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    owner.owner_token.is_some() && owner.leased_until.is_some_and(|deadline| deadline > now)
}

pub(super) fn memory_action(phase: LaneOwnerPhase) -> Option<LaneHookAction> {
    match phase {
        LaneOwnerPhase::Idling => Some(LaneHookAction::Idle),
        LaneOwnerPhase::Busying => Some(LaneHookAction::Busy),
        _ => None,
    }
}

pub(super) fn memory_transition(
    owner: &mut MemoryLaneLock,
    phase: LaneOwnerPhase,
) -> Result<(), TaskRuntimeError> {
    owner.generation = owner.generation.checked_add(1).ok_or_else(|| {
        TaskRuntimeError::TaskExecutionError("lane lifecycle generation overflowed".into())
    })?;
    owner.phase = phase;
    owner.hook_retry_at = None;
    owner.last_hook_error = None;
    Ok(())
}

pub(super) fn memory_activate(owner: &mut MemoryLaneLock) {
    owner.phase = LaneOwnerPhase::Active;
    owner.empty_since = None;
    owner.hook_retry_at = None;
    owner.last_hook_error = None;
}

pub(super) fn memory_release(owner: &mut MemoryLaneLock, phase: LaneOwnerPhase) {
    owner.owner_id = None;
    owner.owner_token = None;
    owner.leased_until = None;
    owner.phase = phase;
    owner.flushing = false;
}

pub(super) fn memory_owner_poll(
    owner: &MemoryLaneLock,
    action: Option<LaneHookAction>,
) -> LaneOwnerPoll {
    LaneOwnerPoll {
        token: owner.owner_token.clone(),
        generation: owner.generation,
        phase: owner.phase,
        action,
        takeover: false,
    }
}

pub(super) fn memory_poll_action(
    lane: TaskLane,
    owner: &MemoryLaneLock,
    action: LaneHookAction,
    wake: Option<Duration>,
) -> Result<LanePoll, TaskRuntimeError> {
    memory_poll_with(lane, owner, Some(action), wake)
}

pub(super) fn memory_poll(
    lane: TaskLane,
    owner: &MemoryLaneLock,
    wake: Option<Duration>,
) -> Result<LanePoll, TaskRuntimeError> {
    memory_poll_with(lane, owner, None, wake)
}

pub(super) fn memory_wait_poll(
    lane: TaskLane,
    owner: &MemoryLaneLock,
    wake: Option<Duration>,
) -> Result<LanePoll, TaskRuntimeError> {
    let mut poll = memory_poll(lane, owner, wake)?;
    if let Some(owner) = &mut poll.owner {
        owner.token = None;
        owner.action = None;
    }
    Ok(poll)
}

pub(super) fn memory_poll_with(
    lane: TaskLane,
    owner: &MemoryLaneLock,
    action: Option<LaneHookAction>,
    wake: Option<Duration>,
) -> Result<LanePoll, TaskRuntimeError> {
    Ok(LanePoll {
        lane,
        tasks: Vec::new(),
        reclaimed: 0,
        saturated: false,
        next_wake_in: wake,
        owner: Some(memory_owner_poll(owner, action)),
    })
}
