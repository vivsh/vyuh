//! In-memory reference implementation of the per-lane task-store contract.

use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::tasks::{
    AbstractTaskStore, IdempotencyRetention, LaneClaim, LaneHookAction, LaneHookResult,
    LaneOwnerPhase, LaneOwnerPoll, LanePoll, ScheduledTaskWrite, TaskCommit, TaskError, TaskFilter,
    TaskId, TaskLane, TaskLease, TaskOutcome, TaskPoll, TaskReceipt, TaskRecord, TaskRetry,
    TaskScheduleSnapshot, TaskStatus, TaskStoreConf, TaskTick, TaskWrite,
};

#[derive(Clone)]
struct RateBucket {
    tokens_micros: i64,
    updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Clone, Default)]
struct MemoryState {
    tasks: Vec<TaskRecord>,
    conf: Option<TaskStoreConf>,
    fingerprint: Option<String>,
    rates: HashMap<String, RateBucket>,
    schedules: HashMap<String, chrono::DateTime<chrono::Utc>>,
    lane_locks: HashMap<String, MemoryLaneLock>,
    #[cfg(test)]
    lane_lock_turns: usize,
}

#[derive(Clone)]
struct MemoryLaneLock {
    owner_id: Option<String>,
    owner_token: Option<String>,
    leased_until: Option<chrono::DateTime<chrono::Utc>>,
    phase: LaneOwnerPhase,
    flushing: bool,
    empty_since: Option<chrono::DateTime<chrono::Utc>>,
    generation: i64,
    hook_retry_at: Option<chrono::DateTime<chrono::Utc>>,
    last_hook_error: Option<String>,
}

struct ClaimReservation {
    claim: LaneClaim,
    permits: usize,
    rate_wake: Option<Duration>,
    candidates: usize,
}

struct MemoryOwnerTurn<'a> {
    runner_id: &'a str,
    claim: &'a LaneClaim,
    lane: &'a crate::tasks::TaskLaneConf,
    lease_duration: Duration,
    now: chrono::DateTime<chrono::Utc>,
}

/// In-memory task store for tests and local development.
///
/// This store is process-local and is not a durable or distributed coordinator.
#[derive(Clone)]
pub struct MemoryTaskStore {
    state: Arc<tokio::sync::Mutex<MemoryState>>,
    batch_size: usize,
    lease_duration: Duration,
}

impl MemoryTaskStore {
    /// Creates a process-local store with one bounded claim size.
    #[cfg(test)]
    pub fn new(batch_size: usize) -> Self {
        Self::with_lease_duration(batch_size, Duration::from_secs(300))
    }

    /// Creates a process-local store with an explicit default lease duration.
    pub fn with_lease_duration(batch_size: usize, lease_duration: Duration) -> Self {
        Self {
            state: Arc::new(tokio::sync::Mutex::new(MemoryState::default())),
            batch_size: batch_size.max(1),
            lease_duration,
        }
    }

    /// Returns the number of records retained by this store.
    #[cfg(test)]
    pub async fn task_count(&self) -> usize {
        self.state.lock().await.tasks.len()
    }

    /// Returns a snapshot of all retained records.
    #[cfg(test)]
    pub async fn tasks(&self) -> Vec<TaskRecord> {
        self.state.lock().await.tasks.clone()
    }

    /// Returns owner-coordination turns performed by this reference store.
    #[cfg(test)]
    pub async fn lane_lock_turns(&self) -> usize {
        self.state.lock().await.lane_lock_turns
    }
}

impl AbstractTaskStore for MemoryTaskStore {
    async fn initialize(&self, conf: TaskStoreConf) -> Result<(), TaskError> {
        let mut state = self.state.lock().await;
        let fingerprint = crate::tasks::store::policy_fingerprint(&conf);
        if state
            .fingerprint
            .as_deref()
            .is_some_and(|value| value != fingerprint)
        {
            return Err(TaskError::InvalidConfig(
                "task workers use incompatible lane or global rate policies".into(),
            ));
        }
        fail_unleased_running(&mut state.tasks, &conf, chrono::Utc::now())?;
        reject_orphaned_tasks(&state.tasks, &conf)?;
        initialize_rates(&mut state, &conf);
        initialize_lane_locks(&mut state, &conf);
        state.fingerprint = Some(fingerprint);
        state.conf = Some(conf);
        Ok(())
    }

    async fn claim_tasks(
        &self,
        runner_id: &str,
        claims: &[LaneClaim],
    ) -> Result<TaskPoll, TaskError> {
        let mut state = self.state.lock().await;
        claim_tasks_state(
            &mut state,
            runner_id,
            claims,
            self.batch_size,
            self.lease_duration,
            chrono::Utc::now(),
        )
    }

    async fn commit_outcomes(
        &self,
        runner_id: &str,
        commits: &[TaskCommit],
    ) -> Result<(), TaskError> {
        let mut state = self.state.lock().await;
        commit_outcomes_state(&mut state, runner_id, commits, chrono::Utc::now())
    }

    async fn renew_leases(
        &self,
        runner_id: &str,
        leases: &[TaskLease],
    ) -> Result<Vec<TaskId>, TaskError> {
        let mut state = self.state.lock().await;
        renew_leases_state(
            &mut state,
            runner_id,
            leases,
            self.lease_duration,
            chrono::Utc::now(),
        )
    }

    async fn tick(
        &self,
        runner_id: &str,
        claims: &[LaneClaim],
        commits: &[TaskCommit],
        renewals: &[TaskLease],
    ) -> Result<TaskTick, TaskError> {
        let mut state = self.state.lock().await;
        let now = chrono::Utc::now();
        commit_outcomes_state(&mut state, runner_id, commits, now)?;
        let lost = renew_leases_state(&mut state, runner_id, renewals, self.lease_duration, now)?;
        let poll = claim_tasks_state(
            &mut state,
            runner_id,
            claims,
            self.batch_size,
            self.lease_duration,
            now,
        )?;
        Ok(TaskTick { poll, lost })
    }

    async fn store_tasks(&self, writes: Vec<TaskWrite>) -> Result<Vec<TaskReceipt>, TaskError> {
        let mut state = self.state.lock().await;
        validate_write_lanes(&state, &writes)?;
        let now = chrono::Utc::now();
        let mut staged = Vec::with_capacity(writes.len());
        let mut receipts = Vec::with_capacity(writes.len());
        for write in writes {
            let receipt = stage_write(&state.tasks, &mut staged, write, now)?;
            receipts.push(receipt);
        }
        state.tasks.extend(staged);
        Ok(receipts)
    }

    async fn schedule_snapshot(&self, names: &[String]) -> Result<TaskScheduleSnapshot, TaskError> {
        let state = self.state.lock().await;
        Ok(TaskScheduleSnapshot {
            now: chrono::Utc::now(),
            cursors: names
                .iter()
                .filter_map(|name| {
                    state
                        .schedules
                        .get(name)
                        .copied()
                        .map(|time| (name.clone(), time))
                })
                .collect(),
        })
    }

    async fn store_scheduled(
        &self,
        scheduled: ScheduledTaskWrite,
    ) -> Result<Option<TaskReceipt>, TaskError> {
        let mut state = self.state.lock().await;
        if state
            .schedules
            .get(&scheduled.name)
            .is_some_and(|last| *last >= scheduled.occurrence)
        {
            return Ok(None);
        }
        validate_write_lanes(&state, std::slice::from_ref(&scheduled.write))?;
        let now = chrono::Utc::now();
        let mut staged = Vec::with_capacity(1);
        let receipt = stage_write(&state.tasks, &mut staged, scheduled.write, now)?;
        state.tasks.extend(staged);
        state
            .schedules
            .insert(scheduled.name, now.max(scheduled.occurrence));
        Ok(Some(receipt))
    }

    async fn reassign_lane(&self, from: &str, to: &str) -> Result<u64, TaskError> {
        let mut state = self.state.lock().await;
        require_lane(&state, to)?;
        if state
            .tasks
            .iter()
            .any(|task| task.lane == from && task.status == TaskStatus::Running)
        {
            return Err(TaskError::LaneBusy(from.into()));
        }
        let mut changed = 0_u64;
        for task in &mut state.tasks {
            if task.lane == from && is_reassignable(task.status) {
                task.lane = to.into();
                task.updated_at = chrono::Utc::now();
                changed += 1;
            }
        }
        Ok(changed)
    }

    async fn resume(&self, id: TaskId, input: String) -> Result<bool, TaskError> {
        let mut state = self.state.lock().await;
        let now = chrono::Utc::now();
        let Some(task) = state.tasks.iter_mut().find(|task| task.id == id) else {
            return Ok(false);
        };
        if task.status != TaskStatus::Suspended {
            return Ok(false);
        }
        task.status = TaskStatus::Pending;
        task.resume_input = Some(input);
        task.ready_at = Some(now);
        task.updated_at = now;
        Ok(true)
    }

    async fn list_tasks(
        &self,
        filter: TaskFilter,
    ) -> Result<crate::routes::Page<TaskRecord>, TaskError> {
        let state = self.state.lock().await;
        let mut records = state
            .tasks
            .iter()
            .filter(|task| matches_filter(task, &filter))
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by_key(|task| std::cmp::Reverse((task.created_at, task.id)));
        Ok(page(records, &filter))
    }

    async fn get_task(&self, id: TaskId) -> Result<Option<TaskRecord>, TaskError> {
        Ok(self
            .state
            .lock()
            .await
            .tasks
            .iter()
            .find(|task| task.id == id)
            .cloned())
    }
}

/// Claims all requested lanes while holding the in-memory store transaction lock.
fn claim_tasks_state(
    state: &mut MemoryState,
    runner_id: &str,
    claims: &[LaneClaim],
    batch_size: usize,
    lease_duration: Duration,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<TaskPoll, TaskError> {
    let conf = state
        .conf
        .clone()
        .ok_or_else(|| TaskError::InvalidConfig("task store is not initialized".into()))?;
    let mut lanes = Vec::with_capacity(claims.len());
    for claim in claims {
        let lane_conf = conf
            .lanes
            .iter()
            .find(|lane| lane.lane() == claim.lane)
            .ok_or_else(|| TaskError::UnknownLane(claim.lane.to_string()))?;
        if lane_conf.lane_lock().is_some() {
            lanes.push(claim_owned_state(
                state,
                runner_id,
                claim,
                lane_conf,
                lease_duration,
                now,
            )?);
            continue;
        }
        let bounded = bounded_claim(claim, batch_size);
        let retry = configured_retry(state, bounded.lane)?;
        fail_exhausted(&mut state.tasks, bounded.lane.as_str(), now, &conf, retry)?;
        let candidates = due_count(state, bounded.lane, now);
        let rate = configured_rate(state, bounded.lane)?;
        let (permits, rate_wake) = reserve_permits(state, bounded.lane, candidates, rate, now)?;
        let reservation = ClaimReservation {
            claim: bounded,
            permits,
            rate_wake,
            candidates,
        };
        lanes.push(claim_lane_state(
            state,
            runner_id,
            reservation,
            lease_duration,
            now,
        )?);
    }
    Ok(TaskPoll { lanes })
}

/// Coordinates one process-local lane owner using the durable-store state machine.
fn claim_owned_state(
    state: &mut MemoryState,
    runner_id: &str,
    claim: &LaneClaim,
    lane: &crate::tasks::TaskLaneConf,
    lease_duration: Duration,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<LanePoll, TaskError> {
    #[cfg(test)]
    {
        state.lane_lock_turns = state.lane_lock_turns.saturating_add(1);
    }
    let name = claim.lane.to_string();
    let mut owner = state.lane_locks.remove(&name).ok_or_else(|| {
        TaskError::InvalidConfig(format!(
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

fn claim_owned_inner(
    state: &mut MemoryState,
    owner: &mut MemoryLaneLock,
    runner_id: &str,
    claim: &LaneClaim,
    lane: &crate::tasks::TaskLaneConf,
    lease_duration: Duration,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<LanePoll, TaskError> {
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
    memory_phase_poll(state, owner, runner_id, claim, lane, lease_duration, now)
}

fn memory_owner(
    owner: &mut MemoryLaneLock,
    runner_id: &str,
    claim: &LaneClaim,
    duration: Duration,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<bool, TaskError> {
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

fn memory_phase_poll(
    state: &mut MemoryState,
    owner: &mut MemoryLaneLock,
    runner_id: &str,
    claim: &LaneClaim,
    lane: &crate::tasks::TaskLaneConf,
    lease_duration: Duration,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<LanePoll, TaskError> {
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
    memory_flush_poll(state, owner, candidates, &turn)
}

/// Selects one ordered, bounded candidate window without claiming its tasks.
fn memory_candidates(
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
fn memory_flush_poll(
    state: &mut MemoryState,
    owner: &mut MemoryLaneLock,
    candidates: Vec<usize>,
    turn: &MemoryOwnerTurn<'_>,
) -> Result<LanePoll, TaskError> {
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
    )
}

fn memory_empty(
    state: &MemoryState,
    owner: &mut MemoryLaneLock,
    claim: &LaneClaim,
    lane: &crate::tasks::TaskLaneConf,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<LanePoll, TaskError> {
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

fn memory_start_idle(
    owner: &mut MemoryLaneLock,
    lane_name: TaskLane,
    lane: &crate::tasks::TaskLaneConf,
) -> Result<LanePoll, TaskError> {
    if lane.lane_lock().and_then(|lock| lock.idle_hook()).is_some() {
        memory_transition(owner, LaneOwnerPhase::Idling)?;
        return memory_poll_action(lane_name, owner, LaneHookAction::Idle, None);
    }
    memory_release(owner, LaneOwnerPhase::Idle);
    memory_poll(lane_name, owner, None)
}

fn memory_start_busy(
    owner: &mut MemoryLaneLock,
    lane_name: TaskLane,
    lane: &crate::tasks::TaskLaneConf,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<LanePoll, TaskError> {
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

fn memory_claim(
    state: &mut MemoryState,
    owner: &MemoryLaneLock,
    runner_id: &str,
    claim: &LaneClaim,
    lane: &crate::tasks::TaskLaneConf,
    lease_duration: Duration,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<LanePoll, TaskError> {
    let size = lane
        .lane_lock()
        .map_or(1, crate::tasks::TaskLaneLock::batch_size);
    let retry = lane.retry_policy();
    let conf = state
        .conf
        .clone()
        .ok_or_else(|| TaskError::InvalidConfig("task store is not initialized".into()))?;
    fail_exhausted(&mut state.tasks, claim.lane.as_str(), now, &conf, retry)?;
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
    let mut poll = claim_lane_state(state, runner_id, reservation, lease_duration, now)?;
    poll.owner = Some(memory_owner_poll(owner, None));
    Ok(poll)
}

fn apply_memory_hook(
    state: &MemoryState,
    owner: &mut MemoryLaneLock,
    hook: &LaneHookResult,
    lane_name: TaskLane,
    lane: &crate::tasks::TaskLaneConf,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(), TaskError> {
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

fn finish_memory_idle(
    state: &MemoryState,
    owner: &mut MemoryLaneLock,
    lane_name: TaskLane,
    lane: &crate::tasks::TaskLaneConf,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(), TaskError> {
    if due_indices(&state.tasks, lane_name.as_str(), now).is_empty() {
        memory_release(owner, LaneOwnerPhase::Idle);
    } else if lane.lane_lock().and_then(|lock| lock.busy_hook()).is_some() {
        memory_transition(owner, LaneOwnerPhase::Busying)?;
    } else {
        memory_activate(owner);
    }
    Ok(())
}

fn memory_fail_idle(owner: &mut MemoryLaneLock, error: &str) {
    owner.last_hook_error = Some(error.into());
    memory_release(owner, LaneOwnerPhase::IdleFailed);
}

fn memory_fail_busy(
    state: &MemoryState,
    owner: &mut MemoryLaneLock,
    error: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(), TaskError> {
    let delay = state
        .conf
        .as_ref()
        .map_or(Duration::from_secs(1), |conf| conf.poll_interval);
    owner.last_hook_error = Some(error.into());
    owner.hook_retry_at = checked_deadline(now, delay)?;
    memory_release(owner, LaneOwnerPhase::BusyFailed);
    Ok(())
}

fn memory_flush(
    tasks: &[TaskRecord],
    candidates: &[usize],
    lane: &crate::tasks::TaskLaneConf,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<bool, TaskError> {
    let lock = lane
        .lane_lock()
        .ok_or_else(|| TaskError::InvalidConfig("locked lane lost its policy".into()))?;
    Ok(candidates.len() >= lock.batch_size()
        || memory_flush_wake(tasks, candidates, lane, now)? == Some(Duration::ZERO))
}

fn memory_flush_wake(
    tasks: &[TaskRecord],
    candidates: &[usize],
    lane: &crate::tasks::TaskLaneConf,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Option<Duration>, TaskError> {
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
            TaskError::InvalidConfig("lane lock deadline exceeds timestamp range".into())
        })?;
    Ok(Some((due - now).to_std().unwrap_or(Duration::ZERO)))
}

fn memory_owner_live(owner: &MemoryLaneLock, now: chrono::DateTime<chrono::Utc>) -> bool {
    owner.owner_token.is_some() && owner.leased_until.is_some_and(|deadline| deadline > now)
}

fn memory_action(phase: LaneOwnerPhase) -> Option<LaneHookAction> {
    match phase {
        LaneOwnerPhase::Idling => Some(LaneHookAction::Idle),
        LaneOwnerPhase::Busying => Some(LaneHookAction::Busy),
        _ => None,
    }
}

fn memory_transition(owner: &mut MemoryLaneLock, phase: LaneOwnerPhase) -> Result<(), TaskError> {
    owner.generation = owner.generation.checked_add(1).ok_or_else(|| {
        TaskError::TaskExecutionError("lane lifecycle generation overflowed".into())
    })?;
    owner.phase = phase;
    owner.hook_retry_at = None;
    owner.last_hook_error = None;
    Ok(())
}

fn memory_activate(owner: &mut MemoryLaneLock) {
    owner.phase = LaneOwnerPhase::Active;
    owner.empty_since = None;
    owner.hook_retry_at = None;
    owner.last_hook_error = None;
}

fn memory_release(owner: &mut MemoryLaneLock, phase: LaneOwnerPhase) {
    owner.owner_id = None;
    owner.owner_token = None;
    owner.leased_until = None;
    owner.phase = phase;
    owner.flushing = false;
}

fn memory_owner_poll(owner: &MemoryLaneLock, action: Option<LaneHookAction>) -> LaneOwnerPoll {
    LaneOwnerPoll {
        token: owner.owner_token.clone(),
        generation: owner.generation,
        phase: owner.phase,
        action,
        takeover: false,
    }
}

fn memory_poll_action(
    lane: TaskLane,
    owner: &MemoryLaneLock,
    action: LaneHookAction,
    wake: Option<Duration>,
) -> Result<LanePoll, TaskError> {
    memory_poll_with(lane, owner, Some(action), wake)
}

fn memory_poll(
    lane: TaskLane,
    owner: &MemoryLaneLock,
    wake: Option<Duration>,
) -> Result<LanePoll, TaskError> {
    memory_poll_with(lane, owner, None, wake)
}

fn memory_wait_poll(
    lane: TaskLane,
    owner: &MemoryLaneLock,
    wake: Option<Duration>,
) -> Result<LanePoll, TaskError> {
    let mut poll = memory_poll(lane, owner, wake)?;
    if let Some(owner) = &mut poll.owner {
        owner.token = None;
        owner.action = None;
    }
    Ok(poll)
}

fn memory_poll_with(
    lane: TaskLane,
    owner: &MemoryLaneLock,
    action: Option<LaneHookAction>,
    wake: Option<Duration>,
) -> Result<LanePoll, TaskError> {
    Ok(LanePoll {
        lane,
        tasks: Vec::new(),
        reclaimed: 0,
        saturated: false,
        next_wake_in: wake,
        owner: Some(memory_owner_poll(owner, action)),
    })
}

/// Commits one bounded batch of outcomes while the in-memory transaction lock is held.
fn commit_outcomes_state(
    state: &mut MemoryState,
    runner_id: &str,
    commits: &[TaskCommit],
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(), TaskError> {
    let conf = state
        .conf
        .clone()
        .ok_or_else(|| TaskError::InvalidConfig("task store is not initialized".into()))?;
    for commit in commits {
        let retry = configured_retry(state, commit.lane)?;
        if !memory_commit_allowed(state, runner_id, commit, now)? {
            continue;
        }
        let Some(task) = owned_task_mut(&mut state.tasks, commit.task_id, runner_id) else {
            continue;
        };
        if task.lane != commit.lane.as_str() {
            return Err(TaskError::UnknownLane(task.lane.clone()));
        }
        apply_outcome(task, commit.outcome.clone(), retry, now)?;
        finalize_idempotency(task, &conf, now)?;
    }
    Ok(())
}

/// Fences outcomes from runners that no longer own an opt-in lane.
fn memory_commit_allowed(
    state: &MemoryState,
    runner_id: &str,
    commit: &TaskCommit,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<bool, TaskError> {
    let locked = state
        .conf
        .as_ref()
        .and_then(|conf| conf.lanes.iter().find(|lane| lane.lane() == commit.lane))
        .is_some_and(|lane| lane.lane_lock().is_some());
    if !locked {
        return Ok(true);
    }
    let Some(owner) = state.lane_locks.get(commit.lane.as_str()) else {
        return Err(TaskError::InvalidConfig(format!(
            "task lane lock '{}' is not initialized",
            commit.lane
        )));
    };
    Ok(owner.owner_id.as_deref() == Some(runner_id)
        && owner.owner_token.as_deref() == commit.owner_token.as_deref()
        && memory_owner_live(owner, now))
}

/// Renews one bounded owned lease set while the in-memory transaction lock is held.
fn renew_leases_state(
    state: &mut MemoryState,
    runner_id: &str,
    leases: &[TaskLease],
    lease_duration: Duration,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Vec<TaskId>, TaskError> {
    let mut lost = Vec::new();
    for lease in leases {
        if !memory_lease_allowed(state, runner_id, lease, now)? {
            lost.push(lease.task_id);
            continue;
        }
        if let Some(task) = owned_task_mut(&mut state.tasks, lease.task_id, runner_id) {
            task.leased_until = checked_deadline(now, lease_duration)?;
            task.updated_at = now;
        } else {
            lost.push(lease.task_id);
        }
    }
    Ok(lost)
}

/// Fences task renewal after locked-lane ownership changes.
fn memory_lease_allowed(
    state: &MemoryState,
    runner_id: &str,
    lease: &TaskLease,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<bool, TaskError> {
    let Some(task) = state.tasks.iter().find(|task| task.id() == lease.task_id) else {
        return Ok(false);
    };
    if task.lane != lease.lane.as_str() {
        return Ok(false);
    }
    let locked = state
        .conf
        .as_ref()
        .and_then(|conf| conf.lanes.iter().find(|lane| lane.lane() == lease.lane))
        .is_some_and(|lane| lane.lane_lock().is_some());
    if !locked {
        return Ok(true);
    }
    let owner = state.lane_locks.get(lease.lane.as_str()).ok_or_else(|| {
        TaskError::InvalidConfig(format!(
            "task lane lock '{}' is not initialized",
            lease.lane
        ))
    })?;
    Ok(owner.owner_id.as_deref() == Some(runner_id)
        && owner.owner_token.as_deref() == lease.owner_token.as_deref()
        && memory_owner_live(owner, now))
}

fn bounded_claim(claim: &LaneClaim, batch_size: usize) -> LaneClaim {
    LaneClaim {
        lane: claim.lane,
        limit: claim.limit.min(batch_size),
        owner: claim.owner.clone(),
    }
}

fn due_count(state: &MemoryState, lane: TaskLane, now: chrono::DateTime<chrono::Utc>) -> usize {
    due_indices(&state.tasks, lane.as_str(), now).len()
}

fn claim_lane_state(
    state: &mut MemoryState,
    runner_id: &str,
    reservation: ClaimReservation,
    lease_duration: Duration,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<LanePoll, TaskError> {
    let mut poll = claim_lane(
        &mut state.tasks,
        runner_id,
        &reservation.claim,
        reservation.permits,
        now,
        lease_duration,
    )?;
    let task_wake = task_deadline(&state.tasks, reservation.claim.lane.as_str(), now);
    poll.next_wake_in = effective_lane_wake(
        reservation.permits < reservation.candidates,
        reservation.rate_wake,
        task_wake,
    );
    Ok(poll)
}

/// Fails legacy running rows that cannot be safely reclaimed by lease expiry.
fn fail_unleased_running(
    tasks: &mut [TaskRecord],
    conf: &TaskStoreConf,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(), TaskError> {
    for task in tasks {
        if task.status != TaskStatus::Running || task.leased_until.is_some() {
            continue;
        }
        fail(task, "Running task has no lease deadline".into(), now);
        task.locked_by = None;
        task.updated_at = now;
        finalize_idempotency(task, conf, now)?;
    }
    Ok(())
}

/// Marks expired leases terminal once their invocation budget is exhausted.
fn fail_exhausted(
    tasks: &mut [TaskRecord],
    lane: &str,
    now: chrono::DateTime<chrono::Utc>,
    conf: &TaskStoreConf,
    retry: TaskRetry,
) -> Result<(), TaskError> {
    for task in tasks {
        let expired = task.lane == lane
            && task.status == TaskStatus::Running
            && task.leased_until.is_some_and(|lease| lease <= now);
        if !expired || !retry.exhausted(task.attempts)? {
            continue;
        }
        fail(task, "Maximum task attempts exhausted".into(), now);
        task.locked_by = None;
        task.leased_until = None;
        task.updated_at = now;
        finalize_idempotency(task, conf, now)?;
    }
    Ok(())
}

/// Rejects low-level writes that bypass the typed client's lane validation.
fn validate_write_lanes(state: &MemoryState, writes: &[TaskWrite]) -> Result<(), TaskError> {
    for write in writes {
        require_lane(state, &write.record.lane)?;
        require_handler(state, &write.record.name)?;
    }
    Ok(())
}

fn require_handler(state: &MemoryState, handler: &str) -> Result<(), TaskError> {
    state
        .conf
        .as_ref()
        .is_some_and(|conf| conf.handlers.iter().any(|name| name == handler))
        .then_some(())
        .ok_or_else(|| TaskError::TaskNotFound(handler.into()))
}

/// Validates one persisted lane name against initialized store policy.
fn require_lane(state: &MemoryState, lane: &str) -> Result<(), TaskError> {
    let configured = state
        .conf
        .as_ref()
        .is_some_and(|conf| conf.lanes.iter().any(|entry| entry.lane().as_str() == lane));
    configured
        .then_some(())
        .ok_or_else(|| TaskError::UnknownLane(lane.into()))
}

/// Claims one in-memory lane while preserving candidate saturation evidence.
fn claim_lane(
    tasks: &mut [TaskRecord],
    runner_id: &str,
    claim: &LaneClaim,
    permit_limit: usize,
    now: chrono::DateTime<chrono::Utc>,
    default_lease: Duration,
) -> Result<LanePoll, TaskError> {
    let requested = claim.limit;
    let claim_count = requested.min(permit_limit);
    let mut candidates = due_indices(tasks, claim.lane.as_str(), now);
    candidates.sort_by_key(|index| tasks.get(*index).map(|task| readiness(task, now)));
    let saturated = requested > 0 && candidates.len() >= requested;
    let reclaimed = candidates
        .iter()
        .take(claim_count)
        .filter(|index| {
            tasks
                .get(**index)
                .is_some_and(|task| task.status == TaskStatus::Running)
        })
        .count();
    let mut claimed = Vec::with_capacity(claim_count);
    for index in candidates.into_iter().take(claim_count) {
        let task = tasks.get_mut(index).ok_or_else(|| {
            TaskError::TaskExecutionError("task candidate disappeared during claim".into())
        })?;
        claim_task(task, runner_id, now, default_lease)?;
        claimed.push(task.clone());
    }
    Ok(LanePoll {
        lane: claim.lane,
        tasks: claimed,
        reclaimed,
        saturated,
        next_wake_in: None,
        owner: None,
    })
}

/// Finds rows eligible by readiness or expired lease within one lane.
fn due_indices(tasks: &[TaskRecord], lane: &str, now: chrono::DateTime<chrono::Utc>) -> Vec<usize> {
    tasks
        .iter()
        .enumerate()
        .filter_map(|(index, task)| (task.lane == lane && is_due(task, now)).then_some(index))
        .collect()
}

fn is_due(task: &TaskRecord, now: chrono::DateTime<chrono::Utc>) -> bool {
    (task.status == TaskStatus::Pending && task.ready_at.is_none_or(|time| time <= now))
        || (task.status == TaskStatus::Running && task.leased_until.is_some_and(|time| time <= now))
}

/// Produces deterministic readiness and submission ordering for one candidate.
fn readiness(
    task: &TaskRecord,
    now: chrono::DateTime<chrono::Utc>,
) -> (
    chrono::DateTime<chrono::Utc>,
    chrono::DateTime<chrono::Utc>,
    TaskId,
) {
    let ready = if task.status == TaskStatus::Running {
        task.leased_until.unwrap_or(now)
    } else {
        task.ready_at.unwrap_or(now)
    };
    (ready, task.created_at, task.id)
}

/// Assigns one bounded lease to the requesting in-memory runner.
fn claim_task(
    task: &mut TaskRecord,
    runner_id: &str,
    now: chrono::DateTime<chrono::Utc>,
    default_lease: Duration,
) -> Result<(), TaskError> {
    let lease = task
        .lease_duration_ms
        .map(|milliseconds| Duration::from_millis(milliseconds.max(0) as u64))
        .unwrap_or(default_lease);
    let leased_until = now
        .checked_add_signed(chrono_duration(lease)?)
        .ok_or_else(|| TaskError::InvalidConfig("task lease exceeds timestamp range".into()))?;
    task.attempts = task
        .attempts
        .checked_add(1)
        .ok_or_else(|| TaskError::TaskExecutionError("task attempt count overflowed".into()))?;
    task.status = TaskStatus::Running;
    task.locked_by = Some(runner_id.into());
    task.leased_until = Some(leased_until);
    task.updated_at = now;
    Ok(())
}

/// Refills and reserves one lane bucket while holding the store mutex.
fn reserve_permits(
    state: &mut MemoryState,
    lane: crate::tasks::TaskLane,
    limit: usize,
    rate: Option<crate::tasks::TaskRate>,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(usize, Option<Duration>), TaskError> {
    if limit == 0 {
        return Ok((0, None));
    }
    let Some(rate) = rate else {
        return Ok((limit, None));
    };
    let bucket = state.rates.entry(lane.to_string()).or_insert(RateBucket {
        tokens_micros: i64::from(rate.burst_size()).saturating_mul(crate::tasks::rate::TOKEN_SCALE),
        updated_at: now,
    });
    crate::tasks::rate::refill(&mut bucket.tokens_micros, &mut bucket.updated_at, rate, now)?;
    let available = usize::try_from(bucket.tokens_micros / crate::tasks::rate::TOKEN_SCALE)
        .unwrap_or(usize::MAX);
    let permits = available.min(limit);
    bucket.tokens_micros = bucket.tokens_micros.saturating_sub(
        i64::try_from(permits)
            .unwrap_or(i64::MAX)
            .saturating_mul(crate::tasks::rate::TOKEN_SCALE),
    );
    let wake = crate::tasks::rate::next_permit(bucket.tokens_micros, rate, bucket.updated_at, now);
    Ok((permits, wake))
}

/// Resolves global rate policy from initialized store state rather than caller input.
fn configured_rate(
    state: &MemoryState,
    lane: crate::tasks::TaskLane,
) -> Result<Option<crate::tasks::TaskRate>, TaskError> {
    state
        .conf
        .as_ref()
        .and_then(|conf| conf.lanes.iter().find(|entry| entry.lane() == lane))
        .map(crate::tasks::TaskLaneConf::global_rate)
        .ok_or_else(|| TaskError::UnknownLane(lane.to_string()))
}

fn configured_retry(
    state: &MemoryState,
    lane: crate::tasks::TaskLane,
) -> Result<TaskRetry, TaskError> {
    state
        .conf
        .as_ref()
        .and_then(|conf| conf.lanes.iter().find(|entry| entry.lane() == lane))
        .map(crate::tasks::TaskLaneConf::retry_policy)
        .ok_or_else(|| TaskError::UnknownLane(lane.to_string()))
}

/// Applies one submission to a cloned batch so conflicts remain atomic.
fn stage_write(
    existing: &[TaskRecord],
    staged: &mut Vec<TaskRecord>,
    mut write: TaskWrite,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<TaskReceipt, TaskError> {
    write.record.created_at = now;
    write.record.updated_at = now;
    write.record.ready_at = match write.initial_delay {
        Some(delay) => checked_deadline(now, delay)?,
        None => Some(now),
    };
    if existing
        .iter()
        .chain(staged.iter())
        .any(|task| task.id == write.record.id)
    {
        return Err(TaskError::AlreadyExists(write.record.id.to_string()));
    }
    if let Some(owner) = matching_key(existing, staged, &write.record, now) {
        let same = owner.idempotency_fingerprint == write.record.idempotency_fingerprint;
        return if same {
            Ok(TaskReceipt::Existing(owner.id))
        } else if write.ignore_conflicts {
            Ok(TaskReceipt::Ignored(owner.id))
        } else {
            Err(TaskError::IdempotencyConflict(owner.id))
        };
    }
    let id = write.record.id;
    staged.push(write.record);
    Ok(TaskReceipt::Queued(id))
}

/// Finds the newest still-held handler-scoped idempotency key.
fn matching_key<'a>(
    existing: &'a [TaskRecord],
    staged: &'a [TaskRecord],
    record: &TaskRecord,
    now: chrono::DateTime<chrono::Utc>,
) -> Option<&'a TaskRecord> {
    let key = record.idempotency_key.as_deref()?;
    staged
        .iter()
        .rev()
        .chain(existing.iter().rev())
        .find(|task| {
            task.name == record.name
                && task.idempotency_key.as_deref() == Some(key)
                && idempotency_held(task, now)
        })
}

fn idempotency_held(task: &TaskRecord, now: chrono::DateTime<chrono::Utc>) -> bool {
    is_active(task.status)
        || task
            .idempotency_expires_at
            .is_some_and(|expiry| expiry > now)
}

/// Applies a payload-free lifecycle transition to an owned task record.
fn apply_outcome(
    task: &mut TaskRecord,
    outcome: TaskOutcome,
    retry_policy: TaskRetry,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(), TaskError> {
    let preserve_resume = matches!(outcome, TaskOutcome::Retry { .. });
    match outcome {
        TaskOutcome::Complete => complete(task, TaskStatus::Succeeded, now),
        TaskOutcome::Suspend { state } => suspend(task, state),
        TaskOutcome::Sleep { state, delay } => sleep(task, state, delay, now)?,
        TaskOutcome::Retry { error } => retry(task, retry_policy, error, now)?,
        TaskOutcome::Fail { error } => fail(task, error, now),
    }
    if !preserve_resume {
        task.resume_input = None;
    }
    task.locked_by = None;
    task.leased_until = None;
    task.updated_at = now;
    Ok(())
}

fn complete(task: &mut TaskRecord, status: TaskStatus, now: chrono::DateTime<chrono::Utc>) {
    task.status = status;
    task.ready_at = None;
    task.completed_at = Some(now);
}

fn suspend(task: &mut TaskRecord, state: String) {
    task.status = TaskStatus::Suspended;
    task.state = Some(state);
    task.ready_at = None;
}

fn sleep(
    task: &mut TaskRecord,
    state: String,
    delay: Duration,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(), TaskError> {
    task.status = TaskStatus::Pending;
    task.state = Some(state);
    task.ready_at = checked_deadline(now, delay)?;
    Ok(())
}

/// Schedules another attempt or marks the task failed at its attempt bound.
fn retry(
    task: &mut TaskRecord,
    policy: TaskRetry,
    error: String,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(), TaskError> {
    task.last_error = Some(error);
    if policy.exhausted(task.attempts)? {
        complete(task, TaskStatus::Failed, now);
        return Ok(());
    }
    task.status = TaskStatus::Pending;
    task.ready_at = checked_deadline(now, policy.delay(task.attempts)?)?;
    Ok(())
}

fn fail(task: &mut TaskRecord, error: String, now: chrono::DateTime<chrono::Utc>) {
    task.last_error = Some(error);
    complete(task, TaskStatus::Failed, now);
}

fn checked_deadline(
    now: chrono::DateTime<chrono::Utc>,
    delay: Duration,
) -> Result<Option<chrono::DateTime<chrono::Utc>>, TaskError> {
    now.checked_add_signed(chrono_duration(delay)?)
        .map(Some)
        .ok_or_else(|| TaskError::InvalidConfig("task delay exceeds timestamp range".into()))
}

/// Releases or archives a key when its task reaches a terminal status.
fn finalize_idempotency(
    task: &mut TaskRecord,
    conf: &TaskStoreConf,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(), TaskError> {
    if !matches!(task.status, TaskStatus::Succeeded | TaskStatus::Failed) {
        return Ok(());
    }
    if task.idempotency_key.is_none() {
        return Ok(());
    }
    let policy = conf.idempotency_for(&task.name).ok_or_else(|| {
        TaskError::InvalidConfig(format!("task '{}' has no idempotency policy", task.name))
    })?;
    task.idempotency_expires_at = match policy {
        IdempotencyRetention::ActiveOnly => None,
        IdempotencyRetention::RetainFor(duration) => checked_deadline(now, duration)?,
    };
    Ok(())
}

/// Selects a task only while the committing runner still owns its lease.
fn owned_task_mut<'a>(
    tasks: &'a mut [TaskRecord],
    id: TaskId,
    runner_id: &str,
) -> Option<&'a mut TaskRecord> {
    tasks.iter_mut().find(|task| {
        task.id == id
            && task.status == TaskStatus::Running
            && task.locked_by.as_deref() == Some(runner_id)
    })
}

/// Returns the earliest future readiness or lease-expiry deadline.
fn task_deadline(
    tasks: &[TaskRecord],
    lane: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Option<Duration> {
    tasks
        .iter()
        .filter(|task| task.lane == lane)
        .filter_map(|task| {
            let deadline = match task.status {
                TaskStatus::Pending => task.ready_at,
                TaskStatus::Running => task.leased_until,
                _ => None,
            }?;
            (deadline > now)
                .then(|| (deadline - now).to_std().ok())
                .flatten()
        })
        .min()
}

/// Combines future work and token readiness without polling a blocked lane early.
fn effective_lane_wake(
    rate_blocked: bool,
    rate_wake: Option<Duration>,
    task_wake: Option<Duration>,
) -> Option<Duration> {
    if rate_blocked {
        return rate_wake;
    }
    task_wake.map(|task| rate_wake.map_or(task, |permit| permit.max(task)))
}

/// Applies every bounded console and inspection filter to one record.
fn matches_filter(task: &TaskRecord, filter: &TaskFilter) -> bool {
    filter.status.is_none_or(|status| task.status == status)
        && filter.name.as_deref().is_none_or(|name| task.name == name)
        && filter.lane.as_deref().is_none_or(|lane| task.lane == lane)
        && filter
            .idempotency_key
            .as_deref()
            .is_none_or(|key| task.idempotency_key.as_deref() == Some(key))
        && filter
            .created_from
            .is_none_or(|time| task.created_at >= time)
        && filter.created_to.is_none_or(|time| task.created_at <= time)
        && matches_query(task, filter.query.as_deref())
}

/// Performs the in-memory store's case-insensitive diagnostic search.
fn matches_query(task: &TaskRecord, query: Option<&str>) -> bool {
    let Some(query) = query else { return true };
    let query = query.to_lowercase();
    task.name.to_lowercase().contains(&query)
        || task.lane.to_lowercase().contains(&query)
        || task
            .idempotency_key
            .as_ref()
            .is_some_and(|value| value.to_lowercase().contains(&query))
        || task
            .last_error
            .as_ref()
            .is_some_and(|value| value.to_lowercase().contains(&query))
}

/// Builds the canonical one-indexed task inspection page.
fn page(records: Vec<TaskRecord>, filter: &TaskFilter) -> crate::routes::Page<TaskRecord> {
    let total = match i64::try_from(records.len()) {
        Ok(value) => value,
        Err(_) => i64::MAX,
    };
    let offset = filter
        .page
        .saturating_sub(1)
        .saturating_mul(filter.per_page);
    let items = records
        .into_iter()
        .skip(offset)
        .take(filter.per_page)
        .collect();
    crate::routes::Page::new(items, total, filter.page, filter.per_page)
}

/// Creates buckets only for lanes with configured global rate limits.
fn initialize_rates(state: &mut MemoryState, conf: &TaskStoreConf) {
    let now = chrono::Utc::now();
    for lane in &conf.lanes {
        if let Some(rate) = lane.global_rate() {
            state
                .rates
                .entry(lane.lane().to_string())
                .or_insert(RateBucket {
                    tokens_micros: i64::from(rate.burst_size())
                        .saturating_mul(crate::tasks::rate::TOKEN_SCALE),
                    updated_at: now,
                });
        }
    }
}

/// Adds missing in-memory lane-owner rows without resetting current lifecycle state.
fn initialize_lane_locks(state: &mut MemoryState, conf: &TaskStoreConf) {
    for lane in conf.lanes.iter().filter(|lane| lane.lane_lock().is_some()) {
        state
            .lane_locks
            .entry(lane.lane().to_string())
            .or_insert(MemoryLaneLock {
                owner_id: None,
                owner_token: None,
                leased_until: None,
                phase: LaneOwnerPhase::Active,
                flushing: false,
                empty_since: None,
                generation: 0,
                hook_retry_at: None,
                last_hook_error: None,
            });
    }
}

/// Prevents active work from silently falling into another configured lane.
fn reject_orphaned_tasks(tasks: &[TaskRecord], conf: &TaskStoreConf) -> Result<(), TaskError> {
    let configured = conf
        .lanes
        .iter()
        .map(|lane| lane.lane().as_str())
        .collect::<std::collections::HashSet<_>>();
    if let Some(task) = tasks
        .iter()
        .find(|task| is_active(task.status) && !configured.contains(task.lane.as_str()))
    {
        return Err(TaskError::UnknownLane(task.lane.clone()));
    }
    let handlers = conf
        .handlers
        .iter()
        .map(String::as_str)
        .collect::<std::collections::HashSet<_>>();
    if let Some(task) = tasks
        .iter()
        .find(|task| is_active(task.status) && !handlers.contains(task.name.as_str()))
    {
        return Err(TaskError::TaskNotFound(task.name.clone()));
    }
    Ok(())
}

fn is_active(status: TaskStatus) -> bool {
    matches!(
        status,
        TaskStatus::Pending | TaskStatus::Running | TaskStatus::Suspended
    )
}

fn is_reassignable(status: TaskStatus) -> bool {
    matches!(status, TaskStatus::Pending | TaskStatus::Suspended)
}

fn chrono_duration(duration: Duration) -> Result<chrono::Duration, TaskError> {
    chrono::Duration::from_std(duration).map_err(|_| {
        TaskError::InvalidConfig("task duration exceeds supported timestamp range".into())
    })
}
