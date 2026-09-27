//! In-memory reference implementation of the per-lane task-store contract.

use std::{collections::HashMap, sync::Arc, time::Duration};

#[path = "memory_all.rs"]
mod all;
#[path = "memory_workflow.rs"]
mod workflow;

#[cfg(test)]
#[path = "tests/memory.rs"]
pub(crate) mod tests;

use crate::tasks::{
    AbstractTaskStore, IdempotencyRetention, LaneClaim, LaneHookAction, LaneHookResult,
    LaneOwnerPhase, LaneOwnerPoll, LanePoll, ScheduledTaskWrite, TaskCommit, TaskFilter, TaskId,
    TaskLane, TaskLease, TaskOutcome, TaskPoll, TaskReceipt, TaskRecord, TaskRetry,
    TaskRuntimeError, TaskScheduleSnapshot, TaskStatus, TaskStoreConf, TaskTick, TaskWrite,
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
    waits: HashMap<TaskId, all::TaskWait>,
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
    async fn cancel(&self, id: TaskId) -> Result<bool, TaskRuntimeError> {
        let mut state = self.state.lock().await;
        let Some(task) = state.tasks.iter_mut().find(|task| task.id == id) else {
            return Ok(false);
        };
        if task.cancelled || !is_active(task.status) {
            return Ok(false);
        }
        let now = chrono::Utc::now();
        task.cancelled = true;
        if task.status == TaskStatus::Suspended {
            task.status = TaskStatus::Pending;
            task.ready_at = Some(now);
        } else if task.status == TaskStatus::Pending && task.ready_at.is_some_and(|at| at > now) {
            task.ready_at = Some(now);
        }
        task.updated_at = now;
        Ok(true)
    }

    async fn initialize(&self, conf: TaskStoreConf) -> Result<(), TaskRuntimeError> {
        let mut current = self.state.lock().await;
        // Initialization stages legacy recovery once; steady-state turns journal only touched rows.
        let mut state = current.clone();
        let fingerprint = crate::tasks::store::policy_fingerprint(&conf);
        if state
            .fingerprint
            .as_deref()
            .is_some_and(|value| value != fingerprint)
        {
            return Err(TaskRuntimeError::InvalidConfig(
                "task workers use incompatible lane or global rate policies".into(),
            ));
        }
        let now = chrono::Utc::now();
        let deliveries = fail_unleased_running(&mut state.tasks, &conf, now)?;
        reject_orphaned_tasks(&state.tasks, &conf)?;
        initialize_rates(&mut state, &conf);
        initialize_lane_locks(&mut state, &conf);
        state.fingerprint = Some(fingerprint);
        state.conf = Some(conf);
        workflow::finalize_workflow(&mut state, Vec::new(), deliveries, now);
        *current = state;
        Ok(())
    }

    async fn claim_tasks(
        &self,
        runner_id: &str,
        claims: &[LaneClaim],
    ) -> Result<TaskPoll, TaskRuntimeError> {
        Ok(self.tick(runner_id, claims, &[], &[]).await?.poll)
    }

    async fn commit_outcomes(
        &self,
        runner_id: &str,
        commits: &[TaskCommit],
    ) -> Result<(), TaskRuntimeError> {
        self.tick(runner_id, &[], commits, &[]).await?;
        Ok(())
    }

    async fn renew_leases(
        &self,
        runner_id: &str,
        leases: &[TaskLease],
    ) -> Result<Vec<TaskId>, TaskRuntimeError> {
        Ok(self.tick(runner_id, &[], &[], leases).await?.lost)
    }

    async fn tick(
        &self,
        runner_id: &str,
        claims: &[LaneClaim],
        commits: &[TaskCommit],
        renewals: &[TaskLease],
    ) -> Result<TaskTick, TaskRuntimeError> {
        let mut state = self.state.lock().await;
        let now = chrono::Utc::now();
        workflow::atomic_turn(
            &mut state,
            runner_id,
            claims,
            commits,
            renewals,
            self.batch_size,
            self.lease_duration,
            now,
        )
    }

    async fn store_tasks(
        &self,
        writes: Vec<TaskWrite>,
    ) -> Result<Vec<TaskReceipt>, TaskRuntimeError> {
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

    async fn schedule_snapshot(
        &self,
        names: &[String],
    ) -> Result<TaskScheduleSnapshot, TaskRuntimeError> {
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
    ) -> Result<Option<TaskReceipt>, TaskRuntimeError> {
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

    async fn reassign_lane(&self, from: &str, to: &str) -> Result<u64, TaskRuntimeError> {
        let mut state = self.state.lock().await;
        require_lane(&state, to)?;
        if state
            .tasks
            .iter()
            .any(|task| task.lane == from && task.status == TaskStatus::Running)
        {
            return Err(TaskRuntimeError::LaneBusy(from.into()));
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

    async fn resume(&self, id: TaskId, input: String) -> Result<bool, TaskRuntimeError> {
        crate::tasks::result::validate_resume(&input)?;
        let mut state = self.state.lock().await;
        let now = chrono::Utc::now();
        let Some(task) = state.tasks.iter_mut().find(|task| task.id == id) else {
            return Ok(false);
        };
        if task.cancelled || task.status != TaskStatus::Suspended {
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
    ) -> Result<crate::routes::Page<TaskRecord>, TaskRuntimeError> {
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

    async fn get_task(&self, id: TaskId) -> Result<Option<TaskRecord>, TaskRuntimeError> {
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
    deliveries: &mut Vec<(TaskId, String)>,
    undo: &mut Vec<TaskRecord>,
) -> Result<TaskPoll, TaskRuntimeError> {
    let conf = state
        .conf
        .clone()
        .ok_or_else(|| TaskRuntimeError::InvalidConfig("task store is not initialized".into()))?;
    let mut lanes = Vec::with_capacity(claims.len());
    for claim in claims {
        let lane_conf = conf
            .lanes
            .iter()
            .find(|lane| lane.lane() == claim.lane)
            .ok_or_else(|| TaskRuntimeError::UnknownLane(claim.lane.to_string()))?;
        if lane_conf.lane_lock().is_some() {
            lanes.push(claim_owned_state(
                state,
                runner_id,
                claim,
                lane_conf,
                lease_duration,
                now,
                deliveries,
                undo,
            )?);
            continue;
        }
        let bounded = bounded_claim(claim, batch_size);
        let retry = configured_retry(state, bounded.lane)?;
        fail_exhausted(
            &mut state.tasks,
            bounded.lane.as_str(),
            bounded.limit,
            now,
            &conf,
            retry,
            deliveries,
            undo,
        )?;
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
            undo,
        )?);
    }
    Ok(TaskPoll { lanes })
}

#[path = "memory_owner.rs"]
mod owner;
use owner::*;

/// Commits one bounded batch of outcomes while the in-memory transaction lock is held.
fn commit_outcomes_state(
    state: &mut MemoryState,
    runner_id: &str,
    commits: &[TaskCommit],
    now: chrono::DateTime<chrono::Utc>,
    undo: &mut Vec<TaskRecord>,
) -> Result<
    (
        Vec<TaskRecord>,
        Vec<(TaskId, String)>,
        Vec<(TaskId, all::TaskWait)>,
    ),
    TaskRuntimeError,
> {
    let (updates, children, deliveries, waits) =
        workflow::stage_outcomes(state, runner_id, commits, now)?;
    for (index, task) in updates {
        let current = state.tasks.get_mut(index).ok_or_else(|| {
            TaskRuntimeError::TaskExecutionError("staged task disappeared".into())
        })?;
        undo.push(std::mem::replace(current, task));
    }
    Ok((children, deliveries, waits))
}

/// Fences outcomes from runners that no longer own an opt-in lane.
fn memory_commit_allowed(
    state: &MemoryState,
    runner_id: &str,
    commit: &TaskCommit,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<bool, TaskRuntimeError> {
    let locked = state
        .conf
        .as_ref()
        .and_then(|conf| conf.lanes.iter().find(|lane| lane.lane() == commit.lane))
        .is_some_and(|lane| lane.lane_lock().is_some());
    if !locked {
        return Ok(true);
    }
    let Some(owner) = state.lane_locks.get(commit.lane.as_str()) else {
        return Err(TaskRuntimeError::InvalidConfig(format!(
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
    undo: &mut Vec<TaskRecord>,
    deliveries: &mut Vec<(TaskId, String)>,
) -> Result<(Vec<TaskId>, Vec<TaskId>), TaskRuntimeError> {
    let mut lost = Vec::new();
    let mut cancelled = Vec::new();
    for lease in leases {
        if !memory_lease_allowed(state, runner_id, lease, now)? {
            lost.push(lease.task_id);
            continue;
        }
        if let Some(task) = owned_task_mut(&mut state.tasks, lease.task_id, runner_id)
            .filter(|task| task.lane == lease.lane.as_str())
        {
            undo.push(task.clone());
            if task.cancelled {
                finalize_cancelled(task, state.conf.as_ref(), now, deliveries)?;
                lost.push(lease.task_id);
                cancelled.push(lease.task_id);
                continue;
            }
            task.leased_until = Some(lease_deadline(task, lease_duration, now)?);
            task.updated_at = now;
        } else {
            lost.push(lease.task_id);
            if state.tasks.iter().any(|task| {
                task.id == lease.task_id
                    && task.lane == lease.lane.as_str()
                    && task.cancelled
                    && task.status == TaskStatus::Failed
            }) {
                cancelled.push(lease.task_id);
            }
        }
    }
    Ok((lost, cancelled))
}

/// Applies the ordinary terminal transition and queues its atomic parent delivery.
fn finalize_cancelled(
    task: &mut TaskRecord,
    conf: Option<&TaskStoreConf>,
    now: chrono::DateTime<chrono::Utc>,
    deliveries: &mut Vec<(TaskId, String)>,
) -> Result<(), TaskRuntimeError> {
    apply_outcome(
        task,
        super::workflow::cancellation(),
        TaskRetry::default(),
        now,
    )?;
    if let Some(conf) = conf {
        finalize_idempotency(task, conf, now)?;
    }
    workflow::queue_delivery(task, deliveries)
}

/// Fences task renewal after locked-lane ownership changes.
fn memory_lease_allowed(
    state: &MemoryState,
    runner_id: &str,
    lease: &TaskLease,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<bool, TaskRuntimeError> {
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
        TaskRuntimeError::InvalidConfig(format!(
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
    state
        .tasks
        .iter()
        .filter(|task| !task.cancelled && task.lane == lane.as_str() && is_due(task, now))
        .count()
}

fn claim_lane_state(
    state: &mut MemoryState,
    runner_id: &str,
    reservation: ClaimReservation,
    lease_duration: Duration,
    now: chrono::DateTime<chrono::Utc>,
    undo: &mut Vec<TaskRecord>,
) -> Result<LanePoll, TaskRuntimeError> {
    let mut poll = claim_lane(
        &mut state.tasks,
        runner_id,
        &reservation.claim,
        reservation.permits,
        now,
        lease_duration,
        undo,
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
) -> Result<Vec<(TaskId, String)>, TaskRuntimeError> {
    let mut deliveries = Vec::new();
    for task in tasks {
        if task.status != TaskStatus::Running || task.leased_until.is_some() {
            continue;
        }
        let outcome = if task.cancelled {
            super::workflow::cancellation()
        } else {
            TaskOutcome::fail("Running task has no lease deadline")
        };
        apply_outcome(task, outcome, TaskRetry::default(), now)?;
        workflow::queue_delivery(task, &mut deliveries)?;
        task.locked_by = None;
        task.updated_at = now;
        finalize_idempotency(task, conf, now)?;
    }
    Ok(deliveries)
}

/// Marks expired leases terminal once their invocation budget is exhausted.
fn fail_exhausted(
    tasks: &mut [TaskRecord],
    lane: &str,
    limit: usize,
    now: chrono::DateTime<chrono::Utc>,
    conf: &TaskStoreConf,
    retry: TaskRetry,
    deliveries: &mut Vec<(TaskId, String)>,
    undo: &mut Vec<TaskRecord>,
) -> Result<(), TaskRuntimeError> {
    let mut candidates = due_indices(tasks, lane, now);
    candidates.sort_by_key(|index| tasks.get(*index).map(|task| readiness(task, now)));
    for index in candidates.into_iter().take(limit) {
        let task = tasks.get_mut(index).ok_or_else(|| {
            TaskRuntimeError::TaskExecutionError(
                "task candidate disappeared during finalization".into(),
            )
        })?;
        let expired = task.lane == lane
            && task.status == TaskStatus::Running
            && task.leased_until.is_some_and(|lease| lease <= now);
        let cancelled = task.cancelled && task.lane == lane && is_due(task, now);
        if !cancelled && (!expired || !retry.exhausted(task.step_attempts)?) {
            continue;
        }
        undo.push(task.clone());
        let outcome = if cancelled {
            super::workflow::cancellation()
        } else {
            TaskOutcome::fail("Maximum task attempts exhausted")
        };
        apply_outcome(task, outcome, retry, now)?;
        workflow::queue_delivery(task, deliveries)?;
        task.locked_by = None;
        task.leased_until = None;
        task.updated_at = now;
        finalize_idempotency(task, conf, now)?;
    }
    Ok(())
}

/// Rejects low-level writes that bypass the typed client's lane validation.
fn validate_write_lanes(state: &MemoryState, writes: &[TaskWrite]) -> Result<(), TaskRuntimeError> {
    for write in writes {
        require_lane(state, &write.record.lane)?;
        require_handler(state, &write.record.name)?;
    }
    Ok(())
}

fn require_handler(state: &MemoryState, handler: &str) -> Result<(), TaskRuntimeError> {
    state
        .conf
        .as_ref()
        .is_some_and(|conf| conf.handlers.iter().any(|(name, _)| name == handler))
        .then_some(())
        .ok_or_else(|| TaskRuntimeError::TaskNotFound(handler.into()))
}

/// Validates one persisted lane name against initialized store policy.
fn require_lane(state: &MemoryState, lane: &str) -> Result<(), TaskRuntimeError> {
    let configured = state
        .conf
        .as_ref()
        .is_some_and(|conf| conf.lanes.iter().any(|entry| entry.lane().as_str() == lane));
    configured
        .then_some(())
        .ok_or_else(|| TaskRuntimeError::UnknownLane(lane.into()))
}

/// Claims one in-memory lane while preserving candidate saturation evidence.
fn claim_lane(
    tasks: &mut [TaskRecord],
    runner_id: &str,
    claim: &LaneClaim,
    permit_limit: usize,
    now: chrono::DateTime<chrono::Utc>,
    default_lease: Duration,
    undo: &mut Vec<TaskRecord>,
) -> Result<LanePoll, TaskRuntimeError> {
    let requested = claim.limit;
    let claim_count = requested.min(permit_limit);
    let mut candidates = due_indices(tasks, claim.lane.as_str(), now);
    candidates.sort_by_key(|index| tasks.get(*index).map(|task| readiness(task, now)));
    let saturated = requested > 0 && candidates.len() >= requested;
    candidates.truncate(requested);
    candidates.retain(|index| tasks.get(*index).is_some_and(|task| !task.cancelled));
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
            TaskRuntimeError::TaskExecutionError("task candidate disappeared during claim".into())
        })?;
        undo.push(task.clone());
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
) -> Result<(), TaskRuntimeError> {
    let leased_until = lease_deadline(task, default_lease, now)?;
    task.attempts = task.attempts.checked_add(1).ok_or_else(|| {
        TaskRuntimeError::TaskExecutionError("task attempt count overflowed".into())
    })?;
    task.step_attempts = task.step_attempts.checked_add(1).ok_or_else(|| {
        TaskRuntimeError::TaskExecutionError("task step attempt count overflowed".into())
    })?;
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
) -> Result<(usize, Option<Duration>), TaskRuntimeError> {
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
) -> Result<Option<crate::tasks::TaskRate>, TaskRuntimeError> {
    state
        .conf
        .as_ref()
        .and_then(|conf| conf.lanes.iter().find(|entry| entry.lane() == lane))
        .map(crate::tasks::TaskLaneConf::global_rate)
        .ok_or_else(|| TaskRuntimeError::UnknownLane(lane.to_string()))
}

fn configured_retry(
    state: &MemoryState,
    lane: crate::tasks::TaskLane,
) -> Result<TaskRetry, TaskRuntimeError> {
    state
        .conf
        .as_ref()
        .and_then(|conf| conf.lanes.iter().find(|entry| entry.lane() == lane))
        .map(crate::tasks::TaskLaneConf::retry_policy)
        .ok_or_else(|| TaskRuntimeError::UnknownLane(lane.to_string()))
}

/// Applies one submission to a cloned batch so conflicts remain atomic.
fn stage_write(
    existing: &[TaskRecord],
    staged: &mut Vec<TaskRecord>,
    mut write: TaskWrite,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<TaskReceipt, TaskRuntimeError> {
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
        return Err(TaskRuntimeError::AlreadyExists(write.record.id.to_string()));
    }
    if let Some(owner) = matching_key(existing, staged, &write.record, now) {
        let same = owner.idempotency_fingerprint == write.record.idempotency_fingerprint;
        return if same {
            Ok(TaskReceipt::Existing(owner.id))
        } else if write.ignore_conflicts {
            Ok(TaskReceipt::Ignored(owner.id))
        } else {
            Err(TaskRuntimeError::IdempotencyConflict(owner.id))
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

/// Applies a lifecycle transition to an owned task record.
fn apply_outcome(
    task: &mut TaskRecord,
    outcome: TaskOutcome,
    retry_policy: TaskRetry,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(), TaskRuntimeError> {
    let preserve_resume = matches!(outcome, TaskOutcome::Retry { .. });
    match outcome {
        TaskOutcome::Complete => {
            task.last_result = Some(crate::tasks::result::UNIT_RESULT.into());
            complete(task, TaskStatus::Succeeded, now)
        }
        TaskOutcome::CompleteWith { output } => {
            match crate::tasks::result::validate_output(&output) {
                Ok(()) => {
                    task.last_result = Some(crate::tasks::result::success(&output));
                    complete(task, TaskStatus::Succeeded, now)
                }
                Err(error) => fail(task, error.to_string(), now),
            }
        }
        TaskOutcome::Suspend { state }
        | TaskOutcome::Spawn { state, .. }
        | TaskOutcome::All { state, .. } => suspend(task, state),
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
    task.last_result = None;
    task.step_attempts = 0;
    task.status = TaskStatus::Suspended;
    task.state = Some(state);
    task.ready_at = None;
}

fn sleep(
    task: &mut TaskRecord,
    state: String,
    delay: Duration,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(), TaskRuntimeError> {
    task.step_attempts = 0;
    task.last_result = None;
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
) -> Result<(), TaskRuntimeError> {
    task.last_result = Some(crate::tasks::result::failure(task.id, error));
    if policy.exhausted(task.step_attempts)? {
        complete(task, TaskStatus::Failed, now);
        return Ok(());
    }
    task.status = TaskStatus::Pending;
    task.ready_at = checked_deadline(now, policy.delay(task.step_attempts)?)?;
    Ok(())
}

fn fail(task: &mut TaskRecord, error: String, now: chrono::DateTime<chrono::Utc>) {
    task.last_result = Some(crate::tasks::result::failure(task.id, error));
    complete(task, TaskStatus::Failed, now);
}

#[path = "memory_utils.rs"]
mod utils;
use utils::*;
