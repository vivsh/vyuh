//! In-memory reference implementation of the per-lane task-store contract.

use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::tasks::{
    AbstractTaskStore, IdempotencyRetention, LaneClaim, LanePoll, ScheduledTaskWrite, TaskCommit,
    TaskError, TaskFilter, TaskId, TaskLane, TaskOutcome, TaskPoll, TaskReceipt, TaskRecord,
    TaskRetry, TaskScheduleSnapshot, TaskStatus, TaskStoreConf, TaskTick, TaskWrite,
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
}

struct ClaimReservation {
    claim: LaneClaim,
    permits: usize,
    rate_wake: Option<Duration>,
    candidates: usize,
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
        task_ids: &[TaskId],
    ) -> Result<Vec<TaskId>, TaskError> {
        let mut state = self.state.lock().await;
        renew_leases_state(
            &mut state,
            runner_id,
            task_ids,
            self.lease_duration,
            chrono::Utc::now(),
        )
    }

    async fn tick(
        &self,
        runner_id: &str,
        claims: &[LaneClaim],
        commits: &[TaskCommit],
        renewals: &[TaskId],
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

/// Renews one bounded owned lease set while the in-memory transaction lock is held.
fn renew_leases_state(
    state: &mut MemoryState,
    runner_id: &str,
    task_ids: &[TaskId],
    lease_duration: Duration,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Vec<TaskId>, TaskError> {
    let mut lost = Vec::new();
    for task_id in task_ids {
        if let Some(task) = owned_task_mut(&mut state.tasks, *task_id, runner_id) {
            task.leased_until = checked_deadline(now, lease_duration)?;
            task.updated_at = now;
        } else {
            lost.push(*task_id);
        }
    }
    Ok(lost)
}

fn bounded_claim(claim: &LaneClaim, batch_size: usize) -> LaneClaim {
    LaneClaim {
        lane: claim.lane,
        limit: claim.limit.min(batch_size),
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
