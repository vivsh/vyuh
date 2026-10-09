//! In-memory reference implementation of the per-lane task-store contract.
#[cfg(test)]
use crate::tasks::TaskLane;

use super::lane_owner::{claim_owned_state, memory_owner_live};
use super::queries::{
    checked_deadline, due_indices, effective_lane_wake, finalize_idempotency,
    initialize_lane_locks, initialize_rates, is_active, is_due, is_reassignable, matches_filter,
    page, readiness, reject_orphaned_tasks, retire_nodes, task_deadline,
};
use super::workflow;
use crate::tasks::store::StoreFailure;
use crate::tasks::{
    CommitResult, LaneClaim, LaneOwnerPhase, LanePoll, ScheduledTaskWrite, TaskCommit, TaskFilter,
    TaskId, TaskKind, TaskOutcome, TaskPoll, TaskReceipt, TaskRecord, TaskRetry, TaskRuntimeError,
    TaskScheduleSnapshot, TaskStatus, TaskStore, TaskStoreConf, TaskWrite,
};
use std::{collections::HashMap, sync::Arc, time::Duration};

#[derive(Clone)]
pub(super) struct RateBucket {
    pub(super) tokens_micros: i64,
    pub(super) updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Clone, Default)]
pub(super) struct MemoryState {
    pub(super) nodes:
        HashMap<uuid::Uuid, (chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>,
    pub(super) tasks: Vec<TaskRecord>,
    pub(super) conf: Option<TaskStoreConf>,
    pub(super) rates: HashMap<String, RateBucket>,
    pub(super) schedules: HashMap<String, chrono::DateTime<chrono::Utc>>,
    pub(super) lane_locks: HashMap<String, MemoryLaneLock>,
    pub(super) waits: HashMap<TaskId, workflow::TaskWait>,
}

#[derive(Clone)]
pub(super) struct MemoryLaneLock {
    pub(super) owner_id: Option<String>,
    pub(super) owner_token: Option<String>,
    pub(super) leased_until: Option<chrono::DateTime<chrono::Utc>>,
    pub(super) phase: LaneOwnerPhase,
    pub(super) flushing: bool,
    pub(super) empty_since: Option<chrono::DateTime<chrono::Utc>>,
    pub(super) generation: i64,
    pub(super) hook_retry_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(super) last_hook_error: Option<String>,
}

pub(super) struct ClaimReservation {
    pub(super) claim: LaneClaim,
    pub(super) permits: usize,
    pub(super) rate_wake: Option<Duration>,
    pub(super) candidates: usize,
}

pub(super) struct MemoryOwnerTurn<'a> {
    pub(super) runner_id: &'a uuid::Uuid,
    pub(super) claim: &'a LaneClaim,
    pub(super) lane: &'a crate::tasks::TaskLaneConf,
    pub(super) lease_duration: Duration,
    pub(super) now: chrono::DateTime<chrono::Utc>,
}

/// In-memory task store for tests and local development.
///
/// This store is process-local and is not a durable or distributed coordinator.
#[derive(Clone)]
pub struct MemoryTaskStore {
    pub(super) state: Arc<tokio::sync::Mutex<MemoryState>>,
    pub(super) commit_batch_size: usize,
    pub(super) lease_duration: Duration,
}

impl MemoryTaskStore {
    /// Creates a process-local store with a bounded output batch; claims use lane policy.
    #[cfg(test)]
    pub fn new(batch_size: usize, conf: TaskStoreConf) -> Self {
        Self::with_lease_duration(batch_size, Duration::from_secs(300), conf)
    }

    /// Creates a process-local store with an explicit default lease duration.
    pub fn with_lease_duration(
        batch_size: usize,
        lease_duration: Duration,
        conf: TaskStoreConf,
    ) -> Self {
        Self {
            state: Arc::new(tokio::sync::Mutex::new(MemoryState {
                conf: Some(conf),
                ..MemoryState::default()
            })),
            commit_batch_size: batch_size.max(1),
            lease_duration,
        }
    }

    /// Returns the number of records retained by this store.
    #[cfg(test)]
    pub async fn task_count(&self) -> usize {
        self.state.lock().await.tasks.len()
    }
}

impl TaskStore for MemoryTaskStore {
    async fn register_node(&self, node: &uuid::Uuid) -> Result<(), TaskRuntimeError> {
        let mut state = self.state.lock().await;
        let now = chrono::Utc::now();
        state.nodes.entry(*node).or_insert((now, now));
        Ok(())
    }

    async fn heartbeat(
        &self,
        node: &uuid::Uuid,
        stale_after: Duration,
    ) -> Result<bool, TaskRuntimeError> {
        let mut state = self.state.lock().await;
        let now = chrono::Utc::now();
        let cutoff = chrono::Duration::from_std(stale_after)
            .ok()
            .and_then(|duration| now.checked_sub_signed(duration))
            .ok_or_else(|| {
                TaskRuntimeError::InvalidOptions(
                    "node retirement duration exceeds clock range".into(),
                )
            })?;
        let Some((_, heartbeat)) = state.nodes.get_mut(node) else {
            return Ok(false);
        };
        *heartbeat = now;
        retire_nodes(&mut state, node, cutoff);
        Ok(true)
    }

    async fn return_claims(&self, node: Option<uuid::Uuid>) -> Result<u64, TaskRuntimeError> {
        let mut state = self.state.lock().await;
        let now = chrono::Utc::now();
        let mut returned = 0;
        for task in &mut state.tasks {
            if !(task.status == TaskStatus::Running
                || (task.node_id.is_some()
                    && matches!(task.status, TaskStatus::Pending | TaskStatus::Suspended)))
                || node.is_some_and(|node| task.node_id != Some(node))
            {
                continue;
            }
            if task.status == TaskStatus::Running {
                task.status = TaskStatus::Pending;
                task.ready_at = Some(now);
            }
            task.node_id = None;
            task.updated_at = now;
            returned += 1;
        }
        Ok(returned)
    }

    async fn initialize(&self) -> Result<(), TaskRuntimeError> {
        let mut current = self.state.lock().await;
        let mut state = current.clone();
        let conf = state.conf.clone().ok_or_else(|| {
            TaskRuntimeError::TaskExecutionError("task store configuration is missing".into())
        })?;
        let now = chrono::Utc::now();
        let deliveries = Vec::new();
        reject_orphaned_tasks(&state.tasks, &conf)?;
        initialize_rates(&mut state, &conf);
        initialize_lane_locks(&mut state, &conf);
        workflow::finalize_workflow(&mut state, Vec::new(), deliveries, now);
        *current = state;
        Ok(())
    }

    async fn claim_tasks(
        &self,
        node: &uuid::Uuid,
        claims: &[LaneClaim],
    ) -> Result<TaskPoll, StoreFailure<TaskPoll>> {
        let mut state = self.state.lock().await;
        if !state.nodes.contains_key(node) && claims.iter().any(|claim| claim.limit > 0) {
            return Err(TaskRuntimeError::TaskExecutionError(
                "task node registration missing".into(),
            )
            .into());
        }
        let (poll, _) = workflow::atomic_operation(
            &mut state,
            Some(node),
            claims,
            &[],
            self.commit_batch_size,
            self.lease_duration,
            chrono::Utc::now(),
        )?;
        Ok(poll)
    }

    async fn coordinate_lanes(
        &self,
        node: &uuid::Uuid,
        claims: &[LaneClaim],
    ) -> Result<TaskPoll, StoreFailure<TaskPoll>> {
        crate::tasks::store::validate_coordination(claims)?;
        self.claim_tasks(node, claims).await
    }

    async fn commit_tasks(
        &self,
        commits: &[TaskCommit],
    ) -> Result<CommitResult, StoreFailure<CommitResult>> {
        let mut state = self.state.lock().await;
        let (_, result) = workflow::atomic_operation(
            &mut state,
            None,
            &[],
            commits,
            self.commit_batch_size,
            self.lease_duration,
            chrono::Utc::now(),
        )?;
        Ok(result)
    }

    async fn commit_admitted(
        &self,
        commits: &[TaskCommit],
        node: &uuid::Uuid,
        limit: usize,
        cached: &[Arc<TaskRecord>],
    ) -> Result<CommitResult, StoreFailure<CommitResult>> {
        let mut state = self.state.lock().await;
        let before = state.tasks.len();
        let now = chrono::Utc::now();
        let (_, mut result) = workflow::atomic_operation(
            &mut state,
            None,
            &[],
            commits,
            self.commit_batch_size,
            self.lease_duration,
            now,
        )?;
        workflow::admit_committed(
            &mut state,
            before,
            commits,
            *node,
            limit,
            cached,
            now,
            &mut result,
        );
        Ok(result)
    }

    async fn recover_claims(
        &self,
        node: &uuid::Uuid,
        claims: &[LaneClaim],
        prepared: TaskPoll,
    ) -> Result<TaskPoll, StoreFailure<TaskPoll>> {
        match self.recover_poll(node, claims, &prepared).await {
            Ok(poll) => Ok(poll),
            Err(error) => Err(StoreFailure::CommitUnknown { error, prepared }),
        }
    }

    async fn recover_outputs(
        &self,
        node: &uuid::Uuid,
        commits: &[TaskCommit],
        prepared: CommitResult,
    ) -> Result<CommitResult, StoreFailure<CommitResult>> {
        match self.commit_tasks(commits).await {
            Ok(mut result) => {
                crate::tasks::store::merge_wakes(&mut result.wake_lanes, prepared.wake_lanes);
                let state = self.state.lock().await;
                for previous in prepared.tasks {
                    if let Some(current) = state.tasks.iter().find(|task| {
                        task.id == previous.id
                            && ((task.status == TaskStatus::Running && task.node_id == Some(*node))
                                || (task.kind == TaskKind::Flow
                                    && task.node_id.is_none()
                                    && task.status != TaskStatus::Running))
                            && task.attempts == previous.attempts
                            && task.status == previous.status
                    }) {
                        result.tasks.push(Arc::new(current.clone()));
                    }
                }
                Ok(result)
            }
            Err(error) => Err(error.retain(prepared)),
        }
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

    async fn store_admitted(
        &self,
        writes: Vec<TaskWrite>,
        node: &uuid::Uuid,
        limit: usize,
    ) -> Result<(Vec<TaskReceipt>, Vec<TaskRecord>), TaskRuntimeError> {
        self.store_admitted_impl(writes, node, limit).await
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
                task.node_id = None;
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
pub(super) fn claim_tasks_state(
    state: &mut MemoryState,
    runner_id: &uuid::Uuid,
    claims: &[LaneClaim],
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
        let bounded = bounded_claim(claim, lane_conf.claim_batch_size_value());
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
        let candidates = state
            .tasks
            .iter()
            .filter(|task| {
                task.kind == crate::tasks::TaskKind::Work
                    && !task.cancelled
                    && task.lane == bounded.lane.as_str()
                    && is_due(task, now)
            })
            .count();
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
    Ok(TaskPoll {
        lanes,
        wake_lanes: Vec::new(),
    })
}

/// Commits one bounded batch of outcomes while the in-memory transaction lock is held.
pub(super) fn commit_outcomes_state(
    state: &mut MemoryState,
    commits: &[TaskCommit],
    now: chrono::DateTime<chrono::Utc>,
    undo: &mut Vec<TaskRecord>,
) -> Result<
    (
        Vec<TaskRecord>,
        Vec<(TaskId, String)>,
        Vec<(TaskId, workflow::TaskWait)>,
    ),
    TaskRuntimeError,
> {
    let (updates, children, deliveries, waits) = workflow::stage_outcomes(state, commits, now)?;
    for (index, task) in updates {
        let current = state.tasks.get_mut(index).ok_or_else(|| {
            TaskRuntimeError::TaskExecutionError("staged task disappeared".into())
        })?;
        undo.push(std::mem::replace(current, task));
    }
    Ok((children, deliveries, waits))
}

/// Fences outcomes from runners that no longer own an opt-in lane.
pub(super) fn memory_commit_allowed(
    state: &MemoryState,
    commit: &TaskCommit,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<bool, TaskRuntimeError> {
    let TaskCommit::Outcome {
        node_id,
        lane,
        owner_token,
        ..
    } = commit
    else {
        return Ok(true);
    };
    let locked = state
        .conf
        .as_ref()
        .and_then(|conf| conf.lanes.iter().find(|entry| entry.lane() == *lane))
        .is_some_and(|lane| lane.lane_lock().is_some());
    if !locked {
        return Ok(true);
    }
    let Some(owner) = state.lane_locks.get(lane.as_str()) else {
        return Err(TaskRuntimeError::InvalidConfig(format!(
            "task lane lock '{}' is not initialized",
            lane
        )));
    };
    Ok(
        owner.owner_id.as_deref() == Some(node_id.to_string().as_str())
            && owner.owner_token.as_deref() == owner_token.as_deref()
            && memory_owner_live(owner, now),
    )
}

pub(super) fn bounded_claim(claim: &LaneClaim, batch_size: usize) -> LaneClaim {
    LaneClaim {
        lane: claim.lane,
        limit: claim.limit.min(batch_size),
        owner: claim.owner.clone(),
    }
}

pub(super) fn claim_lane_state(
    state: &mut MemoryState,
    runner_id: &uuid::Uuid,
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
        state.conf.as_ref().is_some_and(|conf| {
            conf.handlers
                .iter()
                .any(|(_, kind)| *kind == crate::tasks::TaskKind::Flow)
        }),
    );
    Ok(poll)
}

/// Finalizes cancelled or exhausted ownerless candidates before execution or reclamation.
pub(super) fn fail_exhausted(
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
        let cancelled = task.cancelled;
        if !cancelled && !retry.exhausted(task.step_attempts)? {
            continue;
        }
        undo.push(task.clone());
        let outcome = if cancelled {
            crate::tasks::store::transitions::cancellation()
        } else {
            TaskOutcome::fail("Maximum task attempts exhausted")
        };
        apply_outcome(task, outcome, retry, now)?;
        workflow::queue_delivery(task, deliveries)?;
        task.node_id = None;
        task.updated_at = now;
        finalize_idempotency(task, conf, now)?;
    }
    Ok(())
}

/// Rejects low-level writes that bypass the typed client's lane validation.
pub(super) fn validate_write_lanes(
    state: &MemoryState,
    writes: &[TaskWrite],
) -> Result<(), TaskRuntimeError> {
    for write in writes {
        require_lane(state, &write.record.lane)?;
        require_handler(state, &write.record.name)?;
    }
    Ok(())
}

pub(super) fn require_handler(state: &MemoryState, handler: &str) -> Result<(), TaskRuntimeError> {
    state
        .conf
        .as_ref()
        .is_some_and(|conf| conf.handlers.iter().any(|(name, _)| name == handler))
        .then_some(())
        .ok_or_else(|| TaskRuntimeError::TaskNotFound(handler.into()))
}

/// Validates one persisted lane name against initialized store policy.
pub(super) fn require_lane(state: &MemoryState, lane: &str) -> Result<(), TaskRuntimeError> {
    let configured = state
        .conf
        .as_ref()
        .is_some_and(|conf| conf.lanes.iter().any(|entry| entry.lane().as_str() == lane));
    configured
        .then_some(())
        .ok_or_else(|| TaskRuntimeError::UnknownLane(lane.into()))
}

/// Claims one in-memory lane while preserving candidate saturation evidence.
pub(super) fn claim_lane(
    tasks: &mut [TaskRecord],
    runner_id: &uuid::Uuid,
    claim: &LaneClaim,
    permit_limit: usize,
    now: chrono::DateTime<chrono::Utc>,
    default_lease: Duration,
    undo: &mut Vec<TaskRecord>,
) -> Result<LanePoll, TaskRuntimeError> {
    let requested = claim.limit;
    let mut candidates = due_indices(tasks, claim.lane.as_str(), now);
    if permit_limit == 0 {
        candidates.retain(|index| {
            tasks
                .get(*index)
                .is_some_and(|task| task.kind == TaskKind::Flow)
        });
    }
    candidates.sort_by_key(|index| tasks.get(*index).map(|task| readiness(task, now)));
    let saturated = requested > 0 && candidates.len() >= requested;
    candidates.truncate(requested);
    let mut permits = permit_limit;
    candidates.retain(|index| {
        tasks.get(*index).is_some_and(|task| {
            if task.cancelled {
                return false;
            }
            if task.kind == crate::tasks::TaskKind::Flow {
                return true;
            }
            if permits == 0 {
                return false;
            }
            permits -= 1;
            true
        })
    });
    let claim_count = candidates.len();
    let reclaimed = candidates
        .iter()
        .filter(|index| {
            tasks
                .get(**index)
                .is_some_and(|task| task.status == TaskStatus::Running)
        })
        .count();
    let mut claimed = Vec::with_capacity(claim_count);
    for index in candidates {
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

/// Assigns node ownership and checked invocation counters to one candidate.
pub(super) fn claim_task(
    task: &mut TaskRecord,
    runner_id: &uuid::Uuid,
    now: chrono::DateTime<chrono::Utc>,
    default_lease: Duration,
) -> Result<(), TaskRuntimeError> {
    let _ = default_lease;
    task.attempts = task.attempts.checked_add(1).ok_or_else(|| {
        TaskRuntimeError::TaskExecutionError("task attempt count overflowed".into())
    })?;
    task.step_attempts = task.step_attempts.checked_add(1).ok_or_else(|| {
        TaskRuntimeError::TaskExecutionError("task step attempt count overflowed".into())
    })?;
    task.status = TaskStatus::Running;
    task.node_id = Some(*runner_id);
    task.updated_at = now;
    Ok(())
}

/// Refills and reserves one lane bucket while holding the store mutex.
pub(super) fn reserve_permits(
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
pub(super) fn configured_rate(
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

pub(super) fn configured_retry(
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
pub(super) fn stage_write(
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
pub(super) fn matching_key<'a>(
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

pub(super) fn idempotency_held(task: &TaskRecord, now: chrono::DateTime<chrono::Utc>) -> bool {
    is_active(task.status)
        || task
            .idempotency_expires_at
            .is_some_and(|expiry| expiry > now)
}

/// Applies a lifecycle transition to an owned task record.
pub(super) fn apply_outcome(
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
    task.node_id = None;
    task.updated_at = now;
    Ok(())
}

pub(super) fn complete(
    task: &mut TaskRecord,
    status: TaskStatus,
    now: chrono::DateTime<chrono::Utc>,
) {
    task.status = status;
    task.ready_at = None;
    task.completed_at = Some(now);
}

pub(super) fn suspend(task: &mut TaskRecord, state: String) {
    task.last_result = None;
    task.step_attempts = 0;
    task.status = TaskStatus::Suspended;
    task.state = Some(state);
    task.ready_at = None;
}

pub(super) fn sleep(
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
pub(super) fn retry(
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

pub(super) fn fail(task: &mut TaskRecord, error: String, now: chrono::DateTime<chrono::Utc>) {
    task.last_result = Some(crate::tasks::result::failure(task.id, error));
    complete(task, TaskStatus::Failed, now);
}

#[cfg(test)]
#[path = "tests/memory.rs"]
pub(crate) mod tests;

#[cfg(test)]
#[path = "tests/memory_nodes.rs"]
mod node_tests;
