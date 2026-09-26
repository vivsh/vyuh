//! Stateless poll application and spawned invocation execution.
use super::*;

impl PollEffects {
    /// Emits bounded metrics after lane borrows from the poll loop are released.
    pub(super) fn record(&self, metrics: &crate::tasks::TaskMetrics) {
        for lane in &self.acquired {
            metrics.owner_acquired(lane.as_str());
        }
        for lane in &self.takeovers {
            metrics.owner_takeover(lane.as_str());
        }
        for lane in &self.lost {
            metrics.owner_lost(lane.as_str());
        }
        for (lane, phase) in &self.transitions {
            metrics.lifecycle_transition(lane.as_str(), *phase);
        }
    }
}

/// Applies one lane's owned state, queue rows, and next useful central wake.
pub(super) fn apply_lane_poll(
    lane: &mut LaneQueue,
    result: crate::tasks::LanePoll,
    now: tokio::time::Instant,
    poll_interval: tokio::time::Duration,
    fallback_interval: tokio::time::Duration,
    lease_duration: tokio::time::Duration,
    effects: &mut PollEffects,
) {
    if let Some(owner) = result.owner {
        apply_owner_poll(lane, result.lane, owner, now, lease_duration, effects);
    }
    if lane.conf.lane_lock().is_none() {
        lane.consume_local_rate(result.tasks.len(), now);
    }
    lane.tasks.extend(result.tasks.into_iter().map(Arc::new));
    let store_deadline = lane_deadline(
        now,
        result.saturated,
        lane.conf.global_rate(),
        result.next_wake_in,
        poll_interval,
        fallback_interval,
    );
    let rate_deadline = lane
        .local_rate_wake(now)
        .map_or(store_deadline, |wake| store_deadline.max(now + wake));
    lane.poll_after = owned_lane_deadline(lane, rate_deadline);
}

pub(super) fn owned_lane_deadline(
    lane: &LaneQueue,
    rate_deadline: tokio::time::Instant,
) -> tokio::time::Instant {
    if lane.owner_token.is_some()
        && (lane.running > 0 || lane.uncommitted > 0 || !lane.tasks.is_empty())
    {
        lane.owner_renew_at.unwrap_or(rate_deadline)
    } else {
        rate_deadline
    }
}

/// Applies one fenced owner response and captures transition-only side effects.
pub(super) fn apply_owner_poll(
    lane: &mut LaneQueue,
    lane_name: TaskLane,
    owner: crate::tasks::LaneOwnerPoll,
    now: tokio::time::Instant,
    lease_duration: tokio::time::Duration,
    effects: &mut PollEffects,
) {
    let previous = lane.owner_token.clone();
    let previous_phase = lane.owner_phase;
    if owner.takeover {
        effects.takeovers.push(lane_name);
    }
    lane.owner_token = owner.token.clone();
    lane.owner_renew_at = owner.token.as_ref().map(|_| now + lease_duration / 2);
    lane.owner_generation = owner.generation;
    lane.owner_phase = owner.phase;
    lane.completed_work =
        lane.completed_work && owner.phase == crate::tasks::LaneOwnerPhase::IdleFailed;
    lane.hook_result = None;
    capture_owner_edge(lane_name, previous.as_ref(), &owner, effects);
    if previous_phase != owner.phase {
        effects.transitions.push((lane_name, owner.phase));
    }
}

/// Distinguishes intentional idle release from lease or fencing-token loss.
pub(super) fn capture_owner_edge(
    lane: TaskLane,
    previous: Option<&String>,
    owner: &crate::tasks::LaneOwnerPoll,
    effects: &mut PollEffects,
) {
    if previous.is_some() && owner.token.is_none() {
        if matches!(
            owner.phase,
            crate::tasks::LaneOwnerPhase::Active
                | crate::tasks::LaneOwnerPhase::Idling
                | crate::tasks::LaneOwnerPhase::Busying
        ) {
            effects.lost.push(lane);
        }
        effects.hooks.push((lane, None));
    } else {
        if previous.is_none() && owner.token.is_some() {
            effects.acquired.push(lane);
        }
        if let (Some(token), Some(action)) = (&owner.token, owner.action) {
            effects
                .hooks
                .push((lane, Some((token.clone(), owner.generation, action))));
        }
    }
}

/// Adds one owner-renewal request and reserves claim budget only when work may start.
pub(super) fn push_locked_claim(
    lane: &LaneQueue,
    allow_work: bool,
    batch: &mut usize,
    claims: &mut Vec<LaneClaim>,
) {
    if !allow_work && lane.owner_token.is_none() {
        return;
    }
    let size = lane
        .conf
        .lane_lock()
        .map_or(1, crate::tasks::TaskLaneLock::batch_size);
    let quiescent = lane.tasks.is_empty() && lane.running == 0 && lane.uncommitted == 0;
    let allow_claim = allow_work && quiescent && *batch >= size;
    claims.push(locked_claim(lane, allow_claim));
    if allow_claim {
        *batch -= size;
    }
}

/// Forms one invocation from matching rows already held in a lane queue.
pub(super) fn collect_invocation(
    lane: &mut LaneQueue,
    first: Arc<TaskRecord>,
    limit: usize,
    registry: &TaskRegistry,
) -> Vec<Arc<TaskRecord>> {
    if limit <= 1 || !registry.is_batch(first.name()) {
        return vec![first];
    }
    let name = first.name().to_owned();
    let mut records = Vec::with_capacity(limit.min(lane.tasks.len().saturating_add(1)));
    let mut remaining = VecDeque::with_capacity(lane.tasks.len());
    records.push(first);
    while let Some(record) = lane.tasks.pop_front() {
        if records.len() < limit && record.name() == name {
            records.push(record);
        } else {
            remaining.push_back(record);
        }
    }
    lane.tasks = remaining;
    records
}

/// Contains one handler panic and sends its normalized lifecycle completion.
pub(super) async fn execute_task(execution: TaskExecution) {
    let task_name = execution
        .records
        .first()
        .map_or("unknown", |record| record.name())
        .to_owned();
    let started = std::time::Instant::now();
    let results = match std::panic::AssertUnwindSafe(
        execution
            .engine
            .execute_many(execution.site.clone(), execution.records.clone()),
    )
    .catch_unwind()
    .await
    {
        Ok(results) => results,
        Err(_) => {
            tracing::error!(
                task = %task_name,
                count = execution.records.len(),
                "task handler panicked"
            );
            panic_results(execution.records.clone())
        }
    };
    let commits = task_commits(&execution, results);
    execution.metrics.handler_completed(started.elapsed());
    if execution
        .sender
        .send(Completion {
            invocation_id: execution.invocation_id,
            lane: execution.lane,
            commits,
        })
        .await
        .is_err()
    {
        tracing::error!(task = %task_name, "task completion channel closed before commit");
    }
}

pub(super) fn panic_results(
    records: Vec<Arc<TaskRecord>>,
) -> Vec<crate::tasks::handler::TaskExecutionResult> {
    records
        .into_iter()
        .map(|record| crate::tasks::handler::TaskExecutionResult {
            record,
            outcome: crate::tasks::TaskOutcome::fail("Task handler panicked"),
        })
        .collect()
}

pub(super) fn task_commits(
    execution: &TaskExecution,
    results: Vec<crate::tasks::handler::TaskExecutionResult>,
) -> Vec<TaskCommit> {
    results
        .into_iter()
        .map(|result| {
            let outcome = crate::tasks::store::normalize_outcome(
                result.outcome,
                execution.payload_limit,
                execution.error_limit,
            );
            execution.metrics.outcome(result.record.name(), &outcome);
            TaskCommit {
                task_id: result.record.id(),
                lane: execution.lane,
                outcome,
                owner_token: execution.owner_token.clone(),
            }
        })
        .collect()
}

pub(super) struct HookCall {
    pub(super) hook: crate::tasks::lane_lock::LaneHook,
    pub(super) site: Site,
    pub(super) lane: TaskLane,
    pub(super) token: String,
    pub(super) generation: i64,
    pub(super) action: crate::tasks::LaneHookAction,
    pub(super) error_limit: usize,
}

/// Executes one lifecycle hook and reports its fenced result without blocking the runner.
pub(super) async fn execute_hook(call: HookCall, sender: mpsc::Sender<HookCompletion>) {
    let future =
        std::panic::AssertUnwindSafe(call.hook.call(call.site, call.lane, call.generation))
            .catch_unwind();
    let result = match future.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(crate::tasks::store::truncate_utf8(
            error.to_string(),
            call.error_limit,
        )),
        Err(_) => Err("task lane lifecycle hook panicked".into()),
    };
    let completion = HookCompletion {
        lane: call.lane,
        token: call.token,
        result: crate::tasks::LaneHookResult {
            generation: call.generation,
            action: call.action,
            result,
        },
    };
    if sender.send(completion).await.is_err() {
        tracing::error!(lane = %call.lane, "task lane hook completion channel closed");
    }
}

pub(super) fn lane_deadline(
    now: tokio::time::Instant,
    saturated: bool,
    global_rate: Option<crate::tasks::TaskRate>,
    store_wake: Option<std::time::Duration>,
    poll_interval: tokio::time::Duration,
    fallback_interval: tokio::time::Duration,
) -> tokio::time::Instant {
    if saturated {
        let short = now + poll_interval;
        return if global_rate.is_some() {
            store_wake.map_or(short, |wake| short.max(now + wake))
        } else {
            short
        };
    }
    store_wake.map_or(now + fallback_interval, |wake| now + wake)
}

pub(super) fn prior_tick(
    now: tokio::time::Instant,
    poll: tokio::time::Duration,
) -> tokio::time::Instant {
    match now.checked_sub(poll) {
        Some(value) => value,
        None => now,
    }
}

/// Validates internal per-lane results before they enter scheduler queues.
pub(super) fn validate_poll(claims: &[LaneClaim], poll: &TaskPoll) -> Result<(), TaskError> {
    if claims.len() != poll.lanes.len() {
        return Err(TaskError::TaskExecutionError(
            "task store returned incomplete per-lane polling evidence".into(),
        ));
    }
    for claim in claims {
        let mut matches = poll.lanes.iter().filter(|lane| lane.lane == claim.lane);
        let Some(lane) = matches.next() else {
            return Err(invalid_lane_poll(claim.lane));
        };
        let valid = matches.next().is_none()
            && lane.tasks.len() <= claim.limit
            && lane.reclaimed <= lane.tasks.len()
            && lane
                .tasks
                .iter()
                .all(|task| task.lane == claim.lane.as_str());
        if !valid {
            return Err(invalid_lane_poll(claim.lane));
        }
    }
    Ok(())
}

pub(super) fn invalid_lane_poll(lane: TaskLane) -> TaskError {
    TaskError::TaskExecutionError(format!(
        "task store returned invalid polling evidence for lane '{lane}'"
    ))
}
