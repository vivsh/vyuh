//! Lane admission, lifecycle acknowledgement, and bounded hooks.
use super::engine::{
    DbRequest, HookCompletion, LaneState, PollEffects, RunMode, RunningLaneHook, TaskRunner,
};
use crate::Site;
use crate::tasks::{LaneClaim, TaskLane, TaskPoll, TaskRecord, TaskRuntimeError, TaskStore};
use futures::FutureExt;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::mpsc;
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
    lane: &mut LaneState,
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
    lane.tasks.extend(result.tasks.into_iter().map(Arc::new));
    let store_deadline = lane_deadline(
        now,
        result.saturated,
        lane.conf.global_rate(),
        result.next_wake_in,
        poll_interval,
        fallback_interval,
    );
    lane.poll_after = owned_lane_deadline(lane, store_deadline);
}

pub(super) fn owned_lane_deadline(
    lane: &LaneState,
    rate_deadline: tokio::time::Instant,
) -> tokio::time::Instant {
    if lane.owner_token.is_some()
        && (lane.execution_slots.available_permits() < lane.conf.concurrency()
            || lane.uncommitted > 0
            || !lane.tasks.is_empty())
    {
        lane.owner_renew_at.unwrap_or(rate_deadline)
    } else {
        rate_deadline
    }
}

/// Applies one fenced owner response and captures transition-only side effects.
pub(super) fn apply_owner_poll(
    lane: &mut LaneState,
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
    if previous != owner.token {
        lane.flow_cache.clear();
        lane.cached_bytes = 0;
    }
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

impl LaneState {
    /// Invalidates only the disposable cached snapshot, never the durable task.
    pub(super) fn forget_flow(&mut self, id: crate::tasks::TaskId) {
        if let Some(old) = self.flow_cache.remove(&id) {
            self.cached_bytes = self.cached_bytes.saturating_sub(flow_bytes(&old));
        }
    }
    /// Keeps disposable Flow snapshots; cache presence never grants execution ownership.
    pub(super) fn remember_flow(&mut self, record: Arc<TaskRecord>) {
        let id = record.id();
        self.forget_flow(id);
        let terminal = matches!(
            record.status,
            crate::tasks::TaskStatus::Succeeded | crate::tasks::TaskStatus::Failed
        );
        let size = flow_bytes(&record);
        if record.kind != crate::tasks::TaskKind::Flow || terminal || size > self.conf.cache_bytes()
        {
            return;
        }
        while self.cached_bytes.saturating_add(size) > self.conf.cache_bytes() {
            let Some((_, old)) = self.flow_cache.pop_front() else {
                break;
            };
            self.cached_bytes = self.cached_bytes.saturating_sub(flow_bytes(&old));
        }
        self.cached_bytes += size;
        self.flow_cache.insert(id, record);
    }
}

/// Counts retained string capacity and fixed snapshot/index overhead conservatively.
pub(super) fn flow_bytes(record: &TaskRecord) -> usize {
    std::mem::size_of::<TaskRecord>()
        + 128
        + record.input.capacity()
        + record.name.capacity()
        + record.lane.capacity()
        + record.state.as_ref().map_or(0, String::capacity)
        + record.resume_input.as_ref().map_or(0, String::capacity)
        + record.last_result.as_ref().map_or(0, String::capacity)
        + record.idempotency_key.as_ref().map_or(0, String::capacity)
        + record
            .idempotency_fingerprint
            .as_ref()
            .map_or(0, String::capacity)
}

impl<S: TaskStore + Send + Sync + 'static> TaskRunner<S> {
    /// Includes synchronous Flow invocations which deliberately hold no Work permit.
    pub(super) fn lane_running(&self, lane: TaskLane) -> bool {
        self.running_invocations
            .values()
            .any(|invocation| invocation.lane == lane)
    }

    /// Bounds admitted inputs with existing resident/outcome accounting, not another semaphore.
    pub(super) fn input_room(&self) -> usize {
        let capacity = self.ingress_tx.max_capacity()
            + self
                .lanes
                .iter()
                .map(|lane| lane.conf.concurrency())
                .sum::<usize>();
        let outstanding = self.running_tasks.len()
            + self
                .lanes
                .iter()
                .map(|lane| lane.tasks.len() + lane.uncommitted)
                .sum::<usize>();
        let reserved = self
            .in_flight
            .values()
            .map(|entry| match &*entry.request {
                DbRequest::Claim { claims } => claims.iter().map(|claim| claim.limit).sum(),
                DbRequest::Commit { capacity, .. } => *capacity,
                _ => 0,
            })
            .sum::<usize>();
        capacity
            .saturating_sub(outstanding)
            .saturating_sub(reserved)
    }

    /// Applies authoritative snapshots without mutating a queued or executing task.
    pub(super) fn accept_input(&mut self, record: Arc<TaskRecord>) {
        let node = self.node_id;
        let running =
            record.status == crate::tasks::TaskStatus::Running && record.node_id == Some(node);
        let duplicate = self.running_tasks.contains_key(&record.id());
        let Some(queue) = self
            .lanes
            .iter_mut()
            .find(|lane| lane.conf.lane().as_str() == record.lane)
        else {
            return;
        };
        if record.kind == crate::tasks::TaskKind::Flow {
            queue.remember_flow(record.clone());
        }
        if !matches!(self.mode, RunMode::Running | RunMode::Draining) || !running || duplicate {
            return;
        }
        if !queue.tasks.iter().any(|queued| queued.id() == record.id()) {
            self.metrics.claimed(queue.conf.lane().as_str(), 1, 0);
            queue.tasks.push_back(record);
        }
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
fn locked_claim(lane: &LaneState, allow_claim: bool) -> LaneClaim {
    let size = lane
        .conf
        .lane_lock()
        .map_or(1, crate::tasks::TaskLaneLock::batch_size);
    LaneClaim {
        lane: lane.conf.lane(),
        limit: size,
        owner: Some(crate::tasks::LaneOwnerRequest {
            token: lane.owner_token.clone(),
            quiescent: lane.tasks.is_empty()
                && lane.execution_slots.available_permits() == lane.conf.concurrency()
                && lane.uncommitted == 0,
            allow_claim,
            completed_work: lane.completed_work,
            hook: lane.hook_result.clone(),
        }),
    }
}
impl<S: TaskStore + Send + Sync + 'static> TaskRunner<S> {
    /// Selects one due maintenance request without consuming task execution capacity.
    pub(super) fn coordinate_request(&mut self) -> Option<DbRequest> {
        let now = tokio::time::Instant::now();
        let selected = self
            .lanes
            .iter()
            .find(|lane| {
                lane.owner_token.is_some()
                    && !self.lane_busy(lane.conf.lane())
                    && (lane.owner_renew_at.is_some_and(|at| at <= now)
                        || (lane.poll_after <= now
                            && (lane.hook_result.is_some()
                                || (self.mode == RunMode::Draining
                                    && lane.tasks.is_empty()
                                    && lane.uncommitted == 0
                                    && lane.execution_slots.available_permits()
                                        == lane.conf.concurrency()))))
            })?
            .conf
            .lane();
        let fallback = now + self.fallback_interval;
        let running = self.lane_running(selected);
        let queue = self.lane_mut(selected)?;
        let mut claim = locked_claim(queue, false);
        if running && let Some(owner) = &mut claim.owner {
            owner.quiescent = false;
        }
        queue.poll_after = fallback;
        Some(DbRequest::Coordinate { lanes: vec![claim] })
    }

    /// Rotates attempted input lanes, with reservations derived from unresolved requests.
    pub(super) fn claim_request(&mut self) -> Option<DbRequest> {
        if self.mode != RunMode::Running {
            return None;
        }
        let now = tokio::time::Instant::now();
        let capacity = self.claim_capacity();
        let room = self.input_room();
        for _ in 0..self.lanes.len() {
            let name = self.lanes.front()?.conf.lane();
            let busy = self.lane_busy(name);
            let mut lane = self.lanes.pop_front()?;
            let eligible = !busy && lane.poll_after <= now;
            let claim = if eligible {
                self.admit_lane(&mut lane, capacity.min(room), now)
            } else {
                None
            };
            if claim.is_some() {
                lane.poll_after = now + self.fallback_interval;
            }
            self.lanes.push_back(lane);
            if let Some(claim) = claim {
                return Some(DbRequest::Claim {
                    claims: vec![claim],
                });
            }
        }
        None
    }

    /// Allocates ordinary capacity or the existing owned-lane cohort without duplicate requests.
    pub(super) fn admit_lane(
        &self,
        lane: &mut LaneState,
        capacity: usize,
        now: tokio::time::Instant,
    ) -> Option<LaneClaim> {
        if lane.conf.lane_lock().is_some() {
            let mut request = locked_claim(lane, true);
            if self.lane_running(lane.conf.lane())
                && let Some(owner) = &mut request.owner
            {
                owner.quiescent = false;
            }
            let quiescent = request.owner.as_ref().is_some_and(|o| o.quiescent);
            return quiescent.then_some(request);
        }
        let flows = self.has_flows(lane.conf.lane());
        let available = if flows {
            self.input_room()
                .saturating_sub(lane.tasks.len() + lane.uncommitted)
                .min(lane.conf.claim_batch_size_value())
        } else {
            lane.available().min(capacity)
        };
        let limit = if flows {
            available
        } else {
            lane.claim_limit(available, now)
        };
        if limit > 0 {
            return Some(LaneClaim {
                lane: lane.conf.lane(),
                limit,
                owner: None,
            });
        }
        None
    }

    pub(super) fn has_flows(&self, lane: TaskLane) -> bool {
        self.registry.tasks.values().any(|task| {
            task.kind() == crate::tasks::TaskKind::Flow && task.effective_lane() == lane
        })
    }

    /// Applies request-specific hook acknowledgement without clearing later completions or wakes.
    pub(super) fn apply_poll(
        &mut self,
        site: &Site,
        hooks: &mpsc::Sender<HookCompletion>,
        requests: &[LaneClaim],
        poll: TaskPoll,
    ) {
        let now = tokio::time::Instant::now();
        let mut effects = PollEffects::default();
        for result in poll.lanes {
            self.metrics
                .claimed(result.lane.as_str(), result.tasks.len(), result.reclaimed);
            let request = requests.iter().find(|request| request.lane == result.lane);
            let (interval, fallback, lease) = (
                self.poll_interval,
                self.fallback_interval,
                self.lease_duration,
            );
            let Some(lane) = self.lane_mut(result.lane) else {
                continue;
            };
            let wake = lane.poll_after;
            let acknowledged = request
                .and_then(|r| r.owner.as_ref())
                .and_then(|o| o.hook.as_ref());
            let newer = lane.hook_result.as_ref().is_some_and(|hook| {
                acknowledged.is_none_or(|old| {
                    old.generation != hook.generation || old.action != hook.action
                })
            });
            let pending = if newer { lane.hook_result.take() } else { None };
            apply_lane_poll(lane, result, now, interval, fallback, lease, &mut effects);
            if lane.owner_token.is_some() {
                lane.hook_result = pending.filter(|h| h.generation == lane.owner_generation);
            }
            lane.poll_after = lane.poll_after.min(wake).max(now + interval);
        }
        effects.record(&self.metrics);
        for (lane, action) in effects.hooks {
            if let Some((token, generation, action)) = action {
                let completed = self
                    .lanes
                    .iter()
                    .find(|queue| queue.conf.lane() == lane)
                    .and_then(|queue| queue.hook_result.as_ref())
                    .is_some_and(|result| {
                        result.generation == generation && result.action == action
                    });
                if self.mode == RunMode::Running && !completed {
                    self.spawn_hook(site.clone(), hooks.clone(), lane, token, generation, action);
                }
            } else {
                self.drop_lane(lane);
            }
        }
        for lane in poll.wake_lanes {
            self.wake_lane(lane);
        }
    }

    pub(super) fn accept_hook(&mut self, completion: HookCompletion) {
        let matched = self
            .running_hooks
            .get(&completion.lane)
            .is_some_and(|running| {
                running.token == completion.token
                    && running.generation == completion.result.generation
                    && running.action == completion.result.action
            });
        if !matched {
            self.metrics.stale_hook_result(completion.lane.as_str());
            return;
        }
        if let Some(running) = self.running_hooks.remove(&completion.lane) {
            self.metrics.hook_completed(
                completion.lane.as_str(),
                completion.result.action,
                completion.result.result.is_err(),
                running.started.elapsed(),
            );
        }
        let next_poll = tokio::time::Instant::now() + self.poll_interval;
        if let Some(lane) = self.lane_mut(completion.lane)
            && lane.owner_token.as_deref() == Some(completion.token.as_str())
        {
            lane.hook_result = Some(completion.result);
            lane.poll_after = next_poll;
        }
    }
    pub(super) fn spawn_hook(
        &mut self,
        site: Site,
        sender: mpsc::Sender<HookCompletion>,
        lane: TaskLane,
        token: String,
        generation: i64,
        action: crate::tasks::LaneHookAction,
    ) {
        if self.running_hooks.get(&lane).is_some_and(|running| {
            running.token == token && running.generation == generation && running.action == action
        }) {
            return;
        }
        if let Some(previous) = self.running_hooks.remove(&lane) {
            previous.abort.abort();
        }
        let Some(hook) = self.hook_for(lane, action) else {
            return;
        };
        let error_limit = self.registry.config.error_limit();
        let call = HookCall {
            hook,
            site,
            lane,
            token: token.clone(),
            generation,
            action,
            error_limit,
        };
        let future = execute_hook(call, sender);
        let handle = tokio::spawn(future);
        self.metrics.hook_started(lane.as_str(), action);
        self.running_hooks.insert(
            lane,
            RunningLaneHook {
                token,
                generation,
                action,
                abort: handle.abort_handle(),
                started: std::time::Instant::now(),
            },
        );
    }
    pub(super) fn hook_for(
        &self,
        lane: TaskLane,
        action: crate::tasks::LaneHookAction,
    ) -> Option<crate::tasks::lane_lock::LaneHook> {
        let lane_lock = self
            .lanes
            .iter()
            .find(|queue| queue.conf.lane() == lane)?
            .conf
            .lane_lock()?;
        match action {
            crate::tasks::LaneHookAction::Idle => lane_lock.idle_hook().cloned(),
            crate::tasks::LaneHookAction::Busy => lane_lock.busy_hook().cloned(),
        }
    }
    pub(super) fn abort_hooks(&mut self) {
        for (_, hook) in self.running_hooks.drain() {
            hook.abort.abort();
        }
    }
    pub(super) fn drop_lane(&mut self, lane: TaskLane) {
        if let Some(hook) = self.running_hooks.remove(&lane) {
            hook.abort.abort();
        }
        let invocations = self
            .running_invocations
            .iter()
            .filter_map(|(id, invocation)| (invocation.lane == lane).then_some(*id))
            .collect::<Vec<_>>();
        for invocation in invocations {
            self.abort_invocation(invocation);
        }
        let pending = self
            .output_buffer
            .iter()
            .filter_map(|commit| (commit.lane() == Some(lane)).then_some(commit.task_id()))
            .collect::<Vec<_>>();
        self.drop_pending(&pending);
        if let Some(queue) = self.lane_mut(lane) {
            queue.tasks.clear();
        }
    }
    pub(super) fn drop_pending(&mut self, ids: &[crate::tasks::TaskId]) {
        let mut removed = HashMap::<TaskLane, usize>::new();
        self.output_buffer.retain(|commit| {
            let lost = ids.contains(&commit.task_id());
            if lost && let Some(lane) = commit.lane() {
                *removed.entry(lane).or_default() += 1;
            }
            !lost
        });
        for (lane, count) in removed {
            if let Some(queue) = self.lane_mut(lane) {
                queue.uncommitted = queue.uncommitted.saturating_sub(count);
            }
        }
    }
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

/// Validates internal per-lane results before they enter scheduler queues.
pub(super) fn validate_poll(claims: &[LaneClaim], poll: &TaskPoll) -> Result<(), TaskRuntimeError> {
    if claims.len() != poll.lanes.len() {
        return Err(TaskRuntimeError::TaskExecutionError(
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

pub(super) fn invalid_lane_poll(lane: TaskLane) -> TaskRuntimeError {
    TaskRuntimeError::TaskExecutionError(format!(
        "task store returned invalid polling evidence for lane '{lane}'"
    ))
}
