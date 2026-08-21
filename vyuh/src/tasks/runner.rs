//! Fair per-lane task runner and adaptive polling loop.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use futures::FutureExt as _;
use tokio::sync::mpsc;

use crate::Site;

use super::{
    AbstractTaskStore, LaneClaim, TaskCommit, TaskDispatcher, TaskError, TaskLane, TaskLaneConf,
    TaskPoll, TaskRecord, TaskRegistry, TaskTick,
};

struct RunningInvocation {
    lane: TaskLane,
    owner_token: Option<String>,
    task_ids: Vec<super::TaskId>,
    abort: tokio::task::AbortHandle,
}

struct RunningLaneHook {
    token: String,
    generation: i64,
    action: super::LaneHookAction,
    abort: tokio::task::AbortHandle,
    started: std::time::Instant,
}

struct LaneQueue {
    conf: TaskLaneConf,
    local_rate: Option<super::rate::LocalRateBucket>,
    tasks: VecDeque<Arc<TaskRecord>>,
    running: usize,
    uncommitted: usize,
    poll_after: tokio::time::Instant,
    owner_token: Option<String>,
    owner_renew_at: Option<tokio::time::Instant>,
    owner_generation: i64,
    owner_phase: super::LaneOwnerPhase,
    completed_work: bool,
    hook_result: Option<super::LaneHookResult>,
}

impl LaneQueue {
    /// Returns whether this lane has crossed its queued-work refill watermark.
    fn needs_refill(&self) -> bool {
        self.tasks.len().saturating_mul(2) < self.conf.concurrency()
    }

    fn available(&self, batch_size: usize) -> usize {
        if self.needs_refill() {
            self.conf
                .concurrency()
                .saturating_sub(self.running + self.tasks.len())
                .min(batch_size)
        } else {
            0
        }
    }

    fn claim_limit(&mut self, limit: usize, now: tokio::time::Instant) -> usize {
        self.local_rate
            .as_mut()
            .map_or(limit, |rate| limit.min(rate.available(now)))
    }

    fn consume_local_rate(&mut self, permits: usize, now: tokio::time::Instant) {
        if let Some(rate) = &mut self.local_rate {
            rate.consume(permits, now);
        }
    }

    fn local_rate_wake(&mut self, now: tokio::time::Instant) -> Option<std::time::Duration> {
        self.local_rate
            .as_mut()
            .and_then(|rate| rate.next_permit(now))
    }
}

struct Completion {
    invocation_id: uuid::Uuid,
    lane: TaskLane,
    commits: Vec<TaskCommit>,
}

struct HookCompletion {
    lane: TaskLane,
    token: String,
    result: super::LaneHookResult,
}

struct TickResult {
    claims: Vec<LaneClaim>,
    renewals: Vec<super::TaskLease>,
    tick: TaskTick,
    started: std::time::Instant,
}

type HookStart = Option<(String, i64, super::LaneHookAction)>;

#[derive(Default)]
struct PollEffects {
    hooks: Vec<(TaskLane, HookStart)>,
    acquired: Vec<TaskLane>,
    takeovers: Vec<TaskLane>,
    lost: Vec<TaskLane>,
    transitions: Vec<(TaskLane, super::LaneOwnerPhase)>,
}

struct TaskExecution {
    invocation_id: uuid::Uuid,
    engine: Arc<TaskRegistry>,
    site: Site,
    records: Vec<Arc<TaskRecord>>,
    sender: mpsc::Sender<Completion>,
    lane: TaskLane,
    metrics: Arc<super::TaskMetrics>,
    payload_limit: usize,
    error_limit: usize,
    owner_token: Option<String>,
}

struct RunState {
    last_tick: tokio::time::Instant,
    next_poll: tokio::time::Instant,
    poll_error: tokio::time::Duration,
    commits: Vec<TaskCommit>,
    shutting_down: bool,
}

fn locked_claim(lane: &LaneQueue, allow_claim: bool) -> LaneClaim {
    let size = lane
        .conf
        .lane_lock()
        .map_or(1, super::TaskLaneLock::batch_size);
    LaneClaim {
        lane: lane.conf.lane(),
        limit: size,
        owner: Some(super::LaneOwnerRequest {
            token: lane.owner_token.clone(),
            quiescent: lane.tasks.is_empty() && lane.running == 0 && lane.uncommitted == 0,
            allow_claim,
            completed_work: lane.completed_work,
            hook: lane.hook_result.clone(),
        }),
    }
}

impl RunState {
    fn new(batch_size: usize, poll: tokio::time::Duration) -> Self {
        let now = tokio::time::Instant::now();
        Self {
            last_tick: prior_tick(now, poll),
            next_poll: now,
            poll_error: poll,
            commits: Vec::with_capacity(batch_size),
            shutting_down: false,
        }
    }
}

/// Executes durable tasks through one fair per-site lane scheduler.
pub struct AbstractTaskRunner<S: AbstractTaskStore + Send + Sync + 'static> {
    lanes: Vec<LaneQueue>,
    cursor: usize,
    concurrency: usize,
    batch_size: usize,
    lease_duration: tokio::time::Duration,
    running: usize,
    running_tasks: HashMap<super::TaskId, uuid::Uuid>,
    running_invocations: HashMap<uuid::Uuid, RunningInvocation>,
    pending_commits: VecDeque<TaskCommit>,
    running_hooks: HashMap<TaskLane, RunningLaneHook>,
    poll_interval: tokio::time::Duration,
    fallback_interval: tokio::time::Duration,
    runner_id: String,
    notifier: Arc<tokio::sync::Notify>,
    registry: Arc<TaskRegistry>,
    initialized: Arc<tokio::sync::OnceCell<()>>,
    store: Arc<S>,
    metrics: Arc<super::TaskMetrics>,
    health: super::TaskHealth,
    schedules: Arc<[super::TaskScheduleConf]>,
}

impl<S: AbstractTaskStore + Send + Sync + 'static> std::fmt::Debug for AbstractTaskRunner<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TaskRunner")
            .field("running", &self.running)
            .field("lanes", &self.lanes.len())
            .finish()
    }
}

impl<S: AbstractTaskStore + Send + Sync + 'static> AbstractTaskRunner<S> {
    /// Creates a runner from one validated task dispatcher.
    pub fn new(dispatcher: TaskDispatcher<S>) -> Result<Self, TaskError> {
        let config = &dispatcher.registry.config;
        let lanes = dispatcher
            .registry
            .lanes()
            .iter()
            .cloned()
            .map(|conf| {
                let now = tokio::time::Instant::now();
                LaneQueue {
                    local_rate: conf
                        .rate()
                        .map(|rate| super::rate::LocalRateBucket::new(rate, now)),
                    conf,
                    tasks: VecDeque::new(),
                    running: 0,
                    uncommitted: 0,
                    poll_after: now,
                    owner_token: None,
                    owner_renew_at: None,
                    owner_generation: 0,
                    owner_phase: super::LaneOwnerPhase::Active,
                    completed_work: false,
                    hook_result: None,
                }
            })
            .collect();
        Ok(Self {
            lanes,
            cursor: 0,
            concurrency: config.concurrency_value(),
            batch_size: config.batch_size_value(),
            lease_duration: config.lease_duration_value(),
            running: 0,
            running_tasks: HashMap::new(),
            running_invocations: HashMap::new(),
            pending_commits: VecDeque::new(),
            running_hooks: HashMap::new(),
            poll_interval: config.poll_interval_value(),
            fallback_interval: config.fallback_interval(),
            runner_id: uuid::Uuid::now_v7().to_string(),
            notifier: dispatcher.notifier.clone(),
            registry: dispatcher.registry.clone(),
            initialized: dispatcher.initialized.clone(),
            store: dispatcher.store.clone(),
            metrics: dispatcher.metrics.clone(),
            health: dispatcher.health.clone(),
            schedules: dispatcher.schedules.clone(),
        })
    }

    /// Runs until site shutdown while preserving bounded polling and commits.
    pub async fn run(mut self, site: Site) {
        let shutdown = site.shutdown_notifier();
        let (completion_tx, mut completion_rx) = mpsc::channel(self.concurrency);
        let hook_capacity = self.lanes.len().max(1);
        let (hook_tx, mut hook_rx) = mpsc::channel(hook_capacity);
        let mut state = RunState::new(self.batch_size, self.poll_interval);
        loop {
            self.prepare(
                &site,
                &completion_tx,
                &hook_tx,
                &mut completion_rx,
                &mut hook_rx,
                &mut state,
            )
            .await;
            if self.finished(&state) {
                break;
            }
            tokio::select! {
                _ = shutdown.notified(), if !state.shutting_down => {
                    state.shutting_down = true;
                    self.abort_hooks();
                    state.next_poll = tokio::time::Instant::now();
                },
                _ = self.notifier.notified(), if !state.shutting_down => self.wake(&mut state),
                completion = completion_rx.recv() => {
                    if let Some(completion) = completion {
                        self.accept_completion(completion, &mut state.commits);
                        self.schedule_tick(&mut state);
                    }
                },
                completion = hook_rx.recv() => {
                    if let Some(completion) = completion {
                        self.accept_hook(completion);
                        self.schedule_tick(&mut state);
                    }
                },
                _ = tokio::time::sleep_until(state.next_poll) => {},
            }
        }
    }

    async fn prepare(
        &mut self,
        site: &Site,
        sender: &mpsc::Sender<Completion>,
        hook_sender: &mpsc::Sender<HookCompletion>,
        receiver: &mut mpsc::Receiver<Completion>,
        hook_receiver: &mut mpsc::Receiver<HookCompletion>,
        state: &mut RunState,
    ) {
        self.fill_commits(&mut state.commits);
        self.drain_completions(receiver, &mut state.commits);
        self.drain_hooks(hook_receiver);
        self.tick_due(site, sender, hook_sender, state).await;
    }

    /// Performs at most one store turn per configured poll interval.
    async fn tick_due(
        &mut self,
        site: &Site,
        sender: &mpsc::Sender<Completion>,
        hook_sender: &mpsc::Sender<HookCompletion>,
        state: &mut RunState,
    ) {
        let now = tokio::time::Instant::now();
        if now < state.next_poll {
            return;
        }
        let claims = self.claims(!state.shutting_down);
        let renewals = self.renewal_leases(&state.commits);
        let started = std::time::Instant::now();
        let result = self
            .store
            .tick(&self.runner_id, &claims, &state.commits, &renewals)
            .await;
        state.last_tick = now;
        match result {
            Ok(tick) => self.apply_tick(
                site,
                sender,
                hook_sender,
                state,
                TickResult {
                    claims,
                    renewals,
                    tick,
                    started,
                },
            ),
            Err(error) => self.fail_tick(state, error),
        }
    }

    /// Applies a successful atomic scheduler turn to local queues and leases.
    fn apply_tick(
        &mut self,
        site: &Site,
        sender: &mpsc::Sender<Completion>,
        hook_sender: &mpsc::Sender<HookCompletion>,
        state: &mut RunState,
        result: TickResult,
    ) {
        if let Err(error) = validate_poll(&result.claims, &result.tick.poll) {
            self.fail_tick(state, error);
            return;
        }
        self.record_renewals(&result.renewals, &result.tick.lost);
        self.health.succeeded();
        if !state.commits.is_empty() {
            self.metrics.commit(result.started.elapsed(), false);
            self.wake_committed_lanes(&state.commits);
            self.acknowledge_commits(&state.commits);
            state.commits.clear();
        }
        let mut deadline =
            self.apply_poll_with_hooks(site, hook_sender, result.tick.poll, !state.shutting_down);
        if let Some(dispatch) = self.dispatch_ready(site, sender) {
            deadline = deadline.min(dispatch);
        }
        state.next_poll = self.next_tick_deadline(state, deadline);
        state.poll_error = self.poll_interval;
    }

    /// Retains pending work and applies bounded backoff after one failed store turn.
    fn fail_tick(&self, state: &mut RunState, error: TaskError) {
        self.metrics.store_failure();
        self.health.store_failed();
        super::diagnostics::log_runtime_error(&error, "durable task scheduler turn failed");
        let now = tokio::time::Instant::now();
        let retry = now + state.poll_error;
        state.next_poll = self
            .lease_renewal_deadline()
            .map_or(retry, |renewal| retry.min(renewal));
        state.poll_error = (state.poll_error * 2).min(self.fallback_interval);
    }

    /// Validates persistent lane, rate, and orphan state before workers start.
    pub async fn initialize(&self) -> Result<(), TaskError> {
        let conf = self.store_conf()?;
        let result = self
            .initialized
            .get_or_try_init(|| self.store.initialize(conf))
            .await
            .map(|_| ());
        match &result {
            Ok(()) => self.health.initialized(),
            Err(error) => {
                self.health.initialization_failed();
                super::diagnostics::log_runtime_error(
                    error,
                    "durable task runtime initialization failed",
                );
            }
        }
        result
    }

    fn finished(&self, state: &RunState) -> bool {
        state.shutting_down
            && self.running_tasks.is_empty()
            && self.running_hooks.is_empty()
            && self.queued() == 0
            && self.pending_commits.is_empty()
            && state.commits.is_empty()
    }

    fn wake(&mut self, state: &mut RunState) {
        self.wake_lanes();
        self.schedule_tick(state);
    }

    fn schedule_tick(&self, state: &mut RunState) {
        let eligible = state.last_tick + self.poll_interval;
        state.next_poll = state.next_poll.min(eligible);
    }

    /// Chooses the next legal scheduler turn from lane and lease deadlines.
    fn next_tick_deadline(
        &self,
        state: &RunState,
        lane_deadline: tokio::time::Instant,
    ) -> tokio::time::Instant {
        let deadline = self
            .lease_renewal_deadline()
            .map_or(lane_deadline, |renewal| lane_deadline.min(renewal));
        deadline.max(state.last_tick + self.poll_interval)
    }

    /// Renews active leases with half their configured duration still remaining.
    fn lease_renewal_deadline(&self) -> Option<tokio::time::Instant> {
        let task_deadline = (!self.running_tasks.is_empty()
            || self.queued() > 0
            || !self.pending_commits.is_empty())
        .then(|| tokio::time::Instant::now() + self.lease_duration / 2);
        let lane_deadline = self
            .lanes
            .iter()
            .filter_map(|lane| lane.owner_renew_at)
            .min();
        match (task_deadline, lane_deadline) {
            (Some(task), Some(lane)) => Some(task.min(lane)),
            (Some(task), None) => Some(task),
            (None, Some(lane)) => Some(lane),
            (None, None) => None,
        }
    }

    /// Collects every locally owned task, including claimed rows waiting in a lane queue.
    fn renewal_leases(&self, commits: &[TaskCommit]) -> Vec<super::TaskLease> {
        let queued = self
            .lanes
            .iter()
            .map(|lane| lane.tasks.len())
            .sum::<usize>();
        let mut leases = Vec::with_capacity(
            self.running_tasks.len() + queued + commits.len() + self.pending_commits.len(),
        );
        leases.extend(
            self.running_tasks
                .iter()
                .filter_map(|(task_id, invocation_id)| {
                    self.running_invocations
                        .get(invocation_id)
                        .map(|invocation| super::TaskLease {
                            task_id: *task_id,
                            lane: invocation.lane,
                            owner_token: invocation.owner_token.clone(),
                        })
                }),
        );
        leases.extend(self.lanes.iter().flat_map(|lane| {
            lane.tasks.iter().map(|task| super::TaskLease {
                task_id: task.id(),
                lane: lane.conf.lane(),
                owner_token: lane.owner_token.clone(),
            })
        }));
        leases.extend(
            commits
                .iter()
                .chain(&self.pending_commits)
                .map(|commit| super::TaskLease {
                    task_id: commit.task_id,
                    lane: commit.lane,
                    owner_token: commit.owner_token.clone(),
                }),
        );
        leases
    }

    fn record_renewals(&mut self, leases: &[super::TaskLease], lost: &[super::TaskId]) {
        for lease in leases {
            if let Some(invocation) = self
                .running_tasks
                .get(&lease.task_id)
                .and_then(|id| self.running_invocations.get(id))
            {
                self.metrics
                    .renewed(invocation.lane.as_str(), lost.contains(&lease.task_id));
            } else if self
                .pending_commits
                .iter()
                .any(|commit| commit.task_id == lease.task_id)
            {
                self.metrics
                    .renewed(lease.lane.as_str(), lost.contains(&lease.task_id));
            }
        }
        self.drop_lost(lost);
    }

    fn drop_lost(&mut self, ids: &[super::TaskId]) {
        for lane in &mut self.lanes {
            lane.tasks.retain(|task| !ids.contains(&task.id()));
        }
        self.drop_pending(ids);
        let invocations = ids
            .iter()
            .filter_map(|id| self.running_tasks.get(id).copied())
            .collect::<HashSet<_>>();
        for invocation in invocations {
            self.abort_invocation(invocation);
        }
        for id in ids {
            tracing::warn!(task_id = %id, "task lease ownership was lost");
        }
    }

    fn drop_pending(&mut self, ids: &[super::TaskId]) {
        let mut removed = HashMap::<TaskLane, usize>::new();
        self.pending_commits.retain(|commit| {
            let lost = ids.contains(&commit.task_id);
            if lost {
                *removed.entry(commit.lane).or_default() += 1;
            }
            !lost
        });
        for (lane, count) in removed {
            if let Some(queue) = self.lane_mut(lane) {
                queue.uncommitted = queue.uncommitted.saturating_sub(count);
            }
        }
    }

    /// Aborts one invocation and removes every task identity sharing its future.
    fn abort_invocation(&mut self, id: uuid::Uuid) {
        let Some(invocation) = self.running_invocations.remove(&id) else {
            return;
        };
        invocation.abort.abort();
        for task_id in &invocation.task_ids {
            self.running_tasks.remove(task_id);
        }
        self.running = self.running.saturating_sub(1);
        if let Some(lane) = self.lane_mut(invocation.lane) {
            lane.running = lane.running.saturating_sub(1);
        }
    }

    fn queued(&self) -> usize {
        self.lanes.iter().map(|lane| lane.tasks.len()).sum()
    }

    fn fill_commits(&mut self, commits: &mut Vec<TaskCommit>) {
        let remaining = self.batch_size.saturating_sub(commits.len());
        commits.extend(self.pending_commits.drain(..remaining));
    }

    /// Allocates one global claim budget fairly from the current lane cursor.
    fn claims(&mut self, allow_work: bool) -> Vec<LaneClaim> {
        let mut claims = Vec::with_capacity(self.lanes.len());
        let now = tokio::time::Instant::now();
        let mut capacity = self
            .concurrency
            .saturating_sub(self.running + self.queued());
        let mut batch = self.batch_size;
        for offset in 0..self.lanes.len() {
            let Some(index) = self.rotated_index(offset) else {
                continue;
            };
            let Some(lane) = self.lanes.get_mut(index) else {
                continue;
            };
            let locked = lane.conf.lane_lock().is_some();
            let owner_due = lane.owner_renew_at.is_some_and(|deadline| deadline <= now);
            if lane.poll_after > now && !owner_due {
                continue;
            }
            if locked {
                push_locked_claim(lane, allow_work, &mut batch, &mut claims);
                continue;
            }
            if !allow_work {
                continue;
            }
            if capacity == 0 || batch == 0 {
                continue;
            }
            let available = lane.available(self.batch_size).min(capacity).min(batch);
            let limit = lane.claim_limit(available, now);
            if limit > 0 {
                claims.push(LaneClaim {
                    lane: lane.conf.lane(),
                    limit,
                    owner: None,
                });
                capacity -= limit;
                batch -= limit;
            } else if available > 0
                && let Some(wait) = lane.local_rate_wake(now)
            {
                lane.poll_after = now + wait;
            }
        }
        claims
    }

    fn rotated_index(&self, offset: usize) -> Option<usize> {
        if self.lanes.is_empty() {
            None
        } else {
            Some((self.cursor + offset) % self.lanes.len())
        }
    }

    /// Enqueues claimed rows and derives the earliest useful monotonic wake.
    fn apply_poll_with_hooks(
        &mut self,
        site: &Site,
        hook_sender: &mpsc::Sender<HookCompletion>,
        poll: TaskPoll,
        spawn_hooks: bool,
    ) -> tokio::time::Instant {
        let now = tokio::time::Instant::now();
        let fallback = now + self.fallback_interval;
        let batch_size = self.batch_size;
        let poll_interval = self.poll_interval;
        let fallback_interval = self.fallback_interval;
        let lease_duration = self.lease_duration;
        let mut saw_lane = false;
        let mut effects = PollEffects::default();
        for result in poll.lanes {
            self.metrics
                .claimed(result.lane.as_str(), result.tasks.len(), result.reclaimed);
            let Some(lane) = self.lane_mut(result.lane) else {
                continue;
            };
            saw_lane = true;
            apply_lane_poll(
                lane,
                result,
                now,
                poll_interval,
                fallback_interval,
                lease_duration,
                &mut effects,
            );
        }
        self.apply_poll_effects(site, hook_sender, effects, spawn_hooks);
        if !saw_lane {
            self.lanes
                .iter_mut()
                .filter(|lane| lane.available(batch_size) > 0)
                .for_each(|lane| lane.poll_after = fallback);
        }
        self.rotate();
        self.next_lane_deadline(fallback)
    }

    /// Records owner transitions and starts only the currently fenced hook generation.
    fn apply_poll_effects(
        &mut self,
        site: &Site,
        hook_sender: &mpsc::Sender<HookCompletion>,
        effects: PollEffects,
        spawn_hooks: bool,
    ) {
        effects.record(&self.metrics);
        for (lane, action) in effects.hooks {
            if let Some((token, generation, action)) = action {
                if spawn_hooks {
                    self.spawn_hook(
                        site.clone(),
                        hook_sender.clone(),
                        lane,
                        token,
                        generation,
                        action,
                    );
                }
            } else {
                self.drop_lane(lane);
            }
        }
    }

    #[cfg(test)]
    fn apply_poll(&mut self, poll: TaskPoll) -> tokio::time::Instant {
        let now = tokio::time::Instant::now();
        let fallback = now + self.fallback_interval;
        let poll_interval = self.poll_interval;
        let fallback_interval = self.fallback_interval;
        let mut saw_lane = false;
        for result in poll.lanes {
            let Some(lane) = self.lane_mut(result.lane) else {
                continue;
            };
            saw_lane = true;
            lane.tasks.extend(result.tasks.into_iter().map(Arc::new));
            lane.poll_after = lane_deadline(
                now,
                result.saturated,
                lane.conf.global_rate(),
                result.next_wake_in,
                poll_interval,
                fallback_interval,
            );
        }
        if !saw_lane {
            self.wake_fallback(fallback);
        }
        self.rotate();
        self.next_lane_deadline(fallback)
    }

    #[cfg(test)]
    fn wake_fallback(&mut self, fallback: tokio::time::Instant) {
        let batch_size = self.batch_size;
        self.lanes
            .iter_mut()
            .filter(|lane| lane.available(batch_size) > 0)
            .for_each(|lane| lane.poll_after = fallback);
    }

    fn rotate(&mut self) {
        if !self.lanes.is_empty() {
            self.cursor = (self.cursor + 1) % self.lanes.len();
        }
    }

    /// Returns the earliest deadline belonging to a lane with local claim capacity.
    fn next_lane_deadline(&self, fallback: tokio::time::Instant) -> tokio::time::Instant {
        self.lanes
            .iter()
            .filter(|lane| lane.available(self.batch_size) > 0)
            .map(|lane| lane.poll_after)
            .min()
            .unwrap_or(fallback)
    }

    fn dispatch_ready(
        &mut self,
        site: &Site,
        sender: &mpsc::Sender<Completion>,
    ) -> Option<tokio::time::Instant> {
        while self.running < self.concurrency {
            let Some((lane, owner_token, records)) = self.pop_ready() else {
                break;
            };
            self.running += 1;
            self.spawn_task(site.clone(), sender.clone(), lane, owner_token, records);
        }
        self.lanes
            .iter()
            .filter(|lane| lane.conf.lane_lock().is_some() && !lane.tasks.is_empty())
            .map(|lane| lane.poll_after)
            .min()
    }

    /// Pops one runnable invocation while respecting lane quotas and fair rotation.
    fn pop_ready(&mut self) -> Option<(TaskLane, Option<String>, Vec<Arc<TaskRecord>>)> {
        let lane_count = self.lanes.len();
        let now = tokio::time::Instant::now();
        for offset in 0..self.lanes.len() {
            let index = self.rotated_index(offset)?;
            let queue = self.lanes.get_mut(index)?;
            if queue.running >= queue.conf.concurrency() {
                continue;
            }
            let locked = queue.conf.lane_lock().is_some();
            if locked && queue.claim_limit(1, now) == 0 {
                if let Some(wait) = queue.local_rate_wake(now) {
                    let rate_at = now + wait;
                    queue.poll_after = queue
                        .owner_renew_at
                        .map_or(rate_at, |renew| renew.min(rate_at));
                }
                continue;
            }
            let Some(record) = queue.tasks.pop_front() else {
                continue;
            };
            let limit = if locked {
                queue.claim_limit(queue.tasks.len().saturating_add(1), now)
            } else {
                queue.tasks.len().saturating_add(1)
            };
            let records = collect_invocation(queue, record, limit, &self.registry);
            if locked {
                queue.consume_local_rate(records.len(), now);
            }
            queue.running += 1;
            self.cursor = (index + 1) % lane_count;
            return Some((queue.conf.lane(), queue.owner_token.clone(), records));
        }
        None
    }

    /// Executes one claimed row and returns its payload-free lifecycle outcome.
    fn spawn_task(
        &mut self,
        site: Site,
        sender: mpsc::Sender<Completion>,
        lane: TaskLane,
        owner_token: Option<String>,
        records: Vec<Arc<TaskRecord>>,
    ) {
        let engine = self.registry.clone();
        let payload_limit = self.registry.config.payload_limit();
        let error_limit = self.registry.config.error_limit();
        let invocation_id = uuid::Uuid::now_v7();
        self.record_starts(&records);
        let future = execute_task(TaskExecution {
            invocation_id,
            engine,
            site,
            records: records.clone(),
            sender,
            lane,
            metrics: self.metrics.clone(),
            payload_limit,
            error_limit,
            owner_token: owner_token.clone(),
        });
        let handle = tokio::spawn(future);
        let task_ids = records.iter().map(|record| record.id()).collect::<Vec<_>>();
        self.replace_duplicates(&task_ids);
        for task_id in &task_ids {
            self.running_tasks.insert(*task_id, invocation_id);
        }
        let running = RunningInvocation {
            lane,
            owner_token,
            task_ids,
            abort: handle.abort_handle(),
        };
        self.running_invocations.insert(invocation_id, running);
    }

    fn record_starts(&self, records: &[Arc<TaskRecord>]) {
        let now = chrono::Utc::now();
        for record in records {
            let queue_time = (now - record.created_at).to_std().unwrap_or_default();
            self.metrics.started(record.name(), queue_time);
        }
        if let Some(record) = records.first()
            && self.registry.is_batch(record.name())
        {
            self.metrics.batch_started(record.name(), records.len());
        }
    }

    fn replace_duplicates(&mut self, task_ids: &[super::TaskId]) {
        let duplicates = task_ids
            .iter()
            .filter_map(|task_id| self.running_tasks.get(task_id).copied())
            .collect::<HashSet<_>>();
        for invocation in duplicates {
            self.abort_invocation(invocation);
            tracing::error!(%invocation, "duplicate local task invocation was replaced");
        }
    }

    /// Moves ready completions into the common bounded persistence batch.
    fn drain_completions(
        &mut self,
        receiver: &mut mpsc::Receiver<Completion>,
        commits: &mut Vec<TaskCommit>,
    ) {
        while commits.len() < self.batch_size {
            match receiver.try_recv() {
                Ok(completion) => self.accept_completion(completion, commits),
                Err(_) => break,
            }
        }
    }

    fn accept_completion(&mut self, completion: Completion, commits: &mut Vec<TaskCommit>) {
        let Some(invocation) = self.running_invocations.remove(&completion.invocation_id) else {
            return;
        };
        for task_id in invocation.task_ids {
            self.running_tasks.remove(&task_id);
        }
        self.running = self.running.saturating_sub(1);
        if let Some(lane) = self.lane_mut(completion.lane) {
            lane.running = lane.running.saturating_sub(1);
            lane.uncommitted = lane.uncommitted.saturating_add(completion.commits.len());
            lane.completed_work = true;
            lane.poll_after = tokio::time::Instant::now();
        }
        self.queue_commits(completion.commits, commits);
    }

    fn queue_commits(&mut self, values: Vec<TaskCommit>, commits: &mut Vec<TaskCommit>) {
        let remaining = self.batch_size.saturating_sub(commits.len());
        let mut values = values.into_iter();
        commits.extend(values.by_ref().take(remaining));
        self.pending_commits.extend(values);
    }

    fn drain_hooks(&mut self, receiver: &mut mpsc::Receiver<HookCompletion>) {
        while let Ok(completion) = receiver.try_recv() {
            self.accept_hook(completion);
        }
    }

    fn accept_hook(&mut self, completion: HookCompletion) {
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
        if let Some(lane) = self.lane_mut(completion.lane)
            && lane.owner_token.as_deref() == Some(completion.token.as_str())
        {
            lane.hook_result = Some(completion.result);
            lane.poll_after = tokio::time::Instant::now();
        }
    }

    fn spawn_hook(
        &mut self,
        site: Site,
        sender: mpsc::Sender<HookCompletion>,
        lane: TaskLane,
        token: String,
        generation: i64,
        action: super::LaneHookAction,
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
        let future = execute_hook(
            hook,
            site,
            sender,
            lane,
            token.clone(),
            generation,
            action,
            error_limit,
        );
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

    fn hook_for(
        &self,
        lane: TaskLane,
        action: super::LaneHookAction,
    ) -> Option<super::lane_lock::LaneHook> {
        let lane_lock = self
            .lanes
            .iter()
            .find(|queue| queue.conf.lane() == lane)?
            .conf
            .lane_lock()?;
        match action {
            super::LaneHookAction::Idle => lane_lock.idle_hook().cloned(),
            super::LaneHookAction::Busy => lane_lock.busy_hook().cloned(),
        }
    }

    fn abort_hooks(&mut self) {
        for (_, hook) in self.running_hooks.drain() {
            hook.abort.abort();
        }
    }

    fn drop_lane(&mut self, lane: TaskLane) {
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
            .pending_commits
            .iter()
            .filter_map(|commit| (commit.lane == lane).then_some(commit.task_id))
            .collect::<Vec<_>>();
        self.drop_pending(&pending);
        if let Some(queue) = self.lane_mut(lane) {
            queue.tasks.clear();
        }
    }

    fn lane_mut(&mut self, lane: TaskLane) -> Option<&mut LaneQueue> {
        self.lanes
            .iter_mut()
            .find(|queue| queue.conf.lane() == lane)
    }

    fn wake_lanes(&mut self) {
        let now = tokio::time::Instant::now();
        for lane in &mut self.lanes {
            lane.poll_after = now;
        }
    }

    fn wake_committed_lanes(&mut self, commits: &[TaskCommit]) {
        let now = tokio::time::Instant::now();
        for commit in commits {
            if let Some(lane) = self.lane_mut(commit.lane) {
                lane.poll_after = now;
            }
        }
    }

    fn acknowledge_commits(&mut self, commits: &[TaskCommit]) {
        let mut committed = HashMap::<TaskLane, usize>::new();
        for commit in commits {
            *committed.entry(commit.lane).or_default() += 1;
        }
        for (lane, count) in committed {
            if let Some(queue) = self.lane_mut(lane) {
                queue.uncommitted = queue.uncommitted.saturating_sub(count);
            }
        }
    }

    fn store_conf(&self) -> Result<super::TaskStoreConf, TaskError> {
        Ok(super::TaskStoreConf {
            handlers: self.registry.tasks.keys().cloned().collect(),
            lanes: self.lanes.iter().map(|lane| lane.conf.clone()).collect(),
            idempotency: self.registry.idempotency_conf()?,
            schedules: self.schedules.to_vec(),
            poll_interval: self.poll_interval,
        })
    }
}

impl PollEffects {
    /// Emits bounded metrics after lane borrows from the poll loop are released.
    fn record(&self, metrics: &super::TaskMetrics) {
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
fn apply_lane_poll(
    lane: &mut LaneQueue,
    result: super::LanePoll,
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

fn owned_lane_deadline(
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
fn apply_owner_poll(
    lane: &mut LaneQueue,
    lane_name: TaskLane,
    owner: super::LaneOwnerPoll,
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
    lane.completed_work = lane.completed_work && owner.phase == super::LaneOwnerPhase::IdleFailed;
    lane.hook_result = None;
    capture_owner_edge(lane_name, previous.as_ref(), &owner, effects);
    if previous_phase != owner.phase {
        effects.transitions.push((lane_name, owner.phase));
    }
}

/// Distinguishes intentional idle release from lease or fencing-token loss.
fn capture_owner_edge(
    lane: TaskLane,
    previous: Option<&String>,
    owner: &super::LaneOwnerPoll,
    effects: &mut PollEffects,
) {
    if previous.is_some() && owner.token.is_none() {
        if matches!(
            owner.phase,
            super::LaneOwnerPhase::Active
                | super::LaneOwnerPhase::Idling
                | super::LaneOwnerPhase::Busying
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
fn push_locked_claim(
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
        .map_or(1, super::TaskLaneLock::batch_size);
    let quiescent = lane.tasks.is_empty() && lane.running == 0 && lane.uncommitted == 0;
    let allow_claim = allow_work && quiescent && *batch >= size;
    claims.push(locked_claim(lane, allow_claim));
    if allow_claim {
        *batch -= size;
    }
}

/// Forms one invocation from matching rows already held in a lane queue.
fn collect_invocation(
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
async fn execute_task(execution: TaskExecution) {
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

fn panic_results(records: Vec<Arc<TaskRecord>>) -> Vec<super::handler::TaskExecutionResult> {
    records
        .into_iter()
        .map(|record| super::handler::TaskExecutionResult {
            record,
            outcome: super::TaskOutcome::fail("Task handler panicked"),
        })
        .collect()
}

fn task_commits(
    execution: &TaskExecution,
    results: Vec<super::handler::TaskExecutionResult>,
) -> Vec<TaskCommit> {
    results
        .into_iter()
        .map(|result| {
            let outcome = super::store::normalize_outcome(
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

/// Contains one lifecycle hook and reports its fenced result without blocking the runner.
async fn execute_hook(
    hook: super::lane_lock::LaneHook,
    site: Site,
    sender: mpsc::Sender<HookCompletion>,
    lane: TaskLane,
    token: String,
    generation: i64,
    action: super::LaneHookAction,
    error_limit: usize,
) {
    let future = std::panic::AssertUnwindSafe(hook.call(site, lane, generation)).catch_unwind();
    let result = match future.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(super::store::truncate_utf8(error.to_string(), error_limit)),
        Err(_) => Err("task lane lifecycle hook panicked".into()),
    };
    let completion = HookCompletion {
        lane,
        token,
        result: super::LaneHookResult {
            generation,
            action,
            result,
        },
    };
    if sender.send(completion).await.is_err() {
        tracing::error!(lane = %lane, "task lane hook completion channel closed");
    }
}

fn lane_deadline(
    now: tokio::time::Instant,
    saturated: bool,
    global_rate: Option<super::TaskRate>,
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

fn prior_tick(now: tokio::time::Instant, poll: tokio::time::Duration) -> tokio::time::Instant {
    match now.checked_sub(poll) {
        Some(value) => value,
        None => now,
    }
}

/// Validates internal per-lane results before they enter scheduler queues.
fn validate_poll(claims: &[LaneClaim], poll: &TaskPoll) -> Result<(), TaskError> {
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

fn invalid_lane_poll(lane: TaskLane) -> TaskError {
    TaskError::TaskExecutionError(format!(
        "task store returned invalid polling evidence for lane '{lane}'"
    ))
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use super::*;
    use crate::Data;
    use crate::tasks::{
        DEFAULT_TASK_LANE, LanePoll, RegisteredTask, TaskConf, TaskLane, TaskRate, TaskRegistry,
        store::MemoryTaskStore,
    };

    const EMAIL: TaskLane = TaskLane::new("email");
    static HOOK_GATE: tokio::sync::Notify = tokio::sync::Notify::const_new();

    #[derive(Clone, serde::Deserialize, schemars::JsonSchema, serde::Serialize)]
    struct PanicJob;

    #[derive(Clone, serde::Deserialize, schemars::JsonSchema, serde::Serialize)]
    struct BatchJob;

    #[derive(Clone, serde::Deserialize, schemars::JsonSchema, serde::Serialize)]
    struct OtherBatchJob;

    async fn panic_job(_: Data<PanicJob>) {
        panic!("deliberate task panic");
    }

    async fn batch_job(_: Data<super::super::Batch<BatchJob>>) {}

    async fn batch_panic_job(_: Data<super::super::Batch<BatchJob>>) {
        panic!("deliberate batch task panic");
    }

    async fn other_batch_job(_: Data<super::super::Batch<OtherBatchJob>>) {}

    async fn wait_lane_hook() -> Result<(), crate::Error> {
        HOOK_GATE.notified().await;
        Ok(())
    }

    async fn panic_lane_hook() -> Result<(), crate::Error> {
        panic!("deliberate lane hook panic");
    }

    fn panic_record() -> Result<Arc<TaskRecord>, TaskError> {
        let now = chrono::Utc::now();
        Ok(Arc::new(TaskRecord {
            id: super::super::TaskId::new(uuid::Uuid::now_v7()),
            parent_id: None,
            root_id: None,
            kind: super::super::TaskKind::Work,
            name: "panic-job".into(),
            input: serde_json::to_string(&PanicJob)?,
            state: None,
            resume_input: None,
            status: super::super::TaskStatus::Running,
            attempts: 1,
            lane: DEFAULT_TASK_LANE.to_string(),
            lease_duration_ms: None,
            last_error: None,
            idempotency_key: None,
            idempotency_fingerprint: None,
            idempotency_expires_at: None,
            locked_by: Some("runner".into()),
            leased_until: None,
            ready_at: Some(now),
            created_at: now,
            updated_at: now,
            completed_at: None,
        }))
    }

    /// Builds a two-lane runner with a claim batch smaller than global capacity.
    fn lane_runner() -> Result<AbstractTaskRunner<MemoryTaskStore>, TaskError> {
        let conf = TaskConf::default()
            .concurrency(3)
            .batch_size(2)
            .lane(TaskLaneConf::new(DEFAULT_TASK_LANE, 2))
            .lane(
                TaskLaneConf::new(EMAIL, 1)
                    .rate_limit(TaskRate::per_minute(1).burst(1))
                    .global_rate_limit(TaskRate::per_minute(1).burst(1)),
            );
        let registry = Arc::new(TaskRegistry::new().with_config(conf)?);
        let dispatcher = registry.dispatcher(Arc::new(MemoryTaskStore::new(2)), Vec::new());
        AbstractTaskRunner::new(dispatcher)
    }

    fn prefetch_runner() -> Result<AbstractTaskRunner<MemoryTaskStore>, TaskError> {
        let conf = TaskConf::default()
            .concurrency(4)
            .batch_size(4)
            .lane(TaskLaneConf::new(DEFAULT_TASK_LANE, 4));
        let registry = Arc::new(TaskRegistry::new().with_config(conf)?);
        let dispatcher = registry.dispatcher(Arc::new(MemoryTaskStore::new(4)), Vec::new());
        AbstractTaskRunner::new(dispatcher)
    }

    fn batch_runner() -> Result<AbstractTaskRunner<MemoryTaskStore>, TaskError> {
        let conf = TaskConf::default()
            .concurrency(4)
            .batch_size(4)
            .lane(TaskLaneConf::new(DEFAULT_TASK_LANE, 4));
        let mut registry = TaskRegistry::new();
        registry.register(RegisteredTask::new_batch(
            crate::tasks::TaskDefinition::new("batch-job"),
            batch_job,
        ))?;
        let registry = Arc::new(registry.with_config(conf)?);
        let dispatcher = registry.dispatcher(Arc::new(MemoryTaskStore::new(4)), Vec::new());
        AbstractTaskRunner::new(dispatcher)
    }

    fn locked_runner() -> Result<AbstractTaskRunner<MemoryTaskStore>, TaskError> {
        let conf = TaskConf::default()
            .concurrency(2)
            .batch_size(2)
            .lane(TaskLaneConf::new(DEFAULT_TASK_LANE, 1))
            .lane(TaskLaneConf::new(EMAIL, 1).lock(super::super::TaskLaneLock::new(2)));
        let registry = Arc::new(TaskRegistry::new().with_config(conf)?);
        let dispatcher = registry.dispatcher(Arc::new(MemoryTaskStore::new(2)), Vec::new());
        AbstractTaskRunner::new(dispatcher)
    }

    fn locked_batch_runner() -> Result<AbstractTaskRunner<MemoryTaskStore>, TaskError> {
        let conf = TaskConf::default()
            .concurrency(2)
            .batch_size(2)
            .lane(TaskLaneConf::new(DEFAULT_TASK_LANE, 1))
            .lane(TaskLaneConf::new(EMAIL, 1).lock(super::super::TaskLaneLock::new(2)));
        let mut registry = TaskRegistry::new();
        registry.register(RegisteredTask::new_batch(
            crate::tasks::TaskDefinition::new("batch-job").lane(EMAIL),
            batch_job,
        ))?;
        let registry = Arc::new(registry.with_config(conf)?);
        let dispatcher = registry.dispatcher(Arc::new(MemoryTaskStore::new(2)), Vec::new());
        AbstractTaskRunner::new(dispatcher)
    }

    fn multi_batch_runner() -> Result<AbstractTaskRunner<MemoryTaskStore>, TaskError> {
        let conf = TaskConf::default()
            .concurrency(4)
            .batch_size(4)
            .lane(TaskLaneConf::new(DEFAULT_TASK_LANE, 4));
        let mut registry = TaskRegistry::new();
        registry.register(RegisteredTask::new_batch(
            crate::tasks::TaskDefinition::new("batch-job"),
            batch_job,
        ))?;
        registry.register(RegisteredTask::new_batch(
            crate::tasks::TaskDefinition::new("other-batch-job"),
            other_batch_job,
        ))?;
        let registry = Arc::new(registry.with_config(conf)?);
        let dispatcher = registry.dispatcher(Arc::new(MemoryTaskStore::new(4)), Vec::new());
        AbstractTaskRunner::new(dispatcher)
    }

    fn locked_rate_runner() -> Result<AbstractTaskRunner<MemoryTaskStore>, TaskError> {
        let conf = TaskConf::default()
            .concurrency(2)
            .batch_size(2)
            .lane(TaskLaneConf::new(DEFAULT_TASK_LANE, 1))
            .lane(
                TaskLaneConf::new(EMAIL, 1)
                    .rate_limit(TaskRate::per_minute(1).burst(1))
                    .lock(super::super::TaskLaneLock::new(2)),
            );
        let registry = Arc::new(TaskRegistry::new().with_config(conf)?);
        let dispatcher = registry.dispatcher(Arc::new(MemoryTaskStore::new(2)), Vec::new());
        AbstractTaskRunner::new(dispatcher)
    }

    fn locked_batch_rate_runner() -> Result<AbstractTaskRunner<MemoryTaskStore>, TaskError> {
        let conf = TaskConf::default()
            .concurrency(2)
            .batch_size(2)
            .lane(TaskLaneConf::new(DEFAULT_TASK_LANE, 1))
            .lane(
                TaskLaneConf::new(EMAIL, 1)
                    .rate_limit(TaskRate::new(1, Duration::from_millis(10)).burst(1))
                    .lock(super::super::TaskLaneLock::new(2)),
            );
        let mut registry = TaskRegistry::new();
        registry.register(RegisteredTask::new_batch(
            crate::tasks::TaskDefinition::new("batch-job").lane(EMAIL),
            batch_job,
        ))?;
        let registry = Arc::new(registry.with_config(conf)?);
        let dispatcher = registry.dispatcher(Arc::new(MemoryTaskStore::new(2)), Vec::new());
        AbstractTaskRunner::new(dispatcher)
    }

    fn lane_record(lane: TaskLane) -> Result<Arc<TaskRecord>, TaskError> {
        let mut record = panic_record()?.as_ref().clone();
        record.lane = lane.to_string();
        Ok(Arc::new(record))
    }

    fn named_record(name: &str) -> Result<Arc<TaskRecord>, TaskError> {
        let mut record = panic_record()?.as_ref().clone();
        record.name = name.into();
        Ok(Arc::new(record))
    }

    /// Verifies batch handlers drain only matching work already in the local queue.
    #[test]
    fn batch_dispatch_groups_matching_queue_rows() -> Result<(), TaskError> {
        let mut runner = batch_runner()?;
        let lane = runner
            .lane_mut(DEFAULT_TASK_LANE)
            .ok_or_else(|| TaskError::UnknownLane(DEFAULT_TASK_LANE.to_string()))?;
        lane.tasks.push_back(named_record("batch-job")?);
        lane.tasks.push_back(named_record("ordinary-job")?);
        lane.tasks.push_back(named_record("batch-job")?);

        let (_, _, records) = runner
            .pop_ready()
            .ok_or_else(|| TaskError::TaskExecutionError("batch was not dispatched".into()))?;
        assert_eq!(records.len(), 2);
        assert!(records.iter().all(|record| record.name() == "batch-job"));
        let lane = runner
            .lane_mut(DEFAULT_TASK_LANE)
            .ok_or_else(|| TaskError::UnknownLane(DEFAULT_TASK_LANE.to_string()))?;
        assert_eq!(lane.running, 1);
        assert_eq!(
            lane.tasks.front().map(|record| record.name()),
            Some("ordinary-job")
        );
        Ok(())
    }

    /// Verifies interleaved batch-handler names each retain their own stable local ordering.
    #[test]
    fn batch_dispatch_keeps_handler_groups_separate() -> Result<(), TaskError> {
        let mut runner = multi_batch_runner()?;
        let lane = runner
            .lane_mut(DEFAULT_TASK_LANE)
            .ok_or_else(|| TaskError::UnknownLane(DEFAULT_TASK_LANE.to_string()))?;
        lane.tasks.push_back(named_record("batch-job")?);
        lane.tasks.push_back(named_record("other-batch-job")?);
        lane.tasks.push_back(named_record("ordinary-job")?);
        lane.tasks.push_back(named_record("batch-job")?);
        lane.tasks.push_back(named_record("other-batch-job")?);

        let (_, _, first) = runner.pop_ready().ok_or_else(|| {
            TaskError::TaskExecutionError("first batch was not dispatched".into())
        })?;
        assert_eq!(first.len(), 2);
        assert!(first.iter().all(|record| record.name() == "batch-job"));
        let lane = runner
            .lane_mut(DEFAULT_TASK_LANE)
            .ok_or_else(|| TaskError::UnknownLane(DEFAULT_TASK_LANE.to_string()))?;
        lane.running = 0;

        let (_, _, second) = runner.pop_ready().ok_or_else(|| {
            TaskError::TaskExecutionError("second batch was not dispatched".into())
        })?;
        assert_eq!(second.len(), 2);
        assert!(
            second
                .iter()
                .all(|record| record.name() == "other-batch-job")
        );
        assert_eq!(
            runner
                .lane_mut(DEFAULT_TASK_LANE)
                .and_then(|lane| lane.tasks.front())
                .map(|record| record.name()),
            Some("ordinary-job")
        );
        Ok(())
    }

    /// Verifies local handler grouping is unchanged when its lane has durable ownership.
    #[test]
    fn locked_lane_batching_remains_local() -> Result<(), TaskError> {
        let mut runner = locked_batch_runner()?;
        let lane = runner
            .lane_mut(EMAIL)
            .ok_or_else(|| TaskError::UnknownLane(EMAIL.to_string()))?;
        lane.owner_token = Some("owner".into());
        lane.tasks.push_back(lane_record(EMAIL).map(|record| {
            let mut record = record.as_ref().clone();
            record.name = "batch-job".into();
            Arc::new(record)
        })?);
        lane.tasks.push_back(lane_record(EMAIL).map(|record| {
            let mut record = record.as_ref().clone();
            record.name = "batch-job".into();
            Arc::new(record)
        })?);

        let (claimed_lane, owner_token, records) = runner.pop_ready().ok_or_else(|| {
            TaskError::TaskExecutionError("locked batch was not dispatched".into())
        })?;
        assert_eq!(claimed_lane, EMAIL);
        assert_eq!(owner_token.as_deref(), Some("owner"));
        assert_eq!(records.len(), 2);
        assert_eq!(runner.lane_mut(EMAIL).map(|lane| lane.running), Some(1));
        Ok(())
    }

    /// Verifies one local batch invocation consumes one global slot while retaining both commits.
    #[tokio::test]
    async fn batch_invocation_uses_one_global_slot_and_commits_every_member() -> Result<(), String>
    {
        let mut runner = batch_runner().map_err(|error| error.to_string())?;
        let site = crate::Site::build(
            crate::SiteConf::default().log_init(false),
            crate::bundles::bundle([]),
        )
        .await
        .map_err(|error| error.to_string())?;
        let lane = runner
            .lane_mut(DEFAULT_TASK_LANE)
            .ok_or_else(|| "default lane is missing".to_string())?;
        lane.tasks
            .push_back(named_record("batch-job").map_err(|error| error.to_string())?);
        lane.tasks
            .push_back(named_record("batch-job").map_err(|error| error.to_string())?);
        let (sender, mut receiver) = mpsc::channel(1);
        runner.dispatch_ready(&site, &sender);
        assert_eq!(runner.running, 1);
        assert_eq!(runner.running_tasks.len(), 2);
        let completion = receiver
            .recv()
            .await
            .ok_or_else(|| "batch completion is missing".to_string())?;
        assert_eq!(completion.commits.len(), 2);
        let mut commits = Vec::new();
        runner.accept_completion(completion, &mut commits);
        assert_eq!(runner.running, 0);
        assert_eq!(commits.len(), 2);
        site.shutdown_and_wait().await;
        Ok(())
    }

    /// Verifies losing one constituent lease aborts and removes the whole batch future.
    #[tokio::test]
    async fn batch_lease_loss_aborts_invocation() -> Result<(), TaskError> {
        let mut runner = batch_runner()?;
        let first = named_record("batch-job")?;
        let second = named_record("batch-job")?;
        let invocation_id = uuid::Uuid::now_v7();
        let future = tokio::spawn(std::future::pending::<()>());
        let invocation = RunningInvocation {
            lane: DEFAULT_TASK_LANE,
            owner_token: None,
            task_ids: vec![first.id(), second.id()],
            abort: future.abort_handle(),
        };
        runner.running = 1;
        runner.running_tasks.insert(first.id(), invocation_id);
        runner.running_tasks.insert(second.id(), invocation_id);
        runner.running_invocations.insert(invocation_id, invocation);
        let lane = runner
            .lane_mut(DEFAULT_TASK_LANE)
            .ok_or_else(|| TaskError::UnknownLane(DEFAULT_TASK_LANE.to_string()))?;
        lane.running = 1;

        runner.drop_lost(&[first.id()]);

        assert!(runner.running_invocations.is_empty());
        assert!(runner.running_tasks.is_empty());
        assert_eq!(runner.running, 0);
        assert!(future.await.is_err());
        Ok(())
    }

    /// Verifies a completion sent after batch lease loss cannot create stale commits.
    #[tokio::test]
    async fn stale_batch_completion_is_ignored_after_lease_loss() -> Result<(), TaskError> {
        let mut runner = batch_runner()?;
        let first = named_record("batch-job")?;
        let second = named_record("batch-job")?;
        let invocation_id = uuid::Uuid::now_v7();
        let future = tokio::spawn(std::future::pending::<()>());
        runner.running = 1;
        runner.running_tasks.insert(first.id(), invocation_id);
        runner.running_tasks.insert(second.id(), invocation_id);
        runner.running_invocations.insert(
            invocation_id,
            RunningInvocation {
                lane: DEFAULT_TASK_LANE,
                owner_token: None,
                task_ids: vec![first.id(), second.id()],
                abort: future.abort_handle(),
            },
        );
        runner.drop_lost(&[first.id()]);
        let mut commits = Vec::new();
        runner.accept_completion(
            Completion {
                invocation_id,
                lane: DEFAULT_TASK_LANE,
                commits: vec![
                    TaskCommit {
                        task_id: first.id(),
                        lane: DEFAULT_TASK_LANE,
                        outcome: super::super::TaskOutcome::Complete,
                        owner_token: None,
                    },
                    TaskCommit {
                        task_id: second.id(),
                        lane: DEFAULT_TASK_LANE,
                        outcome: super::super::TaskOutcome::Complete,
                        owner_token: None,
                    },
                ],
            },
            &mut commits,
        );
        assert!(commits.is_empty());
        assert!(runner.pending_commits.is_empty());
        assert!(future.await.is_err());
        Ok(())
    }

    /// Verifies a fenced lane-owner loss aborts every local batch member and clears queued work.
    #[tokio::test]
    async fn owner_loss_aborts_the_entire_batch_invocation() -> Result<(), String> {
        let mut runner = locked_batch_runner().map_err(|error| error.to_string())?;
        let first = named_record("batch-job").map_err(|error| error.to_string())?;
        let second = named_record("batch-job").map_err(|error| error.to_string())?;
        let invocation_id = uuid::Uuid::now_v7();
        let future = tokio::spawn(std::future::pending::<()>());
        runner.running = 1;
        runner.running_tasks.insert(first.id(), invocation_id);
        runner.running_tasks.insert(second.id(), invocation_id);
        runner.running_invocations.insert(
            invocation_id,
            RunningInvocation {
                lane: EMAIL,
                owner_token: Some("owner".into()),
                task_ids: vec![first.id(), second.id()],
                abort: future.abort_handle(),
            },
        );
        let lane = runner
            .lane_mut(EMAIL)
            .ok_or_else(|| "locked lane is missing".to_string())?;
        lane.owner_token = Some("owner".into());
        lane.running = 1;
        lane.tasks.push_back(first.clone());
        runner.pending_commits.push_back(TaskCommit {
            task_id: second.id(),
            lane: EMAIL,
            outcome: super::super::TaskOutcome::Complete,
            owner_token: Some("owner".into()),
        });
        let site = crate::Site::build(
            crate::SiteConf::default().log_init(false),
            crate::bundles::bundle([]),
        )
        .await
        .map_err(|error| error.to_string())?;
        let (hook_sender, _hook_receiver) = mpsc::channel(1);
        runner.apply_poll_with_hooks(
            &site,
            &hook_sender,
            TaskPoll {
                lanes: vec![super::super::LanePoll {
                    lane: EMAIL,
                    tasks: Vec::new(),
                    reclaimed: 0,
                    saturated: false,
                    next_wake_in: None,
                    owner: Some(super::super::LaneOwnerPoll {
                        token: None,
                        generation: 2,
                        phase: super::super::LaneOwnerPhase::Active,
                        action: None,
                        takeover: false,
                    }),
                }],
            },
            false,
        );
        assert!(runner.running_invocations.is_empty());
        assert!(runner.running_tasks.is_empty());
        assert!(runner.pending_commits.is_empty());
        assert_eq!(runner.lane_mut(EMAIL).map(|lane| lane.tasks.len()), Some(0));
        assert!(future.await.is_err());
        site.shutdown_and_wait().await;
        Ok(())
    }

    /// Verifies completed rows awaiting a bounded commit remain centrally renewable.
    #[test]
    fn pending_batch_commits_retain_leases() -> Result<(), TaskError> {
        let mut runner = batch_runner()?;
        let record = named_record("batch-job")?;
        runner.pending_commits.push_back(TaskCommit {
            task_id: record.id(),
            lane: DEFAULT_TASK_LANE,
            outcome: super::super::TaskOutcome::Complete,
            owner_token: None,
        });
        let leases = runner.renewal_leases(&[]);
        assert_eq!(leases.len(), 1);
        assert_eq!(leases.first().map(|lease| lease.task_id), Some(record.id()));
        Ok(())
    }

    fn hooked_runner() -> Result<AbstractTaskRunner<MemoryTaskStore>, TaskError> {
        let lane_lock = super::super::TaskLaneLock::new(2)
            .on_idle(wait_lane_hook)
            .on_busy(wait_lane_hook);
        let conf = TaskConf::default()
            .concurrency(2)
            .batch_size(2)
            .lane(TaskLaneConf::new(DEFAULT_TASK_LANE, 1))
            .lane(TaskLaneConf::new(EMAIL, 1).lock(lane_lock));
        let registry = Arc::new(TaskRegistry::new().with_config(conf)?);
        let dispatcher = registry.dispatcher(Arc::new(MemoryTaskStore::new(2)), Vec::new());
        AbstractTaskRunner::new(dispatcher)
    }

    /// Verifies lane leases join central paced polling only at work or renewal deadlines.
    #[test]
    fn lane_lease_renewal_uses_the_central_claim_turn() -> Result<(), TaskError> {
        let mut runner = locked_runner()?;
        let now = tokio::time::Instant::now();
        let lane = runner
            .lane_mut(EMAIL)
            .ok_or_else(|| TaskError::UnknownLane(EMAIL.to_string()))?;
        lane.owner_token = Some("owner".into());
        lane.owner_renew_at = Some(now + Duration::from_secs(30));
        lane.poll_after = now + Duration::from_secs(60);
        assert!(runner.claims(true).iter().all(|claim| claim.lane != EMAIL));

        let lane = runner
            .lane_mut(EMAIL)
            .ok_or_else(|| TaskError::UnknownLane(EMAIL.to_string()))?;
        lane.owner_renew_at = Some(now);
        let claims = runner.claims(false);
        let renewal = claims
            .iter()
            .find(|claim| claim.lane == EMAIL)
            .and_then(|claim| claim.owner.as_ref())
            .ok_or_else(|| TaskError::TaskExecutionError("lane renewal claim is missing".into()))?;
        assert!(!renewal.allow_claim);
        Ok(())
    }

    /// Verifies a running hook leaves its owner renewal on the central scheduler turn.
    #[tokio::test]
    async fn running_hook_keeps_lane_renewal_centrally_scheduled() -> Result<(), String> {
        let mut runner = hooked_runner().map_err(|error| error.to_string())?;
        let site = crate::Site::build(
            crate::SiteConf::default().log_init(false),
            crate::bundles::bundle([]),
        )
        .await
        .map_err(|error| error.to_string())?;
        let now = tokio::time::Instant::now();
        let lane = runner
            .lane_mut(EMAIL)
            .ok_or_else(|| "locked lane is missing".to_string())?;
        lane.owner_token = Some("owner".into());
        lane.owner_renew_at = Some(now);
        let (sender, _receiver) = mpsc::channel(1);
        runner.spawn_hook(
            site.clone(),
            sender,
            EMAIL,
            "owner".into(),
            1,
            super::super::LaneHookAction::Idle,
        );
        let renewal = runner
            .claims(false)
            .into_iter()
            .find(|claim| claim.lane == EMAIL)
            .and_then(|claim| claim.owner)
            .ok_or_else(|| "hook owner renewal claim is missing".to_string())?;
        assert_eq!(renewal.token.as_deref(), Some("owner"));
        assert!(!renewal.allow_claim);
        runner.abort_hooks();
        site.shutdown_and_wait().await;
        Ok(())
    }

    /// Verifies locked cohorts apply local start limits while retaining claimed task leases.
    #[test]
    fn locked_lane_rates_tasks_at_dispatch() -> Result<(), TaskError> {
        let mut runner = locked_rate_runner()?;
        let lane = runner
            .lane_mut(EMAIL)
            .ok_or_else(|| TaskError::UnknownLane(EMAIL.to_string()))?;
        lane.owner_token = Some("owner".into());
        lane.tasks.push_back(lane_record(EMAIL)?);
        lane.tasks.push_back(lane_record(EMAIL)?);

        assert!(runner.pop_ready().is_some());
        let lane = runner
            .lane_mut(EMAIL)
            .ok_or_else(|| TaskError::UnknownLane(EMAIL.to_string()))?;
        lane.running = 0;
        let before = tokio::time::Instant::now();
        assert!(runner.pop_ready().is_none());
        let lane = runner
            .lane_mut(EMAIL)
            .ok_or_else(|| TaskError::UnknownLane(EMAIL.to_string()))?;
        assert!(lane.poll_after.duration_since(before) >= Duration::from_secs(59));
        Ok(())
    }

    /// Verifies local rate availability may split one locked lane's batch-handler queue.
    #[tokio::test]
    async fn locked_lane_rate_limit_splits_a_local_batch() -> Result<(), TaskError> {
        let mut runner = locked_batch_rate_runner()?;
        let lane = runner
            .lane_mut(EMAIL)
            .ok_or_else(|| TaskError::UnknownLane(EMAIL.to_string()))?;
        lane.owner_token = Some("owner".into());
        lane.tasks.push_back(lane_record(EMAIL).map(|record| {
            let mut record = record.as_ref().clone();
            record.name = "batch-job".into();
            Arc::new(record)
        })?);
        lane.tasks.push_back(lane_record(EMAIL).map(|record| {
            let mut record = record.as_ref().clone();
            record.name = "batch-job".into();
            Arc::new(record)
        })?);
        let (_, _, first) = runner.pop_ready().ok_or_else(|| {
            TaskError::TaskExecutionError("first batch member was not dispatched".into())
        })?;
        assert_eq!(first.len(), 1);
        runner
            .lane_mut(EMAIL)
            .ok_or_else(|| TaskError::UnknownLane(EMAIL.to_string()))?
            .running = 0;
        tokio::time::sleep(Duration::from_millis(12)).await;
        let (_, _, second) = runner.pop_ready().ok_or_else(|| {
            TaskError::TaskExecutionError("second batch member was not dispatched".into())
        })?;
        assert_eq!(second.len(), 1);
        Ok(())
    }

    /// Verifies a spawned lane hook neither consumes task concurrency nor blocks other futures.
    #[tokio::test]
    async fn lane_hook_runs_outside_task_concurrency() -> Result<(), String> {
        let mut runner = hooked_runner().map_err(|error| error.to_string())?;
        let site = crate::Site::build(
            crate::SiteConf::default().log_init(false),
            crate::bundles::Bundle::default(),
        )
        .await
        .map_err(|error| error.to_string())?;
        let (sender, _receiver) = mpsc::channel(1);
        runner.spawn_hook(
            site.clone(),
            sender,
            EMAIL,
            "owner".into(),
            1,
            super::super::LaneHookAction::Idle,
        );
        runner
            .lane_mut(DEFAULT_TASK_LANE)
            .ok_or_else(|| "default lane is missing".to_string())?
            .tasks
            .push_back(panic_record().map_err(|error| error.to_string())?);
        let (task_sender, mut task_receiver) = mpsc::channel(1);
        runner.dispatch_ready(&site, &task_sender);
        let completion = tokio::time::timeout(Duration::from_millis(50), task_receiver.recv())
            .await
            .map_err(|_| "task dispatch was blocked by lane hook".to_string())?
            .ok_or_else(|| "task completion channel closed".to_string())?;
        assert_eq!(completion.commits.len(), 1);
        let mut commits = Vec::new();
        runner.accept_completion(completion, &mut commits);
        assert_eq!(commits.len(), 1);
        assert_eq!(runner.running, 0);
        assert!(runner.running_tasks.is_empty());
        assert_eq!(runner.running_hooks.len(), 1);
        runner.drop_lane(EMAIL);
        assert!(runner.running_hooks.is_empty());
        site.shutdown_and_wait().await;
        Ok(())
    }

    /// Verifies a hook completion arriving after ownership loss cannot alter lane state.
    #[tokio::test]
    async fn late_hook_completion_is_rejected_after_ownership_loss() -> Result<(), String> {
        let mut runner = hooked_runner().map_err(|error| error.to_string())?;
        let site = crate::Site::build(
            crate::SiteConf::default().log_init(false),
            crate::bundles::bundle([]),
        )
        .await
        .map_err(|error| error.to_string())?;
        let (sender, _receiver) = mpsc::channel(1);
        runner.spawn_hook(
            site.clone(),
            sender,
            EMAIL,
            "old-owner".into(),
            3,
            super::super::LaneHookAction::Idle,
        );
        runner.drop_lane(EMAIL);
        runner
            .lane_mut(EMAIL)
            .ok_or_else(|| "locked lane is missing".to_string())?
            .owner_token = None;
        runner.accept_hook(HookCompletion {
            lane: EMAIL,
            token: "old-owner".into(),
            result: super::super::LaneHookResult {
                generation: 3,
                action: super::super::LaneHookAction::Idle,
                result: Ok(()),
            },
        });
        assert!(runner.running_hooks.is_empty());
        assert!(
            runner
                .lane_mut(EMAIL)
                .and_then(|lane| lane.hook_result.as_ref())
                .is_none()
        );
        site.shutdown_and_wait().await;
        Ok(())
    }

    /// Verifies a panicking lifecycle hook becomes a fenced failure completion.
    #[tokio::test]
    async fn lane_hook_panics_are_contained() -> Result<(), String> {
        let site = crate::Site::build(
            crate::SiteConf::default().log_init(false),
            crate::bundles::Bundle::default(),
        )
        .await
        .map_err(|error| error.to_string())?;
        let lane_lock = super::super::TaskLaneLock::new(1)
            .on_idle(panic_lane_hook)
            .on_busy(panic_lane_hook);
        let hook = lane_lock
            .idle_hook()
            .cloned()
            .ok_or_else(|| "idle hook is missing".to_string())?;
        let (sender, mut receiver) = mpsc::channel(1);
        execute_hook(
            hook,
            site.clone(),
            sender,
            EMAIL,
            "owner".into(),
            1,
            super::super::LaneHookAction::Idle,
            1024,
        )
        .await;
        let completion = receiver
            .recv()
            .await
            .ok_or_else(|| "hook completion is missing".to_string())?;
        assert!(completion.result.result.is_err());
        site.shutdown_and_wait().await;
        Ok(())
    }

    /// Verifies per-lane claims never reserve more than one global persistence batch.
    #[test]
    fn claims_share_global_capacity_and_batch_budget() -> Result<(), TaskError> {
        let mut runner = lane_runner()?;
        let first = runner.claims(true);
        assert_eq!(first.iter().map(|claim| claim.limit).sum::<usize>(), 2);
        assert_eq!(
            first.first().map(|claim| claim.lane),
            Some(DEFAULT_TASK_LANE)
        );

        runner.rotate();
        let second = runner.claims(true);
        assert_eq!(second.iter().map(|claim| claim.limit).sum::<usize>(), 2);
        assert_eq!(second.first().map(|claim| claim.lane), Some(EMAIL));
        Ok(())
    }

    /// Verifies a local rate bucket bounds claims before any task lease is acquired.
    #[test]
    fn local_rate_limits_lane_claim_budget() -> Result<(), TaskError> {
        let mut runner = lane_runner()?;
        runner.rotate();
        let claims = runner.claims(true);
        assert_eq!(
            claims
                .iter()
                .find(|claim| claim.lane == EMAIL)
                .map(|claim| claim.limit),
            Some(1)
        );

        let now = tokio::time::Instant::now();
        let email = runner
            .lane_mut(EMAIL)
            .ok_or_else(|| TaskError::UnknownLane(EMAIL.to_string()))?;
        email.consume_local_rate(1, now);
        assert_eq!(email.claim_limit(1, now), 0);
        assert!(email.local_rate_wake(now).is_some());
        Ok(())
    }

    /// Verifies a lane refills only after queued work falls below half its capacity.
    #[test]
    fn lane_prefetch_uses_a_half_capacity_low_watermark() -> Result<(), TaskError> {
        let mut runner = prefetch_runner()?;
        let lane = runner
            .lane_mut(DEFAULT_TASK_LANE)
            .ok_or_else(|| TaskError::UnknownLane(DEFAULT_TASK_LANE.to_string()))?;
        lane.tasks.push_back(panic_record()?);
        lane.tasks.push_back(panic_record()?);

        assert!(runner.claims(true).is_empty());

        let lane = runner
            .lane_mut(DEFAULT_TASK_LANE)
            .ok_or_else(|| TaskError::UnknownLane(DEFAULT_TASK_LANE.to_string()))?;
        lane.tasks.pop_back();
        let claims = runner.claims(true);
        assert_eq!(claims.first().map(|claim| claim.limit), Some(3));
        Ok(())
    }

    /// Verifies local wakeups move a fallback tick forward without breaking poll pacing.
    #[test]
    fn local_wake_waits_for_the_next_legal_tick() -> Result<(), TaskError> {
        let runner = prefetch_runner()?;
        let now = tokio::time::Instant::now();
        let mut state = RunState {
            last_tick: now,
            next_poll: now + runner.fallback_interval,
            poll_error: runner.poll_interval,
            commits: Vec::new(),
            shutting_down: false,
        };

        runner.schedule_tick(&mut state);

        assert!(state.next_poll >= now + runner.poll_interval);
        assert!(state.next_poll < now + runner.fallback_interval);
        Ok(())
    }

    /// Verifies an empty local permit budget preserves its next-token wake deadline.
    #[test]
    fn local_rate_wait_does_not_fall_back_to_idle_polling() -> Result<(), TaskError> {
        let conf = TaskConf::default().concurrency(1).lane(
            TaskLaneConf::new(DEFAULT_TASK_LANE, 1).rate_limit(TaskRate::per_minute(1).burst(1)),
        );
        let registry = Arc::new(TaskRegistry::new().with_config(conf)?);
        let dispatcher = registry.dispatcher(Arc::new(MemoryTaskStore::new(1)), Vec::new());
        let mut runner = AbstractTaskRunner::new(dispatcher)?;
        let now = tokio::time::Instant::now();
        let lane = runner
            .lane_mut(DEFAULT_TASK_LANE)
            .ok_or_else(|| TaskError::UnknownLane(DEFAULT_TASK_LANE.to_string()))?;
        lane.consume_local_rate(1, now);

        assert!(runner.claims(true).is_empty());
        let deadline = runner.next_lane_deadline(now + runner.fallback_interval);
        assert!(deadline.duration_since(now) >= tokio::time::Duration::from_secs(59));
        assert!(deadline.duration_since(now) < runner.fallback_interval);
        Ok(())
    }

    /// Verifies saturated work uses the short interval while idle work uses the fallback.
    #[test]
    fn adaptive_deadlines_distinguish_backlog_and_idle() -> Result<(), TaskError> {
        let mut runner = lane_runner()?;
        let before_idle = tokio::time::Instant::now();
        let idle = runner.apply_poll(TaskPoll::empty());
        assert!(idle.duration_since(before_idle) >= tokio::time::Duration::from_secs(299));

        let before_hot = tokio::time::Instant::now();
        let hot = runner.apply_poll(TaskPoll {
            lanes: vec![LanePoll {
                lane: DEFAULT_TASK_LANE,
                tasks: Vec::new(),
                reclaimed: 0,
                saturated: true,
                next_wake_in: None,
                owner: None,
            }],
        });
        assert!(hot.duration_since(before_hot) <= tokio::time::Duration::from_millis(1_100));
        Ok(())
    }

    /// Verifies each lane retains its own rate deadline while another lane stays hot.
    #[test]
    fn adaptive_deadlines_are_isolated_by_lane() -> Result<(), TaskError> {
        let mut runner = lane_runner()?;
        let now = tokio::time::Instant::now();
        let earliest = runner.apply_poll(TaskPoll {
            lanes: vec![
                LanePoll {
                    lane: DEFAULT_TASK_LANE,
                    tasks: Vec::new(),
                    reclaimed: 0,
                    saturated: true,
                    next_wake_in: None,
                    owner: None,
                },
                LanePoll {
                    lane: EMAIL,
                    tasks: Vec::new(),
                    reclaimed: 0,
                    saturated: true,
                    next_wake_in: Some(std::time::Duration::from_secs(60)),
                    owner: None,
                },
            ],
        });
        assert!(earliest.duration_since(now) <= tokio::time::Duration::from_millis(1_100));
        let email = runner
            .lane_mut(EMAIL)
            .ok_or_else(|| TaskError::UnknownLane(EMAIL.to_string()))?;
        assert!(email.poll_after.duration_since(now) >= tokio::time::Duration::from_secs(60));
        Ok(())
    }

    /// Verifies a capacity-blocked lane cannot keep the scheduler in an expired poll loop.
    #[test]
    fn unavailable_lanes_do_not_control_next_poll() -> Result<(), TaskError> {
        let mut runner = lane_runner()?;
        let now = tokio::time::Instant::now();
        let default = runner
            .lane_mut(DEFAULT_TASK_LANE)
            .ok_or_else(|| TaskError::UnknownLane(DEFAULT_TASK_LANE.to_string()))?;
        default.running = default.conf.concurrency();
        default.poll_after = now;
        let email = runner
            .lane_mut(EMAIL)
            .ok_or_else(|| TaskError::UnknownLane(EMAIL.to_string()))?;
        email.poll_after = now + tokio::time::Duration::from_secs(60);
        let deadline = runner.next_lane_deadline(now + runner.fallback_interval);
        assert!(deadline.duration_since(now) >= tokio::time::Duration::from_secs(60));
        Ok(())
    }

    /// Verifies malformed custom-store polling evidence fails before queue mutation.
    #[test]
    fn lane_poll_evidence_must_match_claims() {
        let claims = [LaneClaim {
            lane: DEFAULT_TASK_LANE,
            limit: 1,
            owner: None,
        }];
        assert!(matches!(
            validate_poll(&claims, &TaskPoll::empty()),
            Err(TaskError::TaskExecutionError(_))
        ));
    }

    /// Verifies a panicking handler becomes one generic terminal failure completion.
    #[tokio::test]
    async fn handler_panics_are_contained_as_terminal_failures() -> Result<(), String> {
        let mut registry = TaskRegistry::new()
            .with_config(TaskConf::default())
            .map_err(|error| error.to_string())?;
        registry
            .register(RegisteredTask::new(
                crate::tasks::TaskDefinition::new("panic-job"),
                panic_job,
            ))
            .map_err(|error| error.to_string())?;
        let site = crate::Site::build(
            crate::SiteConf::default().log_init(false),
            crate::bundles::bundle([]),
        )
        .await
        .map_err(|error| error.to_string())?;
        let (sender, mut receiver) = mpsc::channel(1);
        execute_task(TaskExecution {
            invocation_id: uuid::Uuid::now_v7(),
            engine: Arc::new(registry),
            site,
            records: vec![panic_record().map_err(|error| error.to_string())?],
            sender,
            lane: DEFAULT_TASK_LANE,
            metrics: Arc::new(super::super::TaskMetrics::new(
                ["panic-job".into()],
                [DEFAULT_TASK_LANE.to_string()],
            )),
            payload_limit: 1024,
            error_limit: 1024,
            owner_token: None,
        })
        .await;
        let completion = receiver.recv().await.ok_or("missing panic completion")?;
        let commit = completion.commits.first().ok_or("missing panic commit")?;
        assert!(matches!(
            commit.outcome,
            super::super::TaskOutcome::Fail { ref error } if error == "Task handler panicked"
        ));
        Ok(())
    }

    /// Verifies a panicking batch handler fails every constituent task without escaping the runner.
    #[tokio::test]
    async fn batch_handler_panics_are_contained_for_every_member() -> Result<(), String> {
        let mut registry = TaskRegistry::new()
            .with_config(TaskConf::default())
            .map_err(|error| error.to_string())?;
        registry
            .register(RegisteredTask::new_batch(
                crate::tasks::TaskDefinition::new("batch-panic-job"),
                batch_panic_job,
            ))
            .map_err(|error| error.to_string())?;
        let site = crate::Site::build(
            crate::SiteConf::default().log_init(false),
            crate::bundles::bundle([]),
        )
        .await
        .map_err(|error| error.to_string())?;
        let first = named_record("batch-panic-job").map_err(|error| error.to_string())?;
        let second = named_record("batch-panic-job").map_err(|error| error.to_string())?;
        let (sender, mut receiver) = mpsc::channel(1);
        execute_task(TaskExecution {
            invocation_id: uuid::Uuid::now_v7(),
            engine: Arc::new(registry),
            site,
            records: vec![first, second],
            sender,
            lane: DEFAULT_TASK_LANE,
            metrics: Arc::new(super::super::TaskMetrics::new(
                ["batch-panic-job".into()],
                [DEFAULT_TASK_LANE.to_string()],
            )),
            payload_limit: 1024,
            error_limit: 1024,
            owner_token: None,
        })
        .await;
        let completion = receiver
            .recv()
            .await
            .ok_or("missing batch panic completion")?;
        assert_eq!(completion.commits.len(), 2);
        assert!(completion.commits.iter().all(|commit| matches!(
            &commit.outcome,
            super::super::TaskOutcome::Fail { error } if error == "Task handler panicked"
        )));
        Ok(())
    }
}
