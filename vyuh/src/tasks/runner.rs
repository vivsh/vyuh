//! Fair per-lane task runner and adaptive polling loop.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use futures::FutureExt as _;
use tokio::sync::mpsc;

use crate::Site;

use super::{
    AbstractTaskStore, LaneClaim, TaskCommit, TaskDispatcher, TaskLane, TaskLaneConf, TaskPoll,
    TaskRecord, TaskRegistry, TaskRuntimeError, TaskTick,
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

    /// Bounds refills by executing, queued, and completed-but-uncommitted work.
    fn available(&self, batch_size: usize) -> usize {
        if self.needs_refill() {
            self.conf
                .concurrency()
                .saturating_sub(self.running + self.tasks.len())
                .saturating_sub(self.uncommitted)
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
    pub fn new(dispatcher: TaskDispatcher<S>) -> Result<Self, TaskRuntimeError> {
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
        self.limit_commits(&mut state.commits);
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
        self.record_renewals(&result.renewals, &result.tick.lost, &result.tick.cancelled);
        self.health.succeeded();
        let committed = !state.commits.is_empty();
        if committed {
            self.metrics.commit(result.started.elapsed(), false);
            self.wake_committed_lanes(&state.commits);
            self.acknowledge_commits(&state.commits);
            state.commits.clear();
        }
        let mut deadline = self.apply_poll_with_hooks(
            site,
            hook_sender,
            result.tick.poll,
            !state.shutting_down,
            committed,
        );
        let next_poll = state.last_tick + self.poll_interval;
        for lane in result.tick.wake_lanes {
            if let Some(queue) = self.lane_mut(lane) {
                queue.poll_after = next_poll;
                deadline = deadline.min(queue.poll_after);
            }
        }
        if let Some(dispatch) = self.dispatch_ready(site, sender) {
            deadline = deadline.min(dispatch);
        }
        state.next_poll = self.next_tick_deadline(state, deadline);
        state.poll_error = self.poll_interval;
    }

    /// Retains pending work and applies bounded backoff after one failed store turn.
    fn fail_tick(&self, state: &mut RunState, error: TaskRuntimeError) {
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
    pub async fn initialize(&self) -> Result<(), TaskRuntimeError> {
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
            && self.running_invocations.is_empty()
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

    fn record_renewals(
        &mut self,
        leases: &[super::TaskLease],
        lost: &[super::TaskId],
        cancelled: &[super::TaskId],
    ) {
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
        self.detach_cancelled(cancelled);
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
        let available = remaining.min(self.pending_commits.len());
        commits.extend(self.pending_commits.drain(..available));
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
        committed: bool,
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
        // A commit-only turn already woke its lanes; it is not an idle scan.
        if !saw_lane && !committed {
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

    fn queue_commits(&mut self, values: Vec<TaskCommit>, commits: &mut Vec<TaskCommit>) {
        if !self.pending_commits.is_empty() {
            self.pending_commits.extend(values);
            self.fill_commits(commits);
            return;
        }
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

    fn store_conf(&self) -> Result<super::TaskStoreConf, TaskRuntimeError> {
        Ok(super::TaskStoreConf {
            max_all_children: self.registry.config.all_limit(),
            handlers: self
                .registry
                .tasks
                .values()
                .map(|task| (task.name.clone(), task.kind()))
                .collect(),
            lanes: self.lanes.iter().map(|lane| lane.conf.clone()).collect(),
            idempotency: self.registry.idempotency_conf()?,
            schedules: self.schedules.to_vec(),
            poll_interval: self.poll_interval,
        })
    }
}

#[path = "runner_poll.rs"]
mod poll;
use poll::*;
#[path = "runner_all.rs"]
mod all;

#[cfg(test)]
#[path = "tests/runner.rs"]
mod tests;

#[path = "runner_dispatch.rs"]
mod dispatch;
