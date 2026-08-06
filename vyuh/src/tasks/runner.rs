//! Fair per-lane task runner and adaptive polling loop.

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use futures::FutureExt as _;
use tokio::sync::mpsc;

use crate::Site;

use super::{
    AbstractTaskStore, LaneClaim, TaskCommit, TaskDispatcher, TaskError, TaskLane, TaskLaneConf,
    TaskPoll, TaskRecord, TaskRegistry, TaskTick,
};

struct RunningTask {
    lane: TaskLane,
    abort: tokio::task::AbortHandle,
}

struct LaneQueue {
    conf: TaskLaneConf,
    local_rate: Option<super::rate::LocalRateBucket>,
    tasks: VecDeque<Arc<TaskRecord>>,
    running: usize,
    poll_after: tokio::time::Instant,
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
    lane: TaskLane,
    commit: TaskCommit,
}

struct TickResult {
    claims: Vec<LaneClaim>,
    renewals: Vec<super::TaskId>,
    tick: TaskTick,
    started: std::time::Instant,
}

struct TaskExecution {
    engine: Arc<TaskRegistry>,
    site: Site,
    record: Arc<TaskRecord>,
    sender: mpsc::Sender<Completion>,
    lane: TaskLane,
    metrics: Arc<super::TaskMetrics>,
    payload_limit: usize,
    error_limit: usize,
}

struct RunState {
    last_tick: tokio::time::Instant,
    next_poll: tokio::time::Instant,
    poll_error: tokio::time::Duration,
    commits: Vec<TaskCommit>,
    shutting_down: bool,
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
    running_tasks: HashMap<super::TaskId, RunningTask>,
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
                    poll_after: now,
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
        let mut state = RunState::new(self.batch_size, self.poll_interval);
        loop {
            self.prepare(&site, &completion_tx, &mut completion_rx, &mut state)
                .await;
            if self.finished(&state) {
                break;
            }
            tokio::select! {
                _ = shutdown.notified(), if !state.shutting_down => {
                    state.shutting_down = true;
                    state.next_poll = tokio::time::Instant::now();
                },
                _ = self.notifier.notified(), if !state.shutting_down => self.wake(&mut state),
                completion = completion_rx.recv() => {
                    if let Some(completion) = completion {
                        self.accept_completion(completion, &mut state.commits);
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
        receiver: &mut mpsc::Receiver<Completion>,
        state: &mut RunState,
    ) {
        self.drain_completions(receiver, &mut state.commits);
        self.tick_due(site, sender, state).await;
    }

    /// Performs at most one store turn per configured poll interval.
    async fn tick_due(
        &mut self,
        site: &Site,
        sender: &mpsc::Sender<Completion>,
        state: &mut RunState,
    ) {
        let now = tokio::time::Instant::now();
        if now < state.next_poll {
            return;
        }
        let claims = if !state.shutting_down && self.can_poll() {
            self.claims()
        } else {
            Vec::new()
        };
        let renewals = self.running_tasks.keys().copied().collect::<Vec<_>>();
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
            state.commits.clear();
        }
        let deadline = self.apply_poll(result.tick.poll);
        self.dispatch_ready(site, sender);
        state.next_poll = self.next_tick_deadline(state, deadline);
        state.poll_error = self.poll_interval;
    }

    /// Retains pending work and applies bounded backoff after one failed store turn.
    fn fail_tick(&self, state: &mut RunState, error: TaskError) {
        self.metrics.store_failure();
        self.health.store_failed();
        super::diagnostics::log_runtime_error(&error, "durable task scheduler turn failed");
        let now = tokio::time::Instant::now();
        state.next_poll = now + state.poll_error;
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
            && self.queued() == 0
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
        (!self.running_tasks.is_empty())
            .then(|| tokio::time::Instant::now() + self.lease_duration / 2)
    }

    fn record_renewals(&mut self, ids: &[super::TaskId], lost: &[super::TaskId]) {
        for id in ids {
            if let Some(task) = self.running_tasks.get(id) {
                self.metrics.renewed(task.lane.as_str(), lost.contains(id));
            }
        }
        self.drop_lost(lost);
    }

    fn drop_lost(&mut self, ids: &[super::TaskId]) {
        for id in ids {
            let Some(task) = self.running_tasks.remove(id) else {
                continue;
            };
            task.abort.abort();
            self.running = self.running.saturating_sub(1);
            if let Some(lane) = self.lane_mut(task.lane) {
                lane.running = lane.running.saturating_sub(1);
            }
            tracing::warn!(task_id = %id, "task lease ownership was lost");
        }
    }

    fn can_poll(&self) -> bool {
        self.running < self.concurrency
            && self
                .lanes
                .iter()
                .any(|lane| lane.available(self.batch_size) > 0)
    }

    fn queued(&self) -> usize {
        self.lanes.iter().map(|lane| lane.tasks.len()).sum()
    }

    /// Allocates one global claim budget fairly from the current lane cursor.
    fn claims(&mut self) -> Vec<LaneClaim> {
        let mut claims = Vec::with_capacity(self.lanes.len());
        let now = tokio::time::Instant::now();
        let mut remaining = self
            .concurrency
            .saturating_sub(self.running + self.queued())
            .min(self.batch_size);
        for offset in 0..self.lanes.len() {
            if remaining == 0 {
                break;
            }
            let Some(index) = self.rotated_index(offset) else {
                continue;
            };
            let Some(lane) = self.lanes.get_mut(index) else {
                continue;
            };
            if lane.poll_after > now {
                continue;
            }
            let capacity = lane.available(self.batch_size).min(remaining);
            let limit = lane.claim_limit(capacity, now);
            if limit > 0 {
                claims.push(LaneClaim {
                    lane: lane.conf.lane(),
                    limit,
                });
                remaining -= limit;
            } else if capacity > 0
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
    fn apply_poll(&mut self, poll: TaskPoll) -> tokio::time::Instant {
        let now = tokio::time::Instant::now();
        let fallback = now + self.fallback_interval;
        let batch_size = self.batch_size;
        let poll_interval = self.poll_interval;
        let fallback_interval = self.fallback_interval;
        let mut saw_lane = false;
        for result in poll.lanes {
            self.metrics
                .claimed(result.lane.as_str(), result.tasks.len(), result.reclaimed);
            let Some(lane) = self.lane_mut(result.lane) else {
                continue;
            };
            saw_lane = true;
            lane.consume_local_rate(result.tasks.len(), now);
            lane.tasks.extend(result.tasks.into_iter().map(Arc::new));
            let store_deadline = lane_deadline(
                now,
                result.saturated,
                lane.conf.global_rate(),
                result.next_wake_in,
                poll_interval,
                fallback_interval,
            );
            lane.poll_after = lane
                .local_rate_wake(now)
                .map_or(store_deadline, |wake| store_deadline.max(now + wake));
        }
        if !saw_lane {
            self.lanes
                .iter_mut()
                .filter(|lane| lane.available(batch_size) > 0)
                .for_each(|lane| lane.poll_after = fallback);
        }
        self.rotate();
        self.next_lane_deadline(fallback)
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

    fn dispatch_ready(&mut self, site: &Site, sender: &mpsc::Sender<Completion>) {
        while self.running < self.concurrency {
            let Some((lane, record)) = self.pop_ready() else {
                break;
            };
            self.running += 1;
            self.spawn_task(site.clone(), sender.clone(), lane, record);
        }
    }

    /// Pops one runnable row while respecting lane quotas and fair rotation.
    fn pop_ready(&mut self) -> Option<(TaskLane, Arc<TaskRecord>)> {
        let lane_count = self.lanes.len();
        for offset in 0..self.lanes.len() {
            let index = self.rotated_index(offset)?;
            let queue = self.lanes.get_mut(index)?;
            if queue.running >= queue.conf.concurrency() {
                continue;
            }
            if let Some(record) = queue.tasks.pop_front() {
                queue.running += 1;
                self.cursor = (index + 1) % lane_count;
                return Some((queue.conf.lane(), record));
            }
        }
        None
    }

    /// Executes one claimed row and returns its payload-free lifecycle outcome.
    fn spawn_task(
        &mut self,
        site: Site,
        sender: mpsc::Sender<Completion>,
        lane: TaskLane,
        record: Arc<TaskRecord>,
    ) {
        let engine = self.registry.clone();
        let payload_limit = self.registry.config.payload_limit();
        let error_limit = self.registry.config.error_limit();
        let task_id = record.id();
        let task_name = record.name().to_owned();
        let queue_time = (chrono::Utc::now() - record.created_at)
            .to_std()
            .unwrap_or_default();
        self.metrics.started(&task_name, queue_time);
        let future = execute_task(TaskExecution {
            engine,
            site,
            record,
            sender,
            lane,
            metrics: self.metrics.clone(),
            payload_limit,
            error_limit,
        });
        let handle = tokio::spawn(future);
        let running = RunningTask {
            lane,
            abort: handle.abort_handle(),
        };
        if let Some(previous) = self.running_tasks.insert(task_id, running) {
            previous.abort.abort();
            tracing::error!(%task_id, "duplicate local task execution was replaced");
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
        if self
            .running_tasks
            .remove(&completion.commit.task_id)
            .is_none()
        {
            return;
        }
        self.running = self.running.saturating_sub(1);
        if let Some(lane) = self.lane_mut(completion.lane) {
            lane.running = lane.running.saturating_sub(1);
            lane.poll_after = tokio::time::Instant::now();
        }
        commits.push(completion.commit);
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

    fn store_conf(&self) -> Result<super::TaskStoreConf, TaskError> {
        Ok(super::TaskStoreConf {
            handlers: self.registry.tasks.keys().cloned().collect(),
            lanes: self.lanes.iter().map(|lane| lane.conf.clone()).collect(),
            idempotency: self.registry.idempotency_conf()?,
            schedules: self.schedules.to_vec(),
        })
    }
}

/// Contains one handler panic and sends its normalized lifecycle completion.
async fn execute_task(execution: TaskExecution) {
    let task_id = execution.record.id();
    let task_name = execution.record.name().to_owned();
    let started = std::time::Instant::now();
    let outcome = match std::panic::AssertUnwindSafe(
        execution.engine.execute(execution.site, execution.record),
    )
    .catch_unwind()
    .await
    {
        Ok(outcome) => outcome,
        Err(_) => {
            tracing::error!(%task_id, "task handler panicked");
            super::TaskOutcome::fail("Task handler panicked")
        }
    };
    let outcome =
        super::store::normalize_outcome(outcome, execution.payload_limit, execution.error_limit);
    execution
        .metrics
        .completed(&task_name, &outcome, started.elapsed());
    let commit = TaskCommit {
        task_id,
        lane: execution.lane,
        outcome,
    };
    if execution
        .sender
        .send(Completion {
            lane: execution.lane,
            commit,
        })
        .await
        .is_err()
    {
        tracing::error!(%task_id, "task completion channel closed before commit");
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
    use std::sync::Arc;

    use super::*;
    use crate::Data;
    use crate::tasks::{
        DEFAULT_TASK_LANE, LanePoll, RegisteredTask, TaskConf, TaskLane, TaskRate, TaskRegistry,
        store::MemoryTaskStore,
    };

    const EMAIL: TaskLane = TaskLane::new("email");

    #[derive(Clone, serde::Deserialize, schemars::JsonSchema, serde::Serialize)]
    struct PanicJob;

    async fn panic_job(_: Data<PanicJob>) {
        panic!("deliberate task panic");
    }

    fn panic_record() -> Result<Arc<TaskRecord>, TaskError> {
        let now = chrono::Utc::now();
        Ok(Arc::new(TaskRecord {
            id: super::super::TaskId::new(uuid::Uuid::now_v7()),
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

    /// Verifies per-lane claims never reserve more than one global persistence batch.
    #[test]
    fn claims_share_global_capacity_and_batch_budget() -> Result<(), TaskError> {
        let mut runner = lane_runner()?;
        let first = runner.claims();
        assert_eq!(first.iter().map(|claim| claim.limit).sum::<usize>(), 2);
        assert_eq!(
            first.first().map(|claim| claim.lane),
            Some(DEFAULT_TASK_LANE)
        );

        runner.rotate();
        let second = runner.claims();
        assert_eq!(second.iter().map(|claim| claim.limit).sum::<usize>(), 2);
        assert_eq!(second.first().map(|claim| claim.lane), Some(EMAIL));
        Ok(())
    }

    /// Verifies a local rate bucket bounds claims before any task lease is acquired.
    #[test]
    fn local_rate_limits_lane_claim_budget() -> Result<(), TaskError> {
        let mut runner = lane_runner()?;
        runner.rotate();
        let claims = runner.claims();
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

        assert!(runner.claims().is_empty());

        let lane = runner
            .lane_mut(DEFAULT_TASK_LANE)
            .ok_or_else(|| TaskError::UnknownLane(DEFAULT_TASK_LANE.to_string()))?;
        lane.tasks.pop_back();
        let claims = runner.claims();
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

        assert!(runner.claims().is_empty());
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
                },
                LanePoll {
                    lane: EMAIL,
                    tasks: Vec::new(),
                    reclaimed: 0,
                    saturated: true,
                    next_wake_in: Some(std::time::Duration::from_secs(60)),
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
            engine: Arc::new(registry),
            site,
            record: panic_record().map_err(|error| error.to_string())?,
            sender,
            lane: DEFAULT_TASK_LANE,
            metrics: Arc::new(super::super::TaskMetrics::new(
                ["panic-job".into()],
                [DEFAULT_TASK_LANE.to_string()],
            )),
            payload_limit: 1024,
            error_limit: 1024,
        })
        .await;
        let completion = receiver.recv().await.ok_or("missing panic completion")?;
        assert!(matches!(
            completion.commit.outcome,
            super::super::TaskOutcome::Fail { ref error } if error == "Task handler panicked"
        ));
        Ok(())
    }
}
