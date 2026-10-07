//! Non-blocking task coordination; stores exclusively own durable task mutations.
use super::dispatcher::RunnerEvent;
use super::{
    AbstractTaskStore, CommitResult, LaneClaim, TaskCommit, TaskDispatcher, TaskLane, TaskLaneConf,
    TaskPoll, TaskRecord, TaskRegistry, TaskRuntimeError,
};
use crate::Site;
use futures::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
struct RunningInvocation {
    lane: TaskLane,
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
    execution_slots: Arc<Semaphore>,
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
    /// Constructs isolated lane admission and execution state from finalized configuration.
    fn new(conf: TaskLaneConf) -> Self {
        Self {
            local_rate: conf
                .rate()
                .map(|rate| super::rate::LocalRateBucket::new(rate, tokio::time::Instant::now())),
            execution_slots: Arc::new(Semaphore::new(conf.concurrency())),
            conf,
            tasks: VecDeque::new(),
            uncommitted: 0,
            poll_after: tokio::time::Instant::now(),
            owner_token: None,
            owner_renew_at: None,
            owner_generation: 0,
            owner_phase: super::LaneOwnerPhase::Active,
            completed_work: false,
            hook_result: None,
        }
    }

    /// Returns whether this lane has crossed its queued-work refill watermark.
    fn needs_refill(&self) -> bool {
        self.tasks.len().saturating_mul(2) < self.conf.concurrency()
    }

    /// Bounds refills by executing, queued, and completed-but-uncommitted work.
    fn available(&self, batch_size: usize) -> usize {
        if self.needs_refill() {
            self.conf
                .concurrency()
                .saturating_sub(
                    (self.conf.concurrency() - self.execution_slots.available_permits())
                        + self.tasks.len(),
                )
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
    global_permit: OwnedSemaphorePermit,
    lane_permit: OwnedSemaphorePermit,
}

struct HookCompletion {
    lane: TaskLane,
    token: String,
    result: super::LaneHookResult,
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
    node_id: uuid::Uuid,
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
    global_permit: OwnedSemaphorePermit,
    lane_permit: OwnedSemaphorePermit,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RunMode {
    Running,
    Draining,
    Faulted,
    Retired,
    Dead,
    Finished,
}

enum DbRequest {
    Heartbeat,
    GiveBack,
    Claim { claims: Vec<LaneClaim> },
    Flush { commits: Vec<TaskCommit> },
    Coordinate { lanes: Vec<LaneClaim> },
}

enum DbResponse {
    Heartbeat { present: bool },
    Returned,
    Poll(TaskPoll),
    Flushed(CommitResult),
}

struct DbCompletion {
    id: uuid::Uuid,
    result: Result<DbResponse, super::store::StoreFailure<DbResponse>>,
    permit: OwnedSemaphorePermit,
    started: std::time::Instant,
}

/// Coordinates lane-local inputs, spawned execution, and one shared output FIFO.
/// Recovery is poll-paced; cancellation only skips locally queued work, never interrupts handlers.
pub struct AbstractTaskRunner<S: AbstractTaskStore + Send + Sync + 'static> {
    lanes: VecDeque<LaneQueue>,
    concurrency: usize,
    batch_size: usize,
    lease_duration: tokio::time::Duration,
    execution_slots: Arc<Semaphore>,
    db_slots: Arc<Semaphore>,
    in_flight: HashMap<uuid::Uuid, InFlight>,
    operations: FuturesUnordered<BoxFuture<'static, DbCompletion>>,
    running_tasks: HashMap<super::TaskId, uuid::Uuid>,
    running_invocations: HashMap<uuid::Uuid, RunningInvocation>,
    pending_commits: VecDeque<TaskCommit>,
    running_hooks: HashMap<TaskLane, RunningLaneHook>,
    poll_interval: tokio::time::Duration,
    fallback_interval: tokio::time::Duration,
    flush_after: tokio::time::Instant,
    heartbeat_after: Option<tokio::time::Instant>,
    death_at: Option<tokio::time::Instant>,
    mode: RunMode,
    node_id: uuid::Uuid,
    events: tokio::sync::broadcast::Receiver<RunnerEvent>,
    registry: Arc<TaskRegistry>,
    store: Arc<S>,
    metrics: Arc<super::TaskMetrics>,
    health: super::TaskHealth,
}
impl<S: AbstractTaskStore + Send + Sync + 'static> std::fmt::Debug for AbstractTaskRunner<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskRunner")
            .field("node_id", &self.node_id)
            .field("lanes", &self.lanes.len())
            .finish()
    }
}
impl<S: AbstractTaskStore + Send + Sync + 'static> AbstractTaskRunner<S> {
    /// Creates independent handler and database concurrency budgets for one site.
    pub fn new(dispatcher: TaskDispatcher<S>) -> Result<Self, TaskRuntimeError> {
        let config = &dispatcher.registry.config;
        let lanes = dispatcher
            .registry
            .lanes()
            .iter()
            .cloned()
            .map(LaneQueue::new)
            .collect();
        Ok(Self {
            lanes,
            concurrency: config.concurrency_value(),
            batch_size: config.batch_size_value(),
            lease_duration: config.lease_duration_value(),
            execution_slots: Arc::new(Semaphore::new(config.concurrency_value())),
            db_slots: Arc::new(Semaphore::new(config.db_concurrency_value())),
            in_flight: HashMap::new(),
            operations: FuturesUnordered::new(),
            running_tasks: HashMap::new(),
            running_invocations: HashMap::new(),
            pending_commits: VecDeque::new(),
            running_hooks: HashMap::new(),
            poll_interval: config.poll_interval_value(),
            fallback_interval: config.fallback_interval(),
            flush_after: tokio::time::Instant::now(),
            heartbeat_after: None,
            death_at: None,
            mode: RunMode::Running,
            node_id: uuid::Uuid::now_v7(),
            events: dispatcher.events.subscribe(),
            registry: dispatcher.registry.clone(),
            store: dispatcher.store.clone(),
            metrics: dispatcher.metrics.clone(),
            health: dispatcher.health.clone(),
        })
    }

    /// Runs local execution and collection while admitted database futures are pending.
    pub async fn run(mut self, site: Site) {
        if self.registry.config.mode_value() == super::TaskMode::Clustered {
            self.schedule_heartbeat();
        }
        let shutdown = site.shutdown_notifier();
        let (tx, mut rx) = mpsc::channel(self.concurrency);
        let (hooks, mut hook_rx) = mpsc::channel(self.lanes.len().max(1));
        while !self.stopped() {
            if self.expire() {
                break;
            }
            self.collect_ready(&site, &hooks, &mut rx, &mut hook_rx);
            if self.stopped() {
                break;
            }
            self.advance_pipeline(&site, &tx);
            if self.expire() {
                break;
            }
            self.give_back();
            self.wait_event(&site, &hooks, &mut rx, &mut hook_rx, &shutdown)
                .await;
        }
    }

    /// Waits without allowing database work to block collection or the death deadline.
    async fn wait_event(
        &mut self,
        site: &Site,
        hooks: &mpsc::Sender<HookCompletion>,
        rx: &mut mpsc::Receiver<Completion>,
        hook_rx: &mut mpsc::Receiver<HookCompletion>,
        shutdown: &crate::notifiers::CancellationNotifier,
    ) {
        let deadline = self.deadline();
        tokio::select! {
            result = self.operations.next(), if !self.operations.is_empty() => {
                if let Some(result) = result { self.apply_database(site, hooks, result); }
            },
            completion = rx.recv() => {
                if let Some(completion) = completion { self.accept_completion(completion); }
            },
            completion = hook_rx.recv() => {
                if let Some(completion) = completion { self.accept_hook(completion); }
            },
            _ = async { if let Some(at) = self.death_at { tokio::time::sleep_until(at).await; }
                else { std::future::pending::<()>().await; } } => { self.expire(); },
            event = self.events.recv(), if !self.stopped() && !(self.events.is_closed() && self.events.is_empty()) => {
                self.accept_event(event);
            },
            _ = shutdown.notified(), if self.mode != RunMode::Draining => {
                self.mode = RunMode::Draining;
                self.abort_hooks();
            },
            _ = async { if let Some(at) = deadline { tokio::time::sleep_until(at).await; }
                else { std::future::pending::<()>().await; } } => {},
        }
    }

    fn advance_pipeline(&mut self, site: &Site, tx: &mpsc::Sender<Completion>) {
        if matches!(self.mode, RunMode::Running | RunMode::Draining) {
            self.dispatch_ready(site, tx);
            if !self.finished() {
                self.admit();
            }
        }
    }

    /// Drains a bounded number of events before revisiting dispatch and database admission.
    fn collect_ready(
        &mut self,
        site: &Site,
        hooks: &mpsc::Sender<HookCompletion>,
        rx: &mut mpsc::Receiver<Completion>,
        hook_rx: &mut mpsc::Receiver<HookCompletion>,
    ) {
        self.collect_events();
        for _ in 0..self.concurrency {
            match rx.try_recv() {
                Ok(value) => self.accept_completion(value),
                Err(_) => break,
            }
        }
        for _ in 0..self.lanes.len() {
            match hook_rx.try_recv() {
                Ok(value) => self.accept_hook(value),
                Err(_) => break,
            }
        }
        for _ in 0..self.registry.config.db_concurrency_value() {
            match self.operations.next().now_or_never().flatten() {
                Some(value) => self.apply_database(site, hooks, value),
                None => break,
            }
            if self.stopped() {
                break;
            }
        }
    }

    fn finished(&self) -> bool {
        self.mode == RunMode::Draining
            && self.running_invocations.is_empty()
            && self.pending_commits.is_empty()
            && self.in_flight.is_empty()
    }

    /// Admits eligible work without blocking on permits or creating a permit-waiter queue.
    fn admit(&mut self) {
        if self.expire() || self.stopped() {
            return;
        }
        while let Ok(permit) = self.db_slots.clone().try_acquire_owned() {
            if self
                .in_flight
                .values()
                .any(|entry| matches!(&*entry.request, DbRequest::GiveBack))
            {
                if let Some(id) = self.retry_due() {
                    self.retry_database(id, permit);
                }
                break;
            }
            if let Some(id) = self.retry_due() {
                if self.heartbeat_request().is_none() {
                    self.retry_database(id, permit);
                    continue;
                }
            }
            let request = self
                .heartbeat_request()
                .or_else(|| self.coordinate_request())
                .or_else(|| self.flush_request())
                .or_else(|| self.claim_request());
            let Some(request) = request else { break };
            self.start_database(request, permit);
        }
    }

    /// Freezes one weighted FIFO prefix without splitting groups or bypassing owned-lane coordination.
    fn flush_request(&mut self) -> Option<DbRequest> {
        if tokio::time::Instant::now() < self.flush_after || self.pending_commits.is_empty() {
            return None;
        }
        let budget = self
            .batch_size
            .max(self.registry.config.all_limit().saturating_add(1));
        let mut weight = 0usize;
        let count = self
            .pending_commits
            .iter()
            .take(self.batch_size)
            .take_while(|commit| {
                if commit
                    .lane()
                    .is_some_and(|lane| self.locked(lane) && self.lane_busy(lane))
                {
                    return false;
                }
                weight = weight.saturating_add(commit.weight());
                weight <= budget
            })
            .count();
        if count == 0 {
            return None;
        }
        let commits = self.pending_commits.drain(..count).collect();
        self.flush_after = tokio::time::Instant::now() + self.poll_interval;
        Some(DbRequest::Flush { commits })
    }

    /// Computes useful deadlines only for operations that can currently be admitted.
    fn deadline(&self) -> Option<tokio::time::Instant> {
        if self.stopped() || self.db_slots.available_permits() == 0 {
            return None;
        }
        if self
            .in_flight
            .values()
            .any(|entry| matches!(&*entry.request, DbRequest::GiveBack))
        {
            return self.retry_deadline();
        }
        let flush = self
            .pending_commits
            .front()
            .filter(|commit| {
                commit
                    .lane()
                    .is_none_or(|lane| !self.locked(lane) || !self.lane_busy(lane))
            })
            .map(|_| self.flush_after);
        let capacity = self.claim_capacity();
        let lane = self
            .lanes
            .iter()
            .filter(|lane| !self.lane_busy(lane.conf.lane()))
            .filter_map(|lane| {
                if lane.conf.lane_lock().is_some() && lane.owner_token.is_some() {
                    let quiescent = lane.tasks.is_empty()
                        && lane.uncommitted == 0
                        && lane.execution_slots.available_permits() == lane.conf.concurrency();
                    let poll = (quiescent || lane.hook_result.is_some()).then_some(lane.poll_after);
                    lane.owner_renew_at.into_iter().chain(poll).min()
                } else if self.mode == RunMode::Running
                    && lane.available(self.batch_size) > 0
                    && capacity > 0
                {
                    Some(lane.poll_after)
                } else {
                    None
                }
            })
            .min();
        flush
            .into_iter()
            .chain(lane)
            .chain(self.heartbeat_deadline())
            .chain(self.retry_deadline())
            .min()
    }

    fn queued(&self) -> usize {
        self.lanes.iter().map(|lane| lane.tasks.len()).sum()
    }

    /// Derives unresolved task reservations from the immutable claim requests.
    fn claim_capacity(&self) -> usize {
        let reserved: usize = self
            .in_flight
            .values()
            .filter_map(|request| match request.request.as_ref() {
                DbRequest::Claim { claims } => Some(
                    claims
                        .iter()
                        .filter(|c| c.owner.is_none())
                        .map(|c| c.limit)
                        .sum::<usize>(),
                ),
                _ => None,
            })
            .sum();
        self.execution_slots
            .available_permits()
            .saturating_sub(self.queued())
            .saturating_sub(reserved)
    }

    fn lane_busy(&self, lane: TaskLane) -> bool {
        self.in_flight
            .values()
            .any(|request| match request.request.as_ref() {
                DbRequest::Heartbeat | DbRequest::GiveBack => false,
                DbRequest::Claim { claims } | DbRequest::Coordinate { lanes: claims } => {
                    claims.iter().any(|c| c.lane == lane)
                }
                DbRequest::Flush { commits } => {
                    self.locked(lane) && commits.iter().any(|c| c.lane() == Some(lane))
                }
            })
    }

    fn locked(&self, lane: TaskLane) -> bool {
        self.lanes
            .iter()
            .find(|q| q.conf.lane() == lane)
            .is_some_and(|q| q.conf.lane_lock().is_some())
    }

    /// Preserves independent poll pacing when committed work or local submission wakes a lane.
    fn wake_lane(&mut self, lane: TaskLane) {
        let at = tokio::time::Instant::now() + self.poll_interval;
        if let Some(queue) = self.lane_mut(lane) {
            queue.poll_after = queue.poll_after.min(at);
        }
    }

    fn wake_lanes(&mut self) {
        let at = tokio::time::Instant::now() + self.poll_interval;
        for queue in &mut self.lanes {
            queue.poll_after = queue.poll_after.min(at);
        }
    }

    fn lane_mut(&mut self, lane: TaskLane) -> Option<&mut LaneQueue> {
        self.lanes
            .iter_mut()
            .find(|queue| queue.conf.lane() == lane)
    }

    /// Sets up coordination, reclaims standalone tasks, and registers this incarnation.
    /// Registration failure prevents startup; only clustered runners send heartbeats.
    pub fn initialize(
        &self,
    ) -> impl std::future::Future<Output = Result<(), TaskRuntimeError>> + Send + 'static {
        let store = self.store.clone();
        let health = self.health.clone();
        let node = self.node_id;
        let clustered = self.registry.config.mode_value() == super::TaskMode::Clustered;
        let slots = self.db_slots.clone();
        async move {
            let result = async {
                store.initialize().await?;
                if !clustered {
                    return_claims(&*store, None, slots.clone()).await?;
                }
                register_node(&*store, node, slots).await?;
                Ok(())
            }
            .await;
            match &result {
                Ok(()) => health.initialized(),
                Err(error) => {
                    health.initialization_failed();
                    super::diagnostics::log_runtime_error(
                        error,
                        "durable task runtime initialization failed",
                    );
                }
            }
            result
        }
    }

    /// Admits residual-claim return only after every operation and output has settled.
    fn give_back(&mut self) -> bool {
        if !self.finished() {
            return false;
        }
        let Ok(permit) = self.db_slots.clone().try_acquire_owned() else {
            return false;
        };
        self.start_database(DbRequest::GiveBack, permit);
        true
    }

    fn schedule_heartbeat(&mut self) {
        self.heartbeat_after =
            Some(tokio::time::Instant::now() + self.registry.config.heartbeat_interval_value());
    }

    /// Discards unacknowledged local work after confirmed loss of the durable node.
    /// Aborting futures cannot undo external effects or server-side database commits.
    fn retire(&mut self) {
        self.mode = RunMode::Retired;
        self.discard_work();
    }

    /// Clears local evidence only on terminal exit, never as a persistence acknowledgement.
    fn discard_work(&mut self) {
        self.death_at = None;
        self.health.retired();
        self.metrics.store_failure();
        tracing::error!(node_id = %self.node_id, buffered = self.pending_commits.len(),
            unresolved = self.in_flight.len(), "task runner exiting with unacknowledged work");
        for (_, invocation) in self.running_invocations.drain() {
            invocation.abort.abort();
        }
        self.running_tasks.clear();
        self.abort_hooks();
        self.operations.clear();
        self.in_flight.clear();
        self.pending_commits.clear();
        self.heartbeat_after = None;
        for lane in &mut self.lanes {
            lane.tasks.clear();
            lane.uncommitted = 0;
            lane.hook_result = None;
        }
    }

    /// Derives admission from the existing requests; standalone avoids scanning them.
    fn heartbeat_deadline(&self) -> Option<tokio::time::Instant> {
        let deadline = self.heartbeat_after?;
        if self
            .in_flight
            .values()
            .any(|request| matches!(request.request.as_ref(), DbRequest::Heartbeat))
        {
            None
        } else {
            Some(deadline)
        }
    }

    fn heartbeat_request(&self) -> Option<DbRequest> {
        self.heartbeat_deadline()
            .filter(|at| *at <= tokio::time::Instant::now())
            .map(|_| DbRequest::Heartbeat)
    }
}

/// Runs lifecycle-only claim reset under the DB budget and contains uncertain panics.
async fn return_claims<S: AbstractTaskStore>(
    store: &S,
    node: Option<uuid::Uuid>,
    slots: Arc<Semaphore>,
) -> Result<u64, TaskRuntimeError> {
    let permit = slots.try_acquire_owned().map_err(|_| {
        TaskRuntimeError::TaskExecutionError("claim return requires an idle database slot".into())
    })?;
    let result = std::panic::AssertUnwindSafe(store.return_claims(node))
        .catch_unwind()
        .await
        .map_err(|_| {
            TaskRuntimeError::TaskExecutionError(
                "task claim return panicked; outcome unknown".into(),
            )
        })?;
    drop(permit);
    result
}

/// Registers one incarnation using the existing budget and contains store panics.
async fn register_node<S: AbstractTaskStore>(
    store: &S,
    node: uuid::Uuid,
    slots: Arc<Semaphore>,
) -> Result<(), TaskRuntimeError> {
    let permit = slots.try_acquire_owned().map_err(|_| {
        TaskRuntimeError::TaskExecutionError(
            "node registration requires an idle database slot".into(),
        )
    })?;
    let result = std::panic::AssertUnwindSafe(store.register_node(&node))
        .catch_unwind()
        .await
        .map_err(|_| {
            tracing::error!(node_id = %node, "task node registration panicked");
            TaskRuntimeError::TaskExecutionError("task node registration panicked".into())
        })?;
    match &result {
        Ok(()) => tracing::info!(node_id = %node, "task node registered"),
        Err(_) => tracing::error!(node_id = %node, "task node registration failed"),
    }
    drop(permit);
    result
}

#[path = "runner_database.rs"]
mod database;
use database::InFlight;

#[path = "runner_dispatch.rs"]
mod dispatch;
#[path = "runner_lanes.rs"]
mod lanes;
use lanes::*;
#[cfg(test)]
#[path = "tests/runner.rs"]
mod tests;

#[cfg(all(
    test,
    not(any(feature = "postgres", feature = "mysql", feature = "sqlite"))
))]
#[path = "tests/runner_heartbeat.rs"]
mod heartbeat_tests;
