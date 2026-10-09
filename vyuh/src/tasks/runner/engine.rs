//! Non-blocking task coordination; stores exclusively own durable task mutations.

use super::database::InFlight;
use crate::Site;
use crate::tasks::client::RunnerEvent;
use crate::tasks::{
    CommitResult, LaneClaim, TaskClient, TaskCommit, TaskLane, TaskLaneConf, TaskPoll, TaskRecord,
    TaskRegistry, TaskRuntimeError, TaskStore,
};
use futures::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

/// Bounds local drain/dispatch passes independently of database and ingress tuning.
pub(super) const SCHEDULER_BUDGET: usize = 250;
pub(super) struct RunningInvocation {
    pub(super) lane: TaskLane,
    pub(super) task_ids: Vec<crate::tasks::TaskId>,
    pub(super) abort: tokio::task::AbortHandle,
}

pub(super) struct RunningLaneHook {
    pub(super) token: String,
    pub(super) generation: i64,
    pub(super) action: crate::tasks::LaneHookAction,
    pub(super) abort: tokio::task::AbortHandle,
    pub(super) started: std::time::Instant,
}

pub(super) struct LaneState {
    pub(super) conf: TaskLaneConf,
    pub(super) local_rate: Option<crate::tasks::rate::LocalRateBucket>,
    pub(super) tasks: VecDeque<Arc<TaskRecord>>,
    pub(super) execution_slots: Arc<Semaphore>,
    pub(super) uncommitted: usize,
    pub(super) poll_after: tokio::time::Instant,
    pub(super) owner_token: Option<String>,
    pub(super) owner_renew_at: Option<tokio::time::Instant>,
    pub(super) owner_generation: i64,
    pub(super) owner_phase: crate::tasks::LaneOwnerPhase,
    pub(super) completed_work: bool,
    pub(super) hook_result: Option<crate::tasks::LaneHookResult>,
    pub(super) flow_cache: hashlink::LinkedHashMap<crate::tasks::TaskId, Arc<TaskRecord>>,
    pub(super) cached_bytes: usize,
}

impl LaneState {
    /// Constructs isolated lane admission and execution state from finalized configuration.
    pub(super) fn new(conf: TaskLaneConf) -> Self {
        Self {
            local_rate: conf.rate().map(|rate| {
                crate::tasks::rate::LocalRateBucket::new(rate, tokio::time::Instant::now())
            }),
            execution_slots: Arc::new(Semaphore::new(conf.concurrency())),
            conf,
            tasks: VecDeque::new(),
            uncommitted: 0,
            poll_after: tokio::time::Instant::now(),
            owner_token: None,
            owner_renew_at: None,
            owner_generation: 0,
            owner_phase: crate::tasks::LaneOwnerPhase::Active,
            completed_work: false,
            hook_result: None,
            flow_cache: hashlink::LinkedHashMap::new(),
            cached_bytes: 0,
        }
    }

    /// Returns whether this lane has crossed its queued-work refill watermark.
    pub(super) fn needs_refill(&self) -> bool {
        self.tasks.len().saturating_mul(2) < self.conf.concurrency()
    }

    /// Bounds refills by executing, queued, and completed-but-uncommitted work.
    pub(super) fn available(&self) -> usize {
        if self.needs_refill() {
            self.conf
                .concurrency()
                .saturating_sub(
                    (self.conf.concurrency() - self.execution_slots.available_permits())
                        + self.tasks.len(),
                )
                .saturating_sub(self.uncommitted)
                .min(self.conf.claim_batch_size_value())
        } else {
            0
        }
    }

    pub(super) fn claim_limit(&mut self, limit: usize, now: tokio::time::Instant) -> usize {
        self.local_rate
            .as_mut()
            .map_or(limit, |rate| limit.min(rate.available(now)))
    }

    pub(super) fn consume_local_rate(&mut self, permits: usize, now: tokio::time::Instant) {
        if let Some(rate) = &mut self.local_rate {
            rate.consume(permits, now);
        }
    }

    pub(super) fn local_rate_wake(
        &mut self,
        now: tokio::time::Instant,
    ) -> Option<std::time::Duration> {
        self.local_rate
            .as_mut()
            .and_then(|rate| rate.next_permit(now))
    }
}

pub(super) struct Completion {
    pub(super) invocation_id: uuid::Uuid,
    pub(super) lane: TaskLane,
    pub(super) commits: Vec<TaskCommit>,
    pub(super) global_permit: Option<OwnedSemaphorePermit>,
    pub(super) lane_permit: Option<OwnedSemaphorePermit>,
}

pub(super) struct HookCompletion {
    pub(super) lane: TaskLane,
    pub(super) token: String,
    pub(super) result: crate::tasks::LaneHookResult,
}

pub(super) type HookStart = Option<(String, i64, crate::tasks::LaneHookAction)>;

#[derive(Default)]
pub(super) struct PollEffects {
    pub(super) hooks: Vec<(TaskLane, HookStart)>,
    pub(super) acquired: Vec<TaskLane>,
    pub(super) takeovers: Vec<TaskLane>,
    pub(super) lost: Vec<TaskLane>,
    pub(super) transitions: Vec<(TaskLane, crate::tasks::LaneOwnerPhase)>,
}

pub(super) struct TaskExecution {
    pub(super) node_id: uuid::Uuid,
    pub(super) invocation_id: uuid::Uuid,
    pub(super) engine: Arc<TaskRegistry>,
    pub(super) site: Site,
    pub(super) records: Vec<Arc<TaskRecord>>,
    pub(super) sender: mpsc::Sender<Completion>,
    pub(super) lane: TaskLane,
    pub(super) metrics: Arc<crate::tasks::TaskMetrics>,
    pub(super) payload_limit: usize,
    pub(super) error_limit: usize,
    pub(super) owner_token: Option<String>,
    pub(super) global_permit: Option<OwnedSemaphorePermit>,
    pub(super) lane_permit: Option<OwnedSemaphorePermit>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum RunMode {
    Running,
    Draining,
    Faulted,
    Retired,
    Dead,
    Finished,
}

pub(super) enum DbRequest {
    Heartbeat,
    GiveBack,
    Claim {
        claims: Vec<LaneClaim>,
    },
    Commit {
        commits: Vec<TaskCommit>,
        capacity: usize,
        cached: Vec<Arc<TaskRecord>>,
    },
    Coordinate {
        lanes: Vec<LaneClaim>,
    },
}

pub(super) enum DbResponse {
    Heartbeat { present: bool },
    Returned,
    Poll(TaskPoll),
    Flushed(CommitResult),
}

pub(super) struct DbCompletion {
    pub(super) id: uuid::Uuid,
    pub(super) result: Result<DbResponse, crate::tasks::store::StoreFailure<DbResponse>>,
    pub(super) permit: OwnedSemaphorePermit,
    pub(super) started: std::time::Instant,
}

/// Coordinates lane-local inputs, spawned execution, and one shared output FIFO.
/// Recovery is poll-paced; cancellation only skips locally queued work, never interrupts handlers.
pub struct TaskRunner<S: TaskStore + Send + Sync + 'static> {
    pub(super) lanes: VecDeque<LaneState>,
    pub(super) concurrency: usize,
    pub(super) commit_batch_size: usize,
    pub(super) lease_duration: tokio::time::Duration,
    pub(super) execution_slots: Arc<Semaphore>,
    pub(super) db_slots: Arc<Semaphore>,
    pub(super) in_flight: HashMap<uuid::Uuid, InFlight>,
    pub(super) operations: FuturesUnordered<BoxFuture<'static, DbCompletion>>,
    pub(super) running_tasks: HashMap<crate::tasks::TaskId, uuid::Uuid>,
    pub(super) running_invocations: HashMap<uuid::Uuid, RunningInvocation>,
    pub(super) output_buffer: VecDeque<TaskCommit>,
    pub(super) running_hooks: HashMap<TaskLane, RunningLaneHook>,
    pub(super) poll_interval: tokio::time::Duration,
    pub(super) fallback_interval: tokio::time::Duration,
    pub(super) flush_after: tokio::time::Instant,
    pub(super) heartbeat_after: Option<tokio::time::Instant>,
    pub(super) death_at: Option<tokio::time::Instant>,
    pub(super) mode: RunMode,
    pub(super) node_id: uuid::Uuid,
    pub(super) events: tokio::sync::broadcast::Receiver<RunnerEvent>,
    #[cfg(feature = "postgres")]
    pub(super) events_tx: tokio::sync::broadcast::Sender<RunnerEvent>,
    #[cfg(feature = "postgres")]
    pub(super) notification_listener: Option<tokio::task::JoinHandle<()>>,
    pub(super) ingress_tx: mpsc::Sender<Arc<TaskRecord>>,
    pub(super) ingress_rx: mpsc::Receiver<Arc<TaskRecord>>,
    pub(super) registry: Arc<TaskRegistry>,
    pub(super) store: Arc<S>,
    pub(super) metrics: Arc<crate::tasks::TaskMetrics>,
    pub(super) health: crate::tasks::TaskHealth,
}
impl<S: TaskStore + Send + Sync + 'static> Drop for TaskRunner<S> {
    fn drop(&mut self) {
        #[cfg(feature = "postgres")]
        if let Some(listener) = &self.notification_listener {
            listener.abort();
        }
    }
}

impl<S: TaskStore + Send + Sync + 'static> std::fmt::Debug for TaskRunner<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskRunner")
            .field("node_id", &self.node_id)
            .field("lanes", &self.lanes.len())
            .finish()
    }
}
impl<S: TaskStore + Send + Sync + 'static> TaskRunner<S> {
    /// Creates independent handler and database concurrency budgets for one site.
    pub fn new(client: TaskClient<S>) -> Result<Self, TaskRuntimeError> {
        let config = &client.registry.config;
        let (ingress_tx, ingress_rx) = mpsc::channel(config.ingress_capacity_value());
        let lanes = client
            .registry
            .lanes()
            .iter()
            .cloned()
            .map(LaneState::new)
            .collect();
        Ok(Self {
            lanes,
            concurrency: config.concurrency_value(),
            commit_batch_size: config.commit_batch_size_value(),
            lease_duration: config.lease_duration_value(),
            execution_slots: Arc::new(Semaphore::new(config.concurrency_value())),
            db_slots: Arc::new(Semaphore::new(config.db_concurrency_value())),
            in_flight: HashMap::new(),
            operations: FuturesUnordered::new(),
            running_tasks: HashMap::new(),
            running_invocations: HashMap::new(),
            output_buffer: VecDeque::new(),
            running_hooks: HashMap::new(),
            poll_interval: config.poll_interval_value(),
            fallback_interval: config.fallback_interval(),
            flush_after: tokio::time::Instant::now(),
            heartbeat_after: None,
            death_at: None,
            mode: RunMode::Running,
            node_id: uuid::Uuid::now_v7(),
            events: client.events.subscribe(),
            #[cfg(feature = "postgres")]
            events_tx: client.events.clone(),
            #[cfg(feature = "postgres")]
            notification_listener: None,
            ingress_tx,
            ingress_rx,
            registry: client.registry.clone(),
            store: client.store.clone(),
            metrics: client.metrics.clone(),
            health: client.health.clone(),
        })
    }

    /// Publishes only after node registration; the existing site start guard owns availability.
    pub(crate) fn ingress(&self) -> (uuid::Uuid, mpsc::Sender<Arc<TaskRecord>>) {
        (self.node_id, self.ingress_tx.clone())
    }

    /// Runs local execution and collection while admitted database futures are pending.
    pub async fn run(mut self, site: Site) {
        if self.registry.config.mode_value() == crate::tasks::TaskMode::Clustered {
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
    pub(super) async fn wait_event(
        &mut self,
        site: &Site,
        hooks: &mpsc::Sender<HookCompletion>,
        rx: &mut mpsc::Receiver<Completion>,
        hook_rx: &mut mpsc::Receiver<HookCompletion>,
        shutdown: &crate::notifiers::CancellationNotifier,
    ) {
        let deadline = self.deadline();
        tokio::select! {
            record = self.ingress_rx.recv(), if self.mode == RunMode::Running && self.input_room() > 0 => {
                if let Some(record) = record { self.accept_input(record); }
            },
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
                self.ingress_rx.close();
                self.abort_hooks();
            },
            _ = async { if let Some(at) = deadline { tokio::time::sleep_until(at).await; }
                else { std::future::pending::<()>().await; } } => {},
        }
    }

    pub(super) fn advance_pipeline(&mut self, site: &Site, tx: &mpsc::Sender<Completion>) {
        if matches!(self.mode, RunMode::Running | RunMode::Draining) {
            self.dispatch_ready(site, tx);
            if !self.finished() {
                self.admit();
            }
        }
    }

    /// Drains a bounded number of events before revisiting dispatch and database admission.
    pub(super) fn collect_ready(
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

    pub(super) fn finished(&self) -> bool {
        self.mode == RunMode::Draining
            && self.running_invocations.is_empty()
            && self.output_buffer.is_empty()
            && self.in_flight.is_empty()
    }

    /// Admits eligible work without blocking on permits or creating a permit-waiter queue.
    pub(super) fn admit(&mut self) {
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
    pub(super) fn flush_request(&mut self) -> Option<DbRequest> {
        if tokio::time::Instant::now() < self.flush_after || self.output_buffer.is_empty() {
            return None;
        }
        let budget = self
            .commit_batch_size
            .max(self.registry.config.all_limit().saturating_add(1));
        let mut weight = 0usize;
        let count = self
            .output_buffer
            .iter()
            .take(self.commit_batch_size)
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
        let commits: Vec<_> = self.output_buffer.drain(..count).collect();
        self.flush_after = tokio::time::Instant::now() + self.poll_interval;
        Some(self.delivery_request(commits))
    }

    /// Retains bounded immutable bases and reserves room only for this flush's possible inputs.
    pub(super) fn delivery_request(&self, commits: Vec<TaskCommit>) -> DbRequest {
        let cached: Vec<_> = self
            .lanes
            .iter()
            .flat_map(|lane| lane.flow_cache.values().cloned())
            .collect();
        let generated = commits
            .iter()
            .map(|commit| match commit.outcome() {
                Some(crate::tasks::TaskOutcome::Spawn { .. }) => 1,
                Some(crate::tasks::TaskOutcome::All { children, .. }) => children.len(),
                _ => 0,
            })
            .sum::<usize>()
            .saturating_add(cached.len().min(commits.len()));
        let capacity = if self.mode == RunMode::Running {
            self.input_room()
                .min(self.ingress_tx.capacity())
                .min(generated)
        } else {
            0
        };
        DbRequest::Commit {
            commits,
            capacity,
            cached,
        }
    }

    /// Computes useful deadlines only for operations that can currently be admitted.
    pub(super) fn deadline(&self) -> Option<tokio::time::Instant> {
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
            .output_buffer
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
                        && lane.execution_slots.available_permits() == lane.conf.concurrency()
                        && !self.lane_running(lane.conf.lane());
                    let poll = (quiescent || lane.hook_result.is_some()).then_some(lane.poll_after);
                    lane.owner_renew_at.into_iter().chain(poll).min()
                } else if self.mode == RunMode::Running
                    && ((self.has_flows(lane.conf.lane()) && self.input_room() > 0)
                        || (lane.available() > 0 && capacity > 0))
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

    pub(super) fn queued(&self) -> usize {
        self.lanes
            .iter()
            .flat_map(|lane| &lane.tasks)
            .filter(|task| task.kind == crate::tasks::TaskKind::Work)
            .count()
    }

    /// Derives unresolved task reservations from the immutable claim requests.
    pub(super) fn claim_capacity(&self) -> usize {
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

    pub(super) fn lane_busy(&self, lane: TaskLane) -> bool {
        self.in_flight
            .values()
            .any(|request| match request.request.as_ref() {
                DbRequest::Heartbeat | DbRequest::GiveBack => false,
                DbRequest::Claim { claims } | DbRequest::Coordinate { lanes: claims } => {
                    claims.iter().any(|c| c.lane == lane)
                }
                DbRequest::Commit { commits, .. } => {
                    self.locked(lane) && commits.iter().any(|c| c.lane() == Some(lane))
                }
            })
    }

    pub(super) fn locked(&self, lane: TaskLane) -> bool {
        self.lanes
            .iter()
            .find(|q| q.conf.lane() == lane)
            .is_some_and(|q| q.conf.lane_lock().is_some())
    }

    /// Preserves independent poll pacing when committed work or local submission wakes a lane.
    pub(super) fn wake_lane(&mut self, lane: TaskLane) {
        let at = tokio::time::Instant::now() + self.poll_interval;
        if let Some(queue) = self.lane_mut(lane) {
            queue.poll_after = queue.poll_after.min(at);
        }
    }

    pub(super) fn wake_lanes(&mut self) {
        let at = tokio::time::Instant::now() + self.poll_interval;
        for queue in &mut self.lanes {
            queue.poll_after = queue.poll_after.min(at);
        }
    }

    pub(super) fn lane_mut(&mut self, lane: TaskLane) -> Option<&mut LaneState> {
        self.lanes
            .iter_mut()
            .find(|queue| queue.conf.lane() == lane)
    }

    /// Sets up coordination, reclaims standalone tasks, and registers this incarnation.
    /// Registration failure prevents startup; only clustered runners send heartbeats.
    pub async fn initialize(&mut self) -> Result<(), TaskRuntimeError> {
        let store = self.store.clone();
        let health = self.health.clone();
        let node = self.node_id;
        let clustered = self.registry.config.mode_value() == crate::tasks::TaskMode::Clustered;
        let slots = self.db_slots.clone();
        let result = async {
            store.initialize().await?;
            if !clustered {
                return_claims(&*store, None, slots.clone()).await?;
            }
            register_node(&*store, node, slots).await?;
            #[cfg(feature = "postgres")]
            {
                self.notification_listener = store
                    .start_notifications(
                        node,
                        self.events_tx.clone(),
                        crate::notifiers::CancellationNotifier::new(),
                    )
                    .await?;
            }
            Ok(())
        }
        .await;
        match &result {
            Ok(()) => health.initialized(),
            Err(error) => {
                health.initialization_failed();
                crate::tasks::diagnostics::log_runtime_error(
                    error,
                    "durable task runtime initialization failed",
                );
            }
        }
        result
    }

    /// Admits residual-claim return only after every operation and output has settled.
    pub(super) fn give_back(&mut self) -> bool {
        if !self.finished() {
            return false;
        }
        self.ingress_rx.close();
        let Ok(permit) = self.db_slots.clone().try_acquire_owned() else {
            return false;
        };
        self.start_database(DbRequest::GiveBack, permit);
        true
    }

    pub(super) fn schedule_heartbeat(&mut self) {
        self.heartbeat_after =
            Some(tokio::time::Instant::now() + self.registry.config.heartbeat_interval_value());
    }

    /// Discards unacknowledged local work after confirmed loss of the durable node.
    /// Aborting futures cannot undo external effects or server-side database commits.
    pub(super) fn retire(&mut self) {
        self.mode = RunMode::Retired;
        self.discard_work();
    }

    /// Clears local evidence only on terminal exit, never as a persistence acknowledgement.
    pub(super) fn discard_work(&mut self) {
        self.ingress_rx.close();
        while self.ingress_rx.try_recv().is_ok() {}
        self.death_at = None;
        self.health.retired();
        self.metrics.store_failure();
        tracing::error!(node_id = %self.node_id, buffered = self.output_buffer.len(),
            unresolved = self.in_flight.len(), "task runner exiting with unacknowledged work");
        for (_, invocation) in self.running_invocations.drain() {
            invocation.abort.abort();
        }
        self.running_tasks.clear();
        self.abort_hooks();
        self.operations.clear();
        self.in_flight.clear();
        self.output_buffer.clear();
        self.heartbeat_after = None;
        for lane in &mut self.lanes {
            lane.flow_cache.clear();
            lane.cached_bytes = 0;
            lane.tasks.clear();
            lane.uncommitted = 0;
            lane.hook_result = None;
        }
    }

    /// Derives admission from the existing requests; standalone avoids scanning them.
    pub(super) fn heartbeat_deadline(&self) -> Option<tokio::time::Instant> {
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

    pub(super) fn heartbeat_request(&self) -> Option<DbRequest> {
        self.heartbeat_deadline()
            .filter(|at| *at <= tokio::time::Instant::now())
            .map(|_| DbRequest::Heartbeat)
    }
}

/// Runs lifecycle-only claim reset under the DB budget and contains uncertain panics.
async fn return_claims<S: TaskStore>(
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
async fn register_node<S: TaskStore>(
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

#[cfg(test)]
#[path = "tests/engine.rs"]
mod tests;

#[cfg(all(
    test,
    not(any(feature = "postgres", feature = "mysql", feature = "sqlite"))
))]
#[path = "tests/runner_heartbeat.rs"]
mod heartbeat_tests;
