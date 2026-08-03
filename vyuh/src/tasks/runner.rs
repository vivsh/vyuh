//! Fair grouped task runner and adaptive polling loop.

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use futures::FutureExt as _;
use tokio::sync::mpsc;

use crate::Site;

use super::{
    AbstractTaskStore, GroupClaim, TaskCommit, TaskDispatcher, TaskError, TaskGroup, TaskGroupConf,
    TaskPoll, TaskRecord, TaskRegistry,
};

struct RunningTask {
    group: TaskGroup,
    abort: tokio::task::AbortHandle,
}

struct GroupQueue {
    conf: TaskGroupConf,
    local_rate: Option<super::rate::LocalRateBucket>,
    tasks: VecDeque<Arc<TaskRecord>>,
    running: usize,
    poll_after: tokio::time::Instant,
}

impl GroupQueue {
    fn available(&self, batch_size: usize) -> usize {
        self.conf
            .concurrency()
            .saturating_sub(self.running + self.tasks.len())
            .min(batch_size)
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
    group: TaskGroup,
    commit: TaskCommit,
}

struct TaskExecution {
    engine: Arc<TaskRegistry>,
    site: Site,
    record: Arc<TaskRecord>,
    sender: mpsc::Sender<Completion>,
    group: TaskGroup,
    metrics: Arc<super::TaskMetrics>,
    payload_limit: usize,
    error_limit: usize,
}

struct RunState {
    next_poll: tokio::time::Instant,
    poll_error: tokio::time::Duration,
    commits: Vec<TaskCommit>,
    flush_at: Option<tokio::time::Instant>,
    commit_error: tokio::time::Duration,
    next_renew: tokio::time::Instant,
    shutting_down: bool,
}

impl RunState {
    fn new(batch_size: usize, poll: tokio::time::Duration, renew: tokio::time::Duration) -> Self {
        let now = tokio::time::Instant::now();
        Self {
            next_poll: now,
            poll_error: poll,
            commits: Vec::with_capacity(batch_size),
            flush_at: None,
            commit_error: poll,
            next_renew: now + renew,
            shutting_down: false,
        }
    }
}

const OUTCOME_FLUSH_DELAY: tokio::time::Duration = tokio::time::Duration::from_millis(10);

/// Executes durable tasks through one fair per-site group scheduler.
pub struct AbstractTaskRunner<S: AbstractTaskStore + Send + Sync + 'static> {
    groups: Vec<GroupQueue>,
    cursor: usize,
    concurrency: usize,
    batch_size: usize,
    running: usize,
    running_tasks: HashMap<super::TaskId, RunningTask>,
    poll_interval: tokio::time::Duration,
    fallback_interval: tokio::time::Duration,
    renew_interval: tokio::time::Duration,
    runner_id: String,
    notifier: Arc<tokio::sync::Notify>,
    registry: Arc<TaskRegistry>,
    initialized: Arc<tokio::sync::OnceCell<()>>,
    store: Arc<S>,
    metrics: Arc<super::TaskMetrics>,
}

impl<S: AbstractTaskStore + Send + Sync + 'static> std::fmt::Debug for AbstractTaskRunner<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TaskRunner")
            .field("running", &self.running)
            .field("groups", &self.groups.len())
            .finish()
    }
}

impl<S: AbstractTaskStore + Send + Sync + 'static> AbstractTaskRunner<S> {
    /// Creates a runner from one validated task dispatcher.
    pub fn new(dispatcher: TaskDispatcher<S>) -> Result<Self, TaskError> {
        let config = &dispatcher.registry.config;
        let groups = config
            .validate()?
            .into_iter()
            .map(|conf| {
                let now = tokio::time::Instant::now();
                GroupQueue {
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
            groups,
            cursor: 0,
            concurrency: config.concurrency_value(),
            batch_size: config.batch_size_value(),
            running: 0,
            running_tasks: HashMap::new(),
            poll_interval: config.poll_interval_value(),
            fallback_interval: config.fallback_interval(),
            renew_interval: renewal_interval(config.lease_duration_value()),
            runner_id: uuid::Uuid::now_v7().to_string(),
            notifier: dispatcher.notifier.clone(),
            registry: dispatcher.registry.clone(),
            initialized: dispatcher.initialized.clone(),
            store: dispatcher.store.clone(),
            metrics: dispatcher.metrics.clone(),
        })
    }

    /// Runs until site shutdown while preserving bounded polling and commits.
    pub async fn run(mut self, site: Site) {
        let shutdown = site.shutdown_notifier();
        let (completion_tx, mut completion_rx) = mpsc::channel(self.concurrency);
        let mut state = RunState::new(self.batch_size, self.poll_interval, self.renew_interval);
        loop {
            self.prepare(&site, &completion_tx, &mut completion_rx, &mut state)
                .await;
            if self.finished(&state) {
                break;
            }
            let flush_deadline = state.flush_at.unwrap_or(state.next_poll);
            let poll_enabled = !state.shutting_down && self.can_poll();
            let renew_enabled = !self.running_tasks.is_empty();
            tokio::select! {
                _ = shutdown.notified(), if !state.shutting_down => state.shutting_down = true,
                _ = self.notifier.notified(), if !state.shutting_down => self.wake(&mut state),
                completion = completion_rx.recv() => {
                    if let Some(completion) = completion {
                        self.queue_completion(completion, &mut state.commits,
                            &mut state.flush_at, &mut state.next_poll);
                    }
                },
                _ = tokio::time::sleep_until(flush_deadline), if state.flush_at.is_some() => {},
                _ = tokio::time::sleep_until(state.next_poll), if poll_enabled => {},
                _ = tokio::time::sleep_until(state.next_renew), if renew_enabled => {},
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
        self.renew_due(&mut state.next_renew).await;
        if state.shutting_down && !state.commits.is_empty() {
            state.flush_at = Some(tokio::time::Instant::now());
        }
        if self
            .flush_due(
                &mut state.commits,
                &mut state.flush_at,
                &mut state.commit_error,
            )
            .await
        {
            state.next_poll = tokio::time::Instant::now();
        }
        self.dispatch_ready(site, sender);
        if !state.shutting_down {
            self.poll_due(site, sender, &mut state.next_poll, &mut state.poll_error)
                .await;
        }
    }

    /// Polls one due store turn and advances bounded retry state on failure.
    async fn poll_due(
        &mut self,
        site: &Site,
        sender: &mpsc::Sender<Completion>,
        next_poll: &mut tokio::time::Instant,
        error_delay: &mut tokio::time::Duration,
    ) {
        if tokio::time::Instant::now() < *next_poll || !self.can_poll() {
            return;
        }
        match self.poll_store().await {
            Ok(Some(poll)) => {
                *next_poll = self.apply_poll(poll);
                *error_delay = self.poll_interval;
                self.dispatch_ready(site, sender);
            }
            Ok(None) => {
                *next_poll =
                    self.next_group_deadline(tokio::time::Instant::now() + self.fallback_interval);
                *error_delay = self.poll_interval;
            }
            Err(error) => {
                self.metrics.store_failure();
                tracing::error!(%error, "failed to poll durable tasks");
                *next_poll = tokio::time::Instant::now() + *error_delay;
                *error_delay = (*error_delay * 2).min(self.fallback_interval);
            }
        }
    }

    /// Flushes a due outcome batch and advances its bounded failure backoff.
    async fn flush_due(
        &mut self,
        commits: &mut Vec<TaskCommit>,
        deadline: &mut Option<tokio::time::Instant>,
        error_delay: &mut tokio::time::Duration,
    ) -> bool {
        if !should_flush(commits, self.batch_size, *deadline) {
            return false;
        }
        let committed = self.flush_commits(commits).await;
        if committed {
            *deadline = None;
            *error_delay = self.poll_interval;
        } else {
            *deadline = Some(tokio::time::Instant::now() + *error_delay);
            *error_delay = (*error_delay * 2).min(self.fallback_interval);
        }
        committed
    }

    /// Validates persistent group, rate, and orphan state before workers start.
    pub async fn initialize(&self) -> Result<(), TaskError> {
        let conf = self.store_conf()?;
        self.initialized
            .get_or_try_init(|| self.store.initialize(conf))
            .await
            .map(|_| ())
    }

    fn finished(&self, state: &RunState) -> bool {
        state.shutting_down
            && self.running_tasks.is_empty()
            && self.queued() == 0
            && state.commits.is_empty()
    }

    fn wake(&mut self, state: &mut RunState) {
        self.wake_groups();
        state.next_poll = tokio::time::Instant::now();
    }

    async fn renew_due(&mut self, deadline: &mut tokio::time::Instant) {
        if self.running_tasks.is_empty() || tokio::time::Instant::now() < *deadline {
            return;
        }
        let ids = self.running_tasks.keys().copied().collect::<Vec<_>>();
        match self.store.renew_leases(&self.runner_id, &ids).await {
            Ok(lost) => self.record_renewals(&ids, &lost),
            Err(error) => {
                self.metrics.store_failure();
                tracing::error!(%error, "failed to renew task leases");
            }
        }
        *deadline = tokio::time::Instant::now() + self.renew_interval;
    }

    fn record_renewals(&mut self, ids: &[super::TaskId], lost: &[super::TaskId]) {
        for id in ids {
            if let Some(task) = self.running_tasks.get(id) {
                self.metrics.renewed(task.group.as_str(), lost.contains(id));
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
            if let Some(group) = self.group_mut(task.group) {
                group.running = group.running.saturating_sub(1);
            }
            tracing::warn!(task_id = %id, "task lease ownership was lost");
        }
    }

    fn can_poll(&self) -> bool {
        self.running < self.concurrency
            && self
                .groups
                .iter()
                .any(|group| group.available(self.batch_size) > 0)
    }

    fn queued(&self) -> usize {
        self.groups.iter().map(|group| group.tasks.len()).sum()
    }

    async fn poll_store(&mut self) -> Result<Option<TaskPoll>, TaskError> {
        let claims = self.claims();
        if claims.is_empty() {
            return Ok(None);
        }
        let poll = self.store.claim_tasks(&self.runner_id, &claims).await?;
        validate_poll(&claims, &poll)?;
        Ok(Some(poll))
    }

    /// Allocates one global claim budget fairly from the current group cursor.
    fn claims(&mut self) -> Vec<GroupClaim> {
        let mut claims = Vec::with_capacity(self.groups.len());
        let now = tokio::time::Instant::now();
        let mut remaining = self
            .concurrency
            .saturating_sub(self.running + self.queued())
            .min(self.batch_size);
        for offset in 0..self.groups.len() {
            if remaining == 0 {
                break;
            }
            let Some(index) = self.rotated_index(offset) else {
                continue;
            };
            let Some(group) = self.groups.get_mut(index) else {
                continue;
            };
            if group.poll_after > now {
                continue;
            }
            let capacity = group.available(self.batch_size).min(remaining);
            let limit = group.claim_limit(capacity, now);
            if limit > 0 {
                claims.push(GroupClaim {
                    group: group.conf.group(),
                    limit,
                });
                remaining -= limit;
            } else if capacity > 0
                && let Some(wait) = group.local_rate_wake(now)
            {
                group.poll_after = now + wait;
            }
        }
        claims
    }

    fn rotated_index(&self, offset: usize) -> Option<usize> {
        if self.groups.is_empty() {
            None
        } else {
            Some((self.cursor + offset) % self.groups.len())
        }
    }

    /// Enqueues claimed rows and derives the earliest useful monotonic wake.
    fn apply_poll(&mut self, poll: TaskPoll) -> tokio::time::Instant {
        let now = tokio::time::Instant::now();
        let fallback = now + self.fallback_interval;
        let batch_size = self.batch_size;
        let poll_interval = self.poll_interval;
        let fallback_interval = self.fallback_interval;
        let mut saw_group = false;
        for result in poll.groups {
            self.metrics
                .claimed(result.group.as_str(), result.tasks.len(), result.reclaimed);
            let Some(group) = self.group_mut(result.group) else {
                continue;
            };
            saw_group = true;
            group.consume_local_rate(result.tasks.len(), now);
            group.tasks.extend(result.tasks.into_iter().map(Arc::new));
            let store_deadline = group_deadline(
                now,
                result.saturated,
                group.conf.global_rate(),
                result.next_wake_in,
                poll_interval,
                fallback_interval,
            );
            group.poll_after = group
                .local_rate_wake(now)
                .map_or(store_deadline, |wake| store_deadline.max(now + wake));
        }
        if !saw_group {
            self.groups
                .iter_mut()
                .filter(|group| group.available(batch_size) > 0)
                .for_each(|group| group.poll_after = fallback);
        }
        self.rotate();
        self.next_group_deadline(fallback)
    }

    fn rotate(&mut self) {
        if !self.groups.is_empty() {
            self.cursor = (self.cursor + 1) % self.groups.len();
        }
    }

    /// Returns the earliest deadline belonging to a group with local claim capacity.
    fn next_group_deadline(&self, fallback: tokio::time::Instant) -> tokio::time::Instant {
        self.groups
            .iter()
            .filter(|group| group.available(self.batch_size) > 0)
            .map(|group| group.poll_after)
            .min()
            .unwrap_or(fallback)
    }

    fn dispatch_ready(&mut self, site: &Site, sender: &mpsc::Sender<Completion>) {
        while self.running < self.concurrency {
            let Some((group, record)) = self.pop_ready() else {
                break;
            };
            self.running += 1;
            self.spawn_task(site.clone(), sender.clone(), group, record);
        }
    }

    /// Pops one runnable row while respecting group quotas and fair rotation.
    fn pop_ready(&mut self) -> Option<(TaskGroup, Arc<TaskRecord>)> {
        let group_count = self.groups.len();
        for offset in 0..self.groups.len() {
            let index = self.rotated_index(offset)?;
            let queue = self.groups.get_mut(index)?;
            if queue.running >= queue.conf.concurrency() {
                continue;
            }
            if let Some(record) = queue.tasks.pop_front() {
                queue.running += 1;
                self.cursor = (index + 1) % group_count;
                return Some((queue.conf.group(), record));
            }
        }
        None
    }

    /// Executes one claimed row and returns its payload-free lifecycle outcome.
    fn spawn_task(
        &mut self,
        site: Site,
        sender: mpsc::Sender<Completion>,
        group: TaskGroup,
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
            group,
            metrics: self.metrics.clone(),
            payload_limit,
            error_limit,
        });
        let handle = tokio::spawn(future);
        let running = RunningTask {
            group,
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
        if let Some(group) = self.group_mut(completion.group) {
            group.running = group.running.saturating_sub(1);
            group.poll_after = tokio::time::Instant::now();
        }
        commits.push(completion.commit);
    }

    /// Buffers one completion and schedules both persistence and capacity polling.
    fn queue_completion(
        &mut self,
        completion: Completion,
        commits: &mut Vec<TaskCommit>,
        flush_at: &mut Option<tokio::time::Instant>,
        next_poll: &mut tokio::time::Instant,
    ) {
        self.accept_completion(completion, commits);
        flush_at.get_or_insert(tokio::time::Instant::now() + OUTCOME_FLUSH_DELAY);
        *next_poll = tokio::time::Instant::now();
    }

    /// Persists one outcome batch without dropping it when the store fails.
    async fn flush_commits(&mut self, commits: &mut Vec<TaskCommit>) -> bool {
        if commits.is_empty() {
            return false;
        }
        let pending = std::mem::take(commits);
        let started = std::time::Instant::now();
        if let Err(error) = self.store.commit_outcomes(&self.runner_id, &pending).await {
            self.metrics.commit(started.elapsed(), true);
            tracing::error!(%error, "failed to commit task outcome batch");
            commits.extend(pending);
            false
        } else {
            self.metrics.commit(started.elapsed(), false);
            self.wake_committed_groups(&pending);
            true
        }
    }

    fn group_mut(&mut self, group: TaskGroup) -> Option<&mut GroupQueue> {
        self.groups
            .iter_mut()
            .find(|queue| queue.conf.group() == group)
    }

    fn wake_groups(&mut self) {
        let now = tokio::time::Instant::now();
        for group in &mut self.groups {
            group.poll_after = now;
        }
    }

    fn wake_committed_groups(&mut self, commits: &[TaskCommit]) {
        let now = tokio::time::Instant::now();
        for commit in commits {
            if let Some(group) = self.group_mut(commit.group) {
                group.poll_after = now;
            }
        }
    }

    fn store_conf(&self) -> Result<super::TaskStoreConf, TaskError> {
        Ok(super::TaskStoreConf {
            handlers: self.registry.tasks.keys().cloned().collect(),
            groups: self.groups.iter().map(|group| group.conf.clone()).collect(),
            batch_size: self.batch_size,
            lease_duration: self.registry.config.lease_duration_value(),
            idempotency: self.registry.config.idempotency_value(),
            max_error_bytes: self.registry.config.error_limit(),
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
        group: execution.group,
        outcome,
    };
    if execution
        .sender
        .send(Completion {
            group: execution.group,
            commit,
        })
        .await
        .is_err()
    {
        tracing::error!(%task_id, "task completion channel closed before commit");
    }
}

fn should_flush(
    commits: &[TaskCommit],
    batch_size: usize,
    deadline: Option<tokio::time::Instant>,
) -> bool {
    !commits.is_empty()
        && (commits.len() >= batch_size
            || deadline.is_some_and(|value| value <= tokio::time::Instant::now()))
}

fn renewal_interval(lease: std::time::Duration) -> tokio::time::Duration {
    let interval = lease / 3;
    if interval.is_zero() {
        tokio::time::Duration::from_millis(1)
    } else {
        interval
    }
}

fn group_deadline(
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

/// Validates custom-store grouped results before they enter scheduler queues.
fn validate_poll(claims: &[GroupClaim], poll: &TaskPoll) -> Result<(), TaskError> {
    if claims.len() != poll.groups.len() {
        return Err(TaskError::TaskExecutionError(
            "task store returned incomplete grouped polling evidence".into(),
        ));
    }
    for claim in claims {
        let mut matches = poll
            .groups
            .iter()
            .filter(|group| group.group == claim.group);
        let Some(group) = matches.next() else {
            return Err(invalid_group_poll(claim.group));
        };
        let valid = matches.next().is_none()
            && group.tasks.len() <= claim.limit
            && group.reclaimed <= group.tasks.len()
            && group
                .tasks
                .iter()
                .all(|task| task.group == claim.group.as_str());
        if !valid {
            return Err(invalid_group_poll(claim.group));
        }
    }
    Ok(())
}

fn invalid_group_poll(group: TaskGroup) -> TaskError {
    TaskError::TaskExecutionError(format!(
        "task store returned invalid polling evidence for group '{group}'"
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::Data;
    use crate::tasks::{
        DEFAULT_TASK_GROUP, GroupPoll, RegisteredTask, TaskConf, TaskGroup, TaskRate, TaskRegistry,
        store::MemoryTaskStore,
    };

    const EMAIL: TaskGroup = TaskGroup::new("email");

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
            group: DEFAULT_TASK_GROUP.to_string(),
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

    /// Builds a two-group runner with a claim batch smaller than global capacity.
    fn grouped_runner() -> Result<AbstractTaskRunner<MemoryTaskStore>, TaskError> {
        let conf = TaskConf::default().concurrency(3).batch_size(2).groups([
            TaskGroupConf::new(DEFAULT_TASK_GROUP, 2),
            TaskGroupConf::new(EMAIL, 1)
                .rate_limit(TaskRate::per_minute(1).burst(1))
                .global_rate_limit(TaskRate::per_minute(1).burst(1)),
        ]);
        let registry = Arc::new(TaskRegistry::new().with_config(conf));
        let dispatcher = registry.dispatcher(Arc::new(MemoryTaskStore::new(2)));
        AbstractTaskRunner::new(dispatcher)
    }

    /// Verifies grouped claims never reserve more than one global persistence batch.
    #[test]
    fn claims_share_global_capacity_and_batch_budget() -> Result<(), TaskError> {
        let mut runner = grouped_runner()?;
        let first = runner.claims();
        assert_eq!(first.iter().map(|claim| claim.limit).sum::<usize>(), 2);
        assert_eq!(
            first.first().map(|claim| claim.group),
            Some(DEFAULT_TASK_GROUP)
        );

        runner.rotate();
        let second = runner.claims();
        assert_eq!(second.iter().map(|claim| claim.limit).sum::<usize>(), 2);
        assert_eq!(second.first().map(|claim| claim.group), Some(EMAIL));
        Ok(())
    }

    /// Verifies a local rate bucket bounds claims before any task lease is acquired.
    #[test]
    fn local_rate_limits_group_claim_budget() -> Result<(), TaskError> {
        let mut runner = grouped_runner()?;
        runner.rotate();
        let claims = runner.claims();
        assert_eq!(
            claims
                .iter()
                .find(|claim| claim.group == EMAIL)
                .map(|claim| claim.limit),
            Some(1)
        );

        let now = tokio::time::Instant::now();
        let email = runner
            .group_mut(EMAIL)
            .ok_or_else(|| TaskError::UnknownGroup(EMAIL.to_string()))?;
        email.consume_local_rate(1, now);
        assert_eq!(email.claim_limit(1, now), 0);
        assert!(email.local_rate_wake(now).is_some());
        Ok(())
    }

    /// Verifies an empty local permit budget preserves its next-token wake deadline.
    #[test]
    fn local_rate_wait_does_not_fall_back_to_idle_polling() -> Result<(), TaskError> {
        let conf = TaskConf::default()
            .concurrency(1)
            .groups([TaskGroupConf::new(DEFAULT_TASK_GROUP, 1)
                .rate_limit(TaskRate::per_minute(1).burst(1))]);
        let registry = Arc::new(TaskRegistry::new().with_config(conf));
        let dispatcher = registry.dispatcher(Arc::new(MemoryTaskStore::new(1)));
        let mut runner = AbstractTaskRunner::new(dispatcher)?;
        let now = tokio::time::Instant::now();
        let group = runner
            .group_mut(DEFAULT_TASK_GROUP)
            .ok_or_else(|| TaskError::UnknownGroup(DEFAULT_TASK_GROUP.to_string()))?;
        group.consume_local_rate(1, now);

        assert!(runner.claims().is_empty());
        let deadline = runner.next_group_deadline(now + runner.fallback_interval);
        assert!(deadline.duration_since(now) >= tokio::time::Duration::from_secs(59));
        assert!(deadline.duration_since(now) < runner.fallback_interval);
        Ok(())
    }

    /// Verifies saturated work uses the short interval while idle work uses the fallback.
    #[test]
    fn adaptive_deadlines_distinguish_backlog_and_idle() -> Result<(), TaskError> {
        let mut runner = grouped_runner()?;
        let before_idle = tokio::time::Instant::now();
        let idle = runner.apply_poll(TaskPoll::empty());
        assert!(idle.duration_since(before_idle) >= tokio::time::Duration::from_secs(299));

        let before_hot = tokio::time::Instant::now();
        let hot = runner.apply_poll(TaskPoll {
            groups: vec![GroupPoll {
                group: DEFAULT_TASK_GROUP,
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
    fn adaptive_deadlines_are_isolated_by_group() -> Result<(), TaskError> {
        let mut runner = grouped_runner()?;
        let now = tokio::time::Instant::now();
        let earliest = runner.apply_poll(TaskPoll {
            groups: vec![
                GroupPoll {
                    group: DEFAULT_TASK_GROUP,
                    tasks: Vec::new(),
                    reclaimed: 0,
                    saturated: true,
                    next_wake_in: None,
                },
                GroupPoll {
                    group: EMAIL,
                    tasks: Vec::new(),
                    reclaimed: 0,
                    saturated: true,
                    next_wake_in: Some(std::time::Duration::from_secs(60)),
                },
            ],
        });
        assert!(earliest.duration_since(now) <= tokio::time::Duration::from_millis(1_100));
        let email = runner
            .group_mut(EMAIL)
            .ok_or_else(|| TaskError::UnknownGroup(EMAIL.to_string()))?;
        assert!(email.poll_after.duration_since(now) >= tokio::time::Duration::from_secs(60));
        Ok(())
    }

    /// Verifies a capacity-blocked lane cannot keep the scheduler in an expired poll loop.
    #[test]
    fn unavailable_groups_do_not_control_next_poll() -> Result<(), TaskError> {
        let mut runner = grouped_runner()?;
        let now = tokio::time::Instant::now();
        let default = runner
            .group_mut(DEFAULT_TASK_GROUP)
            .ok_or_else(|| TaskError::UnknownGroup(DEFAULT_TASK_GROUP.to_string()))?;
        default.running = default.conf.concurrency();
        default.poll_after = now;
        let email = runner
            .group_mut(EMAIL)
            .ok_or_else(|| TaskError::UnknownGroup(EMAIL.to_string()))?;
        email.poll_after = now + tokio::time::Duration::from_secs(60);
        let deadline = runner.next_group_deadline(now + runner.fallback_interval);
        assert!(deadline.duration_since(now) >= tokio::time::Duration::from_secs(60));
        Ok(())
    }

    /// Verifies malformed custom-store polling evidence fails before queue mutation.
    #[test]
    fn grouped_poll_evidence_must_match_claims() {
        let claims = [GroupClaim {
            group: DEFAULT_TASK_GROUP,
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
        let mut registry = TaskRegistry::new();
        registry
            .register(RegisteredTask::new("panic-job", panic_job))
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
            group: DEFAULT_TASK_GROUP,
            metrics: Arc::new(super::super::TaskMetrics::new(
                ["panic-job".into()],
                [DEFAULT_TASK_GROUP.to_string()],
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
