//! Framework-private durable task-store coordination contract.

use std::{future::Future, sync::Arc, time::Duration};

use super::{TaskError, TaskFilter, TaskId, TaskLane, TaskLaneConf, TaskReceipt};

#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
pub(crate) mod database;
#[cfg(any(
    test,
    not(any(feature = "postgres", feature = "mysql", feature = "sqlite"))
))]
mod memory;

pub use super::handler::TaskOutcome;
pub use super::models::TaskRecord;
pub use super::submission::TaskWrite;
#[cfg(any(
    test,
    not(any(feature = "postgres", feature = "mysql", feature = "sqlite"))
))]
pub(crate) use memory::MemoryTaskStore;

#[cfg(feature = "mysql")]
pub(crate) type MySqlTaskStore = database::DbTaskStore;
#[cfg(feature = "postgres")]
pub(crate) type PgTaskStore = database::DbTaskStore;
#[cfg(feature = "sqlite")]
pub(crate) type SqliteTaskStore = database::DbTaskStore;

/// One lane's available claim capacity for the current scheduling turn.
#[derive(Debug, Clone)]
pub struct LaneClaim {
    /// Named lane to claim.
    pub lane: TaskLane,
    /// Maximum candidate rows requested for this lane.
    pub limit: usize,
}

/// Claimed work and saturation evidence for one lane.
#[derive(Debug, Clone)]
pub struct LanePoll {
    /// Lane represented by this result.
    pub lane: TaskLane,
    /// Rows successfully claimed for the requesting runner.
    pub tasks: Vec<TaskRecord>,
    /// Number of claimed rows reclaimed after an expired lease.
    pub reclaimed: usize,
    /// Whether the candidate query filled its requested limit.
    pub saturated: bool,
    /// This lane's next effective store-relative readiness deadline.
    pub next_wake_in: Option<Duration>,
}

/// One per-lane store poll and its earliest useful future wake.
#[derive(Debug, Clone)]
pub struct TaskPoll {
    /// Per-lane claim results in scheduling order.
    pub lanes: Vec<LanePoll>,
}

/// One paced scheduler turn, including ownership maintenance and new claims.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct TaskTick {
    /// Work claimed after owned outcomes and leases were durably processed.
    pub poll: TaskPoll,
    /// Running tasks that no longer belong to this runner.
    pub lost: Vec<TaskId>,
}

#[cfg(test)]
impl TaskPoll {
    /// Creates an empty poll with no known future task deadline.
    pub fn empty() -> Self {
        Self { lanes: Vec::new() }
    }
}

/// One completed handler outcome waiting for durable commit.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct TaskCommit {
    /// Persisted task receiving the lifecycle outcome.
    pub task_id: TaskId,
    /// Lane whose newly available capacity should be polled after commit.
    pub lane: TaskLane,
    /// Payload-free handler lifecycle outcome.
    pub outcome: TaskOutcome,
}

/// Runtime policy resolved before a task store starts serving workers.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct TaskStoreConf {
    /// Stable handler names understood by this worker deployment.
    pub handlers: Vec<String>,
    /// Validated named lanes; stores coordinate only their global rate policies.
    pub lanes: Vec<TaskLaneConf>,
    /// Key retention policy shared by all workers.
    pub idempotency: super::TaskIdempotency,
}

/// Persistence and coordination boundary for durable task execution.
#[allow(dead_code)]
pub(crate) trait AbstractTaskStore {
    /// Validates or initializes store-wide task-lane coordination state.
    fn initialize(
        &self,
        conf: TaskStoreConf,
    ) -> impl Future<Output = Result<(), TaskError>> + Send + '_;

    /// Claims bounded work for every supplied lane and returns its wake hint.
    fn claim_tasks<'a>(
        &'a self,
        runner_id: &'a str,
        claims: &'a [LaneClaim],
    ) -> impl Future<Output = Result<TaskPoll, TaskError>> + Send + 'a;

    /// Commits multiple outcomes owned by one runner.
    fn commit_outcomes<'a>(
        &'a self,
        runner_id: &'a str,
        commits: &'a [TaskCommit],
    ) -> impl Future<Output = Result<(), TaskError>> + Send + 'a;

    /// Renews leases still owned by one runner and returns ownership losses.
    fn renew_leases<'a>(
        &'a self,
        runner_id: &'a str,
        task_ids: &'a [TaskId],
    ) -> impl Future<Output = Result<Vec<TaskId>, TaskError>> + Send + 'a;

    /// Runs one atomic scheduler turn for one runner.
    fn tick<'a>(
        &'a self,
        runner_id: &'a str,
        claims: &'a [LaneClaim],
        commits: &'a [TaskCommit],
        renewals: &'a [TaskId],
    ) -> impl Future<Output = Result<TaskTick, TaskError>> + Send + 'a;

    /// Stores a batch of task intents and resolves idempotency receipts.
    fn store_tasks(
        &self,
        writes: Vec<TaskWrite>,
    ) -> impl Future<Output = Result<Vec<TaskReceipt>, TaskError>> + Send + '_;

    /// Moves non-running work between configured lanes.
    fn reassign_lane<'a>(
        &'a self,
        from: &'a str,
        to: &'a str,
    ) -> impl Future<Output = Result<u64, TaskError>> + Send + 'a;

    /// Resumes one suspended task with typed serialized input.
    fn resume<'a>(
        &'a self,
        id: TaskId,
        input: String,
    ) -> impl Future<Output = Result<bool, TaskError>> + Send + 'a;

    /// Lists persisted tasks through bounded filters.
    fn list_tasks(
        &self,
        filter: TaskFilter,
    ) -> impl Future<Output = Result<crate::routes::Page<TaskRecord>, TaskError>> + Send + '_;

    /// Reads one persisted task by identifier.
    fn get_task(
        &self,
        id: TaskId,
    ) -> impl Future<Output = Result<Option<TaskRecord>, TaskError>> + Send + '_;
}

impl<T: AbstractTaskStore + Send + Sync + ?Sized> AbstractTaskStore for Arc<T> {
    async fn initialize(&self, conf: TaskStoreConf) -> Result<(), TaskError> {
        (**self).initialize(conf).await
    }

    async fn claim_tasks(
        &self,
        runner_id: &str,
        claims: &[LaneClaim],
    ) -> Result<TaskPoll, TaskError> {
        (**self).claim_tasks(runner_id, claims).await
    }

    async fn commit_outcomes(
        &self,
        runner_id: &str,
        commits: &[TaskCommit],
    ) -> Result<(), TaskError> {
        (**self).commit_outcomes(runner_id, commits).await
    }

    async fn renew_leases(
        &self,
        runner_id: &str,
        task_ids: &[TaskId],
    ) -> Result<Vec<TaskId>, TaskError> {
        (**self).renew_leases(runner_id, task_ids).await
    }

    async fn tick(
        &self,
        runner_id: &str,
        claims: &[LaneClaim],
        commits: &[TaskCommit],
        renewals: &[TaskId],
    ) -> Result<TaskTick, TaskError> {
        (**self).tick(runner_id, claims, commits, renewals).await
    }

    async fn store_tasks(&self, writes: Vec<TaskWrite>) -> Result<Vec<TaskReceipt>, TaskError> {
        (**self).store_tasks(writes).await
    }

    async fn reassign_lane(&self, from: &str, to: &str) -> Result<u64, TaskError> {
        (**self).reassign_lane(from, to).await
    }

    async fn resume(&self, id: TaskId, input: String) -> Result<bool, TaskError> {
        (**self).resume(id, input).await
    }

    async fn list_tasks(
        &self,
        filter: TaskFilter,
    ) -> Result<crate::routes::Page<TaskRecord>, TaskError> {
        (**self).list_tasks(filter).await
    }

    async fn get_task(&self, id: TaskId) -> Result<Option<TaskRecord>, TaskError> {
        (**self).get_task(id).await
    }
}

/// Produces the durable policy identity shared by every store implementation.
pub(crate) fn policy_fingerprint(conf: &TaskStoreConf) -> String {
    let mut hasher = blake3::Hasher::new();
    fingerprint_idempotency(&mut hasher, conf.idempotency);
    let mut handlers = conf.handlers.iter().map(String::as_str).collect::<Vec<_>>();
    handlers.sort_unstable();
    for handler in handlers {
        hasher.update(b"handler\0");
        hasher.update(handler.as_bytes());
        hasher.update(&[0xff]);
    }
    let mut lanes = conf.lanes.iter().collect::<Vec<_>>();
    lanes.sort_unstable_by_key(|lane| lane.lane().as_str());
    for lane in lanes {
        hasher.update(b"lane\0");
        hasher.update(lane.lane().as_str().as_bytes());
        if let Some(rate) = lane.global_rate() {
            hasher.update(&rate.permits().to_le_bytes());
            hasher.update(&rate.period().as_nanos().to_le_bytes());
            hasher.update(&rate.burst_size().to_le_bytes());
        }
        let retry = lane.retry_policy();
        hasher.update(&retry.max_attempts().to_le_bytes());
        hasher.update(&retry.initial_delay().as_nanos().to_le_bytes());
        hasher.update(&retry.maximum_delay().as_nanos().to_le_bytes());
        hasher.update(&[0xff]);
    }
    hasher.finalize().to_hex().to_string()
}

fn fingerprint_idempotency(hasher: &mut blake3::Hasher, policy: super::TaskIdempotency) {
    match policy {
        super::TaskIdempotency::ActiveOnly => {
            hasher.update(b"idempotency-active");
        }
        super::TaskIdempotency::RetainFor(duration) => {
            hasher.update(b"idempotency-retain");
            hasher.update(&duration.as_nanos().to_le_bytes());
        }
    };
}

/// Bounds handler-controlled lifecycle data before it reaches any store.
pub(crate) fn normalize_outcome(
    outcome: TaskOutcome,
    payload_limit: usize,
    error_limit: usize,
) -> TaskOutcome {
    match outcome {
        TaskOutcome::Suspend { state } if state.len() > payload_limit => {
            TaskOutcome::fail("Task continuation state exceeded the configured limit")
        }
        TaskOutcome::Sleep { state, .. } if state.len() > payload_limit => {
            TaskOutcome::fail("Task continuation state exceeded the configured limit")
        }
        TaskOutcome::Sleep { delay, .. } if delay > super::config::MAX_TASK_DELAY => {
            TaskOutcome::fail("Task sleep duration exceeded the configured limit")
        }
        TaskOutcome::Retry { error } => TaskOutcome::Retry {
            error: truncate_utf8(error, error_limit),
        },
        TaskOutcome::Fail { error } => TaskOutcome::Fail {
            error: truncate_utf8(error, error_limit),
        },
        other => other,
    }
}

fn truncate_utf8(mut value: String, limit: usize) -> String {
    if value.len() <= limit {
        return value;
    }
    let mut end = limit;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
    value
}
