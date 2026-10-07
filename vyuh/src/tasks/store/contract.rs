//! Shared task-store turn contract and bounded outcome policy.

use std::{collections::HashMap, future::Future, sync::Arc, time::Duration};

use crate::tasks::{
    IdempotencyRetention, TaskFilter, TaskId, TaskLane, TaskLaneConf, TaskReceipt, TaskRuntimeError,
};

use super::{TaskOutcome, TaskRecord, TaskWrite};

/// Preserves acknowledgement evidence without publishing an uncommitted response.
#[derive(Debug, thiserror::Error)]
pub(crate) enum StoreFailure<T> {
    #[error("{0}")]
    BeforeCommit(#[source] TaskRuntimeError),
    #[error("{error}")]
    CommitUnknown {
        #[source]
        error: TaskRuntimeError,
        prepared: T,
    },
}

#[cfg(test)]
#[path = "tests/errors.rs"]
mod errors;
#[cfg(test)]
pub(crate) use errors::TestError;

impl<T, E> From<E> for StoreFailure<T>
where
    TaskRuntimeError: From<E>,
{
    fn from(error: E) -> Self {
        Self::BeforeCommit(TaskRuntimeError::from(error))
    }
}

impl<T> StoreFailure<T> {
    /// Changes response erasure without dropping unknown-commit evidence.
    pub(crate) fn map<U>(self, convert: impl FnOnce(T) -> U) -> StoreFailure<U> {
        match self {
            Self::BeforeCommit(error) => StoreFailure::BeforeCommit(error),
            Self::CommitUnknown { error, prepared } => StoreFailure::CommitUnknown {
                error,
                prepared: convert(prepared),
            },
        }
    }

    /// A failed recovery must retain the evidence from its original transaction.
    pub(crate) fn retain(self, original: T) -> Self {
        match self {
            Self::BeforeCommit(error) => Self::CommitUnknown {
                error,
                prepared: original,
            },
            Self::CommitUnknown { error, .. } => Self::CommitUnknown {
                error,
                prepared: original,
            },
        }
    }
}

/// Keeps retained recovery wake evidence bounded to distinct configured lanes.
pub(crate) fn merge_wakes(
    target: &mut Vec<TaskLane>,
    incoming: impl IntoIterator<Item = TaskLane>,
) {
    for lane in incoming {
        if !target.contains(&lane) {
            target.push(lane);
        }
    }
}

/// Accepts only ownership evidence from the original request or its prepared response.
pub(crate) fn recovery_token(claim: &LaneClaim, prepared: &TaskPoll, token: Option<&str>) -> bool {
    let Some(token) = token else { return false };
    claim
        .owner
        .as_ref()
        .and_then(|owner| owner.token.as_deref())
        == Some(token)
        || prepared.lanes.iter().any(|lane| {
            lane.lane == claim.lane
                && lane.owner.as_ref().and_then(|owner| owner.token.as_deref()) == Some(token)
        })
}

/// One lane's available claim capacity for the current scheduling turn.
#[derive(Debug, Clone)]
pub struct LaneClaim {
    /// Named lane to claim.
    pub lane: TaskLane,
    /// Maximum candidate rows requested for this lane.
    pub limit: usize,
    /// Current durable owner token and local lifecycle evidence for locked lanes.
    pub owner: Option<LaneOwnerRequest>,
}

/// Runner evidence supplied while coordinating one locked lane.
#[derive(Debug, Clone)]
pub struct LaneOwnerRequest {
    /// Token currently held by this runner, when any.
    pub token: Option<String>,
    /// Whether the runner has no queued, running, or uncommitted lane work.
    pub quiescent: bool,
    /// Whether this scheduler turn has global batch budget to claim a cohort.
    pub allow_claim: bool,
    /// Whether handler work completed since the last acknowledged owner turn.
    pub completed_work: bool,
    /// Completed lifecycle hook waiting for a fenced durable transition.
    pub hook: Option<LaneHookResult>,
}

/// Result returned by one non-blocking lane lifecycle hook.
#[derive(Debug, Clone)]
pub struct LaneHookResult {
    /// Lifecycle generation passed to the hook.
    pub generation: i64,
    /// Hook edge that completed.
    pub action: LaneHookAction,
    /// Success or one bounded diagnostic failure.
    pub result: Result<(), String>,
}

/// Lifecycle hook requested for one durably owned lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaneHookAction {
    /// Reconcile the lane's external capacity to stopped.
    Idle,
    /// Reconcile the lane's external capacity to running.
    Busy,
}

/// Durable phase of one locked lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum LaneOwnerPhase {
    Active = 0,
    Idling = 1,
    Idle = 2,
    Busying = 3,
    IdleFailed = 4,
    BusyFailed = 5,
}

#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
impl LaneOwnerPhase {
    /// Converts one stable persisted phase value.
    pub(crate) fn from_i16(value: i16) -> Result<Self, TaskRuntimeError> {
        match value {
            0 => Ok(Self::Active),
            1 => Ok(Self::Idling),
            2 => Ok(Self::Idle),
            3 => Ok(Self::Busying),
            4 => Ok(Self::IdleFailed),
            5 => Ok(Self::BusyFailed),
            _ => Err(TaskRuntimeError::TaskExecutionError(format!(
                "invalid task lane owner phase {value}"
            ))),
        }
    }
}

/// Store response for one locked lane owner turn.
#[derive(Debug, Clone)]
pub struct LaneOwnerPoll {
    /// Current owner token, omitted when this runner no longer owns the lane.
    pub token: Option<String>,
    /// Current lifecycle generation.
    pub generation: i64,
    /// Current durable lifecycle phase.
    pub phase: LaneOwnerPhase,
    /// Hook the runner must spawn without blocking its scheduler.
    pub action: Option<LaneHookAction>,
    /// Whether this turn replaced an expired prior owner token.
    pub takeover: bool,
}

/// Claimed work and saturation evidence for one lane.
#[derive(Debug, Clone)]
pub struct LanePoll {
    /// Lane represented by this result.
    pub lane: TaskLane,
    /// Rows successfully claimed for the requesting runner.
    pub tasks: Vec<TaskRecord>,
    /// Number of abandoned running rows reclaimed from a retired node.
    pub reclaimed: usize,
    /// Whether the candidate query filled its requested limit.
    pub saturated: bool,
    /// This lane's next effective store-relative readiness deadline.
    pub next_wake_in: Option<Duration>,
    /// Durable ownership result for a locked lane.
    pub owner: Option<LaneOwnerPoll>,
}

/// One per-lane store poll and its earliest useful future wake.
#[derive(Debug, Clone)]
pub struct TaskPoll {
    /// Lanes made ready by claim-side terminal transitions.
    pub wake_lanes: Vec<TaskLane>,
    /// Per-lane claim results in scheduling order.
    pub lanes: Vec<LanePoll>,
}

/// Acknowledgement of one atomic batch of outcomes and cancellations.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct CommitResult {
    /// Lanes made eligible by committed workflow transitions.
    pub wake_lanes: Vec<TaskLane>,
    /// Stale outputs and missing/already-terminal cancellation targets; none were mutated.
    pub rejected: Vec<TaskId>,
}

#[cfg(test)]
impl TaskPoll {
    /// Creates an empty poll with no known future task deadline.
    pub fn empty() -> Self {
        Self {
            lanes: Vec::new(),
            wake_lanes: Vec::new(),
        }
    }
}

/// Operation-local commit authority: a fenced execution output or explicit cancellation.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub enum TaskCommit {
    /// An execution output requires its original node, attempt and lane fencing.
    Outcome {
        /// Node which owns the claimed execution.
        node_id: uuid::Uuid,
        /// Lifetime attempt identifying the immutable claimed execution.
        attempts: i32,
        /// Persisted task receiving the lifecycle outcome.
        task_id: TaskId,
        /// Lane whose newly available capacity should be polled after commit.
        lane: TaskLane,
        /// Handler lifecycle outcome, including any serialized result or checkpoint.
        outcome: TaskOutcome,
        /// Fencing token required when the task belongs to a locked lane.
        owner_token: Option<String>,
    },
    /// An explicit cancellation may terminally fail any nonterminal task.
    Cancel {
        /// Task to cancel without claiming execution ownership.
        task_id: TaskId,
    },
}

impl TaskCommit {
    pub(crate) fn task_id(&self) -> TaskId {
        match self {
            Self::Outcome { task_id, .. } | Self::Cancel { task_id } => *task_id,
        }
    }

    pub(crate) fn lane(&self) -> Option<TaskLane> {
        match self {
            Self::Outcome { lane, .. } => Some(*lane),
            Self::Cancel { .. } => None,
        }
    }

    #[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
    pub(crate) fn node_id(&self) -> Option<uuid::Uuid> {
        match self {
            Self::Outcome { node_id, .. } => Some(*node_id),
            Self::Cancel { .. } => None,
        }
    }

    pub(crate) fn outcome(&self) -> Option<&TaskOutcome> {
        match self {
            Self::Outcome { outcome, .. } => Some(outcome),
            Self::Cancel { .. } => None,
        }
    }

    #[cfg(all(test, feature = "sqlite"))]
    pub(crate) fn attempts(&self) -> Option<i32> {
        match self {
            Self::Outcome { attempts, .. } => Some(*attempts),
            Self::Cancel { .. } => None,
        }
    }

    pub(crate) fn is_cancel(&self) -> bool {
        matches!(self, Self::Cancel { .. })
    }

    /// Checks authoritative execution evidence; cancellation has no execution-owner requirement.
    pub(crate) fn accepts(
        &self,
        status: crate::tasks::TaskStatus,
        node: Option<uuid::Uuid>,
        attempt: i32,
    ) -> bool {
        use crate::tasks::TaskStatus;
        match self {
            Self::Cancel { .. } => matches!(
                status,
                TaskStatus::Pending | TaskStatus::Running | TaskStatus::Suspended
            ),
            Self::Outcome {
                node_id, attempts, ..
            } => status == TaskStatus::Running && node == Some(*node_id) && attempt == *attempts,
        }
    }

    pub(crate) fn weight(&self) -> usize {
        self.outcome().map_or(1, super::workflow::weight)
    }
}

/// Runtime policy resolved before a task store starts serving workers.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct TaskStoreConf {
    /// Maximum members accepted in one atomic all-settled child group.
    pub max_all_children: usize,
    /// Stable handler names and classifications understood by this worker deployment.
    pub handlers: Vec<(String, crate::tasks::TaskKind)>,
    /// Validated named lanes; stores coordinate only their global rate policies.
    pub lanes: Vec<TaskLaneConf>,
    /// Immutable per-handler idempotency policies shared by all workers.
    pub idempotency: Vec<TaskIdempotencyConf>,
    /// Shared polling cadence used to throttle failed lifecycle hooks.
    pub poll_interval: Duration,
}

/// Store-visible identity for one durable task-targeted emitter schedule.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskScheduleConf {
    /// Stable cursor name.
    pub name: String,
    /// Registered task handler receiving the produced payload.
    pub task: String,
    /// Source family, such as `cron` or `periodic`.
    pub source: String,
    /// Normalized source configuration.
    pub expression: String,
    /// Initial activation policy.
    pub start: String,
}

/// One atomically cursor-coordinated scheduled submission.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct ScheduledTaskWrite {
    /// Stable schedule cursor name.
    pub name: String,
    /// Source occurrence represented by this submission.
    pub occurrence: chrono::DateTime<chrono::Utc>,
    /// Fully normalized task write.
    pub write: TaskWrite,
}

/// One batched schedule-cursor snapshot using the store's authoritative clock.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct TaskScheduleSnapshot {
    /// Database-relative time captured with the cursor query.
    pub now: chrono::DateTime<chrono::Utc>,
    /// Existing cursors keyed by stable schedule name.
    pub cursors: HashMap<String, chrono::DateTime<chrono::Utc>>,
}

/// Store-visible identity rule for one idempotent task handler.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskIdempotencyConf {
    /// Stable registered handler name.
    pub handler: String,
    /// Finalized lane that owns this handler's retention policy.
    pub lane: String,
    /// Explicit revision of the typed key derivation rule.
    pub revision: String,
    /// Retention inherited from the handler's effective lane.
    pub retention: IdempotencyRetention,
}

impl TaskStoreConf {
    /// Finds the effective key-retention policy for one registered handler.
    pub(crate) fn idempotency_for(&self, handler: &str) -> Option<IdempotencyRetention> {
        self.idempotency
            .iter()
            .find(|policy| policy.handler == handler)
            .map(|policy| policy.retention)
    }
}

/// Persistence and coordination boundary for durable task execution.
#[allow(dead_code)]
pub(crate) trait AbstractTaskStore {
    /// Registers a runner using store time; duplicate registration preserves timestamps.
    fn register_node<'a>(
        &'a self,
        node: &'a uuid::Uuid,
    ) -> impl Future<Output = Result<(), TaskRuntimeError>> + Send + 'a;

    /// Refreshes an existing node, then retires stale peers using store time.
    /// False means the caller is missing; it is never recreated and no peers are retired.
    fn heartbeat<'a>(
        &'a self,
        node: &'a uuid::Uuid,
        stale_after: Duration,
    ) -> impl Future<Output = Result<bool, TaskRuntimeError>> + Send + 'a;

    /// Returns running claims to pending, preserving checkpoints and attempt budgets.
    /// None is exclusively for standalone startup after the prior runner has stopped.
    /// Some(node) requires that node's execution and database operations to be settled.
    fn return_claims(
        &self,
        node: Option<uuid::Uuid>,
    ) -> impl Future<Output = Result<u64, TaskRuntimeError>> + Send + '_;

    /// Sets up worker coordination using configuration supplied at construction.
    fn initialize(&self) -> impl Future<Output = Result<(), TaskRuntimeError>> + Send + '_;

    /// Claims bounded work for every supplied lane and returns its wake hint.
    fn claim_tasks<'a>(
        &'a self,
        runner_id: &'a uuid::Uuid,
        claims: &'a [LaneClaim],
    ) -> impl Future<Output = Result<TaskPoll, StoreFailure<TaskPoll>>> + Send + 'a;

    /// Atomically accepts fenced execution outcomes or explicit terminal cancellations.
    fn commit_tasks<'a>(
        &'a self,
        commits: &'a [TaskCommit],
    ) -> impl Future<Output = Result<CommitResult, StoreFailure<CommitResult>>> + Send + 'a;

    /// Coordinates existing lane leases and hooks without claiming tasks.
    fn coordinate_lanes<'a>(
        &'a self,
        runner_id: &'a uuid::Uuid,
        lanes: &'a [LaneClaim],
    ) -> impl Future<Output = Result<TaskPoll, StoreFailure<TaskPoll>>> + Send + 'a;

    /// Resolves a claim whose commit acknowledgement was lost, without claiming again.
    fn recover_claims<'a>(
        &'a self,
        node: &'a uuid::Uuid,
        claims: &'a [LaneClaim],
        prepared: TaskPoll,
    ) -> impl Future<Output = Result<TaskPoll, StoreFailure<TaskPoll>>> + Send + 'a;

    /// Reconciles exact output identities before retrying still-owned outcomes.
    fn recover_outputs<'a>(
        &'a self,
        node: &'a uuid::Uuid,
        commits: &'a [TaskCommit],
        prepared: CommitResult,
    ) -> impl Future<Output = Result<CommitResult, StoreFailure<CommitResult>>> + Send + 'a;

    /// Stores a batch of task intents and resolves idempotency receipts.
    fn store_tasks(
        &self,
        writes: Vec<TaskWrite>,
    ) -> impl Future<Output = Result<Vec<TaskReceipt>, TaskRuntimeError>> + Send + '_;

    /// Reads durable cursors and the store-relative current time in one snapshot.
    fn schedule_snapshot<'a>(
        &'a self,
        names: &'a [String],
    ) -> impl Future<Output = Result<TaskScheduleSnapshot, TaskRuntimeError>> + Send + 'a;

    /// Stores one task and advances its durable schedule cursor atomically.
    fn store_scheduled(
        &self,
        write: ScheduledTaskWrite,
    ) -> impl Future<Output = Result<Option<TaskReceipt>, TaskRuntimeError>> + Send + '_;

    /// Moves non-running work between configured lanes.
    fn reassign_lane<'a>(
        &'a self,
        from: &'a str,
        to: &'a str,
    ) -> impl Future<Output = Result<u64, TaskRuntimeError>> + Send + 'a;

    /// Resumes one suspended task with typed serialized input.
    fn resume<'a>(
        &'a self,
        id: TaskId,
        input: String,
    ) -> impl Future<Output = Result<bool, TaskRuntimeError>> + Send + 'a;

    /// Lists persisted tasks through bounded filters.
    fn list_tasks(
        &self,
        filter: TaskFilter,
    ) -> impl Future<Output = Result<crate::routes::Page<TaskRecord>, TaskRuntimeError>> + Send + '_;

    /// Reads one persisted task by identifier.
    fn get_task(
        &self,
        id: TaskId,
    ) -> impl Future<Output = Result<Option<TaskRecord>, TaskRuntimeError>> + Send + '_;
}

impl<T: AbstractTaskStore + Send + Sync + ?Sized> AbstractTaskStore for Arc<T> {
    async fn register_node(&self, node: &uuid::Uuid) -> Result<(), TaskRuntimeError> {
        (**self).register_node(node).await
    }

    async fn heartbeat(
        &self,
        node: &uuid::Uuid,
        stale_after: Duration,
    ) -> Result<bool, TaskRuntimeError> {
        (**self).heartbeat(node, stale_after).await
    }

    async fn return_claims(&self, node: Option<uuid::Uuid>) -> Result<u64, TaskRuntimeError> {
        (**self).return_claims(node).await
    }

    async fn initialize(&self) -> Result<(), TaskRuntimeError> {
        (**self).initialize().await
    }

    async fn claim_tasks(
        &self,
        runner_id: &uuid::Uuid,
        claims: &[LaneClaim],
    ) -> Result<TaskPoll, StoreFailure<TaskPoll>> {
        (**self).claim_tasks(runner_id, claims).await
    }

    async fn commit_tasks(
        &self,
        commits: &[TaskCommit],
    ) -> Result<CommitResult, StoreFailure<CommitResult>> {
        (**self).commit_tasks(commits).await
    }

    async fn coordinate_lanes(
        &self,
        runner_id: &uuid::Uuid,
        lanes: &[LaneClaim],
    ) -> Result<TaskPoll, StoreFailure<TaskPoll>> {
        (**self).coordinate_lanes(runner_id, lanes).await
    }

    async fn recover_claims(
        &self,
        node: &uuid::Uuid,
        claims: &[LaneClaim],
        prepared: TaskPoll,
    ) -> Result<TaskPoll, StoreFailure<TaskPoll>> {
        (**self).recover_claims(node, claims, prepared).await
    }

    async fn recover_outputs(
        &self,
        node: &uuid::Uuid,
        commits: &[TaskCommit],
        prepared: CommitResult,
    ) -> Result<CommitResult, StoreFailure<CommitResult>> {
        (**self).recover_outputs(node, commits, prepared).await
    }

    async fn store_tasks(
        &self,
        writes: Vec<TaskWrite>,
    ) -> Result<Vec<TaskReceipt>, TaskRuntimeError> {
        (**self).store_tasks(writes).await
    }

    async fn schedule_snapshot(
        &self,
        names: &[String],
    ) -> Result<TaskScheduleSnapshot, TaskRuntimeError> {
        (**self).schedule_snapshot(names).await
    }

    async fn store_scheduled(
        &self,
        write: ScheduledTaskWrite,
    ) -> Result<Option<TaskReceipt>, TaskRuntimeError> {
        (**self).store_scheduled(write).await
    }

    async fn reassign_lane(&self, from: &str, to: &str) -> Result<u64, TaskRuntimeError> {
        (**self).reassign_lane(from, to).await
    }

    async fn resume(&self, id: TaskId, input: String) -> Result<bool, TaskRuntimeError> {
        (**self).resume(id, input).await
    }

    async fn list_tasks(
        &self,
        filter: TaskFilter,
    ) -> Result<crate::routes::Page<TaskRecord>, TaskRuntimeError> {
        (**self).list_tasks(filter).await
    }

    async fn get_task(&self, id: TaskId) -> Result<Option<TaskRecord>, TaskRuntimeError> {
        (**self).get_task(id).await
    }
}

/// Bounds handler-controlled lifecycle data before it reaches any store.
pub(crate) fn normalize_outcome(
    outcome: TaskOutcome,
    payload_limit: usize,
    error_limit: usize,
) -> TaskOutcome {
    match outcome {
        TaskOutcome::Spawn { ref state, .. } | TaskOutcome::All { ref state, .. }
            if state.len() > payload_limit =>
        {
            TaskOutcome::fail("Task continuation state exceeded the configured limit")
        }
        TaskOutcome::CompleteWith { ref output }
            if crate::tasks::result::validate_output(output).is_err() =>
        {
            TaskOutcome::fail(
                "Task output was invalid JSON or exceeded the 32768-byte result limit",
            )
        }
        TaskOutcome::Suspend { state } if state.len() > payload_limit => {
            TaskOutcome::fail("Task continuation state exceeded the configured limit")
        }
        TaskOutcome::Sleep { state, .. } if state.len() > payload_limit => {
            TaskOutcome::fail("Task continuation state exceeded the configured limit")
        }
        TaskOutcome::Sleep { delay, .. } if delay > crate::tasks::config::MAX_TASK_DELAY => {
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

pub(crate) fn truncate_utf8(mut value: String, limit: usize) -> String {
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

/// Rejects claim-capable requests at the maintenance-only store boundary.
pub(crate) fn validate_coordination(claims: &[LaneClaim]) -> Result<(), TaskRuntimeError> {
    if claims
        .iter()
        .any(|claim| claim.owner.as_ref().is_none_or(|owner| owner.allow_claim))
    {
        return Err(TaskRuntimeError::InvalidOptions(
            "lane coordination cannot claim work".into(),
        ));
    }
    Ok(())
}
