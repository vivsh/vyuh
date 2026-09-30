//! Shared task-store turn contract and bounded outcome policy.

use std::{collections::HashMap, future::Future, sync::Arc, time::Duration};

use crate::tasks::{
    IdempotencyRetention, TaskFilter, TaskId, TaskLane, TaskLaneConf, TaskReceipt, TaskRuntimeError,
};

use super::{TaskOutcome, TaskRecord, TaskWrite};

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
    /// Number of claimed rows reclaimed after an expired lease.
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
    /// Per-lane claim results in scheduling order.
    pub lanes: Vec<LanePoll>,
}

/// One paced scheduler turn, including ownership maintenance and new claims.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct TaskTick {
    /// Lanes with work created after claim selection; poll these in a later turn.
    pub wake_lanes: Vec<TaskLane>,
    /// Work claimed after owned outcomes and leases were durably processed.
    pub poll: TaskPoll,
    /// Running tasks that no longer belong to this runner.
    pub lost: Vec<TaskId>,
    /// Subset of lost leases whose durable task is terminally cancelled.
    /// Shared invocations may finish while these members' results are discarded.
    pub cancelled: Vec<TaskId>,
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
    /// Fencing token required when the task belongs to a locked lane.
    pub owner_token: Option<String>,
}

/// One task lease renewal fenced by its optional durable lane owner.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct TaskLease {
    /// Persisted task whose execution lease must be extended.
    pub task_id: TaskId,
    /// Lane used to select the ordinary or fenced renewal path.
    pub lane: TaskLane,
    /// Fencing token required when the task belongs to a locked lane.
    pub owner_token: Option<String>,
}

/// Runtime policy resolved before a task store starts serving workers.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct TaskStoreConf {
    /// Maximum members accepted in one atomic all-settled child group.
    pub max_all_children: usize,
    /// Stable handler names and classifications understood by this worker deployment.
    pub handlers: Vec<(String, crate::tasks::TaskKind)>,
    /// Finalized immutable Flow definition compatibility; never populated from task rows.
    pub flows: Vec<(String, String)>,
    /// Validated named lanes; stores coordinate only their global rate policies.
    pub lanes: Vec<TaskLaneConf>,
    /// Immutable per-handler idempotency policies shared by all workers.
    pub idempotency: Vec<TaskIdempotencyConf>,
    /// Normalized durable emitter schedules accepted by this worker deployment.
    pub schedules: Vec<TaskScheduleConf>,
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
    /// Records cancellation intent only for a not-yet-cancelled nonterminal task.
    fn cancel(
        &self,
        id: TaskId,
    ) -> impl Future<Output = Result<bool, TaskRuntimeError>> + Send + '_;

    /// Validates or initializes store-wide task-lane coordination state.
    fn initialize(
        &self,
        conf: TaskStoreConf,
    ) -> impl Future<Output = Result<(), TaskRuntimeError>> + Send + '_;

    /// Claims bounded work for every supplied lane and returns its wake hint.
    fn claim_tasks<'a>(
        &'a self,
        runner_id: &'a str,
        claims: &'a [LaneClaim],
    ) -> impl Future<Output = Result<TaskPoll, TaskRuntimeError>> + Send + 'a;

    /// Commits multiple outcomes owned by one runner.
    fn commit_outcomes<'a>(
        &'a self,
        runner_id: &'a str,
        commits: &'a [TaskCommit],
    ) -> impl Future<Output = Result<(), TaskRuntimeError>> + Send + 'a;

    /// Renews leases still owned by one runner and returns ownership losses.
    fn renew_leases<'a>(
        &'a self,
        runner_id: &'a str,
        leases: &'a [TaskLease],
    ) -> impl Future<Output = Result<Vec<TaskId>, TaskRuntimeError>> + Send + 'a;

    /// Runs one atomic scheduler turn for one runner.
    fn tick<'a>(
        &'a self,
        runner_id: &'a str,
        claims: &'a [LaneClaim],
        commits: &'a [TaskCommit],
        renewals: &'a [TaskLease],
    ) -> impl Future<Output = Result<TaskTick, TaskRuntimeError>> + Send + 'a;

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
    async fn cancel(&self, id: TaskId) -> Result<bool, TaskRuntimeError> {
        (**self).cancel(id).await
    }

    async fn initialize(&self, conf: TaskStoreConf) -> Result<(), TaskRuntimeError> {
        (**self).initialize(conf).await
    }

    async fn claim_tasks(
        &self,
        runner_id: &str,
        claims: &[LaneClaim],
    ) -> Result<TaskPoll, TaskRuntimeError> {
        (**self).claim_tasks(runner_id, claims).await
    }

    async fn commit_outcomes(
        &self,
        runner_id: &str,
        commits: &[TaskCommit],
    ) -> Result<(), TaskRuntimeError> {
        (**self).commit_outcomes(runner_id, commits).await
    }

    async fn renew_leases(
        &self,
        runner_id: &str,
        leases: &[TaskLease],
    ) -> Result<Vec<TaskId>, TaskRuntimeError> {
        (**self).renew_leases(runner_id, leases).await
    }

    async fn tick(
        &self,
        runner_id: &str,
        claims: &[LaneClaim],
        commits: &[TaskCommit],
        renewals: &[TaskLease],
    ) -> Result<TaskTick, TaskRuntimeError> {
        (**self).tick(runner_id, claims, commits, renewals).await
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

/// Produces the durable policy identity shared by every store implementation.
pub(crate) fn policy_fingerprint(conf: &TaskStoreConf) -> String {
    format!("tr-v7:{:.58}", policy_hash(conf, 7).to_hex())
}

#[cfg(all(test, any(feature = "postgres", feature = "mysql", feature = "sqlite")))]
#[path = "tests/contract.rs"]
mod tests;

/// Identifies the exact pre-upgrade policy so protocol migration cannot change live budgets.
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
pub(crate) fn migration_fingerprint(conf: &TaskStoreConf) -> String {
    format!("tr-a0:{:.58}", policy_hash(conf, 0).to_hex())
}

/// Accepts only a ledger-marked predecessor with exactly the same runtime policy.
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
pub(crate) fn is_migrated_policy(conf: &TaskStoreConf, stored: &str) -> bool {
    stored == migration_fingerprint(conf)
        || stored == format!("tr-a1:{:.58}", policy_hash(conf, 1).to_hex())
        || stored == format!("tr-a2:{:.58}", policy_hash(conf, 2).to_hex())
        || stored == format!("tr-a3:{:.58}", policy_hash(conf, 3).to_hex())
        || stored == format!("tr-a4:{:.58}", policy_hash(conf, 4).to_hex())
        || stored == format!("tr-a5:{:.58}", policy_hash(conf, 5).to_hex())
        || stored == format!("tr-a6:{:.58}", policy_hash(conf, 6).to_hex())
}

/// Hashes immutable deployment policy, optionally including the new result protocol.
fn policy_hash(conf: &TaskStoreConf, result_protocol: u8) -> blake3::Hash {
    let mut hasher = blake3::Hasher::new();
    if result_protocol >= 7 {
        fingerprint_flows(&mut hasher, &conf.flows);
    }
    if result_protocol >= 6 {
        hasher.update(b"task-all-claim-results-v6\0");
        hasher.update(&(conf.max_all_children as u64).to_le_bytes());
    }
    if result_protocol > 0 {
        hasher.update(b"task-resume-result-step-attempts-v1\0");
    }
    if result_protocol >= 2 {
        hasher.update(b"task-last-result-json-32768-checkpoint-clear-v2\0");
    }
    if result_protocol >= 3 {
        hasher.update(b"task-store-owned-cancellation-finish-shared-v3\0");
    }
    fingerprint_idempotency(&mut hasher, &conf.idempotency);
    fingerprint_schedules(&mut hasher, &conf.schedules);
    if result_protocol >= 4 {
        hasher.update(b"task-work-flow-capabilities-v4\0");
    }
    if result_protocol >= 5 {
        hasher.update(b"task-typed-returns-work-suspend-v5\0");
    }
    let mut handlers = conf.handlers.iter().collect::<Vec<_>>();
    handlers.sort_unstable_by_key(|(name, _)| name);
    for (handler, kind) in handlers {
        hasher.update(b"handler\0");
        hasher.update(handler.as_bytes());
        if result_protocol >= 4 {
            hasher.update(&[*kind as u8]);
        }
        hasher.update(&[0xff]);
    }
    let mut lanes = conf.lanes.iter().collect::<Vec<_>>();
    lanes.sort_unstable_by_key(|lane| lane.lane().as_str());
    if lanes.iter().any(|lane| lane.lane_lock().is_some()) {
        hasher.update(b"lane-lock-v1\0");
        hasher.update(&conf.poll_interval.as_nanos().to_le_bytes());
    }
    for lane in lanes {
        hasher.update(b"lane\0");
        hasher.update(lane.lane().as_str().as_bytes());
        if let Some(lane_lock) = lane.lane_lock() {
            fingerprint_lock(&mut hasher, lane_lock);
        }
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
    hasher.finalize()
}

/// Includes graph identity and explicit revision without additional store queries.
fn fingerprint_flows(hasher: &mut blake3::Hasher, flows: &[(String, String)]) {
    hasher.update(b"task-flow-factory-v7\0");
    let mut flows: Vec<_> = flows.iter().collect();
    flows.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    for (name, identity) in flows {
        for value in [name, identity] {
            hasher.update(&(value.len() as u64).to_le_bytes());
            hasher.update(value.as_bytes());
        }
    }
}

/// Adds one optional lane-owner policy to the shared deployment identity.
fn fingerprint_lock(hasher: &mut blake3::Hasher, lane_lock: &crate::tasks::TaskLaneLock) {
    hasher.update(b"locked\0");
    hasher.update(&lane_lock.batch_size().to_le_bytes());
    hasher.update(&lane_lock.idle_duration().as_nanos().to_le_bytes());
    if let Some(deadline) = lane_lock.batch_deadline() {
        hasher.update(&deadline.as_nanos().to_le_bytes());
    }
    for hook in [lane_lock.idle_hook(), lane_lock.busy_hook()] {
        if let Some(hook) = hook {
            hasher.update(hook.identity().as_bytes());
        }
        hasher.update(&[0]);
    }
}

fn fingerprint_schedules(hasher: &mut blake3::Hasher, schedules: &[TaskScheduleConf]) {
    let mut values = schedules.iter().collect::<Vec<_>>();
    values.sort_unstable_by(|left, right| left.name.cmp(&right.name));
    for schedule in values {
        for value in [
            schedule.name.as_str(),
            schedule.task.as_str(),
            schedule.source.as_str(),
            schedule.expression.as_str(),
            schedule.start.as_str(),
        ] {
            hasher.update(value.as_bytes());
            hasher.update(&[0]);
        }
        hasher.update(&[0xff]);
    }
}

fn fingerprint_idempotency(hasher: &mut blake3::Hasher, policies: &[TaskIdempotencyConf]) {
    let mut policies = policies.iter().collect::<Vec<_>>();
    policies.sort_unstable_by(|left, right| left.handler.cmp(&right.handler));
    for policy in policies {
        hasher.update(policy.handler.as_bytes());
        hasher.update(&[0]);
        hasher.update(policy.lane.as_bytes());
        hasher.update(&[0]);
        hasher.update(policy.revision.as_bytes());
        hasher.update(&[0]);
        match policy.retention {
            IdempotencyRetention::ActiveOnly => {
                hasher.update(b"idempotency-active");
            }
            IdempotencyRetention::RetainFor(duration) => {
                hasher.update(b"idempotency-retain");
                hasher.update(&duration.as_nanos().to_le_bytes());
            }
        }
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
