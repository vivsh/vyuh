use serde::{Deserialize, Serialize};
use std::{any::TypeId, collections::HashMap, sync::Arc, time::Duration};

use crate::{
    Error, Site,
    callables::{self, Callable},
};

use super::{TaskConf, TaskDispatcher, TaskRecord};

/// Invocation context used internally to extract task data and runtime identity.
#[doc(hidden)]
#[derive(Clone)]
pub struct TaskContext {
    site: Site,
    payload: callables::DataBox,
    record: Arc<TaskRecord>,
    operation_id: crate::OperationId,
}

impl callables::IntoDataBox for TaskContext {
    fn into_data_box(self) -> callables::DataBox {
        self.payload
    }
}

impl callables::HasSite for TaskContext {
    fn site(&self) -> &Site {
        &self.site
    }
}

impl callables::FromContextParts<TaskContext> for crate::OperationId {
    fn from_context_parts(context: &TaskContext) -> Result<Self, callables::CallError> {
        Ok(context.operation_id)
    }
}

impl callables::FromContextParts<TaskContext> for super::TaskId {
    fn from_context_parts(context: &TaskContext) -> Result<Self, callables::CallError> {
        Ok(context.record.id())
    }
}

impl callables::IntoArgPart for super::TaskId {
    fn into_arg_part() -> callables::ArgPart {
        callables::ArgPart::Ignore
    }
}

type TaskHandler = Callable<TaskContext, Error>;

/// Failure produced while configuring, submitting, storing, or executing a task.
#[derive(Debug, thiserror::Error)]
pub enum TaskError {
    #[error("Type mismatch: expected {0}, got {1}")]
    TypeMismatch(String, String),

    #[error("Task '{0}' not found")]
    TaskNotFound(String),

    #[error("Task JSON error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Task execution error: {0}")]
    TaskExecutionError(String),

    #[error("Task already exists: {0}")]
    AlreadyExists(String),

    #[error("Invalid task configuration: {0}")]
    InvalidConfig(String),

    #[error("Invalid task submission options: {0}")]
    InvalidOptions(String),

    #[error("Task lane '{0}' is not configured")]
    UnknownLane(String),

    #[error("Idempotency key conflicts with task {0}")]
    IdempotencyConflict(super::TaskId),

    #[error("Task lane '{0}' still has running work")]
    LaneBusy(String),

    #[error(transparent)]
    CallError(#[from] crate::callables::CallError),

    #[error("Database error: {0}")]
    DatabaseError(#[from] crate::db::sqlx::Error),

    #[error(transparent)]
    StoreError(#[from] crate::db::DbError),
}

/// Durable lifecycle state exposed by task inspection APIs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[repr(i16)]
#[serde(rename_all = "lowercase")]
pub enum TaskStatus {
    Pending = 0,
    Running = 1,
    Suspended = 2,
    Succeeded = 3,
    Failed = 4,
}

impl TaskStatus {
    #[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
    pub(crate) const fn as_i16(self) -> i16 {
        self as i16
    }

    /// Returns the stable lowercase status name used in diagnostics and metrics.
    pub fn as_str(self) -> &'static str {
        match self {
            TaskStatus::Pending => "pending",
            TaskStatus::Running => "running",
            TaskStatus::Suspended => "suspended",
            TaskStatus::Succeeded => "succeeded",
            TaskStatus::Failed => "failed",
        }
    }

    /// Converts the persisted status representation into a task status.
    ///
    /// Invalid values indicate a corrupted or incompatible task row and are
    /// returned as a structured task error.
    #[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
    pub(crate) fn from_i16(value: i16) -> Result<Self, TaskError> {
        match value {
            0 => Ok(Self::Pending),
            1 => Ok(Self::Running),
            2 => Ok(Self::Suspended),
            3 => Ok(Self::Succeeded),
            4 => Ok(Self::Failed),
            _ => Err(TaskError::TaskExecutionError(format!(
                "invalid task status value {value}"
            ))),
        }
    }
}

/// Payload-free lifecycle outcome committed by Vyuh's internal task store.
///
/// Task handlers return [`TaskState`] rather than this low-level store contract.
#[derive(Debug, Clone)]
pub enum TaskOutcome {
    /// Marks the task as successfully completed.
    Complete,
    /// Stores continuation state until an explicit resume.
    Suspend { state: String },
    /// Stores continuation state and schedules another execution.
    Sleep { state: String, delay: Duration },
    /// Schedules a retry and records its error.
    Retry { error: String },
    /// Marks the task as terminally failed.
    Fail { error: String },
}

impl TaskOutcome {
    /// Completes a task without persisting a result value.
    pub const fn complete() -> Self {
        Self::Complete
    }

    /// Suspends a task with durable continuation state.
    pub fn suspend<S: Serialize>(state: &S) -> Result<Self, TaskError> {
        Ok(Self::Suspend {
            state: serde_json::to_string(state)?,
        })
    }

    /// Sleeps a task until the supplied delay with durable continuation state.
    pub fn sleep<S: Serialize>(state: &S, delay: Duration) -> Result<Self, TaskError> {
        Ok(Self::Sleep {
            state: serde_json::to_string(state)?,
            delay,
        })
    }

    /// Retries a task using its lane's exponential-backoff policy.
    pub fn retry(error: impl Into<String>) -> Self {
        Self::Retry {
            error: error.into(),
        }
    }

    /// Fails a task with a safe stored error message.
    pub fn fail(error: impl Into<String>) -> Self {
        Self::Fail {
            error: error.into(),
        }
    }

    pub(crate) fn handler_failed() -> Self {
        Self::fail("Task handler failed")
    }
}

mod task_handler_return {
    pub trait Sealed {}
}

/// Internal marker for the value-less task handler return forms.
///
/// This trait is public only because direct task registration needs it in its
/// bounds. It is sealed and cannot be implemented by applications.
#[doc(hidden)]
pub trait IntoTaskOutcomePart: task_handler_return::Sealed {
    fn into_task_outcome(data: callables::DataBox) -> TaskOutcome;
}

impl task_handler_return::Sealed for () {}

impl IntoTaskOutcomePart for () {
    fn into_task_outcome(data: callables::DataBox) -> TaskOutcome {
        if data.downcast_ref::<()>().is_some() {
            TaskOutcome::Complete
        } else {
            unsupported_task_state()
        }
    }
}

impl<T, E> task_handler_return::Sealed for Result<T, E> where T: IntoTaskOutcomePart {}

impl<T, E> IntoTaskOutcomePart for Result<T, E>
where
    T: IntoTaskOutcomePart,
{
    fn into_task_outcome(data: callables::DataBox) -> TaskOutcome {
        T::into_task_outcome(data)
    }
}

fn unsupported_task_state() -> TaskOutcome {
    TaskOutcome::fail("Task handler returned an unsupported task state")
}

/// Lifecycle control returned by task handlers.
///
/// Create via static constructors: `TaskState::complete`, `TaskState::suspend`,
/// `TaskState::sleep`, `TaskState::retry`, `TaskState::fail`.
pub struct TaskState {
    inner: TaskOutcome,
}

impl TaskState {
    /// Completes a task without persisting a result value.
    pub const fn complete() -> Self {
        Self {
            inner: TaskOutcome::Complete,
        }
    }

    /// Suspends a task with durable continuation state.
    pub fn suspend<S: Serialize>(state: S) -> Result<Self, TaskError> {
        Ok(Self {
            inner: TaskOutcome::Suspend {
                state: serde_json::to_string(&state)?,
            },
        })
    }

    /// Sleeps a task until the supplied delay with durable continuation state.
    pub fn sleep<S: Serialize>(state: S, delay: Duration) -> Result<Self, TaskError> {
        Ok(Self {
            inner: TaskOutcome::Sleep {
                state: serde_json::to_string(&state)?,
                delay,
            },
        })
    }

    /// Requests another attempt under the selected task lane's retry policy.
    pub fn retry(error: impl Into<String>) -> Self {
        Self {
            inner: TaskOutcome::Retry {
                error: error.into(),
            },
        }
    }

    /// Fails a task with a safe stored error message.
    pub fn fail(error: impl Into<String>) -> Self {
        Self {
            inner: TaskOutcome::Fail {
                error: error.into(),
            },
        }
    }
}

impl<E: From<TaskError>> callables::IntoOutput<E> for TaskState {
    fn into_output(self) -> Result<callables::DataBox, E> {
        Ok(callables::DataBox::new(self.inner))
    }
}

impl callables::IntoReturnPart for TaskState {
    fn into_return_part() -> callables::ReturnPart {
        callables::ReturnPart::Empty
    }
}

impl task_handler_return::Sealed for TaskState {}

impl IntoTaskOutcomePart for TaskState {
    fn into_task_outcome(data: callables::DataBox) -> TaskOutcome {
        data.downcast_ref::<TaskOutcome>()
            .cloned()
            .unwrap_or_else(unsupported_task_state)
    }
}

impl TaskState {
    /// Unwraps lifecycle state for focused contract tests.
    #[cfg(test)]
    pub(crate) fn into_outcome(self) -> TaskOutcome {
        self.inner
    }
}

/// Optional typed continuation state and resume input for a durable task.
pub struct Continuation<S, R = ()> {
    state: Option<S>,
    resume: Option<R>,
}

impl<S, R> callables::FromContextParts<TaskContext> for Continuation<S, R>
where
    S: serde::de::DeserializeOwned + Send,
    R: serde::de::DeserializeOwned + Send,
{
    fn from_context_parts(ctx: &TaskContext) -> Result<Self, callables::CallError> {
        Ok(Self {
            state: decode_optional(ctx.record.state.as_deref())?,
            resume: decode_optional(ctx.record.resume_input.as_deref())?,
        })
    }
}

fn decode_optional<T: serde::de::DeserializeOwned>(
    value: Option<&str>,
) -> Result<Option<T>, callables::CallError> {
    value
        .map(serde_json::from_str)
        .transpose()
        .map_err(|_| callables::CallError::DeserializeFailed)
}

impl<S, R> callables::IntoArgPart for Continuation<S, R> {
    fn into_arg_part() -> callables::ArgPart {
        callables::ArgPart::Ignore
    }
}

impl<S, R> Continuation<S, R> {
    /// Returns persisted state from a previous lifecycle transition.
    pub const fn state(&self) -> Option<&S> {
        self.state.as_ref()
    }

    /// Returns the input that resumed this suspended execution.
    pub const fn resume(&self) -> Option<&R> {
        self.resume.as_ref()
    }

    /// Consumes the extractor into its optional state and resume input.
    pub fn into_parts(self) -> (Option<S>, Option<R>) {
        (self.state, self.resume)
    }
}

#[derive(Clone)]
pub(crate) struct RegisteredTask {
    pub name: String,
    pub type_id: TypeId,
    pub type_name: String,
    outcome: fn(callables::DataBox) -> TaskOutcome,
    handler: TaskHandler,
    operation: callables::Operation,
}

impl RegisteredTask {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn operation(&self) -> callables::Operation {
        self.operation.clone()
    }

    pub fn validate_object<T: 'static>(&self, _obj: &T) -> Result<(), TaskError> {
        if self.type_id != TypeId::of::<T>() {
            return Err(TaskError::TypeMismatch(
                self.type_name.clone(),
                std::any::type_name::<T>().to_string(),
            ));
        }
        Ok(())
    }

    pub async fn execute(&self, site: Site, record: Arc<TaskRecord>) -> TaskOutcome {
        let payload = match self.handler.deserialize_input(&record.input) {
            Ok(value) => value,
            Err(error) => {
                log_handler_error(&record, self.operation.id, &error);
                return TaskOutcome::fail("Task input is invalid");
            }
        };

        let ctx = TaskContext {
            site,
            payload,
            record: record.clone(),
            operation_id: self.operation.id,
        };

        let data = match self.handler.call(ctx).await {
            Ok(data) => data,
            Err(error) => {
                log_handler_error(&record, self.operation.id, &error);
                return TaskOutcome::handler_failed();
            }
        };

        (self.outcome)(data)
    }

    pub fn new<T, H, Args>(name: &str, handler: H) -> Self
    where
        T: callables::DataValue,
        H: callables::Specable<Args> + Send + Sync + 'static,
        H::Output: callables::IntoOutput<Error>
            + callables::IntoReturnPart
            + IntoTaskOutcomePart
            + Send
            + 'static,
        Args: callables::FromContext<TaskContext>
            + callables::IntoArgSpecs
            + callables::HasData<T>
            + Send
            + 'static,
    {
        let callable: callables::Callable<TaskContext, Error> = Callable::new(handler);
        let mut operation =
            callables::Operation::from_specs(callables::OperationKind::Task, callable.inspect());
        operation.name = name.to_string();
        RegisteredTask {
            name: name.to_string(),
            type_id: TypeId::of::<T>(),
            type_name: std::any::type_name::<T>().to_string(),
            outcome: H::Output::into_task_outcome,
            handler: callable,
            operation,
        }
    }
}

/// Logs one native handler failure while keeping durable task state generic.
fn log_handler_error(
    record: &TaskRecord,
    operation_id: crate::OperationId,
    error: &(dyn std::error::Error + 'static),
) {
    tracing::error!(
        task_id = %record.id(),
        operation_id = %operation_id,
        lane = %record.lane,
        attempt = record.attempts,
        error = %error_chain(error),
        "durable task handler failed"
    );
}

/// Builds an operator-only causal chain without retaining it in task history.
fn error_chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut chain = error.to_string();
    let mut source = error.source();
    for _ in 0..16 {
        let Some(current) = source else {
            break;
        };
        chain.push_str(": ");
        chain.push_str(&current.to_string());
        source = current.source();
    }
    chain
}

#[derive(Clone)]
pub(crate) struct TaskRegistry {
    pub(crate) config: TaskConf,
    pub(crate) tasks: HashMap<String, RegisteredTask>,
    pub(crate) typed_map: HashMap<TypeId, String>,
}

impl TaskRegistry {
    pub(crate) fn new() -> Self {
        Self {
            config: TaskConf::default(),
            tasks: HashMap::new(),
            typed_map: HashMap::new(),
        }
    }

    pub(crate) fn with_config(self, config: TaskConf) -> Self {
        Self {
            config,
            tasks: self.tasks,
            typed_map: self.typed_map,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    pub(crate) fn register(&mut self, service: RegisteredTask) -> Result<(), TaskError> {
        let name = service.name().to_string();
        validate_task_name(&name)?;
        if self.tasks.contains_key(&name) || self.typed_map.contains_key(&service.type_id) {
            return Err(TaskError::AlreadyExists(name));
        }
        self.typed_map.insert(service.type_id, name.clone());
        self.tasks.insert(name, service);
        Ok(())
    }

    pub(crate) fn merge(&mut self, other: TaskRegistry) -> Result<(), TaskError> {
        for (name, task) in other.tasks {
            validate_task_name(&name)?;
            if self.tasks.contains_key(&name) {
                return Err(TaskError::AlreadyExists(name));
            }
            if self.typed_map.contains_key(&task.type_id) {
                return Err(TaskError::AlreadyExists(name));
            }
            self.typed_map.insert(task.type_id, name.clone());
            self.tasks.insert(name, task);
        }
        Ok(())
    }

    pub(crate) fn dispatcher<S: crate::tasks::store::AbstractTaskStore + Send + Sync + 'static>(
        self: Arc<Self>,
        store: Arc<S>,
    ) -> TaskDispatcher<S> {
        let metrics = Arc::new(super::TaskMetrics::new(
            self.tasks.keys().cloned(),
            self.config
                .resolved_lanes()
                .into_iter()
                .map(|lane| lane.lane().to_string()),
        ));
        TaskDispatcher {
            store,
            registry: self.clone(),
            notifier: Arc::new(tokio::sync::Notify::new()),
            initialized: Arc::new(tokio::sync::OnceCell::new()),
            metrics,
            health: super::TaskHealth::new(self.config.readiness_policy(), !self.is_empty()),
        }
    }

    pub async fn execute(&self, site: Site, record: Arc<TaskRecord>) -> TaskOutcome {
        let task = match self.tasks.get(record.name()) {
            Some(task) => task,
            None => return TaskOutcome::fail(format!("Task '{}' not found", record.name())),
        };
        task.execute(site, record).await
    }
}

impl Default for TaskRegistry {
    fn default() -> Self {
        Self::new()
    }
}

fn validate_task_name(name: &str) -> Result<(), TaskError> {
    if name.is_empty() || name.chars().count() > 191 {
        return Err(TaskError::InvalidConfig(
            "task handler names must contain between 1 and 191 characters".into(),
        ));
    }
    Ok(())
}

impl std::fmt::Debug for TaskRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskRegistry")
            .field("tasks", &self.tasks.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::{
        Data, SiteError,
        tasks::{
            DEFAULT_TASK_LANE, LaneClaim, TaskCommit, TaskId, TaskLane, TaskOptions, TaskReceipt,
            store::AbstractTaskStore, store::MemoryTaskStore,
        },
    };

    #[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
    struct DirectJob {
        id: i64,
    }

    async fn direct_job(_input: Data<DirectJob>) -> Result<TaskState, crate::Error> {
        Ok(TaskState::complete())
    }

    async fn unit_job(_input: Data<DirectJob>) {}

    async fn result_unit_job(_input: Data<DirectJob>) -> Result<(), crate::Error> {
        Ok(())
    }

    async fn failed_job(_input: Data<DirectJob>) -> Result<(), crate::Error> {
        Err(crate::Error::invalid("secret task detail"))
    }

    fn record<T: Serialize>(name: &str, input: &T) -> Result<Arc<TaskRecord>, TaskError> {
        let now = chrono::Utc::now();
        Ok(Arc::new(TaskRecord {
            id: TaskId::new(uuid::Uuid::now_v7()),
            name: name.to_string(),
            input: serde_json::to_string(input)?,
            state: None,
            resume_input: None,
            status: TaskStatus::Running,
            attempts: 0,
            lane: DEFAULT_TASK_LANE.to_string(),
            lease_duration_ms: None,
            last_error: None,
            idempotency_key: None,
            idempotency_fingerprint: None,
            idempotency_expires_at: None,
            locked_by: Some("runner-a".to_string()),
            leased_until: None,
            ready_at: Some(now),
            created_at: now,
            updated_at: now,
            completed_at: None,
        }))
    }

    async fn test_site() -> Result<Site, SiteError> {
        Site::build(
            crate::SiteConf::default().log_init(false),
            crate::bundles::bundle([]),
        )
        .await
    }

    /// Verifies direct task registration retains typed task submission without result storage.
    #[tokio::test]
    async fn direct_registration_supports_typed_submit() -> Result<(), TaskError> {
        let mut registry = TaskRegistry::new();
        registry.register(RegisteredTask::new("direct_job", direct_job))?;

        let store = Arc::new(MemoryTaskStore::new(10));
        let dispatcher = Arc::new(registry).dispatcher(store.clone());
        let task_id = dispatcher.submit(DirectJob { id: 42 }).await?.id();
        let claimed = store
            .claim_tasks(
                "runner-a",
                &[LaneClaim {
                    lane: DEFAULT_TASK_LANE,
                    limit: 10,
                }],
            )
            .await?;
        let task = claimed
            .lanes
            .first()
            .and_then(|lane| lane.tasks.first())
            .ok_or_else(|| TaskError::TaskExecutionError("task was not claimed".into()))?;

        assert_eq!(task.id, task_id);
        assert_eq!(task.name, "direct_job");
        assert_eq!(task.input::<DirectJob>()?.id, 42);

        store
            .commit_outcomes(
                "runner-a",
                &[TaskCommit {
                    task_id,
                    lane: DEFAULT_TASK_LANE,
                    outcome: TaskOutcome::complete(),
                }],
            )
            .await?;
        Ok(())
    }

    /// Verifies invalid lanes and oversized durations surface only from submission terminals.
    #[tokio::test]
    async fn task_options_defer_errors_to_submission() -> Result<(), TaskError> {
        let mut registry = TaskRegistry::new();
        registry.register(RegisteredTask::new("direct_job", direct_job))?;
        let dispatcher = Arc::new(registry).dispatcher(Arc::new(MemoryTaskStore::new(10)));
        let unknown = TaskOptions::new().lane(TaskLane::new("missing"));
        assert!(matches!(
            dispatcher.submit_with(DirectJob { id: 1 }, unknown).await,
            Err(TaskError::UnknownLane(_))
        ));
        let oversized = TaskOptions::new().delay(Duration::from_secs(u64::MAX));
        assert!(matches!(
            dispatcher.submit_with(DirectJob { id: 2 }, oversized).await,
            Err(TaskError::InvalidOptions(_))
        ));
        let panicking = TaskOptions::new().idempotency_key(|_: &DirectJob| panic!("key failed"));
        assert!(matches!(
            dispatcher.submit_with(DirectJob { id: 3 }, panicking).await,
            Err(TaskError::InvalidOptions(_))
        ));
        Ok(())
    }

    /// Verifies typed bulk key derivation preserves ordered queued and existing receipts.
    #[tokio::test]
    async fn typed_bulk_idempotency_preserves_receipt_order() -> Result<(), TaskError> {
        let mut registry = TaskRegistry::new();
        registry.register(RegisteredTask::new("direct_job", direct_job))?;
        let dispatcher = Arc::new(registry).dispatcher(Arc::new(MemoryTaskStore::new(10)));
        let receipts = dispatcher
            .submit_many_with(
                [DirectJob { id: 1 }, DirectJob { id: 1 }],
                TaskOptions::new().idempotency_key(|job: &DirectJob| format!("job:{}", job.id)),
            )
            .await?;
        assert!(matches!(receipts.first(), Some(TaskReceipt::Queued(_))));
        assert!(matches!(receipts.get(1), Some(TaskReceipt::Existing(_))));
        assert_eq!(
            receipts.first().map(|receipt| receipt.id()),
            receipts.get(1).map(|receipt| receipt.id())
        );
        Ok(())
    }

    /// Verifies an empty typed batch succeeds without touching durable storage.
    #[tokio::test]
    async fn empty_bulk_submission_returns_no_receipts() -> Result<(), TaskError> {
        let mut registry = TaskRegistry::new();
        registry.register(RegisteredTask::new("direct_job", direct_job))?;
        let store = Arc::new(MemoryTaskStore::new(10));
        let dispatcher = Arc::new(registry).dispatcher(store.clone());

        let receipts = dispatcher.submit_many(Vec::<DirectJob>::new()).await?;

        assert!(receipts.is_empty());
        assert_eq!(store.task_count().await, 0);
        Ok(())
    }

    /// Verifies a task handler receives the canonical ID stored in its operation metadata.
    #[tokio::test]
    async fn task_extracts_operation_id() -> Result<(), String> {
        let seen = Arc::new(parking_lot::Mutex::new(None));
        let captured = Arc::clone(&seen);
        let handler = move |id: crate::OperationId, _input: Data<DirectJob>| {
            let captured = Arc::clone(&captured);
            async move {
                *captured.lock() = Some(id);
            }
        };
        let service = RegisteredTask::new("operation_job", handler);
        let expected = service.operation().id;
        let task =
            record("operation_job", &DirectJob { id: 1 }).map_err(|error| error.to_string())?;
        service
            .execute(test_site().await.map_err(|error| error.to_string())?, task)
            .await;
        assert_eq!(*seen.lock(), Some(expected));
        Ok(())
    }

    /// Verifies a unit task completes without serializing a synthetic null result.
    #[tokio::test]
    async fn task_unit_completes_without_result() -> Result<(), String> {
        let service = RegisteredTask::new("unit_job", unit_job);
        let outcome = service
            .execute(
                test_site().await.map_err(|error| error.to_string())?,
                record("unit_job", &DirectJob { id: 7 }).map_err(|error| error.to_string())?,
            )
            .await;

        assert!(matches!(outcome, TaskOutcome::Complete));
        Ok(())
    }

    /// Verifies fallible unit tasks complete without a persisted result value.
    #[tokio::test]
    async fn task_result_unit_completes_without_result() -> Result<(), String> {
        let service = RegisteredTask::new("result_unit_job", result_unit_job);
        let outcome = service
            .execute(
                test_site().await.map_err(|error| error.to_string())?,
                record("result_unit_job", &DirectJob { id: 7 })
                    .map_err(|error| error.to_string())?,
            )
            .await;

        assert!(matches!(outcome, TaskOutcome::Complete));
        Ok(())
    }

    /// Verifies handler errors retain no native detail in durable task outcomes.
    #[tokio::test]
    async fn task_handler_failure_uses_safe_summary() -> Result<(), String> {
        let service = RegisteredTask::new("failed_job", failed_job);
        let outcome = service
            .execute(
                test_site().await.map_err(|error| error.to_string())?,
                record("failed_job", &DirectJob { id: 7 }).map_err(|error| error.to_string())?,
            )
            .await;

        assert!(matches!(
            outcome,
            TaskOutcome::Fail { ref error } if error == "Task handler failed"
        ));
        Ok(())
    }

    /// Verifies a task state controls completion without carrying a value.
    #[tokio::test]
    async fn task_state_controls_completion_without_result() -> Result<(), String> {
        let service = RegisteredTask::new("direct_job", direct_job);
        let outcome = service
            .execute(
                test_site().await.map_err(|error| error.to_string())?,
                record("direct_job", &DirectJob { id: 7 }).map_err(|error| error.to_string())?,
            )
            .await;

        assert!(matches!(outcome, TaskOutcome::Complete));
        Ok(())
    }

    /// Verifies continuations expose borrowed state and resume input without cloning.
    #[tokio::test]
    async fn continuation_decodes_state_and_resume_input() -> Result<(), String> {
        let mut record =
            record("direct_job", &DirectJob { id: 7 }).map_err(|error| error.to_string())?;
        let mutable = Arc::get_mut(&mut record).ok_or("task record unexpectedly shared")?;
        mutable.state = Some(serde_json::to_string(&"waiting").map_err(|e| e.to_string())?);
        mutable.resume_input = Some(serde_json::to_string(&42_u32).map_err(|e| e.to_string())?);
        let context = TaskContext {
            site: test_site().await.map_err(|error| error.to_string())?,
            payload: callables::DataBox::new(DirectJob { id: 7 }),
            record,
            operation_id: crate::OperationId::new(),
        };
        let continuation =
            <Continuation<String, u32> as callables::FromContextParts<_>>::from_context_parts(
                &context,
            )
            .map_err(|error| error.to_string())?;
        assert_eq!(continuation.state().map(String::as_str), Some("waiting"));
        assert_eq!(continuation.resume(), Some(&42));
        Ok(())
    }

    /// Verifies every lifecycle control maps to a payload-free store outcome.
    #[tokio::test]
    async fn task_state_encodes_only_lifecycle() -> Result<(), TaskError> {
        assert!(matches!(
            TaskState::complete().into_outcome(),
            TaskOutcome::Complete
        ));
        assert!(matches!(
            TaskState::suspend("approval")?.into_outcome(),
            TaskOutcome::Suspend { .. }
        ));
        assert!(matches!(
            TaskState::sleep("retry", Duration::ZERO)?.into_outcome(),
            TaskOutcome::Sleep { .. }
        ));
        assert!(matches!(
            TaskState::retry("temporary").into_outcome(),
            TaskOutcome::Retry { .. }
        ));
        assert!(matches!(
            TaskState::fail("permanent").into_outcome(),
            TaskOutcome::Fail { .. }
        ));
        Ok(())
    }
}
