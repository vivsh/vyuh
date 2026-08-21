use serde::{Deserialize, Serialize};
use std::{
    any::TypeId,
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};

use crate::{
    Error, Site,
    callables::{self, Callable},
};

use super::models::{IdempotencyPolicy, TaskPolicy};
use super::{TaskConf, TaskDefinition, TaskDispatcher, TaskRecord};

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

#[derive(Clone)]
#[doc(hidden)]
pub struct BatchTaskContext {
    site: Site,
    payload: callables::DataBox,
    operation_id: crate::OperationId,
}

impl callables::IntoDataBox for BatchTaskContext {
    fn into_data_box(self) -> callables::DataBox {
        self.payload
    }
}

impl callables::HasSite for BatchTaskContext {
    fn site(&self) -> &Site {
        &self.site
    }
}

impl callables::FromContextParts<BatchTaskContext> for crate::OperationId {
    fn from_context_parts(context: &BatchTaskContext) -> Result<Self, callables::CallError> {
        Ok(context.operation_id)
    }
}

type BatchHandler = Callable<BatchTaskContext, Error>;
type BatchDecoder = fn(Vec<Arc<TaskRecord>>, crate::OperationId) -> DecodedBatch;
type BatchOutcome = fn(callables::DataBox, usize) -> Result<Vec<TaskOutcome>, TaskError>;

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
    /// Unwraps lifecycle state for framework outcome adapters.
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
    handler: RegisteredHandler,
    operation: callables::Operation,
    policy: TaskPolicy,
}

#[derive(Clone)]
enum RegisteredHandler {
    Single {
        handler: TaskHandler,
        outcome: fn(callables::DataBox) -> TaskOutcome,
    },
    Batch {
        handler: BatchHandler,
        decode: BatchDecoder,
        outcome: BatchOutcome,
    },
}

pub(crate) struct TaskExecutionResult {
    pub(crate) record: Arc<TaskRecord>,
    pub(crate) outcome: TaskOutcome,
}

struct DecodedBatch {
    records: Vec<Arc<TaskRecord>>,
    positions: Vec<usize>,
    outcomes: Vec<Option<TaskOutcome>>,
    payload: callables::DataBox,
}

impl RegisteredTask {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn operation(&self) -> callables::Operation {
        self.operation.clone()
    }

    pub(crate) const fn is_batch(&self) -> bool {
        matches!(self.handler, RegisteredHandler::Batch { .. })
    }

    pub(crate) const fn declared_lane(&self) -> super::TaskLane {
        self.policy.declared_lane
    }

    pub(crate) const fn effective_lane(&self) -> super::TaskLane {
        self.policy.effective_lane
    }

    pub(crate) fn resolve_lane(&mut self, lane: super::TaskLane) {
        self.policy.effective_lane = lane;
    }

    pub(crate) const fn idempotency_policy(&self) -> Option<IdempotencyPolicy> {
        self.policy.idempotency
    }

    pub(crate) fn idempotency_key<T: 'static>(
        &self,
        input: &T,
    ) -> Result<Option<String>, TaskError> {
        self.policy.key_for(input)
    }

    /// Resolves an idempotency key from a verified type-erased emitter payload.
    pub(crate) fn idempotency_key_box(
        &self,
        input: &dyn std::any::Any,
    ) -> Result<Option<String>, TaskError> {
        self.policy.key_for_box(input)
    }

    /// Verifies that one type-erased payload is the input accepted by this task.
    pub(crate) fn validate_box(&self, input: &callables::DataBox) -> Result<(), TaskError> {
        if self.type_id == input.payload_type_id() {
            Ok(())
        } else {
            Err(TaskError::TypeMismatch(
                self.type_name.clone(),
                "emitter payload".into(),
            ))
        }
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

    #[cfg(test)]
    pub async fn execute(&self, site: Site, record: Arc<TaskRecord>) -> TaskOutcome {
        let mut results = self.execute_many(site, vec![record]).await;
        match results.pop() {
            Some(result) => result.outcome,
            None => TaskOutcome::fail("Task handler produced no outcome"),
        }
    }

    pub(crate) async fn execute_many(
        &self,
        site: Site,
        records: Vec<Arc<TaskRecord>>,
    ) -> Vec<TaskExecutionResult> {
        match &self.handler {
            RegisteredHandler::Single { handler, outcome } => {
                execute_singles(handler, *outcome, site, records, self.operation.id).await
            }
            RegisteredHandler::Batch {
                handler,
                decode,
                outcome,
            } => execute_batch(handler, *decode, *outcome, site, records, self.operation.id).await,
        }
    }

    async fn execute_single(
        handler: &TaskHandler,
        outcome: fn(callables::DataBox) -> TaskOutcome,
        site: Site,
        record: Arc<TaskRecord>,
        operation_id: crate::OperationId,
    ) -> TaskOutcome {
        let payload = match handler.deserialize_input(&record.input) {
            Ok(value) => value,
            Err(error) => {
                log_handler_error(&record, operation_id, &error);
                return TaskOutcome::fail("Task input is invalid");
            }
        };

        let ctx = TaskContext {
            site,
            payload,
            record: record.clone(),
            operation_id,
        };

        let data = match handler.call(ctx).await {
            Ok(data) => data,
            Err(error) => {
                log_handler_error(&record, operation_id, &error);
                return TaskOutcome::handler_failed();
            }
        };

        outcome(data)
    }

    pub fn new<T, H, Args>(definition: TaskDefinition<T>, handler: H) -> Self
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
        let (name, policy) = definition.into_parts();
        let callable: callables::Callable<TaskContext, Error> = Callable::new(handler);
        let mut operation =
            callables::Operation::from_specs(callables::OperationKind::Task, callable.inspect());
        operation.name = name.clone();
        RegisteredTask {
            name,
            type_id: TypeId::of::<T>(),
            type_name: std::any::type_name::<T>().to_string(),
            handler: RegisteredHandler::Single {
                outcome: H::Output::into_task_outcome,
                handler: callable,
            },
            operation,
            policy: policy.erase(),
        }
    }

    pub fn new_batch<T, H, Args>(definition: TaskDefinition<T>, handler: H) -> Self
    where
        T: callables::DataValue,
        H: callables::Specable<Args> + Send + Sync + 'static,
        H::Output: callables::IntoOutput<Error>
            + callables::IntoReturnPart
            + super::IntoTaskBatchOutcomePart
            + Send
            + 'static,
        Args: callables::FromContext<BatchTaskContext>
            + callables::IntoArgSpecs
            + callables::HasData<super::Batch<T>>
            + Send
            + 'static,
    {
        let (name, policy) = definition.into_parts();
        let callable: BatchHandler = Callable::new(handler);
        let mut operation =
            callables::Operation::from_specs(callables::OperationKind::Task, callable.inspect());
        operation.name = name.clone();
        Self {
            name,
            type_id: TypeId::of::<T>(),
            type_name: std::any::type_name::<T>().to_string(),
            handler: RegisteredHandler::Batch {
                handler: callable,
                decode: decode_batch::<T>,
                outcome: <H::Output as super::IntoTaskBatchOutcomePart>::into_task_outcomes,
            },
            operation,
            policy: policy.erase(),
        }
    }
}

async fn execute_singles(
    handler: &TaskHandler,
    outcome: fn(callables::DataBox) -> TaskOutcome,
    site: Site,
    records: Vec<Arc<TaskRecord>>,
    operation_id: crate::OperationId,
) -> Vec<TaskExecutionResult> {
    let mut results = Vec::with_capacity(records.len());
    for record in records {
        let task_outcome = RegisteredTask::execute_single(
            handler,
            outcome,
            site.clone(),
            record.clone(),
            operation_id,
        )
        .await;
        results.push(TaskExecutionResult {
            record,
            outcome: task_outcome,
        });
    }
    results
}

async fn execute_batch(
    handler: &BatchHandler,
    decode: BatchDecoder,
    outcome: BatchOutcome,
    site: Site,
    records: Vec<Arc<TaskRecord>>,
    operation_id: crate::OperationId,
) -> Vec<TaskExecutionResult> {
    let mut batch = decode(records, operation_id);
    if !batch.positions.is_empty() {
        let context = BatchTaskContext {
            site,
            payload: batch.payload.clone(),
            operation_id,
        };
        let outcomes = call_batch(
            handler,
            outcome,
            context,
            batch.positions.len(),
            batch.records.first().map(Arc::as_ref),
        )
        .await;
        apply_batch_outcomes(&mut batch, outcomes);
    }
    finish_batch(batch)
}

async fn call_batch(
    handler: &BatchHandler,
    outcome: BatchOutcome,
    context: BatchTaskContext,
    expected: usize,
    record: Option<&TaskRecord>,
) -> Vec<TaskOutcome> {
    let operation_id = context.operation_id;
    match handler.call(context).await {
        Ok(data) => match outcome(data, expected) {
            Ok(outcomes) => outcomes,
            Err(error) => {
                log_batch_error(record, operation_id, expected, &error);
                vec![TaskOutcome::handler_failed(); expected]
            }
        },
        Err(error) => {
            log_batch_error(record, operation_id, expected, &error);
            vec![TaskOutcome::handler_failed(); expected]
        }
    }
}

fn log_batch_error(
    record: Option<&TaskRecord>,
    operation_id: crate::OperationId,
    count: usize,
    error: &(dyn std::error::Error + 'static),
) {
    if let Some(record) = record {
        tracing::error!(
            task_id = %record.id(),
            operation_id = %operation_id,
            lane = %record.lane,
            attempt = record.attempts,
            count,
            error = %error_chain(error),
            "durable task batch failed"
        );
    } else {
        tracing::error!(operation_id = %operation_id, count, error = %error_chain(error),
            "durable task batch failed");
    }
}

fn decode_batch<T: callables::DataValue>(
    records: Vec<Arc<TaskRecord>>,
    operation_id: crate::OperationId,
) -> DecodedBatch {
    let mut values = Vec::with_capacity(records.len());
    let mut positions = Vec::with_capacity(records.len());
    let mut outcomes = vec![None; records.len()];
    for (position, record) in records.iter().enumerate() {
        match serde_json::from_str::<T>(&record.input) {
            Ok(value) => {
                values.push(value);
                positions.push(position);
            }
            Err(error) => {
                log_handler_error(record, operation_id, &error);
                if let Some(slot) = outcomes.get_mut(position) {
                    *slot = Some(TaskOutcome::fail("Task input is invalid"));
                }
            }
        }
    }
    DecodedBatch {
        records,
        positions,
        outcomes,
        payload: callables::DataBox::new_data(super::Batch::new(values)),
    }
}

fn apply_batch_outcomes(batch: &mut DecodedBatch, outcomes: Vec<TaskOutcome>) {
    for (position, outcome) in batch.positions.iter().copied().zip(outcomes) {
        if let Some(slot) = batch.outcomes.get_mut(position) {
            *slot = Some(outcome);
        }
    }
}

fn finish_batch(batch: DecodedBatch) -> Vec<TaskExecutionResult> {
    batch
        .records
        .into_iter()
        .zip(batch.outcomes)
        .map(|(record, outcome)| TaskExecutionResult {
            record,
            outcome: match outcome {
                Some(outcome) => outcome,
                None => TaskOutcome::handler_failed(),
            },
        })
        .collect()
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
    lane_defaults: BTreeMap<super::TaskLane, super::TaskLaneConf>,
    lanes: Vec<super::TaskLaneConf>,
}

impl TaskRegistry {
    pub(crate) fn new() -> Self {
        Self {
            config: TaskConf::default(),
            tasks: HashMap::new(),
            typed_map: HashMap::new(),
            lane_defaults: BTreeMap::new(),
            lanes: Vec::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_config(mut self, config: TaskConf) -> Result<Self, TaskError> {
        self.lanes = config.resolve_lanes(std::iter::empty())?;
        self.config = config;
        Ok(self)
    }

    /// Resolves every immutable task definition against validated site policy.
    pub(crate) fn finalize(mut self, config: TaskConf) -> Result<Self, TaskError> {
        let lanes = config.resolve_lanes(self.lane_defaults.values().cloned())?;
        for task in self.tasks.values_mut() {
            let declared = task.declared_lane();
            validate_key_revision(task.idempotency_policy())?;
            let (effective, fallback) = config
                .resolve_lane(&lanes, declared)
                .map_err(|error| missing_lane_error(error, task.name(), declared, &lanes))?;
            if fallback {
                tracing::warn!(
                    task = task.name(),
                    declared_lane = %declared,
                    effective_lane = %effective,
                    "task lane is not configured; using the default lane"
                );
            }
            task.resolve_lane(effective);
        }
        self.config = config;
        self.lanes = lanes;
        Ok(self)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Returns whether one registered handler consumes local task batches.
    pub(crate) fn is_batch(&self, name: &str) -> bool {
        self.tasks.get(name).is_some_and(RegisteredTask::is_batch)
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

    /// Adds one bundle-owned default for a named non-default task lane.
    pub(crate) fn register_lane(&mut self, lane: super::TaskLaneConf) -> Result<(), TaskError> {
        let name = lane.lane();
        if name == super::DEFAULT_TASK_LANE {
            return Err(TaskError::InvalidConfig(
                "bundles cannot configure the default task lane".into(),
            ));
        }
        if self.lane_defaults.contains_key(&name) {
            return Err(TaskError::AlreadyExists(format!("task lane '{name}'")));
        }
        self.lane_defaults.insert(name, lane);
        Ok(())
    }

    pub(crate) fn merge(&mut self, other: TaskRegistry) -> Result<(), TaskError> {
        for lane in other.lane_defaults.into_values() {
            self.register_lane(lane)?;
        }
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

    /// Returns the complete validated lane set used by the runtime and store.
    pub(crate) fn lanes(&self) -> &[super::TaskLaneConf] {
        &self.lanes
    }

    /// Produces the finalized per-handler idempotency policy shared with stores.
    pub(crate) fn idempotency_conf(
        &self,
    ) -> Result<Vec<super::store::TaskIdempotencyConf>, TaskError> {
        self.tasks
            .values()
            .filter_map(|task| {
                task.idempotency_policy().map(|policy| {
                    lane_retention(&self.lanes, task.effective_lane()).map(|retention| {
                        super::store::TaskIdempotencyConf {
                            handler: task.name.clone(),
                            lane: task.effective_lane().to_string(),
                            revision: policy.revision.into(),
                            retention,
                        }
                    })
                })
            })
            .collect()
    }

    pub(crate) fn dispatcher<S: crate::tasks::store::AbstractTaskStore + Send + Sync + 'static>(
        self: Arc<Self>,
        store: Arc<S>,
        schedules: Vec<super::store::TaskScheduleConf>,
    ) -> TaskDispatcher<S> {
        let metrics = Arc::new(super::TaskMetrics::new(
            self.tasks.keys().cloned(),
            self.lanes.iter().map(|lane| lane.lane().to_string()),
        ));
        TaskDispatcher {
            store,
            registry: self.clone(),
            notifier: Arc::new(tokio::sync::Notify::new()),
            initialized: Arc::new(tokio::sync::OnceCell::new()),
            metrics,
            health: super::TaskHealth::new(self.config.readiness_policy(), !self.is_empty()),
            schedules: schedules.into(),
        }
    }

    pub(crate) async fn execute_many(
        &self,
        site: Site,
        records: Vec<Arc<TaskRecord>>,
    ) -> Vec<TaskExecutionResult> {
        let Some(name) = records.first().map(|record| record.name().to_owned()) else {
            return Vec::new();
        };
        let Some(task) = self.tasks.get(&name) else {
            return missing_results(records, &name);
        };
        task.execute_many(site, records).await
    }
}

fn missing_results(records: Vec<Arc<TaskRecord>>, name: &str) -> Vec<TaskExecutionResult> {
    records
        .into_iter()
        .map(|record| TaskExecutionResult {
            record,
            outcome: TaskOutcome::fail(format!("Task '{name}' not found")),
        })
        .collect()
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

/// Validates the stable identifier used to distinguish idempotency-key semantics.
fn validate_key_revision(policy: Option<IdempotencyPolicy>) -> Result<(), TaskError> {
    let Some(policy) = policy else {
        return Ok(());
    };
    let revision = policy.revision;
    if revision.is_empty() || revision.len() > 64 {
        return Err(TaskError::InvalidConfig(
            "task idempotency revisions must contain between 1 and 64 bytes".into(),
        ));
    }
    if !revision.bytes().all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
    }) {
        return Err(TaskError::InvalidConfig(
            "task idempotency revisions use lowercase letters, digits, '-' or '_'".into(),
        ));
    }
    Ok(())
}

/// Reads the already validated retention policy for one effective registered lane.
fn lane_retention(
    lanes: &[super::TaskLaneConf],
    lane: super::TaskLane,
) -> Result<super::IdempotencyRetention, TaskError> {
    lanes
        .iter()
        .find(|entry| entry.lane() == lane)
        .map(super::TaskLaneConf::idempotency_policy)
        .ok_or_else(|| TaskError::UnknownLane(lane.to_string()))
}

/// Adds task-definition context when strict lane resolution rejects site construction.
fn missing_lane_error(
    error: TaskError,
    task: &str,
    declared: super::TaskLane,
    lanes: &[super::TaskLaneConf],
) -> TaskError {
    if !matches!(error, TaskError::UnknownLane(_)) {
        return error;
    }
    let configured = lanes
        .iter()
        .map(|lane| lane.lane().as_str())
        .collect::<Vec<_>>()
        .join(", ");
    TaskError::InvalidConfig(format!(
        "task '{task}' declares lane '{declared}', but configured lanes are: {configured}"
    ))
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
            DEFAULT_TASK_LANE, LaneClaim, TaskCommit, TaskId, TaskIdempotency, TaskOptions,
            TaskReceipt, store::AbstractTaskStore, store::MemoryTaskStore,
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

    async fn batch_job(
        input: Data<super::super::Batch<DirectJob>>,
    ) -> super::super::Batch<TaskState> {
        input
            .iter()
            .map(|job| {
                if job.id % 2 == 0 {
                    TaskState::complete()
                } else {
                    TaskState::retry("odd job")
                }
            })
            .collect()
    }

    async fn short_batch(
        _input: Data<super::super::Batch<DirectJob>>,
    ) -> super::super::Batch<TaskState> {
        super::super::Batch::new(Vec::new())
    }

    async fn sleeping_batch(
        _input: Data<super::super::Batch<DirectJob>>,
    ) -> Result<TaskState, crate::Error> {
        Ok(TaskState::sleep("state", Duration::from_secs(1))?)
    }

    async fn suspended_batch(
        _input: Data<super::super::Batch<DirectJob>>,
    ) -> Result<TaskState, crate::Error> {
        Ok(TaskState::suspend("state")?)
    }

    async fn unit_batch(_input: Data<super::super::Batch<DirectJob>>) {}

    async fn retrying_batch(_input: Data<super::super::Batch<DirectJob>>) -> TaskState {
        TaskState::retry("try again")
    }

    async fn failing_batch(_input: Data<super::super::Batch<DirectJob>>) -> TaskState {
        TaskState::fail("permanent failure")
    }

    async fn error_batch(_input: Data<super::super::Batch<DirectJob>>) -> Result<(), crate::Error> {
        Err(crate::Error::invalid("batch handler failed"))
    }

    fn record<T: Serialize>(name: &str, input: &T) -> Result<Arc<TaskRecord>, TaskError> {
        let now = chrono::Utc::now();
        Ok(Arc::new(TaskRecord {
            id: TaskId::new(uuid::Uuid::now_v7()),
            parent_id: None,
            root_id: None,
            kind: super::super::TaskKind::Work,
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

    /// Verifies direct batch registration preserves ordered per-task outcomes.
    #[tokio::test]
    async fn batch_registration_maps_ordered_outcomes() -> Result<(), String> {
        let task = RegisteredTask::new_batch(TaskDefinition::new("batch_job"), batch_job);
        let records = vec![
            record("batch_job", &DirectJob { id: 1 }).map_err(|error| error.to_string())?,
            record("batch_job", &DirectJob { id: 2 }).map_err(|error| error.to_string())?,
        ];
        let results = task
            .execute_many(
                test_site().await.map_err(|error| error.to_string())?,
                records,
            )
            .await;
        assert!(matches!(
            results.first().map(|result| &result.outcome),
            Some(TaskOutcome::Retry { error }) if error == "odd job"
        ));
        assert!(matches!(
            results.get(1).map(|result| &result.outcome),
            Some(TaskOutcome::Complete)
        ));
        Ok(())
    }

    /// Verifies malformed rows fail alone while valid rows still reach the batch handler.
    #[tokio::test]
    async fn batch_invalid_input_is_isolated() -> Result<(), String> {
        let task = RegisteredTask::new_batch(TaskDefinition::new("batch_job"), batch_job);
        let mut invalid = record("batch_job", &DirectJob { id: 1 })
            .map_err(|error| error.to_string())?
            .as_ref()
            .clone();
        invalid.input = "{".into();
        let records = vec![
            Arc::new(invalid),
            record("batch_job", &DirectJob { id: 2 }).map_err(|error| error.to_string())?,
        ];
        let results = task
            .execute_many(
                test_site().await.map_err(|error| error.to_string())?,
                records,
            )
            .await;
        assert!(matches!(
            results.first().map(|result| &result.outcome),
            Some(TaskOutcome::Fail { error }) if error == "Task input is invalid"
        ));
        assert!(matches!(
            results.get(1).map(|result| &result.outcome),
            Some(TaskOutcome::Complete)
        ));
        Ok(())
    }

    /// Verifies invalid cardinality fails every valid member of an invocation.
    #[tokio::test]
    async fn batch_cardinality_mismatch_fails_all() -> Result<(), String> {
        let task = RegisteredTask::new_batch(TaskDefinition::new("short_batch"), short_batch);
        let records = vec![
            record("short_batch", &DirectJob { id: 1 }).map_err(|error| error.to_string())?,
            record("short_batch", &DirectJob { id: 2 }).map_err(|error| error.to_string())?,
        ];
        let results = task
            .execute_many(
                test_site().await.map_err(|error| error.to_string())?,
                records,
            )
            .await;
        assert!(
            results
                .iter()
                .all(|result| matches!(result.outcome, TaskOutcome::Fail { .. }))
        );
        Ok(())
    }

    /// Verifies value-only batches reject continuation lifecycle outcomes.
    #[tokio::test]
    async fn batch_sleep_is_rejected() -> Result<(), String> {
        let task = RegisteredTask::new_batch(TaskDefinition::new("sleeping_batch"), sleeping_batch);
        let results = task
            .execute_many(
                test_site().await.map_err(|error| error.to_string())?,
                vec![
                    record("sleeping_batch", &DirectJob { id: 1 })
                        .map_err(|error| error.to_string())?,
                ],
            )
            .await;
        assert!(matches!(
            results.first().map(|result| &result.outcome),
            Some(TaskOutcome::Fail { error }) if error.contains("cannot suspend or sleep")
        ));
        Ok(())
    }

    /// Verifies unit and uniform task-state returns map independently to every batch member.
    #[tokio::test]
    async fn batch_uniform_returns_apply_to_every_member() -> Result<(), String> {
        let site = test_site().await.map_err(|error| error.to_string())?;
        let records = || -> Result<Vec<Arc<TaskRecord>>, String> {
            Ok(vec![
                record("uniform", &DirectJob { id: 1 }).map_err(|error| error.to_string())?,
                record("uniform", &DirectJob { id: 2 }).map_err(|error| error.to_string())?,
            ])
        };
        let unit = RegisteredTask::new_batch(TaskDefinition::new("unit"), unit_batch)
            .execute_many(site.clone(), records()?)
            .await;
        assert!(
            unit.iter()
                .all(|result| matches!(result.outcome, TaskOutcome::Complete))
        );
        let retried = RegisteredTask::new_batch(TaskDefinition::new("retry"), retrying_batch)
            .execute_many(site.clone(), records()?)
            .await;
        assert!(retried.iter().all(|result| matches!(
            &result.outcome,
            TaskOutcome::Retry { error } if error == "try again"
        )));
        let failed = RegisteredTask::new_batch(TaskDefinition::new("fail"), failing_batch)
            .execute_many(site, records()?)
            .await;
        assert!(failed.iter().all(|result| matches!(
            &result.outcome,
            TaskOutcome::Fail { error } if error == "permanent failure"
        )));
        Ok(())
    }

    /// Verifies a handler error becomes one contained terminal failure per valid input.
    #[tokio::test]
    async fn batch_handler_error_fails_every_valid_member() -> Result<(), String> {
        let task = RegisteredTask::new_batch(TaskDefinition::new("error_batch"), error_batch);
        let results = task
            .execute_many(
                test_site().await.map_err(|error| error.to_string())?,
                vec![
                    record("error_batch", &DirectJob { id: 1 })
                        .map_err(|error| error.to_string())?,
                    record("error_batch", &DirectJob { id: 2 })
                        .map_err(|error| error.to_string())?,
                ],
            )
            .await;
        assert!(results.iter().all(|result| matches!(
            &result.outcome,
            TaskOutcome::Fail { error } if error == "Task handler failed"
        )));
        Ok(())
    }

    /// Verifies an all-malformed invocation is rejected without applying handler failure outcomes.
    #[tokio::test]
    async fn batch_all_invalid_inputs_skip_the_handler() -> Result<(), String> {
        let task = RegisteredTask::new_batch(TaskDefinition::new("error_batch"), error_batch);
        let mut first = record("error_batch", &DirectJob { id: 1 })
            .map_err(|error| error.to_string())?
            .as_ref()
            .clone();
        first.input = "{".into();
        let mut second = record("error_batch", &DirectJob { id: 2 })
            .map_err(|error| error.to_string())?
            .as_ref()
            .clone();
        second.input = "[".into();
        let results = task
            .execute_many(
                test_site().await.map_err(|error| error.to_string())?,
                vec![Arc::new(first), Arc::new(second)],
            )
            .await;
        assert!(results.iter().all(|result| matches!(
            &result.outcome,
            TaskOutcome::Fail { error } if error == "Task input is invalid"
        )));
        Ok(())
    }

    /// Verifies value-only batches reject both continuation lifecycle variants.
    #[tokio::test]
    async fn batch_suspend_is_rejected() -> Result<(), String> {
        let task =
            RegisteredTask::new_batch(TaskDefinition::new("suspended_batch"), suspended_batch);
        let results = task
            .execute_many(
                test_site().await.map_err(|error| error.to_string())?,
                vec![
                    record("suspended_batch", &DirectJob { id: 1 })
                        .map_err(|error| error.to_string())?,
                ],
            )
            .await;
        assert!(matches!(
            results.first().map(|result| &result.outcome),
            Some(TaskOutcome::Fail { error }) if error.contains("cannot suspend or sleep")
        ));
        Ok(())
    }

    /// Verifies direct task registration retains typed task submission without result storage.
    #[tokio::test]
    async fn direct_registration_supports_typed_submit() -> Result<(), TaskError> {
        let mut registry = TaskRegistry::new().with_config(TaskConf::default())?;
        registry.register(RegisteredTask::new(
            TaskDefinition::new("direct_job"),
            direct_job,
        ))?;

        let store = Arc::new(MemoryTaskStore::new(10));
        let dispatcher = Arc::new(registry).dispatcher(store.clone(), Vec::new());
        let task_id = dispatcher.submit(DirectJob { id: 42 }).await?.id();
        let claimed = store
            .claim_tasks(
                "runner-a",
                &[LaneClaim {
                    lane: DEFAULT_TASK_LANE,
                    limit: 10,
                    owner: None,
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
        assert_eq!(task.parent_id, None);
        assert_eq!(task.root_id, None);
        assert_eq!(task.kind, super::super::TaskKind::Work);

        store
            .commit_outcomes(
                "runner-a",
                &[TaskCommit {
                    task_id,
                    lane: DEFAULT_TASK_LANE,
                    outcome: TaskOutcome::complete(),
                    owner_token: None,
                }],
            )
            .await?;
        Ok(())
    }

    /// Verifies invalid submission delay values surface only from submission terminals.
    #[tokio::test]
    async fn task_options_defer_errors_to_submission() -> Result<(), TaskError> {
        let mut registry = TaskRegistry::new().with_config(TaskConf::default())?;
        registry.register(RegisteredTask::new(
            TaskDefinition::new("direct_job"),
            direct_job,
        ))?;
        let dispatcher =
            Arc::new(registry).dispatcher(Arc::new(MemoryTaskStore::new(10)), Vec::new());
        let oversized = TaskOptions::new().delay(Duration::from_secs(u64::MAX));
        assert!(matches!(
            dispatcher.submit_with(DirectJob { id: 1 }, oversized).await,
            Err(TaskError::InvalidOptions(_))
        ));
        Ok(())
    }

    /// Verifies typed bulk key derivation preserves ordered queued and existing receipts.
    #[tokio::test]
    async fn typed_bulk_idempotency_preserves_receipt_order() -> Result<(), TaskError> {
        let mut registry = TaskRegistry::new().with_config(TaskConf::default())?;
        registry.register(RegisteredTask::new(
            TaskDefinition::new("direct_job")
                .idempotency(TaskIdempotency::new("direct-job-v1", |job: &DirectJob| {
                    format!("job:{}", job.id)
                })),
            direct_job,
        ))?;
        let dispatcher =
            Arc::new(registry).dispatcher(Arc::new(MemoryTaskStore::new(10)), Vec::new());
        let receipts = dispatcher
            .submit_many_with(
                [DirectJob { id: 1 }, DirectJob { id: 1 }],
                TaskOptions::new(),
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

    /// Verifies a task's static key rule inherits retention from its finalized lane.
    #[test]
    fn static_idempotency_inherits_lane_retention() -> Result<(), TaskError> {
        let mut registry = TaskRegistry::new();
        registry.register(RegisteredTask::new(
            TaskDefinition::new("direct_job")
                .idempotency(TaskIdempotency::new("direct-job-v1", |job: &DirectJob| {
                    format!("job:{}", job.id)
                })),
            direct_job,
        ))?;
        let registry = registry.finalize(
            TaskConf::default().lane(
                super::super::TaskLaneConf::new(DEFAULT_TASK_LANE, 10)
                    .idempotency_retention(Duration::from_secs(60)),
            ),
        )?;
        let policy = registry
            .idempotency_conf()?
            .into_iter()
            .next()
            .ok_or_else(|| TaskError::TaskNotFound("direct_job".into()))?;

        assert_eq!(policy.handler, "direct_job");
        assert_eq!(policy.lane, DEFAULT_TASK_LANE.as_str());
        assert!(matches!(
            policy.retention,
            super::super::IdempotencyRetention::RetainFor(duration)
                if duration == Duration::from_secs(60)
        ));
        Ok(())
    }

    /// Verifies an empty typed batch succeeds without touching durable storage.
    #[tokio::test]
    async fn empty_bulk_submission_returns_no_receipts() -> Result<(), TaskError> {
        let mut registry = TaskRegistry::new().with_config(TaskConf::default())?;
        registry.register(RegisteredTask::new(
            TaskDefinition::new("direct_job"),
            direct_job,
        ))?;
        let store = Arc::new(MemoryTaskStore::new(10));
        let dispatcher = Arc::new(registry).dispatcher(store.clone(), Vec::new());

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
        let service = RegisteredTask::new(TaskDefinition::new("operation_job"), handler);
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
        let service = RegisteredTask::new(TaskDefinition::new("unit_job"), unit_job);
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
        let service = RegisteredTask::new(TaskDefinition::new("result_unit_job"), result_unit_job);
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
        let service = RegisteredTask::new(TaskDefinition::new("failed_job"), failed_job);
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
        let service = RegisteredTask::new(TaskDefinition::new("direct_job"), direct_job);
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
