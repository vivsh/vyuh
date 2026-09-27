use serde::Serialize;
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

use super::TaskRuntimeError;
#[cfg(test)]
use super::TaskState;
#[cfg(test)]
use super::TaskStatus;
use super::diagnostics::causal_chain as error_chain;
use super::models::{IdempotencyPolicy, TaskPolicy};
use super::{TaskConf, TaskDefinition, TaskDispatcher, TaskRecord};

#[path = "flow_handler.rs"]
mod flow;
use super::{IntoTaskBatchOutcomePart, IntoTaskOutcomePart};
pub use flow::FlowContext;

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
type BatchOutcome = fn(callables::DataBox, usize) -> Result<Vec<TaskOutcome>, TaskRuntimeError>;

/// Prepared lifecycle outcome committed by Vyuh's internal task store.
///
/// Work returns [`TaskState`] and Flow returns [`super::FlowState`], not this store contract.
#[derive(Debug, Clone)]
pub enum TaskOutcome {
    /// Suspends this flow until every atomically created child becomes terminal.
    All {
        state: String,
        children: Vec<super::TaskWrite>,
    },
    /// Marks the task as successfully completed.
    Complete,
    /// Completes with a serialized success value, retained and delivered to the parent.
    CompleteWith { output: String },
    /// Suspends this task and atomically creates one child.
    Spawn {
        state: String,
        child: super::TaskWrite,
    },
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
    /// Completes a task, delivering a unit success to an awaiting parent.
    pub const fn complete() -> Self {
        Self::Complete
    }

    /// Suspends a task with durable continuation state.
    pub fn suspend<S: Serialize>(state: &S) -> Result<Self, TaskRuntimeError> {
        Ok(Self::Suspend {
            state: serde_json::to_string(state)?,
        })
    }

    /// Sleeps a task until the supplied delay with durable continuation state.
    pub fn sleep<S: Serialize>(state: &S, delay: Duration) -> Result<Self, TaskRuntimeError> {
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

/// Consumes prepared outcomes without copying successful output strings.
fn prepared_outcome(
    data: callables::DataBox,
    _site: &Site,
) -> Result<TaskOutcome, TaskRuntimeError> {
    if data.downcast_ref::<()>().is_some() {
        return Ok(TaskOutcome::Complete);
    }
    let value = data.downcast_arc::<TaskOutcome>().ok_or_else(|| {
        TaskRuntimeError::TaskExecutionError("Invalid prepared task outcome".into())
    })?;
    Ok(Arc::try_unwrap(value).unwrap_or_else(|shared| (*shared).clone()))
}

/// Optional typed checkpoint and resume input for Work or Flow handlers.
pub struct Continuation<S, R = ()> {
    state: Option<S>,
    resume: Option<Result<R, super::TaskFailure>>,
}

impl<S, R> callables::FromContextParts<FlowContext> for Continuation<S, R>
where
    S: serde::de::DeserializeOwned + Send,
    R: serde::de::DeserializeOwned + Send,
{
    fn from_context_parts(ctx: &FlowContext) -> Result<Self, callables::CallError> {
        Ok(Self {
            state: decode_optional(ctx.record.state.as_deref())?,
            resume: decode_optional(ctx.record.resume_input.as_deref())?,
        })
    }
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
    pub const fn resume(&self) -> Option<&Result<R, super::TaskFailure>> {
        self.resume.as_ref()
    }

    /// Consumes the extractor into its optional state and resume input.
    pub fn into_parts(self) -> (Option<S>, Option<Result<R, super::TaskFailure>>) {
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
    Flow {
        handler: Callable<FlowContext, Error>,
        outcome: fn(callables::DataBox, &Site) -> Result<TaskOutcome, TaskRuntimeError>,
    },
    Single {
        handler: TaskHandler,
        outcome: fn(callables::DataBox, &Site) -> Result<TaskOutcome, TaskRuntimeError>,
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
    ) -> Result<Option<String>, TaskRuntimeError> {
        self.policy.key_for(input)
    }

    /// Resolves an idempotency key from a verified type-erased emitter payload.
    pub(crate) fn idempotency_key_box(
        &self,
        input: &dyn std::any::Any,
    ) -> Result<Option<String>, TaskRuntimeError> {
        self.policy.key_for_box(input)
    }

    /// Verifies that one type-erased payload is the input accepted by this task.
    pub(crate) fn validate_box(&self, input: &callables::DataBox) -> Result<(), TaskRuntimeError> {
        if self.type_id == input.payload_type_id() {
            Ok(())
        } else {
            Err(TaskRuntimeError::TypeMismatch(
                self.type_name.clone(),
                "emitter payload".into(),
            ))
        }
    }

    pub fn validate_object<T: 'static>(&self, _obj: &T) -> Result<(), TaskRuntimeError> {
        if self.type_id != TypeId::of::<T>() {
            return Err(TaskRuntimeError::TypeMismatch(
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
            RegisteredHandler::Flow { handler, outcome } => {
                flow::execute_flows(handler, *outcome, site, records, self.operation.id).await
            }
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

    /// Invokes a handler and contains errors from both execution and child preparation.
    async fn execute_single(
        handler: &TaskHandler,
        outcome: fn(callables::DataBox, &Site) -> Result<TaskOutcome, TaskRuntimeError>,
        site: &Site,
        record: Arc<TaskRecord>,
        operation_id: crate::OperationId,
    ) -> TaskOutcome {
        if record.kind != super::TaskKind::Work {
            return flow::kind_mismatch();
        }
        let payload = match handler.deserialize_input(&record.input) {
            Ok(value) => value,
            Err(error) => {
                log_handler_error(&record, operation_id, &error);
                return TaskOutcome::fail("Task input is invalid");
            }
        };

        let ctx = TaskContext {
            site: site.clone(),
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

        match outcome(data, site) {
            Ok(outcome) => outcome,
            Err(error) => {
                log_handler_error(&record, operation_id, &error);
                TaskOutcome::handler_failed()
            }
        }
    }

    pub fn new<T, H, Args, K>(definition: TaskDefinition<T>, handler: H) -> Self
    where
        T: callables::DataValue,
        H: super::TaskCallable<Args> + 'static,
        H::Output: IntoTaskOutcomePart<K>,
        Args: callables::FromContext<TaskContext>
            + callables::IntoArgSpecs
            + callables::HasData<T>
            + Send
            + 'static,
    {
        let (name, policy) = definition.into_parts();
        let callable = super::callable::work(handler, |value: H::Output| {
            super::returns::erase_outcome(value.into_task_outcome())
        });
        let mut operation =
            callables::Operation::from_specs(callables::OperationKind::Task, callable.inspect());
        operation.name = name.clone();
        RegisteredTask {
            name,
            type_id: TypeId::of::<T>(),
            type_name: std::any::type_name::<T>().to_string(),
            handler: RegisteredHandler::Single {
                outcome: prepared_outcome,
                handler: callable,
            },
            operation,
            policy: policy.erase(),
        }
    }

    pub fn new_batch<T, H, Args>(definition: TaskDefinition<T>, handler: H) -> Self
    where
        T: callables::DataValue,
        H: super::TaskCallable<Args> + 'static,
        H::Output: super::IntoTaskBatchOutcomePart,
        Args: callables::FromContext<BatchTaskContext>
            + callables::IntoArgSpecs
            + callables::HasData<super::Batch<T>>
            + Send
            + 'static,
    {
        let (name, policy) = definition.into_parts();
        let callable: BatchHandler = super::callable::work(handler, H::Output::into_batch_return);
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
                outcome: super::batch::resolve_batch,
            },
            operation,
            policy: policy.erase(),
        }
    }
}

/// Converts queued records to independently prepared outcomes without mutating storage.
async fn execute_singles(
    handler: &TaskHandler,
    outcome: fn(callables::DataBox, &Site) -> Result<TaskOutcome, TaskRuntimeError>,
    site: Site,
    records: Vec<Arc<TaskRecord>>,
    operation_id: crate::OperationId,
) -> Vec<TaskExecutionResult> {
    let mut results = Vec::with_capacity(records.len());
    for record in records {
        let task_outcome =
            RegisteredTask::execute_single(handler, outcome, &site, record.clone(), operation_id)
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
        if record.kind != super::TaskKind::Work {
            if let Some(slot) = outcomes.get_mut(position) {
                *slot = Some(flow::kind_mismatch());
            }
            continue;
        }
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
    pub(crate) fn with_config(mut self, config: TaskConf) -> Result<Self, TaskRuntimeError> {
        self.lanes = config.resolve_lanes(std::iter::empty())?;
        self.config = config;
        Ok(self)
    }

    /// Resolves every immutable task definition against validated site policy.
    pub(crate) fn finalize(mut self, config: TaskConf) -> Result<Self, TaskRuntimeError> {
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

    pub(crate) fn register(&mut self, service: RegisteredTask) -> Result<(), TaskRuntimeError> {
        let name = service.name().to_string();
        validate_task_name(&name)?;
        if self.tasks.contains_key(&name) || self.typed_map.contains_key(&service.type_id) {
            return Err(TaskRuntimeError::AlreadyExists(name));
        }
        self.typed_map.insert(service.type_id, name.clone());
        self.tasks.insert(name, service);
        Ok(())
    }

    /// Adds one bundle-owned default for a named non-default task lane.
    pub(crate) fn register_lane(
        &mut self,
        lane: super::TaskLaneConf,
    ) -> Result<(), TaskRuntimeError> {
        let name = lane.lane();
        if name == super::DEFAULT_TASK_LANE {
            return Err(TaskRuntimeError::InvalidConfig(
                "bundles cannot configure the default task lane".into(),
            ));
        }
        if self.lane_defaults.contains_key(&name) {
            return Err(TaskRuntimeError::AlreadyExists(format!(
                "task lane '{name}'"
            )));
        }
        self.lane_defaults.insert(name, lane);
        Ok(())
    }

    pub(crate) fn merge(&mut self, other: TaskRegistry) -> Result<(), TaskRuntimeError> {
        for lane in other.lane_defaults.into_values() {
            self.register_lane(lane)?;
        }
        for (name, task) in other.tasks {
            validate_task_name(&name)?;
            if self.tasks.contains_key(&name) {
                return Err(TaskRuntimeError::AlreadyExists(name));
            }
            if self.typed_map.contains_key(&task.type_id) {
                return Err(TaskRuntimeError::AlreadyExists(name));
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
    ) -> Result<Vec<super::store::TaskIdempotencyConf>, TaskRuntimeError> {
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

fn validate_task_name(name: &str) -> Result<(), TaskRuntimeError> {
    if name.is_empty() || name.chars().count() > 191 {
        return Err(TaskRuntimeError::InvalidConfig(
            "task handler names must contain between 1 and 191 characters".into(),
        ));
    }
    Ok(())
}

/// Validates the stable identifier used to distinguish idempotency-key semantics.
fn validate_key_revision(policy: Option<IdempotencyPolicy>) -> Result<(), TaskRuntimeError> {
    let Some(policy) = policy else {
        return Ok(());
    };
    let revision = policy.revision;
    if revision.is_empty() || revision.len() > 64 {
        return Err(TaskRuntimeError::InvalidConfig(
            "task idempotency revisions must contain between 1 and 64 bytes".into(),
        ));
    }
    if !revision.bytes().all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
    }) {
        return Err(TaskRuntimeError::InvalidConfig(
            "task idempotency revisions use lowercase letters, digits, '-' or '_'".into(),
        ));
    }
    Ok(())
}

/// Reads the already validated retention policy for one effective registered lane.
fn lane_retention(
    lanes: &[super::TaskLaneConf],
    lane: super::TaskLane,
) -> Result<super::IdempotencyRetention, TaskRuntimeError> {
    lanes
        .iter()
        .find(|entry| entry.lane() == lane)
        .map(super::TaskLaneConf::idempotency_policy)
        .ok_or_else(|| TaskRuntimeError::UnknownLane(lane.to_string()))
}

/// Adds task-definition context when strict lane resolution rejects site construction.
fn missing_lane_error(
    error: TaskRuntimeError,
    task: &str,
    declared: super::TaskLane,
    lanes: &[super::TaskLaneConf],
) -> TaskRuntimeError {
    if !matches!(error, TaskRuntimeError::UnknownLane(_)) {
        return error;
    }
    let configured = lanes
        .iter()
        .map(|lane| lane.lane().as_str())
        .collect::<Vec<_>>()
        .join(", ");
    TaskRuntimeError::InvalidConfig(format!(
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
#[path = "tests/handler.rs"]
mod tests;
