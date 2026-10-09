//! Registered handler invocation, input decoding, and per-task outcome conversion.
use super::TaskRegistry;
use super::{BatchWorkContext, WorkContext};
use std::{any::TypeId, sync::Arc};

use crate::{
    Error, Site,
    callables::{self, Callable},
};

use super::TaskRuntimeError;
#[cfg(test)]
use super::TaskStatus;
#[cfg(test)]
use super::WorkState;
use super::diagnostics::causal_chain as error_chain;
use super::models::{IdempotencyPolicy, TaskPolicy};
#[cfg(test)]
use super::{Continuation, TaskConf};
use super::{TaskDefinition, TaskOutcome, TaskRecord};

use super::FlowContext;
use super::flow::handler as flow;
use super::{IntoWorkBatchOutcomePart, IntoWorkOutcomePart};

type WorkHandler = Callable<WorkContext, Error>;

type BatchHandler = Callable<BatchWorkContext, Error>;
type BatchDecoder = fn(Vec<Arc<TaskRecord>>, crate::OperationId) -> DecodedBatch;
type BatchOutcome = fn(callables::DataBox, usize) -> Result<Vec<TaskOutcome>, TaskRuntimeError>;

/// Consumes prepared outcomes without copying successful output strings.
pub(in crate::tasks) fn prepared_outcome(
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

#[derive(Clone)]
pub(crate) struct RegisteredTask {
    pub name: String,
    pub type_id: TypeId,
    pub type_name: String,
    pub(in crate::tasks) handler: RegisteredHandler,
    pub(in crate::tasks) operation: callables::Operation,
    pub(in crate::tasks) policy: TaskPolicy,
}

#[derive(Clone)]
pub(in crate::tasks) enum RegisteredHandler {
    FlowFactory {
        build: super::flow::factory::BuildFlow,
    },
    Flow {
        handler: Callable<FlowContext, Error>,
        outcome: fn(callables::DataBox, &Site) -> Result<TaskOutcome, TaskRuntimeError>,
    },
    Single {
        handler: WorkHandler,
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

pub(in crate::tasks) struct DecodedBatch {
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
            RegisteredHandler::Flow {
                handler, outcome, ..
            } => flow::execute_flows(handler, *outcome, site, records, self.operation.id).await,
            RegisteredHandler::FlowFactory { .. } => records
                .into_iter()
                .map(|record| TaskExecutionResult {
                    record,
                    outcome: TaskOutcome::fail("Flow factory was not finalized"),
                })
                .collect(),
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
        handler: &WorkHandler,
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

        let ctx = WorkContext {
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
        H: super::WorkCallable<Args> + 'static,
        H::Output: IntoWorkOutcomePart<K>,
        Args: callables::FromContext<WorkContext>
            + callables::IntoArgSpecs
            + callables::HasData<T>
            + Send
            + 'static,
    {
        let (name, policy) = definition.into_parts();
        let callable = super::callable::work(handler, |value: H::Output| {
            super::outcome::erase_outcome(value.into_work_outcome())
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
        H: super::WorkCallable<Args> + 'static,
        H::Output: super::IntoWorkBatchOutcomePart,
        Args: callables::FromContext<BatchWorkContext>
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
                outcome: super::work::batch::resolve_batch,
            },
            operation,
            policy: policy.erase(),
        }
    }
}

/// Converts queued records to independently prepared outcomes without mutating storage.
async fn execute_singles(
    handler: &WorkHandler,
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
        let context = BatchWorkContext {
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
    context: BatchWorkContext,
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
pub(in crate::tasks) fn log_handler_error(
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

impl TaskRegistry {
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

#[cfg(test)]
#[path = "tests/handler.rs"]
mod tests;

#[cfg(test)]
#[path = "tests/work_registration.rs"]
mod work_registration_tests;
