//! Synchronous orchestration registration, restricted extraction, and kind validation.

use super::*;
use crate::tasks::{FlowState, TaskKind};

/// Invocation-local flow input and immutable persisted continuation snapshot.
/// Intentionally provides no site or service extraction capability.
#[doc(hidden)]
pub struct FlowContext {
    pub(super) payload: callables::DataBox,
    pub(super) record: Arc<TaskRecord>,
}

impl callables::IntoDataBox for FlowContext {
    fn into_data_box(self) -> callables::DataBox {
        self.payload
    }
}

impl RegisteredTask {
    /// Registers a synchronous handler; future returns and work capabilities are rejected.
    pub fn new_flow<T, H, Args>(definition: TaskDefinition<T>, handler: H) -> Self
    where
        T: callables::DataValue,
        H: crate::tasks::FlowCallable<Args> + 'static,
        H::Output: crate::tasks::IntoFlowOutcomePart,
        Args: callables::FromContext<FlowContext>
            + callables::IntoArgSpecs
            + callables::HasData<T>
            + Send
            + 'static,
    {
        let (name, policy) = definition.into_parts();
        let callable = crate::tasks::callable::flow(handler, |value: H::Output| {
            crate::tasks::IntoFlowOutcomePart::into_flow_state(value).erase()
        });
        let mut operation =
            callables::Operation::from_specs(callables::OperationKind::Task, callable.inspect());
        operation.name = name.clone();
        Self {
            name,
            type_id: TypeId::of::<T>(),
            type_name: std::any::type_name::<T>().to_string(),
            handler: RegisteredHandler::Flow {
                handler: callable,
                outcome: prepared_flow,
            },
            operation,
            policy: policy.erase(),
        }
    }

    /// Derives execution classification from the registration, without parallel metadata.
    pub(crate) const fn kind(&self) -> TaskKind {
        match self.handler {
            RegisteredHandler::Flow { .. } => TaskKind::Flow,
            _ => TaskKind::Work,
        }
    }
}

/// Produces the shared diagnostic before any mismatched handler can be invoked.
pub(super) fn kind_mismatch() -> TaskOutcome {
    TaskOutcome::fail("Task kind does not match registered handler")
}

/// Invokes each flow through the common callable interface outside the scheduler turn.
pub(super) async fn execute_flows(
    handler: &Callable<FlowContext, Error>,
    outcome: fn(callables::DataBox, &Site) -> Result<TaskOutcome, TaskRuntimeError>,
    site: Site,
    records: Vec<Arc<TaskRecord>>,
    operation_id: crate::OperationId,
) -> Vec<TaskExecutionResult> {
    let mut results = Vec::with_capacity(records.len());
    for record in records {
        let result = invoke_flow(handler, outcome, &site, record.clone()).await;
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(error) => {
                log_handler_error(&record, operation_id, &error);
                TaskOutcome::handler_failed()
            }
        };
        results.push(TaskExecutionResult { record, outcome });
    }
    results
}

/// Decodes and extracts only pure flow inputs before preparing its returned outcome.
async fn invoke_flow(
    handler: &Callable<FlowContext, Error>,
    outcome: fn(callables::DataBox, &Site) -> Result<TaskOutcome, TaskRuntimeError>,
    site: &Site,
    record: Arc<TaskRecord>,
) -> Result<TaskOutcome, Error> {
    if record.kind != TaskKind::Flow {
        return Ok(kind_mismatch());
    }
    let payload = match handler.deserialize_input(&record.input) {
        Ok(payload) => payload,
        Err(_) => return Ok(TaskOutcome::fail("Task input is invalid")),
    };
    let data = handler.call(FlowContext { payload, record }).await?;
    Ok(outcome(data, site)?)
}

/// Resolves only the erased, already-serialized Flow request through the executing site.
fn prepared_flow(data: callables::DataBox, site: &Site) -> Result<TaskOutcome, TaskRuntimeError> {
    if data.payload_type_id() != TypeId::of::<FlowState>() {
        return prepared_outcome(data, site);
    }
    let state = data.downcast_arc::<FlowState>().ok_or_else(|| {
        TaskRuntimeError::TaskExecutionError("Invalid prepared flow outcome".into())
    })?;
    let state = Arc::try_unwrap(state).map_err(|_| {
        TaskRuntimeError::TaskExecutionError("Prepared flow outcome unexpectedly shared".into())
    })?;
    state.resolve_owned(site)
}
