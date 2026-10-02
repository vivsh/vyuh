//! Synchronous orchestration registration, restricted extraction, and kind validation.

use super::*;
use crate::tasks::{FlowState, TaskKind};

/// Invocation-local flow input and immutable persisted continuation snapshot.
/// Intentionally provides no site or service extraction capability.
#[doc(hidden)]
pub struct FlowContext {
    pub(in crate::tasks) payload: callables::DataBox,
    pub(in crate::tasks) record: Arc<TaskRecord>,
}

impl callables::IntoDataBox for FlowContext {
    fn into_data_box(self) -> callables::DataBox {
        self.payload
    }
}

impl RegisteredTask {
    /// Declares a synchronous factory; it is invoked once during site finalization.
    pub fn new_flow<T, H, Args, E, M>(definition: super::super::FlowConf<T, E>, handler: H) -> Self
    where
        T: callables::DataValue,
        H: crate::tasks::FlowCallable<Args> + 'static,
        H::Output: crate::tasks::FlowReturn<T, E, M>,
        Args: crate::tasks::FlowArguments<T>,
        E: Send + Sync + 'static,
    {
        let super::super::FlowConf {
            definition,
            build_dispatcher,
            step_limit,
            revision,
        } = definition;
        let (name, policy) = definition.into_parts();
        let spec = callables::CallSpec::for_types::<callables::specs::Tuple1<callables::Data<T>>, ()>(
            std::any::type_name::<H>(),
        );
        let mut operation = callables::Operation::from_specs(callables::OperationKind::Task, &spec);
        operation.name = name.clone();
        Self {
            name,
            type_id: TypeId::of::<T>(),
            type_name: std::any::type_name::<T>().to_string(),
            handler: RegisteredHandler::FlowFactory {
                build: crate::tasks::flow_build::deferred::<T, H, Args, E, M>(
                    handler,
                    build_dispatcher,
                    step_limit,
                    revision,
                ),
            },
            operation,
            policy: policy.erase(),
        }
    }

    /// Derives execution classification from the registration, without parallel metadata.
    pub(crate) const fn kind(&self) -> TaskKind {
        match self.handler {
            RegisteredHandler::Flow { .. } | RegisteredHandler::FlowFactory { .. } => {
                TaskKind::Flow
            }
            _ => TaskKind::Work,
        }
    }

    /// Contains factory and policy panics before any workers can start.
    pub(crate) fn prepare_flow(
        &mut self,
        site: &crate::PartialSite,
        scratch: &mut crate::tasks::flow_build::DispatcherScratch,
    ) -> Result<(), TaskRuntimeError> {
        let RegisteredHandler::FlowFactory { build } = &self.handler else {
            return Ok(());
        };
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| build(site, scratch)))
                .map_err(|_| {
                    TaskRuntimeError::InvalidConfig(format!(
                        "Flow '{}' factory panicked",
                        self.name
                    ))
                })?;
        let (handler, compatibility) = result.map_err(|error| {
            TaskRuntimeError::InvalidConfig(format!("Flow '{}': {error}", self.name))
        })?;
        self.handler = RegisteredHandler::Flow {
            handler,
            outcome: prepared_flow,
            compatibility,
        };
        Ok(())
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
