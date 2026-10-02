//! Synchronous Pravah advancement through the existing durable task lifecycle.

use super::{
    Continuation, Flow, FlowArguments, FlowError, FlowState, IntoFlow, PravahDispatcher, TaskId,
};
use crate::{
    PartialSite,
    callables::{
        Data, DataValue,
        specs::{Tuple1, Tuple2},
    },
};
use std::{any::TypeId, sync::Arc, time::Duration};

/// Immutable prepared adapter. Execution progress exists only in task checkpoints.
#[doc(hidden)]
pub struct PreparedPravah<I, O, E> {
    compiled: pravah::CompiledFlow<I, O>,
    dispatcher: Arc<E>,
    step_limit: usize,
}

impl<I: DataValue> crate::callables::IntoArgPart for pravah::Flow<I> {
    fn into_arg_part() -> crate::callables::ArgPart {
        crate::callables::ArgPart::Ignore
    }
}

impl<I: DataValue> FlowArguments<I> for Tuple1<pravah::Flow<I>> {
    fn build(_: &PartialSite) -> Self {
        Self(pravah::Flow::root())
    }
}

impl<I: DataValue> FlowArguments<I> for Tuple2<PartialSite, pravah::Flow<I>> {
    fn build(site: &PartialSite) -> Self {
        Self(site.clone(), pravah::Flow::root())
    }
}

impl<I: DataValue, O: DataValue, E: PravahDispatcher> IntoFlow<I, E> for pravah::Flow<O> {
    type Prepared = PreparedPravah<I, O, E>;

    fn into_flow(self, dispatcher: Arc<E>, step_limit: usize) -> Result<Self::Prepared, FlowError> {
        self.finish::<I>()
            .map_err(|_| protocol("Graph compilation failed"))?
            .into_flow(dispatcher, step_limit)
    }
}

impl<I: DataValue, O: DataValue, E: PravahDispatcher> IntoFlow<I, E>
    for pravah::CompiledFlow<I, O>
{
    type Prepared = PreparedPravah<I, O, E>;

    fn into_flow(self, dispatcher: Arc<E>, step_limit: usize) -> Result<Self::Prepared, FlowError> {
        if !(1..=10_000).contains(&step_limit) {
            return Err(protocol("Invalid Flow instruction limit"));
        }
        if TypeId::of::<E>() == TypeId::of::<()>() && requires_fetch(self.graph()) {
            return Err(FlowError::MissingDispatcher);
        }
        Ok(PreparedPravah {
            compiled: self,
            dispatcher,
            step_limit,
        })
    }
}

impl<I: DataValue, O: DataValue, E: PravahDispatcher> Flow for PreparedPravah<I, O, E> {
    type Input = I;
    type Output = O;
    type Checkpoint = pravah::Snapshot;
    type Resume = serde_json::Value;

    fn advance(
        &self,
        task_id: TaskId,
        input: Data<I>,
        continuation: Continuation<Self::Checkpoint, Self::Resume>,
    ) -> Result<FlowState<O>, FlowError> {
        let (snapshot, resume) = continuation.into_parts();
        let mut runtime = match snapshot {
            Some(snapshot) => self.compiled.restore(snapshot),
            None if resume.is_none() => self.compiled.prepared().start(
                pravah::graph::to_value(input.as_ref())
                    .map_err(|_| protocol("Invalid graph input"))?,
                task_id.into_uuid(),
            ),
            None => return Err(protocol("Resume input without graph checkpoint")),
        }
        .map_err(|_| protocol("Invalid graph checkpoint or input"))?;
        if runtime.state().execution_id() != task_id.into_uuid() {
            return Err(protocol("Graph checkpoint belongs to another task"));
        }
        deliver(&mut runtime, resume)?;
        self.drive(runtime)
    }

    fn compatibility(&self) -> String {
        self.compiled.prepared().fingerprint().to_string()
    }
}

impl<I: DataValue, O: DataValue, E: PravahDispatcher> PreparedPravah<I, O, E> {
    /// Processes the final permitted boundary before considering a cooperative yield.
    fn drive(&self, mut runtime: pravah::Runtime) -> Result<FlowState<O>, FlowError> {
        for _ in 0..self.step_limit {
            match runtime
                .next()
                .map_err(|_| protocol("Graph advancement failed"))?
            {
                pravah::Step::Continue => {}
                pravah::Step::Done(value) => {
                    return Ok(FlowState::complete(
                        self.compiled
                            .decode_output(value)
                            .map_err(|_| protocol("Invalid graph output"))?,
                    ));
                }
                pravah::Step::Fetch(fetch) => {
                    let request = self.dispatcher.fetch(&fetch)?;
                    return Ok(FlowState::work(request, snapshot(&runtime)?)?);
                }
                pravah::Step::Suspend(_) => {
                    let suspension = runtime
                        .suspension()
                        .ok_or_else(|| protocol("Missing graph suspension"))?;
                    let request = self.dispatcher.suspend(suspension)?;
                    return match request {
                        Some(request) => Ok(FlowState::work(request, snapshot(&runtime)?)?),
                        None => Ok(FlowState::suspend(snapshot(&runtime)?)?),
                    };
                }
            }
        }
        Ok(FlowState::sleep(snapshot(&runtime)?, Duration::ZERO)?)
    }
}

/// A committed waiting boundary must have a result; cooperative yields must not.
fn deliver(
    runtime: &mut pravah::Runtime,
    resume: Option<Result<serde_json::Value, super::TaskFailure>>,
) -> Result<(), FlowError> {
    let waiting = runtime.pending_fetch().is_some() || runtime.suspension().is_some();
    match (waiting, resume) {
        (false, None) => Ok(()),
        (true, Some(result)) => {
            if let Some(fetch) = runtime.pending_fetch() {
                let id = fetch.id();
                let response = match result {
                    Ok(value) => Ok(serde_json::from_value(value)
                        .map_err(|_| protocol("Work output is not a FetchResponse"))?),
                    Err(failure) => Err(fetch_failure(failure)?),
                };
                runtime
                    .resume_fetch(id, response)
                    .map_err(|_| protocol("Invalid Fetch response"))
            } else {
                runtime
                    .resume(result?)
                    .map_err(|_| protocol("Invalid suspension resume value"))
            }
        }
        _ => Err(protocol("Graph checkpoint and resume input disagree")),
    }
}

fn snapshot(runtime: &pravah::Runtime) -> Result<pravah::Snapshot, FlowError> {
    runtime
        .snapshot()
        .map_err(|_| protocol("Graph checkpoint serialization failed"))
}

fn protocol(message: &'static str) -> FlowError {
    FlowError::fail(message)
}

/// Maps only the outer durable task failure; application domain values remain untouched.
fn fetch_failure(failure: super::TaskFailure) -> Result<pravah::FetchError, FlowError> {
    let mut error = pravah::FetchError::new("vyuh_task_failure", failure.message());
    if let Some(id) = failure.task_id() {
        let details = pravah::graph::to_value(serde_json::json!({ "task_id": id }))
            .map_err(|_| protocol("Invalid task failure identity"))?;
        error = error.with_details(details);
    }
    Ok(error)
}

/// Checks static nested graph declarations once, never during task advancement.
fn requires_fetch(graph: &pravah::graph::UntypedGraph) -> bool {
    use pravah::graph::NodeKind;
    graph.nodes.iter().any(|node| match &node.kind {
        NodeKind::Fetch => true,
        NodeKind::Subflow { graph } | NodeKind::Each { graph } => requires_fetch(graph),
        NodeKind::Either { left, right, .. } => requires_fetch(left) || requires_fetch(right),
        NodeKind::Continuation { children, .. } => children.iter().any(requires_fetch),
        _ => false,
    })
}

#[cfg(test)]
#[path = "tests/pravah_flow.rs"]
mod tests;

#[cfg(test)]
#[path = "tests/pravah_dispatch.rs"]
pub(crate) mod dispatch_tests;
