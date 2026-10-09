//! Synchronous Pravah advancement through the existing durable task lifecycle.

use crate::callables::DataBox;
use crate::tasks::{Continuation, Flow, FlowArguments, FlowError, FlowState, IntoFlow, TaskId};
use crate::tasks::{TaskOptions, TaskRuntimeError};
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
        if TypeId::of::<E>() == TypeId::of::<()>() && requires_agent(self.graph(), self.registry())
        {
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
            None if resume.is_none() => self
                .compiled
                .prepared()
                .start(
                    pravah::graph::to_value(input.as_ref())
                        .map_err(|_| protocol("Invalid graph input"))?,
                    task_id.into_uuid(),
                )
                .and_then(|runtime| runtime.with_history(self.dispatcher.history_policy())),
            None => return Err(protocol("Resume input without graph checkpoint")),
        }
        .map_err(|_| protocol("Invalid graph checkpoint or input"))?;
        if runtime.state().execution_id() != task_id.into_uuid() {
            return Err(protocol("Graph checkpoint belongs to another task"));
        }
        deliver(&mut runtime, resume)?;
        self.drive(runtime)
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
                pravah::Step::Agent(agent) => {
                    let request = self.dispatcher.agent(&agent)?;
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
    resume: Option<Result<serde_json::Value, crate::tasks::TaskFailure>>,
) -> Result<(), FlowError> {
    let waiting = runtime.pending_agent().is_some() || runtime.suspension().is_some();
    match (waiting, resume) {
        (false, None) => Ok(()),
        (true, Some(result)) => {
            if let Some(agent) = runtime.pending_agent() {
                let id = agent.id();
                let response = match result {
                    Ok(value) => serde_json::from_value(value)
                        .map_err(|_| protocol("Work output is not an AgentResponse"))?,
                    Err(failure) => pravah::AgentResponse::new(id, Err(agent_failure(failure)?)),
                };
                runtime
                    .resume_agent(response)
                    .map_err(|_| protocol("Invalid Agent response"))
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
fn agent_failure(failure: crate::tasks::TaskFailure) -> Result<pravah::AgentError, FlowError> {
    let mut error = pravah::AgentError::new("vyuh_task_failure", failure.message());
    if let Some(id) = failure.task_id() {
        let details = pravah::graph::to_value(serde_json::json!({ "task_id": id }))
            .map_err(|_| protocol("Invalid task failure identity"))?;
        error = error.with_details(details);
    }
    Ok(error)
}

/// Checks static nested graph declarations once, never during task advancement.
fn requires_agent(
    graph: &pravah::graph::UntypedGraph,
    registry: &pravah::graph::HandlerRegistry,
) -> bool {
    use pravah::graph::NodeKind;
    graph.nodes.iter().any(|node| match &node.kind {
        NodeKind::Subflow { graph } | NodeKind::Each { graph } => requires_agent(graph, registry),
        NodeKind::Either { left, right, .. } => {
            requires_agent(left, registry) || requires_agent(right, registry)
        }
        NodeKind::Continuation { key, children, .. } => {
            registry.has_agent(key.as_str())
                || children.iter().any(|child| requires_agent(child, registry))
        }
        _ => false,
    })
}

/// Shared, immutable dispatcher, built once per concrete type and site.
///
/// Methods only select Work payloads/options. They must not perform external
/// effects, submit tasks, retain execution progress, or transform returned results.
/// The store creates each Work child atomically with its Flow checkpoint.
pub trait PravahDispatcher: Send + Sync + 'static {
    /// Obtains build-time handles without blocking I/O or runtime service extraction.
    fn build(site: &PartialSite) -> Result<Self, FlowError>
    where
        Self: Sized;

    /// Selects Work returning the complete `pravah::AgentResponse`, including stage
    /// acknowledgements even when its agent outcome is a failure. Routing performs no effects.
    fn agent(&self, request: &pravah::AgentRequest) -> Result<WorkRequest, FlowError>;

    /// Selects Work returning the declared resume value, or ordinary external waiting.
    fn suspend(&self, _: &pravah::Suspension) -> Result<Option<WorkRequest>, FlowError> {
        Ok(None)
    }

    /// Selects history maintenance for new executions; all flags default to off.
    /// Restored executions retain their checkpointed policy. Work services must supply
    /// the requested store/compactor; this method must be deterministic and non-blocking.
    fn history_policy(&self) -> pravah::HistoryPolicy {
        pravah::HistoryPolicy::default()
    }
}

impl PravahDispatcher for () {
    fn build(_: &PartialSite) -> Result<Self, FlowError> {
        Ok(())
    }

    fn agent(&self, _: &pravah::AgentRequest) -> Result<WorkRequest, FlowError> {
        Err(FlowError::MissingDispatcher)
    }
}

/// Invocation-local intent to execute one Work child through an atomic checkpoint.
/// Construction never submits a task; the framework attaches the VM snapshot.
pub struct WorkRequest {
    pub(in crate::tasks) input: DataBox,
    pub(in crate::tasks) options: TaskOptions,
}

impl WorkRequest {
    /// Selects a typed payload; registration and Work kind are checked during preparation.
    pub fn new<I: DataValue>(input: I) -> Self {
        Self {
            input: DataBox::new_data(input),
            options: TaskOptions::new(),
        }
    }

    /// Sets scheduling options, rejecting invalid combinations and `ignore_conflicts`.
    pub fn options(mut self, options: TaskOptions) -> Result<Self, TaskRuntimeError> {
        crate::tasks::submission::validate_spawn_options(&options)?;
        self.options = options;
        Ok(self)
    }

    /// Resolves existing private child preparation and rejects Flow targets before storage.
    pub(in crate::tasks) fn prepare(
        self,
        state: &str,
        site: &crate::Site,
    ) -> Result<crate::tasks::TaskOutcome, TaskRuntimeError> {
        let outcome = site
            .tasks()
            .prepare_child(&self.input, state, &self.options)?;
        if let crate::tasks::TaskOutcome::Spawn { child, .. } = &outcome
            && child.record.kind != crate::tasks::TaskKind::Work
        {
            return Err(TaskRuntimeError::InvalidOptions(
                "Flow dispatch must target Work".into(),
            ));
        }
        Ok(outcome)
    }
}

#[cfg(test)]
#[path = "tests/pravah.rs"]
mod tests;
