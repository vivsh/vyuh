//! Immutable workflow definitions built once for each site.

use crate::PartialSite;
use crate::callables::{Data, DataValue};
use crate::tasks::{Continuation, FlowError, FlowState, TaskId};
use crate::tasks::{TaskDefinition, TaskIdempotency, TaskLane};
use serde::{Serialize, de::DeserializeOwned};
use std::sync::Arc;

/// An immutable, synchronous workflow definition shared by every invocation.
///
/// Progress belongs exclusively to the supplied persisted continuation. Advancement
/// must be short, deterministic and non-blocking; effects belong in Work children.
pub trait Flow: Send + Sync + 'static {
    /// Registered submission payload, independent of factory arguments.
    type Input: DataValue;
    /// Successful persisted output; serialization is checked after advancement.
    type Output: Serialize + 'static;
    /// Durable checkpoint, subject to the configured checkpoint size limit.
    type Checkpoint: Serialize + DeserializeOwned + Send + 'static;
    /// Successful resume value inside Vyuh's ordinary result envelope.
    type Resume: DeserializeOwned + Send + 'static;

    /// Describes the next store-owned transition without performing durable mutations.
    fn advance(
        &self,
        task_id: TaskId,
        input: Data<Self::Input>,
        continuation: Continuation<Self::Checkpoint, Self::Resume>,
    ) -> Result<FlowState<Self::Output>, FlowError>;
}

/// Converts a factory result into an immutable executable definition once per site.
pub trait IntoFlow<I: DataValue, E = ()> {
    /// Definition retained by registration; it must not retain execution progress.
    type Prepared: Flow<Input = I>;

    /// Finalizes a definition and binds its shared dispatcher without performing effects.
    fn into_flow(self, dispatcher: Arc<E>, step_limit: usize) -> Result<Self::Prepared, FlowError>;
}

impl<F: Flow> IntoFlow<F::Input> for F {
    type Prepared = F;

    fn into_flow(self, _: Arc<()>, _: usize) -> Result<Self, FlowError> {
        Ok(self)
    }
}

/// Flow registration policy. Validation occurs during site construction.
pub struct FlowConf<I, E = ()> {
    pub(in crate::tasks) definition: TaskDefinition<I>,
    pub(in crate::tasks) build_dispatcher: fn(&PartialSite) -> Result<E, FlowError>,
    pub(in crate::tasks) step_limit: usize,
}

impl<I: DataValue> FlowConf<I> {
    /// Declares a flow without a dispatcher, using a 256-instruction budget.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            definition: TaskDefinition::new(name),
            build_dispatcher: |_| Ok(()),
            step_limit: 256,
        }
    }
}

impl<I: DataValue, E> FlowConf<I, E> {
    /// Assigns the task lane and its lifecycle gates. Flow advancement does not
    /// consume Work concurrency slots or rate permits.
    pub fn lane(mut self, lane: TaskLane) -> Self {
        self.definition = self.definition.lane(lane);
        self
    }

    /// Uses the existing task idempotency policy for submitted flow inputs.
    pub fn idempotency(mut self, policy: TaskIdempotency<I>) -> Self {
        self.definition = self.definition.idempotency(policy);
        self
    }

    /// Sets the Pravah instruction budget, validated within 1–10,000 at site build.
    pub fn step_limit(mut self, limit: usize) -> Self {
        self.step_limit = limit;
        self
    }

    /// Selects a shared request dispatcher, constructed once per type and site.
    #[cfg(feature = "pravah")]
    pub fn dispatch<P: crate::tasks::PravahDispatcher>(self) -> FlowConf<I, P> {
        FlowConf {
            definition: self.definition,
            build_dispatcher: P::build,
            step_limit: self.step_limit,
        }
    }
}
