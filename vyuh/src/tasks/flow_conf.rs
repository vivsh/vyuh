//! Build-time policy for immutable Flow registrations.

use crate::{PartialSite, callables::DataValue};

use super::{FlowError, TaskDefinition, TaskIdempotency, TaskLane};

/// Flow registration policy. Validation occurs during site construction.
pub struct FlowConf<I, E = ()> {
    pub(super) definition: TaskDefinition<I>,
    pub(super) build_effects: fn(&PartialSite) -> Result<E, FlowError>,
    pub(super) step_limit: usize,
    pub(super) revision: &'static str,
}

impl<I: DataValue> FlowConf<I> {
    /// Declares a flow without an effects policy, using a 256-instruction budget.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            definition: TaskDefinition::new(name),
            build_effects: |_| Ok(()),
            step_limit: 256,
            revision: "1",
        }
    }
}

impl<I: DataValue, E> FlowConf<I, E> {
    /// Assigns the ordinary task lane; Flow has no scheduling exemptions.
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

    /// Identifies replay-relevant application code/configuration changes.
    pub fn revision(mut self, revision: &'static str) -> Self {
        self.revision = revision;
        self
    }

    /// Selects a shared routing policy, constructed once per type and site.
    #[cfg(feature = "pravah")]
    pub fn effects<P: super::PravahEffects>(self) -> FlowConf<I, P> {
        FlowConf {
            definition: self.definition,
            build_effects: P::build,
            step_limit: self.step_limit,
            revision: self.revision,
        }
    }
}
