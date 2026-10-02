//! Immutable workflow definitions built once for each site.

use std::sync::Arc;

use serde::{Serialize, de::DeserializeOwned};

use crate::callables::{Data, DataValue};

use super::{Continuation, FlowError, FlowState, TaskId};

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

    /// Definition compatibility contribution, computed only during registration.
    /// Manual implementations normally use the explicit `FlowConf::revision` instead.
    #[doc(hidden)]
    fn compatibility(&self) -> String {
        std::any::type_name::<Self>().into()
    }
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
