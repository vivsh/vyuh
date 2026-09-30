//! Sealed task-only return categories; markers resolve Serialize/Result coherence.

use super::{TaskOutcome, WorkError, WorkState};
use serde::Serialize;

/// Keeps the existing tiny unit-return allocation rather than boxing a full outcome enum.
pub(super) fn erase_outcome(outcome: TaskOutcome) -> crate::callables::DataBox {
    match outcome {
        TaskOutcome::Complete => crate::callables::DataBox::new(()),
        outcome => crate::callables::DataBox::new(outcome),
    }
}

mod sealed {
    pub trait Work<K> {}
}

/// Inferred category for a directly serializable Work value.
#[doc(hidden)]
pub struct Value;
/// Inferred category for an explicit Work state.
#[doc(hidden)]
pub struct State;
/// Inferred category for a fallible Work return.
#[doc(hidden)]
pub struct Fallible<K>(std::marker::PhantomData<K>);

/// Sealed conversion of Work returns, performed before callable type erasure.
#[doc(hidden)]
pub trait IntoWorkOutcomePart<K>: sealed::Work<K> {
    /// Produces the existing store-facing outcome without mutating any task.
    fn into_work_outcome(self) -> TaskOutcome;
}

impl<T: Serialize + 'static> sealed::Work<Value> for T {}
impl<T: Serialize + 'static> IntoWorkOutcomePart<Value> for T {
    fn into_work_outcome(self) -> TaskOutcome {
        super::state::completion(self)
    }
}
impl<T: Serialize + 'static> sealed::Work<State> for WorkState<T> {}
impl<T: Serialize + 'static> IntoWorkOutcomePart<State> for WorkState<T> {
    fn into_work_outcome(self) -> TaskOutcome {
        self.into_outcome()
    }
}
impl<T: IntoWorkOutcomePart<K>, K> sealed::Work<Fallible<K>> for Result<T, WorkError> {}
impl<T: IntoWorkOutcomePart<K>, K> IntoWorkOutcomePart<Fallible<K>> for Result<T, WorkError> {
    fn into_work_outcome(self) -> TaskOutcome {
        match self {
            Ok(value) => value.into_work_outcome(),
            Err(error) => error.into_outcome(),
        }
    }
}
