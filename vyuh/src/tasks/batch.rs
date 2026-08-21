//! Value-only local batching for durable task handlers.

use serde::{Deserialize, Serialize};

use crate::callables;

use super::{TaskError, TaskOutcome, TaskState};

/// Ordered values supplied to or returned from one local task invocation.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(transparent)]
pub struct Batch<T>(Vec<T>);

impl<T> Batch<T> {
    /// Creates an ordered batch from owned values.
    pub const fn new(values: Vec<T>) -> Self {
        Self(values)
    }

    /// Returns the number of values in the batch.
    pub const fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether the batch contains no values.
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Iterates over the values in durable task order.
    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.0.iter()
    }

    /// Consumes the wrapper and returns its ordered values.
    pub fn into_vec(self) -> Vec<T> {
        self.0
    }
}

impl<T> From<Vec<T>> for Batch<T> {
    fn from(values: Vec<T>) -> Self {
        Self::new(values)
    }
}

impl<T> FromIterator<T> for Batch<T> {
    fn from_iter<I: IntoIterator<Item = T>>(values: I) -> Self {
        Self::new(values.into_iter().collect())
    }
}

impl<T> AsRef<[T]> for Batch<T> {
    fn as_ref(&self) -> &[T] {
        &self.0
    }
}

impl<T> IntoIterator for Batch<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, T> IntoIterator for &'a Batch<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<E: From<TaskError>> callables::IntoOutput<E> for Batch<TaskState> {
    fn into_output(self) -> Result<callables::DataBox, E> {
        let outcomes = self
            .into_iter()
            .map(TaskState::into_outcome)
            .collect::<Vec<_>>();
        Ok(callables::DataBox::new(outcomes))
    }
}

impl callables::IntoReturnPart for Batch<TaskState> {
    fn into_return_part() -> callables::ReturnPart {
        callables::ReturnPart::Empty
    }
}

mod batch_return {
    pub trait Sealed {}
}

/// Internal marker for task-batch handler return forms.
#[doc(hidden)]
pub trait IntoTaskBatchOutcomePart: batch_return::Sealed {
    /// Converts one handler return into exactly one durable outcome per input.
    fn into_task_outcomes(
        data: callables::DataBox,
        expected: usize,
    ) -> Result<Vec<TaskOutcome>, TaskError>;
}

impl batch_return::Sealed for () {}

impl IntoTaskBatchOutcomePart for () {
    fn into_task_outcomes(
        data: callables::DataBox,
        expected: usize,
    ) -> Result<Vec<TaskOutcome>, TaskError> {
        if data.downcast_ref::<()>().is_none() {
            return Err(unsupported_return());
        }
        Ok(vec![TaskOutcome::Complete; expected])
    }
}

impl batch_return::Sealed for TaskState {}

impl IntoTaskBatchOutcomePart for TaskState {
    fn into_task_outcomes(
        data: callables::DataBox,
        expected: usize,
    ) -> Result<Vec<TaskOutcome>, TaskError> {
        let outcome = data
            .downcast_ref::<TaskOutcome>()
            .cloned()
            .ok_or_else(unsupported_return)?;
        Ok(vec![batch_safe(outcome); expected])
    }
}

impl batch_return::Sealed for Batch<TaskState> {}

impl IntoTaskBatchOutcomePart for Batch<TaskState> {
    fn into_task_outcomes(
        data: callables::DataBox,
        expected: usize,
    ) -> Result<Vec<TaskOutcome>, TaskError> {
        let outcomes = data
            .downcast_ref::<Vec<TaskOutcome>>()
            .cloned()
            .ok_or_else(unsupported_return)?;
        if outcomes.len() != expected {
            return Err(TaskError::TaskExecutionError(format!(
                "batch handler returned {} outcomes for {expected} inputs",
                outcomes.len()
            )));
        }
        Ok(outcomes.into_iter().map(batch_safe).collect())
    }
}

impl<T, E> batch_return::Sealed for Result<T, E> where T: IntoTaskBatchOutcomePart {}

impl<T, E> IntoTaskBatchOutcomePart for Result<T, E>
where
    T: IntoTaskBatchOutcomePart,
{
    fn into_task_outcomes(
        data: callables::DataBox,
        expected: usize,
    ) -> Result<Vec<TaskOutcome>, TaskError> {
        T::into_task_outcomes(data, expected)
    }
}

fn batch_safe(outcome: TaskOutcome) -> TaskOutcome {
    match outcome {
        TaskOutcome::Suspend { .. } | TaskOutcome::Sleep { .. } => {
            TaskOutcome::fail("Batch task handlers cannot suspend or sleep")
        }
        outcome => outcome,
    }
}

fn unsupported_return() -> TaskError {
    TaskError::TaskExecutionError("batch handler returned an unsupported task state".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies batch collection and iteration preserve insertion order.
    #[test]
    fn batch_collection_preserves_order() {
        let batch = [1, 2, 3].into_iter().collect::<Batch<_>>();
        assert_eq!(batch.len(), 3);
        assert!(!batch.is_empty());
        assert_eq!(batch.as_ref(), &[1, 2, 3]);
        assert_eq!(batch.into_vec(), vec![1, 2, 3]);
    }
}
