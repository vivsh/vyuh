//! Value-only local batching for durable task handlers.

use serde::{Deserialize, Serialize};

use crate::callables;

use super::{TaskOutcome, TaskRuntimeError, WorkState};

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

mod sealed {
    pub trait Return {}
}

/// Sealed supported return forms for value-only Work batches.
#[doc(hidden)]
pub trait IntoWorkBatchOutcomePart: sealed::Return {
    /// Converts typed items before erasure; never clones application output values.
    fn into_batch_return(self) -> callables::DataBox;
}

impl sealed::Return for () {}
impl IntoWorkBatchOutcomePart for () {
    fn into_batch_return(self) -> callables::DataBox {
        super::returns::erase_outcome(TaskOutcome::Complete)
    }
}
impl<T: Serialize + 'static> sealed::Return for WorkState<T> {}
impl<T: Serialize + 'static> IntoWorkBatchOutcomePart for WorkState<T> {
    fn into_batch_return(self) -> callables::DataBox {
        super::returns::erase_outcome(batch_safe(self.into_outcome()))
    }
}
impl<T: Serialize + 'static> sealed::Return for Batch<WorkState<T>> {}
impl<T: Serialize + 'static> IntoWorkBatchOutcomePart for Batch<WorkState<T>> {
    fn into_batch_return(self) -> callables::DataBox {
        callables::DataBox::new(
            self.into_iter()
                .map(|v| batch_safe(v.into_outcome()))
                .collect::<Vec<_>>(),
        )
    }
}
impl<T: Serialize + 'static> sealed::Return for Batch<Result<WorkState<T>, super::WorkError>> {}
impl<T: Serialize + 'static> IntoWorkBatchOutcomePart
    for Batch<Result<WorkState<T>, super::WorkError>>
{
    fn into_batch_return(self) -> callables::DataBox {
        let outcomes = self
            .into_iter()
            .map(|item| {
                batch_safe(match item {
                    Ok(value) => value.into_outcome(),
                    Err(error) => error.into_outcome(),
                })
            })
            .collect::<Vec<_>>();
        callables::DataBox::new(outcomes)
    }
}
impl<T: IntoWorkBatchOutcomePart> sealed::Return for Result<T, super::WorkError> {}
impl<T: IntoWorkBatchOutcomePart> IntoWorkBatchOutcomePart for Result<T, super::WorkError> {
    fn into_batch_return(self) -> callables::DataBox {
        match self {
            Ok(value) => value.into_batch_return(),
            Err(error) => super::returns::erase_outcome(error.into_outcome()),
        }
    }
}

/// Validates cardinality before any per-task outcome reaches the common commit path.
pub(super) fn resolve_batch(
    data: callables::DataBox,
    expected: usize,
) -> Result<Vec<TaskOutcome>, TaskRuntimeError> {
    if data.downcast_ref::<()>().is_some() {
        return Ok(vec![TaskOutcome::Complete; expected]);
    }
    if let Some(outcome) = data.downcast_ref::<TaskOutcome>() {
        return Ok(vec![outcome.clone(); expected]);
    }
    let shared = data.downcast_arc::<Vec<TaskOutcome>>().ok_or_else(|| {
        TaskRuntimeError::TaskExecutionError("Invalid prepared batch outcome".into())
    })?;
    let outcomes = std::sync::Arc::try_unwrap(shared).unwrap_or_else(|shared| (*shared).clone());
    if outcomes.len() != expected {
        return Err(TaskRuntimeError::TaskExecutionError(format!(
            "batch handler returned {} outcomes for {expected} inputs",
            outcomes.len()
        )));
    }
    Ok(outcomes)
}

fn batch_safe(outcome: TaskOutcome) -> TaskOutcome {
    match outcome {
        TaskOutcome::Suspend { .. }
        | TaskOutcome::Sleep { .. }
        | TaskOutcome::Spawn { .. }
        | TaskOutcome::All { .. } => {
            TaskOutcome::fail("Batch task handlers cannot suspend or sleep or spawn")
        }
        outcome => outcome,
    }
}

#[cfg(test)]
#[path = "tests/batch.rs"]
mod tests;
