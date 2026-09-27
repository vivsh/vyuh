//! Typed Work completion and external-suspension requests.

use super::{TaskOutcome, TaskRuntimeError};
use serde::Serialize;

/// Successful output or suspension returned by an asynchronous Work handler.
/// Retry and failure decisions use TaskError instead.
pub struct TaskState<T = ()> {
    inner: WorkValue<T>,
}

enum WorkValue<T> {
    Complete(T),
    Suspend(String),
}

impl<T> TaskState<T> {
    /// Retains output until framework conversion. Serialization and the 32 KiB
    /// envelope limit are checked after return; failures terminally fail this task.
    pub fn complete(output: T) -> Self {
        Self {
            inner: WorkValue::Complete(output),
        }
    }

    /// Suspends with a checkpoint, returning checkpoint serialization errors.
    /// External resume must follow the commit; early responses are not buffered.
    pub fn suspend<S: Serialize>(state: S) -> Result<Self, TaskRuntimeError> {
        Ok(Self {
            inner: WorkValue::Suspend(serde_json::to_string(&state)?),
        })
    }
}

impl<T: Serialize + 'static> TaskState<T> {
    pub(super) fn into_outcome(self) -> TaskOutcome {
        match self.inner {
            WorkValue::Complete(output) => completion(output),
            WorkValue::Suspend(state) => TaskOutcome::Suspend { state },
        }
    }
}

/// Serializes before erasure; no Clone, Sync or Deserialize bound is required.
pub(super) fn completion<T: Serialize + 'static>(output: T) -> TaskOutcome {
    match complete_output(output) {
        Ok(outcome) => outcome,
        Err(error) => {
            tracing::error!(error = %error, "task output conversion failed");
            match error {
                TaskRuntimeError::ResultTooLarge { .. } => {
                    TaskOutcome::fail("Task result exceeds 32768-byte limit")
                }
                _ => TaskOutcome::fail("Task result serialization failed"),
            }
        }
    }
}

/// Encodes completion once, preserving the allocation-free direct-unit fast path.
fn complete_output<T: Serialize + 'static>(output: T) -> Result<TaskOutcome, TaskRuntimeError> {
    if std::any::TypeId::of::<T>() == std::any::TypeId::of::<()>() {
        return Ok(TaskOutcome::Complete);
    }
    let output = serde_json::to_string(&output)?;
    super::result::validate_output(&output)?;
    Ok(if output == "null" {
        TaskOutcome::Complete
    } else {
        TaskOutcome::CompleteWith { output }
    })
}

#[cfg(test)]
#[path = "tests/state.rs"]
mod tests;
