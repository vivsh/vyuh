//! Typed Work completion and external-suspension requests.

use crate::tasks::outcome::completion;
use crate::tasks::{TaskOutcome, TaskRuntimeError};
use serde::Serialize;

/// Successful output or suspension returned by an asynchronous Work handler.
/// Retry and failure decisions use WorkError instead.
pub struct WorkState<T = ()> {
    inner: WorkValue<T>,
}

enum WorkValue<T> {
    Complete(T),
    Suspend(String),
}

impl<T> WorkState<T> {
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

impl<T: Serialize + 'static> WorkState<T> {
    pub(in crate::tasks) fn into_outcome(self) -> TaskOutcome {
        match self.inner {
            WorkValue::Complete(output) => completion(output),
            WorkValue::Suspend(state) => TaskOutcome::Suspend { state },
        }
    }
}
