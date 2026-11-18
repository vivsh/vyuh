//! Typed Work completion, sleep, and external-suspension requests.

use crate::tasks::outcome::completion;
use crate::tasks::{TaskOutcome, TaskRuntimeError};
use serde::Serialize;
use std::time::Duration;

/// Successful output, sleep, or suspension returned by an asynchronous Work handler.
/// Retry and failure decisions use WorkError instead.
pub struct WorkState<T = ()> {
    inner: WorkValue<T>,
}

enum WorkValue<T> {
    Complete(T),
    Suspend(String),
    Wait { topic: String, state: String },
    Sleep { state: String, delay: Duration },
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

    /// Checkpoints and waits once for a topic publication. Invalid topics and
    /// serialization errors are returned; early publications are not retained.
    pub fn wait(topic: &str, state: impl Serialize) -> Result<Self, TaskRuntimeError> {
        crate::tasks::publication::validate(topic)?;
        Ok(Self {
            inner: WorkValue::Wait {
                topic: topic.to_owned(),
                state: serde_json::to_string(&state)?,
            },
        })
    }

    /// Saves a checkpoint and schedules a later invocation, without requesting a retry.
    /// Returns serialization errors; the store validates checkpoint size and delay.
    /// Single-item Work only; batch handlers reject sleep.
    pub fn sleep<S: Serialize>(state: S, delay: Duration) -> Result<Self, TaskRuntimeError> {
        Ok(Self {
            inner: WorkValue::Sleep {
                state: serde_json::to_string(&state)?,
                delay,
            },
        })
    }
}

impl<T: Serialize + 'static> WorkState<T> {
    pub(in crate::tasks) fn into_outcome(self) -> TaskOutcome {
        match self.inner {
            WorkValue::Complete(output) => completion(output),
            WorkValue::Suspend(state) => TaskOutcome::Suspend { state },
            WorkValue::Wait { topic, state } => TaskOutcome::Wait { topic, state },
            WorkValue::Sleep { state, delay } => TaskOutcome::Sleep { state, delay },
        }
    }
}

#[cfg(test)]
#[path = "tests/state.rs"]
mod tests;
