//! Explicit handler decisions, separate from failures of the task runtime.

use super::{TaskFailure, TaskOutcome, TaskRuntimeError};

/// A Work handler's retry or terminal failure decision. Deliberately not serializable.
#[derive(Debug, thiserror::Error)]
pub enum TaskError {
    /// Requests the lane's normal retry/backoff policy.
    #[error("{0}")]
    Retry(TaskFailure),
    /// Requests terminal failure.
    #[error("{0}")]
    Fail(TaskFailure),
}

impl TaskError {
    /// Requests retry with application-safe diagnostic text that will be persisted.
    pub fn retry(message: impl Into<String>) -> Self {
        Self::Retry(TaskFailure::new(None, message))
    }

    /// Fails terminally with application-safe diagnostic text that will be persisted.
    pub fn fail(message: impl Into<String>) -> Self {
        Self::Fail(TaskFailure::new(None, message))
    }

    pub(super) fn into_outcome(self) -> TaskOutcome {
        match self {
            Self::Retry(failure) => TaskOutcome::retry(failure.into_message()),
            Self::Fail(failure) => TaskOutcome::fail(failure.into_message()),
        }
    }
}

/// A Flow handler's terminal failure decision; Flow cannot request retries.
#[derive(Debug, thiserror::Error)]
#[error("{failure}")]
pub struct FlowError {
    failure: TaskFailure,
}

impl FlowError {
    /// Fails terminally with application-safe diagnostic text that will be persisted.
    pub fn fail(message: impl Into<String>) -> Self {
        Self {
            failure: TaskFailure::new(None, message),
        }
    }

    pub(super) fn into_outcome(self) -> TaskOutcome {
        TaskOutcome::fail(self.failure.into_message())
    }
}

macro_rules! terminal_conversions {
    ($target:ty, $construct:expr) => {
        impl From<TaskFailure> for $target {
            fn from(failure: TaskFailure) -> Self { ($construct)(failure) }
        }
        impl From<TaskRuntimeError> for $target {
            fn from(error: TaskRuntimeError) -> Self {
                tracing::error!(error = %error, "task runtime operation failed in handler");
                Self::fail("Task operation failed")
            }
        }
        impl From<crate::Error> for $target {
            fn from(error: crate::Error) -> Self {
                tracing::error!(error = %error, "task handler operation failed");
                Self::fail("Task handler failed")
            }
        }
    };
}

terminal_conversions!(TaskError, TaskError::Fail);
terminal_conversions!(FlowError, |failure| FlowError { failure });
