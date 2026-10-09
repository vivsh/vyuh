//! Prepared lifecycle outcomes and bounded store-acceptance normalization.
use super::TaskRuntimeError;
use serde::Serialize;
use std::time::Duration;

/// Prepared lifecycle outcome committed by Vyuh's internal task store.
///
/// Work returns [`super::WorkState`] and Flow returns [`super::FlowState`], not this store contract.
#[derive(Debug, Clone)]
pub enum TaskOutcome {
    /// Suspends this flow until every atomically created child becomes terminal.
    All {
        state: String,
        children: Vec<super::TaskWrite>,
    },
    /// Marks the task as successfully completed.
    Complete,
    /// Completes with a serialized success value, retained and delivered to the parent.
    CompleteWith { output: String },
    /// Suspends this task and atomically creates one child.
    Spawn {
        state: String,
        child: super::TaskWrite,
    },
    /// Stores continuation state until an explicit resume.
    Suspend { state: String },
    /// Stores continuation state and schedules another execution.
    Sleep { state: String, delay: Duration },
    /// Schedules a retry and records its error.
    Retry { error: String },
    /// Marks the task as terminally failed.
    Fail { error: String },
}

impl TaskOutcome {
    /// Completes a task, delivering a unit success to an awaiting parent.
    pub const fn complete() -> Self {
        Self::Complete
    }

    /// Suspends a task with durable continuation state.
    pub fn suspend<S: Serialize>(state: &S) -> Result<Self, TaskRuntimeError> {
        Ok(Self::Suspend {
            state: serde_json::to_string(state)?,
        })
    }

    /// Sleeps a task until the supplied delay with durable continuation state.
    pub fn sleep<S: Serialize>(state: &S, delay: Duration) -> Result<Self, TaskRuntimeError> {
        Ok(Self::Sleep {
            state: serde_json::to_string(state)?,
            delay,
        })
    }

    /// Retries a task using its lane's exponential-backoff policy.
    pub fn retry(error: impl Into<String>) -> Self {
        Self::Retry {
            error: error.into(),
        }
    }

    /// Fails a task with a safe stored error message.
    pub fn fail(error: impl Into<String>) -> Self {
        Self::Fail {
            error: error.into(),
        }
    }

    pub(crate) fn handler_failed() -> Self {
        Self::fail("Task handler failed")
    }
}

/// Bounds handler-controlled lifecycle data before it reaches any store.
pub(crate) fn normalize_outcome(
    outcome: TaskOutcome,
    payload_limit: usize,
    error_limit: usize,
) -> TaskOutcome {
    match outcome {
        TaskOutcome::Spawn { ref state, .. } | TaskOutcome::All { ref state, .. }
            if state.len() > payload_limit =>
        {
            TaskOutcome::fail("Task continuation state exceeded the configured limit")
        }
        TaskOutcome::CompleteWith { ref output }
            if crate::tasks::result::validate_output(output).is_err() =>
        {
            TaskOutcome::fail(
                "Task output was invalid JSON or exceeded the 32768-byte result limit",
            )
        }
        TaskOutcome::Suspend { state } if state.len() > payload_limit => {
            TaskOutcome::fail("Task continuation state exceeded the configured limit")
        }
        TaskOutcome::Sleep { state, .. } if state.len() > payload_limit => {
            TaskOutcome::fail("Task continuation state exceeded the configured limit")
        }
        TaskOutcome::Sleep { delay, .. } if delay > crate::tasks::config::MAX_TASK_DELAY => {
            TaskOutcome::fail("Task sleep duration exceeded the configured limit")
        }
        TaskOutcome::Retry { error } => TaskOutcome::Retry {
            error: truncate_utf8(error, error_limit),
        },
        TaskOutcome::Fail { error } => TaskOutcome::Fail {
            error: truncate_utf8(error, error_limit),
        },
        other => other,
    }
}

pub(crate) fn truncate_utf8(mut value: String, limit: usize) -> String {
    if value.len() <= limit {
        return value;
    }
    let mut end = limit;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
    value
}

/// Serializes before erasure; no Clone, Sync or Deserialize bound is required.
pub(in crate::tasks) fn completion<T: Serialize + 'static>(output: T) -> TaskOutcome {
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
    crate::tasks::result::validate_output(&output)?;
    Ok(if output == "null" {
        TaskOutcome::Complete
    } else {
        TaskOutcome::CompleteWith { output }
    })
}

/// Keeps the existing tiny unit-return allocation rather than boxing a full outcome enum.
pub(in crate::tasks) fn erase_outcome(outcome: TaskOutcome) -> crate::callables::DataBox {
    match outcome {
        TaskOutcome::Complete => crate::callables::DataBox::new(()),
        outcome => crate::callables::DataBox::new(outcome),
    }
}
