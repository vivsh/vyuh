//! Handler-owned lifecycle requests resolved before entering the task store.

use std::time::Duration;

use serde::Serialize;

use crate::callables::{self, DataBox, DataValue};

use super::{TaskError, TaskOptions, TaskOutcome};

/// Lifecycle control returned by task handlers.
///
/// Returning a spawn request checkpoints this task and creates a child atomically.
/// Constructing or discarding a request never submits work.
pub struct TaskState {
    inner: StateValue,
}

/// Separates a registry-independent child request from a prepared store outcome.
#[expect(
    clippy::large_enum_variant,
    reason = "Keep ordinary outcomes inline rather than adding an allocation to every handler return"
)]
enum StateValue {
    Outcome(TaskOutcome),
    Spawn {
        input: DataBox,
        state: String,
        options: TaskOptions,
    },
}

impl TaskState {
    /// Requests one child and suspends this task with a serialized checkpoint.
    /// Returns checkpoint serialization errors. After the handler returns, the runtime
    /// resolves and serializes the registered child; preparation errors fail this task.
    pub fn spawn<T: DataValue, S: Serialize>(input: T, state: S) -> Result<Self, TaskError> {
        Self::spawn_with(input, state, TaskOptions::new())
    }

    /// Requests one child with explicit scheduling options, without submitting it.
    /// Invalid options and checkpoint serialization fail immediately. Child registration,
    /// serialization, and configured size limits are checked after the handler returns.
    /// `ignore_conflicts` is invalid: a spawn cannot adopt an independent task.
    pub fn spawn_with<T: DataValue, S: Serialize>(
        input: T,
        state: S,
        options: TaskOptions,
    ) -> Result<Self, TaskError> {
        super::dispatcher::validate_spawn_options(&options)?;
        Ok(Self {
            inner: StateValue::Spawn {
                input: DataBox::new_data(input),
                state: serde_json::to_string(&state)?,
                options,
            },
        })
    }

    /// Completes with a persisted value, also delivered to an awaiting parent.
    /// Returns serialization or size errors if its JSON envelope exceeds 32 KiB.
    pub fn complete<T: Serialize>(output: T) -> Result<Self, TaskError> {
        let output = serde_json::to_string(&output)?;
        super::result::validate_output(&output)?;
        Ok(Self::from_outcome(if output == "null" {
            TaskOutcome::Complete
        } else {
            TaskOutcome::CompleteWith { output }
        }))
    }

    /// Suspends a task with durable continuation state; returns serialization errors.
    pub fn suspend<S: Serialize>(state: S) -> Result<Self, TaskError> {
        Ok(Self::from_outcome(TaskOutcome::Suspend {
            state: serde_json::to_string(&state)?,
        }))
    }

    /// Sleeps until the supplied delay with a checkpoint; returns serialization errors.
    pub fn sleep<S: Serialize>(state: S, delay: Duration) -> Result<Self, TaskError> {
        Ok(Self::from_outcome(TaskOutcome::Sleep {
            state: serde_json::to_string(&state)?,
            delay,
        }))
    }

    /// Requests another attempt under the selected task lane's retry policy.
    pub fn retry(error: impl Into<String>) -> Self {
        Self::from_outcome(TaskOutcome::Retry {
            error: error.into(),
        })
    }

    /// Fails a task with a safe stored error message.
    pub fn fail(error: impl Into<String>) -> Self {
        Self::from_outcome(TaskOutcome::Fail {
            error: error.into(),
        })
    }

    fn from_outcome(outcome: TaskOutcome) -> Self {
        Self {
            inner: StateValue::Outcome(outcome),
        }
    }

    /// Resolves requests only against the site executing the parent handler.
    pub(super) fn resolve(&self, site: &crate::Site) -> Result<TaskOutcome, TaskError> {
        match &self.inner {
            StateValue::Outcome(outcome) => Ok(outcome.clone()),
            StateValue::Spawn {
                input,
                state,
                options,
            } => site.tasks().prepare_child(input, state, options),
        }
    }

    /// Converts uniform batch returns without resolving or preparing child requests.
    pub(super) fn batch_outcome(&self) -> TaskOutcome {
        match &self.inner {
            StateValue::Outcome(outcome) => outcome.clone(),
            StateValue::Spawn { .. } => rejected_batch_spawn(),
        }
    }

    /// Consumes ordered batch returns without resolving or preparing child requests.
    pub(super) fn into_outcome(self) -> TaskOutcome {
        match self.inner {
            StateValue::Outcome(outcome) => outcome,
            StateValue::Spawn { .. } => rejected_batch_spawn(),
        }
    }
}

fn rejected_batch_spawn() -> TaskOutcome {
    TaskOutcome::fail("Batch task handlers cannot suspend or sleep or spawn")
}

impl<E: From<TaskError>> callables::IntoOutput<E> for TaskState {
    fn into_output(self) -> Result<DataBox, E> {
        Ok(DataBox::new(self))
    }
}

impl callables::IntoReturnPart for TaskState {
    fn into_return_part() -> callables::ReturnPart {
        callables::ReturnPart::Empty
    }
}

#[cfg(test)]
#[path = "tests/state.rs"]
mod tests;
