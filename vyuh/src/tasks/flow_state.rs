//! Handler-owned lifecycle requests resolved before entering the task store.

use std::time::Duration;

use serde::Serialize;

use crate::callables::{DataBox, DataValue};

use super::{TaskOptions, TaskOutcome, TaskRuntimeError};

/// Orchestration outcome returned by synchronous flow handlers.
///
/// Returning a spawn request checkpoints this task and creates a child atomically.
/// Constructing or discarding a request never submits work.
pub struct FlowState<T = ()> {
    inner: FlowValue<T>,
}

/// Separates a registry-independent child request from a prepared store outcome.
#[expect(
    clippy::large_enum_variant,
    reason = "Keep ordinary outcomes inline rather than adding an allocation to every handler return"
)]
enum FlowValue<T> {
    Complete(T),
    Outcome(TaskOutcome),
    All {
        inputs: Vec<DataBox>,
        state: String,
    },
    Spawn {
        input: DataBox,
        state: String,
        options: TaskOptions,
    },
}

impl<T> FlowState<T> {
    /// Requests an ordered, all-settled group without submitting any work.
    /// Checkpoint serialization can fail here; registration, size, and fan-out
    /// limits are checked during framework preparation after the handler returns.
    pub fn all<I: DataValue, S: Serialize>(
        children: Vec<I>,
        checkpoint: S,
    ) -> Result<Self, TaskRuntimeError> {
        Ok(Self {
            inner: FlowValue::All {
                inputs: children.into_iter().map(DataBox::new_data).collect(),
                state: serde_json::to_string(&checkpoint)?,
            },
        })
    }

    /// Requests one child and suspends this task with a serialized checkpoint.
    /// Returns checkpoint serialization errors. After the handler returns, the runtime
    /// resolves and serializes the registered child; preparation errors fail this task.
    pub fn spawn<I: DataValue, S: Serialize>(input: I, state: S) -> Result<Self, TaskRuntimeError> {
        Self::spawn_with(input, state, TaskOptions::new())
    }

    /// Requests one child with explicit scheduling options, without submitting it.
    /// Invalid options and checkpoint serialization fail immediately. Child registration,
    /// serialization, and configured size limits are checked after the handler returns.
    /// `ignore_conflicts` is invalid: a spawn cannot adopt an independent task.
    pub fn spawn_with<I: DataValue, S: Serialize>(
        input: I,
        state: S,
        options: TaskOptions,
    ) -> Result<Self, TaskRuntimeError> {
        super::dispatcher::validate_spawn_options(&options)?;
        Ok(Self {
            inner: FlowValue::Spawn {
                input: DataBox::new_data(input),
                state: serde_json::to_string(&state)?,
                options,
            },
        })
    }

    /// Completes with a persisted value, also delivered to an awaiting parent.
    /// Serialization and the 32 KiB limit are validated after return; invalid output
    /// terminally fails the task without affecting other outcomes in the flush.
    pub fn complete(output: T) -> Self {
        Self {
            inner: FlowValue::Complete(output),
        }
    }

    /// Suspends a task with durable continuation state; returns serialization errors.
    pub fn suspend<S: Serialize>(state: S) -> Result<Self, TaskRuntimeError> {
        Ok(Self::from_outcome(TaskOutcome::Suspend {
            state: serde_json::to_string(&state)?,
        }))
    }

    /// Sleeps until the supplied delay with a checkpoint; returns serialization errors.
    pub fn sleep<S: Serialize>(state: S, delay: Duration) -> Result<Self, TaskRuntimeError> {
        Ok(Self::from_outcome(TaskOutcome::Sleep {
            state: serde_json::to_string(&state)?,
            delay,
        }))
    }

    pub(super) fn from_outcome(outcome: TaskOutcome) -> Self {
        Self {
            inner: FlowValue::Outcome(outcome),
        }
    }
}

impl<T: Serialize + 'static> FlowState<T> {
    /// Serializes successful values before erasure, retaining only unresolved child intent.
    pub(super) fn prepare(self) -> FlowState {
        let inner = match self.inner {
            FlowValue::Complete(output) => FlowValue::Outcome(super::state::completion(output)),
            FlowValue::Outcome(outcome) => FlowValue::Outcome(outcome),
            FlowValue::All { inputs, state } => FlowValue::All { inputs, state },
            FlowValue::Spawn {
                input,
                state,
                options,
            } => FlowValue::Spawn {
                input,
                state,
                options,
            },
        };
        FlowState { inner }
    }
}

impl FlowState {
    /// Erases ordinary outcomes directly, preserving the small unit-return fast path.
    pub(super) fn erase(self) -> DataBox {
        match self.inner {
            FlowValue::Complete(()) => super::returns::erase_outcome(TaskOutcome::Complete),
            FlowValue::Outcome(outcome) => super::returns::erase_outcome(outcome),
            _ => DataBox::new(self),
        }
    }

    /// Consumes prepared output bytes; only unresolved child preparation borrows its payload.
    pub(super) fn resolve_owned(self, site: &crate::Site) -> Result<TaskOutcome, TaskRuntimeError> {
        match self.inner {
            FlowValue::Complete(()) => Ok(TaskOutcome::Complete),
            FlowValue::Outcome(outcome) => Ok(outcome),
            FlowValue::All { inputs, state } => site.tasks().prepare_all(&inputs, &state),
            FlowValue::Spawn {
                input,
                state,
                options,
            } => site.tasks().prepare_child(&input, &state, &options),
        }
    }

    /// Resolves requests only against the site executing the parent handler.
    #[cfg(test)]
    pub(super) fn resolve(&self, site: &crate::Site) -> Result<TaskOutcome, TaskRuntimeError> {
        match &self.inner {
            FlowValue::Complete(()) => Ok(TaskOutcome::Complete),
            FlowValue::Outcome(outcome) => Ok(outcome.clone()),
            FlowValue::All { inputs, state } => site.tasks().prepare_all(inputs, state),
            FlowValue::Spawn {
                input,
                state,
                options,
            } => site.tasks().prepare_child(input, state, options),
        }
    }
}
