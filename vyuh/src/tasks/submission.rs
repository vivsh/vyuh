//! Task submission options, persistence requests, and receipts.

use std::{fmt, sync::Arc, time::Duration};

use serde::Serialize;

use super::{DEFAULT_TASK_LANE, TaskError, TaskId, TaskLane, TaskRecord};

type KeyFn<T> = Arc<dyn Fn(&T) -> String + Send + Sync + 'static>;

/// Submission policy evaluated before one or more task records are stored.
pub struct TaskOptions<T> {
    pub(crate) initial_delay: Option<Duration>,
    pub(crate) lane: TaskLane,
    pub(crate) key: Option<KeyFn<T>>,
    pub(crate) ignore_conflicts: bool,
}

impl<T> TaskOptions<T> {
    /// Creates the default submission policy.
    pub fn new() -> Self {
        Self::default()
    }

    /// Delays the first execution attempt.
    pub const fn delay(mut self, delay: Duration) -> Self {
        self.initial_delay = Some(delay);
        self
    }

    /// Selects the named execution lane.
    pub const fn lane(mut self, lane: TaskLane) -> Self {
        self.lane = lane;
        self
    }

    /// Derives one idempotency key from each typed input.
    pub fn idempotency_key(mut self, key: impl Fn(&T) -> String + Send + Sync + 'static) -> Self {
        self.key = Some(Arc::new(key));
        self
    }

    /// Keeps non-conflicting batch entries when an idempotency key is reused.
    pub const fn ignore_conflicts(mut self) -> Self {
        self.ignore_conflicts = true;
        self
    }

    pub(crate) fn key_for(&self, input: &T) -> Result<Option<String>, TaskError> {
        let Some(key) = &self.key else {
            return Ok(None);
        };
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| key(input)))
            .map(Some)
            .map_err(|_| TaskError::InvalidOptions("task idempotency callback panicked".into()))
    }
}

pub(crate) fn canonical_json<T: Serialize>(input: &T) -> Result<String, TaskError> {
    let value = serde_json::to_value(input)?;
    serde_json::to_string(&canonical_value(value)).map_err(TaskError::from)
}

/// Recursively sorts object keys while retaining array order and scalar values.
fn canonical_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonical_value).collect())
        }
        serde_json::Value::Object(values) => {
            let mut sorted = values.into_iter().collect::<Vec<_>>();
            sorted.sort_by(|left, right| left.0.cmp(&right.0));
            serde_json::Value::Object(
                sorted
                    .into_iter()
                    .map(|(key, value)| (key, canonical_value(value)))
                    .collect(),
            )
        }
        other => other,
    }
}

impl<T> Default for TaskOptions<T> {
    fn default() -> Self {
        Self {
            initial_delay: None,
            lane: DEFAULT_TASK_LANE,
            key: None,
            ignore_conflicts: false,
        }
    }
}

impl<T> Clone for TaskOptions<T> {
    fn clone(&self) -> Self {
        Self {
            initial_delay: self.initial_delay,
            lane: self.lane,
            key: self.key.clone(),
            ignore_conflicts: self.ignore_conflicts,
        }
    }
}

impl<T> fmt::Debug for TaskOptions<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaskOptions")
            .field("initial_delay", &self.initial_delay)
            .field("lane", &self.lane)
            .field("has_idempotency_key", &self.key.is_some())
            .field("ignore_conflicts", &self.ignore_conflicts)
            .finish_non_exhaustive()
    }
}

/// Result of submitting one task intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskReceipt {
    /// A new task was durably stored.
    Queued(TaskId),
    /// An identical idempotent task already exists.
    Existing(TaskId),
    /// A conflicting idempotent task was deliberately retained.
    Ignored(TaskId),
}

impl TaskReceipt {
    /// Returns the new or existing task identifier.
    pub const fn id(self) -> TaskId {
        match self {
            Self::Queued(id) | Self::Existing(id) | Self::Ignored(id) => id,
        }
    }
}

/// Store-facing task intent with resolved conflict behavior.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct TaskWrite {
    /// Fully normalized task row waiting for store-relative timestamps.
    pub record: TaskRecord,
    /// Whether an intent mismatch should retain the existing task.
    pub ignore_conflicts: bool,
    /// Store-relative delay before the task first becomes eligible.
    pub initial_delay: Option<Duration>,
}
