//! Durable failures retained in task results and delivered to continuations.

use serde::{Deserialize, Serialize};

use super::TaskId;

/// A safe, serializable failure recorded by a task or supplied by an external resumer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("{message}")]
pub struct TaskFailure {
    task_id: Option<TaskId>,
    message: String,
}

impl TaskFailure {
    /// Creates a failure. Supply only application-safe diagnostic text.
    pub fn new(task_id: Option<TaskId>, message: impl Into<String>) -> Self {
        Self {
            task_id,
            message: message.into(),
        }
    }

    /// Returns the originating task; externally supplied failures may omit it.
    pub const fn task_id(&self) -> Option<&TaskId> {
        self.task_id.as_ref()
    }

    /// Returns the safe diagnostic message.
    pub fn message(&self) -> &str {
        &self.message
    }

    pub(crate) fn bounded(self, limit: usize) -> Self {
        Self {
            task_id: self.task_id,
            message: super::store::truncate_utf8(self.message, limit),
        }
    }
}

/// Failure produced while configuring, submitting, storing, or executing a task.
#[derive(Debug, thiserror::Error)]
pub enum TaskError {
    /// The complete JSON result envelope exceeded the fixed storage limit.
    #[error("Task result is {actual} bytes; the limit is {limit} bytes")]
    ResultTooLarge { actual: usize, limit: usize },

    #[error("Type mismatch: expected {0}, got {1}")]
    TypeMismatch(String, String),

    #[error("Task '{0}' not found")]
    TaskNotFound(String),

    #[error("Task JSON error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Task execution error: {0}")]
    TaskExecutionError(String),

    #[error("Task already exists: {0}")]
    AlreadyExists(String),

    #[error("Invalid task configuration: {0}")]
    InvalidConfig(String),

    #[error("Invalid task submission options: {0}")]
    InvalidOptions(String),

    #[error("Task lane '{0}' is not configured")]
    UnknownLane(String),

    #[error("Idempotency key conflicts with task {0}")]
    IdempotencyConflict(super::TaskId),

    #[error("Task lane '{0}' still has running work")]
    LaneBusy(String),

    #[error(transparent)]
    CallError(#[from] crate::callables::CallError),

    #[error("Database error: {0}")]
    DatabaseError(#[from] crate::db::sqlx::Error),

    #[error(transparent)]
    StoreError(#[from] crate::db::DbError),
}

/// Durable lifecycle state exposed by task inspection APIs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[repr(i16)]
#[serde(rename_all = "lowercase")]
pub enum TaskStatus {
    Pending = 0,
    Running = 1,
    Suspended = 2,
    Succeeded = 3,
    Failed = 4,
}

impl TaskStatus {
    #[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
    pub(crate) const fn as_i16(self) -> i16 {
        self as i16
    }

    /// Returns the stable lowercase status name used in diagnostics and metrics.
    pub fn as_str(self) -> &'static str {
        match self {
            TaskStatus::Pending => "pending",
            TaskStatus::Running => "running",
            TaskStatus::Suspended => "suspended",
            TaskStatus::Succeeded => "succeeded",
            TaskStatus::Failed => "failed",
        }
    }

    /// Converts the persisted status representation into a task status.
    ///
    /// Invalid values indicate a corrupted or incompatible task row and are
    /// returned as a structured task error.
    #[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
    pub(crate) fn from_i16(value: i16) -> Result<Self, TaskError> {
        match value {
            0 => Ok(Self::Pending),
            1 => Ok(Self::Running),
            2 => Ok(Self::Suspended),
            3 => Ok(Self::Succeeded),
            4 => Ok(Self::Failed),
            _ => Err(TaskError::TaskExecutionError(format!(
                "invalid task status value {value}"
            ))),
        }
    }
}
