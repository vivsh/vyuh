//! Task registration, submission, listing, and persisted-record value types.

use std::time::Duration;

use serde::Serialize;

use super::{TaskError, TaskStatus};

#[derive(Debug, Default, Clone, Serialize)]
/// Registration metadata for one task handler.
pub struct TaskHandlerConf {
    pub name: String,
}

impl TaskHandlerConf {
    /// Creates handler registration metadata with one stable task name.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

#[derive(Debug, Default, Clone)]
/// Submission policy applied when a task record is created.
pub struct TaskOptions {
    pub initial_delay: Option<Duration>,
    pub retry_delay: Option<Duration>,
    pub lease_duration: Option<Duration>,
    pub identity: Option<String>,
    pub max_attempts: Option<i32>,
    pub state: Option<String>,
    pub priority: i32,
}

#[derive(Debug, Clone)]
/// Filters and pagination bounds for listing persisted tasks.
pub struct TaskListFilter {
    pub status: Option<TaskStatus>,
    pub name: Option<String>,
    pub priority_min: Option<i32>,
    pub identity: Option<String>,
    pub created_from: Option<chrono::DateTime<chrono::Utc>>,
    pub created_to: Option<chrono::DateTime<chrono::Utc>>,
    pub q: Option<String>,
    pub limit: usize,
    pub offset: usize,
}

impl Default for TaskListFilter {
    fn default() -> Self {
        Self {
            status: None,
            name: None,
            priority_min: None,
            identity: None,
            created_from: None,
            created_to: None,
            q: None,
            limit: 50,
            offset: 0,
        }
    }
}

#[derive(Debug, Clone)]
/// One page of persisted task records and its continuation cursor.
pub struct TaskListPage {
    pub records: Vec<TaskRecord>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone)]
/// Persisted task state exposed through task inspection APIs.
pub struct TaskRecord {
    pub id: uuid::Uuid,
    pub name: String,
    pub input: String,
    pub state: Option<String>,
    pub resume_input: Option<String>,
    pub output: Option<String>,
    pub result: Option<String>,
    pub status: TaskStatus,
    pub attempts: i32,
    pub priority: i32,
    pub max_attempts: Option<i32>,
    pub retry_delay_ms: Option<i64>,
    pub lease_duration_ms: Option<i64>,
    pub last_error: Option<String>,
    pub identity: Option<String>,
    pub locked_by: Option<String>,
    pub leased_until: Option<chrono::DateTime<chrono::Utc>>,
    pub ready_at: Option<chrono::DateTime<chrono::Utc>>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl TaskRecord {
    /// Returns the registered task handler name stored with this record.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Deserializes the submitted task input into its application type.
    pub fn input<T>(&self) -> Result<T, TaskError>
    where
        T: serde::de::DeserializeOwned,
    {
        serde_json::from_str(&self.input).map_err(TaskError::from)
    }

    /// Deserializes suspended task state when one is present.
    pub fn state<T>(&self) -> Result<Option<T>, TaskError>
    where
        T: serde::de::DeserializeOwned,
    {
        self.state
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(TaskError::from)
    }

    /// Deserializes the payload supplied when a suspended task resumed.
    pub fn resume_input<T>(&self) -> Result<Option<T>, TaskError>
    where
        T: serde::de::DeserializeOwned,
    {
        self.resume_input
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(TaskError::from)
    }
}

#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
pub(crate) fn sort_claimed_tasks(tasks: &mut [TaskRecord]) {
    tasks.sort_by_key(|task| {
        (
            std::cmp::Reverse(task.priority),
            task.ready_at.unwrap_or(task.created_at),
            task.created_at,
        )
    });
}
