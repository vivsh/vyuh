//! Task registration, submission, listing, and persisted-record value types.

use std::{any::Any, sync::Arc};

use serde::{Deserialize, Serialize};

use super::{DEFAULT_TASK_LANE, TaskError, TaskLane, TaskStatus};

/// Canonical identifier for one durable task execution.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    schemars::JsonSchema,
)]
#[serde(transparent)]
pub struct TaskId(#[schemars(with = "String")] uuid::Uuid);

impl TaskId {
    pub(crate) const fn new(id: uuid::Uuid) -> Self {
        Self(id)
    }

    #[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
    pub(crate) const fn into_uuid(self) -> uuid::Uuid {
        self.0
    }
}

impl std::fmt::Display for TaskId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::str::FromStr for TaskId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse().map(Self)
    }
}

/// Immutable idempotency policy for one typed task definition.
pub struct TaskIdempotency<T> {
    revision: &'static str,
    key: fn(&T) -> String,
}

impl<T> Copy for TaskIdempotency<T> {}

impl<T> Clone for TaskIdempotency<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> TaskIdempotency<T> {
    /// Creates a stable typed idempotency key rule for one task definition.
    pub const fn new(revision: &'static str, key: fn(&T) -> String) -> Self {
        Self { revision, key }
    }

    pub(crate) const fn policy(self) -> IdempotencyPolicy {
        IdempotencyPolicy {
            revision: self.revision,
        }
    }

    pub(crate) fn key_for(self, input: &T) -> Result<String, TaskError> {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (self.key)(input)))
            .map_err(|_| TaskError::InvalidOptions("task idempotency callback panicked".into()))
    }
}

impl<T> std::fmt::Debug for TaskIdempotency<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TaskIdempotency")
            .field("revision", &self.revision)
            .finish_non_exhaustive()
    }
}

/// Immutable registration definition for one typed durable task.
#[derive(Clone)]
pub struct TaskDefinition<T> {
    name: String,
    lane: TaskLane,
    idempotency: Option<TaskIdempotency<T>>,
}

impl<T> TaskDefinition<T> {
    /// Creates a task definition with the framework default lane.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            lane: DEFAULT_TASK_LANE,
            idempotency: None,
        }
    }

    /// Declares the lane requested by every execution of this task.
    pub const fn lane(mut self, lane: TaskLane) -> Self {
        self.lane = lane;
        self
    }

    /// Declares deterministic idempotency for every execution of this task.
    pub const fn idempotency(mut self, idempotency: TaskIdempotency<T>) -> Self {
        self.idempotency = Some(idempotency);
        self
    }

    pub(crate) fn into_parts(self) -> (String, TaskDefinitionPolicy<T>) {
        (
            self.name,
            TaskDefinitionPolicy {
                declared_lane: self.lane,
                effective_lane: self.lane,
                idempotency: self.idempotency,
            },
        )
    }
}

impl<T> std::fmt::Debug for TaskDefinition<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TaskDefinition")
            .field("name", &self.name)
            .field("lane", &self.lane)
            .field("idempotency", &self.idempotency)
            .finish()
    }
}

/// Stable store-visible metadata for one idempotency rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IdempotencyPolicy {
    pub(crate) revision: &'static str,
}

/// Typed execution policy before task registration erases its input type.
#[derive(Clone)]
pub(crate) struct TaskDefinitionPolicy<T> {
    pub(crate) declared_lane: TaskLane,
    pub(crate) effective_lane: TaskLane,
    pub(crate) idempotency: Option<TaskIdempotency<T>>,
}

type KeyFn = Arc<dyn Fn(&dyn Any) -> Result<String, TaskError> + Send + Sync + 'static>;

/// Resolved immutable execution policy for a registered task.
#[derive(Clone)]
pub(crate) struct TaskPolicy {
    pub(crate) declared_lane: TaskLane,
    pub(crate) effective_lane: TaskLane,
    pub(crate) idempotency: Option<IdempotencyPolicy>,
    key: Option<KeyFn>,
}

impl<T: 'static> TaskDefinitionPolicy<T> {
    pub(crate) fn erase(self) -> TaskPolicy {
        let idempotency = self.idempotency;
        let key = idempotency.map(erased_key::<T>);
        TaskPolicy {
            declared_lane: self.declared_lane,
            effective_lane: self.effective_lane,
            idempotency: idempotency.map(TaskIdempotency::policy),
            key,
        }
    }
}

impl TaskPolicy {
    pub(crate) fn key_for<T: 'static>(&self, input: &T) -> Result<Option<String>, TaskError> {
        self.key.as_ref().map(|key| key(input)).transpose()
    }

    /// Resolves an idempotency key from a type-erased registered task payload.
    pub(crate) fn key_for_box(&self, input: &dyn Any) -> Result<Option<String>, TaskError> {
        self.key.as_ref().map(|key| key(input)).transpose()
    }
}

/// Erases one statically typed idempotency function at task registration.
fn erased_key<T: 'static>(policy: TaskIdempotency<T>) -> KeyFn {
    Arc::new(move |input| {
        input
            .downcast_ref::<T>()
            .ok_or_else(|| TaskError::TaskExecutionError("task key input type changed".into()))
            .and_then(|input| policy.key_for(input))
    })
}

#[derive(Debug, Clone)]
/// Filters and pagination bounds for listing persisted tasks.
pub struct TaskFilter {
    pub(crate) status: Option<TaskStatus>,
    pub(crate) name: Option<String>,
    pub(crate) lane: Option<String>,
    pub(crate) idempotency_key: Option<String>,
    pub(crate) created_from: Option<chrono::DateTime<chrono::Utc>>,
    pub(crate) created_to: Option<chrono::DateTime<chrono::Utc>>,
    pub(crate) query: Option<String>,
    pub(crate) page: usize,
    pub(crate) per_page: usize,
}

impl TaskFilter {
    /// Creates an unfiltered first page with fifty records.
    pub fn new() -> Self {
        Self::default()
    }

    /// Selects one lifecycle status.
    pub const fn status(mut self, status: TaskStatus) -> Self {
        self.status = Some(status);
        self
    }

    /// Selects one registered handler name.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Selects one configured execution lane.
    pub fn lane(mut self, lane: super::TaskLane) -> Self {
        self.lane = Some(lane.to_string());
        self
    }

    pub(crate) fn lane_name(mut self, lane: Option<String>) -> Self {
        self.lane = lane;
        self
    }

    pub(crate) fn optional_name(mut self, name: Option<String>) -> Self {
        self.name = name;
        self
    }

    pub(crate) fn optional_key(mut self, key: Option<String>) -> Self {
        self.idempotency_key = key;
        self
    }

    pub(crate) fn optional_status(mut self, status: Option<TaskStatus>) -> Self {
        self.status = status;
        self
    }

    pub(crate) fn optional_range(
        mut self,
        from: Option<chrono::DateTime<chrono::Utc>>,
        to: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Self {
        self.created_from = from;
        self.created_to = to;
        self
    }

    pub(crate) fn optional_search(mut self, query: Option<String>) -> Self {
        self.query = query;
        self
    }

    /// Selects one submitted idempotency key.
    pub fn idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }

    /// Limits records to executions created at or after this time.
    pub const fn created_from(mut self, value: chrono::DateTime<chrono::Utc>) -> Self {
        self.created_from = Some(value);
        self
    }

    /// Limits records to executions created at or before this time.
    pub const fn created_to(mut self, value: chrono::DateTime<chrono::Utc>) -> Self {
        self.created_to = Some(value);
        self
    }

    /// Applies a bounded text search to safe diagnostic fields.
    pub fn search(mut self, value: impl Into<String>) -> Self {
        self.query = Some(value.into());
        self
    }

    /// Selects a one-indexed page.
    pub const fn page(mut self, page: usize) -> Self {
        self.page = page;
        self
    }

    /// Selects the requested page size; the terminal operation enforces bounds.
    pub const fn per_page(mut self, per_page: usize) -> Self {
        self.per_page = per_page;
        self
    }

    /// Returns the requested lifecycle-status constraint for an advanced store.
    pub const fn requested_status(&self) -> Option<TaskStatus> {
        self.status
    }

    /// Returns the requested handler-name constraint for an advanced store.
    pub fn requested_name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Returns the requested lane-name constraint for an advanced store.
    pub fn requested_lane(&self) -> Option<&str> {
        self.lane.as_deref()
    }

    /// Returns the requested idempotency-key constraint for an advanced store.
    pub fn requested_idempotency_key(&self) -> Option<&str> {
        self.idempotency_key.as_deref()
    }

    /// Returns the inclusive task-creation range for an advanced store.
    pub const fn requested_created_range(
        &self,
    ) -> (
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
    ) {
        (self.created_from, self.created_to)
    }

    /// Returns the requested diagnostic search text for an advanced store.
    pub fn requested_search(&self) -> Option<&str> {
        self.query.as_deref()
    }

    /// Returns the requested one-indexed page for an advanced store.
    pub const fn requested_page(&self) -> usize {
        self.page
    }

    /// Returns the requested page size for an advanced store.
    pub const fn requested_per_page(&self) -> usize {
        self.per_page
    }
}

impl Default for TaskFilter {
    fn default() -> Self {
        Self {
            status: None,
            name: None,
            lane: None,
            idempotency_key: None,
            created_from: None,
            created_to: None,
            query: None,
            page: 1,
            per_page: 50,
        }
    }
}

#[derive(Debug, Clone)]
/// Persisted task state exposed through task inspection APIs.
pub struct TaskRecord {
    pub id: TaskId,
    pub name: String,
    pub input: String,
    pub state: Option<String>,
    pub resume_input: Option<String>,
    pub status: TaskStatus,
    pub attempts: i32,
    pub lane: String,
    pub lease_duration_ms: Option<i64>,
    pub last_error: Option<String>,
    pub idempotency_key: Option<String>,
    pub idempotency_fingerprint: Option<String>,
    pub idempotency_expires_at: Option<chrono::DateTime<chrono::Utc>>,
    pub locked_by: Option<String>,
    pub leased_until: Option<chrono::DateTime<chrono::Utc>>,
    pub ready_at: Option<chrono::DateTime<chrono::Utc>>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl TaskRecord {
    /// Returns this durable execution's canonical identifier.
    pub const fn id(&self) -> TaskId {
        self.id
    }

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

/// Read-only durable task information returned by the site task facade.
#[derive(Debug, Clone)]
pub struct TaskInfo {
    pub(crate) id: TaskId,
    pub(crate) name: String,
    pub(crate) input: String,
    pub(crate) state: Option<String>,
    pub(crate) resume_input: Option<String>,
    pub(crate) status: TaskStatus,
    pub(crate) attempts: i32,
    pub(crate) lane: String,
    pub(crate) last_error: Option<String>,
    pub(crate) idempotency_key: Option<String>,
    pub(crate) idempotency_expires_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(crate) locked_by: Option<String>,
    pub(crate) leased_until: Option<chrono::DateTime<chrono::Utc>>,
    pub(crate) ready_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(crate) created_at: chrono::DateTime<chrono::Utc>,
    pub(crate) updated_at: chrono::DateTime<chrono::Utc>,
    pub(crate) completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl TaskInfo {
    /// Returns the canonical durable execution identifier.
    pub const fn id(&self) -> TaskId {
        self.id
    }

    /// Returns the registered handler name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the current lifecycle status.
    pub const fn status(&self) -> TaskStatus {
        self.status
    }

    /// Returns the configured execution lane name.
    pub fn lane(&self) -> &str {
        &self.lane
    }

    /// Returns the number of handler invocations claimed so far.
    pub const fn attempts(&self) -> i32 {
        self.attempts
    }

    /// Returns the most recent bounded failure message.
    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    /// Returns the submitted idempotency key when one was used.
    pub fn idempotency_key(&self) -> Option<&str> {
        self.idempotency_key.as_deref()
    }

    /// Returns when a retained terminal idempotency key becomes reusable.
    pub const fn idempotency_expires_at(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.idempotency_expires_at
    }

    /// Returns the current lease deadline when the task is running.
    pub const fn leased_until(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.leased_until
    }

    /// Returns the next eligibility timestamp for pending work.
    pub const fn ready_at(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.ready_at
    }

    /// Returns when the durable execution was submitted.
    pub const fn created_at(&self) -> chrono::DateTime<chrono::Utc> {
        self.created_at
    }

    /// Returns when the durable execution last changed.
    pub const fn updated_at(&self) -> chrono::DateTime<chrono::Utc> {
        self.updated_at
    }

    /// Returns when the task reached a terminal state.
    pub const fn completed_at(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.completed_at
    }

    /// Deserializes the original typed input.
    pub fn input<T: serde::de::DeserializeOwned>(&self) -> Result<T, TaskError> {
        serde_json::from_str(&self.input).map_err(TaskError::from)
    }

    /// Deserializes optional continuation state.
    pub fn state<T: serde::de::DeserializeOwned>(&self) -> Result<Option<T>, TaskError> {
        decode_optional(&self.state)
    }

    /// Deserializes optional resume input.
    pub fn resume_input<T: serde::de::DeserializeOwned>(&self) -> Result<Option<T>, TaskError> {
        decode_optional(&self.resume_input)
    }
}

impl From<TaskRecord> for TaskInfo {
    fn from(record: TaskRecord) -> Self {
        Self {
            id: record.id,
            name: record.name,
            input: record.input,
            state: record.state,
            resume_input: record.resume_input,
            status: record.status,
            attempts: record.attempts,
            lane: record.lane,
            last_error: record.last_error,
            idempotency_key: record.idempotency_key,
            idempotency_expires_at: record.idempotency_expires_at,
            locked_by: record.locked_by,
            leased_until: record.leased_until,
            ready_at: record.ready_at,
            created_at: record.created_at,
            updated_at: record.updated_at,
            completed_at: record.completed_at,
        }
    }
}

fn decode_optional<T: serde::de::DeserializeOwned>(
    value: &Option<String>,
) -> Result<Option<T>, TaskError> {
    value
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(TaskError::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies task IDs use the canonical UUID string for display, parsing, and serde.
    #[test]
    fn task_id_round_trips_as_uuid_string() -> Result<(), String> {
        let raw = "018f0f7d-37f6-7c29-8f11-6fbf9f90792a";
        let id = raw.parse::<TaskId>().map_err(|error| error.to_string())?;
        if id.to_string() != raw {
            return Err("task ID display changed".into());
        }
        let encoded = serde_json::to_string(&id).map_err(|error| error.to_string())?;
        let decoded =
            serde_json::from_str::<TaskId>(&encoded).map_err(|error| error.to_string())?;
        (decoded == id)
            .then_some(())
            .ok_or_else(|| "task ID serde changed its value".into())
    }
}
