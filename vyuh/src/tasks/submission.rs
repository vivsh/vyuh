//! Task submission options, persistence requests, and receipts.

use std::{fmt, time::Duration};

use serde::Serialize;

use super::{
    RegisteredTask, TaskFilter, TaskId, TaskLane, TaskRecord, TaskRuntimeError, TaskStatus,
};
use crate::callables::DataBox;

/// Submission policy evaluated before one or more task records are stored.
#[derive(Default)]
pub struct TaskOptions {
    pub(crate) initial_delay: Option<Duration>,
    pub(crate) ignore_conflicts: bool,
}

impl TaskOptions {
    /// Creates the default submission policy.
    pub fn new() -> Self {
        Self::default()
    }

    /// Delays the first execution attempt.
    pub const fn delay(mut self, delay: Duration) -> Self {
        self.initial_delay = Some(delay);
        self
    }

    /// Keeps non-conflicting batch entries when an idempotency key is reused.
    pub const fn ignore_conflicts(mut self) -> Self {
        self.ignore_conflicts = true;
        self
    }
}

pub(crate) fn canonical_json<T: Serialize>(input: &T) -> Result<String, TaskRuntimeError> {
    let value = serde_json::to_value(input)?;
    canonical_json_value(value)
}

/// Serializes an already type-erased JSON input with stable object-key ordering.
pub(crate) fn canonical_json_value(value: serde_json::Value) -> Result<String, TaskRuntimeError> {
    serde_json::to_string(&canonical_value(value)).map_err(TaskRuntimeError::from)
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

impl Clone for TaskOptions {
    fn clone(&self) -> Self {
        Self {
            initial_delay: self.initial_delay,
            ignore_conflicts: self.ignore_conflicts,
        }
    }
}

impl fmt::Debug for TaskOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaskOptions")
            .field("initial_delay", &self.initial_delay)
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

/// Normalizes a typed submission batch before any store mutation occurs.
pub(super) fn build_writes<T: Serialize + 'static>(
    service: &RegisteredTask,
    name: &str,
    inputs: impl IntoIterator<Item = T>,
    options: &TaskOptions,
    config: &super::TaskConf,
) -> Result<Vec<TaskWrite>, TaskRuntimeError> {
    validate_options(options)?;
    let mut writes = Vec::new();
    for input in inputs {
        if writes.len() >= super::config::MAX_BATCH_SIZE {
            return Err(TaskRuntimeError::InvalidOptions(
                "task submission exceeds the 10,000-input safety limit".into(),
            ));
        }
        service.validate_object(&input)?;
        let key = service.idempotency_key(&input)?;
        if key.is_none() && options.ignore_conflicts {
            return Err(TaskRuntimeError::InvalidOptions(
                "ignore_conflicts requires an idempotent task definition".into(),
            ));
        }
        let serialized = if key.is_some() {
            canonical_json(&input)?
        } else {
            serde_json::to_string(&input)?
        };
        validate_payload(&serialized, config.payload_limit())?;
        validate_key(key.as_deref())?;
        let fingerprint = key
            .as_ref()
            .map(|_| fingerprint(name, &serialized, service))
            .transpose()?;
        writes.push(TaskWrite {
            record: build_record(
                name,
                serialized,
                key,
                fingerprint,
                service.effective_lane(),
                service.kind(),
            )?,
            ignore_conflicts: options.ignore_conflicts,
            initial_delay: options.initial_delay,
        });
    }
    Ok(writes)
}

/// Converts one verified type-erased payload into a normal task write.
pub(super) fn build_box_write(
    service: &RegisteredTask,
    input: &DataBox,
    config: &super::TaskConf,
) -> Result<TaskWrite, TaskRuntimeError> {
    service.validate_box(input)?;
    let value = input
        .to_json()
        .ok_or_else(|| {
            TaskRuntimeError::TaskExecutionError("task input cannot be serialized".into())
        })?
        .map_err(TaskRuntimeError::TaskExecutionError)?;
    let key = service.idempotency_key_box(input.as_any())?;
    let serialized = if key.is_some() {
        canonical_json_value(value)?
    } else {
        serde_json::to_string(&value)?
    };
    validate_payload(&serialized, config.payload_limit())?;
    validate_key(key.as_deref())?;
    let fingerprint = key
        .as_ref()
        .map(|_| fingerprint(service.name(), &serialized, service))
        .transpose()?;
    Ok(TaskWrite {
        record: build_record(
            service.name(),
            serialized,
            key,
            fingerprint,
            service.effective_lane(),
            service.kind(),
        )?,
        ignore_conflicts: false,
        initial_delay: None,
    })
}

/// Rejects incompatible conflict behavior before a child request can be returned.
pub(super) fn validate_spawn_options(options: &TaskOptions) -> Result<(), TaskRuntimeError> {
    if options.ignore_conflicts {
        return Err(TaskRuntimeError::InvalidOptions(
            "spawn cannot ignore idempotency conflicts".into(),
        ));
    }
    validate_options(options)
}

/// Surfaces all accumulated builder failures at the submission terminal.
pub(super) fn validate_options(options: &TaskOptions) -> Result<(), TaskRuntimeError> {
    if options
        .initial_delay
        .is_some_and(|value| value > super::config::MAX_TASK_DELAY)
    {
        return Err(TaskRuntimeError::InvalidOptions(
            "task delays cannot exceed ten years".into(),
        ));
    }
    duration_ms(options.initial_delay)?;
    Ok(())
}

/// Creates one pending record without assigning store-relative readiness time.
pub(super) fn build_record(
    name: &str,
    input: String,
    key: Option<String>,
    fingerprint: Option<String>,
    lane: TaskLane,
    kind: super::TaskKind,
) -> Result<TaskRecord, TaskRuntimeError> {
    let now = chrono::Utc::now();
    Ok(TaskRecord {
        id: TaskId::new(uuid::Uuid::now_v7()),
        parent_id: None,
        root_id: None,
        kind,
        name: name.into(),
        input,
        state: None,
        resume_input: None,
        status: TaskStatus::Pending,
        cancelled: false,
        attempts: 0,
        step_attempts: 0,
        lane: lane.to_string(),
        last_result: None,
        idempotency_key: key,
        idempotency_fingerprint: fingerprint,
        idempotency_expires_at: None,
        node_id: None,
        ready_at: Some(now),
        created_at: now,
        updated_at: now,
        completed_at: None,
    })
}

/// Fingerprints canonical input and immutable idempotency-key semantics.
pub(super) fn fingerprint(
    name: &str,
    input: &str,
    service: &RegisteredTask,
) -> Result<String, TaskRuntimeError> {
    let revision = service
        .idempotency_policy()
        .ok_or_else(|| {
            TaskRuntimeError::TaskExecutionError("idempotent task is missing policy".into())
        })?
        .revision;
    let mut hasher = blake3::Hasher::new();
    for part in [name, revision, input] {
        hasher.update(part.as_bytes());
        hasher.update(&[0]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

pub(super) fn validate_payload(payload: &str, limit: usize) -> Result<(), TaskRuntimeError> {
    if payload.len() > limit {
        Err(TaskRuntimeError::InvalidOptions(format!(
            "task payload exceeds the configured {limit}-byte limit"
        )))
    } else {
        Ok(())
    }
}

pub(super) fn validate_filter(filter: &TaskFilter) -> Result<(), TaskRuntimeError> {
    if filter.page == 0 || filter.per_page == 0 || filter.per_page > 200 {
        return Err(TaskRuntimeError::InvalidOptions(
            "task pages are one-indexed and contain at most 200 records".into(),
        ));
    }
    if filter.query.as_ref().is_some_and(|query| query.len() > 256) {
        return Err(TaskRuntimeError::InvalidOptions(
            "task search text cannot exceed 256 bytes".into(),
        ));
    }
    for (value, limit, label) in [
        (filter.name.as_deref(), 191, "task name"),
        (filter.lane.as_deref(), 64, "task lane"),
        (
            filter.idempotency_key.as_deref(),
            512,
            "task idempotency key",
        ),
    ] {
        if value.is_some_and(|value| value.len() > limit) {
            return Err(TaskRuntimeError::InvalidOptions(format!(
                "{label} filters cannot exceed {limit} bytes"
            )));
        }
    }
    if filter
        .created_from
        .zip(filter.created_to)
        .is_some_and(|(from, to)| from > to)
    {
        return Err(TaskRuntimeError::InvalidOptions(
            "task creation range starts after it ends".into(),
        ));
    }
    Ok(())
}

pub(super) fn validate_key(key: Option<&str>) -> Result<(), TaskRuntimeError> {
    if key.is_some_and(|value| value.is_empty() || value.len() > 512) {
        Err(TaskRuntimeError::InvalidOptions(
            "task idempotency keys must contain between 1 and 512 bytes".into(),
        ))
    } else {
        Ok(())
    }
}

pub(super) fn duration_ms(duration: Option<Duration>) -> Result<Option<i64>, TaskRuntimeError> {
    duration
        .map(|value| {
            i64::try_from(value.as_millis()).map_err(|_| {
                TaskRuntimeError::InvalidOptions("task duration exceeds the supported range".into())
            })
        })
        .transpose()
}
