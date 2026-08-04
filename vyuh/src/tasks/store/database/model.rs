//! Mool-backed task row and write shapes.

use crate::{
    db,
    tasks::{TaskError, TaskRecord},
};

/// Private persistence representation for a durable task.
///
/// Application inspection receives the read-only `TaskInfo` projection. This
/// row stores status as a stable integer representation independent of SQLx
/// derives.
#[derive(Debug, Clone, db::Model)]
#[table(name = "vyuh_tasks")]
pub(super) struct TaskRow {
    #[column(primary_key, type = "uuid")]
    pub(super) id: uuid::Uuid,
    #[column(type = "varchar(191)")]
    pub(super) name: String,
    pub(super) input: String,
    pub(super) state: Option<String>,
    pub(super) resume_input: Option<String>,
    pub(super) status: i16,
    pub(super) attempts: i32,
    #[column(type = "varchar(64)", default = "'default'")]
    pub(super) lane_name: String,
    pub(super) lease_duration_ms: Option<i64>,
    pub(super) last_error: Option<String>,
    #[column(type = "varchar(512)")]
    pub(super) idempotency_key: Option<String>,
    #[column(type = "varchar(64)")]
    pub(super) idempotency_fingerprint: Option<String>,
    pub(super) idempotency_expires_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(super) locked_by: Option<String>,
    pub(super) leased_until: Option<chrono::DateTime<chrono::Utc>>,
    pub(super) ready_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(super) created_at: chrono::DateTime<chrono::Utc>,
    pub(super) updated_at: chrono::DateTime<chrono::Utc>,
    pub(super) completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl From<TaskRecord> for TaskRow {
    fn from(record: TaskRecord) -> Self {
        Self {
            id: record.id.into_uuid(),
            name: record.name,
            input: record.input,
            state: record.state,
            resume_input: record.resume_input,
            status: record.status.as_i16(),
            attempts: record.attempts,
            lane_name: record.lane,
            lease_duration_ms: record.lease_duration_ms,
            last_error: record.last_error,
            idempotency_key: record.idempotency_key,
            idempotency_fingerprint: record.idempotency_fingerprint,
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

impl TryFrom<TaskRow> for TaskRecord {
    type Error = TaskError;

    fn try_from(row: TaskRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: crate::tasks::TaskId::new(row.id),
            name: row.name,
            input: row.input,
            state: row.state,
            resume_input: row.resume_input,
            status: crate::tasks::TaskStatus::from_i16(row.status)?,
            attempts: row.attempts,
            lane: row.lane_name,
            lease_duration_ms: row.lease_duration_ms,
            last_error: row.last_error,
            idempotency_key: row.idempotency_key,
            idempotency_fingerprint: row.idempotency_fingerprint,
            idempotency_expires_at: row.idempotency_expires_at,
            locked_by: row.locked_by,
            leased_until: row.leased_until,
            ready_at: row.ready_at,
            created_at: row.created_at,
            updated_at: row.updated_at,
            completed_at: row.completed_at,
        })
    }
}

/// Current owner of one task-handler-scoped idempotency key.
#[derive(Debug, Clone, db::Model)]
#[table(name = "vyuh_task_idempotency")]
pub(super) struct TaskIdempotencyRow {
    #[column(primary_key, type = "uuid")]
    pub(super) id: uuid::Uuid,
    #[column(type = "varchar(191)")]
    pub(super) task_name: String,
    #[column(type = "varchar(512)")]
    pub(super) key_value: String,
    #[column(type = "varchar(64)")]
    pub(super) fingerprint: String,
    #[column(type = "uuid")]
    pub(super) task_id: uuid::Uuid,
    pub(super) expires_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(super) created_at: chrono::DateTime<chrono::Utc>,
    pub(super) updated_at: chrono::DateTime<chrono::Utc>,
}

/// Durable token bucket and policy identity for one globally rate-limited lane.
#[derive(Debug, Clone, db::Model)]
#[table(name = "vyuh_task_lane_rates")]
pub(super) struct TaskRateRow {
    #[column(primary_key, type = "uuid")]
    pub(super) id: uuid::Uuid,
    #[column(type = "varchar(64)")]
    pub(super) lane_name: String,
    #[column(type = "varchar(64)")]
    pub(super) policy_fingerprint: String,
    pub(super) tokens_micros: i64,
    pub(super) updated_at: chrono::DateTime<chrono::Utc>,
}

/// Store-wide task scheduling policy identity shared by all workers.
#[derive(Debug, Clone, db::Model)]
#[table(name = "vyuh_task_runtime")]
pub(super) struct TaskRuntimeRow {
    #[column(primary_key, type = "uuid")]
    pub(super) id: uuid::Uuid,
    #[column(type = "varchar(64)")]
    pub(super) policy_fingerprint: String,
    pub(super) updated_at: chrono::DateTime<chrono::Utc>,
}

/// Mutable singleton task-runtime policy fields.
#[derive(Debug, Clone, db::Record)]
#[table(name = "vyuh_task_runtime")]
pub(super) struct RuntimePolicyPatch {
    pub(super) policy_fingerprint: String,
    pub(super) updated_at: chrono::DateTime<chrono::Utc>,
}

/// Mutable rate-bucket reservation fields.
#[derive(Debug, Clone, db::Record)]
#[table(name = "vyuh_task_lane_rates")]
pub(super) struct RatePatch {
    pub(super) tokens_micros: i64,
    pub(super) updated_at: chrono::DateTime<chrono::Utc>,
}

/// Mutable idempotency retention fields.
#[derive(Debug, Clone, db::Record)]
#[table(name = "vyuh_task_idempotency")]
pub(super) struct IdempotencyExpiryPatch {
    pub(super) expires_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(super) updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, db::Record)]
#[table(name = "vyuh_tasks")]
pub(super) struct ResumePatch {
    pub(super) status: i16,
    pub(super) resume_input: Option<String>,
    pub(super) ready_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(super) updated_at: chrono::DateTime<chrono::Utc>,
}
