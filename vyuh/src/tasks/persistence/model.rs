//! Mool-backed task row and write shapes.

use crate::{
    db,
    tasks::{TaskError, TaskRecord},
};

/// Private persistence representation for a durable task.
///
/// `TaskRecord` remains Vyuh's public runtime type. This row deliberately
/// stores status as its stable integer representation so database metadata is
/// independent of the public task enum's SQLx compatibility derive.
#[derive(Debug, Clone, db::Model)]
#[table(name = "vyuh_tasks")]
pub(super) struct TaskRow {
    #[column(primary_key, type = "uuid")]
    pub(super) id: uuid::Uuid,
    pub(super) name: String,
    pub(super) input: String,
    pub(super) state: Option<String>,
    pub(super) resume_input: Option<String>,
    pub(super) output: Option<String>,
    pub(super) result: Option<String>,
    pub(super) status: i16,
    pub(super) attempts: i32,
    pub(super) priority: i32,
    pub(super) max_attempts: Option<i32>,
    pub(super) retry_delay_ms: Option<i64>,
    pub(super) lease_duration_ms: Option<i64>,
    pub(super) last_error: Option<String>,
    pub(super) identity: Option<String>,
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
            id: record.id,
            name: record.name,
            input: record.input,
            state: record.state,
            resume_input: record.resume_input,
            output: record.output,
            result: record.result,
            status: record.status.as_i16(),
            attempts: record.attempts,
            priority: record.priority,
            max_attempts: record.max_attempts,
            retry_delay_ms: record.retry_delay_ms,
            lease_duration_ms: record.lease_duration_ms,
            last_error: record.last_error,
            identity: record.identity,
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
            id: row.id,
            name: row.name,
            input: row.input,
            state: row.state,
            resume_input: row.resume_input,
            output: row.output,
            result: row.result,
            status: crate::tasks::TaskStatus::from_i16(row.status)?,
            attempts: row.attempts,
            priority: row.priority,
            max_attempts: row.max_attempts,
            retry_delay_ms: row.retry_delay_ms,
            lease_duration_ms: row.lease_duration_ms,
            last_error: row.last_error,
            identity: row.identity,
            locked_by: row.locked_by,
            leased_until: row.leased_until,
            ready_at: row.ready_at,
            created_at: row.created_at,
            updated_at: row.updated_at,
            completed_at: row.completed_at,
        })
    }
}

#[derive(Debug, Clone, db::Record)]
#[table(name = "vyuh_tasks")]
pub(super) struct ClaimPatch {
    pub(super) status: i16,
    pub(super) locked_by: Option<String>,
    pub(super) leased_until: Option<chrono::DateTime<chrono::Utc>>,
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

#[derive(Debug, Clone, db::Record)]
#[table(name = "vyuh_tasks")]
pub(super) struct CompletePatch {
    pub(super) status: i16,
    pub(super) resume_input: Option<String>,
    pub(super) output: Option<String>,
    pub(super) result: Option<String>,
    pub(super) last_error: Option<String>,
    pub(super) ready_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(super) completed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(super) locked_by: Option<String>,
    pub(super) leased_until: Option<chrono::DateTime<chrono::Utc>>,
    pub(super) updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, db::Record)]
#[table(name = "vyuh_tasks")]
pub(super) struct SuspendPatch {
    pub(super) status: i16,
    pub(super) state: String,
    pub(super) resume_input: Option<String>,
    pub(super) output: Option<String>,
    pub(super) result: Option<String>,
    pub(super) last_error: Option<String>,
    pub(super) ready_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(super) completed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(super) locked_by: Option<String>,
    pub(super) leased_until: Option<chrono::DateTime<chrono::Utc>>,
    pub(super) updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, db::Record)]
#[table(name = "vyuh_tasks")]
pub(super) struct SleepPatch {
    pub(super) status: i16,
    pub(super) state: String,
    pub(super) resume_input: Option<String>,
    pub(super) output: Option<String>,
    pub(super) result: Option<String>,
    pub(super) last_error: Option<String>,
    pub(super) ready_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(super) completed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(super) locked_by: Option<String>,
    pub(super) leased_until: Option<chrono::DateTime<chrono::Utc>>,
    pub(super) updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, db::Record)]
#[table(name = "vyuh_tasks")]
pub(super) struct FailPatch {
    pub(super) status: i16,
    pub(super) resume_input: Option<String>,
    pub(super) output: Option<String>,
    pub(super) result: Option<String>,
    pub(super) last_error: Option<String>,
    pub(super) ready_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(super) completed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(super) locked_by: Option<String>,
    pub(super) leased_until: Option<chrono::DateTime<chrono::Utc>>,
    pub(super) updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, db::Record)]
#[table(name = "vyuh_tasks")]
pub(super) struct RetryPatch {
    pub(super) status: i16,
    pub(super) attempts: i32,
    pub(super) last_error: Option<String>,
    pub(super) ready_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(super) completed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(super) locked_by: Option<String>,
    pub(super) leased_until: Option<chrono::DateTime<chrono::Utc>>,
    pub(super) updated_at: chrono::DateTime<chrono::Utc>,
}
