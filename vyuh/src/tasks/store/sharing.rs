//! Store-owned shared Work identity and terminal subscriber transitions.

use crate::tasks::{TaskId, TaskRecord, TaskRuntimeError, TaskStatus, models::ShareRole};
use chrono::{DateTime, Utc};

pub(crate) const PAGE_SIZE: usize = 32;
pub(crate) const DRAIN_NAME: &str = "vyuh.shared-drain";
pub(crate) const PUBLISH_WEIGHT: usize = 1 + 2 * PAGE_SIZE;
// Keep the transaction budget independent of the page-size policy.
pub(crate) const FLUSH_FANOUT_BYTES: usize = 2 * 1024 * 1024;

/// Estimates subscriber result copies without loading subscribers or re-encoding outcomes.
pub(super) fn fanout_bytes(outcome: &crate::tasks::TaskOutcome) -> usize {
    use crate::tasks::TaskOutcome;

    // Allow for short framework failures (including cancellations), the task ID,
    // and JSON escaping if a handler failure or an exhausted retry is published.
    const DIAGNOSTIC_BYTES: usize = 256;
    let bytes = match outcome {
        TaskOutcome::CompleteWith { output } => {
            output.len().saturating_add(7).max(DIAGNOSTIC_BYTES)
        }
        TaskOutcome::Fail { error } | TaskOutcome::Retry { error } => error
            .len()
            .saturating_mul(6)
            .saturating_add(DIAGNOSTIC_BYTES),
        _ => DIAGNOSTIC_BYTES,
    };
    (PAGE_SIZE * bytes.min(crate::tasks::result::RESULT_LIMIT)).min(FLUSH_FANOUT_BYTES)
}

/// Only opted-in page producers reserve additional flush capacity.
pub(crate) fn estimate(role: ShareRole, outcome: &crate::tasks::TaskOutcome) -> Option<usize> {
    match role {
        ShareRole::Publisher => Some(fanout_bytes(outcome)),
        ShareRole::Drainer => {
            Some(page_size(crate::tasks::result::RESULT_LIMIT) * crate::tasks::result::RESULT_LIMIT)
        }
        _ => None,
    }
}

/// Public topics reject NUL, and a task UUID identifies one generation of a key.
pub(super) fn topic(id: TaskId) -> Vec<u8> {
    let mut topic = Vec::with_capacity(17);
    topic.push(0);
    topic.extend_from_slice(id.into_uuid().as_bytes());
    topic
}

pub(crate) fn page_size(bytes: usize) -> usize {
    PAGE_SIZE.min(FLUSH_FANOUT_BYTES / bytes.clamp(1, crate::tasks::result::RESULT_LIMIT))
}

/// Creates the sole continuation for this publisher generation, without inheriting lineage or ownership.
pub(crate) fn drainer(publisher: TaskId, lane: String, now: DateTime<Utc>) -> TaskRecord {
    TaskRecord {
        id: TaskId::new(uuid::Uuid::now_v7()),
        share_role: ShareRole::Drainer,
        name: DRAIN_NAME.into(),
        kind: crate::tasks::TaskKind::Work,
        input: format!("\"{publisher}\""),
        lane,
        status: TaskStatus::Pending,
        parent_id: None,
        root_id: None,
        state: None,
        resume_input: None,
        last_result: None,
        cancelled: false,
        attempts: 0,
        step_attempts: 0,
        idempotency_key: None,
        idempotency_fingerprint: None,
        idempotency_expires_at: None,
        node_id: None,
        ready_at: Some(now),
        created_at: now,
        updated_at: now,
        completed_at: None,
    }
}

/// Creates a suspended subscriber, or a terminal one when the result already exists.
pub(super) fn subscribe(
    record: &mut TaskRecord,
    owner: &TaskRecord,
    now: DateTime<Utc>,
) -> Result<Option<Vec<u8>>, TaskRuntimeError> {
    record.share_role = ShareRole::Subscriber;
    record.ready_at = None;
    if let Some(result) =
        super::transitions::terminal_result(owner.status, owner.last_result.as_deref())?
    {
        complete(record, owner.status, result, now);
        Ok(None)
    } else {
        record.status = TaskStatus::Suspended;
        Ok(Some(topic(owner.id)))
    }
}

pub(super) fn complete(
    record: &mut TaskRecord,
    status: TaskStatus,
    result: &str,
    now: DateTime<Utc>,
) {
    record.status = status;
    record.last_result = Some(result.to_owned());
    record.updated_at = now;
    record.completed_at = Some(now);
}
