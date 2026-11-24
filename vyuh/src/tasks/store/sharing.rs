//! Store-owned shared Work identity and terminal subscriber transitions.

use crate::tasks::{TaskId, TaskRecord, TaskRuntimeError, TaskStatus, models::ShareRole};
use chrono::{DateTime, Utc};

pub(crate) const FOLLOWER_LIMIT: usize = 32;
pub(crate) const PUBLISH_WEIGHT: usize = 1 + 2 * FOLLOWER_LIMIT;
// A maximum-sized result uses one MiB across 32 subscribers. Never split a group.
pub(crate) const FLUSH_FANOUT_BYTES: usize =
    2 * FOLLOWER_LIMIT * crate::tasks::result::RESULT_LIMIT;

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
    FOLLOWER_LIMIT * bytes.min(crate::tasks::result::RESULT_LIMIT)
}

/// Public topics reject NUL, and a task UUID identifies one generation of a key.
pub(super) fn topic(id: TaskId) -> Vec<u8> {
    let mut topic = Vec::with_capacity(17);
    topic.push(0);
    topic.extend_from_slice(id.into_uuid().as_bytes());
    topic
}

pub(super) fn compatible(input: &TaskRecord, owner: &TaskRecord) -> bool {
    owner.share_role == ShareRole::Publisher
        && input.idempotency_fingerprint.is_some()
        && input.idempotency_fingerprint == owner.idempotency_fingerprint
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
