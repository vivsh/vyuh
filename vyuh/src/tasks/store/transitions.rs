//! Stateless encoding shared by durable and in-memory workflow flushes.

use crate::tasks::{
    TaskCommit, TaskId, TaskOutcome, TaskRuntimeError, TaskStatus, TaskStoreConf, TaskWrite,
};

/// Sticky unlocked lanes prefer local child/cache admission, never retained Pending ownership.
pub(crate) fn sticky(record: &crate::tasks::TaskRecord, conf: &TaskStoreConf) -> bool {
    conf.lanes
        .iter()
        .find(|lane| lane.lane().as_str() == record.lane)
        .is_some_and(|lane| lane.is_sticky())
}

/// Checks direct admission without consulting a separate lane row or client clock.
pub(crate) fn direct(
    record: &crate::tasks::TaskRecord,
    conf: &TaskStoreConf,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    record.status == TaskStatus::Pending
        && !record.cancelled
        && record.ready_at.is_none_or(|at| at <= now)
        && conf
            .lanes
            .iter()
            .find(|lane| lane.lane().as_str() == record.lane)
            .is_some_and(|lane| {
                lane.lane_lock().is_none()
                    && (record.kind == crate::tasks::TaskKind::Flow || lane.global_rate().is_none())
            })
}

/// Establishes one fresh execution in the originating store transaction.
pub(crate) fn admit(
    record: &mut crate::tasks::TaskRecord,
    node: uuid::Uuid,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(), TaskRuntimeError> {
    record.attempts = record.attempts.checked_add(1).ok_or_else(|| {
        TaskRuntimeError::TaskExecutionError("task attempt count overflowed".into())
    })?;
    record.step_attempts = record.step_attempts.checked_add(1).ok_or_else(|| {
        TaskRuntimeError::TaskExecutionError("task step attempt count overflowed".into())
    })?;
    record.status = TaskStatus::Running;
    record.node_id = Some(node);
    record.updated_at = now;
    Ok(())
}

/// Validates capabilities using already-loaded rows; it changes no execution policy.
pub(super) fn capability_error(
    kind: crate::tasks::TaskKind,
    outcome: &crate::tasks::TaskOutcome,
    handlers: &[(String, crate::tasks::TaskKind)],
) -> Option<&'static str> {
    use crate::tasks::{TaskKind, TaskOutcome};
    match (kind, outcome) {
        (
            TaskKind::Work,
            TaskOutcome::Sleep { .. } | TaskOutcome::Spawn { .. } | TaskOutcome::All { .. },
        ) => Some("Work tasks cannot sleep or spawn"),
        (TaskKind::Flow, TaskOutcome::Retry { .. }) => Some("Flow handlers cannot request retry"),
        (_, TaskOutcome::Spawn { child, .. })
            if !handlers
                .iter()
                .any(|(name, kind)| name == &child.record.name && *kind == child.record.kind) =>
        {
            Some("Child task kind does not match registered handler")
        }
        _ => None,
    }
}

/// Produces the same safe terminal failure for every cancellation acceptance path.
pub(super) fn cancellation() -> crate::tasks::TaskOutcome {
    crate::tasks::TaskOutcome::fail("Task cancelled")
}

/// Borrows the accepted terminal result; retries and checkpoints do not resume parents.
pub(super) fn terminal_result(
    status: TaskStatus,
    result: Option<&str>,
) -> Result<Option<&str>, TaskRuntimeError> {
    match status {
        TaskStatus::Succeeded | TaskStatus::Failed => result.map(Some).ok_or_else(|| {
            TaskRuntimeError::TaskExecutionError("Terminal outcome omitted its result".into())
        }),
        _ => Ok(None),
    }
}

/// Derives work weight without introducing runner-owned lifecycle state.
pub(crate) fn weight(outcome: &TaskOutcome) -> usize {
    match outcome {
        TaskOutcome::All { children, .. } => children.len().saturating_add(1),
        _ => 1,
    }
}

/// Rejects oversized low-level join turns before any authoritative mutation.
pub(crate) fn validate_turn(
    commits: &[TaskCommit],
    batch: usize,
    maximum: usize,
) -> Result<(), TaskRuntimeError> {
    if !commits
        .iter()
        .any(|commit| matches!(commit.outcome(), Some(TaskOutcome::All { .. })))
    {
        return Ok(());
    }
    let total = commits
        .iter()
        .try_fold(0usize, |sum, commit| sum.checked_add(commit.weight()));
    if commits.len() > batch || total.is_none_or(|sum| sum > batch.max(maximum.saturating_add(1))) {
        return Err(TaskRuntimeError::InvalidOptions(
            "Expanded all flush exceeds its budget".into(),
        ));
    }
    Ok(())
}

/// Validates the whole group before allocating reservations or inserting children.
pub(super) fn validate_group(
    children: &[TaskWrite],
    conf: &TaskStoreConf,
) -> Result<(), TaskRuntimeError> {
    if children.len() > conf.max_all_children {
        return Err(TaskRuntimeError::InvalidOptions(
            "Too many all children".into(),
        ));
    }
    let mut keys = std::collections::HashSet::new();
    let mut ids = std::collections::HashSet::new();
    for child in children {
        let record = &child.record;
        if child.ignore_conflicts
            || child
                .initial_delay
                .is_some_and(|delay| delay > crate::tasks::config::MAX_TASK_DELAY)
            || !ids.insert(record.id)
            || children
                .first()
                .is_some_and(|first| first.record.name != record.name)
            || !conf
                .handlers
                .iter()
                .any(|(name, kind)| name == &record.name && *kind == record.kind)
            || !conf
                .lanes
                .iter()
                .any(|lane| lane.lane().as_str() == record.lane)
            || record.status != TaskStatus::Pending
            || record.cancelled
            || (record.idempotency_key.is_some() && record.idempotency_fingerprint.is_none())
        {
            return Err(TaskRuntimeError::InvalidOptions("Invalid all child".into()));
        }
        if let Some(key) = &record.idempotency_key
            && !keys.insert((&record.name, key))
        {
            return Err(TaskRuntimeError::InvalidOptions(
                "Duplicate all child key".into(),
            ));
        }
    }
    Ok(())
}

/// Appends already-encoded member results, bounding the entire outer envelope.
pub(super) fn append_result(
    buffer: &mut String,
    result: Option<&str>,
    terminal: bool,
) -> Result<(), &'static str> {
    let result = result
        .filter(|_| terminal)
        .ok_or("All child result is missing or nonterminal")?;
    if crate::tasks::result::validate_resume(result).is_err() {
        return Err("All child result is malformed");
    }
    let separator = usize::from(buffer.len() > 7);
    if buffer
        .len()
        .saturating_add(separator)
        .saturating_add(result.len())
        .saturating_add(2)
        > crate::tasks::result::RESULT_LIMIT
    {
        return Err("All results exceed the 32768-byte resume limit");
    }
    let needed = buffer.len() + result.len() + separator + 2;
    if needed > buffer.capacity() {
        let capacity = needed
            .max(buffer.capacity().saturating_mul(2))
            .min(crate::tasks::result::RESULT_LIMIT);
        buffer.reserve_exact(capacity - buffer.len());
    }
    if separator != 0 {
        buffer.push(',');
    }
    buffer.push_str(result);
    Ok(())
}

/// Produces an outer failure without altering accepted member results.
pub(super) fn failure(parent: TaskId, message: &str) -> String {
    tracing::error!(task_id = %parent, error = message, "Task all join failed");
    crate::tasks::result::failure(parent, message.into())
}

#[cfg(test)]
#[path = "tests/all_encoding.rs"]
mod tests;
