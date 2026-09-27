//! Stateless validation, flush accounting, and bounded ordered join encoding.

use crate::tasks::{
    TaskCommit, TaskId, TaskOutcome, TaskRuntimeError, TaskStatus, TaskStoreConf, TaskWrite,
};

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
        .any(|commit| matches!(commit.outcome, TaskOutcome::All { .. }))
    {
        return Ok(());
    }
    let total = commits.iter().try_fold(0usize, |sum, commit| {
        sum.checked_add(weight(&commit.outcome))
    });
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
