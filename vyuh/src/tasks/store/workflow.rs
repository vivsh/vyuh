//! Stateless encoding shared by durable and in-memory workflow flushes.

use crate::tasks::{TaskRuntimeError, TaskStatus};

/// Validates capabilities using already-loaded rows; it changes no execution policy.
pub(super) fn capability_error(
    kind: crate::tasks::TaskKind,
    outcome: &crate::tasks::TaskOutcome,
    handlers: &[(String, crate::tasks::TaskKind)],
) -> Option<&'static str> {
    use crate::tasks::{TaskKind, TaskOutcome};
    match (kind, outcome) {
        (TaskKind::Work, TaskOutcome::Sleep { .. } | TaskOutcome::Spawn { .. }) => {
            Some("Work tasks cannot sleep or spawn")
        }
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
