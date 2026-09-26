//! Stateless encoding shared by durable and in-memory workflow flushes.

use crate::tasks::{TaskError, TaskStatus};

/// Borrows the accepted terminal result; retries and checkpoints do not resume parents.
pub(super) fn terminal_result(
    status: TaskStatus,
    result: Option<&str>,
) -> Result<Option<&str>, TaskError> {
    match status {
        TaskStatus::Succeeded | TaskStatus::Failed => result.map(Some).ok_or_else(|| {
            TaskError::TaskExecutionError("Terminal outcome omitted its result".into())
        }),
        _ => Ok(None),
    }
}
