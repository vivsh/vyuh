//! Shared limits and receipt for one-shot task publications.

use super::{TaskOutcome, TaskRuntimeError};
use serde::Serialize;

pub(super) const LIMIT: usize = 128;
const MAX_TOPIC_BYTES: usize = 256;

/// Acknowledgement of one bounded publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishResult {
    /// Number of waits consumed and made ready in the committed transaction.
    pub resumed: u64,
    /// An additional waiter was observed. Another call is a fresh publication,
    /// not a cursor over the original audience.
    pub more: bool,
}

/// Validates without changing the bytes used for topic matching.
pub(super) fn validate(topic: &str) -> Result<(), TaskRuntimeError> {
    if topic.len() > MAX_TOPIC_BYTES || topic.trim().is_empty() || topic.contains('\0') {
        return Err(TaskRuntimeError::InvalidOptions(
            "Wait topic must be nonblank, contain no NUL and fit within 256 UTF-8 bytes".into(),
        ));
    }
    Ok(())
}

pub(super) fn wait(topic: &str, state: impl Serialize) -> Result<TaskOutcome, TaskRuntimeError> {
    validate(topic)?;
    Ok(TaskOutcome::Wait {
        topic: topic.to_owned(),
        state: serde_json::to_string(&state)?,
    })
}
