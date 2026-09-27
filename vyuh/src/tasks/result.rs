//! Shared JSON result encoding for persistence and continuation delivery.

use super::{TaskFailure, TaskId, TaskRuntimeError};

pub(crate) const RESULT_LIMIT: usize = 32_768;
pub(crate) const UNIT_RESULT: &str = "{\"Ok\":null}";

/// Checks the complete serialized envelope, including JSON escaping overhead.
pub(crate) fn validate_size(value: &str) -> Result<(), TaskRuntimeError> {
    check_size(value.len())
}

fn check_size(actual: usize) -> Result<(), TaskRuntimeError> {
    if actual > RESULT_LIMIT {
        return Err(TaskRuntimeError::ResultTooLarge {
            actual,
            limit: RESULT_LIMIT,
        });
    }
    Ok(())
}

/// Validates a successful JSON value before adding its seven-byte envelope.
pub(crate) fn validate_output(output: &str) -> Result<(), TaskRuntimeError> {
    check_size(output.len().saturating_add(7))?;
    serde_json::from_str::<&serde_json::value::RawValue>(output)?;
    Ok(())
}

/// Validates a low-level resume request without interpreting its success type.
pub(crate) fn validate_resume(value: &str) -> Result<(), TaskRuntimeError> {
    validate_size(value)?;
    let _ = serde_json::from_str::<Result<&serde_json::value::RawValue, TaskFailure>>(value)?;
    Ok(())
}

/// Wraps an already validated successful value without parsing or reserializing it.
pub(crate) fn success(output: &str) -> String {
    format!("{{\"Ok\":{output}}}")
}

/// Encodes safe diagnostics and shortens only the message if JSON escaping exceeds the limit.
pub(crate) fn failure(id: TaskId, message: String) -> String {
    let mut message = message;
    loop {
        let result = format!(
            "{{\"Err\":{{\"task_id\":\"{id}\",\"message\":{}}}}}",
            serde_json::Value::String(message.clone())
        );
        if result.len() <= RESULT_LIMIT {
            return result;
        }
        let excess = result.len() - RESULT_LIMIT;
        let mut end = message.len().saturating_sub(excess.div_ceil(6));
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        message.truncate(end);
    }
}

#[cfg(test)]
#[path = "tests/result.rs"]
mod tests;
