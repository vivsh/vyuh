//! Native failure-chain logging for task runtime operations.

use std::error::Error as StdError;

use super::TaskError;

/// Logs a native task-store chain without placing it in durable task history.
pub(crate) fn log_runtime_error(error: &TaskError, message: &'static str) {
    tracing::error!(error = %causal_chain(error), "{message}");
}

/// Builds a bounded causal chain for operator-only logs and tracing.
fn causal_chain(error: &TaskError) -> String {
    let mut chain = error.to_string();
    let mut source = error.source();
    for _ in 0..16 {
        let Some(current) = source else {
            break;
        };
        chain.push_str(": ");
        chain.push_str(&current.to_string());
        source = current.source();
    }
    chain
}
