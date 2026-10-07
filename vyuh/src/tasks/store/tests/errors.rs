use super::*;

/// Typed test failures retain the original store acknowledgement evidence.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TestError {
    #[error(transparent)]
    Runtime(#[from] TaskRuntimeError),
    #[error("{0}")]
    Poll(#[from] StoreFailure<TaskPoll>),
    #[error("{0}")]
    Flush(#[from] StoreFailure<CommitResult>),
}
