//! Typed, invocation-local views of persisted checkpoints and resume results.
use std::sync::Arc;

use super::{FlowContext, TaskRecord, WorkContext};
use crate::callables;

/// Optional typed checkpoint and resume input for Work or Flow handlers.
pub struct Continuation<S, R = ()> {
    state: Option<S>,
    resume: Option<Result<R, super::TaskFailure>>,
    record: Arc<TaskRecord>,
}

impl<S, R> callables::FromContextParts<FlowContext> for Continuation<S, R>
where
    S: serde::de::DeserializeOwned + Send,
    R: serde::de::DeserializeOwned + Send,
{
    fn from_context_parts(ctx: &FlowContext) -> Result<Self, callables::CallError> {
        Self::decode(Arc::clone(&ctx.record))
    }
}

impl<S, R> callables::FromContextParts<WorkContext> for Continuation<S, R>
where
    S: serde::de::DeserializeOwned + Send,
    R: serde::de::DeserializeOwned + Send,
{
    fn from_context_parts(ctx: &WorkContext) -> Result<Self, callables::CallError> {
        Self::decode(Arc::clone(&ctx.record))
    }
}

fn decode_optional<T: serde::de::DeserializeOwned>(
    value: Option<&str>,
) -> Result<Option<T>, callables::CallError> {
    value
        .map(serde_json::from_str)
        .transpose()
        .map_err(|_| callables::CallError::DeserializeFailed)
}

impl<S, R> callables::IntoArgPart for Continuation<S, R> {
    fn into_arg_part() -> callables::ArgPart {
        callables::ArgPart::Ignore
    }
}

impl<S, R> Continuation<S, R> {
    /// Decodes invocation-local continuation values from the immutable store snapshot.
    pub(super) fn decode(record: Arc<TaskRecord>) -> Result<Self, callables::CallError>
    where
        S: serde::de::DeserializeOwned,
        R: serde::de::DeserializeOwned,
    {
        Ok(Self {
            state: decode_optional(record.state.as_deref())?,
            resume: decode_optional(record.resume_input.as_deref())?,
            record,
        })
    }

    /// Returns persisted state from a previous lifecycle transition.
    pub const fn state(&self) -> Option<&S> {
        self.state.as_ref()
    }

    /// Returns the input that resumed this suspended execution.
    pub const fn resume(&self) -> Option<&Result<R, super::TaskFailure>> {
        self.resume.as_ref()
    }

    /// Borrows this task's persisted idempotency key, if it was submitted with one.
    pub fn idempotency_key(&self) -> Option<&str> {
        self.record.idempotency_key.as_deref()
    }

    /// Consumes the extractor into its optional state and resume input.
    pub fn into_parts(self) -> (Option<S>, Option<Result<R, super::TaskFailure>>) {
        (self.state, self.resume)
    }
}
