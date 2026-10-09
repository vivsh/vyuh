//! Work and batch extraction contexts; invocation snapshots remain immutable.
use crate::tasks::TaskRecord;
use crate::{Site, callables};
use std::sync::Arc;

/// Value-only batch context; exposes services and invocation identity, not continuations.
#[derive(Clone)]
#[doc(hidden)]
pub struct BatchWorkContext {
    pub(in crate::tasks) site: Site,
    pub(in crate::tasks) payload: callables::DataBox,
    pub(in crate::tasks) operation_id: crate::OperationId,
}

impl callables::IntoDataBox for BatchWorkContext {
    fn into_data_box(self) -> callables::DataBox {
        self.payload
    }
}

impl callables::HasSite for BatchWorkContext {
    fn site(&self) -> &Site {
        &self.site
    }
}

impl callables::FromContextParts<BatchWorkContext> for crate::OperationId {
    fn from_context_parts(context: &BatchWorkContext) -> Result<Self, callables::CallError> {
        Ok(context.operation_id)
    }
}

/// Work invocation context used internally to extract task data and runtime identity.
#[doc(hidden)]
#[derive(Clone)]
pub struct WorkContext {
    pub(in crate::tasks) site: Site,
    pub(in crate::tasks) payload: callables::DataBox,
    pub(in crate::tasks) record: Arc<TaskRecord>,
    pub(in crate::tasks) operation_id: crate::OperationId,
}

impl callables::IntoDataBox for WorkContext {
    fn into_data_box(self) -> callables::DataBox {
        self.payload
    }
}

impl callables::HasSite for WorkContext {
    fn site(&self) -> &Site {
        &self.site
    }
}

impl callables::FromContextParts<WorkContext> for crate::OperationId {
    fn from_context_parts(context: &WorkContext) -> Result<Self, callables::CallError> {
        Ok(context.operation_id)
    }
}

impl callables::FromContextParts<WorkContext> for crate::tasks::TaskId {
    fn from_context_parts(context: &WorkContext) -> Result<Self, callables::CallError> {
        Ok(context.record.id())
    }
}

impl callables::IntoArgPart for crate::tasks::TaskId {
    fn into_arg_part() -> callables::ArgPart {
        callables::ArgPart::Ignore
    }
}
