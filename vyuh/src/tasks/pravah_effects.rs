//! Explicit request routing; external effects execute only inside registered Work.

use super::{FlowError, TaskOptions, TaskRuntimeError};
use crate::{
    PartialSite,
    callables::{DataBox, DataValue},
};

/// Shared, immutable application routing policy, built once per type and site.
///
/// Methods only select Work payloads/options. They must not perform external
/// effects, submit tasks, retain execution progress, or transform returned results.
pub trait PravahEffects: Send + Sync + 'static {
    /// Obtains build-time handles without blocking I/O or runtime service extraction.
    fn build(site: &PartialSite) -> Result<Self, FlowError>
    where
        Self: Sized;

    /// Selects registered Work which successfully returns `pravah::FetchResponse`.
    fn fetch(&self, request: &pravah::Fetch) -> Result<WorkRequest, FlowError>;

    /// Selects Work returning the declared resume value, or ordinary external waiting.
    fn suspend(&self, _: &pravah::Suspension) -> Result<Option<WorkRequest>, FlowError> {
        Ok(None)
    }
}

impl PravahEffects for () {
    fn build(_: &PartialSite) -> Result<Self, FlowError> {
        Ok(())
    }

    fn fetch(&self, _: &pravah::Fetch) -> Result<WorkRequest, FlowError> {
        Err(FlowError::MissingEffects)
    }
}

/// Invocation-local intent to execute one Work child through an atomic checkpoint.
/// Construction never submits a task; the framework attaches the VM snapshot.
pub struct WorkRequest {
    pub(super) input: DataBox,
    pub(super) options: TaskOptions,
}

impl WorkRequest {
    /// Selects a typed payload; registration and Work kind are checked during preparation.
    pub fn new<I: DataValue>(input: I) -> Self {
        Self {
            input: DataBox::new_data(input),
            options: TaskOptions::new(),
        }
    }

    /// Sets scheduling options, rejecting invalid combinations and `ignore_conflicts`.
    pub fn options(mut self, options: TaskOptions) -> Result<Self, TaskRuntimeError> {
        super::dispatcher::validate_spawn_options(&options)?;
        self.options = options;
        Ok(self)
    }

    /// Resolves existing private child preparation and rejects Flow targets before storage.
    pub(super) fn prepare(
        self,
        state: &str,
        site: &crate::Site,
    ) -> Result<super::TaskOutcome, TaskRuntimeError> {
        let outcome = site
            .tasks()
            .prepare_child(&self.input, state, &self.options)?;
        if let super::TaskOutcome::Spawn { child, .. } = &outcome
            && child.record.kind != super::TaskKind::Work
        {
            return Err(TaskRuntimeError::InvalidOptions(
                "Flow effects must target Work".into(),
            ));
        }
        Ok(outcome)
    }
}
