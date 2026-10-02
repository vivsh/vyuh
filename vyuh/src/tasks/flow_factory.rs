//! Typed factory arguments and return conversion, separate from runtime extraction.

use std::sync::Arc;

use crate::{
    PartialSite,
    callables::{DataValue, IntoArgSpecs, specs::Tuple1},
};

use super::{FlowError, IntoFlow};

/// Build-only argument construction. Runtime inputs cannot satisfy this contract.
#[doc(hidden)]
pub trait FlowArguments<I>: IntoArgSpecs {
    /// Produces construction arguments, never a submitted task payload.
    fn build(site: &PartialSite) -> Self;
}

impl<I> FlowArguments<I> for () {
    fn build(_: &PartialSite) -> Self {}
}

impl<I> FlowArguments<I> for Tuple1<PartialSite> {
    fn build(site: &PartialSite) -> Self {
        Self(site.clone())
    }
}

impl crate::callables::IntoArgPart for PartialSite {
    fn into_arg_part() -> crate::callables::ArgPart {
        crate::callables::ArgPart::Ignore
    }
}

/// Inference marker distinguishing direct definitions from fallible factories.
#[doc(hidden)]
pub struct Definition;

/// Inference marker for `Result<Definition, FlowError>` factory returns.
#[doc(hidden)]
pub struct Fallible;

/// Task-specific factory return conversion; no callable-wide return rules change.
#[doc(hidden)]
pub trait FlowReturn<I: DataValue, E, M> {
    /// The immutable executable definition after successful preparation.
    type Prepared: super::Flow<Input = I>;
    /// Prepares a direct or fallible factory result exactly once.
    fn prepare(self, dispatcher: Arc<E>, limit: usize) -> Result<Self::Prepared, FlowError>;
}

impl<I: DataValue, E, F: IntoFlow<I, E>> FlowReturn<I, E, Definition> for F {
    type Prepared = F::Prepared;

    fn prepare(self, dispatcher: Arc<E>, limit: usize) -> Result<Self::Prepared, FlowError> {
        self.into_flow(dispatcher, limit)
    }
}

impl<I: DataValue, E, F: IntoFlow<I, E>> FlowReturn<I, E, Fallible> for Result<F, FlowError> {
    type Prepared = F::Prepared;

    fn prepare(self, dispatcher: Arc<E>, limit: usize) -> Result<Self::Prepared, FlowError> {
        self?.into_flow(dispatcher, limit)
    }
}
