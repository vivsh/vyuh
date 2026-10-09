//! Typed factory arguments and return conversion, separate from runtime extraction.

use crate::callables::{self, Callable, Data};
use crate::tasks::{Flow, FlowCallable, FlowContext};
use crate::tasks::{FlowError, IntoFlow};
use crate::{
    PartialSite,
    callables::{DataValue, IntoArgSpecs, specs::Tuple1},
};
use std::sync::Arc;
use std::{
    any::{Any, TypeId},
    collections::HashMap,
};

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
    type Prepared: crate::tasks::Flow<Input = I>;
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

pub(in crate::tasks) type DispatcherScratch = HashMap<TypeId, Arc<dyn Any + Send + Sync>>;
pub(in crate::tasks) type BuiltFlow = Callable<FlowContext, crate::Error>;
pub(in crate::tasks) type BuildFlow =
    Arc<dyn Fn(&PartialSite, &mut DispatcherScratch) -> Result<BuiltFlow, FlowError> + Send + Sync>;

/// Builds a dispatcher once by concrete type; scratch is discarded after site construction.
fn shared_dispatcher<E: Send + Sync + 'static>(
    site: &PartialSite,
    scratch: &mut DispatcherScratch,
    build: fn(&PartialSite) -> Result<E, FlowError>,
) -> Result<Arc<E>, FlowError> {
    if let Some(existing) = scratch.get(&TypeId::of::<E>()) {
        return existing
            .clone()
            .downcast::<E>()
            .map_err(|_| FlowError::fail("Flow policy identity mismatch"));
    }
    let dispatcher = Arc::new(build(site)?);
    scratch.insert(TypeId::of::<E>(), dispatcher.clone());
    Ok(dispatcher)
}

/// Captures only declaration inputs until site finalization replaces this closure.
pub(in crate::tasks) fn deferred<I, H, Args, E, M>(
    factory: H,
    build: fn(&PartialSite) -> Result<E, FlowError>,
    limit: usize,
) -> BuildFlow
where
    I: DataValue,
    H: FlowCallable<Args> + 'static,
    H::Output: FlowReturn<I, E, M>,
    Args: FlowArguments<I>,
    E: Send + Sync + 'static,
{
    Arc::new(move |site, scratch| {
        if !(1..=10_000).contains(&limit) {
            return Err(FlowError::fail("Invalid Flow instruction limit"));
        }
        let dispatcher = shared_dispatcher(site, scratch, build)?;
        let definition = factory
            .invoke(Args::build(site))
            .prepare(dispatcher, limit)?;
        Ok(executable::<_, H>(definition))
    })
}

/// Creates input metadata from `Flow::Input`, retaining the original factory identity.
fn executable<F: Flow, H>(definition: F) -> Callable<FlowContext, crate::Error> {
    let definition = Arc::new(definition);
    Callable::from_invocation::<Tuple1<Data<F::Input>>>(
        std::any::type_name::<H>(),
        move |context: FlowContext| {
            let definition = definition.clone();
            Box::pin(async move {
                let input = context
                    .payload
                    .downcast_arc::<F::Input>()
                    .ok_or(callables::CallError::TypeMismatch)?;
                let continuation = crate::tasks::Continuation::decode(&context.record)?;
                let result = definition.advance(context.record.id(), Data(input), continuation);
                Ok(match result {
                    Ok(state) => state.prepare().erase(),
                    Err(error) => crate::tasks::outcome::erase_outcome(error.into_outcome()),
                })
            })
        },
    )
}
