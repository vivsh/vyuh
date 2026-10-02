//! One-time factory preparation and immutable invocation erasure.

use std::{
    any::{Any, TypeId},
    collections::HashMap,
    sync::Arc,
};

use crate::{
    PartialSite,
    callables::{self, Callable, Data, DataValue, specs::Tuple1},
};

use super::{Flow, FlowArguments, FlowCallable, FlowContext, FlowError, FlowReturn};

pub(super) type DispatcherScratch = HashMap<TypeId, Arc<dyn Any + Send + Sync>>;
pub(super) type BuiltFlow = (Callable<FlowContext, crate::Error>, String);
pub(super) type BuildFlow =
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
pub(super) fn deferred<I, H, Args, E, M>(
    factory: H,
    build: fn(&PartialSite) -> Result<E, FlowError>,
    limit: usize,
    revision: &'static str,
) -> BuildFlow
where
    I: DataValue,
    H: FlowCallable<Args> + 'static,
    H::Output: FlowReturn<I, E, M>,
    Args: FlowArguments<I>,
    E: Send + Sync + 'static,
{
    Arc::new(move |site, scratch| {
        if !(1..=10_000).contains(&limit) || revision.is_empty() {
            return Err(FlowError::fail(
                "Invalid Flow instruction limit or revision",
            ));
        }
        let dispatcher = shared_dispatcher(site, scratch, build)?;
        let definition = factory
            .invoke(Args::build(site))
            .prepare(dispatcher, limit)?;
        let compatibility = serde_json::to_string(&(
            "flow-factory-v1",
            definition.compatibility(),
            std::any::type_name::<E>(),
            revision,
            limit,
        ))
        .map_err(|_| FlowError::fail("Invalid Flow compatibility metadata"))?;
        Ok((executable::<_, H>(definition), compatibility))
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
                let continuation = super::Continuation::decode(&context.record)?;
                let result = definition.advance(context.record.id(), Data(input), continuation);
                Ok(match result {
                    Ok(state) => state.prepare().erase(),
                    Err(error) => super::returns::erase_outcome(error.into_outcome()),
                })
            })
        },
    )
}
