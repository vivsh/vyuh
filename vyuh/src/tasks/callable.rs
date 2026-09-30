//! Task-only typed return conversion through the existing callable erasure boundary.

use crate::callables::specs::{Tuple1, Tuple2, Tuple3, Tuple4, Tuple5, Tuple6};
use crate::callables::{self, Callable, DataBox, IntoArgPart, IntoArgSpecs};
use std::{future::Future, sync::Arc};

/// Async Work signature without unrelated transport return metadata requirements.
#[doc(hidden)]
pub trait WorkCallable<Args: IntoArgSpecs>: Send + Sync {
    /// Typed return consumed immediately after handler completion.
    type Output;
    /// The original handler future; no extra boxed adapter future is required.
    type Future: Future<Output = Self::Output> + Send;
    /// Invokes the handler with the normal extracted argument tuple.
    fn invoke(&self, args: Args) -> Self::Future;
}

/// Synchronous factory signature; a future return cannot satisfy Flow conversion.
#[doc(hidden)]
pub trait FlowCallable<Args: IntoArgSpecs>: Send + Sync {
    /// Direct typed return.
    type Output;
    /// Invokes the non-blocking factory during site construction.
    fn invoke(&self, args: Args) -> Self::Output;
}

impl<H, R> FlowCallable<()> for H
where
    H: Fn() -> R + Send + Sync,
{
    type Output = R;

    fn invoke(&self, (): ()) -> R {
        self()
    }
}

macro_rules! task_callable {
    ($tuple:ident; $($ty:ident),+) => {
        #[allow(non_snake_case)]
        impl<H, F, R, $($ty: IntoArgPart),+> WorkCallable<$tuple<$($ty),+>> for H
        where H: Fn($($ty),+) -> F + Send + Sync, F: Future<Output = R> + Send {
            type Output = R;
            type Future = F;
            fn invoke(&self, $tuple($($ty),+): $tuple<$($ty),+>) -> F { (self)($($ty),+) }
        }
        #[allow(non_snake_case)]
        impl<H, R, $($ty: IntoArgPart),+> FlowCallable<$tuple<$($ty),+>> for H
        where H: Fn($($ty),+) -> R + Send + Sync {
            type Output = R;
            fn invoke(&self, $tuple($($ty),+): $tuple<$($ty),+>) -> R { (self)($($ty),+) }
        }
    };
}
task_callable!(Tuple1; T1);
task_callable!(Tuple2; T1, T2);
task_callable!(Tuple3; T1, T2, T3);
task_callable!(Tuple4; T1, T2, T3, T4);
task_callable!(Tuple5; T1, T2, T3, T4, T5);
task_callable!(Tuple6; T1, T2, T3, T4, T5, T6);

/// Serializes typed Work/batch returns inside the original single invocation future.
pub(super) fn work<C, H, Args>(
    handler: H,
    convert: fn(H::Output) -> DataBox,
) -> Callable<C, crate::Error>
where
    C: Send + 'static,
    H: WorkCallable<Args> + 'static,
    Args: callables::FromContext<C> + IntoArgSpecs + 'static,
{
    let handler = Arc::new(handler);
    Callable::from_invocation::<Args>(std::any::type_name::<H>(), move |ctx| {
        let handler = handler.clone();
        Box::pin(async move {
            let args = Args::from_context(ctx)?;
            Ok(convert(handler.invoke(args).await))
        })
    })
}
