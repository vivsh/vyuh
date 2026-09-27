//! Synchronous handler registration using the common callable dispatch boundary.

use super::*;

/// Synchronous callable with the same typed argument tuples as asynchronous handlers.
/// The return is produced directly; registration may further restrict supported outputs.
pub trait SyncSpecable<Args: IntoArgSpecs>: Send {
    /// Direct return value, not an asynchronously resolved output.
    type Output: IntoReturnPart;
    /// Invokes the function synchronously with extracted arguments.
    fn call_sync(&self, args: Args) -> Self::Output;
}

macro_rules! sync_handler {
    ($tuple:ident; $($ty:ident),+) => {
        #[allow(non_snake_case)]
        impl<F, R, $($ty),+> SyncSpecable<$tuple<$($ty),+>> for F
        where
            F: Fn($($ty),+) -> R + Send + Sync,
            R: IntoReturnPart,
            $($ty: super::super::IntoArgPart),+
        {
            type Output = R;
            fn call_sync(&self, $tuple($($ty),+): $tuple<$($ty),+>) -> R {
                (self)($($ty),+)
            }
        }
    };
}

impl<F, R> SyncSpecable<()> for F
where
    F: Fn() -> R + Send + Sync,
    R: IntoReturnPart,
{
    type Output = R;
    fn call_sync(&self, _args: ()) -> R {
        (self)()
    }
}

sync_handler!(Tuple1; T1);
sync_handler!(Tuple2; T1, T2);
sync_handler!(Tuple3; T1, T2, T3);
sync_handler!(Tuple4; T1, T2, T3, T4);
sync_handler!(Tuple5; T1, T2, T3, T4, T5);
sync_handler!(Tuple6; T1, T2, T3, T4, T5, T6);

impl<C, E> Callable<C, E>
where
    C: Send + 'static,
    E: From<CallError> + Send + 'static,
{
    /// Registers a synchronous function through the existing invocation interface.
    /// Extraction and output errors propagate normally. The function must not block;
    /// no thread dispatch or preemption is introduced by this constructor.
    pub fn new_sync<H, Args>(handler: H) -> Self
    where
        H: SyncSpecable<Args> + Send + Sync + 'static,
        H::Output: IntoOutput<E> + Send + 'static,
        Args: FromContext<C> + IntoArgSpecs,
    {
        let spec = Arc::new(CallSpec::for_types::<Args, H::Output>(
            std::any::type_name::<H>(),
        ));
        let type_id = spec.payload_type().unwrap_or(TypeId::of::<()>());
        let handler = Arc::new(handler);
        let inner = Arc::new(move |ctx: C| -> HandlerFuture<E> {
            let handler = Arc::clone(&handler);
            Box::pin(async move {
                let args = Args::from_context(ctx).map_err(E::from)?;
                handler.call_sync(args).into_output()
            })
        });
        Self {
            spec,
            type_id,
            deserializer: Args::deserializer(),
            inner,
        }
    }
}
