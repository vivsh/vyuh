use crate::{
    Error, Site,
    callables::{self, CallError, Callable, DataBox, HasSite, IntoDataBox},
};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::{any::TypeId, collections::HashMap, sync::Arc};

/// Registered handler for one signal payload type.
///
/// Handlers are built by bundle registration and dispatched by `SignalEngine`.
#[derive(Clone)]
pub struct SignalHandler {
    type_id: TypeId,
    func: Callable<SignalContext, Error>,
    operation_id: crate::OperationId,
}

impl SignalHandler {
    async fn call(&self, ctx: SignalContext) -> Result<(), Error> {
        self.func.call(ctx).await?;
        Ok(())
    }
}

/// Context passed to a signal handler invocation.
///
/// It carries the site handle and the emitted payload. Handler arguments
/// extract typed `Data<T>` and site-derived values from this context.
#[derive(Clone)]
pub struct SignalContext {
    site: Site,
    payload: DataBox,
    operation_id: crate::OperationId,
}

impl HasSite for SignalContext {
    fn site(&self) -> &Site {
        &self.site
    }
}

impl callables::FromContextParts<SignalContext> for crate::OperationId {
    fn from_context_parts(context: &SignalContext) -> Result<Self, CallError> {
        Ok(context.operation_id)
    }
}

/// Configuration recorded for a signal handler registration.
///
/// Signal handlers currently have no runtime knobs, but the struct keeps the
/// bundle API extensible without changing registration shape later.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct SignalConf {}

pub(crate) struct Signaller {
    pub(crate) handler: SignalHandler,
    pub(crate) options: SignalConf,
    operation: crate::callables::Operation,
}

impl Signaller {
    pub(crate) fn operation(&self) -> crate::callables::Operation {
        self.operation.clone().with_conf(&self.options)
    }
}

pub(crate) fn signal<T, H, Args>(handler: H, options: SignalConf) -> Signaller
where
    T: callables::DataValue,
    H: callables::Specable<Args> + Send + Sync + 'static,
    H::Output: callables::IntoOutput<Error> + callables::IntoReturnPart + Send + 'static,
    Args: callables::FromContext<SignalContext>
        + callables::IntoArgSpecs
        + callables::HasData<T>
        + Send
        + 'static,
{
    let callable = Callable::new(handler);
    let operation = crate::callables::Operation::from_specs(
        crate::callables::OperationKind::Signal,
        callable.inspect(),
    );

    Signaller {
        handler: SignalHandler {
            func: callable,
            type_id: TypeId::of::<T>(),
            operation_id: operation.id,
        },
        options,
        operation,
    }
}

impl IntoDataBox for SignalContext {
    fn into_data_box(self) -> DataBox {
        self.payload
    }
}

/// Errors that can occur during signal registration and dispatch.
#[derive(Debug, thiserror::Error)]
pub enum SignalError {
    #[error("Signal data type mismatch")]
    DataTypeMismatch,

    #[error(transparent)]
    CallError(#[from] CallError),
}

/// Central registry for signal handlers that dispatches events asynchronously.
pub struct SignalRegistry {
    handlers: HashMap<TypeId, Vec<SignalHandler>>,
}

impl fmt::Debug for SignalRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SignalRegistry")
            .field("handlers", &self.handlers.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl SignalRegistry {
    /// Creates a new, empty signal engine.
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
        }
    }

    /// Registers a handler for the named signal; all handlers for a signal must use the same data type.
    pub(crate) fn register(&mut self, signaller: Signaller) {
        let type_id = signaller.handler.type_id;
        let entry = self.handlers.entry(type_id).or_default();
        entry.push(signaller.handler);
    }

    pub(crate) fn merge(&mut self, other: SignalRegistry) {
        for (name, other_container) in other.handlers.into_iter() {
            let entry = self.handlers.entry(name).or_default();
            entry.extend(other_container);
        }
    }

    /// Creates a signal engine from this registry.
    /// Any changes to the registry after this call will not affect the engine.
    pub fn engine(&self) -> SignalEngine {
        let registry = Self {
            handlers: self.handlers.clone(),
        };
        SignalEngine::new(registry)
    }
}

/// Site-scoped signal client.
///
/// Signals are fire-and-forget in-process notifications. Emitting a signal
/// queues dispatch on the site's runtime and also offers the payload to channel
/// subscribers for that type. Vyuh does not guarantee delivery, ordering,
/// retries, durability, or handler completion for signals.
#[derive(Clone)]
pub struct SignalClient {
    site: Site,
    engine: SignalEngine,
}

impl SignalClient {
    pub(crate) fn new(site: Site, engine: SignalEngine) -> Self {
        Self { site, engine }
    }

    /// Emits a typed signal through the site's fire-and-forget signal path.
    ///
    /// The payload is queued on the site runtime, delivered to registered
    /// signal handlers, and offered to channel subscribers for the same Rust
    /// data type. The call returns `Ok(())` even when no handler or channel
    /// consumes the payload.
    pub fn emit<T>(&self, item: T) -> Result<(), SignalError>
    where
        T: callables::DataValue,
    {
        let item = Arc::new(item);
        self.emit_arc(item);
        Ok(())
    }

    fn emit_arc<T>(&self, item: Arc<T>)
    where
        T: callables::DataValue,
    {
        let site = self.site.clone();
        let engine = self.engine.clone();
        self.site.spawn(async move {
            dispatch_signal(site, engine, item).await;
        });
    }
}

async fn dispatch_signal<T>(site: Site, engine: SignalEngine, item: Arc<T>)
where
    T: callables::DataValue,
{
    if engine.has_handler(std::any::TypeId::of::<T>()) {
        engine
            .dispatch_data_fire_and_forget(site.clone(), DataBox::from_arc(Arc::clone(&item)))
            .await;
    }
    if let Err(err) = site.channels().publish_signal(item.as_ref()).await {
        tracing::error!("Error delivering signal to channels: {}", err);
    }
}

/// Immutable signal dispatcher built from registered signal handlers.
///
/// The engine is site-scoped and in-process. It does not provide durability,
/// retries, ordering guarantees, or delayed scheduling.
#[derive(Clone)]
pub struct SignalEngine {
    registry: Arc<SignalRegistry>,
}

impl SignalEngine {
    /// Creates a signal engine from a finalized registry snapshot.
    pub fn new(registry: SignalRegistry) -> Self {
        Self {
            registry: Arc::new(registry),
        }
    }

    pub(crate) fn has_handler(&self, type_id: TypeId) -> bool {
        self.registry.handlers.contains_key(&type_id)
    }

    pub(crate) async fn dispatch_data_fire_and_forget(&self, site: Site, payload: DataBox) {
        if let Err(err) = self.dispatch_payload(site, payload).await {
            tracing::error!("Error dispatching signal: {}", err);
        }
    }

    /// Dispatches a signal with the given data to all registered handlers asynchronously.
    pub(crate) async fn dispatch_payload(
        &self,
        site: Site,
        payload: DataBox,
    ) -> Result<(), SignalError> {
        let type_id = payload.payload_type_id();
        let handlers = match self.registry.handlers.get(&type_id) {
            Some(handlers) => handlers,
            None => return Ok(()),
        };

        for handler in handlers.iter() {
            let ctx = SignalContext {
                site: site.clone(),
                payload: payload.clone(),
                operation_id: handler.operation_id,
            };
            if let Err(err) = handler.call(ctx).await {
                tracing::error!("Error executing signal handler: {}", err);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::callables::Data;
    use schemars::JsonSchema;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
    struct TestSignal {
        value: usize,
    }

    #[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
    struct TestChannelSignal {
        user_id: usize,
    }

    /// Verifies each signal invocation receives its handler's canonical operation ID.
    #[tokio::test]
    async fn signal_extracts_operation_id() -> Result<(), String> {
        let seen = Arc::new(parking_lot::Mutex::new(None));
        let captured = Arc::clone(&seen);
        let handler = move |id: crate::OperationId, _data: Data<TestSignal>| {
            let captured = Arc::clone(&captured);
            async move {
                *captured.lock() = Some(id);
            }
        };
        let signaller = signal::<TestSignal, _, _>(handler, SignalConf::default());
        let expected = signaller.operation().id;
        let mut registry = SignalRegistry::new();
        registry.register(signaller);
        let site = crate::Site::build(
            crate::SiteConf::default().log_init(false),
            crate::bundles::Bundle::new(),
        )
        .await
        .map_err(|error| error.to_string())?;
        registry
            .engine()
            .dispatch_payload(site, DataBox::new(TestSignal { value: 1 }))
            .await
            .map_err(|error| error.to_string())?;
        assert_eq!(*seen.lock(), Some(expected));
        Ok(())
    }

    static HANDLER_COUNT: AtomicUsize = AtomicUsize::new(0);

    async fn record_signal(_payload: Data<TestSignal>) {}

    async fn count_signal(_payload: Data<TestSignal>) {
        HANDLER_COUNT.fetch_add(1, Ordering::SeqCst);
    }

    #[test]
    fn direct_registration_records_signal_operation() {
        let signaller = signal::<TestSignal, _, _>(record_signal, SignalConf::default());
        let op = signaller.operation();

        assert_eq!(op.kind, crate::callables::OperationKind::Signal);
        assert!(!op.hidden);
        assert_eq!(op.path, "");
    }

    #[test]
    fn registry_engine_detects_registered_data_type() {
        let signaller = signal::<TestSignal, _, _>(record_signal, SignalConf::default());
        let mut registry = SignalRegistry::new();
        registry.register(signaller);

        let engine = registry.engine();
        assert!(engine.has_handler(TypeId::of::<TestSignal>()));
        assert!(!engine.has_handler(TypeId::of::<String>()));
    }

    #[test]
    fn merge_appends_handlers_for_same_data_type() {
        async fn first(_payload: Data<TestSignal>) {}
        async fn second(_payload: Data<TestSignal>) {}

        let mut left = SignalRegistry::new();
        left.register(signal::<TestSignal, _, _>(first, SignalConf::default()));

        let mut right = SignalRegistry::new();
        right.register(signal::<TestSignal, _, _>(second, SignalConf::default()));

        left.merge(right);

        assert_eq!(
            left.handlers.get(&TypeId::of::<TestSignal>()).map(Vec::len),
            Some(2)
        );
    }

    #[tokio::test]
    async fn emit_without_consumers_is_ok() -> Result<(), Box<dyn std::error::Error>> {
        let site = crate::Site::build(
            crate::SiteConf::default().log_init(false),
            crate::bundles::bundle([]),
        )
        .await?;

        site.signals().emit(TestSignal { value: 1 })?;
        site.shutdown_and_wait().await;
        Ok(())
    }

    #[tokio::test]
    async fn emit_reaches_signal_handlers() -> Result<(), Box<dyn std::error::Error>> {
        HANDLER_COUNT.store(0, Ordering::SeqCst);
        let bundle = crate::bundles::bundle([crate::bundles::signal::<TestSignal, _, _>(
            count_signal,
            SignalConf::default(),
        )]);
        let site = crate::Site::build(crate::SiteConf::default().log_init(false), bundle).await?;

        site.signals().emit(TestSignal { value: 1 })?;
        wait_for_count(&HANDLER_COUNT, 1).await?;
        site.shutdown_and_wait().await;
        Ok(())
    }

    #[tokio::test]
    async fn emit_reaches_channel_users() -> Result<(), Box<dyn std::error::Error>> {
        let site = crate::Site::build(
            crate::SiteConf::default().log_init(false),
            crate::bundles::bundle([]),
        )
        .await?;
        let stream = site
            .channels()
            .user(crate::channels::UserKey::new("42")?)
            .deliver_if::<TestChannelSignal, _>(|event| event.user_id == 42);
        let mut open = site.channels().open_stream(stream, None).await?;

        site.signals().emit(TestChannelSignal { user_id: 42 })?;
        let event =
            match tokio::time::timeout(std::time::Duration::from_millis(100), open.receiver.recv())
                .await?
            {
                Some(event) => event,
                None => return Err("channel closed before event arrived".into()),
            };
        assert_eq!(event.event_type, "TestChannelSignal");
        site.shutdown_and_wait().await;
        Ok(())
    }

    async fn wait_for_count(
        counter: &'static AtomicUsize,
        expected: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        tokio::time::timeout(std::time::Duration::from_millis(250), async {
            while counter.load(Ordering::SeqCst) < expected {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await?;
        Ok(())
    }
}
