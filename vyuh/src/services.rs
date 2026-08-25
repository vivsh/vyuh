use std::{
    any::TypeId,
    ops::Deref,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
};

use indexmap::IndexMap;
use serde::Serialize;

use crate::{
    Site,
    callables::{self, FromSite, IntoArgPart},
    db::DbPool,
    site::PartialSite,
};

#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error("Service already registered for type: {0}")]
    AlreadyRegistered(&'static str),

    #[error("Service not found for type: {0}")]
    NotFound(String),

    #[error("Handler returned an unexpected output type")]
    UnexpectedOutput,

    #[error("Service Arc was shared when exclusive access was required during build")]
    ArcShared,

    #[error("Facade downcast failed: stored type did not match expected Arc<T>")]
    FacadeDowncast,

    #[error(transparent)]
    CallError(#[from] callables::CallError),
}

pub struct ServiceExposer<T: Send + Sync + 'static> {
    _marker: std::marker::PhantomData<T>,
    facades: Vec<ServiceFacade>,
}

impl<T: Send + Sync + 'static> ServiceExposer<T> {
    fn new() -> Self {
        Self {
            _marker: std::marker::PhantomData,
            facades: Vec::new(),
        }
    }

    pub fn expose<U: ?Sized + Send + Sync + 'static>(
        &mut self,
        coercer: impl Fn(Arc<T>) -> Arc<U> + Send + Sync + 'static,
    ) -> Result<(), ServiceError> {
        let facade = ServiceFacade {
            type_id: TypeId::of::<U>(),
            type_name: std::any::type_name::<U>(),
            // `obj` is an Arc<dyn Any> whose concrete type is Arc<T>.
            // We downcast it to Arc<T>, then apply the user-supplied coercer.
            coerce_fn: Box::new(move |obj: AnyArc| {
                let t: Arc<T> = obj
                    .downcast::<T>()
                    .map_err(|_| ServiceError::FacadeDowncast)?;
                let u: Arc<U> = coercer(t);
                Ok(Box::new(u) as AnyBox)
            }),
        };
        self.facades.push(facade);
        Ok(())
    }
}

pub struct ServiceRunner {
    workers: Vec<ServiceWorker>,
}

impl ServiceRunner {
    fn new() -> Self {
        Self {
            workers: Vec::new(),
        }
    }

    pub fn run<H, Args>(&mut self, name: &str, handler: H) -> Result<(), ServiceError>
    where
        H: callables::Specable<Args, Output = Result<(), ServiceError>> + Send + Sync + 'static,
        Args: callables::FromContext<ServiceWorkContext> + callables::IntoArgSpecs + Send + 'static,
    {
        let worker = ServiceWorker::from_callable(name.to_string(), handler);
        self.workers.push(worker);
        Ok(())
    }
}

#[derive(Clone)]
pub struct ServiceBuildContext {
    site: PartialSite,
}

impl ServiceBuildContext {
    pub fn db(&self) -> DbPool {
        self.site.db()
    }
}

impl callables::FromContextParts<ServiceBuildContext> for ServiceBuildContext {
    fn from_context_parts(ctx: &ServiceBuildContext) -> Result<Self, callables::CallError> {
        Ok(ctx.clone())
    }
}

impl callables::FromContext<ServiceBuildContext> for ServiceBuildContext {
    fn from_context(ctx: ServiceBuildContext) -> Result<Self, callables::CallError> {
        Ok(ctx)
    }
}

impl IntoArgPart for ServiceBuildContext {
    fn into_arg_part() -> callables::ArgPart {
        callables::ArgPart::Ignore
    }
}

#[derive(Clone)]
pub struct ServiceWorkContext {
    site: Site,
}

impl ServiceWorkContext {
    pub fn site(&self) -> &Site {
        &self.site
    }
}

impl callables::HasSite for ServiceWorkContext {
    fn site(&self) -> &Site {
        &self.site
    }
}

impl callables::FromContextParts<ServiceWorkContext> for ServiceWorkContext {
    fn from_context_parts(ctx: &ServiceWorkContext) -> Result<Self, callables::CallError> {
        Ok(ctx.clone())
    }
}

impl callables::FromContext<ServiceWorkContext> for ServiceWorkContext {
    fn from_context(ctx: ServiceWorkContext) -> Result<Self, callables::CallError> {
        Ok(ctx)
    }
}

impl IntoArgPart for ServiceWorkContext {
    fn into_arg_part() -> callables::ArgPart {
        callables::ArgPart::Ignore
    }
}

impl callables::FromContextParts<ServiceBuildContext> for DbPool {
    fn from_context_parts(ctx: &ServiceBuildContext) -> Result<Self, callables::CallError> {
        Ok(ctx.db())
    }
}

impl callables::FromContext<ServiceBuildContext> for DbPool {
    fn from_context(ctx: ServiceBuildContext) -> Result<Self, callables::CallError> {
        Ok(ctx.db())
    }
}

impl IntoArgPart for DbPool {
    fn into_arg_part() -> callables::ArgPart {
        callables::ArgPart::Ignore
    }
}

#[derive(Clone)]
struct ServiceWorker {
    name: String,
    func: callables::Callable<ServiceWorkContext, ServiceError>,
    status: Arc<AtomicU8>,
}

/// Observable lifecycle state of one configured service worker.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceWorkerStatus {
    Ready,
    Running,
    Completed,
    Failed,
}

impl ServiceWorkerStatus {
    /// Returns the stable console and serialization label for this worker state.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

/// Aggregate lifecycle state of one configured service.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceStatus {
    Ready,
    Running,
    Completed,
    Failed,
}

impl ServiceStatus {
    /// Returns the stable console and serialization label for this service state.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

/// Safe console-facing metadata for one configured service worker.
#[derive(Debug, Clone, Serialize)]
pub struct ServiceWorkerInfo {
    pub name: String,
    pub status: ServiceWorkerStatus,
}

impl ServiceWorker {
    fn from_callable<H, Args>(name: String, handler: H) -> Self
    where
        H: callables::Specable<Args, Output = Result<(), ServiceError>> + Send + Sync + 'static,
        Args: callables::FromContext<ServiceWorkContext> + callables::IntoArgSpecs + Send + 'static,
    {
        let callable = callables::Callable::new(handler);

        ServiceWorker {
            name,
            func: callable,
            status: Arc::new(AtomicU8::new(ServiceWorkerStatus::Ready as u8)),
        }
    }

    /// Invokes the worker and records its terminal result for runtime inspection.
    async fn call(&self, ctx: ServiceWorkContext) -> Result<(), ServiceError> {
        self.status
            .store(ServiceWorkerStatus::Running as u8, Ordering::Release);
        let result = self.func.call(ctx).await.map(|_| ());
        self.status.store(
            match result {
                Ok(()) => ServiceWorkerStatus::Completed,
                Err(_) => ServiceWorkerStatus::Failed,
            } as u8,
            Ordering::Release,
        );
        result
    }

    fn info(&self) -> ServiceWorkerInfo {
        ServiceWorkerInfo {
            name: self.name.clone(),
            status: worker_status(self.status.load(Ordering::Acquire)),
        }
    }
}

type AnyBox = Box<dyn std::any::Any + Send + Sync>;
type AnyArc = Arc<dyn std::any::Any + Send + Sync>;

struct ServiceFacade {
    type_id: TypeId,
    type_name: &'static str,
    // Receives a clone of the concrete Arc<T> (type-erased) and produces the
    // coerced Arc<U> for the requested interface, also type-erased.
    coerce_fn: Box<dyn Fn(AnyArc) -> Result<AnyBox, ServiceError> + Send + Sync>,
}

// Stored inside the serviceengine
struct ServiceEntry {
    inner: AnyArc,
    type_name: &'static str,
    workers: Vec<ServiceWorker>,
    facades: Vec<ServiceFacade>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ServiceInfo {
    pub type_name: &'static str,
    pub facades: Vec<&'static str>,
    pub workers: Vec<ServiceWorkerInfo>,
    pub status: ServiceStatus,
}

struct ServiceRecord {
    type_name: &'static str,
    facades: Vec<&'static str>,
    workers: Vec<ServiceWorker>,
}

type ServiceBuildFuture =
    Pin<Box<dyn std::future::Future<Output = Result<ServiceEntry, ServiceError>> + Send>>;
type ServiceBuildFn = dyn Fn(ServiceBuildContext) -> ServiceBuildFuture + Send + Sync;

#[derive(Clone)]
pub struct ServiceHandler {
    type_id: TypeId,
    type_name: &'static str,
    spec: callables::CallSpec,
    build_fn: Arc<ServiceBuildFn>,
}

impl ServiceHandler {
    pub fn new<T, H, Args>(handler: H) -> Self
    where
        T: Service,
        H: callables::Specable<Args, Output = ServiceInstance<T>> + Send + Sync + 'static,
        Args:
            callables::FromContext<ServiceBuildContext> + callables::IntoArgSpecs + Send + 'static,
    {
        let spec = callables::CallSpec::new::<Args, H>(&handler);
        let callable: callables::Callable<ServiceBuildContext, ServiceError> =
            callables::Callable::new(handler);

        let build_fn = move |ctx: ServiceBuildContext| {
            let callable = callable.clone();
            Box::pin(async move {
                let output = callable.call(ctx).await?;

                let inner = output.into_any_arc();

                let mut inner_svc: Arc<T> = inner
                    .downcast::<T>()
                    .map_err(|_| ServiceError::UnexpectedOutput)?;

                let service = Arc::get_mut(&mut inner_svc).ok_or(ServiceError::ArcShared)?;
                let mut exposer = ServiceExposer::<T>::new();
                // Expose the concrete type itself for internal use. This allows services to depend
                // on each other using their concrete types, while still exposing only the intended
                // interfaces to the service engine.
                exposer.expose(std::convert::identity)?;
                T::expose(&mut exposer)?;
                let mut sr = ServiceRunner::new();
                service.run(&mut sr)?;

                let inner_t: Arc<T> = inner_svc;

                Ok(ServiceEntry {
                    inner: inner_t as AnyArc,
                    type_name: std::any::type_name::<T>(),
                    workers: sr.workers,
                    facades: exposer.facades,
                })
            })
                as Pin<
                    Box<
                        dyn std::future::Future<Output = Result<ServiceEntry, ServiceError>> + Send,
                    >,
                >
        };

        ServiceHandler {
            type_id: TypeId::of::<T>(),
            type_name: std::any::type_name::<T>(),
            spec,
            build_fn: Arc::new(build_fn),
        }
    }

    pub(crate) fn operation(&self) -> callables::Operation {
        callables::Operation::from_specs(callables::OperationKind::Service, &self.spec)
    }
}

pub trait Service: Sized + Send + Sync + 'static {
    fn expose(_exposer: &mut ServiceExposer<Self>) -> Result<(), ServiceError> {
        Ok(())
    }

    /// Registers background workers for the serving runtime.
    ///
    /// Vyuh invokes this while assembling a site, so implementations must only
    /// register work through `runner` and must not perform runtime work directly.
    fn run(&mut self, _runner: &mut ServiceRunner) -> Result<(), ServiceError> {
        Ok(())
    }
}

pub struct ServiceInstance<T: Service>(pub T);

impl<E, T: Service> callables::IntoOutput<E> for ServiceInstance<T> {
    fn into_output(self) -> Result<callables::DataBox, E> {
        Ok(callables::DataBox::new(self.0))
    }
}

impl<T: Service> callables::IntoReturnPart for ServiceInstance<T> {
    fn into_return_part() -> callables::ReturnPart {
        callables::ReturnPart::Empty
    }
}

impl<T: Service> From<T> for ServiceInstance<T> {
    fn from(t: T) -> Self {
        ServiceInstance(t)
    }
}

#[derive(Clone)]
pub struct ServiceRegistry {
    services: IndexMap<TypeId, ServiceHandler>,
}

impl Default for ServiceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ServiceRegistry {
    pub fn new() -> Self {
        Self {
            services: IndexMap::new(),
        }
    }

    pub(crate) fn register(&mut self, entry: ServiceHandler) -> Result<(), ServiceError> {
        let type_id = entry.type_id;
        if self.services.contains_key(&type_id) {
            return Err(ServiceError::AlreadyRegistered(entry.type_name));
        }
        self.services.insert(type_id, entry);
        Ok(())
    }

    pub(crate) fn merge(&mut self, other: ServiceRegistry) -> Result<(), ServiceError> {
        for entry in other.services.into_values() {
            self.register(entry)?;
        }
        Ok(())
    }
}

pub struct ServiceEngine {
    services: IndexMap<TypeId, AnyBox>,
    workers: Vec<ServiceWorker>,
    records: Vec<ServiceRecord>,
}

impl Default for ServiceEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl ServiceEngine {
    pub fn new() -> Self {
        Self {
            services: IndexMap::new(),
            workers: vec![],
            records: Vec::new(),
        }
    }

    pub(crate) async fn load(
        &mut self,
        registry: ServiceRegistry,
        partial_site: PartialSite,
    ) -> Result<(), ServiceError> {
        for (_, handler) in registry.services {
            let entry_future = (handler.build_fn)(ServiceBuildContext {
                site: partial_site.clone(),
            });
            let entry = entry_future.await?;
            let record = ServiceRecord {
                type_name: entry.type_name,
                facades: entry
                    .facades
                    .iter()
                    .map(|facade| facade.type_name)
                    .collect(),
                workers: entry.workers.clone(),
            };
            for facade in entry.facades {
                let iface = (facade.coerce_fn)(entry.inner.clone())?;
                if self.services.contains_key(&facade.type_id) {
                    return Err(ServiceError::AlreadyRegistered(facade.type_name));
                }
                self.services.insert(facade.type_id, iface);
            }
            self.workers.extend(entry.workers);
            self.records.push(record);
        }
        Ok(())
    }

    pub(crate) fn start_workers(
        &self,
        site: Site,
        joinset: &mut tokio::task::JoinSet<()>,
    ) -> Result<(), ServiceError> {
        for worker in &self.workers {
            let ctx = ServiceWorkContext { site: site.clone() };
            let worker = worker.clone();
            joinset.spawn(async move {
                if let Err(e) = worker.call(ctx).await {
                    tracing::error!("Service worker {} failed with error: {:?}", worker.name, e);
                }
            });
        }
        Ok(())
    }

    pub fn get<T: ?Sized + 'static>(&self) -> Option<Arc<T>> {
        let type_id = TypeId::of::<T>();
        self.services
            .get(&type_id)
            .and_then(|boxed_iface| boxed_iface.downcast_ref::<Arc<T>>().cloned())
    }

    pub(crate) fn infos(&self) -> Vec<ServiceInfo> {
        self.records.iter().map(ServiceRecord::info).collect()
    }
}

impl ServiceRecord {
    fn info(&self) -> ServiceInfo {
        let workers = self
            .workers
            .iter()
            .map(ServiceWorker::info)
            .collect::<Vec<_>>();
        ServiceInfo {
            type_name: self.type_name,
            facades: self.facades.clone(),
            status: service_status(&workers),
            workers,
        }
    }
}

fn service_status(workers: &[ServiceWorkerInfo]) -> ServiceStatus {
    if workers
        .iter()
        .any(|worker| matches!(worker.status, ServiceWorkerStatus::Failed))
    {
        return ServiceStatus::Failed;
    }
    if workers
        .iter()
        .any(|worker| matches!(worker.status, ServiceWorkerStatus::Running))
    {
        return ServiceStatus::Running;
    }
    if !workers.is_empty()
        && workers
            .iter()
            .all(|worker| matches!(worker.status, ServiceWorkerStatus::Completed))
    {
        return ServiceStatus::Completed;
    }
    ServiceStatus::Ready
}

fn worker_status(value: u8) -> ServiceWorkerStatus {
    match value {
        1 => ServiceWorkerStatus::Running,
        2 => ServiceWorkerStatus::Completed,
        3 => ServiceWorkerStatus::Failed,
        _ => ServiceWorkerStatus::Ready,
    }
}

#[derive(Clone)]
pub struct ServiceRef<T: ?Sized + Send + Sync + 'static> {
    inner: Arc<T>,
}

impl<T: ?Sized + Send + Sync + 'static> ServiceRef<T> {
    pub fn get_ref(&self) -> &T {
        &self.inner
    }

    pub fn into_inner(self) -> Arc<T> {
        self.inner
    }
}

impl<T: ?Sized + Send + Sync + 'static> Deref for ServiceRef<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T: ?Sized + Send + Sync + 'static> From<Arc<T>> for ServiceRef<T> {
    fn from(inner: Arc<T>) -> Self {
        Self { inner }
    }
}

impl<T: ?Sized + Send + Sync + 'static> FromSite for ServiceRef<T> {
    fn from_site(site: &Site) -> Result<Self, callables::CallError> {
        site.service::<T>()
            .map(Self::from)
            .map_err(|err| match err {
                ServiceError::NotFound(name) => callables::CallError::NotFound(name.into()),
                err => callables::CallError::Other(Box::new(err)),
            })
    }
}

impl<T: ?Sized + Send + Sync + 'static> axum::extract::FromRequestParts<Site> for ServiceRef<T> {
    type Rejection = ServiceError;

    async fn from_request_parts(
        _parts: &mut axum::http::request::Parts,
        state: &Site,
    ) -> Result<Self, Self::Rejection> {
        state.service::<T>().map(Self::from)
    }
}

impl<T: ?Sized + Send + Sync + 'static> IntoArgPart for ServiceRef<T> {
    fn into_arg_part() -> callables::ArgPart {
        callables::ArgPart::Ignore
    }
}

impl axum::response::IntoResponse for ServiceError {
    fn into_response(self) -> axum::response::Response {
        axum::response::IntoResponse::into_response(crate::errors::ErrorReport::new(
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            crate::errors::ErrorSourceKind::Application,
            "service_error",
            self.to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{ServiceStatus, ServiceWorkerInfo, ServiceWorkerStatus, service_status};

    /// Verifies worker failures take precedence in the aggregate service state.
    #[test]
    fn failed_worker_marks_service_failed() {
        let workers = [
            worker(ServiceWorkerStatus::Completed),
            worker(ServiceWorkerStatus::Failed),
        ];
        assert_eq!(service_status(&workers), ServiceStatus::Failed);
    }

    /// Verifies active workers keep a service running until every worker completes.
    #[test]
    fn running_worker_marks_service_running() {
        let workers = [
            worker(ServiceWorkerStatus::Completed),
            worker(ServiceWorkerStatus::Running),
        ];
        assert_eq!(service_status(&workers), ServiceStatus::Running);
    }

    /// Verifies services stay ready before workers start and complete after all finish.
    #[test]
    fn service_status_tracks_worker_lifecycle() {
        assert_eq!(service_status(&[]), ServiceStatus::Ready);
        let workers = [worker(ServiceWorkerStatus::Completed)];
        assert_eq!(service_status(&workers), ServiceStatus::Completed);
    }

    fn worker(status: ServiceWorkerStatus) -> ServiceWorkerInfo {
        ServiceWorkerInfo {
            name: "worker".to_string(),
            status,
        }
    }
}
