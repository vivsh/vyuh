use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use vyuh::{
    Site, SiteConf, bundles,
    db::DbPool,
    routes::Html,
    services::{
        Service, ServiceBuildContext, ServiceError, ServiceExposer, ServiceHandler,
        ServiceInstance, ServiceRef, ServiceRunner,
    },
    testing::TestSite,
};

fn test_conf() -> SiteConf {
    SiteConf {
        log_init: false,
        logging: vyuh::logging::LoggingConf {
            env_prefix: None,
            rules: vec![],
        },
        ..SiteConf::default()
    }
}

#[bundles::service]
async fn fallible_greeting() -> Result<ServiceInstance<GreetingService>, ServiceError> {
    Ok(GreetingService.into())
}

#[bundles::service]
async fn failed_construction() -> Result<ServiceInstance<CounterService>, ServiceError> {
    Err(construction_error())
}

/// Preserves the application's concrete source error through the existing callable path.
fn construction_error() -> ServiceError {
    vyuh::callables::CallError::Other(Box::new(std::io::Error::new(
        std::io::ErrorKind::PermissionDenied,
        "vault is locked",
    )))
    .into()
}

/// Checks the structured site error without reducing its source to a message.
fn assert_construction_error(error: vyuh::SiteError) {
    assert!(
        matches!(
            &error,
            vyuh::SiteError::ServiceError(ServiceError::Initialization { .. })
        ),
        "unexpected construction error: {error:?}"
    );
    assert!(error.to_string().contains("CounterService"));
    assert!(error.to_string().contains("vault is locked"));
    if let vyuh::SiteError::ServiceError(ServiceError::Initialization { source, .. }) = error {
        let ServiceError::CallError(vyuh::callables::CallError::Other(source)) = *source else {
            panic!("original callable source was lost");
        };
        let source = source.downcast_ref::<std::io::Error>();
        assert_eq!(
            source.map(std::io::Error::kind),
            Some(std::io::ErrorKind::PermissionDenied)
        );
    }
}

/// Fallible direct factories retain build-context extraction and concrete service lookup.
#[tokio::test]
async fn fallible_direct_factory_builds() -> Result<(), vyuh::SiteError> {
    async fn factory(db: DbPool) -> Result<ServiceInstance<DbBackedService>, ServiceError> {
        Ok(DbBackedService { _db: db }.into())
    }
    let site = Site::build(test_conf(), bundles::bundle([bundles::service(factory)])).await?;
    assert!(site.service::<DbBackedService>().is_ok());
    site.shutdown_and_wait().await;
    Ok(())
}

/// Macro fallible factories retain both concrete and trait-object facade access.
#[tokio::test]
async fn fallible_macro_factory_exposes_facades() -> Result<(), vyuh::SiteError> {
    let site = Site::build(test_conf(), bundles::bundle! { fallible_greeting }).await?;
    assert!(site.service::<GreetingService>().is_ok());
    assert_eq!(site.service::<dyn Greeting>()?.greeting(), "hello");
    site.shutdown_and_wait().await;
    Ok(())
}

/// Direct registration propagates the original construction failure through SiteError.
#[tokio::test]
async fn fallible_direct_factory_fails() {
    let result = Site::build(
        test_conf(),
        bundles::bundle([bundles::service(failed_construction)]),
    )
    .await;
    assert!(result.is_err());
    if let Err(error) = result {
        assert_construction_error(error);
    }
}

/// Macro registration fails before the serving entrypoint can bind or start workers.
#[tokio::test]
async fn fallible_macro_factory_prevents_serving() {
    let result = Site::serve(test_conf(), bundles::bundle! { failed_construction }).await;
    assert!(result.is_err());
    if let Err(error) = result {
        assert_construction_error(error);
    }
}

/// Existing generic wrappers need no new bounds, and all three generic positions remain valid.
#[test]
fn typed_factory_registration_preserves_inference() {
    fn legacy<T, H, Args>(handler: H) -> bundles::BundlePart
    where
        T: Service,
        H: vyuh::callables::Specable<Args, Output = ServiceInstance<T>> + Send + Sync + 'static,
        Args: vyuh::callables::FromContext<ServiceBuildContext>
            + vyuh::callables::IntoArgSpecs
            + Send
            + 'static,
    {
        bundles::service::<T, H, Args>(handler)
    }
    let _ = legacy(direct_counter_service);
    let _ = legacy(|| async { CounterService::default().into() });
    let _ = bundles::service(|| async { ServiceInstance(CounterService::default()) });
    let _ = bundles::service::<CounterService, _, ()>(direct_counter_service);
    let _ = bundles::service::<GreetingService, _, ()>(fallible_greeting);
    let _ = ServiceHandler::new(direct_counter_service);
    let _ = ServiceHandler::new(fallible_greeting);
    let _ = ServiceHandler::new::<CounterService, _, ()>(direct_counter_service);
    let _ = ServiceHandler::new::<GreetingService, _, ()>(fallible_greeting);
}

struct DropProbe(Arc<AtomicUsize>);

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct OwnedService {
    _resource: DropProbe,
    worker_resource: Option<DropProbe>,
    initialized: Arc<AtomicUsize>,
    started: Arc<AtomicUsize>,
}

impl Service for OwnedService {
    fn run(&mut self, runner: &mut ServiceRunner) -> Result<(), ServiceError> {
        self.initialized.fetch_add(1, Ordering::SeqCst);
        let resource = Arc::new(self.worker_resource.take());
        let started = self.started.clone();
        runner.run("owned-worker", move |site: Site| {
            let resource = resource.clone();
            let started = started.clone();
            async move {
                started.fetch_add(1, Ordering::SeqCst);
                site.shutdown_notifier().notified().await;
                drop(resource);
                Ok(())
            }
        })
    }
}

/// A later construction failure releases earlier instances and registered worker captures.
#[tokio::test]
async fn failed_assembly_drops_owned_resources() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let initialized = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(AtomicUsize::new(0));
    let factory = owned_factory(&dropped, &initialized, &started);
    let later_calls = Arc::new(AtomicUsize::new(0));
    let later = later_calls.clone();
    let skipped = move || {
        later.fetch_add(1, Ordering::SeqCst);
        std::future::ready(ServiceInstance(GreetingService))
    };
    let failed_drops = dropped.clone();
    let failure = move || {
        let resource = DropProbe(failed_drops.clone());
        async move {
            let _resource = resource;
            Err::<ServiceInstance<CounterService>, _>(construction_error())
        }
    };
    let result = Site::build(
        test_conf(),
        bundles::bundle([
            bundles::service(factory),
            bundles::service(failure),
            bundles::service(skipped),
        ]),
    )
    .await;
    assert!(result.is_err());
    if let Err(error) = result {
        assert_construction_error(error);
    }
    assert_eq!(initialized.load(Ordering::SeqCst), 1);
    assert_eq!(started.load(Ordering::SeqCst), 0);
    assert_eq!(dropped.load(Ordering::SeqCst), 3);
    assert_eq!(later_calls.load(Ordering::SeqCst), 0);
}

/// Creates each resource inside the factory so registration does not keep extra owners alive.
fn owned_factory(
    dropped: &Arc<AtomicUsize>,
    initialized: &Arc<AtomicUsize>,
    started: &Arc<AtomicUsize>,
) -> impl Fn() -> std::future::Ready<Result<ServiceInstance<OwnedService>, ServiceError>> + Clone + use<>
{
    let dropped = dropped.clone();
    let initialized = initialized.clone();
    let started = started.clone();
    move || {
        std::future::ready(Ok(OwnedService {
            _resource: DropProbe(dropped.clone()),
            worker_resource: Some(DropProbe(dropped.clone())),
            initialized: initialized.clone(),
            started: started.clone(),
        }
        .into()))
    }
}

/// Successful fallible construction still initializes exclusively and stops workers on shutdown.
#[tokio::test]
async fn fallible_service_preserves_worker_lifecycle() -> Result<(), vyuh::SiteError> {
    let dropped = Arc::new(AtomicUsize::new(0));
    let initialized = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(AtomicUsize::new(0));
    let factory = owned_factory(&dropped, &initialized, &started);
    let site = Site::build(test_conf(), bundles::bundle([bundles::service(factory)])).await?;
    assert_eq!(initialized.load(Ordering::SeqCst), 1);
    assert_eq!(started.load(Ordering::SeqCst), 0);
    let client = TestSite::new(site.clone());
    client.start_runtime().await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while started.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|error| ServiceError::CallError(vyuh::callables::CallError::Other(Box::new(error))))?;
    site.shutdown_and_wait().await;
    drop(client);
    drop(site);
    assert_eq!(started.load(Ordering::SeqCst), 1);
    assert_eq!(dropped.load(Ordering::SeqCst), 2);
    Ok(())
}

#[derive(Default)]
struct CounterService {
    value: AtomicUsize,
}

impl CounterService {
    fn increment(&self) -> usize {
        self.value.fetch_add(1, Ordering::SeqCst) + 1
    }
}

impl Service for CounterService {}

#[bundles::service]
async fn macro_counter_service() -> ServiceInstance<CounterService> {
    CounterService::default().into()
}

async fn direct_counter_service() -> ServiceInstance<CounterService> {
    CounterService::default().into()
}

#[bundles::route(path = "/count")]
async fn count(counter: ServiceRef<CounterService>) -> Html<String> {
    Html(counter.increment().to_string())
}

#[tokio::test]
async fn services_can_be_retrieved_from_site() {
    let site = vyuh::Site::build(
        test_conf(),
        bundles::bundle! {
            macro_counter_service,
        },
    )
    .await
    .unwrap();

    let counter = site.service::<CounterService>().unwrap();
    assert_eq!(counter.increment(), 1);
    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn services_ref_works_in_routes() {
    let site = vyuh::Site::build(
        test_conf(),
        bundles::bundle! {
            macro_counter_service,
            count,
        },
    )
    .await
    .unwrap();
    let client = TestSite::new(site.clone());

    client
        .get("/count")
        .send()
        .await
        .assert_text(axum::http::StatusCode::OK, "1")
        .await;
    client
        .get("/count")
        .send()
        .await
        .assert_text(axum::http::StatusCode::OK, "2")
        .await;
    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn services_direct_registration_matches_macro_registration() {
    let site = vyuh::Site::build(
        test_conf(),
        bundles::bundle([bundles::service(direct_counter_service)]),
    )
    .await
    .unwrap();

    let counter = site.service::<CounterService>().unwrap();
    assert_eq!(counter.increment(), 1);
    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn services_duplicate_concrete_services_fail_site_build() {
    async fn one() -> ServiceInstance<CounterService> {
        CounterService::default().into()
    }

    async fn two() -> ServiceInstance<CounterService> {
        CounterService::default().into()
    }

    let err = vyuh::Site::build(
        test_conf(),
        bundles::bundle([bundles::service(one), bundles::service(two)]),
    )
    .await
    .unwrap_err();

    assert!(
        err.to_string()
            .contains("Service already registered for type:")
    );
    assert!(err.to_string().contains("Register each concrete service"));
}

trait Greeting: Send + Sync {
    fn greeting(&self) -> &'static str;
}

struct GreetingService;

impl Greeting for GreetingService {
    fn greeting(&self) -> &'static str {
        "hello"
    }
}

impl Service for GreetingService {
    fn expose(exposer: &mut ServiceExposer<Self>) -> Result<(), ServiceError> {
        exposer.expose(|service| service as Arc<dyn Greeting>)
    }
}

#[bundles::service]
async fn greeting_service() -> ServiceInstance<GreetingService> {
    GreetingService.into()
}

#[tokio::test]
async fn services_trait_facade_exposure_returns_trait_object() {
    let site = vyuh::Site::build(
        test_conf(),
        bundles::bundle! {
            greeting_service,
        },
    )
    .await
    .unwrap();

    let greeting = site.service::<dyn Greeting>().unwrap();
    assert_eq!(greeting.greeting(), "hello");
    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn services_duplicate_trait_facades_fail_site_build() {
    struct OtherGreetingService;

    impl Greeting for OtherGreetingService {
        fn greeting(&self) -> &'static str {
            "other"
        }
    }

    impl Service for OtherGreetingService {
        fn expose(exposer: &mut ServiceExposer<Self>) -> Result<(), ServiceError> {
            exposer.expose(|service| service as Arc<dyn Greeting>)
        }
    }

    async fn other_greeting_service() -> ServiceInstance<OtherGreetingService> {
        OtherGreetingService.into()
    }

    let err = vyuh::Site::build(
        test_conf(),
        bundles::bundle([
            bundles::service(greeting_service),
            bundles::service(other_greeting_service),
        ]),
    )
    .await
    .unwrap_err();

    assert!(format!("{err:?}").contains("dyn services::Greeting"));
}

struct DbBackedService {
    _db: DbPool,
}

impl Service for DbBackedService {}

async fn db_backed_service(db: DbPool) -> ServiceInstance<DbBackedService> {
    DbBackedService { _db: db }.into()
}

struct ContextBuiltService {
    _db: DbPool,
}

impl Service for ContextBuiltService {}

async fn context_built_service(ctx: ServiceBuildContext) -> ServiceInstance<ContextBuiltService> {
    ContextBuiltService { _db: ctx.db() }.into()
}

#[tokio::test]
async fn services_build_handlers_can_extract_db_pool() {
    let site = vyuh::Site::build(
        test_conf(),
        bundles::bundle([bundles::service(db_backed_service)]),
    )
    .await
    .unwrap();

    assert!(site.service::<DbBackedService>().is_ok());
    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn services_build_handlers_can_extract_build_context() {
    let site = vyuh::Site::build(
        test_conf(),
        bundles::bundle([bundles::service(context_built_service)]),
    )
    .await
    .unwrap();

    assert!(site.service::<ContextBuiltService>().is_ok());
    site.shutdown_and_wait().await;
}

struct WorkerProbe {
    calls: Arc<AtomicUsize>,
}

impl Service for WorkerProbe {
    fn run(&mut self, runner: &mut ServiceRunner) -> Result<(), ServiceError> {
        let calls = self.calls.clone();
        runner.run("probe-worker", move |site: Site| {
            let calls = calls.clone();
            async move {
                let _ = site.uptime();
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
    }
}

async fn worker_probe_service() -> ServiceInstance<WorkerProbe> {
    WorkerProbe {
        calls: Arc::new(AtomicUsize::new(0)),
    }
    .into()
}

#[tokio::test]
async fn services_worker_starts_only_after_test_runtime_opt_in() {
    let site = vyuh::Site::build(
        test_conf(),
        bundles::bundle([bundles::service(worker_probe_service)]),
    )
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;
    let probe = site.service::<WorkerProbe>().unwrap();
    assert_eq!(probe.calls.load(Ordering::SeqCst), 0);

    let client = TestSite::new(site.clone());
    client.start_runtime().await.unwrap();
    client.start_runtime().await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(probe.calls.load(Ordering::SeqCst), 1);
    site.shutdown_and_wait().await;
}

struct FailingWorkerProbe {
    calls: Arc<AtomicUsize>,
}

impl Service for FailingWorkerProbe {
    fn run(&mut self, runner: &mut ServiceRunner) -> Result<(), ServiceError> {
        let calls = self.calls.clone();
        runner.run("failing-worker", move || {
            let calls = calls.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(ServiceError::NotFound("expected worker failure".into()))
            }
        })
    }
}

async fn failing_worker_probe_service() -> ServiceInstance<FailingWorkerProbe> {
    FailingWorkerProbe {
        calls: Arc::new(AtomicUsize::new(0)),
    }
    .into()
}

#[tokio::test]
async fn services_worker_error_runs_only_after_test_runtime_opt_in() {
    let site = vyuh::Site::build(
        test_conf(),
        bundles::bundle([bundles::service(failing_worker_probe_service)]),
    )
    .await
    .unwrap();

    let client = TestSite::new(site.clone());
    client.start_runtime().await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let probe = site.service::<FailingWorkerProbe>().unwrap();
    assert_eq!(probe.calls.load(Ordering::SeqCst), 1);
    site.shutdown_and_wait().await;
}
