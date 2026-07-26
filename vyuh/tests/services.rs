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
        Service, ServiceBuildContext, ServiceError, ServiceExposer, ServiceInstance, ServiceRef,
        ServiceRunner,
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

    assert!(format!("{err:?}").contains("AlreadyRegistered"));
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
