use crate::auth::Authenticator;
use crate::bundles::{Bundle, IntoBundle};
use crate::cache::CacheRegistry;
use crate::callables::{self, DataBox};
use crate::channels::{Channels, SubscriptionRuntime};
use crate::commands::CommandRegistry;
use crate::conf::{self, SiteConf};
use crate::db::{DbError, DbPool, Notify, PgNotifyDbExt, Pool};
use crate::logging::{self, LoggingGuard};
use crate::notifiers::CancellationNotifier;
use crate::observability::Observability;
use crate::signals::SignalClient;
#[cfg(not(any(feature = "postgres", feature = "mysql", feature = "sqlite")))]
use crate::tasks::MemoryTaskStore;
use crate::tasks::{TaskDispatcher, TaskRunner, TaskStore, Tasks};
use crate::templates::{TemplateEngine, TemplateError, Templates};
use crate::{services, watch};
use axum::ServiceExt;
use axum::body::{self, Body};
use axum::extract::{Request, State};
use axum::http::Method;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use chrono_tz::Tz;
use std::net::{SocketAddr, ToSocketAddrs as _};
use std::{fmt, path::PathBuf, sync::Arc};
use tokio::sync::{OnceCell, mpsc};
use tower::ServiceExt as TowerServiceExt;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use std::path::Path;
use thiserror::Error; // Import the Path type

async fn error_report_middleware(State(site): State<Site>, req: Request, next: Next) -> Response {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let path = uri.path().to_string();
    let headers = req.headers().clone();
    let response = next.run(req).await;
    let Some(report) = response
        .extensions()
        .get::<crate::errors::ErrorReport>()
        .cloned()
    else {
        return response;
    };
    let ctx = crate::errors::ErrorContext {
        method,
        uri,
        path,
        headers,
    };
    log_server_error(&report, &ctx);
    let mut rendered = site.inner.conf.errors.render(ctx, report).await;
    for challenge in response
        .headers()
        .get_all(axum::http::header::WWW_AUTHENTICATE)
    {
        rendered
            .headers_mut()
            .append(axum::http::header::WWW_AUTHENTICATE, challenge.clone());
    }
    rendered
}

/// Records a structured attached server error without request secrets.
fn log_server_error(report: &crate::ErrorReport, context: &crate::ErrorContext) {
    if !report.status.is_server_error() {
        return;
    }
    if cfg!(debug_assertions) {
        log_debug_server_error(report, context);
        return;
    }
    log_release_server_error(report, context);
}

/// Records debug-only diagnostics for a server error in the privileged console log stream.
fn log_debug_server_error(report: &crate::ErrorReport, context: &crate::ErrorContext) {
    let Some(diagnostics) = report.debug.as_ref() else {
        log_release_server_error(report, context);
        return;
    };
    tracing::error!(
        status = report.status.as_u16(),
        source = ?report.source,
        code = %report.code,
        method = %context.method,
        path = %context.path,
        debug_details = %diagnostics.details.as_deref().unwrap_or_default(),
        debug_context = ?diagnostics.context.as_slice(),
        debug_causes = ?diagnostics.causes.as_slice(),
        debug_backtrace = %diagnostics.backtrace.as_deref().unwrap_or_default(),
        "request failed with server error"
    );
}

/// Records the release-safe classification for a server error.
fn log_release_server_error(report: &crate::ErrorReport, context: &crate::ErrorContext) {
    tracing::error!(
        status = report.status.as_u16(),
        source = ?report.source,
        code = %report.code,
        method = %context.method,
        path = %context.path,
        "request failed with server error"
    );
}

/// Renders Vyuh's 405 response after Axum has matched a route path but no method handler.
async fn method_not_allowed() -> crate::ErrorReport {
    crate::ErrorReport::method_not_allowed()
}

fn console_runtime(
    conf: &crate::console::ConsoleConf,
    routes: &crate::routes::RouteRegistry,
    assets: &crate::assets::AssetUrls,
    logging: &crate::logging::LoggingConf,
    project_dir: &Path,
    secrets: &crate::auth::SecretRing,
) -> Result<Option<crate::console::ConsoleRuntime>, crate::bundles::BundleError> {
    if !conf.enabled {
        return Ok(None);
    }
    crate::console::ConsoleRuntime::new(
        crate::routes::Routes::new(routes),
        assets,
        logging,
        project_dir,
        secrets,
    )
    .map(Some)
    .map_err(crate::bundles::BundleError::RouteRegistry)
}

#[derive(Debug, Clone)]
pub(crate) struct PartialSite {
    db: DbPool,
}

impl PartialSite {
    pub(crate) fn db(&self) -> DbPool {
        self.db.clone()
    }
}

#[derive(Error)]
pub enum SiteError {
    #[error("Database error: {0}")]
    DatabaseError(#[from] DbError),

    #[error("Service not found for type: {0}")]
    ServiceNotFound(String),

    #[error("Configuration error: {0}")]
    ConfError(#[from] conf::ConfError),

    #[error(transparent)]
    AuthBuildError(#[from] crate::auth::AuthBuildError),

    #[error("schema asset error: {0}")]
    SchemaAsset(#[from] crate::schema_assets::SchemaAssetError),

    #[error("Template file error: {0}")]
    TemplateFileError(String),

    #[error("Address resolution error: {0}")]
    AddressResolutionError(String),

    #[error("Invalid timezone: {0}")]
    TimezoneError(String),

    #[error("File watch error: {0}")]
    FileWatchError(String),

    #[error(transparent)]
    BundleError(#[from] crate::bundles::BundleError),

    #[error(transparent)]
    TemplateError(#[from] TemplateError),

    #[error("Serve error: {0}")]
    ServeError(#[from] axum::Error),

    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),

    #[error(transparent)]
    EmitterError(#[from] crate::emitters::EmitterError),

    #[error(transparent)]
    SignalError(#[from] crate::signals::SignalError),

    #[error(transparent)]
    LoggingError(#[from] logging::LoggingError),

    #[error(transparent)]
    ServiceError(#[from] services::ServiceError),

    #[error(transparent)]
    CommandError(#[from] crate::commands::CommandError),

    #[error(transparent)]
    TaskError(#[from] crate::tasks::TaskError),
}

impl fmt::Debug for SiteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CommandError(crate::commands::CommandError::Exit(output)) => {
                formatter.write_str(output.strip_prefix("Error: ").unwrap_or(output))
            }
            _ => formatter.write_str(&self.to_string()),
        }
    }
}

struct SiteBuilder {
    conf: SiteConf,
}

impl SiteBuilder {
    fn new(conf: SiteConf) -> Self {
        Self { conf }
    }

    /// Starts all site-owned background engines exactly once per site runtime.
    async fn start_runtime(site: &Site) -> Result<(), SiteError> {
        let channels = site.inner.channels.clone();
        let channel_shutdown = site.shutdown_notifier();
        site.inner.joinset.lock().spawn(async move {
            channels.run_debounce(channel_shutdown).await;
        });

        {
            let mut joinset = site.inner.joinset.lock();
            site.inner
                ._logging_guard
                .start_mail_admins(site.inner.shutdown_notifier.clone(), &mut joinset);
        }

        if site.inner.task_engine.has_tasks() {
            let task_runner = TaskRunner::new(site.inner.task_engine.clone())?;
            task_runner.initialize().await?;

            let signal_site = site.clone();
            let task_site = signal_site.clone();

            site.inner.joinset.lock().spawn(async move {
                task_runner.run(task_site).await;
            });
        }

        let signal_site = site.clone();

        let emitter_engine = site.inner.emitter_engine.clone();
        site.inner.joinset.lock().spawn(async move {
            if let Err(err) = emitter_engine.run(signal_site).await {
                tracing::error!("Emitter engine error: {}", err);
            }
        });

        site.inner
            .service_engine
            .start_workers(site.clone(), &mut site.inner.joinset.lock())?;

        Ok(())
    }

    async fn start_server(site: Site) -> Result<(), SiteError> {
        let listener = Self::bind_listener(&site).await?;
        site.start_runtime().await?;
        Self::serve_listener(site, listener).await
    }

    /// Binds the configured HTTP listener before runtime work can start.
    async fn bind_listener(site: &Site) -> Result<tokio::net::TcpListener, SiteError> {
        let host = site.inner.conf.host.clone();
        let port = site.inner.conf.port;
        let addr: SocketAddr = format!("{}:{}", host, port)
            .to_socket_addrs()
            .ok()
            .and_then(|mut iter| iter.next())
            .ok_or_else(|| {
                SiteError::AddressResolutionError(format!(
                    "Failed to resolve address for {}:{}. Ensure the address is valid.",
                    host, port
                ))
            })?;

        Ok(tokio::net::TcpListener::bind(addr).await?)
    }

    /// Runs the HTTP server and stops site-owned background work on exit.
    async fn serve_listener(
        site: Site,
        listener: tokio::net::TcpListener,
    ) -> Result<(), SiteError> {
        let make_svc =
            ServiceExt::<Request>::into_make_service_with_connect_info::<SocketAddr>(site.router());
        let touch_reload = site.inner.conf.touch_reload.clone();
        let shutdown = watch::ShutdownController::new(
            touch_reload,
            site.inner.shutdown_notifier.clone(),
            std::time::Duration::from_millis(site.inner.conf.http.shutdown.grace_period_ms),
        );
        let forced = shutdown.force_notifier();
        let server =
            axum::serve(listener, make_svc).with_graceful_shutdown(shutdown.clone().graceful());

        let forced_shutdown = tokio::select! {
            result = server => {
                result?;
                false
            },
            _ = forced.notified() => {
                tracing::warn!("Forced shutdown requested");
                true
            },
        };

        if !forced_shutdown {
            Self::drain_runtime(&site, forced.clone()).await;
            shutdown.complete();
        }

        Self::abort_runtime(&site);
        Ok(())
    }

    /// Waits for site-owned engines until they finish or forced shutdown fires.
    async fn drain_runtime(site: &Site, forced: CancellationNotifier) {
        loop {
            let empty = {
                let mut tasks = site.inner.joinset.lock();
                while tasks.try_join_next().is_some() {}
                tasks.is_empty()
            };
            if empty {
                return;
            }
            tokio::select! {
                _ = forced.notified() => return,
                _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {},
            }
        }
    }

    /// Aborts runtime work after the HTTP server stops accepting requests.
    fn abort_runtime(site: &Site) {
        site.inner.joinset.lock().abort_all();
        while site.inner.joinset.lock().try_join_next().is_some() {}
    }

    async fn build(
        &self,
        pool: Option<Pool>,
        bundle: impl IntoBundle,
    ) -> Result<SiteInner, SiteError> {
        self.conf.validate()?;
        let asset_urls = crate::assets::AssetUrls::parse(&self.conf.static_url)
            .map_err(|error| conf::ConfError::Other(error.to_string()))?;

        #[cfg(feature = "email")]
        let mail_delivery = crate::email::delivery(&self.conf.mail).map_err(|error| {
            conf::ConfError::Other(format!("mail configuration error: {error}"))
        })?;
        #[cfg(feature = "email")]
        let mail_reporter = crate::email::reporter(&self.conf.mail, mail_delivery.clone());

        let bundle = bundle.into_bundle();

        let default_audience = self
            .conf
            .auth
            .default_audience_id()
            .map_err(|error| conf::ConfError::Other(format!("Auth config error: {error}")))?;
        let project_dir = PathBuf::from(&self.conf.project_dir);

        let timezone = match &self.conf.tz {
            Some(tz_str) => tz_str
                .parse::<Tz>()
                .map_err(|_| SiteError::TimezoneError(tz_str.clone()))?,
            None => Tz::UTC,
        };

        let mut bundle = if self.conf.console.enabled {
            let console_bundle = crate::console::bundle(&self.conf.console);
            bundle.merge(console_bundle)
        } else {
            bundle
        };

        bundle.finalize_conf(default_audience)?;

        #[cfg(feature = "mcp")]
        bundle
            .mcp_engine
            .finalize(
                &bundle.ops,
                &bundle.topology,
                &mut bundle.mcp_registry,
                &mut bundle.mcp_resources,
            )
            .map_err(|error| crate::bundles::BundleError::Mcp(error.to_string()))?;

        bundle.normalize_authorization()?;

        bundle.validate()?;

        let auth_conf = self
            .conf
            .auth
            .clone()
            .extend_definitions(bundle.auth_definitions()?);
        auth_conf
            .validate_provider_names()
            .map_err(|error| conf::ConfError::Other(format!("Auth config error: {error}")))?;
        let mut effective_conf = self.conf.clone();
        effective_conf.auth = auth_conf.clone();
        effective_conf.validate()?;

        let observability = Observability::new(self.conf.observability.clone());
        validate_observability_paths(&bundle, &observability)?;

        let mut router = bundle.to_router();

        if observability.enabled() {
            let [liveness_path, readiness_path, metrics_path] = observability.paths();
            router = router
                .route(liveness_path, get(crate::observability::liveness))
                .route(readiness_path, get(crate::observability::readiness))
                .route(metrics_path, get(crate::observability::metrics));
        }

        if !bundle.asset_dirs.is_empty() {
            let assets = crate::assets::AssetServe::from_dirs(
                bundle.asset_dirs.clone(),
                crate::assets::PUBLIC_ASSETS_FOLDER,
            )
            .strip_url_prefix(asset_urls.mount_path().trim_start_matches('/'))
            .precompressed(true)
            .with_etag(true);
            router = router.nest_service(asset_urls.mount_path(), assets);
        }

        let mut template_engine =
            TemplateEngine::from_conf(&self.conf.templates, timezone, &asset_urls);

        let pool = if let Some(pool) = pool {
            DbPool::from_pool(pool)
        } else {
            DbPool::from_conf(&self.conf.database).await?
        };

        template_engine.inject_templates(&bundle)?;

        let cache = Arc::new(
            CacheRegistry::build(&self.conf.cache)
                .map_err(|err| conf::ConfError::Other(format!("Cache config error: {err}")))?,
        );
        let auth_audiences = bundle
            .ops
            .values()
            .filter_map(|operation| operation.audience.clone())
            .collect();
        let authenticator = Authenticator::new(
            &auth_conf,
            &self.conf.secret_key,
            &self.conf.secret_key_fallbacks,
            &project_dir,
            auth_audiences,
        )
        .await?;

        bundle
            .doc_engine
            .setup(&mut router, &bundle.ops, &auth_conf, &bundle.topology)?;

        #[cfg(feature = "mcp")]
        bundle.mcp_engine.setup(
            &mut router,
            &bundle.ops,
            &bundle.mcp_registry,
            &bundle.mcp_resources,
            &authenticator,
        )?;

        router = router
            .fallback(route_not_found)
            .method_not_allowed_fallback(method_not_allowed);

        let slash_router = Arc::new(
            crate::middlewares::SlashRouter::from_operations(
                bundle.ops.values().cloned(),
                self.conf.http.slash.policy,
            )
            .map_err(crate::bundles::BundleError::DocGen)?,
        );
        let route_registry =
            crate::routes::RouteRegistry::build(bundle.ops.values(), self.conf.http.slash.policy)
                .map_err(crate::bundles::BundleError::RouteRegistry)?;
        let console_runtime = console_runtime(
            &self.conf.console,
            &route_registry,
            &asset_urls,
            &self.conf.logging,
            &project_dir,
            authenticator.secret_ring(),
        )?;

        let mut bundle = bundle.with_router_unchecked(router);

        #[cfg(feature = "migrations")]
        crate::schema_assets::register(
            &mut bundle.migrations,
            &bundle.asset_dirs,
            &self.conf.database.url,
        )?;

        #[cfg(not(feature = "migrations"))]
        crate::schema_assets::reject(&bundle.asset_dirs)?;

        #[cfg(all(
            feature = "migrations",
            any(feature = "postgres", feature = "mysql", feature = "sqlite")
        ))]
        bundle
            .migrations
            .register_schema(crate::db::crate_schema(
                "vyuh",
                crate::tasks::store::database::schema::task_schema,
            ))
            .map_err(|error| crate::bundles::BundleError::Migration(Arc::new(error)))?;

        #[cfg(feature = "migrations")]
        let migration_runner = crate::commands::core::migration_runner(
            bundle.migrations.clone(),
            self.conf.database.url.clone(),
        )
        .map_err(|error| {
            conf::ConfError::Other(format!("migration runner configuration: {error}"))
        })?;

        let task_config = self.conf.tasks.clone();

        #[cfg(not(any(feature = "postgres", feature = "mysql", feature = "sqlite")))]
        if self.conf.deployment == crate::conf::DeploymentMode::Production
            && !bundle.tasks.is_empty()
        {
            return Err(conf::ConfError::Other(
                "production sites with registered tasks require a durable database backend".into(),
            )
            .into());
        }

        #[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
        let task_store = TaskStore::new(
            pool.as_sqlx().clone(),
            task_config.batch_size_value(),
            task_config.lease_duration_value(),
        );

        #[cfg(not(any(feature = "postgres", feature = "mysql", feature = "sqlite")))]
        let task_store = MemoryTaskStore::with_lease_duration(
            task_config.batch_size_value(),
            task_config.lease_duration_value(),
        );

        let task_registry = Arc::new(bundle.tasks.clone().finalize(task_config)?);

        let task_schedules = bundle.emitters.task_schedules(&task_registry)?;

        let task_dispatcher = task_registry.dispatcher(Arc::new(task_store), task_schedules);

        let signal_engine = bundle.signals.engine();

        let emitter_engine = bundle
            .emitters
            .create_engine_with_conf(self.conf.emitters.clone());

        let mut command_registry = std::mem::replace(
            &mut bundle.commands,
            crate::commands::CommandRegistry::new(),
        );
        let builtins = crate::commands::builtin_registry()?;
        for mut operation in builtins.operations() {
            operation.hidden = true;
            bundle.ops.insert(operation.id, operation);
        }
        command_registry.merge(builtins)?;

        let logging_guard = if self.conf.log_init {
            #[cfg(feature = "email")]
            {
                logging::init_tracing(&project_dir, &self.conf.logging, Some(mail_reporter))?
            }
            #[cfg(not(feature = "email"))]
            {
                logging::init_tracing(&project_dir, &self.conf.logging)?
            }
        } else {
            LoggingGuard::noop()
        };

        let operation_audiences = bundle
            .ops
            .iter()
            .filter_map(|(id, operation)| {
                operation
                    .audience_id()
                    .cloned()
                    .map(|audience| (*id, audience))
            })
            .collect();

        let mut site = SiteInner {
            _logging_guard: logging_guard,
            project_dir,
            start_time: std::time::Instant::now(),
            conf: effective_conf,
            observability,
            pool,
            shutdown_notifier: CancellationNotifier::new(),
            service_engine: services::ServiceEngine::new(),
            runtime: OnceCell::new(),
            timezone,
            authenticator,
            operation_audiences,
            cache,
            template_engine,
            slash_router,
            route_registry,
            joinset: Arc::new(parking_lot::Mutex::new(tokio::task::JoinSet::new())),
            channels: SubscriptionRuntime::new(
                self.conf.channels.clone(),
                crate::channels::beacon_keys(&bundle.ops, &bundle.beacons),
            ),
            console_runtime,
            asset_urls,
            bundle,
            #[cfg(feature = "migrations")]
            migration_runner,
            signal_engine,
            emitter_engine,
            commands: command_registry,
            task_engine: task_dispatcher,
            #[cfg(feature = "email")]
            mail_delivery,
        };

        site.load_services().await?;

        Ok(site)
    }
}

struct SiteInner {
    start_time: std::time::Instant,
    project_dir: PathBuf,
    conf: SiteConf,
    observability: Observability,
    authenticator: Authenticator,
    operation_audiences: std::collections::BTreeMap<crate::OperationId, crate::auth::AudienceId>,
    cache: Arc<CacheRegistry>,
    pool: DbPool,
    channels: SubscriptionRuntime,
    console_runtime: Option<crate::console::ConsoleRuntime>,
    asset_urls: crate::assets::AssetUrls,
    template_engine: TemplateEngine,
    slash_router: Arc<crate::middlewares::SlashRouter>,
    route_registry: crate::routes::RouteRegistry,
    timezone: Tz,
    bundle: Bundle,
    signal_engine: crate::signals::SignalEngine,
    emitter_engine: crate::emitters::EmitterEngine,
    commands: CommandRegistry,
    #[cfg(feature = "migrations")]
    migration_runner: Option<crate::commands::core::SharedMigrationRunner>,
    task_engine: TaskDispatcher<TaskStore>,
    service_engine: services::ServiceEngine,
    runtime: OnceCell<()>,
    shutdown_notifier: CancellationNotifier,
    _logging_guard: LoggingGuard,
    joinset: Arc<parking_lot::Mutex<tokio::task::JoinSet<()>>>,
    #[cfg(feature = "email")]
    mail_delivery: crate::email::SharedMailDelivery,
}

impl SiteInner {
    async fn load_services(&mut self) -> Result<(), SiteError> {
        let registry = self.bundle.services.clone();
        let partial_site = PartialSite {
            db: self.pool.clone(),
        };

        self.service_engine.load(registry, partial_site).await?;

        Ok(())
    }
}

/// A fully built Vyuh application.
///
/// `Site` owns the composed bundle, runtime configuration, services, database
/// pool, task runner, signal/emitter engines, templates, commands, console, and
/// router state used by handlers and framework subsystems.
#[derive(Clone)]
pub struct Site {
    inner: Arc<SiteInner>,
}

#[derive(Debug, Clone)]
pub struct SiteConfig(SiteConf);

impl SiteConfig {
    pub fn into_inner(self) -> SiteConf {
        self.0
    }
}

impl std::ops::Deref for SiteConfig {
    type Target = SiteConf;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<SiteConf> for SiteConfig {
    fn as_ref(&self) -> &SiteConf {
        &self.0
    }
}

impl callables::FromSite for SiteConfig {
    fn from_site(site: &Site) -> Result<Self, callables::CallError> {
        Ok(Self(site.conf().clone()))
    }
}

impl callables::IntoArgPart for SiteConfig {
    fn into_arg_part() -> callables::ArgPart {
        callables::ArgPart::Ignore
    }
}

impl std::fmt::Debug for Site {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Site")
            .field("project_dir", &self.inner.project_dir)
            .field("conf", &self.inner.conf)
            .finish()
    }
}

impl Site {
    /// Wraps finalized immutable site state.
    fn from_inner(inner: SiteInner) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Builds an inert `Site` from configuration and a bundle.
    ///
    /// The site exposes its router, database, services, and commands, but does
    /// not start task workers, emitters, PgNotify listeners, or service workers.
    pub async fn build(conf: SiteConf, bundle: impl IntoBundle) -> Result<Self, SiteError> {
        let builder = SiteBuilder::new(conf);
        let inner = builder.build(None, bundle).await?;
        Ok(Self::from_inner(inner))
    }

    /// Runs Vyuh's standard command-aware application entrypoint.
    ///
    /// With no command arguments this runs the built-in `serve` command. Other
    /// commands execute against an inert site and do not start background work.
    pub async fn run(conf: SiteConf, bundle: impl IntoBundle) -> Result<(), SiteError> {
        Self::run_with_args(conf, bundle, std::env::args().skip(1)).await
    }

    /// Builds the site and starts the HTTP server directly.
    ///
    /// This is the advanced server-only entrypoint. Prefer [`Self::run`] for
    /// ordinary application binaries that support built-in and application commands.
    pub async fn serve(conf: SiteConf, bundle: impl IntoBundle) -> Result<(), SiteError> {
        let site = Self::build(conf, bundle).await?;
        site.start().await
    }

    /// Builds an inert test site using an already-created database pool.
    ///
    /// This avoids creating a pool from configuration and leaves runtime
    /// engines stopped. Use [`crate::testing::TestSite::start_runtime`] only
    /// when an integration test must exercise background execution.
    pub async fn test(
        conf: SiteConf,
        bundle: impl IntoBundle,
        pool: Pool,
    ) -> Result<Self, SiteError> {
        let builder = SiteBuilder::new(conf);
        let inner = builder.build(Some(pool), bundle).await?;
        Ok(Self::from_inner(inner))
    }

    /// Starts runtime engines and the HTTP server for an already-built site.
    ///
    /// This consumes the site handle and returns when the server shuts down or fails.
    pub async fn start(self) -> Result<(), SiteError> {
        SiteBuilder::start_server(self).await
    }

    /// Starts background engines for Vyuh's test harness.
    ///
    /// Production applications start runtime engines only through `serve` or
    /// `start`; this crate-visible method supports deliberate test coverage.
    pub(crate) async fn start_runtime(&self) -> Result<(), SiteError> {
        self.inner
            .runtime
            .get_or_try_init(|| SiteBuilder::start_runtime(self))
            .await
            .map(|_| ())
    }

    pub(crate) async fn run_with_args(
        conf: SiteConf,
        bundle: impl IntoBundle,
        args: impl IntoIterator<Item = String>,
    ) -> Result<(), SiteError> {
        let args: Vec<String> = args.into_iter().collect();
        let (command_name, command_args) = Self::command_from_args(&args);
        let bundle = bundle.into_bundle();
        let preview_commands = Self::command_registry_for_bundle(&bundle)?;
        let command_arg_refs: Vec<&str> = command_args.iter().map(String::as_str).collect();
        if let Some(output) = preview_commands.early_output(&command_name, &command_arg_refs) {
            match output {
                Ok(output) => {
                    println!("{output}");
                    return Ok(());
                }
                Err(err) => {
                    let output = conf.errors.render_command(
                        crate::errors::ErrorCommandContext {
                            command: command_name,
                            args: command_args,
                        },
                        err.to_view(),
                    );
                    return Err(crate::commands::CommandError::Exit(output).into());
                }
            }
        }

        let site = Self::build(conf, bundle).await?;
        if let Err(err) = site.execute_command(&command_name, &command_arg_refs).await {
            let output = site.inner.conf.errors.render_command(
                crate::errors::ErrorCommandContext {
                    command: command_name,
                    args: command_args,
                },
                err.to_view(),
            );
            return Err(crate::commands::CommandError::Exit(output).into());
        }
        Ok(())
    }

    fn command_registry_for_bundle(
        bundle: &crate::bundles::Bundle,
    ) -> Result<crate::commands::CommandRegistry, crate::commands::CommandError> {
        let mut registry = bundle.commands.clone();
        registry.merge(crate::commands::builtin_registry()?)?;
        Ok(registry)
    }

    pub(crate) fn command_from_args(args: &[String]) -> (String, Vec<String>) {
        match args.first().map(String::as_str) {
            None => ("serve".to_string(), Vec::new()),
            Some("help" | "--help" | "-h") => ("help".to_string(), Vec::new()),
            Some(name) => (name.to_string(), args[1..].to_vec()),
        }
    }

    /// Return how long this site has been alive since build completion.
    /// The value is monotonic and intended for health, status, and diagnostics.
    pub fn uptime(&self) -> std::time::Duration {
        self.inner.start_time.elapsed()
    }

    /// Returns read-only access to all registered operation metadata.
    pub fn operations(&self) -> crate::Operations<'_> {
        crate::Operations::new(&self.inner.bundle.ops)
    }

    pub(crate) fn asset_dirs(&self) -> Vec<crate::embed::Dir> {
        self.inner.bundle.asset_dirs.clone()
    }

    pub(crate) fn static_asset_path(&self) -> PathBuf {
        self.inner.asset_urls.output_path()
    }

    #[cfg(feature = "migrations")]
    pub(crate) fn migration_registry(&self) -> &crate::db::MigrationRegistry {
        &self.inner.bundle.migrations
    }

    /// Returns the site-owned serialized Mool migration runner when migrations are registered.
    #[cfg(feature = "migrations")]
    pub(crate) fn migration_runner(&self) -> Option<crate::commands::core::SharedMigrationRunner> {
        self.inner.migration_runner.clone()
    }

    /// Collect URL metadata contributed by bundles.
    /// Collectors use this to decide which ordinary GET routes can be rendered as files.
    pub async fn url_info(
        &self,
    ) -> Result<Vec<crate::collectors::UrlInfo>, crate::collectors::StaticExportError> {
        self.inner.bundle.url_info.collect(self.clone()).await
    }

    /// Create a child notifier that resolves when the site begins shutdown.
    /// Long-lived handlers, services, and transports should observe this signal.
    pub fn shutdown_notifier(&self) -> CancellationNotifier {
        self.inner.shutdown_notifier.child()
    }

    /// Notify all site-managed components that shutdown has started.
    /// This does not wait for background workers to finish.
    pub fn shutdown(&self) {
        self.inner.shutdown_notifier.notify_waiters();
    }

    /// Notify site-managed components to shut down and abort remaining background tasks.
    /// Use this in tests or programmatic shutdown paths that must wait for cleanup.
    pub async fn shutdown_and_wait(&self) {
        self.inner.shutdown_notifier.notify_waiters();
        let grace = std::time::Duration::from_millis(self.inner.conf.http.shutdown.grace_period_ms);
        let forced = CancellationNotifier::new();
        let _ = tokio::time::timeout(grace, SiteBuilder::drain_runtime(self, forced)).await;
        self.inner.joinset.lock().abort_all();
        while let Some(_) = self.inner.joinset.lock().try_join_next() {}
    }

    /// Return the signal client for emitting typed in-process events.
    /// Emitted signals are dispatched to signal handlers and channel transports.
    pub fn signals(&self) -> SignalClient {
        SignalClient::new(self.clone(), self.inner.signal_engine.clone())
    }

    pub(crate) async fn dispatch_payload(&self, payload: DataBox) -> Result<(), SiteError> {
        self.inner
            .signal_engine
            .dispatch_data_fire_and_forget(self.clone(), payload.clone())
            .await;
        if let Err(err) = self.channels().publish_box(&payload).await {
            tracing::error!("Error delivering emitted signal to channels: {}", err);
        }
        Ok(())
    }

    /// Reads framework-owned task schedule state from the shared durable runtime.
    pub(crate) async fn task_schedule_snapshot(
        &self,
        names: &[String],
    ) -> Result<crate::tasks::TaskScheduleSnapshot, SiteError> {
        self.inner
            .task_engine
            .schedule_snapshot(names)
            .await
            .map_err(SiteError::from)
    }

    /// Submits one framework-owned scheduled task through the shared task runtime.
    pub(crate) async fn submit_scheduled_task(
        &self,
        schedule: &str,
        occurrence: chrono::DateTime<chrono::Utc>,
        payload: DataBox,
    ) -> Result<(), SiteError> {
        self.inner
            .task_engine
            .submit_scheduled(schedule, occurrence, payload)
            .await
            .map(|_| ())
            .map_err(SiteError::from)
    }

    pub(crate) async fn consume_notify(
        &self,
        topics: &[String],
    ) -> Result<mpsc::Receiver<Notify>, DbError> {
        let conf = &self.inner.conf.emitters;
        self.db()
            .consume_notify(
                topics,
                conf.notify_channel_capacity(),
                conf.pgnotify_reconnect_initial_ms(),
                conf.pgnotify_reconnect_max_ms(),
                self.shutdown_notifier(),
            )
            .await
    }

    pub(crate) fn spawn(&self, fut: impl std::future::Future<Output = ()> + Send + 'static) {
        self.inner.joinset.lock().spawn(fut);
    }

    /// Return the configured project directory.
    /// Relative runtime paths such as logs, uploads, and local resources are resolved from it.
    pub fn project_dir(&self) -> &Path {
        self.inner.project_dir.as_path()
    }

    /// Return the effective site configuration.
    /// Treat this as operational configuration; avoid exposing it directly to untrusted clients.
    pub fn conf(&self) -> &SiteConf {
        &self.inner.conf
    }

    /// Returns route reversal and method-aware URL resolution for this site.
    pub fn routes(&self) -> crate::routes::Routes<'_> {
        crate::routes::Routes::new(&self.inner.route_registry)
    }

    /// Returns validated public URL construction for bundle-owned assets.
    pub fn assets(&self) -> crate::assets::Assets<'_> {
        crate::assets::Assets::new(&self.inner.asset_urls)
    }

    /// Return the template rendering facade for this site.
    /// Templates are loaded from bundle-owned private `templates/**` assets.
    pub fn templates(&self) -> Templates {
        Templates::new(self.clone())
    }

    /// Return the outbound SMTP mail facade configured for this site.
    #[cfg(feature = "email")]
    pub fn mail(&self) -> crate::email::Mailer {
        crate::email::Mailer::new(self.clone(), self.inner.mail_delivery.clone())
    }

    /// Returns the automatically installed test mail outbox.
    #[cfg(all(feature = "email", feature = "test-support"))]
    pub fn mail_outbox(&self) -> crate::email::MailOutbox {
        self.inner.mail_delivery.outbox().unwrap_or_default()
    }

    pub(crate) fn template_engine(&self) -> &TemplateEngine {
        &self.inner.template_engine
    }

    /// Returns the configured provider-oriented authenticator.
    ///
    /// Routes and commands use it to authenticate scoped identities and to issue,
    /// refresh, or revoke credentials through configured providers.
    pub fn auth(&self) -> &Authenticator {
        &self.inner.authenticator
    }

    pub(crate) fn operation_audience(
        &self,
        id: crate::OperationId,
    ) -> Option<&crate::auth::AudienceId> {
        self.inner.operation_audiences.get(&id)
    }

    /// Returns the default typed cache provider configured for this site.
    pub fn cache(&self) -> crate::cache::Cache {
        self.inner.cache.default_handle()
    }

    pub(crate) fn cache_metrics(&self) -> String {
        self.inner.cache.render_metrics()
    }

    /// Return the configured timezone, defaulting to UTC.
    /// Use this for application date/time rendering and operational timestamps.
    pub fn timezone(&self) -> Tz {
        self.inner.timezone
    }

    /// Return the database pool facade.
    /// The concrete backend is selected by the crate feature and site configuration.
    pub fn db(&self) -> DbPool {
        self.inner.pool.clone()
    }

    /// Return the channel facade for live client delivery.
    /// Channels consume typed signal payloads and expose them over transports such as SSE.
    pub fn channels(&self) -> Channels {
        Channels::new(self.inner.channels.clone())
    }

    pub(crate) fn console_status(&self) -> crate::console::status::StatusOut {
        let ttl = std::time::Duration::from_secs(self.conf().console.status_cache_ttl_seconds);
        match &self.inner.console_runtime {
            Some(runtime) => runtime.status(self, ttl),
            None => crate::console::status::collect(self),
        }
    }

    pub(crate) fn console_urls(&self) -> Option<&crate::console::ViewUrls> {
        self.inner
            .console_runtime
            .as_ref()
            .map(crate::console::ConsoleRuntime::urls)
    }

    /// Returns the built per-site console log reader when the console is enabled.
    pub(crate) fn console_logs(&self) -> Option<&crate::console::logs::LogRuntime> {
        self.inner
            .console_runtime
            .as_ref()
            .map(crate::console::ConsoleRuntime::logs)
    }

    /// Returns the runtime observability registry for framework-owned routes.
    pub(crate) fn observability(&self) -> Observability {
        self.inner.observability.clone()
    }

    pub(crate) fn console_command_infos(&self) -> Vec<crate::commands::CommandInfo> {
        self.inner.commands.infos()
    }

    pub(crate) fn console_service_infos(&self) -> Vec<crate::services::ServiceInfo> {
        self.inner.service_engine.infos()
    }

    pub(crate) fn console_has_tasks(&self) -> bool {
        self.inner.task_engine.has_tasks()
    }

    /// Return the configured local file storage facade.
    /// This is used by upload handlers to persist and address saved files.
    pub fn file_storage(&self) -> crate::file_storage::LocalStorage {
        crate::file_storage::LocalStorage::from_conf(
            &self.inner.project_dir,
            &self.inner.conf.uploads,
        )
    }

    /// Resolve a registered service by type.
    /// Services are constructed once during site build and shared by handlers.
    pub fn service<T: ?Sized + 'static>(&self) -> Result<Arc<T>, services::ServiceError> {
        self.inner
            .service_engine
            .get::<T>()
            .ok_or_else(|| services::ServiceError::NotFound(std::any::type_name::<T>().to_string()))
    }

    /// Mainly needed for testing purposes.
    /// and before running the server.
    pub(crate) fn router(&self) -> axum::Router {
        let http = &self.inner.conf.http;
        let mut router = self.inner.bundle.to_router();

        router = router.layer(axum::middleware::from_fn_with_state(
            self.inner.slash_router.clone(),
            crate::middlewares::slash_middleware,
        ));

        if http.security_headers.enabled {
            router = router.layer(axum::middleware::from_fn_with_state(
                http.security_headers.clone(),
                crate::middlewares::security_headers_middleware,
            ));
        }

        if http.body_limit.enabled {
            router = router.layer(axum::middleware::from_fn_with_state(
                http.body_limit.clone(),
                crate::middlewares::body_limit_middleware,
            ));
        }

        if http.timeout.enabled {
            router = router.layer(axum::middleware::from_fn_with_state(
                http.timeout.clone(),
                crate::middlewares::timeout_middleware,
            ));
        }

        if http.request_id.enabled {
            router = router.layer(axum::middleware::from_fn_with_state(
                http.request_id.clone(),
                crate::middlewares::request_id_middleware,
            ));
        }

        if http.cors.enabled && http.cors.permissive {
            router = router.layer(CorsLayer::permissive());
        }

        if http.compression.enabled {
            router = router.layer(CompressionLayer::new());
        }

        if http.trace.enabled {
            router = router.layer(TraceLayer::new_for_http());
        }

        if self.inner.observability.enabled() {
            router = router.layer(axum::middleware::from_fn_with_state(
                self.clone(),
                crate::observability::metrics_middleware,
            ));
        }

        if http.catch_panic.enabled {
            let observability = self.inner.observability.clone();
            router = router.layer(CatchPanicLayer::custom(move |_panic| {
                observability.record_panic();
                crate::ErrorReport::internal_error().into_response()
            }));
        }

        router = router.layer(axum::middleware::from_fn_with_state(
            self.clone(),
            error_report_middleware,
        ));

        router.with_state(self.clone())
    }

    /// Return the task client for submitting durable background work.
    /// Registered task handlers determine how submitted payloads are executed.
    pub fn tasks(&self) -> Tasks {
        Tasks::new(self.inner.task_engine.clone())
    }

    pub(crate) fn task_metrics(&self) -> String {
        self.inner.task_engine.render_metrics()
    }

    pub(crate) fn task_ready(&self) -> bool {
        self.inner.task_engine.readiness()
    }

    pub(crate) fn task_health(&self) -> crate::tasks::TaskHealthSnapshot {
        self.inner.task_engine.health_snapshot()
    }

    pub(crate) async fn execute_command(
        &self,
        name: &str,
        args: &[&str],
    ) -> Result<(), crate::commands::CommandError> {
        self.inner.commands.execute(name, args, self.clone()).await
    }

    /// Return the collectors facade for bundle-owned assets and renderable pages.
    /// Use it to collect public assets or render selected URL-info routes to files.
    pub fn collectors(&self) -> crate::collectors::Collectors {
        crate::collectors::Collectors::new(self.clone())
    }

    pub(crate) async fn render_get(
        &self,
        uri: &str,
    ) -> Result<crate::collectors::RenderedResponse, crate::Error> {
        let req = Request::builder()
            .method(Method::GET)
            .uri(uri)
            .body(Body::empty())
            .map_err(crate::Error::other)?;
        let resp = self
            .router()
            .oneshot(req)
            .await
            .map_err(|err| crate::Error::invalid(format!("route rendering failed: {err}")))?;
        let status = resp.status();
        let content_type = resp
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let body = body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .map_err(crate::Error::other)?;
        Ok(crate::collectors::RenderedResponse {
            status,
            content_type,
            body,
        })
    }
}

async fn route_not_found() -> crate::ErrorReport {
    crate::ErrorReport::not_found()
}

/// Prevents framework-owned probe paths from replacing application routes.
fn validate_observability_paths(
    bundle: &Bundle,
    observability: &Observability,
) -> Result<(), SiteError> {
    if !observability.enabled() {
        return Ok(());
    }
    for path in observability.paths() {
        if bundle.ops.values().any(|operation| operation.path == path) {
            return Err(conf::ConfError::InvalidValue {
                field: "observability".into(),
                reason: format!("path {path} conflicts with an application route"),
                expected: Some("a unique probe or metrics path".into()),
            }
            .into());
        }
    }
    Ok(())
}

impl axum::extract::FromRequestParts<Site> for Site {
    type Rejection = axum::http::StatusCode;

    // Suppress the unused variable warning for `req` in `from_request`
    #[allow(unused_variables)]
    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &Site,
    ) -> Result<Self, Self::Rejection> {
        Ok(state.clone())
    }
}

impl axum::extract::FromRequestParts<Site> for SiteConfig {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        _parts: &mut axum::http::request::Parts,
        state: &Site,
    ) -> Result<Self, Self::Rejection> {
        Ok(Self(state.conf().clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::{Site, log_server_error};
    use crate::{
        Error, ErrorReport, ErrorSourceKind, SiteError, bundles,
        callables::Data,
        commands::CommandConf,
        emitters::PeriodicConf,
        errors::ErrorDebug,
        observability::ObservabilityConf,
        routes::{Json, Methods, RouteConf},
        signals::SignalConf,
        tasks::{TaskDefinition, TaskLane, TaskLaneConf, TaskLanePolicy},
        testing::TestSite,
    };
    use axum::http::{HeaderMap, Method, StatusCode, Uri};
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};
    use std::{
        collections::BTreeMap,
        fmt,
        path::PathBuf,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };
    use tracing::{
        Event, Subscriber,
        field::{Field, Visit},
    };
    use tracing_subscriber::{Layer, layer::Context, layer::SubscriberExt};

    static RUNTIME_SIGNALS: AtomicUsize = AtomicUsize::new(0);
    static RUNTIME_TASKS: AtomicUsize = AtomicUsize::new(0);
    const EMAIL_TASK_LANE: TaskLane = TaskLane::new("email");

    #[derive(Clone, Default)]
    struct ErrorEventCapture(Arc<Mutex<Vec<BTreeMap<String, String>>>>);

    impl<S> Layer<S> for ErrorEventCapture
    where
        S: Subscriber,
    {
        fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
            if *event.metadata().level() != tracing::Level::ERROR {
                return;
            }
            let mut fields = FieldCapture::default();
            event.record(&mut fields);
            if let Ok(mut events) = self.0.lock() {
                events.push(fields.0);
            }
        }
    }

    #[derive(Default)]
    struct FieldCapture(BTreeMap<String, String>);

    impl Visit for FieldCapture {
        fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
            self.0
                .insert(field.name().to_string(), format!("{value:?}"));
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            self.0.insert(field.name().to_string(), value.to_string());
        }

        fn record_u64(&mut self, field: &Field, value: u64) {
            self.0.insert(field.name().to_string(), value.to_string());
        }
    }

    #[derive(Clone, Deserialize, JsonSchema, Serialize)]
    struct RuntimeSignal;

    #[derive(Clone, Deserialize, JsonSchema, Serialize)]
    struct RuntimeTask;

    #[derive(Clone, Deserialize, JsonSchema, Serialize)]
    struct WaitArgs {}

    #[derive(Clone, Deserialize, JsonSchema, Serialize)]
    struct FailingArgs {}

    /// Builds an error report containing every debug diagnostic category.
    fn debug_report() -> ErrorReport {
        let mut report = ErrorReport::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorSourceKind::Application,
            "login_failed",
            "secret response detail",
        );
        report.debug = Some(ErrorDebug {
            details: Some("secret diagnostic".into()),
            context: vec!["secret context".into()],
            causes: vec!["secret cause".into()],
            backtrace: Some("secret backtrace".into()),
        });
        report
    }

    /// Builds a request context containing values that must never reach error logs.
    fn secret_context() -> crate::ErrorContext {
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            axum::http::HeaderValue::from_static("Bearer secret-token"),
        );
        crate::ErrorContext {
            method: Method::POST,
            uri: Uri::from_static("/login?token=secret-query"),
            path: "/login".into(),
            headers,
        }
    }

    /// Verifies attached 5xx reports expose complete diagnostics only in debug builds.
    #[test]
    fn server_error_logging_is_structured_and_redacted() -> Result<(), String> {
        let capture = ErrorEventCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let report = debug_report();
        let context = secret_context();

        tracing::subscriber::with_default(subscriber, || log_server_error(&report, &context));

        let events = capture.0.lock().map_err(|error| error.to_string())?;
        assert_eq!(events.len(), 1);
        let event = events
            .first()
            .ok_or_else(|| "missing error event".to_string())?;
        assert_eq!(event.get("status"), Some(&"500".to_string()));
        assert!(
            event
                .get("code")
                .is_some_and(|value| value.contains("login_failed"))
        );
        assert!(
            event
                .get("method")
                .is_some_and(|value| value.contains("POST"))
        );
        assert_eq!(event.get("path"), Some(&"/login".to_string()));
        let fields = format!("{event:?}");
        assert!(!fields.contains("secret-token"));
        assert!(!fields.contains("secret-query"));
        if cfg!(debug_assertions) {
            for (key, value) in [
                ("debug_details", "secret diagnostic"),
                ("debug_context", "secret context"),
                ("debug_causes", "secret cause"),
                ("debug_backtrace", "secret backtrace"),
            ] {
                assert!(event.get(key).is_some_and(|field| field.contains(value)));
            }
        } else {
            assert!(!event.keys().any(|key| key.starts_with("debug_")));
        }
        assert!(!event.contains_key("detail"));
        assert!(!event.contains_key("headers"));
        assert!(!event.contains_key("debug"));
        Ok(())
    }

    /// Verifies the final error middleware logs an attached 5xx report exactly once.
    #[tokio::test(flavor = "current_thread")]
    async fn error_middleware_logs_attached_server_error() -> Result<(), String> {
        let capture = ErrorEventCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let guard = tracing::subscriber::set_default(subscriber);
        let site = Site::build(
            crate::SiteConf::default().log_init(false),
            response_bundle(),
        )
        .await
        .map_err(|error| error.to_string())?;
        let response = TestSite::new(site)
            .get("/internal-error")
            .header("accept", "application/json")
            .send()
            .await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        drop(guard);

        let events = capture.0.lock().map_err(|error| error.to_string())?;
        assert_eq!(events.len(), 1);
        let event = events
            .first()
            .ok_or_else(|| "missing error event".to_string())?;
        assert_eq!(event.get("status"), Some(&"500".to_string()));
        assert!(
            event
                .get("code")
                .is_some_and(|value| value.contains("internal_error"))
        );
        assert_eq!(event.get("path"), Some(&"/internal-error".to_string()));
        Ok(())
    }

    async fn emit_runtime_signal() -> Data<RuntimeSignal> {
        Data::new(RuntimeSignal)
    }

    async fn count_runtime_signal(_signal: Data<RuntimeSignal>) {
        RUNTIME_SIGNALS.fetch_add(1, Ordering::SeqCst);
    }

    async fn count_runtime_task(_task: Data<RuntimeTask>) {
        RUNTIME_TASKS.fetch_add(1, Ordering::SeqCst);
    }

    /// Verifies a bundle can contribute the static lane used by its task definition.
    #[tokio::test]
    async fn bundle_task_lane_resolves_static_task_definition() -> Result<(), SiteError> {
        let bundle = bundles::bundle([bundles::task(
            count_runtime_task,
            TaskDefinition::new("email-runtime-task").lane(EMAIL_TASK_LANE),
        )])
        .with_conf(bundles::conf().task_lane(TaskLaneConf::new(EMAIL_TASK_LANE, 1)));
        let site = Site::build(crate::SiteConf::default().log_init(false), bundle).await?;
        let receipt = site.tasks().submit(RuntimeTask).await?;
        let task = site.tasks().get(receipt.id()).await?.ok_or_else(|| {
            SiteError::TaskError(crate::tasks::TaskError::TaskNotFound(
                receipt.id().to_string(),
            ))
        })?;

        assert_eq!(task.lane(), EMAIL_TASK_LANE.as_str());
        Ok(())
    }

    /// Verifies strict lane policy reports the task and unavailable static lane at site build.
    #[tokio::test]
    async fn strict_task_lane_policy_rejects_unconfigured_bundle_task() {
        let bundle = bundles::bundle([bundles::task(
            count_runtime_task,
            TaskDefinition::new("email-runtime-task").lane(EMAIL_TASK_LANE),
        )]);
        let result = Site::build(
            crate::SiteConf::default().log_init(false).tasks(
                crate::tasks::TaskConf::default().missing_lane(TaskLanePolicy::RequireConfigured),
            ),
            bundle,
        )
        .await;

        assert!(result.is_err());
        let message = result
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();
        assert!(message.contains("email-runtime-task"));
        assert!(message.contains(EMAIL_TASK_LANE.as_str()));
    }

    async fn wait_command(site: Site, _args: Data<WaitArgs>) -> Result<(), Error> {
        site.tasks()
            .submit(RuntimeTask)
            .await
            .map_err(Error::other)?;
        tokio::time::sleep(Duration::from_millis(25)).await;
        Ok(())
    }

    async fn failing_command(_args: Data<FailingArgs>) -> Result<(), Error> {
        Err(Error::from(crate::db::DbError::Mock {
            operation: "fetch users",
            reason: "table users does not exist".to_string(),
        })
        .with_context("rebuild user index"))
    }

    async fn response_probe() -> Json<&'static str> {
        Json("ok")
    }

    async fn created_response_probe() -> StatusCode {
        StatusCode::CREATED
    }

    async fn accepted_response_probe() -> StatusCode {
        StatusCode::ACCEPTED
    }

    async fn deleted_response_probe() -> StatusCode {
        StatusCode::NO_CONTENT
    }

    async fn raw_not_found() -> StatusCode {
        StatusCode::NOT_FOUND
    }

    async fn internal_error_probe() -> Result<(), Error> {
        Err(Error::new(crate::ErrorKind::Other))
    }

    fn response_bundle() -> crate::bundles::Bundle {
        bundles::bundle([
            bundles::route(
                response_probe,
                RouteConf {
                    name: "response_probe".into(),
                    methods: Methods::GET,
                    path: "/response-probe".into(),
                    slash: None,
                },
            ),
            bundles::route(
                raw_not_found,
                RouteConf {
                    name: "raw_not_found".into(),
                    methods: Methods::GET,
                    path: "/raw-not-found".into(),
                    slash: None,
                },
            ),
            bundles::route(
                internal_error_probe,
                RouteConf {
                    name: "internal_error_probe".into(),
                    methods: Methods::GET,
                    path: "/internal-error".into(),
                    slash: None,
                },
            ),
        ])
    }

    /// Registers distinct GET and POST handlers at one path for routing regression coverage.
    fn get_post_bundle() -> crate::bundles::Bundle {
        bundles::bundle([
            bundles::route(
                response_probe,
                RouteConf {
                    name: "read_shared".into(),
                    methods: Methods::GET,
                    path: "/shared".into(),
                    slash: None,
                },
            ),
            bundles::route(
                created_response_probe,
                RouteConf {
                    name: "create_shared".into(),
                    methods: Methods::POST,
                    path: "/shared".into(),
                    slash: None,
                },
            ),
        ])
    }

    /// Registers three disjoint method handlers at one path for routing regression coverage.
    fn get_patch_delete_bundle() -> crate::bundles::Bundle {
        bundles::bundle([
            bundles::route(
                response_probe,
                RouteConf {
                    name: "read_many".into(),
                    methods: Methods::GET,
                    path: "/many".into(),
                    slash: None,
                },
            ),
            bundles::route(
                accepted_response_probe,
                RouteConf {
                    name: "patch_many".into(),
                    methods: Methods::PATCH,
                    path: "/many".into(),
                    slash: None,
                },
            ),
            bundles::route(
                deleted_response_probe,
                RouteConf {
                    name: "delete_many".into(),
                    methods: Methods::DELETE,
                    path: "/many".into(),
                    slash: None,
                },
            ),
        ])
    }

    /// Verifies GET and POST handlers registered at one path build and dispatch independently.
    #[tokio::test]
    async fn shared_path_get_and_post_routes_build_and_dispatch() -> Result<(), crate::SiteError> {
        let site = Site::build(
            crate::SiteConf::default().log_init(false),
            get_post_bundle(),
        )
        .await?;
        let client = TestSite::new(site);

        assert_eq!(client.get("/shared").send().await.status(), StatusCode::OK);
        assert_eq!(
            client.post("/shared").send().await.status(),
            StatusCode::CREATED
        );
        assert_eq!(
            client
                .request(Method::HEAD, "/shared")
                .send()
                .await
                .status(),
            StatusCode::OK
        );
        Ok(())
    }

    /// Verifies GET, PATCH, and DELETE handlers at one path build and dispatch independently.
    #[tokio::test]
    async fn shared_path_get_patch_delete_routes_build_and_dispatch() -> Result<(), crate::SiteError>
    {
        let site = Site::build(
            crate::SiteConf::default().log_init(false),
            get_patch_delete_bundle(),
        )
        .await?;
        let client = TestSite::new(site);

        assert_eq!(client.get("/many").send().await.status(), StatusCode::OK);
        assert_eq!(
            client.patch("/many").send().await.status(),
            StatusCode::ACCEPTED
        );
        assert_eq!(
            client.delete("/many").send().await.status(),
            StatusCode::NO_CONTENT
        );
        Ok(())
    }

    /// Verifies the site-level fallback returns a structured 405 with Axum's `Allow` header.
    #[tokio::test]
    async fn shared_path_method_error_is_structured_with_axum_allow_header()
    -> Result<(), crate::SiteError> {
        let site = Site::build(
            crate::SiteConf::default().log_init(false),
            get_post_bundle(),
        )
        .await?;
        let response = TestSite::new(site)
            .put("/shared")
            .header("accept", "application/json")
            .send()
            .await;

        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(
            response
                .header(axum::http::header::ALLOW.as_str())
                .and_then(|value| value.to_str().ok()),
            Some("GET,HEAD,POST")
        );
        let body: serde_json::Value = response.json().await;
        assert_eq!(
            body.get("code"),
            Some(&serde_json::json!("method_not_allowed"))
        );
        Ok(())
    }

    /// Verifies duplicate path and method registration returns a bundle error instead of panicking.
    #[tokio::test]
    async fn duplicate_path_method_returns_bundle_validation_error() {
        let bundle = bundles::bundle([
            bundles::route(
                response_probe,
                RouteConf {
                    name: "first_duplicate".into(),
                    methods: Methods::GET,
                    path: "/duplicate".into(),
                    slash: None,
                },
            ),
            bundles::route(
                created_response_probe,
                RouteConf {
                    name: "second_duplicate".into(),
                    methods: Methods::GET,
                    path: "/duplicate".into(),
                    slash: None,
                },
            ),
        ]);

        assert!(matches!(
            Site::build(crate::SiteConf::default().log_init(false), bundle).await,
            Err(crate::SiteError::BundleError(
                crate::bundles::BundleError::ErrorList(errors)
            )) if matches!(
                errors.as_slice(),
                [crate::bundles::BundleError::DuplicateRoutePathMethod { .. }]
            )
        ));
    }

    /// Verifies unmatched paths and unsupported methods use the ErrorReport JSON contract.
    #[tokio::test]
    async fn route_failures_use_error_report() -> Result<(), crate::SiteError> {
        let site = Site::build(
            crate::SiteConf::default().log_init(false),
            response_bundle(),
        )
        .await?;
        let client = TestSite::new(site);

        let missing = client
            .get("/missing")
            .header("accept", "application/json")
            .send()
            .await;
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        let missing: serde_json::Value = missing.json().await;
        assert_eq!(missing.get("code"), Some(&serde_json::json!("not_found")));
        assert_eq!(missing.get("source"), Some(&serde_json::json!("framework")));

        let method = client
            .post("/response-probe")
            .header("accept", "application/json")
            .send()
            .await;
        assert_eq!(method.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(
            method
                .header(axum::http::header::ALLOW.as_str())
                .and_then(|value| value.to_str().ok()),
            Some("GET,HEAD")
        );
        let method: serde_json::Value = method.json().await;
        assert_eq!(
            method.get("code"),
            Some(&serde_json::json!("method_not_allowed"))
        );
        assert_eq!(method.get("source"), Some(&serde_json::json!("framework")));
        Ok(())
    }

    /// Verifies an explicit raw response is not rewritten into the JSON error envelope.
    #[tokio::test]
    async fn raw_error_response_is_unchanged() -> Result<(), crate::SiteError> {
        let site = Site::build(
            crate::SiteConf::default().log_init(false),
            response_bundle(),
        )
        .await?;
        let response = TestSite::new(site)
            .get("/raw-not-found")
            .header("accept", "application/json")
            .send()
            .await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(response.text().await, "");
        Ok(())
    }

    /// Verifies the public asset facade uses the configured static URL.
    #[tokio::test]
    async fn configured_static_url_builds_asset_links() -> Result<(), crate::SiteError> {
        let site = Site::build(
            crate::SiteConf::default()
                .log_init(false)
                .static_url("/public"),
            bundles::bundle([]),
        )
        .await?;

        assert_eq!(site.assets().static_url(), "/public/");
        assert_eq!(
            site.assets().url("console/app.js"),
            Ok("/public/console/app.js".to_string())
        );
        Ok(())
    }

    /// Verifies an absolute static URL keeps its CDN origin while retaining a local mount path.
    #[tokio::test]
    async fn cdn_static_url_builds_cdn_asset_links() -> Result<(), crate::SiteError> {
        let site = Site::build(
            crate::SiteConf::default()
                .log_init(false)
                .static_url("https://cdn.example.com/static")
                .console(crate::console::ConsoleConf::default().enabled(false)),
            crate::bundles::bundle([]),
        )
        .await?;

        assert_eq!(
            site.assets().static_url(),
            "https://cdn.example.com/static/"
        );
        assert_eq!(
            site.assets().url("dashboard/app.js"),
            Ok("https://cdn.example.com/static/dashboard/app.js".to_string())
        );
        assert_eq!(site.static_asset_path(), PathBuf::from("static"));
        Ok(())
    }

    fn strings(args: &[&str]) -> Vec<String> {
        args.iter().map(|arg| (*arg).to_string()).collect()
    }

    #[test]
    fn command_from_empty_args_defaults_to_serve() {
        let (command, args) = Site::command_from_args(&[]);

        assert_eq!(command, "serve");
        assert!(args.is_empty());
    }

    #[test]
    fn command_from_help_args_selects_help() {
        for input in [strings(&["help"]), strings(&["--help"]), strings(&["-h"])] {
            let (command, args) = Site::command_from_args(&input);

            assert_eq!(command, "help");
            assert!(args.is_empty());
        }
    }

    #[test]
    fn command_from_named_args_preserves_command_args() {
        let input = strings(&["greet", "--name", "Vyuh"]);
        let (command, args) = Site::command_from_args(&input);

        assert_eq!(command, "greet");
        assert_eq!(args, strings(&["--name", "Vyuh"]));
    }

    #[tokio::test]
    async fn command_help_does_not_build_site() {
        let conf = crate::SiteConf::default()
            .log_init(false)
            .project_dir("/path/that/does/not/exist");
        let result =
            Site::run_with_args(conf, crate::bundles::bundle([]), strings(&["help"])).await;

        assert!(result.is_ok(), "command failed: {result:?}");
    }

    #[tokio::test]
    async fn unknown_command_does_not_build_site() {
        let conf = crate::SiteConf::default()
            .log_init(false)
            .project_dir("/path/that/does/not/exist");
        let err = Site::run_with_args(conf, crate::bundles::bundle([]), strings(&["missing"]))
            .await
            .unwrap_err();

        assert!(err.to_string().contains("missing"));
    }

    /// Verifies a command failure formats as terminal text without enum wrappers.
    #[tokio::test]
    async fn command_failure_formats_terminal_text() -> Result<(), String> {
        let command = bundles::command::<
            FailingArgs,
            _,
            crate::callables::specs::Tuple1<Data<FailingArgs>>,
        >(failing_command, CommandConf::new("users:reindex"));
        let result = Site::run_with_args(
            crate::SiteConf::default().log_init(false),
            bundles::bundle([command]),
            strings(&["users:reindex"]),
        )
        .await;
        let error = match result {
            Err(error) => error,
            Ok(()) => return Err("command unexpectedly succeeded".to_string()),
        };
        let terminal = format!("Error: {error:?}");

        assert!(terminal.contains("rebuild user index"));
        assert!(terminal.contains("mock fetch users failed: table users does not exist"));
        assert!(!terminal.contains("CommandError(Exit"));
        assert!(!terminal.contains("Error: Error:"));
        Ok(())
    }

    /// Verifies one-shot commands do not activate emitters, signals, or task workers.
    #[tokio::test]
    async fn command_does_not_start_runtime() {
        RUNTIME_SIGNALS.store(0, Ordering::SeqCst);
        RUNTIME_TASKS.store(0, Ordering::SeqCst);
        let bundle = bundles::bundle([
            bundles::signal::<RuntimeSignal, _, _>(count_runtime_signal, SignalConf::default()),
            bundles::periodic::<RuntimeSignal, _, _>(
                emit_runtime_signal,
                PeriodicConf::new(Duration::from_millis(1)),
            ),
            bundles::task::<RuntimeTask, _, _>(
                count_runtime_task,
                TaskDefinition::new("runtime-task-probe"),
            ),
            bundles::command::<WaitArgs, _, _>(
                wait_command,
                CommandConf::new("wait-runtime-probe"),
            ),
        ]);
        let result = Site::run_with_args(
            crate::SiteConf::default().log_init(false),
            bundle,
            strings(&["wait-runtime-probe"]),
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(RUNTIME_SIGNALS.load(Ordering::SeqCst), 0);
        assert_eq!(RUNTIME_TASKS.load(Ordering::SeqCst), 0);
    }

    /// Verifies task readiness changes from in-memory runtime state without a second store probe.
    #[tokio::test]
    async fn task_readiness_waits_for_runtime_initialization() -> Result<(), SiteError> {
        let bundle = bundles::bundle([bundles::task::<RuntimeTask, _, _>(
            count_runtime_task,
            TaskDefinition::new("readiness-task"),
        )]);
        let site = Site::build(
            crate::SiteConf::default()
                .log_init(false)
                .observability(ObservabilityConf::production()),
            bundle,
        )
        .await?;
        let client = TestSite::new(site);

        assert_eq!(
            client.get("/readyz").send().await.status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        client.start_runtime().await?;
        assert_eq!(client.get("/readyz").send().await.status(), StatusCode::OK);
        client.shutdown_and_wait().await;
        Ok(())
    }
}
