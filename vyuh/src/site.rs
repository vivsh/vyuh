use crate::auth::Authenticator;
use crate::bundles::{Bundle, IntoBundle};
use crate::callables::{self, DataBox};
use crate::channels::{Channels, LocalChannelBackend};
use crate::commands::CommandRegistry;
use crate::conf::{self, SiteConf};
use crate::db::{DbError, DbPool, Notify, PgNotifyDbExt, Pool};
use crate::emitters::EmitTarget;
use crate::logging::{self, LoggingGuard};
use crate::notifiers::CancellationNotifier;
use crate::observability::Observability;
use crate::signals::SignalClient;
#[cfg(not(any(feature = "postgres", feature = "mysql", feature = "sqlite")))]
use crate::tasks::MemoryTaskStore;
use crate::tasks::{TaskClient, TaskDispatcher, TaskRunner, TaskStore};
use crate::templates::{TemplateEngine, TemplateError, Templates};
use crate::{services, watch};
use axum::ServiceExt;
use axum::body::{self, Body};
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use chrono_tz::Tz;
use std::net::{SocketAddr, ToSocketAddrs as _};
use std::{path::PathBuf, sync::Arc};
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
    site.inner.conf.errors.render(ctx, report).await
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

#[derive(Debug, Error)]
pub enum SiteError {
    #[error("Database error: {0}")]
    DatabaseError(#[from] DbError),

    #[error("Service not found for type: {0}")]
    ServiceNotFound(String),

    #[error("Configuration error: {0}")]
    ConfError(#[from] conf::ConfError),

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
        if site.inner.task_engine.has_tasks() {
            let task_runner = TaskRunner::new(site.inner.task_engine.clone());

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

        tokio::select! {
            result = server => {
                shutdown.complete();
                result?;
            },
            _ = forced.notified() => {
                tracing::warn!("Forced shutdown requested");
            },
        }

        Self::abort_runtime(&site);
        Ok(())
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

        let mut bundle = bundle.into_bundle();

        let default_audience = self
            .conf
            .auth
            .default_audience_id()
            .map_err(|error| conf::ConfError::Other(format!("Auth config error: {error}")))?;
        bundle.apply_default_audience(default_audience.clone());

        bundle.validate()?;

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

        bundle.apply_default_audience(default_audience);

        bundle.validate()?;

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
            .strip_url_prefix(crate::assets::PUBLIC_ASSETS_URL_PREFIX.trim_start_matches('/'))
            .precompressed(true)
            .with_etag(true);
            router = router.nest_service(crate::assets::PUBLIC_ASSETS_URL_PREFIX, assets);
        }

        let mut template_engine = TemplateEngine::from_conf(&self.conf.templates, timezone);

        let pool = if let Some(pool) = pool {
            DbPool::from_pool(pool)
        } else {
            DbPool::from_conf(&self.conf.database).await?
        };

        template_engine.inject_templates(&bundle)?;

        let mut authenticator = Authenticator::new(
            &self.conf.auth,
            &self.conf.secret_key,
            &self.conf.secret_key_fallbacks,
            &project_dir,
        )
        .await
        .map_err(|err| conf::ConfError::Other(format!("Auth config error: {err}")))?;

        if self.conf.console.enabled {
            let login = crate::console::auth::login_provider();
            let cookie = crate::auth::CookieConf::new(&self.conf.console.cookie_name)
                .path(&self.conf.console.path)
                .secure(self.conf.console.secure_cookie);
            let token = crate::console::auth::token_provider(cookie);
            authenticator
                .register_provider(crate::console::auth::CONSOLE_LOGIN, login)
                .map_err(|err| conf::ConfError::Other(format!("Console auth error: {err}")))?;
            authenticator
                .register_provider(crate::console::auth::CONSOLE_TOKEN, token)
                .map_err(|err| conf::ConfError::Other(format!("Console auth error: {err}")))?;
        }

        bundle
            .doc_engine
            .setup(&mut router, &bundle.ops, &self.conf.auth)?;

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
                crate::tasks::persistence::schema::task_schema,
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

        #[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
        let task_store = TaskStore::new(
            pool.as_sqlx().clone(),
            task_config.batch_size,
            std::time::Duration::from_millis(task_config.lease_duration_ms as u64),
        );

        #[cfg(not(any(feature = "postgres", feature = "mysql", feature = "sqlite")))]
        let task_store = MemoryTaskStore::new(task_config.batch_size);

        let task_registry = Arc::new(bundle.tasks.clone().with_config(task_config));

        let task_dispatcher = task_registry.dispatcher(Arc::new(task_store));

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
            logging::init_tracing(&project_dir, &self.conf.logging)?
        } else {
            LoggingGuard::noop()
        };

        let mut site = SiteInner {
            _logging_guard: logging_guard,
            project_dir,
            start_time: std::time::Instant::now(),
            conf: self.conf.clone(),
            observability,
            pool,
            shutdown_notifier: CancellationNotifier::new(),
            service_engine: services::ServiceEngine::new(),
            runtime: OnceCell::new(),
            timezone,
            authenticator,
            template_engine,
            slash_router,
            route_registry,
            joinset: Arc::new(parking_lot::Mutex::new(tokio::task::JoinSet::new())),
            channels: LocalChannelBackend::new(self.conf.channels.clone()),
            console_status_cache: self
                .conf
                .console
                .enabled
                .then(crate::console::status::ConsoleStatusCache::new),
            bundle,
            #[cfg(feature = "migrations")]
            migration_runner,
            signal_engine,
            emitter_engine,
            commands: command_registry,
            task_engine: task_dispatcher,
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
    pool: DbPool,
    channels: LocalChannelBackend,
    console_status_cache: Option<crate::console::status::ConsoleStatusCache>,
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
    /// Builds an inert `Site` from configuration and a bundle.
    ///
    /// The site exposes its router, database, services, and commands, but does
    /// not start task workers, emitters, PgNotify listeners, or service workers.
    pub async fn build(conf: SiteConf, bundle: impl IntoBundle) -> Result<Self, SiteError> {
        let builder = SiteBuilder::new(conf);
        let site = builder.build(None, bundle).await?;
        Ok(Self {
            inner: Arc::new(site),
        })
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
        let site = builder.build(Some(pool), bundle).await?;
        Ok(Self {
            inner: Arc::new(site),
        })
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
        self.inner.joinset.lock().abort_all();
        while let Some(_) = self.inner.joinset.lock().try_join_next() {}
    }

    /// Return the signal client for emitting typed in-process events.
    /// Emitted signals are dispatched to signal handlers and channel transports.
    pub fn signals(&self) -> SignalClient {
        SignalClient::new(self.clone(), self.inner.signal_engine.clone())
    }

    pub(crate) async fn dispatch_payload(
        &self,
        payload: DataBox,
        target: EmitTarget,
    ) -> Result<(), SiteError> {
        match target {
            EmitTarget::Signal => {
                self.inner
                    .signal_engine
                    .dispatch_data_fire_and_forget(self.clone(), payload.clone())
                    .await;
                if let Err(err) = self.channels().publish_box(&payload).await {
                    tracing::error!("Error delivering emitted signal to channels: {}", err);
                }
            }
            EmitTarget::Task => {
                let _ = payload;
            }
        }
        Ok(())
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

    /// Return the template rendering facade for this site.
    /// Templates are loaded from bundle-owned private `templates/**` assets.
    pub fn templates(&self) -> Templates {
        Templates::new(self.clone())
    }

    /// Return the outbound SMTP mail facade configured for this site.
    #[cfg(feature = "email")]
    pub fn mail(&self) -> crate::email::Mailer {
        crate::email::Mailer::new(self.clone())
    }

    pub(crate) fn template_engine(&self) -> &TemplateEngine {
        &self.inner.template_engine
    }

    /// Return the configured authenticator.
    /// Routes and commands can use it for JWT and role-aware authentication behavior.
    pub fn auth(&self) -> &Authenticator {
        &self.inner.authenticator
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
        self.inner
            .console_status_cache
            .as_ref()
            .map(|cache| cache.get(self, ttl))
            .unwrap_or_else(|| crate::console::status::collect(self))
    }

    pub(crate) fn console_bundle_id(&self) -> Option<uuid::Uuid> {
        self.operations()
            .list()
            .find(|operation| operation.name == "console_home")
            .and_then(|operation| operation.bundle_id)
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
                (StatusCode::INTERNAL_SERVER_ERROR, "Service panicked").into_response()
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
    pub fn tasks(&self) -> TaskClient<TaskStore> {
        TaskClient::new(self.inner.task_engine.clone())
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
    use super::Site;
    use crate::{
        Error, bundles,
        callables::Data,
        commands::CommandConf,
        emitters::{EmitTarget, PeriodicConf},
        signals::SignalConf,
        tasks::TaskHandlerConf,
    };
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    static RUNTIME_SIGNALS: AtomicUsize = AtomicUsize::new(0);
    static RUNTIME_TASKS: AtomicUsize = AtomicUsize::new(0);

    #[derive(Clone, Deserialize, JsonSchema, Serialize)]
    struct RuntimeSignal;

    #[derive(Clone, Deserialize, JsonSchema, Serialize)]
    struct RuntimeTask;

    #[derive(Clone, Deserialize, JsonSchema, Serialize)]
    struct WaitArgs {}

    async fn emit_runtime_signal() -> Data<RuntimeSignal> {
        Data::new(RuntimeSignal)
    }

    async fn count_runtime_signal(_signal: Data<RuntimeSignal>) {
        RUNTIME_SIGNALS.fetch_add(1, Ordering::SeqCst);
    }

    async fn count_runtime_task(_task: Data<RuntimeTask>) {
        RUNTIME_TASKS.fetch_add(1, Ordering::SeqCst);
    }

    async fn wait_command(site: Site, _args: Data<WaitArgs>) -> Result<(), Error> {
        site.tasks()
            .submit(RuntimeTask)
            .await
            .map_err(Error::other)?;
        tokio::time::sleep(Duration::from_millis(25)).await;
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
            Site::run_with_args(conf, crate::bundles::Bundle::new(), strings(&["help"])).await;

        assert!(result.is_ok(), "command failed: {result:?}");
    }

    #[tokio::test]
    async fn unknown_command_does_not_build_site() {
        let conf = crate::SiteConf::default()
            .log_init(false)
            .project_dir("/path/that/does/not/exist");
        let err = Site::run_with_args(conf, crate::bundles::Bundle::new(), strings(&["missing"]))
            .await
            .unwrap_err();

        assert!(err.to_string().contains("missing"));
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
                PeriodicConf {
                    interval: Duration::from_millis(1),
                    target: EmitTarget::Signal,
                },
            ),
            bundles::task::<RuntimeTask, _, _>(
                count_runtime_task,
                TaskHandlerConf::new("runtime-task-probe"),
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
}
