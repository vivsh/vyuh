use std::sync::Arc;

use super::{Bundle, BundleError};
use crate::{
    Error, Site,
    callables::{self},
    commands::{self},
    embed, emitters,
    services::{Service, ServiceBuildContext, ServiceHandler, ServiceInstance},
    signals::{self, SignalConf},
    tasks::TaskDefinition,
};

/// Exact registration class for one HTTP operation during bundle assembly.
pub(super) enum RouteMarker {
    Route,
    Beacon,
}

pub(super) enum BundlePartInner {
    Route(
        axum::routing::MethodRouter<Site>,
        crate::callables::Operation,
        RouteMarker,
    ),
    Emitter(emitters::Emitter),
    #[allow(dead_code)]
    Task(crate::tasks::RegisteredTask),
    Signal(signals::Signaller),
    Error(BundleError),
    AssetDir(embed::Dir),
    Command(commands::Command),
    Service(ServiceHandler),
    UrlInfo(crate::collectors::UrlInfoProvider),
    #[cfg(feature = "migrations")]
    Migrations(crate::db::MigrationSource),
    #[cfg(feature = "migrations")]
    Schema(crate::db::SchemaSource),
    #[cfg(feature = "mcp")]
    McpTool(crate::mcp::McpDirectRegistration),
    #[cfg(feature = "mcp")]
    McpResource {
        name: String,
        resource: crate::mcp::McpResource,
    },
}

/// A single registerable piece of a bundle: a route, emitter, signal, service, etc.
///
/// Constructed by the free functions in this module (`route`, `cron`, `signal`, …)
/// or by the proc-macro equivalents. Call `.patch(PatchOp)` to amend metadata.
pub struct BundlePart {
    pub(super) part: BundlePartInner,
    pub(super) operation: Option<crate::callables::Operation>,
}

impl BundlePart {
    /// Amends the operation metadata for this part (name, description, arg names, etc.).
    pub fn patch(mut self, f: callables::PatchOp) -> Self {
        if let Some(op) = &mut self.operation {
            f.apply(op);
        } else {
            match &mut self.part {
                BundlePartInner::Route(_, operation, _) => f.apply(operation),
                #[cfg(feature = "mcp")]
                BundlePartInner::McpTool(registration) => f.apply(&mut registration.operation),
                _ => {}
            }
        }
        self
    }
}

// ---------------------------------------------------------------------------
// Bundle injection
// ---------------------------------------------------------------------------

impl Bundle {
    pub(super) fn add_part(mut self, part: BundlePart) -> Self {
        // Non-route parts contribute an operation to the ops store (no name_index entry
        // since reversal is only meaningful for HTTP routes).
        if !matches!(&part.part, BundlePartInner::Route(..))
            && let Some(mut op) = part.operation
        {
            op.assign_bundle_id(self.id);
            self.ops.insert(op.id, op);
        }
        match part.part {
            BundlePartInner::Route(router, mut op, marker) => {
                op.assign_bundle_id(self.id);
                self = self.register_route(router, op, marker);
            }
            BundlePartInner::Emitter(em) => {
                if let Err(e) = self.emitters.register(em) {
                    self.errors.push(BundleError::Emitter(Arc::new(e)));
                }
            }
            BundlePartInner::Signal(sig) => {
                self.signals.register(sig);
            }
            BundlePartInner::Error(e) => {
                self.errors.push(e);
            }
            BundlePartInner::AssetDir(d) => {
                self.asset_dirs.push(d);
            }
            BundlePartInner::Service(entry) => {
                if let Err(e) = self.services.register(entry) {
                    self.errors.push(BundleError::Service(Arc::new(e)));
                }
            }
            BundlePartInner::Task(ts) => {
                if let Err(e) = self.tasks.register(ts) {
                    self.errors.push(BundleError::Task(Arc::new(e)));
                }
            }
            BundlePartInner::Command(cmd) => {
                if let Err(e) = self.commands.register(cmd) {
                    self.errors.push(BundleError::Command(Arc::new(e)));
                }
            }
            BundlePartInner::UrlInfo(provider) => {
                self.url_info.register(provider);
            }
            #[cfg(feature = "migrations")]
            BundlePartInner::Migrations(source) => {
                if let Err(e) = self.migrations.register(source) {
                    self.errors.push(BundleError::Migration(Arc::new(e)));
                }
            }
            #[cfg(feature = "migrations")]
            BundlePartInner::Schema(source) => {
                if let Err(e) = self.migrations.register_schema(source) {
                    self.errors.push(BundleError::Migration(Arc::new(e)));
                }
            }
            #[cfg(feature = "mcp")]
            BundlePartInner::McpTool(mut registration) => {
                registration.operation.assign_bundle_id(self.id);
                let id = registration.operation.id;
                self.ops.insert(id, registration.operation);
                self.mcp_registry
                    .register_direct(id, registration.callable, registration.conf);
            }
            #[cfg(feature = "mcp")]
            BundlePartInner::McpResource { name, resource } => {
                if let Err(error) = self.mcp_resources.register(name, resource, self.id) {
                    self.errors.push(BundleError::Mcp(error.to_string()));
                }
            }
        }
        self
    }

    /// Registers an HTTP route and its operation metadata.
    pub(super) fn register_route(
        mut self,
        router: axum::routing::MethodRouter<Site>,
        op: crate::callables::Operation,
        marker: RouteMarker,
    ) -> Self {
        if let Err(e) = self.validate_route_operation(&op) {
            self.errors.push(e);
            return self;
        }
        let id = op.id;
        let router = if op.path != "/" && op.path.ends_with('/') {
            router.route_layer(crate::slash::RouteSlashLayer::redirect())
        } else if !op.trim {
            router.route_layer(crate::slash::RouteSlashLayer::reject())
        } else {
            router
        };
        let router = router.layer(axum::Extension(id));
        let path = crate::slash::internal_path(&op.path);
        self.inner_router = self.inner_router.route(path, router);
        let name = op.name.clone();
        self.ops.insert(id, op);
        self.name_index.insert(name, id);
        if matches!(marker, RouteMarker::Beacon) {
            self.beacons.insert(id);
        }
        self
    }
}

// ---------------------------------------------------------------------------
// Free constructor functions
// ---------------------------------------------------------------------------

/// Creates a route part from a handler function and its routing configuration.
pub fn route<H, T, Args>(handler: H, meta: crate::routes::RouteConf) -> BundlePart
where
    H: axum::handler::Handler<T, Site> + callables::Specable<Args> + Clone + Send + Sync + 'static,
    T: 'static,
    Args: callables::IntoArgSpecs + 'static,
{
    let spec = callables::CallSpec::new(&handler);
    let mut op =
        crate::callables::Operation::from_specs(crate::callables::OperationKind::Route, &spec);
    op.path = meta.path.clone().into();
    op.name = meta.name.clone().into();
    op.methods = meta.methods;
    op.trim = meta.trim;
    op = op.with_conf(&meta);
    op.returns
        .push(callables::ReturnSpec::error(405, "Method not allowed."));

    let router = axum::routing::on(meta.methods.into(), handler);
    BundlePart {
        operation: None,
        part: BundlePartInner::Route(router, op, RouteMarker::Route),
    }
}

/// Creates an authenticated declarative Beacon route.
pub fn beacon(beacon: crate::channels::Beacon, conf: crate::channels::BeaconConf) -> BundlePart {
    if let Err(error) = beacon.validate(&conf) {
        return BundlePart {
            operation: None,
            part: BundlePartInner::Error(BundleError::Config(error.to_string())),
        };
    }
    let mut operation = crate::callables::Operation::from_api_doc(conf.name, conf.path);
    operation.kind = crate::callables::OperationKind::Route;
    operation.hidden = false;
    operation.openapi_id = Some(conf.name.to_string());
    operation.trim = conf.trim;
    operation.args.push(
        crate::callables::ArgSpec::from_type::<crate::auth::AuthUser>(
            0,
            "user",
            "Authenticated Beacon subscriber.",
        ),
    );
    operation
        .returns
        .push(crate::callables::ReturnSpec::from_type::<
            crate::channels::ChannelResponse,
        >(
            Some("Negotiated WebSocket, SSE, or long-poll subscription.".into()),
            Some(200),
        ));
    operation.returns.push(crate::callables::ReturnSpec::error(
        403,
        "No Beacon rule is eligible for this identity.",
    ));
    operation.returns.push(crate::callables::ReturnSpec::error(
        405,
        "Method not allowed.",
    ));
    let operation_id = operation.id;
    let beacon = std::sync::Arc::new(beacon);
    let modes = conf.modes;
    let handler = move |user: crate::auth::AuthUser,
                        subscriber: crate::routes::Subscriber,
                        axum::extract::State(site): axum::extract::State<Site>| {
        let beacon = std::sync::Arc::clone(&beacon);
        async move {
            beacon
                .open(site, operation_id, user, subscriber, modes)
                .await
        }
    };
    let router = axum::routing::on(crate::routes::Methods::GET.into(), handler);
    BundlePart {
        operation: None,
        part: BundlePartInner::Route(router, operation, RouteMarker::Beacon),
    }
}

/// Creates a semantic MCP-only tool from a typed Vyuh callable.
#[cfg(feature = "mcp")]
pub fn mcp_tool<T, H, O, Args>(
    name: impl Into<String>,
    handler: H,
    conf: crate::mcp::McpToolConf,
) -> BundlePart
where
    T: callables::DataValue,
    H: callables::Specable<Args, Output = O> + Send + Sync + 'static,
    O: callables::IntoOutput<Error> + callables::IntoReturnPart + Send + 'static,
    Args: callables::FromContext<crate::mcp::McpToolContext>
        + callables::IntoArgSpecs
        + callables::HasData<T>
        + Send
        + 'static,
{
    let callable = callables::Callable::new(handler);
    let mut operation = crate::callables::Operation::from_specs(
        crate::callables::OperationKind::McpTool,
        callable.inspect(),
    );
    operation.name = name.into();
    BundlePart {
        operation: None,
        part: BundlePartInner::McpTool(crate::mcp::McpDirectRegistration {
            operation,
            callable,
            conf,
        }),
    }
}

/// Creates one immutable static MCP resource from a factory result.
#[cfg(feature = "mcp")]
pub fn mcp_resource(name: impl Into<String>, resource: crate::mcp::McpResource) -> BundlePart {
    BundlePart {
        operation: None,
        part: BundlePartInner::McpResource {
            name: name.into(),
            resource,
        },
    }
}

/// Creates a cron-scheduled emitter part.
pub fn cron<O, H, Args>(handler: H, options: emitters::CronConf) -> BundlePart
where
    O: callables::DataValue,
    Args:
        callables::FromContext<emitters::EmitterContext> + callables::IntoArgSpecs + Send + 'static,
    H: callables::Specable<Args> + Send + Sync + 'static,
    H::Output: callables::IntoOutput<Error>
        + callables::IntoReturnPart
        + emitters::EmitsData<O>
        + Send
        + 'static,
{
    match emitters::cron::<H, Args, O>(handler, options) {
        Ok(em) => BundlePart {
            operation: Some(em.operation()),
            part: BundlePartInner::Emitter(em),
        },
        Err(e) => BundlePart {
            operation: None,
            part: BundlePartInner::Error(BundleError::Emitter(Arc::new(e))),
        },
    }
}

/// Creates a time-interval emitter part.
pub fn periodic<O, H, Args>(handler: H, options: emitters::PeriodicConf) -> BundlePart
where
    O: callables::DataValue,
    Args:
        callables::FromContext<emitters::EmitterContext> + callables::IntoArgSpecs + Send + 'static,
    H: callables::Specable<Args> + Send + Sync + 'static,
    H::Output: callables::IntoOutput<Error>
        + callables::IntoReturnPart
        + emitters::EmitsData<O>
        + Send
        + 'static,
{
    match emitters::periodic::<H, Args, O>(handler, options) {
        Ok(em) => BundlePart {
            operation: Some(em.operation()),
            part: BundlePartInner::Emitter(em),
        },
        Err(e) => BundlePart {
            operation: None,
            part: BundlePartInner::Error(BundleError::Emitter(Arc::new(e))),
        },
    }
}

/// Creates a Postgres NOTIFY listener emitter part.
pub fn pgnotify<O, H, Args>(handler: H, options: emitters::PgNotifyConf) -> BundlePart
where
    O: callables::DataValue,
    Args:
        callables::FromContext<emitters::EmitterContext> + callables::IntoArgSpecs + Send + 'static,
    H: callables::Specable<Args> + Send + Sync + 'static,
    H::Output: callables::IntoOutput<Error>
        + callables::IntoReturnPart
        + emitters::EmitsData<O>
        + Send
        + 'static,
{
    match emitters::pgnotify::<H, Args, O>(handler, options) {
        Ok(em) => BundlePart {
            operation: Some(em.operation()),
            part: BundlePartInner::Emitter(em),
        },
        Err(e) => BundlePart {
            operation: None,
            part: BundlePartInner::Error(BundleError::Emitter(Arc::new(e))),
        },
    }
}

/// Creates a signal handler part.
pub fn signal<T, H, Args>(handler: H, options: SignalConf) -> BundlePart
where
    T: callables::DataValue,
    H: callables::Specable<Args> + Send + Sync + 'static,
    H::Output: callables::IntoOutput<Error> + callables::IntoReturnPart + Send + 'static,
    Args: callables::FromContext<signals::SignalContext>
        + callables::IntoArgSpecs
        + callables::HasData<T>
        + Send
        + 'static,
{
    let sig = crate::signals::signal::<T, H, Args>(handler, options);
    let op = sig.operation();
    BundlePart {
        operation: Some(op),
        part: BundlePartInner::Signal(sig),
    }
}

/// Creates a durable task part.
pub fn task<T, H, Args>(handler: H, definition: TaskDefinition<T>) -> BundlePart
where
    T: callables::DataValue,
    H: callables::Specable<Args> + Send + Sync + 'static,
    H::Output: callables::IntoOutput<Error>
        + callables::IntoReturnPart
        + crate::tasks::IntoTaskOutcomePart
        + Send
        + 'static,
    Args: callables::FromContext<crate::tasks::TaskContext>
        + callables::IntoArgSpecs
        + callables::HasData<T>
        + Send
        + 'static,
{
    let task = crate::tasks::RegisteredTask::new::<T, H, Args>(definition, handler);
    let op = task.operation();
    BundlePart {
        operation: Some(op),
        part: BundlePartInner::Task(task),
    }
}

/// Creates a background service part.
pub fn service<T, H, Args>(handler: H) -> BundlePart
where
    T: Service,
    H: callables::Specable<Args, Output = ServiceInstance<T>> + Send + Sync + 'static,
    Args: callables::FromContext<ServiceBuildContext> + callables::IntoArgSpecs + Send + 'static,
{
    let entry = ServiceHandler::new(handler);
    let op = entry.operation();
    BundlePart {
        part: BundlePartInner::Service(entry),
        operation: Some(op),
    }
}

/// Creates a CLI command part.
pub fn command<T, H, Args>(handler: H, conf: commands::CommandConf) -> BundlePart
where
    T: callables::DataValue,
    H: callables::Specable<Args, Output = Result<(), Error>> + Send + Sync + 'static,
    Args: callables::FromContext<commands::CommandContext>
        + callables::IntoArgSpecs
        + callables::HasData<T>
        + Send
        + 'static,
{
    match commands::command::<T, H, Args>(handler, conf) {
        Ok(cmd) => {
            let op = cmd.operation();
            BundlePart {
                part: BundlePartInner::Command(cmd),
                operation: Some(op),
            }
        }
        Err(err) => BundlePart {
            part: BundlePartInner::Error(BundleError::Command(Arc::new(err))),
            operation: None,
        },
    }
}

/// Creates a URL info provider part.
pub fn url_info<H, Args>(handler: H) -> BundlePart
where
    H: callables::Specable<Args, Output = Result<Vec<crate::collectors::UrlInfo>, Error>>
        + Send
        + Sync
        + 'static,
    Args: callables::FromContext<crate::collectors::UrlInfoContext>
        + callables::IntoArgSpecs
        + Send
        + 'static,
{
    BundlePart {
        operation: None,
        part: BundlePartInner::UrlInfo(crate::collectors::url_info_provider(handler)),
    }
}

/// Creates a static asset directory part.
pub fn asset_dir(dir: embed::Dir) -> BundlePart {
    BundlePart {
        operation: None,
        part: BundlePartInner::AssetDir(dir),
    }
}

/// Creates a crate-owned migration source part.
#[cfg(feature = "migrations")]
pub fn migrations(source: crate::db::MigrationSource) -> BundlePart {
    BundlePart {
        operation: None,
        part: BundlePartInner::Migrations(source),
    }
}

/// Creates a database schema contribution part.
#[cfg(feature = "migrations")]
pub fn schema(source: crate::db::SchemaSource) -> BundlePart {
    BundlePart {
        operation: None,
        part: BundlePartInner::Schema(source),
    }
}

/// Builds a [`Bundle`] from an iterable of [`BundlePart`]s.
///
/// This is the primary way to construct a bundle. Parts are registered in
/// iteration order.
pub fn bundle(parts: impl IntoIterator<Item = BundlePart>) -> Bundle {
    parts.into_iter().fold(Bundle::new(), Bundle::add_part)
}
