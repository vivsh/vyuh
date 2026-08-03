use std::sync::Arc;

use crate::{
    Error, Site,
    callables::{self},
    commands::{self},
    embed, emitters,
    services::{Service, ServiceBuildContext, ServiceHandler, ServiceInstance},
    signals::{self, SignalConf},
    tasks::TaskHandlerConf,
};
use axum::{
    http::{HeaderValue, header},
    response::IntoResponse,
};

use super::{Bundle, BundleError};

pub(super) enum BundlePartInner {
    Route(
        axum::routing::MethodRouter<Site>,
        crate::callables::Operation,
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
        } else if let BundlePartInner::Route(_, ref mut op) = self.part {
            f.apply(op);
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
        if !matches!(&part.part, BundlePartInner::Route(..)) {
            if let Some(mut op) = part.operation {
                op.assign_bundle_id(self.id);
                self.ops.insert(op.id, op);
            }
        }
        match part.part {
            BundlePartInner::Route(router, mut op) => {
                op.assign_bundle_id(self.id);
                self = self.register_route(router, op);
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
        }
        self
    }

    /// Registers an HTTP route and its operation metadata.
    pub(super) fn register_route(
        mut self,
        router: axum::routing::MethodRouter<Site>,
        op: crate::callables::Operation,
    ) -> Self {
        if let Err(e) = self.validate_route_operation(&op) {
            self.errors.push(e);
            return self;
        }
        let id = op.id;
        let allow = allow_header(&op.methods);
        let router = router
            .fallback(move || method_not_allowed(allow.clone()))
            .layer(axum::Extension(id));
        self.inner_router = self.inner_router.route(op.path.as_ref(), router);
        let name = op.name.clone();
        self.ops.insert(id, op);
        self.name_index.insert(name, id);
        self
    }
}

fn allow_header(methods: &crate::routes::Methods) -> Option<HeaderValue> {
    HeaderValue::from_str(&methods.to_vec().join(", ")).ok()
}

async fn method_not_allowed(allow: Option<HeaderValue>) -> axum::response::Response {
    let mut response = crate::ErrorReport::method_not_allowed().into_response();
    if let Some(allow) = allow {
        response.headers_mut().insert(header::ALLOW, allow);
    }
    response
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
    op.methods = meta.methods.clone().into();
    op.slash_policy = meta.slash;
    op = op.with_conf(&meta);
    op.returns
        .push(callables::ReturnSpec::error(405, "Method not allowed."));

    let router = axum::routing::on(meta.methods.into(), handler);
    BundlePart {
        operation: None,
        part: BundlePartInner::Route(router, op),
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
pub fn task<T, H, Args>(handler: H, options: TaskHandlerConf) -> BundlePart
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
    let task = crate::tasks::RegisteredTask::new::<T, H, Args>(&options.name, handler);
    let op = task.operation().with_conf(&options);
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
