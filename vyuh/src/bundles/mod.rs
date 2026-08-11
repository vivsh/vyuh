mod config;
mod error;
mod openapi;
mod part;

use std::{borrow::Cow, collections::BTreeMap, sync::Arc};

use crate::middlewares::SlashPolicy;
use crate::{
    Site,
    auth::{AudienceId, ProviderDefinitionInner},
    callables::OperationKind,
    commands::CommandRegistry,
    embed, emitters,
    routes::{self},
    services::ServiceRegistry,
    signals::{self, SignalRegistry},
    tasks::TaskRegistry,
};

// ---------------------------------------------------------------------------
// Public re-exports
// ---------------------------------------------------------------------------

#[cfg(feature = "mcp")]
use crate::mcp::{McpEngine, McpToolRegistry};
use openapi::DocEngine;

pub use crate::apidocs::{DocViewer, OpenApiVersion};
#[cfg(feature = "mcp")]
pub use crate::mcp::{McpConf, McpToolConf};
pub use config::{BundleConf, conf};
pub use error::BundleError;
pub use openapi::{OpenApiConf, OpenApiViewerConf};
#[cfg(feature = "mcp")]
pub use part::mcp_tool;
pub use part::{
    BundlePart, asset_dir, bundle, command, cron, periodic, pgnotify, route, service, signal, task,
    url_info,
};
#[cfg(feature = "migrations")]
pub use part::{migrations, schema};

#[cfg(feature = "mcp")]
pub use vyuh_macros::mcp_tool;
pub use vyuh_macros::{
    asset_dir, bundle, cron, periodic, pgnotify, route, service, signal, task, url_info,
};
#[cfg(feature = "migrations")]
pub use vyuh_macros::{migrations, schema};

pub use {
    crate::apidocs::ApiMeta, crate::routes::RouteConf, emitters::CronConf,
    emitters::EmitterExecutor, emitters::PeriodicConf, emitters::PgNotifyConf,
    emitters::ScheduleStart, signals::SignalConf,
};

// ---------------------------------------------------------------------------
// IntoBundle
// ---------------------------------------------------------------------------

pub trait IntoBundle {
    fn into_bundle(self) -> Bundle;
}

/// Immutable parent links retained while one bundle graph is composed.
#[derive(Clone, Default)]
pub(crate) struct BundleTopology {
    parents: BTreeMap<uuid::Uuid, uuid::Uuid>,
}

impl BundleTopology {
    fn absorb(&mut self, child_root: uuid::Uuid, other: Self, parent: uuid::Uuid) {
        self.parents.extend(other.parents);
        self.parents.insert(child_root, parent);
    }

    pub(crate) fn contains(&self, root: uuid::Uuid, value: uuid::Uuid) -> bool {
        let mut current = Some(value);
        while let Some(node) = current {
            if node == root {
                return true;
            }
            current = self.parents.get(&node).copied();
        }
        false
    }

    pub(crate) fn lineage(&self, value: uuid::Uuid) -> Vec<uuid::Uuid> {
        let mut values = vec![value];
        let mut current = value;
        while let Some(parent) = self.parents.get(&current).copied() {
            values.push(parent);
            current = parent;
        }
        values.reverse();
        values
    }
}

/// Request-local metadata used by generated endpoint adapters.
#[derive(Clone, Debug)]
pub(crate) struct BundleRequestContext {
    pub(crate) audience: Option<AudienceId>,
}

impl IntoBundle for Bundle {
    fn into_bundle(self) -> Bundle {
        self
    }
}

impl IntoBundle for axum::Router<Site> {
    fn into_bundle(self) -> Bundle {
        Bundle {
            inner_router: self,
            ..Bundle::new()
        }
    }
}

// ---------------------------------------------------------------------------
// Bundle
// ---------------------------------------------------------------------------

/// A composable collection of HTTP routes, background services, emitters,
/// signals, commands, and asset directories that form one logical unit of
/// the application.
///
/// Build one with [`bundle()`], then compose with [`merge`], [`with_prefix`],
/// [`layer`], and [`with_conf`].
///
/// [`merge`]: Bundle::merge
/// [`with_prefix`]: Bundle::with_prefix
/// [`layer`]: Bundle::layer
/// [`with_conf`]: Bundle::with_conf
pub struct Bundle {
    pub(super) inner_router: routes::AxumRouter<Site>,
    /// All operations keyed by their stable UUID — routes, signals, tasks, commands,
    /// services, and hidden doc-engine markers alike.
    pub(crate) ops: BTreeMap<crate::OperationId, crate::callables::Operation>,
    /// Secondary index: route name → UUID. Populated only for HTTP routes so that
    /// `Routes::reverse_url()` can look up a path by the human-readable name.
    pub(crate) name_index: BTreeMap<String, crate::OperationId>,
    pub(super) id: uuid::Uuid,
    configs: BTreeMap<uuid::Uuid, BundleConf>,
    pub(crate) topology: BundleTopology,
    label: Option<String>,
    pub(crate) signals: SignalRegistry,
    pub(crate) emitters: emitters::EmitterRegistry,
    pub(super) errors: Vec<BundleError>,
    pub(crate) tasks: TaskRegistry,
    pub(crate) asset_dirs: Vec<embed::Dir>,
    pub(crate) url_info: crate::collectors::UrlInfoRegistry,
    pub(crate) services: ServiceRegistry,
    pub(crate) commands: CommandRegistry,
    pub(crate) doc_engine: DocEngine,
    #[cfg(feature = "mcp")]
    pub(crate) mcp_engine: McpEngine,
    #[cfg(feature = "mcp")]
    pub(crate) mcp_registry: McpToolRegistry,
    #[cfg(feature = "migrations")]
    pub(crate) migrations: crate::db::MigrationRegistry,
}

impl Bundle {
    /// Creates a new empty bundle with a framework-generated immutable identity.
    ///
    /// This constructor is intentionally private. Do not add a caller-supplied
    /// bundle ID: macro and direct composition must both create a fresh identity.
    fn new() -> Self {
        Self {
            id: uuid::Uuid::new_v4(),
            configs: BTreeMap::new(),
            topology: BundleTopology::default(),
            label: None,
            inner_router: routes::AxumRouter::new(),
            ops: BTreeMap::new(),
            name_index: BTreeMap::new(),
            signals: SignalRegistry::new(),
            emitters: emitters::EmitterRegistry::new(),
            errors: Vec::new(),
            tasks: TaskRegistry::new(),
            asset_dirs: Vec::new(),
            url_info: crate::collectors::UrlInfoRegistry::new(),
            services: ServiceRegistry::new(),
            commands: CommandRegistry::new(),
            doc_engine: DocEngine::new(),
            #[cfg(feature = "mcp")]
            mcp_engine: McpEngine::new(),
            #[cfg(feature = "mcp")]
            mcp_registry: McpToolRegistry::new(),
            #[cfg(feature = "migrations")]
            migrations: crate::db::MigrationRegistry::new(),
        }
    }

    /// Returns the unique identifier for this bundle.
    pub fn id(&self) -> uuid::Uuid {
        self.id
    }

    /// Returns the label assigned via [`with_label`], if any.
    ///
    /// [`with_label`]: Bundle::with_label
    pub fn label(&self) -> Option<&str> {
        self.label.as_deref()
    }

    /// Attaches declarative configuration to this bundle.
    pub fn with_conf(mut self, mut conf: BundleConf) -> Self {
        for openapi in conf.take_openapi() {
            self.register_openapi(openapi);
        }
        #[cfg(feature = "mcp")]
        if let Some(mcp) = conf.take_mcp() {
            self.register_mcp(mcp);
        }
        self.configs
            .entry(self.id)
            .and_modify(|current| current.merge(conf.clone()))
            .or_insert(conf);
        self
    }

    /// Resolves inherited bundle configuration once before runtime construction.
    pub(crate) fn finalize_conf(
        &mut self,
        default_audience: Option<AudienceId>,
    ) -> Result<(), BundleError> {
        self.validate_configurations()?;
        self.register_task_lanes()?;
        let updates = self.operation_updates(default_audience)?;
        for (id, audience, tags, slash_policy) in updates {
            let Some(operation) = self.ops.get_mut(&id) else {
                continue;
            };
            operation.audience = audience;
            append_tags(&mut operation.tags, tags);
            if matches!(operation.kind, OperationKind::Route | OperationKind::ApiDoc) {
                operation.slash_policy = slash_policy;
            }
        }
        Ok(())
    }

    /// Returns central provider definitions contributed by audience-bearing bundles.
    pub(crate) fn auth_definitions(&self) -> Result<Vec<ProviderDefinitionInner>, BundleError> {
        let mut output = Vec::new();
        for (id, conf) in &self.configs {
            let Some(audience) = self.declared_audience(*id)? else {
                if !conf.providers.is_empty() {
                    return Err(BundleError::Auth(
                        "bundle auth providers require BundleConf::audience".into(),
                    ));
                }
                continue;
            };
            for provider in &conf.providers {
                validate_provider_audience(provider, &audience)?;
                output.push(provider.clone());
            }
        }
        Ok(output)
    }

    pub(crate) fn normalize_authorization(&mut self) -> Result<(), BundleError> {
        for operation in self.ops.values_mut() {
            operation.normalize_authorization().map_err(|reason| {
                BundleError::Auth(format!("operation '{}': {reason}", operation.name))
            })?;
        }
        Ok(())
    }

    /// Assigns a human-readable label to this bundle, for example in logging or diagnostics.
    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Validates that no errors were accumulated during bundle construction.
    pub fn validate(&self) -> Result<(), BundleError> {
        if !self.errors.is_empty() {
            return Err(BundleError::ErrorList(self.errors.clone()));
        }
        #[cfg(feature = "mcp")]
        {
            let unclaimed = self
                .mcp_registry
                .unclaimed()
                .filter_map(|id| self.ops.get(&id).map(|operation| operation.name.as_str()))
                .collect::<Vec<_>>();
            if !unclaimed.is_empty() {
                return Err(BundleError::Mcp(format!(
                    "MCP registrations are not claimed by a service: {}",
                    unclaimed.join(", ")
                )));
            }
        }
        for operation in self.ops.values() {
            if operation.requires_bundle_audience() && operation.audience.is_none() {
                return Err(BundleError::MissingAuthAudience {
                    name: operation.name.clone(),
                    path: operation.path.clone(),
                });
            }
        }
        Ok(())
    }

    /// Returns the axum router for the HTTP routes registered in this bundle.
    pub fn to_router(&self) -> routes::AxumRouter<Site> {
        self.inner_router.clone()
    }

    /// Merges another bundle into this one.
    ///
    /// Routes, operations, services, signals, emitters, tasks, and commands from
    /// `other` are all absorbed. Use `.merge(other.with_prefix("/path"))` to
    /// compose with a prefix.
    ///
    /// Any errors (e.g. duplicate registrations) are accumulated and surface the
    /// next time `validate()` is called. `SiteBuilder::build` always calls
    /// `validate()` before the site starts, so no error is silently swallowed.
    pub fn merge<B: IntoBundle>(mut self, other: B) -> Self {
        let other = other.into_bundle();
        let router = match self.absorb(other) {
            Ok(r) => r,
            Err(e) => {
                self.errors.push(e);
                return self;
            }
        };
        self.inner_router = self.inner_router.merge(router);
        self
    }

    /// Prefixes all routes and operation paths in this bundle with `path`.
    ///
    /// `path` must start with `'/'` and must not end with `'/'`.
    /// All operations — including hidden doc-engine markers — are updated by the
    /// same loop, so `DocEngine::setup` always sees fully-qualified paths.
    pub fn with_prefix(mut self, path: &str) -> Self {
        if let Err(reason) = validate_route_prefix(path) {
            self.errors.push(BundleError::InvalidRoutePrefix {
                prefix: path.to_string(),
                reason,
            });
            return self;
        }

        for op in self.ops.values_mut() {
            op.nest(path);
        }
        self.url_info.with_prefix(path);
        self.inner_router = routes::AxumRouter::new().nest(path, self.inner_router);
        self
    }

    /// Applies a middleware layer to all routes in this bundle.
    ///
    /// Accepts any type implementing [`Middleware`]. If the middleware provides a
    /// [`LayerSpec`] via [`Middleware::layer_spec`], it is injected into every
    /// operation in this bundle so that apidocs can render it.
    ///
    /// To apply a plain tower layer without documentation, wrap it with
    /// [`routes::layer_from`].
    ///
    /// [`Middleware`]: crate::routes::Middleware
    /// [`LayerSpec`]: crate::callables::LayerSpec
    /// [`routes::layer_from`]: crate::routes::layer_from
    pub fn layer<M>(mut self, mw: M) -> Self
    where
        M: routes::middleware::Middleware,
        <M::Layer as tower::Layer<axum::routing::Route>>::Service:
            tower::Service<axum::http::Request<axum::body::Body>> + Clone + Send + Sync + 'static,
        <<M::Layer as tower::Layer<axum::routing::Route>>::Service as tower::Service<
            axum::http::Request<axum::body::Body>,
        >>::Response: axum::response::IntoResponse + 'static,
        <<M::Layer as tower::Layer<axum::routing::Route>>::Service as tower::Service<
            axum::http::Request<axum::body::Body>,
        >>::Error: Into<std::convert::Infallible> + 'static,
        <<M::Layer as tower::Layer<axum::routing::Route>>::Service as tower::Service<
            axum::http::Request<axum::body::Body>,
        >>::Future: Send + 'static,
    {
        if let Some(spec) = mw.layer_spec() {
            for op in self.ops.values_mut() {
                op.layers.push(spec.clone());
            }
        }
        self.inner_router = self.inner_router.layer(mw.into_layer());
        self
    }

    /// Replaces the inner axum router directly.
    ///
    /// Used by `SiteBuilder` to inject the fully-configured router back into the
    /// bundle after doc-engine routes have been mounted. Not part of the public API.
    pub(crate) fn with_router_unchecked(mut self, router: routes::AxumRouter<Site>) -> Self {
        self.inner_router = router;
        self
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Consumes `other`, absorbing all non-router state into `self`.
    /// Returns the other bundle's router so the caller can merge it.
    fn absorb(&mut self, mut other: Bundle) -> Result<axum::Router<Site>, BundleError> {
        self.errors.append(&mut other.errors);
        for name in other.name_index.keys() {
            if self.name_index.contains_key(name) {
                return Err(BundleError::DuplicateRouteName { name: name.clone() });
            }
        }
        for op in other
            .ops
            .values()
            .filter(|op| op.kind == OperationKind::Route)
        {
            if let Some((path, methods)) = self.find_route_collision(op) {
                return Err(BundleError::DuplicateRoutePathMethod { path, methods });
            }
        }
        self.ops.extend(other.ops);
        self.name_index.extend(other.name_index);
        self.topology.absorb(other.id, other.topology, self.id);
        for (id, conf) in other.configs {
            self.configs
                .entry(id)
                .and_modify(|current| current.merge(conf.clone()))
                .or_insert(conf);
        }
        self.signals.merge(other.signals);
        self.asset_dirs.extend(other.asset_dirs);
        self.url_info.merge(other.url_info);
        if let Err(e) = self.emitters.merge(other.emitters) {
            return Err(BundleError::Emitter(Arc::new(e)));
        }
        if let Err(e) = self.services.merge(other.services) {
            return Err(BundleError::Service(Arc::new(e)));
        }
        if let Err(e) = self.tasks.merge(other.tasks) {
            return Err(BundleError::Task(Arc::new(e)));
        }
        if let Err(e) = self.commands.merge(other.commands) {
            return Err(BundleError::Command(Arc::new(e)));
        }
        #[cfg(feature = "migrations")]
        if let Err(e) = self.migrations.merge(other.migrations) {
            return Err(BundleError::Migration(Arc::new(e)));
        }
        self.doc_engine.merge(other.doc_engine);
        #[cfg(feature = "mcp")]
        self.mcp_engine.merge(other.mcp_engine);
        #[cfg(feature = "mcp")]
        self.mcp_registry.merge(other.mcp_registry);
        Ok(other.inner_router)
    }

    fn register_task_lanes(&mut self) -> Result<(), BundleError> {
        for conf in self.configs.values() {
            for lane in &conf.task_lanes {
                self.tasks
                    .register_lane(lane.clone())
                    .map_err(|error| BundleError::Task(Arc::new(error)))?;
            }
        }
        Ok(())
    }

    fn validate_configurations(&self) -> Result<(), BundleError> {
        let errors = self
            .configs
            .values()
            .flat_map(|conf| conf.errors.iter().cloned())
            .map(BundleError::Config)
            .collect::<Vec<_>>();
        if errors.is_empty() {
            Ok(())
        } else {
            Err(BundleError::ErrorList(errors))
        }
    }

    fn operation_updates(
        &self,
        default_audience: Option<AudienceId>,
    ) -> Result<Vec<OperationUpdate>, BundleError> {
        self.ops
            .values()
            .map(|operation| self.operation_update(operation, default_audience.clone()))
            .collect()
    }

    fn operation_update(
        &self,
        operation: &crate::callables::Operation,
        default_audience: Option<AudienceId>,
    ) -> Result<OperationUpdate, BundleError> {
        let Some(bundle_id) = operation.bundle_id else {
            return Ok((
                operation.id,
                operation.audience.clone(),
                Vec::new(),
                operation.slash_policy,
            ));
        };
        let configs = self.configs_for(bundle_id);
        let audience = configs
            .iter()
            .filter_map(|conf| conf.audience)
            .next_back()
            .map(AudienceId::declared)
            .transpose()
            .map_err(|error| BundleError::Auth(error.to_string()))?
            .or_else(|| operation.audience.clone())
            .or_else(|| {
                (operation.requires_bundle_audience()
                    || aggregate_requires_audience(&operation.kind))
                .then_some(default_audience)
                .flatten()
            });
        let tags = configs
            .iter()
            .flat_map(|conf| conf.tags.iter().cloned())
            .collect();
        let slash_policy = configs
            .iter()
            .filter_map(|conf| conf.slash_policy)
            .next_back()
            .or(operation.slash_policy);
        Ok((operation.id, audience, tags, slash_policy))
    }

    fn configs_for(&self, id: uuid::Uuid) -> Vec<&BundleConf> {
        self.topology
            .lineage(id)
            .into_iter()
            .filter_map(|node| self.configs.get(&node))
            .collect()
    }

    fn declared_audience(&self, id: uuid::Uuid) -> Result<Option<AudienceId>, BundleError> {
        self.configs_for(id)
            .into_iter()
            .filter_map(|conf| conf.audience)
            .next_back()
            .map(AudienceId::declared)
            .transpose()
            .map_err(|error| BundleError::Auth(error.to_string()))
    }

    pub(super) fn validate_route_operation(
        &self,
        op: &crate::callables::Operation,
    ) -> Result<(), BundleError> {
        if op.name.trim().is_empty() {
            return Err(BundleError::InvalidRouteName {
                name: op.name.clone(),
                reason: "route name cannot be empty".to_string(),
            });
        }
        if let Err(reason) = validate_route_path(&op.path) {
            return Err(BundleError::InvalidRoutePath {
                name: op.name.clone(),
                path: op.path.clone(),
                reason,
            });
        }
        if self.name_index.contains_key(&op.name) {
            return Err(BundleError::DuplicateRouteName {
                name: op.name.clone(),
            });
        }
        if let Some((path, methods)) = self.find_route_collision(op) {
            return Err(BundleError::DuplicateRoutePathMethod { path, methods });
        }
        Ok(())
    }

    fn find_route_collision(&self, op: &crate::callables::Operation) -> Option<(String, String)> {
        self.ops
            .values()
            .filter(|existing| existing.kind == OperationKind::Route)
            .find(|existing| existing.path == op.path && existing.methods.intersects(op.methods))
            .map(|existing| {
                let methods = existing
                    .methods
                    .to_vec()
                    .into_iter()
                    .filter(|name| op.methods.to_vec().contains(name))
                    .collect::<Vec<_>>()
                    .join("|");
                (op.path.clone(), methods)
            })
    }
}

type OperationUpdate = (
    crate::OperationId,
    Option<AudienceId>,
    Vec<Cow<'static, str>>,
    Option<SlashPolicy>,
);

fn append_tags(target: &mut Vec<Cow<'static, str>>, additions: Vec<Cow<'static, str>>) {
    for tag in additions {
        if !target.contains(&tag) {
            target.push(tag);
        }
    }
}

fn validate_provider_audience(
    provider: &ProviderDefinitionInner,
    audience: &AudienceId,
) -> Result<(), BundleError> {
    let Some(audiences) = provider.declared_audiences() else {
        return Err(BundleError::Auth(format!(
            "bundle auth provider '{}' must explicitly declare audience '{}'",
            provider.name.as_str(),
            audience.as_str()
        )));
    };
    if audiences.len() != 1
        || audiences
            .first()
            .is_none_or(|value| value.as_str() != audience.as_str())
    {
        return Err(BundleError::Auth(format!(
            "bundle auth provider '{}' must declare exactly audience '{}'",
            provider.name.as_str(),
            audience.as_str()
        )));
    }
    Ok(())
}

impl Default for Bundle {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) fn validate_route_path(path: &str) -> Result<(), String> {
    if path.is_empty() {
        return Err("path cannot be empty".to_string());
    }
    if !path.starts_with('/') {
        return Err("path must start with '/'".to_string());
    }
    if path.contains("//") {
        return Err("path cannot contain '//'".to_string());
    }
    if path.contains(['?', '#']) {
        return Err("path cannot contain a query or fragment".to_string());
    }
    Ok(())
}

/// Validates a non-root route prefix shared by bundle and site configuration.
pub(crate) fn validate_route_prefix(path: &str) -> Result<(), String> {
    validate_route_path(path)?;
    if path == "/" {
        return Err("prefix cannot be '/'".to_string());
    }
    if path.ends_with('/') {
        return Err("prefix must not end with '/'".to_string());
    }
    Ok(())
}

fn aggregate_requires_audience(kind: &OperationKind) -> bool {
    if matches!(kind, OperationKind::ApiDoc) {
        return true;
    }
    #[cfg(feature = "mcp")]
    {
        matches!(kind, OperationKind::McpTool)
    }
    #[cfg(not(feature = "mcp"))]
    {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::{Json, RouteConf};

    const EMAIL_TASK_LANE: crate::tasks::TaskLane = crate::tasks::TaskLane::new("email");

    async fn ping() -> Json<&'static str> {
        Json("pong")
    }

    fn route_op(name: &str, path: &str, methods: routes::Methods) -> crate::callables::Operation {
        crate::callables::Operation {
            id: crate::OperationId::new(),
            name: name.to_string(),
            description: None,
            summary: None,
            openapi_id: None,
            deprecated: false,
            path: path.to_string(),
            kind: OperationKind::Route,
            methods,
            args: Vec::new(),
            layers: Vec::new(),
            returns: Vec::new(),
            tags: Vec::new(),
            conf: None,
            owner: None,
            hidden: false,
            audience: None,
            bundle_id: None,
            slash_policy: None,
        }
    }

    fn bundle_with_route(op: crate::callables::Operation) -> Bundle {
        let mut bundle = Bundle::new();
        bundle.name_index.insert(op.name.clone(), op.id);
        bundle.ops.insert(op.id, op);
        bundle
    }

    fn non_route_part(op: crate::callables::Operation) -> part::BundlePart {
        part::BundlePart {
            part: part::BundlePartInner::Error(BundleError::InvalidRouteName {
                name: "ignored".to_string(),
                reason: "test marker".to_string(),
            }),
            operation: Some(op),
        }
    }

    #[test]
    fn route_operation_gets_origin_bundle_id_on_insertion() {
        let bundle = crate::bundles::bundle([crate::bundles::route(
            ping,
            RouteConf {
                name: "ping".into(),
                methods: routes::Methods::GET,
                path: "/ping".into(),
                slash: None,
            },
        )]);
        let route_id = *bundle.name_index.get("ping").unwrap();
        let op = bundle.ops.get(&route_id).unwrap();

        assert_eq!(op.bundle_id, Some(bundle.id));
    }

    #[test]
    fn non_route_operation_gets_origin_bundle_id_on_insertion() {
        let op = crate::callables::Operation {
            kind: OperationKind::Signal,
            ..route_op("note_changed", "", routes::Methods::POST)
        };
        let op_id = op.id;
        let bundle = Bundle::new().add_part(non_route_part(op));
        let op = bundle.ops.get(&op_id).unwrap();

        assert_eq!(op.bundle_id, Some(bundle.id));
    }

    #[test]
    fn merge_does_not_rewrite_child_origin_bundle_id() {
        let child = crate::bundles::bundle([crate::bundles::route(
            ping,
            RouteConf {
                name: "ping".into(),
                methods: routes::Methods::GET,
                path: "/ping".into(),
                slash: None,
            },
        )]);
        let child_id = child.id;
        let route_id = *child.name_index.get("ping").unwrap();
        let parent = Bundle::new();
        let parent_id = parent.id;
        let merged = parent.merge(child);
        let op = merged.ops.get(&route_id).unwrap();

        assert_eq!(op.bundle_id, Some(child_id));
        assert_ne!(op.bundle_id, Some(parent_id));
    }

    #[test]
    fn validates_route_path() {
        let bundle = Bundle::new();
        let err = bundle
            .validate_route_operation(&route_op("bad", "bad", routes::Methods::GET))
            .unwrap_err();
        assert!(matches!(err, BundleError::InvalidRoutePath { .. }));
    }

    /// Verifies route paths and prefixes reject URL query and fragment syntax.
    #[test]
    fn rejects_query_and_fragment_route_syntax() {
        for path in ["/users?active=true", "/users#details"] {
            assert!(validate_route_path(path).is_err());
            assert!(validate_route_prefix(path).is_err());
        }
    }

    #[test]
    fn validates_route_name() {
        let bundle = Bundle::new();
        let err = bundle
            .validate_route_operation(&route_op("", "/ok", routes::Methods::GET))
            .unwrap_err();
        assert!(matches!(err, BundleError::InvalidRouteName { .. }));
    }

    #[test]
    fn rejects_duplicate_route_names() {
        let existing = route_op("notes", "/notes", routes::Methods::GET);
        let bundle = bundle_with_route(existing);
        let err = bundle
            .validate_route_operation(&route_op("notes", "/other", routes::Methods::GET))
            .unwrap_err();
        assert!(matches!(err, BundleError::DuplicateRouteName { .. }));
    }

    #[test]
    fn rejects_duplicate_route_path_method_pairs() {
        let existing = route_op(
            "list_notes",
            "/notes",
            routes::Methods::GET | routes::Methods::HEAD,
        );
        let bundle = bundle_with_route(existing);
        let err = bundle
            .validate_route_operation(&route_op("read_notes", "/notes", routes::Methods::HEAD))
            .unwrap_err();
        assert!(matches!(err, BundleError::DuplicateRoutePathMethod { .. }));
    }

    #[test]
    fn allows_same_path_with_different_methods() {
        let existing = route_op("list_notes", "/notes", routes::Methods::GET);
        let bundle = bundle_with_route(existing);
        assert!(
            bundle
                .validate_route_operation(&route_op("create_note", "/notes", routes::Methods::POST))
                .is_ok()
        );
    }

    /// Verifies every Vyuh route documents the shared 405 ErrorReport response.
    #[test]
    fn route_documents_method_error() -> Result<(), String> {
        let bundle = crate::bundles::bundle([crate::bundles::route(
            ping,
            RouteConf {
                name: "ping".into(),
                methods: routes::Methods::GET,
                path: "/ping".into(),
                slash: None,
            },
        )]);
        let id = bundle
            .name_index
            .get("ping")
            .ok_or_else(|| "route name was not registered".to_string())?;
        let operation = bundle
            .ops
            .get(id)
            .ok_or_else(|| "route operation was not registered".to_string())?;
        let response = operation
            .returns
            .iter()
            .find(|response| response.status_code == Some(405))
            .ok_or_else(|| "route did not document 405".to_string())?;

        assert_eq!(response.schema_name.as_deref(), Some("ErrorReport"));
        Ok(())
    }

    #[test]
    fn invalid_prefix_accumulates_error() {
        let bundle = Bundle::new().with_prefix("api");
        assert!(matches!(
            bundle.errors.as_slice(),
            [BundleError::InvalidRoutePrefix { .. }]
        ));
    }

    /// Verifies two reusable bundles cannot silently contribute the same lane default.
    #[test]
    fn duplicate_task_lane_defaults_accumulate_a_bundle_error() {
        let left = Bundle::new()
            .with_conf(conf().task_lane(crate::tasks::TaskLaneConf::new(EMAIL_TASK_LANE, 1)));
        let right = Bundle::new()
            .with_conf(conf().task_lane(crate::tasks::TaskLaneConf::new(EMAIL_TASK_LANE, 1)));
        let mut bundle = left.merge(right);
        let result = bundle.finalize_conf(None);

        assert!(matches!(result, Err(BundleError::Task(_))));
    }
}
