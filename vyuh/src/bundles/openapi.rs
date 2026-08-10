use std::collections::BTreeMap;

use axum::{
    extract::FromRequestParts,
    response::{IntoResponse, Response},
};
use bytes::Bytes;

use crate::OperationId;
use crate::Site;
use crate::apidocs::{ApiDocGenerator, ApiMeta, DocViewer, OpenApiVersion};
use crate::auth::{AudienceId, AuthConf, AuthUser};
use crate::callables::{Operation, OperationKind};
use crate::routes::AxumRouter;

use super::{Bundle, BundleError};

// ---------------------------------------------------------------------------
// Public configuration type
// ---------------------------------------------------------------------------

/// Configuration for the OpenAPI spec endpoint.
#[derive(Clone, Debug)]
pub struct OpenApiConf {
    /// Path where the OpenAPI JSON spec is served (e.g. `"/api/openapi.json"`).
    pub spec_path: String,
    pub meta: ApiMeta,
    pub openapi_version: OpenApiVersion,
    pub viewer: Option<OpenApiViewerConf>,
    /// Authentication policy for OpenAPI endpoints.
    ///
    /// The default requires a valid configured credential. Returning `false`
    /// from the predicate yields `403 Forbidden`; use [`Self::public`] only
    /// when publishing the complete API inventory is intentional.
    pub auth: Option<fn(&AuthUser) -> bool>,
}

/// Optional OpenAPI documentation viewer declaration.
#[derive(Clone, Debug)]
pub struct OpenApiViewerConf {
    pub path: String,
    pub viewer: DocViewer,
}

impl OpenApiViewerConf {
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            viewer: DocViewer::Swagger,
        }
    }

    pub fn with_viewer(path: impl Into<String>, viewer: DocViewer) -> Self {
        Self {
            path: path.into(),
            viewer,
        }
    }
}

impl Default for OpenApiConf {
    fn default() -> Self {
        Self {
            spec_path: "/openapi.json".to_string(),
            meta: ApiMeta::default(),
            openapi_version: OpenApiVersion::V30,
            viewer: None,
            auth: Some(authenticated),
        }
    }
}

impl OpenApiConf {
    /// Override the path for the JSON spec endpoint.
    pub fn spec(mut self, path: impl Into<String>) -> Self {
        self.spec_path = path.into();
        self
    }

    /// Enable the Swagger viewer UI at the given path.
    pub fn viewer(mut self, path: impl Into<String>) -> Self {
        self.viewer = Some(OpenApiViewerConf::new(path));
        self
    }

    /// Enable a specific documentation viewer UI at the given path.
    pub fn viewer_with(mut self, path: impl Into<String>, viewer: DocViewer) -> Self {
        self.viewer = Some(OpenApiViewerConf::with_viewer(path, viewer));
        self
    }

    /// Deliberately exposes the OpenAPI spec and optional viewer without authentication.
    pub fn public(mut self) -> Self {
        self.auth = None;
        self
    }

    /// Set the API title.
    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.meta.title = title.into();
        self
    }

    /// Set the API version string.
    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.meta.version = version.into();
        self
    }

    /// Set the OpenAPI document version.
    pub fn openapi_version(mut self, version: OpenApiVersion) -> Self {
        self.openapi_version = version;
        self
    }

    /// Set the API description.
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.meta.description = Some(description.into());
        self
    }

    /// Set the API tags.
    pub fn tags(mut self, tags: Vec<crate::apidocs::TagInfo>) -> Self {
        self.meta.tags = tags;
        self
    }

    /// Restricts the OpenAPI endpoints to authenticated users accepted by `pred`.
    ///
    /// The predicate receives the extracted `AuthUser`; returning `false`
    /// yields `403 Forbidden`.
    pub fn auth(mut self, pred: fn(&AuthUser) -> bool) -> Self {
        self.auth = Some(pred);
        self
    }
}

// ---------------------------------------------------------------------------
// DocEngine internals
// ---------------------------------------------------------------------------

/// A single OpenAPI doc registration inside a bundle.
/// Holds stable operation UUIDs rather than live references so that
/// `with_prefix` path updates propagate automatically before `setup` is called.
pub(super) struct DocNode {
    /// UUID of the hidden spec-route operation in `Bundle::ops`.
    spec_op_id: OperationId,
    /// UUID of the hidden viewer-route operation in `Bundle::ops`, if any.
    doc_op_id: Option<OperationId>,
    /// UUIDs of the visible operations to include in the generated spec.
    operation_ids: Vec<OperationId>,
    meta: ApiMeta,
    openapi_version: OpenApiVersion,
    viewer: DocViewer,
    auth: Option<fn(&AuthUser) -> bool>,
}

/// Collects OpenAPI doc registrations from across the bundle graph.
/// Merged together in `Bundle::absorb`; finalised by `setup` in `SiteBuilder`.
pub(crate) struct DocEngine {
    nodes: Vec<DocNode>,
}

impl DocEngine {
    pub(super) fn new() -> Self {
        Self { nodes: Vec::new() }
    }

    pub(super) fn register(&mut self, node: DocNode) {
        self.nodes.push(node);
    }

    pub(crate) fn merge(&mut self, other: DocEngine) {
        self.nodes.extend(other.nodes);
    }

    /// Mounts spec routes, plus hidden viewer routes when explicitly enabled,
    /// for every registered `DocNode`.
    ///
    /// Called once from `SiteBuilder::build`, after all `merge` / `with_prefix`
    /// calls are complete. Generates spec JSON synchronously so errors surface
    /// at startup. Returns a `BundleError` on doc-generation failure.
    pub(crate) fn setup(
        &self,
        router: &mut AxumRouter<Site>,
        ops: &BTreeMap<OperationId, Operation>,
        auth: &AuthConf,
    ) -> Result<(), BundleError> {
        for node in &self.nodes {
            let spec_path = ops
                .get(&node.spec_op_id)
                .map(|op| op.path.clone())
                .unwrap_or_else(|| node.spec_op_id.to_string());
            let spec_audience = ops
                .get(&node.spec_op_id)
                .and_then(|operation| operation.audience_id().cloned());

            let views: Vec<&Operation> = node
                .operation_ids
                .iter()
                .filter_map(|id| ops.get(id))
                .collect();

            let spec_bytes = generate_spec(&views, &node.meta, node.openapi_version, auth)?;

            // Both captures are plain Bytes / String — cheap clones on each request.
            let auth_pred = node.auth;
            let spec_route = {
                let b = spec_bytes;
                let audience = spec_audience.clone();
                axum::routing::get(
                    move |axum::extract::State(site): axum::extract::State<Site>,
                          req: axum::extract::Request| {
                        let body = b.clone();
                        async move {
                            use axum::http::{StatusCode, header};
                            if let Err(response) =
                                authorize_docs(&site, req, audience.clone(), auth_pred).await
                            {
                                return response;
                            }
                            (
                                StatusCode::OK,
                                [(header::CONTENT_TYPE, "application/json")],
                                body,
                            )
                                .into_response()
                        }
                    },
                )
            };

            *router = std::mem::take(router).route(&spec_path, spec_route);

            if let Some(doc_op_id) = node.doc_op_id {
                let doc_path = ops
                    .get(&doc_op_id)
                    .map(|op| op.path.clone())
                    .unwrap_or_else(|| doc_op_id.to_string());
                let viewer_audience = ops
                    .get(&doc_op_id)
                    .and_then(|operation| operation.audience_id().cloned());
                let viewer_html = generate_viewer(&doc_path, &spec_path, node.viewer);
                let viewer_route = {
                    let h = viewer_html;
                    let audience = viewer_audience;
                    axum::routing::get(
                        move |axum::extract::State(site): axum::extract::State<Site>,
                              req: axum::extract::Request| {
                            let body = h.clone();
                            async move {
                                use axum::http::{StatusCode, header};
                                if let Err(response) =
                                    authorize_docs(&site, req, audience.clone(), auth_pred).await
                                {
                                    return response;
                                }
                                (
                                    StatusCode::OK,
                                    [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
                                    body,
                                )
                                    .into_response()
                            }
                        },
                    )
                };
                *router = std::mem::take(router).route(&doc_path, viewer_route);
            }
        }
        Ok(())
    }
}

fn authenticated(_: &AuthUser) -> bool {
    true
}

/// Authenticates one documentation request and applies its configured access policy.
async fn authorize_docs(
    site: &Site,
    request: axum::extract::Request,
    audience: Option<AudienceId>,
    predicate: Option<fn(&AuthUser) -> bool>,
) -> Result<(), Response> {
    let Some(predicate) = predicate else {
        return Ok(());
    };
    let (mut parts, _) = request.into_parts();
    if let Some(audience) = audience {
        parts.extensions.insert(super::BundleRequestContext {
            audience: Some(audience),
        });
    }
    let user = AuthUser::from_request_parts(&mut parts, site)
        .await
        .map_err(IntoResponse::into_response)?;
    predicate(&user)
        .then_some(())
        .ok_or_else(|| axum::http::StatusCode::FORBIDDEN.into_response())
}

// ---------------------------------------------------------------------------
// Bundle::with_openapi
// ---------------------------------------------------------------------------

impl Bundle {
    /// Registers an OpenAPI JSON spec endpoint for this bundle.
    ///
    /// Hidden `Operation` markers (kind `ApiDoc`, `hidden = true`) are inserted
    /// into the operation map so that `with_prefix` updates their paths automatically.
    /// `DocEngine::setup` resolves them to final paths at startup.
    pub fn with_openapi(mut self, conf: OpenApiConf) -> Self {
        // Snapshot UUIDs of visible operations now; the ops map is the canonical store.
        let operation_ids: Vec<OperationId> = self
            .ops
            .values()
            .filter(|op| !op.hidden && op.kind == OperationKind::Route)
            .map(|op| op.id)
            .collect();

        let mut spec_op = crate::callables::Operation::from_api_doc(
            &format!("__spec__{}", conf.spec_path),
            &conf.spec_path,
        );
        spec_op.assign_bundle_id(self.id);
        let spec_op_id = spec_op.id;
        self.ops.insert(spec_op_id, spec_op);

        let viewer = conf.viewer;
        let doc_op_id = viewer.as_ref().map(|viewer| {
            let mut op = crate::callables::Operation::from_api_doc(
                &format!("__doc__{}", viewer.path),
                &viewer.path,
            );
            op.assign_bundle_id(self.id);
            let id = op.id;
            self.ops.insert(id, op);
            id
        });

        self.doc_engine.register(DocNode {
            spec_op_id,
            doc_op_id,
            operation_ids,
            meta: conf.meta,
            openapi_version: conf.openapi_version,
            viewer: viewer
                .as_ref()
                .map(|viewer| viewer.viewer)
                .unwrap_or(DocViewer::Swagger),
            auth: conf.auth,
        });
        self
    }
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

fn generate_spec(
    views: &[&Operation],
    meta: &ApiMeta,
    openapi_version: OpenApiVersion,
    auth: &AuthConf,
) -> Result<Bytes, BundleError> {
    let doc_gen = ApiDocGenerator::new(meta.clone())
        .with_auth(auth.clone())
        .with_version(openapi_version);
    let api = doc_gen
        .generate(views)
        .map_err(|e| BundleError::DocGen(e.to_string()))?;
    let vec = serde_json::to_vec(&api).map_err(|e| BundleError::DocGen(e.to_string()))?;
    Ok(Bytes::from(vec))
}

fn generate_viewer(doc_path: &str, spec_path: &str, viewer: DocViewer) -> String {
    // Compute a relative URL so the viewer works regardless of the mount prefix.
    let from_dir = doc_path.rfind('/').map(|i| &doc_path[..=i]).unwrap_or("/");
    let relative = spec_path.strip_prefix(from_dir).unwrap_or(spec_path);
    let html = ApiDocGenerator::serve_doc(relative, viewer);
    html.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::Methods;

    fn operation(kind: OperationKind, name: &str, path: &str) -> Operation {
        Operation {
            id: OperationId::new(),
            name: name.to_string(),
            description: None,
            summary: None,
            openapi_id: None,
            deprecated: false,
            path: path.to_string(),
            kind,
            methods: Methods::GET,
            args: Vec::new(),
            layers: Vec::new(),
            returns: Vec::new(),
            tags: Vec::new(),
            conf: None,
            owner: None,
            hidden: false,
            #[cfg(feature = "mcp")]
            mcp: None,
            audience: None,
            bundle_id: None,
            slash_policy: None,
        }
    }

    #[test]
    fn viewer_uses_relative_spec_path_after_prefixing() {
        let html = generate_viewer("/v1/api/docs", "/v1/api/openapi.json", DocViewer::Swagger);
        assert!(html.contains("openapi.json"));
        assert!(!html.contains("/v1/api/openapi.json"));
    }

    #[test]
    fn with_openapi_snapshots_only_visible_routes() {
        let route = operation(OperationKind::Route, "list_notes", "/notes");
        let signal = operation(OperationKind::Signal, "note_changed", "");
        let hidden_route = Operation {
            hidden: true,
            ..operation(OperationKind::Route, "hidden_notes", "/hidden")
        };

        let route_id = route.id;
        let signal_id = signal.id;
        let hidden_route_id = hidden_route.id;

        let mut bundle = Bundle::new();
        bundle.ops.insert(route_id, route);
        bundle.ops.insert(signal_id, signal);
        bundle.ops.insert(hidden_route_id, hidden_route);

        let bundle = bundle.with_openapi(OpenApiConf::default());
        let operation_ids = &bundle.doc_engine.nodes[0].operation_ids;

        assert_eq!(operation_ids, &[route_id]);
    }

    #[test]
    fn default_openapi_registers_only_json_spec_marker() {
        let bundle = Bundle::new().with_openapi(OpenApiConf::default());
        let node = &bundle.doc_engine.nodes[0];
        let spec_op = bundle.ops.get(&node.spec_op_id).unwrap();

        assert_eq!(spec_op.kind, OperationKind::ApiDoc);
        assert_eq!(spec_op.path, "/openapi.json");
        assert_eq!(spec_op.bundle_id, Some(bundle.id));
        assert!(node.doc_op_id.is_none());
    }

    #[test]
    fn optional_viewer_registers_hidden_viewer_marker_with_origin_bundle_id() {
        let bundle = Bundle::new()
            .with_openapi(OpenApiConf::default().viewer_with("/docs", DocViewer::Redoc));
        let node = &bundle.doc_engine.nodes[0];
        let doc_op_id = node.doc_op_id.unwrap();
        let doc_op = bundle.ops.get(&doc_op_id).unwrap();

        assert_eq!(node.viewer, DocViewer::Redoc);
        assert_eq!(doc_op.kind, OperationKind::ApiDoc);
        assert!(doc_op.hidden);
        assert_eq!(doc_op.path, "/docs");
        assert_eq!(doc_op.bundle_id, Some(bundle.id));
    }

    #[test]
    fn prefixed_openapi_viewer_uses_final_paths_and_relative_spec_url() {
        let bundle = Bundle::new()
            .with_openapi(
                OpenApiConf::default()
                    .spec("/api/openapi.json")
                    .viewer("/api/docs"),
            )
            .with_prefix("/v1");
        let node = &bundle.doc_engine.nodes[0];
        let spec_path = &bundle.ops.get(&node.spec_op_id).unwrap().path;
        let doc_path = &bundle.ops.get(&node.doc_op_id.unwrap()).unwrap().path;

        assert_eq!(spec_path, "/v1/api/openapi.json");
        assert_eq!(doc_path, "/v1/api/docs");

        let html = generate_viewer(doc_path, spec_path, node.viewer);
        assert!(html.contains("openapi.json"));
        assert!(!html.contains("/v1/api/openapi.json"));
    }
}
