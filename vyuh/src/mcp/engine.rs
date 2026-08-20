//! Bundle registration and HTTP endpoint runtime for MCP.

use axum::{
    Json,
    body::to_bytes,
    extract::{Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};

use crate::{
    OperationId, Site,
    auth::{AudienceId, AuthError, AuthProtectedResource, Authenticator},
    callables::Operation,
    routes::AxumRouter,
};

use super::{
    McpConf, McpError, McpResourceRegistry, McpToolRegistry, protocol,
    resources::ResourceDefinition, tools, tools::ToolDefinition,
};

const MAX_REQUEST_BYTES: usize = 1024 * 1024;

struct McpNode {
    marker_id: OperationId,
    anchor_id: uuid::Uuid,
    operation_ids: Vec<OperationId>,
    resource_uris: Vec<String>,
    conf: McpConf,
}

/// Bundle-owned MCP declarations finalized after composition and prefixing.
pub(crate) struct McpEngine {
    nodes: Vec<McpNode>,
}

impl McpEngine {
    pub(crate) fn new() -> Self {
        Self { nodes: Vec::new() }
    }

    pub(crate) fn merge(&mut self, other: Self) {
        self.nodes.extend(other.nodes);
    }

    pub(crate) fn finalize(
        &mut self,
        operations: &BTreeMap<OperationId, Operation>,
        topology: &crate::bundles::BundleTopology,
        registry: &mut McpToolRegistry,
        resources: &mut McpResourceRegistry,
    ) -> Result<(), McpError> {
        registry.clear_claims();
        resources.clear_claims();
        for node in &mut self.nodes {
            node.operation_ids.clear();
            node.resource_uris.clear();
        }
        for operation in operations
            .values()
            .filter(|value| value.kind == crate::callables::OperationKind::McpTool)
        {
            let Some(bundle_id) = operation.bundle_id else {
                continue;
            };
            let Some(index) = self.nearest_node(topology, bundle_id) else {
                continue;
            };
            let marker = operations
                .get(&self.nodes[index].marker_id)
                .ok_or_else(|| McpError::Config("MCP endpoint marker is missing".into()))?;
            if marker.audience_id() != operation.audience_id() {
                return Err(McpError::Config(format!(
                    "MCP tool '{}' has a different effective audience than its service",
                    operation.name
                )));
            }
            self.nodes[index].operation_ids.push(operation.id);
        }
        let claimed = self
            .nodes
            .iter()
            .flat_map(|node| node.operation_ids.iter().copied());
        registry.claim(claimed);
        self.assign_resources(topology, resources);
        Ok(())
    }

    /// Assigns static resources to their nearest enclosing MCP service.
    fn assign_resources(
        &mut self,
        topology: &crate::bundles::BundleTopology,
        resources: &mut McpResourceRegistry,
    ) {
        let registrations = resources
            .owners()
            .map(|(uri, owner)| (uri.to_string(), owner))
            .collect::<Vec<_>>();
        for (uri, owner) in registrations {
            let Some(index) = self.nearest_node(topology, owner) else {
                continue;
            };
            self.nodes[index].resource_uris.push(uri);
        }
        let claimed = self
            .nodes
            .iter()
            .flat_map(|node| node.resource_uris.iter().cloned());
        resources.claim(claimed);
    }

    fn nearest_node(
        &self,
        topology: &crate::bundles::BundleTopology,
        bundle_id: uuid::Uuid,
    ) -> Option<usize> {
        self.nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| topology.contains(node.anchor_id, bundle_id))
            .map(|(index, _)| index)
            .max_by_key(|index| self.depth(topology, self.nodes[*index].anchor_id))
    }

    fn depth(&self, topology: &crate::bundles::BundleTopology, id: uuid::Uuid) -> usize {
        topology.lineage(id).len()
    }

    pub(crate) fn setup(
        &self,
        router: &mut AxumRouter<Site>,
        operations: &BTreeMap<OperationId, Operation>,
        registry: &McpToolRegistry,
        resources: &McpResourceRegistry,
        auth: &Authenticator,
    ) -> Result<(), crate::bundles::BundleError> {
        self.validate_nodes(operations)
            .map_err(|error| crate::bundles::BundleError::Mcp(error.to_string()))?;
        for node in &self.nodes {
            self.setup_node(router, operations, registry, resources, auth, node)
                .map_err(|error| crate::bundles::BundleError::Mcp(error.to_string()))?;
        }
        Ok(())
    }

    fn setup_node(
        &self,
        router: &mut AxumRouter<Site>,
        operations: &BTreeMap<OperationId, Operation>,
        registry: &McpToolRegistry,
        resources: &McpResourceRegistry,
        auth: &Authenticator,
        node: &McpNode,
    ) -> Result<(), McpError> {
        let endpoint = operations
            .get(&node.marker_id)
            .map(|operation| operation.path.clone())
            .ok_or_else(|| McpError::Config("MCP endpoint marker is missing".to_string()))?;
        validate_collisions(operations, &endpoint, node.marker_id)?;
        let resource_definitions = resources.definitions(&node.resource_uris)?;
        let definitions = tools::definitions(
            &node.operation_ids,
            operations,
            registry,
            &resource_definitions,
        )?;
        if node.conf.auth.is_none()
            && definitions
                .values()
                .any(|tool| tool.authorization.requires_identity())
        {
            return Err(McpError::Config(format!(
                "anonymous MCP service '{endpoint}' contains authenticated tools"
            )));
        }
        let audience = operations
            .get(&node.marker_id)
            .and_then(|operation| operation.audience_id().cloned());
        let protected = audience
            .as_ref()
            .and_then(|value| auth.mcp_protected_resource(value));
        if protected.is_some() {
            let metadata = protected_resource_path(&endpoint);
            validate_well_known(operations, &metadata)?;
        }
        let runtime = Arc::new(McpRuntime::new(
            node.conf.clone(),
            audience,
            definitions,
            resource_definitions,
            protected,
        )?);
        let endpoint_runtime = Arc::clone(&runtime);
        let endpoint_route =
            axum::routing::post(move |State(site): State<Site>, request: Request| {
                let runtime = Arc::clone(&endpoint_runtime);
                async move { handle_mcp(runtime, site, request).await }
            })
            .get(|| async { StatusCode::METHOD_NOT_ALLOWED });
        *router = crate::slash::route(std::mem::take(router), &endpoint, endpoint_route);
        if let Some(metadata) = runtime.metadata() {
            let well_known = protected_resource_path(&endpoint);
            let metadata_route = axum::routing::get(move || {
                let metadata = metadata.clone();
                async move { metadata }
            });
            *router = crate::slash::route(std::mem::take(router), &well_known, metadata_route);
        }
        Ok(())
    }

    fn validate_nodes(
        &self,
        operations: &BTreeMap<OperationId, Operation>,
    ) -> Result<(), McpError> {
        let mut endpoints = std::collections::BTreeSet::new();
        let mut resources = std::collections::BTreeSet::new();
        let mut errors = Vec::new();
        for node in &self.nodes {
            validate_node_collisions(
                node,
                operations,
                &mut endpoints,
                &mut resources,
                &mut errors,
            );
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(McpError::Config(errors.join("; ")))
        }
    }
}

impl crate::bundles::Bundle {
    pub(crate) fn register_mcp(&mut self, mut conf: McpConf) {
        if let Err(error) = conf.validate() {
            self.errors
                .push(crate::bundles::BundleError::Mcp(error.to_string()));
            return;
        }
        let mut marker =
            Operation::from_api_doc(&format!("__mcp__{}", conf.endpoint), &conf.endpoint);
        marker.methods = crate::routes::Methods::POST;
        marker.assign_bundle_id(self.id);
        let marker_id = marker.id;
        self.ops.insert(marker_id, marker);
        self.mcp_engine.nodes.push(McpNode {
            marker_id,
            anchor_id: self.id,
            operation_ids: Vec::new(),
            resource_uris: Vec::new(),
            conf,
        });
    }
}

struct McpRuntime {
    conf: McpConf,
    audience: Option<AudienceId>,
    tools: BTreeMap<String, ToolDefinition>,
    resources: BTreeMap<String, ResourceDefinition>,
    resource_metadata_url: Option<String>,
    metadata: Option<Json<Value>>,
    required_scopes: Vec<String>,
}

impl McpRuntime {
    fn new(
        conf: McpConf,
        audience: Option<AudienceId>,
        tools: BTreeMap<String, ToolDefinition>,
        resources: BTreeMap<String, ResourceDefinition>,
        protected: Option<AuthProtectedResource>,
    ) -> Result<Self, McpError> {
        let resource_metadata_url = protected
            .as_ref()
            .map(|_| {
                audience
                    .as_ref()
                    .ok_or_else(|| {
                        McpError::Config("protected MCP endpoint requires an audience".into())
                    })
                    .and_then(|value| protected_resource_url(value.as_str()))
            })
            .transpose()?;
        let required_scopes = protected
            .as_ref()
            .map(|value| value.required_scopes.clone())
            .unwrap_or_default();
        let metadata = protected.map(|value| {
            Json(json!({
                "resource": audience.as_ref().map(AudienceId::as_str), "authorization_servers": [value.issuer],
                "scopes_supported": value.advertised_scopes, "bearer_methods_supported": ["header"]
            }))
        });
        Ok(Self {
            conf,
            audience,
            tools,
            resources,
            resource_metadata_url,
            metadata,
            required_scopes,
        })
    }

    fn metadata(&self) -> Option<Json<Value>> {
        self.metadata.clone()
    }

    async fn process(
        &self,
        site: Site,
        request: RpcEnvelope,
        user: Option<crate::auth::AuthUser>,
    ) -> RpcReply {
        if let Err(response) = request.rpc.validate() {
            return RpcReply::status(StatusCode::BAD_REQUEST, response);
        }
        let revision = match protocol::revision(&request.headers, &request.rpc) {
            Ok(value) => value,
            Err(response) => return RpcReply::status(StatusCode::BAD_REQUEST, response),
        };
        self.method(site, request.rpc, user, revision).await
    }

    async fn method(
        &self,
        site: Site,
        request: protocol::RpcRequest,
        user: Option<crate::auth::AuthUser>,
        revision: protocol::Revision,
    ) -> RpcReply {
        if request.method == "notifications/initialized" && !revision.modern() {
            return RpcReply::empty();
        }
        if let Some(value) = protocol::discovery(&request.method, revision) {
            return RpcReply::result(request.id, value);
        }
        match request.method.as_str() {
            "tools/list" => {
                let tools = self
                    .tools
                    .values()
                    .filter(|tool| tool.authorization.allows(user.as_ref()));
                RpcReply::result(request.id, protocol::tool_list(tools, revision))
            }
            "resources/list" => RpcReply::result(
                request.id,
                protocol::resource_list(self.resources.values(), revision),
            ),
            "resources/read" => self.read_resource(request, revision),
            "tools/call" => self.call(site, request, user, revision.modern()).await,
            _ if revision.modern() => RpcReply::status(
                StatusCode::NOT_FOUND,
                protocol::RpcResponse::error(request.id, -32601, "method not found"),
            ),
            _ => RpcReply::response(protocol::RpcResponse::error(
                request.id,
                -32601,
                "method not found",
            )),
        }
    }

    /// Reads one static resource after the endpoint authentication boundary.
    fn read_resource(
        &self,
        request: protocol::RpcRequest,
        revision: protocol::Revision,
    ) -> RpcReply {
        let read: protocol::ResourceRead = match serde_json::from_value(request.params) {
            Ok(value) => value,
            Err(_) => return RpcReply::invalid_params(request.id),
        };
        let Some(resource) = self.resources.get(&read.uri) else {
            return RpcReply::response(protocol::resource_not_found(request.id, read.uri));
        };
        RpcReply::result(request.id, protocol::resource_read(resource, revision))
    }

    async fn call(
        &self,
        site: Site,
        request: protocol::RpcRequest,
        user: Option<crate::auth::AuthUser>,
        modern: bool,
    ) -> RpcReply {
        let call: protocol::ToolCall = match serde_json::from_value(request.params) {
            Ok(value) => value,
            Err(_) => return RpcReply::invalid_params(request.id),
        };
        let Some(tool) = self
            .tools
            .get(&call.name)
            .filter(|tool| tool.authorization.allows(user.as_ref()))
        else {
            return unknown_tool(request.id);
        };
        match super::dispatch::invoke(site, tool, call.arguments, user, modern).await {
            Ok(value) => RpcReply::result(request.id, value),
            Err(McpError::Arguments(_)) => RpcReply::invalid_params(request.id),
            Err(error) => RpcReply::result(
                request.id,
                protocol::tool_error(500, public_tool_error(&error), modern),
            ),
        }
    }
}

struct RpcEnvelope {
    headers: HeaderMap,
    rpc: protocol::RpcRequest,
}

enum RpcReply {
    Json(StatusCode, Box<protocol::RpcResponse>),
    Empty,
}

impl RpcReply {
    fn response(value: protocol::RpcResponse) -> Self {
        Self::Json(StatusCode::OK, Box::new(value))
    }

    fn status(status: StatusCode, value: impl Into<Box<protocol::RpcResponse>>) -> Self {
        Self::Json(status, value.into())
    }

    fn result(id: Option<Value>, value: Value) -> Self {
        Self::Json(
            StatusCode::OK,
            Box::new(protocol::RpcResponse::result(id, value)),
        )
    }

    fn invalid_params(id: Option<Value>) -> Self {
        Self::Json(
            StatusCode::OK,
            Box::new(protocol::RpcResponse::error(id, -32602, "invalid params")),
        )
    }

    fn empty() -> Self {
        Self::Empty
    }

    fn into_response(self) -> Response {
        match self {
            Self::Json(status, value) => (status, Json(value)).into_response(),
            Self::Empty => StatusCode::ACCEPTED.into_response(),
        }
    }
}

async fn handle_mcp(runtime: Arc<McpRuntime>, site: Site, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let headers = parts.headers.clone();
    let resource = runtime
        .audience
        .as_ref()
        .map(AudienceId::as_str)
        .unwrap_or("http://localhost");
    if let Err(status) = validate_transport(&headers, resource) {
        return status.into_response();
    }
    let user = match (runtime.conf.auth, runtime.audience.as_ref()) {
        (Some(predicate), Some(audience)) => match site.auth().authenticate(&parts, audience).await
        {
            Ok(user) if predicate(&user) => Some(user),
            Ok(_) => return StatusCode::FORBIDDEN.into_response(),
            Err(error) => return auth_error(&runtime, error),
        },
        (Some(_), None) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        (None, _) => None,
    };
    let bytes = match to_bytes(body, MAX_REQUEST_BYTES).await {
        Ok(value) => value,
        Err(_) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
    };
    let rpc = match serde_json::from_slice(&bytes) {
        Ok(value) => value,
        Err(_) => {
            return Json(protocol::RpcResponse::error(None, -32700, "parse error")).into_response();
        }
    };
    runtime
        .process(site, RpcEnvelope { headers, rpc }, user)
        .await
        .into_response()
}

fn auth_error(runtime: &McpRuntime, error: AuthError) -> Response {
    let (status, code) = match &error {
        AuthError::InsufficientScope | AuthError::AudienceMismatch | AuthError::Forbidden => {
            (StatusCode::FORBIDDEN, "insufficient_scope")
        }
        AuthError::ProviderUnavailable => {
            (StatusCode::SERVICE_UNAVAILABLE, "temporarily_unavailable")
        }
        _ => (StatusCode::UNAUTHORIZED, "invalid_token"),
    };
    let mut response = (status, Json(json!({"error": code}))).into_response();
    if let Some(metadata) = runtime.resource_metadata_url.as_deref() {
        let scope = if status == StatusCode::FORBIDDEN && !runtime.required_scopes.is_empty() {
            format!(", scope=\"{}\"", runtime.required_scopes.join(" "))
        } else {
            String::new()
        };
        let challenge = format!("Bearer resource_metadata=\"{metadata}\", error=\"{code}\"{scope}");
        if let Ok(value) = HeaderValue::from_str(&challenge) {
            response
                .headers_mut()
                .insert(header::WWW_AUTHENTICATE, value);
        }
    }
    response
}

fn validate_transport(headers: &HeaderMap, resource_url: &str) -> Result<(), StatusCode> {
    validate_origin(headers, resource_url)?;
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    let media_type = content_type.split(';').next().unwrap_or("").trim();
    if !media_type.eq_ignore_ascii_case("application/json") {
        return Err(StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }
    let accept = headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("*/*");
    let accept = accept.to_ascii_lowercase();
    if accept.contains("application/json") || accept.contains("*/*") {
        return Ok(());
    }
    Err(StatusCode::NOT_ACCEPTABLE)
}

fn validate_origin(headers: &HeaderMap, resource_url: &str) -> Result<(), StatusCode> {
    let Some(origin) = headers.get(header::ORIGIN) else {
        return Ok(());
    };
    let origin = origin.to_str().map_err(|_| StatusCode::FORBIDDEN)?;
    let resource = url::Url::parse(resource_url).map_err(|_| StatusCode::FORBIDDEN)?;
    let supplied = url::Url::parse(origin).map_err(|_| StatusCode::FORBIDDEN)?;
    let clean = supplied.username().is_empty()
        && supplied.password().is_none()
        && supplied.path() == "/"
        && supplied.query().is_none()
        && supplied.fragment().is_none();
    if clean && supplied.origin() == resource.origin() {
        return Ok(());
    }
    Err(StatusCode::FORBIDDEN)
}

fn public_tool_error(error: &McpError) -> &'static str {
    let _ = error;
    "tool execution failed"
}

fn unknown_tool(id: Option<Value>) -> RpcReply {
    RpcReply::response(protocol::RpcResponse::error(id, -32602, "unknown tool"))
}

fn validate_node_collisions(
    node: &McpNode,
    operations: &BTreeMap<OperationId, Operation>,
    endpoints: &mut std::collections::BTreeSet<String>,
    resources: &mut std::collections::BTreeSet<String>,
    errors: &mut Vec<String>,
) {
    let Some(endpoint) = operations
        .get(&node.marker_id)
        .map(|operation| operation.path.clone())
    else {
        errors.push("MCP endpoint marker is missing".to_string());
        return;
    };
    collect_validation(
        validate_collisions(operations, &endpoint, node.marker_id),
        errors,
    );
    let internal_endpoint = crate::slash::internal_path(&endpoint).to_owned();
    if !endpoints.insert(internal_endpoint.clone()) {
        errors.push(format!("duplicate MCP endpoint '{endpoint}'"));
    }
    if !resources.insert(internal_endpoint) {
        errors.push(format!("duplicate MCP resource URL '{endpoint}'"));
    }
}

fn collect_validation(result: Result<(), McpError>, errors: &mut Vec<String>) {
    if let Err(error) = result {
        errors.push(error.to_string());
    }
}

fn validate_collisions(
    operations: &BTreeMap<OperationId, Operation>,
    endpoint: &str,
    marker: OperationId,
) -> Result<(), McpError> {
    let collision = operations.values().any(|operation| {
        operation.id != marker
            && crate::slash::internal_path(&operation.path) == crate::slash::internal_path(endpoint)
            && (operation.methods.contains(crate::routes::Methods::POST)
                || operation.methods.contains(crate::routes::Methods::GET))
    });
    if collision {
        return Err(McpError::Config(format!(
            "MCP endpoint conflicts with GET or POST {endpoint}"
        )));
    }
    Ok(())
}

fn validate_well_known(
    operations: &BTreeMap<OperationId, Operation>,
    path: &str,
) -> Result<(), McpError> {
    let collision = operations.values().any(|operation| {
        crate::slash::internal_path(&operation.path) == crate::slash::internal_path(path)
            && operation.methods.contains(crate::routes::Methods::GET)
    });
    if collision {
        return Err(McpError::Config(format!(
            "protected resource metadata conflicts with GET {path}"
        )));
    }
    Ok(())
}

fn protected_resource_path(endpoint: &str) -> String {
    format!(
        "/.well-known/oauth-protected-resource{}",
        endpoint.trim_end_matches('/')
    )
}

fn protected_resource_url(resource: &str) -> Result<String, McpError> {
    let mut url = url::Url::parse(resource).map_err(|error| McpError::Config(error.to_string()))?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(McpError::Config(
            "OAuth MCP audiences must be clean absolute HTTPS resource URLs".into(),
        ));
    }
    let path = protected_resource_path(url.path());
    url.set_path(&path);
    url.set_query(None);
    url.set_fragment(None);
    Ok(url.to_string())
}
