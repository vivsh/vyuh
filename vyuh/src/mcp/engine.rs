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
    auth::{AuthError, AuthProtectedResource, Authenticator},
    callables::Operation,
    routes::AxumRouter,
};

use super::{
    McpConf, McpError, McpToolRegistry, config::McpAuth, protocol, tools, tools::ToolDefinition,
};

const MAX_REQUEST_BYTES: usize = 1024 * 1024;

struct McpNode {
    marker_id: OperationId,
    operation_ids: Vec<OperationId>,
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

    pub(crate) fn setup(
        &self,
        router: &mut AxumRouter<Site>,
        operations: &BTreeMap<OperationId, Operation>,
        registry: &McpToolRegistry,
        auth: &Authenticator,
    ) -> Result<(), crate::bundles::BundleError> {
        self.validate_nodes(operations)
            .map_err(|error| crate::bundles::BundleError::Mcp(error.to_string()))?;
        for node in &self.nodes {
            self.setup_node(router, operations, registry, auth, node)
                .map_err(|error| crate::bundles::BundleError::Mcp(error.to_string()))?;
        }
        Ok(())
    }

    fn setup_node(
        &self,
        router: &mut AxumRouter<Site>,
        operations: &BTreeMap<OperationId, Operation>,
        registry: &McpToolRegistry,
        auth: &Authenticator,
        node: &McpNode,
    ) -> Result<(), McpError> {
        let endpoint = operations
            .get(&node.marker_id)
            .map(|operation| operation.path.clone())
            .ok_or_else(|| McpError::Config("MCP endpoint marker is missing".to_string()))?;
        validate_collisions(operations, &endpoint, node.marker_id)?;
        let definitions = tools::definitions(&node.operation_ids, operations, registry)?;
        if matches!(node.conf.auth, Some(McpAuth::Anonymous))
            && definitions
                .values()
                .any(|tool| tool.authorization.requires_identity())
        {
            return Err(McpError::Config(format!(
                "anonymous MCP service '{endpoint}' contains authenticated tools"
            )));
        }
        let protected = match node.conf.auth {
            Some(McpAuth::Provider(provider)) => {
                let selected = auth.using(provider);
                selected
                    .mcp_eligible()
                    .map_err(|error| McpError::Config(error.to_string()))?;
                selected
                    .protected_resource(node.conf.audience)
                    .map_err(|error| McpError::Config(error.to_string()))?
            }
            _ => None,
        };
        if protected.is_some() {
            let metadata = protected_resource_path(&endpoint);
            validate_well_known(operations, &metadata)?;
        }
        let runtime = Arc::new(McpRuntime::new(node.conf.clone(), definitions, protected)?);
        let endpoint_runtime = Arc::clone(&runtime);
        let endpoint_route =
            axum::routing::post(move |State(site): State<Site>, request: Request| {
                let runtime = Arc::clone(&endpoint_runtime);
                async move { handle_mcp(runtime, site, request).await }
            })
            .get(|| async { StatusCode::METHOD_NOT_ALLOWED });
        *router = std::mem::take(router).route(&endpoint, endpoint_route);
        if let Some(metadata) = runtime.metadata() {
            let well_known = protected_resource_path(&endpoint);
            let metadata_route = axum::routing::get(move || {
                let metadata = metadata.clone();
                async move { metadata }
            });
            *router = std::mem::take(router).route(&well_known, metadata_route);
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
    /// Claims unowned direct and route-backed tools for one MCP service endpoint.
    ///
    /// Configuration and ownership failures are accumulated on the bundle and
    /// returned during site construction.
    pub fn with_mcp(mut self, mut conf: McpConf) -> Self {
        if let Err(error) = conf.validate() {
            self.errors
                .push(crate::bundles::BundleError::Mcp(error.to_string()));
            return self;
        }
        let operation_ids = self.mcp_registry.claim_unclaimed();
        if operation_ids.is_empty() {
            self.errors.push(crate::bundles::BundleError::Mcp(
                "MCP service has no unclaimed tool registrations".to_string(),
            ));
            return self;
        }
        let mut marker =
            Operation::from_api_doc(&format!("__mcp__{}", conf.endpoint), &conf.endpoint);
        marker.methods = crate::routes::Methods::POST;
        marker.assign_bundle_id(self.id);
        let marker_id = marker.id;
        self.ops.insert(marker_id, marker);
        self.mcp_engine.nodes.push(McpNode {
            marker_id,
            operation_ids,
            conf,
        });
        self
    }
}

struct McpRuntime {
    conf: McpConf,
    tools: BTreeMap<String, ToolDefinition>,
    resource_metadata_url: Option<String>,
    metadata: Option<Json<Value>>,
    required_scopes: Vec<String>,
}

impl McpRuntime {
    fn new(
        conf: McpConf,
        tools: BTreeMap<String, ToolDefinition>,
        protected: Option<AuthProtectedResource>,
    ) -> Result<Self, McpError> {
        let resource_metadata_url = protected
            .as_ref()
            .map(|_| protected_resource_url(conf.audience.as_str()))
            .transpose()?;
        let required_scopes = protected
            .as_ref()
            .map(|value| value.required_scopes.clone())
            .unwrap_or_default();
        let metadata = protected.map(|value| {
            Json(json!({
                "resource": conf.audience.as_str(), "authorization_servers": [value.issuer],
                "scopes_supported": value.advertised_scopes, "bearer_methods_supported": ["header"]
            }))
        });
        Ok(Self {
            conf,
            tools,
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
    if let Err(status) = validate_transport(&headers, runtime.conf.audience.as_str()) {
        return status.into_response();
    }
    let user = match runtime.conf.auth {
        Some(McpAuth::Provider(provider)) => match site
            .auth()
            .using(provider)
            .authenticate(&parts, runtime.conf.audience)
            .await
        {
            Ok(user) => Some(user),
            Err(error) => return auth_error(&runtime, site.auth(), error),
        },
        Some(McpAuth::Anonymous) => None,
        None => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
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

fn auth_error(runtime: &McpRuntime, auth: &Authenticator, error: AuthError) -> Response {
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
    let provider = match runtime.conf.auth {
        Some(McpAuth::Provider(provider)) => auth.using(provider).challenge(&error).ok().flatten(),
        _ => None,
    };
    if let (Some(metadata), Some(provider)) = (runtime.resource_metadata_url.as_deref(), provider) {
        let scope = if status == StatusCode::FORBIDDEN && !runtime.required_scopes.is_empty() {
            format!(", scope=\"{}\"", runtime.required_scopes.join(" "))
        } else {
            String::new()
        };
        let challenge = format!(
            "{} resource_metadata=\"{metadata}\", error=\"{code}\"{scope}",
            provider.scheme
        );
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
    if !endpoints.insert(endpoint.clone()) {
        errors.push(format!("duplicate MCP endpoint '{endpoint}'"));
    }
    if !resources.insert(node.conf.audience.as_str().to_owned()) {
        errors.push(format!(
            "duplicate MCP resource URL '{}'",
            node.conf.audience.as_str()
        ));
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
            && operation.path == endpoint
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
        operation.path == path && operation.methods.contains(crate::routes::Methods::GET)
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
