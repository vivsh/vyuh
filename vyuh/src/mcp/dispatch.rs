//! Invocation of semantic direct and route-backed MCP tools.

use axum::{
    body::{Body, to_bytes},
    http::{Request, header},
};
use serde_json::Value;
use tower::ServiceExt;

use crate::{ErrorKind, Site, auth::AuthUser};

use super::{McpError, McpToolContext, McpToolTarget, protocol, tools::ToolDefinition};

const MAX_TOOL_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

/// Invokes one already-authorized tool with semantic object arguments.
pub(crate) async fn invoke(
    site: Site,
    tool: &ToolDefinition,
    arguments: Value,
    user: Option<AuthUser>,
    modern: bool,
) -> Result<Value, McpError> {
    if !arguments.is_object() {
        return Err(McpError::Arguments(
            "tool arguments must be a JSON object".to_string(),
        ));
    }
    if !tool.validator.is_valid(&arguments) {
        return Err(McpError::Arguments(
            "tool arguments do not match the declared schema".to_string(),
        ));
    }
    match &tool.target {
        McpToolTarget::Direct(callable) => {
            invoke_direct(site, callable, arguments, user, modern).await
        }
        McpToolTarget::Route(id) => invoke_route(site, *id, arguments, user, modern).await,
    }
}

async fn invoke_direct(
    site: Site,
    callable: &crate::callables::Callable<McpToolContext, crate::Error>,
    arguments: Value,
    user: Option<AuthUser>,
    modern: bool,
) -> Result<Value, McpError> {
    let encoded = serde_json::to_string(&arguments)
        .map_err(|error| McpError::Arguments(error.to_string()))?;
    let payload = callable
        .deserialize_input(&encoded)
        .map_err(|error| McpError::Arguments(error.to_string()))?;
    let context = McpToolContext::new(site, payload, user);
    match callable.call(context).await {
        Ok(output) => direct_output(output, modern),
        Err(error) => Ok(application_error(error.kind, modern)),
    }
}

fn direct_output(output: crate::callables::DataBox, modern: bool) -> Result<Value, McpError> {
    let value = match output.to_json() {
        Some(value) => value.map_err(McpError::Dispatch)?,
        None if output.payload_type_id() == std::any::TypeId::of::<()>() => Value::Null,
        None => {
            return Err(McpError::Dispatch(
                "direct tool returned an unserializable value".to_string(),
            ));
        }
    };
    Ok(protocol::tool_result(value, modern))
}

fn application_error(kind: ErrorKind, modern: bool) -> Value {
    let (status, message) = match kind {
        ErrorKind::BadRequest | ErrorKind::Invalid => (400, "tool arguments were rejected"),
        ErrorKind::Unauthorized | ErrorKind::Forbidden => (403, "tool access was denied"),
        ErrorKind::NotFound => (404, "tool resource was not found"),
        ErrorKind::Conflict | ErrorKind::Integrity => (409, "tool request conflicted"),
        ErrorKind::RateLimited => (429, "tool request was rate limited"),
        ErrorKind::Unavailable => (503, "tool service is unavailable"),
        ErrorKind::Other => (500, "tool execution failed"),
    };
    protocol::tool_error(status, message, modern)
}

async fn invoke_route(
    site: Site,
    operation_id: crate::OperationId,
    arguments: Value,
    user: Option<AuthUser>,
    modern: bool,
) -> Result<Value, McpError> {
    let operation = site
        .operations()
        .find(operation_id)
        .ok_or_else(|| McpError::Dispatch("tool route is unavailable".to_string()))?;
    let method = route_method(operation)?;
    let body = serde_json::to_vec(&arguments)
        .map(Body::from)
        .map_err(|error| McpError::Arguments(error.to_string()))?;
    let mut request = Request::builder()
        .method(method)
        .uri(&operation.path)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body)
        .map_err(|error| McpError::Arguments(error.to_string()))?;
    if let Some(user) = user {
        request.extensions_mut().insert(user);
    }
    let response = match site.router().oneshot(request).await {
        Ok(response) => response,
        Err(never) => match never {},
    };
    response_value(response, modern).await
}

fn route_method(operation: &crate::Operation) -> Result<axum::http::Method, McpError> {
    let methods = operation.http_methods();
    let Some(method) = methods.first() else {
        return Err(McpError::Dispatch(
            "tool route method is unavailable".to_string(),
        ));
    };
    axum::http::Method::from_bytes(method.as_bytes())
        .map_err(|error| McpError::Dispatch(error.to_string()))
}

async fn response_value(
    response: axum::response::Response,
    modern: bool,
) -> Result<Value, McpError> {
    let status = response.status();
    if !status.is_success() {
        return Ok(protocol::tool_error(
            status.as_u16(),
            "route request failed",
            modern,
        ));
    }
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_string();
    let bytes = to_bytes(response.into_body(), MAX_TOOL_RESPONSE_BYTES)
        .await
        .map_err(|error| McpError::Dispatch(error.to_string()))?;
    let value = decode_body(&content_type, &bytes)?;
    Ok(protocol::tool_result(value, modern))
}

fn decode_body(content_type: &str, bytes: &[u8]) -> Result<Value, McpError> {
    if bytes.is_empty() {
        return Ok(Value::Null);
    }
    if !content_type.starts_with("application/json") {
        return Err(McpError::Dispatch(
            "tool route returned a non-JSON response".to_string(),
        ));
    }
    serde_json::from_slice(bytes).map_err(|error| McpError::Dispatch(error.to_string()))
}
