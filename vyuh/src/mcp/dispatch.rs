//! Invocation of semantic direct MCP tools.

use serde_json::Value;

use crate::{ErrorKind, Site, auth::AuthUser};

use super::{McpError, McpToolContext, protocol, tools::ToolDefinition};

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
    invoke_direct(site, &tool.target.callable, arguments, user, modern).await
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
