//! Versioned JSON-RPC messages for the tool-only MCP surface.

use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::tools::ToolDefinition;

pub(crate) const CURRENT_VERSION: &str = "2026-07-28";
pub(crate) const PREVIOUS_VERSION: &str = "2025-11-25";
pub(crate) const LEGACY_VERSION: &str = "2025-06-18";
const SUPPORTED_VERSIONS: [&str; 3] = [CURRENT_VERSION, PREVIOUS_VERSION, LEGACY_VERSION];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Revision {
    Modern,
    Legacy(&'static str),
}

impl Revision {
    pub(crate) const fn modern(self) -> bool {
        matches!(self, Self::Modern)
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct RpcRequest {
    pub(crate) jsonrpc: String,
    #[serde(default)]
    pub(crate) id: Option<Value>,
    pub(crate) method: String,
    #[serde(default)]
    pub(crate) params: Value,
}

impl RpcRequest {
    pub(crate) fn validate(&self) -> Result<(), Box<RpcResponse>> {
        let valid_id = self
            .id
            .as_ref()
            .is_none_or(|id| id.is_string() || id.is_number());
        let notification = self.method == "notifications/initialized";
        let valid_shape = !self.method.is_empty()
            && (self.params.is_object() || self.params.is_null())
            && valid_id
            && (notification == self.id.is_none());
        if self.jsonrpc == "2.0" && valid_shape {
            return Ok(());
        }
        Err(Box::new(RpcResponse::error(
            self.id.clone(),
            -32600,
            "invalid request",
        )))
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct ToolCall {
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) arguments: Value,
}

#[derive(Debug, Serialize)]
pub(crate) struct RpcResponse {
    jsonrpc: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RpcError>,
}

#[derive(Debug, Serialize)]
struct RpcError {
    code: i32,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
}

impl RpcResponse {
    pub(crate) fn result(id: Option<Value>, result: Value) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: Some(result),
            error: None,
        }
    }

    pub(crate) fn error(id: Option<Value>, code: i32, message: impl Into<String>) -> Self {
        Self::error_with_data(id, code, message, None)
    }

    fn error_with_data(
        id: Option<Value>,
        code: i32,
        message: impl Into<String>,
        data: Option<Value>,
    ) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(RpcError {
                code,
                message: message.into(),
                data,
            }),
        }
    }
}

pub(crate) fn revision(
    headers: &HeaderMap,
    request: &RpcRequest,
) -> Result<Revision, Box<RpcResponse>> {
    if request.method == "initialize" {
        return legacy_initialization(headers, request);
    }
    if let Some(version) = metadata_version(&request.params) {
        return modern_revision(headers, request, version);
    }
    legacy_revision(headers, request)
}

fn legacy_initialization(
    headers: &HeaderMap,
    request: &RpcRequest,
) -> Result<Revision, Box<RpcResponse>> {
    let requested = request
        .params
        .get("protocolVersion")
        .and_then(Value::as_str)
        .unwrap_or("");
    let revision = legacy_value(requested).ok_or_else(|| unsupported(request, requested))?;
    if let Some(header) = header(headers, "mcp-protocol-version")
        && header != requested
    {
        return Err(header_mismatch(request, "MCP-Protocol-Version"));
    }
    Ok(revision)
}

fn modern_revision(
    headers: &HeaderMap,
    request: &RpcRequest,
    requested: &str,
) -> Result<Revision, Box<RpcResponse>> {
    if requested != CURRENT_VERSION {
        return Err(unsupported(request, requested));
    }
    if header(headers, "mcp-protocol-version") != Some(requested) {
        return Err(header_mismatch(request, "MCP-Protocol-Version"));
    }
    validate_modern_metadata(request)?;
    validate_mirrored_headers(headers, request)?;
    Ok(Revision::Modern)
}

fn legacy_revision(
    headers: &HeaderMap,
    request: &RpcRequest,
) -> Result<Revision, Box<RpcResponse>> {
    let requested = header(headers, "mcp-protocol-version").unwrap_or("");
    legacy_value(requested).ok_or_else(|| unsupported(request, requested))
}

fn legacy_value(value: &str) -> Option<Revision> {
    match value {
        PREVIOUS_VERSION => Some(Revision::Legacy(PREVIOUS_VERSION)),
        LEGACY_VERSION => Some(Revision::Legacy(LEGACY_VERSION)),
        _ => None,
    }
}

fn validate_modern_metadata(request: &RpcRequest) -> Result<(), Box<RpcResponse>> {
    let metadata = request.params.get("_meta").and_then(Value::as_object);
    let client_info = metadata
        .and_then(|value| value.get("io.modelcontextprotocol/clientInfo"))
        .and_then(Value::as_object);
    let capabilities = metadata
        .and_then(|value| value.get("io.modelcontextprotocol/clientCapabilities"))
        .and_then(Value::as_object);
    let valid_info = client_info.is_some_and(|value| {
        value.get("name").and_then(Value::as_str).is_some()
            && value.get("version").and_then(Value::as_str).is_some()
    });
    if valid_info && capabilities.is_some() {
        return Ok(());
    }
    Err(Box::new(RpcResponse::error(
        request.id.clone(),
        -32600,
        "missing required MCP client metadata",
    )))
}

fn validate_mirrored_headers(
    headers: &HeaderMap,
    request: &RpcRequest,
) -> Result<(), Box<RpcResponse>> {
    if header(headers, "mcp-method") != Some(request.method.as_str()) {
        return Err(header_mismatch(request, "Mcp-Method"));
    }
    if request.method != "tools/call" {
        return Ok(());
    }
    let body_name = request.params.get("name").and_then(Value::as_str);
    if body_name.is_some() && header(headers, "mcp-name") == body_name {
        return Ok(());
    }
    Err(header_mismatch(request, "Mcp-Name"))
}

fn metadata_version(params: &Value) -> Option<&str> {
    params
        .get("_meta")?
        .get("io.modelcontextprotocol/protocolVersion")?
        .as_str()
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

fn header_mismatch(request: &RpcRequest, name: &str) -> Box<RpcResponse> {
    Box::new(RpcResponse::error(
        request.id.clone(),
        -32020,
        format!("header mismatch: {name}"),
    ))
}

fn unsupported(request: &RpcRequest, requested: &str) -> Box<RpcResponse> {
    Box::new(RpcResponse::error_with_data(
        request.id.clone(),
        -32022,
        "unsupported protocol version",
        Some(json!({"supported": SUPPORTED_VERSIONS, "requested": requested})),
    ))
}

pub(crate) fn discovery(method: &str, revision: Revision) -> Option<Value> {
    match (method, revision) {
        ("initialize", Revision::Legacy(version)) => Some(json!({
            "protocolVersion": version,
            "capabilities": {"tools": {"listChanged": false}},
            "serverInfo": {"name": "vyuh", "version": env!("CARGO_PKG_VERSION")}
        })),
        ("server/discover", Revision::Modern) => Some(json!({
            "resultType": "complete",
            "supportedVersions": SUPPORTED_VERSIONS,
            "capabilities": {"tools": {"listChanged": false}},
            "_meta": {"io.modelcontextprotocol/serverInfo": {
                "name": "vyuh", "version": env!("CARGO_PKG_VERSION")
            }},
            "cacheScope": "public"
        })),
        _ => None,
    }
}

pub(crate) fn tool_list<'a>(
    tools: impl Iterator<Item = &'a ToolDefinition>,
    revision: Revision,
) -> Value {
    let mut result = json!({"tools": tools.collect::<Vec<_>>()});
    if revision.modern() {
        result["resultType"] = Value::String("complete".to_string());
        result["cacheScope"] = Value::String("private".to_string());
    }
    result
}

pub(crate) fn tool_result(value: Value, modern: bool) -> Value {
    let text = serde_json::to_string(&value).unwrap_or_else(|_| "null".to_string());
    let mut result = json!({
        "content": [{"type": "text", "text": text}],
        "structuredContent": value,
        "isError": false
    });
    if modern {
        result["resultType"] = Value::String("complete".to_string());
    }
    result
}

pub(crate) fn tool_error(status: u16, message: &str, modern: bool) -> Value {
    let body = json!({"status": status, "error": message});
    let text = serde_json::to_string(&body).unwrap_or_else(|_| "tool call failed".to_string());
    let mut result = json!({
        "content": [{"type": "text", "text": text}],
        "structuredContent": body,
        "isError": true
    });
    if modern {
        result["resultType"] = Value::String("complete".to_string());
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderMap, HeaderValue};

    fn current_request(method: &str) -> RpcRequest {
        RpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(json!(1)),
            method: method.to_string(),
            params: json!({"_meta": {
                "io.modelcontextprotocol/protocolVersion": CURRENT_VERSION,
                "io.modelcontextprotocol/clientInfo": {"name": "test", "version": "1"},
                "io.modelcontextprotocol/clientCapabilities": {}
            }}),
        }
    }

    fn current_headers(method: &'static str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            "mcp-protocol-version",
            HeaderValue::from_static(CURRENT_VERSION),
        );
        headers.insert("mcp-method", HeaderValue::from_static(method));
        headers
    }

    /// Verifies the modern revision requires matching per-request metadata headers.
    #[test]
    fn accepts_current_revision_metadata() {
        let request = current_request("tools/list");
        let headers = current_headers("tools/list");
        assert!(matches!(revision(&headers, &request), Ok(Revision::Modern)));
    }

    /// Verifies the prior Streamable HTTP revision remains initialization-compatible.
    #[test]
    fn accepts_previous_initialization() {
        let request = RpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(json!(1)),
            method: "initialize".to_string(),
            params: json!({"protocolVersion": PREVIOUS_VERSION}),
        };
        assert!(matches!(
            revision(&HeaderMap::new(), &request),
            Ok(Revision::Legacy(PREVIOUS_VERSION))
        ));
    }

    /// Verifies modern header/body disagreements fail with the allocated MCP error.
    #[test]
    fn rejects_mirrored_header_mismatch() {
        let request = current_request("tools/list");
        let headers = current_headers("tools/call");
        let response = revision(&headers, &request).err();
        let value = response.and_then(|value| serde_json::to_value(value).ok());
        assert_eq!(
            value.and_then(|value| value.pointer("/error/code").cloned()),
            Some(json!(-32020))
        );
    }

    /// Verifies unsupported revisions advertise every implemented compatibility version.
    #[test]
    fn rejects_unknown_revision() {
        let mut request = current_request("tools/list");
        request.params["_meta"]["io.modelcontextprotocol/protocolVersion"] =
            Value::String("2024-01-01".to_string());
        let headers = current_headers("tools/list");
        let response = revision(&headers, &request).err();
        let value = response.and_then(|value| serde_json::to_value(value).ok());
        assert_eq!(
            value.and_then(|value| value.pointer("/error/code").cloned()),
            Some(json!(-32022))
        );
    }

    /// Verifies malformed JSON-RPC identifiers and notification shapes fail validation.
    #[test]
    fn rejects_invalid_json_rpc_shape() {
        let mut request = current_request("tools/list");
        request.id = Some(json!({"invalid": true}));
        let response = request.validate().err();
        let value = response.and_then(|value| serde_json::to_value(value).ok());
        assert_eq!(
            value.and_then(|value| value.pointer("/error/code").cloned()),
            Some(json!(-32600))
        );
    }
}
