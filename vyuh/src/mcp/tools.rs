//! Conversion of opted-in Vyuh operations into semantic MCP tool contracts.

use serde::Serialize;
use serde_json::{Value, json};
use std::collections::BTreeMap;

use crate::{
    OperationId,
    auth::{AuthUser, Scope},
    callables::{ArgPart, Operation, OperationKind, ReturnPart, TypeSchema},
};

use super::{
    McpError, McpToolConf, McpToolRegistry, McpToolTarget,
    resources::{ResourceDefinition, validate_ui_uri},
};

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ToolAnnotations {
    #[serde(skip_serializing_if = "Option::is_none")]
    read_only_hint: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    destructive_hint: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    idempotent_hint: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    open_world_hint: Option<bool>,
}

#[derive(Clone, Debug)]
struct PermitRequirement {
    scopes: Vec<Scope>,
    all: bool,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ToolAuthorization {
    authenticated: bool,
    permits: Vec<PermitRequirement>,
}

impl ToolAuthorization {
    /// Returns whether this tool requires a mapped application identity.
    pub(crate) fn requires_identity(&self) -> bool {
        self.authenticated
    }

    /// Evaluates all declared permit extractors against one request identity.
    pub(crate) fn allows(&self, user: Option<&AuthUser>) -> bool {
        if !self.authenticated && self.permits.is_empty() {
            return true;
        }
        let Some(user) = user else {
            return false;
        };
        self.permits.iter().all(|permit| {
            if permit.all {
                user.has_all(&permit.scopes)
            } else {
                user.has_any(&permit.scopes)
            }
        })
    }
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ToolDefinition {
    pub(crate) name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    input_schema: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_schema: Option<Value>,
    annotations: ToolAnnotations,
    #[serde(rename = "_meta", skip_serializing_if = "Option::is_none")]
    metadata: Option<ToolMetadata>,
    #[serde(skip)]
    pub(crate) authorization: ToolAuthorization,
    #[serde(skip)]
    pub(crate) target: McpToolTarget,
    #[serde(skip)]
    pub(crate) validator: jsonschema::Validator,
}

#[derive(Clone, Serialize)]
struct ToolMetadata {
    ui: ToolUiMetadata,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ToolUiMetadata {
    resource_uri: String,
}

impl std::fmt::Debug for ToolDefinition {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ToolDefinition")
            .field("name", &self.name)
            .field("authorization", &self.authorization)
            .finish_non_exhaustive()
    }
}

/// Builds one deterministic service catalog from claimed registry entries.
pub(crate) fn definitions(
    operation_ids: &[OperationId],
    operations: &BTreeMap<OperationId, Operation>,
    registry: &McpToolRegistry,
    resources: &BTreeMap<String, ResourceDefinition>,
) -> Result<BTreeMap<String, ToolDefinition>, McpError> {
    let mut output = BTreeMap::new();
    for id in operation_ids {
        let operation = operations
            .get(id)
            .ok_or_else(|| McpError::Config(format!("MCP operation {id} is missing")))?;
        let target = registry
            .target(*id)
            .cloned()
            .ok_or_else(|| McpError::Config(format!("MCP target {id} is missing")))?;
        let tool = definition(operation, target, resources)?;
        if output.insert(tool.name.clone(), tool).is_some() {
            return Err(McpError::InvalidTool(operation.name.clone()));
        }
    }
    if output.is_empty() {
        return Err(McpError::Config(
            "MCP service has no explicitly registered tools".to_string(),
        ));
    }
    Ok(output)
}

fn definition(
    operation: &Operation,
    target: McpToolTarget,
    resources: &BTreeMap<String, ResourceDefinition>,
) -> Result<ToolDefinition, McpError> {
    validate_name(&operation.name)?;
    validate_target(operation)?;
    let (input_schema, authorization) = input_contract(operation)?;
    let output_schema = output_schema(operation)?;
    let validator = jsonschema::validator_for(&input_schema)
        .map_err(|error| McpError::Config(format!("invalid generated tool schema: {error}")))?;
    Ok(ToolDefinition {
        name: operation.name.clone(),
        description: tool_description(operation),
        input_schema,
        output_schema,
        annotations: annotations(&target.conf),
        metadata: tool_metadata(operation, &target.conf, resources)?,
        authorization,
        target,
        validator,
    })
}

/// Resolves one optional MCP Apps UI resource attachment for a tool definition.
fn tool_metadata(
    operation: &Operation,
    conf: &McpToolConf,
    resources: &BTreeMap<String, ResourceDefinition>,
) -> Result<Option<ToolMetadata>, McpError> {
    let Some(uri) = conf.ui_resource_uri.as_deref() else {
        return Ok(None);
    };
    validate_ui_uri(uri)?;
    let resource = resources.get(uri).ok_or_else(|| {
        McpError::Config(format!(
            "MCP tool '{}' references an unknown UI resource '{uri}'",
            operation.name
        ))
    })?;
    if !resource.is_mcp_app_html() {
        return Err(McpError::Config(format!(
            "MCP tool '{}' UI resource '{uri}' must use text/html;profile=mcp-app",
            operation.name
        )));
    }
    Ok(Some(ToolMetadata {
        ui: ToolUiMetadata {
            resource_uri: uri.to_string(),
        },
    }))
}

fn validate_target(operation: &Operation) -> Result<(), McpError> {
    if operation.kind == OperationKind::McpTool {
        return Ok(());
    }
    unsupported(operation, "only direct mcp_tool callables are supported")
}

fn input_contract(operation: &Operation) -> Result<(Value, ToolAuthorization), McpError> {
    let mut state = InputState::default();
    for argument in &operation.args {
        inspect_part(&argument.part, true, operation, &mut state)?;
    }
    for layer in &operation.layers {
        for part in &layer.parts {
            inspect_part(part, false, operation, &mut state)?;
        }
    }
    let schema = state.body.ok_or_else(|| {
        unsupported_error(operation, "exactly one required JSON body is required")
    })?;
    let mut value = schema_value(&schema)?;
    if !is_object_schema(&value) {
        return unsupported(operation, "tool input schema must have an object root");
    }
    if let Some(object) = value.as_object_mut() {
        object
            .entry("additionalProperties")
            .or_insert(Value::Bool(false));
    }
    Ok((value, state.authorization))
}

#[derive(Default)]
struct InputState {
    body: Option<TypeSchema>,
    authorization: ToolAuthorization,
}

fn inspect_part(
    part: &ArgPart,
    allow_body: bool,
    operation: &Operation,
    state: &mut InputState,
) -> Result<(), McpError> {
    match part {
        ArgPart::Composite(parts) => inspect_composite(parts, allow_body, operation, state),
        ArgPart::Body(schema, content_type) if content_type.as_ref() == "application/json" => {
            register_body(schema, allow_body, operation, state)
        }
        ArgPart::BodyWith {
            schema,
            content_type,
            multipart: None,
        } if content_type.as_ref() == "application/json" => {
            register_body(schema, allow_body, operation, state)
        }
        ArgPart::Authentication => {
            state.authorization.authenticated = true;
            Ok(())
        }
        ArgPart::Authorization { scopes, all } => {
            state.authorization.authenticated = true;
            state.authorization.permits.push(PermitRequirement {
                scopes: scopes.clone(),
                all: *all,
            });
            Ok(())
        }
        ArgPart::Ignore | ArgPart::Zone | ArgPart::Security { .. } | ArgPart::Response(_) => Ok(()),
        ArgPart::Optional(nested) | ArgPart::Fallible(nested) => {
            if contains_auth(nested) {
                return unsupported(
                    operation,
                    "optional or fallible authentication is unsupported",
                );
            }
            unsupported(
                operation,
                "tool payload and transport inputs must be required",
            )
        }
        ArgPart::Path(_) | ArgPart::Query(_) | ArgPart::Header(_) | ArgPart::Cookie(_) => {
            unsupported(operation, "HTTP transport arguments are not supported")
        }
        ArgPart::RawRequest => unsupported(operation, "raw requests are not supported"),
        ArgPart::Body(_, _) | ArgPart::BodyWith { .. } => {
            unsupported(operation, "only one required JSON body is supported")
        }
    }
}

fn inspect_composite(
    parts: &[ArgPart],
    allow_body: bool,
    operation: &Operation,
    state: &mut InputState,
) -> Result<(), McpError> {
    if parts.iter().any(contains_security) && !parts.iter().any(contains_authentication) {
        return unsupported(
            operation,
            "alternate credential extractors are not supported",
        );
    }
    for part in parts {
        inspect_part(part, allow_body, operation, state)?;
    }
    Ok(())
}

fn register_body(
    schema: &TypeSchema,
    allowed: bool,
    operation: &Operation,
    state: &mut InputState,
) -> Result<(), McpError> {
    if !allowed {
        return unsupported(operation, "middleware cannot supply a tool payload");
    }
    if state.body.replace(schema.clone()).is_some() {
        return unsupported(operation, "exactly one JSON body is supported");
    }
    Ok(())
}

fn contains_auth(part: &ArgPart) -> bool {
    match part {
        ArgPart::Authentication | ArgPart::Authorization { .. } => true,
        ArgPart::Composite(parts) => parts.iter().any(contains_auth),
        ArgPart::Optional(nested) | ArgPart::Fallible(nested) => contains_auth(nested),
        _ => false,
    }
}

fn contains_security(part: &ArgPart) -> bool {
    match part {
        ArgPart::Security { .. } => true,
        ArgPart::Composite(parts) => parts.iter().any(contains_security),
        ArgPart::Optional(nested) | ArgPart::Fallible(nested) => contains_security(nested),
        _ => false,
    }
}

fn contains_authentication(part: &ArgPart) -> bool {
    match part {
        ArgPart::Authentication => true,
        ArgPart::Composite(parts) => parts.iter().any(contains_authentication),
        ArgPart::Optional(nested) | ArgPart::Fallible(nested) => contains_authentication(nested),
        _ => false,
    }
}

fn output_schema(operation: &Operation) -> Result<Option<Value>, McpError> {
    let response = operation.returns.iter().find(|value| {
        value
            .status_code
            .map(|status| (200..300).contains(&status))
            .unwrap_or(true)
    });
    let Some(response) = response else {
        return unsupported(operation, "a successful response schema is required");
    };
    match &response.part {
        ReturnPart::Body(schema, content_type)
        | ReturnPart::Created(schema, content_type)
        | ReturnPart::Accepted(schema, content_type)
            if content_type.as_ref() == "application/json" =>
        {
            schema_value(schema).map(Some)
        }
        ReturnPart::Empty => Ok(Some(json!({"type": "null"}))),
        _ => unsupported(operation, "only typed JSON or unit responses are supported"),
    }
}

fn schema_value(schema: &TypeSchema) -> Result<Value, McpError> {
    serde_json::to_value(schema.root_schema())
        .map_err(|error| McpError::Config(format!("schema generation failed: {error}")))
}

fn is_object_schema(schema: &Value) -> bool {
    schema.get("type").and_then(Value::as_str) == Some("object")
        || schema.get("properties").is_some()
}

fn annotations(conf: &McpToolConf) -> ToolAnnotations {
    ToolAnnotations {
        read_only_hint: conf.read_only,
        destructive_hint: conf.destructive,
        idempotent_hint: conf.idempotent,
        open_world_hint: conf.open_world,
    }
}

fn tool_description(operation: &Operation) -> Option<String> {
    match (&operation.summary, &operation.description) {
        (Some(summary), Some(description)) => Some(format!("{summary}\n\n{description}")),
        (Some(summary), None) => Some(summary.clone()),
        (None, Some(description)) => Some(description.clone()),
        (None, None) => None,
    }
}

fn validate_name(name: &str) -> Result<(), McpError> {
    let valid = !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'));
    if valid {
        return Ok(());
    }
    Err(McpError::InvalidTool(name.to_string()))
}

fn unsupported_error(operation: &Operation, reason: &str) -> McpError {
    McpError::UnsupportedTool {
        tool: operation.name.clone(),
        reason: reason.to_string(),
    }
}

fn unsupported<T>(operation: &Operation, reason: &str) -> Result<T, McpError> {
    Err(unsupported_error(operation, reason))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies MCP tool filtering enforces the same exact application scopes as routes.
    #[test]
    fn tool_authorization_uses_application_scopes() {
        let authorization = ToolAuthorization {
            authenticated: true,
            permits: vec![PermitRequirement {
                scopes: vec![Scope::of("tools:read"), Scope::of("tools:write")],
                all: true,
            }],
        };
        let partial = AuthUser::new("user-1").with_scope(Scope::of("tools:read"));
        let complete = partial.clone().with_scope(Scope::of("tools:write"));

        assert!(!authorization.allows(None));
        assert!(!authorization.allows(Some(&partial)));
        assert!(authorization.allows(Some(&complete)));
    }
}
