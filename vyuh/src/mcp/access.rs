//! Finalizes endpoint access and evaluates the shared tool visibility and call policy.

use std::collections::BTreeMap;

use crate::{
    auth::{AudienceId, AuthProtectedResource, AuthUser, Authenticator},
    scopes::valid_name,
};

use super::{McpError, config::McpAccess, engine::protected_resource_url, tools::ToolDefinition};

#[derive(PartialEq, Eq)]
pub(super) enum ToolAccess {
    Allowed,
    NeedsOAuth,
    Hidden,
}

impl ToolAccess {
    /// Evaluates one tool for listing or invocation using the same identity.
    pub(super) fn evaluate(
        access: McpAccess,
        tool: &ToolDefinition,
        user: Option<&AuthUser>,
    ) -> Self {
        if matches!(access, McpAccess::MixedOAuth { .. }) && user.is_none() {
            let conf = &tool.target.conf;
            if conf.allow_anonymous {
                Self::Allowed
            } else if conf.oauth_security {
                Self::NeedsOAuth
            } else {
                Self::Hidden
            }
        } else if tool.authorization.allows(user) {
            Self::Allowed
        } else {
            Self::Hidden
        }
    }
}

/// Validates declared tool access and resolves protected-resource metadata.
pub(super) fn endpoint_resource(
    access: McpAccess,
    auth: &Authenticator,
    audience: Option<&AudienceId>,
    endpoint: &str,
    tools: &BTreeMap<String, ToolDefinition>,
) -> Result<Option<AuthProtectedResource>, McpError> {
    validate_catalog(access, tools)?;
    let publishes_oauth = tools.values().any(|tool| tool.target.conf.oauth_security);
    if !matches!(access, McpAccess::MixedOAuth { .. }) && !publishes_oauth {
        return Ok(audience.and_then(|audience| auth.mcp_protected_resource(audience)));
    }
    let audience = audience
        .filter(|audience| matches_endpoint(audience.as_str(), endpoint))
        .ok_or_else(|| {
            McpError::Config(format!(
                "MCP endpoint '{endpoint}' requires a canonical HTTPS audience matching its path"
            ))
        })?;
    let (resource, same_name_scopes) = auth.mcp_tool_resource(audience).ok_or_else(|| {
        McpError::Config(format!(
            "MCP endpoint '{endpoint}' requires exactly one OAuth audience provider using Authorization Bearer"
        ))
    })?;
    validate_oauth(tools, &resource, same_name_scopes)?;
    Ok(Some(resource))
}

/// Checks opt-in compatibility before the endpoint can serve requests.
fn validate_catalog(
    access: McpAccess,
    tools: &BTreeMap<String, ToolDefinition>,
) -> Result<(), McpError> {
    let mut profile_seen = false;
    for tool in tools.values() {
        let conf = &tool.target.conf;
        let identity = tool.authorization.requires_identity();
        if matches!(access, McpAccess::Public) && identity {
            return Err(invalid_tool(
                tool,
                "anonymous MCP service contains authenticated tools",
            ));
        }
        if conf.allow_anonymous
            && (!matches!(access, McpAccess::MixedOAuth { .. })
                || identity
                || conf.oauth_security
                || conf.ui_resource_uri.is_some())
        {
            return Err(invalid_tool(
                tool,
                "anonymous tools require mixed OAuth and cannot require identity, publish OAuth security, or attach UI",
            ));
        }
        if conf.oauth_security && !identity {
            return Err(invalid_tool(
                tool,
                "OAuth publication requires an identity extractor",
            ));
        }
        if conf.openai_profile {
            if profile_seen {
                return Err(invalid_tool(
                    tool,
                    "only one profile tool is allowed per MCP endpoint",
                ));
            }
            if !conf.oauth_security
                || conf.read_only != Some(true)
                || conf.destructive == Some(true)
            {
                return Err(invalid_tool(
                    tool,
                    "profile tools require OAuth publication and read-only access",
                ));
            }
            profile_seen = true;
        }
    }
    Ok(())
}

/// Confirms the advertised scopes can express the same requirement as the tool permits.
fn validate_oauth(
    tools: &BTreeMap<String, ToolDefinition>,
    resource: &AuthProtectedResource,
    same_name_scopes: bool,
) -> Result<(), McpError> {
    if !resource
        .required_scopes
        .iter()
        .all(|scope| valid_name(scope))
    {
        return Err(McpError::Config(
            "invalid required OAuth scope for MCP tools".into(),
        ));
    }
    for tool in tools
        .values()
        .filter(|tool| tool.target.conf.oauth_security)
    {
        if !tool.authorization.conjunctive() {
            return Err(invalid_tool(
                tool,
                "OAuth publication requires conjunctive tool scopes",
            ));
        }
        if !same_name_scopes && tool.authorization.scopes().next().is_some() {
            return Err(invalid_tool(
                tool,
                "OAuth provider cannot verify same-name tool scopes",
            ));
        }
        for scope in tool.authorization.scopes() {
            let known = resource
                .advertised_scopes
                .iter()
                .chain(&resource.required_scopes)
                .any(|candidate| candidate == scope);
            if !known || !valid_name(scope) {
                return Err(invalid_tool(
                    tool,
                    "tool scope cannot be advertised by this OAuth resource",
                ));
            }
        }
    }
    Ok(())
}

fn matches_endpoint(resource: &str, endpoint: &str) -> bool {
    url::Url::parse(resource).is_ok_and(|url| url.as_str() == resource && url.path() == endpoint)
        && protected_resource_url(resource).is_ok_and(|url| !url.contains(['"', '\\']))
}

fn invalid_tool(tool: &ToolDefinition, reason: &str) -> McpError {
    McpError::Config(format!("MCP tool '{}': {reason}", tool.name))
}
