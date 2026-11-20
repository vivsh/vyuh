//! OpenAI current-account profile tool contract.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The closed, empty argument object accepted by a designated profile tool.
#[derive(Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(extend("properties" = {}))]
pub struct McpProfileArgs {}

/// The profile represented by the authenticated MCP request.
///
/// The application supplies a stable, opaque ID for the connected profile and
/// chooses which optional labels to disclose.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct McpProfile {
    /// Nonblank ID unique within the app, stable across reconnects and never reassigned.
    #[schemars(regex(pattern = "\\S"))]
    pub id: String,
    /// Optional display name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "String")]
    pub name: Option<String>,
    /// Optional email address.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "String")]
    pub email: Option<String>,
    /// Optional nickname.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "String")]
    pub nickname: Option<String>,
}
