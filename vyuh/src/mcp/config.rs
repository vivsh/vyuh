//! MCP endpoint and tool configuration.

use serde_json::Value;

use crate::auth::AuthUser;

use super::McpError;

/// Protocol annotations, metadata, and access options for one direct MCP tool.
#[derive(Clone, Debug, Default)]
pub struct McpToolConf {
    pub(crate) read_only: Option<bool>,
    pub(crate) destructive: Option<bool>,
    pub(crate) idempotent: Option<bool>,
    pub(crate) open_world: Option<bool>,
    pub(crate) ui_resource_uri: Option<String>,
    pub(crate) allow_anonymous: bool,
    pub(crate) oauth_security: bool,
    pub(crate) openai_profile: bool,
    pub(crate) meta: Option<Value>,
}
impl McpToolConf {
    /// Declares the MCP Apps UI resource rendered for this tool.
    pub fn ui_resource_uri(mut self, value: impl Into<String>) -> Self {
        self.ui_resource_uri = Some(value.into());
        self
    }

    /// Replaces the tool's client-visible extension metadata with a JSON object.
    ///
    /// Startup rejects non-object values and keys reserved by MCP or Vyuh.
    /// Names in `openai/fileParams` must be distinct, nonblank, and match
    /// declared top-level input fields. Keep secrets out of tool metadata.
    pub fn meta(mut self, value: Value) -> Self {
        self.meta = Some(value);
        self
    }

    /// Sets the MCP read-only annotation for this tool.
    pub fn read_only(mut self, value: bool) -> Self {
        self.read_only = Some(value);
        self
    }

    /// Sets the MCP destructive annotation for this tool.
    pub fn destructive(mut self, value: bool) -> Self {
        self.destructive = Some(value);
        self
    }

    /// Sets the MCP idempotent annotation for this tool.
    pub fn idempotent(mut self, value: bool) -> Self {
        self.idempotent = Some(value);
        self
    }

    /// Sets the MCP open-world annotation for this tool.
    pub fn open_world(mut self, value: bool) -> Self {
        self.open_world = Some(value);
        self
    }

    /// Allows signed-out callers to list and invoke this tool on a mixed OAuth endpoint.
    ///
    /// Startup rejects identity extractors, OAuth publication, and UI resources
    /// on anonymous tools. The handler and its layers must be safe to run without a user.
    pub fn allow_anonymous(mut self) -> Self {
        self.allow_anonymous = true;
        self
    }

    /// Publishes the OAuth requirements of a tool with a required identity extractor.
    ///
    /// On mixed endpoints the descriptor becomes visible before sign-in. Keep
    /// private data out of it. Startup checks that the OAuth provider and any
    /// declared tool scopes can support the published contract.
    pub fn oauth_security(mut self) -> Self {
        self.oauth_security = true;
        self
    }

    /// Designates this tool as the current-account profile source for OpenAI clients.
    ///
    /// Set `read_only(true)` and `oauth_security()` and require an identity.
    /// Startup requires the profile DTOs and at most one designation per endpoint.
    pub fn openai_profile(mut self) -> Self {
        self.openai_profile = true;
        self
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum McpAccess {
    Public,
    Required { predicate: fn(&AuthUser) -> bool },
    MixedOAuth { predicate: fn(&AuthUser) -> bool },
}

/// Configuration for one bundle-owned remote MCP endpoint.
#[derive(Clone, Debug)]
pub struct McpConf {
    pub(crate) endpoint: String,
    pub(crate) access: McpAccess,
    pub(crate) server_name: Option<String>,
    pub(crate) server_version: Option<String>,
    pub(crate) instructions: Option<String>,
    pub(crate) errors: Vec<String>,
}
impl McpConf {
    /// Creates an MCP endpoint protected by its owning bundle's audience.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            access: McpAccess::Required {
                predicate: authenticated,
            },
            server_name: None,
            server_version: None,
            instructions: None,
            errors: Vec::new(),
        }
    }
    /// Restricts authenticated requests to users accepted by `pred`.
    ///
    /// Keeps the anonymous tool opt-ins on a mixed endpoint.
    pub fn auth(mut self, pred: fn(&AuthUser) -> bool) -> Self {
        self.access = match self.access {
            McpAccess::MixedOAuth { .. } => McpAccess::MixedOAuth { predicate: pred },
            _ => McpAccess::Required { predicate: pred },
        };
        self
    }
    /// Deliberately exposes this endpoint without authentication.
    pub fn public(mut self) -> Self {
        self.access = McpAccess::Public;
        self
    }

    /// Enables anonymous and OAuth tools on one endpoint through explicit tool opt-ins.
    ///
    /// Retains any existing account predicate. Startup requires a canonical HTTPS
    /// resource URL and a single OAuth provider using the Authorization Bearer header.
    pub fn mixed_oauth(mut self) -> Self {
        let predicate = match self.access {
            McpAccess::Required { predicate } | McpAccess::MixedOAuth { predicate } => predicate,
            McpAccess::Public => authenticated,
        };
        self.access = McpAccess::MixedOAuth { predicate };
        self
    }

    /// Sets this endpoint's discovery name, which defaults to `vyuh`.
    ///
    /// Startup rejects blank names and names containing control characters.
    pub fn server_name(mut self, value: impl Into<String>) -> Self {
        self.server_name = Some(value.into());
        self
    }

    /// Sets this endpoint's discovery version, which defaults to the Vyuh crate version.
    ///
    /// Startup rejects blank versions and versions containing control characters.
    pub fn server_version(mut self, value: impl Into<String>) -> Self {
        self.server_version = Some(value.into());
        self
    }

    /// Sets client-visible guidance shared by all tools on this endpoint.
    ///
    /// Discovery exposes this text to MCP clients. Keep secrets and private records
    /// out of it. The text grants no permissions and enforces no policy.
    pub fn instructions(mut self, value: impl Into<String>) -> Self {
        self.instructions = Some(value.into());
        self
    }

    pub(crate) fn validate(&mut self) -> Result<(), McpError> {
        if let Err(reason) = crate::bundles::validate_route_path(&self.endpoint) {
            self.errors.push(format!("invalid endpoint: {reason}"));
        }
        if self.endpoint == "/" {
            self.errors.push("endpoint cannot be the root path".into());
        }
        for (field, value) in [
            ("server name", &self.server_name),
            ("server version", &self.server_version),
        ] {
            if value.as_deref().is_some_and(invalid_identity) {
                self.errors.push(format!(
                    "MCP {field} must be nonblank and free of control characters"
                ));
            }
        }
        if self.errors.is_empty() {
            Ok(())
        } else {
            Err(McpError::Config(self.errors.join("; ")))
        }
    }
}

fn invalid_identity(value: &str) -> bool {
    value.trim().is_empty() || value.chars().any(char::is_control)
}

fn authenticated(_: &AuthUser) -> bool {
    true
}
