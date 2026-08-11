//! MCP endpoint and tool configuration.

use crate::auth::{Audience, AuthProvider};

use super::McpError;

/// Protocol annotations attached to one direct MCP tool.
#[derive(Clone, Debug, Default)]
pub struct McpToolConf {
    pub(crate) read_only: Option<bool>,
    pub(crate) destructive: Option<bool>,
    pub(crate) idempotent: Option<bool>,
    pub(crate) open_world: Option<bool>,
}
impl McpToolConf {
    pub fn read_only(mut self, value: bool) -> Self {
        self.read_only = Some(value);
        self
    }
    pub fn destructive(mut self, value: bool) -> Self {
        self.destructive = Some(value);
        self
    }
    pub fn idempotent(mut self, value: bool) -> Self {
        self.idempotent = Some(value);
        self
    }
    pub fn open_world(mut self, value: bool) -> Self {
        self.open_world = Some(value);
        self
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum McpAuth {
    Provider(AuthProvider),
    Anonymous,
}

/// Configuration for one bundle-owned remote MCP endpoint.
#[derive(Clone, Debug)]
pub struct McpConf {
    pub(crate) endpoint: String,
    pub(crate) audience: Audience,
    pub(crate) auth: Option<McpAuth>,
    pub(crate) errors: Vec<String>,
}
impl McpConf {
    /// Creates an MCP endpoint. Choose [`Self::auth`] or [`Self::anonymous`] explicitly.
    pub fn new(endpoint: impl Into<String>, audience: Audience) -> Self {
        Self {
            endpoint: endpoint.into(),
            audience,
            auth: None,
            errors: Vec::new(),
        }
    }
    /// Selects the one configured Vyuh credential provider accepted by this endpoint.
    pub fn auth(mut self, provider: AuthProvider) -> Self {
        self.auth = Some(McpAuth::Provider(provider));
        self
    }
    /// Explicitly exposes tools without credentials.
    pub fn anonymous(mut self) -> Self {
        self.auth = Some(McpAuth::Anonymous);
        self
    }
    pub(crate) fn validate(&mut self) -> Result<(), McpError> {
        if let Err(reason) = crate::bundles::validate_route_path(&self.endpoint) {
            self.errors.push(format!("invalid endpoint: {reason}"));
        }
        if self.endpoint == "/" {
            self.errors.push("endpoint cannot be the root path".into());
        }
        if self.auth.is_none() {
            self.errors
                .push("MCP service must select auth(provider) or anonymous()".into());
        }
        if self.errors.is_empty() {
            Ok(())
        } else {
            Err(McpError::Config(self.errors.join("; ")))
        }
    }
}
