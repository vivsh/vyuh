//! MCP endpoint and tool configuration.

use crate::auth::AuthUser;

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

/// Configuration for one bundle-owned remote MCP endpoint.
#[derive(Clone, Debug)]
pub struct McpConf {
    pub(crate) endpoint: String,
    pub(crate) auth: Option<fn(&AuthUser) -> bool>,
    pub(crate) errors: Vec<String>,
}
impl McpConf {
    /// Creates an MCP endpoint protected by its owning bundle's audience.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            auth: Some(authenticated),
            errors: Vec::new(),
        }
    }
    /// Restricts this endpoint to authenticated users accepted by `pred`.
    pub fn auth(mut self, pred: fn(&AuthUser) -> bool) -> Self {
        self.auth = Some(pred);
        self
    }
    /// Deliberately exposes this endpoint without authentication.
    pub fn public(mut self) -> Self {
        self.auth = None;
        self
    }
    pub(crate) fn validate(&mut self) -> Result<(), McpError> {
        if let Err(reason) = crate::bundles::validate_route_path(&self.endpoint) {
            self.errors.push(format!("invalid endpoint: {reason}"));
        }
        if self.endpoint == "/" {
            self.errors.push("endpoint cannot be the root path".into());
        }
        if self.errors.is_empty() {
            Ok(())
        } else {
            Err(McpError::Config(self.errors.join("; ")))
        }
    }
}

fn authenticated(_: &AuthUser) -> bool {
    true
}
