//! MCP configuration, authentication, and protocol errors.

/// Failure raised while configuring or serving an MCP endpoint.
#[derive(Debug, thiserror::Error)]
pub enum McpError {
    /// Endpoint or OAuth configuration is invalid.
    #[error("invalid MCP configuration: {0}")]
    Config(String),
    /// An operation cannot be represented safely as an MCP tool.
    #[error("operation '{tool}' cannot be exposed as MCP: {reason}")]
    UnsupportedTool { tool: String, reason: String },
    /// An MCP tool name is invalid or duplicated.
    #[error("invalid MCP tool '{0}'")]
    InvalidTool(String),
    /// A tool request could not be translated into HTTP safely.
    #[error("invalid MCP tool arguments: {0}")]
    Arguments(String),
    /// The target route could not be invoked or decoded.
    #[error("MCP tool dispatch failed: {0}")]
    Dispatch(String),
}
