//! Feature-gated semantic MCP tools and independently configured services.
//!
//! Direct tools execute through [`McpToolContext`]. Eligible route-backed tools
//! reconstruct a controlled JSON request and execute through the built router.

mod config;
mod context;
mod dispatch;
mod engine;
mod error;
mod future;
mod protocol;
mod registry;
mod tools;

#[cfg(test)]
mod tests;

pub use config::{McpConf, McpToolConf};
pub use context::McpToolContext;
pub use error::McpError;
pub use future::McpFuture;

pub(crate) use engine::McpEngine;
pub(crate) use registry::{McpDirectRegistration, McpToolRegistry, McpToolTarget};
