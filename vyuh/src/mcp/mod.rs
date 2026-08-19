//! Feature-gated semantic MCP tools and independently configured services.
//!
//! Direct tools execute through [`McpToolContext`] without a route adapter.

mod config;
mod context;
mod dispatch;
mod engine;
mod error;
mod future;
mod protocol;
mod registry;
mod resources;
mod tools;

#[cfg(test)]
mod tests;

pub use config::{McpConf, McpToolConf};
pub use context::McpToolContext;
pub use error::McpError;
pub use future::McpFuture;
pub use resources::{McpResource, McpUiResourceMeta};

pub(crate) use engine::McpEngine;
pub(crate) use registry::{
    McpDirectRegistration, McpResourceRegistry, McpToolRegistry, McpToolTarget,
};
