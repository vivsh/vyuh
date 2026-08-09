//! Shared object-safe future type for MCP extension contracts.

use std::{future::Future, pin::Pin};

/// Boxed async result returned by object-safe MCP extension traits.
pub type McpFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
