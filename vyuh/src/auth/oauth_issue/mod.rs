//! OAuth authorization for locally hosted MCP resources.

mod browser;
mod config;
mod protocol;
mod runtime;
mod store;

pub use browser::{OAuthConsent, OAuthIssue};
pub use config::{OAuthClient, OAuthGrantVerifier, OAuthIssuer};
pub use protocol::OAuthIssueError;
pub(crate) use protocol::setup;
pub(crate) use runtime::IssuerRuntime;
pub(crate) use store::schema;

#[cfg(test)]
#[path = "tests/flow.rs"]
mod tests;
