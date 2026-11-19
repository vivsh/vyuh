//! OAuth authorization for locally hosted MCP resources.

mod assertion;
mod browser;
mod cimd;
mod clients;
mod config;
mod preflight;
mod protocol;
mod registration;
mod runtime;
mod store;

pub use browser::{OAuthConsent, OAuthIssue};
pub use config::{
    OAuthCimdAuth, OAuthCimdClient, OAuthClient, OAuthDcr, OAuthDcrAdmission, OAuthDcrMethod,
    OAuthGrantVerifier, OAuthIssuer,
};
pub use preflight::mcp_preflight;
pub use protocol::OAuthIssueError;
pub(crate) use protocol::setup;
pub(crate) use runtime::IssuerRuntime;
pub(crate) use store::schema;

#[cfg(test)]
#[path = "tests/flow.rs"]
mod tests;
