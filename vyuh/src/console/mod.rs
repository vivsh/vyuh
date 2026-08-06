//! Built-in authenticated runtime console.

mod access;
mod api;
mod conf;
pub(crate) mod logs;
mod middleware;
mod pages;
mod query;
mod routes;
mod runtime;
mod schedules;
mod schema_view;
pub(crate) mod status;
#[cfg(test)]
mod tests;
mod types;

use crate::embed;

pub use access::{CONSOLE_AUDIENCE, ConsoleAccess};
pub use conf::ConsoleConf;
pub(crate) use runtime::{ConsoleRuntime, ViewUrls};

const WEB_ASSETS: embed::Dir = embed::embed_assets!("web", force = true);
const FALLBACK_STYLESHEET_NAME: &str = "vyuh.css";

/// Builds the console's single immutable bundle from its registered parts.
pub(crate) fn bundle(conf: &ConsoleConf) -> crate::bundles::Bundle {
    routes::bundle(conf)
}

pub(crate) fn redacted_config(site: &crate::Site) -> types::ConfigOut {
    types::ConfigOut::from_site(site)
}
