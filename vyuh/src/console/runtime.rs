//! Per-site runtime data used by the built-in console.

use std::{path::Path, time::Duration};

use crate::{Site, assets::AssetUrls, auth::SecretRing, logging::LoggingConf, routes::Routes};

use super::{FALLBACK_STYLESHEET_NAME, WEB_ASSETS, logs::LogRuntime, status::ConsoleStatusCache};

/// Immutable console links and bounded status state for one built site.
pub(crate) struct ConsoleRuntime {
    urls: ViewUrls,
    status: ConsoleStatusCache,
    logs: LogRuntime,
}

impl ConsoleRuntime {
    /// Creates console state after the site's immutable route registry is available.
    pub(crate) fn new(
        routes: Routes<'_>,
        assets: &AssetUrls,
        logging: &LoggingConf,
        project_dir: &Path,
        secrets: &SecretRing,
    ) -> Result<Self, String> {
        let urls = ViewUrls::new(&routes, assets)?;
        Ok(Self {
            urls,
            status: ConsoleStatusCache::new(),
            logs: LogRuntime::new(logging, project_dir, secrets)?,
        })
    }

    /// Returns the links resolved from this site's registered console routes.
    pub(crate) const fn urls(&self) -> &ViewUrls {
        &self.urls
    }

    /// Returns a status snapshot using this site's bounded cache.
    pub(crate) fn status(&self, site: &Site, ttl: Duration) -> super::status::StatusOut {
        self.status.get(site, ttl)
    }

    /// Returns the per-site bounded console log runtime.
    pub(crate) const fn logs(&self) -> &LogRuntime {
        &self.logs
    }
}

/// Browser-facing URLs required by the built-in console templates.
pub(crate) struct ViewUrls {
    pub(crate) home: String,
    pub(crate) overview: String,
    pub(crate) runtime: String,
    pub(crate) tasks: String,
    pub(crate) schedules: String,
    pub(crate) operations: String,
    pub(crate) logs: String,
    pub(crate) conf: String,
    pub(crate) openapi: String,
    pub(crate) api_openapi: String,
    pub(crate) stylesheet_path: String,
    pub(crate) script_path: String,
    pub(crate) favicon_path: String,
    pub(crate) logo_path: String,
}

impl ViewUrls {
    /// Resolves every console route from one finalized route registry.
    fn new(routes: &Routes<'_>, assets: &AssetUrls) -> Result<Self, String> {
        let home = required_url(&routes, "console_home")?;
        let stylesheet_path = assets
            .url(&format!("css/{}", stylesheet_name()))
            .map_err(|error| error.to_string())?;
        Ok(Self {
            overview: required_url(&routes, "console_overview")?,
            runtime: required_url(&routes, "console_runtime")?,
            tasks: required_url(&routes, "console_tasks")?,
            schedules: required_url(&routes, "console_schedules")?,
            operations: required_url(&routes, "console_operations")?,
            logs: required_url(&routes, "console_logs")?,
            conf: required_url(&routes, "console_conf")?,
            openapi: required_url(&routes, "console_openapi")?,
            api_openapi: required_url(&routes, "console_api_openapi")?,
            home,
            stylesheet_path,
            script_path: assets
                .url("console/js/console.js")
                .map_err(|error| error.to_string())?,
            favicon_path: assets
                .url("console/img/favicon.svg")
                .map_err(|error| error.to_string())?,
            logo_path: assets
                .url("console/img/vyuh-logo-transparent.png")
                .map_err(|error| error.to_string())?,
        })
    }
}

fn required_url(routes: &Routes<'_>, name: &'static str) -> Result<String, String> {
    routes
        .reverse_url(name, &[])
        .ok_or_else(|| format!("required console route '{name}' is not registered"))
}

fn stylesheet_name() -> String {
    read_stylesheet_name().unwrap_or_else(|| FALLBACK_STYLESHEET_NAME.into())
}

fn read_stylesheet_name() -> Option<String> {
    let file = WEB_ASSETS.get_file("public/css/manifest.json")?;
    let bytes = file.read_bytes_sync().ok()?;
    let manifest: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    manifest
        .get("vyuh.css")
        .and_then(|value| value.as_str())
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::{
        auth::SecretRing, logging::LoggingConf, middlewares::SlashPolicy, routes::RouteRegistry,
    };

    use super::ConsoleRuntime;

    /// Verifies console URL setup rejects an incomplete finalized route registry.
    #[test]
    fn missing_console_route_is_an_error() {
        let secrets = SecretRing::new(
            "a sufficiently long test secret",
            &[],
            Path::new("/tmp"),
            16,
        )
        .map_err(|error| error.to_string());
        let registry = RouteRegistry::build(std::iter::empty(), SlashPolicy::Exact)
            .map_err(|error| error.to_string());
        let error = registry
            .and_then(|registry| {
                secrets.and_then(|secrets| {
                    ConsoleRuntime::new(
                        crate::routes::Routes::new(&registry),
                        &crate::assets::AssetUrls::default_url(),
                        &LoggingConf::default(),
                        Path::new("/tmp"),
                        &secrets,
                    )
                })
            })
            .err();

        assert_eq!(
            error.as_deref(),
            Some("required console route 'console_home' is not registered")
        );
    }
}
