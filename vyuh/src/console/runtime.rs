//! Per-site runtime data used by the built-in console.

use std::{collections::BTreeSet, time::Duration};

use axum::http::{Method, StatusCode, Uri};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};

use crate::{
    OperationId, Site,
    assets::AssetUrls,
    errors::{ErrorReport, ErrorSourceKind},
    routes::Routes,
};

use super::{FALLBACK_STYLESHEET_NAME, WEB_ASSETS, status::ConsoleStatusCache};

/// Immutable console links and bounded status state for one built site.
pub(crate) struct ConsoleRuntime {
    urls: ViewUrls,
    status: ConsoleStatusCache,
    page_operations: BTreeSet<OperationId>,
}

impl ConsoleRuntime {
    /// Creates console state after the site's immutable route registry is available.
    pub(crate) fn new(routes: Routes<'_>, assets: &AssetUrls) -> Result<Self, String> {
        let urls = ViewUrls::new(&routes, assets)?;
        Ok(Self {
            page_operations: page_operations(&routes)?,
            urls,
            status: ConsoleStatusCache::new(),
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

    /// Builds a safe login redirect for an unauthenticated console HTML route.
    pub(crate) fn login_redirect(
        &self,
        routes: Routes<'_>,
        method: &Method,
        uri: &Uri,
        report: &ErrorReport,
    ) -> Option<String> {
        if report.status != StatusCode::UNAUTHORIZED
            || report.source != ErrorSourceKind::Auth
            || *method != Method::GET
        {
            return None;
        }
        let target = uri.path_and_query()?.as_str();
        let operation = routes.resolve_url(method.clone(), target)?;
        self.page_operations.contains(&operation).then(|| {
            let next = utf8_percent_encode(target, NON_ALPHANUMERIC);
            format!("{}?next={next}", self.urls.login)
        })
    }

    /// Selects a validated console page as the post-login destination.
    pub(crate) fn destination(&self, routes: Routes<'_>, next: Option<&str>) -> String {
        let Some(next) = next else {
            return self.urls.home.clone();
        };
        let Some(operation) = routes.resolve_url(Method::GET, next) else {
            return self.urls.home.clone();
        };
        self.page_operations
            .contains(&operation)
            .then(|| next.to_owned())
            .unwrap_or_else(|| self.urls.home.clone())
    }
}

/// Browser-facing URLs required by the built-in console templates.
pub(crate) struct ViewUrls {
    pub(crate) home: String,
    pub(crate) login: String,
    pub(crate) overview: String,
    pub(crate) runtime: String,
    pub(crate) tasks: String,
    pub(crate) operations: String,
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
            login: required_url(&routes, "console_login")?,
            overview: required_url(&routes, "console_overview")?,
            runtime: required_url(&routes, "console_runtime")?,
            tasks: required_url(&routes, "console_tasks")?,
            operations: required_url(&routes, "console_operations")?,
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

fn page_operations(routes: &Routes<'_>) -> Result<BTreeSet<OperationId>, String> {
    [
        "console_home",
        "console_overview",
        "console_runtime",
        "console_operations",
        "console_operation_detail",
        "console_tasks",
        "console_task_detail",
        "console_conf",
        "console_openapi",
        "console_not_found",
    ]
    .into_iter()
    .map(|name| route_operation(routes, name))
    .collect()
}

fn route_operation(routes: &Routes<'_>, name: &'static str) -> Result<OperationId, String> {
    routes
        .operation_id(name)
        .ok_or_else(|| format!("required console route '{name}' is not registered"))
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
    use crate::{middlewares::SlashPolicy, routes::RouteRegistry};

    use super::ConsoleRuntime;

    /// Verifies console URL setup rejects an incomplete finalized route registry.
    #[test]
    fn missing_console_route_is_an_error() {
        let registry = RouteRegistry::build(std::iter::empty(), SlashPolicy::Exact)
            .map_err(|error| error.to_string());
        let error = registry
            .and_then(|registry| {
                ConsoleRuntime::new(
                    crate::routes::Routes::new(&registry),
                    &crate::assets::AssetUrls::default_url(),
                )
            })
            .err();

        assert_eq!(
            error.as_deref(),
            Some("required console route 'console_home' is not registered")
        );
    }
}
