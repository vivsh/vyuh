//! Console route registration.

use crate::{
    bundles,
    routes::{Methods, RouteConf},
};

use super::{ConsoleConf, WEB_ASSETS, api, auth, login, pages};

/// Builds the console's one immutable bundle with all routes and assets.
pub(crate) fn bundle(conf: &ConsoleConf) -> crate::bundles::Bundle {
    let mut parts = root_parts(conf);
    parts.extend(dashboard_parts(&conf.path));
    parts.extend(inspection_parts(&conf.path));
    parts.extend(api_parts(&conf.path));
    parts.extend(login_parts(&conf.path));
    parts.extend(fallback_parts(&conf.path));
    bundles::bundle(parts).with_audience(auth::CONSOLE_AUDIENCE)
}

/// Returns the public route that exchanges a command credential for a cookie.
fn login_parts(prefix: &str) -> Vec<bundles::BundlePart> {
    vec![route(
        "console_login",
        path(prefix, "/login"),
        Methods::GET | Methods::POST,
        login::route,
    )]
}

/// Returns the site-root console parts, including its public assets.
fn root_parts(conf: &ConsoleConf) -> Vec<bundles::BundlePart> {
    vec![
        bundles::asset_dir(WEB_ASSETS.clone()),
        route(
            "console_home",
            conf.path.clone(),
            Methods::GET,
            pages::overview,
        ),
    ]
}

/// Returns console navigation routes under the configured console path.
fn dashboard_parts(prefix: &str) -> Vec<bundles::BundlePart> {
    vec![
        route(
            "console_overview",
            path(prefix, "/overview"),
            Methods::GET,
            pages::overview,
        ),
        route(
            "console_runtime",
            path(prefix, "/runtime"),
            Methods::GET,
            pages::runtime,
        ),
        route(
            "console_conf",
            path(prefix, "/conf"),
            Methods::GET,
            pages::conf,
        ),
        route(
            "console_openapi",
            path(prefix, "/openapi"),
            Methods::GET,
            pages::openapi,
        ),
    ]
}

/// Returns console operation and task inspection routes under the configured path.
fn inspection_parts(prefix: &str) -> Vec<bundles::BundlePart> {
    vec![
        route(
            "console_operations",
            path(prefix, "/operations"),
            Methods::GET,
            pages::operations,
        ),
        route(
            "console_operation_detail",
            path(prefix, "/operations/{id}"),
            Methods::GET,
            pages::operation_detail,
        ),
        route(
            "console_tasks",
            path(prefix, "/tasks"),
            Methods::GET,
            pages::tasks,
        ),
        route(
            "console_logs",
            path(prefix, "/logs"),
            Methods::GET,
            pages::logs,
        ),
        route(
            "console_task_detail",
            path(prefix, "/tasks/{id}"),
            Methods::GET,
            pages::task_detail,
        ),
    ]
}

/// Returns console API routes under the configured path.
fn api_parts(prefix: &str) -> Vec<bundles::BundlePart> {
    vec![
        route(
            "console_logout",
            path(prefix, "/api/logout"),
            Methods::POST,
            api::logout,
        ),
        route(
            "console_session",
            path(prefix, "/api/session"),
            Methods::GET,
            api::session,
        ),
        route(
            "console_api_operations",
            path(prefix, "/api/operations"),
            Methods::GET,
            api::operations,
        ),
        route(
            "console_api_operation_detail",
            path(prefix, "/api/operations/{id}"),
            Methods::GET,
            api::operation_detail,
        ),
        route(
            "console_api_tasks",
            path(prefix, "/api/tasks"),
            Methods::GET,
            api::tasks,
        ),
        route(
            "console_api_logs",
            path(prefix, "/api/logs"),
            Methods::GET,
            api::logs,
        ),
        route(
            "console_api_task_detail",
            path(prefix, "/api/tasks/{id}"),
            Methods::GET,
            api::task_detail,
        ),
        route(
            "console_api_status",
            path(prefix, "/api/status"),
            Methods::GET,
            api::status,
        ),
        route(
            "console_api_conf",
            path(prefix, "/api/conf"),
            Methods::GET,
            api::conf,
        ),
        route(
            "console_api_openapi",
            path(prefix, "/api/openapi"),
            Methods::GET,
            api::openapi,
        ),
    ]
}

/// Returns the console fallback route after all concrete console routes.
fn fallback_parts(prefix: &str) -> Vec<bundles::BundlePart> {
    vec![route(
        "console_not_found",
        path(prefix, "/{*path}"),
        Methods::GET,
        pages::not_found_page,
    )]
}

/// Registers a configured console handler as one bundle part.
fn route<H, T, Args>(name: &str, path: String, methods: Methods, handler: H) -> bundles::BundlePart
where
    H: axum::handler::Handler<T, crate::Site>
        + crate::callables::Specable<Args>
        + Clone
        + Send
        + Sync
        + 'static,
    T: 'static,
    Args: crate::callables::IntoArgSpecs + 'static,
{
    bundles::route(
        handler,
        RouteConf {
            name: name.to_owned().into(),
            methods,
            path: path.into(),
            slash: None,
        },
    )
}

/// Joins a validated console prefix with one route suffix.
fn path(prefix: &str, suffix: &str) -> String {
    format!("{prefix}{suffix}")
}
