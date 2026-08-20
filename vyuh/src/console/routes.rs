//! Console route registration.

use crate::{
    bundles,
    routes::{Methods, RouteConf},
};

use super::{CONSOLE_AUDIENCE, ConsoleConf, WEB_ASSETS, api, pages};

/// Builds the console's one immutable bundle with all routes and assets.
pub(crate) fn bundle(conf: &ConsoleConf) -> crate::bundles::Bundle {
    let mut parts = root_parts(conf);
    parts.extend(dashboard_parts(&conf.path));
    parts.extend(inspection_parts(&conf.path));
    parts.extend(api_parts(&conf.path));
    parts.extend(fallback_parts(&conf.path));
    bundles::bundle(parts).with_conf(bundles::conf().audience(CONSOLE_AUDIENCE))
}

/// Returns the site-root console parts, including its public assets.
fn root_parts(conf: &ConsoleConf) -> Vec<bundles::BundlePart> {
    vec![
        bundles::asset_dir(WEB_ASSETS.clone()),
        route(
            "console_home",
            conf.path.clone(),
            Methods::GET,
            pages::runtime,
        ),
    ]
}

/// Returns console navigation routes under the configured console path.
fn dashboard_parts(prefix: &str) -> Vec<bundles::BundlePart> {
    vec![
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
            "console_migrations",
            path(prefix, "/migrations"),
            Methods::GET,
            pages::migrations,
        ),
        route(
            "console_openapi",
            path(prefix, "/openapi"),
            Methods::GET,
            pages::openapi,
        ),
    ]
}

/// Returns console inspection routes under the configured path.
fn inspection_parts(prefix: &str) -> Vec<bundles::BundlePart> {
    vec![
        route(
            "console_routes",
            path(prefix, "/routes"),
            Methods::GET,
            pages::routes,
        ),
        route(
            "console_commands",
            path(prefix, "/commands"),
            Methods::GET,
            pages::commands,
        ),
        route(
            "console_services",
            path(prefix, "/services"),
            Methods::GET,
            pages::services,
        ),
        route(
            "console_emitters",
            path(prefix, "/emitters"),
            Methods::GET,
            pages::emitters,
        ),
        route(
            "console_signals",
            path(prefix, "/signals"),
            Methods::GET,
            pages::signals,
        ),
        route(
            "console_tasks",
            path(prefix, "/tasks"),
            Methods::GET,
            pages::tasks,
        ),
        route(
            "console_schedules",
            path(prefix, "/schedules"),
            Methods::GET,
            pages::schedules,
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
            "console_session",
            path(prefix, "/api/session"),
            Methods::GET,
            api::session,
        ),
        route(
            "console_api_routes",
            path(prefix, "/api/routes"),
            Methods::GET,
            api::routes,
        ),
        route(
            "console_api_commands",
            path(prefix, "/api/commands"),
            Methods::GET,
            api::commands,
        ),
        route(
            "console_api_services",
            path(prefix, "/api/services"),
            Methods::GET,
            api::services,
        ),
        route(
            "console_api_emitters",
            path(prefix, "/api/emitters"),
            Methods::GET,
            api::emitters,
        ),
        route(
            "console_api_signals",
            path(prefix, "/api/signals"),
            Methods::GET,
            api::signals,
        ),
        route(
            "console_api_tasks",
            path(prefix, "/api/tasks"),
            Methods::GET,
            api::tasks,
        ),
        route(
            "console_api_schedules",
            path(prefix, "/api/schedules"),
            Methods::GET,
            api::schedules,
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
            trim: true,
        },
    )
}

/// Joins a validated console prefix with one route suffix.
fn path(prefix: &str, suffix: &str) -> String {
    format!("{prefix}{suffix}")
}
