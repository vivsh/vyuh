use axum::{
    http::{StatusCode, header},
    response::{IntoResponse, Redirect, Response},
};
use schemars::JsonSchema;
use serde::Deserialize;

use crate::{
    Site,
    console::{
        auth::{ConsoleCookies, ConsoleSessionUser, expired_cookie, session_cookie},
        query::{
            OperationQuery, TaskQuery, filter_operations, is_console_operation, task_limit_max,
        },
        types::{ConfigOut, OperationOut, Page, SessionOut, TaskDetailOut, TaskOut},
    },
    routes::{Json, Path, Query},
};

#[derive(Debug, Deserialize, JsonSchema)]
pub struct LoginQuery {
    token: String,
}

pub async fn login(
    site: Site,
    Query(query): Query<LoginQuery>,
    ConsoleCookies(jar): ConsoleCookies,
) -> Result<(axum_extra::extract::CookieJar, Response), StatusCode> {
    let runtime = site.console_runtime().ok_or(StatusCode::NOT_FOUND)?;
    let conf = &site.conf().console;
    let ttl = std::time::Duration::from_secs(conf.session_ttl_seconds);
    let token = runtime
        .consume_bootstrap(&query.token, ttl)
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let max_age = time::Duration::seconds(conf.session_ttl_seconds as i64);
    let jar = jar.add(session_cookie(&conf.cookie_name, token, max_age));
    Ok((jar, Redirect::to(&conf.path).into_response()))
}

pub async fn logout(
    site: Site,
    ConsoleSessionUser(_user): ConsoleSessionUser,
    ConsoleCookies(jar): ConsoleCookies,
) -> (axum_extra::extract::CookieJar, Json<serde_json::Value>) {
    let conf = &site.conf().console;
    if let Some(cookie) = jar.get(&conf.cookie_name)
        && let Some(runtime) = site.console_runtime()
    {
        runtime.clear_session(cookie.value());
    }
    let jar = jar.add(expired_cookie(&conf.cookie_name));
    (jar, Json(serde_json::json!({ "ok": true })))
}

pub async fn session(ConsoleSessionUser(user): ConsoleSessionUser) -> Json<SessionOut> {
    Json(SessionOut {
        subject: user.subject,
        roles: user.roles,
        role_names: user.role_names,
    })
}

pub async fn operations(
    site: Site,
    ConsoleSessionUser(_user): ConsoleSessionUser,
    Query(query): Query<OperationQuery>,
) -> Json<Page<OperationOut>> {
    let conf = &site.conf().console;
    let console_bundle_id = console_bundle_id(&site);
    let (items, next_cursor) = filter_operations(
        site.iter_operations(),
        &query,
        console_bundle_id,
        conf.page_size_default,
        conf.page_size_max,
    );
    Json(Page {
        items: items
            .into_iter()
            .map(|op| OperationOut::from_operation(op, &site))
            .collect(),
        next_cursor,
    })
}

pub async fn operation_detail(
    site: Site,
    ConsoleSessionUser(_user): ConsoleSessionUser,
    Path(id): Path<String>,
) -> Result<Json<OperationOut>, StatusCode> {
    let id = uuid::Uuid::parse_str(&id).map_err(|_| StatusCode::NOT_FOUND)?;
    let console_bundle_id = console_bundle_id(&site);
    site.iter_operations()
        .find(|op| op.id == id && !is_console_operation(op, console_bundle_id))
        .map(|op| OperationOut::from_operation(op, &site))
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

fn console_bundle_id(site: &Site) -> Option<uuid::Uuid> {
    site.console_runtime().map(|runtime| runtime.bundle_id())
}

pub async fn tasks(
    site: Site,
    ConsoleSessionUser(_user): ConsoleSessionUser,
    Query(query): Query<TaskQuery>,
) -> Result<Json<Page<TaskOut>>, StatusCode> {
    let conf = &site.conf().console;
    let filter = query.to_filter(conf.page_size_default, task_limit_max(conf.page_size_max));
    let page = site
        .tasks()
        .list(filter)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(Page {
        items: page.records.iter().map(TaskOut::from).collect(),
        next_cursor: page.next_cursor,
    }))
}

pub async fn task_detail(
    site: Site,
    ConsoleSessionUser(_user): ConsoleSessionUser,
    Path(id): Path<String>,
) -> Result<Json<TaskDetailOut>, StatusCode> {
    let id = uuid::Uuid::parse_str(&id).map_err(|_| StatusCode::NOT_FOUND)?;
    let record = site
        .tasks()
        .get(id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(TaskDetailOut::from(&record)))
}

pub async fn status(
    site: Site,
    ConsoleSessionUser(_user): ConsoleSessionUser,
) -> Json<crate::console::status::StatusOut> {
    let ttl = std::time::Duration::from_secs(site.conf().console.status_cache_ttl_seconds);
    let status = site
        .console_runtime()
        .map(|runtime| runtime.status(&site, ttl))
        .unwrap_or_else(|| crate::console::status::collect(&site));
    Json(status)
}

pub async fn conf(site: Site, ConsoleSessionUser(_user): ConsoleSessionUser) -> Json<ConfigOut> {
    Json(ConfigOut::from_site(&site))
}

pub async fn openapi(
    site: Site,
    ConsoleSessionUser(_user): ConsoleSessionUser,
) -> Result<Response, StatusCode> {
    let body = openapi_json(&site).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        body,
    )
        .into_response())
}

pub(super) fn openapi_json(site: &Site) -> Result<String, String> {
    let console_bundle_id = console_bundle_id(site);
    let routes = site
        .iter_operations()
        .filter(|op| {
            op.kind == crate::OperationKind::Route
                && !op.hidden
                && !is_console_operation(op, console_bundle_id)
        })
        .collect::<Vec<_>>();
    let generator = crate::apidocs::ApiDocGenerator::new(crate::apidocs::ApiMeta {
        title: "Application API".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        description: Some("Routes registered outside the Vyuh console.".to_string()),
        tags: Vec::new(),
    });
    let spec = generator.generate(&routes).map_err(|e| e.to_string())?;
    serde_json::to_string_pretty(&spec).map_err(|e| e.to_string())
}
