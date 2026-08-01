use axum::{
    http::{StatusCode, header},
    response::{IntoResponse, Redirect, Response},
};
use schemars::JsonSchema;
use serde::Deserialize;

use crate::{
    Error, ErrorKind, Site,
    auth::AuthUser,
    console::{
        auth::{CONSOLE_AUDIENCE, CONSOLE_LOGIN, CONSOLE_TOKEN},
        query::{
            OperationQuery, TaskQuery, filter_operations, is_console_operation, task_limit_max,
        },
        types::{ConfigOut, OperationOut, Page, SessionOut, TaskDetailOut, TaskOut},
    },
    routes::{ClientIp, Form, Json, Path, Query},
};

#[derive(Debug, Deserialize, JsonSchema)]
pub struct LoginForm {
    token: String,
}

pub async fn login(
    site: Site,
    ClientIp(ip): ClientIp,
    Form(form): Form<LoginForm>,
) -> Result<Response, Error> {
    let parts = axum::http::Request::new(axum::body::Body::empty())
        .into_parts()
        .0;
    let user = site
        .auth()
        .verify_raw(CONSOLE_LOGIN, &form.token, &parts, CONSOLE_AUDIENCE)
        .await
        .map_err(|error| {
            tracing::debug!(error = %error, "console login credential rejected");
            Error::new(ErrorKind::Unauthorized)
        })?;
    let login = site
        .auth()
        .using(CONSOLE_TOKEN)
        .login_bound(user, &[CONSOLE_AUDIENCE], ip.to_string())
        .await
        .map_err(Error::other)?;
    let destination = site
        .routes()
        .reverse_url("console_home", &[])
        .ok_or_else(|| Error::not_found("console home route is unavailable"))?;
    let mut response = Redirect::to(&destination).into_response();
    login.write(&mut response);
    Ok(response)
}

pub async fn logout(site: Site, _user: AuthUser) -> Result<Response, Error> {
    let parts = axum::http::Request::new(axum::body::Body::empty())
        .into_parts()
        .0;
    let mut response = Json(serde_json::json!({ "ok": true })).into_response();
    let logout = site
        .auth()
        .using(CONSOLE_TOKEN)
        .logout(&parts)
        .await
        .map_err(Error::other)?;
    logout.write(&mut response);
    Ok(response)
}

pub async fn session(user: AuthUser) -> Json<SessionOut> {
    Json(SessionOut {
        subject: user.key.to_string(),
        roles: user.roles,
        role_names: Vec::new(),
    })
}

pub async fn operations(
    site: Site,
    _user: AuthUser,
    Query(query): Query<OperationQuery>,
) -> Json<Page<OperationOut>> {
    let conf = &site.conf().console;
    let console_bundle_id = console_bundle_id(&site);
    let (items, next_cursor) = filter_operations(
        site.operations().list(),
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
    _user: AuthUser,
    Path(id): Path<String>,
) -> Result<Json<OperationOut>, Error> {
    let id = id
        .parse::<crate::OperationId>()
        .map_err(|_| Error::not_found("operation was not found"))?;
    let console_bundle_id = console_bundle_id(&site);
    site.operations()
        .find(id)
        .filter(|op| !is_console_operation(op, console_bundle_id))
        .map(|op| OperationOut::from_operation(op, &site))
        .map(Json)
        .ok_or_else(|| Error::not_found("operation was not found"))
}

fn console_bundle_id(site: &Site) -> Option<uuid::Uuid> {
    site.console_bundle_id()
}

pub async fn tasks(
    site: Site,
    _user: AuthUser,
    Query(query): Query<TaskQuery>,
) -> Result<Json<Page<TaskOut>>, Error> {
    let conf = &site.conf().console;
    let filter = query.to_filter(conf.page_size_default, task_limit_max(conf.page_size_max));
    let page = site.tasks().list(filter).await.map_err(Error::other)?;
    Ok(Json(Page {
        items: page.records.iter().map(TaskOut::from).collect(),
        next_cursor: page.next_cursor,
    }))
}

pub async fn task_detail(
    site: Site,
    _user: AuthUser,
    Path(id): Path<String>,
) -> Result<Json<TaskDetailOut>, Error> {
    let id = uuid::Uuid::parse_str(&id).map_err(|_| Error::not_found("task was not found"))?;
    let record = site
        .tasks()
        .get(id)
        .await
        .map_err(Error::other)?
        .ok_or_else(|| Error::not_found("task was not found"))?;
    Ok(Json(TaskDetailOut::from(&record)))
}

pub async fn status(site: Site, _user: AuthUser) -> Json<crate::console::status::StatusOut> {
    Json(site.console_status())
}

pub async fn conf(site: Site, _user: AuthUser) -> Json<ConfigOut> {
    Json(ConfigOut::from_site(&site))
}

pub async fn openapi(site: Site, _user: AuthUser) -> Result<Response, Error> {
    let body = openapi_json(&site).map_err(|_| Error::new(ErrorKind::Other))?;
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
        .operations()
        .list()
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
