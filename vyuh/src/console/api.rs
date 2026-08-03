use crate::{
    Error, ErrorKind, OperationId, Site,
    auth::AuthUser,
    console::{
        query::{
            OperationQuery, TaskQuery, filter_operations, is_console_operation, task_limit_max,
        },
        types::{ConfigOut, OperationOut, Page, SessionOut, TaskDetailOut, TaskOut},
    },
    routes::{Json, Path, Query, Request},
};
use axum::{
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};

pub async fn logout(site: Site, _user: AuthUser, request: Request) -> Result<Response, Error> {
    let (parts, _) = request.into_parts();
    let mut response = Json(serde_json::json!({ "ok": true })).into_response();
    let logout = site.console().logout(&parts).await.map_err(Error::other)?;
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
    operation_id: OperationId,
    _user: AuthUser,
    Query(query): Query<OperationQuery>,
) -> Json<Page<OperationOut>> {
    let conf = &site.conf().console;
    let console_bundle_id = console_bundle_id(&site, operation_id);
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
    operation_id: OperationId,
    _user: AuthUser,
    Path(id): Path<String>,
) -> Result<Json<OperationOut>, Error> {
    let id = id
        .parse::<crate::OperationId>()
        .map_err(|_| Error::not_found("operation was not found"))?;
    let console_bundle_id = console_bundle_id(&site, operation_id);
    site.operations()
        .find(id)
        .filter(|op| !is_console_operation(op, console_bundle_id))
        .map(|op| OperationOut::from_operation(op, &site))
        .map(Json)
        .ok_or_else(|| Error::not_found("operation was not found"))
}

fn console_bundle_id(site: &Site, operation_id: OperationId) -> Option<uuid::Uuid> {
    site.operations().find(operation_id)?.bundle_id
}

pub async fn tasks(
    site: Site,
    _user: AuthUser,
    Query(query): Query<TaskQuery>,
) -> Result<Json<crate::routes::Page<TaskOut>>, Error> {
    let conf = &site.conf().console;
    let filter = query.to_filter(conf.page_size_default, task_limit_max(conf.page_size_max));
    let page = site.tasks().list(filter).await.map_err(Error::other)?;
    Ok(Json(page.map(|record| TaskOut::from(&record))))
}

pub async fn task_detail(
    site: Site,
    _user: AuthUser,
    Path(id): Path<String>,
) -> Result<Json<TaskDetailOut>, Error> {
    let id = id
        .parse::<crate::tasks::TaskId>()
        .map_err(|_| Error::not_found("task was not found"))?;
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

pub async fn openapi(
    site: Site,
    operation_id: OperationId,
    _user: AuthUser,
) -> Result<Response, Error> {
    let body = openapi_json(&site, operation_id).map_err(|_| Error::new(ErrorKind::Other))?;
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        body,
    )
        .into_response())
}

pub(super) fn openapi_json(site: &Site, operation_id: OperationId) -> Result<String, String> {
    let console_bundle_id = console_bundle_id(site, operation_id);
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
