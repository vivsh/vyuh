use crate::{
    Error, ErrorKind, OperationId, Site,
    console::{
        access::ConsoleGuard,
        logs::{LogError, limit as log_limit},
        query::{InspectionQuery, LogQuery, TaskQuery, is_console_operation, task_limit_max},
        schedules::{SchedulePage, ScheduleQuery, page as schedule_page},
        types::{
            CommandOut, ConfigOut, OperationOut, Page, ServiceOut, SessionOut, TaskDetailOut,
            TaskOut,
        },
    },
    routes::{Json, Path, Query},
};
use axum::{
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};

pub async fn session(guard: ConsoleGuard) -> Json<SessionOut> {
    Json(SessionOut::from(guard.user()))
}

pub async fn routes(
    site: Site,
    operation_id: OperationId,
    _guard: ConsoleGuard,
    Query(query): Query<InspectionQuery>,
) -> Json<Page<OperationOut>> {
    let console_bundle_id = console_bundle_id(&site, operation_id);
    Json(super::pages::inspection_page(
        &site,
        &query,
        console_bundle_id,
        is_route,
    ))
}

pub async fn signals(
    site: Site,
    operation_id: OperationId,
    _guard: ConsoleGuard,
    Query(query): Query<InspectionQuery>,
) -> Json<Page<OperationOut>> {
    let console_bundle_id = console_bundle_id(&site, operation_id);
    Json(super::pages::inspection_page(
        &site,
        &query,
        console_bundle_id,
        is_signal,
    ))
}

pub async fn emitters(
    site: Site,
    operation_id: OperationId,
    _guard: ConsoleGuard,
    Query(query): Query<InspectionQuery>,
) -> Json<Page<OperationOut>> {
    let console_bundle_id = console_bundle_id(&site, operation_id);
    Json(super::pages::inspection_page(
        &site,
        &query,
        console_bundle_id,
        is_emitter,
    ))
}

pub async fn commands(
    site: Site,
    _guard: ConsoleGuard,
    Query(query): Query<InspectionQuery>,
) -> Json<Page<CommandOut>> {
    Json(super::pages::command_page(&site, &query))
}

pub async fn services(
    site: Site,
    _guard: ConsoleGuard,
    Query(query): Query<InspectionQuery>,
) -> Json<Page<ServiceOut>> {
    Json(super::pages::service_page(&site, &query))
}

fn console_bundle_id(site: &Site, operation_id: OperationId) -> Option<uuid::Uuid> {
    site.operations().find(operation_id)?.bundle_id
}

fn is_route(kind: &crate::OperationKind) -> bool {
    matches!(kind, crate::OperationKind::Route)
}

fn is_signal(kind: &crate::OperationKind) -> bool {
    matches!(kind, crate::OperationKind::Signal)
}

fn is_emitter(kind: &crate::OperationKind) -> bool {
    matches!(
        kind,
        crate::OperationKind::Cron
            | crate::OperationKind::Periodic
            | crate::OperationKind::PgNotify
    )
}

pub async fn tasks(
    site: Site,
    _guard: ConsoleGuard,
    Query(query): Query<TaskQuery>,
) -> Result<Json<crate::routes::Page<TaskOut>>, Error> {
    let conf = &site.conf().console;
    let filter = query.to_filter(conf.page_size_default, task_limit_max(conf.page_size_max));
    let page = site.tasks().list(filter).await.map_err(Error::other)?;
    Ok(Json(page.map(|record| TaskOut::from(&record))))
}

/// Returns immutable task schedule definitions with their durable cursors.
pub async fn schedules(
    site: Site,
    _guard: ConsoleGuard,
    Query(query): Query<ScheduleQuery>,
) -> Result<Json<SchedulePage>, Error> {
    let conf = &site.conf().console;
    schedule_page(&site, &query, conf.page_size_default, conf.page_size_max)
        .await
        .map(Json)
        .map_err(Error::other)
}

/// Returns one bounded page of configured JSON file logs.
pub async fn logs(
    site: Site,
    _guard: ConsoleGuard,
    Query(query): Query<LogQuery>,
) -> Result<Response, Error> {
    let limit = log_limit(
        &query,
        site.conf().console.page_size_default,
        site.conf().console.page_size_max,
    );
    let runtime = site
        .console_logs()
        .ok_or_else(|| Error::new(ErrorKind::Other))?;
    let page = runtime.page(&query, limit).await.map_err(log_error)?;
    let mut response = Json(page).into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );
    Ok(response)
}

fn log_error(error: LogError) -> Error {
    match error {
        LogError::InvalidQuery | LogError::InvalidCursor => {
            Error::bad_request("Invalid log query.")
        }
        LogError::EntryUnavailable | LogError::Unavailable => Error::new(ErrorKind::Other),
    }
}

pub async fn task_detail(
    site: Site,
    _guard: ConsoleGuard,
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

pub async fn status(site: Site, _guard: ConsoleGuard) -> Json<crate::console::status::StatusOut> {
    Json(site.console_status())
}

pub async fn conf(site: Site, _guard: ConsoleGuard) -> Json<ConfigOut> {
    Json(ConfigOut::from_site(&site))
}

pub async fn openapi(
    site: Site,
    operation_id: OperationId,
    _guard: ConsoleGuard,
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
