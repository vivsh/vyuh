use axum::{
    http::StatusCode,
    response::{Html, IntoResponse, Response},
};
use serde_json::json;
use url::form_urlencoded;

use crate::routes::{Path, Query};
use crate::{
    OperationId, Site,
    console::{
        access::ConsoleGuard,
        logs::limit as log_limit,
        query::{
            LogQuery, OperationQuery, TaskQuery, filter_operations, is_console_operation,
            task_limit_max, task_per_page,
        },
        schedules::{ScheduleOut, ScheduleQuery, page as schedule_page},
        schema_view::OperationView,
        status::StatusOut,
        types::{ConfigOut, OperationOut, Page, TaskDetailOut, TaskOut},
    },
    templates::TemplateError,
};

/// Renders the console overview page or its safe internal-error page.
pub async fn overview(site: Site, _guard: ConsoleGuard) -> Response {
    render_or_error(
        &site,
        render_page(
            &site,
            "console/overview.html",
            "overview",
            "Overview",
            json!({ "status": status_snapshot(&site) }),
        ),
    )
}

/// Renders the console runtime-inspection page or its safe internal-error page.
pub async fn runtime(site: Site, _guard: ConsoleGuard) -> Response {
    render_or_error(
        &site,
        render_page(
            &site,
            "console/runtime.html",
            "runtime",
            "Runtime",
            runtime_context(status_snapshot(&site)),
        ),
    )
}

/// Renders the filtered application operation inspector.
pub async fn operations(
    site: Site,
    operation_id: OperationId,
    _guard: ConsoleGuard,
    Query(query): Query<OperationQuery>,
) -> Response {
    let conf = &site.conf().console;
    let console_bundle_id = console_bundle_id(&site, operation_id);
    let (items, next_cursor) = filter_operations(
        site.operations().list(),
        &query,
        console_bundle_id,
        conf.page_size_default,
        conf.page_size_max,
    );
    let page = Page {
        items: items
            .into_iter()
            .map(|op| OperationOut::from_operation(op, &site))
            .collect::<Vec<_>>(),
        next_cursor,
    };
    let selected_operation = selected_operation(&site, &query, console_bundle_id);
    render_or_error(
        &site,
        render_page(
            &site,
            "console/operations.html",
            "operations",
            "Operations",
            json!({
                "page": page,
                "query": query,
                "kinds": operation_kinds(),
                "selected_operation": selected_operation,
            }),
        ),
    )
}

/// Renders one application operation or a console not-found page.
pub async fn operation_detail(
    site: Site,
    operation_id: OperationId,
    _guard: ConsoleGuard,
    Path(id): Path<String>,
) -> Response {
    let Ok(id) = id.parse::<crate::OperationId>() else {
        return not_found(&site);
    };
    let Some(operation) = site
        .operations()
        .find(id)
        .filter(|operation| {
            !is_console_operation(operation, console_bundle_id(&site, operation_id))
        })
        .map(|op| OperationOut::from_operation(op, &site))
    else {
        return not_found(&site);
    };
    render_or_error(
        &site,
        render_page(
            &site,
            "console/operation_detail.html",
            "operations",
            "Operation",
            json!({ "operation": operation }),
        ),
    )
}

/// Renders the task list or a safe console error page when inspection fails.
pub async fn tasks(site: Site, _guard: ConsoleGuard, Query(query): Query<TaskQuery>) -> Response {
    if !site.console_has_tasks() {
        let per_page = task_per_page(
            &query,
            site.conf().console.page_size_default,
            site.conf().console.page_size_max,
        );
        return render_or_error(&site, render_tasks(&site, query, empty_tasks(per_page)));
    }

    let conf = &site.conf().console;
    let filter = query.to_filter(conf.page_size_default, task_limit_max(conf.page_size_max));
    let Ok(page) = site.tasks().list(filter).await else {
        return internal_error(&site);
    };
    let page = page.map(|record| TaskOut::from(&record));
    render_or_error(&site, render_tasks(&site, query, page))
}

/// Renders immutable task schedule definitions and their durable cursors.
pub async fn schedules(
    site: Site,
    _guard: ConsoleGuard,
    Query(query): Query<ScheduleQuery>,
) -> Response {
    let conf = &site.conf().console;
    let Ok(page) = schedule_page(&site, &query, conf.page_size_default, conf.page_size_max).await
    else {
        return internal_error(&site);
    };
    render_or_error(&site, render_schedules(&site, query, page))
}

/// Renders bounded configured file logs with the standard console inspector layout.
pub async fn logs(site: Site, _guard: ConsoleGuard, Query(query): Query<LogQuery>) -> Response {
    let limit = log_limit(
        &query,
        site.conf().console.page_size_default,
        site.conf().console.page_size_max,
    );
    let Some(runtime) = site.console_logs() else {
        return internal_error(&site);
    };
    let page = match runtime.page(&query, limit).await {
        Ok(page) => page,
        Err(_) => return internal_error(&site),
    };
    let selected = match runtime.selected(&query).await {
        Ok(value) => value,
        Err(_) => return internal_error(&site),
    };
    let rendered = render_page(
        &site,
        "console/logs.html",
        "logs",
        "Logs",
        json!({
            "page": page,
            "query": query,
            "selected_log": selected,
            "next_url": log_url(site.console_urls().map(|urls| urls.logs.as_str()), &query, page.next_cursor.as_deref()),
        }),
    );
    no_store(render_or_error(&site, rendered))
}

fn log_url(base: Option<&str>, query: &LogQuery, cursor: Option<&str>) -> Option<String> {
    let base = base?;
    let mut pairs = form_urlencoded::Serializer::new(String::new());
    append_log_pair(&mut pairs, "rule", query.rule.as_deref());
    append_log_pair(&mut pairs, "level", query.level.as_deref());
    append_log_pair(&mut pairs, "target", query.target.as_deref());
    append_log_pair(&mut pairs, "from", query.from.as_deref());
    append_log_pair(&mut pairs, "to", query.to.as_deref());
    append_log_pair(&mut pairs, "q", query.q.as_deref());
    append_log_pair(&mut pairs, "cursor", cursor);
    let encoded = pairs.finish();
    (!encoded.is_empty()).then(|| format!("{base}?{encoded}"))
}

fn append_log_pair(
    serializer: &mut form_urlencoded::Serializer<'_, String>,
    key: &str,
    value: Option<&str>,
) {
    if let Some(value) = value {
        serializer.append_pair(key, value);
    }
}

/// Renders one task or an appropriate console error page.
pub async fn task_detail(site: Site, _guard: ConsoleGuard, Path(id): Path<String>) -> Response {
    let Ok(id) = id.parse::<crate::tasks::TaskId>() else {
        return not_found(&site);
    };
    let record = match site.tasks().get(id).await {
        Ok(Some(record)) => record,
        Ok(None) => return not_found(&site),
        Err(_) => return internal_error(&site),
    };
    let detail = TaskDetailOut::from(&record);
    let payload = match serde_json::to_string_pretty(&detail) {
        Ok(payload) => payload,
        Err(_) => return internal_error(&site),
    };
    render_or_error(
        &site,
        render_page(
            &site,
            "console/task_detail.html",
            "tasks",
            "Task",
            json!({ "task": detail, "payload": payload }),
        ),
    )
}

/// Renders the redacted console configuration page.
pub async fn conf(site: Site, _guard: ConsoleGuard) -> Response {
    let conf = ConfigOut::from_site(&site);
    let Ok(payload) = serde_json::to_string_pretty(&conf) else {
        return internal_error(&site);
    };
    render_or_error(
        &site,
        render_page(
            &site,
            "console/conf.html",
            "conf",
            "Config",
            json!({ "conf": conf, "payload": payload }),
        ),
    )
}

/// Renders the application OpenAPI console page.
pub async fn openapi(site: Site, _guard: ConsoleGuard) -> Response {
    render_or_error(
        &site,
        render_page(
            &site,
            "console/openapi.html",
            "openapi",
            "OpenAPI",
            json!({}),
        ),
    )
}

/// Renders the console-specific not-found page.
pub async fn not_found_page(site: Site, _guard: ConsoleGuard) -> Response {
    not_found(&site)
}

fn render_page(
    site: &Site,
    template: &str,
    active: &str,
    title: &str,
    mut context: serde_json::Value,
) -> Result<Html<String>, TemplateError> {
    context["active"] = json!(active);
    context["title"] = json!(title);
    let urls = site
        .console_urls()
        .ok_or_else(|| TemplateError::NotFound("console runtime".into()))?;
    context["urls"] = json!({
        "home": &urls.home,
        "overview": &urls.overview,
        "runtime": &urls.runtime,
        "tasks": &urls.tasks,
        "schedules": &urls.schedules,
        "operations": &urls.operations,
        "logs": &urls.logs,
        "conf": &urls.conf,
        "openapi": &urls.openapi,
        "api_openapi": &urls.api_openapi,
    });
    context["assets"] = json!({
        "favicon": &urls.favicon_path,
        "logo": &urls.logo_path,
        "script": &urls.script_path,
    });
    context["version"] = json!(env!("CARGO_PKG_VERSION"));
    context["stylesheet_path"] = json!(&urls.stylesheet_path);
    context["development_access"] = json!(site.conf().console.development_access());
    render(site, template, context)
}

fn status_snapshot(site: &Site) -> StatusOut {
    site.console_status()
}

fn runtime_context(status: StatusOut) -> serde_json::Value {
    let process_memory = format_optional_bytes(status.process.memory_bytes);
    let process_virtual = format_optional_bytes(status.process.virtual_memory_bytes);
    let total_memory = format_bytes(status.system.total_memory_bytes);
    let used_memory = format_bytes(status.system.used_memory_bytes);
    let available_memory = format_bytes(status.system.available_memory_bytes);
    let total_swap = format_bytes(status.system.total_swap_bytes);
    let used_swap = format_bytes(status.system.used_swap_bytes);
    let process_cpu = format_optional_percent(status.process.cpu_percent);
    let global_cpu = format_percent(status.system.global_cpu_percent);
    let load = format!(
        "{:.2} / {:.2} / {:.2}",
        status.system.load_average.one,
        status.system.load_average.five,
        status.system.load_average.fifteen
    );
    json!({
        "status": status,
        "memory": {
            "process": process_memory,
            "process_virtual": process_virtual,
            "total": total_memory,
            "used": used_memory,
            "available": available_memory,
            "swap_total": total_swap,
            "swap_used": used_swap,
        },
        "cpu": {
            "process": process_cpu,
            "global": global_cpu,
        },
        "load": load,
    })
}

fn format_optional_bytes(value: Option<u64>) -> String {
    value
        .map(format_bytes)
        .unwrap_or_else(|| "not available".to_string())
}

fn format_bytes(value: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    let value = value as f64;
    if value >= GIB {
        format!("{:.2} GiB", value / GIB)
    } else if value >= MIB {
        format!("{:.2} MiB", value / MIB)
    } else if value >= KIB {
        format!("{:.2} KiB", value / KIB)
    } else {
        format!("{value:.0} B")
    }
}

fn format_optional_percent(value: Option<f32>) -> String {
    value
        .map(format_percent)
        .unwrap_or_else(|| "not available".to_string())
}

fn format_percent(value: f32) -> String {
    format!("{value:.1}%")
}

fn render(
    site: &Site,
    template: &str,
    context: serde_json::Value,
) -> Result<Html<String>, TemplateError> {
    site.template_engine().html(template, &context)
}

fn not_found(site: &Site) -> Response {
    error_page(
        site,
        StatusCode::NOT_FOUND,
        "Console page not found",
        "The requested console page or resource does not exist.",
    )
}

/// Converts a console template failure into the console's safe HTML error response.
fn render_or_error(site: &Site, rendered: Result<Html<String>, TemplateError>) -> Response {
    match rendered {
        Ok(page) => page.into_response(),
        Err(_) => internal_error(site),
    }
}

fn no_store(mut response: Response) -> Response {
    response.headers_mut().insert(
        axum::http::header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-store"),
    );
    response
}

/// Returns the generic console page used when an internal operation fails.
fn internal_error(site: &Site) -> Response {
    error_page(
        site,
        StatusCode::INTERNAL_SERVER_ERROR,
        "Internal Server Error",
        "The console could not complete this request.",
    )
}

fn error_page(site: &Site, status: StatusCode, title: &str, message: &str) -> Response {
    let Some(urls) = site.console_urls() else {
        return (status, title.to_string()).into_response();
    };
    let context = json!({
        "urls": {
            "home": &urls.home,
            "overview": &urls.overview,
            "runtime": &urls.runtime,
            "tasks": &urls.tasks,
            "schedules": &urls.schedules,
            "operations": &urls.operations,
            "conf": &urls.conf,
            "openapi": &urls.openapi,
            "api_openapi": &urls.api_openapi,
        },
        "assets": {
            "favicon": &urls.favicon_path,
            "logo": &urls.logo_path,
            "script": &urls.script_path,
        },
        "version": env!("CARGO_PKG_VERSION"),
        "stylesheet_path": &urls.stylesheet_path,
        "status": status.as_u16(),
        "title": title,
        "message": message,
    });
    match site.template_engine().html("console/error.html", &context) {
        Ok(body) => (status, body).into_response(),
        Err(_) => (status, title.to_string()).into_response(),
    }
}

fn render_tasks(
    site: &Site,
    query: TaskQuery,
    page: crate::routes::Page<TaskOut>,
) -> Result<Html<String>, TemplateError> {
    let conf = &site.conf().console;
    let task_per_page = task_per_page(&query, conf.page_size_default, conf.page_size_max);
    let task_items = page.items.iter().map(task_view).collect::<Vec<_>>();
    let selected_task = selected_task(&query, &task_items);
    let task_counts = task_counts(&task_items);
    let previous_page = (page.page > 1).then(|| page.page - 1);
    let next_page = (page.page < page.total_pages).then(|| page.page + 1);
    render_page(
        site,
        "console/tasks.html",
        "tasks",
        "Tasks",
        json!({
            "page": {
                "items": task_items,
                "page": page.page,
                "total": page.total,
                "total_pages": page.total_pages,
                "previous_page": previous_page,
                "next_page": next_page,
            },
            "query": query,
            "statuses": task_statuses(),
            "page_sizes": task_page_sizes(),
            "task_per_page": task_per_page,
            "selected_task": selected_task,
            "task_counts": task_counts,
        }),
    )
}

/// Builds the template context for the read-only durable schedule inspector.
fn render_schedules(
    site: &Site,
    query: ScheduleQuery,
    page: crate::console::schedules::SchedulePage,
) -> Result<Html<String>, TemplateError> {
    let previous_page = (page.page > 1).then(|| page.page - 1);
    let next_page = (page.page < page.total_pages).then(|| page.page + 1);
    let selected = selected_schedule(&query, &page.items);
    let items = page.items.iter().map(schedule_view).collect::<Vec<_>>();
    let selected = selected.as_ref().map(schedule_view);
    render_page(
        site,
        "console/schedules.html",
        "schedules",
        "Schedules",
        json!({
            "page": {
                "items": items,
                "tasks": page.tasks,
                "lanes": page.lanes,
                "total": page.total,
                "page": page.page,
                "total_pages": page.total_pages,
                "configured": page.configured,
                "cron": page.cron,
                "periodic": page.periodic,
                "awaiting_first_run": page.awaiting_first_run,
            },
            "query": query,
            "selected_schedule": selected,
            "previous_page": previous_page,
            "next_page": next_page,
            "page_sizes": task_page_sizes(),
        }),
    )
}

/// Adds compact presentation times while preserving the API's RFC 3339 values.
fn schedule_view(schedule: &ScheduleOut) -> serde_json::Value {
    let mut value = match serde_json::to_value(schedule) {
        Ok(value) => value,
        Err(_) => json!({}),
    };
    value["last_submitted_display"] = json!(
        schedule
            .last_submitted_at
            .as_ref()
            .map(|time| compact_time(time))
    );
    value["next_expected_display"] = json!(
        schedule
            .next_expected_at
            .as_ref()
            .map(|time| compact_time(time))
    );
    value
}

/// Finds the selected schedule only among the inspected page's safe values.
fn selected_schedule(query: &ScheduleQuery, schedules: &[ScheduleOut]) -> Option<ScheduleOut> {
    let name = query.selected.as_deref()?;
    schedules
        .iter()
        .find(|schedule| schedule.name == name)
        .cloned()
}

fn task_page_sizes() -> Vec<usize> {
    vec![25, 50, 100]
}

fn task_view(task: &TaskOut) -> serde_json::Value {
    let mut value = match serde_json::to_value(task) {
        Ok(value) => value,
        Err(_) => json!({}),
    };
    value["created_at_display"] = json!(compact_time(&task.created_at));
    value["updated_at_display"] = json!(compact_time(&task.updated_at));
    value["ready_at_display"] = json!(compact_optional(task.ready_at.as_deref()));
    value["completed_at_display"] = json!(compact_optional(task.completed_at.as_deref()));
    value
}

fn selected_task(query: &TaskQuery, tasks: &[serde_json::Value]) -> Option<serde_json::Value> {
    let id = query.selected.as_deref()?;
    tasks
        .iter()
        .find(|task| task.get("id").and_then(|value| value.as_str()) == Some(id))
        .cloned()
}

fn task_counts(tasks: &[serde_json::Value]) -> serde_json::Value {
    let active = count_tasks(tasks, "running");
    let queued = count_tasks(tasks, "pending");
    let completed = count_tasks(tasks, "succeeded");
    let failed = count_tasks(tasks, "failed");
    json!({
        "total": tasks.len(),
        "active": active,
        "queued": queued,
        "completed": completed,
        "failed": failed,
    })
}

fn count_tasks(tasks: &[serde_json::Value], status: &str) -> usize {
    tasks
        .iter()
        .filter(|task| task.get("status").and_then(|value| value.as_str()) == Some(status))
        .count()
}

fn compact_optional(value: Option<&str>) -> Option<String> {
    value.map(compact_time)
}

fn compact_time(value: &str) -> String {
    match chrono::DateTime::parse_from_rfc3339(value) {
        Ok(value) => value.format("%Y-%m-%d %H:%M:%S").to_string(),
        Err(_) => value.to_string(),
    }
}

fn selected_operation(
    site: &Site,
    query: &OperationQuery,
    console_bundle_id: Option<uuid::Uuid>,
) -> Option<OperationView> {
    let id = query.selected.as_deref()?;
    let id = id.parse::<crate::OperationId>().ok()?;
    operation_view(site, id, console_bundle_id)
}

fn console_bundle_id(site: &Site, operation_id: OperationId) -> Option<uuid::Uuid> {
    site.operations().find(operation_id)?.bundle_id
}

fn operation_view(
    site: &Site,
    id: OperationId,
    console_bundle_id: Option<uuid::Uuid>,
) -> Option<OperationView> {
    site.operations()
        .find(id)
        .filter(|operation| !is_console_operation(operation, console_bundle_id))
        .map(|operation| OperationView::from_operation(operation, site))
}

fn empty_tasks(per_page: usize) -> crate::routes::Page<TaskOut> {
    crate::routes::Page::new(Vec::new(), 0, 1, per_page)
}

fn operation_kinds() -> [&'static str; 9] {
    [
        "route", "command", "task", "service", "signal", "cron", "periodic", "pgnotify", "api_doc",
    ]
}

fn task_statuses() -> [&'static str; 5] {
    ["pending", "running", "suspended", "succeeded", "failed"]
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;

    use super::render_or_error;
    use crate::{Site, SiteConf, bundles, console::ConsoleConf, templates::TemplateError};

    /// Verifies failed console page rendering produces the safe console HTML error page.
    #[tokio::test]
    async fn render_failure_uses_safe_console_error_page() {
        let site = Site::build(
            SiteConf::default()
                .log_init(false)
                .console(ConsoleConf::default().enabled(true)),
            bundles::bundle([]),
        )
        .await
        .expect("console site should build");
        let response = render_or_error(&site, Err(TemplateError::NotFound("missing".into())));
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("error page body should be readable");
        let body = String::from_utf8_lossy(&body);

        assert_eq!(status, axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        assert!(body.contains("Internal Server Error"));
        assert!(body.contains("The console could not complete this request."));
        assert!(!body.contains("missing"));
    }
}
