use axum::{
    http::StatusCode,
    response::{Html, IntoResponse, Response},
};
use serde_json::json;

use crate::routes::{Path, Query};
use crate::{
    Site,
    console::{
        auth::ConsoleSessionUser,
        query::{
            OperationQuery, TaskQuery, filter_operations, is_console_operation, task_limit,
            task_limit_max,
        },
        schema_view::OperationView,
        status::StatusOut,
        types::{ConfigOut, OperationOut, Page, TaskDetailOut, TaskOut},
    },
    templates::TemplateError,
};

pub async fn login(site: Site) -> Result<Html<String>, TemplateError> {
    render(
        &site,
        "console/login.html",
        json!({
            "base_path": base_path(&site),
            "version": env!("CARGO_PKG_VERSION"),
            "stylesheet_path": crate::console::stylesheet_path(),
        }),
    )
}

pub async fn overview(
    site: Site,
    ConsoleSessionUser(_user): ConsoleSessionUser,
) -> Result<Html<String>, TemplateError> {
    let status = status_snapshot(&site);
    render_page(
        &site,
        "console/overview.html",
        "overview",
        "Overview",
        json!({ "status": status }),
    )
}

pub async fn runtime(
    site: Site,
    ConsoleSessionUser(_user): ConsoleSessionUser,
) -> Result<Html<String>, TemplateError> {
    let status = status_snapshot(&site);
    render_page(
        &site,
        "console/runtime.html",
        "runtime",
        "Runtime",
        runtime_context(status),
    )
}

pub async fn operations(
    site: Site,
    ConsoleSessionUser(_user): ConsoleSessionUser,
    Query(query): Query<OperationQuery>,
) -> Result<Html<String>, TemplateError> {
    let conf = &site.conf().console;
    let console_bundle_id = console_bundle_id(&site);
    let (items, next_cursor) = filter_operations(
        site.iter_operations(),
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
    )
}

pub async fn operation_detail(
    site: Site,
    ConsoleSessionUser(_user): ConsoleSessionUser,
    Path(id): Path<String>,
) -> Response {
    let Ok(id) = uuid::Uuid::parse_str(&id) else {
        return not_found(&site);
    };
    let Some(operation) = site
        .iter_operations()
        .find(|op| op.id == id)
        .map(|op| OperationOut::from_operation(op, &site))
    else {
        return not_found(&site);
    };
    render_page(
        &site,
        "console/operation_detail.html",
        "operations",
        "Operation",
        json!({ "operation": operation }),
    )
    .into_response()
}

pub async fn tasks(
    site: Site,
    ConsoleSessionUser(_user): ConsoleSessionUser,
    Query(query): Query<TaskQuery>,
) -> Result<Html<String>, StatusCode> {
    if !site.console_has_tasks() {
        return render_tasks(&site, query, empty_tasks())
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR);
    }

    let conf = &site.conf().console;
    let filter = query.to_filter(conf.page_size_default, task_limit_max(conf.page_size_max));
    let page = site
        .tasks()
        .list(filter)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let page = Page {
        items: page.records.iter().map(TaskOut::from).collect::<Vec<_>>(),
        next_cursor: page.next_cursor,
    };
    render_tasks(&site, query, page).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

pub async fn task_detail(
    site: Site,
    ConsoleSessionUser(_user): ConsoleSessionUser,
    Path(id): Path<String>,
) -> Response {
    let Ok(id) = uuid::Uuid::parse_str(&id) else {
        return not_found(&site);
    };
    let record = match site.tasks().get(id).await {
        Ok(Some(record)) => record,
        Ok(None) => return not_found(&site),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let detail = TaskDetailOut::from(&record);
    let payload = match serde_json::to_string_pretty(&detail) {
        Ok(payload) => payload,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    render_page(
        &site,
        "console/task_detail.html",
        "tasks",
        "Task",
        json!({ "task": detail, "payload": payload }),
    )
    .into_response()
}

pub async fn conf(
    site: Site,
    ConsoleSessionUser(_user): ConsoleSessionUser,
) -> Result<Html<String>, StatusCode> {
    let conf = ConfigOut::from_site(&site);
    let payload =
        serde_json::to_string_pretty(&conf).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    render_page(
        &site,
        "console/conf.html",
        "conf",
        "Config",
        json!({ "conf": conf, "payload": payload }),
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

pub async fn openapi(
    site: Site,
    ConsoleSessionUser(_user): ConsoleSessionUser,
) -> Result<Html<String>, StatusCode> {
    render_page(
        &site,
        "console/openapi.html",
        "openapi",
        "OpenAPI",
        json!({}),
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

pub async fn not_found_page(site: Site, ConsoleSessionUser(_user): ConsoleSessionUser) -> Response {
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
    context["base_path"] = json!(base_path(site));
    context["version"] = json!(env!("CARGO_PKG_VERSION"));
    context["stylesheet_path"] = json!(crate::console::stylesheet_path());
    render(site, template, context)
}

fn status_snapshot(site: &Site) -> StatusOut {
    let ttl = std::time::Duration::from_secs(site.conf().console.status_cache_ttl_seconds);
    site.console_runtime()
        .map(|runtime| runtime.status(site, ttl))
        .unwrap_or_else(|| crate::console::status::collect(site))
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

fn error_page(site: &Site, status: StatusCode, title: &str, message: &str) -> Response {
    let context = json!({
        "base_path": base_path(site),
        "version": env!("CARGO_PKG_VERSION"),
        "stylesheet_path": crate::console::stylesheet_path(),
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
    page: Page<TaskOut>,
) -> Result<Html<String>, TemplateError> {
    let conf = &site.conf().console;
    let task_limit = task_limit(&query, conf.page_size_default, conf.page_size_max);
    let task_items = page.items.iter().map(task_view).collect::<Vec<_>>();
    let selected_task = selected_task(&query, &task_items);
    let task_counts = task_counts(&task_items);
    render_page(
        site,
        "console/tasks.html",
        "tasks",
        "Tasks",
        json!({
            "page": {
                "items": task_items,
                "next_cursor": page.next_cursor,
            },
            "query": query,
            "statuses": task_statuses(),
            "page_sizes": task_page_sizes(),
            "task_limit": task_limit,
            "selected_task": selected_task,
            "task_counts": task_counts,
        }),
    )
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
    let id = uuid::Uuid::parse_str(id).ok()?;
    site.iter_operations()
        .find(|op| op.id == id && !is_console_operation(op, console_bundle_id))
        .map(|op| OperationView::from_operation(op, site))
}

fn console_bundle_id(site: &Site) -> Option<uuid::Uuid> {
    site.console_runtime().map(|runtime| runtime.bundle_id())
}

fn empty_tasks() -> Page<TaskOut> {
    Page {
        items: Vec::new(),
        next_cursor: None,
    }
}

fn base_path(site: &Site) -> &str {
    &site.conf().console.path
}

fn operation_kinds() -> [&'static str; 9] {
    [
        "route", "command", "task", "service", "signal", "cron", "periodic", "pgnotify", "api_doc",
    ]
}

fn task_statuses() -> [&'static str; 5] {
    ["pending", "running", "suspended", "succeeded", "failed"]
}
