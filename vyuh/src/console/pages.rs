use std::collections::BTreeSet;

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
        logs::{LogError, limit as log_limit},
        query::{
            InspectionQuery, LogQuery, TaskQuery, filter_inspections, is_console_operation,
            task_limit_max, task_per_page,
        },
        schedules::{ScheduleOut, ScheduleQuery, page as schedule_page},
        schema_view::OperationView,
        status::StatusOut,
        types::{CommandOut, ConfigOut, OperationOut, Page, ServiceOut, TaskDetailOut, TaskOut},
    },
    templates::TemplateError,
};

/// Renders the console runtime-inspection page or its safe internal-error page.
pub async fn runtime(site: Site, _guard: ConsoleGuard) -> Response {
    render_or_error(
        &site,
        render_page(
            &site,
            "console/runtime.html",
            "runtime",
            "System Info",
            runtime_context(status_snapshot(&site)),
        ),
    )
}

/// Renders application HTTP routes and their typed request metadata.
pub async fn routes(
    site: Site,
    operation_id: OperationId,
    _guard: ConsoleGuard,
    Query(query): Query<InspectionQuery>,
) -> Response {
    render_inspection(&site, operation_id, query, ROUTES_PAGE)
}

/// Renders application signal handlers and their typed payload contracts.
pub async fn signals(
    site: Site,
    operation_id: OperationId,
    _guard: ConsoleGuard,
    Query(query): Query<InspectionQuery>,
) -> Response {
    render_inspection(&site, operation_id, query, SIGNALS_PAGE)
}

/// Renders configured cron, periodic, and database-notify emitters.
pub async fn emitters(
    site: Site,
    operation_id: OperationId,
    _guard: ConsoleGuard,
    Query(query): Query<InspectionQuery>,
) -> Response {
    render_inspection(&site, operation_id, query, EMITTERS_PAGE)
}

/// Renders registered commands and their validated CLI arguments.
pub async fn commands(
    site: Site,
    _guard: ConsoleGuard,
    Query(query): Query<InspectionQuery>,
) -> Response {
    let page = command_page(&site, &query);
    let selected = selected_command(&page.items, &query);
    let summary = command_summary(&page.items);
    let list_url = inspection_page_url(
        site.console_urls().map(|urls| urls.commands.as_str()),
        &query,
        None,
    );
    let next_url = inspection_page_url(
        site.console_urls().map(|urls| urls.commands.as_str()),
        &query,
        page.next_cursor.as_deref(),
    );
    let selected_url = inspection_selected_url(
        site.console_urls().map(|urls| urls.commands.as_str()),
        &query,
    );
    render_or_error(
        &site,
        render_page(
            &site,
            "console/commands.html",
            "commands",
            "Commands",
            json!({ "page": page, "query": query, "selected_command": selected, "summary": summary,
                "list_url": list_url, "selected_url": selected_url, "next_url": next_url }),
        ),
    )
}

/// Renders configured services with live worker lifecycle state.
pub async fn services(
    site: Site,
    _guard: ConsoleGuard,
    Query(query): Query<InspectionQuery>,
) -> Response {
    let page = service_page(&site, &query);
    let selected = selected_service(&page.items, &query);
    let summary = service_summary(&page.items);
    let list_url = inspection_page_url(
        site.console_urls().map(|urls| urls.services.as_str()),
        &query,
        None,
    );
    let next_url = inspection_page_url(
        site.console_urls().map(|urls| urls.services.as_str()),
        &query,
        page.next_cursor.as_deref(),
    );
    let selected_url = inspection_selected_url(
        site.console_urls().map(|urls| urls.services.as_str()),
        &query,
    );
    render_or_error(
        &site,
        render_page(
            &site,
            "console/services.html",
            "services",
            "Services",
            json!({ "page": page, "query": query, "selected_service": selected, "summary": summary,
                "list_url": list_url, "selected_url": selected_url, "next_url": next_url }),
        ),
    )
}

/// Renders one focused operation category with the shared console inspector.
fn render_inspection(
    site: &Site,
    operation_id: OperationId,
    query: InspectionQuery,
    page: InspectionPage,
) -> Response {
    let Some(urls) = site.console_urls() else {
        return internal_error(site);
    };
    let list_url = inspection_url(urls, page.active);
    let bundle_id = console_bundle_id(site, operation_id);
    let operations = inspection_page(site, &query, bundle_id, page.matches_kind);
    let selected = selected_operation(site, &query, bundle_id, page.matches_kind);
    let summary = inspection_summary(&operations.items);
    let filters = inspection_filter_values(site, bundle_id, page.matches_kind);
    let page_url = inspection_page_url(Some(list_url), &query, None);
    let selected_url = inspection_selected_url(Some(list_url), &query);
    let next_url = inspection_page_url(Some(list_url), &query, operations.next_cursor.as_deref());
    render_or_error(
        site,
        render_page(
            site,
            "console/inspection.html",
            page.active,
            page.title,
            json!({ "page": operations, "query": query, "selected_operation": selected, "summary": summary,
                "filters": filters,
                "list_url": page_url, "selected_url": selected_url, "next_url": next_url,
                "section": { "title": page.title, "description": page.description,
                    "singular": page.singular, "url": list_url, "active": page.active } }),
        ),
    )
}

/// Collects exact metadata values so inspector filters are discoverable and valid.
fn inspection_filter_values(
    site: &Site,
    console_bundle_id: Option<uuid::Uuid>,
    matches_kind: fn(&crate::OperationKind) -> bool,
) -> serde_json::Value {
    let mut owners = BTreeSet::new();
    let mut tags = BTreeSet::new();
    for operation in site.operations().list().filter(|operation| {
        !is_console_operation(operation, console_bundle_id) && matches_kind(&operation.kind)
    }) {
        if let Some(owner) = &operation.owner {
            owners.insert(owner.clone());
        }
        tags.extend(operation.tags.iter().cloned());
    }
    json!({
        "owners": owners.into_iter().collect::<Vec<_>>(),
        "tags": tags.into_iter().collect::<Vec<_>>(),
    })
}

#[derive(Clone, Copy)]
struct InspectionPage {
    active: &'static str,
    title: &'static str,
    description: &'static str,
    singular: &'static str,
    matches_kind: fn(&crate::OperationKind) -> bool,
}

const ROUTES_PAGE: InspectionPage = InspectionPage {
    active: "routes",
    title: "Routes",
    description: "Registered HTTP endpoints and their request contracts",
    singular: "Route",
    matches_kind: is_route,
};

const SIGNALS_PAGE: InspectionPage = InspectionPage {
    active: "signals",
    title: "Signals",
    description: "In-process signal handlers and payload contracts",
    singular: "Signal",
    matches_kind: is_signal,
};

const EMITTERS_PAGE: InspectionPage = InspectionPage {
    active: "emitters",
    title: "Emitters",
    description: "Scheduled and external producers",
    singular: "Emitter",
    matches_kind: is_emitter,
};

fn inspection_url<'a>(urls: &'a crate::console::ViewUrls, active: &str) -> &'a str {
    match active {
        "routes" => &urls.routes,
        "signals" => &urls.signals,
        "emitters" => &urls.emitters,
        _ => &urls.routes,
    }
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
    let page = match site.tasks().list(filter).await {
        Ok(page) => page,
        Err(_) => return internal_error(&site),
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
    let (selected, selected_unavailable) = match runtime.selected(&query).await {
        Ok(value) => (value, false),
        Err(LogError::EntryUnavailable | LogError::InvalidCursor) => (None, true),
        Err(LogError::Unavailable | LogError::InvalidQuery) => return internal_error(&site),
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
            "selected_unavailable": selected_unavailable,
            "next_url": log_url(site.console_urls().map(|urls| urls.logs.as_str()), &query, page.next_cursor.as_deref()),
            "selected_url": selected_log_url(site.console_urls().map(|urls| urls.logs.as_str()), &query),
            "list_url": log_url(site.console_urls().map(|urls| urls.logs.as_str()), &query, query.cursor.as_deref()),
        }),
    );
    no_store(render_or_error(&site, rendered))
}

fn selected_log_url(base: Option<&str>, query: &LogQuery) -> Option<String> {
    let base = base?;
    Some(match log_url(Some(base), query, None) {
        Some(url) => format!("{url}&selected="),
        None => format!("{base}?selected="),
    })
}

/// Builds an inspection URL without discarding the active list location.
fn inspection_page_url(
    base: Option<&str>,
    query: &InspectionQuery,
    cursor: Option<&str>,
) -> Option<String> {
    let base = base?;
    let mut pairs = Vec::new();
    append_query_pair(&mut pairs, "q", query.q.as_deref());
    append_query_pair(&mut pairs, "tag", query.tag.as_deref());
    append_query_pair(&mut pairs, "owner", query.owner.as_deref());
    if let Some(hidden) = query.hidden {
        pairs.push(("hidden", hidden.to_string()));
    }
    if let Some(limit) = query.limit {
        pairs.push(("limit", limit.to_string()));
    }
    append_query_pair(&mut pairs, "cursor", cursor.or(query.cursor.as_deref()));
    Some(query_url(base, pairs))
}

/// Returns the active inspection location with an unfinished selected value.
fn inspection_selected_url(base: Option<&str>, query: &InspectionQuery) -> Option<String> {
    let url = inspection_page_url(base, query, query.cursor.as_deref())?;
    Some(selected_url_prefix(&url))
}

/// Encodes an application-owned set of query values into one relative console URL.
fn query_url(base: &str, pairs: Vec<(&str, String)>) -> String {
    let mut serializer = form_urlencoded::Serializer::new(String::new());
    for (key, value) in pairs {
        serializer.append_pair(key, &value);
    }
    let encoded = serializer.finish();
    if encoded.is_empty() {
        base.to_string()
    } else {
        format!("{base}?{encoded}")
    }
}

/// Adds one optional string value to a stable query sequence.
fn append_query_pair(pairs: &mut Vec<(&str, String)>, key: &'static str, value: Option<&str>) {
    if let Some(value) = value {
        pairs.push((key, value.to_string()));
    }
}

/// Appends a selected-record parameter without losing prior filters or location.
fn selected_url_prefix(url: &str) -> String {
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}selected=")
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
    append_log_pair(&mut pairs, "cursor", cursor.or(query.cursor.as_deref()));
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
            "Configurations",
            json!({ "conf": conf, "payload": payload }),
        ),
    )
}

/// Renders read-only migration status and composed schema entities for the site.
pub async fn migrations(site: Site, _guard: ConsoleGuard) -> Response {
    #[cfg(feature = "migrations")]
    {
        let context = migration_context(&site).await;
        return render_or_error(
            &site,
            render_page(
                &site,
                "console/migrations.html",
                "migrations",
                "Migrations",
                context,
            ),
        );
    }

    #[cfg(not(feature = "migrations"))]
    render_or_error(
        &site,
        render_page(
            &site,
            "console/migrations.html",
            "migrations",
            "Migrations",
            json!({ "available": false, "migrations": [], "entities": [],
                "summary": { "applied": 0, "pending": 0, "sources": 0, "entities": 0 } }),
        ),
    )
}

#[cfg(feature = "migrations")]
/// Builds a safe migration dashboard from the serialized Mool runner and schema registry.
async fn migration_context(site: &Site) -> serde_json::Value {
    let entities = migration_entities(site);
    let sources = migration_sources(site);
    match migration_status_rows(site).await {
        Ok(migrations) => migration_context_value(migrations, entities, sources, true),
        Err(()) => migration_context_value(Vec::new(), entities, sources, false),
    }
}

#[cfg(feature = "migrations")]
/// Reads migration state without applying, generating, or repairing migrations.
async fn migration_status_rows(site: &Site) -> Result<Vec<serde_json::Value>, ()> {
    use crate::db::engine::{CommandResult, MigrationCommand};

    let runner = site.migration_runner().ok_or(())?;
    let mut runner = runner.lock().await;
    let result = runner
        .run_command(&MigrationCommand::Status {
            reverse: false,
            search: None,
        })
        .await
        .map_err(|_| ())?;
    let CommandResult::Status(statuses) = result else {
        return Err(());
    };
    Ok(statuses
        .into_iter()
        .map(|status| {
            let id = status.id;
            let source = migration_source(&id).to_string();
            json!({
                "id": id,
                "source": source,
                "applied": status.applied,
            })
        })
        .collect())
}

#[cfg(feature = "migrations")]
/// Lists source labels registered in the composed migration graph.
fn migration_sources(site: &Site) -> Vec<String> {
    let registry = site.migration_registry();
    let mut sources = registry
        .root()
        .map(|_| "Application".to_string())
        .into_iter()
        .collect::<Vec<_>>();
    sources.extend(
        registry
            .crates()
            .map(|(namespace, _)| namespace.to_string()),
    );
    sources
}

#[cfg(feature = "migrations")]
/// Lists desired schema entities for each registered migration source.
fn migration_entities(site: &Site) -> Vec<serde_json::Value> {
    let registry = site.migration_registry();
    let mut entities = Vec::new();
    if registry.root().is_some() {
        append_schema_entities(&mut entities, "Application", registry.schema_for(None));
    }
    for (namespace, _) in registry.crates() {
        append_schema_entities(
            &mut entities,
            namespace,
            registry.schema_for(Some(namespace)),
        );
    }
    entities.sort_by(|left, right| {
        left["source"]
            .as_str()
            .cmp(&right["source"].as_str())
            .then_with(|| left["name"].as_str().cmp(&right["name"].as_str()))
    });
    entities
}

#[cfg(feature = "migrations")]
/// Adds one source schema's tables, views, functions, and enums when it builds successfully.
fn append_schema_entities(
    entities: &mut Vec<serde_json::Value>,
    source: &str,
    schema: Result<crate::db::Schema, crate::db::MigrationError>,
) {
    let Ok(schema) = schema else {
        return;
    };
    for (name, table) in schema.tables {
        entities.push(json!({
            "name": name,
            "source": source,
            "kind": "Table",
            "detail": format!("{} columns", table.columns.len()),
        }));
    }
    for name in schema.views.into_keys() {
        entities.push(
            json!({ "name": name, "source": source, "kind": "View", "detail": "Database view" }),
        );
    }
    for name in schema.functions.into_keys() {
        entities.push(json!({ "name": name, "source": source, "kind": "Function", "detail": "Database function" }));
    }
    for name in schema.enums.into_keys() {
        entities.push(
            json!({ "name": name, "source": source, "kind": "Enum", "detail": "Database enum" }),
        );
    }
}

#[cfg(feature = "migrations")]
/// Produces stable dashboard counters without presenting unavailable tracking data as empty data.
fn migration_context_value(
    migrations: Vec<serde_json::Value>,
    entities: Vec<serde_json::Value>,
    sources: Vec<String>,
    tracking_available: bool,
) -> serde_json::Value {
    let applied = migrations
        .iter()
        .filter(|migration| migration["applied"].as_bool() == Some(true))
        .count();
    let pending = migrations.len().saturating_sub(applied);
    let source_count = sources.len();
    let entity_count = entities.len();
    json!({
        "available": true,
        "tracking_available": tracking_available,
        "migrations": migrations,
        "entities": entities,
        "sources": sources,
        "summary": {
            "applied": applied,
            "pending": pending,
            "sources": source_count,
            "entities": entity_count,
        },
    })
}

#[cfg(feature = "migrations")]
/// Identifies the application source or crate namespace encoded in a migration ID.
fn migration_source(id: &str) -> &str {
    id.split_once('/')
        .map_or("Application", |(source, _)| source)
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
    let default_refresh_url = match active {
        "runtime" => &urls.runtime,
        "routes" => &urls.routes,
        "commands" => &urls.commands,
        "services" => &urls.services,
        "emitters" => &urls.emitters,
        "signals" => &urls.signals,
        "tasks" => &urls.tasks,
        "schedules" => &urls.schedules,
        "logs" => &urls.logs,
        "conf" => &urls.conf,
        "migrations" => &urls.migrations,
        "openapi" => &urls.openapi,
        _ => &urls.home,
    };
    let refresh_url = context
        .get("list_url")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| default_refresh_url.to_owned());
    context["subtitle"] = json!(page_subtitle(active));
    context["refresh_url"] = json!(refresh_url);
    context["page_action"] = if active == "openapi" {
        json!({ "label": "JSON", "url": &urls.api_openapi })
    } else {
        serde_json::Value::Null
    };
    context["urls"] = json!({
        "home": &urls.home,
        "runtime": &urls.runtime,
        "routes": &urls.routes,
        "commands": &urls.commands,
        "services": &urls.services,
        "emitters": &urls.emitters,
        "signals": &urls.signals,
        "tasks": &urls.tasks,
        "schedules": &urls.schedules,
        "logs": &urls.logs,
        "conf": &urls.conf,
        "migrations": &urls.migrations,
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
    context["migrations_available"] = json!(cfg!(feature = "migrations"));
    context["health"] = console_health(site);
    render(site, template, context)
}

/// Returns the concise, page-specific description shown in the shared console chrome.
fn page_subtitle(active: &str) -> &'static str {
    match active {
        "runtime" => "Cached snapshot of the running system",
        "routes" => "Registered HTTP endpoints and their request contracts",
        "commands" => "Registered CLI commands and their validated arguments",
        "services" => "Built services, exposed interfaces, and worker lifecycle state",
        "emitters" => "Scheduled and external producers",
        "signals" => "In-process signal handlers and payload contracts",
        "tasks" => "Background jobs and their current status",
        "schedules" => "Durable cron and periodic task submissions",
        "logs" => "Bounded newest-first view of configured JSON file logs",
        "conf" => "Complete redacted SiteConf for this Vyuh application",
        "migrations" => "Read-only migration state and composed database entities",
        "openapi" => "Application route documentation inside the Vyuh console",
        _ => "Operational visibility for the running application",
    }
}

/// Provides one truthful, cached runtime-health indicator for the console chrome.
fn console_health(site: &Site) -> serde_json::Value {
    let status = status_snapshot(site);
    let ready = status.tasks.ready;
    let label = if ready {
        "Runtime ready"
    } else {
        "Task runtime needs attention"
    };
    json!({ "ready": ready, "label": label })
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
            "runtime": &urls.runtime,
            "routes": &urls.routes,
            "commands": &urls.commands,
            "services": &urls.services,
            "emitters": &urls.emitters,
            "signals": &urls.signals,
            "tasks": &urls.tasks,
            "schedules": &urls.schedules,
            "logs": &urls.logs,
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
    let task_url = site.console_urls().map(|urls| urls.tasks.as_str());
    let list_url = task_page_url(task_url, &query, query.page, None);
    let selected_url = task_selected_url(task_url, &query);
    let previous_url =
        previous_page.and_then(|page| task_page_url(task_url, &query, Some(page), None));
    let next_url = next_page.and_then(|page| task_page_url(task_url, &query, Some(page), None));
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
            "list_url": list_url,
            "selected_url": selected_url,
            "previous_url": previous_url,
            "next_url": next_url,
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
    let schedule_url = site.console_urls().map(|urls| urls.schedules.as_str());
    let list_url = schedule_page_url(schedule_url, &query, query.page, None);
    let selected_url = schedule_selected_url(schedule_url, &query);
    let previous_url =
        previous_page.and_then(|page| schedule_page_url(schedule_url, &query, Some(page), None));
    let next_url =
        next_page.and_then(|page| schedule_page_url(schedule_url, &query, Some(page), None));
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
            "list_url": list_url,
            "selected_url": selected_url,
            "previous_url": previous_url,
            "next_url": next_url,
        }),
    )
}

/// Builds a task-list URL while retaining every active filter and page size.
fn task_page_url(
    base: Option<&str>,
    query: &TaskQuery,
    page: Option<usize>,
    selected: Option<&str>,
) -> Option<String> {
    let base = base?;
    let mut pairs = Vec::new();
    append_query_pair(
        &mut pairs,
        "status",
        query.status.map(crate::tasks::TaskStatus::as_str),
    );
    append_query_pair(&mut pairs, "name", query.name.as_deref());
    append_query_pair(&mut pairs, "lane", query.lane.as_deref());
    append_query_pair(
        &mut pairs,
        "idempotency_key",
        query.idempotency_key.as_deref(),
    );
    append_query_pair(&mut pairs, "created_from", query.created_from.as_deref());
    append_query_pair(&mut pairs, "created_to", query.created_to.as_deref());
    append_query_pair(&mut pairs, "q", query.q.as_deref());
    if let Some(page) = page.or(query.page) {
        pairs.push(("page", page.to_string()));
    }
    if let Some(per_page) = query.per_page {
        pairs.push(("per_page", per_page.to_string()));
    }
    append_query_pair(&mut pairs, "selected", selected);
    Some(query_url(base, pairs))
}

/// Returns a task URL ready to receive a selected task ID.
fn task_selected_url(base: Option<&str>, query: &TaskQuery) -> Option<String> {
    let url = task_page_url(base, query, query.page, None)?;
    Some(selected_url_prefix(&url))
}

/// Builds a schedule-list URL while retaining the active filter location.
fn schedule_page_url(
    base: Option<&str>,
    query: &ScheduleQuery,
    page: Option<usize>,
    selected: Option<&str>,
) -> Option<String> {
    let base = base?;
    let mut pairs = Vec::new();
    append_query_pair(&mut pairs, "source", query.source.as_deref());
    append_query_pair(&mut pairs, "task", query.task.as_deref());
    append_query_pair(&mut pairs, "lane", query.lane.as_deref());
    append_query_pair(&mut pairs, "q", query.q.as_deref());
    if let Some(awaiting) = query.awaiting_first_run {
        pairs.push(("awaiting_first_run", awaiting.to_string()));
    }
    if let Some(page) = page.or(query.page) {
        pairs.push(("page", page.to_string()));
    }
    if let Some(per_page) = query.per_page {
        pairs.push(("per_page", per_page.to_string()));
    }
    append_query_pair(&mut pairs, "selected", selected);
    Some(query_url(base, pairs))
}

/// Returns a schedule URL ready to receive a selected schedule name.
fn schedule_selected_url(base: Option<&str>, query: &ScheduleQuery) -> Option<String> {
    let url = schedule_page_url(base, query, query.page, None)?;
    Some(selected_url_prefix(&url))
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

fn inspection_summary(items: &[OperationOut]) -> serde_json::Value {
    let documented = items
        .iter()
        .filter(|item| item.summary.is_some() || item.description.is_some())
        .count();
    let inputs = items.iter().map(|item| item.args.len()).sum::<usize>();
    let outputs = items.iter().map(|item| item.returns.len()).sum::<usize>();
    json!({ "total": items.len(), "documented": documented, "inputs": inputs, "outputs": outputs })
}

fn command_summary(items: &[CommandOut]) -> serde_json::Value {
    let documented = items.iter().filter(|item| item.summary.is_some()).count();
    let arguments = items.iter().map(|item| item.args.len()).sum::<usize>();
    let argument_free = items.iter().filter(|item| item.args.is_empty()).count();
    json!({ "total": items.len(), "documented": documented, "arguments": arguments, "argument_free": argument_free })
}

fn service_summary(items: &[ServiceOut]) -> serde_json::Value {
    let running = items.iter().filter(|item| item.status == "running").count();
    let workers = items.iter().map(|item| item.workers.len()).sum::<usize>();
    let interfaces = items.iter().map(|item| item.facades.len()).sum::<usize>();
    json!({ "total": items.len(), "running": running, "workers": workers, "interfaces": interfaces })
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
    query: &InspectionQuery,
    console_bundle_id: Option<uuid::Uuid>,
    matches_kind: fn(&crate::OperationKind) -> bool,
) -> Option<OperationView> {
    let id = query.selected.as_deref()?;
    let id = id.parse::<crate::OperationId>().ok()?;
    operation_view(site, id, console_bundle_id, matches_kind)
}

fn console_bundle_id(site: &Site, operation_id: OperationId) -> Option<uuid::Uuid> {
    site.operations().find(operation_id)?.bundle_id
}

fn operation_view(
    site: &Site,
    id: OperationId,
    console_bundle_id: Option<uuid::Uuid>,
    matches_kind: fn(&crate::OperationKind) -> bool,
) -> Option<OperationView> {
    site.operations()
        .find(id)
        .filter(|operation| {
            !is_console_operation(operation, console_bundle_id) && matches_kind(&operation.kind)
        })
        .map(|operation| OperationView::from_operation(operation, site))
}

fn empty_tasks(per_page: usize) -> crate::routes::Page<TaskOut> {
    crate::routes::Page::new(Vec::new(), 0, 1, per_page)
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

pub(super) fn inspection_page(
    site: &Site,
    query: &InspectionQuery,
    bundle_id: Option<uuid::Uuid>,
    matches_kind: fn(&crate::OperationKind) -> bool,
) -> Page<OperationOut> {
    let conf = &site.conf().console;
    let (items, next_cursor) = filter_inspections(
        site.operations().list(),
        query,
        bundle_id,
        conf.page_size_default,
        conf.page_size_max,
        matches_kind,
    );
    Page {
        items: items
            .into_iter()
            .map(|operation| OperationOut::from_operation(operation, site))
            .collect(),
        next_cursor,
    }
}

pub(super) fn command_page(site: &Site, query: &InspectionQuery) -> Page<CommandOut> {
    let query_text = query.q.as_deref().map(str::to_lowercase);
    let mut items = site
        .console_command_infos()
        .iter()
        .map(CommandOut::from)
        .filter(|command| {
            query_text.as_ref().is_none_or(|text| {
                contains_text(&command.name, text)
                    || command
                        .summary
                        .as_deref()
                        .is_some_and(|summary| contains_text(summary, text))
            })
        })
        .collect::<Vec<_>>();
    items.sort_by(|left, right| left.name.cmp(&right.name));
    let conf = &site.conf().console;
    page_items(items, query, conf.page_size_default, conf.page_size_max)
}

pub(super) fn service_page(site: &Site, query: &InspectionQuery) -> Page<ServiceOut> {
    let query_text = query.q.as_deref().map(str::to_lowercase);
    let mut items = site
        .console_service_infos()
        .iter()
        .map(ServiceOut::from)
        .filter(|service| {
            query_text
                .as_ref()
                .is_none_or(|text| contains_text(&service.type_name, text))
        })
        .collect::<Vec<_>>();
    items.sort_by(|left, right| left.type_name.cmp(&right.type_name));
    let conf = &site.conf().console;
    page_items(items, query, conf.page_size_default, conf.page_size_max)
}

fn page_items<T>(
    items: Vec<T>,
    query: &InspectionQuery,
    default_limit: usize,
    max_limit: usize,
) -> Page<T> {
    let start = crate::console::query::parse_cursor(query.cursor.as_deref());
    let limit = crate::console::query::clamp_limit(query.limit, default_limit, max_limit);
    let mut values = items
        .into_iter()
        .skip(start)
        .take(limit + 1)
        .collect::<Vec<_>>();
    let next_cursor = (values.len() > limit).then(|| (start + limit).to_string());
    values.truncate(limit);
    Page {
        items: values,
        next_cursor,
    }
}

fn selected_command(items: &[CommandOut], query: &InspectionQuery) -> Option<CommandOut> {
    let selected = query.selected.as_deref()?;
    items.iter().find(|item| item.name == selected).cloned()
}

fn selected_service(items: &[ServiceOut], query: &InspectionQuery) -> Option<ServiceOut> {
    let selected = query.selected.as_deref()?;
    items
        .iter()
        .find(|item| item.type_name == selected)
        .cloned()
}

fn contains_text(value: &str, query: &str) -> bool {
    value.to_lowercase().contains(query)
}

fn task_statuses() -> [&'static str; 5] {
    ["pending", "running", "suspended", "succeeded", "failed"]
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;

    use super::{
        inspection_selected_url, render_or_error, schedule_selected_url, task_selected_url,
    };
    use crate::{
        Site, SiteConf, bundles,
        console::{ConsoleConf, query::TaskQuery, schedules::ScheduleQuery},
        templates::TemplateError,
    };

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

    /// Verifies selecting an operation retains the active filter and cursor location.
    #[test]
    fn inspection_selection_url_preserves_list_state() {
        let query = crate::console::query::InspectionQuery {
            q: Some("orders api".into()),
            selected: None,
            tag: Some("billing".into()),
            owner: Some("orders".into()),
            hidden: Some(false),
            limit: Some(25),
            cursor: Some("50".into()),
        };
        assert_eq!(
            inspection_selected_url(Some("/console/routes"), &query).as_deref(),
            Some(
                "/console/routes?q=orders+api&tag=billing&owner=orders&hidden=false&limit=25&cursor=50&selected="
            )
        );
    }

    /// Verifies task and schedule selection URLs retain filters, page size, and page location.
    #[test]
    fn task_and_schedule_selection_urls_preserve_list_state() {
        let task_query = TaskQuery {
            status: None,
            name: Some("send_email".into()),
            lane: Some("background".into()),
            idempotency_key: Some("invoice-42".into()),
            created_from: Some("2026-08-01".into()),
            created_to: Some("2026-08-10".into()),
            selected: None,
            q: Some("failed".into()),
            page: Some(3),
            per_page: Some(100),
        };
        let schedule_query = ScheduleQuery {
            source: Some("cron".into()),
            task: Some("send_email".into()),
            lane: Some("background".into()),
            q: Some("receipt".into()),
            awaiting_first_run: Some(false),
            selected: None,
            page: Some(2),
            per_page: Some(50),
        };
        assert!(
            task_selected_url(Some("/console/tasks"), &task_query)
                .is_some_and(|url| url.contains("page=3&per_page=100&selected="))
        );
        assert!(
            schedule_selected_url(Some("/console/schedules"), &schedule_query)
                .is_some_and(|url| url.contains("page=2&per_page=50&selected="))
        );
    }
}
