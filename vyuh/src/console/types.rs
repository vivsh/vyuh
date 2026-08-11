use schemars::JsonSchema;
use serde::Serialize;

use crate::{
    Operation, OperationKind, Site,
    callables::{ArgPart, ArgSpec, ReturnPart, ReturnSpec, TypeSchema},
    console::middleware::{MiddlewareInfo, operation_middleware},
    logging::LogSink,
    tasks::{TaskInfo, TaskStatus},
};

#[derive(Debug, Serialize, JsonSchema)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct SessionOut {
    pub subject: String,
    pub scopes: Vec<String>,
}

/// Read-only console metadata for one registered command.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct CommandOut {
    pub name: String,
    pub summary: Option<String>,
    pub args: Vec<CommandArgOut>,
}

/// One command argument rendered by the console inspector.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct CommandArgOut {
    pub name: String,
    pub type_name: String,
    pub required: bool,
    pub description: Option<String>,
    pub hints: Vec<String>,
}

/// Read-only runtime metadata for one configured service.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct ServiceOut {
    pub type_name: String,
    pub status: String,
    pub facades: Vec<String>,
    pub workers: Vec<ServiceWorkerOut>,
}

/// One service worker and its current in-process lifecycle state.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct ServiceWorkerOut {
    pub name: String,
    pub status: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct AuthorizationOut {
    pub mode: &'static str,
    pub scopes: Vec<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct OperationOut {
    pub id: String,
    pub name: String,
    pub kind: OperationKind,
    pub summary: Option<String>,
    pub description: Option<String>,
    pub path: String,
    pub methods: Vec<&'static str>,
    pub tags: Vec<String>,
    pub owner: Option<String>,
    pub hidden: bool,
    pub authorization: Option<AuthorizationOut>,
    pub conf: Option<serde_json::Value>,
    pub args: Vec<SchemaItem>,
    pub middleware: Vec<MiddlewareOut>,
    pub returns: Vec<SchemaItem>,
}

impl OperationOut {
    pub(crate) fn from_operation(op: &Operation, site: &Site) -> Self {
        Self {
            id: op.id.to_string(),
            name: op.name.clone(),
            kind: op.kind.clone(),
            summary: op.summary.clone(),
            description: op.description.clone(),
            path: op.path.clone(),
            methods: op.http_methods(),
            tags: op.tags.iter().map(|tag| tag.to_string()).collect(),
            owner: op.owner.clone(),
            hidden: op.hidden,
            authorization: op
                .scope_requirement()
                .map(AuthorizationOut::from_requirement),
            conf: op.conf.clone(),
            args: op.args.iter().map(SchemaItem::from_arg).collect(),
            middleware: operation_middleware(site, op)
                .iter()
                .map(MiddlewareOut::from_info)
                .collect(),
            returns: op.returns.iter().map(SchemaItem::from_return).collect(),
        }
    }
}

impl From<&crate::commands::CommandInfo> for CommandOut {
    fn from(command: &crate::commands::CommandInfo) -> Self {
        Self {
            name: command.name.clone(),
            summary: command.summary.clone(),
            args: command.args.iter().map(CommandArgOut::from).collect(),
        }
    }
}

impl From<&crate::commands::CommandArgInfo> for CommandArgOut {
    fn from(arg: &crate::commands::CommandArgInfo) -> Self {
        Self {
            name: arg.name.clone(),
            type_name: arg.type_name.to_string(),
            required: arg.required,
            description: arg.description.clone(),
            hints: arg.hints.clone(),
        }
    }
}

impl From<&crate::services::ServiceInfo> for ServiceOut {
    fn from(service: &crate::services::ServiceInfo) -> Self {
        Self {
            type_name: service.type_name.to_string(),
            status: service.status.as_str().to_owned(),
            facades: service.facades.iter().map(ToString::to_string).collect(),
            workers: service.workers.iter().map(ServiceWorkerOut::from).collect(),
        }
    }
}

impl From<&crate::services::ServiceWorkerInfo> for ServiceWorkerOut {
    fn from(worker: &crate::services::ServiceWorkerInfo) -> Self {
        Self {
            name: worker.name.clone(),
            status: worker.status.as_str().to_owned(),
        }
    }
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct MiddlewareOut {
    pub name: String,
    pub scope: String,
    pub description: Option<String>,
    pub request_parts: Vec<SchemaItem>,
    pub settings: Vec<SettingOut>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct SettingOut {
    pub key: String,
    pub value: String,
}

impl MiddlewareOut {
    fn from_info(info: &MiddlewareInfo) -> Self {
        Self {
            name: info.name.clone(),
            scope: info.scope.to_string(),
            description: info.description.clone(),
            request_parts: info
                .request_parts
                .iter()
                .map(|part| SchemaItem::from_part(&part.name, part.description.clone(), &part.part))
                .collect(),
            settings: info.settings.iter().map(SettingOut::from_setting).collect(),
        }
    }
}

impl SettingOut {
    fn from_setting(setting: &crate::console::middleware::MiddlewareSetting) -> Self {
        Self {
            key: setting.key.clone(),
            value: setting.value.clone(),
        }
    }
}

impl AuthorizationOut {
    fn from_requirement(requirement: crate::callables::specs::ScopeRequirement<'_>) -> Self {
        Self {
            mode: if requirement.all { "all" } else { "any" },
            scopes: requirement.scopes.iter().map(ToString::to_string).collect(),
        }
    }
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct SchemaItem {
    pub name: String,
    pub location: String,
    pub description: Option<String>,
    pub status_code: Option<u16>,
    pub content_type: Option<String>,
    pub schema: Option<String>,
}

impl SchemaItem {
    fn from_arg(arg: &ArgSpec) -> Self {
        let (location, schema, content_type) = arg_item(&arg.part);
        Self {
            name: arg.name.clone(),
            location,
            description: arg.description.clone(),
            status_code: None,
            content_type,
            schema,
        }
    }

    fn from_part(name: &str, description: Option<String>, part: &ArgPart) -> Self {
        let (location, schema, content_type) = arg_item(part);
        Self {
            name: name.to_string(),
            location,
            description,
            status_code: None,
            content_type,
            schema,
        }
    }

    fn from_return(ret: &ReturnSpec) -> Self {
        let (location, schema, content_type) = return_part(&ret.part);
        Self {
            name: "response".to_string(),
            location,
            description: ret.description.clone(),
            status_code: ret.status_code,
            content_type,
            schema,
        }
    }
}

fn arg_item(part: &ArgPart) -> (String, Option<String>, Option<String>) {
    let flat = first_arg_part(part);
    let (location, schema, content_type) = arg_part(flat.part);
    (location_label(location, flat), schema, content_type)
}

fn arg_part(part: &ArgPart) -> (String, Option<String>, Option<String>) {
    match part {
        ArgPart::Header(schema) => ("header".into(), schema_json(schema), None),
        ArgPart::Cookie(schema) => ("cookie".into(), schema_json(schema), None),
        ArgPart::Query(schema) => ("query".into(), schema_json(schema), None),
        ArgPart::Path(schema) => ("path".into(), schema_json(schema), None),
        ArgPart::Body(schema, content_type) => (
            "body".into(),
            schema_json(schema),
            Some(content_type.to_string()),
        ),
        ArgPart::BodyWith {
            schema,
            content_type,
            ..
        } => (
            "body".into(),
            schema_json(schema),
            Some(content_type.to_string()),
        ),
        ArgPart::Security { scheme, .. } => (format!("security: {scheme}"), None, None),
        ArgPart::Authorization { .. } => ("authorization".to_string(), None, None),
        ArgPart::Response(_) => ("response".into(), None, None),
        ArgPart::Composite(_) => ("composite".into(), None, None),
        ArgPart::Optional(_) => ("optional".into(), None, None),
        ArgPart::Fallible(_) => ("fallible".into(), None, None),
        ArgPart::Zone => ("zone".into(), None, None),
        ArgPart::Ignore | ArgPart::Authentication => ("runtime".into(), None, None),
        #[cfg(feature = "mcp")]
        ArgPart::RawRequest => ("raw request".into(), None, None),
    }
}

#[derive(Clone, Copy)]
struct FlatArgPart<'a> {
    part: &'a ArgPart,
    optional: bool,
    fallible: bool,
}

fn first_arg_part(part: &ArgPart) -> FlatArgPart<'_> {
    let mut output = None;
    push_first_arg_part(part, false, false, &mut output);
    output.unwrap_or(FlatArgPart {
        part,
        optional: false,
        fallible: false,
    })
}

fn push_first_arg_part<'a>(
    part: &'a ArgPart,
    optional: bool,
    fallible: bool,
    output: &mut Option<FlatArgPart<'a>>,
) {
    if output.is_some() {
        return;
    }
    match part {
        ArgPart::Composite(parts) => {
            for nested in parts {
                push_first_arg_part(nested, optional, fallible, output);
            }
        }
        ArgPart::Optional(nested) => push_first_arg_part(nested, true, fallible, output),
        ArgPart::Fallible(nested) => push_first_arg_part(nested, optional, true, output),
        ArgPart::Response(_) => {}
        other => {
            *output = Some(FlatArgPart {
                part: other,
                optional,
                fallible,
            });
        }
    }
}

fn location_label(location: String, part: FlatArgPart<'_>) -> String {
    match (part.optional, part.fallible) {
        (true, true) => format!("optional fallible {location}"),
        (true, false) => format!("optional {location}"),
        (false, true) => format!("fallible {location}"),
        (false, false) => location,
    }
}

fn return_part(part: &ReturnPart) -> (String, Option<String>, Option<String>) {
    match part {
        ReturnPart::Header(schema) => ("header".into(), schema_json(schema), None),
        ReturnPart::Body(schema, content_type) => (
            "body".into(),
            schema_json(schema),
            Some(content_type.to_string()),
        ),
        ReturnPart::Created(schema, content_type) => (
            "created".into(),
            schema_json(schema),
            Some(content_type.to_string()),
        ),
        ReturnPart::Accepted(schema, content_type) => (
            "accepted".into(),
            schema_json(schema),
            Some(content_type.to_string()),
        ),
        ReturnPart::Empty => ("empty".into(), None, None),
        ReturnPart::Redirect { status_code } => (
            format!("redirect {status_code}"),
            None,
            Some("Location".into()),
        ),
        ReturnPart::Binary(content_type) => ("binary".into(), None, Some(content_type.to_string())),
        ReturnPart::Unknown => ("unknown".into(), None, None),
    }
}

fn schema_json(schema: &TypeSchema) -> Option<String> {
    serde_json::to_string_pretty(schema).ok()
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct TaskOut {
    pub id: String,
    pub name: String,
    pub status: TaskStatus,
    pub attempts: i32,
    pub lane: String,
    pub idempotency_key: Option<String>,
    pub idempotency_expires_at: Option<String>,
    pub last_error: Option<String>,
    pub locked_by: Option<String>,
    pub leased_until: Option<String>,
    pub ready_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub completed_at: Option<String>,
}

impl From<&TaskInfo> for TaskOut {
    fn from(record: &TaskInfo) -> Self {
        Self {
            id: record.id.to_string(),
            name: record.name.clone(),
            status: record.status,
            attempts: record.attempts,
            lane: record.lane.clone(),
            idempotency_key: record.idempotency_key.clone(),
            idempotency_expires_at: record
                .idempotency_expires_at
                .map(|value| value.to_rfc3339()),
            last_error: record.last_error.clone(),
            locked_by: record.locked_by.clone(),
            leased_until: record.leased_until.map(|value| value.to_rfc3339()),
            ready_at: record.ready_at.map(|value| value.to_rfc3339()),
            created_at: record.created_at.to_rfc3339(),
            updated_at: record.updated_at.to_rfc3339(),
            completed_at: record.completed_at.map(|value| value.to_rfc3339()),
        }
    }
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct TaskDetailOut {
    #[serde(flatten)]
    pub task: TaskOut,
    pub input: Option<serde_json::Value>,
    pub state: Option<serde_json::Value>,
    pub resume_input: Option<serde_json::Value>,
}

impl From<&TaskInfo> for TaskDetailOut {
    fn from(record: &TaskInfo) -> Self {
        Self {
            task: TaskOut::from(record),
            input: parse_json(&record.input),
            state: record.state.as_deref().and_then(parse_json),
            resume_input: record.resume_input.as_deref().and_then(parse_json),
        }
    }
}

impl From<Option<&crate::auth::AuthUser>> for SessionOut {
    fn from(user: Option<&crate::auth::AuthUser>) -> Self {
        match user {
            Some(user) => Self {
                subject: user.subject().to_owned(),
                scopes: user.scopes().iter().map(ToString::to_string).collect(),
            },
            None => Self {
                subject: "anonymous".to_string(),
                scopes: Vec::new(),
            },
        }
    }
}

fn parse_json(value: &str) -> Option<serde_json::Value> {
    serde_json::from_str(value).ok()
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ConfigOut {
    pub site: SiteConfigOut,
    pub database: DatabaseConfigOut,
    pub auth: AuthConfigOut,
    pub console: ConsoleConfigOut,
    pub tasks: TaskConfigOut,
    pub emitters: EmitterConfigOut,
    pub uploads: UploadConfigOut,
    pub channels: ChannelConfigOut,
    pub http: HttpConfigOut,
    pub logging: LoggingConfigOut,
}

impl ConfigOut {
    pub fn from_site(site: &Site) -> Self {
        let conf = site.conf();
        Self {
            site: SiteConfigOut {
                host: conf.host.clone(),
                port: conf.port,
                static_url: conf.static_url.clone(),
                project_dir: conf.project_dir.clone(),
                timezone: conf.tz.clone().unwrap_or_else(|| "UTC".to_string()),
                log_init: conf.log_init,
                touch_reload: conf.touch_reload.clone(),
            },
            database: DatabaseConfigOut {
                backend: database_backend(),
                min_connections: conf.database.min_connections,
                max_connections: conf.database.max_connections,
                lazy: conf.database.lazy,
                url: "<redacted>".to_string(),
            },
            auth: AuthConfigOut {
                summary: conf.auth.summary(),
            },
            console: ConsoleConfigOut {
                enabled: conf.console.enabled,
                path: conf.console.path.clone(),
                access_mode: conf.console.access_mode().to_string(),
                page_size_default: conf.console.page_size_default,
                page_size_max: conf.console.page_size_max,
                status_cache_ttl_seconds: conf.console.status_cache_ttl_seconds,
            },
            tasks: TaskConfigOut {
                poll_interval_ms: duration_ms(conf.tasks.poll_interval_value()),
                fallback_poll_interval_ms: duration_ms(conf.tasks.fallback_interval()),
                concurrency: conf.tasks.concurrency_value(),
                batch_size: conf.tasks.batch_size_value(),
                lease_duration_ms: duration_ms(conf.tasks.lease_duration_value()),
                readiness: conf.tasks.readiness_policy().as_str().to_string(),
                lanes: site
                    .tasks()
                    .lane_configs()
                    .iter()
                    .cloned()
                    .map(TaskLaneConfigOut::from)
                    .collect(),
                schedules: site
                    .tasks()
                    .schedule_configs()
                    .iter()
                    .map(TaskScheduleConfigOut::from)
                    .collect(),
            },
            emitters: EmitterConfigOut {
                notify_channel_capacity: conf.emitters.notify_channel_capacity,
                max_in_flight_handlers: conf.emitters.max_in_flight_handlers,
                pgnotify_reconnect_initial_ms: conf.emitters.pgnotify_reconnect_initial_ms,
                pgnotify_reconnect_max_ms: conf.emitters.pgnotify_reconnect_max_ms,
            },
            uploads: UploadConfigOut {
                dir: conf.uploads.dir.clone(),
                base_url: conf.uploads.base_url.clone(),
                temp_dir: conf.uploads.temp_dir.clone(),
                max_request_bytes: conf.uploads.max_request_bytes,
                max_file_bytes: conf.uploads.max_file_bytes,
                max_files: conf.uploads.max_files,
                max_fields: conf.uploads.max_fields,
                memory_threshold_bytes: conf.uploads.memory_threshold_bytes,
            },
            channels: ChannelConfigOut {
                enabled: conf.channels.enabled,
                subscriber_queue: conf.channels.subscriber_queue,
                replay_limit: conf.channels.replay_limit,
                retention_events: conf.channels.retention_events,
                max_message_bytes: conf.channels.max_message_bytes,
                long_poll_timeout_ms: conf.channels.long_poll_timeout_ms,
                sse_keepalive_ms: conf.channels.sse_keepalive_ms,
                slow_subscriber_policy: format!("{:?}", conf.channels.slow_subscriber_policy),
            },
            http: HttpConfigOut {
                slash_policy: format!("{:?}", conf.http.slash.policy),
                catch_panic_enabled: conf.http.catch_panic.enabled,
                request_id_enabled: conf.http.request_id.enabled,
                request_id_header: conf.http.request_id.header.clone(),
                trace_enabled: conf.http.trace.enabled,
                compression_enabled: conf.http.compression.enabled,
                cors_enabled: conf.http.cors.enabled,
                cors_permissive: conf.http.cors.permissive,
                timeout_enabled: conf.http.timeout.enabled,
                timeout_ms: conf.http.timeout.timeout_ms,
                body_limit_enabled: conf.http.body_limit.enabled,
                body_limit_max_bytes: conf.http.body_limit.max_bytes,
                security_headers_enabled: conf.http.security_headers.enabled,
                shutdown_grace_period_ms: conf.http.shutdown.grace_period_ms,
            },
            logging: LoggingConfigOut {
                env_prefix: conf.logging.resolved_env_prefix().to_string(),
                rules: conf.logging.rules.iter().map(LogRuleOut::from).collect(),
            },
        }
    }
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct SiteConfigOut {
    pub host: String,
    pub port: u16,
    pub static_url: String,
    pub project_dir: String,
    pub timezone: String,
    pub log_init: bool,
    pub touch_reload: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct DatabaseConfigOut {
    pub backend: &'static str,
    pub min_connections: u32,
    pub max_connections: u32,
    pub lazy: bool,
    pub url: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct AuthConfigOut {
    pub summary: crate::auth::AuthSummary,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ConsoleConfigOut {
    pub enabled: bool,
    pub path: String,
    pub access_mode: String,
    pub page_size_default: usize,
    pub page_size_max: usize,
    pub status_cache_ttl_seconds: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct TaskConfigOut {
    pub poll_interval_ms: u64,
    pub fallback_poll_interval_ms: u64,
    pub concurrency: usize,
    pub batch_size: usize,
    pub lease_duration_ms: u64,
    pub readiness: String,
    pub lanes: Vec<TaskLaneConfigOut>,
    pub schedules: Vec<TaskScheduleConfigOut>,
}

fn duration_ms(duration: std::time::Duration) -> u64 {
    match u64::try_from(duration.as_millis()) {
        Ok(value) => value,
        Err(_) => u64::MAX,
    }
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct TaskLaneConfigOut {
    pub name: String,
    pub concurrency: usize,
    pub rate_limit: Option<String>,
    pub global_rate_limit: Option<String>,
}

impl From<crate::tasks::TaskLaneConf> for TaskLaneConfigOut {
    fn from(conf: crate::tasks::TaskLaneConf) -> Self {
        Self {
            name: conf.lane().to_string(),
            concurrency: conf.concurrency(),
            rate_limit: conf.rate().map(format_task_rate),
            global_rate_limit: conf.global_rate().map(format_task_rate),
        }
    }
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct TaskScheduleConfigOut {
    pub name: String,
    pub task: String,
    pub source: String,
    pub expression: String,
    pub start: String,
}

impl From<&crate::tasks::TaskScheduleConf> for TaskScheduleConfigOut {
    fn from(conf: &crate::tasks::TaskScheduleConf) -> Self {
        Self {
            name: conf.name.clone(),
            task: conf.task.clone(),
            source: conf.source.clone(),
            expression: conf.expression.clone(),
            start: conf.start.clone(),
        }
    }
}

fn format_task_rate(rate: crate::tasks::TaskRate) -> String {
    format!(
        "{} per {:?}, burst {}",
        rate.permits(),
        rate.period(),
        rate.burst_size(),
    )
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct EmitterConfigOut {
    pub notify_channel_capacity: usize,
    pub max_in_flight_handlers: usize,
    pub pgnotify_reconnect_initial_ms: u64,
    pub pgnotify_reconnect_max_ms: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct UploadConfigOut {
    pub dir: String,
    pub base_url: Option<String>,
    pub temp_dir: Option<String>,
    pub max_request_bytes: u64,
    pub max_file_bytes: u64,
    pub max_files: usize,
    pub max_fields: usize,
    pub memory_threshold_bytes: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ChannelConfigOut {
    pub enabled: bool,
    pub subscriber_queue: usize,
    pub replay_limit: usize,
    pub retention_events: usize,
    pub max_message_bytes: usize,
    pub long_poll_timeout_ms: u64,
    pub sse_keepalive_ms: u64,
    pub slow_subscriber_policy: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct HttpConfigOut {
    pub slash_policy: String,
    pub catch_panic_enabled: bool,
    pub request_id_enabled: bool,
    pub request_id_header: String,
    pub trace_enabled: bool,
    pub compression_enabled: bool,
    pub cors_enabled: bool,
    pub cors_permissive: bool,
    pub timeout_enabled: bool,
    pub timeout_ms: u64,
    pub body_limit_enabled: bool,
    pub body_limit_max_bytes: u64,
    pub security_headers_enabled: bool,
    pub shutdown_grace_period_ms: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct LoggingConfigOut {
    pub env_prefix: String,
    pub rules: Vec<LogRuleOut>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct LogRuleOut {
    pub name: String,
    pub sink: String,
    pub path: Option<String>,
    pub rotation: Option<String>,
    pub default_filter: String,
}

impl From<&crate::logging::LogRule> for LogRuleOut {
    fn from(rule: &crate::logging::LogRule) -> Self {
        let (sink, path, rotation) = match &rule.sink {
            LogSink::File { dir, rotation } => (
                "file".to_string(),
                Some(dir.clone()),
                Some(format!("{:?}", rotation)),
            ),
            LogSink::Stdout { .. } => ("stdout".to_string(), None, None),
            LogSink::Stderr { .. } => ("stderr".to_string(), None, None),
            LogSink::MailAdmins(_) => ("mail_admins".to_string(), None, None),
        };
        Self {
            name: rule.name.clone(),
            sink,
            path,
            rotation,
            default_filter: rule.default_filter.clone(),
        }
    }
}

fn database_backend() -> &'static str {
    #[cfg(feature = "postgres")]
    {
        return "postgres";
    }
    #[cfg(all(not(feature = "postgres"), feature = "mysql"))]
    {
        return "mysql";
    }
    #[cfg(all(not(any(feature = "postgres", feature = "mysql")), feature = "sqlite"))]
    {
        return "sqlite";
    }
    #[cfg(not(any(feature = "postgres", feature = "mysql", feature = "sqlite")))]
    {
        "memory"
    }
}
