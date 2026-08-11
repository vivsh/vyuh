use crate::auth::AuthError;
use crate::callables::CallError;
use crate::db::DbError;
use crate::utils::html;
use crate::validation::{PathSeg, ValidationError, ValidationReport};
use axum::{
    Json,
    http::{HeaderMap, Method, StatusCode, Uri, header},
    response::{IntoResponse, Response},
};
use schemars::JsonSchema;
use serde::Serialize;
use smallvec::SmallVec;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::{borrow::Cow, error::Error as StdError};

/// Transport-facing source category for rendered error reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ErrorSourceKind {
    Framework,
    Parse,
    Validation,
    Auth,
    Database,
    Template,
    Application,
    Other,
}

/// Normalized error payload used for route/extractor/application failures.
///
/// `ErrorReport` is intentionally transport-oriented. Application errors can
/// convert into it, but should not use it as their domain error type.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct ErrorReport {
    #[schemars(skip)]
    #[serde(skip_serializing)]
    pub status: StatusCode,
    pub source: ErrorSourceKind,
    pub code: Cow<'static, str>,
    pub detail: Cow<'static, str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub errors: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub debug: Option<ErrorDebug>,
}

/// Optional diagnostic payload for development error responses.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct ErrorDebug {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Cow<'static, str>>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub context: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub causes: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backtrace: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ErrorRequestContext {
    pub method: Method,
    pub uri: Uri,
    pub path: String,
    pub headers: HeaderMap,
}

pub type ErrorContext = ErrorRequestContext;

#[derive(Debug, Clone)]
pub struct ErrorCommandContext {
    pub command: String,
    pub args: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorRenderTarget {
    Json,
    Html,
    Command,
}

#[derive(Debug, Clone)]
pub struct ErrorRenderContext {
    pub target: ErrorRenderTarget,
    pub request: Option<ErrorRequestContext>,
    pub command: Option<ErrorCommandContext>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpErrorRenderMode {
    Json,
    Html,
    Auto,
}

impl Default for HttpErrorRenderMode {
    fn default() -> Self {
        Self::Auto
    }
}

#[derive(Debug, Clone)]
pub struct ErrorView {
    pub status: StatusCode,
    pub source: ErrorSourceKind,
    pub kind: ErrorKind,
    pub code: Cow<'static, str>,
    pub message: Cow<'static, str>,
    pub errors: Option<serde_json::Value>,
    pub validation: Option<ValidationReport>,
}

type ErrorHandler = Arc<
    dyn Fn(ErrorRequestContext, ErrorReport) -> Pin<Box<dyn Future<Output = Response> + Send>>
        + Send
        + Sync,
>;

type HttpErrorViewHandler = Arc<
    dyn Fn(ErrorRequestContext, ErrorView) -> Pin<Box<dyn Future<Output = Response> + Send>>
        + Send
        + Sync,
>;

type CommandErrorRenderer = Arc<dyn Fn(ErrorCommandContext, ErrorView) -> String + Send + Sync>;

#[derive(Clone, Default)]
pub struct ErrorConf {
    handler: Option<ErrorHandler>,
    json_handler: Option<HttpErrorViewHandler>,
    html_handler: Option<HttpErrorViewHandler>,
    command_renderer: Option<CommandErrorRenderer>,
    http_mode: HttpErrorRenderMode,
}

impl std::fmt::Debug for ErrorConf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ErrorConf")
            .field("handler", &self.handler.as_ref().map(|_| "<custom>"))
            .field(
                "json_handler",
                &self.json_handler.as_ref().map(|_| "<custom>"),
            )
            .field(
                "html_handler",
                &self.html_handler.as_ref().map(|_| "<custom>"),
            )
            .field(
                "command_renderer",
                &self.command_renderer.as_ref().map(|_| "<custom>"),
            )
            .field("http_mode", &self.http_mode)
            .finish()
    }
}

impl ErrorConf {
    /// Uses Vyuh's standard JSON API error body for JSON requests.
    ///
    /// The response contains `source`, `code`, `detail`, `path`, and `errors`.
    /// HTML requests continue to use the default HTML renderer unless a custom
    /// HTML handler is configured.
    pub fn api_json(self) -> Self {
        self.json(|ctx, view| async move {
            (
                view.status,
                Json(serde_json::json!({
                    "source": view.source,
                    "code": view.code,
                    "detail": view.message,
                    "path": ctx.path,
                    "errors": view.errors,
                })),
            )
                .into_response()
        })
        .http_mode(HttpErrorRenderMode::Auto)
    }

    pub fn handler<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(ErrorRequestContext, ErrorReport) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Response> + Send + 'static,
    {
        self.handler = Some(Arc::new(move |ctx, report| Box::pin(handler(ctx, report))));
        self
    }

    pub fn json<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(ErrorRequestContext, ErrorView) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Response> + Send + 'static,
    {
        self.json_handler = Some(Arc::new(move |ctx, view| Box::pin(handler(ctx, view))));
        self
    }

    pub fn html<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(ErrorRequestContext, ErrorView) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Response> + Send + 'static,
    {
        self.html_handler = Some(Arc::new(move |ctx, view| Box::pin(handler(ctx, view))));
        self
    }

    pub fn command<F>(mut self, renderer: F) -> Self
    where
        F: Fn(ErrorCommandContext, ErrorView) -> String + Send + Sync + 'static,
    {
        self.command_renderer = Some(Arc::new(renderer));
        self
    }

    pub fn http_mode(mut self, mode: HttpErrorRenderMode) -> Self {
        self.http_mode = mode;
        self
    }

    pub(crate) async fn render(&self, ctx: ErrorRequestContext, report: ErrorReport) -> Response {
        if let Some(handler) = &self.handler {
            return handler(ctx, report).await;
        }
        let target = self.http_target(&ctx);
        let view = ErrorView::from_report(report.clone());
        match target {
            ErrorRenderTarget::Html => {
                if let Some(handler) = &self.html_handler {
                    return handler(ctx, view).await;
                }
                return default_html_error(ctx, view);
            }
            ErrorRenderTarget::Json => {
                if let Some(handler) = &self.json_handler {
                    return handler(ctx, view).await;
                }
            }
            ErrorRenderTarget::Command => {}
        }
        report.into_response()
    }

    pub(crate) fn render_command(&self, ctx: ErrorCommandContext, view: ErrorView) -> String {
        if let Some(renderer) = &self.command_renderer {
            return renderer(ctx, view);
        }
        default_command_error(ctx, view)
    }

    fn http_target(&self, ctx: &ErrorRequestContext) -> ErrorRenderTarget {
        match self.http_mode {
            HttpErrorRenderMode::Json => ErrorRenderTarget::Json,
            HttpErrorRenderMode::Html => ErrorRenderTarget::Html,
            HttpErrorRenderMode::Auto => http_auto_target(&ctx.headers),
        }
    }
}

fn http_auto_target(headers: &HeaderMap) -> ErrorRenderTarget {
    if is_json_content_type(headers) || accepts_json(headers) {
        ErrorRenderTarget::Json
    } else {
        ErrorRenderTarget::Html
    }
}

fn is_json_content_type(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(media_header_has_json)
}

fn accepts_json(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(media_header_has_json)
}

fn media_header_has_json(value: &str) -> bool {
    value.split(',').any(|part| {
        let media = part.split(';').next().unwrap_or("").trim();
        media_type_is_json(media)
    })
}

fn media_type_is_json(media: &str) -> bool {
    let media = media.to_ascii_lowercase();
    media == "application/json" || media == "application/*+json" || media.ends_with("+json")
}

fn default_html_error(ctx: ErrorRequestContext, view: ErrorView) -> Response {
    let body = format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><title>{status} {code}</title></head><body><main><h1>{status}</h1><p>{message}</p><p><code>{method} {path}</code></p></main></body></html>",
        status = view.status.as_u16(),
        code = html::escape(view.code.as_ref()),
        message = html::escape(view.message.as_ref()),
        method = html::escape(ctx.method.as_str()),
        path = html::escape(&ctx.path),
    );
    (
        view.status,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        body,
    )
        .into_response()
}

impl ErrorReport {
    pub fn new(
        status: StatusCode,
        source: ErrorSourceKind,
        code: impl Into<Cow<'static, str>>,
        detail: impl Into<Cow<'static, str>>,
    ) -> Self {
        Self {
            status,
            source,
            code: code.into(),
            detail: detail.into(),
            errors: None,
            debug: None,
        }
    }

    pub fn bad_request(detail: impl Into<Cow<'static, str>>) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            ErrorSourceKind::Parse,
            "bad_request",
            detail,
        )
    }

    /// Creates the standard framework response for an unmatched route.
    pub fn not_found() -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            ErrorSourceKind::Framework,
            "not_found",
            "Resource not found.",
        )
    }

    /// Creates the standard framework response for an unsupported route method.
    pub fn method_not_allowed() -> Self {
        Self::new(
            StatusCode::METHOD_NOT_ALLOWED,
            ErrorSourceKind::Framework,
            "method_not_allowed",
            "Method not allowed.",
        )
    }

    /// Creates the safe standard framework response for an unexpected failure.
    pub fn internal_error() -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorSourceKind::Framework,
            "internal_error",
            "Internal server error.",
        )
    }

    pub fn validation(report: ValidationReport) -> Self {
        Self {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            source: ErrorSourceKind::Validation,
            code: Cow::Borrowed("validation_error"),
            detail: Cow::Borrowed("Validation failed."),
            errors: Some(report.to_nested_errors()),
            debug: None,
        }
    }

    pub fn with_errors(mut self, errors: serde_json::Value) -> Self {
        self.errors = Some(errors);
        self
    }
}

impl IntoResponse for ErrorReport {
    fn into_response(self) -> Response {
        let status = self.status;
        let mut response = (status, Json(self.clone())).into_response();
        response.extensions_mut().insert(self);
        response
    }
}

impl ErrorView {
    pub fn from_report(report: ErrorReport) -> Self {
        let kind = kind_from_status(report.status);
        Self {
            status: report.status,
            source: report.source,
            kind,
            code: report.code,
            message: report.detail,
            errors: report.errors,
            validation: None,
        }
    }

    pub fn from_validation(report: ValidationReport) -> Self {
        Self {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            source: ErrorSourceKind::Validation,
            kind: ErrorKind::Invalid,
            code: Cow::Borrowed("validation_error"),
            message: Cow::Borrowed("Validation failed."),
            errors: Some(report.to_nested_errors()),
            validation: Some(report),
        }
    }

    pub fn from_error(error: &Error) -> Self {
        if let Some(ErrorSource::Validation(report)) = &error.source {
            return Self::from_validation(report.clone());
        }

        let source = match &error.source {
            Some(ErrorSource::Database(_)) | Some(ErrorSource::Sqlx(_)) => {
                ErrorSourceKind::Database
            }
            Some(ErrorSource::Auth(_)) => ErrorSourceKind::Auth,
            Some(ErrorSource::Other(_)) | None => ErrorSourceKind::Application,
            Some(ErrorSource::Validation(_)) => ErrorSourceKind::Validation,
        };

        Self {
            status: error.kind.status_code(),
            source,
            kind: error.kind,
            code: Cow::Borrowed(error.kind.error_code()),
            message: Cow::Owned(error.display_compact()),
            errors: None,
            validation: None,
        }
    }

    pub fn to_report(&self) -> ErrorReport {
        if let Some(report) = &self.validation {
            return ErrorReport::validation(report.clone());
        }
        let mut report = ErrorReport::new(
            self.status,
            self.source,
            self.code.to_ascii_lowercase(),
            self.message.to_string(),
        );
        if let Some(errors) = &self.errors {
            report.errors = Some(errors.clone());
        }
        report
    }
}

fn kind_from_status(status: StatusCode) -> ErrorKind {
    match status {
        StatusCode::BAD_REQUEST => ErrorKind::BadRequest,
        StatusCode::UNAUTHORIZED => ErrorKind::Unauthorized,
        StatusCode::FORBIDDEN => ErrorKind::Forbidden,
        StatusCode::NOT_FOUND => ErrorKind::NotFound,
        StatusCode::CONFLICT => ErrorKind::Conflict,
        StatusCode::UNPROCESSABLE_ENTITY => ErrorKind::Invalid,
        StatusCode::TOO_MANY_REQUESTS => ErrorKind::RateLimited,
        StatusCode::SERVICE_UNAVAILABLE | StatusCode::GATEWAY_TIMEOUT => ErrorKind::Unavailable,
        _ if status.is_client_error() => ErrorKind::BadRequest,
        _ => ErrorKind::Other,
    }
}

fn default_command_error(ctx: ErrorCommandContext, view: ErrorView) -> String {
    if let Some(report) = &view.validation {
        return render_command_validation(&ctx.command, report);
    }
    match view.source {
        ErrorSourceKind::Validation => format!(
            "Validation failed for command '{}'.\n\nUse '{} --help' for usage.",
            ctx.command, ctx.command
        ),
        _ => view.message.into_owned(),
    }
}

fn render_command_validation(command: &str, report: &ValidationReport) -> String {
    let mut output = format!("Validation failed for command '{command}':\n\n");

    for issue in &report.issues {
        let field = if issue.path.is_root() {
            "non_field_errors".to_string()
        } else {
            issue
                .path
                .segments()
                .iter()
                .map(|segment| match segment {
                    PathSeg::Field(field) => field.to_string(),
                    PathSeg::Index(index) => index.to_string(),
                    PathSeg::Key(key) => key.to_string(),
                })
                .collect::<Vec<_>>()
                .join(".")
        };
        output.push_str(&format!("  --{field}\n"));
        output.push_str(&format!("    {}\n\n", issue.invalid.message));
    }

    output.push_str(&format!("Use '{command} --help' for usage."));
    output
}

/// Source of an error, avoiding boxing for common error types.
#[derive(Debug)]
pub enum ErrorSource {
    Validation(ValidationReport),
    Database(DbError),
    Auth(AuthError),
    Sqlx(crate::db::sqlx::Error),
    Other(Box<dyn StdError + Send + Sync + 'static>),
}

/// Semantic error categories for consistent error handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    NotFound,
    BadRequest,
    Unauthorized,
    Forbidden,
    Invalid,     // Validation failures
    Integrity,   // Database constraint violations
    Conflict,    // Business logic conflicts (e.g., version mismatch)
    RateLimited, // API rate limiting
    Unavailable, // Service unavailable
    Other,       // Unexpected errors
}

impl ErrorKind {
    pub fn default_message(&self) -> &'static str {
        match self {
            Self::NotFound => "Resource not found",
            Self::BadRequest => "Bad request",
            Self::Unauthorized => "Authentication required",
            Self::Forbidden => "Permission denied",
            Self::Invalid => "Validation failed",
            Self::Integrity => "Data integrity violation",
            Self::Conflict => "Resource conflict",
            Self::RateLimited => "Too many requests",
            Self::Unavailable => "Service unavailable",
            Self::Other => "Internal server error",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::BadRequest => StatusCode::BAD_REQUEST,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::Invalid => StatusCode::UNPROCESSABLE_ENTITY,
            Self::Integrity => StatusCode::CONFLICT,
            Self::Conflict => StatusCode::CONFLICT,
            Self::RateLimited => StatusCode::TOO_MANY_REQUESTS,
            Self::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::Other => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_code(&self) -> &'static str {
        match self {
            Self::NotFound => "NOT_FOUND",
            Self::BadRequest => "BAD_REQUEST",
            Self::Unauthorized => "UNAUTHORIZED",
            Self::Forbidden => "FORBIDDEN",
            Self::Invalid => "VALIDATION_ERROR",
            Self::Integrity => "INTEGRITY_ERROR",
            Self::Conflict => "CONFLICT",
            Self::RateLimited => "RATE_LIMITED",
            Self::Unavailable => "UNAVAILABLE",
            Self::Other => "INTERNAL_ERROR",
        }
    }
}

/// Universal error type for all vyuh handlers (signals, tasks, commands, routes).
/// Provides semantic error categories while preserving full error chains.
/// Context uses SmallVec to avoid heap allocations for typical error chains (0-4 items).
#[derive(Debug)]
pub struct Error {
    pub kind: ErrorKind,
    pub source: Option<ErrorSource>,
    pub context: SmallVec<[Cow<'static, str>; 4]>,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.kind.default_message())?;
        if !self.context.is_empty() {
            write!(f, ": {}", self.context.join(" -> "))?;
        }
        Ok(())
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source.as_ref().map(|src| match src {
            ErrorSource::Auth(e) => e as &(dyn StdError + 'static),
            ErrorSource::Database(e) => e as &(dyn StdError + 'static),
            ErrorSource::Sqlx(e) => e as &(dyn StdError + 'static),
            ErrorSource::Other(e) => e.as_ref() as &(dyn StdError + 'static),
            ErrorSource::Validation(e) => e as &(dyn StdError + 'static),
        })
    }
}

impl Error {
    /// Create error with kind only (use sparingly - prefer wrapping source errors)
    pub fn new(kind: ErrorKind) -> Self {
        Self {
            kind,
            source: None,
            context: SmallVec::new(),
        }
    }

    pub fn bad_request(message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(ErrorKind::BadRequest).with_context(message)
    }

    pub fn not_found(message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(ErrorKind::NotFound).with_context(message)
    }

    pub fn invalid(message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(ErrorKind::Invalid).with_context(message)
    }

    pub fn conflict(message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(ErrorKind::Conflict).with_context(message)
    }

    pub fn unavailable(message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(ErrorKind::Unavailable).with_context(message)
    }

    /// Wrap an external error as ErrorKind::Other (most common case)
    pub fn other<E>(err: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self {
            kind: ErrorKind::Other,
            source: Some(ErrorSource::Other(Box::new(err))),
            context: SmallVec::new(),
        }
    }

    /// Wrap source error with explicit kind (use when semantic categorization matters)
    pub fn wrap<E>(kind: ErrorKind, err: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self {
            kind,
            source: Some(ErrorSource::Other(Box::new(err))),
            context: SmallVec::new(),
        }
    }

    /// Add context to the error chain
    pub fn with_context(mut self, ctx: impl Into<Cow<'static, str>>) -> Self {
        self.context.push(ctx.into());
        self
    }

    /// Pretty format for CLI/command output with full error chain
    pub fn display_verbose(&self) -> String {
        let mut output = String::new();

        // Main error message
        output.push_str("Error: ");
        output.push_str(self.kind.default_message());
        output.push('\n');

        // Context chain
        if !self.context.is_empty() {
            for (i, ctx) in self.context.iter().enumerate() {
                output.push_str(&format!(
                    "  {} {}\n",
                    if i == self.context.len() - 1 {
                        "↳"
                    } else {
                        "│"
                    },
                    ctx
                ));
            }
        }

        // Source chain (walk the error chain)
        let mut source_chain: Vec<String> = Vec::new();
        if let Some(src) = &self.source {
            match src {
                ErrorSource::Validation(report) => {
                    if !report.is_empty() {
                        source_chain.push(format!(
                            "Validation failed with {} error(s)",
                            report.issues.len()
                        ));
                    }
                }
                ErrorSource::Database(e) => {
                    source_chain.push(e.to_string());
                }
                ErrorSource::Auth(e) => {
                    source_chain.push(e.to_string());
                }
                ErrorSource::Sqlx(e) => {
                    source_chain.push(e.to_string());
                    // Walk sqlx's source chain
                    let mut current: Option<&(dyn StdError + 'static)> = e.source();
                    while let Some(err) = current {
                        source_chain.push(err.to_string());
                        current = err.source();
                    }
                }
                ErrorSource::Other(e) => {
                    source_chain.push(e.to_string());
                    // Walk generic error's source chain
                    let mut current: Option<&(dyn StdError + 'static)> = e.source();
                    while let Some(err) = current {
                        source_chain.push(err.to_string());
                        current = err.source();
                    }
                }
            }
        }

        if !source_chain.is_empty() {
            output.push('\n');
            output.push_str("Caused by:\n");
            for (i, cause) in source_chain.iter().enumerate() {
                output.push_str(&format!("  {}: {}\n", i, cause));
            }
        }

        output
    }

    /// Compact single-line format for logging
    pub fn display_compact(&self) -> String {
        let mut parts = vec![self.kind.default_message().to_string()];
        if !self.context.is_empty() {
            parts.extend(self.context.iter().map(|c| c.to_string()));
        }
        parts.join(": ")
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        ErrorReport::from(self).into_response()
    }
}

impl From<Error> for ErrorReport {
    fn from(err: Error) -> Self {
        let debug = debug_error_payload(&err);
        let mut report = ErrorView::from_error(&err).to_report();
        if let Some(debug) = debug {
            report.debug = Some(debug);
        }
        report
    }
}

fn debug_error_payload(err: &Error) -> Option<ErrorDebug> {
    if !cfg!(debug_assertions) {
        return None;
    }

    let context = err
        .context
        .iter()
        .map(|c| c.to_string())
        .collect::<Vec<_>>();
    let mut causes = Vec::new();
    if let Some(source) = &err.source {
        collect_error_source_chain(source, &mut causes);
    }

    if context.is_empty() && causes.is_empty() {
        return None;
    }

    Some(ErrorDebug {
        details: None,
        context,
        causes,
        backtrace: None,
    })
}

fn collect_error_source_chain(source: &ErrorSource, causes: &mut Vec<String>) {
    match source {
        ErrorSource::Validation(report) => {
            if !report.is_empty() {
                causes.push(format!(
                    "Validation failed with {} error(s)",
                    report.issues.len()
                ));
            }
        }
        ErrorSource::Database(err) => collect_std_error_chain(err, causes),
        ErrorSource::Auth(err) => collect_std_error_chain(err, causes),
        ErrorSource::Sqlx(err) => collect_std_error_chain(err, causes),
        ErrorSource::Other(err) => collect_std_error_chain(err.as_ref(), causes),
    }
}

fn collect_std_error_chain(err: &(dyn StdError + 'static), causes: &mut Vec<String>) {
    causes.push(err.to_string());
    let mut current = err.source();
    while let Some(source) = current {
        causes.push(source.to_string());
        current = source.source();
    }
}

impl From<ValidationReport> for ErrorReport {
    fn from(report: ValidationReport) -> Self {
        Self::validation(report)
    }
}

impl From<ValidationReport> for Error {
    fn from(report: ValidationReport) -> Self {
        Self {
            kind: ErrorKind::Invalid,
            source: Some(ErrorSource::Validation(report)),
            context: SmallVec::new(),
        }
    }
}

impl From<ValidationError> for Error {
    fn from(err: ValidationError) -> Self {
        let mut report = ValidationReport::empty();
        report.push_root(err);
        Self::from(report)
    }
}

impl From<DbError> for Error {
    fn from(err: DbError) -> Self {
        let kind = match &err {
            DbError::DoesNotExist => ErrorKind::NotFound,
            DbError::Integrity { .. } => ErrorKind::Integrity,
            DbError::QuerySet(_) => ErrorKind::BadRequest,
            DbError::MultipleObjects => ErrorKind::Conflict,
            _ => ErrorKind::Other,
        };
        Self {
            kind,
            source: Some(ErrorSource::Database(err)),
            context: SmallVec::new(),
        }
    }
}

impl From<AuthError> for Error {
    fn from(err: AuthError) -> Self {
        let kind = match &err {
            AuthError::Forbidden => ErrorKind::Forbidden,
            _ => ErrorKind::Unauthorized,
        };
        Self {
            kind,
            source: Some(ErrorSource::Auth(err)),
            context: SmallVec::new(),
        }
    }
}

impl From<crate::auth::PasswordError> for Error {
    fn from(err: crate::auth::PasswordError) -> Self {
        Self::other(err)
    }
}

impl From<crate::db::sqlx::Error> for Error {
    fn from(err: crate::db::sqlx::Error) -> Self {
        let kind = match &err {
            crate::db::sqlx::Error::RowNotFound => ErrorKind::NotFound,
            _ => ErrorKind::Other,
        };
        Self {
            kind,
            source: Some(ErrorSource::Sqlx(err)),
            context: SmallVec::new(),
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Self::other(err).with_context("JSON parsing failed")
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::other(err).with_context("I/O operation failed")
    }
}

impl From<crate::tasks::TaskError> for Error {
    fn from(err: crate::tasks::TaskError) -> Self {
        Self::other(err)
    }
}

impl From<CallError> for Error {
    fn from(err: CallError) -> Self {
        match err {
            CallError::Validation(report) => Self::from(report),
            CallError::DeserializeFailed => Self::bad_request("failed to deserialize handler data"),
            CallError::SerializeFailed => Self::other(err),
            CallError::TypeMismatch => Self::bad_request("handler data type mismatch"),
            CallError::ExtractionFailed(msg) => Self::bad_request(msg),
            CallError::MissingField(field) => Self::bad_request(format!("missing field: {field}")),
            CallError::InvalidArgument(msg) => Self::bad_request(msg),
            CallError::Unauthorized => Self::new(ErrorKind::Unauthorized),
            CallError::Forbidden => Self::new(ErrorKind::Forbidden),
            CallError::NotFound(msg) => Self::not_found(msg),
            CallError::Other(err) => Self {
                kind: ErrorKind::Other,
                source: Some(ErrorSource::Other(err)),
                context: SmallVec::new(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;

    use super::{ErrorReport, ErrorSourceKind};

    /// Verifies framework fallback reports use stable safe transport metadata.
    #[test]
    fn framework_reports_are_safe_and_standardized() {
        let reports = [
            (ErrorReport::not_found(), StatusCode::NOT_FOUND, "not_found"),
            (
                ErrorReport::method_not_allowed(),
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
            ),
            (
                ErrorReport::internal_error(),
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
            ),
        ];

        for (report, status, code) in reports {
            assert_eq!(report.status, status);
            assert_eq!(report.source, ErrorSourceKind::Framework);
            assert_eq!(report.code, code);
            assert!(report.errors.is_none());
        }
    }
}
