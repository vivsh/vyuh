use std::time::Duration;

use axum::{
    extract::{Request, State},
    http::{HeaderName, HeaderValue, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};

use crate::errors::{ErrorReport, ErrorSourceKind};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpConf {
    pub catch_panic: CatchPanicConf,
    pub request_id: RequestIdConf,
    pub trace: TraceConf,
    pub compression: CompressionConf,
    pub cors: CorsConf,
    pub timeout: TimeoutConf,
    pub body_limit: BodyLimitConf,
    pub security_headers: SecurityHeadersConf,
    #[serde(default)]
    pub shutdown: ShutdownConf,
}

impl Default for HttpConf {
    fn default() -> Self {
        Self {
            catch_panic: CatchPanicConf::default(),
            request_id: RequestIdConf::default(),
            trace: TraceConf::default(),
            compression: CompressionConf::default(),
            cors: CorsConf::default(),
            timeout: TimeoutConf::default(),
            body_limit: BodyLimitConf::default(),
            security_headers: SecurityHeadersConf::default(),
            shutdown: ShutdownConf::default(),
        }
    }
}

impl HttpConf {
    /// Returns the baseline HTTP policy for an internet-facing deployment.
    ///
    /// CORS remains disabled because allowed origins are application-specific.
    pub fn production() -> Self {
        Self {
            trace: TraceConf { enabled: true },
            compression: CompressionConf { enabled: true },
            timeout: TimeoutConf {
                enabled: true,
                ..TimeoutConf::default()
            },
            body_limit: BodyLimitConf {
                enabled: true,
                ..BodyLimitConf::default()
            },
            security_headers: SecurityHeadersConf {
                enabled: true,
                ..SecurityHeadersConf::default()
            },
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatchPanicConf {
    pub enabled: bool,
}

impl Default for CatchPanicConf {
    fn default() -> Self {
        Self { enabled: true }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestIdConf {
    pub enabled: bool,
    pub header: String,
}

impl Default for RequestIdConf {
    fn default() -> Self {
        Self {
            enabled: true,
            header: "x-request-id".into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceConf {
    pub enabled: bool,
}

impl Default for TraceConf {
    fn default() -> Self {
        Self { enabled: false }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionConf {
    pub enabled: bool,
}

impl Default for CompressionConf {
    fn default() -> Self {
        Self { enabled: false }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorsConf {
    pub enabled: bool,
    pub permissive: bool,
}

impl Default for CorsConf {
    fn default() -> Self {
        Self {
            enabled: false,
            permissive: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeoutConf {
    pub enabled: bool,
    pub timeout_ms: u64,
}

impl Default for TimeoutConf {
    fn default() -> Self {
        Self {
            enabled: false,
            timeout_ms: 30_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BodyLimitConf {
    pub enabled: bool,
    pub max_bytes: u64,
}

impl Default for BodyLimitConf {
    fn default() -> Self {
        Self {
            enabled: false,
            max_bytes: 2 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityHeadersConf {
    pub enabled: bool,
    pub x_content_type_options: bool,
    pub x_frame_options: Option<String>,
    pub referrer_policy: Option<String>,
}

impl Default for SecurityHeadersConf {
    fn default() -> Self {
        Self {
            enabled: false,
            x_content_type_options: true,
            x_frame_options: Some("DENY".into()),
            referrer_policy: Some("same-origin".into()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownConf {
    #[serde(default = "default_shutdown_grace_period_ms")]
    pub grace_period_ms: u64,
}

impl Default for ShutdownConf {
    fn default() -> Self {
        Self {
            grace_period_ms: default_shutdown_grace_period_ms(),
        }
    }
}

fn default_shutdown_grace_period_ms() -> u64 {
    10_000
}

pub(crate) async fn request_id_middleware(
    State(conf): State<RequestIdConf>,
    mut req: Request,
    next: Next,
) -> Response {
    let header_name = HeaderName::from_bytes(conf.header.as_bytes())
        .unwrap_or_else(|_| HeaderName::from_static("x-request-id"));
    let request_id = req
        .headers()
        .get(&header_name)
        .cloned()
        .unwrap_or_else(|| HeaderValue::from_str(&uuid::Uuid::now_v7().to_string()).unwrap());
    req.headers_mut()
        .insert(header_name.clone(), request_id.clone());
    let mut response = next.run(req).await;
    response.headers_mut().insert(header_name, request_id);
    response
}

pub(crate) async fn body_limit_middleware(
    State(conf): State<BodyLimitConf>,
    req: Request,
    next: Next,
) -> Response {
    if let Some(content_length) = req
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
    {
        if content_length > conf.max_bytes {
            return ErrorReport::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                ErrorSourceKind::Parse,
                "request_body_too_large",
                format!("Request body exceeds {} bytes.", conf.max_bytes),
            )
            .into_response();
        }
    }
    next.run(req).await
}

pub(crate) async fn timeout_middleware(
    State(conf): State<TimeoutConf>,
    req: Request,
    next: Next,
) -> Response {
    match tokio::time::timeout(Duration::from_millis(conf.timeout_ms), next.run(req)).await {
        Ok(response) => response,
        Err(_) => ErrorReport::new(
            StatusCode::GATEWAY_TIMEOUT,
            ErrorSourceKind::Framework,
            "request_timeout",
            format!("Request exceeded {} ms.", conf.timeout_ms),
        )
        .into_response(),
    }
}

pub(crate) async fn security_headers_middleware(
    State(conf): State<SecurityHeadersConf>,
    req: Request,
    next: Next,
) -> Response {
    let mut response = next.run(req).await;
    if conf.x_content_type_options {
        response.headers_mut().insert(
            header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        );
    }
    if let Some(value) = conf.x_frame_options.as_deref().and_then(header_value) {
        response
            .headers_mut()
            .insert(HeaderName::from_static("x-frame-options"), value);
    }
    if let Some(value) = conf.referrer_policy.as_deref().and_then(header_value) {
        response
            .headers_mut()
            .insert(HeaderName::from_static("referrer-policy"), value);
    }
    response
}

fn header_value(value: &str) -> Option<HeaderValue> {
    HeaderValue::from_str(value).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shutdown_grace_defaults_to_ten_seconds() {
        assert_eq!(HttpConf::default().shutdown.grace_period_ms, 10_000);
    }

    #[test]
    fn empty_shutdown_conf_uses_default_grace() {
        let parsed = serde_json::from_str::<ShutdownConf>("{}");
        assert!(matches!(
            parsed,
            Ok(ShutdownConf {
                grace_period_ms: 10_000
            })
        ));
    }
}
