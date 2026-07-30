//! Health probes and bounded-cardinality Prometheus metrics for a Vyuh site.

use std::{
    collections::BTreeMap,
    fmt::Write as _,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use axum::{
    extract::{MatchedPath, Request, State},
    http::{StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};

use crate::{Site, conf::ConfError};

const DURATION_BUCKETS: [f64; 8] = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0];

/// Configures built-in health probes and Prometheus metric exposition.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ObservabilityConf {
    /// Enables all observability routes and request instrumentation.
    pub enabled: bool,
    /// Liveness probe path.
    pub liveness_path: String,
    /// Readiness probe path.
    pub readiness_path: String,
    /// Prometheus exposition path.
    pub metrics_path: String,
    /// Maximum time spent checking database connectivity for readiness.
    pub readiness_timeout_ms: u64,
}

impl Default for ObservabilityConf {
    fn default() -> Self {
        Self {
            enabled: false,
            liveness_path: "/healthz".into(),
            readiness_path: "/readyz".into(),
            metrics_path: "/metrics".into(),
            readiness_timeout_ms: 1_000,
        }
    }
}

impl ObservabilityConf {
    /// Returns the route configuration enabled by the production site profile.
    pub fn production() -> Self {
        Self {
            enabled: true,
            ..Self::default()
        }
    }

    /// Adds configuration errors for invalid or colliding observability paths.
    pub(crate) fn validate(&self, errors: &mut Vec<ConfError>) {
        if !self.enabled {
            return;
        }
        let paths = [
            ("observability.liveness_path", &self.liveness_path),
            ("observability.readiness_path", &self.readiness_path),
            ("observability.metrics_path", &self.metrics_path),
        ];
        validate_paths(&paths, errors);
        if self.readiness_timeout_ms == 0 {
            errors.push(ConfError::InvalidValue {
                field: "observability.readiness_timeout_ms".into(),
                reason: "must be greater than zero".into(),
                expected: Some("a positive duration in milliseconds".into()),
            });
        }
    }
}

/// Runtime metrics state shared by a built site.
#[derive(Clone)]
pub(crate) struct Observability {
    conf: ObservabilityConf,
    metrics: Arc<Metrics>,
}

impl Observability {
    /// Creates the shared state used by routes and request middleware.
    pub(crate) fn new(conf: ObservabilityConf) -> Self {
        Self {
            conf,
            metrics: Arc::new(Metrics::default()),
        }
    }

    /// Returns whether routes and instrumentation are enabled.
    pub(crate) fn enabled(&self) -> bool {
        self.conf.enabled
    }

    /// Returns the configured observability paths.
    pub(crate) fn paths(&self) -> [&str; 3] {
        [
            &self.conf.liveness_path,
            &self.conf.readiness_path,
            &self.conf.metrics_path,
        ]
    }

    /// Records a recovered HTTP-service panic.
    pub(crate) fn record_panic(&self) {
        self.metrics.panics.fetch_add(1, Ordering::Relaxed);
    }

    /// Marks a request as active until its response has been recorded.
    fn request_started(&self) {
        self.metrics.in_flight.fetch_add(1, Ordering::Relaxed);
    }

    /// Marks a completed request as no longer active.
    fn request_finished(&self) {
        self.metrics.in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    /// Renders the current Prometheus exposition without application data labels.
    pub(crate) fn render(&self) -> String {
        self.metrics.render()
    }

    /// Returns the timeout used by the readiness database check.
    #[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
    pub(crate) fn readiness_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.conf.readiness_timeout_ms)
    }

    /// Records one completed request using the matched route template.
    fn record_request(&self, method: String, route: String, status: StatusCode, started: Instant) {
        self.metrics
            .record(method, route, status, started.elapsed());
    }
}

/// Returns success once the site has accepted requests.
pub(crate) async fn liveness() -> StatusCode {
    StatusCode::OK
}

/// Checks whether the site and its configured database are ready to serve traffic.
pub(crate) async fn readiness(State(site): State<Site>) -> StatusCode {
    if readiness_database(&site).await {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// Returns Prometheus text exposition for the current process.
pub(crate) async fn metrics(State(site): State<Site>) -> Response {
    let mut output = site.observability().render();
    output.push_str(&site.auth().render_metrics());
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        output,
    )
        .into_response()
}

/// Measures completed requests without recording user-controlled paths as labels.
pub(crate) async fn metrics_middleware(
    State(site): State<Site>,
    req: Request,
    next: Next,
) -> Response {
    if !site.observability().enabled() {
        return next.run(req).await;
    }
    let started = Instant::now();
    let method = req.method().as_str().to_owned();
    let route = matched_route(&req);
    site.observability().request_started();
    let response = next.run(req).await;
    site.observability()
        .record_request(method, route, response.status(), started);
    site.observability().request_finished();
    response
}

/// Checks the selected backend without imposing a database requirement in no-backend mode.
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
async fn readiness_database(site: &Site) -> bool {
    tokio::time::timeout(
        site.observability().readiness_timeout(),
        crate::db::sqlx::query("SELECT 1").execute(site.db().as_sqlx()),
    )
    .await
    .is_ok_and(|result| result.is_ok())
}

/// Backendless sites are ready once the runtime has started.
#[cfg(not(any(feature = "postgres", feature = "mysql", feature = "sqlite")))]
async fn readiness_database(_site: &Site) -> bool {
    true
}

/// Uses Axum's matched route template, with a single stable fallback for misses.
fn matched_route(request: &Request) -> String {
    request
        .extensions()
        .get::<MatchedPath>()
        .map(|path| path.as_str().to_owned())
        .unwrap_or_else(|| "<unmatched>".into())
}

/// Validates that enabled observability routes are absolute, distinct paths.
fn validate_paths(paths: &[(&str, &String)], errors: &mut Vec<ConfError>) {
    for (field, path) in paths {
        if !path.starts_with('/') || path.as_str() == "/" {
            errors.push(ConfError::InvalidValue {
                field: (*field).into(),
                reason: "must be an absolute non-root path".into(),
                expected: Some("a path such as /healthz".into()),
            });
        }
    }
    for (index, (_, path)) in paths.iter().enumerate() {
        if paths.iter().skip(index + 1).any(|(_, other)| other == path) {
            errors.push(ConfError::InvalidValue {
                field: "observability".into(),
                reason: "probe and metrics paths must be distinct".into(),
                expected: None,
            });
            return;
        }
    }
}

#[derive(Default)]
struct Metrics {
    in_flight: AtomicU64,
    panics: AtomicU64,
    routes: parking_lot::Mutex<BTreeMap<MetricKey, RouteMetrics>>,
}

impl Metrics {
    fn record(
        &self,
        method: String,
        route: String,
        status: StatusCode,
        duration: std::time::Duration,
    ) {
        let key = MetricKey {
            method,
            route,
            status: status_class(status),
        };
        let seconds = duration.as_secs_f64();
        let mut routes = self.routes.lock();
        routes
            .entry(key)
            .or_default()
            .record(seconds, status.is_server_error());
    }

    fn render(&self) -> String {
        let mut output = String::new();
        write_help(&mut output);
        let routes = self.routes.lock();
        for (key, values) in routes.iter() {
            write_route_metrics(&mut output, key, values);
        }
        let _ = writeln!(
            output,
            "vyuh_http_requests_in_flight {}",
            self.in_flight.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            output,
            "vyuh_http_panics_total {}",
            self.panics.load(Ordering::Relaxed)
        );
        output
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
struct MetricKey {
    method: String,
    route: String,
    status: &'static str,
}

#[derive(Default)]
struct RouteMetrics {
    requests: u64,
    errors: u64,
    duration_sum_seconds: f64,
    buckets: [u64; DURATION_BUCKETS.len()],
}

impl RouteMetrics {
    fn record(&mut self, seconds: f64, server_error: bool) {
        self.requests += 1;
        if server_error {
            self.errors += 1;
        }
        self.duration_sum_seconds += seconds;
        if seconds.is_sign_positive() {
            for (index, bucket) in DURATION_BUCKETS.iter().enumerate() {
                if seconds <= *bucket {
                    self.buckets[index] += 1;
                }
            }
        }
    }
}

fn write_help(output: &mut String) {
    let _ = writeln!(output, "# TYPE vyuh_http_requests_total counter");
    let _ = writeln!(
        output,
        "# TYPE vyuh_http_request_duration_seconds histogram"
    );
    let _ = writeln!(output, "# TYPE vyuh_http_errors_total counter");
    let _ = writeln!(output, "# TYPE vyuh_http_requests_in_flight gauge");
    let _ = writeln!(output, "# TYPE vyuh_http_panics_total counter");
}

fn write_route_metrics(output: &mut String, key: &MetricKey, values: &RouteMetrics) {
    let labels = metric_labels(key);
    let _ = writeln!(
        output,
        "vyuh_http_requests_total{{{labels}}} {}",
        values.requests
    );
    let _ = writeln!(
        output,
        "vyuh_http_errors_total{{{labels}}} {}",
        values.errors
    );
    for (index, bucket) in DURATION_BUCKETS.iter().enumerate() {
        let _ = writeln!(
            output,
            "vyuh_http_request_duration_seconds_bucket{{{labels},le=\"{bucket}\"}} {}",
            values.buckets[index]
        );
    }
    let _ = writeln!(
        output,
        "vyuh_http_request_duration_seconds_bucket{{{labels},le=\"+Inf\"}} {}",
        values.requests
    );
    let _ = writeln!(
        output,
        "vyuh_http_request_duration_seconds_sum{{{labels}}} {}",
        values.duration_sum_seconds
    );
    let _ = writeln!(
        output,
        "vyuh_http_request_duration_seconds_count{{{labels}}} {}",
        values.requests
    );
}

fn metric_labels(key: &MetricKey) -> String {
    format!(
        "method=\"{}\",route=\"{}\",status=\"{}\"",
        escape_label(&key.method),
        escape_label(&key.route),
        key.status
    )
}

fn escape_label(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn status_class(status: StatusCode) -> &'static str {
    match status.as_u16() / 100 {
        1 => "1xx",
        2 => "2xx",
        3 => "3xx",
        4 => "4xx",
        _ => "5xx",
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use axum::http::StatusCode;

    use super::{Observability, ObservabilityConf};

    /// Verifies that HTTP metrics retain route templates and never create a raw-path label.
    #[test]
    fn metrics_use_bounded_route_labels() {
        let observability = Observability::new(ObservabilityConf::production());
        observability.request_started();
        observability.record_request(
            "GET".to_string(),
            "/accounts/{account_id}".to_string(),
            StatusCode::OK,
            Instant::now() - Duration::from_millis(5),
        );
        observability.request_finished();
        observability.record_panic();

        let metrics = observability.render();
        assert!(metrics.contains("route=\"/accounts/{account_id}\""));
        assert!(metrics.contains("vyuh_http_panics_total 1"));
        assert!(!metrics.contains("path=\""));
    }
}
