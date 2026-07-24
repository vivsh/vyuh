//! Production-profile integration coverage.

use std::time::Duration;

use axum::{
    body::Body,
    http::{StatusCode, header},
};
use vyuh::{
    Site, SiteConf,
    auth::{AuthConf, CookieConf},
    bundles,
    console::ConsoleConf,
    middlewares::{CorsConf, HttpConf, TimeoutConf},
    routes::{BodyBytes, Html, Json},
    testing::TestSite,
};

#[bundles::route(path = "/echo", method = "POST")]
async fn echo(BodyBytes(body): BodyBytes) -> Html<String> {
    Html(body.len().to_string())
}

#[bundles::route(path = "/slow")]
async fn slow() -> Json<&'static str> {
    tokio::time::sleep(Duration::from_millis(20)).await;
    Json("finished")
}

fn production_bundle() -> bundles::Bundle {
    bundles::bundle! {
        echo,
        slow,
    }
}

/// Verifies that the production profile exposes the standard probes and bounded metrics output.
#[tokio::test]
async fn production_profile_exposes_probes_metrics_and_security_headers() {
    let site = Site::build(SiteConf::production().log_init(false), production_bundle())
        .await
        .expect("production profile should build with its secure defaults");
    let client = TestSite::new(site);

    client
        .get("/healthz")
        .send()
        .await
        .assert_status(StatusCode::OK);
    client
        .get("/readyz")
        .send()
        .await
        .assert_status(StatusCode::OK);

    let response = client.get("/metrics").send().await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .header(header::X_CONTENT_TYPE_OPTIONS.as_str())
            .and_then(|value| value.to_str().ok()),
        Some("nosniff")
    );
    let metrics = response.text().await;
    assert!(metrics.contains("vyuh_http_requests_total"));
    assert!(metrics.contains("route=\"/healthz\""));
    assert!(!metrics.contains("path=\""));
}

/// Verifies that production body limits and request timeouts are applied at the site boundary.
#[tokio::test]
async fn production_profile_enforces_body_limits_and_timeouts() {
    let site = Site::build(
        SiteConf::production().log_init(false).http(HttpConf {
            timeout: TimeoutConf {
                enabled: true,
                timeout_ms: 1,
            },
            ..HttpConf::production()
        }),
        production_bundle(),
    )
    .await
    .expect("production timeout configuration should build");
    let client = TestSite::new(site);

    client
        .post("/echo")
        .header("content-length", "2097153")
        .body(Body::from("x"))
        .send()
        .await
        .assert_status(StatusCode::PAYLOAD_TOO_LARGE);
    client
        .get("/slow")
        .send()
        .await
        .assert_status(StatusCode::GATEWAY_TIMEOUT);
}

/// Verifies that production validation rejects permissive CORS and unsafe browser sessions.
#[test]
fn production_profile_rejects_permissive_cors_and_insecure_cookies() {
    let invalid = SiteConf::production()
        .http(HttpConf {
            cors: CorsConf {
                enabled: true,
                permissive: true,
            },
            ..HttpConf::production()
        })
        .console(ConsoleConf::production().enabled(true).secure_cookie(false))
        .auth(AuthConf::default().access_cookie(CookieConf::new("access").secure(false)));

    let error = invalid
        .validate()
        .expect_err("unsafe production settings must be rejected");
    let message = error.to_string();
    assert!(message.contains("http.cors.permissive"));
    assert!(message.contains("console.secure_cookie"));
    assert!(message.contains("auth.access_cookie"));
}

/// Verifies that observability paths cannot overlap before a site is started.
#[test]
fn production_profile_rejects_colliding_observability_paths() {
    let mut invalid = SiteConf::production();
    invalid.observability.metrics_path = "/healthz".to_string();

    let error = invalid
        .validate()
        .expect_err("colliding probes must be rejected before startup");
    assert!(
        error
            .to_string()
            .contains("probe and metrics paths must be distinct")
    );
}
