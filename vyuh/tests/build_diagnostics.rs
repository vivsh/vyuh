//! Site-assembly diagnostics use real validation paths without starting workers.

use vyuh::{Site, SiteConf, auth::AuthUser, bundles, routes::RouteConf};

async fn endpoint() -> vyuh::routes::Json<()> {
    vyuh::routes::Json(())
}
async fn protected(_user: AuthUser) -> vyuh::routes::Json<()> {
    vyuh::routes::Json(())
}

fn route(name: &'static str, path: &'static str) -> RouteConf {
    RouteConf {
        name: name.into(),
        path: path.into(),
        ..RouteConf::default()
    }
}

/// A normalized path collision identifies the method and remedy without panicking.
#[tokio::test]
async fn collision_is_actionable() {
    let bundle = bundles::bundle([
        bundles::route(endpoint, route("first", "/items")),
        bundles::route(endpoint, route("second", "/items/")),
    ]);
    let error = Site::build(SiteConf::default().log_init(false), bundle)
        .await
        .unwrap_err();
    let output = error.to_string();
    assert!(output.contains("GET /items"));
    assert!(output.contains("Keep one registration"));
    assert!(!output.contains("ErrorList"));
}

/// Strict audience validation recommends declaring policy rather than removing auth.
#[tokio::test]
async fn missing_audience_is_actionable() {
    let conf = SiteConf::default()
        .log_init(false)
        .auth(vyuh::auth::AuthConf::default().require_explicit_audiences());
    let error = Site::build(
        conf,
        bundles::bundle([bundles::route(protected, route("protected", "/private"))]),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("bundles::conf().audience"));
    assert!(error.to_string().contains("/private"));
}

/// Cache registry errors survive configuration validation as typed sources.
#[tokio::test]
async fn cache_validation_keeps_source() {
    let conf = SiteConf::default()
        .log_init(false)
        .cache(vyuh::cache::CacheConf::empty());
    let error = Site::build(conf, bundles::bundle! {}).await.unwrap_err();
    assert!(error.to_string().contains("CacheConf::default_provider"));
    let vyuh::SiteError::ConfError(error) = error else {
        panic!("expected accumulated configuration errors");
    };
    assert!(
        error
            .to_string()
            .contains("cache configuration has no default provider")
    );
}
