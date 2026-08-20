use std::sync::atomic::{AtomicUsize, Ordering};

use axum::{
    extract::{OriginalUri, Path, Request},
    http::StatusCode,
};
use serde_json::{Value, json};
use vyuh::{
    Site, SiteConf, bundles,
    routes::{Html, Json, RouteConf},
    testing::TestSite,
};

static STRICT_CALLS: AtomicUsize = AtomicUsize::new(0);

#[bundles::route(path = "/items")]
async fn items() -> Json<Value> {
    Json(json!({ "kind": "items" }))
}

#[bundles::route(path = "/items/{id}")]
async fn item(Path(id): Path<String>) -> Json<Value> {
    Json(json!({ "id": id }))
}

#[bundles::route(path = "/docs/")]
async fn docs() -> Html<String> {
    Html("docs".to_owned())
}

#[bundles::route(path = "/files/{*path}", trim = false)]
async fn file(Path(path): Path<String>) -> Json<Value> {
    STRICT_CALLS.fetch_add(1, Ordering::Relaxed);
    Json(json!({ "path": path }))
}

#[bundles::route(path = "/strict-items/{id}", trim = false)]
async fn strict_item(Path(id): Path<String>) -> Json<Value> {
    Json(json!({ "id": id }))
}

#[bundles::route(path = "/")]
async fn root() -> Json<Value> {
    Json(json!({ "root": true }))
}

#[bundles::route(path = "/observed")]
async fn observed(request: Request) -> Json<Vec<String>> {
    let original = request
        .extensions()
        .get::<OriginalUri>()
        .map_or_else(|| request.uri().clone(), |original| original.0.clone());
    Json(vec![original.to_string(), request.uri().to_string()])
}

#[bundles::route(path = "/shared", method = "GET")]
async fn shared_get() -> Json<Value> {
    Json(json!({ "method": "GET" }))
}

#[bundles::route(path = "/shared", method = "POST")]
async fn shared_post() -> Json<Value> {
    Json(json!({ "method": "POST" }))
}

#[bundles::route(path = "/duplicate")]
async fn duplicate_slashless() -> Json<Value> {
    Json(json!({ "path": "slashless" }))
}

#[bundles::route(path = "/duplicate/")]
async fn duplicate_slashful() -> Json<Value> {
    Json(json!({ "path": "slashful" }))
}

fn bundle() -> bundles::Bundle {
    bundles::bundle! {
        items,
        item,
        docs,
        file,
        strict_item,
        observed,
        shared_get,
        shared_post,
    }
    .with_prefix("/api")
}

async fn site() -> vyuh::testing::TestSite {
    TestSite::new(
        Site::build(SiteConf::default().log_init(false), bundle())
            .await
            .unwrap(),
    )
}

/// Verifies slashless routes accept one alternate terminal slash without redirecting or redispatching.
#[tokio::test]
async fn slashless_routes_trim_once() {
    let site = site().await;

    site.get("/api/items").send().await.assert_ok();
    site.get("/api/items/").send().await.assert_ok();
    site.get("/api/items//")
        .send()
        .await
        .assert_status(StatusCode::NOT_FOUND);
}

/// Verifies terminal slash trimming occurs after a prefix and does not consume unrelated segments.
#[tokio::test]
async fn dynamic_routes_trim_after_prefixing() {
    let site = site().await;

    site.get("/api/items/42/")
        .send()
        .await
        .assert_json(StatusCode::OK, &json!({ "id": "42" }))
        .await;
    site.get("/api/items/42/extra/")
        .send()
        .await
        .assert_status(StatusCode::NOT_FOUND);
}

/// Verifies slashful declarations redirect their alternate form to the declared canonical URL.
#[tokio::test]
async fn slashful_routes_redirect_to_declared_path() {
    let site = site().await;

    site.get("/api/docs/").send().await.assert_ok();
    let response = site.get("/api/docs?page=1").send().await;
    assert_eq!(response.status(), StatusCode::PERMANENT_REDIRECT);
    assert_eq!(
        response
            .header("location")
            .and_then(|value| value.to_str().ok()),
        Some("/api/docs/?page=1")
    );
}

/// Verifies strict routes reject the alternate slash form without invoking their handler.
#[tokio::test]
async fn strict_routes_reject_trimmed_requests() {
    STRICT_CALLS.store(0, Ordering::Relaxed);
    let site = site().await;

    site.get("/api/files/image.png")
        .send()
        .await
        .assert_json(StatusCode::OK, &json!({ "path": "image.png" }))
        .await;
    site.get("/api/files/image.png/")
        .send()
        .await
        .assert_status(StatusCode::NOT_FOUND);
    assert_eq!(STRICT_CALLS.load(Ordering::Relaxed), 1);
    site.get("/api/strict-items/42").send().await.assert_ok();
    site.get("/api/strict-items/42/")
        .send()
        .await
        .assert_status(StatusCode::NOT_FOUND);
}

/// Verifies the root path is already canonical and dispatches without normalization.
#[tokio::test]
async fn root_is_never_redirected() {
    let site = TestSite::new(
        Site::build(
            SiteConf::default().log_init(false),
            bundles::bundle! { root },
        )
        .await
        .unwrap(),
    );

    site.get("/").send().await.assert_ok();
}

/// Verifies rewritten requests retain the public URI while exposing the normalized URI to handlers.
#[tokio::test]
async fn original_uri_is_preserved() {
    let site = site().await;

    site.get("/api/observed/?page=1")
        .send()
        .await
        .assert_json(
            StatusCode::OK,
            &json!(["/api/observed/?page=1", "/observed?page=1"]),
        )
        .await;
}

/// Verifies shared paths aggregate distinct methods while retaining implicit HEAD and structured 405 behavior.
#[tokio::test]
async fn shared_method_routes_remain_stable() {
    let site = site().await;

    site.get("/api/shared").send().await.assert_ok();
    site.post("/api/shared").send().await.assert_ok();
    site.request(axum::http::Method::HEAD, "/api/shared")
        .send()
        .await
        .assert_ok();
    site.patch("/api/shared")
        .send()
        .await
        .assert_status(StatusCode::METHOD_NOT_ALLOWED);
}

/// Verifies slash variants of one method are rejected as a controlled normalized collision.
#[tokio::test]
async fn normalized_duplicate_routes_are_rejected() {
    let result = Site::build(
        SiteConf::default(),
        bundles::bundle! { duplicate_slashless, duplicate_slashful },
    )
    .await;

    assert!(matches!(result, Err(vyuh::SiteError::BundleError(_))));
}

/// Verifies direct construction rejects strict configuration on a slashful declaration.
#[tokio::test]
async fn strict_slashful_route_is_rejected() {
    async fn direct() -> Json<Value> {
        Json(json!({}))
    }
    let bundle = bundles::bundle([bundles::route(
        direct,
        RouteConf {
            name: "direct".into(),
            path: "/direct/".into(),
            trim: false,
            ..RouteConf::default()
        },
    )]);

    let result = Site::build(SiteConf::default(), bundle).await;
    assert!(matches!(result, Err(vyuh::SiteError::BundleError(_))));
}
