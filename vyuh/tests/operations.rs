use axum::{Extension, Router, body::Body, http::Request, routing::get};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tower::ServiceExt;
use vyuh::{
    Data, Error, OperationId, OperationKind, Site, SiteConf, bundles,
    commands::CommandConf,
    routes::{HttpMethod, Json, StatusCode},
    testing::TestSite,
};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct RuntimeInput {
    value: usize,
}

#[bundles::route(path = "/runtime/{id}", method = "GET")]
async fn runtime_route(
    operation_id: OperationId,
    Extension(extension_id): Extension<OperationId>,
) -> Json<bool> {
    Json(operation_id == extension_id)
}

#[bundles::task(name = "runtime_task")]
async fn runtime_task(_operation_id: OperationId, _input: Data<RuntimeInput>) {}

#[bundles::signal]
async fn runtime_signal(_operation_id: OperationId, _input: Data<RuntimeInput>) {}

#[bundles::signal]
async fn second_runtime_signal(_operation_id: OperationId, _input: Data<RuntimeInput>) {}

async fn runtime_command(
    _operation_id: OperationId,
    _input: Data<RuntimeInput>,
) -> Result<(), Error> {
    Ok(())
}

async fn raw_route(_operation_id: OperationId) -> StatusCode {
    StatusCode::OK
}

fn operation_bundle() -> bundles::Bundle {
    let command = bundles::command(runtime_command, CommandConf::new("runtime-command"));
    bundles::bundle! { runtime_route, runtime_task, runtime_signal, second_runtime_signal }
        .merge(bundles::bundle([command]))
}

async fn operation_site() -> Result<Site, vyuh::SiteError> {
    Site::build(SiteConf::default().log_init(false), operation_bundle()).await
}

/// Verifies reversal, method-aware resolution, dynamic paths, and query removal.
#[tokio::test]
async fn routes_reverse_and_resolve() -> Result<(), String> {
    let site = operation_site().await.map_err(|error| error.to_string())?;
    let routes = site.routes();
    assert_eq!(
        routes.reverse_url("runtime_route", &[("id", "a/b c")]),
        Some("/runtime/a%2Fb%20c".to_string())
    );
    let id = routes
        .resolve_url(HttpMethod::GET, "/runtime/42?view=full#details")
        .ok_or_else(|| "route did not resolve".to_string())?;
    assert_eq!(
        site.operations().find(id).map(|value| value.name.as_str()),
        Some("runtime_route")
    );
    assert_eq!(routes.resolve_url(HttpMethod::POST, "/runtime/42"), None);
    assert_eq!(routes.reverse_url("runtime_route", &[]), None);
    Ok(())
}

/// Verifies canonical IDs serialize, parse, and find their exact operation.
#[tokio::test]
async fn operation_ids_round_trip() -> Result<(), String> {
    let site = operation_site().await.map_err(|error| error.to_string())?;
    let id = site
        .routes()
        .resolve_url(HttpMethod::GET, "/runtime/7")
        .ok_or_else(|| "route did not resolve".to_string())?;
    let encoded = serde_json::to_string(&id).map_err(|error| error.to_string())?;
    let decoded: OperationId = serde_json::from_str(&encoded).map_err(|error| error.to_string())?;
    let parsed = id
        .to_string()
        .parse::<OperationId>()
        .map_err(|error| error.to_string())?;
    assert_eq!(decoded, id);
    assert_eq!(parsed, id);
    assert_eq!(site.operations().find(id).map(|value| value.id), Some(id));
    Ok(())
}

/// Verifies routes, tasks, signals, and commands all contribute operation metadata.
#[tokio::test]
async fn all_handler_kinds_are_listed() -> Result<(), String> {
    let site = operation_site().await.map_err(|error| error.to_string())?;
    let operations = site.operations();
    for kind in [
        OperationKind::Route,
        OperationKind::Task,
        OperationKind::Signal,
        OperationKind::Command,
    ] {
        assert!(operations.list().any(|operation| operation.kind == kind));
    }
    assert!(
        operations
            .list()
            .any(|operation| operation.kind == OperationKind::Command && operation.hidden)
    );
    let signal_ids = operations
        .list()
        .filter(|operation| operation.kind == OperationKind::Signal)
        .map(|operation| operation.id)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(signal_ids.len(), 2);
    Ok(())
}

/// Verifies the direct extractor and Axum extension expose the same route identity.
#[tokio::test]
async fn route_injects_operation_id() -> Result<(), String> {
    let fixture = TestSite::new(operation_site().await.map_err(|error| error.to_string())?);
    fixture
        .get("/runtime/42")
        .send()
        .await
        .assert_json(StatusCode::OK, &true)
        .await;
    Ok(())
}

/// Verifies using the extractor outside a Vyuh operation fails as framework wiring.
#[tokio::test]
async fn missing_route_extension_is_internal_error() -> Result<(), String> {
    let site = operation_site().await.map_err(|error| error.to_string())?;
    let app = Router::new().route("/raw", get(raw_route)).with_state(site);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/raw")
                .body(Body::empty())
                .map_err(|error| error.to_string())?,
        )
        .await
        .map_err(|error| error.to_string())?;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    Ok(())
}
