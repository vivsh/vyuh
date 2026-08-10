use axum::http::{StatusCode, header};
use tower_http::cors::CorsLayer;

use super::*;
use crate::routes::CorsMiddleware;

/// Verifies all rendered console pages work with the configured CORS pipeline and assets.
#[tokio::test]
async fn console_html_pages_and_assets_work() -> Result<(), String> {
    let conf = SiteConf::default()
        .log_init(false)
        .console(ConsoleConf::default().enabled(true));
    let bundle = inspection_bundle().layer(CorsMiddleware::new(CorsLayer::permissive()));
    let site = Site::build(conf, bundle)
        .await
        .map_err(|error| error.to_string())?;
    let ping_id = operation_id(&site, |operation| operation.name == "ping", "ping route")?;
    let signal_id = operation_id(
        &site,
        |operation| operation.kind == crate::OperationKind::Signal,
        "invoice signal",
    )?;
    let console_route_id = operation_id(
        &site,
        |operation| operation.name == "console_routes",
        "console route",
    )?;
    let cookie = console_cookie(&site)
        .await
        .map_err(|error| error.to_string())?;
    let stylesheet = site
        .console_urls()
        .map(|urls| urls.stylesheet_path.clone())
        .ok_or_else(|| "console runtime was not initialized".to_string())?;
    let client = TestSite::new(site);

    assert_page(&client, "/console", &cookie, "System Info").await?;
    assert_runtime(&client, &cookie).await?;
    assert_routes(&client, &cookie, ping_id, console_route_id).await?;
    assert_signal(&client, &cookie, signal_id).await?;
    assert_tasks(&client, &cookie).await?;
    assert_config(&client, &cookie).await?;
    assert_openapi(&client, &cookie).await?;
    assert_stylesheet(&client, &stylesheet).await
}

fn operation_id(
    site: &Site,
    predicate: impl Fn(&crate::Operation) -> bool,
    label: &str,
) -> Result<crate::OperationId, String> {
    site.operations()
        .list()
        .find(|operation| predicate(operation))
        .map(|operation| operation.id)
        .ok_or_else(|| format!("{label} was not registered"))
}

async fn assert_page(
    client: &TestSite,
    path: &str,
    cookie: &str,
    text: &str,
) -> Result<(), String> {
    let response = client
        .get(path)
        .header(header::COOKIE.as_str(), cookie)
        .send()
        .await;
    if response.status() != StatusCode::OK || !response.text().await.contains(text) {
        return Err(format!("console page '{path}' did not render '{text}'"));
    }
    Ok(())
}

async fn assert_runtime(client: &TestSite, cookie: &str) -> Result<(), String> {
    let response = client
        .get("/console/runtime")
        .header(header::COOKIE.as_str(), cookie)
        .send()
        .await;
    let status = response.status();
    let body = response.text().await;
    let expected = [
        "System Info",
        "aria-current=\"page\"",
        "System Environment",
        "Resource Usage",
        "Build Information",
    ];
    if status != StatusCode::OK || expected.iter().any(|text| !body.contains(text)) {
        return Err("console runtime page did not render its expected sections".to_string());
    }
    Ok(())
}

async fn assert_routes(
    client: &TestSite,
    cookie: &str,
    ping_id: crate::OperationId,
    console_route_id: crate::OperationId,
) -> Result<(), String> {
    assert_route_list(client, cookie).await?;
    assert_route_inspector(client, cookie, ping_id, console_route_id).await
}

async fn assert_route_list(client: &TestSite, cookie: &str) -> Result<(), String> {
    let filtered = client
        .get("/console/routes?q=ping")
        .header(header::COOKIE.as_str(), cookie)
        .send()
        .await;
    if filtered.status() != StatusCode::OK || !filtered.text().await.contains("ping") {
        return Err("filtered routes page did not render ping".to_string());
    }
    let routes = client
        .get("/console/routes")
        .header(header::COOKIE.as_str(), cookie)
        .send()
        .await;
    let routes = routes.text().await;
    if routes.contains("console_routes") || routes.contains("console_api_status") {
        return Err("console routes leaked into the application route page".to_string());
    }
    let api = client
        .get("/console/api/routes")
        .header(header::COOKIE.as_str(), cookie)
        .send()
        .await;
    if api.status() != StatusCode::OK || !api.text().await.contains("ping") {
        return Err("route API did not render ping".to_string());
    }
    Ok(())
}

async fn assert_route_inspector(
    client: &TestSite,
    cookie: &str,
    ping_id: crate::OperationId,
    console_route_id: crate::OperationId,
) -> Result<(), String> {
    let console_detail = client
        .get(&format!("/console/api/routes?selected={console_route_id}"))
        .header(header::COOKIE.as_str(), cookie)
        .send()
        .await;
    if console_detail.status() != StatusCode::OK {
        return Err("route API did not handle a console selection".to_string());
    }
    let selected = client
        .get(&format!("/console/routes?selected={ping_id}"))
        .header(header::COOKIE.as_str(), cookie)
        .send()
        .await;
    let status = selected.status();
    let body = selected.text().await;
    let expected = [
        "aria-selected=\"true\"",
        "Methods",
        "Input",
        "Output",
        "Metadata",
    ];
    if status != StatusCode::OK || expected.iter().any(|text| !body.contains(text)) {
        return Err("route inspector did not render selected metadata".to_string());
    }
    Ok(())
}

async fn assert_signal(
    client: &TestSite,
    cookie: &str,
    signal_id: crate::OperationId,
) -> Result<(), String> {
    let response = client
        .get(&format!("/console/signals?selected={signal_id}"))
        .header(header::COOKIE.as_str(), cookie)
        .send()
        .await;
    let status = response.status();
    let body = response.text().await;
    if status != StatusCode::OK
        || !body.contains("InvoiceSignal")
        || !body.contains("invoice_id")
        || !body.contains("Raw JSON schema")
    {
        return Err("signal inspector did not render its typed payload".to_string());
    }
    Ok(())
}

async fn assert_tasks(client: &TestSite, cookie: &str) -> Result<(), String> {
    let response = client
        .get("/console/tasks")
        .header(header::COOKIE.as_str(), cookie)
        .send()
        .await;
    let status = response.status();
    let body = response.text().await;
    let expected = ["No task records yet.", "name=\"per_page\"", "100 per page"];
    if status != StatusCode::OK
        || expected.iter().any(|text| !body.contains(text))
        || body.contains("api/tasks")
    {
        return Err(format!(
            "task page did not preserve its HTML-only inspection contract: status={status}, expected={:?}, api_tasks={}",
            expected
                .iter()
                .filter(|text| !body.contains(**text))
                .collect::<Vec<_>>(),
            body.contains("api/tasks")
        ));
    }
    Ok(())
}

async fn assert_config(client: &TestSite, cookie: &str) -> Result<(), String> {
    let response = client
        .get("/console/conf")
        .header(header::COOKIE.as_str(), cookie)
        .send()
        .await;
    let status = response.status();
    let body = response.text().await;
    let expected = [
        "Configuration",
        "Authentication",
        "HTTP Pipeline",
        "&lt;redacted&gt;",
    ];
    let forbidden = [
        "Open raw",
        "Download as JSON",
        "api/conf",
        ">01<",
        "secret_key",
        "DATABASE_URL",
    ];
    if status != StatusCode::OK
        || expected.iter().any(|text| !body.contains(text))
        || forbidden.iter().any(|text| body.contains(text))
    {
        return Err("configuration page did not preserve its redacted HTML contract".to_string());
    }
    Ok(())
}

async fn assert_openapi(client: &TestSite, cookie: &str) -> Result<(), String> {
    let response = client
        .get("/console/openapi")
        .header(header::COOKIE.as_str(), cookie)
        .send()
        .await;
    let status = response.status();
    let body = response.text().await;
    let expected = [
        "OpenAPI",
        "vyuh-console-sidebar",
        "redoc",
        "spec-url",
        "is-redoc",
    ];
    let forbidden = ["Raw JSON", "Application routes only", "console_routes"];
    if status != StatusCode::OK
        || expected.iter().any(|text| !body.contains(text))
        || forbidden.iter().any(|text| body.contains(text))
    {
        return Err("OpenAPI console page did not preserve its application-only view".to_string());
    }
    Ok(())
}

async fn assert_stylesheet(client: &TestSite, stylesheet: &str) -> Result<(), String> {
    let response = client.get(stylesheet).send().await;
    if response.status() != StatusCode::OK
        || response
            .header(header::CONTENT_TYPE.as_str())
            .and_then(|value| value.to_str().ok())
            != Some("text/css")
    {
        return Err("console stylesheet did not resolve from the site asset registry".to_string());
    }
    Ok(())
}
