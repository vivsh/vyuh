mod api;
mod auth;
mod conf;
mod middleware;
mod pages;
mod query;
mod schema_view;
mod status;
mod types;

use std::{sync::OnceLock, time::Duration};

use rust_silos::{Silo, embed_silo};

use crate::{Site, bundles, embed, routes::Methods};

pub use auth::{ConsoleRole, ConsoleUser};
pub use conf::{ConsoleBootstrapMode, ConsoleConf};

pub(crate) use auth::ConsoleRuntime;

const WEB_ASSETS: Silo = embed_silo!("web", force = true);
const FALLBACK_STYLESHEET_PATH: &str = "/assets/css/vyuh.css";

pub(crate) fn bundle(conf: &ConsoleConf) -> crate::bundles::Bundle {
    web_assets()
        .merge(home_routes(conf))
        .merge(routes().with_prefix(&conf.path))
        .with_owning_bundle_id()
}

fn web_assets() -> crate::bundles::Bundle {
    bundles::bundle([bundles::asset_dir(embed::Dir::new(WEB_ASSETS.clone()))])
}

pub(crate) fn stylesheet_path() -> &'static str {
    static PATH: OnceLock<String> = OnceLock::new();
    PATH.get_or_init(resolve_stylesheet_path).as_str()
}

pub(crate) fn redacted_config(site: &Site) -> types::ConfigOut {
    types::ConfigOut::from_site(site)
}

fn resolve_stylesheet_path() -> String {
    read_stylesheet_name()
        .map(|name| format!("/assets/css/{name}"))
        .unwrap_or_else(|| FALLBACK_STYLESHEET_PATH.to_string())
}

fn read_stylesheet_name() -> Option<String> {
    let file = embed::Dir::new(WEB_ASSETS.clone()).get_file("public/css/manifest.json")?;
    let bytes = file.read_bytes_sync().ok()?;
    let manifest: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    manifest
        .get("vyuh.css")
        .and_then(|value| value.as_str())
        .map(str::to_owned)
}

fn home_routes(conf: &ConsoleConf) -> crate::bundles::Bundle {
    bundles::bundle([bundles::route(
        pages::overview,
        crate::routes::RouteConf {
            name: "console_home".into(),
            methods: Methods::GET,
            path: conf.path.clone().into(),
            slash: None,
        },
    )])
}

fn routes() -> crate::bundles::Bundle {
    macro_rules! route {
        ($name:literal, $path:literal, $methods:expr, $handler:path $(,)?) => {
            bundles::route(
                $handler,
                crate::routes::RouteConf {
                    name: $name.into(),
                    methods: $methods,
                    path: $path.into(),
                    slash: None,
                },
            )
        };
    }

    bundles::bundle([
        route!(
            "console_overview",
            "/overview",
            Methods::GET,
            pages::overview,
        ),
        route!("console_runtime", "/runtime", Methods::GET, pages::runtime),
        route!(
            "console_login_page",
            "/login-page",
            Methods::GET,
            pages::login,
        ),
        route!("console_login", "/login", Methods::GET, api::login),
        route!("console_logout", "/api/logout", Methods::POST, api::logout),
        route!(
            "console_session",
            "/api/session",
            Methods::GET,
            api::session,
        ),
        route!(
            "console_operations",
            "/operations",
            Methods::GET,
            pages::operations,
        ),
        route!(
            "console_operation_detail",
            "/operations/{id}",
            Methods::GET,
            pages::operation_detail,
        ),
        route!("console_tasks", "/tasks", Methods::GET, pages::tasks),
        route!(
            "console_task_detail",
            "/tasks/{id}",
            Methods::GET,
            pages::task_detail,
        ),
        route!("console_conf", "/conf", Methods::GET, pages::conf),
        route!("console_openapi", "/openapi", Methods::GET, pages::openapi),
        route!(
            "console_api_operations",
            "/api/operations",
            Methods::GET,
            api::operations,
        ),
        route!(
            "console_api_operation_detail",
            "/api/operations/{id}",
            Methods::GET,
            api::operation_detail,
        ),
        route!("console_api_tasks", "/api/tasks", Methods::GET, api::tasks),
        route!(
            "console_api_task_detail",
            "/api/tasks/{id}",
            Methods::GET,
            api::task_detail,
        ),
        route!(
            "console_api_status",
            "/api/status",
            Methods::GET,
            api::status,
        ),
        route!("console_api_conf", "/api/conf", Methods::GET, api::conf),
        route!(
            "console_api_openapi",
            "/api/openapi",
            Methods::GET,
            api::openapi,
        ),
        route!(
            "console_not_found",
            "/{*path}",
            Methods::GET,
            pages::not_found_page,
        ),
    ])
}

pub(crate) fn runtime(conf: &ConsoleConf, bundle_id: uuid::Uuid) -> Option<ConsoleRuntime> {
    conf.enabled.then(|| {
        ConsoleRuntime::new(
            Duration::from_secs(conf.bootstrap_token_ttl_seconds),
            bundle_id,
        )
    })
}

pub(crate) fn maybe_print_bootstrap_url(site: &Site) {
    let conf = &site.conf().console;
    if !conf.enabled || !should_print(conf, &site.conf().host) {
        return;
    }
    let Some(runtime) = site.console_runtime() else {
        return;
    };
    let Some(token) = runtime.bootstrap_token() else {
        return;
    };
    println!(
        "Vyuh console enabled:\nhttp://{}:{}{}/login?token={}\nToken expires in {} seconds.",
        site.conf().host,
        site.conf().port,
        conf.path,
        token,
        conf.bootstrap_token_ttl_seconds
    );
}

fn should_print(conf: &ConsoleConf, host: &str) -> bool {
    match conf.print_bootstrap_url {
        ConsoleBootstrapMode::Always => true,
        ConsoleBootstrapMode::Disabled => false,
        ConsoleBootstrapMode::LocalOnly => matches!(host, "localhost" | "127.0.0.1" | "::1"),
    }
}

#[cfg(test)]
mod tests {
    use axum::http::{StatusCode, header};
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};

    #[cfg(feature = "cors")]
    use crate::routes::CorsMiddleware;
    use crate::{
        Data, Site, SiteConf, bundles,
        console::ConsoleConf,
        middlewares::{CorsConf, HttpConf},
        routes::{Json, Methods, RouteConf},
        testing::TestSite,
    };
    #[cfg(feature = "cors")]
    use tower_http::cors::CorsLayer;

    async fn ping() -> Json<&'static str> {
        Json("pong")
    }

    #[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
    struct InvoiceSignal {
        invoice_id: String,
        amount: f64,
    }

    async fn invoice_signal() -> Data<InvoiceSignal> {
        Data(std::sync::Arc::new(InvoiceSignal {
            invoice_id: "inv_001".to_string(),
            amount: 42.0,
        }))
    }

    #[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
    struct ConsoleTaskJob {
        message: String,
    }

    #[bundles::task(name = "console_test_task")]
    async fn console_test_task(Data(job): Data<ConsoleTaskJob>) {
        println!("console task test: {}", job.message);
    }

    fn app_bundle() -> crate::bundles::Bundle {
        bundles::bundle([
            bundles::route(
                ping,
                RouteConf {
                    name: "ping".into(),
                    methods: Methods::GET,
                    path: "/ping".into(),
                    slash: None,
                },
            ),
            bundles::route(
                invoice_signal,
                RouteConf {
                    name: "invoice_signal".into(),
                    methods: Methods::GET,
                    path: "/invoice-signal".into(),
                    slash: None,
                },
            ),
        ])
    }

    fn task_app_bundle() -> crate::bundles::Bundle {
        app_bundle().merge(bundles::bundle! {
            console_test_task,
        })
    }

    #[tokio::test]
    async fn disabled_console_mounts_no_routes() {
        let site = Site::build(
            SiteConf::default()
                .log_init(false)
                .console(ConsoleConf::default().enabled(false)),
            app_bundle(),
        )
        .await
        .unwrap();
        let client = TestSite::new(site);

        client
            .get("/console/api/status")
            .send()
            .await
            .assert_status(StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn console_local_debug_allows_direct_access() {
        let conf = SiteConf::default().log_init(false);
        let site = Site::build(conf, app_bundle()).await.unwrap();
        let client = TestSite::new(site);

        let status = client.get("/console/api/status").send().await;
        assert_eq!(status.status(), StatusCode::OK);

        let session = client.get("/console/api/session").send().await;
        assert_eq!(session.status(), StatusCode::OK);
        let session = session.text().await;
        assert!(session.contains("local-debug"));

        let missing = client.get("/console/missing").send().await;
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        let missing = missing.text().await;
        assert!(missing.contains("Console page not found"));
    }

    #[tokio::test]
    async fn console_bootstrap_cookie_authenticates_api() {
        let conf = SiteConf::default()
            .host("example.com")
            .log_init(false)
            .http(HttpConf {
                cors: CorsConf {
                    enabled: true,
                    permissive: true,
                },
                ..HttpConf::default()
            })
            .console(ConsoleConf::default().enabled(true));
        let site = Site::build(conf, app_bundle()).await.unwrap();
        let token = site
            .console_runtime()
            .and_then(|runtime| runtime.bootstrap_token())
            .unwrap();
        let client = TestSite::new(site);

        client
            .get("/console/api/status")
            .send()
            .await
            .assert_status(StatusCode::FORBIDDEN);
        let forbidden = client.get("/console/api/conf").send().await;
        assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);
        let forbidden = forbidden.text().await;
        assert!(forbidden.contains("Console access denied"));
        let forbidden_page = client.get("/console/overview").send().await;
        assert_eq!(forbidden_page.status(), StatusCode::FORBIDDEN);
        let forbidden_page = forbidden_page.text().await;
        assert!(forbidden_page.contains("Console access denied"));
        client
            .get("/console/api/openapi")
            .send()
            .await
            .assert_status(StatusCode::FORBIDDEN);

        let login = client
            .get(&format!("/console/login?token={token}"))
            .send()
            .await;
        assert_eq!(login.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            login
                .header(header::LOCATION.as_str())
                .and_then(|value| value.to_str().ok()),
            Some("/console")
        );
        client
            .get(&format!("/console/login?token={token}"))
            .send()
            .await
            .assert_status(StatusCode::UNAUTHORIZED);
        let cookie = login
            .header(header::SET_COOKIE.as_str())
            .unwrap()
            .to_str()
            .unwrap();
        assert!(cookie.contains("Max-Age=28800"));
        let cookie = cookie.split(';').next().unwrap().to_string();

        client
            .get("/console/api/operations?kind=route&q=ping")
            .header(header::COOKIE.as_str(), &cookie)
            .send()
            .await
            .assert_ok();

        let operations = client
            .get("/console/api/operations?kind=route&q=ping")
            .header(header::COOKIE.as_str(), &cookie)
            .send()
            .await;
        assert_eq!(operations.status(), StatusCode::OK);
        let operations = operations.text().await;
        assert!(operations.contains("\"middleware\""));
        assert!(operations.contains("\"request_id\""));
        assert!(operations.contains("\"cors\""));
        assert!(operations.contains("\"scope\":\"site\""));

        let conf = client
            .get("/console/api/conf")
            .header(header::COOKIE.as_str(), &cookie)
            .send()
            .await;
        assert_eq!(conf.status(), StatusCode::OK);
        let conf = conf.text().await;
        assert!(conf.contains("\"url\":\"<redacted>\""));
        assert!(conf.contains("\"shutdown_grace_period_ms\":10000"));
        assert!(!conf.contains("secret_key"));
        assert!(!conf.contains("DATABASE_URL"));
        assert!(!conf.contains(token.as_str()));

        let openapi = client
            .get("/console/api/openapi")
            .header(header::COOKIE.as_str(), &cookie)
            .send()
            .await;
        assert_eq!(openapi.status(), StatusCode::OK);
        let openapi = openapi.text().await;
        assert!(openapi.contains("\"/ping\""));
        assert!(!openapi.contains("/console"));
        assert!(!openapi.contains("console_operations"));
        assert!(!openapi.contains("request_id"));
        assert!(!openapi.contains("catch_panic"));
        assert!(!openapi.contains("\"origin\""));
    }

    #[cfg(feature = "cors")]
    #[tokio::test]
    async fn console_html_pages_and_assets_work() {
        let conf = SiteConf::default()
            .log_init(false)
            .console(ConsoleConf::default().enabled(true));
        let bundle = app_bundle().layer(CorsMiddleware::new(CorsLayer::permissive()));
        let site = Site::build(conf, bundle).await.unwrap();
        let ping_id = site
            .iter_operations()
            .find(|op| op.name == "ping")
            .map(|op| op.id)
            .unwrap();
        let invoice_signal_id = site
            .iter_operations()
            .find(|op| op.name == "invoice_signal")
            .map(|op| op.id)
            .unwrap();
        let console_operation_id = site
            .iter_operations()
            .find(|op| op.name == "console_operations")
            .map(|op| op.id)
            .unwrap();
        let token = site
            .console_runtime()
            .and_then(|runtime| runtime.bootstrap_token())
            .unwrap();
        let client = TestSite::new(site);

        let login = client
            .get(&format!("/console/login?token={token}"))
            .send()
            .await;
        assert_eq!(login.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            login
                .header(header::LOCATION.as_str())
                .and_then(|value| value.to_str().ok()),
            Some("/console")
        );
        let cookie = login
            .header(header::SET_COOKIE.as_str())
            .unwrap()
            .to_str()
            .unwrap();
        assert!(cookie.contains("Max-Age=28800"));
        let cookie = cookie.split(';').next().unwrap().to_string();

        let overview = client
            .get("/console")
            .header(header::COOKIE.as_str(), &cookie)
            .send()
            .await;
        assert_eq!(overview.status(), StatusCode::OK, "home page failed");
        let overview = overview.text().await;
        assert!(overview.contains("Overview"));

        let overview = client
            .get("/console/overview")
            .header(header::COOKIE.as_str(), &cookie)
            .send()
            .await;
        assert_eq!(overview.status(), StatusCode::OK, "overview page failed");
        let overview = overview.text().await;
        assert!(overview.contains("Overview"));

        let runtime = client
            .get("/console/runtime")
            .header(header::COOKIE.as_str(), &cookie)
            .send()
            .await;
        assert_eq!(runtime.status(), StatusCode::OK, "runtime page failed");
        let runtime = runtime.text().await;
        assert!(runtime.contains("System Info"));
        assert!(runtime.contains("aria-current=\"page\""));
        assert!(runtime.contains("System Environment"));
        assert!(runtime.contains("Resource Usage"));
        assert!(runtime.contains("Build Information"));
        assert!(!runtime.contains("api/status"));

        let operations = client
            .get("/console/operations?kind=route&q=ping")
            .header(header::COOKIE.as_str(), &cookie)
            .send()
            .await;
        assert_eq!(
            operations.status(),
            StatusCode::OK,
            "operations page failed"
        );
        let operations = operations.text().await;
        assert!(operations.contains("ping"));
        assert!(!operations.contains("api/operations"));

        let operations = client
            .get("/console/operations")
            .header(header::COOKIE.as_str(), &cookie)
            .send()
            .await;
        assert_eq!(
            operations.status(),
            StatusCode::OK,
            "default operations page failed"
        );
        let operations = operations.text().await;
        assert!(operations.contains("ping"));
        assert!(!operations.contains("value=\"none\""));
        assert!(!operations.contains("console_operations"));
        assert!(!operations.contains("console_api_status"));

        let api_operations = client
            .get("/console/api/operations")
            .header(header::COOKIE.as_str(), &cookie)
            .send()
            .await;
        assert_eq!(
            api_operations.status(),
            StatusCode::OK,
            "api operations page failed"
        );
        let api_operations = api_operations.text().await;
        assert!(api_operations.contains("ping"));
        assert!(!api_operations.contains("console_operations"));

        let console_detail = client
            .get(&format!("/console/api/operations/{console_operation_id}"))
            .header(header::COOKIE.as_str(), &cookie)
            .send()
            .await;
        assert_eq!(console_detail.status(), StatusCode::NOT_FOUND);

        let selected = client
            .get(&format!("/console/operations?selected={ping_id}"))
            .header(header::COOKIE.as_str(), &cookie)
            .send()
            .await;
        assert_eq!(
            selected.status(),
            StatusCode::OK,
            "selected operation page failed"
        );
        let selected = selected.text().await;
        assert!(selected.contains("aria-selected=\"true\""));
        assert!(selected.contains("Methods"));
        assert!(selected.contains("Request"));
        assert!(selected.contains("Response"));
        assert!(selected.contains("Middleware"));
        assert!(selected.contains("operation middleware"));
        assert!(selected.contains("API-visible request metadata"));
        assert!(selected.contains("origin"));
        assert!(selected.contains("request_id"));
        assert!(selected.contains("site middleware"));

        let selected = client
            .get(&format!("/console/operations?selected={invoice_signal_id}"))
            .header(header::COOKIE.as_str(), &cookie)
            .send()
            .await;
        assert_eq!(
            selected.status(),
            StatusCode::OK,
            "selected typed operation page failed"
        );
        let selected = selected.text().await;
        assert!(selected.contains("InvoiceSignal"));
        assert!(selected.contains("invoice_id"));
        assert!(selected.contains("Raw JSON schema"));

        let tasks = client
            .get("/console/tasks")
            .header(header::COOKIE.as_str(), &cookie)
            .send()
            .await;
        assert_eq!(tasks.status(), StatusCode::OK, "tasks page failed");
        let tasks = tasks.text().await;
        assert!(tasks.contains("No task records yet."));
        assert!(tasks.contains("name=\"limit\""));
        assert!(tasks.contains("100 per page"));
        assert!(!tasks.contains("api/tasks"));

        let conf = client
            .get("/console/conf")
            .header(header::COOKIE.as_str(), &cookie)
            .send()
            .await;
        assert_eq!(conf.status(), StatusCode::OK, "config page failed");
        let conf = conf.text().await;
        assert!(conf.contains("Configuration"));
        assert!(conf.contains("aria-current=\"page\""));
        assert!(conf.contains("Authentication"));
        assert!(conf.contains("HTTP Pipeline"));
        assert!(!conf.contains("Open raw"));
        assert!(!conf.contains("Download as JSON"));
        assert!(!conf.contains("api/conf"));
        assert!(!conf.contains(">01<"));
        assert!(conf.contains("&lt;redacted&gt;"));
        assert!(!conf.contains("secret_key"));
        assert!(!conf.contains("DATABASE_URL"));
        assert!(!conf.contains(token.as_str()));

        let openapi = client
            .get("/console/openapi")
            .header(header::COOKIE.as_str(), &cookie)
            .send()
            .await;
        assert_eq!(openapi.status(), StatusCode::OK, "openapi page failed");
        let openapi = openapi.text().await;
        assert!(openapi.contains("OpenAPI"));
        assert!(openapi.contains("vyuh-console-sidebar"));
        assert!(openapi.contains("redoc"));
        assert!(openapi.contains("spec-url"));
        assert!(openapi.contains("is-redoc"));
        assert!(!openapi.contains("Raw JSON"));
        assert!(!openapi.contains("Application routes only"));
        assert!(!openapi.contains("console_operations"));

        let stylesheet = super::stylesheet_path();
        let css = client.get(stylesheet).send().await;
        assert_eq!(css.status(), StatusCode::OK, "stylesheet failed");
        assert_eq!(
            css.header(header::CONTENT_TYPE.as_str())
                .and_then(|value| value.to_str().ok()),
            Some("text/css")
        );
    }

    #[tokio::test]
    async fn console_task_pages_show_submitted_tasks() {
        let conf = SiteConf::default()
            .log_init(false)
            .console(ConsoleConf::default().enabled(true));
        let site = Site::build(conf, task_app_bundle()).await.unwrap();
        site.tasks()
            .submit(ConsoleTaskJob {
                message: "hello".to_string(),
            })
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let token = site
            .console_runtime()
            .and_then(|runtime| runtime.bootstrap_token())
            .unwrap();
        let client = TestSite::new(site);
        let login = client
            .get(&format!("/console/login?token={token}"))
            .send()
            .await;
        let cookie = login
            .header(header::SET_COOKIE.as_str())
            .unwrap()
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_string();

        let api_tasks = client
            .get("/console/api/tasks")
            .header(header::COOKIE.as_str(), &cookie)
            .send()
            .await;
        assert_eq!(api_tasks.status(), StatusCode::OK);
        let api_tasks = api_tasks.text().await;
        assert!(api_tasks.contains("console_test_task"));

        let tasks = client
            .get("/console/tasks")
            .header(header::COOKIE.as_str(), &cookie)
            .send()
            .await;
        assert_eq!(tasks.status(), StatusCode::OK);
        let tasks = tasks.text().await;
        assert!(tasks.contains("console_test_task"));
        assert!(!tasks.contains("No task records yet."));
    }

    #[tokio::test]
    async fn console_status_is_cached_within_ttl() {
        let conf = SiteConf::default()
            .log_init(false)
            .console(ConsoleConf::default().enabled(true));
        let site = Site::build(conf, app_bundle()).await.unwrap();
        let runtime = site.console_runtime().unwrap();

        let first = runtime.status(&site, std::time::Duration::from_secs(5));
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let second = runtime.status(&site, std::time::Duration::from_secs(5));

        assert_eq!(first.site.uptime_seconds, second.site.uptime_seconds);
    }
}
