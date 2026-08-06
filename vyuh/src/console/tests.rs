use axum::http::{StatusCode, header};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[cfg(feature = "cors")]
use crate::middlewares::{CorsConf, HttpConf};
#[cfg(feature = "cors")]
use crate::routes::CorsMiddleware;
use crate::{
    Data, Site, SiteConf, bundles,
    console::{CONSOLE_AUDIENCE, ConsoleAccess, ConsoleConf},
    emitters::{CronConf, EmitterExecutor},
    routes::{Json, Methods, RouteConf},
    testing::TestSite,
};
#[cfg(feature = "cors")]
use tower_http::cors::CorsLayer;

async fn ping() -> Json<&'static str> {
    Json("pong")
}

async fn protected_ping(_user: crate::auth::AuthUser) -> Json<&'static str> {
    Json("protected")
}

async fn console_credential(site: &Site) -> Result<String, crate::auth::AuthError> {
    let login = site
        .auth()
        .login(
            crate::auth::AuthUser::new("console-user"),
            &[CONSOLE_AUDIENCE],
        )
        .await?;
    Ok(login.credentials().access().to_owned())
}

async fn console_cookie(site: &Site) -> Result<String, crate::auth::AuthError> {
    console_credential(site).await
}

struct AllowConsole;

impl ConsoleAccess for AllowConsole {
    fn allows(&self, _site: &Site, user: Option<&crate::auth::AuthUser>) -> bool {
        user.is_some_and(|user| user.key.as_ref() == "console-user")
    }
}

struct DenyConsole;

impl ConsoleAccess for DenyConsole {
    fn allows(&self, _site: &Site, _user: Option<&crate::auth::AuthUser>) -> bool {
        false
    }
}

struct AllowAnonymous;

impl ConsoleAccess for AllowAnonymous {
    fn allows(&self, _site: &Site, _user: Option<&crate::auth::AuthUser>) -> bool {
        true
    }
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

async fn scheduled_console_task() -> Data<ConsoleTaskJob> {
    Data(std::sync::Arc::new(ConsoleTaskJob {
        message: "scheduled".into(),
    }))
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
        bundles::route(
            protected_ping,
            RouteConf {
                name: "protected_ping".into(),
                methods: Methods::GET,
                path: "/protected-ping".into(),
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

fn scheduled_task_bundle() -> crate::bundles::Bundle {
    task_app_bundle().merge(bundles::bundle([bundles::cron(
        scheduled_console_task,
        CronConf::new("0 0 2 * * *")
            .executor(EmitterExecutor::Task)
            .schedule("nightly-console-task"),
    )]))
}

/// Verifies console URLs use the shared site-wide static URL for each built site.
#[tokio::test]
async fn console_urls_are_site_local() {
    let root = Site::build(
        SiteConf::default()
            .log_init(false)
            .console(ConsoleConf::default().enabled(true)),
        app_bundle(),
    )
    .await
    .unwrap();
    let nested = Site::build(
        SiteConf::default()
            .log_init(false)
            .console(ConsoleConf::default().enabled(true).path("/dynrs/console")),
        app_bundle(),
    )
    .await
    .unwrap();

    assert_eq!(
        root.console_urls().map(|urls| urls.home.as_str()),
        Some("/console")
    );
    assert_eq!(
        root.console_urls().map(|urls| urls.script_path.as_str()),
        Some("/static/console/js/console.js")
    );
    assert_eq!(
        nested.console_urls().map(|urls| urls.home.as_str()),
        Some("/dynrs/console")
    );
    assert_eq!(
        nested.console_urls().map(|urls| urls.script_path.as_str()),
        Some("/static/console/js/console.js")
    );
}

/// Verifies every rendered console page uses the shared static URL beside a nested mount.
#[tokio::test]
async fn nested_console_pages_use_shared_assets() {
    let conf = SiteConf::default()
        .log_init(false)
        .console(ConsoleConf::default().enabled(true).path("/dynrs/console"));
    let site = Site::build(conf, app_bundle()).await.unwrap();
    let cookie = console_cookie(&site).await.unwrap();
    let client = TestSite::new(site);
    let overview = client
        .get("/dynrs/console")
        .header(header::COOKIE.as_str(), &cookie)
        .send()
        .await;
    assert_eq!(overview.status(), StatusCode::OK);
    let overview = overview.text().await;
    assert_console_assets(&overview);
    assert!(overview.contains("&#x2f;static&#x2f;console&#x2f;js&#x2f;console.js"));

    let error = client
        .get("/dynrs/console/operations/not-a-uuid")
        .header(header::COOKIE.as_str(), &cookie)
        .send()
        .await;
    assert_eq!(error.status(), StatusCode::NOT_FOUND);
    assert_console_assets(&error.text().await);

    let script = client.get("/static/console/js/console.js").send().await;
    assert_eq!(script.status(), StatusCode::OK);
}

/// Verifies custom relative static URLs serve console assets beside a nested console path.
#[tokio::test]
async fn custom_static_url_serves_nested_console_assets() {
    let root = Site::build(
        SiteConf::default()
            .log_init(false)
            .static_url("/public")
            .console(ConsoleConf::default().enabled(true)),
        app_bundle(),
    )
    .await
    .unwrap();
    assert_eq!(
        root.console_urls().map(|urls| urls.script_path.as_str()),
        Some("/public/console/js/console.js")
    );

    let conf = SiteConf::default()
        .log_init(false)
        .static_url("/public")
        .console(ConsoleConf::default().enabled(true).path("/dynrs/console"));
    let site = Site::build(conf, app_bundle()).await.unwrap();
    let cookie = console_cookie(&site).await.unwrap();
    let client = TestSite::new(site);

    let overview = client
        .get("/dynrs/console")
        .header(header::COOKIE.as_str(), &cookie)
        .send()
        .await;
    assert_eq!(overview.status(), StatusCode::OK);
    let overview = overview.text().await;
    assert!(overview.contains("&#x2f;public&#x2f;console&#x2f;js&#x2f;console.js"));
    assert!(overview.contains("&#x2f;public&#x2f;console&#x2f;img&#x2f;favicon.svg"));

    let script = client.get("/public/console/js/console.js").send().await;
    assert_eq!(script.status(), StatusCode::OK);
}

/// Verifies CDN static URLs are rendered for console assets without changing console routes.
#[tokio::test]
async fn cdn_static_url_is_rendered_for_nested_console_assets() {
    let root = Site::build(
        SiteConf::default()
            .log_init(false)
            .static_url("https://cdn.example.com/static")
            .console(ConsoleConf::default().enabled(true)),
        app_bundle(),
    )
    .await
    .unwrap();
    assert_eq!(
        root.console_urls().map(|urls| urls.script_path.as_str()),
        Some("https://cdn.example.com/static/console/js/console.js")
    );

    let conf = SiteConf::default()
        .log_init(false)
        .static_url("https://cdn.example.com/static")
        .console(ConsoleConf::default().enabled(true).path("/dynrs/console"));
    let site = Site::build(conf, app_bundle()).await.unwrap();
    let cookie = console_cookie(&site).await.unwrap();
    let client = TestSite::new(site);

    let overview = client
        .get("/dynrs/console")
        .header(header::COOKIE.as_str(), &cookie)
        .send()
        .await;
    assert_eq!(overview.status(), StatusCode::OK);
    let overview = overview.text().await;
    assert!(overview.contains(
        "https:&#x2f;&#x2f;cdn.example.com&#x2f;static&#x2f;console&#x2f;js&#x2f;console.js"
    ));
    assert!(overview.contains("https://unpkg.com/htmx.org@2.0.4"));

    let openapi = client
        .get("/dynrs/console/openapi")
        .header(header::COOKIE.as_str(), &cookie)
        .send()
        .await;
    assert_eq!(openapi.status(), StatusCode::OK);
    assert!(
        openapi
            .text()
            .await
            .contains("https://cdn.jsdelivr.net/npm/redoc@2/bundles/redoc.standalone.js")
    );
}

/// Verifies console paths use the same strict prefix validation as bundle prefixes.
#[tokio::test]
async fn invalid_console_paths_fail_site_construction() {
    for path in [
        "/",
        "/console/",
        "/console//admin",
        "/console?debug",
        "/console#top",
    ] {
        let site = Site::build(
            SiteConf::default()
                .log_init(false)
                .console(ConsoleConf::default().enabled(true).path(path)),
            app_bundle(),
        )
        .await;
        assert!(site.is_err(), "console path {path:?} unexpectedly built");
    }
}

fn assert_console_assets(html: &str) {
    assert!(html.contains("&#x2f;static&#x2f;css&#x2f;"));
    assert!(html.contains("&#x2f;static&#x2f;console&#x2f;img&#x2f;favicon.svg"));
    assert!(html.contains("&#x2f;static&#x2f;console&#x2f;img&#x2f;vyuh-logo-transparent.png"));
    assert!(!html.contains("src=\"/static/"));
    assert!(!html.contains("href=\"&#x2f;static/"));
    assert!(!html.contains("src=\"&#x2f;static/"));
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

/// Verifies debug Console development access is explicit in the rendered shell.
#[cfg(debug_assertions)]
#[tokio::test]
async fn console_development_access_is_open_and_warns() {
    let site = Site::build(SiteConf::default().log_init(false), app_bundle())
        .await
        .unwrap();
    let client = TestSite::new(site);

    let page = client.get("/console/overview").send().await;
    assert_eq!(page.status(), StatusCode::OK);
    assert!(page.text().await.contains("Development access is open"));
    client
        .get("/console/api/status")
        .send()
        .await
        .assert_status(StatusCode::OK);
}

/// Verifies a normal configured access credential can authorize the console.
#[tokio::test]
async fn console_policy_uses_normal_access_credentials() {
    let site = Site::build(
        SiteConf::default()
            .log_init(false)
            .console(ConsoleConf::default().enabled(true).access(AllowConsole)),
        app_bundle(),
    )
    .await
    .unwrap();
    let credential = console_credential(&site).await.unwrap();
    let authorization = format!("Bearer {credential}");
    let client = TestSite::new(site);

    client
        .get("/console/api/status")
        .header(header::AUTHORIZATION.as_str(), &authorization)
        .send()
        .await
        .assert_status(StatusCode::OK);
    client
        .get("/protected-ping")
        .header(header::AUTHORIZATION.as_str(), &authorization)
        .send()
        .await
        .assert_status(StatusCode::FORBIDDEN);
}

/// Verifies ConsoleAccess receives absent credentials but malformed ones remain authentication failures.
#[tokio::test]
async fn console_policy_distinguishes_absent_and_invalid_credentials() {
    let site = Site::build(
        SiteConf::default()
            .log_init(false)
            .console(ConsoleConf::default().enabled(true).access(DenyConsole)),
        app_bundle(),
    )
    .await
    .unwrap();
    let credential = console_credential(&site).await.unwrap();
    let authorization = format!("Bearer {credential}");
    let client = TestSite::new(site);

    client
        .get("/console/api/status")
        .send()
        .await
        .assert_status(StatusCode::FORBIDDEN);
    client
        .get("/console/api/status")
        .header(header::AUTHORIZATION.as_str(), "Bearer malformed")
        .send()
        .await
        .assert_status(StatusCode::UNAUTHORIZED);

    client
        .get("/console/api/status")
        .header(header::AUTHORIZATION.as_str(), &authorization)
        .send()
        .await
        .assert_status(StatusCode::FORBIDDEN);
}

/// Verifies credentials missing the console audience remain an authentication failure.
#[tokio::test]
async fn console_rejects_credentials_without_its_audience() {
    const API: crate::auth::Audience = crate::auth::Audience::new("api");
    let site = Site::build(
        SiteConf::default()
            .log_init(false)
            .console(ConsoleConf::default().enabled(true).access(AllowConsole)),
        app_bundle(),
    )
    .await
    .unwrap();
    let login = site
        .auth()
        .login(crate::auth::AuthUser::new("console-user"), &[API])
        .await
        .unwrap();
    let authorization = format!("Bearer {}", login.credentials().access());
    let client = TestSite::new(site);

    client
        .get("/console/api/status")
        .header(header::AUTHORIZATION.as_str(), &authorization)
        .send()
        .await
        .assert_status(StatusCode::UNAUTHORIZED);
}

/// Verifies an application policy may deliberately expose Console without a credential.
#[tokio::test]
async fn console_policy_may_allow_anonymous_access() {
    let site = Site::build(
        SiteConf::default()
            .log_init(false)
            .console(ConsoleConf::default().enabled(true).access(AllowAnonymous)),
        app_bundle(),
    )
    .await
    .unwrap();
    let client = TestSite::new(site);

    client
        .get("/console/api/status")
        .send()
        .await
        .assert_status(StatusCode::OK);
}

/// Verifies release builds reject an enabled Console without an access policy.
#[cfg(not(debug_assertions))]
#[tokio::test]
async fn release_console_requires_access_policy() {
    let result = Site::build(
        SiteConf::default()
            .log_init(false)
            .console(ConsoleConf::default().enabled(true)),
        app_bundle(),
    )
    .await;
    let message = result.err().map(|error| error.to_string());
    assert!(
        message
            .as_deref()
            .is_some_and(|message| message.contains("console.access"))
    );
}

/// Verifies the protected logs views explain the file-sink requirement without caching output.
#[tokio::test]
async fn console_logs_require_file_sink() {
    let site = Site::build(
        SiteConf::default()
            .log_init(false)
            .console(ConsoleConf::default().enabled(true)),
        app_bundle(),
    )
    .await
    .unwrap();
    let cookie = console_cookie(&site).await.unwrap();
    let client = TestSite::new(site);

    let page = client
        .get("/console/logs")
        .header(header::COOKIE.as_str(), &cookie)
        .send()
        .await;
    assert_eq!(page.status(), StatusCode::OK);
    assert_eq!(
        page.header(header::CACHE_CONTROL.as_str())
            .and_then(|value| value.to_str().ok()),
        Some("no-store")
    );
    assert!(page.text().await.contains("File logging is not configured"));

    let api = client
        .get("/console/api/logs")
        .header(header::COOKIE.as_str(), &cookie)
        .send()
        .await;
    assert_eq!(api.status(), StatusCode::OK);
    assert_eq!(
        api.header(header::CACHE_CONTROL.as_str())
            .and_then(|value| value.to_str().ok()),
        Some("no-store")
    );
    assert!(api.text().await.contains("\"configured\":false"));
}

/// Verifies console access adds no process-global authentication state.
#[test]
fn console_access_has_no_global_runtime_state() {
    for source in [include_str!("access.rs"), include_str!("runtime.rs")] {
        assert!(!source.contains("OnceLock"));
        assert!(!source.contains("lazy_static"));
        assert!(!source.contains("static mut"));
    }
}

#[cfg(feature = "cors")]
#[tokio::test]
async fn console_html_pages_and_assets_work() -> Result<(), crate::auth::AuthError> {
    let conf = SiteConf::default()
        .log_init(false)
        .console(ConsoleConf::default().enabled(true));
    let bundle = app_bundle().layer(CorsMiddleware::new(CorsLayer::permissive()));
    let site = Site::build(conf, bundle).await.unwrap();
    let ping_id = site
        .operations()
        .list()
        .find(|op| op.name == "ping")
        .map(|op| op.id)
        .unwrap();
    let invoice_signal_id = site
        .operations()
        .list()
        .find(|op| op.name == "invoice_signal")
        .map(|op| op.id)
        .unwrap();
    let console_operation_id = site
        .operations()
        .list()
        .find(|op| op.name == "console_operations")
        .map(|op| op.id)
        .unwrap();
    let cookie = console_cookie(&site).await.unwrap();
    let stylesheet = site
        .console_urls()
        .map(|urls| urls.stylesheet_path.clone())
        .ok_or_else(|| crate::auth::AuthError::Internal("console runtime missing".into()))?;
    let client = TestSite::new(site);

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

    let css = client.get(&stylesheet).send().await;
    assert_eq!(css.status(), StatusCode::OK, "stylesheet failed");
    assert_eq!(
        css.header(header::CONTENT_TYPE.as_str())
            .and_then(|value| value.to_str().ok()),
        Some("text/css")
    );
    Ok(())
}

/// Verifies console task inspection exposes lifecycle data without task values.
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

    let cookie = console_cookie(&site).await.unwrap();
    let task_id = site
        .tasks()
        .list(crate::tasks::TaskFilter::new())
        .await
        .unwrap()
        .items
        .into_iter()
        .next()
        .unwrap()
        .id;
    let client = TestSite::new(site);

    let api_tasks = client
        .get("/console/api/tasks")
        .header(header::COOKIE.as_str(), &cookie)
        .send()
        .await;
    assert_eq!(api_tasks.status(), StatusCode::OK);
    let api_tasks = api_tasks.text().await;
    assert!(api_tasks.contains("console_test_task"));

    let detail = client
        .get(&format!("/console/api/tasks/{task_id}"))
        .header(header::COOKIE.as_str(), &cookie)
        .send()
        .await;
    assert_eq!(detail.status(), StatusCode::OK);
    let detail: serde_json::Value = serde_json::from_str(&detail.text().await).unwrap();
    assert!(detail.get("input").is_some());
    assert!(detail.get("output").is_none());
    assert!(detail.get("result").is_none());

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

/// Verifies the console shows immutable task schedules and their durable cursor fields.
#[tokio::test]
async fn console_schedule_pages_show_task_schedules() {
    let site = Site::build(
        SiteConf::default()
            .log_init(false)
            .console(ConsoleConf::default().enabled(true)),
        scheduled_task_bundle(),
    )
    .await
    .unwrap();
    let cookie = console_cookie(&site).await.unwrap();
    let client = TestSite::new(site);

    let page = client
        .get("/console/schedules?selected=nightly-console-task")
        .header(header::COOKIE.as_str(), &cookie)
        .send()
        .await;
    assert_eq!(page.status(), StatusCode::OK);
    let page = page.text().await;
    assert!(page.contains("Registered schedules"));
    assert!(page.contains("nightly-console-task"));
    assert!(page.contains("Schedule Details"));
    assert!(page.contains("Coalesced recovery"));

    let api = client
        .get("/console/api/schedules")
        .header(header::COOKIE.as_str(), &cookie)
        .send()
        .await;
    assert_eq!(api.status(), StatusCode::OK);
    let api: serde_json::Value = serde_json::from_str(&api.text().await).unwrap();
    assert_eq!(api["configured"], 1);
    assert_eq!(api["items"][0]["name"], "nightly-console-task");
    assert_eq!(api["items"][0]["lane"], "default");
    assert!(api["items"][0]["next_expected_at"].is_string());
}

#[tokio::test]
async fn console_status_is_cached_within_ttl() {
    let conf = SiteConf::default()
        .log_init(false)
        .console(ConsoleConf::default().enabled(true));
    let site = Site::build(conf, app_bundle()).await.unwrap();
    let first = site.console_status();
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let second = site.console_status();

    assert_eq!(first.site.uptime_seconds, second.site.uptime_seconds);
}

/// Verifies console route fragments retain one immutable shared origin bundle ID.
#[tokio::test]
async fn console_operations_share_origin_bundle() {
    let site = Site::build(
        SiteConf::default()
            .log_init(false)
            .console(ConsoleConf::default().enabled(true)),
        app_bundle(),
    )
    .await
    .unwrap();
    let console_id = site
        .operations()
        .list()
        .find(|operation| operation.name == "console_operations")
        .and_then(|operation| operation.bundle_id);
    let app_id = site
        .operations()
        .list()
        .find(|operation| operation.name == "ping")
        .and_then(|operation| operation.bundle_id);
    assert!(console_id.is_some());
    assert_ne!(console_id, app_id);
    assert!(site.operations().list().all(|operation| {
        !operation.name.starts_with("console_") || operation.bundle_id == console_id
    }));
}

/// Verifies Console does not inject providers, commands, or credential-exchange routes.
#[tokio::test]
async fn console_does_not_add_private_auth_components() {
    let site = Site::build(
        SiteConf::default()
            .log_init(false)
            .console(ConsoleConf::default().enabled(true).access(AllowConsole)),
        app_bundle(),
    )
    .await
    .unwrap();

    assert!(
        site.conf()
            .auth
            .summary()
            .providers
            .iter()
            .all(|provider| !provider.id.starts_with("vyuh-console"))
    );
    assert!(
        !site
            .console_command_infos()
            .iter()
            .any(|command| command.name == "console-token")
    );
    assert!(site.routes().reverse_url("console_login", &[]).is_none());
    assert!(site.routes().reverse_url("console_logout", &[]).is_none());
}

/// Verifies application provider IDs cannot use Vyuh's reserved namespace.
#[tokio::test]
async fn framework_provider_prefix_is_reserved() {
    use crate::auth::{AuthConf, AuthProvider, Jwt, TokenConf, TokenProvider};

    let auth = AuthConf::empty().provider(
        AuthProvider::new("vyuh-example"),
        TokenProvider::new(Jwt::hs256_site_secret())
            .without_refresh()
            .access(TokenConf::bearer()),
    );
    let error = Site::build(
        SiteConf::default()
            .log_init(false)
            .auth(auth)
            .console(ConsoleConf::default().enabled(false)),
        app_bundle(),
    )
    .await
    .err()
    .map(|error| error.to_string());

    assert!(
        error
            .as_deref()
            .is_some_and(|message| message.contains("reserved 'vyuh-' prefix"))
    );

    let valid_auth = AuthConf::empty().provider(
        AuthProvider::new("app-example"),
        TokenProvider::new(Jwt::hs256_site_secret())
            .without_refresh()
            .access(TokenConf::bearer()),
    );
    let valid = Site::build(
        SiteConf::default()
            .log_init(false)
            .auth(valid_auth)
            .console(ConsoleConf::default().enabled(false)),
        app_bundle(),
    )
    .await;

    assert!(valid.is_ok());
}
