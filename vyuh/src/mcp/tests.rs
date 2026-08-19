use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tower::ServiceExt;

use crate::{
    Data, Error, Site, SiteConf,
    auth::{Audience, AuthUser, Permit, Scope, ScopeExpr, ScopeRule},
    bundles,
};

use super::McpConf;
use super::{McpResource, McpToolConf, McpUiResourceMeta};

const MCP: Audience = Audience::new("https://api.example.com/mcp");
const OTHER_AUDIENCE: Audience = Audience::new("https://api.example.com/other");
static NOTES_READ_SCOPES: &[Scope] = &[Scope::of("notes:read")];

#[derive(Deserialize, Serialize, JsonSchema)]
struct Echo {
    value: String,
}

#[derive(Deserialize, Serialize, JsonSchema)]
struct ToolIdentity {
    subject: String,
}

struct ReadNotes;

impl ScopeRule for ReadNotes {
    const EXPR: ScopeExpr = ScopeExpr::all(NOTES_READ_SCOPES);
}

#[bundles::mcp_tool]
async fn echo(input: Data<Echo>) -> Data<Echo> {
    input
}

#[bundles::mcp_resource]
fn member_card() -> McpResource {
    McpResource::text(
        "ui://widget/member-card.html",
        "text/html;profile=mcp-app",
        "<main>Member card</main>",
    )
    .ui(McpUiResourceMeta::default().prefers_border(true))
}

#[bundles::mcp_tool(ui_resource_uri = "ui://widget/member-card.html")]
async fn render_member(input: Data<Echo>) -> Data<Echo> {
    input
}

#[bundles::mcp_tool]
async fn read_note(_permit: Permit<ReadNotes>, input: Data<Echo>) -> Data<Echo> {
    input
}

#[bundles::mcp_tool]
async fn tool_identity(user: AuthUser, _input: Data<Echo>) -> Data<ToolIdentity> {
    Data::new(ToolIdentity {
        subject: user.subject().to_string(),
    })
}

#[bundles::mcp_tool]
async fn missing_note(_input: Data<Echo>) -> Result<Data<Echo>, Error> {
    Err(Error::not_found("internal note details"))
}

#[test]
fn mcp_defaults_to_audience_authentication() {
    let mut conf = McpConf::new("/mcp");
    assert!(conf.validate().is_ok());
}

#[test]
fn accepts_predicate_or_public_access_modes() {
    let mut protected = McpConf::new("/mcp").auth(|_| true);
    let mut anonymous = McpConf::new("/public").public();
    assert!(protected.validate().is_ok());
    assert!(anonymous.validate().is_ok());
}

#[tokio::test]
async fn uses_the_central_audience_authenticator() -> Result<(), Box<dyn std::error::Error>> {
    let bundle = bundles::bundle! { echo }
        .with_conf(bundles::conf().audience(MCP).mcp(McpConf::new("/mcp")));
    let site = Site::build(
        SiteConf::default()
            .log_init(false)
            .auth(crate::auth::AuthConf::development()),
        bundle,
    )
    .await?;
    let login = site
        .auth()
        .using(crate::auth::DEFAULT_AUTH_PROVIDER)
        .issue(AuthUser::new("tool-user"), &[MCP])
        .await?;
    let token = login.credentials().access();
    let request = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("content-type", "application/json")
        .header("accept", "application/json")
        .header("mcp-protocol-version", "2025-11-25")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from(
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}"#,
        ))?;
    let response = site.router().oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::OK);

    let wrong_audience = site
        .auth()
        .using(crate::auth::DEFAULT_AUTH_PROVIDER)
        .issue(AuthUser::new("wrong-audience"), &[OTHER_AUDIENCE])
        .await?;
    let request = authenticated_mcp_request(
        wrong_audience.credentials().access(),
        r#"{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}"#,
    )?;
    let response = site.router().oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    Ok(())
}

/// Verifies MCP executes a direct callable without adapting an HTTP route.
#[tokio::test]
async fn invokes_direct_tool() -> Result<(), Box<dyn std::error::Error>> {
    let bundle = bundles::bundle! { echo }.with_conf(
        bundles::conf()
            .audience(MCP)
            .mcp(McpConf::new("/mcp").public()),
    );
    let site = Site::build(SiteConf::default().log_init(false), bundle).await?;
    let request = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("content-type", "application/json")
        .header("mcp-protocol-version", "2025-11-25")
        .body(Body::from(
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"echo","arguments":{"value":"hello"}}}"#,
        ))?;
    let response = site.router().oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await?;
    let response: Value = serde_json::from_slice(&bytes)?;
    assert_eq!(
        response.pointer("/result/structuredContent"),
        Some(&json!({"value": "hello"}))
    );
    Ok(())
}

/// Verifies MCP rejects invalid semantic arguments and keeps application failures private.
#[tokio::test]
async fn validates_arguments_and_sanitizes_application_failures() {
    let bundle = bundles::bundle! { echo, missing_note }.with_conf(
        bundles::conf()
            .audience(MCP)
            .mcp(McpConf::new("/mcp").public()),
    );
    let site = Site::build(SiteConf::default().log_init(false), bundle)
        .await
        .expect("public MCP site builds");

    let invalid = send_json(
        &site,
        mcp_request(
            "/mcp",
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"echo","arguments":{"value":"ok","extra":true}}}"#,
        )
        .expect("request builds"),
    )
    .await;
    assert_eq!(invalid.pointer("/error/code"), Some(&json!(-32602)));

    let non_object = send_json(
        &site,
        mcp_request(
            "/mcp",
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"echo","arguments":[]}}"#,
        )
        .expect("request builds"),
    )
    .await;
    assert_eq!(non_object.pointer("/error/code"), Some(&json!(-32602)));

    let failed = send_json(
        &site,
        mcp_request(
            "/mcp",
            r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"missing_note","arguments":{"value":"x"}}}"#,
        )
        .expect("request builds"),
    )
    .await;
    assert_eq!(
        failed.pointer("/result/structuredContent"),
        Some(&json!({"status": 404, "error": "tool resource was not found"}))
    );
    assert!(!failed.to_string().contains("internal note details"));
}

/// Verifies authenticated discovery filters permits and hides unauthorized tool calls.
#[tokio::test]
async fn filters_tools_and_enforces_permit_authorization() {
    let bundle = bundles::bundle! { echo, read_note, tool_identity }
        .with_conf(bundles::conf().audience(MCP).mcp(McpConf::new("/mcp")));
    let site = Site::build(
        SiteConf::default()
            .log_init(false)
            .auth(crate::auth::AuthConf::development()),
        bundle,
    )
    .await
    .expect("authenticated MCP site builds");
    let unscoped = issue_token(&site, AuthUser::new("viewer")).await;
    assert_unscoped_tool_access(&site, &unscoped).await;
    let scoped = issue_token(
        &site,
        AuthUser::new("reader").with_scope(Scope::of("notes:read")),
    )
    .await;
    assert_scoped_tool_access(&site, &scoped).await;
}

/// Checks role-filtered discovery and indistinguishable denied tool calls.
async fn assert_unscoped_tool_access(site: &Site, token: &str) {
    let listed = authenticated_json(site, token, 1, "tools/list", "{}").await;
    assert_eq!(tool_names(&listed), vec!["echo", "tool_identity"]);
    let denied = authenticated_tool_call(site, token, 2, "read_note").await;
    let unknown = authenticated_tool_call(site, token, 3, "does_not_exist").await;
    assert_eq!(
        denied.pointer("/error/code"),
        unknown.pointer("/error/code")
    );
    assert_eq!(
        denied.pointer("/error/message"),
        unknown.pointer("/error/message")
    );
}

/// Checks permit admission and the identity visible to a direct callable.
async fn assert_scoped_tool_access(site: &Site, token: &str) {
    let authorized = authenticated_json(site, token, 4, "tools/list", "{}").await;
    assert_eq!(
        tool_names(&authorized),
        vec!["echo", "read_note", "tool_identity"]
    );
    let identity = authenticated_tool_call(site, token, 5, "tool_identity").await;
    assert_eq!(
        identity.pointer("/result/structuredContent/subject"),
        Some(&json!("reader"))
    );
}

/// Verifies Streamable HTTP transport and JSON-RPC failures are stable and correctly classified.
#[tokio::test]
async fn enforces_transport_and_json_rpc_contract() {
    let bundle = bundles::bundle! { echo }.with_conf(
        bundles::conf()
            .audience(MCP)
            .mcp(McpConf::new("/mcp").public()),
    );
    let site = Site::build(SiteConf::default().log_init(false), bundle)
        .await
        .expect("public MCP site builds");

    let get = Request::builder()
        .method("GET")
        .uri("/mcp")
        .body(Body::empty())
        .expect("GET request builds");
    assert_eq!(
        site.router()
            .oneshot(get)
            .await
            .expect("router responds")
            .status(),
        StatusCode::METHOD_NOT_ALLOWED
    );

    let unsupported = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("content-type", "text/plain")
        .body(Body::from("{}"))
        .expect("request builds");
    assert_eq!(
        site.router()
            .oneshot(unsupported)
            .await
            .expect("router responds")
            .status(),
        StatusCode::UNSUPPORTED_MEDIA_TYPE
    );

    let forbidden_origin = mcp_request("/mcp", "{}").expect("request builds");
    let mut forbidden_origin = forbidden_origin;
    forbidden_origin.headers_mut().insert(
        "origin",
        "https://untrusted.example".parse().expect("header parses"),
    );
    assert_eq!(
        site.router()
            .oneshot(forbidden_origin)
            .await
            .expect("router responds")
            .status(),
        StatusCode::FORBIDDEN
    );

    let parse_error = send_json(&site, mcp_request("/mcp", "{").expect("request builds")).await;
    assert_eq!(parse_error.pointer("/error/code"), Some(&json!(-32700)));

    let invalid_request = site
        .router()
        .oneshot(
            mcp_request(
                "/mcp",
                r#"{"jsonrpc":"1.0","id":1,"method":"tools/list","params":{}}"#,
            )
            .expect("request builds"),
        )
        .await
        .expect("router responds");
    assert_eq!(invalid_request.status(), StatusCode::BAD_REQUEST);
    let invalid_request = response_json(invalid_request)
        .await
        .expect("JSON-RPC error decodes");
    assert_eq!(invalid_request.pointer("/error/code"), Some(&json!(-32600)));
}

/// Verifies the current MCP revision exposes discovery and complete tool results.
#[tokio::test]
async fn supports_current_protocol_discovery_and_calls() {
    let bundle = bundles::bundle! { echo }.with_conf(
        bundles::conf()
            .audience(MCP)
            .mcp(McpConf::new("/mcp").public()),
    );
    let site = Site::build(SiteConf::default().log_init(false), bundle)
        .await
        .expect("public MCP site builds");
    let discover = send_json(
        &site,
        modern_mcp_request(
            "server/discover",
            None,
            r#"{"jsonrpc":"2.0","id":1,"method":"server/discover","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientInfo":{"name":"test","version":"1"},"io.modelcontextprotocol/clientCapabilities":{}}}}"#,
        )
        .expect("request builds"),
    )
    .await;
    assert_eq!(
        discover.pointer("/result/capabilities/resources"),
        Some(&json!({}))
    );
    assert_eq!(
        discover.pointer("/result/resultType"),
        Some(&json!("complete"))
    );

    let call = send_json(
        &site,
        modern_mcp_request(
            "tools/call",
            Some("echo"),
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"echo","arguments":{"value":"modern"},"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientInfo":{"name":"test","version":"1"},"io.modelcontextprotocol/clientCapabilities":{}}}}"#,
        )
        .expect("request builds"),
    )
    .await;
    assert_eq!(
        call.pointer("/result/structuredContent"),
        Some(&json!({"value": "modern"}))
    );
    assert_eq!(call.pointer("/result/resultType"), Some(&json!("complete")));
}

/// Verifies nested MCP services retain independent endpoint catalogs after bundle composition.
#[tokio::test]
async fn isolates_multiple_service_catalogs() {
    let reader = bundles::bundle([bundles::mcp_tool(
        "reader_echo",
        echo,
        McpToolConf::default(),
    )])
    .with_conf(
        bundles::conf()
            .audience(MCP)
            .mcp(McpConf::new("/mcp/reader").public()),
    );
    let writer = bundles::bundle([bundles::mcp_tool(
        "writer_echo",
        echo,
        McpToolConf::default(),
    )])
    .with_conf(
        bundles::conf()
            .audience(MCP)
            .mcp(McpConf::new("/mcp/writer").public()),
    );
    let site = Site::build(SiteConf::default().log_init(false), reader.merge(writer))
        .await
        .expect("independent MCP services build");
    let reader = send_json(
        &site,
        mcp_request(
            "/mcp/reader",
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}"#,
        )
        .expect("request builds"),
    )
    .await;
    let writer = send_json(
        &site,
        mcp_request(
            "/mcp/writer",
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}"#,
        )
        .expect("request builds"),
    )
    .await;
    assert_eq!(tool_names(&reader), vec!["reader_echo"]);
    assert_eq!(tool_names(&writer), vec!["writer_echo"]);
}

/// Verifies anonymous services and unclaimed resources fail before an endpoint is mounted.
#[tokio::test]
async fn rejects_protected_anonymous_and_unclaimed_registrations() {
    let protected = bundles::bundle! { read_note }.with_conf(
        bundles::conf()
            .audience(MCP)
            .mcp(McpConf::new("/mcp").public()),
    );
    assert!(
        Site::build(SiteConf::default().log_init(false), protected)
            .await
            .is_err()
    );

    let resource = bundles::mcp_resource(
        "unclaimed",
        McpResource::text("https://example.test/unclaimed", "text/plain", "static"),
    );
    let unclaimed = bundles::bundle([resource]);
    assert!(
        Site::build(SiteConf::default().log_init(false), unclaimed)
            .await
            .is_err()
    );
}

/// Verifies static MCP resources are advertised, read, and attached to only annotated tools.
#[tokio::test]
async fn serves_static_resources_and_ui_tool_metadata() -> Result<(), Box<dyn std::error::Error>> {
    let bundle = bundles::bundle! { member_card, render_member, echo }.with_conf(
        bundles::conf()
            .audience(MCP)
            .mcp(McpConf::new("/mcp").public()),
    );
    let site = Site::build(SiteConf::default().log_init(false), bundle).await?;
    let initialize = mcp_request(
        "/mcp",
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25"}}"#,
    )?;
    let initialized = response_json(site.router().oneshot(initialize).await?).await?;
    assert_eq!(
        initialized.pointer("/result/capabilities/resources"),
        Some(&json!({}))
    );

    let listed = response_json(
        site.router()
            .oneshot(mcp_request(
                "/mcp",
                r#"{"jsonrpc":"2.0","id":2,"method":"resources/list","params":{}}"#,
            )?)
            .await?,
    )
    .await?;
    assert_eq!(
        listed.pointer("/result/resources/0"),
        Some(&json!({
            "uri": "ui://widget/member-card.html",
            "name": "member_card",
            "mimeType": "text/html;profile=mcp-app",
            "size": 24,
        }))
    );

    let resource = response_json(site.router().oneshot(mcp_request(
        "/mcp",
        r#"{"jsonrpc":"2.0","id":3,"method":"resources/read","params":{"uri":"ui://widget/member-card.html"}}"#,
    )?).await?).await?;
    assert_eq!(
        resource.pointer("/result/contents/0/_meta/ui/prefersBorder"),
        Some(&json!(true))
    );

    let tools = response_json(
        site.router()
            .oneshot(mcp_request(
                "/mcp",
                r#"{"jsonrpc":"2.0","id":4,"method":"tools/list","params":{}}"#,
            )?)
            .await?,
    )
    .await?;
    let definitions = tools
        .pointer("/result/tools")
        .and_then(Value::as_array)
        .ok_or("missing tool definitions")?;
    let render = definitions
        .iter()
        .find(|definition| definition["name"] == "render_member")
        .ok_or("missing render tool")?;
    let echo = definitions
        .iter()
        .find(|definition| definition["name"] == "echo")
        .ok_or("missing echo tool")?;
    assert_eq!(
        render.pointer("/_meta/ui/resourceUri"),
        Some(&json!("ui://widget/member-card.html"))
    );
    assert!(echo.get("_meta").is_none());
    Ok(())
}

/// Verifies unknown resource reads use the standard resource-not-found error and URI data.
#[tokio::test]
async fn rejects_unknown_static_resource() -> Result<(), Box<dyn std::error::Error>> {
    let bundle = bundles::bundle! { member_card, render_member }.with_conf(
        bundles::conf()
            .audience(MCP)
            .mcp(McpConf::new("/mcp").public()),
    );
    let site = Site::build(SiteConf::default().log_init(false), bundle).await?;
    let request = mcp_request(
        "/mcp",
        r#"{"jsonrpc":"2.0","id":1,"method":"resources/read","params":{"uri":"ui://widget/missing.html"}}"#,
    )?;
    let response = response_json(site.router().oneshot(request).await?).await?;
    assert_eq!(response.pointer("/error/code"), Some(&json!(-32002)));
    assert_eq!(
        response.pointer("/error/data/uri"),
        Some(&json!("ui://widget/missing.html"))
    );
    Ok(())
}

/// Verifies tools cannot attach a missing UI resource during site construction.
#[tokio::test]
async fn rejects_missing_ui_resource() {
    let tool = bundles::mcp_tool(
        "missing_resource",
        echo,
        McpToolConf::default().ui_resource_uri("ui://widget/missing.html"),
    );
    let bundle = bundles::bundle([tool]).with_conf(
        bundles::conf()
            .audience(MCP)
            .mcp(McpConf::new("/mcp").public()),
    );
    let site = Site::build(SiteConf::default().log_init(false), bundle).await;
    assert!(site.is_err());
}

/// Verifies UI-attached tools require the MCP Apps HTML MIME type.
#[tokio::test]
async fn rejects_non_html_ui_resource() {
    let resource = bundles::mcp_resource(
        "plain_ui",
        McpResource::text("ui://widget/plain.txt", "text/plain", "plain text"),
    );
    let tool = bundles::mcp_tool(
        "plain_ui_tool",
        echo,
        McpToolConf::default().ui_resource_uri("ui://widget/plain.txt"),
    );
    let bundle = bundles::bundle([resource, tool]).with_conf(
        bundles::conf()
            .audience(MCP)
            .mcp(McpConf::new("/mcp").public()),
    );
    let site = Site::build(SiteConf::default().log_init(false), bundle).await;
    assert!(site.is_err());
}

/// Verifies protected resource reads use the same central audience credential boundary as tools.
#[tokio::test]
async fn protects_static_resources_with_the_service_audience()
-> Result<(), Box<dyn std::error::Error>> {
    let bundle = bundles::bundle! { member_card, render_member }
        .with_conf(bundles::conf().audience(MCP).mcp(McpConf::new("/mcp")));
    let site = Site::build(
        SiteConf::default()
            .log_init(false)
            .auth(crate::auth::AuthConf::development()),
        bundle,
    )
    .await?;
    let unauthenticated = site
        .router()
        .oneshot(mcp_request(
            "/mcp",
            r#"{"jsonrpc":"2.0","id":1,"method":"resources/read","params":{"uri":"ui://widget/member-card.html"}}"#,
        )?)
        .await?;
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);

    let login = site
        .auth()
        .using(crate::auth::DEFAULT_AUTH_PROVIDER)
        .issue(AuthUser::new("resource-user"), &[MCP])
        .await?;
    let request = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("content-type", "application/json")
        .header("accept", "application/json")
        .header("mcp-protocol-version", "2025-11-25")
        .header("authorization", format!("Bearer {}", login.credentials().access()))
        .body(Body::from(
            r#"{"jsonrpc":"2.0","id":2,"method":"resources/read","params":{"uri":"ui://widget/member-card.html"}}"#,
        ))?;
    let response = response_json(site.router().oneshot(request).await?).await?;
    assert_eq!(
        response.pointer("/result/contents/0/uri"),
        Some(&json!("ui://widget/member-card.html"))
    );
    Ok(())
}

/// Verifies duplicate resource URIs are accumulated as site construction errors.
#[tokio::test]
async fn rejects_duplicate_resource_uris() {
    let first = bundles::mcp_resource(
        "first",
        McpResource::text("https://example.test/member", "text/plain", "first"),
    );
    let second = bundles::mcp_resource(
        "second",
        McpResource::text("https://example.test/member", "text/plain", "second"),
    );
    let bundle = bundles::bundle([first, second]).with_conf(
        bundles::conf()
            .audience(MCP)
            .mcp(McpConf::new("/mcp").public()),
    );
    let site = Site::build(SiteConf::default().log_init(false), bundle).await;
    assert!(site.is_err());
}

/// Verifies a UI-bound tool cannot use a resource owned by a different MCP service.
#[tokio::test]
async fn rejects_cross_service_ui_resource() {
    let resource_service = bundles::bundle([bundles::mcp_resource(
        "member_card",
        McpResource::text(
            "ui://widget/member-card.html",
            "text/html;profile=mcp-app",
            "<main></main>",
        ),
    )])
    .with_conf(
        bundles::conf()
            .audience(MCP)
            .mcp(McpConf::new("/mcp/resources").public()),
    );
    let tool_service = bundles::bundle([bundles::mcp_tool(
        "cross_service",
        echo,
        McpToolConf::default().ui_resource_uri("ui://widget/member-card.html"),
    )])
    .with_conf(
        bundles::conf()
            .audience(MCP)
            .mcp(McpConf::new("/mcp/tools").public()),
    );
    let site = Site::build(
        SiteConf::default().log_init(false),
        resource_service.merge(tool_service),
    )
    .await;
    assert!(site.is_err());
}

/// Builds a legacy MCP JSON request accepted by the stateless Streamable HTTP endpoint.
fn mcp_request(path: &str, body: &'static str) -> Result<Request<Body>, axum::http::Error> {
    Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json")
        .header("accept", "application/json")
        .header("mcp-protocol-version", "2025-11-25")
        .body(Body::from(body))
}

/// Builds an audience-authenticated legacy MCP JSON request.
fn authenticated_mcp_request(
    token: &str,
    body: &'static str,
) -> Result<Request<Body>, axum::http::Error> {
    Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("content-type", "application/json")
        .header("accept", "application/json")
        .header("mcp-protocol-version", "2025-11-25")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from(body))
}

/// Sends one authenticated MCP method request with a JSON parameter fragment.
async fn authenticated_json(site: &Site, token: &str, id: u8, method: &str, params: &str) -> Value {
    let body = format!(r#"{{"jsonrpc":"2.0","id":{id},"method":"{method}","params":{params}}}"#);
    let request = authenticated_request(token, body).expect("request builds");
    send_json(site, request).await
}

/// Calls one authenticated tool with the standard test object input.
async fn authenticated_tool_call(site: &Site, token: &str, id: u8, name: &str) -> Value {
    let params = format!(r#"{{"name":"{name}","arguments":{{"value":"x"}}}}"#);
    authenticated_json(site, token, id, "tools/call", &params).await
}

/// Builds an authenticated MCP request with an owned JSON body.
fn authenticated_request(token: &str, body: String) -> Result<Request<Body>, axum::http::Error> {
    Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("content-type", "application/json")
        .header("accept", "application/json")
        .header("mcp-protocol-version", "2025-11-25")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from(body))
}

/// Builds a current-revision MCP request with its mirrored method and tool name headers.
fn modern_mcp_request(
    method: &'static str,
    name: Option<&'static str>,
    body: &'static str,
) -> Result<Request<Body>, axum::http::Error> {
    let mut request = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("content-type", "application/json")
        .header("accept", "application/json")
        .header("mcp-protocol-version", super::protocol::CURRENT_VERSION)
        .header("mcp-method", method)
        .body(Body::from(body))?;
    if let Some(name) = name {
        request.headers_mut().insert(
            "mcp-name",
            name.parse().expect("tool name is a valid header"),
        );
    }
    Ok(request)
}

/// Issues one central development-provider token for the MCP service audience.
async fn issue_token(site: &Site, user: AuthUser) -> String {
    let login = site
        .auth()
        .using(crate::auth::DEFAULT_AUTH_PROVIDER)
        .issue(user, &[MCP])
        .await
        .expect("development provider issues MCP token");
    login.credentials().access().to_string()
}

/// Sends one MCP request and decodes its required JSON response.
async fn send_json(site: &Site, request: Request<Body>) -> Value {
    let response = site
        .router()
        .oneshot(request)
        .await
        .expect("router responds");
    response_json(response).await.expect("MCP response is JSON")
}

/// Extracts the deterministic names from a successful `tools/list` result.
fn tool_names(response: &Value) -> Vec<&str> {
    response
        .pointer("/result/tools")
        .and_then(Value::as_array)
        .expect("tool list is present")
        .iter()
        .filter_map(|tool| tool.get("name").and_then(Value::as_str))
        .collect()
}

/// Decodes a successful or JSON-RPC error response into an inspectable JSON value.
async fn response_json(
    response: axum::response::Response,
) -> Result<Value, Box<dyn std::error::Error>> {
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await?;
    Ok(serde_json::from_slice(&bytes)?)
}
