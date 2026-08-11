use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tower::ServiceExt;

use crate::{
    Data, Site, SiteConf,
    auth::{Audience, AuthProvider, AuthUser, DEFAULT_AUTH_PROVIDER},
    bundles,
};

use super::McpConf;

const MCP: Audience = Audience::new("https://api.example.com/mcp");
const TOKEN: AuthProvider = AuthProvider::new("token");

#[derive(Deserialize, Serialize, JsonSchema)]
struct Echo {
    value: String,
}

#[bundles::mcp_tool]
async fn echo(input: Data<Echo>) -> Data<Echo> {
    input
}

#[test]
fn requires_an_explicit_authentication_mode() {
    let mut conf = McpConf::new("/mcp", MCP);
    assert!(conf.validate().is_err());
}

#[test]
fn accepts_a_selected_provider_or_anonymous_mode() {
    let mut protected = McpConf::new("/mcp", MCP).auth(TOKEN);
    let mut anonymous = McpConf::new("/public", MCP).anonymous();
    assert!(protected.validate().is_ok());
    assert!(anonymous.validate().is_ok());
}

#[tokio::test]
async fn uses_only_the_selected_vyuh_provider() -> Result<(), Box<dyn std::error::Error>> {
    let bundle =
        bundles::bundle! { echo }.with_mcp(McpConf::new("/mcp", MCP).auth(DEFAULT_AUTH_PROVIDER));
    let site = Site::build(SiteConf::default().log_init(false), bundle).await?;
    let login = site
        .auth()
        .login(AuthUser::new("tool-user"), &[MCP])
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
    Ok(())
}

/// Verifies MCP executes a direct callable without adapting an HTTP route.
#[tokio::test]
async fn invokes_direct_tool() -> Result<(), Box<dyn std::error::Error>> {
    let bundle = bundles::bundle! { echo }.with_mcp(McpConf::new("/mcp", MCP).anonymous());
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
