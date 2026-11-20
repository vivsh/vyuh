use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::json;

use crate::mcp::{McpProfile, McpProfileArgs, McpResource, McpToolConf};

use super::*;

const METADATA: &str = "/.well-known/oauth-protected-resource/mcp";
const WIDGET: &str = "ui://widget/profile.html";
const PROFILE: &str = "profile";
const SERVER_NAME: &str = "Fixture Notes";
const SERVER_VERSION: &str = "test-build";
const SERVER_INSTRUCTIONS: &str = "Read sample notes after signing in.";
static PROFILE_CALLS: AtomicUsize = AtomicUsize::new(0);
static READ_CALLS: AtomicUsize = AtomicUsize::new(0);
static READ_SCOPE: &[Scope] = &[READ];
static ALTERNATIVES: &[Scope] = &[READ, WRITE];
static UNADVERTISED_SCOPE: &[Scope] = &[Scope::of("notes:archive")];

struct Read;
struct Either;
struct Unadvertised;

impl ScopeRule for Read {
    const EXPR: ScopeExpr = ScopeExpr::all(READ_SCOPE);
}

impl ScopeRule for Either {
    const EXPR: ScopeExpr = ScopeExpr::any(ALTERNATIVES);
}

impl ScopeRule for Unadvertised {
    const EXPR: ScopeExpr = ScopeExpr::all(UNADVERTISED_SCOPE);
}

#[bundles::mcp_tool(allow_anonymous = true)]
async fn public(input: Data<Echo>) -> Data<Echo> {
    echo(input).await
}

#[bundles::mcp_tool(
    allow_anonymous = false,
    oauth_security = false,
    openai_profile = false
)]
async fn internal(input: Data<Echo>) -> Data<Echo> {
    echo(input).await
}

#[bundles::mcp_tool(oauth_security = true)]
async fn read_note(_: Permit<Read>, input: Data<Echo>) -> Data<Echo> {
    READ_CALLS.fetch_add(1, Ordering::Relaxed);
    input
}

async fn either_note(_: Permit<Either>, input: Data<Echo>) -> Data<Echo> {
    input
}

async fn archive_note(_: Permit<Unadvertised>, input: Data<Echo>) -> Data<Echo> {
    input
}

/// Returns a stable synthetic account ID and an invalid ID for the failure fixture.
#[bundles::mcp_tool(
    read_only = true,
    destructive = false,
    idempotent = true,
    open_world = false,
    oauth_security = true,
    openai_profile = true,
    ui_resource_uri = "ui://widget/profile.html"
)]
async fn profile(user: AuthUser, _: Data<McpProfileArgs>) -> Data<McpProfile> {
    PROFILE_CALLS.fetch_add(1, Ordering::Relaxed);
    let id = match user.subject() {
        "blank" => " \t".into(),
        subject => format!("fixture:{subject}"),
    };
    Data::new(McpProfile {
        id,
        name: Some("Test profile label".into()),
        email: None,
        nickname: None,
    })
}

#[bundles::mcp_tool(read_only = true, destructive = true, oauth_security, openai_profile)]
async fn bad_profile(user: AuthUser, args: Data<McpProfileArgs>) -> Data<McpProfile> {
    profile(user, args).await
}

async fn wrong_profile(user: AuthUser, _: Data<McpProfileArgs>) -> Data<Echo> {
    Data::new(Echo {
        message: user.subject().into(),
    })
}

async fn anonymous_profile(_: Data<McpProfileArgs>) -> Data<McpProfile> {
    Data::new(McpProfile {
        id: "anonymous".into(),
        name: None,
        email: None,
        nickname: None,
    })
}

fn profile_conf() -> McpToolConf {
    McpToolConf::default()
        .read_only(true)
        .oauth_security()
        .openai_profile()
}

fn allowed(user: &AuthUser) -> bool {
    user.subject() != "blocked"
}

/// Registers the same callable and tool config as the attributed fixture.
fn direct_tools() -> bundles::Bundle {
    bundles::bundle([
        bundles::mcp_tool("public", public, McpToolConf::default().allow_anonymous()),
        bundles::mcp_tool("internal", internal, McpToolConf::default()),
        bundles::mcp_tool(
            "read_note",
            read_note,
            McpToolConf::default().oauth_security(),
        ),
        bundles::mcp_tool(
            PROFILE,
            profile,
            profile_conf()
                .destructive(false)
                .idempotent(true)
                .open_world(false)
                .ui_resource_uri(WIDGET),
        )
        .patch(crate::callables::PatchOp::new().description(
            "Returns a stable synthetic account ID and an invalid ID for the failure fixture.",
        )),
    ])
}

/// Mounts either registration form with the same profile UI resource and endpoint policy.
fn mixed_bundle(tools: bundles::Bundle) -> bundles::Bundle {
    let resources = bundles::bundle([bundles::mcp_resource(
        "profile_widget",
        McpResource::text(WIDGET, "text/html;profile=mcp-app", "<main>Profile</main>"),
    )]);
    let tools = tools.merge(resources).with_conf(
        bundles::conf().audience(MCP).mcp(
            McpConf::new("/mcp")
                .auth(allowed)
                .mixed_oauth()
                .server_name(SERVER_NAME)
                .server_version(SERVER_VERSION)
                .instructions(SERVER_INSTRUCTIONS),
        ),
    );
    browser_bundle().merge(tools).merge(other_endpoint())
}

/// Registers a fully protected endpoint with the same profile tool and a separate audience.
fn other_endpoint() -> bundles::Bundle {
    bundles::bundle([
        bundles::mcp_tool("other_echo", other_echo, McpToolConf::default()),
        bundles::mcp_tool(PROFILE, profile, profile_conf()),
    ])
    .with_conf(
        bundles::conf()
            .audience(OTHER_MCP)
            .mcp(McpConf::new("/other")),
    )
}

/// Uses the same SQLite migration and real OAuth provider as the standalone smoke tests.
async fn mixed_site(bundle: bundles::Bundle) -> Result<(tempfile::TempDir, Site), String> {
    let dir = tempfile::tempdir().map_err(err)?;
    let database = db::DbConf::from_url(&format!(
        "sqlite://{}/mixed.db?mode=rwc",
        dir.path().display(),
    ))
    .map_err(err)?;
    install(database.clone()).await?;
    let site = Site::build(conf(database), bundle).await.map_err(err)?;
    Ok((dir, site))
}

/// Issues an opaque access token for a particular account and resource via normal consent.
async fn access(
    site: &Site,
    subject: &str,
    resource: Audience,
    scope: Scope,
) -> Result<String, String> {
    let (cookie, csrf) = session_for(site, subject).await?;
    let path = format!(
        "/consent?{}",
        query_for("client", REDIRECT, scope.as_str(), resource)
    );
    let start = parts("GET", &path, &[("cookie", &cookie)])?;
    let oauth = site.oauth().map_err(err)?;
    let OAuthConsent::Ready { continuation, .. } = oauth.request(&start).await.map_err(err)? else {
        return Err("expected consent".into());
    };
    let approve = parts(
        "POST",
        "/consent",
        &[("cookie", &cookie), ("x-csrf-token", &csrf)],
    )?;
    let result = oauth
        .approve(&approve, &continuation, &[scope])
        .await
        .map_err(err)?;
    let code = param(&location(result)?, "code")?;
    let (status, _, body) =
        send(site, "/oauth/token", &code_for(&code, VERIFIER, resource)).await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    token(&body, "access_token")
}

/// Sends MCP POSTs with revision-specific routing headers and optional raw Authorization.
async fn rpc(
    site: &Site,
    bearer: Option<&str>,
    method: &str,
    mut params: Value,
    modern: bool,
) -> Result<(StatusCode, HeaderMap, Value), String> {
    let mut request = Request::post("/mcp").header(header::CONTENT_TYPE, "application/json");
    if let Some(bearer) = bearer {
        request = request.header(header::AUTHORIZATION, bearer);
    }
    if modern {
        params["_meta"] = json!({
            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
            "io.modelcontextprotocol/clientInfo": {"name": "fixture", "version": "1"},
            "io.modelcontextprotocol/clientCapabilities": {},
        });
        request = request
            .header("mcp-protocol-version", "2026-07-28")
            .header("mcp-method", method);
        if method == "tools/call" {
            let name = params
                .get("name")
                .and_then(Value::as_str)
                .ok_or("missing tool name")?;
            request = request.header("mcp-name", name);
        }
    } else {
        request = request.header("mcp-protocol-version", "2025-11-25");
    }
    let body = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
    super::request(
        site,
        request.body(Body::from(body.to_string())).map_err(err)?,
    )
    .await
}

/// Calls a tool through the same revision-specific transport path as listing.
async fn call(
    site: &Site,
    bearer: Option<&str>,
    name: &str,
    args: Value,
    modern: bool,
) -> Result<(StatusCode, HeaderMap, Value), String> {
    rpc(
        site,
        bearer,
        "tools/call",
        json!({"name": name, "arguments": args}),
        modern,
    )
    .await
}

/// Returns the list payload after checking its transport and cache boundary.
async fn list(site: &Site, bearer: Option<&str>, modern: bool) -> Result<Vec<Value>, String> {
    let (status, headers, value) = rpc(site, bearer, "tools/list", json!({}), modern).await?;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(
        headers
            .get(header::CACHE_CONTROL)
            .and_then(|v| v.to_str().ok()),
        Some("no-store")
    );
    if modern {
        assert_eq!(value.pointer("/result/cacheScope"), Some(&json!("private")));
        assert_eq!(value.pointer("/result/ttlMs"), Some(&json!(0)));
    } else {
        assert!(value.pointer("/result/ttlMs").is_none());
    }
    value
        .pointer("/result/tools")
        .and_then(Value::as_array)
        .cloned()
        .ok_or("missing catalog".into())
}

fn names(tools: &[Value]) -> Vec<&str> {
    tools
        .iter()
        .filter_map(|tool| tool.get("name").and_then(Value::as_str))
        .collect()
}

/// Verifies a signed-out caller sees only opted-in tools and gets a challenge before dispatch.
async fn signed_out(site: &Site, modern: bool) -> Result<(), String> {
    let tools = list(site, None, modern).await?;
    assert_eq!(names(&tools), ["profile", "public", "read_note"]);
    for tool in &tools {
        let expected = match tool.get("name").and_then(Value::as_str) {
            Some(PROFILE) => json!([{"type": "oauth2", "scopes": []}]),
            Some("public") => json!([{"type": "noauth"}]),
            _ => json!([{"type": "oauth2", "scopes": ["notes:read"]}]),
        };
        assert_eq!(tool.get("securitySchemes"), Some(&expected));
        assert_eq!(tool.pointer("/_meta/securitySchemes"), Some(&expected));
        if tool.get("name") == Some(&json!(PROFILE)) {
            assert_profile_schema(tool);
        } else {
            assert!(tool.pointer("/_meta/openai~1profile").is_none());
        }
    }
    let (_, _, public) = call(site, None, "public", json!({"message":"hello"}), modern).await?;
    assert_eq!(
        public.pointer("/result/structuredContent/message"),
        Some(&json!("hello"))
    );
    let (_, _, hidden) = call(site, None, "internal", json!({"message":"hidden"}), modern).await?;
    assert_eq!(
        hidden.pointer("/error/message"),
        Some(&json!("unknown tool"))
    );
    for (name, scope) in [("read_note", ", scope=\"notes:read\""), (PROFILE, "")] {
        let (status, headers, denied) = call(site, None, name, json!({}), modern).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            headers
                .get(header::CACHE_CONTROL)
                .and_then(|v| v.to_str().ok()),
            Some("no-store")
        );
        assert_eq!(denied.pointer("/result/isError"), Some(&json!(true)));
        let challenge = format!(
            "Bearer resource_metadata=\"https://app.example.com{METADATA}\", error=\"insufficient_scope\", error_description=\"Sign in to use this tool\"{scope}"
        );
        assert_eq!(
            denied.pointer("/result/_meta/mcp~1www_authenticate"),
            Some(&json!([challenge]))
        );
        assert_eq!(
            denied.pointer("/result/resultType"),
            modern.then(|| json!("complete")).as_ref()
        );
    }
    for (method, params) in [
        ("resources/list", json!({})),
        ("resources/read", json!({"uri": WIDGET})),
    ] {
        let (status, headers, body) = rpc(site, None, method, params, modern).await?;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(headers.contains_key(header::WWW_AUTHENTICATE));
        assert!(body.get("result").is_none());
    }
    Ok(())
}

/// Checks that profile metadata, UI data, and the strict argument and result schemas coexist.
fn assert_profile_schema(tool: &Value) {
    assert_eq!(tool.pointer("/_meta/openai~1profile"), Some(&json!(true)));
    assert_eq!(tool.pointer("/_meta/ui/resourceUri"), Some(&json!(WIDGET)));
    assert_eq!(
        tool.pointer("/inputSchema/additionalProperties"),
        Some(&json!(false))
    );
    assert_eq!(tool.pointer("/inputSchema/properties"), Some(&json!({})));
    assert_eq!(
        tool.pointer("/outputSchema/additionalProperties"),
        Some(&json!(false))
    );
    assert_eq!(tool.pointer("/outputSchema/required"), Some(&json!(["id"])));
    for field in ["id", "name", "email", "nickname"] {
        assert_eq!(
            tool.pointer(&format!("/outputSchema/properties/{field}/type")),
            Some(&json!("string"))
        );
    }
}

/// Compares both registration paths through the same OAuth and MCP transport checks.
#[tokio::test]
async fn mixed_oauth_registration_parity() -> Result<(), String> {
    let (_direct_dir, direct) = mixed_site(mixed_bundle(direct_tools())).await?;
    let macro_tools = bundles::bundle! { public, internal, read_note, profile };
    let (_macro_dir, attributed) = mixed_site(mixed_bundle(macro_tools)).await?;
    for modern in [false, true] {
        assert_eq!(
            list(&direct, None, modern).await?,
            list(&attributed, None, modern).await?
        );
    }
    for site in [&direct, &attributed] {
        PROFILE_CALLS.store(0, Ordering::Relaxed);
        READ_CALLS.store(0, Ordering::Relaxed);
        check_mixed_transport(site).await?;
    }
    Ok(())
}

/// Exercises discovery and authorization in both MCP revisions for one registration path.
async fn check_mixed_transport(site: &Site) -> Result<(), String> {
    let (status, _, metadata) = request(
        site,
        Request::get(METADATA).body(Body::empty()).map_err(err)?,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(metadata.get("resource"), Some(&json!(MCP.as_str())));
    for (method, params, modern) in [
        (
            "initialize",
            json!({"protocolVersion":"2025-11-25","clientInfo":{"name":"test","version":"1"},"capabilities":{}}),
            false,
        ),
        ("server/discover", json!({}), true),
    ] {
        let (status, headers, body) = rpc(site, None, method, params, modern).await?;
        assert_eq!(status, StatusCode::OK);
        assert!(!headers.contains_key(header::CACHE_CONTROL));
        assert!(body.pointer("/result/capabilities/tools").is_some());
        assert_eq!(
            body.pointer("/result/instructions"),
            Some(&json!(SERVER_INSTRUCTIONS))
        );
        assert_eq!(
            body.pointer("/result/ttlMs"),
            modern.then(|| json!(0)).as_ref()
        );
        signed_out(site, modern).await?;
    }
    assert_eq!(PROFILE_CALLS.load(Ordering::Relaxed), 0);
    assert_eq!(READ_CALLS.load(Ordering::Relaxed), 0);
    assert_signed_in(site).await?;
    assert_auth_failures(site).await
}

/// Requires identical startup rejection for the same invalid profile options on both paths.
#[tokio::test]
async fn rejects_invalid_profile_on_both_paths() -> Result<(), String> {
    let direct = bundles::bundle([bundles::mcp_tool(
        "bad_profile",
        bad_profile,
        profile_conf().destructive(true),
    )]);
    let attributed = bundles::bundle! { bad_profile };
    let direct_error = mixed_site(mixed_bundle(direct))
        .await
        .err()
        .ok_or("invalid direct profile was accepted")?;
    let macro_error = mixed_site(mixed_bundle(attributed))
        .await
        .err()
        .ok_or("invalid attributed profile was accepted")?;
    assert_eq!(macro_error, direct_error);
    assert!(macro_error.contains("profile tools require OAuth publication and read-only"));
    Ok(())
}

/// Exercises permit denial, successful dispatch, and stable per-account profile results.
async fn assert_signed_in(site: &Site) -> Result<(), String> {
    let alice = format!("Bearer {}", access(site, "alice", MCP, READ).await?);
    let bob = format!("Bearer {}", access(site, "bob", MCP, WRITE).await?);
    for modern in [false, true] {
        let listed = list(site, Some(&alice), modern).await?;
        assert_eq!(
            names(&listed),
            ["internal", "profile", "public", "read_note"]
        );
        let internal = listed.first().ok_or("missing internal tool")?;
        assert!(internal.get("securitySchemes").is_none());
        assert!(internal.get("_meta").is_none());
        let listed = list(site, Some(&bob), modern).await?;
        assert_eq!(names(&listed), ["internal", "profile", "public"]);
        let (_, _, accepted) = call(
            site,
            Some(&alice),
            "read_note",
            json!({"message":"visible"}),
            modern,
        )
        .await?;
        assert_eq!(
            accepted.pointer("/result/structuredContent/message"),
            Some(&json!("visible"))
        );
        let (_, _, denied) = call(
            site,
            Some(&bob),
            "read_note",
            json!({"message":"denied"}),
            modern,
        )
        .await?;
        let (_, _, absent) = call(site, Some(&bob), "absent", json!({}), modern).await?;
        assert_eq!(denied, absent);
        for (bearer, id) in [(&alice, "fixture:alice"), (&bob, "fixture:bob")] {
            let (_, _, value) = call(site, Some(bearer), PROFILE, json!({}), modern).await?;
            let expected = json!({"id":id,"name":"Test profile label"});
            assert_eq!(value.pointer("/result/structuredContent"), Some(&expected));
            assert_eq!(
                value.pointer("/result/content/0/text"),
                Some(&json!(expected.to_string()))
            );
        }
        let (_, _, injected) =
            call(site, Some(&alice), PROFILE, json!({"id":"bob"}), modern).await?;
        assert_eq!(
            injected.pointer("/error/message"),
            Some(&json!("invalid params"))
        );
    }
    assert_eq!(PROFILE_CALLS.load(Ordering::Relaxed), 4);
    assert_eq!(READ_CALLS.load(Ordering::Relaxed), 2);
    let again = format!("Bearer {}", access(site, "alice", MCP, WRITE).await?);
    let (_, _, reconnected) = call(site, Some(&again), PROFILE, json!({}), true).await?;
    assert_eq!(
        reconnected.pointer("/result/structuredContent/id"),
        Some(&json!("fixture:alice"))
    );
    let blank = format!("Bearer {}", access(site, "blank", MCP, READ).await?);
    let (_, _, invalid) = call(site, Some(&blank), PROFILE, json!({}), true).await?;
    assert_eq!(invalid.pointer("/result/isError"), Some(&json!(true)));
    assert_eq!(
        invalid.pointer("/result/structuredContent/error"),
        Some(&json!("tool execution failed"))
    );
    assert!(!invalid.to_string().contains("Test profile label"));
    let (status, _, resource) = rpc(
        site,
        Some(&alice),
        "resources/read",
        json!({"uri":WIDGET}),
        true,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(resource.pointer("/result/ttlMs"), Some(&json!(0)));
    assert_eq!(
        resource.pointer("/result/cacheScope"),
        Some(&json!("private"))
    );
    assert_eq!(
        resource.pointer("/result/contents/0/text"),
        Some(&json!("<main>Profile</main>"))
    );
    let (status, _, resources) = rpc(site, Some(&alice), "resources/list", json!({}), true).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(resources.pointer("/result/ttlMs"), Some(&json!(0)));
    assert_eq!(
        resources.pointer("/result/cacheScope"),
        Some(&json!("private"))
    );
    Ok(())
}

/// Confirms supplying a bad credential never falls back even for an anonymous tool.
async fn assert_auth_failures(site: &Site) -> Result<(), String> {
    let wrong = access(site, "reader", OTHER_MCP, READ).await?;
    assert_eq!(mcp(site, "/other", None).await?.0, StatusCode::UNAUTHORIZED);
    let (status, _, list) = mcp(site, "/other", Some(&wrong)).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        list.pointer("/result/tools/1/_meta/openai~1profile"),
        Some(&json!(true))
    );
    let wrong = format!("Bearer {wrong}");
    let blocked = format!("Bearer {}", access(site, "blocked", MCP, READ).await?);
    let revoked = access(site, "revoked", MCP, READ).await?;
    let (status, _, _) = send(
        site,
        "/oauth/revoke",
        &[("client_id", "client"), ("token", &revoked)],
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    let expired = access(site, "expired", MCP, READ).await?;
    let digest = super::super::runtime::credential_digest(&expired).ok_or("bad test credential")?;
    site.db()
        .execute(
            super::super::store::statement(
                "UPDATE vyuh_oauth_credentials SET expires_at = 1 WHERE digest = ?",
            )
            .bind(digest),
        )
        .await
        .map_err(err)?;
    for (auth, expected) in [
        ("Bearer invalid".into(), StatusCode::UNAUTHORIZED),
        ("Basic unsupported".into(), StatusCode::UNAUTHORIZED),
        (wrong, StatusCode::UNAUTHORIZED),
        (blocked, StatusCode::FORBIDDEN),
        (format!("Bearer {revoked}"), StatusCode::UNAUTHORIZED),
        (format!("Bearer {expired}"), StatusCode::UNAUTHORIZED),
    ] {
        let (status, headers, body) = call(
            site,
            Some(&auth),
            "public",
            json!({"message":"hello"}),
            true,
        )
        .await?;
        assert_eq!(status, expected, "{body}");
        assert_eq!(
            headers
                .get(header::CACHE_CONTROL)
                .and_then(|v| v.to_str().ok()),
            Some("no-store")
        );
        assert!(body.get("result").is_none());
    }
    let offline = format!("Bearer {}", access(site, "offline", MCP, READ).await?);
    site.db()
        .execute(db::Statement::raw(
            "ALTER TABLE oauth_test_account RENAME TO offline_account",
        ))
        .await
        .map_err(err)?;
    let (status, _, _) = call(
        site,
        Some(&offline),
        "public",
        json!({"message":"hello"}),
        true,
    )
    .await?;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    Ok(())
}

/// Rejects ambiguous scopes, incompatible annotations, invalid profile types and duplicate designations.
#[tokio::test]
async fn rejects_invalid_tool_configuration() -> Result<(), String> {
    let cases = [
        (
            vec![bundles::mcp_tool(
                "either",
                either_note,
                McpToolConf::default().oauth_security(),
            )],
            "conjunctive tool scopes",
        ),
        (
            vec![bundles::mcp_tool(
                "archive",
                archive_note,
                McpToolConf::default().oauth_security(),
            )],
            "tool scope cannot be advertised",
        ),
        (
            vec![bundles::mcp_tool(
                PROFILE,
                profile,
                profile_conf().destructive(true),
            )],
            "require OAuth publication and read-only",
        ),
        (
            vec![bundles::mcp_tool(PROFILE, wrong_profile, profile_conf())],
            "return Data<McpProfile>",
        ),
        (
            vec![bundles::mcp_tool(
                PROFILE,
                anonymous_profile,
                profile_conf(),
            )],
            "requires an identity extractor",
        ),
        (
            vec![
                bundles::mcp_tool("first", profile, profile_conf()),
                bundles::mcp_tool("second", profile, profile_conf()),
            ],
            "only one profile tool",
        ),
        (
            vec![bundles::mcp_tool(
                "public",
                read_note,
                McpToolConf::default().allow_anonymous(),
            )],
            "cannot require identity",
        ),
    ];
    for (tools, expected) in cases {
        let service = bundles::bundle(tools).with_conf(
            bundles::conf()
                .audience(MCP)
                .mcp(McpConf::new("/mcp").mixed_oauth()),
        );
        let bundle = browser_bundle().merge(service).merge(other_endpoint());
        let failure = mixed_site(bundle)
            .await
            .err()
            .ok_or("unexpectedly accepted tool configuration")?;
        assert!(failure.contains(expected), "{failure}");
    }
    Ok(())
}
