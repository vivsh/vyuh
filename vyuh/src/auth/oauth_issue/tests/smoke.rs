use axum::{Json, http::HeaderValue};
use serde_json::json;

use crate::auth::{
    OAuthCimdAuth, OAuthDcrAdmission,
    oauth_issue::protocol::{Parameters, no_store},
};

use super::{
    cimd_flow::{NEW, OLD, SIGNED, claims, fixture, rotate_and_revoke, rule, send_signed, signed},
    registration::{authenticate, dcr as dcr_policy, discover, register},
    *,
};

const ISSUER: &str = "https://app.example.com";
const LOGIN: &str = "/fixture/login";
const CONSENT: &str = "/consent";

/// Runs both replicas against the real migrated schema and fixture application routes.
async fn smoke_sites(issuer: OAuthIssuer) -> Result<(tempfile::TempDir, Site, Site), String> {
    let dir = tempfile::tempdir().map_err(err)?;
    let url = format!("sqlite://{}/oauth.db?mode=rwc", dir.path().display());
    let database = db::DbConf::from_url(&url).map_err(err)?;
    install(database.clone()).await?;
    let site = Site::build(
        conf_with(database.clone(), issuer.clone()),
        browser_bundle(),
    )
    .await
    .map_err(err)?;
    let peer = Site::build(conf_with(database, issuer), browser_bundle())
        .await
        .map_err(err)?;
    Ok((dir, site, peer))
}

fn browser_bundle() -> bundles::Bundle {
    bundles::bundle([
        bundles::route(authorize, fixture_route("authorize", CONSENT, Methods::GET)),
        bundles::route(approve, fixture_route("approve", CONSENT, Methods::POST)),
        bundles::route(login, fixture_route("fixture_login", LOGIN, Methods::POST)),
    ])
    .with_conf(bundles::conf().audience(BROWSER))
    .merge(tool_bundles())
}

fn fixture_route(name: &'static str, path: &'static str, methods: Methods) -> RouteConf {
    RouteConf {
        name: name.into(),
        path: path.into(),
        methods,
        trim: false,
    }
}

/// Converts the real authorization outcome into a fixture login redirect or consent response.
async fn authorize(site: Site, request: Request<Body>) -> Result<Response, OAuthIssueError> {
    let (parts, _) = request.into_parts();
    match site.oauth()?.request(&parts).await? {
        OAuthConsent::Redirect(response) => Ok(response),
        OAuthConsent::SignIn { return_to } => {
            let path = format!("{LOGIN}?{}", encode(&[("return_to", &return_to)]));
            see_other(&path)
        }
        OAuthConsent::Ready {
            continuation,
            requested_scopes,
            ..
        } => Ok(no_store(Json(
            json!({ "continuation": continuation, "scopes": requested_scopes }),
        ))),
    }
}

/// Issues a normal test-account cookie and accepts only a local return to the consent route.
async fn login(site: Site, request: Request<Body>) -> Result<Response, OAuthIssueError> {
    let bad = OAuthIssueError::InvalidRequest;
    let query = Parameters::parse(request.uri().query().ok_or(bad)?.as_bytes())?;
    let target = query.get("return_to").ok_or(bad)?;
    let parsed: axum::http::Uri = target.parse().map_err(|_| bad)?;
    if parsed.scheme().is_some()
        || parsed.authority().is_some()
        || parsed.path() != CONSENT
        || parsed.query().is_none()
    {
        return Err(bad);
    }
    let login = site
        .auth()
        .using(COOKIES)
        .issue(
            AuthUser::new("person").with_scopes([READ, WRITE]),
            &[BROWSER],
        )
        .await?;
    let mut response = see_other(target)?;
    login.write(&mut response);
    Ok(response)
}

/// Submits the fixture's read-only consent through the normal cookie and CSRF checks.
async fn approve(site: Site, request: Request<Body>) -> Result<Response, OAuthIssueError> {
    let (parts, body) = request.into_parts();
    let bad = OAuthIssueError::InvalidRequest;
    let bytes = to_bytes(body, 16 * 1024).await.map_err(|_| bad)?;
    let json: Value = serde_json::from_slice(&bytes).map_err(|_| bad)?;
    let continuation = json
        .get("continuation")
        .and_then(Value::as_str)
        .ok_or(bad)?;
    site.oauth()?.approve(&parts, continuation, &[READ]).await
}

fn see_other(path: &str) -> Result<Response, OAuthIssueError> {
    let path = HeaderValue::from_str(path).map_err(|_| OAuthIssueError::InvalidRequest)?;
    Ok(no_store((
        StatusCode::SEE_OTHER,
        [(header::LOCATION, path)],
    )))
}

async fn raw(site: &Site, request: Request<Body>) -> Result<Response, String> {
    site.router().oneshot(request).await.map_err(err)
}

fn relative_location(response: &Response) -> Result<&str, String> {
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    response
        .headers()
        .get(header::LOCATION)
        .ok_or("missing redirect")?
        .to_str()
        .map_err(err)
}

/// Follows the application's redirects, keeps its cookies, and posts consent over HTTP.
async fn browser_code(site: &Site, id: &str) -> Result<String, String> {
    let path = format!("{CONSENT}?{}", client_query(id, REDIRECT, READ.as_str()));
    let start = raw(site, Request::get(&path).body(Body::empty()).map_err(err)?).await?;
    let login_uri = relative_location(&start)?;
    assert!(login_uri.starts_with("/fixture/login?"));
    let login = raw(
        site,
        Request::post(login_uri).body(Body::empty()).map_err(err)?,
    )
    .await?;
    assert_eq!(relative_location(&login)?, path);
    let (cookie, csrf) = browser_cookies(login.headers())?;
    let (status, _, consent) = request(
        site,
        Request::get(relative_location(&login)?)
            .header(header::COOKIE, &cookie)
            .body(Body::empty())
            .map_err(err)?,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(consent["scopes"], json!([READ.as_str()]));
    let continuation = token(&consent, "continuation")?;
    let body = json!({"continuation": continuation}).to_string();
    let post = || {
        Request::post(CONSENT)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::COOKIE, &cookie)
    };
    let denied = raw(site, post().body(Body::from(body.clone())).map_err(err)?).await?;
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    let accepted = raw(
        site,
        post()
            .header("x-csrf-token", &csrf)
            .body(Body::from(body))
            .map_err(err)?,
    )
    .await?;
    let callback = location(accepted)?;
    assert_eq!(
        callback.origin().ascii_serialization(),
        "https://client.example.com"
    );
    assert_eq!(callback.path(), "/callback");
    assert_eq!(param(&callback, "iss")?, ISSUER);
    assert_eq!(param(&callback, "state")?, "state / round-trip");
    param(&callback, "code")
}

fn exchange_fields(code: &str) -> [(&str, &str); 5] {
    [
        ("grant_type", "authorization_code"),
        ("code", code),
        ("code_verifier", VERIFIER),
        ("redirect_uri", REDIRECT),
        ("resource", MCP.as_str()),
    ]
}

fn refresh_fields(refresh: &str) -> [(&str, &str); 3] {
    [
        ("grant_type", "refresh_token"),
        ("refresh_token", refresh),
        ("resource", MCP.as_str()),
    ]
}

/// Calls a fixture MCP tool over the same authenticated route as the discovery request.
async fn tool(site: &Site, access: &str, name: &str) -> Result<Value, String> {
    let args = json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{
        "name":name,"arguments":{"message":"read-only smoke"} }});
    let (_, _, result) = request(
        site,
        Request::post("/mcp")
            .header(header::CONTENT_TYPE, "application/json")
            .header("mcp-protocol-version", "2025-11-25")
            .header(header::AUTHORIZATION, format!("Bearer {access}"))
            .body(Body::from(args.to_string()))
            .map_err(err)?,
    )
    .await?;
    Ok(result)
}

/// Proves scope enforcement with an actual tool call and a different resource.
async fn read_only(site: &Site, access: &str) -> Result<(), String> {
    let (status, _, reply) = mcp(site, "/mcp", Some(access)).await?;
    assert_eq!(status, StatusCode::OK);
    let tools = reply
        .pointer("/result/tools")
        .and_then(Value::as_array)
        .ok_or("missing tools")?;
    assert_eq!(tools.len(), 1);
    assert_eq!(
        tools
            .first()
            .and_then(|tool| tool.get("name"))
            .and_then(Value::as_str),
        Some("echo")
    );
    let result = tool(site, access, "echo").await?;
    assert_eq!(
        result.pointer("/result/structuredContent/message"),
        Some(&json!("read-only smoke"))
    );
    let denied = tool(site, access, "write_note").await?;
    assert!(
        denied.get("error").is_some()
            || denied.pointer("/result/isError") == Some(&Value::Bool(true))
    );
    assert_eq!(
        mcp(site, "/other", Some(access)).await?.0,
        StatusCode::UNAUTHORIZED
    );
    Ok(())
}

/// Signs in and consents over HTTP, then proves replay defense, key rotation and revocation.
#[tokio::test]
async fn signed_cimd() -> Result<(), String> {
    let config = issuer_base().cimd_client(rule(SIGNED, OAuthCimdAuth::PrivateKeyJwt));
    let (_dir, site, peer) = smoke_sites(config).await?;
    let discovery = discover(&site).await?;
    assert_eq!(discovery["client_id_metadata_document_supported"], true);
    assert!(discovery.get("registration_endpoint").is_none());
    let no_registration = raw(
        &site,
        Request::post("/oauth/register")
            .body(Body::empty())
            .map_err(err)?,
    )
    .await?;
    assert_eq!(no_registration.status(), StatusCode::NOT_FOUND);
    let refresh = fixture(&OLD, signed_exchange(&site, &peer)).await?;
    fixture(&NEW, rotate_and_revoke(&site, &peer, &refresh)).await
}

/// Exchanges a browser code only with a signed client assertion and rejects reuse on a peer.
async fn signed_exchange(site: &Site, peer: &Site) -> Result<String, String> {
    let code = browser_code(site, SIGNED).await?;
    let fields = exchange_fields(&code);
    let mut missing = fields.to_vec();
    missing.push(("client_id", SIGNED));
    assert_eq!(
        send(site, "/oauth/token", &missing).await?.0,
        StatusCode::UNAUTHORIZED
    );
    let jwt = signed(&OLD, &claims("one-code-jti"))?;
    let (status, _, reply) = send_signed(peer, "/oauth/token", &fields, &jwt).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        send_signed(site, "/oauth/token", &fields, &jwt).await?.0,
        StatusCode::UNAUTHORIZED
    );
    read_only(site, &token(&reply, "access_token")?).await?;
    token(&reply, "refresh_token")
}

/// Registers public and confidential clients separately with CIMD disabled.
#[tokio::test]
async fn dcr() -> Result<(), String> {
    let issuer = issuer_base().dynamic_registration(dcr_policy(OAuthDcrAdmission::Unauthenticated));
    let (_dir, site, peer) = smoke_sites(issuer).await?;
    let discovery = discover(&site).await?;
    assert!(
        discovery
            .get("client_id_metadata_document_supported")
            .is_none()
    );
    assert_eq!(
        discovery["registration_endpoint"],
        format!("{ISSUER}/oauth/register")
    );
    rejects_registration(&site).await?;
    for method in ["none", "client_secret_basic"] {
        dcr_flow(&site, &peer, method).await?;
    }
    let (id, _, reply) = dcr_exchange(&site, &peer, "none").await?;
    let access = token(&reply, "access_token")?;
    assert_eq!(mcp(&peer, "/mcp", Some(&access)).await?.0, StatusCode::OK);
    assert!(
        site.oauth()
            .map_err(err)?
            .disable_registration(&id)
            .await
            .map_err(err)?
    );
    assert_eq!(
        mcp(&peer, "/mcp", Some(&access)).await?.0,
        StatusCode::UNAUTHORIZED
    );
    Ok(())
}

/// Requires registration to reject both an unapproved callback and a write scope.
async fn rejects_registration(site: &Site) -> Result<(), String> {
    for (input, error) in [
        (
            json!({"redirect_uris": ["https://client.example.com/wrong"]}),
            "invalid_redirect_uri",
        ),
        (
            json!({"redirect_uris": [REDIRECT], "scope": WRITE.as_str()}),
            "invalid_client_metadata",
        ),
    ] {
        let (status, _, reply) = register(site, input, None).await?;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(reply["error"], error);
    }
    Ok(())
}

/// Checks registered authentication across instances, then rotates and revokes the grant.
async fn dcr_flow(site: &Site, peer: &Site, method: &str) -> Result<(), String> {
    let (id, secret, reply) = dcr_exchange(site, peer, method).await?;
    let access = token(&reply, "access_token")?;
    read_only(site, &access).await?;
    let refresh = token(&reply, "refresh_token")?;
    let (status, _, rotated) = authenticate(
        site,
        "/oauth/token",
        &refresh_fields(&refresh),
        &id,
        method,
        secret.as_deref(),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        mcp(peer, "/mcp", Some(&access)).await?.0,
        StatusCode::UNAUTHORIZED
    );
    let rotated_access = token(&rotated, "access_token")?;
    assert_eq!(
        mcp(peer, "/mcp", Some(&rotated_access)).await?.0,
        StatusCode::OK
    );
    assert_eq!(
        authenticate(
            peer,
            "/oauth/revoke",
            &[("token", &rotated_access)],
            &id,
            method,
            secret.as_deref()
        )
        .await?
        .0,
        StatusCode::OK
    );
    assert_eq!(
        mcp(site, "/mcp", Some(&rotated_access)).await?.0,
        StatusCode::UNAUTHORIZED
    );
    Ok(())
}

/// Obtains a registered client and exchanges a code on a second site using its exact method.
async fn dcr_exchange(
    site: &Site,
    peer: &Site,
    method: &str,
) -> Result<(String, Option<String>, Value), String> {
    let (status, _, registration) = register(site, json!({"redirect_uris": [REDIRECT],
        "token_endpoint_auth_method": method, "grant_types": ["authorization_code", "refresh_token"]}), None).await?;
    assert_eq!(status, StatusCode::CREATED);
    let id = token(&registration, "client_id")?;
    let secret = registration
        .get("client_secret")
        .and_then(Value::as_str)
        .map(str::to_owned);
    assert_eq!(secret.is_some(), method == "client_secret_basic");
    let code = browser_code(site, &id).await?;
    let fields = exchange_fields(&code);
    let wrong_secret = secret.as_deref().unwrap_or("unexpected-test-secret");
    assert_eq!(
        authenticate(
            peer,
            "/oauth/token",
            &fields,
            &id,
            "client_secret_post",
            Some(wrong_secret)
        )
        .await?
        .0,
        StatusCode::UNAUTHORIZED
    );
    let (status, _, reply) = authenticate(
        peer,
        "/oauth/token",
        &fields,
        &id,
        method,
        secret.as_deref(),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    Ok((id, secret, reply))
}
