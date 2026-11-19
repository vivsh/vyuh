use axum::{
    body::{Body, to_bytes},
    http::{HeaderMap, Request, StatusCode, header, request::Parts},
    response::Response,
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde_json::Value;
use tower::ServiceExt;

use crate::{
    Data, Site, SiteConf,
    auth::{
        Audience, AuthConf, AuthError, AuthProvider, AuthUser, Jwt, Permit, Scope, ScopeExpr,
        ScopeRule, TokenConf, TokenProvider,
    },
    bundles,
    db::{self, DbSession},
    mcp::McpConf,
    routes::{Methods, RouteConf},
};

use super::{OAuthClient, OAuthConsent, OAuthGrantVerifier, OAuthIssueError, OAuthIssuer};

#[path = "cimd_flow.rs"]
pub(super) mod cimd_flow;
#[path = "registration.rs"]
mod registration;
#[cfg(feature = "sqlite")]
#[path = "smoke.rs"]
mod smoke;

const BROWSER: Audience = Audience::new("browser");
const MCP: Audience = Audience::new("https://app.example.com/mcp");
const OTHER_MCP: Audience = Audience::new("https://app.example.com/other");
const COOKIES: AuthProvider = AuthProvider::new("browser");
const READ: Scope = Scope::of("notes:read");
const WRITE: Scope = Scope::of("notes:write");
const VERIFIER: &str = "abcdefghijklmnopqrstuvwxyz0123456789abcdefghi";
const REDIRECT: &str = "https://client.example.com/callback";

struct Account;

impl OAuthGrantVerifier for Account {
    async fn verify(&self, site: &Site, subject: &str) -> Result<AuthUser, AuthError> {
        let mut pool = site.db();
        let (active,): (bool,) = pool
            .fetch_one(db::Statement::raw("SELECT enabled FROM oauth_test_account"))
            .await
            .map_err(|_| AuthError::ProviderUnavailable)?;
        if !active {
            return Err(AuthError::InvalidCredential);
        }
        Ok(AuthUser::new(subject).with_scopes([READ, WRITE]))
    }
}

#[derive(serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
struct Echo {
    message: String,
}

#[bundles::mcp_tool]
async fn echo(Data(value): Data<Echo>) -> Data<Echo> {
    Data(value)
}

struct Write;
const WRITE_SCOPES: &[Scope] = &[WRITE];

impl ScopeRule for Write {
    const EXPR: ScopeExpr = ScopeExpr::all(WRITE_SCOPES);
}

#[bundles::mcp_tool]
async fn write_note(_permit: Permit<Write>, Data(value): Data<Echo>) -> Data<Echo> {
    Data(value)
}

#[bundles::mcp_tool]
async fn other_echo(Data(value): Data<Echo>) -> Data<Echo> {
    Data(value)
}

fn issuer() -> OAuthIssuer {
    issuer_base()
        .client(OAuthClient::new("client", "Client").redirect_uri(REDIRECT))
        .client(OAuthClient::new("other-client", "Other client").redirect_uri(REDIRECT))
}

pub(super) fn issuer_base() -> OAuthIssuer {
    OAuthIssuer::new("https://app.example.com")
        .authorize_route("authorize")
        .browser_audience(BROWSER)
        .resource(MCP, [READ, WRITE])
        .resource(OTHER_MCP, [READ])
        .verifier(Account)
}

fn conf(database: db::DbConf) -> SiteConf {
    conf_with(database, issuer())
}

pub(super) fn conf_with(database: db::DbConf, issuer: OAuthIssuer) -> SiteConf {
    SiteConf::default().database(database).log_init(false).auth(
        AuthConf::default()
            .provider(
                COOKIES,
                TokenProvider::new(Jwt::hs256_site_secret())
                    .access(TokenConf::cookie("browser_session"))
                    .without_refresh()
                    .audiences([BROWSER]),
            )
            .oauth_issuer(issuer),
    )
}

pub(super) fn bundle() -> bundles::Bundle {
    let browser = bundles::bundle([bundles::route(
        || async {},
        RouteConf {
            name: "authorize".into(),
            methods: Methods::GET,
            path: "/consent".into(),
            trim: false,
        },
    )])
    .with_conf(bundles::conf().audience(BROWSER));
    browser.merge(tool_bundles())
}

fn tool_bundles() -> bundles::Bundle {
    bundles::bundle! { echo, write_note }
        .with_conf(bundles::conf().audience(MCP).mcp(McpConf::new("/mcp")))
        .merge(
            bundles::bundle! { other_echo }.with_conf(
                bundles::conf()
                    .audience(OTHER_MCP)
                    .mcp(McpConf::new("/other")),
            ),
        )
}

#[cfg(feature = "sqlite")]
const DIALECT: db::Dialect = db::Dialect::Sqlite;
#[cfg(feature = "postgres")]
const DIALECT: db::Dialect = db::Dialect::Postgres;
#[cfg(feature = "mysql")]
const DIALECT: db::Dialect = db::Dialect::Mysql;

/// Applies the actual generated Mool/Gaman schema, not parallel handwritten DDL.
async fn install(mut database: db::DbConf) -> Result<(), String> {
    // SQLite's generated rebuild uses connection-local PRAGMAs.
    database.max_connections = 1;
    database.min_connections = 1;
    let mut pool = db::DbPool::from_conf(&database).await.map_err(err)?;
    let planner = db::gaman::OfflinePlanner::new(DIALECT);
    let desired = super::schema().map_err(err)?;
    let migration = planner
        .make_migration(desired, &[])
        .map_err(err)?
        .ok_or("empty migration")?;
    for statement in planner.sql_migrate(&[migration]).map_err(err)? {
        pool.execute(db::Statement::raw(&statement))
            .await
            .map_err(|error| format!("{error}: {statement}"))?;
    }
    pool.execute(db::Statement::raw(
        "CREATE TABLE oauth_test_account (enabled BOOLEAN NOT NULL)",
    ))
    .await
    .map_err(err)?;
    pool.execute(db::Statement::raw(
        "INSERT INTO oauth_test_account (enabled) VALUES (TRUE)",
    ))
    .await
    .map_err(err)?;
    Ok(())
}

fn err(error: impl std::fmt::Display) -> String {
    error.to_string()
}

async fn session(site: &Site) -> Result<(String, String), String> {
    let login = site
        .auth()
        .using(COOKIES)
        .issue(
            AuthUser::new("person").with_scopes([READ, WRITE]),
            &[BROWSER],
        )
        .await
        .map_err(err)?;
    let mut response = Response::new(Body::empty());
    login.write(&mut response);
    browser_cookies(response.headers())
}

fn browser_cookies(headers: &HeaderMap) -> Result<(String, String), String> {
    let cookies = headers.get_all(header::SET_COOKIE);
    let cookies = cookies
        .iter()
        .map(|value| value.to_str().map_err(err))
        .map(|value| value.map(|value| value.split(';').next().unwrap_or_default()))
        .collect::<Result<Vec<_>, _>>()?;
    let csrf = cookies
        .iter()
        .find_map(|cookie| cookie.strip_prefix("browser_session_csrf="))
        .ok_or("missing CSRF cookie")?
        .to_owned();
    Ok((cookies.join("; "), csrf))
}

fn parts(method: &str, path: &str, headers: &[(&str, &str)]) -> Result<Parts, String> {
    let mut request = Request::builder().method(method).uri(path);
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    Ok(request.body(()).map_err(err)?.into_parts().0)
}

fn query() -> String {
    client_query("client", REDIRECT, "notes:write notes:read")
}

fn client_query(id: &str, redirect: &str, scope: &str) -> String {
    let challenge = URL_SAFE_NO_PAD.encode(ring::digest::digest(
        &ring::digest::SHA256,
        VERIFIER.as_bytes(),
    ));
    encode(&[
        ("client_id", id),
        ("redirect_uri", redirect),
        ("resource", MCP.as_str()),
        ("response_type", "code"),
        ("code_challenge", &challenge),
        ("code_challenge_method", "S256"),
        ("scope", scope),
        ("state", "state / round-trip"),
    ])
}

fn encode(parameters: &[(&str, &str)]) -> String {
    url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(parameters.iter())
        .finish()
}

async fn consent(site: &Site, cookies: &str) -> Result<String, String> {
    let path = format!("/consent?{}", query());
    let request = parts("GET", &path, &[("cookie", cookies)])?;
    let OAuthConsent::Ready {
        continuation,
        requested_scopes,
        ..
    } = site
        .oauth()
        .map_err(err)?
        .request(&request)
        .await
        .map_err(err)?
    else {
        return Err("expected application consent".into());
    };
    assert_eq!(requested_scopes, [READ, WRITE]);
    Ok(continuation)
}

fn location(response: Response) -> Result<url::Url, String> {
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    let location = response
        .headers()
        .get(header::LOCATION)
        .ok_or("missing location")?
        .to_str()
        .map_err(err)?;
    url::Url::parse(location).map_err(err)
}

fn param(url: &url::Url, key: &str) -> Result<String, String> {
    url.query_pairs()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.into_owned())
        .ok_or_else(|| format!("missing {key}"))
}

async fn request(
    site: &Site,
    request: Request<Body>,
) -> Result<(StatusCode, HeaderMap, Value), String> {
    let (parts, body) = site
        .router()
        .oneshot(request)
        .await
        .map_err(err)?
        .into_parts();
    let bytes = to_bytes(body, 128 * 1024).await.map_err(err)?;
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).map_err(err)?
    };
    Ok((parts.status, parts.headers, json))
}

async fn send(
    site: &Site,
    path: &str,
    data: &[(&str, &str)],
) -> Result<(StatusCode, HeaderMap, Value), String> {
    request(
        site,
        Request::post(path)
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(encode(data)))
            .map_err(err)?,
    )
    .await
}

fn code_params<'a>(code: &'a str, verifier: &'a str) -> [(&'static str, &'a str); 6] {
    [
        ("grant_type", "authorization_code"),
        ("client_id", "client"),
        ("code", code),
        ("code_verifier", verifier),
        ("redirect_uri", REDIRECT),
        ("resource", MCP.as_str()),
    ]
}

fn refresh_params(token: &str) -> [(&'static str, &str); 4] {
    [
        ("grant_type", "refresh_token"),
        ("client_id", "client"),
        ("refresh_token", token),
        ("resource", MCP.as_str()),
    ]
}

async fn mcp(
    site: &Site,
    path: &str,
    access: Option<&str>,
) -> Result<(StatusCode, HeaderMap, Value), String> {
    let mut builder = Request::post(path)
        .header(header::CONTENT_TYPE, "application/json")
        .header("mcp-protocol-version", "2025-11-25");
    if let Some(access) = access {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {access}"));
    }
    request(
        site,
        builder
            .body(Body::from(
                r#"{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}"#,
            ))
            .map_err(err)?,
    )
    .await
}

fn token(json: &Value, name: &str) -> Result<String, String> {
    json.get(name)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("missing {name}"))
}

/// Confirms sign-in, exact browser binding, CSRF, and concurrent single-use decisions.
async fn authorization(site: &Site, peer: &Site) -> Result<String, String> {
    let path = format!("/consent?{}", query());
    let oauth = site.oauth().map_err(err)?;
    match oauth
        .request(&parts("GET", &path, &[])?)
        .await
        .map_err(err)?
    {
        OAuthConsent::SignIn { return_to } => assert_eq!(return_to, path),
        _ => return Err("expected sign-in without browser credential".into()),
    }
    let (cookie, csrf) = session(site).await?;
    let continuation = consent(site, &cookie).await?;
    let no_csrf = parts("POST", "/consent", &[("cookie", &cookie)])?;
    assert!(matches!(
        oauth.approve(&no_csrf, &continuation, &[READ]).await,
        Err(OAuthIssueError::AccessDenied)
    ));
    let (other_cookie, other_csrf) = session(peer).await?;
    let other = parts(
        "POST",
        "/consent",
        &[("cookie", &other_cookie), ("x-csrf-token", &other_csrf)],
    )?;
    assert!(matches!(
        oauth.approve(&other, &continuation, &[READ]).await,
        Err(OAuthIssueError::AccessDenied)
    ));
    let post = parts(
        "POST",
        "/consent",
        &[("cookie", &cookie), ("x-csrf-token", &csrf)],
    )?;
    let deny = consent(site, &cookie).await?;
    assert_eq!(
        param(
            &location(oauth.deny(&post, &deny).await.map_err(err)?)?,
            "error"
        )?,
        "access_denied"
    );
    assert!(matches!(
        oauth.approve(&post, &deny, &[READ]).await,
        Err(OAuthIssueError::InvalidRequest)
    ));
    let peer = peer.oauth().map_err(err)?;
    let allowed = [READ];
    let (a, b) = tokio::join!(
        oauth.approve(&post, &continuation, &allowed),
        peer.approve(&post, &continuation, &allowed)
    );
    let url = match (a, b) {
        (Ok(response), Err(OAuthIssueError::InvalidRequest))
        | (Err(OAuthIssueError::InvalidRequest), Ok(response)) => location(response)?,
        _ => return Err("concurrent consent did not produce exactly one decision".into()),
    };
    assert_eq!(param(&url, "state")?, "state / round-trip");
    assert_eq!(param(&url, "iss")?, "https://app.example.com");
    param(&url, "code")
}

/// Only registered callbacks receive protocol errors and validated state.
async fn check_front_channel(site: &Site) -> Result<(), String> {
    let oauth = site.oauth().map_err(err)?;
    let path = format!("/consent?{}", query().replace("S256", "plain"));
    let request = parts("GET", &path, &[])?;
    let OAuthConsent::Redirect(response) = oauth.request(&request).await.map_err(err)? else {
        return Err("missing OAuth protocol error redirect".into());
    };
    let url = location(response)?;
    assert_eq!(param(&url, "error")?, "invalid_request");
    assert_eq!(param(&url, "state")?, "state / round-trip");
    let path = format!(
        "/consent?{}",
        query().replace("client.example.com", "attacker.example.com")
    );
    assert!(matches!(
        oauth.request(&parts("GET", &path, &[])?).await,
        Err(OAuthIssueError::InvalidRequest)
    ));
    Ok(())
}

/// Exercises the two-table migration and a browser-to-MCP flow through independent sites.
#[tokio::test]
#[cfg_attr(
    not(feature = "sqlite"),
    ignore = "requires a fresh dbharness database via VYUH_OAUTH_TEST_URL"
)]
async fn consent_exchange_mcp_and_refresh_race() -> Result<(), String> {
    #[cfg(feature = "sqlite")]
    let dir = tempfile::tempdir().map_err(err)?;
    #[cfg(feature = "sqlite")]
    let url = format!("sqlite://{}/oauth.db?mode=rwc", dir.path().display());
    #[cfg(not(feature = "sqlite"))]
    let url = std::env::var("VYUH_OAUTH_TEST_URL").map_err(err)?;
    let database = db::DbConf::from_url(&url).map_err(err)?;
    install(database.clone()).await?;
    let site = Site::build(conf(database.clone()), bundle())
        .await
        .map_err(err)?;
    let peer = Site::build(conf(database), bundle()).await.map_err(err)?;
    let (status, headers, _) = mcp(&site, "/mcp", None).await?;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(
        headers[header::WWW_AUTHENTICATE]
            .to_str()
            .map_err(err)?
            .contains("/.well-known/oauth-protected-resource/mcp")
    );
    let (_, _, metadata) = request(
        &site,
        Request::get("/.well-known/oauth-authorization-server")
            .body(Body::empty())
            .map_err(err)?,
    )
    .await?;
    assert_eq!(
        metadata["authorization_endpoint"],
        "https://app.example.com/consent"
    );
    check_front_channel(&site).await?;
    let code = authorization(&site, &peer).await?;
    let (status, _, _) = send(&peer, "/oauth/token", &code_params(&code, "invalid")).await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, headers, body) =
        send(&peer, "/oauth/token", &code_params(&code, VERIFIER)).await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(headers[header::CACHE_CONTROL], "no-store");
    assert_eq!(body["scope"], "notes:read");
    let access = token(&body, "access_token")?;
    let (status, _, listed) = mcp(&site, "/mcp", Some(&access)).await?;
    assert_eq!(status, StatusCode::OK);
    let tools = listed
        .pointer("/result/tools")
        .and_then(Value::as_array)
        .ok_or("missing tools")?;
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0]["name"], "echo");
    assert_eq!(
        mcp(&site, "/other", Some(&access)).await?.0,
        StatusCode::UNAUTHORIZED
    );
    let (status, _, _) = send(&site, "/oauth/token", &code_params(&code, "invalid")).await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(mcp(&site, "/mcp", Some(&access)).await?.0, StatusCode::OK);
    race_refresh(&site, &peer, &token(&body, "refresh_token")?).await?;
    revoke_and_prune(&site, &peer).await
}

/// A validated refresh replay revokes the winning replacement across both sites.
async fn race_refresh(site: &Site, peer: &Site, raw: &str) -> Result<(), String> {
    let params = refresh_params(raw);
    let (a, b) = tokio::join!(
        send(site, "/oauth/token", &params),
        send(peer, "/oauth/token", &params)
    );
    let (a, b) = (a?, b?);
    let (success, replay) = if a.0 == StatusCode::OK {
        (a, b)
    } else {
        (b, a)
    };
    assert_eq!(success.0, StatusCode::OK, "{}", success.2);
    assert_eq!(replay.0, StatusCode::BAD_REQUEST);
    assert_eq!(replay.2["error"], "invalid_grant");
    let access = token(&success.2, "access_token")?;
    assert_eq!(
        mcp(peer, "/mcp", Some(&access)).await?.0,
        StatusCode::UNAUTHORIZED
    );
    Ok(())
}

/// Revocation is client-bound and bounded pruning removes the dependent digests.
async fn revoke_and_prune(site: &Site, peer: &Site) -> Result<(), String> {
    let (cookie, csrf) = session(site).await?;
    let pending = consent(site, &cookie).await?;
    let post = parts(
        "POST",
        "/consent",
        &[("cookie", &cookie), ("x-csrf-token", &csrf)],
    )?;
    let url = location(
        site.oauth()
            .map_err(err)?
            .approve(&post, &pending, &[READ])
            .await
            .map_err(err)?,
    )?;
    let code = param(&url, "code")?;
    let (_, _, body) = send(site, "/oauth/token", &code_params(&code, VERIFIER)).await?;
    let access = token(&body, "access_token")?;
    let refresh = token(&body, "refresh_token")?;
    let revoke = [("client_id", "other-client"), ("token", refresh.as_str())];
    assert_eq!(
        send(peer, "/oauth/revoke", &revoke).await?.0,
        StatusCode::OK
    );
    assert_eq!(mcp(site, "/mcp", Some(&access)).await?.0, StatusCode::OK);
    site.db()
        .execute(db::Statement::raw(
            "UPDATE oauth_test_account SET enabled=FALSE",
        ))
        .await
        .map_err(err)?;
    assert_eq!(
        mcp(peer, "/mcp", Some(&access)).await?.0,
        StatusCode::UNAUTHORIZED
    );
    site.db()
        .execute(db::Statement::raw(
            "UPDATE oauth_test_account SET enabled=TRUE",
        ))
        .await
        .map_err(err)?;
    let revoke = [("client_id", "client"), ("token", refresh.as_str())];
    assert_eq!(
        send(peer, "/oauth/revoke", &revoke).await?.0,
        StatusCode::OK
    );
    assert_eq!(
        mcp(site, "/mcp", Some(&access)).await?.0,
        StatusCode::UNAUTHORIZED
    );
    let future = super::runtime::now() + super::runtime::GRANT_TTL + 1;
    assert_eq!(
        super::store::prune(site.db(), "https://app.example.com", future, 1)
            .await
            .map_err(err)?,
        1
    );
    super::store::prune(site.db(), "https://app.example.com", future, 100)
        .await
        .map_err(err)?;
    let (remaining,): (i64,) = site
        .db()
        .fetch_one(db::Statement::raw(
            "SELECT COUNT(*) FROM vyuh_oauth_credentials",
        ))
        .await
        .map_err(err)?;
    assert_eq!(remaining, 0);
    Ok(())
}
