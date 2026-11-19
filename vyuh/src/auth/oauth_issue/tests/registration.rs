use axum::http::header;
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::json;
use uuid::Uuid;

use super::*;
use crate::auth::oauth_issue::{runtime::now, store};
use crate::auth::{KeySource, OAuthDcr, OAuthDcrAdmission, OAuthDcrMethod};

const ISSUER: &str = "https://app.example.com";
const LEGACY_ACCESS: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const INITIAL: &str = "test-registration-admission-token-with-32-bytes";

pub(super) fn dcr(admission: OAuthDcrAdmission) -> OAuthDcr {
    OAuthDcr::new("Approved integration", admission)
        .redirect_uri(REDIRECT)
        .method(OAuthDcrMethod::None)
        .method(OAuthDcrMethod::ClientSecretBasic)
        .method(OAuthDcrMethod::ClientSecretPost)
        .scopes([READ])
        .allow_refresh()
}

/// Applies an upgrade to a populated legacy schema and verifies a static access grant still works.
async fn upgrade(database: &db::DbConf) -> Result<(), String> {
    let mut migration_db = database.clone();
    migration_db.max_connections = 1;
    migration_db.min_connections = 1;
    let mut pool = db::DbPool::from_conf(&migration_db).await.map_err(err)?;
    let planner = db::gaman::OfflinePlanner::new(DIALECT);
    let desired = super::super::schema().map_err(err)?;
    let mut legacy = desired.clone();
    legacy.tables.remove("vyuh_oauth_assertions");
    legacy.tables.remove("vyuh_oauth_registrations");
    legacy
        .tables
        .get_mut("vyuh_oauth_grants")
        .ok_or("missing grant table")?
        .columns
        .retain(|column| column.name != "client_binding");
    let initial = planner
        .make_migration(legacy, &[])
        .map_err(err)?
        .ok_or("empty initial migration")?;
    for sql in planner
        .sql_migrate(std::slice::from_ref(&initial))
        .map_err(err)?
    {
        pool.execute(db::Statement::raw(&sql))
            .await
            .map_err(|error| format!("{error}: {sql}"))?;
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
    seed_legacy(&mut pool).await?;
    let planner = planner.from_migrations(vec![initial]);
    let migration = planner
        .make_migration(desired, &[])
        .map_err(err)?
        .ok_or("empty upgrade")?;
    let statements = planner.sql_migrate(&[migration]).map_err(err)?;
    assert!(statements.iter().any(|sql| sql.contains("client_binding")));
    assert!(statements.iter().all(|sql| {
        !sql.trim_start()
            .to_ascii_uppercase()
            .starts_with("DROP TABLE")
    }));
    for sql in statements {
        pool.execute(db::Statement::raw(&sql))
            .await
            .map_err(|error| format!("{error}: {sql}"))?;
    }
    Ok(())
}

/// Seeds the old grant shape without a client-mode column.
async fn seed_legacy(pool: &mut db::DbPool) -> Result<(), String> {
    let grant = Uuid::new_v4();
    let expires_at = now() + 600;
    pool.execute(
        store::statement(
            "INSERT INTO vyuh_oauth_grants
        (id,issuer,client_id,subject,resource,scopes,redirect_uri,expires_at,generation,approved)
        VALUES (?,?,?,?,?,?,?,?,1,TRUE)",
        )
        .bind(grant)
        .bind(ISSUER)
        .bind("client")
        .bind("person")
        .bind(MCP.as_str())
        .bind(READ.as_str().to_owned())
        .bind(REDIRECT)
        .bind(expires_at),
    )
    .await
    .map_err(err)?;
    let digest = super::super::runtime::credential_digest(LEGACY_ACCESS)
        .ok_or("invalid static access fixture")?;
    pool.execute(
        store::statement(
            "INSERT INTO vyuh_oauth_credentials
        (digest,grant_id,kind,generation,expires_at) VALUES (?,?,?,1,?)",
        )
        .bind(digest)
        .bind(grant)
        .bind(store::ACCESS)
        .bind(expires_at),
    )
    .await
    .map_err(err)?;
    Ok(())
}

pub(super) async fn register(
    site: &Site,
    metadata: Value,
    auth: Option<&str>,
) -> Result<(StatusCode, HeaderMap, Value), String> {
    let mut req = Request::post("/oauth/register").header(header::CONTENT_TYPE, "application/json");
    if let Some(auth) = auth {
        req = req.header(header::AUTHORIZATION, format!("Bearer {auth}"));
    }
    request(
        site,
        req.body(Body::from(metadata.to_string())).map_err(err)?,
    )
    .await
}

pub(super) async fn discover(site: &Site) -> Result<Value, String> {
    let (status, _, docs) = request(
        site,
        Request::get("/.well-known/oauth-authorization-server")
            .body(Body::empty())
            .map_err(err)?,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    Ok(docs)
}

/// Uses the real browser proof and consent terminal for a newly discovered client.
pub(super) async fn issue(site: &Site, id: &str) -> Result<String, String> {
    let path = format!("/consent?{}", client_query(id, REDIRECT, READ.as_str()));
    let oauth = site.oauth().map_err(err)?;
    assert!(
        matches!(oauth.request(&parts("GET", &path, &[])?).await.map_err(err)?,
        OAuthConsent::SignIn { return_to } if return_to == path)
    );
    let (cookie, csrf) = session(site).await?;
    let request = parts("GET", &path, &[("cookie", &cookie)])?;
    let OAuthConsent::Ready {
        continuation,
        requested_scopes,
        ..
    } = oauth.request(&request).await.map_err(err)?
    else {
        return Err("expected consent".into());
    };
    assert_eq!(requested_scopes, [READ]);
    let post = parts(
        "POST",
        "/consent",
        &[("cookie", &cookie), ("x-csrf-token", &csrf)],
    )?;
    let reply = location(
        oauth
            .approve(&post, &continuation, &[READ])
            .await
            .map_err(err)?,
    )?;
    assert_eq!(param(&reply, "iss")?, ISSUER);
    param(&reply, "code")
}

pub(super) async fn authenticate(
    site: &Site,
    path: &str,
    params: &[(&str, &str)],
    id: &str,
    method: &str,
    secret: Option<&str>,
) -> Result<(StatusCode, HeaderMap, Value), String> {
    let mut params = params.to_vec();
    let mut req =
        Request::post(path).header(header::CONTENT_TYPE, "application/x-www-form-urlencoded");
    if method == "client_secret_basic" {
        let basic = STANDARD.encode(format!("{id}:{}", secret.ok_or("missing fixture secret")?));
        req = req.header(header::AUTHORIZATION, format!("Basic {basic}"));
    } else {
        params.push(("client_id", id));
        if let Some(secret) = secret {
            params.push(("client_secret", secret));
        }
    }
    request(site, req.body(Body::from(encode(&params))).map_err(err)?).await
}

/// Exercises new credentials from registration through denial of access on disablement.
async fn round_trip(site: &Site, peer: &Site, method: &str) -> Result<(), String> {
    let input = json!({"redirect_uris": [REDIRECT], "token_endpoint_auth_method": method,
        "grant_types": ["authorization_code", "refresh_token"], "client_name": "Arbitrary name"});
    let (status, headers, registration) = register(site, input, None).await?;
    assert_eq!(status, StatusCode::CREATED, "{registration}");
    assert_eq!(headers[header::CACHE_CONTROL], "no-store");
    assert_eq!(registration["client_name"], "Approved integration");
    assert_eq!(registration["scope"], READ.as_str());
    let id = token(&registration, "client_id")?;
    let secret = registration.get("client_secret").and_then(Value::as_str);
    assert_eq!(secret.is_some(), method != "none");
    check_verifier(peer, &id, secret).await?;
    let code = issue(site, &id).await?;
    let code_fields = [
        ("grant_type", "authorization_code"),
        ("code", code.as_str()),
        ("code_verifier", VERIFIER),
        ("redirect_uri", REDIRECT),
        ("resource", MCP.as_str()),
    ];
    let wrong = if method == "client_secret_post" {
        "client_secret_basic"
    } else {
        "client_secret_post"
    };
    let wrong_secret = secret.or(Some("unexpected-credential"));
    let (status, _, _) =
        authenticate(peer, "/oauth/token", &code_fields, &id, wrong, wrong_secret).await?;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _, reply) =
        authenticate(peer, "/oauth/token", &code_fields, &id, method, secret).await?;
    assert_eq!(status, StatusCode::OK, "{reply}");
    let refresh = token(&reply, "refresh_token")?;
    let refresh_fields = [
        ("grant_type", "refresh_token"),
        ("refresh_token", refresh.as_str()),
        ("resource", MCP.as_str()),
    ];
    let (status, _, reply) =
        authenticate(site, "/oauth/token", &refresh_fields, &id, method, secret).await?;
    assert_eq!(status, StatusCode::OK, "{reply}");
    let access = token(&reply, "access_token")?;
    assert_eq!(mcp(peer, "/mcp", Some(&access)).await?.0, StatusCode::OK);
    assert_eq!(
        authenticate(
            peer,
            "/oauth/revoke",
            &[("token", &access)],
            &id,
            method,
            secret
        )
        .await?
        .0,
        StatusCode::OK
    );
    assert_eq!(
        mcp(site, "/mcp", Some(&access)).await?.0,
        StatusCode::UNAUTHORIZED
    );
    assert!(
        peer.oauth()
            .map_err(err)?
            .disable_registration(&id)
            .await
            .map_err(err)?
    );
    assert!(
        !site
            .oauth()
            .map_err(err)?
            .disable_registration(&id)
            .await
            .map_err(err)?
    );
    assert_eq!(
        authenticate(site, "/oauth/token", &refresh_fields, &id, method, secret)
            .await?
            .0,
        StatusCode::UNAUTHORIZED
    );
    Ok(())
}

/// Confirms a replacement site can read the registration and only a secret verifier is durable.
async fn check_verifier(site: &Site, id: &str, secret: Option<&str>) -> Result<(), String> {
    let id = super::super::registration::registration_id(id).ok_or("malformed server client ID")?;
    let (_, _, stored, _, _) = store::registration(site.db(), ISSUER, id)
        .await
        .map_err(err)?
        .ok_or("registration not visible across sites")?;
    assert_eq!(
        stored,
        secret.map(|secret| blake3::hash(secret.as_bytes()).to_hex().to_string())
    );
    Ok(())
}

/// Checks registration policy, default grant handling, and disablement of active bearer tokens.
async fn registration_policy(site: &Site, peer: &Site) -> Result<(), String> {
    for (input, error) in [
        (
            json!({"redirect_uris": ["https://wrong.example/callback"]}),
            "invalid_redirect_uri",
        ),
        (
            json!({"redirect_uris": [REDIRECT], "scope": WRITE.as_str()}),
            "invalid_client_metadata",
        ),
        (
            json!({"redirect_uris": [REDIRECT], "grant_types": ["client_credentials"]}),
            "invalid_client_metadata",
        ),
        (
            json!({"redirect_uris": [REDIRECT], "token_endpoint_auth_method": "private_key_jwt"}),
            "invalid_client_metadata",
        ),
    ] {
        let (status, _, body) = register(site, input, None).await?;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], error);
    }
    let input = json!({"redirect_uris": [REDIRECT], "token_endpoint_auth_method": "none"});
    let (status, _, registration) = register(site, input, None).await?;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(registration["grant_types"], json!(["authorization_code"]));
    let id = token(&registration, "client_id")?;
    let code = issue(site, &id).await?;
    let (status, _, reply) = send(
        site,
        "/oauth/token",
        &[
            ("grant_type", "authorization_code"),
            ("code", &code),
            ("client_id", &id),
            ("code_verifier", VERIFIER),
            ("redirect_uri", REDIRECT),
            ("resource", MCP.as_str()),
        ],
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert!(reply.get("refresh_token").is_none());
    let access = token(&reply, "access_token")?;
    assert_eq!(mcp(peer, "/mcp", Some(&access)).await?.0, StatusCode::OK);
    assert!(
        peer.oauth()
            .map_err(err)?
            .disable_registration(&id)
            .await
            .map_err(err)?
    );
    assert_eq!(
        mcp(site, "/mcp", Some(&access)).await?.0,
        StatusCode::UNAUTHORIZED
    );
    Ok(())
}

/// Exercises durable registration, protected admission, and an additive upgrade with a legacy grant.
#[tokio::test]
#[cfg_attr(
    not(feature = "sqlite"),
    ignore = "requires a fresh dbharness database via VYUH_OAUTH_TEST_URL"
)]
async fn dcr_upgrade_and_round_trips() -> Result<(), String> {
    #[cfg(feature = "sqlite")]
    let dir = tempfile::tempdir().map_err(err)?;
    #[cfg(feature = "sqlite")]
    let url = format!("sqlite://{}/oauth.db?mode=rwc", dir.path().display());
    #[cfg(not(feature = "sqlite"))]
    let url = std::env::var("VYUH_OAUTH_TEST_URL").map_err(err)?;
    let database = db::DbConf::from_url(&url).map_err(err)?;
    upgrade(&database).await?;
    let config = issuer().dynamic_registration(dcr(OAuthDcrAdmission::Unauthenticated));
    let site = Site::build(conf_with(database.clone(), config.clone()), bundle())
        .await
        .map_err(err)?;
    let peer = Site::build(conf_with(database.clone(), config), bundle())
        .await
        .map_err(err)?;
    let metadata = discover(&site).await?;
    assert_eq!(
        metadata["registration_endpoint"],
        "https://app.example.com/oauth/register"
    );
    assert!(
        metadata
            .get("client_id_metadata_document_supported")
            .is_none()
    );
    assert_eq!(
        mcp(&site, "/mcp", Some(LEGACY_ACCESS)).await?.0,
        StatusCode::OK
    );
    legacy_continuation(&site, &peer).await?;
    for method in ["none", "client_secret_basic", "client_secret_post"] {
        round_trip(&site, &peer, method).await?;
    }
    registration_policy(&site, &peer).await?;
    last_slot(&site, &peer).await?;
    protected_admission(database).await
}

/// An in-flight static consent sealed by the old binary can still be approved.
async fn legacy_continuation(site: &Site, peer: &Site) -> Result<(), String> {
    let (cookie, csrf) = session(site).await?;
    let continuation = consent(site, &cookie).await?;
    let codec = &site.oauth().map_err(err)?.issuer.codec;
    let mut pending: Value = codec.open(&continuation).map_err(err)?;
    let authorization = pending
        .get_mut("authorization")
        .and_then(Value::as_object_mut)
        .ok_or("missing authorization in continuation")?;
    assert!(authorization.remove("client_binding").is_some());
    let legacy = codec.seal(&pending).map_err(err)?;
    let post = parts(
        "POST",
        "/consent",
        &[("cookie", &cookie), ("x-csrf-token", &csrf)],
    )?;
    let response = peer
        .oauth()
        .map_err(err)?
        .approve(&post, &legacy, &[READ])
        .await
        .map_err(err)?;
    let code = param(&location(response)?, "code")?;
    assert_eq!(
        send(peer, "/oauth/token", &code_params(&code, VERIFIER))
            .await?
            .0,
        StatusCode::OK
    );
    Ok(())
}

/// Concurrent callers cannot exceed the durable cap, and a disabled slot is reusable.
async fn last_slot(site: &Site, peer: &Site) -> Result<(), String> {
    let redirects = json!([REDIRECT]).to_string();
    let registrations: Vec<_> = (0..store::REGISTRATION_CAP - 1)
        .map(|slot| store::RegistrationRow {
            id: Uuid::new_v4(),
            issuer: ISSUER.into(),
            slot,
            redirects: redirects.clone(),
            auth_method: "none".into(),
            secret_digest: None,
            allow_refresh: false,
            scopes: READ.as_str().into(),
            issued_at: now(),
        })
        .collect();
    let table = <store::RegistrationRow as db::Model>::table();
    let mut pool = site.db();
    assert_eq!(
        db::from(&table)
            .insert_many(&registrations)
            .exec(&mut pool)
            .await
            .map_err(err)?,
        registrations.len() as u64
    );
    let input = json!({"redirect_uris": [REDIRECT], "token_endpoint_auth_method": "none"});
    let (first, second) = tokio::join!(
        register(site, input.clone(), None),
        register(peer, input.clone(), None)
    );
    let (first, second) = (first?, second?);
    let winner = match (first.0, second.0) {
        (StatusCode::CREATED, StatusCode::SERVICE_UNAVAILABLE) => first.2,
        (StatusCode::SERVICE_UNAVAILABLE, StatusCode::CREATED) => second.2,
        statuses => {
            return Err(format!(
                "expected exactly one registration, got {statuses:?}"
            ));
        }
    };
    assert!(
        store::free_slot(peer.db(), ISSUER)
            .await
            .map_err(err)?
            .is_none()
    );
    let id = token(&winner, "client_id")?;
    assert!(
        peer.oauth()
            .map_err(err)?
            .disable_registration(&id)
            .await
            .map_err(err)?
    );
    let (status, _, replacement) = register(site, input, None).await?;
    assert_eq!(status, StatusCode::CREATED);
    let replacement = token(&replacement, "client_id")?;
    assert_ne!(id, replacement);
    assert!(
        site.oauth()
            .map_err(err)?
            .disable_registration(&replacement)
            .await
            .map_err(err)?
    );
    Ok(())
}

/// Verifies that protected DCR requires its own configured Bearer credential.
async fn protected_admission(database: db::DbConf) -> Result<(), String> {
    let config = issuer_base().dynamic_registration(dcr(OAuthDcrAdmission::InitialAccessToken(
        KeySource::inline(INITIAL),
    )));
    let site = Site::build(conf_with(database, config), bundle())
        .await
        .map_err(err)?;
    let input = json!({"redirect_uris": [REDIRECT]});
    for auth in [None, Some("incorrect-registration-admission-token")] {
        let (status, headers, reply) = register(&site, input.clone(), auth).await?;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(reply["error"], "invalid_token");
        assert!(
            headers[header::WWW_AUTHENTICATE]
                .to_str()
                .map_err(err)?
                .starts_with("Bearer ")
        );
    }
    let (status, _, body) = register(&site, input, Some(INITIAL)).await?;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["token_endpoint_auth_method"], "client_secret_basic");
    Ok(())
}
