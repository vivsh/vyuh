use std::future::Future;

use jsonwebtoken::{Algorithm, EncodingKey, Header, encode as sign};
use serde_json::json;

use super::{
    registration::{authenticate, dcr, discover, issue},
    *,
};
use crate::auth::oauth_issue::{
    assertion, cimd::tests::documents, protocol::LEGACY_CHATGPT_CIMD, runtime::now,
};
use crate::auth::{KeySource, OAuthCimdAuth, OAuthCimdClient, OAuthDcrAdmission};

const ISSUER: &str = "https://app.example.com";
pub(in crate::auth::oauth_issue) const SIGNED: &str = "https://client.example.com/signed.json";
const PUBLIC: &str = "https://client.example.com/public.json";
const KEYS: &str = "https://client.example.com/jwks.json";
const CHATGPT_KEYS: &str = "https://chatgpt.com/oauth/jwks.json";
const CONFIDENTIAL: &str = "static-secret-with-at-least-32-bytes";

pub(in crate::auth::oauth_issue) struct SigningKey {
    kid: &'static str,
    n: &'static str,
    pem: &'static [u8],
}

pub(in crate::auth::oauth_issue) const OLD: SigningKey = SigningKey {
    kid: "old",
    n: "sO784VwDxnB4rJNDfnfdtxKeHCBg218T6_AHI0oTm1LOS8wY1rAT7epEubGBUIZfS98e7dlRTyCmJg6wiqzzf7ctKjhoJAZNtqzuli_ieM6Xm4P4AcUmdKPzMJF6Q4Em6NDLogZeigjw4GEzocqXEFKJv8q0xQsZzkMcKmxB3LnD4pu9isWnKOpxoicRUIZz-Y79nRdComMrAcfcScjCdCRedN3BN90dH7mBTQsqSvSao0JdG1ZS1DYpPNGpnwWqC7lzm9waPpz40PldYhlOIivwDGtIlOgyAdujlDEYFW4xqVaMFClgSemL8OnS2U_M53NExhobmQIFaDejtrFAwQ",
    pem: include_bytes!("../../../../tests/fixtures/jwt-old-private.pem"),
};

pub(super) const NEW: SigningKey = SigningKey {
    kid: "new",
    n: "moSalpiGhTgZRuw1f6q8BHf3tQWlOaw7pdGeUN1nexEbsqCcJxBAHCvFazIDgG8UUwsm1-Mo9YyUQOvpzX72PVkJZyQceELB2SgAcNzd9Rlqm_V7owKXrpIYMZ-l2IHqhqNa5aHzZG2kXpO7YkvIj7SnEQ54IVoGEOXqSKrvjihM0atJxTBWaxmoxxjUhYLkaq8faFnNMOHzJyYp0DRFGL9So31HWEdReE3mB2gE9F_NaI5Nxu_X7r-ORHuQ_CZz88jnjkMh3Az3dzHlPNDope81LWGz_-Sts8RxqBDDfVLpydE_rBqJ7H1bXbqXkUSMs1_NRftLIwFje7dVUmchxw",
    pem: include_bytes!("../../../../tests/fixtures/jwt-new-private.pem"),
};

pub(in crate::auth::oauth_issue) fn rule(id: &str, auth: OAuthCimdAuth) -> OAuthCimdClient {
    OAuthCimdClient::new(id, "Approved metadata client", auth).redirect_uri(REDIRECT)
}

fn metadata(id: &str) -> Value {
    let mut doc = json!({
        "client_id": id, "redirect_uris": [REDIRECT], "scope": READ.as_str(),
        "grant_types": ["authorization_code", "refresh_token", "urn:custom:grant"],
        "response_types": ["code"],
    });
    if matches!(id, SIGNED | LEGACY_CHATGPT_CIMD) {
        doc["token_endpoint_auth_method"] = json!("private_key_jwt");
        doc["token_endpoint_auth_methods_supported"] = json!(["none", "private_key_jwt"]);
        doc["token_endpoint_auth_signing_alg"] = json!("RS256");
        doc["jwks_uri"] = json!(if id == SIGNED { KEYS } else { CHATGPT_KEYS });
    }
    doc
}

pub(in crate::auth::oauth_issue) async fn fixture<T>(
    key: &SigningKey,
    work: impl Future<Output = T>,
) -> T {
    let jwk = json!({"alg": "RS256", "kty": "RSA", "kid": key.kid, "n": key.n,
        "e": "AQAB", "use": "sig"});
    documents(
        [
            (SIGNED, Ok(metadata(SIGNED))),
            (LEGACY_CHATGPT_CIMD, Ok(metadata(LEGACY_CHATGPT_CIMD))),
            (PUBLIC, Ok(metadata(PUBLIC))),
            (CHATGPT_KEYS, Ok(json!({"keys": [jwk.clone()]}))),
            (KEYS, Ok(json!({"keys": [jwk]}))),
        ],
        work,
    )
    .await
}

pub(super) fn claims(jti: &str) -> Value {
    let at = now();
    json!({"iss": SIGNED, "sub": SIGNED, "aud": ISSUER,
        "exp": at + 180, "iat": at, "nbf": at, "jti": jti})
}

pub(super) fn signed(key: &SigningKey, claims: &Value) -> Result<String, String> {
    let mut header = Header::new(Algorithm::RS256);
    header.typ = Some("client-authentication+jwt".into());
    header.kid = Some(key.kid.into());
    let key = EncodingKey::from_rsa_pem(key.pem).map_err(err)?;
    sign(&header, claims, &key).map_err(err)
}

pub(super) async fn send_signed(
    site: &Site,
    path: &str,
    fields: &[(&str, &str)],
    raw: &str,
) -> Result<(StatusCode, HeaderMap, Value), String> {
    let mut params = fields.to_vec();
    params.extend([
        ("client_assertion_type", assertion::TYPE),
        ("client_assertion", raw),
    ]);
    send(site, path, &params).await
}

/// Exercises actual token authentication with a dual-method document and a cross-replica replay.
async fn signed_code(site: &Site, peer: &Site) -> Result<String, String> {
    let code = issue(site, SIGNED).await?;
    let fields = [
        ("grant_type", "authorization_code"),
        ("code", &code),
        ("code_verifier", VERIFIER),
        ("redirect_uri", REDIRECT),
        ("resource", MCP.as_str()),
    ];
    let mut without_assertion = fields.to_vec();
    without_assertion.push(("client_id", SIGNED));
    assert_eq!(
        send(site, "/oauth/token", &without_assertion).await?.0,
        StatusCode::UNAUTHORIZED
    );
    rejects_claims(site, &fields).await?;
    let raw = signed(&OLD, &claims("one-code-jti"))?;
    let mut wrong_id = fields.to_vec();
    wrong_id.push(("client_id", PUBLIC));
    assert_eq!(
        send_signed(site, "/oauth/token", &wrong_id, &raw).await?.0,
        StatusCode::UNAUTHORIZED
    );
    let (first, second) = tokio::join!(
        send_signed(site, "/oauth/token", &fields, &raw),
        send_signed(peer, "/oauth/token", &fields, &raw)
    );
    let (first, second) = (first?, second?);
    let reply = match (first.0, second.0) {
        (StatusCode::OK, StatusCode::UNAUTHORIZED) => first.2,
        (StatusCode::UNAUTHORIZED, StatusCode::OK) => second.2,
        other => return Err(format!("unexpected duplicate assertion result: {other:?}")),
    };
    assert_eq!(
        mcp(peer, "/mcp", Some(&token(&reply, "access_token")?))
            .await?
            .0,
        StatusCode::OK
    );
    token(&reply, "refresh_token")
}

/// Valid signatures with the wrong type of claims still fail before exchange.
async fn rejects_claims(site: &Site, fields: &[(&str, &str)]) -> Result<(), String> {
    let wrong_key = SigningKey {
        kid: OLD.kid,
        ..NEW
    };
    let invalid_signature = signed(&wrong_key, &claims("invalid-signature"))?;
    assert_eq!(
        send_signed(site, "/oauth/token", fields, &invalid_signature)
            .await?
            .0,
        StatusCode::UNAUTHORIZED
    );
    for (field, denied) in [
        ("aud", json!(format!("{ISSUER}/oauth/token"))),
        ("aud", json!([ISSUER, "https://other.example.com"])),
        ("sub", json!(PUBLIC)),
        ("exp", json!(now() - 100)),
        ("exp", json!(now() + 600)),
        ("iat", json!(now() + 120)),
        ("nbf", json!(now() + 120)),
        ("jti", json!("")),
    ] {
        let mut input = claims("invalid-claim");
        input[field] = denied;
        let raw = signed(&OLD, &input)?;
        assert_eq!(
            send_signed(site, "/oauth/token", fields, &raw).await?.0,
            StatusCode::UNAUTHORIZED,
            "{field}"
        );
    }
    Ok(())
}

/// A removed key fails immediately and a new key can refresh without another consent.
pub(super) async fn rotate_and_revoke(
    site: &Site,
    peer: &Site,
    refresh: &str,
) -> Result<(), String> {
    let fields = [
        ("grant_type", "refresh_token"),
        ("refresh_token", refresh),
        ("resource", MCP.as_str()),
    ];
    let old = signed(&OLD, &claims("removed-key"))?;
    assert_eq!(
        send_signed(site, "/oauth/token", &fields, &old).await?.0,
        StatusCode::UNAUTHORIZED
    );
    let reused = signed(&NEW, &claims("one-code-jti"))?;
    assert_eq!(
        send_signed(site, "/oauth/token", &fields, &reused).await?.0,
        StatusCode::UNAUTHORIZED
    );
    let fresh = signed(&NEW, &claims("after-rotation"))?;
    let (status, _, reply) = send_signed(peer, "/oauth/token", &fields, &fresh).await?;
    assert_eq!(status, StatusCode::OK);
    let access = token(&reply, "access_token")?;
    assert_eq!(mcp(site, "/mcp", Some(&access)).await?.0, StatusCode::OK);
    let revoke = signed(&NEW, &claims("revoke"))?;
    assert_eq!(
        send_signed(site, "/oauth/revoke", &[("token", &access)], &revoke)
            .await?
            .0,
        StatusCode::OK
    );
    assert_eq!(
        mcp(peer, "/mcp", Some(&access)).await?.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        send_signed(peer, "/oauth/revoke", &[("token", &access)], &revoke)
            .await?
            .0,
        StatusCode::UNAUTHORIZED
    );
    Ok(())
}

/// Exercises public metadata clients and proves the resolver keeps static authentication compatible.
async fn public_round_trip(
    site: &Site,
    peer: &Site,
    id: &str,
    post_secret: Option<&str>,
) -> Result<String, String> {
    let code = issue(site, id).await?;
    let fields = [
        ("grant_type", "authorization_code"),
        ("code", &code),
        ("code_verifier", VERIFIER),
        ("redirect_uri", REDIRECT),
        ("resource", MCP.as_str()),
    ];
    let method = if post_secret.is_some() {
        "client_secret_post"
    } else {
        "none"
    };
    let (status, _, reply) =
        authenticate(peer, "/oauth/token", &fields, id, method, post_secret).await?;
    assert_eq!(status, StatusCode::OK);
    let refresh = token(&reply, "refresh_token")?;
    let fields = [
        ("grant_type", "refresh_token"),
        ("refresh_token", &refresh),
        ("resource", MCP.as_str()),
    ];
    let rotate = if post_secret.is_some() {
        "client_secret_basic"
    } else {
        "none"
    };
    let (status, _, reply) =
        authenticate(site, "/oauth/token", &fields, id, rotate, post_secret).await?;
    assert_eq!(status, StatusCode::OK);
    let access = token(&reply, "access_token")?;
    assert_eq!(mcp(peer, "/mcp", Some(&access)).await?.0, StatusCode::OK);
    Ok(access)
}

/// Rejects new metadata client errors without an authorization redirect to an unverified URI.
async fn failures(site: &Site) -> Result<(), String> {
    let oauth = site.oauth().map_err(err)?;
    for (id, redirect, error) in [
        (
            "https://not-admitted.example/client.json",
            REDIRECT,
            OAuthIssueError::InvalidClient,
        ),
        (
            SIGNED,
            "https://other.example/callback",
            OAuthIssueError::InvalidRequest,
        ),
    ] {
        let path = format!("/consent?{}", client_query(id, redirect, READ.as_str()));
        assert!(
            matches!(oauth.request(&parts("GET", &path, &[])?).await, Err(actual)
            if actual.code() == error.code())
        );
    }
    let request = parts(
        "GET",
        &format!(
            "/consent?{}",
            client_query(SIGNED, REDIRECT, WRITE.as_str())
        ),
        &[],
    )?;
    let result = oauth.request(&request).await.map_err(err)?;
    let OAuthConsent::Redirect(response) = result else {
        return Err("expected scope error redirect".into());
    };
    let callback = location(response)?;
    assert_eq!(param(&callback, "error")?, "invalid_scope");
    assert_eq!(param(&callback, "iss")?, ISSUER);
    let request = parts(
        "GET",
        &format!("/consent?{}", client_query(SIGNED, REDIRECT, READ.as_str())),
        &[],
    )?;
    assert!(matches!(
        documents(
            [(SIGNED, Err(OAuthIssueError::Unavailable))],
            oauth.request(&request)
        )
        .await,
        Err(OAuthIssueError::Unavailable)
    ));
    Ok(())
}

/// Exercises neither, CIMD-only, and combined discovery, plus live grant binding across config changes.
#[tokio::test]
#[cfg_attr(
    not(feature = "sqlite"),
    ignore = "requires a fresh dbharness database via VYUH_OAUTH_TEST_URL"
)]
async fn cimd_public_and_signed() -> Result<(), String> {
    #[cfg(feature = "sqlite")]
    let dir = tempfile::tempdir().map_err(err)?;
    #[cfg(feature = "sqlite")]
    let url = format!("sqlite://{}/oauth.db?mode=rwc", dir.path().display());
    #[cfg(not(feature = "sqlite"))]
    let url = std::env::var("VYUH_OAUTH_TEST_URL").map_err(err)?;
    let database = db::DbConf::from_url(&url).map_err(err)?;
    install(database.clone()).await?;
    let static_only = issuer().client(
        OAuthClient::new("confidential", "Static confidential")
            .redirect_uri(REDIRECT)
            .secret(KeySource::inline(CONFIDENTIAL)),
    );
    let previous = Site::build(conf_with(database.clone(), static_only.clone()), bundle())
        .await
        .map_err(err)?;
    let docs = discover(&previous).await?;
    assert!(docs.get("registration_endpoint").is_none());
    assert!(docs.get("client_id_metadata_document_supported").is_none());
    let legacy = public_round_trip(&previous, &previous, "client", None).await?;
    let config = static_only
        .cimd_client(rule(SIGNED, OAuthCimdAuth::PrivateKeyJwt))
        .cimd_client(rule(PUBLIC, OAuthCimdAuth::Public));
    let site = Site::build(conf_with(database.clone(), config.clone()), bundle())
        .await
        .map_err(err)?;
    let both = config.dynamic_registration(dcr(OAuthDcrAdmission::Unauthenticated));
    let peer = Site::build(conf_with(database.clone(), both), bundle())
        .await
        .map_err(err)?;
    assert!(
        discover(&site)
            .await?
            .get("registration_endpoint")
            .is_none()
    );
    assert_eq!(
        discover(&site).await?["client_id_metadata_document_supported"],
        true
    );
    let both_docs = discover(&peer).await?;
    assert_eq!(
        both_docs["registration_endpoint"],
        format!("{ISSUER}/oauth/register")
    );
    assert_eq!(both_docs["client_id_metadata_document_supported"], true);
    assert_eq!(
        both_docs["token_endpoint_auth_methods_supported"],
        json!([
            "none",
            "client_secret_basic",
            "client_secret_post",
            "private_key_jwt"
        ])
    );
    assert_eq!(mcp(&peer, "/mcp", Some(&legacy)).await?.0, StatusCode::OK);
    check_public_and_static(&site, &peer).await?;
    fixture(&OLD, failures(&site)).await?;
    let refresh = fixture(&OLD, signed_code(&site, &peer)).await?;
    reject_downgrade(database, &refresh).await?;
    fixture(&NEW, rotate_and_revoke(&site, &peer, &refresh)).await?;
    prune_replay_claims(&site).await
}

/// Bounded cleanup deletes expired replay entries while retaining used, live assertions.
async fn prune_replay_claims(site: &Site) -> Result<(), String> {
    use crate::auth::oauth_issue::store::{self, AssertionRow};
    for digest in ["expired-1", "expired-2"] {
        assert!(
            store::claim_assertion(
                site.db(),
                &AssertionRow {
                    digest: digest.into(),
                    issuer: ISSUER.into(),
                    expires_at: now() - 1,
                }
            )
            .await
            .map_err(err)?
        );
    }
    let oauth = site.oauth().map_err(err)?;
    assert_eq!(oauth.prune_assertions(1).await.map_err(err)?, 1);
    assert_eq!(oauth.prune_assertions(10).await.map_err(err)?, 1);
    assert_eq!(oauth.prune_assertions(10).await.map_err(err)?, 0);
    Ok(())
}

/// Tests revocation by public metadata and static confidential clients alongside enabled DCR.
async fn check_public_and_static(site: &Site, peer: &Site) -> Result<(), String> {
    let access = fixture(&OLD, public_round_trip(site, peer, PUBLIC, None)).await?;
    assert_eq!(
        fixture(
            &OLD,
            send(
                peer,
                "/oauth/revoke",
                &[("token", &access), ("client_id", PUBLIC)]
            )
        )
        .await?
        .0,
        StatusCode::OK
    );
    assert_eq!(
        mcp(site, "/mcp", Some(&access)).await?.0,
        StatusCode::UNAUTHORIZED
    );
    let access = public_round_trip(site, peer, "confidential", Some(CONFIDENTIAL)).await?;
    assert_eq!(
        authenticate(
            peer,
            "/oauth/revoke",
            &[("token", &access)],
            "confidential",
            "client_secret_post",
            Some(CONFIDENTIAL)
        )
        .await?
        .0,
        StatusCode::OK
    );
    assert_eq!(
        mcp(site, "/mcp", Some(&access)).await?.0,
        StatusCode::UNAUTHORIZED
    );
    Ok(())
}

/// Even an explicit operator change to public mode cannot redeem a grant issued for a signed client.
async fn reject_downgrade(database: db::DbConf, refresh: &str) -> Result<(), String> {
    let config = issuer_base().cimd_client(rule(SIGNED, OAuthCimdAuth::Public));
    let site = Site::build(conf_with(database, config), bundle())
        .await
        .map_err(err)?;
    assert_eq!(
        discover(&site).await?["token_endpoint_auth_methods_supported"],
        json!(["none"])
    );
    let fields = [
        ("grant_type", "refresh_token"),
        ("refresh_token", refresh),
        ("client_id", SIGNED),
        ("resource", MCP.as_str()),
    ];
    let (status, _, reply) = fixture(&OLD, send(&site, "/oauth/token", &fields)).await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(reply["error"], "invalid_grant");
    Ok(())
}
