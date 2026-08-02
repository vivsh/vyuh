use super::*;

/// Verifies default login returns access and refresh credentials from one provider.
#[tokio::test]
async fn default_login_returns_pair() -> Result<(), AuthError> {
    let site = Site::build(config(), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let login = site
        .auth()
        .login(AuthUser::new("user-1"), &[REPORTS])
        .await?;
    assert!(!login.credentials().access().is_empty());
    assert!(login.credentials().refresh().is_some());
    Ok(())
}

/// Verifies the default response body exposes both body-delivered credentials.
#[tokio::test]
async fn default_login_body_contains_token_pair() -> Result<(), AuthError> {
    let site = Site::build(config(), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let login = site
        .auth()
        .login(AuthUser::new("user-1"), &[REPORTS])
        .await?;
    let response = login.into_response();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .map_err(auth_error)?;
    let json = serde_json::from_slice::<serde_json::Value>(&body).map_err(auth_error)?;
    assert!(json["access_token"].is_string());
    assert!(json["refresh_token"].is_string());
    assert_eq!(json["token_type"], "Bearer");
    assert_eq!(json["expires_in"], 3600);
    Ok(())
}

/// Verifies access credentials resolve through the single AuthUser extractor.
#[tokio::test]
async fn access_authenticates_user() -> Result<(), AuthError> {
    let site = Site::build(config(), bundle()).await.map_err(auth_error)?;
    let login = site
        .auth()
        .login(AuthUser::new("user-1"), &[REPORTS])
        .await?;
    let header = format!("Bearer {}", login.credentials().access());
    let response = TestSite::new(site)
        .get("/me")
        .header("authorization", &header)
        .send()
        .await;
    response.assert_status(vyuh::routes::StatusCode::OK);
    Ok(())
}

/// Verifies refresh rotates both credentials and keeps its audiences.
#[tokio::test]
async fn refresh_rotates_pair() -> Result<(), AuthError> {
    let site = Site::build(config(), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let login = site
        .auth()
        .login(AuthUser::new("user-1"), &[REPORTS])
        .await?;
    let refresh = login
        .credentials()
        .refresh()
        .ok_or(AuthError::UnsupportedProviderCapability)?;
    let parts = bearer_parts(refresh)?;
    let rotated = site.auth().refresh(&parts, &[REPORTS]).await?;
    assert_ne!(rotated.credentials().access(), login.credentials().access());
    assert_ne!(
        rotated.credentials().refresh(),
        login.credentials().refresh()
    );
    Ok(())
}

/// Verifies an access credential cannot be used through the refresh helper.
#[tokio::test]
async fn refresh_rejects_access() -> Result<(), AuthError> {
    let site = Site::build(config(), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let login = site
        .auth()
        .login(AuthUser::new("user-1"), &[REPORTS])
        .await?;
    let parts = bearer_parts(login.credentials().access())?;
    let error = site
        .auth()
        .refresh(&parts, &[REPORTS])
        .await
        .err()
        .ok_or_else(|| AuthError::Internal("access refresh unexpectedly succeeded".into()))?;
    assert!(matches!(error, AuthError::WrongTokenKind));
    Ok(())
}

/// Verifies refresh may narrow but cannot add audiences.
#[tokio::test]
async fn refresh_rejects_escalation() -> Result<(), AuthError> {
    let site = Site::build(config(), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let login = site
        .auth()
        .login(AuthUser::new("user-1"), &[REPORTS])
        .await?;
    let refresh = login
        .credentials()
        .refresh()
        .ok_or(AuthError::UnsupportedProviderCapability)?;
    let parts = bearer_parts(refresh)?;
    let error = site
        .auth()
        .refresh(&parts, &[ADMIN])
        .await
        .err()
        .ok_or_else(|| AuthError::Internal("audience escalation unexpectedly succeeded".into()))?;
    assert!(matches!(error, AuthError::AudienceMismatch));
    Ok(())
}

/// Verifies a selected provider owns login and refresh as one unit.
#[tokio::test]
async fn selected_provider_is_complete() -> Result<(), AuthError> {
    let provider = TokenProvider::new(Jwt::hs256_site_secret())
        .access(TokenConf::header("x-access"))
        .refresh(TokenConf::header("x-refresh"));
    let conf = config().auth(AuthConf::empty().provider(ALTERNATE, provider));
    let site = Site::build(conf, bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let login = site
        .auth()
        .using(ALTERNATE)
        .login(AuthUser::new("user-1"), &[REPORTS])
        .await?;
    assert!(login.credentials().refresh().is_some());
    Ok(())
}

/// Verifies shared signing material cannot move a token between provider identities.
#[tokio::test]
async fn locally_issued_tokens_are_provider_bound() -> Result<(), AuthError> {
    let first = TokenProvider::new(Jwt::hs256_site_secret())
        .without_refresh()
        .access(TokenConf::header("x-auth-a"));
    let second = TokenProvider::new(Jwt::hs256_site_secret())
        .without_refresh()
        .access(TokenConf::header("x-auth-b"));
    let auth = AuthConf::empty()
        .provider(SHARED_A, first)
        .provider(SHARED_B, second);
    let site = Site::build(config().auth(auth), bundle())
        .await
        .map_err(auth_error)?;
    let login = site
        .auth()
        .using(SHARED_A)
        .login(AuthUser::new("user-1"), &[REPORTS])
        .await?;
    TestSite::new(site)
        .get("/me")
        .header("x-auth-b", login.credentials().access())
        .send()
        .await
        .assert_status(vyuh::routes::StatusCode::UNAUTHORIZED);
    Ok(())
}

/// Verifies a malformed presented credential prevents provider fallthrough.
#[tokio::test]
async fn malformed_credential_short_circuits_other_providers() -> Result<(), AuthError> {
    let first = TokenProvider::new(Jwt::hs256_site_secret())
        .without_refresh()
        .access(TokenConf::header("x-auth-a"));
    let second = TokenProvider::new(Jwt::hs256_site_secret())
        .without_refresh()
        .access(TokenConf::header("x-auth-b"));
    let auth = AuthConf::empty()
        .provider(SHARED_A, first)
        .provider(SHARED_B, second);
    let site = Site::build(config().auth(auth), bundle())
        .await
        .map_err(auth_error)?;
    let valid = site
        .auth()
        .using(SHARED_B)
        .login(AuthUser::new("user-1"), &[REPORTS])
        .await?;
    TestSite::new(site)
        .get("/me")
        .header("x-auth-a", "malformed")
        .header("x-auth-b", valid.credentials().access())
        .send()
        .await
        .assert_status(vyuh::routes::StatusCode::UNAUTHORIZED);
    Ok(())
}

/// Verifies per-kind size limits reject encoded input before codec parsing.
#[tokio::test]
async fn credential_size_limit_is_enforced() -> Result<(), AuthError> {
    let provider = TokenProvider::new(Jwt::hs256_site_secret())
        .without_refresh()
        .access(TokenConf::bearer().max_credential_bytes(16));
    let conf = config().auth(AuthConf::empty().provider(ALTERNATE, provider));
    let site = Site::build(conf, bundle()).await.map_err(auth_error)?;
    TestSite::new(site)
        .get("/me")
        .header(
            "authorization",
            "Bearer this-credential-is-longer-than-sixteen-bytes",
        )
        .send()
        .await
        .assert_status(vyuh::routes::StatusCode::UNAUTHORIZED);
    Ok(())
}

/// Verifies one provider may encode access and refresh with different formats.
#[tokio::test]
async fn mixed_access_and_refresh_codecs_rotate() -> Result<(), AuthError> {
    let provider = TokenProvider::new(Jwt::hs256_site_secret()).refresh(
        TokenConf::bearer()
            .ttl(chrono::Duration::days(7))
            .codec(DjangoSigning::site_secret()),
    );
    let conf = config().auth(AuthConf::empty().provider(MIXED, provider));
    let site = Site::build(conf, bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let login = site
        .auth()
        .using(MIXED)
        .login(AuthUser::new("user-1"), &[REPORTS])
        .await?;
    assert_eq!(login.credentials().access().split('.').count(), 3);
    let refresh = login
        .credentials()
        .refresh()
        .ok_or(AuthError::UnsupportedProviderCapability)?;
    assert_eq!(refresh.split(':').count(), 3);
    let rotated = site
        .auth()
        .using(MIXED)
        .refresh(&bearer_parts(refresh)?, &[REPORTS])
        .await?;
    assert!(rotated.credentials().refresh().is_some());
    Ok(())
}

/// Verifies a Python-produced Django signing fixture authenticates in Vyuh.
#[tokio::test]
async fn django_cross_language_fixture_authenticates() -> Result<(), AuthError> {
    let fixture =
        serde_json::from_str::<DjangoFixture>(include_str!("../fixtures/django_signing.json"))
            .map_err(auth_error)?;
    let provider = TokenProvider::new(DjangoSigning::site_secret()).without_refresh();
    let conf = config()
        .secret_key(fixture.secret)
        .auth(AuthConf::empty().provider(DJANGO, provider));
    let site = Site::build(conf, bundle()).await.map_err(auth_error)?;
    let response = TestSite::new(site)
        .get("/me")
        .header("authorization", &format!("Bearer {}", fixture.django_token))
        .send()
        .await;
    response
        .assert_json(
            vyuh::routes::StatusCode::OK,
            &serde_json::json!({ "key": "django-user", "provider": "django" }),
        )
        .await;
    Ok(())
}

/// Verifies a Django-produced token without `aud` maps to the configured default.
#[tokio::test]
async fn django_legacy_fixture_uses_default_audience() -> Result<(), AuthError> {
    let fixture =
        serde_json::from_str::<DjangoFixture>(include_str!("../fixtures/django_signing.json"))
            .map_err(auth_error)?;
    let provider = TokenProvider::new(DjangoSigning::site_secret()).without_refresh();
    let auth = AuthConf::empty()
        .default_audience(REPORTS)
        .provider(DJANGO, provider);
    let conf = config().secret_key(fixture.secret).auth(auth);
    let site = Site::build(conf, bundle_without_audience())
        .await
        .map_err(auth_error)?;
    TestSite::new(site)
        .get("/me-default")
        .header("authorization", &format!("Bearer {}", fixture.legacy_token))
        .send()
        .await
        .assert_status(vyuh::routes::StatusCode::OK);
    Ok(())
}

/// Verifies Django-signed tokens are rejected after authenticated content changes.
#[tokio::test]
async fn django_signing_rejects_tampering() -> Result<(), AuthError> {
    let provider = TokenProvider::new(DjangoSigning::site_secret()).without_refresh();
    let conf = config().auth(AuthConf::empty().provider(DJANGO, provider));
    let site = Site::build(conf, bundle()).await.map_err(auth_error)?;
    let login = site
        .auth()
        .using(DJANGO)
        .login(AuthUser::new("user-1"), &[REPORTS])
        .await?;
    let mut token = login.credentials().access().to_owned();
    token.push('x');
    TestSite::new(site)
        .get("/me")
        .header("authorization", &format!("Bearer {token}"))
        .send()
        .await
        .assert_status(vyuh::routes::StatusCode::UNAUTHORIZED);
    Ok(())
}

/// Verifies BRANCA providers issue and authenticate normalized access tokens.
#[cfg(feature = "branca")]
#[tokio::test]
async fn branca_round_trip() -> Result<(), AuthError> {
    let provider = TokenProvider::new(vyuh::auth::Branca::site_secret());
    assert_provider_round_trip(BRANCA_PROVIDER, provider).await
}

/// Verifies PASETO v4.local providers issue and authenticate normalized tokens.
#[cfg(feature = "paseto")]
#[tokio::test]
async fn paseto_local_round_trip() -> Result<(), AuthError> {
    let provider = TokenProvider::new(vyuh::auth::Paseto::v4_local_site_secret());
    assert_provider_round_trip(PASETO_PROVIDER, provider).await
}

/// Verifies PASETO v4.public signing and footer key selection round-trip.
#[cfg(feature = "paseto")]
#[tokio::test]
async fn paseto_public_round_trip() -> Result<(), AuthError> {
    use pasetors::{
        keys::{AsymmetricKeyPair, Generate},
        paserk::FormatAsPaserk,
        version4::V4,
    };

    let pair = AsymmetricKeyPair::<V4>::generate().map_err(auth_error)?;
    let mut secret = String::new();
    pair.secret.fmt(&mut secret).map_err(auth_error)?;
    let mut public = String::new();
    pair.public.fmt(&mut public).map_err(auth_error)?;
    let codec = vyuh::auth::Paseto::v4_public(KeySource::inline(secret), KeySource::inline(public))
        .key_id("active");
    assert_provider_round_trip(PASETO_PROVIDER, TokenProvider::new(codec)).await
}

#[cfg(any(feature = "branca", feature = "paseto"))]
async fn assert_provider_round_trip(
    name: AuthProvider,
    provider: TokenProvider,
) -> Result<(), AuthError> {
    let conf = config().auth(AuthConf::empty().provider(name, provider));
    let site = Site::build(conf, bundle()).await.map_err(auth_error)?;
    let login = site
        .auth()
        .using(name)
        .login(AuthUser::new("user-1"), &[REPORTS])
        .await?;
    let client = TestSite::new(site);
    let response = client
        .get("/me")
        .header(
            "authorization",
            &format!("Bearer {}", login.credentials().access()),
        )
        .send()
        .await;
    response.assert_status(vyuh::routes::StatusCode::OK);
    let mut tampered = login.credentials().access().to_owned();
    let replacement = if tampered.ends_with('a') { 'b' } else { 'a' };
    tampered.pop();
    tampered.push(replacement);
    client
        .get("/me")
        .header("authorization", &format!("Bearer {tampered}"))
        .send()
        .await
        .assert_status(vyuh::routes::StatusCode::UNAUTHORIZED);
    Ok(())
}

/// Verifies lifecycle storage rejects reuse of a rotated refresh token.
#[tokio::test]
async fn lifecycle_rejects_replay() -> Result<(), AuthError> {
    let provider = TokenProvider::new(Jwt::hs256_site_secret()).lifecycle(ReplayStore::default());
    let conf = config().auth(AuthConf::empty().provider(ROTATING, provider));
    let site = Site::build(conf, bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let login = site
        .auth()
        .using(ROTATING)
        .login(AuthUser::new("user-1"), &[REPORTS])
        .await?;
    let refresh = login
        .credentials()
        .refresh()
        .ok_or(AuthError::UnsupportedProviderCapability)?;
    let parts = bearer_parts(refresh)?;
    site.auth()
        .using(ROTATING)
        .refresh(&parts, &[REPORTS])
        .await?;
    let error = site
        .auth()
        .using(ROTATING)
        .refresh(&parts, &[REPORTS])
        .await
        .err()
        .ok_or_else(|| AuthError::Internal("refresh replay unexpectedly succeeded".into()))?;
    assert!(matches!(error, AuthError::InvalidCredential));
    Ok(())
}

/// Verifies an opaque key provider resolves the normal AuthUser extractor.
#[tokio::test]
async fn opaque_key_authenticates_user() -> Result<(), AuthError> {
    let auth =
        AuthConf::default().provider(API_KEY, AuthKey::header("x-api-key", StaticKeyVerifier));
    let site = Site::build(config().auth(auth), bundle())
        .await
        .map_err(auth_error)?;
    let response = TestSite::new(site)
        .get("/me")
        .header("x-api-key", "service-secret")
        .send()
        .await;
    response
        .assert_json(
            vyuh::routes::StatusCode::OK,
            &serde_json::json!({ "key": "service-1", "provider": "api-key" }),
        )
        .await;
    Ok(())
}

/// Verifies opaque-key logout calls its optional server-side revocation lifecycle.
#[tokio::test]
async fn opaque_key_logout_revokes_when_configured() -> Result<(), AuthError> {
    let revoked = Arc::new(AtomicBool::new(false));
    let key =
        AuthKey::header("x-api-key", StaticKeyVerifier).lifecycle(KeyRevoker(revoked.clone()));
    let auth = AuthConf::default().provider(API_KEY, key);
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let request = axum::http::Request::builder()
        .header("x-api-key", "service-secret")
        .body(axum::body::Body::empty())
        .map_err(auth_error)?;
    let parts = request.into_parts().0;
    let mut response = axum::response::Response::new(axum::body::Body::empty());
    let logout = site.auth().using(API_KEY).logout(&parts).await?;
    logout.write(&mut response);
    assert!(revoked.load(Ordering::Relaxed));
    Ok(())
}

/// Verifies an external decoder authenticates while refusing login and refresh issuance.
#[tokio::test]
async fn verify_only_token_provider_is_not_an_issuer() -> Result<(), AuthError> {
    let provider = TokenProvider::verify_only(ExternalDecoder, "External");
    let conf = config().auth(AuthConf::empty().provider(EXTERNAL, provider));
    let site = Site::build(conf, bundle()).await.map_err(auth_error)?;
    let response = TestSite::new(site.clone())
        .get("/me")
        .header("authorization", "Bearer externally-verified")
        .send()
        .await;
    response.assert_status(vyuh::routes::StatusCode::OK);
    let login = site
        .auth()
        .using(EXTERNAL)
        .login(AuthUser::new("external-user"), &[REPORTS])
        .await;
    assert!(matches!(
        login,
        Err(AuthError::UnsupportedProviderCapability)
    ));
    let refresh = site
        .auth()
        .using(EXTERNAL)
        .refresh(&bearer_parts("externally-verified")?, &[REPORTS])
        .await;
    assert!(matches!(
        refresh,
        Err(AuthError::UnsupportedProviderCapability)
    ));
    Ok(())
}

/// Verifies cookie login hides both raw tokens while attaching cookies.
#[tokio::test]
async fn cookie_login_hides_tokens() -> Result<(), AuthError> {
    let auth = configured_token_auth(TokenConf::cookie("access"), TokenConf::cookie("refresh"));
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let login = site
        .auth()
        .login(AuthUser::new("user-1"), &[REPORTS])
        .await?;
    let mut response = axum::response::Response::new(axum::body::Body::empty());
    login.write(&mut response);
    assert_eq!(response.headers().get_all("set-cookie").iter().count(), 4);
    Ok(())
}

/// Verifies an all-cookie login uses a token-free success body.
#[tokio::test]
async fn cookie_login_default_body_is_ok() -> Result<(), AuthError> {
    let auth = configured_token_auth(TokenConf::cookie("access"), TokenConf::cookie("refresh"));
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let login = site
        .auth()
        .login(AuthUser::new("user-1"), &[REPORTS])
        .await?;
    let response = login.into_response();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .map_err(auth_error)?;
    let json = serde_json::from_slice::<serde_json::Value>(&body).map_err(auth_error)?;
    assert_eq!(json, serde_json::json!({ "ok": true }));
    Ok(())
}

/// Verifies response-header delivery works independently of the request source.
#[tokio::test]
async fn response_header_delivery_is_explicit() -> Result<(), AuthError> {
    let provider = TokenProvider::new(Jwt::hs256_site_secret())
        .without_refresh()
        .access(TokenConf::cookie("access").response_header("x-new-auth-token"));
    let auth = AuthConf::empty().provider(DEFAULT_AUTH_PROVIDER, provider);
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let login = site.auth().login(AuthUser::new("user-1"), &[]).await?;
    let mut response = axum::response::Response::new(axum::body::Body::empty());
    login.write(&mut response);
    assert!(response.headers().contains_key("x-new-auth-token"));
    assert_eq!(response.headers().get_all("set-cookie").iter().count(), 1);
    Ok(())
}

/// Verifies unsafe requests using a cookie credential require its double-submit token.
#[tokio::test]
async fn cookie_authentication_requires_csrf() -> Result<(), AuthError> {
    let auth = configured_token_auth(TokenConf::cookie("access"), TokenConf::cookie("refresh"));
    let site = Site::build(config().auth(auth), bundle())
        .await
        .map_err(auth_error)?;
    let login = site
        .auth()
        .login(AuthUser::new("user-1"), &[REPORTS])
        .await?;
    let mut response = axum::response::Response::new(axum::body::Body::empty());
    login.write(&mut response);
    let cookies = response_cookie_values(&response)?;
    let access = cookie_value(&cookies, "access")?;
    let csrf = cookie_value(&cookies, "access_csrf")?;
    let cookie_header = format!("access={access}; access_csrf={csrf}");
    let client = TestSite::new(site);
    client
        .post("/me")
        .header("cookie", &cookie_header)
        .send()
        .await
        .assert_status(vyuh::routes::StatusCode::FORBIDDEN);
    client
        .post("/me")
        .header("cookie", &cookie_header)
        .header("x-csrf-token", csrf)
        .send()
        .await
        .assert_status(vyuh::routes::StatusCode::OK);
    Ok(())
}

/// Verifies refresh-cookie rotation enforces its own CSRF double-submit value.
#[tokio::test]
async fn refresh_cookie_requires_csrf() -> Result<(), AuthError> {
    let auth = configured_token_auth(TokenConf::bearer(), TokenConf::cookie("refresh"));
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let login = site
        .auth()
        .login(AuthUser::new("user-1"), &[REPORTS])
        .await?;
    let mut response = axum::response::Response::new(axum::body::Body::empty());
    login.write(&mut response);
    let cookies = response_cookie_values(&response)?;
    let refresh = cookie_value(&cookies, "refresh")?;
    let csrf = cookie_value(&cookies, "refresh_csrf")?;
    let cookie_header = format!("refresh={refresh}; refresh_csrf={csrf}");
    let missing = cookie_parts(&cookie_header, None)?;
    assert!(matches!(
        site.auth().refresh(&missing, &[REPORTS]).await,
        Err(AuthError::InvalidCsrfToken)
    ));
    let valid = cookie_parts(&cookie_header, Some(csrf))?;
    let rotated = site.auth().refresh(&valid, &[REPORTS]).await?;
    assert!(rotated.credentials().refresh().is_some());
    Ok(())
}

/// Verifies logout clears access, refresh, and CSRF cookies together.
#[tokio::test]
async fn cookie_logout_clears_complete_provider_state() -> Result<(), AuthError> {
    let auth = configured_token_auth(TokenConf::cookie("access"), TokenConf::cookie("refresh"));
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let login = site
        .auth()
        .login(AuthUser::new("user-1"), &[REPORTS])
        .await?;
    let mut issued = axum::response::Response::new(axum::body::Body::empty());
    login.write(&mut issued);
    let cookies = response_cookie_values(&issued)?;
    let access = cookie_value(&cookies, "access")?;
    let csrf = cookie_value(&cookies, "access_csrf")?;
    let request = cookie_parts(&format!("access={access}; access_csrf={csrf}"), Some(csrf))?;
    let mut response = axum::response::Response::new(axum::body::Body::empty());
    let logout = site.auth().logout(&request).await?;
    logout.write(&mut response);
    assert_eq!(response.headers().get_all("set-cookie").iter().count(), 4);
    Ok(())
}

fn response_cookie_values(response: &axum::response::Response) -> Result<Vec<String>, AuthError> {
    response
        .headers()
        .get_all("set-cookie")
        .iter()
        .map(|value| value.to_str().map(str::to_owned).map_err(auth_error))
        .collect()
}

fn cookie_value<'a>(cookies: &'a [String], name: &str) -> Result<&'a str, AuthError> {
    let prefix = format!("{name}=");
    cookies
        .iter()
        .find_map(|cookie| cookie.strip_prefix(&prefix))
        .and_then(|value| value.split(';').next())
        .ok_or(AuthError::InvalidCredential)
}

/// Builds unsafe request parts with an optional CSRF header for cookie helper tests.
fn cookie_parts(cookie: &str, csrf: Option<&str>) -> Result<axum::http::request::Parts, AuthError> {
    let mut request = axum::http::Request::builder()
        .method("POST")
        .header("cookie", cookie);
    if let Some(csrf) = csrf {
        request = request.header("x-csrf-token", csrf);
    }
    request
        .body(axum::body::Body::empty())
        .map(|request| request.into_parts().0)
        .map_err(auth_error)
}

/// Verifies LoginResponse data replacement preserves credential attachments.
#[tokio::test]
async fn login_response_preserves_data() -> Result<(), AuthError> {
    let site = Site::build(config(), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let login = site
        .auth()
        .login(AuthUser::new("user-1"), &[REPORTS])
        .await?
        .data(serde_json::json!({ "user": "user-1" }));
    assert_eq!(login.data_ref(), &serde_json::json!({ "user": "user-1" }));
    assert!(!login.credentials().access().is_empty());
    Ok(())
}

/// Verifies empty audience slices resolve through the site default audience.
#[tokio::test]
async fn login_uses_default_for_empty_audiences() -> Result<(), AuthError> {
    let site = Site::build(config(), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let result = site.auth().login(AuthUser::new("user-1"), &[]).await;
    assert!(result.is_ok());
    Ok(())
}

/// Verifies an authenticated route without metadata uses the configured default audience.
#[tokio::test]
async fn route_without_audience_uses_site_default() -> Result<(), AuthError> {
    let auth = AuthConf::default().default_audience(REPORTS);
    let site = Site::build(config().auth(auth), bundle_without_audience())
        .await
        .map_err(auth_error)?;
    let operation = site
        .operations()
        .list()
        .find(|operation| operation.path == "/me-default")
        .ok_or_else(|| AuthError::Internal("default-audience route is missing".into()))?;
    assert_eq!(operation.audience(), Some("reports"));
    let login = site.auth().login(AuthUser::new("user-1"), &[]).await?;
    TestSite::new(site)
        .get("/me-default")
        .header(
            "authorization",
            &format!("Bearer {}", login.credentials().access()),
        )
        .send()
        .await
        .assert_status(vyuh::routes::StatusCode::OK);
    Ok(())
}

/// Verifies a legacy token without `aud` owns only the configured default audience.
#[tokio::test]
async fn missing_token_audience_maps_only_to_default() -> Result<(), AuthError> {
    let provider = TokenProvider::verify_only(AudienceLessDecoder, "Legacy");
    let auth = AuthConf::empty()
        .default_audience(REPORTS)
        .provider(EXTERNAL, provider);
    let site = Site::build(config().auth(auth), bundle_without_audience())
        .await
        .map_err(auth_error)?;
    TestSite::new(site)
        .get("/me-default")
        .header("authorization", "Bearer legacy-token")
        .send()
        .await
        .assert_status(vyuh::routes::StatusCode::OK);
    Ok(())
}

/// Verifies default-audience normalization cannot authorize another explicit audience.
#[tokio::test]
async fn missing_token_audience_cannot_escalate() -> Result<(), AuthError> {
    let provider = TokenProvider::verify_only(AudienceLessDecoder, "Legacy");
    let auth = AuthConf::empty()
        .default_audience(REPORTS)
        .provider(EXTERNAL, provider);
    let protected = bundle_without_audience().with_audience(ADMIN);
    let site = Site::build(config().auth(auth), protected)
        .await
        .map_err(auth_error)?;
    TestSite::new(site)
        .get("/me-default")
        .header("authorization", "Bearer legacy-token")
        .send()
        .await
        .assert_status(vyuh::routes::StatusCode::FORBIDDEN);
    Ok(())
}

/// Verifies a refresh with an empty audience slice preserves only the site default.
#[tokio::test]
async fn refresh_empty_audiences_uses_site_default() -> Result<(), AuthError> {
    let site = Site::build(config(), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let login = site.auth().login(AuthUser::new("user-1"), &[]).await?;
    let refresh = login
        .credentials()
        .refresh()
        .ok_or(AuthError::InvalidCredential)?;
    let rotated = site.auth().refresh(&bearer_parts(refresh)?, &[]).await?;
    assert!(rotated.credentials().refresh().is_some());
    Ok(())
}

/// Verifies an explicitly empty external audience claim is never normalized.
#[test]
fn explicit_empty_token_audience_is_invalid() -> Result<(), AuthError> {
    let issued_at = chrono::Utc::now();
    let token = AuthToken::builder(
        EXTERNAL,
        TokenKind::Access,
        "legacy-user",
        issued_at,
        issued_at + chrono::Duration::hours(1),
    )
    .audiences(Vec::<String>::new())
    .build();
    assert!(matches!(token, Err(AuthError::InvalidCredential)));
    Ok(())
}

/// Verifies strict audience policy rejects empty login audience slices.
#[tokio::test]
async fn strict_audience_mode_rejects_empty_login_audiences() -> Result<(), AuthError> {
    let auth = AuthConf::default().require_explicit_audiences();
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let result = site.auth().login(AuthUser::new("user-1"), &[]).await;
    assert!(result.is_err());
    Ok(())
}

/// Verifies strict audience policy rejects authenticated routes without metadata at startup.
#[tokio::test]
async fn strict_audience_mode_rejects_route_without_audience() -> Result<(), AuthError> {
    let auth = AuthConf::default().require_explicit_audiences();
    let result = Site::build(config().auth(auth), bundle_without_audience()).await;
    assert!(result.is_err());
    Ok(())
}

/// Verifies providers cannot silently share one access credential selector.
#[tokio::test]
async fn duplicate_access_selector_is_rejected() -> Result<(), AuthError> {
    let duplicate = TokenProvider::new(Jwt::hs256_site_secret());
    let result = Site::build(
        config().auth(AuthConf::default().provider(ALTERNATE, duplicate)),
        bundles::Bundle::default(),
    )
    .await;
    assert!(result.is_err());
    Ok(())
}

/// Verifies access and refresh selectors cannot collide across providers.
#[tokio::test]
async fn cross_kind_selector_collision_is_rejected() -> Result<(), AuthError> {
    let provider = TokenProvider::new(Jwt::hs256_site_secret())
        .access(TokenConf::header("x-alternate"))
        .refresh(TokenConf::bearer());
    let result = Site::build(
        config().auth(AuthConf::default().provider(ALTERNATE, provider)),
        bundles::Bundle::default(),
    )
    .await;
    assert!(result.is_err());
    Ok(())
}

/// Verifies duplicate bearer values fail before refresh token parsing.
#[tokio::test]
async fn duplicate_bearer_values_are_rejected() -> Result<(), AuthError> {
    let site = Site::build(config(), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let mut request = axum::http::Request::new(axum::body::Body::empty());
    request.headers_mut().append(
        "authorization",
        axum::http::HeaderValue::from_static("Bearer first"),
    );
    request.headers_mut().append(
        "authorization",
        axum::http::HeaderValue::from_static("Bearer second"),
    );
    let result = site.auth().refresh(&request.into_parts().0, &[]).await;
    assert!(matches!(result, Err(AuthError::MalformedLocation)));
    Ok(())
}

/// Verifies duplicate query credentials fail rather than selecting one value.
#[tokio::test]
async fn duplicate_query_credentials_are_rejected() -> Result<(), AuthError> {
    let provider = TokenProvider::new(Jwt::hs256_site_secret())
        .refresh(TokenConf::query("token", UnsafeQueryCredentials::allow()));
    let auth = AuthConf::empty().provider(DEFAULT_AUTH_PROVIDER, provider);
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let request = axum::http::Request::builder()
        .uri("/refresh?token=one&token=two")
        .body(axum::body::Body::empty())
        .map_err(auth_error)?;
    let result = site.auth().refresh(&request.into_parts().0, &[]).await;
    assert!(matches!(result, Err(AuthError::MalformedLocation)));
    Ok(())
}

/// Verifies duplicate target cookie names fail rather than selecting one value.
#[tokio::test]
async fn duplicate_cookie_credentials_are_rejected() -> Result<(), AuthError> {
    let provider = TokenProvider::new(Jwt::hs256_site_secret())
        .without_refresh()
        .access(TokenConf::cookie("access"));
    let auth = AuthConf::empty().provider(DEFAULT_AUTH_PROVIDER, provider);
    let site = Site::build(config().auth(auth), bundle())
        .await
        .map_err(auth_error)?;
    let login = site
        .auth()
        .login(AuthUser::new("cookie-user"), &[REPORTS])
        .await?;
    let cookie = format!("access={0}; access={0}", login.credentials().access());
    TestSite::new(site)
        .get("/me")
        .header("cookie", &cookie)
        .send()
        .await
        .assert_status(vyuh::routes::StatusCode::UNAUTHORIZED);
    Ok(())
}

/// Verifies runtime-only extras remain redacted and identity-neutral.
#[test]
fn auth_user_extra_is_runtime_only() {
    let user = AuthUser::new("user-1").with_extra(String::from("row"));
    assert_eq!(user.extra::<String>().map(String::as_str), Some("row"));
    assert!(format!("{user:?}").contains("<redacted>"));
    assert_eq!(user.without_extra(), AuthUser::new("user-1"));
}

/// Verifies site diagnostics redact active and fallback authentication secrets.
#[test]
fn site_configuration_debug_redacts_secret_ring() {
    let conf = config()
        .secret_key("recognizable-active-auth-secret-value")
        .secret_key_fallbacks(["recognizable-fallback-auth-secret-value"]);
    let diagnostic = format!("{conf:?}");
    assert!(!diagnostic.contains("recognizable-active"));
    assert!(!diagnostic.contains("recognizable-fallback"));
    assert!(diagnostic.contains("<redacted>"));
}
