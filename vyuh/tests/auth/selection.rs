use axum::response::IntoResponse;

use super::*;

const UNKNOWN_PROVIDER: AuthProvider = AuthProvider::new("unknown-provider");
const INVALID_PROVIDER: AuthProvider = AuthProvider::new("invalid provider");
const UNKNOWN_PASSWORD: LoginMethod<PasswordCredentials> = LoginMethod::new("unknown-password");
const UNKNOWN_MFA: LoginMethod<PasswordCredentials, MfaResponse> = LoginMethod::new("unknown-mfa");

/// Verifies provider selectors are infallible and login reports lookup failures.
#[tokio::test]
async fn provider_login_errors_are_terminal() -> Result<(), AuthError> {
    let site = Site::build(config(), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let unknown = site
        .auth()
        .using(UNKNOWN_PROVIDER)
        .login(AuthUser::new("user-1"), &[REPORTS])
        .await;
    assert!(matches!(unknown, Err(AuthError::ProviderNotFound(_))));
    let invalid = site
        .auth()
        .using(INVALID_PROVIDER)
        .login(AuthUser::new("user-1"), &[REPORTS])
        .await;
    assert!(matches!(invalid, Err(AuthError::InvalidProviderId(_))));
    Ok(())
}

/// Verifies refresh and logout report provider lookup failures at their terminals.
#[tokio::test]
async fn provider_request_errors_are_terminal() -> Result<(), AuthError> {
    let site = Site::build(config(), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let parts = empty_parts();
    assert!(matches!(
        site.auth()
            .using(UNKNOWN_PROVIDER)
            .refresh(&parts, &[REPORTS])
            .await,
        Err(AuthError::ProviderNotFound(_))
    ));
    assert!(matches!(
        site.auth().using(UNKNOWN_PROVIDER).logout(&parts).await,
        Err(AuthError::ProviderNotFound(_))
    ));
    Ok(())
}

/// Verifies multi-step provider failures are deferred through both flow terminals.
#[tokio::test]
async fn flow_provider_errors_are_terminal() -> Result<(), AuthError> {
    let method = PasswordLogin::new(TestPasswords).then(MfaLogin::new(TestFactors).totp());
    let auth = AuthConf::default().method(PASSWORD_MFA, method);
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let selected = site.auth().using(UNKNOWN_PROVIDER).via(PASSWORD_MFA);
    let begin = selected
        .begin(
            PasswordCredentials::new("user@example.com", "correct-password"),
            &[REPORTS],
        )
        .await;
    assert!(matches!(begin, Err(AuthError::ProviderNotFound(_))));
    let complete = selected
        .complete(MfaResponse::totp("opaque-state", "123456"))
        .await;
    assert!(matches!(complete, Err(AuthError::ProviderNotFound(_))));
    Ok(())
}

/// Verifies unknown one-step and flow methods fail only at their terminal operations.
#[tokio::test]
async fn login_method_errors_are_terminal() -> Result<(), AuthError> {
    let site = Site::build(config(), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let login = site
        .auth()
        .via(UNKNOWN_PASSWORD)
        .login(
            PasswordCredentials::new("user@example.com", "correct-password"),
            &[REPORTS],
        )
        .await;
    assert!(matches!(login, Err(AuthError::LoginMethodNotFound(_))));
    let begin = site
        .auth()
        .via(UNKNOWN_MFA)
        .begin(
            PasswordCredentials::new("user@example.com", "correct-password"),
            &[REPORTS],
        )
        .await;
    assert!(matches!(begin, Err(AuthError::LoginMethodNotFound(_))));
    let complete = site
        .auth()
        .via(UNKNOWN_MFA)
        .complete(MfaResponse::totp("opaque-state", "123456"))
        .await;
    assert!(matches!(complete, Err(AuthError::LoginMethodNotFound(_))));
    Ok(())
}

/// Verifies default refresh never probes a separately selected provider.
#[tokio::test]
async fn default_refresh_does_not_probe_other_providers() -> Result<(), AuthError> {
    let auth = AuthConf::empty().provider(ALTERNATE, TokenProvider::new(Jwt::hs256_site_secret()));
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let login = site
        .auth()
        .using(ALTERNATE)
        .login(AuthUser::new("user-1"), &[REPORTS])
        .await?;
    let refresh = login
        .credentials()
        .refresh()
        .ok_or(AuthError::UnsupportedProviderCapability)?;
    let parts = bearer_parts(refresh)?;
    assert!(matches!(
        site.auth().refresh(&parts, &[REPORTS]).await,
        Err(AuthError::ProviderNotFound(_))
    ));
    assert!(
        site.auth()
            .using(ALTERNATE)
            .refresh(&parts, &[REPORTS])
            .await
            .is_ok()
    );
    Ok(())
}

/// Verifies default logout does not clear a separately selected provider's cookies.
#[tokio::test]
async fn default_logout_does_not_touch_other_providers() -> Result<(), AuthError> {
    let provider = TokenProvider::new(Jwt::hs256_site_secret())
        .access(TokenConf::cookie("alternate_access"))
        .refresh(TokenConf::cookie("alternate_refresh"));
    let auth = AuthConf::default().provider(ALTERNATE, provider);
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let parts = alternate_cookie_parts(&site).await?;
    let mut default_response = axum::response::Response::new(axum::body::Body::empty());
    site.auth()
        .logout(&parts)
        .await?
        .write(&mut default_response);
    assert_eq!(
        default_response
            .headers()
            .get_all("set-cookie")
            .iter()
            .count(),
        0
    );
    let mut selected_response = axum::response::Response::new(axum::body::Body::empty());
    site.auth()
        .using(ALTERNATE)
        .logout(&parts)
        .await?
        .write(&mut selected_response);
    assert_eq!(
        selected_response
            .headers()
            .get_all("set-cookie")
            .iter()
            .count(),
        4
    );
    Ok(())
}

/// Verifies absent logout is idempotent and its default response is successful JSON.
#[tokio::test]
async fn logout_response_is_response_ready() -> Result<(), AuthError> {
    let site = Site::build(config(), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let response = site.auth().logout(&empty_parts()).await?.into_response();
    assert_eq!(response.status(), vyuh::routes::StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024)
        .await
        .map_err(auth_error)?;
    assert_eq!(body.as_ref(), br#"{"ok":true}"#);
    Ok(())
}

/// Verifies a malformed presented credential still fails selected-provider logout.
#[tokio::test]
async fn malformed_logout_credential_fails() -> Result<(), AuthError> {
    let site = Site::build(config(), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let result = site.auth().logout(&bearer_parts("malformed")?).await;
    assert!(matches!(result, Err(AuthError::InvalidCredential)));
    Ok(())
}

/// Creates request parts with no presented credentials.
fn empty_parts() -> axum::http::request::Parts {
    axum::http::Request::new(axum::body::Body::empty())
        .into_parts()
        .0
}

/// Issues the alternate cookie pair and creates a valid unsafe logout request.
async fn alternate_cookie_parts(site: &Site) -> Result<axum::http::request::Parts, AuthError> {
    let login = site
        .auth()
        .using(ALTERNATE)
        .login(AuthUser::new("user-1"), &[REPORTS])
        .await?;
    let mut response = axum::response::Response::new(axum::body::Body::empty());
    login.write(&mut response);
    let cookies = response_cookies(&response)?;
    let access = cookie_value(&cookies, "alternate_access")?;
    let csrf = cookie_value(&cookies, "alternate_access_csrf")?;
    cookie_parts(
        &format!("alternate_access={access}; alternate_access_csrf={csrf}"),
        csrf,
    )
}

/// Returns all cookie response headers as owned strings.
fn response_cookies(response: &axum::response::Response) -> Result<Vec<String>, AuthError> {
    response
        .headers()
        .get_all("set-cookie")
        .iter()
        .map(|value| value.to_str().map(str::to_owned).map_err(auth_error))
        .collect()
}

/// Finds one named cookie value in response cookie headers.
fn cookie_value<'a>(cookies: &'a [String], name: &str) -> Result<&'a str, AuthError> {
    let prefix = format!("{name}=");
    cookies
        .iter()
        .find_map(|cookie| cookie.strip_prefix(&prefix))
        .and_then(|value| value.split(';').next())
        .ok_or(AuthError::InvalidCredential)
}

/// Creates an unsafe request with matching credential and CSRF cookies.
fn cookie_parts(cookie: &str, csrf: &str) -> Result<axum::http::request::Parts, AuthError> {
    axum::http::Request::builder()
        .method("POST")
        .header("cookie", cookie)
        .header("x-csrf-token", csrf)
        .body(axum::body::Body::empty())
        .map(|request| request.into_parts().0)
        .map_err(auth_error)
}
