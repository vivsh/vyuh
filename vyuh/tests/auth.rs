use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use axum::response::IntoResponse;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use vyuh::auth::KeySource;
#[cfg(feature = "oidc")]
use vyuh::auth::{OidcIdentity, OidcLogin, OidcStart, OidcUserMapper};
use vyuh::{
    Site, SiteConf,
    auth::{
        Audience, AuthConf, AuthError, AuthKey, AuthProvider, AuthToken, AuthTokenBuilder,
        AuthUser, BasicCredentials, BasicLogin, DEFAULT_AUTH_PROVIDER, DjangoSigning, Jwt,
        KeyLifecycle, KeyRequest, KeyVerifier, LoginMethod, LoginResponse, LoginStateStore,
        MfaLogin, MfaMethod, MfaResponse, MfaVerifier, PasswordCredentials, PasswordLogin,
        PasswordVerifier, PresentedCredential, PresentedSecret, RefreshMetadata, TokenClaims,
        TokenConf, TokenDecoder, TokenKind, TokenLifecycle, TokenProvider, UnsafeQueryCredentials,
    },
    bundles,
    routes::{Json, Methods, RouteConf},
    testing::TestSite,
};

#[path = "auth/provider_flows.rs"]
mod provider_flows;
#[path = "auth/rotation.rs"]
mod rotation;
#[path = "auth/selection.rs"]
mod selection;

const REPORTS: Audience = Audience::new("reports");
const ADMIN: Audience = Audience::new("admin");
const API_KEY: AuthProvider = AuthProvider::new("api-key");
const ALTERNATE: AuthProvider = AuthProvider::new("alternate");
const ROTATING: AuthProvider = AuthProvider::new("rotating");
const DJANGO: AuthProvider = AuthProvider::new("django");
const MIXED: AuthProvider = AuthProvider::new("mixed");
const EXTERNAL: AuthProvider = AuthProvider::new("external");
const SHARED_A: AuthProvider = AuthProvider::new("shared-a");
const SHARED_B: AuthProvider = AuthProvider::new("shared-b");
const PASSWORD: LoginMethod<PasswordCredentials> = LoginMethod::new("password");
const BASIC: LoginMethod<BasicCredentials> = LoginMethod::new("basic");
const PASSWORD_MFA: LoginMethod<PasswordCredentials, MfaResponse> =
    LoginMethod::new("password-mfa");
const BASIC_MFA: LoginMethod<BasicCredentials, MfaResponse> = LoginMethod::new("basic-mfa");
#[cfg(feature = "oidc")]
const OIDC: LoginMethod<OidcStart, vyuh::auth::OidcCallback> = LoginMethod::new("oidc");
#[cfg(feature = "branca")]
const BRANCA_PROVIDER: AuthProvider = AuthProvider::new("branca");
#[cfg(feature = "paseto")]
const PASETO_PROVIDER: AuthProvider = AuthProvider::new("paseto");

#[derive(Serialize, JsonSchema)]
struct WhoAmI {
    key: String,
    provider: String,
}

#[derive(Deserialize)]
struct DjangoFixture {
    secret: String,
    django_token: String,
    legacy_token: String,
}

#[derive(Serialize, Deserialize)]
struct ExternalJwtClaims {
    kind: String,
    subject: String,
    roles: u64,
    iat: i64,
    exp: i64,
    jti: String,
}

impl TokenClaims for ExternalJwtClaims {
    fn auth_token(self, builder: AuthTokenBuilder) -> Result<AuthToken, AuthError> {
        let issued_at =
            chrono::DateTime::from_timestamp(self.iat, 0).ok_or(AuthError::InvalidCredential)?;
        let expires_at =
            chrono::DateTime::from_timestamp(self.exp, 0).ok_or(AuthError::InvalidCredential)?;
        let kind = match self.kind.as_str() {
            "access" => TokenKind::Access,
            "refresh" => TokenKind::Refresh,
            _ => return Err(AuthError::InvalidCredential),
        };
        builder
            .kind(kind)
            .subject(self.subject)
            .issued_at(issued_at)
            .expires_at(expires_at)
            .roles(self.roles)
            .audiences([REPORTS.as_str()])
            .token_id(Some(self.jti))
            .build()
    }
}

async fn me(user: AuthUser) -> Json<WhoAmI> {
    Json(WhoAmI {
        key: user.key.to_string(),
        provider: user.provider().to_owned(),
    })
}

async fn assurance(user: AuthUser) -> Json<Vec<String>> {
    Json(user.authentication().methods().to_vec())
}

async fn basic_exchange(
    site: Site,
    credentials: BasicCredentials,
) -> Result<LoginResponse, vyuh::Error> {
    Ok(site
        .auth()
        .via(BASIC)
        .login(credentials, &[REPORTS])
        .await?)
}

fn config() -> SiteConf {
    SiteConf::default()
        .secret_key("auth-test-secret-minimum-32-chars")
        .log_init(false)
}

fn configured_token_auth(access: TokenConf, refresh: TokenConf) -> AuthConf {
    AuthConf::empty().provider(
        DEFAULT_AUTH_PROVIDER,
        TokenProvider::new(Jwt::hs256_site_secret())
            .access(access)
            .refresh(refresh),
    )
}

fn bundle() -> bundles::Bundle {
    bundles::bundle([bundles::route(
        me,
        RouteConf {
            name: "me".into(),
            methods: Methods::GET | Methods::POST,
            path: "/me".into(),
            slash: None,
        },
    )])
    .with_audience(REPORTS)
}

fn bundle_without_audience() -> bundles::Bundle {
    bundles::bundle([bundles::route(
        me,
        RouteConf {
            name: "me_default".into(),
            methods: Methods::GET,
            path: "/me-default".into(),
            slash: None,
        },
    )])
}

fn auth_error(error: impl std::fmt::Display) -> AuthError {
    AuthError::Internal(error.to_string())
}

fn bearer_parts(value: &str) -> Result<axum::http::request::Parts, AuthError> {
    let request = axum::http::Request::builder()
        .header("authorization", format!("Bearer {value}"))
        .body(axum::body::Body::empty())
        .map_err(auth_error)?;
    Ok(request.into_parts().0)
}

struct StaticKeyVerifier;

#[derive(Clone)]
struct KeyRevoker(Arc<AtomicBool>);

impl KeyLifecycle for KeyRevoker {
    async fn revoke(&self, credential: &PresentedCredential<'_>) -> Result<(), AuthError> {
        if credential.expose() != "service-secret" {
            return Err(AuthError::InvalidCredential);
        }
        self.0.store(true, Ordering::Relaxed);
        Ok(())
    }
}

impl KeyVerifier for StaticKeyVerifier {
    async fn verify(
        &self,
        credential: &PresentedCredential<'_>,
        request: KeyRequest<'_>,
    ) -> Result<AuthUser, AuthError> {
        if credential.expose() != "service-secret" || request.audience() != REPORTS.as_str() {
            return Err(AuthError::InvalidCredential);
        }
        Ok(AuthUser::new("service-1").with_extra("service-record".to_owned()))
    }
}

#[derive(Clone, Default)]
struct ReplayStore {
    consumed: Arc<Mutex<BTreeSet<String>>>,
}

struct ExternalDecoder;

impl TokenDecoder for ExternalDecoder {
    async fn decode(&self, presented: &PresentedCredential<'_>) -> Result<AuthToken, AuthError> {
        if presented.expose() != "externally-verified" {
            return Err(AuthError::InvalidCredential);
        }
        let issued_at = chrono::Utc::now();
        AuthToken::builder(EXTERNAL)
            .kind(TokenKind::Access)
            .subject("external-user")
            .issued_at(issued_at)
            .expires_at(issued_at + chrono::Duration::hours(1))
            .audiences([REPORTS.as_str()])
            .build()
    }
}

struct AudienceLessDecoder;

impl TokenDecoder for AudienceLessDecoder {
    async fn decode(&self, presented: &PresentedCredential<'_>) -> Result<AuthToken, AuthError> {
        if presented.expose() != "legacy-token" {
            return Err(AuthError::InvalidCredential);
        }
        let issued_at = chrono::Utc::now();
        AuthToken::builder(EXTERNAL)
            .kind(TokenKind::Access)
            .subject("legacy-user")
            .issued_at(issued_at)
            .expires_at(issued_at + chrono::Duration::hours(1))
            .build()
    }
}

impl TokenLifecycle for ReplayStore {
    async fn validate(&self, token: &AuthToken) -> Result<(), AuthError> {
        let consumed = self
            .consumed
            .lock()
            .map_err(|_| AuthError::Internal("refresh store lock failed".into()))?;
        let token_id = token.token_id().ok_or(AuthError::InvalidCredential)?;
        if token.kind() == TokenKind::Refresh && consumed.contains(token_id) {
            return Err(AuthError::InvalidCredential);
        }
        Ok(())
    }

    async fn rotate(
        &self,
        current: &AuthToken,
        _replacement: &RefreshMetadata,
    ) -> Result<(), AuthError> {
        let mut consumed = self
            .consumed
            .lock()
            .map_err(|_| AuthError::Internal("refresh store lock failed".into()))?;
        let token_id = current.token_id().ok_or(AuthError::InvalidCredential)?;
        if !consumed.insert(token_id.to_owned()) {
            return Err(AuthError::InvalidCredential);
        }
        Ok(())
    }

    async fn revoke(&self, token: &AuthToken) -> Result<(), AuthError> {
        let token_id = token.token_id().ok_or(AuthError::InvalidCredential)?;
        self.consumed
            .lock()
            .map_err(|_| AuthError::Internal("refresh store lock failed".into()))?
            .insert(token_id.to_owned());
        Ok(())
    }
}

impl LoginStateStore for ReplayStore {
    async fn consume(&self, state_id: &str, _expires_at: i64) -> Result<bool, AuthError> {
        self.consumed
            .lock()
            .map_err(|_| AuthError::Internal("login state store lock failed".into()))
            .map(|mut consumed| consumed.insert(state_id.to_owned()))
    }
}

#[derive(Clone, Copy)]
struct TestPasswords;

impl PasswordVerifier for TestPasswords {
    async fn verify(
        &self,
        username: &str,
        password: &PresentedSecret,
    ) -> Result<AuthUser, AuthError> {
        if username != "user@example.com" || password.expose() != "correct-password" {
            return Err(AuthError::InvalidCredential);
        }
        Ok(AuthUser::new("login-user"))
    }
}

#[derive(Clone, Copy)]
struct TestFactors;

impl MfaVerifier for TestFactors {
    async fn methods(&self, _user: &AuthUser) -> Result<Vec<MfaMethod>, AuthError> {
        Ok(vec![MfaMethod::Totp, MfaMethod::RecoveryCode])
    }

    async fn verify(&self, user: &AuthUser, response: &MfaResponse) -> Result<AuthUser, AuthError> {
        let accepted = match response.method() {
            MfaMethod::Totp => response.answer().expose() == "123456",
            MfaMethod::RecoveryCode => response.answer().expose() == "recovery-code",
            _ => false,
        };
        if !accepted {
            return Err(AuthError::InvalidCredential);
        }
        Ok(user.clone())
    }
}

#[cfg(feature = "oidc")]
#[derive(Clone, Copy)]
struct TestOidcMapper;

#[cfg(feature = "oidc")]
impl OidcUserMapper for TestOidcMapper {
    async fn map(&self, identity: &OidcIdentity) -> Result<AuthUser, AuthError> {
        Ok(AuthUser::new(identity.subject()))
    }
}

/// Verifies authenticated external JWT claims normalize through a provider-bound builder.
#[tokio::test]
async fn custom_jwt_claims_authenticate_as_normal_users() -> Result<(), AuthError> {
    let auth = AuthConf::empty().provider(
        EXTERNAL,
        TokenProvider::new(Jwt::hs256_site_secret())
            .custom_claims::<ExternalJwtClaims>()
            .access(TokenConf::header_with_scheme("authorization", "JWT")),
    );
    let site = Site::build(config().auth(auth), bundle())
        .await
        .map_err(auth_error)?;
    let now = chrono::Utc::now().timestamp();
    let claims = ExternalJwtClaims {
        kind: "access".into(),
        subject: "partner-user".into(),
        roles: 7,
        iat: now,
        exp: now + 300,
        jti: "partner-token-1".into(),
    };
    let token = jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
        &claims,
        &jsonwebtoken::EncodingKey::from_secret(b"auth-test-secret-minimum-32-chars"),
    )
    .map_err(auth_error)?;
    let response = TestSite::new(site)
        .get("/me")
        .header("authorization", &format!("JWT {token}"))
        .send()
        .await;
    response
        .assert_json(
            vyuh::routes::StatusCode::OK,
            &serde_json::json!({ "key": "partner-user", "provider": "external" }),
        )
        .await;
    Ok(())
}

/// Verifies typed claims cannot use a refresh credential to access a protected operation.
#[tokio::test]
async fn custom_jwt_claims_reject_refresh_tokens_for_access() -> Result<(), AuthError> {
    let auth = AuthConf::empty().provider(
        EXTERNAL,
        TokenProvider::new(Jwt::hs256_site_secret())
            .custom_claims::<ExternalJwtClaims>()
            .access(TokenConf::header_with_scheme("authorization", "JWT")),
    );
    let site = Site::build(config().auth(auth), bundle())
        .await
        .map_err(auth_error)?;
    let now = chrono::Utc::now().timestamp();
    let claims = ExternalJwtClaims {
        kind: "refresh".into(),
        subject: "partner-user".into(),
        roles: 7,
        iat: now,
        exp: now + 300,
        jti: "partner-refresh-1".into(),
    };
    let token = jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
        &claims,
        &jsonwebtoken::EncodingKey::from_secret(b"auth-test-secret-minimum-32-chars"),
    )
    .map_err(auth_error)?;
    TestSite::new(site)
        .get("/me")
        .header("authorization", &format!("JWT {token}"))
        .send()
        .await
        .assert_status(vyuh::routes::StatusCode::UNAUTHORIZED);
    Ok(())
}

/// Verifies external-claims providers remain verify-only even with an access configuration.
#[tokio::test]
async fn custom_jwt_claims_provider_cannot_issue_credentials() -> Result<(), AuthError> {
    let auth = AuthConf::empty().provider(
        EXTERNAL,
        TokenProvider::new(Jwt::hs256_site_secret())
            .custom_claims::<ExternalJwtClaims>()
            .access(TokenConf::bearer()),
    );
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let error = site
        .auth()
        .using(EXTERNAL)
        .login(AuthUser::new("partner-user"), &[REPORTS])
        .await
        .err()
        .ok_or(AuthError::InvalidCredential)?;
    assert!(matches!(error, AuthError::UnsupportedProviderCapability));
    Ok(())
}

/// Verifies a custom-claims provider rejects an attempted refresh configuration at build time.
#[tokio::test]
async fn custom_jwt_claims_provider_rejects_refresh_configuration() -> Result<(), AuthError> {
    let auth = AuthConf::empty().provider(
        EXTERNAL,
        TokenProvider::new(Jwt::hs256_site_secret())
            .custom_claims::<ExternalJwtClaims>()
            .refresh(TokenConf::header("x-partner-refresh")),
    );
    let error = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .err()
        .ok_or(AuthError::InvalidCredential)?;
    assert!(error.to_string().contains("verify-only providers"));
    Ok(())
}

/// Verifies typed password login delegates successful proof to the default token provider.
#[tokio::test]
async fn password_login_issues_default_credentials() -> Result<(), AuthError> {
    let auth = AuthConf::default().method(PASSWORD, PasswordLogin::new(TestPasswords));
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let login = site
        .auth()
        .via(PASSWORD)
        .login(
            PasswordCredentials::new("user@example.com", "correct-password"),
            &[REPORTS],
        )
        .await?;
    assert!(!login.credentials().access().is_empty());
    assert!(login.credentials().refresh().is_some());
    Ok(())
}

/// Verifies credential-provider and login-method selection compose in one chain.
#[tokio::test]
async fn password_login_uses_explicit_credential_provider() -> Result<(), AuthError> {
    let provider = TokenProvider::new(Jwt::hs256_site_secret())
        .access(TokenConf::header("x-login-access"))
        .refresh(TokenConf::header("x-login-refresh"));
    let auth = AuthConf::empty()
        .provider(ALTERNATE, provider)
        .method(PASSWORD, PasswordLogin::new(TestPasswords));
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let login = site
        .auth()
        .using(ALTERNATE)
        .via(PASSWORD)
        .login(
            PasswordCredentials::new("user@example.com", "correct-password"),
            &[REPORTS],
        )
        .await?;
    assert!(!login.credentials().access().is_empty());
    Ok(())
}

/// Verifies a descriptor with the right name but wrong input type is rejected safely.
#[tokio::test]
async fn login_method_type_mismatch_is_rejected() -> Result<(), AuthError> {
    const WRONG_PASSWORD: LoginMethod<BasicCredentials> = LoginMethod::new("password");

    let auth = AuthConf::default().method(PASSWORD, PasswordLogin::new(TestPasswords));
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let error = site
        .auth()
        .via(WRONG_PASSWORD)
        .login(
            BasicCredentials::new("user@example.com", "correct-password"),
            &[REPORTS],
        )
        .await
        .err()
        .ok_or_else(|| AuthError::Internal("mismatched login method was accepted".into()))?;
    assert!(matches!(error, AuthError::LoginMethodTypeMismatch(_)));
    Ok(())
}

/// Verifies duplicate login method names fail during site construction.
#[tokio::test]
async fn duplicate_login_methods_are_rejected() -> Result<(), AuthError> {
    let auth = AuthConf::default()
        .method(PASSWORD, PasswordLogin::new(TestPasswords))
        .method(PASSWORD, PasswordLogin::new(TestPasswords));
    let error = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .err()
        .ok_or_else(|| AuthError::Internal("duplicate login method was accepted".into()))?;
    assert!(error.to_string().contains("registered more than once"));
    Ok(())
}

/// Verifies Basic credentials are request input for token exchange rather than API authentication.
#[tokio::test]
async fn basic_login_exchanges_header_for_tokens() -> Result<(), AuthError> {
    let route = bundles::route(
        basic_exchange,
        RouteConf {
            name: "basic-login".into(),
            methods: Methods::POST,
            path: "/basic-login".into(),
            slash: None,
        },
    );
    let auth = AuthConf::default().method(BASIC, BasicLogin::new(TestPasswords));
    let site = Site::build(config().auth(auth), bundles::bundle([route]))
        .await
        .map_err(auth_error)?;
    let encoded = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        "user@example.com:correct-password",
    );
    let response = TestSite::new(site)
        .post("/basic-login")
        .header("authorization", &format!("Basic {encoded}"))
        .send()
        .await;
    let response = response.assert_status(vyuh::routes::StatusCode::OK);
    assert!(response.json::<serde_json::Value>().await["access_token"].is_string());
    Ok(())
}

/// Verifies MFA begin returns only a challenge and completion issues assurance-bearing tokens.
#[tokio::test]
async fn password_mfa_completes_with_assurance() -> Result<(), AuthError> {
    let method =
        PasswordLogin::new(TestPasswords).then(MfaLogin::new(TestFactors).totp().recovery_codes());
    let auth = AuthConf::default().method(PASSWORD_MFA, method);
    let route = bundles::route(
        assurance,
        RouteConf {
            name: "assurance".into(),
            methods: Methods::GET,
            path: "/assurance".into(),
            slash: None,
        },
    );
    let site = Site::build(
        config().auth(auth),
        bundles::bundle([route]).with_audience(REPORTS),
    )
    .await
    .map_err(auth_error)?;
    let selected = site.auth().via(PASSWORD_MFA);
    let challenge = selected
        .begin(
            PasswordCredentials::new("user@example.com", "correct-password"),
            &[REPORTS],
        )
        .await?;
    let token = challenge.token().ok_or(AuthError::InvalidLoginState)?;
    let login = selected
        .complete(MfaResponse::totp(token, "123456"))
        .await?;
    let response = TestSite::new(site)
        .get("/assurance")
        .header(
            "authorization",
            &format!("Bearer {}", login.credentials().access()),
        )
        .send()
        .await;
    response
        .assert_json(
            vyuh::routes::StatusCode::OK,
            &serde_json::json!(["password", "totp"]),
        )
        .await;
    Ok(())
}

/// Verifies HTTP Basic can compose with the same local MFA challenge API.
#[tokio::test]
async fn basic_mfa_completes_with_assurance() -> Result<(), AuthError> {
    let method = BasicLogin::new(TestPasswords).then(MfaLogin::new(TestFactors).totp());
    let auth = AuthConf::default().method(BASIC_MFA, method);
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let challenge = site
        .auth()
        .via(BASIC_MFA)
        .begin(
            BasicCredentials::new("user@example.com", "correct-password"),
            &[REPORTS],
        )
        .await?;
    let token = challenge.token().ok_or(AuthError::InvalidLoginState)?;
    let login = site
        .auth()
        .via(BASIC_MFA)
        .complete(MfaResponse::totp(token, "123456"))
        .await?;
    assert!(login.credentials().access().len() > 32);
    Ok(())
}

/// Verifies an optional login state store rejects successful challenge replay.
#[tokio::test]
async fn mfa_state_store_rejects_replay() -> Result<(), AuthError> {
    let method = PasswordLogin::new(TestPasswords).then(MfaLogin::new(TestFactors).totp());
    let auth = AuthConf::default()
        .method(PASSWORD_MFA, method)
        .login_state_store(ReplayStore::default());
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let selected = site.auth().via(PASSWORD_MFA);
    let challenge = selected
        .begin(
            PasswordCredentials::new("user@example.com", "correct-password"),
            &[REPORTS],
        )
        .await?;
    let token = challenge.token().ok_or(AuthError::InvalidLoginState)?;
    selected
        .complete(MfaResponse::totp(token, "123456"))
        .await?;
    let error = selected
        .complete(MfaResponse::totp(token, "123456"))
        .await
        .err()
        .ok_or_else(|| AuthError::Internal("MFA replay unexpectedly succeeded".into()))?;
    assert!(matches!(error, AuthError::InvalidLoginState));
    Ok(())
}

/// Verifies completion cannot switch away from the credential provider bound at begin.
#[tokio::test]
async fn mfa_completion_cannot_switch_credential_provider() -> Result<(), AuthError> {
    let provider = TokenProvider::new(Jwt::hs256_site_secret())
        .access(TokenConf::header("x-mfa-access"))
        .refresh(TokenConf::header("x-mfa-refresh"));
    let method = PasswordLogin::new(TestPasswords).then(MfaLogin::new(TestFactors).totp());
    let auth = AuthConf::default()
        .provider(ALTERNATE, provider)
        .method(PASSWORD_MFA, method);
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let challenge = site
        .auth()
        .using(ALTERNATE)
        .via(PASSWORD_MFA)
        .begin(
            PasswordCredentials::new("user@example.com", "correct-password"),
            &[REPORTS],
        )
        .await?;
    let token = challenge.token().ok_or(AuthError::InvalidLoginState)?;
    let error = site
        .auth()
        .via(PASSWORD_MFA)
        .complete(MfaResponse::totp(token, "123456"))
        .await
        .err()
        .ok_or_else(|| AuthError::Internal("credential provider switch succeeded".into()))?;
    assert!(matches!(error, AuthError::InvalidLoginState));
    Ok(())
}

/// Verifies active login challenges survive Django-style site-secret rotation.
#[tokio::test]
async fn login_challenge_accepts_site_secret_fallback() -> Result<(), AuthError> {
    let auth = AuthConf::default().method(
        PASSWORD_MFA,
        PasswordLogin::new(TestPasswords).then(MfaLogin::new(TestFactors).totp()),
    );
    let old = Site::build(
        config()
            .secret_key("old-login-secret-at-least-32-characters")
            .auth(auth),
        bundles::Bundle::default(),
    )
    .await
    .map_err(auth_error)?;
    let challenge = old
        .auth()
        .via(PASSWORD_MFA)
        .begin(
            PasswordCredentials::new("user@example.com", "correct-password"),
            &[REPORTS],
        )
        .await?;
    let token = challenge.token().ok_or(AuthError::InvalidLoginState)?;
    let rotated_auth = AuthConf::default().method(
        PASSWORD_MFA,
        PasswordLogin::new(TestPasswords).then(MfaLogin::new(TestFactors).totp()),
    );
    let rotated = Site::build(
        config()
            .secret_key("new-login-secret-at-least-32-characters")
            .secret_key_fallbacks(["old-login-secret-at-least-32-characters"])
            .auth(rotated_auth),
        bundles::Bundle::default(),
    )
    .await
    .map_err(auth_error)?;
    let login = rotated
        .auth()
        .via(PASSWORD_MFA)
        .complete(MfaResponse::totp(token, "123456"))
        .await?;
    assert!(!login.credentials().access().is_empty());
    Ok(())
}

/// Verifies OIDC begin performs discovery and emits state, nonce, and PKCE parameters.
#[cfg(feature = "oidc")]
#[tokio::test]
async fn oidc_begin_uses_authorization_code_pkce() -> Result<(), AuthError> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(auth_error)?;
    let address = listener.local_addr().map_err(auth_error)?;
    let issuer = format!("http://{address}");
    let oidc_state = MockOidcState::new(issuer.clone());
    let router = oidc_discovery_router(oidc_state);
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    let login = OidcLogin::discovery(&issuer)
        .client_id("test-client")
        .client_secret(vyuh::auth::KeySource::inline("test-secret"))
        .redirect_uri(format!("{issuer}/callback"))
        .scopes(["email", "profile"])
        .mapper(TestOidcMapper);
    let auth = AuthConf::default().method(OIDC, login);
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let challenge = site
        .auth()
        .via(OIDC)
        .begin(OidcStart::new().return_to("/dashboard"), &[REPORTS])
        .await?;
    let url = challenge
        .redirect_url()
        .ok_or(AuthError::InvalidLoginState)?;
    assert!(url.starts_with(&format!("{issuer}/authorize?")));
    assert!(url.contains("response_type=code"));
    assert!(url.contains("code_challenge_method=S256"));
    assert!(url.contains("state="));
    assert!(url.contains("nonce="));
    server.abort();
    Ok(())
}

/// Verifies OIDC callback verification maps identity before issuing credentials.
#[cfg(feature = "oidc")]
#[tokio::test]
async fn oidc_callback_verifies_and_issues_credentials() -> Result<(), AuthError> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(auth_error)?;
    let address = listener.local_addr().map_err(auth_error)?;
    let issuer = format!("http://{address}");
    let oidc_state = MockOidcState::new(issuer.clone());
    let router = oidc_discovery_router(oidc_state.clone());
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    let login = OidcLogin::discovery(&issuer)
        .client_id("test-client")
        .client_secret(vyuh::auth::KeySource::inline("test-secret"))
        .redirect_uri(format!("{issuer}/callback"))
        .mapper(TestOidcMapper);
    let auth = AuthConf::default().method(OIDC, login);
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let selected = site.auth().via(OIDC);
    let challenge = selected.begin(OidcStart::new(), &[REPORTS]).await?;
    let redirect = url::Url::parse(
        challenge
            .redirect_url()
            .ok_or(AuthError::InvalidLoginState)?,
    )
    .map_err(auth_error)?;
    let params = redirect
        .query_pairs()
        .collect::<std::collections::BTreeMap<_, _>>();
    let nonce = params
        .get("nonce")
        .ok_or(AuthError::InvalidLoginState)?
        .to_string();
    let state = params
        .get("state")
        .ok_or(AuthError::InvalidLoginState)?
        .to_string();
    oidc_state.set_nonce(nonce)?;
    let callback = serde_json::from_value::<vyuh::auth::OidcCallback>(serde_json::json!({
        "code": "valid-code",
        "state": state,
    }))
    .map_err(auth_error)?;
    let completed = selected.complete(callback).await?;
    assert!(!completed.credentials().access().is_empty());
    server.abort();
    Ok(())
}

/// Verifies non-loopback OIDC endpoints require HTTPS at startup.
#[cfg(feature = "oidc")]
#[tokio::test]
async fn oidc_rejects_insecure_remote_endpoints() -> Result<(), AuthError> {
    let login = OidcLogin::discovery("http://accounts.example.com")
        .client_id("test-client")
        .redirect_uri("http://app.example.com/callback")
        .mapper(TestOidcMapper);
    let auth = AuthConf::default().method(OIDC, login);
    let error = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .err()
        .ok_or_else(|| AuthError::Internal("insecure OIDC config was accepted".into()))?;
    assert!(error.to_string().contains("must use HTTPS"));
    Ok(())
}

/// Verifies OIDC discovery transport failures are reported as provider outages.
#[cfg(feature = "oidc")]
#[tokio::test]
async fn oidc_discovery_outage_is_provider_unavailable() -> Result<(), AuthError> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(auth_error)?;
    let issuer = format!("http://{}", listener.local_addr().map_err(auth_error)?);
    drop(listener);
    let login = OidcLogin::discovery(&issuer)
        .client_id("test-client")
        .redirect_uri(format!("{issuer}/callback"))
        .mapper(TestOidcMapper);
    let auth = AuthConf::default().method(OIDC, login);
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let error = site
        .auth()
        .via(OIDC)
        .begin(OidcStart::new(), &[REPORTS])
        .await
        .err()
        .ok_or_else(|| AuthError::Internal("OIDC discovery unexpectedly succeeded".into()))?;
    assert!(matches!(error, AuthError::ProviderUnavailable));
    Ok(())
}

#[cfg(feature = "oidc")]
#[derive(Clone)]
struct MockOidcState {
    issuer: String,
    nonce: Arc<Mutex<Option<String>>>,
}

#[cfg(feature = "oidc")]
impl MockOidcState {
    fn new(issuer: String) -> Self {
        Self {
            issuer,
            nonce: Arc::new(Mutex::new(None)),
        }
    }

    fn set_nonce(&self, nonce: String) -> Result<(), AuthError> {
        *self
            .nonce
            .lock()
            .map_err(|_| AuthError::Internal("OIDC nonce lock failed".into()))? = Some(nonce);
        Ok(())
    }
}

#[cfg(feature = "oidc")]
fn oidc_discovery_router(state: MockOidcState) -> axum::Router {
    use axum::{Json as AxumJson, extract::State, routing::get};

    async fn discovery(State(state): State<MockOidcState>) -> AxumJson<serde_json::Value> {
        let issuer = state.issuer;
        AxumJson(serde_json::json!({
            "issuer": issuer,
            "authorization_endpoint": format!("{issuer}/authorize"),
            "token_endpoint": format!("{issuer}/token"),
            "jwks_uri": format!("{issuer}/jwks"),
            "response_types_supported": ["code"],
            "subject_types_supported": ["public"],
            "id_token_signing_alg_values_supported": ["HS256"]
        }))
    }

    async fn jwks() -> AxumJson<serde_json::Value> {
        AxumJson(serde_json::json!({ "keys": [] }))
    }

    async fn token(State(state): State<MockOidcState>) -> AxumJson<serde_json::Value> {
        let nonce = state
            .nonce
            .lock()
            .ok()
            .and_then(|value| value.clone())
            .unwrap_or_default();
        let now = chrono::Utc::now().timestamp();
        let claims = serde_json::json!({
            "iss": state.issuer,
            "sub": "oidc-user",
            "aud": "test-client",
            "exp": now + 300,
            "iat": now,
            "auth_time": now,
            "nonce": nonce,
            "amr": ["pwd"]
        });
        let id_token = jsonwebtoken::encode(
            &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
            &claims,
            &jsonwebtoken::EncodingKey::from_secret(b"test-secret"),
        )
        .unwrap_or_default();
        AxumJson(serde_json::json!({
            "access_token": "mock-access-token",
            "token_type": "Bearer",
            "expires_in": 300,
            "id_token": id_token
        }))
    }

    axum::Router::new()
        .route("/.well-known/openid-configuration", get(discovery))
        .route("/jwks", get(jwks))
        .route("/token", axum::routing::post(token))
        .with_state(state)
}
