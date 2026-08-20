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
#[cfg(feature = "email")]
use vyuh::auth::UnsafeReusableMagicLinks;
#[cfg(feature = "federated")]
use vyuh::auth::{FederatedIdentity, FederatedLogin, FederatedStart, FederatedUserMapper};
use vyuh::{
    Site, SiteConf,
    auth::{
        Audience, AuthConf, AuthError, AuthKey, AuthProvider, AuthToken, AuthTokenBuilder,
        AuthUser, BasicCredentials, BasicLogin, DEFAULT_AUTH_PROVIDER, DjangoSigning, Jwt,
        KeyLifecycle, KeyRequest, KeyVerifier, LoginMethod, LoginResponse, LoginStateStore,
        MfaLogin, MfaMethod, MfaResponse, MfaVerifier, PasswordCredentials, PasswordLogin,
        PasswordVerifier, Permit, PresentedCredential, PresentedSecret, RefreshMetadata, Scope,
        ScopeExpr, ScopeRule, TokenClaims, TokenConf, TokenDecoder, TokenKind, TokenLifecycle,
        TokenProvider, TokenVerifier, UnsafeQueryCredentials,
    },
    bundles,
    routes::{Json, Methods, RouteConf},
    testing::TestSite,
};

#[path = "auth/passwordless.rs"]
mod passwordless;
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
const PARTNER_READ: Scope = Scope::of("partner:read");
const REPORTS_READ: Scope = Scope::of("reports:read");
const REPORTS_WRITE: Scope = Scope::of("reports:write");
const REPORT_SCOPES: &[Scope] = &[REPORTS_WRITE, REPORTS_READ];
const INVALID_SCOPES: &[Scope] = &[Scope::of("invalid scope")];

struct ReadReports;

impl ScopeRule for ReadReports {
    const EXPR: ScopeExpr = ScopeExpr::any(REPORT_SCOPES);
}

struct InvalidRule;

impl ScopeRule for InvalidRule {
    const EXPR: ScopeExpr = ScopeExpr::all(INVALID_SCOPES);
}
const PASSWORD: LoginMethod<PasswordCredentials> = LoginMethod::new("password");
const BASIC: LoginMethod<BasicCredentials> = LoginMethod::new("basic");
const PASSWORD_MFA: LoginMethod<PasswordCredentials, MfaResponse> =
    LoginMethod::new("password-mfa");
const BASIC_MFA: LoginMethod<BasicCredentials, MfaResponse> = LoginMethod::new("basic-mfa");
#[cfg(feature = "federated")]
const OIDC: LoginMethod<FederatedStart, vyuh::auth::FederatedCallback> = LoginMethod::new("oidc");
#[cfg(feature = "branca")]
const BRANCA_PROVIDER: AuthProvider = AuthProvider::new("branca");
#[cfg(feature = "paseto")]
const PASETO_PROVIDER: AuthProvider = AuthProvider::new("paseto");

#[derive(Serialize, JsonSchema)]
struct WhoAmI {
    subject: String,
    provider: Option<String>,
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
        let scopes = (self.roles & 1 != 0).then_some(PARTNER_READ).into_iter();
        builder
            .kind(kind)
            .subject(self.subject)
            .issued_at(issued_at)
            .expires_at(expires_at)
            .scopes(scopes)
            .audiences([REPORTS.as_str()])
            .token_id(Some(self.jti))
            .build()
    }
}

async fn me(user: AuthUser) -> Json<WhoAmI> {
    Json(WhoAmI {
        subject: user.subject().to_owned(),
        provider: user.provider().map(str::to_owned),
    })
}

async fn assurance(user: AuthUser) -> Json<Vec<String>> {
    Json(user.authentication().methods().to_vec())
}

async fn scoped_reports(permit: Permit<ReadReports>) -> Json<String> {
    Json(permit.user().subject().to_owned())
}

async fn optional_reports(permit: Option<Permit<ReadReports>>) -> Json<bool> {
    Json(permit.is_some())
}

async fn invalid_scope_rule(_permit: Permit<InvalidRule>) -> Json<bool> {
    Json(true)
}

fn scope_bundle() -> bundles::Bundle {
    bundles::bundle([
        bundles::route(
            scoped_reports,
            RouteConf {
                name: "scoped_reports".into(),
                methods: Methods::GET,
                path: "/scoped-reports".into(),
                trim: true,
            },
        ),
        bundles::route(
            optional_reports,
            RouteConf {
                name: "optional_reports".into(),
                methods: Methods::GET,
                path: "/optional-reports".into(),
                trim: true,
            },
        ),
    ])
    .with_conf(bundles::conf().audience(REPORTS))
}

async fn basic_exchange(
    site: Site,
    credentials: BasicCredentials,
) -> Result<LoginResponse, vyuh::Error> {
    Ok(site
        .auth()
        .using(DEFAULT_AUTH_PROVIDER)
        .via(BASIC)
        .login(credentials, &[REPORTS])
        .await?)
}

fn config() -> SiteConf {
    SiteConf::default()
        .secret_key("auth-test-secret-minimum-32-chars")
        .log_init(false)
        .auth(AuthConf::development())
}

fn configured_token_auth(access: TokenConf, refresh: TokenConf) -> AuthConf {
    AuthConf::default().provider(
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
            trim: true,
        },
    )])
    .with_conf(bundles::conf().audience(REPORTS))
}

fn bundle_without_audience() -> bundles::Bundle {
    bundles::bundle([bundles::route(
        me,
        RouteConf {
            name: "me_default".into(),
            methods: Methods::GET,
            path: "/me-default".into(),
            trim: true,
        },
    )])
}

fn dual_audience_bundle() -> bundles::Bundle {
    let admin = bundles::bundle([bundles::route(
        me,
        RouteConf {
            name: "admin_me".into(),
            methods: Methods::GET,
            path: "/admin-me".into(),
            trim: true,
        },
    )])
    .with_conf(bundles::conf().audience(ADMIN));
    bundle().merge(admin)
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

struct CurrentScopeVerifier;

impl TokenVerifier for CurrentScopeVerifier {
    async fn verify(&self, token: &AuthToken) -> Result<AuthUser, AuthError> {
        Ok(AuthUser::new(token.subject()).with_scope(REPORTS_WRITE))
    }
}

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
struct UnavailableVerifier;

impl TokenVerifier for UnavailableVerifier {
    async fn verify(&self, _token: &AuthToken) -> Result<AuthUser, AuthError> {
        Err(AuthError::ProviderUnavailable)
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

#[cfg(feature = "federated")]
#[derive(Clone, Copy)]
struct TestOidcMapper;

#[cfg(feature = "federated")]
impl FederatedUserMapper for TestOidcMapper {
    async fn map(&self, identity: &FederatedIdentity) -> Result<AuthUser, AuthError> {
        Ok(AuthUser::new(identity.subject()))
    }
}

/// Verifies scope permits authorize exact grants and retain deterministic metadata.
#[tokio::test]
async fn scope_permits_enforce_exact_grants() -> Result<(), AuthError> {
    let site = Site::build(config(), scope_bundle())
        .await
        .map_err(auth_error)?;
    let login = site
        .auth()
        .using(DEFAULT_AUTH_PROVIDER)
        .issue(
            AuthUser::new("scoped-user").with_scope(REPORTS_READ),
            &[REPORTS],
        )
        .await?;
    let authorization = format!("Bearer {}", login.credentials().access());
    let operation = site
        .operations()
        .list()
        .find(|operation| operation.name == "scoped_reports")
        .ok_or(AuthError::InvalidCredential)?;
    let metadata = serde_json::to_string(operation).map_err(auth_error)?;
    let read = metadata.find("reports:read");
    let write = metadata.find("reports:write");
    assert!(matches!((read, write), (Some(left), Some(right)) if left < right));
    TestSite::new(site)
        .get("/scoped-reports")
        .header("authorization", &authorization)
        .send()
        .await
        .assert_status(vyuh::routes::StatusCode::OK);
    Ok(())
}

/// Verifies optional permits distinguish absent credentials from forbidden identities.
#[tokio::test]
async fn optional_permits_only_hide_absence() -> Result<(), AuthError> {
    let site = Site::build(config(), scope_bundle())
        .await
        .map_err(auth_error)?;
    let login = site
        .auth()
        .using(DEFAULT_AUTH_PROVIDER)
        .issue(AuthUser::new("unscoped-user"), &[REPORTS])
        .await?;
    let authorization = format!("Bearer {}", login.credentials().access());
    let client = TestSite::new(site);
    client
        .get("/optional-reports")
        .send()
        .await
        .assert_json(vyuh::routes::StatusCode::OK, &serde_json::json!(false))
        .await;
    client
        .get("/optional-reports")
        .header("authorization", &authorization)
        .send()
        .await
        .assert_status(vyuh::routes::StatusCode::FORBIDDEN);
    Ok(())
}

/// Multiple configured access credentials are rejected before either provider runs.
#[tokio::test]
async fn conflicting_access_credentials_are_rejected() -> Result<(), AuthError> {
    let auth =
        AuthConf::development().provider(API_KEY, AuthKey::header("x-api-key", StaticKeyVerifier));
    let site = Site::build(config().auth(auth), bundle())
        .await
        .map_err(auth_error)?;
    let login = site
        .auth()
        .using(DEFAULT_AUTH_PROVIDER)
        .issue(AuthUser::new("token-user"), &[REPORTS])
        .await?;
    TestSite::new(site)
        .get("/me")
        .header(
            "authorization",
            &format!("Bearer {}", login.credentials().access()),
        )
        .header("x-api-key", "service-secret")
        .send()
        .await
        .assert_status(vyuh::routes::StatusCode::UNAUTHORIZED);
    Ok(())
}

/// Verifies malformed static scope rules fail before a site can serve requests.
#[tokio::test]
async fn invalid_scope_rules_fail_site_build() -> Result<(), AuthError> {
    let bundle = bundles::bundle([bundles::route(
        invalid_scope_rule,
        RouteConf {
            name: "invalid_scope_rule".into(),
            methods: Methods::GET,
            path: "/invalid-scope-rule".into(),
            trim: true,
        },
    )])
    .with_conf(bundles::conf().audience(REPORTS));
    let error = Site::build(config(), bundle)
        .await
        .err()
        .ok_or(AuthError::InvalidCredential)?;
    assert!(error.to_string().contains("scope rule"));
    Ok(())
}

/// Verifies infallible identity builders defer malformed scope rejection to login.
#[tokio::test]
async fn manual_login_rejects_invalid_identity_scopes() -> Result<(), AuthError> {
    let site = Site::build(config(), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let error = site
        .auth()
        .using(DEFAULT_AUTH_PROVIDER)
        .issue(
            AuthUser::new("invalid-user").with_scope(Scope::of("invalid scope")),
            &[REPORTS],
        )
        .await
        .err()
        .ok_or(AuthError::InvalidCredential)?;
    assert!(matches!(error, AuthError::InvalidCredential));
    Ok(())
}

/// Verifies refresh replacements carry the current scopes returned by the verifier.
#[tokio::test]
async fn refresh_uses_verifier_scopes_for_replacement_tokens() -> Result<(), AuthError> {
    let provider = TokenProvider::new(Jwt::hs256_site_secret())
        .access(TokenConf::bearer())
        .refresh(TokenConf::bearer())
        .verifier(CurrentScopeVerifier);
    let auth = AuthConf::default().provider(DEFAULT_AUTH_PROVIDER, provider);
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let login = site
        .auth()
        .using(DEFAULT_AUTH_PROVIDER)
        .issue(
            AuthUser::new("refresh-user").with_scope(REPORTS_READ),
            &[REPORTS],
        )
        .await?;
    let refresh = login
        .credentials()
        .refresh()
        .ok_or(AuthError::UnsupportedProviderCapability)?;
    let parts = bearer_parts(refresh)?;
    let replacement = site
        .auth()
        .using(DEFAULT_AUTH_PROVIDER)
        .refresh(&parts)
        .await?;
    let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256);
    validation.validate_aud = false;
    let decoded = jsonwebtoken::decode::<AuthToken>(
        replacement.credentials().access(),
        &jsonwebtoken::DecodingKey::from_secret(b"auth-test-secret-minimum-32-chars"),
        &validation,
    )
    .map_err(auth_error)?;

    assert_eq!(decoded.claims.scopes(), &[REPORTS_WRITE]);
    Ok(())
}

/// Verifies authenticated external JWT claims normalize through a provider-bound builder.
#[tokio::test]
async fn custom_jwt_claims_authenticate_as_normal_users() -> Result<(), AuthError> {
    let auth = AuthConf::default().provider(
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
            &serde_json::json!({ "subject": "partner-user", "provider": "external" }),
        )
        .await;
    Ok(())
}

/// Verifies typed claims cannot use a refresh credential to access a protected operation.
#[tokio::test]
async fn custom_jwt_claims_reject_refresh_tokens_for_access() -> Result<(), AuthError> {
    let auth = AuthConf::default().provider(
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
    let auth = AuthConf::default().provider(
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
        .issue(AuthUser::new("partner-user"), &[REPORTS])
        .await
        .err()
        .ok_or(AuthError::InvalidCredential)?;
    assert!(matches!(error, AuthError::UnsupportedProviderCapability));
    Ok(())
}

/// Verifies a custom-claims provider rejects an attempted refresh configuration at build time.
#[tokio::test]
async fn custom_jwt_claims_provider_rejects_refresh_configuration() -> Result<(), AuthError> {
    let auth = AuthConf::default().provider(
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
    let auth = AuthConf::development().method(PASSWORD, PasswordLogin::new(TestPasswords));
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let login = site
        .auth()
        .using(DEFAULT_AUTH_PROVIDER)
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

/// Verifies test-site login can authenticate a request through the bearer selector.
#[tokio::test]
async fn test_site_login_authenticates_a_bearer_request() -> Result<(), AuthError> {
    let site = Site::build(config(), bundle()).await.map_err(auth_error)?;
    let test = TestSite::new(site);
    let login = test
        .login(
            DEFAULT_AUTH_PROVIDER,
            AuthUser::new("test-user"),
            &[REPORTS],
        )
        .await?;
    test.get("/me")
        .with_login(&login)
        .send()
        .await
        .assert_status(vyuh::routes::StatusCode::OK);
    Ok(())
}

/// Verifies test-site login includes cookie and CSRF credentials for unsafe requests.
#[tokio::test]
async fn test_site_login_authenticates_a_cookie_request() -> Result<(), AuthError> {
    let auth = configured_token_auth(TokenConf::cookie("access"), TokenConf::cookie("refresh"));
    let site = Site::build(config().auth(auth), bundle())
        .await
        .map_err(auth_error)?;
    let test = TestSite::new(site);
    let login = test
        .login(
            DEFAULT_AUTH_PROVIDER,
            AuthUser::new("cookie-user"),
            &[REPORTS],
        )
        .await?;
    test.post("/me")
        .with_login(&login)
        .send()
        .await
        .assert_status(vyuh::routes::StatusCode::OK);
    Ok(())
}

/// Verifies credential-provider and login-method selection compose in one chain.
#[tokio::test]
async fn password_login_uses_explicit_credential_provider() -> Result<(), AuthError> {
    let provider = TokenProvider::new(Jwt::hs256_site_secret())
        .access(TokenConf::header("x-login-access"))
        .refresh(TokenConf::header("x-login-refresh"));
    let auth = AuthConf::default()
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

    let auth = AuthConf::development().method(PASSWORD, PasswordLogin::new(TestPasswords));
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let error = site
        .auth()
        .using(DEFAULT_AUTH_PROVIDER)
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
    let auth = AuthConf::development()
        .method(PASSWORD, PasswordLogin::new(TestPasswords))
        .method(PASSWORD, PasswordLogin::new(TestPasswords));
    let error = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .err()
        .ok_or_else(|| AuthError::Internal("duplicate login method was accepted".into()))?;
    assert!(matches!(&error, vyuh::SiteError::AuthBuildError(_)));
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
            trim: true,
        },
    );
    let auth = AuthConf::development().method(BASIC, BasicLogin::new(TestPasswords));
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
    let auth = AuthConf::development()
        .method(PASSWORD_MFA, method)
        .login_state_store(ReplayStore::default());
    let route = bundles::route(
        assurance,
        RouteConf {
            name: "assurance".into(),
            methods: Methods::GET,
            path: "/assurance".into(),
            trim: true,
        },
    );
    let site = Site::build(
        config().auth(auth),
        bundles::bundle([route]).with_conf(bundles::conf().audience(REPORTS)),
    )
    .await
    .map_err(auth_error)?;
    let selected = site.auth().using(DEFAULT_AUTH_PROVIDER).via(PASSWORD_MFA);
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
    let auth = AuthConf::development()
        .method(BASIC_MFA, method)
        .login_state_store(ReplayStore::default());
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let challenge = site
        .auth()
        .using(DEFAULT_AUTH_PROVIDER)
        .via(BASIC_MFA)
        .begin(
            BasicCredentials::new("user@example.com", "correct-password"),
            &[REPORTS],
        )
        .await?;
    let token = challenge.token().ok_or(AuthError::InvalidLoginState)?;
    let login = site
        .auth()
        .using(DEFAULT_AUTH_PROVIDER)
        .via(BASIC_MFA)
        .complete(MfaResponse::totp(token, "123456"))
        .await?;
    assert!(login.credentials().access().len() > 32);
    Ok(())
}

/// Verifies atomic login-state consumption permits exactly one concurrent MFA completion.
#[tokio::test]
async fn mfa_state_store_rejects_replay() -> Result<(), AuthError> {
    let method = PasswordLogin::new(TestPasswords).then(MfaLogin::new(TestFactors).totp());
    let auth = AuthConf::development()
        .method(PASSWORD_MFA, method)
        .login_state_store(ReplayStore::default());
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let selected = site.auth().using(DEFAULT_AUTH_PROVIDER).via(PASSWORD_MFA);
    let challenge = selected
        .begin(
            PasswordCredentials::new("user@example.com", "correct-password"),
            &[REPORTS],
        )
        .await?;
    let token = challenge.token().ok_or(AuthError::InvalidLoginState)?;
    let first = selected.complete(MfaResponse::totp(token, "123456"));
    let second = selected.complete(MfaResponse::totp(token, "123456"));
    let (first, second) = tokio::join!(first, second);
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    assert!(
        first
            .err()
            .or_else(|| second.err())
            .is_some_and(|error| matches!(error, AuthError::InvalidLoginState))
    );
    Ok(())
}

/// Verifies completion cannot switch away from the credential provider bound at begin.
#[tokio::test]
async fn mfa_completion_cannot_switch_credential_provider() -> Result<(), AuthError> {
    let provider = TokenProvider::new(Jwt::hs256_site_secret())
        .access(TokenConf::header("x-mfa-access"))
        .refresh(TokenConf::header("x-mfa-refresh"));
    let method = PasswordLogin::new(TestPasswords).then(MfaLogin::new(TestFactors).totp());
    let auth = AuthConf::development()
        .provider(ALTERNATE, provider)
        .method(PASSWORD_MFA, method)
        .login_state_store(ReplayStore::default());
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
        .using(DEFAULT_AUTH_PROVIDER)
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
    let auth = AuthConf::development()
        .method(
            PASSWORD_MFA,
            PasswordLogin::new(TestPasswords).then(MfaLogin::new(TestFactors).totp()),
        )
        .login_state_store(ReplayStore::default());
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
        .using(DEFAULT_AUTH_PROVIDER)
        .via(PASSWORD_MFA)
        .begin(
            PasswordCredentials::new("user@example.com", "correct-password"),
            &[REPORTS],
        )
        .await?;
    let token = challenge.token().ok_or(AuthError::InvalidLoginState)?;
    let rotated_auth = AuthConf::development()
        .method(
            PASSWORD_MFA,
            PasswordLogin::new(TestPasswords).then(MfaLogin::new(TestFactors).totp()),
        )
        .login_state_store(ReplayStore::default());
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
        .using(DEFAULT_AUTH_PROVIDER)
        .via(PASSWORD_MFA)
        .complete(MfaResponse::totp(token, "123456"))
        .await?;
    assert!(!login.credentials().access().is_empty());
    Ok(())
}

/// Verifies OIDC begin performs discovery and emits state, nonce, and PKCE parameters.
#[cfg(feature = "federated")]
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
    let login = FederatedLogin::oidc(&issuer)
        .client_id("test-client")
        .client_secret(vyuh::auth::KeySource::inline("test-secret"))
        .redirect_uri(format!("{issuer}/callback"))
        .scopes(["email", "profile"])
        .mapper(TestOidcMapper);
    let auth = AuthConf::development()
        .method(OIDC, login)
        .login_state_store(ReplayStore::default());
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let challenge = site
        .auth()
        .using(DEFAULT_AUTH_PROVIDER)
        .via(OIDC)
        .begin(FederatedStart::new().return_to("/dashboard"), &[REPORTS])
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
#[cfg(feature = "federated")]
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
    let login = FederatedLogin::oidc(&issuer)
        .client_id("test-client")
        .client_secret(vyuh::auth::KeySource::inline("test-secret"))
        .redirect_uri(format!("{issuer}/callback"))
        .mapper(TestOidcMapper);
    let auth = AuthConf::development()
        .method(OIDC, login)
        .login_state_store(ReplayStore::default());
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let selected = site.auth().using(DEFAULT_AUTH_PROVIDER).via(OIDC);
    let challenge = selected.begin(FederatedStart::new(), &[REPORTS]).await?;
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
    let callback = serde_json::from_value::<vyuh::auth::FederatedCallback>(serde_json::json!({
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
#[cfg(feature = "federated")]
#[tokio::test]
async fn oidc_rejects_insecure_remote_endpoints() -> Result<(), AuthError> {
    let login = FederatedLogin::oidc("http://accounts.example.com")
        .client_id("test-client")
        .redirect_uri("http://app.example.com/callback")
        .mapper(TestOidcMapper);
    let auth = AuthConf::development()
        .method(OIDC, login)
        .login_state_store(ReplayStore::default());
    let error = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .err()
        .ok_or_else(|| AuthError::Internal("insecure OIDC config was accepted".into()))?;
    assert!(error.to_string().contains("must use HTTPS"));
    Ok(())
}

/// Verifies OIDC discovery transport failures are reported as provider outages.
#[cfg(feature = "federated")]
#[tokio::test]
async fn oidc_discovery_outage_is_provider_unavailable() -> Result<(), AuthError> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(auth_error)?;
    let issuer = format!("http://{}", listener.local_addr().map_err(auth_error)?);
    drop(listener);
    let login = FederatedLogin::oidc(&issuer)
        .client_id("test-client")
        .redirect_uri(format!("{issuer}/callback"))
        .mapper(TestOidcMapper);
    let auth = AuthConf::development()
        .method(OIDC, login)
        .login_state_store(ReplayStore::default());
    let error = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .err()
        .ok_or_else(|| AuthError::Internal("OIDC discovery unexpectedly succeeded".into()))?;
    assert!(matches!(
        &error,
        vyuh::SiteError::AuthBuildError(vyuh::auth::AuthBuildError::ProviderInitialization(_))
    ));
    assert!(error.to_string().contains("provider initialization failed"));
    Ok(())
}

#[cfg(feature = "federated")]
#[derive(Clone)]
struct MockOidcState {
    issuer: String,
    nonce: Arc<Mutex<Option<String>>>,
}

#[cfg(feature = "federated")]
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

#[cfg(feature = "federated")]
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
