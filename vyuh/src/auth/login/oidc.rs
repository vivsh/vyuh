//! OpenID Connect Authorization Code login with PKCE and sealed state.

use std::{
    future::Future,
    sync::{Arc, OnceLock},
    time::{Duration as StdDuration, Instant},
};

use chrono::{Duration, Utc};
use futures::future::BoxFuture;
use openidconnect::{
    AccessTokenHash, AuthorizationCode, ClientId, ClientSecret, CsrfToken, IssuerUrl, Nonce,
    OAuth2TokenResponse, PkceCodeChallenge, PkceCodeVerifier, RedirectUrl, Scope,
    TokenResponse as OidcTokenResponse,
    core::{CoreAuthenticationFlow, CoreClient, CoreProviderMetadata},
    reqwest,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    BoxLoginInput, ChallengeCodec, ErasedLoginRuntime, LoginChallenge, LoginCompletion,
    LoginMethodId, LoginProviderDefinition, LoginProviderKind, LoginTarget, SealedLoginState,
    VerifiedLogin,
    runtime::{CompletedLogin, LoginFuture, completion_sealed},
};
use crate::auth::{AuthError, AuthUser, KeySource, LoginStateStoreRuntime, SecretRing};

/// Input used to start an OIDC authorization redirect.
#[derive(Clone, Default)]
pub struct OidcStart {
    return_to: Option<String>,
}

impl OidcStart {
    /// Creates an OIDC start request without an application return path.
    pub fn new() -> Self {
        Self::default()
    }

    /// Carries one safe application-relative return path through the OIDC flow.
    pub fn return_to(mut self, value: impl Into<String>) -> Self {
        self.return_to = Some(value.into());
        self
    }
}

/// Query parameters returned by an OIDC authorization endpoint.
#[derive(Deserialize, JsonSchema)]
pub struct OidcCallback {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
}

impl completion_sealed::Sealed for OidcCallback {}
impl LoginCompletion for OidcCallback {}

/// Protocol-verified OIDC identity supplied to application account mapping.
pub struct OidcIdentity {
    subject: String,
    issuer: String,
    email: Option<String>,
    email_verified: Option<bool>,
    name: Option<String>,
    return_to: Option<String>,
    claims: serde_json::Value,
}

impl OidcIdentity {
    /// Returns the provider-local stable subject.
    pub fn subject(&self) -> &str {
        &self.subject
    }

    /// Returns the cryptographically verified issuer.
    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    /// Returns the asserted email address when present.
    pub fn email(&self) -> Option<&str> {
        self.email.as_deref()
    }

    /// Returns whether the provider asserted email verification.
    pub const fn email_verified(&self) -> Option<bool> {
        self.email_verified
    }

    /// Returns the display name when present.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Returns the application-relative path carried from `OidcStart`.
    pub fn return_to(&self) -> Option<&str> {
        self.return_to.as_deref()
    }

    /// Returns the complete protocol-verified ID-token claims.
    pub fn claims(&self) -> &serde_json::Value {
        &self.claims
    }
}

/// Maps a protocol-verified OIDC identity to an application user.
pub trait OidcUserMapper: Send + Sync + 'static {
    /// Links or resolves the external identity after all OIDC checks pass.
    fn map<'a>(
        &'a self,
        identity: &'a OidcIdentity,
    ) -> impl Future<Output = Result<AuthUser, AuthError>> + Send + 'a;
}

trait ErasedOidcMapper: Send + Sync {
    fn map<'a>(&'a self, identity: &'a OidcIdentity) -> BoxFuture<'a, Result<AuthUser, AuthError>>;
}

impl<T: OidcUserMapper> ErasedOidcMapper for T {
    fn map<'a>(&'a self, identity: &'a OidcIdentity) -> BoxFuture<'a, Result<AuthUser, AuthError>> {
        Box::pin(OidcUserMapper::map(self, identity))
    }
}

/// Configures one OIDC Authorization Code login method.
#[derive(Clone)]
pub struct OidcLogin {
    issuer: String,
    client_id: String,
    client_secret: Option<KeySource>,
    redirect_uri: String,
    scopes: Vec<String>,
    mapper: Option<Arc<dyn ErasedOidcMapper>>,
    ttl_seconds: i64,
}

impl OidcLogin {
    /// Creates a provider using OIDC discovery at the given issuer.
    pub fn discovery(issuer: impl Into<String>) -> Self {
        Self {
            issuer: issuer.into(),
            client_id: String::new(),
            client_secret: None,
            redirect_uri: String::new(),
            scopes: Vec::new(),
            mapper: None,
            ttl_seconds: Duration::minutes(5).num_seconds(),
        }
    }

    /// Sets the registered OIDC client identifier.
    pub fn client_id(mut self, value: impl Into<String>) -> Self {
        self.client_id = value.into();
        self
    }

    /// Sets a redacted client-secret source for a confidential client.
    pub fn client_secret(mut self, value: KeySource) -> Self {
        self.client_secret = Some(value);
        self
    }

    /// Sets the exact callback URI registered with the provider.
    pub fn redirect_uri(mut self, value: impl Into<String>) -> Self {
        self.redirect_uri = value.into();
        self
    }

    /// Adds requested scopes; `openid` is always requested.
    pub fn scopes<I, S>(mut self, values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.scopes = values.into_iter().map(Into::into).collect();
        self
    }

    /// Sets application account linking after OIDC protocol verification.
    pub fn mapper(mut self, value: impl OidcUserMapper) -> Self {
        self.mapper = Some(Arc::new(value));
        self
    }

    /// Sets the bounded lifetime of an unfinished OIDC flow.
    pub fn ttl(mut self, value: Duration) -> Self {
        self.ttl_seconds = value.num_seconds();
        self
    }
}

struct OidcRuntime {
    conf: OidcLogin,
    metadata: tokio::sync::RwLock<Option<CachedMetadata>>,
    http: Result<reqwest::Client, ()>,
    client_secret: OnceLock<Option<String>>,
}

struct CachedMetadata {
    value: CoreProviderMetadata,
    loaded_at: Instant,
}

#[derive(Serialize, Deserialize)]
struct PendingOidc {
    nonce: String,
    pkce_verifier: String,
    return_to: Option<String>,
}

impl ErasedLoginRuntime for OidcRuntime {
    fn is_flow(&self) -> bool {
        true
    }

    fn validate(&self) -> Result<(), AuthError> {
        self.validate_conf()
    }

    fn prepare(&self, secrets: &SecretRing) -> Result<(), AuthError> {
        let value = self.resolve_client_secret(secrets)?;
        self.client_secret
            .set(value)
            .map_err(|_| AuthError::InvalidProviderConfig("OIDC login prepared twice".into()))
    }

    fn begin<'a>(
        &'a self,
        method: &'a LoginMethodId,
        input: BoxLoginInput,
        target: LoginTarget,
        codec: &'a ChallengeCodec,
        _secrets: &'a SecretRing,
    ) -> LoginFuture<'a, LoginChallenge> {
        Box::pin(async move { self.begin_inner(method, input, target, codec).await })
    }

    fn complete<'a>(
        &'a self,
        method: &'a LoginMethodId,
        input: BoxLoginInput,
        codec: &'a ChallengeCodec,
        _secrets: &'a SecretRing,
        state_store: Option<&'a LoginStateStoreRuntime>,
    ) -> LoginFuture<'a, CompletedLogin> {
        Box::pin(async move { self.complete_inner(method, input, codec, state_store).await })
    }
}

impl OidcRuntime {
    async fn begin_inner(
        &self,
        method: &LoginMethodId,
        input: BoxLoginInput,
        target: LoginTarget,
        codec: &ChallengeCodec,
    ) -> Result<LoginChallenge, AuthError> {
        self.validate_conf()?;
        let input = input
            .downcast::<OidcStart>()
            .map_err(|_| AuthError::InvalidCredential)?;
        validate_return_to(input.return_to.as_deref())?;
        let client = self.client(false).await?;
        let nonce = Nonce::new_random();
        let (challenge, verifier) = PkceCodeChallenge::new_random_sha256();
        let pending = PendingOidc {
            nonce: nonce.secret().to_owned(),
            pkce_verifier: verifier.secret().to_owned(),
            return_to: input.return_to.clone(),
        };
        let state = self.seal_state(method, target, pending, codec)?;
        let url = authorization_url(&client, &self.conf.scopes, challenge, nonce, state);
        Ok(LoginChallenge::redirect(url, self.conf.ttl_seconds))
    }

    async fn complete_inner(
        &self,
        method: &LoginMethodId,
        input: BoxLoginInput,
        codec: &ChallengeCodec,
        state_store: Option<&LoginStateStoreRuntime>,
    ) -> Result<CompletedLogin, AuthError> {
        let callback = input
            .downcast::<OidcCallback>()
            .map_err(|_| AuthError::InvalidCredential)?;
        let (code, state_token) = callback_values(&callback)?;
        let state: SealedLoginState = codec.open(state_token)?;
        validate_state(&state, method)?;
        let pending: PendingOidc = serde_json::from_value(state.payload.clone())
            .map_err(|_| AuthError::InvalidLoginState)?;
        let client = self.client(false).await?;
        let identity = self.exchange(&client, code, pending).await?;
        let mapper = self.mapper()?;
        let user = mapper.map(&identity).await?;
        if let Some(store) = state_store {
            store.consume(&state).await?;
        }
        let login = verified_oidc(user, &identity.claims);
        Ok(CompletedLogin {
            login,
            target: state.target,
        })
    }

    async fn metadata(&self, force: bool) -> Result<CoreProviderMetadata, AuthError> {
        if !force && let Some(value) = self.cached_metadata().await {
            return Ok(value);
        }
        let mut cache = self.metadata.write().await;
        if !force && let Some(value) = fresh_metadata(cache.as_ref()) {
            return Ok(value);
        }
        match self.discover().await {
            Ok(value) => {
                *cache = Some(CachedMetadata {
                    value: value.clone(),
                    loaded_at: Instant::now(),
                });
                Ok(value)
            }
            Err(error) => cache.as_ref().map(|value| value.value.clone()).ok_or(error),
        }
    }

    async fn client(&self, force_metadata: bool) -> Result<ConfiguredClient, AuthError> {
        let metadata = self.metadata(force_metadata).await?;
        let secret = self.client_secret.get().cloned().flatten();
        let client = CoreClient::from_provider_metadata(
            metadata,
            ClientId::new(self.conf.client_id.clone()),
            secret.map(ClientSecret::new),
        )
        .set_redirect_uri(
            RedirectUrl::new(self.conf.redirect_uri.clone()).map_err(|_| {
                AuthError::InvalidProviderConfig("invalid OIDC redirect URI".into())
            })?,
        );
        Ok(client)
    }

    async fn exchange(
        &self,
        client: &ConfiguredClient,
        code: &str,
        pending: PendingOidc,
    ) -> Result<OidcIdentity, AuthError> {
        let response = client
            .exchange_code(AuthorizationCode::new(code.to_owned()))
            .map_err(|_| AuthError::InvalidCredential)?
            .set_pkce_verifier(PkceCodeVerifier::new(pending.pkce_verifier))
            .request_async(self.http()?)
            .await
            .map_err(|_| AuthError::ProviderUnavailable)?;
        let id_token = response.id_token().ok_or(AuthError::InvalidCredential)?;
        let verifier = client.id_token_verifier();
        let nonce = Nonce::new(pending.nonce);
        if let Ok(claims) = id_token.claims(&verifier, &nonce) {
            verify_access_hash(&response, id_token, claims, &verifier)?;
            return identity_from_claims(claims, pending.return_to);
        }
        let refreshed = self.client(true).await?;
        let verifier = refreshed.id_token_verifier();
        let claims = id_token
            .claims(&verifier, &nonce)
            .map_err(|_| AuthError::InvalidCredential)?;
        verify_access_hash(&response, id_token, claims, &verifier)?;
        identity_from_claims(claims, pending.return_to)
    }

    fn seal_state(
        &self,
        method: &LoginMethodId,
        target: LoginTarget,
        pending: PendingOidc,
        codec: &ChallengeCodec,
    ) -> Result<String, AuthError> {
        let state = SealedLoginState {
            version: 1,
            state_id: uuid::Uuid::new_v4().to_string(),
            method: method.as_str().into(),
            target,
            expires_at: Utc::now().timestamp() + self.conf.ttl_seconds,
            payload: serde_json::to_value(pending).map_err(|_| AuthError::InvalidLoginState)?,
        };
        codec.seal(&state)
    }

    fn resolve_client_secret(&self, secrets: &SecretRing) -> Result<Option<String>, AuthError> {
        self.conf
            .client_secret
            .as_ref()
            .map(|source| {
                secrets.active(source).and_then(|value| {
                    String::from_utf8(value).map_err(|_| AuthError::InvalidCredential)
                })
            })
            .transpose()
    }

    fn mapper(&self) -> Result<&Arc<dyn ErasedOidcMapper>, AuthError> {
        self.conf.mapper.as_ref().ok_or_else(|| {
            AuthError::InvalidProviderConfig("OIDC account mapper is required".into())
        })
    }

    fn http(&self) -> Result<&reqwest::Client, AuthError> {
        self.http.as_ref().map_err(|_| {
            AuthError::InvalidProviderConfig("unable to build secure OIDC HTTP client".into())
        })
    }

    fn validate_conf(&self) -> Result<(), AuthError> {
        if self.conf.client_id.is_empty()
            || self.conf.client_id.len() > 512
            || self.conf.issuer.len() > 2048
            || self.conf.redirect_uri.len() > 2048
            || self.conf.redirect_uri.is_empty()
            || !(1..=900).contains(&self.conf.ttl_seconds)
        {
            return Err(AuthError::InvalidProviderConfig(
                "OIDC requires client ID, redirect URI, and a TTL between 1 and 900 seconds".into(),
            ));
        }
        validate_secure_url(&self.conf.issuer, "issuer")?;
        validate_secure_url(&self.conf.redirect_uri, "redirect URI")?;
        if self.conf.scopes.len() > 32
            || self
                .conf
                .scopes
                .iter()
                .any(|scope| scope.trim().is_empty() || scope.len() > 128)
        {
            return Err(AuthError::InvalidProviderConfig(
                "OIDC scopes must not be empty".into(),
            ));
        }
        self.http()?;
        self.mapper().map(|_| ())
    }

    async fn cached_metadata(&self) -> Option<CoreProviderMetadata> {
        fresh_metadata(self.metadata.read().await.as_ref())
    }

    async fn discover(&self) -> Result<CoreProviderMetadata, AuthError> {
        let issuer = IssuerUrl::new(self.conf.issuer.clone())
            .map_err(|_| AuthError::InvalidProviderConfig("invalid OIDC issuer".into()))?;
        CoreProviderMetadata::discover_async(issuer, self.http()?)
            .await
            .map_err(|_| AuthError::ProviderUnavailable)
    }
}

type ConfiguredClient = openidconnect::core::CoreClient<
    openidconnect::EndpointSet,
    openidconnect::EndpointNotSet,
    openidconnect::EndpointNotSet,
    openidconnect::EndpointNotSet,
    openidconnect::EndpointMaybeSet,
    openidconnect::EndpointMaybeSet,
>;

impl LoginProviderDefinition<OidcStart, OidcCallback> for OidcLogin {
    fn define(self) -> LoginProviderKind {
        let http = reqwest::ClientBuilder::new()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(StdDuration::from_secs(5))
            .timeout(StdDuration::from_secs(10))
            .build()
            .map_err(|_| ());
        LoginProviderKind {
            runtime: Arc::new(OidcRuntime {
                conf: self,
                metadata: tokio::sync::RwLock::new(None),
                http,
                client_secret: OnceLock::new(),
            }),
        }
    }
}

fn fresh_metadata(value: Option<&CachedMetadata>) -> Option<CoreProviderMetadata> {
    value
        .filter(|value| value.loaded_at.elapsed() < StdDuration::from_secs(3600))
        .map(|value| value.value.clone())
}

fn authorization_url(
    client: &ConfiguredClient,
    scopes: &[String],
    challenge: PkceCodeChallenge,
    nonce: Nonce,
    state: String,
) -> String {
    let mut request = client.authorize_url(
        CoreAuthenticationFlow::AuthorizationCode,
        move || CsrfToken::new(state),
        move || nonce,
    );
    for scope in scopes {
        request = request.add_scope(Scope::new(scope.clone()));
    }
    request.set_pkce_challenge(challenge).url().0.to_string()
}

fn callback_values(callback: &OidcCallback) -> Result<(&str, &str), AuthError> {
    if callback.error.is_some() {
        return Err(AuthError::InvalidCredential);
    }
    let code = callback
        .code
        .as_deref()
        .ok_or(AuthError::InvalidCredential)?;
    let state = callback
        .state
        .as_deref()
        .ok_or(AuthError::InvalidLoginState)?;
    if code.is_empty() || code.len() > 8192 || state.is_empty() || state.len() > 16 * 1024 {
        return Err(AuthError::InvalidCredential);
    }
    Ok((code, state))
}

fn validate_state(state: &SealedLoginState, method: &LoginMethodId) -> Result<(), AuthError> {
    if state.version != 1 || state.method != method.as_str() {
        return Err(AuthError::InvalidLoginState);
    }
    if state.expires_at <= Utc::now().timestamp() {
        return Err(AuthError::ExpiredLoginState);
    }
    Ok(())
}

fn validate_return_to(value: Option<&str>) -> Result<(), AuthError> {
    if value
        .is_some_and(|path| path.len() > 2048 || !path.starts_with('/') || path.starts_with("//"))
    {
        return Err(AuthError::InvalidLoginState);
    }
    Ok(())
}

fn validate_secure_url(value: &str, label: &str) -> Result<(), AuthError> {
    let url = url::Url::parse(value)
        .map_err(|_| AuthError::InvalidProviderConfig(format!("invalid OIDC {label}")))?;
    let loopback = url.host_str().is_some_and(|host| {
        host == "localhost"
            || host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    if url.scheme() != "https" && !(url.scheme() == "http" && loopback) {
        return Err(AuthError::InvalidProviderConfig(format!(
            "OIDC {label} must use HTTPS except on loopback"
        )));
    }
    Ok(())
}

fn identity_from_claims(
    claims: &openidconnect::core::CoreIdTokenClaims,
    return_to: Option<String>,
) -> Result<OidcIdentity, AuthError> {
    let value = serde_json::to_value(claims).map_err(|_| AuthError::InvalidCredential)?;
    if serde_json::to_vec(&value)
        .map_err(|_| AuthError::InvalidCredential)?
        .len()
        > 16 * 1024
    {
        return Err(AuthError::InvalidCredential);
    }
    Ok(OidcIdentity {
        subject: claims.subject().as_str().to_owned(),
        issuer: claims.issuer().as_str().to_owned(),
        email: claims.email().map(|value| value.as_str().to_owned()),
        email_verified: claims.email_verified(),
        name: value
            .get("name")
            .and_then(|item| item.as_str())
            .map(str::to_owned),
        return_to,
        claims: value,
    })
}

fn verified_oidc(user: AuthUser, claims: &serde_json::Value) -> VerifiedLogin {
    let auth_time = claims
        .get("auth_time")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or_else(|| Utc::now().timestamp());
    let mut methods = vec!["oidc".into()];
    if let Some(values) = claims.get("amr").and_then(serde_json::Value::as_array) {
        methods.extend(
            values
                .iter()
                .filter_map(|value| value.as_str().map(str::to_owned)),
        );
    }
    VerifiedLogin {
        user,
        methods,
        auth_time,
        acr: claims
            .get("acr")
            .and_then(|value| value.as_str())
            .map(str::to_owned),
    }
}

fn verify_access_hash(
    response: &openidconnect::core::CoreTokenResponse,
    id_token: &openidconnect::core::CoreIdToken,
    claims: &openidconnect::core::CoreIdTokenClaims,
    verifier: &openidconnect::core::CoreIdTokenVerifier,
) -> Result<(), AuthError> {
    let Some(expected) = claims.access_token_hash() else {
        return Ok(());
    };
    let actual = AccessTokenHash::from_token(
        response.access_token(),
        id_token
            .signing_alg()
            .map_err(|_| AuthError::InvalidCredential)?,
        id_token
            .signing_key(verifier)
            .map_err(|_| AuthError::InvalidCredential)?,
    )
    .map_err(|_| AuthError::InvalidCredential)?;
    if actual != *expected {
        return Err(AuthError::InvalidCredential);
    }
    Ok(())
}
