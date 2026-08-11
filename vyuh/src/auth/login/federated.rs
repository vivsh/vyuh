//! Federated Authorization Code login with provider-specific verification.

mod http;
mod oidc;
mod social;

use std::{
    future::Future,
    sync::{Arc, OnceLock},
};

use chrono::{Duration, Utc};
use futures::future::BoxFuture;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use self::http::FederatedHttpClient;
use super::{
    BoxLoginInput, ChallengeCodec, ErasedLoginRuntime, LoginChallenge, LoginCompletion,
    LoginMethodId, LoginProviderDefinition, LoginRuntimeDefinition, LoginTarget, SealedLoginState,
    VerifiedLogin,
    runtime::{CompletedLogin, LoginFuture, completion_sealed},
};
use crate::auth::{AuthError, AuthUser, KeySource, LoginStateStoreRuntime, SecretRing};

/// Input used to start a federated authorization redirect.
#[derive(Clone, Default)]
pub struct FederatedStart {
    return_to: Option<String>,
}

impl FederatedStart {
    /// Creates a federated start request without an application return path.
    pub fn new() -> Self {
        Self::default()
    }

    /// Carries one safe application-relative return path through the login flow.
    pub fn return_to(mut self, value: impl Into<String>) -> Self {
        self.return_to = Some(value.into());
        self
    }
}

/// Query parameters returned by a federated authorization endpoint.
#[derive(Deserialize, JsonSchema)]
pub struct FederatedCallback {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
}

impl completion_sealed::Sealed for FederatedCallback {}
impl LoginCompletion for FederatedCallback {}

/// A supported federated identity protocol or provider preset.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FederatedProvider {
    /// A generic OpenID Connect provider.
    Oidc,
    /// Google OpenID Connect.
    Google,
    /// GitHub OAuth login.
    GitHub,
    /// Facebook Login.
    Facebook,
}

/// Protocol-verified identity supplied to application account mapping.
pub struct FederatedIdentity {
    provider: FederatedProvider,
    subject: String,
    issuer: String,
    email: Option<String>,
    email_verified: Option<bool>,
    name: Option<String>,
    picture: Option<String>,
    scopes: Vec<String>,
    return_to: Option<String>,
    claims: serde_json::Value,
}

impl FederatedIdentity {
    /// Returns the protocol preset that authenticated this identity.
    pub const fn provider(&self) -> FederatedProvider {
        self.provider
    }

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

    /// Returns the asserted profile image URL when present.
    pub fn picture(&self) -> Option<&str> {
        self.picture.as_deref()
    }

    /// Returns the upstream scopes granted to the login client.
    pub fn scopes(&self) -> &[String] {
        &self.scopes
    }

    /// Returns the application-relative path carried from `FederatedStart`.
    pub fn return_to(&self) -> Option<&str> {
        self.return_to.as_deref()
    }

    /// Returns bounded authenticated protocol claims or profile data.
    pub fn claims(&self) -> &serde_json::Value {
        &self.claims
    }
}

/// Maps a protocol-verified federated identity to an application user.
pub trait FederatedUserMapper: Send + Sync + 'static {
    /// Links or resolves the external identity after protocol checks pass.
    fn map<'a>(
        &'a self,
        identity: &'a FederatedIdentity,
    ) -> impl Future<Output = Result<AuthUser, AuthError>> + Send + 'a;
}

trait ErasedFederatedMapper: Send + Sync {
    fn map<'a>(
        &'a self,
        identity: &'a FederatedIdentity,
    ) -> BoxFuture<'a, Result<AuthUser, AuthError>>;
}

impl<T: FederatedUserMapper> ErasedFederatedMapper for T {
    fn map<'a>(
        &'a self,
        identity: &'a FederatedIdentity,
    ) -> BoxFuture<'a, Result<AuthUser, AuthError>> {
        Box::pin(FederatedUserMapper::map(self, identity))
    }
}

/// Configures one supported federated Authorization Code login method.
#[derive(Clone)]
pub struct FederatedLogin {
    provider: FederatedProvider,
    issuer: String,
    client_id: String,
    client_secret: Option<KeySource>,
    redirect_uri: String,
    scopes: Vec<String>,
    mapper: Option<Arc<dyn ErasedFederatedMapper>>,
    ttl_seconds: i64,
}

impl FederatedLogin {
    /// Creates a generic provider using OIDC discovery at the given issuer.
    pub fn oidc(issuer: impl Into<String>) -> Self {
        Self {
            provider: FederatedProvider::Oidc,
            issuer: issuer.into(),
            client_id: String::new(),
            client_secret: None,
            redirect_uri: String::new(),
            scopes: Vec::new(),
            mapper: None,
            ttl_seconds: Duration::minutes(5).num_seconds(),
        }
    }

    /// Creates the hardened Google OpenID Connect preset.
    pub fn google() -> Self {
        let mut value = Self::oidc("https://accounts.google.com");
        value.provider = FederatedProvider::Google;
        value.scopes = vec!["email".into(), "profile".into()];
        value
    }

    /// Creates the hardened GitHub OAuth login preset.
    pub fn github() -> Self {
        let mut value = Self::oidc("https://github.com");
        value.provider = FederatedProvider::GitHub;
        value.scopes = vec!["read:user".into(), "user:email".into()];
        value
    }

    /// Creates the hardened Facebook Login preset.
    pub fn facebook() -> Self {
        let mut value = Self::oidc("https://www.facebook.com");
        value.provider = FederatedProvider::Facebook;
        value.scopes = vec!["public_profile".into(), "email".into()];
        value
    }

    /// Sets the registered provider client identifier.
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

    /// Replaces the upstream scopes requested from the selected provider.
    pub fn scopes<I, S>(mut self, values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.scopes = values.into_iter().map(Into::into).collect();
        self
    }

    /// Sets application account linking after protocol verification.
    pub fn mapper(mut self, value: impl FederatedUserMapper) -> Self {
        self.mapper = Some(Arc::new(value));
        self
    }

    /// Sets the bounded lifetime of an unfinished login flow.
    pub fn ttl(mut self, value: Duration) -> Self {
        self.ttl_seconds = value.num_seconds();
        self
    }
}

struct FederatedRuntime {
    conf: FederatedLogin,
    oidc_client: arc_swap::ArcSwapOption<oidc::CachedClient>,
    oidc_discovery: tokio::sync::Mutex<oidc::DiscoveryState>,
    oidc_notify: tokio::sync::Notify,
    http: Result<FederatedHttpClient, ()>,
    client_secret: OnceLock<Option<String>>,
}

#[derive(Serialize, Deserialize)]
enum PendingFederated {
    Oidc {
        nonce: String,
        pkce_verifier: String,
        return_to: Option<String>,
    },
    Social {
        pkce_verifier: String,
        return_to: Option<String>,
    },
}

impl ErasedLoginRuntime for FederatedRuntime {
    fn is_flow(&self) -> bool {
        true
    }

    fn requires_login_state_store(&self) -> bool {
        true
    }

    fn validate(&self) -> Result<(), AuthError> {
        self.validate_conf()
    }

    fn initialize(&self) -> LoginFuture<'_, ()> {
        Box::pin(async move {
            if self.is_oidc() {
                self.initialize_oidc().await?;
            }
            Ok(())
        })
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
        _passwordless_store: Option<&'a super::PasswordlessStoreRuntime>,
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
        _passwordless_store: Option<&'a super::PasswordlessStoreRuntime>,
    ) -> LoginFuture<'a, CompletedLogin> {
        Box::pin(async move { self.complete_inner(method, input, codec, state_store).await })
    }
}

impl FederatedRuntime {
    fn begin_social(
        &self,
        return_to: Option<String>,
        method: &LoginMethodId,
        target: LoginTarget,
        codec: &ChallengeCodec,
    ) -> Result<String, AuthError> {
        let (challenge, verifier) = oauth2::PkceCodeChallenge::new_random_sha256();
        let pending = PendingFederated::Social {
            pkce_verifier: verifier.secret().to_owned(),
            return_to,
        };
        let state = self.seal_state(method, target, pending, codec)?;
        social::authorization_url(&self.conf, challenge, state)
    }

    async fn begin_inner(
        &self,
        method: &LoginMethodId,
        input: BoxLoginInput,
        target: LoginTarget,
        codec: &ChallengeCodec,
    ) -> Result<LoginChallenge, AuthError> {
        self.validate_conf()?;
        let input = input
            .downcast::<FederatedStart>()
            .map_err(|_| AuthError::InvalidCredential)?;
        validate_return_to(input.return_to.as_deref())?;
        let url = if self.is_oidc() {
            self.begin_oidc(input.return_to.clone(), method, target, codec)
                .await?
        } else {
            self.begin_social(input.return_to.clone(), method, target, codec)?
        };
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
            .downcast::<FederatedCallback>()
            .map_err(|_| AuthError::InvalidCredential)?;
        let (code, state_token) = callback_values(&callback)?;
        let state: SealedLoginState = codec.open(state_token)?;
        validate_state(&state, method)?;
        let pending: PendingFederated = serde_json::from_value(state.payload.clone())
            .map_err(|_| AuthError::InvalidLoginState)?;
        let identity = match pending {
            PendingFederated::Oidc {
                nonce,
                pkce_verifier,
                return_to,
            } => {
                self.exchange_oidc(code, nonce, pkce_verifier, return_to)
                    .await?
            }
            PendingFederated::Social {
                pkce_verifier,
                return_to,
            } => {
                social::exchange(
                    &self.conf,
                    self.http()?,
                    self.client_secret(),
                    code,
                    pkce_verifier,
                    return_to,
                )
                .await?
            }
        };
        let mapper = self.mapper()?;
        let user = mapper.map(&identity).await?;
        let store = state_store.ok_or(AuthError::InvalidLoginState)?;
        store.consume(&state).await?;
        let login = verified_federated(user, &identity);
        Ok(CompletedLogin {
            login,
            target: state.target,
        })
    }

    fn seal_state(
        &self,
        method: &LoginMethodId,
        target: LoginTarget,
        pending: PendingFederated,
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

    fn client_secret(&self) -> Option<&str> {
        self.client_secret.get().and_then(|value| value.as_deref())
    }

    const fn is_oidc(&self) -> bool {
        matches!(
            self.conf.provider,
            FederatedProvider::Oidc | FederatedProvider::Google
        )
    }

    fn mapper(&self) -> Result<&Arc<dyn ErasedFederatedMapper>, AuthError> {
        self.conf.mapper.as_ref().ok_or_else(|| {
            AuthError::InvalidProviderConfig("federated account mapper is required".into())
        })
    }

    fn http(&self) -> Result<&FederatedHttpClient, AuthError> {
        self.http.as_ref().map_err(|_| {
            AuthError::InvalidProviderConfig("unable to build secure federated HTTP client".into())
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
                "federated login requires client ID, redirect URI, and a TTL between 1 and 900 seconds".into(),
            ));
        }
        if matches!(
            self.conf.provider,
            FederatedProvider::GitHub | FederatedProvider::Facebook
        ) && self.conf.client_secret.is_none()
        {
            return Err(AuthError::InvalidProviderConfig(
                "GitHub and Facebook login require a client secret".into(),
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
                "federated scopes must be non-empty bounded values".into(),
            ));
        }
        self.http()?;
        self.mapper().map(|_| ())
    }
}

impl LoginProviderDefinition<FederatedStart, FederatedCallback> for FederatedLogin {
    fn define(self) -> LoginRuntimeDefinition {
        let http = FederatedHttpClient::build().map_err(|_| ());
        LoginRuntimeDefinition {
            runtime: Arc::new(FederatedRuntime {
                conf: self,
                oidc_client: arc_swap::ArcSwapOption::empty(),
                oidc_discovery: tokio::sync::Mutex::new(oidc::DiscoveryState::default()),
                oidc_notify: tokio::sync::Notify::new(),
                http,
                client_secret: OnceLock::new(),
            }),
        }
    }
}

impl super::model::definition_sealed::Sealed for FederatedLogin {}

fn callback_values(callback: &FederatedCallback) -> Result<(&str, &str), AuthError> {
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
    let Some(path) = value else {
        return Ok(());
    };
    if path.len() > 2048
        || !path.starts_with('/')
        || path.starts_with("//")
        || path.contains('\\')
        || path.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(AuthError::InvalidLoginState);
    }
    let base =
        url::Url::parse("https://vyuh.invalid/").map_err(|_| AuthError::InvalidLoginState)?;
    let resolved = base.join(path).map_err(|_| AuthError::InvalidLoginState)?;
    if resolved.host_str() != base.host_str() || resolved.fragment().is_some() {
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

fn verified_federated(user: AuthUser, identity: &FederatedIdentity) -> VerifiedLogin {
    let auth_time = identity
        .claims
        .get("auth_time")
        .and_then(serde_json::Value::as_i64);
    let mut methods = vec![federated_method(identity.provider).into()];
    if let Some(values) = identity
        .claims
        .get("amr")
        .and_then(serde_json::Value::as_array)
    {
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
        acr: identity
            .claims
            .get("acr")
            .and_then(|value| value.as_str())
            .map(str::to_owned),
    }
}

const fn federated_method(provider: FederatedProvider) -> &'static str {
    match provider {
        FederatedProvider::Oidc => "oidc",
        FederatedProvider::Google => "google",
        FederatedProvider::GitHub => "github",
        FederatedProvider::Facebook => "facebook",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Return paths remain same-origin paths and reject browser path ambiguities.
    #[test]
    fn validates_safe_federated_return_paths() {
        assert!(validate_return_to(Some("/dashboard?tab=profile")).is_ok());
        assert!(validate_return_to(Some("https://example.com")).is_err());
        assert!(validate_return_to(Some("//example.com/path")).is_err());
        assert!(validate_return_to(Some("/\\example.com/path")).is_err());
        assert!(validate_return_to(Some("/dashboard#fragment")).is_err());
    }
}
