//! Provider registry, request authentication, login, refresh, and logout runtime.

mod build;
pub(crate) mod contract;
mod indexes;
mod key_runtime;
mod registry;
#[cfg(test)]
mod session_contract_tests;
mod token_runtime;
mod validation;

use std::sync::Arc;

use axum::http::request::Parts;

use contract::{ProviderCapabilities, ProviderRuntime};
use indexes::RuntimeIndexes;

use super::{
    Audience, AudienceId, AuthConf, AuthError, AuthMetrics, AuthProvider, AuthUser, LoginAuth,
    LoginDefinitionInner, LoginMethod, LoginMethodId, LoginResponse, LogoutResponse, ProviderId,
    RequestCredentialScan, SecretRing,
    identity::{DEFAULT_AUTH_PROVIDER, resolve_audiences},
};

use key_runtime::KeyRuntime;
use token_runtime::{KindRuntime, TokenRuntime};
/// Runtime authentication registry built from [`AuthConf`].
#[derive(Clone)]
pub struct Authenticator {
    providers: Arc<Vec<ProviderRuntime>>,
    login_methods: Arc<Vec<LoginDefinitionInner>>,
    indexes: Arc<RuntimeIndexes>,
    login_state_store: Option<super::LoginStateStoreRuntime>,
    passwordless_store: Option<super::PasswordlessStoreRuntime>,
    secrets: SecretRing,
    challenge_codec: super::ChallengeCodec,
    metrics: Arc<AuthMetrics>,
    default_audience: Option<AudienceId>,
}

impl std::fmt::Debug for Authenticator {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Authenticator")
            .field("providers", &self.providers.len())
            .field("login_methods", &self.login_methods.len())
            .field("access_bindings", &self.indexes.access_binding_count())
            .finish()
    }
}

/// A facade constrained to one configured provider.
pub struct ProviderAuth<'a> {
    authenticator: &'a Authenticator,
    provider: AuthProvider,
}

impl Authenticator {
    pub(crate) async fn new(
        conf: &AuthConf,
        secret: &str,
        fallbacks: &[String],
        project_dir: &std::path::Path,
    ) -> Result<Self, AuthError> {
        build::authenticator(conf, secret, fallbacks, project_dir).await
    }

    /// Retains one provider descriptor for a later terminal operation.
    pub const fn using(&self, provider: AuthProvider) -> ProviderAuth<'_> {
        ProviderAuth {
            authenticator: self,
            provider,
        }
    }

    /// Retains one identity-proof descriptor using the default credential provider.
    pub fn via<Start, Complete>(
        &self,
        method: LoginMethod<Start, Complete>,
    ) -> LoginAuth<'_, Start, Complete>
    where
        Start: Send + 'static,
        Complete: Send + 'static,
    {
        super::login::select(self, DEFAULT_AUTH_PROVIDER, method)
    }

    /// Creates access and optional refresh credentials through the default provider.
    pub async fn login(
        &self,
        user: AuthUser,
        audiences: &[Audience],
    ) -> Result<LoginResponse, AuthError> {
        self.using(DEFAULT_AUTH_PROVIDER)
            .login(user, audiences)
            .await
    }

    /// Creates bound credentials through the default provider.
    pub async fn login_with_binding(
        &self,
        user: AuthUser,
        audiences: &[Audience],
        binding: super::AuthBinding,
    ) -> Result<LoginResponse, AuthError> {
        self.using(DEFAULT_AUTH_PROVIDER)
            .login_with_binding(user, audiences, binding)
            .await
    }

    /// Rotates a presented refresh credential through the default provider.
    pub async fn refresh(
        &self,
        parts: &Parts,
        audiences: &[Audience],
    ) -> Result<LoginResponse, AuthError> {
        self.using(DEFAULT_AUTH_PROVIDER)
            .refresh(parts, audiences)
            .await
    }

    /// Applies default-provider logout behavior and returns response attachments.
    pub async fn logout(&self, parts: &Parts) -> Result<LogoutResponse, AuthError> {
        self.using(DEFAULT_AUTH_PROVIDER).logout(parts).await
    }

    pub(crate) async fn authenticate(
        &self,
        parts: &Parts,
        audience: &AudienceId,
    ) -> Result<AuthUser, AuthError> {
        let mut selected = None;
        let mut scan = RequestCredentialScan::new(parts);
        for position in self.indexes.access_for(audience) {
            let provider = &self.providers[*position];
            let Some(raw) = provider.access_location().extract_from(&mut scan)? else {
                continue;
            };
            if selected.is_some() {
                return Err(AuthError::MalformedLocation);
            }
            selected = Some((*position, raw));
        }
        let Some((position, raw)) = selected else {
            return Err(AuthError::NoCredential);
        };
        let provider = &self.providers[position];
        let result = provider.authenticate(raw.as_ref(), parts, audience).await;
        self.record(provider.id(), &result);
        result
    }

    pub(crate) fn challenge_codec(&self) -> &super::ChallengeCodec {
        &self.challenge_codec
    }

    pub(crate) fn secret_ring(&self) -> &SecretRing {
        &self.secrets
    }

    pub(crate) fn login_state_store(&self) -> Option<&super::LoginStateStoreRuntime> {
        self.login_state_store.as_ref()
    }

    pub(crate) fn passwordless_store(&self) -> Option<&super::PasswordlessStoreRuntime> {
        self.passwordless_store.as_ref()
    }

    pub(crate) fn default_audience(&self) -> Option<&AudienceId> {
        self.default_audience.as_ref()
    }

    pub(crate) fn resolve_audiences(
        &self,
        audiences: &[Audience],
    ) -> Result<Vec<AudienceId>, AuthError> {
        resolve_audiences(audiences, self.default_audience.as_ref())
    }

    pub(crate) fn login_method(
        &self,
        id: &LoginMethodId,
    ) -> Result<&LoginDefinitionInner, AuthError> {
        self.login_methods
            .get(
                *self
                    .indexes
                    .login_methods
                    .get(id)
                    .ok_or_else(|| AuthError::LoginMethodNotFound(id.as_str().into()))?,
            )
            .ok_or_else(|| AuthError::LoginMethodNotFound(id.as_str().into()))
    }

    pub(crate) async fn login_verified(
        &self,
        provider: &ProviderId,
        verified: super::login::VerifiedLogin,
        audiences: Vec<AudienceId>,
    ) -> Result<LoginResponse, AuthError> {
        let authentication =
            super::AuthenticationContext::new(verified.auth_time, verified.methods, verified.acr);
        let user = verified.user.with_authentication(authentication);
        let runtime = self.provider(provider)?;
        runtime.login(user, audiences, None).await
    }

    pub(crate) fn resolve_login_provider(
        &self,
        provider: AuthProvider,
    ) -> Result<ProviderId, AuthError> {
        let id = ProviderId::new(provider.as_str())?;
        if !self.provider(&id)?.capabilities().login {
            return Err(AuthError::UnsupportedProviderCapability);
        }
        Ok(id)
    }

    fn provider(&self, id: &ProviderId) -> Result<&ProviderRuntime, AuthError> {
        self.indexes
            .providers
            .get(id)
            .and_then(|position| self.providers.get(*position))
            .ok_or_else(|| AuthError::ProviderNotFound(id.to_string()))
    }

    pub(crate) fn render_metrics(&self) -> String {
        self.metrics.render()
    }

    fn record<T>(&self, provider: &ProviderId, result: &Result<T, AuthError>) {
        self.record_provider(provider.as_str(), result);
    }

    pub(crate) fn record_provider<T>(&self, provider: &str, result: &Result<T, AuthError>) {
        self.metrics.record(provider, result);
    }

    pub(crate) fn record_login<T>(&self, method: &str, result: &Result<T, AuthError>) {
        self.metrics.record_login(method, result);
    }
}

impl<'a> ProviderAuth<'a> {
    #[cfg(feature = "mcp")]
    pub(crate) fn mcp_eligible(&self) -> Result<(), AuthError> {
        let provider = self.provider()?;
        if provider.capabilities().authenticate && provider.access_location().is_header() {
            Ok(())
        } else {
            Err(AuthError::InvalidProviderConfig(
                "MCP providers must authenticate through one request header".into(),
            ))
        }
    }

    #[cfg(feature = "mcp")]
    pub(crate) fn protected_resource(
        &self,
        audience: Audience,
    ) -> Result<Option<super::AuthProtectedResource>, AuthError> {
        let audience = AudienceId::declared(audience)?;
        Ok(self.provider()?.0.protected_resource(&audience))
    }

    #[cfg(feature = "mcp")]
    pub(crate) fn challenge(
        &self,
        error: &AuthError,
    ) -> Result<Option<super::AuthChallenge>, AuthError> {
        Ok(self.provider()?.0.challenge(error))
    }
    #[cfg(feature = "mcp")]
    /// Authenticates one request through exactly this configured provider.
    pub(crate) async fn authenticate(
        &self,
        parts: &Parts,
        audience: Audience,
    ) -> Result<AuthUser, AuthError> {
        let result = async {
            let audience = AudienceId::declared(audience)?;
            let provider = self.provider()?;
            let raw = provider
                .access_location()
                .extract(parts)?
                .ok_or(AuthError::NoCredential)?;
            provider.authenticate(&raw, parts, &audience).await
        }
        .await;
        self.record(&result);
        result
    }

    /// Retains one identity-proof descriptor with this credential provider.
    pub fn via<Start, Complete>(
        &self,
        method: LoginMethod<Start, Complete>,
    ) -> LoginAuth<'a, Start, Complete>
    where
        Start: Send + 'static,
        Complete: Send + 'static,
    {
        super::login::select(self.authenticator, self.provider, method)
    }

    /// Creates access and optional refresh credentials through this provider.
    pub async fn login(
        &self,
        user: AuthUser,
        audiences: &[Audience],
    ) -> Result<LoginResponse, AuthError> {
        let result = async {
            let audiences =
                resolve_audiences(audiences, self.authenticator.default_audience.as_ref())?;
            let provider = self.provider()?;
            if !provider.capabilities().login {
                return Err(AuthError::UnsupportedProviderCapability);
            }
            provider.login(user, audiences, None).await
        }
        .await;
        self.record(&result);
        result
    }

    /// Creates bound credentials through this provider.
    pub async fn login_with_binding(
        &self,
        user: AuthUser,
        audiences: &[Audience],
        binding: super::AuthBinding,
    ) -> Result<LoginResponse, AuthError> {
        let result = async {
            let audiences =
                resolve_audiences(audiences, self.authenticator.default_audience.as_ref())?;
            let provider = self.provider()?;
            if !provider.capabilities().login {
                return Err(AuthError::UnsupportedProviderCapability);
            }
            provider
                .login(user, audiences, Some(binding.into_inner()))
                .await
        }
        .await;
        self.record(&result);
        result
    }

    /// Rotates a refresh credential accepted only by this provider.
    pub async fn refresh(
        &self,
        parts: &Parts,
        audiences: &[Audience],
    ) -> Result<LoginResponse, AuthError> {
        let result = async {
            let provider = self.provider()?;
            if !provider.capabilities().refresh {
                return Err(AuthError::UnsupportedProviderCapability);
            }
            let location = provider
                .refresh_location()
                .ok_or(AuthError::UnsupportedProviderCapability)?;
            let raw = location.extract(parts)?.ok_or(AuthError::NoCredential)?;
            let audiences =
                resolve_audiences(audiences, self.authenticator.default_audience.as_ref())?;
            provider.refresh(&raw, parts, &audiences).await
        }
        .await;
        self.record(&result);
        result
    }

    /// Applies logout behavior only for this provider and returns response attachments.
    pub async fn logout(&self, parts: &Parts) -> Result<LogoutResponse, AuthError> {
        let result = async {
            let attachments = self.provider()?.logout(parts).await?;
            Ok(LogoutResponse::new(attachments))
        }
        .await;
        self.record(&result);
        result
    }

    fn provider(&self) -> Result<&ProviderRuntime, AuthError> {
        let id = ProviderId::new(self.provider.as_str())?;
        self.authenticator.provider(&id)
    }

    fn record<T>(&self, result: &Result<T, AuthError>) {
        self.authenticator
            .record_provider(self.provider.as_str(), result);
    }
}

impl ProviderRuntime {
    fn openapi(&self) -> super::ProviderDoc {
        self.0.openapi()
    }

    fn id(&self) -> &ProviderId {
        self.0.id()
    }

    fn access_location(&self) -> &super::CredentialLocation {
        self.0.access_location()
    }

    fn audiences(&self) -> &contract::ProviderAudienceSet {
        self.0.audiences()
    }

    fn refresh_location(&self) -> Option<&super::CredentialLocation> {
        self.0.refresh_location()
    }

    fn capabilities(&self) -> ProviderCapabilities {
        self.0.capabilities()
    }

    async fn login(
        &self,
        user: AuthUser,
        audiences: Vec<AudienceId>,
        binding: Option<String>,
    ) -> Result<LoginResponse, AuthError> {
        self.audiences().validate_requested(&audiences)?;
        self.0.login(user, audiences, binding).await
    }

    async fn authenticate(
        &self,
        raw: &str,
        parts: &Parts,
        audience: &AudienceId,
    ) -> Result<AuthUser, AuthError> {
        if !self.capabilities().authenticate {
            return Err(AuthError::UnsupportedProviderCapability);
        }
        if !self.audiences().supports(audience) {
            return Err(AuthError::AudienceMismatch);
        }
        self.0.authenticate(raw, parts, audience).await
    }

    async fn refresh(
        &self,
        raw: &str,
        parts: &Parts,
        audiences: &[AudienceId],
    ) -> Result<LoginResponse, AuthError> {
        self.audiences().validate_requested(audiences)?;
        self.0.refresh(raw, parts, audiences).await
    }

    async fn logout(
        &self,
        parts: &Parts,
    ) -> Result<Vec<(axum::http::HeaderName, axum::http::HeaderValue)>, AuthError> {
        if !self.capabilities().logout {
            return Err(AuthError::UnsupportedProviderCapability);
        }
        self.0.logout(parts).await
    }
}
