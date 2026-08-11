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

use std::{collections::BTreeMap, sync::Arc};

use axum::http::request::Parts;

use contract::{ProviderCapabilities, ProviderRuntime};
use indexes::RuntimeIndexes;

use super::{
    Audience, AudienceId, AuthBuildError, AuthConf, AuthError, AuthMetrics, AuthProvider, AuthUser,
    LoginAuth, LoginDefinitionInner, LoginMethod, LoginMethodId, LoginResponse, LogoutResponse,
    ProviderId, RequestCredentialScan, SecretRing, identity::resolve_audiences,
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
    challenges: Arc<BTreeMap<AudienceId, Arc<[axum::http::HeaderValue]>>>,
}

impl std::fmt::Debug for Authenticator {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Authenticator")
            .field("providers", &self.providers.len())
            .field("login_methods", &self.login_methods.len())
            .field("access_bindings", &self.indexes.access_binding_count())
            .field("challenge_audiences", &self.challenges.len())
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
        audiences: Vec<AudienceId>,
    ) -> Result<Self, AuthBuildError> {
        build::authenticator(conf, secret, fallbacks, project_dir, audiences).await
    }

    /// Retains one provider descriptor for a later terminal operation.
    pub const fn using(&self, provider: AuthProvider) -> ProviderAuth<'_> {
        ProviderAuth {
            authenticator: self,
            provider,
        }
    }

    pub(crate) async fn authenticate(
        &self,
        parts: &Parts,
        audience: &AudienceId,
    ) -> Result<AuthUser, AuthError> {
        let mut selected = None;
        let mut scan = RequestCredentialScan::new(parts);
        for position in self.indexes.access_for(audience) {
            let provider = self.provider_at(*position)?;
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
        let provider = self.provider_at(position)?;
        let result = provider.authenticate(raw.as_ref(), parts, audience).await;
        self.record(provider.id(), &result);
        result
    }

    pub(crate) fn rejection(
        &self,
        error: AuthError,
        audience: &AudienceId,
    ) -> super::AuthRejection {
        let challenges = if error.status() == axum::http::StatusCode::UNAUTHORIZED {
            self.challenges.get(audience).cloned()
        } else {
            None
        };
        super::AuthRejection::new(error, challenges)
    }

    /// Returns OAuth protected-resource metadata only when one provider for the
    /// audience can describe it unambiguously.
    #[cfg(feature = "mcp")]
    pub(crate) fn mcp_protected_resource(
        &self,
        audience: &AudienceId,
    ) -> Option<super::AuthProtectedResource> {
        let mut protected = None;
        for position in self.indexes.access_for(audience) {
            let Some(provider) = self.providers.get(*position) else {
                continue;
            };
            let Some(value) = provider.0.protected_resource(audience) else {
                continue;
            };
            if protected.is_some() {
                return None;
            }
            protected = Some(value);
        }
        protected
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
        runtime.login(user, audiences).await
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

    fn provider_at(&self, position: usize) -> Result<&ProviderRuntime, AuthError> {
        self.providers
            .get(position)
            .ok_or_else(|| AuthError::Internal("authentication provider index is invalid".into()))
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

    /// Issues access and optional refresh credentials through this provider.
    pub async fn issue(
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
            provider.login(user, audiences).await
        }
        .await;
        self.record(&result);
        result
    }

    /// Rotates a refresh credential accepted only by this provider.
    pub async fn refresh(&self, parts: &Parts) -> Result<LoginResponse, AuthError> {
        let result = async {
            let provider = self.provider()?;
            if !provider.capabilities().refresh {
                return Err(AuthError::UnsupportedProviderCapability);
            }
            let location = provider
                .refresh_location()
                .ok_or(AuthError::UnsupportedProviderCapability)?;
            let raw = location.extract(parts)?.ok_or(AuthError::NoCredential)?;
            provider.refresh(&raw, parts).await
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
    ) -> Result<LoginResponse, AuthError> {
        self.audiences().validate_requested(&audiences)?;
        self.0.login(user, audiences).await
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

    async fn refresh(&self, raw: &str, parts: &Parts) -> Result<LoginResponse, AuthError> {
        self.0.refresh(raw, parts).await
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
