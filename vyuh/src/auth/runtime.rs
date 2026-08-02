//! Provider registry, request authentication, login, refresh, and logout runtime.

mod contract;
mod indexes;
mod registry;
#[cfg(test)]
mod session_contract_tests;
mod validation;

use std::{path::Path, sync::Arc};

use axum::http::request::Parts;
use futures::future::BoxFuture;

use contract::{ProviderCapabilities, ProviderRuntime, ProviderRuntimeContract};
use indexes::RuntimeIndexes;
use registry::{build_provider, validate_definitions, validate_login_definitions};
use validation::*;

use super::{
    Audience, AudienceId, AuthConf, AuthError, AuthMetrics, AuthProvider, AuthToken, AuthUser,
    BindingResolver, CodecDefinition, CodecRuntime, Credentials, ErasedLifecycle,
    ErasedTokenVerifier, KeyRequest, LoginAuth, LoginDefinitionInner, LoginMethod, LoginMethodId,
    LoginResponse, LogoutResponse, ProviderDefinitionInner, ProviderId, ProviderKind,
    RefreshMetadata, SecretRing, TokenConf, TokenKind, TokenProvider, build_codec,
    identity::{DEFAULT_AUTH_PROVIDER, resolve_audiences},
    validate_token_conf,
};
/// Runtime authentication registry built from [`AuthConf`].
#[derive(Clone)]
pub struct Authenticator {
    providers: Arc<Vec<ProviderRuntime>>,
    login_methods: Arc<Vec<LoginDefinitionInner>>,
    indexes: Arc<RuntimeIndexes>,
    login_state_store: Option<super::LoginStateStoreRuntime>,
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
            .field("access_selectors", &self.indexes.access.len())
            .field("refresh_selectors", &self.indexes.refresh.len())
            .finish()
    }
}

#[derive(Clone)]
struct TokenRuntime {
    id: ProviderId,
    format: String,
    access: KindRuntime,
    refresh: Option<KindRuntime>,
    verifier: Arc<dyn ErasedTokenVerifier>,
    lifecycle: Option<Arc<dyn ErasedLifecycle>>,
    binding: Option<BindingResolver>,
    leeway_seconds: i64,
    default_audience: Option<AudienceId>,
}

#[derive(Clone)]
struct KindRuntime {
    location: super::CredentialLocation,
    response_header: Option<String>,
    ttl_seconds: i64,
    codec: CodecRuntime,
    issuer: Option<String>,
    csrf: Option<super::CsrfConf>,
    max_credential_bytes: usize,
}

#[derive(Clone)]
struct KeyRuntime {
    id: ProviderId,
    definition: super::AuthKey,
}

/// A facade constrained to one configured provider.
pub struct ProviderAuth<'a> {
    authenticator: &'a Authenticator,
    provider: AuthProvider,
}

struct TokenPair {
    access: AuthToken,
    refresh: Option<AuthToken>,
}

impl Authenticator {
    pub(crate) async fn new(
        conf: &AuthConf,
        secret: &str,
        fallbacks: &[String],
        project_dir: &Path,
    ) -> Result<Self, AuthError> {
        let conf = conf.clone();
        let secret = secret.to_owned();
        let fallbacks = fallbacks.to_vec();
        let project_dir = project_dir.to_path_buf();
        tokio::task::spawn_blocking(move || {
            Self::new_sync(&conf, &secret, &fallbacks, &project_dir)
        })
        .await
        .map_err(|_| AuthError::Internal("authentication startup task failed".into()))?
    }

    /// Resolves key material and builds immutable provider indexes off request threads.
    fn new_sync(
        conf: &AuthConf,
        secret: &str,
        fallbacks: &[String],
        project_dir: &Path,
    ) -> Result<Self, AuthError> {
        let definitions = conf.definitions();
        let login_methods = conf.login_definitions();
        validate_definitions(&definitions)?;
        validate_login_definitions(&login_methods)?;
        let secrets =
            SecretRing::new(secret, fallbacks, project_dir, conf.minimum_secret_length())?;
        let default_audience = conf.default_audience_id()?;
        let metric_providers = definitions
            .iter()
            .map(|value| value.name.as_str().to_owned())
            .collect::<Vec<_>>();
        let metric_methods = login_methods
            .iter()
            .map(|value| value.name.to_owned())
            .collect::<Vec<_>>();
        let challenge_codec = super::ChallengeCodec::new(&secrets)?;
        for method in &login_methods {
            method.runtime.prepare(&secrets)?;
        }
        let providers = definitions
            .into_iter()
            .map(|definition| build_provider(definition, &secrets, default_audience.clone()))
            .collect::<Result<Vec<_>, _>>()?;
        let indexes = RuntimeIndexes::build(&providers, &login_methods)?;
        Ok(Self {
            providers: Arc::new(providers),
            login_methods: Arc::new(login_methods),
            indexes: Arc::new(indexes),
            login_state_store: conf.login_state_store_runtime(),
            secrets,
            challenge_codec,
            metrics: Arc::new(AuthMetrics::new(metric_providers, metric_methods)),
            default_audience,
        })
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
        for position in &self.indexes.access_order {
            let provider = &self.providers[*position];
            let Some(raw) = provider.access_location().extract(parts)? else {
                continue;
            };
            let result = provider.authenticate(&raw, parts, audience).await;
            self.record(provider.id(), &result);
            return result;
        }
        Err(AuthError::NoCredential)
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
        let authentication = super::AuthenticationContext::new(
            Some(verified.auth_time),
            verified.methods,
            verified.acr,
        );
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
    /// Authenticates one request through this selected provider only.
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
    pub async fn login_bound(
        &self,
        user: AuthUser,
        audiences: &[Audience],
        binding: impl Into<String>,
    ) -> Result<LoginResponse, AuthError> {
        let result = async {
            let audiences =
                resolve_audiences(audiences, self.authenticator.default_audience.as_ref())?;
            let provider = self.provider()?;
            if !provider.capabilities().login {
                return Err(AuthError::UnsupportedProviderCapability);
            }
            provider.login(user, audiences, Some(binding.into())).await
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
        self.0.authenticate(raw, parts, audience).await
    }

    async fn refresh(
        &self,
        raw: &str,
        parts: &Parts,
        audiences: &[AudienceId],
    ) -> Result<LoginResponse, AuthError> {
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

impl ProviderRuntimeContract for TokenRuntime {
    fn id(&self) -> &ProviderId {
        &self.id
    }
    fn access_location(&self) -> &super::CredentialLocation {
        &self.access.location
    }
    fn refresh_location(&self) -> Option<&super::CredentialLocation> {
        self.refresh.as_ref().map(|value| &value.location)
    }
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            authenticate: true,
            login: self.access.codec.can_encode(),
            refresh: self
                .refresh
                .as_ref()
                .is_some_and(|value| value.codec.can_encode()),
            logout: true,
        }
    }
    fn openapi(&self) -> super::ProviderDoc {
        super::ProviderDoc {
            id: self.id.to_string(),
            credential_type: super::CredentialType::Token(Some(self.format.clone())),
            location: self.access.location.doc(),
            csrf_header: self
                .access
                .csrf
                .as_ref()
                .map(|csrf| csrf.header_name.clone()),
        }
    }
    fn authenticate<'a>(
        &'a self,
        raw: &'a str,
        parts: &'a Parts,
        audience: &'a AudienceId,
    ) -> BoxFuture<'a, Result<AuthUser, AuthError>> {
        Box::pin(TokenRuntime::authenticate(self, raw, parts, audience))
    }
    fn login<'a>(
        &'a self,
        user: AuthUser,
        audiences: Vec<AudienceId>,
        binding: Option<String>,
    ) -> BoxFuture<'a, Result<LoginResponse, AuthError>> {
        Box::pin(TokenRuntime::login(self, user, audiences, binding))
    }
    fn refresh<'a>(
        &'a self,
        raw: &'a str,
        parts: &'a Parts,
        audiences: &'a [AudienceId],
    ) -> BoxFuture<'a, Result<LoginResponse, AuthError>> {
        Box::pin(TokenRuntime::refresh(self, raw, parts, audiences))
    }
    fn logout<'a>(
        &'a self,
        parts: &'a Parts,
    ) -> BoxFuture<'a, Result<Vec<(axum::http::HeaderName, axum::http::HeaderValue)>, AuthError>>
    {
        Box::pin(TokenRuntime::logout(self, parts))
    }
}

impl ProviderRuntimeContract for KeyRuntime {
    fn id(&self) -> &ProviderId {
        &self.id
    }
    fn access_location(&self) -> &super::CredentialLocation {
        &self.definition.location
    }
    fn refresh_location(&self) -> Option<&super::CredentialLocation> {
        None
    }
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            authenticate: true,
            login: false,
            refresh: false,
            logout: true,
        }
    }
    fn openapi(&self) -> super::ProviderDoc {
        super::ProviderDoc {
            id: self.id.to_string(),
            credential_type: super::CredentialType::Key,
            location: self.definition.location.doc(),
            csrf_header: self
                .definition
                .csrf
                .as_ref()
                .map(|csrf| csrf.header_name.clone()),
        }
    }
    fn authenticate<'a>(
        &'a self,
        raw: &'a str,
        parts: &'a Parts,
        audience: &'a AudienceId,
    ) -> BoxFuture<'a, Result<AuthUser, AuthError>> {
        Box::pin(KeyRuntime::authenticate(self, raw, parts, audience))
    }
    fn login<'a>(
        &'a self,
        _user: AuthUser,
        _audiences: Vec<AudienceId>,
        _binding: Option<String>,
    ) -> BoxFuture<'a, Result<LoginResponse, AuthError>> {
        Box::pin(async { Err(AuthError::UnsupportedProviderCapability) })
    }
    fn refresh<'a>(
        &'a self,
        _raw: &'a str,
        _parts: &'a Parts,
        _audiences: &'a [AudienceId],
    ) -> BoxFuture<'a, Result<LoginResponse, AuthError>> {
        Box::pin(async { Err(AuthError::UnsupportedProviderCapability) })
    }
    fn logout<'a>(
        &'a self,
        parts: &'a Parts,
    ) -> BoxFuture<'a, Result<Vec<(axum::http::HeaderName, axum::http::HeaderValue)>, AuthError>>
    {
        Box::pin(KeyRuntime::logout(self, parts))
    }
}

impl TokenRuntime {
    async fn authenticate(
        &self,
        raw: &str,
        parts: &Parts,
        audience: &AudienceId,
    ) -> Result<AuthUser, AuthError> {
        validate_credential_size(raw, self.access.max_credential_bytes)?;
        let token = self.normalize(self.access.codec.decode(raw).await?)?;
        validate_csrf(self.access.csrf.as_ref(), parts)?;
        self.accept(
            &token,
            TokenKind::Access,
            parts,
            std::slice::from_ref(audience),
        )
        .await
    }

    async fn login(
        &self,
        user: AuthUser,
        audiences: Vec<AudienceId>,
        binding: Option<String>,
    ) -> Result<LoginResponse, AuthError> {
        validate_subject(&user)?;
        validate_issued_binding(self.binding, &binding)?;
        let pair = self.tokens(&user, audiences, binding, None)?;
        self.response(pair).await.map(|(response, _)| response)
    }

    async fn refresh(
        &self,
        raw: &str,
        parts: &Parts,
        audiences: &[AudienceId],
    ) -> Result<LoginResponse, AuthError> {
        let refresh = self
            .refresh
            .as_ref()
            .ok_or(AuthError::UnsupportedProviderCapability)?;
        validate_credential_size(raw, refresh.max_credential_bytes)?;
        let current = self.normalize(refresh.codec.decode(raw).await?)?;
        validate_csrf(refresh.csrf.as_ref(), parts)?;
        let user = self
            .accept(&current, TokenKind::Refresh, parts, audiences)
            .await?;
        let pair = self.tokens(
            &user,
            audiences.to_vec(),
            current.binding_value().map(str::to_owned),
            current.family_id().map(str::to_owned),
        )?;
        let (response, replacement) = self.response(pair).await?;
        self.rotate(&current, replacement.as_ref()).await?;
        Ok(response)
    }

    async fn accept(
        &self,
        token: &AuthToken,
        kind: TokenKind,
        parts: &Parts,
        audiences: &[AudienceId],
    ) -> Result<AuthUser, AuthError> {
        let expected = self.kind(kind)?;
        validate_token(
            token,
            &self.id,
            kind,
            audiences,
            self.leeway_seconds,
            expected.issuer.as_deref(),
        )?;
        if kind == TokenKind::Refresh && token.family_id().is_none() {
            return Err(AuthError::InvalidCredential);
        }
        if self.lifecycle.is_some() && token.token_id().is_none() {
            return Err(AuthError::InvalidCredential);
        }
        validate_binding(token.binding_value(), self.binding, parts)?;
        if let Some(lifecycle) = &self.lifecycle {
            lifecycle.validate(token).await?;
        }
        let authentication = token.authentication();
        let user = self.verifier.verify(token).await?;
        validate_subject(&user)?;
        Ok(user
            .set_provider(self.id.clone())
            .with_authentication(authentication))
    }

    fn kind(&self, kind: TokenKind) -> Result<&KindRuntime, AuthError> {
        match kind {
            TokenKind::Access => Ok(&self.access),
            TokenKind::Refresh => self
                .refresh
                .as_ref()
                .ok_or(AuthError::UnsupportedProviderCapability),
        }
    }

    fn tokens(
        &self,
        user: &AuthUser,
        audiences: Vec<AudienceId>,
        binding: Option<String>,
        family: Option<String>,
    ) -> Result<TokenPair, AuthError> {
        let family = self
            .refresh
            .as_ref()
            .map(|_| family.unwrap_or_else(|| uuid::Uuid::new_v4().to_string()));
        let access = issue_token(
            &self.id,
            TokenKind::Access,
            user,
            audiences.clone(),
            &self.access,
            family.clone(),
            binding.clone(),
        )?;
        let refresh = self
            .refresh
            .as_ref()
            .map(|conf| {
                issue_token(
                    &self.id,
                    TokenKind::Refresh,
                    user,
                    audiences,
                    conf,
                    family,
                    binding,
                )
            })
            .transpose()?;
        Ok(TokenPair { access, refresh })
    }

    fn normalize(&self, mut token: AuthToken) -> Result<AuthToken, AuthError> {
        if token.audience_ids().is_none() {
            let audience = self
                .default_audience
                .clone()
                .ok_or(AuthError::AudienceMismatch)?;
            token.set_audiences(vec![audience]);
        }
        super::token::validate_structure(&token)?;
        Ok(token)
    }

    async fn response(
        &self,
        pair: TokenPair,
    ) -> Result<(LoginResponse, Option<AuthToken>), AuthError> {
        let access = self.access.codec.encode(&pair.access).await?;
        validate_credential_size(&access, self.access.max_credential_bytes)?;
        let refresh_value = match (&self.refresh, &pair.refresh) {
            (Some(conf), Some(token)) => {
                let encoded = conf.codec.encode(token).await?;
                validate_credential_size(&encoded, conf.max_credential_bytes)?;
                Some(encoded)
            }
            _ => None,
        };
        let (access_body, access_attachments) = delivery(&self.access, &access)?;
        let (refresh_body, refresh_attachments) = match (&self.refresh, &refresh_value) {
            (Some(conf), Some(value)) => delivery(conf, value)?,
            _ => (None, Vec::new()),
        };
        let attachments = access_attachments
            .into_iter()
            .chain(refresh_attachments)
            .collect();
        let credentials = Credentials::new(access, refresh_value);
        let response = LoginResponse::new(
            credentials,
            access_body,
            refresh_body,
            self.access.ttl_seconds,
            attachments,
        );
        Ok((response, pair.refresh))
    }

    async fn rotate(
        &self,
        current: &AuthToken,
        replacement: Option<&AuthToken>,
    ) -> Result<(), AuthError> {
        let Some(lifecycle) = &self.lifecycle else {
            return Ok(());
        };
        let replacement = replacement.ok_or(AuthError::UnsupportedProviderCapability)?;
        let metadata = RefreshMetadata::from_token(replacement)?;
        lifecycle.rotate(current, &metadata).await
    }

    async fn logout(
        &self,
        parts: &Parts,
    ) -> Result<Vec<(axum::http::HeaderName, axum::http::HeaderValue)>, AuthError> {
        if let Some(token) = self.presented(parts).await?
            && let Some(lifecycle) = &self.lifecycle
        {
            lifecycle.revoke(&token).await?;
        }
        clear_locations([
            (Some(&self.access.location), self.access.csrf.as_ref()),
            (
                self.refresh.as_ref().map(|item| &item.location),
                self.refresh.as_ref().and_then(|item| item.csrf.as_ref()),
            ),
        ])
    }

    async fn presented(&self, parts: &Parts) -> Result<Option<AuthToken>, AuthError> {
        if let Some(raw) = self.access.location.extract(parts)? {
            validate_credential_size(&raw, self.access.max_credential_bytes)?;
            let token = self.normalize(self.access.codec.decode(&raw).await?)?;
            validate_csrf(self.access.csrf.as_ref(), parts)?;
            self.validate_logout_token(&token, parts)?;
            return Ok(Some(token));
        }
        let Some(refresh) = &self.refresh else {
            return Ok(None);
        };
        let Some(raw) = refresh.location.extract(parts)? else {
            return Ok(None);
        };
        validate_credential_size(&raw, refresh.max_credential_bytes)?;
        let token = self.normalize(refresh.codec.decode(&raw).await?)?;
        validate_csrf(refresh.csrf.as_ref(), parts)?;
        self.validate_logout_token(&token, parts)?;
        Ok(Some(token))
    }

    fn validate_logout_token(&self, token: &AuthToken, parts: &Parts) -> Result<(), AuthError> {
        validate_common(token, &self.id, self.leeway_seconds)?;
        let conf = self.kind(token.kind())?;
        if conf.issuer.is_some() && token.issuer() != conf.issuer.as_deref() {
            return Err(AuthError::InvalidCredential);
        }
        if token.kind() == TokenKind::Refresh && token.family_id().is_none() {
            return Err(AuthError::InvalidCredential);
        }
        if self.lifecycle.is_some() && token.token_id().is_none() {
            return Err(AuthError::InvalidCredential);
        }
        validate_binding(token.binding_value(), self.binding, parts)?;
        Ok(())
    }
}

impl KeyRuntime {
    async fn authenticate(
        &self,
        raw: &str,
        parts: &Parts,
        audience: &AudienceId,
    ) -> Result<AuthUser, AuthError> {
        validate_credential_size(raw, self.definition.max_credential_bytes)?;
        validate_csrf(self.definition.csrf.as_ref(), parts)?;
        let binding = match self.definition.binding {
            Some(resolve) => Some(resolve(parts)?.ok_or(AuthError::BindingMismatch)?),
            None => None,
        };
        let request = KeyRequest::new(audience.as_str(), binding.as_deref());
        let presented = super::PresentedCredential::new(raw);
        let user = self.definition.verifier.verify(&presented, request).await?;
        validate_subject(&user)?;
        Ok(user.set_provider(self.id.clone()))
    }

    async fn logout(
        &self,
        parts: &Parts,
    ) -> Result<Vec<(axum::http::HeaderName, axum::http::HeaderValue)>, AuthError> {
        if let Some(raw) = self.definition.location.extract(parts)? {
            validate_credential_size(&raw, self.definition.max_credential_bytes)?;
            validate_csrf(self.definition.csrf.as_ref(), parts)?;
            if let Some(lifecycle) = &self.definition.lifecycle {
                let credential = super::PresentedCredential::new(&raw);
                lifecycle.revoke(&credential).await?;
            }
        }
        clear_locations([
            (
                Some(&self.definition.location),
                self.definition.csrf.as_ref(),
            ),
            (None, None),
        ])
    }
}
