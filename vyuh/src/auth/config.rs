//! Provider, token policy, and key-source configuration.

use std::{fmt, future::Future, path::PathBuf, sync::Arc};

use axum::http::{HeaderName, request::Parts};
use chrono::Duration;
use futures::future::BoxFuture;
use serde::Serialize;

use super::{
    Audience, AudienceId, AuthError, AuthProvider, AuthToken, AuthUser, CodecDefinition,
    CredentialLocation, CsrfConf, ErasedDecoder, ErasedEncoder, ErasedKeyLifecycle,
    ErasedLifecycle, Jwt, LoginDefinitionInner, LoginMethod, LoginProviderDefinition,
    LoginStateStore, LoginStateStoreRuntime, ProviderDocLocation, SecretRing, TokenDecoder,
    TokenEncoder, TokenLifecycle, identity::DEFAULT_AUTH_PROVIDER,
};

/// Source for token signing, verification, or encryption key material.
#[derive(Clone, PartialEq, Eq)]
pub struct KeySource(KeySourceKind);

#[derive(Clone, PartialEq, Eq)]
pub(crate) enum KeySourceKind {
    SiteSecret,
    Inline(String),
    Env(String),
    File(PathBuf),
}

impl Default for KeySource {
    fn default() -> Self {
        Self::site_secret()
    }
}

impl KeySource {
    /// Uses `SiteConf.secret_key` and its configured verification fallbacks.
    pub const fn site_secret() -> Self {
        Self(KeySourceKind::SiteSecret)
    }

    /// Uses inline key material that remains redacted from diagnostics.
    pub fn inline(value: impl Into<String>) -> Self {
        Self(KeySourceKind::Inline(value.into()))
    }

    /// Reads key material from an environment variable during site construction.
    pub fn env(name: impl Into<String>) -> Self {
        Self(KeySourceKind::Env(name.into()))
    }

    /// Reads key material from an absolute or project-relative file during site construction.
    pub fn file(path: impl Into<PathBuf>) -> Self {
        Self(KeySourceKind::File(path.into()))
    }

    pub(crate) const fn kind(&self) -> &KeySourceKind {
        &self.0
    }

    pub(crate) const fn is_site_secret(&self) -> bool {
        matches!(self.0, KeySourceKind::SiteSecret)
    }
}

impl fmt::Debug for KeySource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind() {
            KeySourceKind::SiteSecret => formatter.write_str("SiteSecret"),
            KeySourceKind::Inline(_) => formatter.write_str("Inline(<redacted>)"),
            KeySourceKind::Env(_) => formatter.write_str("Env(<redacted>)"),
            KeySourceKind::File(_) => formatter.write_str("File(<redacted>)"),
        }
    }
}

/// Resolves one authenticated token into the identity exposed to handlers.
pub trait TokenVerifier: Send + Sync + 'static {
    /// Rejects, resolves, or enriches a token after framework validation.
    fn verify<'a>(
        &'a self,
        token: &'a AuthToken,
    ) -> impl Future<Output = Result<AuthUser, AuthError>> + Send + 'a;
}

pub(crate) trait ErasedTokenVerifier: Send + Sync {
    fn verify<'a>(&'a self, token: &'a AuthToken) -> BoxFuture<'a, Result<AuthUser, AuthError>>;
}

impl<T: TokenVerifier> ErasedTokenVerifier for T {
    fn verify<'a>(&'a self, token: &'a AuthToken) -> BoxFuture<'a, Result<AuthUser, AuthError>> {
        Box::pin(TokenVerifier::verify(self, token))
    }
}

#[derive(Clone, Copy)]
struct EmbeddedVerifier;

impl TokenVerifier for EmbeddedVerifier {
    async fn verify(&self, token: &AuthToken) -> Result<AuthUser, AuthError> {
        Ok(token.embedded_user())
    }
}

/// Context supplied while resolving an opaque authentication key.
pub struct KeyRequest<'a> {
    audience: &'a str,
    binding: Option<&'a str>,
}

impl<'a> KeyRequest<'a> {
    pub(crate) const fn new(audience: &'a str, binding: Option<&'a str>) -> Self {
        Self { audience, binding }
    }

    /// Returns the API audience requested by the current operation.
    pub const fn audience(&self) -> &'a str {
        self.audience
    }

    /// Returns application-resolved request binding state when configured.
    pub const fn binding(&self) -> Option<&'a str> {
        self.binding
    }
}

/// Resolves an opaque key through application-controlled storage and policy.
pub trait KeyVerifier: Send + Sync + 'static {
    /// Returns the accepted identity or rejects the opaque credential.
    fn verify<'a>(
        &'a self,
        credential: &'a super::PresentedCredential<'a>,
        request: KeyRequest<'a>,
    ) -> impl Future<Output = Result<AuthUser, AuthError>> + Send + 'a;
}

pub(crate) trait ErasedKeyVerifier: Send + Sync {
    fn verify<'a>(
        &'a self,
        credential: &'a super::PresentedCredential<'a>,
        request: KeyRequest<'a>,
    ) -> BoxFuture<'a, Result<AuthUser, AuthError>>;
}

impl<T: KeyVerifier> ErasedKeyVerifier for T {
    fn verify<'a>(
        &'a self,
        credential: &'a super::PresentedCredential<'a>,
        request: KeyRequest<'a>,
    ) -> BoxFuture<'a, Result<AuthUser, AuthError>> {
        Box::pin(KeyVerifier::verify(self, credential, request))
    }
}

pub(crate) type BindingResolver = fn(&Parts) -> Result<Option<String>, AuthError>;

/// Extraction, delivery, and lifetime policy for one token kind.
#[derive(Clone)]
pub struct TokenConf {
    pub(crate) location: CredentialLocation,
    pub(crate) response_header: Option<String>,
    pub(crate) ttl_seconds: i64,
    pub(crate) codec: Option<CodecDefinition>,
    pub(crate) csrf: Option<CsrfConf>,
    pub(crate) max_credential_bytes: usize,
}

impl TokenConf {
    /// Reads a bearer token and returns newly issued credentials in JSON.
    pub fn bearer() -> Self {
        Self {
            location: CredentialLocation::bearer(),
            response_header: None,
            ttl_seconds: 3600,
            codec: None,
            csrf: None,
            max_credential_bytes: 16 * 1024,
        }
    }

    /// Reads and writes an HttpOnly token cookie.
    pub fn cookie(value: impl Into<super::CookieConf>) -> Self {
        Self::from_location(CredentialLocation::cookie(value))
    }

    /// Reads a token from a header and returns newly issued credentials in JSON.
    pub fn header(name: impl Into<String>) -> Self {
        Self::from_location(CredentialLocation::header(name))
    }

    /// Reads a token from a header with a case-insensitive authentication scheme.
    pub fn header_with_scheme(name: impl Into<String>, scheme: impl Into<String>) -> Self {
        Self::from_location(CredentialLocation::header_with_scheme(name, scheme))
    }

    /// Reads a token from a query parameter after explicit leakage acknowledgement.
    pub fn query(name: impl Into<String>, risk: super::UnsafeQueryCredentials) -> Self {
        Self::from_location(CredentialLocation::query(name, risk))
    }

    fn from_location(location: CredentialLocation) -> Self {
        let csrf = location.default_csrf();
        Self {
            location,
            response_header: None,
            ttl_seconds: 3600,
            codec: None,
            csrf,
            max_credential_bytes: 16 * 1024,
        }
    }

    /// Delivers issued credentials through a response header independent of extraction.
    pub fn response_header(mut self, name: impl Into<String>) -> Self {
        self.response_header = Some(name.into());
        self
    }

    /// Sets the finite token lifetime.
    pub fn ttl(mut self, value: Duration) -> Self {
        self.ttl_seconds = value.num_seconds();
        self
    }

    /// Overrides the provider's default codec for this token kind.
    pub fn codec<C: Into<CodecDefinition>>(mut self, value: C) -> Self {
        self.codec = Some(value.into());
        self
    }

    /// Replaces the default double-submit policy for a cookie token.
    pub fn csrf(mut self, value: CsrfConf) -> Self {
        self.csrf = Some(value);
        self
    }

    /// Explicitly disables CSRF checks for this token kind.
    pub fn without_csrf(mut self) -> Self {
        self.csrf = None;
        self
    }

    /// Lowers the maximum encoded credential size accepted before decoding.
    pub fn max_credential_bytes(mut self, value: usize) -> Self {
        self.max_credential_bytes = value;
        self
    }
}

/// A complete parseable-token provider with access and optional refresh behavior.
#[derive(Clone)]
pub struct TokenProvider {
    pub(crate) codec: CodecDefinition,
    pub(crate) access: TokenConf,
    pub(crate) refresh: Option<TokenConf>,
    pub(crate) verifier: Arc<dyn ErasedTokenVerifier>,
    pub(crate) lifecycle: Option<Arc<dyn ErasedLifecycle>>,
    pub(crate) binding: Option<BindingResolver>,
    pub(crate) leeway_seconds: i64,
    pub(crate) issuer: Option<String>,
}

impl TokenProvider {
    /// Creates a complete access-and-refresh provider using one codec by default.
    pub fn new<C: Into<CodecDefinition>>(codec: C) -> Self {
        Self {
            codec: codec.into(),
            access: TokenConf::bearer().ttl(Duration::hours(1)),
            refresh: Some(TokenConf::bearer().ttl(Duration::days(7))),
            verifier: Arc::new(EmbeddedVerifier),
            lifecycle: None,
            binding: None,
            leeway_seconds: 0,
            issuer: None,
        }
    }

    /// Creates a provider that can authenticate but cannot issue or refresh tokens.
    pub fn verify_only(decoder: impl TokenDecoder, format: impl Into<String>) -> Self {
        let custom = super::CustomCodec {
            encoder: None,
            decoder: Arc::new(decoder),
            format: format.into(),
        };
        Self::new(CodecDefinition::Custom(custom)).without_refresh()
    }

    /// Creates a provider from an application-owned issuing and verifying codec.
    pub fn custom<C>(codec: C, format: impl Into<String>) -> Self
    where
        C: TokenEncoder + TokenDecoder,
    {
        let codec = Arc::new(codec);
        let custom = super::CustomCodec {
            encoder: Some(codec.clone() as Arc<dyn ErasedEncoder>),
            decoder: codec as Arc<dyn ErasedDecoder>,
            format: format.into(),
        };
        Self::new(CodecDefinition::Custom(custom))
    }

    /// Replaces access-token extraction, delivery, lifetime, and codec policy.
    pub fn access(mut self, value: TokenConf) -> Self {
        self.access = value;
        self
    }

    /// Enables and configures refresh-token rotation.
    pub fn refresh(mut self, value: TokenConf) -> Self {
        self.refresh = Some(value);
        self
    }

    /// Disables refresh-token issuance and refresh operations.
    pub fn without_refresh(mut self) -> Self {
        self.refresh = None;
        self
    }

    /// Replaces the default embedded-identity verifier.
    pub fn verifier(mut self, value: impl TokenVerifier) -> Self {
        self.verifier = Arc::new(value);
        self
    }

    /// Adds optional replay protection and revocation storage.
    pub fn lifecycle(mut self, value: impl TokenLifecycle) -> Self {
        self.lifecycle = Some(Arc::new(value));
        self
    }

    /// Binds issued tokens to application-resolved request state.
    pub fn binding(mut self, value: BindingResolver) -> Self {
        self.binding = Some(value);
        self
    }

    /// Allows bounded clock skew during framework temporal validation.
    pub fn leeway(mut self, value: Duration) -> Self {
        self.leeway_seconds = value.num_seconds().max(0);
        self
    }

    /// Requires and emits one authenticated issuer across this provider.
    pub fn issuer(mut self, value: impl Into<String>) -> Self {
        self.issuer = Some(value.into());
        self
    }
}

impl fmt::Debug for TokenProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TokenProvider")
            .field("access", &self.access.location)
            .field(
                "refresh",
                &self.refresh.as_ref().map(|value| &value.location),
            )
            .finish_non_exhaustive()
    }
}

/// Verify-only provider for opaque authentication keys.
#[derive(Clone)]
pub struct AuthKey {
    pub(crate) location: CredentialLocation,
    pub(crate) verifier: Arc<dyn ErasedKeyVerifier>,
    pub(crate) binding: Option<BindingResolver>,
    pub(crate) csrf: Option<CsrfConf>,
    pub(crate) max_credential_bytes: usize,
    pub(crate) lifecycle: Option<Arc<dyn ErasedKeyLifecycle>>,
}

impl AuthKey {
    /// Reads an opaque key from one request header.
    pub fn header(name: impl Into<String>, verifier: impl KeyVerifier) -> Self {
        Self::from_location(CredentialLocation::header(name), verifier)
    }

    /// Reads an opaque key from a header with a case-insensitive authentication scheme.
    pub fn header_with_scheme(
        name: impl Into<String>,
        scheme: impl Into<String>,
        verifier: impl KeyVerifier,
    ) -> Self {
        Self::from_location(
            CredentialLocation::header_with_scheme(name, scheme),
            verifier,
        )
    }

    /// Reads an opaque key from an authentication cookie.
    pub fn cookie(cookie: impl Into<super::CookieConf>, verifier: impl KeyVerifier) -> Self {
        Self::from_location(CredentialLocation::cookie(cookie), verifier)
    }

    /// Reads an opaque key from a query parameter after explicit leakage acknowledgement.
    pub fn query(
        name: impl Into<String>,
        risk: super::UnsafeQueryCredentials,
        verifier: impl KeyVerifier,
    ) -> Self {
        Self::from_location(CredentialLocation::query(name, risk), verifier)
    }

    fn from_location(location: CredentialLocation, verifier: impl KeyVerifier) -> Self {
        let csrf = location.default_csrf();
        Self {
            location,
            verifier: Arc::new(verifier),
            binding: None,
            csrf,
            max_credential_bytes: 16 * 1024,
            lifecycle: None,
        }
    }

    /// Requires application-resolved request binding for accepted keys.
    pub fn binding(mut self, resolver: BindingResolver) -> Self {
        self.binding = Some(resolver);
        self
    }

    /// Replaces the default double-submit policy for a cookie key.
    pub fn csrf(mut self, value: CsrfConf) -> Self {
        self.csrf = Some(value);
        self
    }

    /// Explicitly disables CSRF checks for this cookie key.
    pub fn without_csrf(mut self) -> Self {
        self.csrf = None;
        self
    }

    /// Lowers the maximum encoded opaque credential size accepted before lookup.
    pub fn max_credential_bytes(mut self, value: usize) -> Self {
        self.max_credential_bytes = value;
        self
    }

    /// Adds server-side revocation for a key presented during logout.
    pub fn lifecycle(mut self, value: impl super::KeyLifecycle) -> Self {
        self.lifecycle = Some(Arc::new(value));
        self
    }
}

impl fmt::Debug for AuthKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthKey")
            .field("location", &self.location)
            .finish_non_exhaustive()
    }
}

pub(crate) mod sealed {
    pub trait ProviderDefinition {}
}

/// A framework-owned provider family accepted by [`AuthConf::provider`].
pub trait ProviderDefinition: sealed::ProviderDefinition + Sized {
    #[doc(hidden)]
    fn define(self) -> ProviderKind;
}

impl sealed::ProviderDefinition for TokenProvider {}
impl ProviderDefinition for TokenProvider {
    fn define(self) -> ProviderKind {
        ProviderKind::Token(Box::new(self))
    }
}

impl sealed::ProviderDefinition for AuthKey {}
impl ProviderDefinition for AuthKey {
    fn define(self) -> ProviderKind {
        ProviderKind::Key(self)
    }
}

/// Internal provider representation used while building a site.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub enum ProviderKind {
    Token(Box<TokenProvider>),
    Key(AuthKey),
}

impl ProviderKind {
    pub(crate) fn access_location(&self) -> &CredentialLocation {
        match self {
            Self::Token(value) => &value.access.location,
            Self::Key(value) => &value.location,
        }
    }

    pub(crate) fn refresh_location(&self) -> Option<&CredentialLocation> {
        match self {
            Self::Token(value) => value.refresh.as_ref().map(|item| &item.location),
            Self::Key(_) => None,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ProviderDefinitionInner {
    pub(crate) name: AuthProvider,
    pub(crate) kind: ProviderKind,
}

/// Authentication configuration and runtime provider registrations.
#[derive(Clone, Debug)]
pub struct AuthConf {
    default_enabled: bool,
    default_audience: Option<Audience>,
    providers: Vec<ProviderDefinitionInner>,
    login_methods: Vec<LoginDefinitionInner>,
    login_state_store: Option<LoginStateStoreRuntime>,
}

impl Default for AuthConf {
    fn default() -> Self {
        Self {
            default_enabled: true,
            default_audience: Some(super::DEFAULT_AUDIENCE),
            providers: Vec::new(),
            login_methods: Vec::new(),
            login_state_store: None,
        }
    }
}

impl AuthConf {
    /// Creates configuration without an implicit JWT provider.
    pub fn empty() -> Self {
        Self {
            default_enabled: false,
            ..Self::default()
        }
    }

    /// Replaces the compatibility audience used by omitted route and login audiences.
    pub fn default_audience(mut self, value: Audience) -> Self {
        self.default_audience = Some(value);
        self
    }

    /// Requires every authenticated route, login, and token to name an audience.
    pub fn require_explicit_audiences(mut self) -> Self {
        self.default_audience = None;
        self
    }

    /// Registers one complete named authentication provider.
    pub fn provider<D: ProviderDefinition>(mut self, name: AuthProvider, provider: D) -> Self {
        self.providers.push(ProviderDefinitionInner {
            name,
            kind: provider.define(),
        });
        self
    }

    /// Registers one named identity-proof method selected through `Authenticator::via`.
    pub fn method<Start, Complete, Definition>(
        mut self,
        method: LoginMethod<Start, Complete>,
        definition: Definition,
    ) -> Self
    where
        Start: Send + 'static,
        Complete: Send + 'static,
        Definition: LoginProviderDefinition<Start, Complete>,
    {
        self.login_methods
            .push(LoginDefinitionInner::new(method, definition));
        self
    }

    /// Enables atomic one-time consumption of OIDC and MFA continuation state.
    pub fn login_state_store(mut self, value: impl LoginStateStore) -> Self {
        self.login_state_store = Some(LoginStateStoreRuntime::new(value));
        self
    }

    pub(crate) fn definitions(&self) -> Vec<ProviderDefinitionInner> {
        let mut output = Vec::with_capacity(self.providers.len() + 1);
        if self.default_enabled {
            output.push(self.default_definition());
        }
        output.extend(self.providers.clone());
        output
    }

    pub(crate) fn login_definitions(&self) -> Vec<LoginDefinitionInner> {
        self.login_methods.clone()
    }

    pub(crate) fn login_state_store_runtime(&self) -> Option<LoginStateStoreRuntime> {
        self.login_state_store.clone()
    }

    pub(crate) fn default_audience_id(&self) -> Result<Option<AudienceId>, AuthError> {
        self.default_audience.map(AudienceId::declared).transpose()
    }

    pub(crate) const fn minimum_secret_length(&self) -> usize {
        32
    }

    fn default_definition(&self) -> ProviderDefinitionInner {
        let provider = TokenProvider::new(Jwt::hs256_site_secret())
            .access(TokenConf::bearer().ttl(Duration::hours(1)))
            .refresh(TokenConf::bearer().ttl(Duration::days(7)));
        ProviderDefinitionInner {
            name: DEFAULT_AUTH_PROVIDER,
            kind: ProviderKind::Token(Box::new(provider)),
        }
    }

    pub(crate) fn provider_docs(&self) -> Vec<ProviderDoc> {
        self.definitions()
            .into_iter()
            .map(|definition| ProviderDoc {
                id: definition.name.as_str().to_owned(),
                credential_type: match &definition.kind {
                    ProviderKind::Token(value) => {
                        CredentialType::Token(Some(value.codec.format().to_owned()))
                    }
                    ProviderKind::Key(_) => CredentialType::Key,
                },
                location: definition.kind.access_location().doc(),
                csrf_header: match &definition.kind {
                    ProviderKind::Token(value) => value
                        .access
                        .csrf
                        .as_ref()
                        .map(|csrf| csrf.header_name.clone()),
                    ProviderKind::Key(value) => {
                        value.csrf.as_ref().map(|csrf| csrf.header_name.clone())
                    }
                },
            })
            .collect()
    }

    pub(crate) fn validate_production(&self) -> Result<(), AuthError> {
        for definition in self.definitions() {
            match definition.kind {
                ProviderKind::Token(value) => validate_token_cookies(&value)?,
                ProviderKind::Key(value) => {
                    value.location.validate_production_cookie()?;
                    if value.location.is_cookie() && value.csrf.is_none() {
                        return Err(AuthError::InvalidProviderConfig(
                            "cookie credentials require CSRF validation in production".into(),
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

fn validate_token_cookies(value: &TokenProvider) -> Result<(), AuthError> {
    for conf in std::iter::once(&value.access).chain(value.refresh.iter()) {
        conf.location.validate_production_cookie()?;
        if conf.location.is_cookie() && conf.csrf.is_none() {
            return Err(AuthError::InvalidProviderConfig(
                "cookie credentials require CSRF validation in production".into(),
            ));
        }
    }
    Ok(())
}

/// Provider category used by generated OpenAPI security schemes.
#[derive(Clone)]
pub(crate) enum CredentialType {
    Token(Option<String>),
    Key,
}

/// Provider metadata used by generated OpenAPI documents.
#[derive(Clone)]
pub(crate) struct ProviderDoc {
    pub(crate) id: String,
    pub(crate) credential_type: CredentialType,
    pub(crate) location: ProviderDocLocation,
    pub(crate) csrf_header: Option<String>,
}

/// Redacted authentication configuration safe for consoles and diagnostics.
#[derive(Clone, Debug, Serialize, schemars::JsonSchema)]
pub struct AuthSummary {
    /// Effective compatibility audience, or `None` in strict mode.
    pub default_audience: Option<String>,
    /// Configured credential providers without key material.
    pub providers: Vec<AuthProviderSummary>,
    /// Number of registered identity-proof methods.
    pub login_method_count: usize,
}

/// Redacted capabilities for one configured credential provider.
#[derive(Clone, Debug, Serialize, schemars::JsonSchema)]
pub struct AuthProviderSummary {
    /// Stable provider identifier.
    pub id: String,
    /// Credential family or authenticated token format.
    pub format: String,
    /// Validated request selector description.
    pub source: String,
}

impl AuthConf {
    /// Returns a projection that never includes credentials or key material.
    pub fn summary(&self) -> AuthSummary {
        let providers = self
            .provider_docs()
            .into_iter()
            .map(provider_summary)
            .collect();
        AuthSummary {
            default_audience: self.default_audience.map(|value| value.as_str().to_owned()),
            providers,
            login_method_count: self.login_methods.len(),
        }
    }
}

fn provider_summary(value: ProviderDoc) -> AuthProviderSummary {
    let format = match value.credential_type {
        CredentialType::Token(format) => format.unwrap_or_else(|| "token".into()),
        CredentialType::Key => "api-key".into(),
    };
    let source = match value.location {
        ProviderDocLocation::Header { name, scheme } => scheme
            .map(|scheme| format!("header:{name}:{scheme}"))
            .unwrap_or_else(|| format!("header:{name}")),
        ProviderDocLocation::Cookie(name) => format!("cookie:{name}"),
        ProviderDocLocation::Query(name) => format!("query:{name}"),
    };
    AuthProviderSummary {
        id: value.id,
        format,
        source,
    }
}

pub(crate) fn validate_token_conf(value: &TokenConf) -> Result<(), AuthError> {
    value.location.validate()?;
    if let Some(name) = &value.response_header {
        HeaderName::try_from(name).map_err(|_| {
            AuthError::InvalidProviderConfig("invalid response credential header".into())
        })?;
    }
    if let Some(csrf) = &value.csrf {
        csrf.validate()?;
    }
    if value.ttl_seconds <= 0 {
        return Err(AuthError::InvalidProviderConfig(
            "token TTL must be positive".into(),
        ));
    }
    if value.max_credential_bytes == 0 || value.max_credential_bytes > 16 * 1024 {
        return Err(AuthError::InvalidProviderConfig(
            "token credential limit must be between 1 and 16384 bytes".into(),
        ));
    }
    Ok(())
}

pub(crate) fn build_codec(
    value: &CodecDefinition,
    secrets: &SecretRing,
) -> Result<super::CodecRuntime, AuthError> {
    super::codecs::build(value, secrets)
}
