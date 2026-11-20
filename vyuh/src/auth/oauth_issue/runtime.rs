//! Immutable issuer policy and delegated MCP credential provider.

use std::{collections::BTreeMap, sync::Arc};

use axum::http::request::Parts;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use futures::future::BoxFuture;
use ring::rand::{SecureRandom, SystemRandom};

use crate::{
    Site,
    auth::{
        AudienceId, AuthError, AuthProtectedResource, AuthUser, AuthenticationContext,
        ChallengeCodec, CredentialLocation, CredentialType, KeySource, LoginResponse,
        ProviderDefinitionInner, ProviderDoc, ProviderId, ProviderKind, Scope, SecretRing,
        runtime::contract::{
            ProviderAudienceSet, ProviderCapabilities, ProviderRuntimeContract, ResponseHeaders,
        },
    },
};

use super::{
    OAuthCimdClient, OAuthIssuer, cimd,
    config::{AccountVerifier, bounded, clean_url, invalid},
    registration::RegistrationPolicy,
    store,
};

pub(super) const CODE_TTL: i64 = 120;
pub(super) const CONSENT_TTL: i64 = 600;
pub(super) const ACCESS_TTL: i64 = 600;
pub(super) const GRANT_TTL: i64 = 30 * 86_400;

pub(super) struct Client {
    pub(super) name: String,
    pub(super) redirects: Vec<String>,
    pub(super) secret: Option<blake3::Hash>,
}

pub(crate) struct IssuerRuntime {
    pub(super) issuer: String,
    pub(super) authorize_route: String,
    pub(super) browser: AudienceId,
    pub(super) clients: BTreeMap<String, Client>,
    pub(super) cimd: BTreeMap<String, OAuthCimdClient>,
    pub(super) cimd_http: Option<reqwest::Client>,
    pub(super) dcr: Option<RegistrationPolicy>,
    pub(super) resources: BTreeMap<AudienceId, Vec<Scope>>,
    pub(super) codec: ChallengeCodec,
    verifier: Arc<dyn AccountVerifier>,
    id: ProviderId,
    audiences: ProviderAudienceSet,
    location: CredentialLocation,
}

impl IssuerRuntime {
    /// Freezes configured clients, exclusive resource audiences and consent sealing keys.
    pub(crate) fn build(
        config: &OAuthIssuer,
        secrets: &SecretRing,
        providers: &[ProviderDefinitionInner],
    ) -> Result<Self, AuthError> {
        if providers.len() >= 64 || !(1..=64).contains(&config.resources.len()) {
            return Err(invalid(
                "OAuth issuer exceeds the provider or resource limit",
            ));
        }
        let (origin, authorize_route, browser) = issuer_urls(config)?;
        validate_browser(browser, providers)?;
        let resources = resources(config, providers, &origin)?;
        let audiences = ProviderAudienceSet::only(resources.keys().cloned().collect())?;
        let verifier = config
            .verifier
            .clone()
            .ok_or_else(|| invalid("OAuth issuer requires an account verifier"))?;
        let clients = clients(config, secrets)?;
        let cimd = cimd_clients(config, &clients)?;
        let cimd_http = (!cimd.is_empty()).then(cimd::transport).transpose()?;
        let dcr = config
            .dcr
            .as_ref()
            .map(|policy| {
                RegistrationPolicy::build(policy, secrets, |scope| {
                    resources
                        .values()
                        .any(|allowed| allowed.binary_search(scope).is_ok())
                })
            })
            .transpose()?;
        Ok(Self {
            issuer: config.issuer.clone(),
            authorize_route: authorize_route.into(),
            browser: AudienceId::declared(browser)?,
            clients,
            cimd,
            cimd_http,
            dcr,
            resources,
            codec: ChallengeCodec::with_context(secrets, b"vyuh-oauth-consent-v1")?,
            verifier,
            id: ProviderId::new("vyuh-oauth-issuer")?,
            audiences,
            location: CredentialLocation::bearer(),
        })
    }

    pub(super) fn resource_scopes(&self, resource: &str) -> Option<&[Scope]> {
        self.resources
            .get(&AudienceId::new(resource).ok()?)
            .map(Vec::as_slice)
    }

    /// Returns the immutable local audiences reserved to this issuer.
    pub(crate) fn resource_audiences(&self) -> impl Iterator<Item = &AudienceId> {
        self.resources.keys()
    }

    pub(super) async fn verify_account(
        &self,
        site: &Site,
        subject: &str,
    ) -> Result<AuthUser, AuthError> {
        let user = self.verifier.verify(site, subject).await?;
        user.validate()?;
        if user.subject() != subject || !bounded(subject, 512) {
            return Err(AuthError::InvalidCredential);
        }
        Ok(user)
    }

    /// Authenticates an opaque current-generation access token for exactly this resource.
    async fn authenticate_access(
        &self,
        site: &Site,
        raw: &str,
        audience: &AudienceId,
    ) -> Result<AuthUser, AuthError> {
        let digest = credential_digest(raw).ok_or(AuthError::InvalidCredential)?;
        let Some((subject, client, binding, resource, scopes, redirect)) =
            store::access(site.db(), &self.issuer, digest, now())
                .await
                .map_err(|error| {
                    super::protocol::log_store("resource_lookup", &error);
                    AuthError::ProviderUnavailable
                })?
        else {
            return Err(AuthError::InvalidCredential);
        };
        if resource != audience.as_str() {
            return Err(AuthError::InvalidCredential);
        }
        super::clients::access(site, self, &client, binding, &redirect).await?;
        let configured = self
            .resources
            .get(audience)
            .ok_or(AuthError::InvalidCredential)?;
        let granted = scopes
            .split_ascii_whitespace()
            .filter_map(|scope| {
                configured
                    .binary_search_by(|candidate| candidate.as_str().cmp(scope))
                    .ok()
                    .and_then(|index| configured.get(index))
                    .cloned()
            })
            .collect::<Vec<_>>();
        let user = self.verify_account(site, &subject).await?;
        Ok(user
            .restrict_scopes(&granted)
            .set_provider(self.id.clone())
            .with_authentication(AuthenticationContext::new(None, vec!["oauth".into()], None)))
    }
}

impl ProviderRuntimeContract for IssuerRuntime {
    fn id(&self) -> &ProviderId {
        &self.id
    }
    fn audiences(&self) -> &ProviderAudienceSet {
        &self.audiences
    }
    fn access_location(&self) -> &CredentialLocation {
        &self.location
    }
    fn refresh_location(&self) -> Option<&CredentialLocation> {
        None
    }
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            authenticate: true,
            login: false,
            refresh: false,
            logout: false,
        }
    }
    fn openapi(&self) -> ProviderDoc {
        ProviderDoc {
            id: self.id.to_string(),
            audiences: Some(
                self.resources
                    .keys()
                    .map(|audience| audience.as_str().into())
                    .collect(),
            ),
            credential_type: CredentialType::Token(None),
            location: self.location.doc(),
            csrf_header: None,
        }
    }
    fn protected_resource(&self, audience: &AudienceId) -> Option<AuthProtectedResource> {
        Some(AuthProtectedResource {
            issuer: self.issuer.clone(),
            advertised_scopes: self
                .resources
                .get(audience)?
                .iter()
                .map(ToString::to_string)
                .collect(),
            required_scopes: Vec::new(),
        })
    }
    fn mcp_same_name_scopes(&self) -> bool {
        true
    }
    fn authenticate<'a>(
        &'a self,
        site: &'a Site,
        raw: &'a str,
        _: &'a Parts,
        audience: &'a AudienceId,
    ) -> BoxFuture<'a, Result<AuthUser, AuthError>> {
        Box::pin(self.authenticate_access(site, raw, audience))
    }
    fn login<'a>(
        &'a self,
        _: AuthUser,
        _: Vec<AudienceId>,
    ) -> BoxFuture<'a, Result<LoginResponse, AuthError>> {
        Box::pin(async { Err(AuthError::UnsupportedProviderCapability) })
    }
    fn refresh<'a>(
        &'a self,
        _: &'a Site,
        _: &'a str,
        _: &'a Parts,
    ) -> BoxFuture<'a, Result<LoginResponse, AuthError>> {
        Box::pin(async { Err(AuthError::UnsupportedProviderCapability) })
    }
    fn logout<'a>(&'a self, _: &'a Parts) -> BoxFuture<'a, Result<ResponseHeaders, AuthError>> {
        Box::pin(async { Err(AuthError::UnsupportedProviderCapability) })
    }
}

/// Validates the issuer origin and required browser route configuration.
fn issuer_urls(config: &OAuthIssuer) -> Result<(url::Url, &str, crate::auth::Audience), AuthError> {
    let origin = clean_url(&config.issuer, false)
        .filter(|url| {
            url.origin().ascii_serialization() == config.issuer && config.issuer.len() <= 512
        })
        .ok_or_else(|| invalid("OAuth issuer must be a canonical HTTPS origin without a path"))?;
    let authorize = config
        .authorize_route
        .as_deref()
        .filter(|name| bounded(name, 128))
        .ok_or_else(|| invalid("OAuth issuer requires a named authorization route"))?;
    let browser = config
        .browser_audience
        .ok_or_else(|| invalid("OAuth issuer requires a browser audience"))?;
    Ok((origin, authorize, browser))
}

/// Validates the immutable trust allowlist and prevents collisions with existing clients.
fn cimd_clients(
    config: &OAuthIssuer,
    static_clients: &BTreeMap<String, Client>,
) -> Result<BTreeMap<String, OAuthCimdClient>, AuthError> {
    if config.cimd.len() > 64 {
        return Err(invalid(
            "OAuth issuer supports at most 64 client metadata rules",
        ));
    }
    let mut clients = BTreeMap::new();
    for rule in &config.cimd {
        cimd::validate(rule)?;
        if static_clients.contains_key(&rule.client_id)
            || clients
                .insert(rule.client_id.clone(), rule.clone())
                .is_some()
        {
            return Err(invalid("duplicate OAuth client ID"));
        }
    }
    Ok(clients)
}

/// Prevents bearer access or a CSRF-less provider from becoming browser consent authority.
fn validate_browser(
    browser: crate::auth::Audience,
    providers: &[ProviderDefinitionInner],
) -> Result<(), AuthError> {
    let mut eligible = providers.iter().filter(|provider| {
        provider
            .declared_audiences()
            .is_none_or(|audiences| audiences.contains(&browser))
    });
    let valid = eligible.next().is_some_and(|provider| matches!(&provider.kind,
        ProviderKind::Token(token) if token.access.location.is_cookie() && token.access.csrf.is_some()));
    if !valid || eligible.next().is_some() {
        return Err(invalid(
            "OAuth browser audience needs exactly one CSRF-protected cookie token provider",
        ));
    }
    Ok(())
}

/// Configures only canonical same-origin resources with no competing provider.
fn resources(
    config: &OAuthIssuer,
    providers: &[ProviderDefinitionInner],
    origin: &url::Url,
) -> Result<BTreeMap<AudienceId, Vec<Scope>>, AuthError> {
    let mut resources = BTreeMap::new();
    for (audience, scopes) in &config.resources {
        let raw = audience.as_str();
        let valid_url = clean_url(raw, false).is_some_and(|url| {
            url.origin() == origin.origin()
                && url.as_str() == raw
                && url.query().is_none()
                && url.path() != "/"
                && !url.path().ends_with('/')
                && !url.path().contains("//")
                && !super::protocol::reserved_path(url.path(), config.dcr.is_some())
        });
        if !valid_url
            || providers.iter().any(|provider| {
                provider
                    .declared_audiences()
                    .is_none_or(|audiences| audiences.contains(audience))
            })
        {
            return Err(invalid(
                "OAuth resource must be a canonical same-origin URL exclusive to the issuer",
            ));
        }
        let mut scopes = scopes.clone();
        crate::scopes::validate_scopes(&scopes)
            .map_err(|_| invalid("invalid OAuth resource scopes"))?;
        scopes.sort_unstable();
        if scopes.windows(2).any(|pair| pair[0] == pair[1])
            || resources
                .insert(AudienceId::declared(*audience)?, scopes)
                .is_some()
        {
            return Err(invalid("duplicate OAuth resource or scope"));
        }
    }
    Ok(resources)
}

/// Resolves deployment credentials once and bounds the immutable client registry.
fn clients(
    config: &OAuthIssuer,
    secrets: &SecretRing,
) -> Result<BTreeMap<String, Client>, AuthError> {
    if config.clients.len() > 64
        || (config.clients.is_empty() && config.cimd.is_empty() && config.dcr.is_none())
    {
        return Err(invalid(
            "OAuth issuer needs a client policy and supports at most 64 static clients",
        ));
    }
    let mut clients = BTreeMap::new();
    for client in &config.clients {
        client.validate()?;
        let secret = client
            .secret
            .as_ref()
            .map(|source: &KeySource| {
                if source.is_site_secret() {
                    return Err(invalid(
                        "OAuth clients must not use the site signing secret",
                    ));
                }
                let secret = secrets.active(source)?;
                if !(32..=1024).contains(&secret.len()) {
                    return Err(invalid("OAuth client secret length is out of bounds"));
                }
                Ok(blake3::hash(&secret))
            })
            .transpose()?;
        if clients
            .insert(
                client.id.clone(),
                Client {
                    name: client.name.clone(),
                    redirects: client.redirects.clone(),
                    secret,
                },
            )
            .is_some()
        {
            return Err(invalid("duplicate OAuth client ID"));
        }
    }
    Ok(clients)
}

pub(super) fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

pub(super) fn random_credential() -> Result<String, AuthError> {
    let mut bytes = [0_u8; 32];
    fill_random(&mut bytes)?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

pub(super) fn fill_random(bytes: &mut [u8]) -> Result<(), AuthError> {
    SystemRandom::new()
        .fill(bytes)
        .map_err(|_| AuthError::Internal("OAuth credential generation failed".into()))
}

pub(super) fn credential_digest(raw: &str) -> Option<String> {
    if raw.len() != 43
        || !raw
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_".contains(&byte))
    {
        return None;
    }
    Some(blake3::hash(raw.as_bytes()).to_hex().to_string())
}
