//! Public OAuth resource-server configuration and identity mapping.

use std::{collections::BTreeSet, future::Future, sync::Arc};

use futures::future::BoxFuture;
use huskarl_resource_server::core::secrets::SecretString;

use super::{
    super::{Audience, AudienceId, AuthError, AuthUser, CredentialLocation, KeySource, SecretRing},
    http::validate_remote_url,
};

const MAX_OAUTH_SCOPES: usize = 128;
const MAX_OAUTH_SCOPE_BYTES: usize = 8 * 1024;
const MAX_OAUTH_RESOURCES: usize = 64;

/// JWT algorithms accepted from an OAuth authorization server.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum OAuthJwtAlgorithm {
    /// RSA PKCS#1 v1.5 with SHA-256.
    Rs256,
    /// RSA PKCS#1 v1.5 with SHA-384.
    Rs384,
    /// RSA PKCS#1 v1.5 with SHA-512.
    Rs512,
    /// ECDSA P-256 with SHA-256.
    Es256,
    /// ECDSA P-384 with SHA-384.
    Es384,
    /// EdDSA with an Ed25519 key.
    EdDsa,
}

impl OAuthJwtAlgorithm {
    pub(super) const fn name(self) -> &'static str {
        match self {
            Self::Rs256 => "RS256",
            Self::Rs384 => "RS384",
            Self::Rs512 => "RS512",
            Self::Es256 => "ES256",
            Self::Es384 => "ES384",
            Self::EdDsa => "EdDSA",
        }
    }
}

/// Validated external OAuth claims from local JWT verification or token introspection.
#[derive(Clone)]
pub struct OAuthClaims {
    /// Stable upstream subject identifier.
    pub subject: String,
    /// Validated authorization-server issuer.
    pub issuer: String,
    /// Validated token audiences.
    pub audiences: Vec<String>,
    /// Upstream OAuth scopes, distinct from Vyuh application scopes.
    pub scopes: BTreeSet<String>,
    /// Validated standard claims and bounded upstream extensions for mapping.
    /// This is not the raw credential or the introspection response body.
    pub raw: serde_json::Value,
}

impl std::fmt::Debug for OAuthClaims {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OAuthClaims")
            .field("audience_count", &self.audiences.len())
            .field("scope_count", &self.scopes.len())
            .finish_non_exhaustive()
    }
}

/// Resolves a validated external OAuth identity into a Vyuh identity.
pub trait OAuthIdentityMapper: Send + Sync + 'static {
    /// Maps authenticated upstream claims without weakening token validation.
    /// Borrows the invoking site for application account resolution.
    fn map<'a>(
        &'a self,
        site: &'a crate::Site,
        claims: &'a OAuthClaims,
    ) -> impl Future<Output = Result<AuthUser, AuthError>> + Send + 'a;
}

pub(super) trait ErasedOAuthIdentityMapper: Send + Sync {
    fn map<'a>(
        &'a self,
        site: &'a crate::Site,
        claims: &'a OAuthClaims,
    ) -> BoxFuture<'a, Result<AuthUser, AuthError>>;
}

impl<T: OAuthIdentityMapper> ErasedOAuthIdentityMapper for T {
    fn map<'a>(
        &'a self,
        site: &'a crate::Site,
        claims: &'a OAuthClaims,
    ) -> BoxFuture<'a, Result<AuthUser, AuthError>> {
        Box::pin(OAuthIdentityMapper::map(self, site, claims))
    }
}

#[derive(Debug)]
struct SubjectMapper;

impl OAuthIdentityMapper for SubjectMapper {
    async fn map(&self, _site: &crate::Site, claims: &OAuthClaims) -> Result<AuthUser, AuthError> {
        Ok(AuthUser::new(&claims.subject))
    }
}

/// Upstream OAuth scopes advertised and required for one Vyuh audience.
#[derive(Clone, Debug)]
pub struct OAuthResource {
    pub(super) token_audience: String,
    pub(super) advertised: BTreeSet<String>,
    pub(super) required: BTreeSet<String>,
}

impl OAuthResource {
    /// Creates a policy requiring one exact protocol-level token audience.
    pub fn new(token_audience: impl Into<String>) -> Self {
        Self {
            token_audience: token_audience.into(),
            advertised: BTreeSet::new(),
            required: BTreeSet::new(),
        }
    }

    /// Adds scopes advertised by protected-resource metadata.
    pub fn advertise_scopes(mut self, values: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.advertised.extend(values.into_iter().map(Into::into));
        self
    }

    /// Adds upstream OAuth scopes required before identity mapping.
    pub fn require_scopes(mut self, values: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let values = values.into_iter().map(Into::into).collect::<Vec<_>>();
        self.required.extend(values.iter().cloned());
        self.advertised.extend(values);
        self
    }

    pub(super) fn validate(&self) -> Result<(), AuthError> {
        validate_token_audience(&self.token_audience)?;
        validate_scope_set(&self.advertised)?;
        validate_scope_set(&self.required)?;
        Ok(())
    }
}

#[derive(Clone)]
pub(super) struct IntrospectionConf {
    pub(super) client_id: String,
    client_secret: KeySource,
}

/// A discovery-backed, verify-only OAuth resource server for external access tokens.
#[derive(Clone)]
pub struct OAuthResourceServer {
    pub(super) issuer: String,
    pub(super) resources: Vec<(Audience, OAuthResource)>,
    pub(super) algorithms: Option<Vec<OAuthJwtAlgorithm>>,
    pub(super) introspection: Option<IntrospectionConf>,
    pub(super) mapper: Arc<dyn ErasedOAuthIdentityMapper>,
    location: CredentialLocation,
}

impl std::fmt::Debug for OAuthResourceServer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OAuthResourceServer")
            .field("resources", &self.resources.len())
            .field("validation", &self.format())
            .finish_non_exhaustive()
    }
}

impl OAuthResourceServer {
    /// Creates a discovery-backed, verify-only OAuth provider.
    pub fn discovery(issuer: impl Into<String>) -> Self {
        Self {
            issuer: issuer.into(),
            resources: Vec::new(),
            algorithms: None,
            introspection: None,
            mapper: Arc::new(SubjectMapper),
            location: CredentialLocation::bearer(),
        }
    }

    /// Adds an audience-specific protected-resource policy.
    pub fn resource(mut self, audience: Audience, resource: OAuthResource) -> Self {
        self.resources.push((audience, resource));
        self
    }

    /// Replaces the accepted asymmetric JWT algorithm allowlist.
    pub fn algorithms(mut self, values: impl IntoIterator<Item = OAuthJwtAlgorithm>) -> Self {
        self.algorithms = Some(values.into_iter().collect());
        self
    }

    /// Validates access tokens at the discovered HTTPS introspection endpoint using
    /// dedicated client credentials. Cannot be combined with a JWT algorithm list.
    pub fn introspection(mut self, client_id: impl Into<String>, secret: KeySource) -> Self {
        self.introspection = Some(IntrospectionConf {
            client_id: client_id.into(),
            client_secret: secret,
        });
        self
    }

    /// Replaces the application identity mapper.
    pub fn mapper(mut self, mapper: impl OAuthIdentityMapper) -> Self {
        self.mapper = Arc::new(mapper);
        self
    }

    pub(crate) fn location(&self) -> &CredentialLocation {
        &self.location
    }

    /// Iterates the local route audiences owned by this resource server.
    pub(crate) fn route_audiences(&self) -> impl Iterator<Item = Audience> + '_ {
        self.resources.iter().map(|item| item.0)
    }

    pub(crate) fn validate(&self) -> Result<(), AuthError> {
        validate_remote_url(&self.issuer)?;
        if let Some(conf) = &self.introspection {
            if self.algorithms.is_some() {
                return Err(invalid_introspection(
                    "introspection cannot use JWT algorithms",
                ));
            }
            if !self.issuer.starts_with("https://")
                || conf.client_secret.is_site_secret()
                || !valid_client_value(&conf.client_id, 256)
            {
                return Err(invalid_introspection(
                    "introspection requires HTTPS and dedicated client credentials",
                ));
            }
        } else if let Some(algorithms) = &self.algorithms {
            validate_algorithms(algorithms)?;
        }
        validate_resources(&self.resources)
    }

    pub(crate) fn format(&self) -> &'static str {
        if self.introspection.is_some() {
            "OAuth Bearer"
        } else {
            "OAuth JWT"
        }
    }

    /// Resolves dedicated credentials on the blocking startup worker; never logs key-source details.
    pub(crate) fn prepare_secret(
        &self,
        secrets: &SecretRing,
    ) -> Result<Option<SecretString>, AuthError> {
        self.validate()?;
        let Some(conf) = &self.introspection else {
            return Ok(None);
        };
        let invalid = || invalid_introspection("invalid introspection client secret");
        let bytes = secrets.active(&conf.client_secret).map_err(|_| invalid())?;
        let secret = std::str::from_utf8(&bytes)
            .ok()
            .filter(|secret| valid_client_value(secret, 1024))
            .ok_or_else(invalid)?;
        Ok(Some(SecretString::new(secret)))
    }
}

fn valid_client_value(value: &str, max: usize) -> bool {
    !value.trim().is_empty() && value.len() <= max && !value.chars().any(char::is_control)
}

pub(super) fn invalid_introspection(reason: &str) -> AuthError {
    AuthError::InvalidProviderConfig(format!("OAuth {reason}"))
}

fn validate_token_audience(value: &str) -> Result<(), AuthError> {
    if value.is_empty()
        || value.len() > 512
        || value
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        return Err(AuthError::InvalidProviderConfig(
            "OAuth token audiences must be non-empty bounded strings".into(),
        ));
    }
    Ok(())
}

fn validate_algorithms(values: &[OAuthJwtAlgorithm]) -> Result<(), AuthError> {
    let unique = values.iter().copied().collect::<BTreeSet<_>>();
    if values.is_empty() || unique.len() != values.len() {
        return Err(AuthError::InvalidProviderConfig(
            "OAuth algorithms must be non-empty and unique".into(),
        ));
    }
    Ok(())
}

fn validate_resources(values: &[(Audience, OAuthResource)]) -> Result<(), AuthError> {
    if values.is_empty() || values.len() > MAX_OAUTH_RESOURCES {
        return Err(AuthError::InvalidProviderConfig(
            "OAuth providers require between 1 and 64 resources".into(),
        ));
    }
    let mut audiences = BTreeSet::new();
    for (audience, resource) in values {
        let id = AudienceId::declared(*audience)?;
        if !audiences.insert(id.as_str().to_owned()) {
            return Err(AuthError::InvalidProviderConfig(
                "OAuth resource audiences must be unique".into(),
            ));
        }
        resource.validate()?;
    }
    Ok(())
}

pub(super) fn validate_scope_set(values: &BTreeSet<String>) -> Result<(), AuthError> {
    let bytes = values.iter().map(String::len).sum::<usize>() + values.len().saturating_sub(1);
    if values.len() > MAX_OAUTH_SCOPES
        || bytes > MAX_OAUTH_SCOPE_BYTES
        || values.iter().any(|value| !valid_scope(value))
    {
        return Err(AuthError::InvalidProviderConfig(
            "OAuth scopes exceed the supported syntax or bounds".into(),
        ));
    }
    Ok(())
}

pub(super) fn parse_scopes(value: &str) -> Result<BTreeSet<String>, AuthError> {
    if value.len() > MAX_OAUTH_SCOPE_BYTES {
        return Err(AuthError::InvalidCredential);
    }
    let scopes = value
        .split_ascii_whitespace()
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    validate_scope_set(&scopes).map_err(|_| AuthError::InvalidCredential)?;
    Ok(scopes)
}

fn valid_scope(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.bytes().all(|byte| {
            byte == b'!' || (b'#'..=b'[').contains(&byte) || (b']'..=b'~').contains(&byte)
        })
}

#[cfg(test)]
#[path = "tests/config.rs"]
mod tests;
