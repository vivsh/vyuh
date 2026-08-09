//! Public OAuth resource-server configuration and identity mapping.

use std::{collections::BTreeSet, future::Future, sync::Arc};

use futures::future::BoxFuture;

use super::{
    super::{Audience, AudienceId, AuthError, AuthUser, CredentialLocation},
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

/// Validated OAuth JWT claims available to an application identity mapper.
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
    /// Authenticated JWT claims for application-specific mapping.
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
    fn map(&self, claims: &OAuthClaims)
    -> impl Future<Output = Result<AuthUser, AuthError>> + Send;
}

pub(super) trait ErasedOAuthIdentityMapper: Send + Sync {
    fn map<'a>(&'a self, claims: &'a OAuthClaims) -> BoxFuture<'a, Result<AuthUser, AuthError>>;
}

impl<T: OAuthIdentityMapper> ErasedOAuthIdentityMapper for T {
    fn map<'a>(&'a self, claims: &'a OAuthClaims) -> BoxFuture<'a, Result<AuthUser, AuthError>> {
        Box::pin(OAuthIdentityMapper::map(self, claims))
    }
}

#[derive(Debug)]
struct SubjectMapper;

impl OAuthIdentityMapper for SubjectMapper {
    async fn map(&self, claims: &OAuthClaims) -> Result<AuthUser, AuthError> {
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

/// A discovery-backed OAuth resource-server provider for JWT access tokens.
#[derive(Clone)]
pub struct OAuthResourceServer {
    pub(super) issuer: String,
    pub(super) resources: Vec<(Audience, OAuthResource)>,
    pub(super) algorithms: Vec<OAuthJwtAlgorithm>,
    pub(super) mapper: Arc<dyn ErasedOAuthIdentityMapper>,
    location: CredentialLocation,
}

impl std::fmt::Debug for OAuthResourceServer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OAuthResourceServer")
            .field("issuer", &self.issuer)
            .field("resources", &self.resources.len())
            .field("algorithms", &self.algorithms)
            .finish_non_exhaustive()
    }
}

impl OAuthResourceServer {
    /// Creates a discovery-backed, verify-only OAuth provider.
    pub fn discovery(issuer: impl Into<String>) -> Self {
        Self {
            issuer: issuer.into(),
            resources: Vec::new(),
            algorithms: vec![OAuthJwtAlgorithm::Rs256],
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
        self.algorithms = values.into_iter().collect();
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
        validate_algorithms(&self.algorithms)?;
        validate_resources(&self.resources)
    }
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
mod tests {
    use super::*;

    const MCP: Audience = Audience::new("https://api.example.com/mcp");

    /// Required scopes are automatically advertised by the protected resource.
    #[test]
    fn required_scopes_are_advertised() {
        let resource = OAuthResource::new(MCP.as_str()).require_scopes(["mcp"]);
        assert!(resource.advertised.contains("mcp"));
        assert!(resource.validate().is_ok());
    }

    /// A complete resource policy accepts multiple valid upstream scopes.
    #[test]
    fn accepts_multiple_advertised_and_required_scopes() {
        let result = OAuthResourceServer::discovery("https://auth.example.com").resource(
            MCP,
            OAuthResource::new(MCP.as_str())
                .advertise_scopes(["mcp", "mcp.read"])
                .require_scopes(["mcp", "mcp.read"]),
        );
        assert!(result.validate().is_ok());
    }

    /// Duplicate audiences are rejected during terminal provider validation.
    #[test]
    fn rejects_duplicate_resource_audiences() {
        let result = OAuthResourceServer::discovery("https://auth.example.com")
            .resource(MCP, OAuthResource::new(MCP.as_str()))
            .resource(MCP, OAuthResource::new(MCP.as_str()))
            .validate();
        assert!(result.is_err());
    }

    /// Public algorithm choices map exactly to the identifiers accepted by Huskarl.
    #[test]
    fn maps_supported_algorithms() {
        let values = [
            (OAuthJwtAlgorithm::Rs256, "RS256"),
            (OAuthJwtAlgorithm::Rs384, "RS384"),
            (OAuthJwtAlgorithm::Rs512, "RS512"),
            (OAuthJwtAlgorithm::Es256, "ES256"),
            (OAuthJwtAlgorithm::Es384, "ES384"),
            (OAuthJwtAlgorithm::EdDsa, "EdDSA"),
        ];
        for (algorithm, expected) in values {
            assert_eq!(algorithm.name(), expected);
        }
    }

    /// The safe default mapper never copies upstream OAuth scopes into the user.
    #[tokio::test]
    async fn default_mapper_grants_no_application_scopes() -> Result<(), AuthError> {
        let claims = OAuthClaims {
            subject: "oauth-user".to_string(),
            issuer: "https://auth.example.com".to_string(),
            audiences: vec![MCP.as_str().to_string()],
            scopes: BTreeSet::from(["upstream:read".to_string()]),
            raw: serde_json::Value::Null,
        };
        let user = OAuthIdentityMapper::map(&SubjectMapper, &claims).await?;
        assert!(user.scopes().is_empty());
        Ok(())
    }
}
