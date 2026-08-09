//! Huskarl-backed OAuth provider runtime.

#[cfg(test)]
mod tests;

use std::{collections::BTreeSet, sync::Arc, time::UNIX_EPOCH};

use axum::http::request::Parts;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures::future::BoxFuture;
use huskarl_resource_server::{
    DefaultJwsVerifierPlatform,
    core::{
        EndpointUrl, Error as HuskarlError,
        crypto::verifier::{JwsVerifier, JwsVerifierFactory, JwsVerifierPlatform},
        http::HttpClient,
        jwk::JwksSource,
        jwt::validator::JwtValidationError,
        platform::MaybeSendBoxFuture,
        server_metadata::AuthorizationServerMetadata,
    },
    error::{ToRfc6750Error, TokenValidationError},
    validator::{ValidatedRequest, custom::CustomValidator, error::ValidateHeadersError},
};
use serde::Deserialize;

use super::{
    config::{OAuthClaims, OAuthResource, OAuthResourceServer, parse_scopes},
    http::{AuthHttpClient, unsupported_discovery, validate_remote_url},
};
use crate::auth::{
    AudienceId, AuthError, AuthUser, AuthenticationContext, CredentialLocation, ProviderId,
    runtime::contract::{
        ProviderAudienceSet, ProviderCapabilities, ProviderRuntimeContract, ResponseHeaders,
    },
};
#[cfg(feature = "mcp")]
use crate::auth::{AuthChallenge, AuthProtectedResource};

const MAX_OAUTH_CREDENTIAL_BYTES: usize = 16 * 1024;

#[derive(Clone, Deserialize)]
struct ExtraClaims {
    #[serde(default)]
    scope: String,
    #[serde(flatten)]
    _other: serde_json::Map<String, serde_json::Value>,
}

struct ResourceRuntime {
    audience: AudienceId,
    policy: OAuthResource,
    validator: CustomValidator<ExtraClaims>,
}

pub(crate) struct OAuthRuntime {
    id: ProviderId,
    #[cfg(feature = "mcp")]
    issuer: String,
    mapper: Arc<dyn super::config::ErasedOAuthIdentityMapper>,
    resources: Vec<ResourceRuntime>,
    location: CredentialLocation,
    audiences: ProviderAudienceSet,
}

impl OAuthRuntime {
    pub(crate) async fn build(
        id: ProviderId,
        conf: OAuthResourceServer,
    ) -> Result<Self, AuthError> {
        let client = AuthHttpClient::build()
            .map_err(|error| startup_failure(&id, "HTTP client construction", error))?;
        Self::build_with_client(id, conf, client).await
    }

    async fn build_with_client<C>(
        id: ProviderId,
        conf: OAuthResourceServer,
        client: C,
    ) -> Result<Self, AuthError>
    where
        C: HttpClient + Clone + 'static,
    {
        conf.validate()?;
        let metadata = discover(&client, &conf.issuer)
            .await
            .map_err(|error| startup_failure(&id, "metadata discovery", error))?;
        let factory = shared_verifier(&client, &metadata)
            .await
            .map_err(|error| startup_failure(&id, "initial JWKS load", error))?;
        let resources = build_resources(&id, &conf, &metadata, factory).await?;
        let audiences = ProviderAudienceSet::only(
            resources
                .iter()
                .map(|resource| resource.audience.clone())
                .collect(),
        )?;
        Ok(Self {
            id,
            #[cfg(feature = "mcp")]
            issuer: conf.issuer.clone(),
            mapper: conf.mapper,
            resources,
            location: CredentialLocation::bearer(),
            audiences,
        })
    }

    fn resource(&self, audience: &AudienceId) -> Result<&ResourceRuntime, AuthError> {
        self.resources
            .iter()
            .find(|resource| resource.audience == *audience)
            .ok_or(AuthError::AudienceMismatch)
    }

    async fn authenticate(
        &self,
        raw: &str,
        parts: &Parts,
        audience: &AudienceId,
    ) -> Result<AuthUser, AuthError> {
        if raw.is_empty() || raw.len() > MAX_OAUTH_CREDENTIAL_BYTES {
            return Err(AuthError::InvalidCredential);
        }
        crate::auth::reject_duplicate_claims(raw).map_err(|_| AuthError::InvalidCredential)?;
        let resource = self.resource(audience)?;
        let validated = validate(resource, parts).await?;
        let auth_time = validated.iat.map(unix_timestamp).transpose()?;
        let claims = normalize_claims(raw, validated)?;
        require_scopes(&claims.scopes, &resource.policy.required)?;
        let user = self.mapper.map(&claims).await?;
        user.validate()?;
        Ok(user
            .set_provider(self.id.clone())
            .with_authentication(AuthenticationContext::new(
                auth_time,
                vec!["oauth".into()],
                None,
            )))
    }
}

async fn discover<C: HttpClient>(
    client: &C,
    issuer: &str,
) -> Result<AuthorizationServerMetadata, HuskarlError> {
    let result = AuthorizationServerMetadata::fetch()
        .http_client(client)
        .issuer(issuer)
        .call()
        .await;
    match result {
        Err(error) if unsupported_discovery(&error) => {
            AuthorizationServerMetadata::oidc_fetch()
                .http_client(client)
                .issuer(issuer)
                .call()
                .await
        }
        other => other,
    }
}

async fn shared_verifier<C: HttpClient + Clone + 'static>(
    client: &C,
    metadata: &AuthorizationServerMetadata,
) -> Result<Arc<dyn JwsVerifierFactory>, HuskarlError> {
    let jwks_uri = metadata.jwks_uri.as_ref().ok_or_else(|| {
        HuskarlError::from(huskarl_resource_server::core::ErrorKind::Protocol)
            .with_context("authorization-server metadata has no JWKS URI")
    })?;
    validate_remote_url(jwks_uri.as_uri().to_string().as_str()).map_err(|_| {
        HuskarlError::from(huskarl_resource_server::core::ErrorKind::Protocol)
            .with_context("authorization-server metadata has an invalid JWKS URI")
    })?;
    let platform: Arc<dyn JwsVerifierPlatform> = DefaultJwsVerifierPlatform::default().into();
    let source = JwksSource::builder().http_client(client.clone()).build();
    let verifier = source.build(Some(jwks_uri), platform).await?;
    Ok(Arc::new(SharedVerifierFactory(verifier)))
}

struct SharedVerifierFactory(Arc<dyn JwsVerifier>);

impl JwsVerifierFactory for SharedVerifierFactory {
    fn build(
        &self,
        _jwks_uri: Option<&EndpointUrl>,
        _platform: Arc<dyn JwsVerifierPlatform>,
    ) -> MaybeSendBoxFuture<'static, Result<Arc<dyn JwsVerifier>, HuskarlError>> {
        let verifier = self.0.clone();
        Box::pin(async move { Ok(verifier) })
    }
}

async fn build_resources(
    provider: &ProviderId,
    conf: &OAuthResourceServer,
    metadata: &AuthorizationServerMetadata,
    factory: Arc<dyn JwsVerifierFactory>,
) -> Result<Vec<ResourceRuntime>, AuthError> {
    let algorithms = conf
        .algorithms
        .iter()
        .map(|algorithm| algorithm.name().to_owned())
        .collect::<Vec<_>>();
    let mut result = Vec::with_capacity(conf.resources.len());
    for (audience, policy) in &conf.resources {
        let id = AudienceId::declared(*audience)?;
        let validator = build_validator(
            conf,
            metadata,
            factory.clone(),
            &policy.token_audience,
            &algorithms,
        )
        .await
        .map_err(|error| startup_failure(provider, "resource validator construction", error))?;
        result.push(ResourceRuntime {
            audience: id,
            policy: policy.clone(),
            validator,
        });
    }
    Ok(result)
}

async fn build_validator(
    conf: &OAuthResourceServer,
    metadata: &AuthorizationServerMetadata,
    factory: Arc<dyn JwsVerifierFactory>,
    token_audience: &str,
    algorithms: &[String],
) -> Result<CustomValidator<ExtraClaims>, HuskarlError> {
    CustomValidator::builder_from_metadata(metadata)
        .with_claims::<ExtraClaims>()
        .iss(conf.issuer.clone())
        .aud(token_audience)
        .require_exp(true)
        .require_iat(false)
        .require_jti(false)
        .allowed_signing_algorithms(algorithms.to_vec())
        .jws_verifier_factory(factory)
        .build()
        .await
}

async fn validate(
    resource: &ResourceRuntime,
    parts: &Parts,
) -> Result<ValidatedRequest<ExtraClaims>, AuthError> {
    let outcome = resource
        .validator
        .validate_request(&parts.headers, &parts.method, &parts.uri, None)
        .await
        .outcome
        .map_err(map_validation_error)?;
    outcome
        .ok_or_else(|| AuthError::Internal("OAuth credential disappeared during validation".into()))
}

fn normalize_claims(
    token: &str,
    validated: ValidatedRequest<ExtraClaims>,
) -> Result<OAuthClaims, AuthError> {
    let subject = validated.sub.ok_or(AuthError::InvalidCredential)?;
    let issuer = validated.iss.ok_or(AuthError::InvalidCredential)?;
    let scopes = parse_scopes(&validated.claims.scope)?;
    let raw = raw_claims(token)?;
    Ok(OAuthClaims {
        subject,
        issuer,
        audiences: validated.aud,
        scopes,
        raw,
    })
}

fn raw_claims(token: &str) -> Result<serde_json::Value, AuthError> {
    let mut segments = token.split('.');
    let _header = segments.next().ok_or(AuthError::InvalidCredential)?;
    let claims = segments.next().ok_or(AuthError::InvalidCredential)?;
    let bytes = URL_SAFE_NO_PAD
        .decode(claims)
        .map_err(|_| AuthError::InvalidCredential)?;
    serde_json::from_slice(&bytes).map_err(|_| AuthError::InvalidCredential)
}

fn require_scopes(actual: &BTreeSet<String>, required: &BTreeSet<String>) -> Result<(), AuthError> {
    if required.is_subset(actual) {
        Ok(())
    } else {
        Err(AuthError::InsufficientScope)
    }
}

fn unix_timestamp(value: std::time::SystemTime) -> Result<i64, AuthError> {
    let duration = value
        .duration_since(UNIX_EPOCH)
        .map_err(|_| AuthError::InvalidCredential)?;
    i64::try_from(duration.as_secs()).map_err(|_| AuthError::InvalidCredential)
}

fn startup_failure(provider: &ProviderId, operation: &str, error: HuskarlError) -> AuthError {
    tracing::error!(provider = %provider, operation, error = ?error, "OAuth provider initialization failed");
    AuthError::InvalidProviderConfig(format!(
        "OAuth provider '{provider}' failed during {operation}"
    ))
}

fn map_validation_error(error: ValidateHeadersError) -> AuthError {
    if matches!(
        error,
        ValidateHeadersError::InvalidJwt {
            source: JwtValidationError::Expired { .. },
            ..
        }
    ) {
        return AuthError::ExpiredCredential;
    }
    match error.token_error() {
        TokenValidationError::Client(_) => AuthError::InvalidCredential,
        TokenValidationError::Server(_) => {
            tracing::warn!(error = ?error, "OAuth token verifier is unavailable");
            AuthError::ProviderUnavailable
        }
    }
}

impl ProviderRuntimeContract for OAuthRuntime {
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

    fn openapi(&self) -> crate::auth::ProviderDoc {
        crate::auth::ProviderDoc {
            id: self.id.to_string(),
            audiences: self.audiences.restricted().map(|values| {
                values
                    .iter()
                    .map(|value| value.as_str().to_owned())
                    .collect()
            }),
            credential_type: crate::auth::CredentialType::Token(Some("OAuth JWT".into())),
            location: self.location.doc(),
            csrf_header: None,
        }
    }

    #[cfg(feature = "mcp")]
    fn protected_resource(&self, audience: &AudienceId) -> Option<AuthProtectedResource> {
        self.resource(audience)
            .ok()
            .map(|resource| AuthProtectedResource {
                issuer: self.issuer.clone(),
                advertised_scopes: resource.policy.advertised.iter().cloned().collect(),
                required_scopes: resource.policy.required.iter().cloned().collect(),
            })
    }

    #[cfg(feature = "mcp")]
    fn challenge(&self, _error: &AuthError) -> Option<AuthChallenge> {
        Some(AuthChallenge { scheme: "Bearer" })
    }

    fn authenticate<'a>(
        &'a self,
        raw: &'a str,
        parts: &'a Parts,
        audience: &'a AudienceId,
    ) -> BoxFuture<'a, Result<AuthUser, AuthError>> {
        Box::pin(self.authenticate(raw, parts, audience))
    }

    fn login<'a>(
        &'a self,
        _: AuthUser,
        _: Vec<AudienceId>,
        _: Option<String>,
    ) -> BoxFuture<'a, Result<crate::auth::LoginResponse, AuthError>> {
        Box::pin(async { Err(AuthError::UnsupportedProviderCapability) })
    }

    fn refresh<'a>(
        &'a self,
        _: &'a str,
        _: &'a Parts,
        _: &'a [AudienceId],
    ) -> BoxFuture<'a, Result<crate::auth::LoginResponse, AuthError>> {
        Box::pin(async { Err(AuthError::UnsupportedProviderCapability) })
    }

    fn logout<'a>(&'a self, _: &'a Parts) -> BoxFuture<'a, Result<ResponseHeaders, AuthError>> {
        Box::pin(async { Err(AuthError::UnsupportedProviderCapability) })
    }
}
