//! Huskarl-backed external identity-token runtime.

use std::{sync::Arc, time::UNIX_EPOCH};

use axum::http::{HeaderMap, HeaderValue, header, request::Parts};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures::future::BoxFuture;
use huskarl_resource_server::{
    DefaultJwsVerifierPlatform,
    core::{
        EndpointUrl, Error as HuskarlError,
        crypto::verifier::{JwsVerifier, JwsVerifierFactory, JwsVerifierPlatform},
        http::HttpClient,
        jwk::JwksSource,
        jwt::validator::{ClaimCheck, JwtValidationError},
        platform::MaybeSendBoxFuture,
        server_metadata::AuthorizationServerMetadata,
    },
    error::{ToRfc6750Error, TokenValidationError},
    validator::{ValidatedRequest, custom::CustomValidator, error::ValidateHeadersError},
};
use serde::Deserialize;

use super::config::{ErasedIdTokenMapper, IdToken, IdTokenClaims};
use crate::auth::{
    AudienceId, AuthError, AuthUser, AuthenticationContext, CredentialLocation, ProviderId,
    oauth::http::{AuthHttpClient, validate_remote_url},
    runtime::contract::{
        ProviderAudienceSet, ProviderCapabilities, ProviderRuntimeContract, ResponseHeaders,
    },
};

const MAX_CREDENTIAL_BYTES: usize = 16 * 1024;
const MAX_CLAIMS_BYTES: usize = 16 * 1024;

#[derive(Clone, Deserialize)]
struct ExtraClaims {
    #[serde(default)]
    azp: Option<String>,
    #[serde(default)]
    auth_time: Option<i64>,
    #[serde(flatten)]
    _other: serde_json::Map<String, serde_json::Value>,
}

struct ResourceRuntime {
    audience: AudienceId,
    token_audience: String,
    validator: CustomValidator<ExtraClaims>,
}

pub(crate) struct IdTokenRuntime {
    id: ProviderId,
    mapper: Arc<dyn ErasedIdTokenMapper>,
    resources: Vec<ResourceRuntime>,
    location: CredentialLocation,
    csrf: Option<crate::auth::CsrfConf>,
    audiences: ProviderAudienceSet,
}

impl IdTokenRuntime {
    /// Builds discovery metadata, initial JWKS state, and audience validators.
    pub(crate) async fn build(id: ProviderId, conf: IdToken) -> Result<Self, AuthError> {
        let client = AuthHttpClient::build()
            .map_err(|error| startup_failure(&id, "HTTP client construction", error))?;
        Self::build_with_client(id, conf, client).await
    }

    /// Builds a runtime with an injectable transport for deterministic verification tests.
    async fn build_with_client<C>(
        id: ProviderId,
        conf: IdToken,
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
        let mapper = conf.mapper.ok_or_else(|| {
            AuthError::InvalidProviderConfig(
                "identity-token providers require an application mapper".into(),
            )
        })?;
        Ok(Self {
            id,
            mapper,
            resources,
            location: conf.location,
            csrf: conf.csrf,
            audiences,
        })
    }

    /// Verifies protocol claims before invoking the application identity mapper.
    async fn authenticate(
        &self,
        raw: &str,
        parts: &Parts,
        audience: &AudienceId,
    ) -> Result<AuthUser, AuthError> {
        validate_encoded(raw)?;
        if let Some(csrf) = &self.csrf {
            csrf.verify(parts)?;
        }
        let resource = self.resource(audience)?;
        let validated = validate(resource, raw, parts).await?;
        validate_authorized_party(&validated, &resource.token_audience)?;
        let authentication = authentication(&validated)?;
        let claims = normalize_claims(raw, validated)?;
        let user = self.mapper.map(&claims).await?;
        user.validate()?;
        Ok(user
            .set_provider(self.id.clone())
            .with_authentication(authentication))
    }

    fn resource(&self, audience: &AudienceId) -> Result<&ResourceRuntime, AuthError> {
        self.resources
            .iter()
            .find(|resource| resource.audience == *audience)
            .ok_or(AuthError::AudienceMismatch)
    }

    /// Validates any presented value before returning client-side logout attachments.
    fn logout_headers(&self, parts: &Parts) -> Result<ResponseHeaders, AuthError> {
        if let Some(raw) = self.location.extract(parts)? {
            validate_encoded(&raw)?;
            if let Some(csrf) = &self.csrf {
                csrf.verify(parts)?;
            }
        }
        let mut headers = Vec::with_capacity(2);
        if let Some(header) = self.location.clear()? {
            headers.push(header);
        }
        if let Some(csrf) = &self.csrf {
            headers.push(csrf.clear()?);
        }
        Ok(headers)
    }
}

async fn discover<C: HttpClient>(
    client: &C,
    issuer: &str,
) -> Result<AuthorizationServerMetadata, HuskarlError> {
    AuthorizationServerMetadata::oidc_fetch()
        .http_client(client)
        .issuer(issuer)
        .call()
        .await
}

/// Loads one bounded JWKS source shared by all local audience policies.
async fn shared_verifier<C: HttpClient + Clone + 'static>(
    client: &C,
    metadata: &AuthorizationServerMetadata,
) -> Result<Arc<dyn JwsVerifierFactory>, HuskarlError> {
    let jwks_uri = metadata.jwks_uri.as_ref().ok_or_else(|| {
        HuskarlError::from(huskarl_resource_server::core::ErrorKind::Protocol)
            .with_context("identity-provider metadata has no JWKS URI")
    })?;
    validate_remote_url(jwks_uri.as_uri().to_string().as_str()).map_err(|_| {
        HuskarlError::from(huskarl_resource_server::core::ErrorKind::Protocol)
            .with_context("identity-provider metadata has an invalid JWKS URI")
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

/// Builds exact external-audience validators for every configured route audience.
async fn build_resources(
    provider: &ProviderId,
    conf: &IdToken,
    metadata: &AuthorizationServerMetadata,
    factory: Arc<dyn JwsVerifierFactory>,
) -> Result<Vec<ResourceRuntime>, AuthError> {
    let mut output = Vec::with_capacity(conf.resources.len());
    for (audience, token_audience) in &conf.resources {
        let validator = build_validator(conf, metadata, factory.clone(), token_audience)
            .await
            .map_err(|error| startup_failure(provider, "resource validator construction", error))?;
        output.push(ResourceRuntime {
            audience: AudienceId::declared(*audience)?,
            token_audience: token_audience.clone(),
            validator,
        });
    }
    Ok(output)
}

/// Builds one algorithm-pinned identity-token validator.
async fn build_validator(
    conf: &IdToken,
    metadata: &AuthorizationServerMetadata,
    factory: Arc<dyn JwsVerifierFactory>,
    token_audience: &str,
) -> Result<CustomValidator<ExtraClaims>, HuskarlError> {
    let issuers = std::iter::once(conf.issuer.clone()).chain(conf.issuer_aliases.iter().cloned());
    CustomValidator::builder_from_metadata(metadata)
        .with_claims::<ExtraClaims>()
        .iss(ClaimCheck::require_any(issuers))
        .aud(token_audience)
        .require_exp(true)
        .require_iat(true)
        .require_jti(false)
        .allowed_signing_algorithms(vec!["RS256".to_owned()])
        .jws_verifier_factory(factory)
        .build()
        .await
}

/// Adapts an already extracted credential to Huskarl's header validator.
async fn validate(
    resource: &ResourceRuntime,
    raw: &str,
    parts: &Parts,
) -> Result<ValidatedRequest<ExtraClaims>, AuthError> {
    let mut headers = HeaderMap::new();
    let value =
        HeaderValue::try_from(format!("Bearer {raw}")).map_err(|_| AuthError::InvalidCredential)?;
    headers.insert(header::AUTHORIZATION, value);
    let result = resource
        .validator
        .validate_request(&headers, &parts.method, &parts.uri, None)
        .await
        .outcome
        .map_err(map_validation_error)?;
    result.ok_or(AuthError::InvalidCredential)
}

fn validate_encoded(raw: &str) -> Result<(), AuthError> {
    if raw.is_empty() || raw.len() > MAX_CREDENTIAL_BYTES {
        return Err(AuthError::InvalidCredential);
    }
    crate::auth::reject_duplicate_claims(raw).map_err(|_| AuthError::InvalidCredential)
}

fn validate_authorized_party(
    validated: &ValidatedRequest<ExtraClaims>,
    token_audience: &str,
) -> Result<(), AuthError> {
    let Some(authorized) = validated.claims.azp.as_deref() else {
        return if validated.aud.len() > 1 {
            Err(AuthError::InvalidCredential)
        } else {
            Ok(())
        };
    };
    if authorized.is_empty() || authorized.len() > 512 || token_audience.is_empty() {
        return Err(AuthError::InvalidCredential);
    }
    Ok(())
}

fn authentication(
    validated: &ValidatedRequest<ExtraClaims>,
) -> Result<AuthenticationContext, AuthError> {
    let auth_time = match validated.claims.auth_time {
        Some(value) => {
            Some(chrono::DateTime::from_timestamp(value, 0).ok_or(AuthError::InvalidCredential)?)
        }
        None => None,
    };
    Ok(AuthenticationContext::new(
        auth_time.map(|value| value.timestamp()),
        vec!["id_token".into()],
        None,
    ))
}

/// Converts verified protocol claims into the bounded application mapper view.
fn normalize_claims(
    raw: &str,
    validated: ValidatedRequest<ExtraClaims>,
) -> Result<IdTokenClaims, AuthError> {
    let issued_at = validated.iat.ok_or(AuthError::InvalidCredential)?;
    let expires_at = validated.exp.ok_or(AuthError::InvalidCredential)?;
    Ok(IdTokenClaims {
        subject: validated.sub.ok_or(AuthError::InvalidCredential)?,
        issuer: validated.iss.ok_or(AuthError::InvalidCredential)?,
        audiences: validated.aud,
        issued_at: system_timestamp(issued_at)?,
        expires_at: system_timestamp(expires_at)?,
        token_id: validated.jti,
        raw: raw_claims(raw)?,
    })
}

fn raw_claims(raw: &str) -> Result<serde_json::Map<String, serde_json::Value>, AuthError> {
    let claims = raw.split('.').nth(1).ok_or(AuthError::InvalidCredential)?;
    let bytes = URL_SAFE_NO_PAD
        .decode(claims)
        .map_err(|_| AuthError::InvalidCredential)?;
    if bytes.len() > MAX_CLAIMS_BYTES {
        return Err(AuthError::InvalidCredential);
    }
    serde_json::from_slice(&bytes).map_err(|_| AuthError::InvalidCredential)
}

fn system_timestamp(
    value: std::time::SystemTime,
) -> Result<chrono::DateTime<chrono::Utc>, AuthError> {
    let seconds = value
        .duration_since(UNIX_EPOCH)
        .map_err(|_| AuthError::InvalidCredential)?
        .as_secs();
    let seconds = i64::try_from(seconds).map_err(|_| AuthError::InvalidCredential)?;
    chrono::DateTime::from_timestamp(seconds, 0).ok_or(AuthError::InvalidCredential)
}

fn startup_failure(provider: &ProviderId, operation: &str, error: HuskarlError) -> AuthError {
    tracing::error!(provider = %provider, operation, error = ?error, "identity-token provider initialization failed");
    AuthError::InvalidProviderConfig(format!(
        "identity-token provider '{provider}' failed during {operation}"
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
        TokenValidationError::Server(_) => AuthError::ProviderUnavailable,
    }
}

impl ProviderRuntimeContract for IdTokenRuntime {
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
            logout: true,
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
            credential_type: crate::auth::CredentialType::Token(Some("JWT".into())),
            location: self.location.doc(),
            csrf_header: self.csrf.as_ref().map(|csrf| csrf.header_name.clone()),
        }
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

    fn logout<'a>(&'a self, parts: &'a Parts) -> BoxFuture<'a, Result<ResponseHeaders, AuthError>> {
        Box::pin(async move { self.logout_headers(parts) })
    }
}

#[cfg(test)]
mod tests;
