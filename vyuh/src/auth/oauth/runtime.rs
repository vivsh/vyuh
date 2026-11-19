//! Huskarl-backed OAuth provider runtime.

#[cfg(test)]
#[path = "tests/introspection.rs"]
mod introspection_tests;
#[cfg(test)]
#[path = "tests/runtime.rs"]
mod tests;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::UNIX_EPOCH,
};

use axum::http::request::Parts;
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
        secrets::SecretString,
        server_metadata::AuthorizationServerMetadata,
    },
    error::{ToRfc6750Error, TokenValidationError},
    validator::{ValidatedRequest, custom::CustomValidator, error::ValidateHeadersError},
};
use serde::Deserialize;

use super::{
    config::{OAuthClaims, OAuthJwtAlgorithm, OAuthResource, OAuthResourceServer, parse_scopes},
    http::{AuthHttpClient, unsupported_discovery, validate_remote_url},
    introspection::{self, Introspection},
};
#[cfg(feature = "mcp")]
use crate::auth::AuthProtectedResource;
use crate::auth::{
    AudienceId, AuthError, AuthUser, AuthenticationContext, CredentialLocation, ProviderId,
    runtime::contract::{
        ProviderAudienceSet, ProviderCapabilities, ProviderRuntimeContract, ResponseHeaders,
    },
};

const MAX_OAUTH_CREDENTIAL_BYTES: usize = 16 * 1024;

#[derive(Clone, Deserialize)]
pub(super) struct ExtraClaims {
    #[serde(default)]
    scope: String,
    #[serde(flatten)]
    pub(super) other: serde_json::Map<String, serde_json::Value>,
}

struct ResourceRuntime {
    policy: OAuthResource,
    validator: Validator,
}

enum Validator {
    Jwt(Box<CustomValidator<ExtraClaims>>),
    Introspection(Box<Introspection>),
}

pub(crate) struct OAuthRuntime {
    id: ProviderId,
    issuer: String,
    mapper: Arc<dyn super::config::ErasedOAuthIdentityMapper>,
    resources: BTreeMap<AudienceId, ResourceRuntime>,
    location: CredentialLocation,
    audiences: ProviderAudienceSet,
}

impl OAuthRuntime {
    pub(crate) async fn build(
        id: ProviderId,
        conf: OAuthResourceServer,
        secret: Option<SecretString>,
    ) -> Result<Self, AuthError> {
        let client = if conf.introspection.is_some() {
            AuthHttpClient::introspection()
        } else {
            AuthHttpClient::build()
        }
        .map_err(|_| startup_failure(&id, "HTTP client construction"))?;
        Self::build_with_client(id, conf, client, secret).await
    }

    async fn build_with_client<C>(
        id: ProviderId,
        conf: OAuthResourceServer,
        client: C,
        secret: Option<SecretString>,
    ) -> Result<Self, AuthError>
    where
        C: HttpClient + Clone + 'static,
    {
        conf.validate()?;
        let metadata = discover(&client, &conf.issuer)
            .await
            .map_err(|_| startup_failure(&id, "metadata discovery"))?;
        let resources = if let Some(conf_mode) = &conf.introspection {
            let secret = secret.ok_or_else(|| startup_failure(&id, "introspection credentials"))?;
            build_introspection(&id, &conf, &metadata, client, &conf_mode.client_id, secret).await?
        } else {
            let factory = shared_verifier(&client, &metadata)
                .await
                .map_err(|_| startup_failure(&id, "initial JWKS load"))?;
            build_resources(&id, &conf, &metadata, factory).await?
        };
        let audiences = ProviderAudienceSet::only(resources.keys().cloned().collect())?;
        Ok(Self {
            id,
            issuer: conf.issuer,
            mapper: conf.mapper,
            resources,
            location: CredentialLocation::bearer(),
            audiences,
        })
    }

    fn resource(&self, audience: &AudienceId) -> Result<&ResourceRuntime, AuthError> {
        self.resources
            .get(audience)
            .ok_or(AuthError::AudienceMismatch)
    }

    async fn authenticate(
        &self,
        site: &crate::Site,
        raw: &str,
        parts: &Parts,
        audience: &AudienceId,
    ) -> Result<AuthUser, AuthError> {
        if raw.is_empty() || raw.len() > MAX_OAUTH_CREDENTIAL_BYTES {
            return Err(AuthError::InvalidCredential);
        }
        let resource = self.resource(audience)?;
        let (validated, auth_time) = match &resource.validator {
            Validator::Jwt(validator) => {
                crate::auth::reject_duplicate_claims(raw)
                    .map_err(|_| AuthError::InvalidCredential)?;
                let token = validate(validator, parts).await?;
                let time = token.iat.map(unix_timestamp).transpose()?;
                (token, time)
            }
            Validator::Introspection(validator) => {
                if !introspection::valid_bearer(raw) {
                    return Err(AuthError::InvalidCredential);
                }
                (
                    introspection::validate(validator, parts, &self.issuer).await?,
                    None,
                )
            }
        };
        let claims = normalize_claims(validated)?;
        require_scopes(&claims.scopes, &resource.policy.required)?;
        let user = self.mapper.map(site, &claims).await?;
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
        HuskarlError::new(
            huskarl_resource_server::core::RetryAdvice::No,
            "authorization-server metadata has no JWKS URI",
        )
    })?;
    validate_remote_url(jwks_uri.as_uri().to_string().as_str()).map_err(|_| {
        HuskarlError::new(
            huskarl_resource_server::core::RetryAdvice::No,
            "authorization-server metadata has an invalid JWKS URI",
        )
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
) -> Result<BTreeMap<AudienceId, ResourceRuntime>, AuthError> {
    let algorithms = conf
        .algorithms
        .as_deref()
        .unwrap_or(&[OAuthJwtAlgorithm::Rs256])
        .iter()
        .map(|algorithm| algorithm.name().to_owned())
        .collect::<Vec<_>>();
    let mut result = BTreeMap::new();
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
        .map_err(|_| startup_failure(provider, "resource validator construction"))?;
        result.insert(
            id,
            ResourceRuntime {
                policy: policy.clone(),
                validator: Validator::Jwt(Box::new(validator)),
            },
        );
    }
    Ok(result)
}

/// Builds configured introspection validators without fetching keys or probing the endpoint.
async fn build_introspection(
    provider: &ProviderId,
    conf: &OAuthResourceServer,
    metadata: &AuthorizationServerMetadata,
    client: impl HttpClient + Clone + 'static,
    client_id: &str,
    secret: SecretString,
) -> Result<BTreeMap<AudienceId, ResourceRuntime>, AuthError> {
    let endpoint = introspection::endpoint(metadata)?;
    let auth = introspection::client_auth(secret);
    let mut resources = BTreeMap::new();
    for (audience, policy) in &conf.resources {
        let validator = introspection::build(
            &conf.issuer,
            endpoint,
            client.clone(),
            client_id,
            auth.clone(),
            policy,
        )
        .await
        .map_err(|_| startup_failure(provider, "introspection validator construction"))?;
        resources.insert(
            AudienceId::declared(*audience)?,
            ResourceRuntime {
                policy: policy.clone(),
                validator: Validator::Introspection(Box::new(validator)),
            },
        );
    }
    Ok(resources)
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
    validator: &CustomValidator<ExtraClaims>,
    parts: &Parts,
) -> Result<ValidatedRequest<ExtraClaims>, AuthError> {
    let outcome = validator
        .validate_request(&parts.headers, &parts.method, &parts.uri, None)
        .await
        .outcome
        .map_err(map_validation_error)?;
    outcome
        .ok_or_else(|| AuthError::Internal("OAuth credential disappeared during validation".into()))
}

fn normalize_claims(validated: ValidatedRequest<ExtraClaims>) -> Result<OAuthClaims, AuthError> {
    let subject = validated.sub.ok_or(AuthError::InvalidCredential)?;
    let issuer = validated.iss.ok_or(AuthError::InvalidCredential)?;
    let scopes = parse_scopes(&validated.claims.scope)?;
    let mut raw = validated.claims.other;
    raw.insert("sub".into(), subject.clone().into());
    raw.insert("iss".into(), issuer.clone().into());
    raw.insert("aud".into(), serde_json::json!(&validated.aud));
    raw.insert("scope".into(), validated.claims.scope.into());
    Ok(OAuthClaims {
        subject,
        issuer,
        audiences: validated.aud,
        scopes,
        raw: serde_json::Value::Object(raw),
    })
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

fn startup_failure(provider: &ProviderId, operation: &str) -> AuthError {
    tracing::error!(provider = %provider, operation, "OAuth provider initialization failed");
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
    match error.challenge().error {
        TokenValidationError::Client(_) => AuthError::InvalidCredential,
        TokenValidationError::Server { .. } => {
            tracing::warn!("OAuth JWT verifier is unavailable");
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
            credential_type: crate::auth::CredentialType::Token(Some(
                if self
                    .resources
                    .values()
                    .any(|value| matches!(value.validator, Validator::Introspection(_)))
                {
                    "OAuth Bearer"
                } else {
                    "OAuth JWT"
                }
                .into(),
            )),
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

    fn authenticate<'a>(
        &'a self,
        site: &'a crate::Site,
        raw: &'a str,
        parts: &'a Parts,
        audience: &'a AudienceId,
    ) -> BoxFuture<'a, Result<AuthUser, AuthError>> {
        Box::pin(self.authenticate(site, raw, parts, audience))
    }

    fn login<'a>(
        &'a self,
        _: AuthUser,
        _: Vec<AudienceId>,
    ) -> BoxFuture<'a, Result<crate::auth::LoginResponse, AuthError>> {
        Box::pin(async { Err(AuthError::UnsupportedProviderCapability) })
    }

    fn refresh<'a>(
        &'a self,
        _site: &'a crate::Site,
        _: &'a str,
        _: &'a Parts,
    ) -> BoxFuture<'a, Result<crate::auth::LoginResponse, AuthError>> {
        Box::pin(async { Err(AuthError::UnsupportedProviderCapability) })
    }

    fn logout<'a>(&'a self, _: &'a Parts) -> BoxFuture<'a, Result<ResponseHeaders, AuthError>> {
        Box::pin(async { Err(AuthError::UnsupportedProviderCapability) })
    }
}
