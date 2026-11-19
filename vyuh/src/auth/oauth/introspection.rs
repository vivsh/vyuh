//! Explicit remote OAuth token validation through the issuer's RFC 7662 endpoint.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::http::request::Parts;
use huskarl_resource_server::{
    core::{
        EndpointUrl, Error as HuskarlError,
        client_auth::ClientSecret,
        http::HttpClient,
        secrets::{ProvidedSecret, SecretString},
        server_metadata::AuthorizationServerMetadata,
    },
    error::{ToRfc6750Error, TokenValidationError},
    validator::{ValidatedRequest, introspection::IntrospectionValidator},
};
use serde_json::Value;

use crate::auth::AuthError;

use super::{
    config::{OAuthResource, invalid_introspection},
    http::validate_remote_url,
    runtime::ExtraClaims,
};

pub(super) type Introspection = IntrospectionValidator<ExtraClaims>;

/// Checks Bearer syntax without interpreting the token as a JWT or any other format.
pub(super) fn valid_bearer(raw: &str) -> bool {
    let value = raw.trim_end_matches('=');
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-._~+/".contains(&byte))
}

/// Selects only HTTPS endpoints that support the configured client-auth policy.
pub(super) fn endpoint(metadata: &AuthorizationServerMetadata) -> Result<&EndpointUrl, AuthError> {
    let endpoint = metadata
        .introspection_endpoint
        .as_ref()
        .ok_or_else(|| invalid_introspection("metadata has no introspection endpoint"))?;
    let uri = endpoint.as_uri();
    if uri.scheme_str() != Some("https") || validate_remote_url(&uri.to_string()).is_err() {
        return Err(invalid_introspection(
            "introspection endpoint must be a clean HTTPS URL",
        ));
    }
    if metadata
        .introspection_endpoint_auth_methods_supported
        .as_ref()
        .is_some_and(|methods| !methods.iter().any(|method| method == "client_secret_basic"))
    {
        return Err(invalid_introspection(
            "introspection endpoint does not support client_secret_basic",
        ));
    }
    Ok(endpoint)
}

/// Uses a resolved secret with Basic only; there is no credential-source I/O per token.
pub(super) fn client_auth(secret: SecretString) -> ClientSecret {
    ClientSecret::builder()
        .client_secret(ProvidedSecret::new(secret))
        .prefer_basic_auth(true)
        .build()
}

/// Configures JSON-only introspection without a JWT response verifier or a JWKS load.
pub(super) async fn build(
    issuer: &str,
    endpoint: &EndpointUrl,
    client: impl HttpClient + 'static,
    client_id: &str,
    auth: ClientSecret,
    resource: &OAuthResource,
) -> Result<Introspection, HuskarlError> {
    // builder_from_metadata would also attach the issuer's JWKS URI.
    IntrospectionValidator::builder()
        .with_claims::<ExtraClaims>()
        .issuer(issuer)
        .introspection_endpoint(endpoint.clone())
        .client_id(client_id)
        .client_auth(auth)
        .http_client(client)
        .aud(resource.token_audience.as_str())
        .build()
        .await
}

/// Authenticates the one selected request and checks claims before application mapping.
pub(super) async fn validate(
    validator: &Introspection,
    parts: &Parts,
    issuer: &str,
) -> Result<ValidatedRequest<ExtraClaims>, AuthError> {
    let mut token = validator
        .validate_request(&parts.headers, &parts.method, &parts.uri, None)
        .await
        .outcome
        .map_err(|error| match error.challenge().error {
            TokenValidationError::Client(_) => AuthError::InvalidCredential,
            TokenValidationError::Server { .. } => {
                tracing::warn!("OAuth introspection endpoint is unavailable");
                AuthError::ProviderUnavailable
            }
        })?
        .ok_or_else(|| {
            AuthError::Internal("OAuth credential disappeared during introspection".into())
        })?;
    validate_claims(&mut token, issuer, SystemTime::now())?;
    Ok(token)
}

/// Checks semantics not enforced by Huskarl's active, audience and binding checks.
fn validate_claims(
    token: &mut ValidatedRequest<ExtraClaims>,
    issuer: &str,
    now: SystemTime,
) -> Result<(), AuthError> {
    if token.cnf.is_some()
        || token.iss.as_deref().is_some_and(|value| value != issuer)
        || token.sub.as_deref().is_none_or(|sub| {
            sub.trim().is_empty() || sub.len() > 512 || sub.chars().any(char::is_control)
        })
    {
        return Err(AuthError::InvalidCredential);
    }
    if token.exp.is_some_and(|expiry| expiry <= now) {
        return Err(AuthError::ExpiredCredential);
    }
    let claims = &mut token.claims.other;
    if let Some(value) = claims.get("nbf") {
        let seconds = value.as_u64().ok_or(AuthError::ProviderUnavailable)?;
        let start = UNIX_EPOCH
            .checked_add(Duration::from_secs(seconds))
            .ok_or(AuthError::ProviderUnavailable)?;
        if start > now {
            return Err(AuthError::InvalidCredential);
        }
    }
    if let Some(value) = claims.get("token_type") {
        let kind = value.as_str().ok_or(AuthError::ProviderUnavailable)?;
        if !kind.eq_ignore_ascii_case("bearer") {
            return Err(AuthError::InvalidCredential);
        }
    }
    token.iss.get_or_insert_with(|| issuer.to_owned());
    normalize_extensions(claims);
    preserve_timestamp(claims, "exp", token.exp)?;
    preserve_timestamp(claims, "iat", token.iat)?;
    Ok(())
}

/// Does not expose introspection-envelope fields or credential-shaped extensions to mappers.
fn normalize_extensions(claims: &mut serde_json::Map<String, Value>) {
    for field in [
        "token",
        "access_token",
        "refresh_token",
        "id_token",
        "client_secret",
    ] {
        claims.remove(field);
    }
}

fn preserve_timestamp(
    claims: &mut serde_json::Map<String, Value>,
    name: &str,
    value: Option<SystemTime>,
) -> Result<(), AuthError> {
    if let Some(value) = value {
        let elapsed = value
            .duration_since(UNIX_EPOCH)
            .map_err(|_| AuthError::ProviderUnavailable)?;
        claims.insert(name.into(), elapsed.as_secs().into());
    }
    Ok(())
}
