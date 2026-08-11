//! Opaque-key provider runtime.

use axum::http::request::Parts;
use futures::future::BoxFuture;

use super::{
    contract::{ProviderAudienceSet, ProviderCapabilities, ProviderRuntimeContract},
    validation::{clear_locations, validate_credential_size, validate_csrf, validate_subject},
};
use crate::auth::{
    AudienceId, AuthError, AuthKey, AuthUser, CredentialLocation, CredentialType, KeyRequest,
    LoginResponse, PresentedCredential, ProviderDoc, ProviderId,
};

#[derive(Clone)]
pub(super) struct KeyRuntime {
    pub(super) id: ProviderId,
    pub(super) definition: AuthKey,
    pub(super) audiences: ProviderAudienceSet,
}

impl ProviderRuntimeContract for KeyRuntime {
    fn id(&self) -> &ProviderId {
        &self.id
    }

    fn audiences(&self) -> &ProviderAudienceSet {
        &self.audiences
    }

    fn access_location(&self) -> &CredentialLocation {
        &self.definition.location
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

    fn openapi(&self) -> ProviderDoc {
        ProviderDoc {
            id: self.id.to_string(),
            audiences: self.audiences.restricted().map(|values| {
                values
                    .iter()
                    .map(|value| value.as_str().to_owned())
                    .collect()
            }),
            credential_type: CredentialType::Key,
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
    ) -> BoxFuture<'a, Result<LoginResponse, AuthError>> {
        Box::pin(async { Err(AuthError::UnsupportedProviderCapability) })
    }

    fn refresh<'a>(
        &'a self,
        _raw: &'a str,
        _parts: &'a Parts,
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

impl KeyRuntime {
    /// Validates one opaque credential and assigns this accepting provider to the identity.
    async fn authenticate(
        &self,
        raw: &str,
        parts: &Parts,
        audience: &AudienceId,
    ) -> Result<AuthUser, AuthError> {
        validate_credential_size(raw, self.definition.max_credential_bytes)?;
        validate_csrf(self.definition.csrf.as_ref(), parts)?;
        let request = KeyRequest::new(audience.as_str());
        let presented = PresentedCredential::new(raw);
        let user = self.definition.verifier.verify(&presented, request).await?;
        validate_subject(&user)?;
        Ok(user.set_provider(self.id.clone()))
    }

    /// Revokes a presented key when supported and returns validated client-state removals.
    async fn logout(
        &self,
        parts: &Parts,
    ) -> Result<Vec<(axum::http::HeaderName, axum::http::HeaderValue)>, AuthError> {
        if let Some(raw) = self.definition.location.extract(parts)? {
            validate_credential_size(&raw, self.definition.max_credential_bytes)?;
            validate_csrf(self.definition.csrf.as_ref(), parts)?;
            if let Some(lifecycle) = &self.definition.lifecycle {
                let credential = PresentedCredential::new(&raw);
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
