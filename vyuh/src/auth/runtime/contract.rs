//! Private runtime contract shared by framework-owned credential providers.

use std::sync::Arc;

use axum::http::request::Parts;
use futures::future::BoxFuture;

use super::{AudienceId, AuthError, AuthUser, LoginResponse, ProviderId};
use crate::auth::ProviderDoc;

/// Static route-audience coverage used for deterministic provider dispatch.
#[derive(Clone, Debug)]
pub(crate) enum ProviderAudienceSet {
    Any,
    Only(Arc<[AudienceId]>),
}

impl ProviderAudienceSet {
    /// Builds a finite non-empty audience set and rejects duplicate declarations.
    pub(crate) fn only(mut values: Vec<AudienceId>) -> Result<Self, AuthError> {
        values.sort_unstable();
        let original_len = values.len();
        values.dedup();
        if values.is_empty() || values.len() > 64 || values.len() != original_len {
            return Err(AuthError::InvalidProviderConfig(
                "provider audiences must contain between 1 and 64 unique values".into(),
            ));
        }
        Ok(Self::Only(values.into()))
    }

    /// Returns whether this provider may authenticate the local route audience.
    pub(crate) fn supports(&self, audience: &AudienceId) -> bool {
        match self {
            Self::Any => true,
            Self::Only(values) => values.binary_search(audience).is_ok(),
        }
    }

    /// Returns finite audience coverage, or `None` for an unrestricted provider.
    pub(crate) fn restricted(&self) -> Option<&[AudienceId]> {
        match self {
            Self::Any => None,
            Self::Only(values) => Some(values),
        }
    }

    /// Rejects login or refresh audiences outside the provider's static coverage.
    pub(crate) fn validate_requested(&self, audiences: &[AudienceId]) -> Result<(), AuthError> {
        if audiences.iter().all(|audience| self.supports(audience)) {
            Ok(())
        } else {
            Err(AuthError::AudienceMismatch)
        }
    }
}

#[derive(Clone)]
pub(crate) struct ProviderRuntime(pub(crate) Arc<dyn ProviderRuntimeContract>);

pub(crate) trait ProviderRuntimeContract: Send + Sync {
    fn id(&self) -> &ProviderId;
    fn audiences(&self) -> &ProviderAudienceSet;
    fn access_location(&self) -> &crate::auth::CredentialLocation;
    fn refresh_location(&self) -> Option<&crate::auth::CredentialLocation>;
    fn capabilities(&self) -> ProviderCapabilities;
    fn openapi(&self) -> ProviderDoc;
    #[cfg(feature = "mcp")]
    fn protected_resource(
        &self,
        _audience: &AudienceId,
    ) -> Option<crate::auth::AuthProtectedResource> {
        None
    }
    #[cfg(feature = "mcp")]
    fn challenge(&self, _error: &AuthError) -> Option<crate::auth::AuthChallenge> {
        None
    }
    fn authenticate<'a>(
        &'a self,
        raw: &'a str,
        parts: &'a Parts,
        audience: &'a AudienceId,
    ) -> BoxFuture<'a, Result<AuthUser, AuthError>>;
    fn login<'a>(
        &'a self,
        user: AuthUser,
        audiences: Vec<AudienceId>,
        binding: Option<String>,
    ) -> BoxFuture<'a, Result<LoginResponse, AuthError>>;
    fn refresh<'a>(
        &'a self,
        raw: &'a str,
        parts: &'a Parts,
        audiences: &'a [AudienceId],
    ) -> BoxFuture<'a, Result<LoginResponse, AuthError>>;
    fn logout<'a>(&'a self, parts: &'a Parts) -> BoxFuture<'a, Result<ResponseHeaders, AuthError>>;
}

pub(crate) type ResponseHeaders = Vec<(axum::http::HeaderName, axum::http::HeaderValue)>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ProviderCapabilities {
    pub(crate) authenticate: bool,
    pub(crate) login: bool,
    pub(crate) refresh: bool,
    pub(crate) logout: bool,
}

impl ProviderRuntime {
    pub(crate) fn new(value: impl ProviderRuntimeContract + 'static) -> Self {
        Self(Arc::new(value))
    }
}
