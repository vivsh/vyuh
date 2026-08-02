//! Private runtime contract shared by framework-owned credential providers.

use std::sync::Arc;

use axum::http::request::Parts;
use futures::future::BoxFuture;

use super::{AudienceId, AuthError, AuthUser, LoginResponse, ProviderId};
use crate::auth::ProviderDoc;

#[derive(Clone)]
pub(super) struct ProviderRuntime(pub(super) Arc<dyn ProviderRuntimeContract>);

pub(super) trait ProviderRuntimeContract: Send + Sync {
    fn id(&self) -> &ProviderId;
    fn access_location(&self) -> &crate::auth::CredentialLocation;
    fn refresh_location(&self) -> Option<&crate::auth::CredentialLocation>;
    fn capabilities(&self) -> ProviderCapabilities;
    fn openapi(&self) -> ProviderDoc;
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

pub(super) type ResponseHeaders = Vec<(axum::http::HeaderName, axum::http::HeaderValue)>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ProviderCapabilities {
    pub(super) authenticate: bool,
    pub(super) login: bool,
    pub(super) refresh: bool,
    pub(super) logout: bool,
}

impl ProviderRuntime {
    pub(super) fn new(value: impl ProviderRuntimeContract + 'static) -> Self {
        Self(Arc::new(value))
    }
}
